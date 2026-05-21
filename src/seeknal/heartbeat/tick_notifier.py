"""Proactive Telegram notifier for heartbeat tick summaries.

Sends a single short message to a configured Telegram chat after every
heartbeat tick so operators see ingest/DAG/exposure activity without
polling ``runs.jsonl``. Best-effort: any failure is logged at WARNING
and swallowed — a Telegram outage must never crash the heartbeat daemon
(Principle 4: best-effort over fail-stop).

The module uses ``urllib.request`` from stdlib to call Telegram's HTTP
Bot API directly. It does NOT import the ``python-telegram-bot``
library and does NOT import any code under ``seeknal.ask.gateway`` —
this keeps the heartbeat daemon's deterministic core insulated from
the agent surface (Principle 5: file boundary) and satisfies the lint
contract in ``tests/heartbeat/test_telegram_bridge.py::
test_no_telegram_imports_in_heartbeat``.

Configuration (environment variables, read at notification time):

- ``TELEGRAM_BOT_TOKEN``: the bot's API token. Required; absent → no-op.
- ``HEARTBEAT_TELEGRAM_CHAT_ID``: the target chat. Required; absent → no-op.
- ``HEARTBEAT_TELEGRAM_NOTIFY``: optional toggle. ``"0"`` / ``"false"``
  / ``"off"`` disables notifications even when token and chat are set.
  Any other value (or unset) enables them when both required vars exist.

The notifier is intentionally configured via env vars rather than
``HEARTBEAT.md`` because the chat id is operator-specific (one chat
per deployment) while the rest of HEARTBEAT.md describes project-level
behavior shared across deployments.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

# Hard cap on message length. Telegram's documented limit is 4096
# characters for plain text; staying well under that leaves room for
# any HTML escaping. Truncated messages get a "(truncated)" suffix.
_MAX_TEXT_LEN = 3500

# Hard timeout on the Telegram POST. We never want to block a tick on
# an unresponsive network — observability is a nice-to-have, not a
# blocker for the deterministic core.
_TIMEOUT_SECONDS = 8.0


def _disabled_via_env() -> bool:
    """Return True when ``HEARTBEAT_TELEGRAM_NOTIFY`` explicitly disables."""
    raw = os.environ.get("HEARTBEAT_TELEGRAM_NOTIFY")
    if raw is None:
        return False
    return raw.strip().lower() in {"0", "false", "off", "no", "disabled"}


def _resolve_credentials() -> tuple[str | None, str | None]:
    """Return ``(token, chat_id)`` from env, or ``(None, None)`` if absent."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("HEARTBEAT_TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        return None, None
    return token, chat_id


def format_tick_message(run: Any, project_name: str | None = None) -> str:
    """Render a HeartbeatRun into a compact human-readable Telegram message.

    Defensive against partial run records — every field is read via
    ``getattr`` so a future schema addition (e.g. classifier entries)
    does not crash the notifier on older records.
    """
    status = getattr(run, "status", "unknown")
    duration_ms = getattr(run, "duration_ms", 0)
    ingested = getattr(run, "ingested", []) or []
    dag = getattr(run, "dag", None)
    exposures = getattr(run, "exposure", []) or []
    errors = getattr(run, "errors", []) or []
    run_id = getattr(run, "run_id", "")[:12]  # short for readability

    icon = {"ok": "🫀", "partial": "⚠️", "skipped": "⏸"}.get(status, "❔")
    header_proj = f" — {project_name}" if project_name else ""
    lines = [f"{icon} Heartbeat{header_proj} ({status})"]
    lines.append(f"run: {run_id}  ·  {duration_ms}ms")

    if ingested:
        ok = [i for i in ingested if getattr(i, "status", "") == "ok"]
        bad = [i for i in ingested if getattr(i, "status", "") != "ok"]
        if ok:
            for it in ok[:3]:
                table = getattr(it, "table", "?")
                rows = getattr(it, "rows", 0)
                lines.append(f"+ {table} ({rows} rows)")
            if len(ok) > 3:
                lines.append(f"  ... +{len(ok) - 3} more ingested")
        if bad:
            for it in bad[:3]:
                fname = getattr(it, "file", "?")
                reason = getattr(it, "reason", None) or getattr(it, "error", "?")
                # Keep just basename so the line stays short.
                fname_short = fname.rsplit("/", 1)[-1] if "/" in fname else fname
                lines.append(f"! quarantined: {fname_short} ({reason})")
    else:
        lines.append("- no new files")

    if dag is not None:
        nodes_attempted = getattr(dag, "nodes_attempted", 0)
        nodes_failed = getattr(dag, "nodes_failed", 0)
        if nodes_failed:
            lines.append(f"DAG: {nodes_failed} failed / {nodes_attempted} attempted")
        elif nodes_attempted:
            lines.append(f"DAG: {nodes_attempted} ok")

    if exposures:
        ok_exp = sum(1 for e in exposures if getattr(e, "status", "") == "ok")
        fail_exp = len(exposures) - ok_exp
        if fail_exp:
            lines.append(f"exposure: {ok_exp} ok, {fail_exp} failed")
        elif ok_exp:
            lines.append(f"exposure: {ok_exp} ok")

    if errors:
        # Cap the error block so a noisy traceback doesn't blow the message budget.
        for err in errors[:2]:
            err_short = str(err)
            if len(err_short) > 200:
                err_short = err_short[:197] + "..."
            lines.append(f"! {err_short}")
        if len(errors) > 2:
            lines.append(f"  ... +{len(errors) - 2} more errors")

    text = "\n".join(lines)
    if len(text) > _MAX_TEXT_LEN:
        text = text[: _MAX_TEXT_LEN - 14] + "\n(truncated)"
    return text


def notify_tick(run: Any, project_name: str | None = None) -> bool:
    """Best-effort Telegram notification for one heartbeat tick.

    Returns ``True`` when the message was POSTed successfully, ``False``
    otherwise (including the no-op cases: missing env vars, explicitly
    disabled, or network/API failure). Never raises.

    The caller (``HeartbeatRunner._tick_locked``) ignores the return
    value — observability is best-effort by design.
    """
    if _disabled_via_env():
        return False

    token, chat_id = _resolve_credentials()
    if token is None or chat_id is None:
        return False

    try:
        text = format_tick_message(run, project_name=project_name)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[heartbeat] tick message formatting failed: %s", exc)
        return False

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = urllib.parse.urlencode(
        {"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            data = json.loads(body) if body else {}
        if data.get("ok") is True:
            return True
        logger.warning(
            "[heartbeat] Telegram API returned ok=false: %s",
            data.get("description", body[:200]),
        )
        return False
    except (urllib.error.URLError, urllib.error.HTTPError, OSError, json.JSONDecodeError) as exc:
        logger.warning("[heartbeat] Telegram notification failed: %s", exc)
        return False


__all__ = ["format_tick_message", "notify_tick"]
