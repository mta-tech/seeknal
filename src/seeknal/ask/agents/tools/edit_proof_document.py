"""Apply a full-document rewrite to an existing Proof Editor document.

Uses the `rewrite.apply` op on `POST /api/agent/{slug}/ops`. Before the
rewrite we must first fetch `baseToken` from `GET /api/agent/{slug}/state`
(handled by `read_proof_document._do_read`).

This tool is gated by an explicit user confirmation — mirroring the
`publish_to_proof` flow. The agent MUST call `ask_user` with the edit-specific
option set before calling this tool.

See: https://github.com/EveryInc/proof-sdk (agent-docs — "Edit Via Ops")
"""

from __future__ import annotations

import os

import httpx

from seeknal.ask.agents.tools._proof_common import (
    parse_proof_url,
    sanitize,
    upgrade_scheme_if_remote,
    validate_base_url,
)
from seeknal.ask.agents.tools.publish_to_proof import resolve_proof_base_url
from seeknal.ask.agents.tools.read_proof_document import _do_read

_TIMEOUT_SECONDS = 30.0
_MAX_ERROR_BODY = 200


def _do_edit(
    url_or_slug: str,
    new_markdown: str,
    fallback_base: str,
    api_key: str | None,
) -> str:
    """Core edit logic. Returns a user-facing success or error string."""
    if not new_markdown or not new_markdown.strip():
        return "Error: new_markdown cannot be empty."

    # Parse URL separately so we can validate before doing any network I/O
    parsed = parse_proof_url(url_or_slug)
    if isinstance(parsed, str):
        return parsed

    base = parsed["base"] or fallback_base
    # Hosted Proof redirects http → https; httpx follows 3xx on POST as GET,
    # silently dropping the body. Upgrade the scheme BEFORE any call.
    base = upgrade_scheme_if_remote(base)
    slug = parsed["slug"]
    token = parsed["token"]

    base_error = validate_base_url(base)
    if base_error:
        return base_error

    # Fetch current state to get baseToken (required by rewrite.apply).
    # Re-pass the original URL but with the scheme upgraded so the nested read
    # doesn't hit an http redirect either.
    upgraded_url = url_or_slug
    try:
        from urllib.parse import urlparse as _up
        parsed_original = _up(url_or_slug)
        if parsed_original.scheme == "http" and (parsed_original.hostname or "").lower() not in {"localhost", "127.0.0.1", "::1", "0.0.0.0"}:
            upgraded_url = parsed_original._replace(scheme="https").geturl()
    except ValueError:
        pass
    state = _do_read(upgraded_url, fallback_base, api_key)
    if isinstance(state, str):
        # Error bubbled up from the read
        return f"Error: could not read current state before edit: {state[7:] if state.startswith('Error: ') else state}"

    base_token = state.get("base_token", "")
    revision = state.get("revision")

    # Detect PROJECTION_STALE — hosted Proof refuses all rewrites until the
    # projection catches up. Surface a clear, actionable error instead of
    # falling into the server's confusing "Invalid baseRevision" cascade.
    raw_state = state.get("raw", {}) if isinstance(state, dict) else {}
    warning = raw_state.get("warning") if isinstance(raw_state, dict) else None
    if revision is None and isinstance(warning, dict):
        code = warning.get("code", "")
        if code == "PROJECTION_STALE":
            return (
                "Error: Proof document is in PROJECTION_STALE state — the "
                "server is serving Yjs fallback content and the revision "
                "counter hasn't caught up yet. rewrite.apply will be rejected "
                "by the server until repair completes. Retry in a moment, or "
                "publish a fresh memo and edit that instead."
            )

    if revision is None and not base_token:
        return (
            "Error: Proof state response included neither a revision nor a "
            "baseToken. The rewrite has no precondition to anchor against."
        )

    headers = {
        "Content-Type": "application/json",
        "X-Agent-Id": "agent:seeknal",
    }
    auth_token = token or api_key
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    ops_url = f"{base.rstrip('/')}/api/agent/{slug}/ops"
    # Per proof agent-docs, rewrite.apply takes `content` (not `markdown`).
    # Send both so self-hosted forks that still expect `markdown` keep working.
    payload: dict[str, object] = {
        "type": "rewrite.apply",
        "by": "ai:seeknal",
        "content": new_markdown,
        "markdown": new_markdown,
    }
    # Precondition: hosted Proof requires EXACTLY ONE of baseRevision /
    # baseToken / baseUpdatedAt for rewrite.apply and rejects combinations
    # with a CONFLICTING_BASE error. baseRevision is the most widely accepted
    # form; fall back to baseToken only if revision is unavailable.
    revision = state.get("revision")
    if revision is not None:
        payload["baseRevision"] = revision
    elif base_token:
        payload["baseToken"] = base_token

    try:
        with httpx.Client(timeout=_TIMEOUT_SECONDS, follow_redirects=True) as client:
            response = client.post(ops_url, json=payload, headers=headers)
    except httpx.TimeoutException:
        return (
            f"Error: Proof edit request timed out after {_TIMEOUT_SECONDS:.0f}s "
            f"({sanitize(ops_url, max_len=120)})."
        )
    except httpx.RequestError as exc:
        return (
            f"Error: failed to reach Proof at {sanitize(ops_url, max_len=120)}: "
            f"{sanitize(str(exc), max_len=200)}"
        )

    if response.status_code in (401, 403):
        return (
            "Error: Proof rejected the edit (auth). The share URL must include "
            "a ?token=... granting write access, or set PROOF_API_KEY."
        )
    if response.status_code == 404:
        return (
            f"Error: Proof document not found: slug "
            f"'{sanitize(slug, max_len=80)}' at {sanitize(base, max_len=120)}."
        )
    if response.status_code == 409:
        snippet = sanitize(response.text or "", max_len=_MAX_ERROR_BODY)
        return (
            "Error: Proof returned 409 CONFLICT — document is likely open by "
            f"live collaborators or the baseToken went stale. {snippet}"
        )
    if response.status_code >= 400:
        snippet = sanitize(response.text or "", max_len=_MAX_ERROR_BODY)
        return (
            f"Error: Proof returned HTTP {response.status_code} from "
            f"{sanitize(ops_url, max_len=120)}. {snippet}"
        )

    try:
        data = response.json()
    except ValueError:
        data = {}

    share_url = state.get("share_url") or f"{base.rstrip('/')}/d/{slug}"
    lines = [
        "Proof document updated via rewrite.apply!",
        "",
        f"Share link: {sanitize(share_url, max_len=500)}",
        f"Slug: {sanitize(slug, max_len=80)}",
        f"New markdown length: {len(new_markdown)} chars",
    ]
    if isinstance(data, dict):
        if data.get("success") is False:
            lines.append(
                f"Warning: server responded success=false — raw: "
                f"{sanitize(str(data), max_len=200)}"
            )
        for key in ("revision", "baseToken", "updatedAt"):
            value = data.get(key)
            if value:
                lines.append(f"{key}: {sanitize(value, max_len=120)}")
    return "\n".join(lines)


async def edit_proof_document(
    url: str,
    new_markdown: str,
) -> str:
    """Rewrite the full markdown body of an existing Proof Editor document.

    See the `edit-proof-document` skill for the read→diff→ask→edit flow, the
    approval-gate menu (discriminator: "Apply edit to Proof"), the
    PROOF_BASE_URL / PROOF_API_KEY environment variables, and the
    LIVE_CLIENTS_PRESENT / PROJECTION_STALE / 409 CONFLICT error paths.

    CAUTION: `rewrite.apply` is disruptive. Hosted Proof rejects when live
    collaborators are present.

    Args:
        url: Proof share URL (preferred, includes ?token=...) or bare slug.
        new_markdown: The complete new markdown body for the document.
    """
    from seeknal.ask.agents.tools._context import require_proof_edit_approval

    guard_error = require_proof_edit_approval("edit_proof_document")
    if guard_error:
        return guard_error

    fallback_base = resolve_proof_base_url(None)
    api_key = os.environ.get("PROOF_API_KEY") or None

    return _do_edit(
        url_or_slug=url,
        new_markdown=new_markdown,
        fallback_base=fallback_base,
        api_key=api_key,
    )
