"""Save preference tool — append a durable user preference to preferences.yml.

Preferences are one-line statements that are auto-loaded into the
system prompt at every session start. Use when the user says things
like *"always filter test accounts"*, *"remember that revenue = gmv"*,
or *"from now on, prefer GROUP BY over DISTINCT"*.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

_PREF_MAX_LEN = 500
_PREF_MIN_LEN = 4
_PREFERENCES_FILENAME = "preferences.yml"
_SECRET_KEY_RE = re.compile(
    r"(?i)(api[_-]?key|secret|token|password|passwd|pwd|dsn|database[_-]?url)\s*[:=]"
)
_BEARER_RE = re.compile(r"(?i)bearer\s+[a-z0-9._~+/=-]{12,}")
_URL_RE = re.compile(r"[a-z][a-z0-9+.-]*://[^\s'\"]+")


def _looks_like_secret(text: str) -> bool:
    if _SECRET_KEY_RE.search(text) or _BEARER_RE.search(text):
        return True
    for match in _URL_RE.finditer(text):
        parsed = urlparse(match.group(0))
        if (
            parsed.password
            or parsed.username
            and parsed.scheme
            in {
                "postgres",
                "postgresql",
                "mysql",
                "mariadb",
                "redshift",
                "snowflake",
                "clickhouse",
                "mongodb",
                "redis",
            }
        ):
            return True
    return False


def save_preference(preference: str) -> str:
    """Append a user preference to ``{project}/preferences.yml``.

    The preference persists across sessions and is injected into the
    system prompt next run. Use only for statements the user explicitly
    asked to remember.

    Args:
        preference: One short sentence describing the preference. E.g.
            "Always join orders to customers via orders.customer_id =
            customers.id" or "Revenue means gmv column, not the revenue
            column".
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    if not preference or not isinstance(preference, str):
        return "Error: preference is required."
    cleaned = preference.strip()
    if len(cleaned) < _PREF_MIN_LEN:
        return f"Error: preference too short (minimum {_PREF_MIN_LEN} characters)."
    if len(cleaned) > _PREF_MAX_LEN:
        return (
            f"Error: preference is {len(cleaned)} chars, exceeds "
            f"{_PREF_MAX_LEN} limit. Write a focused domain rule, not a "
            "conversation summary."
        )
    if _looks_like_secret(cleaned):
        return (
            "Error: preference appears to contain a secret or connection string. "
            "Do not save passwords, API keys, tokens, DSNs, or DATABASE_URL values."
        )

    # YAML safety: escape backslashes and quotes; strip newlines.
    cleaned = cleaned.replace("\\", "\\\\").replace('"', '\\"').replace("\n", " ")

    path = ctx.project_path / _PREFERENCES_FILENAME
    existing = _read_existing(path)
    # Dedupe — preferences should be idempotent.
    if any(cleaned == line.strip() for line in existing):
        return f"Preference already present; skipped duplicate:\n  {cleaned}"

    existing.append(cleaned)

    with ctx.fs_lock:
        _write_preferences(path, existing)

    return (
        f"Saved to `{_PREFERENCES_FILENAME}` ({len(existing)} preferences "
        "total). Will be injected into the system prompt on the next "
        "session start."
    )


# ---------------------------------------------------------------------------
# Loader used both by this tool and by create_agent() at session init
# ---------------------------------------------------------------------------


def load_preferences(project_path) -> list[str]:
    """Read preferences.yml from a project path. Returns [] if missing or bad."""
    from pathlib import Path as _Path

    path = _Path(project_path) / _PREFERENCES_FILENAME
    return _read_existing(path)


# ---------------------------------------------------------------------------
# Internal helpers — minimal YAML I/O, no ruamel dependency
# ---------------------------------------------------------------------------


_PREF_LINE_RE = re.compile(r'^\s*-\s*["\']?(.*?)["\']?\s*$')


def _read_existing(path) -> list[str]:
    """Parse the bulleted list under ``preferences:`` — tolerates minor drift."""
    if not path.exists() or not path.is_file():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []
    out: list[str] = []
    in_block = False
    for raw in text.splitlines():
        stripped = raw.strip()
        if stripped.startswith("preferences:"):
            in_block = True
            continue
        if in_block:
            if not stripped:
                continue
            if not stripped.startswith("-"):
                # Any other top-level key ends the block.
                if not raw.startswith(" ") and not raw.startswith("\t"):
                    break
                continue
            m = _PREF_LINE_RE.match(raw)
            if m:
                val = m.group(1).strip()
                # Unescape basic sequences we wrote out
                val = val.replace('\\"', '"').replace("\\\\", "\\")
                if val:
                    out.append(val)
    return out


def _write_preferences(path, entries: list[str]) -> None:
    lines = ["preferences:"]
    for entry in entries:
        lines.append(f'  - "{entry}"')
    # Trailing newline — YAML convention.
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


__all__ = ["save_preference", "load_preferences"]
