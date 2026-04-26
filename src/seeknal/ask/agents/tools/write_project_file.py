"""Write project file tool — persist agent-authored domain knowledge.

Scope: writes are restricted to ``{project}/context/`` to keep pipeline
code and agent output strictly separated. Path traversal and oversized
content are rejected. Use this for glossaries, join-pattern notes,
metric definitions, lessons-learned summaries — anything the agent
should be able to re-read in a future session.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

_SUPPORTED_SUFFIXES = {".md", ".yml", ".yaml", ".txt"}
_MAX_CONTENT_BYTES = 100 * 1024  # 100 KB per file
_SAFE_SEGMENT_RE = re.compile(r"^[a-zA-Z0-9_\-. ]+$")
_SECRET_KEY_RE = re.compile(
    r"(?i)(api[_-]?key|secret|token|password|passwd|pwd|dsn|database[_-]?url)\s*[:=]"
)
_BEARER_RE = re.compile(r"(?i)bearer\s+[a-z0-9._~+/=-]{12,}")
_URL_RE = re.compile(r"[a-z][a-z0-9+.-]*://[^\s'\"]+")


def write_project_file(path: str, content: str) -> str:
    """Write ``content`` to ``{project}/context/{path}``.

    Use this to persist domain knowledge the agent discovered this
    session — a glossary entry, a newly-understood join pattern, a
    summarized lessons-learned doc. The next session's
    ``list_context_files`` will surface it automatically.

    Args:
        path: Path relative to ``context/`` (e.g. ``glossary.md`` or
            ``metric_definitions/revenue.md``). Must contain only
            letters, digits, underscores, dots, hyphens, spaces, and
            directory separators. No leading slashes, no ``..``.
        content: Text to write. Capped at 100 KB.

    Returns:
        A confirmation with the final absolute path, or an ``Error: ...``
        line on validation failure.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    if not path or not isinstance(path, str):
        return "Error: path is required."

    raw = path.strip()
    if not raw:
        return "Error: path is required."

    # Reject absolute paths BEFORE any normalisation — "/etc/passwd.md" must
    # not be silently coerced into a relative path by stripping the leading
    # slash.
    if raw.startswith("/") or raw.startswith("\\") or Path(raw).is_absolute():
        return f"Error: path '{path}' must be relative to context/."

    normalised = raw

    # Reject path traversal.
    if ".." in normalised.split("/") or ".." in normalised.split("\\"):
        return f"Error: path '{path}' contains '..' — traversal not allowed."

    parts = Path(normalised).parts
    for part in parts:
        if not _SAFE_SEGMENT_RE.match(part):
            return (
                f"Error: invalid path segment '{part}'. Use letters, digits, "
                "dots, hyphens, underscores, or spaces."
            )

    candidate = Path(normalised)
    if candidate.suffix.lower() not in _SUPPORTED_SUFFIXES:
        return (
            f"Error: unsupported extension '{candidate.suffix}'. "
            f"Supported: {', '.join(sorted(_SUPPORTED_SUFFIXES))}."
        )

    if content is None:
        return "Error: content is required."
    if not isinstance(content, str):
        return "Error: content must be a string."
    if not content.strip():
        return "Error: content is empty."
    body = content.encode("utf-8")
    if len(body) > _MAX_CONTENT_BYTES:
        return (
            f"Error: content is {len(body)} bytes, exceeds "
            f"{_MAX_CONTENT_BYTES} byte limit."
        )
    if _looks_like_secret(content):
        return (
            "Error: content appears to contain a secret or connection string. "
            "Do not save passwords, API keys, tokens, DSNs, or DATABASE_URL values."
        )

    project_root = ctx.project_path.resolve()
    context_root = project_root / "context"
    try:
        context_root.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return f"Error: cannot create context/ directory: {exc}"

    target = (context_root / normalised).resolve()
    # Defense in depth: confirm the resolved path is still under context/.
    try:
        target.relative_to(context_root.resolve())
    except ValueError:
        return f"Error: resolved path escapes context/: {target}"

    target.parent.mkdir(parents=True, exist_ok=True)
    with ctx.fs_lock:
        target.write_text(content, encoding="utf-8")

    rel = target.relative_to(project_root)
    return (
        f"Wrote `{rel}` ({len(body):,} bytes). "
        "Available to future sessions via `list_context_files` and "
        "`read_project_file`."
    )


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


__all__ = ["write_project_file"]
