"""Write Seeknal project-prep assets safely.

This tool is intentionally narrower than a general coding-agent filesystem
writer.  It lets Ask project-setup mode create or update durable Seeknal Ask
assets (agent config, context notes, SQL pairs, Ask tests, and project skills)
without granting arbitrary repository write access.
"""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse

import yaml

_SUPPORTED_SUFFIXES_BY_ROOT = {
    "context": {".md", ".yml", ".yaml", ".txt"},
    "seeknal/sql_pairs": {".yml", ".yaml"},
    "seeknal/tests": {".yml", ".yaml"},
}
_TOP_LEVEL_FILES = {"SEEKNAL_ASK.md", "seeknal_agent.yml"}
_MAX_CONTENT_BYTES = 100 * 1024
_SAFE_SEGMENT_RE = re.compile(r"^[a-zA-Z0-9_\-. ]+$")
_SECRET_KEY_RE = re.compile(
    r"(?i)(api[_-]?key|secret|token|password|passwd|pwd|dsn|database[_-]?url)\s*[:=]"
)
_BEARER_RE = re.compile(r"(?i)bearer\s+[a-z0-9._~+/=-]{12,}")
_URL_RE = re.compile(r"[a-z][a-z0-9+.-]*://[^\s'\"]+")


def write_seeknal_project_asset(
    path: str,
    content: str,
    overwrite: bool = False,
) -> str:
    """Create or update a durable Seeknal project-prep asset.

    Allowed targets are deliberately scoped to Ask/project-setup assets:

    - ``SEEKNAL_ASK.md``
    - ``seeknal_agent.yml``
    - ``context/**/*.md|yml|yaml|txt``
    - ``seeknal/sql_pairs/**/*.yml|yaml``
    - ``seeknal/tests/**/*.yml|yaml``
    - ``seeknal/skills/<skill-name>/SKILL.md``

    Use ``draft_node``/``dry_run_draft``/``apply_draft`` for managed pipeline
    definitions instead of writing ``seeknal/sources`` or ``seeknal/transforms``
    directly.

    Args:
        path: Project-relative path from the allowed set above. No absolute
            paths or ``..`` traversal.
        content: UTF-8 text content. Secrets, DSNs, and credential-looking
            values are rejected.
        overwrite: Existing files are protected unless this is ``True``.

    Returns:
        A confirmation with the written relative path, or ``Error: ...``.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    project_root = ctx.project_path.resolve()

    validation = _validate_write_request(path, content)
    if validation.startswith("Error:"):
        return validation
    rel_path = validation

    target = (project_root / rel_path).resolve()
    try:
        target.relative_to(project_root)
    except ValueError:
        return f"Error: resolved path escapes project root: {target}"

    if target.exists() and not overwrite:
        return (
            f"Error: `{rel_path}` already exists. Re-read it first, then call "
            "write_seeknal_project_asset(..., overwrite=True) only when the "
            "replacement is intentional."
        )

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with ctx.fs_lock:
            target.write_text(content, encoding="utf-8")
    except OSError as exc:
        return f"Error: failed to write `{rel_path}`: {exc}"

    try:
        rel_display = target.relative_to(project_root).as_posix()
    except ValueError:
        rel_display = rel_path
    return f"Wrote `{rel_display}` ({len(content.encode('utf-8')):,} bytes)."


def _validate_write_request(path: str, content: str) -> str:
    if not path or not isinstance(path, str):
        return "Error: path is required."
    if content is None or not isinstance(content, str):
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
            "Use .env for real credentials and store only env var names such "
            "as dsn_env: WAREHOUSE_URL in committed project assets."
        )

    raw = path.strip()
    if raw.startswith("/") or raw.startswith("\\") or Path(raw).is_absolute():
        return f"Error: path `{path}` must be relative to the project root."
    if ".." in raw.split("/") or ".." in raw.split("\\"):
        return f"Error: path `{path}` contains '..' — traversal not allowed."

    normalized = Path(raw).as_posix()
    parts = Path(normalized).parts
    for part in parts:
        if not _SAFE_SEGMENT_RE.match(part):
            return (
                f"Error: invalid path segment `{part}`. Use letters, digits, "
                "dots, hyphens, underscores, or spaces."
            )

    if normalized in _TOP_LEVEL_FILES:
        if normalized == "seeknal_agent.yml":
            config_error = _validate_seeknal_agent_yml(content)
            if config_error:
                return config_error
        return normalized

    if normalized.startswith("seeknal/skills/") and normalized.endswith("/SKILL.md"):
        if len(Path(normalized).parts) != 4:
            return "Error: project skills must use `seeknal/skills/<skill-name>/SKILL.md`."
        return normalized

    suffix = Path(normalized).suffix.lower()
    for root, suffixes in _SUPPORTED_SUFFIXES_BY_ROOT.items():
        prefix = f"{root}/"
        if normalized.startswith(prefix):
            if suffix not in suffixes:
                return (
                    f"Error: unsupported extension `{suffix}` for `{root}`. "
                    f"Supported: {', '.join(sorted(suffixes))}."
                )
            if normalized == prefix.rstrip("/"):
                return f"Error: `{normalized}` is a directory, not a file."
            return normalized

    return (
        "Error: unsupported path. Allowed targets: SEEKNAL_ASK.md, "
        "seeknal_agent.yml, context/**, seeknal/sql_pairs/**, "
        "seeknal/tests/**, and seeknal/skills/<skill-name>/SKILL.md."
    )


def _validate_seeknal_agent_yml(content: str) -> str | None:
    try:
        data = yaml.safe_load(content)
    except yaml.YAMLError as exc:
        return f"Error: seeknal_agent.yml is not valid YAML: {exc}"
    if data is not None and not isinstance(data, dict):
        return "Error: seeknal_agent.yml must be a YAML mapping."
    return None


def _looks_like_secret(text: str) -> bool:
    if _SECRET_KEY_RE.search(text) or _BEARER_RE.search(text):
        return True
    for match in _URL_RE.finditer(text):
        parsed = urlparse(match.group(0))
        if parsed.password:
            return True
        if parsed.username and parsed.scheme in {
            "postgres",
            "postgresql",
            "mysql",
            "mariadb",
            "redshift",
            "snowflake",
            "clickhouse",
            "mongodb",
            "redis",
        }:
            return True
    return False


__all__ = ["write_seeknal_project_asset"]
