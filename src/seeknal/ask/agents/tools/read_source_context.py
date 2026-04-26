"""Read generated source-context files for read-only connected sources."""
from __future__ import annotations

from pathlib import Path

_MAX_BYTES = 64 * 1024
_SUPPORTED_SUFFIXES = {".md", ".yml", ".yaml", ".jsonl", ".txt"}


def read_source_context(path: str) -> str:
    """Read one file listed by `list_source_context`.

    Args:
        path: Relative path under `.seeknal/context/sources`, for example
            `warehouse/tables/analytics.orders/columns.md`.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    if not path or not isinstance(path, str):
        return "Error: path is required."
    if Path(path).is_absolute() or ".." in Path(path).parts:
        return "Error: path must be relative and cannot contain '..'."

    ctx = get_tool_context()
    root = (ctx.project_path / ".seeknal" / "context" / "sources").resolve()
    target = (root / path.strip()).resolve()
    try:
        target.relative_to(root)
    except ValueError:
        return "Error: path escapes source context root."
    if target.suffix.lower() not in _SUPPORTED_SUFFIXES:
        return f"Error: unsupported file extension `{target.suffix}`."
    if not target.exists() or not target.is_file():
        return f"Error: source context file not found: `{path}`."

    data = target.read_bytes()
    truncated = len(data) > _MAX_BYTES
    if truncated:
        data = data[:_MAX_BYTES]
    text = data.decode("utf-8", errors="replace")
    header = f"# Source context: {target.relative_to(root).as_posix()}\n\n"
    if truncated:
        return header + text + "\n\n... (truncated)"
    return header + text


__all__ = ["read_source_context"]
