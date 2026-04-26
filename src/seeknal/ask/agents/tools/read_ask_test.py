"""Read one project-local Seeknal Ask SQL test."""

from __future__ import annotations

from pathlib import Path

_MAX_BYTES = 64 * 1024


def read_ask_test(name_or_path: str) -> str:
    """Read an Ask SQL QA test by name, filename, stem, or relative path.

    Args:
        name_or_path: Test name or path listed by `list_ask_tests`.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.testing import resolve_ask_sql_test_path

    if not name_or_path or not isinstance(name_or_path, str):
        return "Error: name_or_path is required."
    candidate = Path(name_or_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return "Error: name_or_path must be relative and cannot contain '..'."

    ctx = get_tool_context()
    path = resolve_ask_sql_test_path(ctx.project_path, name_or_path.strip())
    if path is None:
        return f"Error: Ask SQL test not found or ambiguous: `{name_or_path}`. Use `list_ask_tests` first."

    data = path.read_bytes()
    truncated = len(data) > _MAX_BYTES
    if truncated:
        data = data[:_MAX_BYTES]
    text = data.decode("utf-8", errors="replace")
    header = (
        f"# Ask SQL test: {path.relative_to(ctx.project_path.resolve()).as_posix()}\n\n"
    )
    if truncated:
        return header + text + "\n\n... (truncated)"
    return header + text


__all__ = ["read_ask_test"]
