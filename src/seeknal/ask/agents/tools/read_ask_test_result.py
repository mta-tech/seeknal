"""Read a saved Seeknal Ask SQL test run result."""

from __future__ import annotations

from pathlib import Path

_MAX_BYTES = 96 * 1024


def read_ask_test_result(name_or_path: str = "latest") -> str:
    """Read a saved Ask SQL QA result by path, filename/stem, or `latest`.

    Args:
        name_or_path: Result path/name, or `latest` for the newest run.
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.testing import resolve_ask_sql_result_path

    candidate = Path(name_or_path or "latest")
    if candidate.is_absolute() or ".." in candidate.parts:
        return "Error: name_or_path must be relative and cannot contain '..'."

    ctx = get_tool_context()
    path = resolve_ask_sql_result_path(ctx.project_path, name_or_path or "latest")
    if path is None:
        return (
            f"Error: Ask SQL test result not found or ambiguous: `{name_or_path}`. "
            "Use `list_ask_test_results` first."
        )

    data = path.read_bytes()
    truncated = len(data) > _MAX_BYTES
    if truncated:
        data = data[:_MAX_BYTES]
    text = data.decode("utf-8", errors="replace")
    header = (
        f"# Ask SQL test result: "
        f"{path.relative_to(ctx.project_path.resolve()).as_posix()}\n\n"
    )
    if truncated:
        return header + text + "\n\n... (truncated)"
    return header + text


__all__ = ["read_ask_test_result"]
