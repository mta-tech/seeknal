"""Read project file tool — read any file in the project securely."""

from langchain_core.tools import tool

from seeknal.ask.agents.tools._security import MAX_FILE_SIZE, validate_project_path


def _do_read(path: str, project_path, start_line: int = 1, max_lines: int = 200) -> str:
    """Core read logic, testable without @tool decorator."""
    try:
        resolved = validate_project_path(path, project_path)
    except ValueError as e:
        return f"Error: {e}"

    if not resolved.exists():
        return f"File not found: {path}"

    if not resolved.is_file():
        return f"Not a file: {path}"

    # Check file size
    try:
        size = resolved.stat().st_size
    except OSError as e:
        return f"Cannot read file: {e}"

    if size > MAX_FILE_SIZE:
        return f"File too large ({size:,} bytes, max {MAX_FILE_SIZE:,} bytes): {path}"

    # Try reading as text (block binary files)
    try:
        content = resolved.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return f"Cannot read binary file: {path}"
    except OSError as e:
        return f"Error reading file: {e}"

    lines = content.splitlines()
    total_lines = len(lines)

    # Apply pagination
    start_idx = max(0, start_line - 1)
    end_idx = start_idx + max_lines
    selected = lines[start_idx:end_idx]

    if not selected:
        return f"No content at line {start_line} (file has {total_lines} lines)"

    # Format with line numbers
    numbered = []
    for i, line in enumerate(selected, start=start_idx + 1):
        numbered.append(f"{i:4d} | {line}")

    # Detect language for syntax highlighting hint
    suffix = resolved.suffix.lstrip(".")
    lang = {"py": "python", "yml": "yaml", "yaml": "yaml", "sql": "sql",
            "toml": "toml", "json": "json", "md": "markdown"}.get(suffix, "")

    result = f"```{lang}\n" + "\n".join(numbered) + "\n```"

    if end_idx < total_lines:
        result += f"\n\n({total_lines - end_idx} more lines. Use start_line={end_idx + 1} to continue.)"

    return result


@tool
def read_project_file(
    path: str,
    start_line: int = 1,
    max_lines: int = 200,
) -> str:
    """Read any file in the project by its relative path.

    View source code, configuration files, scripts, or any text file
    in the project to understand how data is processed or configured.

    Use this for non-pipeline files (configs, scripts, notebooks).
    Use read_pipeline for pipeline definition files specifically.

    Args:
        path: File path relative to the project root.
        start_line: Line number to start reading from (default 1).
        max_lines: Maximum number of lines to return (default 200).
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    return _do_read(path, ctx.project_path, start_line, max_lines)
