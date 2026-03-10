"""Search project files tool — regex search across all project files."""

import fnmatch
import os
import re
from pathlib import Path
from typing import Iterator

from langchain_core.tools import tool

from seeknal.ask.agents.tools._security import (
    BLOCKED_FILES,
    EXCLUDED_DIRS,
    EXCLUDED_EXTENSIONS,
    MAX_SCAN_FILE_SIZE,
)

# Maximum files to scan before stopping (ReDoS / performance protection)
_MAX_FILES_TO_SCAN = 5000


def _walk_project(root: Path) -> Iterator[Path]:
    """Walk project tree, skipping excluded directories and binary files."""
    try:
        with os.scandir(root) as entries:
            for entry in entries:
                if entry.name in EXCLUDED_DIRS:
                    continue
                if entry.name.lower() in BLOCKED_FILES:
                    continue
                if entry.is_dir(follow_symlinks=False):
                    yield from _walk_project(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    p = Path(entry.path)
                    if p.suffix in EXCLUDED_EXTENSIONS:
                        continue
                    if p.name.endswith(".egg-info"):
                        continue
                    yield p
    except PermissionError:
        pass


def _do_search(
    pattern: str,
    project_path: Path,
    file_pattern: str = "",
    max_results: int = 30,
) -> str:
    """Core search logic, testable without @tool decorator."""
    try:
        regex = re.compile(pattern)
    except re.error as e:
        return f"Invalid regex pattern: {e}"

    project_root = project_path.resolve()
    matches: list[str] = []
    files_scanned = 0

    for file_path in _walk_project(project_root):
        if files_scanned >= _MAX_FILES_TO_SCAN:
            break
        files_scanned += 1

        # Apply optional glob filter
        if file_pattern:
            rel = str(file_path.relative_to(project_root))
            if not fnmatch.fnmatch(rel, file_pattern) and not fnmatch.fnmatch(file_path.name, file_pattern):
                continue

        # Skip large files
        try:
            size = file_path.stat().st_size
        except OSError:
            continue
        if size > MAX_SCAN_FILE_SIZE:
            continue

        # Try reading as text (skip binary)
        try:
            content = file_path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue

        rel_path = str(file_path.relative_to(project_root))
        for i, line in enumerate(content.splitlines(), 1):
            if regex.search(line):
                display_line = line.strip()
                if len(display_line) > 200:
                    display_line = display_line[:200] + "..."
                matches.append(f"{rel_path}:{i}: {display_line}")
                if len(matches) >= max_results:
                    break
        if len(matches) >= max_results:
            break

    if not matches:
        return f"No matches found for pattern: {pattern}"

    header = f"Found {len(matches)} match{'es' if len(matches) != 1 else ''}:\n\n"
    return header + "\n".join(f"- `{m}`" for m in matches)


@tool
def search_project_files(
    pattern: str,
    file_pattern: str = "",
    max_results: int = 30,
) -> str:
    """Search across all project files using a regex pattern.

    Find where columns are defined, trace data lineage, or locate
    specific code patterns across YAML configs, Python scripts,
    and other project files.

    Use this for broad searches across the entire project.
    Use search_pipelines for structured pipeline metadata lookups.

    Args:
        pattern: A regex pattern to search for in file contents.
        file_pattern: Optional glob to filter files (e.g. "*.yml", "*.py").
        max_results: Maximum number of matching lines to return (default 30).
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    return _do_search(pattern, ctx.project_path, file_pattern, max_results)
