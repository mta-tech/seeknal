"""List context files tool — discovers project-specific domain knowledge.

Scans ``{project}/context/`` for ``.md / .yml / .yaml / .txt`` files and
returns their relative paths plus a one-line hint so the agent knows
which file to load via ``read_project_file``.

Keeps the agent's context lean by *listing* rather than *embedding* —
the system prompt stays small, files load on demand when relevant.
"""

from __future__ import annotations

from pathlib import Path

_SUPPORTED_SUFFIXES = {".md", ".yml", ".yaml", ".txt"}
_MAX_FILES_SHOWN = 50
_HINT_MAX_LEN = 140


def list_context_files() -> str:
    """List domain-knowledge files under ``{project}/context/``.

    Returns a markdown list of relative paths plus a short hint pulled
    from each file's first non-empty line. Use this before writing SQL
    against tables with encoded values, or when answering questions
    about business metrics / glossary terms — then call
    ``read_project_file`` on the specific file.

    Extensions scanned: .md, .yml, .yaml, .txt. Returns a clear message
    when ``context/`` does not exist or is empty.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    context_dir = ctx.project_path / "context"

    if not context_dir.exists():
        return (
            f"No `context/` directory at {ctx.project_path}. "
            "Ask the data team to add glossaries, join patterns, or metric "
            "definitions there (see `write_project_file`), then call this "
            "again."
        )
    if not context_dir.is_dir():
        return f"`{context_dir}` exists but is not a directory."

    entries = []
    for path in sorted(context_dir.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in _SUPPORTED_SUFFIXES:
            continue
        # Skip hidden / dotfiles
        if any(part.startswith(".") for part in path.relative_to(context_dir).parts):
            continue
        rel = path.relative_to(ctx.project_path)
        hint = _first_line_hint(path)
        entries.append((rel, hint))

    if not entries:
        return (
            f"`{context_dir}` is empty. Supported extensions: "
            f"{', '.join(sorted(_SUPPORTED_SUFFIXES))}. Use "
            "`write_project_file(path='context/glossary.md', content=...)` "
            "to create one."
        )

    lines = [f"## Context files ({len(entries)} available)", ""]
    for rel, hint in entries[:_MAX_FILES_SHOWN]:
        if hint:
            lines.append(f"- `{rel}` — {hint}")
        else:
            lines.append(f"- `{rel}`")
    if len(entries) > _MAX_FILES_SHOWN:
        lines.append("")
        lines.append(
            f"(showing {_MAX_FILES_SHOWN} of {len(entries)}; narrow the "
            "scan by descending into a subdirectory via `read_project_file`.)"
        )
    lines.append("")
    lines.append(
        "Load a file's full content via `read_project_file('<path>')`."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _first_line_hint(path: Path) -> str:
    """Read the first non-empty, non-comment-only line of a text file."""
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.strip()
                if not line:
                    continue
                # Strip leading markdown heading markers for readability
                line = line.lstrip("#").strip()
                if not line:
                    continue
                if len(line) > _HINT_MAX_LEN:
                    line = line[: _HINT_MAX_LEN - 1] + "…"
                return line
    except Exception:
        return ""
    return ""


__all__ = ["list_context_files"]
