"""List generated source-context files for read-only connected sources."""
from __future__ import annotations

from pathlib import Path

_SUPPORTED_SUFFIXES = {".md", ".yml", ".yaml", ".jsonl", ".txt"}
_MAX_FILES = 80


def list_source_context(source: str | None = None, query: str | None = None) -> str:
    """List generated source context files under `.seeknal/context/sources`.

    Use this before answering questions that need database documentation,
    relationship hints, profiling, or generated table context.  The returned
    paths can be loaded with `read_source_context`.

    Args:
        source: Optional source name/namespace filter, e.g. `warehouse`.
        query: Optional case-insensitive substring filter for paths.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    root = ctx.project_path / ".seeknal" / "context" / "sources"
    if not root.exists():
        return "No generated source context found. Run `seeknal source sync --project <project>` first."

    source_filter = (source or "").strip().lower()
    query_filter = (query or "").strip().lower()
    entries: list[tuple[Path, str]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in _SUPPORTED_SUFFIXES:
            continue
        rel = path.relative_to(root)
        rel_text = rel.as_posix()
        if source_filter and not rel.parts[0].lower().startswith(source_filter):
            continue
        if query_filter and query_filter not in rel_text.lower():
            continue
        entries.append((rel, _first_hint(path)))

    if not entries:
        detail = []
        if source_filter:
            detail.append(f"source={source_filter}")
        if query_filter:
            detail.append(f"query={query_filter}")
        suffix = f" for {'; '.join(detail)}" if detail else ""
        return f"No source context files found{suffix}."

    lines = [f"## Source context files ({len(entries)} available)", ""]
    for rel, hint in entries[:_MAX_FILES]:
        if hint:
            lines.append(f"- `{rel.as_posix()}` — {hint}")
        else:
            lines.append(f"- `{rel.as_posix()}`")
    if len(entries) > _MAX_FILES:
        lines.append(f"- ... {len(entries) - _MAX_FILES} more files; use `query` to narrow")
    lines.append("")
    lines.append("Load a file with `read_source_context(path='<listed path>')`.")
    return "\n".join(lines)


def _first_hint(path: Path) -> str:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                line = raw.strip().lstrip("#").strip()
                if line:
                    return line[:140]
    except Exception:
        return ""
    return ""


__all__ = ["list_source_context"]
