"""List reusable prompt-to-SQL examples stored in the project."""
from __future__ import annotations

import re
from pathlib import Path

_SUPPORTED_SUFFIXES = {".yml", ".yaml", ".md"}
_MAX_ITEMS = 80


def list_sql_pairs(query: str | None = None) -> str:
    """List reusable SQL-pair examples for this project.

    Search locations:
    - `seeknal/sql_pairs/`
    - `context/sql_pairs/`

    Args:
        query: Optional case-insensitive substring filter across file path and hint.
    """
    from seeknal.ask.agents.tools._context import get_tool_context, mark_sql_pairs_checked

    ctx = get_tool_context()
    mark_sql_pairs_checked(ctx)
    project_root = ctx.project_path.resolve()
    roots = [project_root / "seeknal" / "sql_pairs", project_root / "context" / "sql_pairs"]
    q = (query or "").strip().lower()
    entries: list[tuple[str, str]] = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in _SUPPORTED_SUFFIXES:
                continue
            rel = path.relative_to(project_root).as_posix()
            hint = _sql_pair_hint(path)
            haystack = f"{rel}\n{hint}\n{_sql_pair_search_text(path)}".lower()
            if q and not _matches_query(haystack, q):
                continue
            entries.append((rel, hint))

    if not entries:
        return "No SQL pairs found. Add examples under `seeknal/sql_pairs/` or `context/sql_pairs/`."

    lines = [f"## SQL pairs ({len(entries)} available)", ""]
    for rel, hint in entries[:_MAX_ITEMS]:
        lines.append(f"- `{rel}`" + (f" — {hint}" if hint else ""))
    if len(entries) > _MAX_ITEMS:
        lines.append(f"- ... {len(entries) - _MAX_ITEMS} more; use `query` to narrow")
    lines.append("")
    lines.append(
        "Execute a relevant pair with "
        "`execute_sql_pair(name_or_path='<listed path or stem>')`; use "
        "`read_sql_pair(...)` only when you need to inspect its notes first."
    )
    return "\n".join(lines)


def _sql_pair_hint(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    for raw in text.splitlines():
        line = raw.strip().strip("#").strip()
        if not line or line in {"---", "```"}:
            continue
        if line.lower().startswith(("name:", "prompt:", "intent:", "description:")):
            return line[:140]
        if path.suffix.lower() == ".md":
            return line[:140]
    return ""


def _sql_pair_search_text(path: Path) -> str:
    """Return bounded full-file text for matching without changing display."""
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:8000]
    except Exception:
        return ""


def _matches_query(haystack: str, query: str) -> bool:
    """Match query terms across common filename separators.

    Agents and users naturally search for phrases like "jumlah permohonan",
    while project files often use slugs such as "jumlah_permohonan".  Treat
    underscores, hyphens, punctuation, and whitespace as equivalent without
    introducing domain-specific matching.
    """
    if query in haystack:
        return True
    normalized_haystack = _normalize_search_text(haystack)
    normalized_query = _normalize_search_text(query)
    if normalized_query and normalized_query in normalized_haystack:
        return True
    compact_haystack = normalized_haystack.replace(" ", "")
    compact_query = normalized_query.replace(" ", "")
    if compact_query and compact_query in compact_haystack:
        return True

    query_terms = [
        term
        for term in normalized_query.split()
        if len(term) > 2 and term not in _SEARCH_STOP_WORDS
    ]
    if not query_terms:
        return False
    haystack_terms = normalized_haystack.split()
    return all(
        any(
            haystack_term == query_term
            or haystack_term.startswith(query_term)
            or query_term.startswith(haystack_term)
            for haystack_term in haystack_terms
        )
        for query_term in query_terms
    )


def _normalize_search_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^0-9a-zA-Z]+", " ", value)).strip().lower()


_SEARCH_STOP_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "by",
    "for",
    "how",
    "in",
    "is",
    "many",
    "of",
    "on",
    "or",
    "per",
    "the",
    "to",
    "what",
    "yang",
    "berapa",
    "dan",
    "di",
    "ke",
    "berdasarkan",
}


__all__ = ["list_sql_pairs"]
