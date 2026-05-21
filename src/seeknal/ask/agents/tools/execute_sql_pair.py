"""Execute a project-owned prompt-to-SQL example exactly as stored."""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.agents.tools.read_sql_pair import _resolve_sql_pair


def execute_sql_pair(
    name_or_path: str,
    limit: int = 100,
    refresh: bool = False,
    authoritative: bool = False,
) -> str:
    """Load a SQL pair by name/path and execute its ``sql`` field.

    Use this when a relevant project-owned SQL pair exists. It avoids manual
    copy/paste drift: the SQL executed is exactly the SQL stored in the pair.
    Set ``authoritative=True`` only when the pair's prompt, filters, and output
    grain directly match the user's current question. Leave it false when the
    pair is only a related example or partial match.

    Args:
        name_or_path: SQL-pair listed path, file name, or stem.
        limit: Maximum rows to return through ``execute_sql``.
        refresh: Re-run even when the same normalized SQL already succeeded.
        authoritative: Stop-after-answer hint for exact/direct pair matches.
    """
    from seeknal.ask.agents.tools._context import (
        get_loaded_sql_pairs,
        get_tool_context,
        mark_sql_pairs_checked,
        record_authoritative_sql_pair_result,
    )
    from seeknal.ask.agents.tools.errors import RETRYABLE_SYNTAX, format_tool_error

    if not name_or_path or not isinstance(name_or_path, str):
        return format_tool_error(
            RETRYABLE_SYNTAX,
            "name_or_path is required.",
            hint="Call list_sql_pairs first, then pass the listed path or stem.",
        )
    candidate = Path(name_or_path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return format_tool_error(
            RETRYABLE_SYNTAX,
            "name_or_path must be relative and cannot contain '..'.",
            hint="Use a listed SQL-pair path such as seeknal/sql_pairs/example.yml.",
        )

    ctx = get_tool_context()
    project_root = ctx.project_path.resolve()
    target = _resolve_sql_pair(project_root, name_or_path.strip())
    if target is None:
        return format_tool_error(
            RETRYABLE_SYNTAX,
            f"SQL pair not found or ambiguous: {name_or_path!r}.",
            hint="Use list_sql_pairs(query=...) and pass one listed path or exact stem.",
        )

    try:
        data = yaml.safe_load(target.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001 - surface as tool error
        return format_tool_error(
            RETRYABLE_SYNTAX,
            f"Could not read SQL pair YAML: {exc}",
            hint="Fix the SQL-pair YAML, then retry.",
        )
    if not isinstance(data, dict):
        return format_tool_error(
            RETRYABLE_SYNTAX,
            "SQL pair YAML must contain a mapping.",
            hint="Expected fields include name, prompt, intent, sql, notes.",
        )

    sql = data.get("sql")
    if not isinstance(sql, str) or not sql.strip():
        return format_tool_error(
            RETRYABLE_SYNTAX,
            "SQL pair does not define a non-empty sql field.",
            hint="Add sql: | ... to the SQL-pair file.",
        )

    rel = target.relative_to(project_root).as_posix()
    pair_name = _pair_name(data, rel)
    mark_sql_pairs_checked(ctx)
    if authoritative:
        get_loaded_sql_pairs(ctx)[pair_name] = sql
    result = execute_sql(sql=sql, limit=limit, refresh=refresh)
    if authoritative and _tool_succeeded(result) and not _result_looks_empty(result):
        record_authoritative_sql_pair_result(
            name=pair_name,
            path=rel,
            result=result,
            ctx=ctx,
        )
        return (
            f"# Executed SQL pair: {rel}\n\n"
            "AUTHORITATIVE_RESULT: Use this SQL-pair result to answer the "
            "current business question. Do not run alternate SQL unless the "
            "user explicitly requested a different grain/filter or deeper "
            "post-query analysis.\n\n"
            + result
        )
    if authoritative and _tool_succeeded(result) and _result_looks_empty(result):
        return (
            f"# Executed SQL pair: {rel}\n\n"
            "SQL_PAIR_RESULT (not authoritative — empty/null-only result; the "
            "pair's SQL likely has a bug, e.g. a wrongly quoted qualified table "
            "name). Inspect the SQL and run a corrected query with execute_sql "
            "instead of treating this pair as authoritative.\n\n"
            + result
        )
    return (
        f"# Executed SQL pair: {rel}\n\n"
        "SQL_PAIR_RESULT: Treat this as trusted project-owned evidence. "
        "If the user's question asks for a different grain, dimension, filter, "
        "or deeper analysis than this pair returns, adapt the SQL or inspect "
        "another pair instead of answering from this result alone.\n\n"
        + result
    )


def _pair_name(data: dict[str, Any], fallback: str) -> str:
    name = data.get("name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return fallback


def _tool_succeeded(result: str) -> bool:
    stripped = (result or "").strip()
    if not stripped:
        return False
    if not stripped.startswith("{"):
        return True
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return True
    return not (isinstance(payload, dict) and "category" in payload)


_AGG_NULL_ROW = re.compile(
    r"\|\s*(?:NULL|0|0\.0+|\[blank\])\s*(?:\|\s*(?:NULL|0|0\.0+|\[blank\])\s*)*\|"
)


def _result_looks_empty(result: str) -> bool:
    """Detect a successful query whose payload is effectively empty.

    A SQL pair that silently produces zero rows or an all-NULL/zero aggregate
    is almost never a trustworthy authoritative answer for a business
    question — most often it indicates a pair-level SQL bug (e.g. a wrongly
    quoted fully-qualified table name that DuckDB resolves as a missing
    relation but the surrounding UNION ALL hides). Demote those results so
    the agent does not lock the turn to wrong numbers.
    """
    text = result or ""
    if "Query executed successfully but returned no results." in text:
        return True
    if "(0 rows)" in text or "(0 row)" in text:
        return True
    # Single-row aggregate where every cell is NULL/0/[blank] is the
    # silent-failure shape of a busted UNION-ALL leg.
    if "(1 row)" in text:
        lines = [ln for ln in text.splitlines() if ln.startswith("|") and "---" not in ln]
        # Skip the header row; check the lone data row(s).
        data_rows = lines[1:] if len(lines) >= 2 else []
        if data_rows and all(_AGG_NULL_ROW.fullmatch(row.strip()) for row in data_rows):
            return True
    return False


__all__ = ["execute_sql_pair"]
