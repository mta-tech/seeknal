"""Execute a project-owned prompt-to-SQL example exactly as stored."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.agents.tools.read_sql_pair import _resolve_sql_pair


def execute_sql_pair(
    name_or_path: str,
    limit: int = 100,
    refresh: bool = False,
) -> str:
    """Load a SQL pair by name/path and execute its ``sql`` field.

    Use this when a relevant project-owned SQL pair exists. It avoids manual
    copy/paste drift: the SQL executed is exactly the SQL stored in the pair.

    Args:
        name_or_path: SQL-pair listed path, file name, or stem.
        limit: Maximum rows to return through ``execute_sql``.
        refresh: Re-run even when the same normalized SQL already succeeded.
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
    get_loaded_sql_pairs(ctx)[pair_name] = sql
    result = execute_sql(sql=sql, limit=limit, refresh=refresh)
    if _tool_succeeded(result):
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
    return f"# Executed SQL pair: {rel}\n\n" + result


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


__all__ = ["execute_sql_pair"]
