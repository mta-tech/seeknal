"""Hooks for seeknal ask agent.

Implements Claude Code-style PRE_TOOL_USE hooks for SQL validation
and POST_TOOL_USE hooks for self-correction hint injection.

The SQL validation logic lives in security.py (unchanged) — this module
wires it into pydantic-deep's hook lifecycle.
"""

import json
import logging
import re

from pydantic_deep.capabilities.hooks import Hook, HookEvent, HookInput, HookResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# PRE_TOOL_USE: SQL security validation (existing)
# ---------------------------------------------------------------------------


async def _sql_security_handler(hook_input: HookInput) -> HookResult:
    """Validate SQL queries before execution via PRE_TOOL_USE hook.

    Calls validate_sql_for_agent() from security.py. On validation failure,
    returns HookResult(allow=False) which triggers ModelRetry — the agent
    sees the denial reason and can adjust its query.

    The entire handler is wrapped in try/except to prevent unhandled
    exceptions from propagating up and failing the tool call entirely.
    """
    try:
        from seeknal.ask.security import validate_sql_for_agent

        sql = hook_input.tool_input.get("sql", "")
        validate_sql_for_agent(sql)
        return HookResult(allow=True)
    except ValueError as e:
        return HookResult(allow=False, reason=f"SQL validation error: {e}")
    except Exception as e:
        return HookResult(allow=False, reason=f"Unexpected validation error: {e}")


# ---------------------------------------------------------------------------
# POST_TOOL_USE: SQL self-correction hints
# ---------------------------------------------------------------------------

# Common DuckDB syntax hints for known error patterns.
_SYNTAX_HINTS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(r"trailing semicolon|unterminated", re.IGNORECASE),
        "DuckDB rejects trailing semicolons — remove the ';' at the end of the query.",
    ),
    (
        re.compile(r"INTERVAL", re.IGNORECASE),
        "Use CAST('2024-01-01' AS TIMESTAMP) before INTERVAL arithmetic.",
    ),
    (
        re.compile(r"GROUP BY", re.IGNORECASE),
        "All non-aggregate SELECT columns must appear in GROUP BY.",
    ),
]


async def _sql_self_correction_handler(hook_input: HookInput) -> HookResult:
    """Enrich SQL error results with correction hints via POST_TOOL_USE hook.

    Parses the tool result for structured error JSON (from errors.py).
    For retryable errors, injects additional hints such as available table
    names or DuckDB syntax tips so the agent can self-correct.

    Returns HookResult() unchanged for non-error / non-JSON results.
    The entire handler is wrapped in try/except — it must never propagate
    exceptions.
    """
    try:
        tool_result = hook_input.tool_result
        if not tool_result:
            return HookResult()

        # Try to parse as structured error JSON
        try:
            error_data = json.loads(tool_result)
        except (json.JSONDecodeError, TypeError):
            return HookResult()

        if not isinstance(error_data, dict) or "category" not in error_data:
            return HookResult()

        category = error_data.get("category", "")
        message = error_data.get("message", "")
        hint = error_data.get("hint") or ""

        if category == "retryable_missing_ref":
            hint = _enrich_missing_ref_hint(message, hint)
        elif category == "retryable_syntax":
            hint = _enrich_syntax_hint(message, hint)

        if hint != (error_data.get("hint") or ""):
            error_data["hint"] = hint
            return HookResult(modified_result=json.dumps(error_data))

        return HookResult()

    except Exception:
        logger.debug("Self-correction hook error (suppressed)", exc_info=True)
        return HookResult()


def _enrich_missing_ref_hint(message: str, existing_hint: str) -> str:
    """Try to append available table names to the hint for missing-ref errors."""
    try:
        from seeknal.ask.agents.tools._context import get_tool_context

        ctx = get_tool_context()
        with ctx.db_lock:
            columns, rows = ctx.repl.execute_oneshot("SHOW TABLES")
        if rows:
            table_names = [str(row[0]) for row in rows]
            tables_str = ", ".join(table_names)
            suffix = f"Available tables: {tables_str}"
            if existing_hint:
                return f"{existing_hint}. {suffix}"
            return suffix
    except Exception:
        pass
    return existing_hint


def _enrich_syntax_hint(message: str, existing_hint: str) -> str:
    """Check for common DuckDB syntax patterns and append fix suggestions."""
    additions: list[str] = []
    for pattern, suggestion in _SYNTAX_HINTS:
        if pattern.search(message):
            additions.append(suggestion)

    if not additions:
        return existing_hint

    suffix = " ".join(additions)
    if existing_hint:
        return f"{existing_hint}. {suffix}"
    return suffix


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_ask_hooks() -> list[Hook]:
    """Return all hooks for the seeknal ask agent.

    Includes:
    - PRE_TOOL_USE: SQL security validation
    - POST_TOOL_USE: SQL self-correction hints
    """
    return [
        Hook(
            event=HookEvent.PRE_TOOL_USE,
            handler=_sql_security_handler,
            matcher="execute_sql",
        ),
        Hook(
            event=HookEvent.POST_TOOL_USE,
            handler=_sql_self_correction_handler,
            matcher="execute_sql",
        ),
    ]


# Backward-compatible alias
get_security_hooks = get_ask_hooks
