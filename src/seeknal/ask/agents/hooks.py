"""Hooks for seeknal ask agent.

Implements Claude Code-style PRE_TOOL_USE hooks for SQL validation
and POST_TOOL_USE hooks for self-correction hint injection.

The SQL validation logic lives in security.py (unchanged) — this module
wires it into pydantic-deep's hook lifecycle.
"""

import json
import logging
import os
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

        # execute_sql accepts either `sql` or the `query` alias; mirror that fallback
        # here so the PRE_TOOL_USE hook validates the SQL the tool will actually run.
        # (Without this, a model that passes only `query=` yields sql=None and
        # validate_sql_for_agent(None) raises AttributeError, hard-blocking every query.)
        tool_input = hook_input.tool_input or {}
        sql = tool_input.get("sql") or tool_input.get("query") or ""
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
# POST_TOOL_USE: CSV upload reminder (FC2d)
# ---------------------------------------------------------------------------

# Reminder threshold (module-level — HookInput carries no config object).
_CSV_REMINDER_MIN_ROWS = int(os.environ.get("SEEKNAL_CSV_REMINDER_MIN_ROWS", "20"))

# Count markdown-table data rows: lines like "| 123 | ..." (skip separators).
_TABLE_ROW_RE = re.compile(r"^\|\s*[^:\-][^|]*\|", re.MULTILINE)


def _count_result_rows(tool_result: str) -> int:
    """Best-effort row count from an execute_sql markdown table result.

    execute_sql returns a markdown table; data rows match the pattern while
    separator rows (``|---|``) and the "N rows" footer do not. The header row
    DOES match the pattern, so when a separator is present (the standard
    execute_sql shape) we subtract it. Returns 0 on any parse trouble — the
    hook then no-ops (safe default).
    """
    if not tool_result:
        return 0
    try:
        matches = _TABLE_ROW_RE.findall(tool_result)
        has_separator = bool(re.search(r"^\|[\s:\-]+\|", tool_result, re.MULTILINE))
        count = len(matches)
        if has_separator and count > 0:
            count -= 1  # exclude the header row
        return max(0, count)
    except TypeError:
        return 0


async def _csv_upload_reminder_handler(hook_input: HookInput) -> HookResult:
    """Nudge the agent to offer upload_to_s3 when execute_sql returns many rows.

    Reminder ONLY — never enforces. Appends a short hint to the tool result via
    ``modified_result``; the agent decides whether to act on it. Triggers on
    ``execute_sql`` exclusively. Independent of ``_sql_self_correction_handler``
    (different condition; both run).
    """
    try:
        if hook_input.tool_name != "execute_sql" or not hook_input.tool_result:
            return HookResult()
        rows = _count_result_rows(hook_input.tool_result)
        if rows < _CSV_REMINDER_MIN_ROWS:
            return HookResult()
        nudge = (
            f"\n\n_({rows} rows returned — consider offering "
            f"`upload_to_s3(filename=..., sql=...)` if the user wants a CSV export.)_"
        )
        return HookResult(modified_result=hook_input.tool_result + nudge)
    except Exception:  # noqa: BLE001 - a hook must never propagate failures
        logger.exception("[csv_reminder_hook] unexpected error; passing through")
        return HookResult()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_ask_hooks(config: dict | None = None) -> list[Hook]:
    """Return all hooks for the seeknal ask agent.

    Includes:
    - PRE_TOOL_USE: SQL security validation
    - POST_TOOL_USE: SQL self-correction hints
    - POST_TOOL_USE: CSV upload reminder (FC2d, execute_sql only)
    """
    cfg = config or {}
    if cfg.get("enabled", True) is False:
        return []

    hooks: list[Hook] = []
    if cfg.get("sql_security", True):
        hooks.append(
            Hook(
                event=HookEvent.PRE_TOOL_USE,
                handler=_sql_security_handler,
                matcher="execute_sql",
            )
        )
    if cfg.get("sql_self_correction", True):
        hooks.append(
            Hook(
                event=HookEvent.POST_TOOL_USE,
                handler=_sql_self_correction_handler,
                matcher="execute_sql",
            )
        )
    if cfg.get("csv_upload_reminder", True):
        hooks.append(
            Hook(
                event=HookEvent.POST_TOOL_USE,
                handler=_csv_upload_reminder_handler,
                matcher="execute_sql",
            )
        )
    return hooks


# Backward-compatible alias
get_security_hooks = get_ask_hooks
