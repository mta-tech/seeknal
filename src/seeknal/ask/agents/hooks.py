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
# CSV export — no hook here by design
# ---------------------------------------------------------------------------
#
# There used to be a POST_TOOL_USE hook that auto-uploaded the result of
# every `execute_sql` call with at least one row. It has been removed
# entirely, not just disabled. The problem: it fired after EVERY query, not
# just the one behind the final answer. A single question typically runs
# several queries before landing on the right one (checking a date range,
# profiling a min/max, trying a filter that turns out wrong) — each of those
# got its own upload. Confirmed live: a "monthly trend" question produced
# two separate Download buttons, one for the real trend and one for a
# throwaway `SELECT MAX(...)` profiling query the agent ran along the way.
# The hook had no way to know which query the answer was actually about —
# it just fired on all of them.
#
# CSV export is still fully automatic — it just moved from "a hook watching
# tool calls" to "a step in the agent's own answering workflow"
# (`bpom-analyst/SKILL.md`): once the agent has verified its answer and is
# about to write it, it calls `upload_to_s3(filename, sql=...)` itself,
# exactly once, with the SQL it knows its own answer is about. That's a
# judgment only the agent can make correctly — a hook watching tool calls
# from outside can't tell "exploratory query" from "the query behind the
# answer" without understanding the conversation. Contrast with
# `run_forecast` (`forecast.py`), which always produces exactly one
# canonical dataset (the projection points) and can safely upload it
# deterministically in code, no agent decision needed — there's no
# equivalent single "the data" for a general data question. `upload_to_s3`
# itself is unchanged — same repair/row-cap/governance parity as
# `execute_sql`, same structured error shape on failure so the
# self-correction hook below still enriches its errors.
#
# Known tradeoff: this makes CSV export dependent on the agent following its
# own workflow instruction rather than a hook that fires unconditionally.
# An end-of-turn safety net (catch the case where the agent forgot) was
# considered, but the mechanism it would need — a hook that runs once after
# the whole turn finishes — isn't actually wired into the agent's execution
# loop in this codebase (verified directly in the installed hook framework:
# the method exists but nothing ever calls it), so it was not implemented
# rather than shipping something that silently does nothing.


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_ask_hooks(config: dict | None = None) -> list[Hook]:
    """Return all hooks for the seeknal ask agent.

    Includes:
    - PRE_TOOL_USE: SQL security validation — execute_sql AND upload_to_s3
      (audit fix, 2026-07-09: upload_to_s3's Mode 1 executes agent-supplied
      SQL through the same DuckDB seam as execute_sql, but was never covered
      by this hook -- its `sql=` argument bypassed read-only/dangerous-
      function validation entirely. `_sql_security_handler` already reads
      `tool_input.get("sql")`, matching upload_to_s3's parameter name as-is;
      only the matcher needed extending).
    - POST_TOOL_USE: SQL self-correction hints — execute_sql AND upload_to_s3
      (audit fix, 2026-07-09: `upload_to_s3` Mode 1 now returns the same
      `format_tool_error`/`classify_duckdb_error` JSON shape execute_sql
      does on failure, so this hook's existing generic hint-enrichment
      applies to both without new logic).

    `visualize_chart` is covered by both for the same reason `upload_to_s3` is:
    its Mode 1 runs agent-supplied SQL through the same DuckDB seam, and it
    names the argument `sql`, so the existing handlers apply unchanged.

    No CSV-upload hook lives here anymore: `execute_sql` no longer
    auto-uploads per call (see the module comment above this function for
    why); `run_forecast` self-uploads its own projection points from inside
    `forecast.py`; regular data answers are exported by the agent calling
    `upload_to_s3` explicitly, once, per `bpom-analyst/SKILL.md`.
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
                matcher="execute_sql|upload_to_s3|run_forecast|detect_anomaly|visualize_chart",
            )
        )
    if cfg.get("sql_self_correction", True):
        hooks.append(
            Hook(
                event=HookEvent.POST_TOOL_USE,
                handler=_sql_self_correction_handler,
                matcher="execute_sql|upload_to_s3|run_forecast|detect_anomaly|visualize_chart",
            )
        )
    return hooks


# Backward-compatible alias
get_security_hooks = get_ask_hooks
