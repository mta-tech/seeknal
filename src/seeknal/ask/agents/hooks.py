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
import time

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
# POST_TOOL_USE: CSV auto-upload (FC2d r4)
# ---------------------------------------------------------------------------
#
# Design (r4): the hook AUTOMATICALLY calls upload_to_s3 when execute_sql
# returns a substantial result. The agent does NOT decide — from the user's
# perspective, every substantial data answer comes with a Download button
# without asking. This is the "tools csv otomatis upload" behavior.
#
# For run_forecast the hook cannot auto-upload (forecast.points are computed
# in the engine, not re-extractable from the markdown tool_result), so it
# falls back to a strong nudge that tells the agent to call upload_to_s3 in
# data mode with the projection rows.

# Auto-upload threshold. r4: default 5. r5: lowered to 1 — any non-empty
# tabular result (≥1 row) gets auto-uploaded. The user wants every tabular
# answer to have a Download button, even for small results.
# Override via the SEEKNAL_CSV_REMINDER_MIN_ROWS env var.
_CSV_REMINDER_MIN_ROWS = int(os.environ.get("SEEKNAL_CSV_REMINDER_MIN_ROWS", "1"))

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


def _derive_filename_from_sql(sql: str) -> str:
    """Best-effort CSV filename derived from the SQL query.

    Falls back to a timestamped generic name when introspection fails. The
    name only affects the user-facing Download label, not storage — the
    object key is namespaced under ``csv-exports/{uuid}/{filename}`` so
    collisions are impossible regardless.
    """
    table = "result"
    try:
        # Capture the full FROM target (e.g. "warehouse.public.t_produk_3_erba"),
        # then split on dots to get the bare table name. Word boundaries +
        # greedy [\w.]+ ensure we don't stop at the first segment.
        m = re.search(r"\bFROM\s+([\w\.]+)", sql, re.IGNORECASE)
        if m:
            table_full = m.group(1).strip("`\"'")
            table = table_full.split(".")[-1].lower()
            # Strip common table-name prefixes for tidier filenames. The hook
            # is generic; these patterns just produce cleaner names when present.
            table = re.sub(r"^(t_|tbl_|table_|ft_)", "", table) or "result"
    except Exception:
        table = "result"
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    return f"{table}-{ts}.csv"


async def _csv_upload_reminder_handler(hook_input: HookInput) -> HookResult:
    """Auto-upload substantial query results to S3; nudge for forecast points.

    Two branches (matcher=``execute_sql|run_forecast``):

    **execute_sql (Mode 1, AUTO-UPLOAD):** when the result has ≥
    ``_CSV_REMINDER_MIN_ROWS`` rows, the hook DIRECTLY calls
    ``upload_to_s3(filename, sql=<the agent's SQL>)``. No agent decision —
    the upload is automatic. ``ctx.pending_upload`` is set inside
    ``upload_to_s3``; the gateway then emits ``upload_complete`` and a
    Download button appears alongside the answer. The hook appends a short
    note to ``tool_result`` so the agent knows the CSV was uploaded (and
    can mention the download URL in its answer).

    **run_forecast (Mode 2, NUDGE):** the projection points live only in the
    tool_result markdown — they are computed by the engine, not SQL-queryable.
    The hook cannot re-execute them. It therefore nudges the agent to call
    ``upload_to_s3(filename, data=points, columns=[...])`` (Mode 2). Nudge
    only fires on forecast success (``## Ringkasan``); ``## Kesalahan`` and
    ``## Ditolak`` produce no exportable data and are skipped.

    Failures (storage unreachable, PUT rejected) are swallowed — the hook
    must never crash the agent turn. The agent is told via the modified
    result so it can mention the failure or retry.
    """
    try:
        tool_name = hook_input.tool_name
        result = hook_input.tool_result or ""
        if not result:
            return HookResult()

        if tool_name == "execute_sql":
            return await _auto_upload_sql_result(hook_input, result)

        if tool_name == "run_forecast":
            return _nudge_forecast_data_export(result)

        return HookResult()
    except Exception:  # noqa: BLE001 - hooks must never propagate failures
        logger.exception("[csv_upload_hook] unexpected error; passing through")
        return HookResult()


async def _auto_upload_sql_result(
    hook_input: HookInput, result: str
) -> HookResult:
    """Auto-upload Mode 1: re-execute the SQL via upload_to_s3.

    Called by ``_csv_upload_reminder_handler`` when execute_sql returns a
    substantial result. Imports upload_to_s3 lazily to avoid an import cycle
    (hooks.py is imported early in agent construction).
    """
    rows = _count_result_rows(result)
    if rows < _CSV_REMINDER_MIN_ROWS:
        return HookResult()

    # The SQL the agent used for this execute_sql call. Re-execute it inside
    # upload_to_s3 (Mode 1) to build the CSV — most queries hit PostgreSQL's
    # page cache on the second run, so the latency cost is acceptable.
    tool_input = hook_input.tool_input or {}
    sql = tool_input.get("sql") or tool_input.get("query") or ""
    if not sql:
        # No SQL captured (defensive) — fall back to a soft nudge so the agent
        # can offer the upload manually.
        nudge = (
            f"\n\n_({rows} rows returned — auto-upload skipped (no SQL captured). "
            f"Offer `upload_to_s3(filename=..., sql=...)` if the user wants a CSV.)_"
        )
        return HookResult(modified_result=result + nudge)

    filename = _derive_filename_from_sql(sql)
    try:
        # Lazy import to keep hooks.py import-time light and avoid any cycle.
        from seeknal.ask.agents.tools.upload_to_s3 import upload_to_s3

        upload_outcome = upload_to_s3(filename, sql=sql)
    except Exception as exc:
        # Storage unreachable / PUT rejected / etc — surface to agent, but
        # never crash the turn. The user still gets the query answer.
        nudge = (
            f"\n\n_({rows} rows returned — auto-upload failed: "
            f"{type(exc).__name__}. The user can still request CSV manually.)_"
        )
        return HookResult(modified_result=result + nudge)

    # Upload succeeded — tell the agent so it can mention the Download button.
    # upload_outcome deliberately carries no raw URL (see upload_to_s3.py) —
    # do not reconstruct one here either, or the agent will paste it into the
    # answer as a markdown link, duplicating/breaking the dedicated UI widget.
    nudge = f"\n\n_(Auto-uploaded {rows} rows to S3. {upload_outcome})_"
    return HookResult(modified_result=result + nudge)


def _nudge_forecast_data_export(result: str) -> HookResult:
    """Mode 2 nudge for run_forecast (cannot auto-upload — points are computed).

    Forecast success is marked by ``## Ringkasan``. ``## Kesalahan`` and
    ``## Ditolak`` produce no exportable data — skip the nudge so the agent
    focuses on the error/reason instead of offering CSV.
    """
    if "## Ringkasan" not in result:
        return HookResult()
    nudge = (
        "\n\n_(Forecast complete — automatically offer "
        "`upload_to_s3(filename=..., data=points, columns="
        "[\"period\",\"point\",\"lower_80\",\"upper_80\",\"lower_95\",\"upper_95\"])` "
        "so the user gets a Download button for the projection points.)_"
    )
    return HookResult(modified_result=result + nudge)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_ask_hooks(config: dict | None = None) -> list[Hook]:
    """Return all hooks for the seeknal ask agent.

    Includes:
    - PRE_TOOL_USE: SQL security validation (execute_sql only)
    - POST_TOOL_USE: SQL self-correction hints (execute_sql only)
    - POST_TOOL_USE: CSV upload reminder (FC2d) — fires after both
      ``execute_sql`` and ``run_forecast``; matcher is the regex
      ``"execute_sql|run_forecast"`` so pydantic_deep routes both tools to the
      single handler, which dispatches internally on ``tool_name``.
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
                # Regex matched against tool_name by pydantic_deep._match_hooks.
                # Extending to new data-producing tools = extend this regex,
                # then add a branch in _csv_upload_reminder_handler.
                matcher="execute_sql|run_forecast",
            )
        )
    return hooks


# Backward-compatible alias
get_security_hooks = get_ask_hooks
