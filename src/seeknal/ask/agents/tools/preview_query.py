"""Preview query tool — cheap pre-execution hooks to steer the agent.

Runs four diagnostic probes against a SELECT before the real query
executes:

1. **Row count pre-check** — ``SELECT COUNT(*) FROM (sql) AS _q``.
   If the count is huge, block execution and return guidance only; if
   large, warn but allow.
2. **Column count pre-check** — ``DESCRIBE SELECT * FROM (sql) AS _q``.
   Warns when the projection is wide.
3. **Fan-out detection** — when the query has a JOIN, compare the
   estimated result rows to the primary table's rows. If it multiplies
   beyond a threshold, warn.
4. **Schema/reachability dry-run** — run ``DESCRIBE`` first; if it fails
   the table is unreachable and we surface that cleanly with the list
   of currently registered schemas.

The tool is **opt-out-able** via ``run_hooks=False`` for known-small
queries (e.g. aggregations, LIMIT 1). Pure aggregations without row
output (``COUNT(*)``, ``SUM(...)`` at top level) and ``LIMIT 1`` queries
auto-skip the row-count check since it would cost more than it saves.
"""

from __future__ import annotations

import re

# Thresholds — tuneable but not per-call configurable on purpose.
_ROW_WARN_THRESHOLD = 10_000
_ROW_BLOCK_THRESHOLD = 100_000
_COLUMN_WARN_THRESHOLD = 50
_FANOUT_RATIO_THRESHOLD = 3.0  # result rows / primary rows
_FANOUT_MIN_ROWS = 1_000  # don't flag small queries

# Precompiled helpers
_LIMIT_1_RE = re.compile(r"\bLIMIT\s+1\s*$", re.IGNORECASE)
_PURE_AGG_RE = re.compile(
    r"""
    ^\s*SELECT\s+                   # SELECT
    (?:                             # one-or-more aggregate expressions
        (?:COUNT|SUM|AVG|MIN|MAX|MEDIAN|STDDEV|VARIANCE)\s*\([^)]*\)
        (?:\s+AS\s+[\w_]+)?
        \s*,?\s*
    )+
    \s+FROM\s                       # then FROM
    """,
    re.IGNORECASE | re.VERBOSE,
)
_FROM_TABLE_RE = re.compile(r"\bFROM\s+([\"\w\.]+)", re.IGNORECASE)
_HAS_JOIN_RE = re.compile(r"\bJOIN\b", re.IGNORECASE)


def preview_query(sql: str, run_hooks: bool = True) -> str:
    """Dry-run diagnostics for a SELECT before executing it.

    Returns a markdown report with a ``decision`` line:

    - ``OK`` — safe to execute as-is
    - ``WARN`` — large but fetchable; proceed with caution
    - ``BLOCK`` — would exceed the hard cap; refine the query first
    - ``ERROR`` — the query could not be analyzed (unreachable table, etc.)

    Use this before ``execute_sql`` when you're unsure how big a result
    will be, when a query spans JOINs, or when the user asks about a
    brand-new table.

    Args:
        sql: A DuckDB-compatible SELECT query (same dialect as execute_sql).
        run_hooks: If False, returns ``OK`` immediately without any probes.
            Use for known-small queries to save a round trip.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    trimmed = sql.strip().rstrip(";").strip()
    if not trimmed:
        return "ERROR: empty SQL."

    if not run_hooks:
        return "OK (hooks skipped by caller)."

    # Hooks are cheap for pure aggregations and LIMIT 1 queries — skip them.
    if _is_pure_aggregation(trimmed) or _LIMIT_1_RE.search(trimmed):
        return "OK (aggregation / LIMIT 1 — hooks skipped)."

    notices: list[str] = []
    decision = "OK"
    total_rows: int | None = None
    column_count: int | None = None

    # ── Hook 4: reachability dry-run ──
    schema_err = _dry_run_describe(ctx, trimmed)
    if schema_err is not None:
        return _format_report("ERROR", [schema_err], row_count=None, column_count=None)

    # ── Hook 2: column count ──
    try:
        column_count = _describe_columns(ctx, trimmed)
    except Exception as exc:
        notices.append(f"Column introspection failed: {exc}")

    if column_count is not None and column_count > _COLUMN_WARN_THRESHOLD:
        notices.append(
            f"Query projects {column_count} columns (>{_COLUMN_WARN_THRESHOLD}). "
            "Use `SELECT col1, col2, ...` to narrow. Call "
            "`describe_table('table_name')` to identify the columns you need."
        )
        decision = _escalate(decision, "WARN")

    # ── Hook 1: row count pre-check ──
    try:
        total_rows = _count_rows(ctx, trimmed)
    except Exception as exc:
        notices.append(f"Row pre-check failed: {exc}")

    if total_rows is not None:
        if total_rows >= _ROW_BLOCK_THRESHOLD:
            notices.append(
                f"Query would return {total_rows:,} rows "
                f"(>={_ROW_BLOCK_THRESHOLD:,}). Execution blocked. "
                "Add a WHERE clause to filter, or use GROUP BY to aggregate."
            )
            decision = "BLOCK"
        elif total_rows >= _ROW_WARN_THRESHOLD:
            notices.append(
                f"Query would return {total_rows:,} rows "
                f"(>={_ROW_WARN_THRESHOLD:,}). Consider a tighter WHERE or "
                "GROUP BY before calling `execute_sql`."
            )
            decision = _escalate(decision, "WARN")

    # ── Hook 3: fan-out detection ──
    if (
        decision != "BLOCK"
        and total_rows is not None
        and total_rows >= _FANOUT_MIN_ROWS
        and _HAS_JOIN_RE.search(trimmed)
    ):
        primary_rows = _primary_table_row_count(ctx, trimmed)
        if (
            primary_rows is not None
            and primary_rows > 0
            and total_rows / primary_rows >= _FANOUT_RATIO_THRESHOLD
        ):
            notices.append(
                f"JOIN appears to multiply rows: primary table has "
                f"{primary_rows:,} rows but the result has {total_rows:,} "
                f"(ratio {total_rows / primary_rows:.1f}x). This may indicate "
                "a many-to-many join without deduplication. Verify the join keys."
            )
            decision = _escalate(decision, "WARN")

    return _format_report(decision, notices, row_count=total_rows, column_count=column_count)


# ---------------------------------------------------------------------------
# Probe helpers
# ---------------------------------------------------------------------------


def _dry_run_describe(ctx, sql: str) -> str | None:
    """Return an error string if the query doesn't describe, else None."""
    wrapped = f"DESCRIBE SELECT * FROM ({sql}) AS _seek_q"
    try:
        with ctx.db_lock:
            ctx.repl.execute_oneshot(wrapped, limit=1)
    except Exception as exc:
        schemas = _list_schemas(ctx)
        schema_hint = (
            f" Accessible schemas: {', '.join(schemas)}."
            if schemas
            else ""
        )
        return (
            f"The query references something DuckDB can't resolve: {exc}."
            + schema_hint
            + " Call `list_tables()` to see available tables/views."
        )
    return None


def _describe_columns(ctx, sql: str) -> int:
    wrapped = f"DESCRIBE SELECT * FROM ({sql}) AS _seek_q"
    with ctx.db_lock:
        _cols, rows = ctx.repl.execute_oneshot(wrapped, limit=10_000)
    return len(rows)


def _count_rows(ctx, sql: str) -> int | None:
    wrapped = f"SELECT CAST(COUNT(*) AS BIGINT) AS c FROM ({sql}) AS _seek_q"
    with ctx.db_lock:
        _cols, rows = ctx.repl.execute_oneshot(wrapped, limit=1)
    if rows and rows[0] and rows[0][0] is not None:
        try:
            return int(rows[0][0])
        except (TypeError, ValueError):
            return None
    return None


def _primary_table_row_count(ctx, sql: str) -> int | None:
    """Extract the table immediately after FROM and COUNT(*) it."""
    match = _FROM_TABLE_RE.search(sql)
    if not match:
        return None
    table = match.group(1)
    if not _is_safe_identifier(table):
        return None
    wrapped = f"SELECT CAST(COUNT(*) AS BIGINT) AS c FROM {table}"
    try:
        with ctx.db_lock:
            _cols, rows = ctx.repl.execute_oneshot(wrapped, limit=1)
    except Exception:
        return None
    if rows and rows[0] and rows[0][0] is not None:
        try:
            return int(rows[0][0])
        except (TypeError, ValueError):
            return None
    return None


def _list_schemas(ctx) -> list[str]:
    try:
        with ctx.db_lock:
            _cols, rows = ctx.repl.execute_oneshot(
                "SELECT DISTINCT table_schema FROM information_schema.tables "
                "WHERE table_catalog = 'memory' ORDER BY 1",
                limit=20,
            )
    except Exception:
        return []
    return [row[0] for row in rows if row and row[0]]


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

_IDENT_RE = re.compile(r'^[\"\w][\"\w\.]*$')


def _is_safe_identifier(ident: str) -> bool:
    """Guard against SQL injection via FROM-clause interpolation.

    Only quoted or unquoted identifiers (plus dotted qualifiers) pass.
    Anything with spaces, parens, or quotes beyond the anchors is rejected.
    """
    return bool(_IDENT_RE.match(ident))


def _is_pure_aggregation(sql: str) -> bool:
    """Rough check: every projected expression is an aggregate function."""
    return bool(_PURE_AGG_RE.match(sql))


def _escalate(current: str, new: str) -> str:
    order = {"OK": 0, "WARN": 1, "BLOCK": 2, "ERROR": 3}
    return new if order[new] > order[current] else current


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------


def _format_report(
    decision: str,
    notices: list[str],
    row_count: int | None,
    column_count: int | None,
) -> str:
    lines = [f"## Preview query: {decision}"]
    if row_count is not None:
        lines.append(f"- Estimated rows: **{row_count:,}**")
    if column_count is not None:
        lines.append(f"- Columns in projection: **{column_count}**")
    if notices:
        lines.append("")
        lines.append("### Findings")
        for n in notices:
            lines.append(f"- ⚠ {n}")
    lines.append("")
    if decision == "OK":
        lines.append("Safe to execute via `execute_sql`.")
    elif decision == "WARN":
        lines.append(
            "Fetchable, but narrow the query if you can. Call `execute_sql` "
            "when ready."
        )
    elif decision == "BLOCK":
        lines.append(
            "**Do not call `execute_sql` with this query as-is.** Refine "
            "first (WHERE, GROUP BY, LIMIT) and preview again."
        )
    elif decision == "ERROR":
        lines.append("Fix the query above before calling `execute_sql`.")
    return "\n".join(lines)


__all__ = ["preview_query"]
