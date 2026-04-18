"""Execute SQL tool — runs read-only queries via the seeknal REPL.

Guards the LLM's context window by enforcing hard caps on row count,
column count, per-cell length, and total byte size. Whenever any cap
kicks in, an explicit ``Result truncated`` note is appended so the agent
knows to narrow its next query.
"""

from __future__ import annotations

# Hard caps — not overridable from the agent. Tune here if needed.
_ROW_HARD_CAP = 500
_COLUMN_HARD_CAP = 50
_CELL_MAX_LEN = 200
_BYTE_BUDGET = 50 * 1024  # 50 KB of markdown table (roughly ~12k tokens)


def execute_sql(sql: str, limit: int = 100) -> str:
    """Execute a read-only SQL query against seeknal project data.

    Use this to query entities, feature groups, and intermediate tables.
    Only SELECT/WITH queries are allowed (read-only). Only query tables shown
    in list_tables output. Never reference file paths in SQL.
    Results are returned as a formatted table.

    Result safety guards (applied silently, flagged explicitly when tripped):
    - Row count is capped at 500 regardless of the ``limit`` argument. If
      the underlying query has more rows, the returned markdown shows the
      first 500 plus a note with the real total.
    - Column count is capped at 50. Extra columns are dropped and a
      note is appended.
    - Each cell value is truncated to 200 chars with an ellipsis.
    - If the assembled markdown exceeds 50 KB, trailing rows are dropped
      with a note. Narrow the query (WHERE / GROUP BY) to recover detail.

    DuckDB SQL dialect notes:
    - Do NOT include trailing semicolons in queries
    - Use CAST('2024-01-01' AS TIMESTAMP) for timestamp literals before INTERVAL arithmetic
    - Use CAST(COUNT(*) AS BIGINT) for aggregation counts
    - Use CAST(SUM(x) AS DOUBLE) for numeric aggregations
    - All non-aggregate SELECT columns must appear in GROUP BY
    - Access struct fields with dot notation: column_name.field_name
    - Use ILIKE for case-insensitive matching
    - In Python code: NEVER put # comments inside SQL strings — DuckDB does
      not recognize # as a comment. Use -- for SQL comments

    Tool errors include a JSON structure with 'category' and 'retryable' fields.
    For retryable errors, adjust your approach based on the 'hint'.
    For terminal errors, explain the limitation to the user.

    Args:
        sql: A DuckDB-compatible SELECT query.
        limit: Maximum rows to return (default 100). Capped at 500
            regardless of what is passed — use WHERE/GROUP BY to narrow
            beyond that, not a larger limit.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    # Strip trailing semicolons — LLMs often include them but DuckDB rejects them
    sql = sql.strip().rstrip(";").strip()

    # SQL validation is handled by the PRE_TOOL_USE hook (see hooks.py)

    # Clamp caller-supplied limit to the hard cap. Fetch one extra row so we
    # can detect "there are more rows beyond this page".
    try:
        asked = max(1, int(limit))
    except (TypeError, ValueError):
        asked = 100
    effective_limit = min(asked, _ROW_HARD_CAP)
    fetch_limit = effective_limit + 1

    try:
        with ctx.db_lock:
            columns, rows = ctx.repl.execute_oneshot(sql, limit=fetch_limit)
    except Exception as e:
        from seeknal.ask.agents.tools.errors import (
            classify_duckdb_error,
            format_tool_error,
        )

        return format_tool_error(classify_duckdb_error(str(e)), str(e))

    if not columns:
        return "Query executed successfully but returned no results."

    has_more_rows = len(rows) > effective_limit
    trimmed_rows = rows[:effective_limit]

    # Resolve the real total only when we actually hit the cap — saves the
    # round-trip for normal-sized results.
    total_rows: int | None = None
    if has_more_rows:
        try:
            total_rows = _count_total(ctx, sql)
        except Exception:
            total_rows = None

    return _format_table(
        columns=columns,
        rows=trimmed_rows,
        total_rows=total_rows,
        has_more_rows=has_more_rows,
        requested_limit=asked,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _count_total(ctx, sql: str) -> int | None:
    """Return the true row count of ``sql`` by wrapping it in COUNT(*).

    Returns None on any parse / execution failure so the caller can fall
    back to ``"many rows"`` without crashing the tool.
    """
    wrapped = f"SELECT CAST(COUNT(*) AS BIGINT) AS c FROM ({sql}) AS _seek_q"
    with ctx.db_lock:
        _cols, rows = ctx.repl.execute_oneshot(wrapped, limit=1)
    if rows and rows[0] and rows[0][0] is not None:
        try:
            return int(rows[0][0])
        except (TypeError, ValueError):
            return None
    return None


def _truncate_cell(value) -> tuple[str, bool]:
    """Stringify and cap cell content. Returns (text, was_truncated)."""
    if value is None:
        return "NULL", False
    text = str(value)
    truncated = False
    if len(text) > _CELL_MAX_LEN:
        text = text[: _CELL_MAX_LEN - 1] + "…"
        truncated = True
    # Pipes and newlines break markdown rows.
    if "|" in text:
        text = text.replace("|", "\\|")
    if "\n" in text:
        text = text.replace("\n", " ")
    return text, truncated


def _format_table(
    columns: list[str],
    rows: list[tuple],
    total_rows: int | None,
    has_more_rows: bool,
    requested_limit: int,
) -> str:
    notices: list[str] = []

    # Column cap
    original_column_count = len(columns)
    columns_truncated = False
    if original_column_count > _COLUMN_HARD_CAP:
        columns = columns[:_COLUMN_HARD_CAP]
        columns_truncated = True
        notices.append(
            f"Result truncated: showing {_COLUMN_HARD_CAP} of "
            f"{original_column_count} columns. Use SELECT col1, col2, ... to "
            "narrow the projection."
        )

    # Build rows with cell-length truncation applied.
    any_cell_truncated = False
    body_lines: list[str] = []
    for row in rows:
        slice_row = row[: len(columns)] if columns_truncated else row
        cells = []
        for value in slice_row:
            text, was_truncated = _truncate_cell(value)
            if was_truncated:
                any_cell_truncated = True
            cells.append(text)
        body_lines.append("| " + " | ".join(cells) + " |")

    if any_cell_truncated:
        notices.append(
            f"Result truncated: long cell values capped at "
            f"{_CELL_MAX_LEN} chars. Query specific columns or use "
            "substring/length filters if you need full values."
        )

    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"

    # Byte budget — drop rows from the end until the markdown fits.
    rows_dropped_for_budget = 0
    while body_lines:
        assembled = "\n".join([header, separator, *body_lines])
        if len(assembled.encode("utf-8")) <= _BYTE_BUDGET:
            break
        body_lines.pop()
        rows_dropped_for_budget += 1

    if rows_dropped_for_budget > 0:
        notices.append(
            f"Result truncated: output exceeded {_BYTE_BUDGET // 1024} KB; "
            f"dropped {rows_dropped_for_budget} trailing row(s). Narrow the "
            "query (WHERE / GROUP BY / aggregate) to see more."
        )

    shown_rows = len(body_lines)
    # The hard cap actually clamps only when the caller requested more than it.
    hard_cap_hit = requested_limit > _ROW_HARD_CAP
    if has_more_rows:
        if total_rows is not None:
            footer = f"({shown_rows:,} of {total_rows:,} rows shown)"
        else:
            footer = f"({shown_rows:,} of many rows shown)"
        # Only warn loudly when the hard cap is the reason we truncated; the
        # caller's own limit doing its job isn't "truncation", just pagination.
        if hard_cap_hit:
            if total_rows is not None:
                notices.append(
                    f"Result truncated: returned {shown_rows:,} of "
                    f"{total_rows:,} total rows (hard cap {_ROW_HARD_CAP}). "
                    "Use WHERE, GROUP BY, or LIMIT with pagination to see others."
                )
            else:
                notices.append(
                    f"Result truncated: returned {shown_rows:,} rows; the full "
                    "result set is larger (exact count unavailable). Use "
                    "WHERE or aggregation to narrow."
                )
    else:
        footer = f"({shown_rows:,} row{'s' if shown_rows != 1 else ''})"
        if hard_cap_hit:
            notices.append(
                f"Note: requested limit {requested_limit} was clamped to "
                f"{_ROW_HARD_CAP}; full result set fit within the cap."
            )

    result = "\n".join([header, separator, *body_lines, "", footer])
    if notices:
        result += "\n\n" + "\n".join(f"⚠ {n}" for n in notices)
    return result


__all__ = ["execute_sql"]
