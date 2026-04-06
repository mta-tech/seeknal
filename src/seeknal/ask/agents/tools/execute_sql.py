"""Execute SQL tool — runs read-only queries via the seeknal REPL."""

def execute_sql(sql: str, limit: int = 100) -> str:
    """Execute a read-only SQL query against seeknal project data.

    Use this to query entities, feature groups, and intermediate tables.
    Only SELECT/WITH queries are allowed (read-only). Only query tables shown
    in list_tables output. Never reference file paths in SQL.
    Results are returned as a formatted table.

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
        limit: Maximum rows to return (default 100).
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    # Strip trailing semicolons — LLMs often include them but DuckDB rejects them
    sql = sql.strip().rstrip(";").strip()

    # SQL validation is handled by the PRE_TOOL_USE hook (see hooks.py)

    try:
        with ctx.db_lock:
            columns, rows = ctx.repl.execute_oneshot(sql, limit=limit)
    except Exception as e:
        from seeknal.ask.agents.tools.errors import (
            classify_duckdb_error,
            format_tool_error,
        )

        return format_tool_error(classify_duckdb_error(str(e)), str(e))

    if not columns:
        return "Query executed successfully but returned no results."

    # Format as markdown table
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    body_lines = []
    for row in rows:
        cells = [str(v) if v is not None else "NULL" for v in row]
        body_lines.append("| " + " | ".join(cells) + " |")

    result = f"{header}\n{separator}\n" + "\n".join(body_lines)
    result += f"\n\n({len(rows)} row{'s' if len(rows) != 1 else ''})"
    return result
