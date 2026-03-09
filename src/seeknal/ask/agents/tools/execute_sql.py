"""Execute SQL tool — runs read-only queries via the seeknal REPL."""

from langchain_core.tools import tool


@tool
def execute_sql(sql: str, limit: int = 100) -> str:
    """Execute a read-only SQL query against seeknal project data.

    Use this to query entities, feature groups, and intermediate tables.
    Only SELECT queries are allowed. Results are returned as a formatted table.

    DuckDB SQL dialect notes:
    - Do NOT include trailing semicolons in queries
    - Use CAST('2024-01-01' AS TIMESTAMP) for timestamp literals before INTERVAL arithmetic
    - Use CAST(COUNT(*) AS BIGINT) for aggregation counts
    - Use CAST(SUM(x) AS DOUBLE) for numeric aggregations
    - All non-aggregate SELECT columns must appear in GROUP BY
    - Access struct fields with dot notation: column_name.field_name

    Args:
        sql: A DuckDB-compatible SELECT query.
        limit: Maximum rows to return (default 100).
    """
    from seeknal.ask.agents.tools._context import get_tool_context
    from seeknal.ask.security import validate_sql_for_agent

    ctx = get_tool_context()

    # Strip trailing semicolons — LLMs often include them but DuckDB rejects them
    sql = sql.strip().rstrip(";").strip()

    try:
        validate_sql_for_agent(sql)
    except ValueError as e:
        return f"SQL validation error: {e}"

    try:
        with ctx.db_lock:
            columns, rows = ctx.repl.execute_oneshot(sql, limit=limit)
    except Exception as e:
        return f"SQL execution error: {e}"

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
