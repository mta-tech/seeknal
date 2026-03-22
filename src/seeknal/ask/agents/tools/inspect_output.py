"""Inspect output tool — query pipeline output parquets directly after run."""

from langchain_core.tools import tool


@tool
def inspect_output(node_name: str, sql: str = "", limit: int = 20) -> str:
    """Query the output of a pipeline node directly from its parquet file.

    Use this after run_pipeline to verify results. This bypasses REPL view
    registration and reads directly from target/intermediate/.

    Args:
        node_name: Node ID (e.g., 'transform.customer_intel' or 'source.customers').
                   Also accepts flat names like 'transform_customer_intel'.
        sql: Optional SQL query to run against the output.
             If empty, returns SELECT * with the given limit.
             Use 'this' as the table name in your query
             (e.g., "SELECT category, COUNT(*) FROM this GROUP BY category").
        limit: Max rows to return (default 20). Ignored when sql is provided.
    """
    from pathlib import Path

    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    intermediate_dir = ctx.project_path / "target" / "intermediate"

    if not intermediate_dir.exists():
        return "No pipeline outputs found. Run the pipeline first with run_pipeline(confirmed=True, full=True)."

    # Normalize node_name: transform.name → transform_name
    flat_name = node_name.replace(".", "_")

    # Find the parquet file
    parquet_path = intermediate_dir / f"{flat_name}.parquet"

    if not parquet_path.exists():
        # Try listing available outputs
        available = sorted(
            f.stem for f in intermediate_dir.glob("*.parquet")
        )
        if available:
            suggestions = "\n".join(f"  - {name}" for name in available[:20])
            return (
                f"Output not found: {flat_name}.parquet\n\n"
                f"Available outputs:\n{suggestions}"
            )
        return f"Output not found: {flat_name}.parquet and no other outputs exist."

    # Query the parquet file
    import duckdb

    con = duckdb.connect(":memory:")
    safe_path = str(parquet_path.resolve()).replace("'", "''")

    try:
        if sql:
            # Replace 'this' with the parquet path
            actual_sql = sql.replace("this", f"read_parquet('{safe_path}')")
            result = con.execute(actual_sql).fetchall()
            columns = [desc[0] for desc in con.description]
        else:
            result = con.execute(
                f"SELECT * FROM read_parquet('{safe_path}') LIMIT {limit}"
            ).fetchall()
            columns = [desc[0] for desc in con.description]

        # Get total row count
        total = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{safe_path}')"
        ).fetchone()[0]

    except Exception as e:
        con.close()
        return f"Error querying {flat_name}: {e}"

    con.close()

    if not result:
        return f"Node {node_name} output is empty (0 rows)."

    # Format as markdown table
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    rows = []
    for row in result:
        cells = []
        for val in row:
            s = str(val) if val is not None else "NULL"
            if len(s) > 50:
                s = s[:50] + "..."
            cells.append(s)
        rows.append("| " + " | ".join(cells) + " |")

    table = "\n".join([header, separator] + rows)
    shown = len(result)

    return f"**{node_name}** — {total:,} total rows (showing {shown})\n\n{table}"
