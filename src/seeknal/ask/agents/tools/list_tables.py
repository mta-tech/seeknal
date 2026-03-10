"""List tables tool — shows all registered views and tables in the REPL."""

from langchain_core.tools import tool


@tool
def list_tables() -> str:
    """List all available tables and views in the seeknal project.

    Returns table names that can be queried with execute_sql.
    Includes consolidated entities, intermediate outputs, and attached databases.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    try:
        with ctx.db_lock:
            columns, rows = ctx.repl.execute_oneshot("SHOW TABLES")
    except Exception as e:
        return f"Error listing tables: {e}"

    if not rows:
        return "No tables found. The project may not have been run yet."

    table_names = [row[0] for row in rows]
    return "Available tables:\n" + "\n".join(f"- {name}" for name in table_names)
