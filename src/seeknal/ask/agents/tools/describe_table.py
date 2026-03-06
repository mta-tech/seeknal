"""Describe table tool — shows column names and types for a table."""

from langchain_core.tools import tool


@tool
def describe_table(table_name: str) -> str:
    """Get the schema (column names and types) for a specific table or view.

    Use this before writing SQL queries to understand the available columns.

    Args:
        table_name: Name of the table or view to describe.
    """
    import re

    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    # Validate table name to prevent injection
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", table_name):
        return f"Invalid table name: '{table_name}'"

    try:
        columns, rows = ctx.repl.execute_oneshot(f"DESCRIBE {table_name}")
    except Exception as e:
        return f"Error describing table '{table_name}': {e}"

    if not rows:
        return f"Table '{table_name}' not found or has no columns."

    lines = [f"Schema for `{table_name}`:\n"]
    for row in rows:
        col_name = row[0]
        col_type = row[1]
        nullable = row[2] if len(row) > 2 else ""
        lines.append(f"- `{col_name}` ({col_type}){' NULL' if nullable == 'YES' else ''}")

    return "\n".join(lines)
