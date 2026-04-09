"""List tables tool — shows all registered views and tables in the REPL."""


def list_tables() -> str:
    """List all available tables and views in the seeknal project.

    Returns table names that can be queried with execute_sql.
    Includes consolidated entities, intermediate outputs, and attached databases.
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    try:
        with ctx.db_lock:
            lines: list[str] = []

            # Tables in the default (main) catalog
            _, rows = ctx.repl.execute_oneshot("SHOW TABLES")
            for row in rows:
                lines.append(f"- {row[0]}")

            # Tables in attached databases (e.g. PostgreSQL via ATTACH)
            for name in sorted(ctx.repl.attached):
                try:
                    _, attached_rows = ctx.repl.execute_oneshot(
                        f'SELECT table_name FROM information_schema.tables '
                        f"WHERE table_catalog = '{name}'"
                    )
                    for row in attached_rows:
                        lines.append(f"- {name}.{row[0]}")
                except Exception:
                    pass

    except Exception as e:
        return f"Error listing tables: {e}"

    if not lines:
        return "No tables found. The project may not have been run yet."

    return "Available tables:\n" + "\n".join(lines)
