"""List tables tool — shows all registered views and attached source tables."""


def list_tables(query: str | None = None) -> str:
    """List all available tables and views in the seeknal project.

    Returns table names that can be queried with execute_sql.
    Includes consolidated entities, intermediate outputs, ask-ingested tables,
    and attached database sources such as PostgreSQL catalogs.

    Args:
        query: Optional case-insensitive table-name filter. Supports ``*`` as a
            glob wildcard, e.g. ``warehouse.analytics.*``.
    """
    from fnmatch import fnmatchcase

    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    entries: list[tuple[str, str]] = []
    errors: list[str] = []

    try:
        with ctx.db_lock:
            _columns, rows = ctx.repl.execute_oneshot("SHOW TABLES")
        for row in rows:
            if row and row[0]:
                entries.append((str(row[0]), "project"))
    except Exception as e:
        errors.append(f"project tables: {e}")

    # DuckDB's plain SHOW TABLES only reports the active/default catalog. For
    # read-only "tap-in" users, the useful tables often live in attached
    # catalogs like warehouse.analytics.orders, so enumerate each attached
    # source through its information_schema. This remains a thin discovery
    # primitive; business selection/ranking belongs in skills/prompts.
    attached = sorted(getattr(ctx.repl, "attached", set()) or [])
    for source_name in attached:
        try:
            sql = f"""
                SELECT table_schema, table_name, table_type
                FROM "{source_name}".information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name
            """
            with ctx.db_lock:
                _columns, rows = ctx.repl.execute_oneshot(sql, limit=500)
            for schema, table, table_type in rows:
                qualified = f"{source_name}.{schema}.{table}"
                entries.append((qualified, str(table_type or "attached")))
        except Exception as e:
            errors.append(f"{source_name}: {e}")

    # Preserve order but de-duplicate names that appear in multiple discovery
    # paths. If a duplicate appears, keep the first source/type label.
    seen: set[str] = set()
    unique_entries: list[tuple[str, str]] = []
    for name, kind in entries:
        if name in seen:
            continue
        seen.add(name)
        unique_entries.append((name, kind))

    if query:
        normalized_query = str(query).strip().lower()
        has_glob = "*" in normalized_query or "?" in normalized_query

        def matches(name: str) -> bool:
            normalized_name = name.lower()
            if has_glob:
                return fnmatchcase(normalized_name, normalized_query)
            return normalized_query in normalized_name

        unique_entries = [(name, kind) for name, kind in unique_entries if matches(name)]

    if not unique_entries:
        if errors:
            prefix = f"No tables found matching '{query}'." if query else "No tables found."
            return prefix + " Discovery errors:\n" + "\n".join(f"- {err}" for err in errors)
        if query:
            return f"No tables found matching '{query}'."
        return "No tables found. The project may not have been run yet."

    if query:
        lines = [f"Available tables and views matching '{query}':"]
    else:
        lines = ["Available tables and views:"]
    for name, kind in unique_entries:
        lines.append(f"- {name} ({kind})")

    if errors:
        lines.append("\nPartial discovery warnings:")
        lines.extend(f"- {err}" for err in errors)

    return "\n".join(lines)
