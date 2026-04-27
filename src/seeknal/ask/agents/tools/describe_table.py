"""Describe table tool — shows column names and types for a table."""

def describe_table(table_name: str) -> str:
    """Get the schema (column names and types) for a specific table or view.

    Use this before writing SQL queries to understand the available columns.

    Args:
        table_name: Name of the table or view to describe.
    """
    import re

    from seeknal.ask.agents.tools._context import (
        get_discovery_cache_value,
        get_tool_context,
        set_discovery_cache_value,
    )

    ctx = get_tool_context()
    table_name = _normalize_attached_table_name(str(table_name), ctx)

    # Validate table name to prevent injection
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", table_name):
        return f"Invalid table name: '{table_name}'"

    cache_key = f"describe_table:{table_name.lower()}"
    cached = get_discovery_cache_value(cache_key)
    if cached is not None:
        return cached

    try:
        with ctx.db_lock:
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

    output = "\n".join(lines)
    set_discovery_cache_value(cache_key, output)
    return output


def _normalize_attached_table_name(table_name: str, ctx) -> str:
    """Repair a common weak-model typo for attached catalog names.

    Small local models sometimes collapse the separator after a short catalog
    alias, e.g. ``whanalytics.monthly_revenue`` instead of
    ``wh.analytics.monthly_revenue``.  If the prefix matches an attached
    source, insert the missing dot.  This keeps the tool thin and deterministic
    while reducing avoidable validation/error-retry loops.
    """
    stripped = table_name.strip()
    if "." not in stripped:
        match = _find_unique_attached_table(stripped, ctx)
        if match:
            return match

    attached = sorted(getattr(ctx.repl, "attached", set()) or [], key=len, reverse=True)
    for source_name in attached:
        if stripped.startswith(f"{source_name}."):
            return stripped
        if stripped.startswith(source_name) and len(stripped) > len(source_name):
            rest = stripped[len(source_name):]
            if rest and rest[0].isalpha() and "." in rest:
                return f"{source_name}.{rest}"
    return stripped


def _find_unique_attached_table(table_name: str, ctx) -> str | None:
    """Resolve an unqualified table name to a unique attached table."""
    from seeknal.ask.agents.tools._context import (
        get_discovery_cache_value,
        set_discovery_cache_value,
    )

    cache_key = f"find_attached_table:{table_name.lower()}"
    cached = get_discovery_cache_value(cache_key)
    if cached is not None:
        return cached or None

    matches: list[str] = []
    attached = sorted(getattr(ctx.repl, "attached", set()) or [])
    for source_name in attached:
        try:
            sql = f"""
                SELECT table_schema, table_name
                FROM "{source_name}".information_schema.tables
                WHERE lower(table_name) = lower('{table_name.replace("'", "''")}')
                  AND table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name
            """
            with ctx.db_lock:
                _columns, rows = ctx.repl.execute_oneshot(sql, limit=2)
        except Exception:  # noqa: BLE001 - best-effort name repair
            continue
        matches.extend(f"{source_name}.{schema}.{table}" for schema, table in rows)

    result = matches[0] if len(matches) == 1 else None
    set_discovery_cache_value(cache_key, result or "")
    return result
