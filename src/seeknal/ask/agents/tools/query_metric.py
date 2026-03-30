"""Query metric tool — compile and execute semantic layer queries.

Uses the existing MetricCompiler to translate metric/dimension/filter
requests into governed SQL, then executes via the REPL's DuckDB connection.
"""

import re

from langchain_core.tools import tool


def _resolve_table_names(sql: str, available_views: set[str]) -> str:
    """Map short table names in compiled SQL to REPL-registered view names.

    MetricCompiler strips model_ref to short names (e.g., 'orders') but
    the REPL registers views with kind prefix (e.g., 'transform_orders').
    This replaces ALL occurrences of the short name (FROM, JOIN, ON clauses,
    column qualifiers) with the prefixed view name.
    """
    # First pass: identify which short names need mapping
    # by scanning FROM and JOIN clauses for table identifiers
    from_join = re.compile(r'\b(?:FROM|JOIN)\s+(\w+)', re.IGNORECASE)
    replacements: dict[str, str] = {}

    for match in from_join.finditer(sql):
        table_name = match.group(1)
        if table_name in available_views or table_name in replacements:
            continue
        # Try common kind prefixes
        for prefix in ("transform", "source", "feature_group", "model"):
            prefixed = f"{prefix}_{table_name}"
            if prefixed in available_views:
                replacements[table_name] = prefixed
                break

    # Second pass: replace all occurrences of each short name as a word boundary
    # This covers FROM, JOIN, ON clause references, and column qualifiers
    for short_name, prefixed_name in replacements.items():
        sql = re.sub(rf'\b{re.escape(short_name)}\b', prefixed_name, sql)

    return sql


@tool
def query_metric(
    metrics: str,
    dimensions: str = "",
    filters: str = "",
    order_by: str = "",
    limit: int = 100,
) -> str:
    """Query business metrics through the semantic layer.

    Compiles metric definitions into SQL with correct aggregation,
    automatic joins, and time grain resolution. Use this instead of
    execute_sql when a matching metric exists in the semantic layer.

    Metrics are defined in seeknal/semantic_models/*.yml and
    seeknal/metrics/*.yml. Check the system prompt for available
    metrics and their aliases.

    Args:
        metrics: Comma-separated metric names (e.g., 'total_revenue,order_count').
        dimensions: Comma-separated dimensions to group by
                    (e.g., 'region,ordered_at__month'). Use __grain suffix
                    for time dimensions (day, week, month, quarter, year).
        filters: Comma-separated SQL filter expressions
                 (e.g., 'region = \\'US\\',order_date >= \\'2025-01-01\\'').
        order_by: Comma-separated order columns. Prefix '-' for DESC
                  (e.g., '-total_revenue').
        limit: Maximum rows to return (default 100).
    """
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()

    # Load semantic layer
    try:
        from seeknal.workflow.semantic.loader import load_semantic_layer

        models, metric_list = load_semantic_layer(ctx.project_path)
    except Exception as e:
        return f"Error loading semantic layer: {e}"

    if not models:
        return (
            "No semantic models found in this project.\n\n"
            "Create YAML files in `seeknal/semantic_models/` or run "
            "`seeknal semantic bootstrap` to auto-generate from your data."
        )

    if not metric_list:
        return (
            "No metrics or measures found. Define measures in "
            "`seeknal/semantic_models/` or metrics in `seeknal/metrics/`."
        )

    # Parse comma-separated string arguments to lists
    metric_names = [m.strip() for m in metrics.split(",") if m.strip()]
    dim_list = [d.strip() for d in dimensions.split(",") if d.strip()] if dimensions else []
    filter_list = [f.strip() for f in filters.split(",") if f.strip()] if filters else []
    order_list = [o.strip() for o in order_by.split(",") if o.strip()] if order_by else []

    if not metric_names:
        return "Error: At least one metric name is required."

    # Resolve aliases to canonical metric names
    alias_map: dict[str, str] = {}  # lowercase alias -> canonical name
    for m in metric_list:
        alias_map[m.name.lower()] = m.name
        for alias in m.aliases:
            alias_map[alias.lower()] = m.name

    resolved_metrics = []
    for name in metric_names:
        canonical = alias_map.get(name.lower())
        if canonical:
            resolved_metrics.append(canonical)
        else:
            available = ", ".join(sorted(alias_map.keys()))
            return (
                f"Unknown metric: '{name}'.\n\n"
                f"Available metrics and aliases: {available}"
            )

    # Build MetricQuery and compile
    try:
        from seeknal.workflow.semantic.models import MetricQuery
        from seeknal.workflow.semantic.compiler import MetricCompiler

        compiler = MetricCompiler(models, metric_list)
        mq = MetricQuery(
            metrics=resolved_metrics,
            dimensions=dim_list,
            filters=filter_list,
            order_by=order_list,
            limit=limit,
        )
        sql = compiler.compile(mq)
    except ValueError as e:
        return f"Compilation error: {e}"

    # Resolve table names to match REPL-registered views
    try:
        with ctx.db_lock:
            view_result = ctx.repl.conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main'"
            )
            available_views = {row[0] for row in view_result.fetchall()}
        sql = _resolve_table_names(sql, available_views)
    except Exception:
        pass  # Proceed with original SQL if view lookup fails

    # Execute the compiled SQL
    try:
        with ctx.db_lock:
            columns, rows = ctx.repl.execute_oneshot(sql, limit=limit)
    except Exception as e:
        return (
            f"Query execution error: {e}\n\n"
            f"Compiled SQL:\n```sql\n{sql}\n```"
        )

    # Format results with metadata
    parts = []

    # Metadata header
    parts.append(f"**Metrics:** {', '.join(resolved_metrics)}")
    if dim_list:
        parts.append(f"**Dimensions:** {', '.join(dim_list)}")
    if filter_list:
        parts.append(f"**Filters:** {', '.join(filter_list)}")
    parts.append(f"\n```sql\n{sql}\n```\n")

    # Result table (markdown format, same as execute_sql)
    if not rows:
        parts.append("No results returned.")
    else:
        header = "| " + " | ".join(str(c) for c in columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"
        table_rows = []
        for row in rows[:limit]:
            table_rows.append("| " + " | ".join(str(v) for v in row) + " |")

        parts.append(header)
        parts.append(separator)
        parts.extend(table_rows)

        if len(rows) > limit:
            parts.append(f"\n({len(rows)} total rows, showing {limit})")
        else:
            parts.append(f"\n({len(rows)} rows)")

    return "\n".join(parts)
