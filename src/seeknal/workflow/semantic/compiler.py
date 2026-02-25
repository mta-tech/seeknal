"""
SQL compilation engine for the semantic layer.

Compiles MetricQuery objects into SQL by resolving metric definitions,
planning joins across semantic models, and generating appropriate
aggregate expressions for each metric type.
"""
import re
from seeknal.workflow.semantic.models import (
    AggregationType,
    EntityGraph,
    Join,
    JoinRelationship,
    Measure,
    Metric,
    MetricQuery,
    MetricRegistry,
    MetricType,
    SemanticModel,
)


# ── Filter Validation ───────────────────────────────────────────────────────

# Patterns that indicate SQL injection attempts in filter expressions
_FORBIDDEN_FILTER_PATTERNS = [
    r"\b(DROP|ALTER|TRUNCATE|CREATE|INSERT|UPDATE|DELETE)\b",
    r"\b(EXEC|EXECUTE|GRANT|REVOKE)\b",
    r"\bUNION\b",
    r"--",
    r"/\*",
    r"\*/",
    r";",
]

_FORBIDDEN_FILTER_RE = re.compile(
    "|".join(_FORBIDDEN_FILTER_PATTERNS), re.IGNORECASE
)


def validate_filter(expr: str) -> str:
    """
    Validate a filter expression for SQL injection.

    Args:
        expr: The filter expression to validate.

    Returns:
        The validated expression.

    Raises:
        ValueError: If the expression contains forbidden patterns.
    """
    if _FORBIDDEN_FILTER_RE.search(expr):
        raise ValueError(f"Filter contains forbidden SQL pattern: {expr!r}")
    return expr


# ── Time Grain Resolution ───────────────────────────────────────────────────

_SUPPORTED_GRAINS = {"day", "week", "month", "quarter", "year"}


def resolve_dimension_expr(dim_spec: str, models: dict[str, SemanticModel]) -> str:
    """
    Resolve a dimension specification to a SQL expression.

    Supports time grain syntax: `ordered_at__month` -> `date_trunc('month', ordered_at)`

    Args:
        dim_spec: Dimension name, optionally with __grain suffix.
        models: Available semantic models for expression lookup.

    Returns:
        SQL expression string.
    """
    if "__" in dim_spec:
        parts = dim_spec.rsplit("__", 1)
        dim_name, grain = parts[0], parts[1].lower()
        if grain in _SUPPORTED_GRAINS:
            # Find the dimension's expr from models
            dim_expr = _find_dimension_expr(dim_name, models)
            return f"date_trunc('{grain}', {dim_expr})"

    # Plain dimension - find its expr
    return _find_dimension_expr(dim_spec, models)


def _find_dimension_expr(dim_name: str, models: dict[str, SemanticModel]) -> str:
    """Find a dimension's SQL expression across all semantic models."""
    for model in models.values():
        dim = model.get_dimension(dim_name)
        if dim:
            return dim.expr
    # Fallback to the dimension name itself
    return dim_name


# ── Aggregation SQL ─────────────────────────────────────────────────────────

def _agg_sql(measure: Measure) -> str:
    """Generate the SQL aggregation expression for a measure.

    If the measure has filters, wraps expr with CASE WHEN for filtered aggregation
    (Cube.js-style measure filters).
    """
    expr = measure.expr
    agg = measure.agg

    # Wrap expr with CASE WHEN if measure has filters
    if measure.filters:
        filter_condition = " AND ".join(measure.filters)
        expr = f"CASE WHEN {filter_condition} THEN {expr} END"

    if agg == AggregationType.SUM:
        return f"SUM({expr})"
    elif agg == AggregationType.COUNT:
        return f"COUNT({expr})"
    elif agg == AggregationType.COUNT_DISTINCT:
        return f"COUNT(DISTINCT {expr})"
    elif agg in (AggregationType.AVERAGE, AggregationType.AVG):
        return f"AVG({expr})"
    elif agg == AggregationType.MIN:
        return f"MIN({expr})"
    elif agg == AggregationType.MAX:
        return f"MAX({expr})"
    else:
        return f"SUM({expr})"


# ── Metric Compiler ─────────────────────────────────────────────────────────

class MetricCompiler:
    """
    Compiles MetricQuery objects into SQL.

    The compiler resolves metric definitions, plans joins across semantic
    models via the entity graph, and generates appropriate SQL for each
    metric type (simple, ratio, cumulative, derived).
    """

    def __init__(
        self,
        semantic_models: list[SemanticModel],
        metrics: list[Metric],
    ):
        self.entity_graph = EntityGraph()
        self.metric_registry = MetricRegistry()

        for model in semantic_models:
            self.entity_graph.add_model(model)

        for metric in metrics:
            self.metric_registry.add(metric)

    def compile(self, query: MetricQuery) -> str:
        """
        Compile a MetricQuery into SQL.

        Args:
            query: The metric query to compile.

        Returns:
            Generated SQL string.

        Raises:
            ValueError: If metrics or measures cannot be resolved.
        """
        if not query.metrics:
            raise ValueError("MetricQuery must have at least one metric")

        # Validate filters
        for f in query.filters:
            validate_filter(f)

        # Resolve which models are needed and collect SELECT/GROUP BY parts
        select_parts: list[str] = []
        group_by_parts: list[str] = []
        needed_models: dict[str, SemanticModel] = {}
        all_filters: list[str] = list(query.filters)

        # Resolve dimensions
        models_dict = self.entity_graph.models
        dim_aliases: list[str] = []
        for dim_spec in query.dimensions:
            dim_expr = resolve_dimension_expr(dim_spec, models_dict)
            alias = dim_spec.replace("__", "_")
            dim_aliases.append(alias)
            if dim_expr != alias:
                select_parts.append(f"{dim_expr} AS {alias}")
            else:
                select_parts.append(dim_expr)
            group_by_parts.append(dim_expr)

            # Track which model provides this dimension
            self._track_dimension_model(dim_spec, needed_models)

        # Classify metrics into cumulative vs non-cumulative
        cumulative_metrics: list[tuple[str, Metric]] = []
        other_metrics: list[tuple[str, Metric]] = []
        for metric_name in query.metrics:
            metric = self.metric_registry.get(metric_name)
            if not metric:
                raise ValueError(f"Unknown metric: {metric_name}")
            if metric.type == MetricType.CUMULATIVE:
                cumulative_metrics.append((metric_name, metric))
            else:
                other_metrics.append((metric_name, metric))

        # If cumulative metrics + dimensions, use CTE to pre-aggregate
        if cumulative_metrics and group_by_parts:
            return self._compile_with_cumulative_cte(
                query, cumulative_metrics, other_metrics,
                needed_models, select_parts, group_by_parts,
                dim_aliases, all_filters,
            )

        # Compile each metric (flat query — no cumulative or no dimensions)
        for metric_name in query.metrics:
            metric = self.metric_registry.get(metric_name)
            if not metric:
                raise ValueError(f"Unknown metric: {metric_name}")

            metric_sql = self._compile_metric(metric, needed_models, all_filters)
            select_parts.append(f"{metric_sql} AS {metric_name}")

        # Determine the base model and FROM clause
        from_clause = self._build_from_clause(needed_models)

        # Build the full query
        sep = ",\n  "
        sql_parts = ["SELECT\n  " + sep.join(select_parts)]
        sql_parts.append(f"FROM {from_clause}")

        if all_filters:
            sql_parts.append(f"WHERE {' AND '.join(all_filters)}")

        if group_by_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_by_parts)}")

        if query.order_by:
            order_clauses = []
            for ob in query.order_by:
                if ob.startswith("-"):
                    order_clauses.append(f"{ob[1:]} DESC")
                else:
                    order_clauses.append(f"{ob} ASC")
            sql_parts.append(f"ORDER BY {', '.join(order_clauses)}")

        if query.limit is not None:
            sql_parts.append(f"LIMIT {int(query.limit)}")

        return "\n".join(sql_parts)

    def _compile_with_cumulative_cte(
        self,
        query: MetricQuery,
        cumulative_metrics: list[tuple[str, Metric]],
        other_metrics: list[tuple[str, Metric]],
        needed_models: dict[str, SemanticModel],
        dim_select_parts: list[str],
        group_by_parts: list[str],
        dim_aliases: list[str],
        all_filters: list[str],
    ) -> str:
        """
        Compile a query containing cumulative metrics using a CTE.

        The CTE pre-aggregates base measures with GROUP BY, then the outer
        query applies window functions on the aggregated values.
        """
        from_clause = self._build_from_clause(needed_models)

        # Collect metric-level filters from cumulative metrics
        for _, metric in cumulative_metrics:
            if metric.filter:
                validate_filter(metric.filter)
                all_filters.append(metric.filter)

        # --- CTE: dimensions + aggregated base measures + non-cumulative metrics ---
        cte_select = list(dim_select_parts)

        # Aggregate base measures for each cumulative metric
        base_aliases: dict[str, tuple[str, Metric]] = {}
        for metric_name, metric in cumulative_metrics:
            _, measure = self._resolve_measure(metric.measure or "", needed_models)
            base_alias = f"_base_{metric_name}"
            cte_select.append(f"{_agg_sql(measure)} AS {base_alias}")
            base_aliases[metric_name] = (base_alias, metric)

        # Compute non-cumulative metrics in the CTE
        for metric_name, metric in other_metrics:
            metric_sql = self._compile_metric(metric, needed_models, all_filters)
            cte_select.append(f"{metric_sql} AS {metric_name}")

        sep = ",\n    "
        cte = f"WITH _base AS (\n  SELECT\n    {sep.join(cte_select)}"
        cte += f"\n  FROM {from_clause}"
        if all_filters:
            cte += f"\n  WHERE {' AND '.join(all_filters)}"
        cte += f"\n  GROUP BY {', '.join(group_by_parts)}"
        cte += "\n)"

        # --- Outer query: window functions for cumulative, pass-through for rest ---
        outer_parts: list[str] = list(dim_aliases)

        for metric_name, (base_alias, metric) in base_aliases.items():
            model_name, _ = self._resolve_measure(metric.measure or "", needed_models)
            model = self.entity_graph.get_model(model_name)
            time_dim = model.default_time_dimension if model else None

            if metric.grain_to_date and time_dim:
                grain = metric.grain_to_date
                outer_parts.append(
                    f"SUM({base_alias}) OVER ("
                    f"PARTITION BY date_trunc('{grain}', {time_dim}) "
                    f"ORDER BY {time_dim} "
                    f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                    f") AS {metric_name}"
                )
            elif time_dim:
                outer_parts.append(
                    f"SUM({base_alias}) OVER ("
                    f"ORDER BY {time_dim} "
                    f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                    f") AS {metric_name}"
                )
            else:
                outer_parts.append(f"{base_alias} AS {metric_name}")

        # Pass through non-cumulative metrics
        for metric_name, _ in other_metrics:
            outer_parts.append(metric_name)

        sql = f"{cte}\nSELECT\n  {sep.join(outer_parts)}\nFROM _base"

        if query.order_by:
            order_clauses = []
            for ob in query.order_by:
                if ob.startswith("-"):
                    order_clauses.append(f"{ob[1:]} DESC")
                else:
                    order_clauses.append(f"{ob} ASC")
            sql += f"\nORDER BY {', '.join(order_clauses)}"

        if query.limit is not None:
            sql += f"\nLIMIT {int(query.limit)}"

        return sql

    def _compile_metric(
        self,
        metric: Metric,
        needed_models: dict[str, SemanticModel],
        all_filters: list[str],
    ) -> str:
        """Compile a single metric to a SQL expression."""
        if metric.type == MetricType.SIMPLE:
            return self._compile_simple(metric, needed_models, all_filters)
        elif metric.type == MetricType.RATIO:
            return self._compile_ratio(metric, needed_models)
        elif metric.type == MetricType.CUMULATIVE:
            return self._compile_cumulative(metric, needed_models, all_filters)
        elif metric.type == MetricType.DERIVED:
            return self._compile_derived(metric, needed_models, all_filters)
        else:
            raise ValueError(f"Unsupported metric type: {metric.type}")

    def _compile_simple(
        self,
        metric: Metric,
        needed_models: dict[str, SemanticModel],
        all_filters: list[str],
    ) -> str:
        """Compile a simple metric: direct aggregation of a measure."""
        _, measure = self._resolve_measure(metric.measure or "", needed_models)
        # Add metric-level filter
        if metric.filter:
            validate_filter(metric.filter)
            all_filters.append(metric.filter)
        return _agg_sql(measure)

    def _compile_ratio(
        self,
        metric: Metric,
        needed_models: dict[str, SemanticModel],
    ) -> str:
        """Compile a ratio metric: numerator / NULLIF(denominator, 0)."""
        _, num_measure = self._resolve_measure(metric.numerator or "", needed_models)
        _, den_measure = self._resolve_measure(metric.denominator or "", needed_models)
        num_sql = _agg_sql(num_measure)
        den_sql = _agg_sql(den_measure)
        return f"{num_sql} / NULLIF({den_sql}, 0)"

    def _compile_cumulative(
        self,
        metric: Metric,
        needed_models: dict[str, SemanticModel],
        all_filters: list[str],
    ) -> str:
        """
        Compile a cumulative metric.

        If grain_to_date is set, generates a window function that resets
        at the grain boundary. Otherwise generates a running total.
        """
        model_name, measure = self._resolve_measure(metric.measure or "", needed_models)
        if metric.filter:
            validate_filter(metric.filter)
            all_filters.append(metric.filter)

        agg_expr = _agg_sql(measure)

        if metric.grain_to_date:
            # Find the time dimension for partitioning
            model = self.entity_graph.get_model(model_name)
            time_dim = model.default_time_dimension if model else None
            if time_dim:
                grain = metric.grain_to_date
                return (
                    f"SUM({measure.expr}) OVER ("
                    f"PARTITION BY date_trunc('{grain}', {time_dim}) "
                    f"ORDER BY {time_dim} "
                    f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
                )

        # Fallback: simple running sum
        return agg_expr

    def _compile_derived(
        self,
        metric: Metric,
        needed_models: dict[str, SemanticModel],
        all_filters: list[str],
    ) -> str:
        """
        Compile a derived metric by substituting input metric references
        with their compiled SQL expressions.
        """
        if not metric.expr:
            raise ValueError(f"Derived metric '{metric.name}' has no expr")

        result_expr = metric.expr
        for inp in metric.inputs:
            ref_metric = self.metric_registry.get(inp.metric)
            if not ref_metric:
                raise ValueError(
                    f"Derived metric '{metric.name}' references "
                    f"unknown metric '{inp.metric}'"
                )
            ref_sql = self._compile_metric(ref_metric, needed_models, all_filters)
            alias = inp.alias or inp.metric
            result_expr = result_expr.replace(alias, ref_sql)

        return result_expr

    def _resolve_measure(
        self,
        measure_name: str,
        needed_models: dict[str, SemanticModel],
    ) -> tuple[str, Measure]:
        """Find which model contains a measure and track it as needed."""
        result = self.metric_registry.find_measure_model(
            measure_name, self.entity_graph
        )
        if not result:
            raise ValueError(f"Measure '{measure_name}' not found in any semantic model")

        model_name, measure = result
        model = self.entity_graph.get_model(model_name)
        if model:
            needed_models[model_name] = model
        return model_name, measure

    def _track_dimension_model(
        self,
        dim_spec: str,
        needed_models: dict[str, SemanticModel],
    ) -> None:
        """Track which model provides a dimension."""
        dim_name = dim_spec.split("__")[0] if "__" in dim_spec else dim_spec
        for model_name, model in self.entity_graph.models.items():
            if model.get_dimension(dim_name):
                needed_models[model_name] = model
                return

    def _build_from_clause(
        self, needed_models: dict[str, SemanticModel]
    ) -> str:
        """Build the FROM clause, including JOINs if multiple models are needed.

        Uses explicit join definitions (Cube.js-style) when available,
        falls back to entity-graph auto-join.
        """
        model_names = list(needed_models.keys())

        if not model_names:
            raise ValueError("No semantic models resolved for query")

        # Single model - simple FROM
        base_model = needed_models[model_names[0]]
        base_ref = self._model_ref_to_table(base_model.model_ref)

        if len(model_names) == 1:
            return base_ref

        # Multiple models - plan joins
        parts = [base_ref]
        for model_name in model_names[1:]:
            model = needed_models[model_name]
            table_ref = self._model_ref_to_table(model.model_ref)

            # Priority 1: Explicit join definition on the base model
            explicit_join = base_model.get_join(model_name)
            if explicit_join:
                join_sql = self._resolve_join_refs(explicit_join.sql, needed_models)
                join_type = self._join_type_sql(explicit_join.relationship)
                parts.append(f"{join_type} {table_ref} ON {join_sql}")
                continue

            # Priority 2: Explicit join defined on the target model (reverse)
            reverse_join = model.get_join(base_model.name)
            if reverse_join:
                join_sql = self._resolve_join_refs(reverse_join.sql, needed_models)
                join_type = self._join_type_sql(reverse_join.relationship)
                parts.append(f"{join_type} {table_ref} ON {join_sql}")
                continue

            # Priority 3: Auto-join via entity graph
            join_path = self.entity_graph.find_join_path(
                model_names[0], model_name
            )
            if join_path:
                entity_name = join_path[-1][0]
                parts.append(
                    f"LEFT JOIN {table_ref} ON {base_ref}.{entity_name} = {table_ref}.{entity_name}"
                )
            else:
                parts.append(f"CROSS JOIN {table_ref}")

        return "\n".join(parts)

    @staticmethod
    def _join_type_sql(relationship: JoinRelationship) -> str:
        """Map join relationship to SQL JOIN type."""
        # All Cube.js joins are LEFT JOINs
        return "LEFT JOIN"

    def _resolve_join_refs(
        self, sql: str, needed_models: dict[str, SemanticModel]
    ) -> str:
        """Resolve {model_name} placeholders in join SQL to table names."""
        result = sql
        # Replace {CUBE} with the first model's table
        models_list = list(needed_models.values())
        if models_list:
            result = result.replace("{CUBE}", self._model_ref_to_table(models_list[0].model_ref))
        # Replace {model_name} with table references
        for model_name, model in needed_models.items():
            table_ref = self._model_ref_to_table(model.model_ref)
            result = result.replace(f"{{{model_name}}}", table_ref)
        return result

    @staticmethod
    def _model_ref_to_table(model_ref: str) -> str:
        """Convert a model reference like ref('transform.orders') to a table name."""
        # Strip ref() wrapper
        match = re.match(r"ref\(['\"](.+?)['\"]\)", model_ref)
        if match:
            ref_str = match.group(1)
            # ref format is 'kind.name' — DuckDB uses just the name
            return ref_str.split(".")[-1] if "." in ref_str else ref_str
        return model_ref
