"""
Python pipeline decorators for defining data sources, transforms, and feature groups.

Provides @source, @transform, @feature_group, and @materialize decorators that
register pipeline nodes in a global registry for later discovery and execution.
"""

import re
from functools import wraps
from typing import Callable, Optional, Any, Union

from seeknal.pipeline.materialization_config import MaterializationConfig  # ty: ignore[unresolved-import]

# Global registry for discovered nodes
_PIPELINE_REGISTRY: dict[str, dict] = {}


def _validate_materialization_config(
    materialization: Optional[Union[dict, MaterializationConfig]]
) -> None:
    """
    Validate materialization configuration parameters.

    Args:
        materialization: Dict or MaterializationConfig to validate

    Raises:
        ValueError: If configuration is invalid
        TypeError: If materialization is not dict or MaterializationConfig
    """
    if materialization is None:
        return

    # Convert to dict for validation
    if isinstance(materialization, MaterializationConfig):
        config = materialization.to_dict()
    elif isinstance(materialization, dict):
        config = materialization
    else:
        raise TypeError(
            f"materialization must be dict or MaterializationConfig, "
            f"got {type(materialization).__name__}"
        )

    enabled = config.get("enabled")
    table = config.get("table")
    mode = config.get("mode")

    # If enabled, table is required
    if enabled is True and not table:
        raise ValueError(
            "materialization.table is required when materialization.enabled=True"
        )

    # Validate table name format (catalog.namespace.table)
    if table:
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*){2}$'
        if not re.match(pattern, table):
            raise ValueError(
                f"Invalid table name '{table}'. "
                "Expected format: catalog.namespace.table "
                "(e.g., 'warehouse.prod.sales_forecast')"
            )

    # Validate mode
    if mode and mode not in ("append", "overwrite"):
        raise ValueError(
            f"Invalid materialization.mode '{mode}'. "
            "Must be 'append' or 'overwrite'"
        )


def _set_node_meta(wrapper: Callable, node_id: str, meta: dict) -> None:
    """Store node metadata on the wrapper and register it globally."""
    wrapper.__dict__["_seeknal_node"] = meta
    _PIPELINE_REGISTRY[node_id] = meta


def materialize(
    type: str = "iceberg",
    connection: Optional[str] = None,
    table: str = "",
    mode: str = "full",
    time_column: Optional[str] = None,
    lookback: Optional[str] = None,
    unique_keys: Optional[list[str]] = None,
    **kwargs,
):
    """Stackable decorator to attach materialization targets to a pipeline node.

    Apply one or more @materialize decorators before @source, @transform, or
    @feature_group to define multi-target materialization. Each decorator
    appends a config dict to ``func._seeknal_materializations``.

    Args:
        type: Materialization target type ("iceberg", "postgresql")
        connection: Named connection profile (required for postgresql)
        table: Target table name
        mode: Write mode ("full", "incremental_by_time", "upsert_by_key",
              "append", "overwrite")
        time_column: Column for incremental mode partitioning
        lookback: Lookback window for incremental mode (e.g., "7d")
        unique_keys: Key columns for upsert mode
        **kwargs: Additional target-specific parameters

    Example:
        @transform(name="order_products", inputs=["source.orders"])
        @materialize(type='postgresql', connection='local_pg',
                     table='analytics.order_products', mode='upsert_by_key',
                     unique_keys=['order_id'])
        @materialize(type='iceberg', table='atlas.ns.order_products')
        def order_products(ctx):
            return ctx.ref('source.orders')
    """
    def decorator(func: Callable) -> Callable:
        mat_config: dict[str, Any] = {"type": type, "table": table, "mode": mode}
        if connection is not None:
            mat_config["connection"] = connection
        if time_column is not None:
            mat_config["time_column"] = time_column
        if lookback is not None:
            mat_config["lookback"] = lookback
        if unique_keys is not None:
            mat_config["unique_keys"] = unique_keys
        mat_config.update(kwargs)

        # Stackable: append to existing list or create new
        mats = func.__dict__.setdefault("_seeknal_materializations", [])
        mats.append(mat_config)

        return func
    return decorator


def source(
    name: str,
    source: str = "csv",
    table: str = "",
    columns: Optional[dict] = None,
    tags: Optional[list[str]] = None,
    materialization: Optional[Union[dict, MaterializationConfig]] = None,
    query: Optional[str] = None,
    connection: Optional[str] = None,
    **params,
):
    """Decorator to define a data source node.

    Sources define external data inputs (CSV, Parquet, database tables, etc.).
    The decorated function typically doesn't contain logic - data is loaded
    by the executor based on the source configuration.

    Args:
        name: Source name (e.g., "raw_users")
        source: Source type (csv, parquet, postgres, etc.)
        table: Table or file path
        columns: Optional column schema definitions
        tags: Optional tags for organization
        materialization: Optional Iceberg materialization config (dict or MaterializationConfig)
        query: Optional pushdown SQL query for database sources
        connection: Optional named connection profile
        **params: Additional source-specific parameters

    Example:
        @source(name="users", source="csv", table="data/users.csv")
        def users():
            pass

        @source(
            name="pg_orders",
            source="postgres",
            connection="my_pg",
            query="SELECT * FROM orders WHERE status = 'active'",
        )
        def pg_orders():
            pass
    """
    # Validate materialization config
    _validate_materialization_config(materialization)

    # Pass query and connection through params
    if query is not None:
        params["query"] = query
    if connection is not None:
        params["connection"] = connection

    def decorator(func: Callable) -> Callable:
        node_id = f"source.{name}"

        @wraps(func)
        def wrapper(ctx=None):
            # Sources return data (loaded by function or executor)
            result = func() if ctx is None else func(ctx)
            # Store output if context is provided
            if ctx is not None and result is not None:
                ctx._store_output(node_id, result)
            return result

        # Normalize materialization to dict
        mat_dict = None
        if materialization is not None:
            if isinstance(materialization, dict):
                mat_dict = materialization
            elif isinstance(materialization, MaterializationConfig):
                mat_dict = materialization.to_dict()

        # Register node metadata
        _set_node_meta(wrapper, node_id, {
            "kind": "source",
            "name": name,
            "id": node_id,
            "source": source,
            "table": table,
            "columns": columns or {},
            "tags": tags or [],
            "params": params,
            "materialization": mat_dict,
            "func": func,
        })
        return wrapper
    return decorator


def transform(
    name: Optional[str] = None,
    sql: Optional[str] = None,
    inputs: Optional[list[str]] = None,
    tags: Optional[list[str]] = None,
    materialization: Optional[Union[dict, MaterializationConfig]] = None,
    **params,
):
    """Decorator to define a transformation node.

    Transforms process data from upstream sources and produce transformed outputs.
    The decorated function receives a PipelineContext (ctx) and must return a DataFrame.

    Args:
        name: Transform name (defaults to function name if not specified)
        sql: Optional SQL transform (alternative to Python function)
        inputs: List of upstream node IDs this transform depends on
        tags: Optional tags for organization
        materialization: Optional Iceberg materialization config (dict or MaterializationConfig)
        **params: Additional transform-specific parameters

    Example:
        @transform(name="clean_users")
        def clean_users(ctx):
            df = ctx.ref("source.raw_users")
            return ctx.duckdb.sql("SELECT * FROM df WHERE active = true").df()

        @transform(
            name="sales_forecast",
            inputs=["source.orders"],
            tags=["ml"],
            materialization=MaterializationConfig(
                enabled=True,
                table="warehouse.prod.sales_forecast",
                mode="overwrite",
            )
        )
        def sales_forecast(ctx):
            df = ctx.ref("source.orders")
            return ctx.duckdb.sql("SELECT * FROM df").df()
    """
    # Validate materialization config
    _validate_materialization_config(materialization)

    def decorator(func: Callable) -> Callable:
        node_name = name or getattr(func, "__name__", "unknown")
        node_id = f"transform.{node_name}"

        @wraps(func)
        def wrapper(ctx):
            result = func(ctx)
            if result is not None and ctx is not None:
                ctx._store_output(node_id, result)
            return result

        # Extract dependencies from inputs or analyze function
        deps = inputs or []

        # Normalize materialization to dict
        mat_dict = None
        if materialization is not None:
            if isinstance(materialization, dict):
                mat_dict = materialization
            elif isinstance(materialization, MaterializationConfig):
                mat_dict = materialization.to_dict()

        _set_node_meta(wrapper, node_id, {
            "kind": "transform",
            "name": node_name,
            "id": node_id,
            "sql": sql,
            "inputs": [{"ref": d} for d in deps],
            "tags": tags or [],
            "params": params,
            "materialization": mat_dict,
            "func": func,
        })
        return wrapper
    return decorator


def feature_group(
    name: Optional[str] = None,
    entity: Optional[Any] = None,
    features: Optional[dict] = None,
    inputs: Optional[list[str]] = None,
    materialization: Optional[Any] = None,
    tags: Optional[list[str]] = None,
    **params,
):
    """Decorator to define a feature group node.

    Feature groups define sets of related features for ML models.
    The decorated function receives a PipelineContext (ctx) and must return a DataFrame.

    Args:
        name: Feature group name (defaults to function name if not specified)
        entity: Entity name or config for joins. Can be a string (e.g., "customer")
            which auto-infers join_keys as ["customer_id"], or a dict with explicit
            keys: {"name": "customer", "join_keys": ["cust_id"]}
        features: Optional feature schema definitions
        inputs: List of upstream node IDs this feature group depends on
        materialization: Optional Materialization config for offline/online stores
            (Can be Materialization, MaterializationConfig, or dict)
        tags: Optional tags for organization
        **params: Additional feature group-specific parameters

    Example:
        from seeknal.pipeline.materialization import Materialization, OfflineConfig
        from seeknal.pipeline.materialization_config import MaterializationConfig

        # With Materialization (offline/online stores)
        @feature_group(
            name="user_features",
            entity="user",
            materialization=Materialization(offline=OfflineConfig(format="parquet")),
        )
        def user_features(ctx):
            df = ctx.ref("transform.clean_users")
            return ctx.duckdb.sql("SELECT * FROM df").df()

        # With MaterializationConfig (Iceberg inline)
        @feature_group(
            name="user_features_v2",
            entity="user",
            materialization=MaterializationConfig(
                enabled=True,
                table="warehouse.online.user_features_v2",
                mode="append",
            ),
        )
        def user_features_v2(ctx):
            return ctx.duckdb.sql("SELECT * FROM df").df()
    """
    def decorator(func: Callable) -> Callable:
        node_name = name or getattr(func, "__name__", "unknown")
        node_id = f"feature_group.{node_name}"

        @wraps(func)
        def wrapper(ctx):
            result = func(ctx)
            if result is not None and ctx is not None:
                ctx._store_output(node_id, result)
            return result

        deps = inputs or []

        # Convert materialization to dict if provided
        mat_dict = None
        if materialization is not None:
            # Handle both old Materialization and new MaterializationConfig
            if isinstance(materialization, dict):
                mat_dict = materialization
            elif isinstance(materialization, MaterializationConfig):
                mat_dict = materialization.to_dict()
            elif hasattr(materialization, 'to_dict'):
                # Old Materialization object
                mat_dict = materialization.to_dict()

        _set_node_meta(wrapper, node_id, {
            "kind": "feature_group",
            "name": node_name,
            "id": node_id,
            "entity": entity,
            "features": features or {},
            "inputs": [{"ref": d} for d in deps],
            "materialization": mat_dict,
            "tags": tags or [],
            "params": params,
            "func": func,
        })
        return wrapper
    return decorator


def second_order_aggregation(
    name: str,
    id_col: str,
    feature_date_col: str,
    features: dict,
    application_date_col: Optional[str] = None,
    description: Optional[str] = None,
    owner: Optional[str] = None,
    tags: Optional[list[str]] = None,
    materialization: Optional[Union[dict, Any]] = None,
    **params,
):
    """Decorator to define a second-order aggregation node using the built-in SOA engine.

    The decorated function loads data via ctx.ref() (supporting multiple sources),
    optionally joins/filters, and returns a DataFrame. The built-in SOA engine
    (SecondOrderAggregator) then applies the declared aggregations automatically.

    Args:
        name: Second-order aggregation name (e.g., "region_metrics")
        id_col: Entity ID column for grouping (e.g., "region")
        feature_date_col: Date column for time-based operations
        features: Aggregation specifications dict, same structure as YAML features block
        application_date_col: Optional reference date column for window calculations
        description: Optional human-readable description
        owner: Optional team/person responsible
        tags: Optional tags for organization
        materialization: Optional Iceberg materialization config (dict or MaterializationConfig)
        **params: Additional parameters

    Example:
        @second_order_aggregation(
            name="region_metrics",
            id_col="region",
            feature_date_col="order_date",
            application_date_col="application_date",
            features={
                "daily_amount": {"basic": ["sum", "mean", "max", "stddev"]},
                "daily_count": {"basic": ["sum", "mean"]},
            },
        )
        def region_metrics(ctx):
            return ctx.ref("transform.customer_daily_agg")
    """
    # Validate materialization config
    _validate_materialization_config(materialization)

    def decorator(func: Callable) -> Callable:
        node_id = f"second_order_aggregation.{name}"

        @wraps(func)
        def wrapper(ctx):
            result = func(ctx)
            if result is not None and ctx is not None:
                ctx._store_output(node_id, result)
            return result

        # Normalize materialization to dict
        mat_dict = None
        if materialization is not None:
            if isinstance(materialization, dict):
                mat_dict = materialization
            elif hasattr(materialization, 'to_dict'):
                mat_dict = materialization.to_dict()

        _set_node_meta(wrapper, node_id, {
            "kind": "second_order_aggregation",
            "name": name,
            "id": node_id,
            "description": description,
            "owner": owner,
            "id_col": id_col,
            "feature_date_col": feature_date_col,
            "application_date_col": application_date_col,
            "features": features,
            "tags": tags or [],
            "materialization": mat_dict,
            "params": params,
            "func": func,
        })
        return wrapper
    return decorator


def get_registered_nodes() -> dict[str, dict]:
    """Get all registered pipeline nodes.

    Returns a copy of the global registry containing all nodes
    registered via @source, @transform, and @feature_group decorators.

    Returns:
        Dictionary mapping node_id to node metadata

    Example:
        from seeknal.pipeline.decorators import get_registered_nodes

        nodes = get_registered_nodes()
        for node_id, node_meta in nodes.items():
            print(f"{node_id}: {node_meta['kind']}")
    """
    return _PIPELINE_REGISTRY.copy()


def clear_registry() -> None:
    """Clear the node registry.

    Removes all registered nodes from the global registry.
    Primarily used for testing to ensure isolation between test cases.

    Example:
        from seeknal.pipeline.decorators import clear_registry

        def test_pipeline():
            clear_registry()
            # Test code that registers nodes
            assert len(get_registered_nodes()) == 0
    """
    _PIPELINE_REGISTRY.clear()
