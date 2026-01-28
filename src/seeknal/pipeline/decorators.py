"""
Python pipeline decorators for defining data sources, transforms, and feature groups.

Provides @source, @transform, and @feature_group decorators that register
pipeline nodes in a global registry for later discovery and execution.
"""

from functools import wraps
from typing import Callable, Optional, Any

# Global registry for discovered nodes
_PIPELINE_REGISTRY: dict[str, dict] = {}


def source(
    name: str,
    source: str = "csv",
    table: str = "",
    columns: Optional[dict] = None,
    tags: Optional[list[str]] = None,
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
        **params: Additional source-specific parameters

    Example:
        @source(name="users", source="csv", table="data/users.csv")
        def users():
            pass

        @source(
            name="events",
            source="postgres",
            table="public.events",
            columns={"event_id": "int", "user_id": "int"},
            tags=["production"]
        )
        def events():
            pass
    """
    def decorator(func: Callable) -> Callable:
        node_id = f"source.{name}"

        @wraps(func)
        def wrapper(ctx=None):
            # Sources typically don't have logic - data loaded by executor
            return func() if ctx is None else func(ctx)

        # Register node metadata
        wrapper._seeknal_node = {
            "kind": "source",
            "name": name,
            "id": node_id,
            "source": source,
            "table": table,
            "columns": columns or {},
            "tags": tags or [],
            "params": params,
            "func": func,
        }
        _PIPELINE_REGISTRY[node_id] = wrapper._seeknal_node
        return wrapper
    return decorator


def transform(
    name: Optional[str] = None,
    sql: Optional[str] = None,
    inputs: Optional[list[str]] = None,
    tags: Optional[list[str]] = None,
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
        **params: Additional transform-specific parameters

    Example:
        @transform(name="clean_users")
        def clean_users(ctx):
            df = ctx.ref("source.raw_users")
            return ctx.duckdb.sql("SELECT * FROM df WHERE active = true").df()

        @transform(
            name="user_features",
            inputs=["source.users", "source.events"],
            tags=["feature-engineering"]
        )
        def user_features(ctx):
            users = ctx.ref("source.users")
            events = ctx.ref("source.events")
            sql = "SELECT u.user_id, COUNT(e.event_id) as event_count " \
                  "FROM users u LEFT JOIN events e ON u.user_id = e.user_id " \
                  "GROUP BY u.user_id"
            return ctx.duckdb.sql(sql).df()
    """
    def decorator(func: Callable) -> Callable:
        node_name = name or func.__name__
        node_id = f"transform.{node_name}"

        @wraps(func)
        def wrapper(ctx):
            result = func(ctx)
            if result is not None and ctx is not None:
                ctx._store_output(node_id, result)
            return result

        # Extract dependencies from inputs or analyze function
        deps = inputs or []

        wrapper._seeknal_node = {
            "kind": "transform",
            "name": node_name,
            "id": node_id,
            "sql": sql,
            "inputs": [{"ref": d} for d in deps],
            "tags": tags or [],
            "params": params,
            "func": func,
        }
        _PIPELINE_REGISTRY[node_id] = wrapper._seeknal_node
        return wrapper
    return decorator


def feature_group(
    name: Optional[str] = None,
    entity: Optional[str] = None,
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
        entity: Entity name for joins (e.g., "user", "product")
        features: Optional feature schema definitions
        inputs: List of upstream node IDs this feature group depends on
        materialization: Optional Materialization config for offline/online stores
        tags: Optional tags for organization
        **params: Additional feature group-specific parameters

    Example:
        from seeknal.pipeline.materialization import Materialization, OfflineConfig

        @feature_group(
            name="user_features",
            entity="user",
            materialization=Materialization(offline=OfflineConfig(format="parquet")),
        )
        def user_features(ctx):
            df = ctx.ref("transform.clean_users")
            sql = "SELECT user_id, COUNT(*) as total_events, " \
                  "AVG(session_duration) as avg_session_duration " \
                  "FROM df GROUP BY user_id"
            return ctx.duckdb.sql(sql).df()
    """
    def decorator(func: Callable) -> Callable:
        node_name = name or func.__name__
        node_id = f"feature_group.{node_name}"

        @wraps(func)
        def wrapper(ctx):
            result = func(ctx)
            if result is not None and ctx is not None:
                ctx._store_output(node_id, result)
            return result

        deps = inputs or []

        # Convert Materialization to dict if provided
        mat_dict = None
        if materialization is not None:
            mat_dict = materialization.to_dict()

        wrapper._seeknal_node = {
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
        }
        _PIPELINE_REGISTRY[node_id] = wrapper._seeknal_node
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
