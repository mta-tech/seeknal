"""
Core dependency declaration functions.

These functions are the primary API for declaring dependencies in Seeknal 2.0:
- source() - Reference external data sources
- ref() - Reference other nodes (feature groups, transforms, models)
- use_transform() - Apply a reusable transformation
- use_rule() - Apply a reusable business rule
- use_aggregation() - Apply a reusable aggregation
"""
from dataclasses import dataclass, field
from typing import Any, Optional

from seeknal.dag.registry import DependencyRegistry, DependencyType

# Node ID prefixes for dependency registration
_PREFIX_SOURCE = "source."
_PREFIX_TRANSFORM = "transform."
_PREFIX_RULE = "rule."
_PREFIX_AGGREGATION = "aggregation."


@dataclass(slots=True)
class SourceReference:
    """Reference to a data source."""
    name: str
    source_type: str = "common_config"
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NodeReference:
    """Reference to another node in the DAG."""
    name: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class RuleExpression:
    """Expression from a business rule."""
    rule_id: str
    params: dict[str, Any] = field(default_factory=dict)


def source(
    source_id_or_type: str,
    table_or_path: Optional[str] = None,
    **params: Any
) -> SourceReference:
    """
    Declare a dependency on a data source.

    Can be used in two ways:
    1. Reference from common config: source("traffic_day")
    2. Inline declaration: source("hive", "db.table")

    Args:
        source_id_or_type: Either source ID from common config, or source type
        table_or_path: Table name or path (for inline declaration)
        **params: Additional parameters

    Returns:
        SourceReference object
    """
    registry = DependencyRegistry.get_instance()
    current_node = registry.get_current_node()

    if table_or_path is not None:
        # Inline declaration: source("hive", "db.table")
        source_type = source_id_or_type
        source_name = table_or_path
        source_ref = SourceReference(
            name=source_name,
            source_type=source_type,
            params=dict(params)
        )
    else:
        # Common config reference: source("traffic_day")
        source_name = source_id_or_type
        source_ref = SourceReference(
            name=source_name,
            source_type="common_config",
            params=dict(params)
        )

    # Register the dependency if we have a current node context
    if current_node:
        registry.register_dependency(
            node_id=current_node,
            dependency_id=f"{_PREFIX_SOURCE}{source_name}",
            dependency_type=DependencyType.SOURCE,
            params=dict(params)
        )

    return source_ref


def ref(node_name: str, **params: Any) -> NodeReference:
    """
    Declare a dependency on another node.

    Creates a DAG edge from the current node to the referenced node.

    Args:
        node_name: Name of the node to reference
        **params: Additional parameters

    Returns:
        NodeReference object
    """
    registry = DependencyRegistry.get_instance()
    current_node = registry.get_current_node()

    node_ref = NodeReference(name=node_name, params=dict(params))

    # Register the dependency if we have a current node context
    if current_node:
        registry.register_dependency(
            node_id=current_node,
            dependency_id=node_name,
            dependency_type=DependencyType.REF,
            params=dict(params)
        )

    return node_ref


def use_transform(
    transform_id: str,
    df: Any,
    **params: Any
) -> Any:
    """
    Apply a reusable transformation from common config.

    Args:
        transform_id: ID of the transformation in common config
        df: DataFrame to transform
        **params: Parameters to pass to the transformation (for templating)

    Returns:
        Transformed DataFrame (currently returns input unchanged)
    """
    registry = DependencyRegistry.get_instance()
    current_node = registry.get_current_node()

    # Register the dependency if we have a current node context
    if current_node:
        registry.register_dependency(
            node_id=current_node,
            dependency_id=f"{_PREFIX_TRANSFORM}{transform_id}",
            dependency_type=DependencyType.TRANSFORM,
            params=dict(params)
        )

    # TODO: Actually apply the transformation from common config
    # For now, just return the input DataFrame
    return df


def use_rule(rule_id: str, **params: Any) -> RuleExpression:
    """
    Get a business rule expression from common config.

    Args:
        rule_id: ID of the rule in common config
        **params: Parameters for rule templating

    Returns:
        RuleExpression that can be used for filtering
    """
    registry = DependencyRegistry.get_instance()
    current_node = registry.get_current_node()

    # Register the dependency if we have a current node context
    if current_node:
        registry.register_dependency(
            node_id=current_node,
            dependency_id=f"{_PREFIX_RULE}{rule_id}",
            dependency_type=DependencyType.RULE,
            params=dict(params)
        )

    return RuleExpression(rule_id=rule_id, params=dict(params))


@dataclass(slots=True)
class AggregationReference:
    """Reference to an aggregation definition from common config."""
    aggregation_id: str
    params: dict[str, Any] = field(default_factory=dict)


def use_aggregation(
    aggregation_id: str,
    df: Any,
    **params: Any
) -> Any:
    """
    Apply a reusable aggregation from common config.

    Args:
        aggregation_id: ID of the aggregation in common config
        df: DataFrame to aggregate
        **params: Parameters for aggregation templating

    Returns:
        Aggregated DataFrame (currently returns input unchanged)
    """
    registry = DependencyRegistry.get_instance()
    current_node = registry.get_current_node()

    # Register the dependency if we have a current node context
    if current_node:
        registry.register_dependency(
            node_id=current_node,
            dependency_id=f"{_PREFIX_AGGREGATION}{aggregation_id}",
            dependency_type=DependencyType.AGGREGATION,
            params=dict(params)
        )

    # TODO: Actually apply the aggregation from common config
    # For now, just return the input DataFrame
    return df
