"""
Core dependency declaration functions.

These functions are the primary API for declaring dependencies in Seeknal 2.0:
- source() - Reference external data sources
- ref() - Reference other nodes (feature groups, transforms, models)
- use_transform() - Apply a reusable transformation
- use_rule() - Apply a reusable business rule
"""
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union
import pandas as pd

from seeknal.dag.registry import DependencyRegistry, DependencyType


@dataclass
class SourceReference:
    """Reference to a data source."""
    name: str
    source_type: str = "common_config"
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dataframe(self) -> pd.DataFrame:
        """Load the source as a DataFrame (placeholder for actual implementation)."""
        # This will be implemented to actually load data
        raise NotImplementedError("Source loading not yet implemented")


@dataclass
class NodeReference:
    """Reference to another node in the DAG."""
    name: str
    params: Dict[str, Any] = field(default_factory=dict)

    def offline(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> "NodeReference":
        """Get offline store data for this node."""
        self.params["store"] = "offline"
        self.params["start_date"] = start_date
        self.params["end_date"] = end_date
        return self

    def online(self) -> "NodeReference":
        """Get online store data for this node."""
        self.params["store"] = "online"
        return self


@dataclass
class RuleExpression:
    """Expression from a business rule."""
    rule_id: str
    params: Dict[str, Any] = field(default_factory=dict)

    def to_sql(self) -> str:
        """Convert rule to SQL expression (placeholder)."""
        # This will be implemented to resolve rule from common config
        raise NotImplementedError("Rule resolution not yet implemented")


def source(
    source_id_or_type: str,
    table_or_path: Optional[str] = None,
    **params
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
        ref = SourceReference(
            name=source_name,
            source_type=source_type,
            params=params
        )
    else:
        # Common config reference: source("traffic_day")
        source_name = source_id_or_type
        ref = SourceReference(
            name=source_name,
            source_type="common_config",
            params=params
        )

    # Register the dependency if we have a current node context
    if current_node:
        registry.register_dependency(
            node_id=current_node,
            dependency_id=f"source.{source_name}",
            dependency_type=DependencyType.SOURCE,
            params=params
        )

    return ref


def ref(node_name: str, **params) -> NodeReference:
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

    node_ref = NodeReference(name=node_name, params=params)

    # Register the dependency if we have a current node context
    if current_node:
        registry.register_dependency(
            node_id=current_node,
            dependency_id=node_name,
            dependency_type=DependencyType.REF,
            params=params
        )

    return node_ref


def use_transform(
    transform_id: str,
    df: Any,
    **params
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
            dependency_id=f"transform.{transform_id}",
            dependency_type=DependencyType.TRANSFORM,
            params=params
        )

    # TODO: Actually apply the transformation from common config
    # For now, just return the input DataFrame
    return df


def use_rule(rule_id: str, **params) -> RuleExpression:
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
            dependency_id=f"rule.{rule_id}",
            dependency_type=DependencyType.RULE,
            params=params
        )

    return RuleExpression(rule_id=rule_id, params=params)
