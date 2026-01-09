"""DAG dependency tracking for Seeknal 2.0."""
from seeknal.dag.registry import DependencyRegistry, DependencyType, Dependency
from seeknal.dag.functions import (
    source,
    ref,
    use_transform,
    use_rule,
    use_aggregation,
    SourceReference,
    NodeReference,
    RuleExpression,
    AggregationReference,
)
from seeknal.dag.manifest import (
    Manifest,
    ManifestMetadata,
    Node,
    NodeType,
    Edge,
)
from seeknal.dag.parser import ProjectParser
from seeknal.dag.diff import ManifestDiff, DiffType, NodeChange, EdgeChange

__all__ = [
    # Registry
    "DependencyRegistry",
    "DependencyType",
    "Dependency",
    # Functions
    "source",
    "ref",
    "use_transform",
    "use_rule",
    "use_aggregation",
    "SourceReference",
    "NodeReference",
    "RuleExpression",
    "AggregationReference",
    # Manifest
    "Manifest",
    "ManifestMetadata",
    "Node",
    "NodeType",
    "Edge",
    # Parser
    "ProjectParser",
    # Diff
    "ManifestDiff",
    "DiffType",
    "NodeChange",
    "EdgeChange",
]
