"""
Workflow executors for Seeknal node execution.

This package provides executors for all node types in the workflow DAG.
Each executor implements the BaseExecutor interface and handles execution
of a specific node type.

Usage:
    from seeknal.workflow.executors import get_executor, ExecutionContext

    # Create execution context
    context = ExecutionContext(
        project_name="my_project",
        workspace_path=Path("~/seeknal"),
        target_path=Path("target"),
    )

    # Get executor for a node
    executor = get_executor(node, context)
    result = executor.run()
"""

from pathlib import Path
from typing import TYPE_CHECKING

from seeknal.workflow.executors.base import (  # ty: ignore[unresolved-import]
    # Core interfaces
    BaseExecutor,
    ExecutorResult,
    ExecutionContext,
    ExecutionStatus,

    # Registry
    ExecutorRegistry,
    register_executor,

    # Exceptions
    ExecutorError,
    ExecutorNotFoundError,
    ExecutorExecutionError,
    ExecutorValidationError,
)

# Import parameter helper
from seeknal.workflow.parameters.helpers import get_param, list_params, has_param  # ty: ignore[unresolved-import]

# Import executors to trigger registration via @register_executor decorator
from seeknal.workflow.executors.source_executor import SourceExecutor  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.transform_executor import TransformExecutor  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.aggregation_executor import AggregationExecutor  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.second_order_aggregation_executor import SecondOrderAggregationExecutor  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.feature_group_executor import FeatureGroupExecutor  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.model_executor import ModelExecutor  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.rule_executor import RuleExecutor  # ty: ignore[unresolved-import]
from seeknal.workflow.executors.exposure_executor import (  # ty: ignore[unresolved-import]
    ExposureExecutor,
    ExposureType,
    FileFormat,
)

# Import PythonExecutor for registration
from seeknal.workflow.executors.python_executor import PythonExecutor  # ty: ignore[unresolved-import]

# Import semantic model executors for registration
from seeknal.workflow.executors.semantic_model_executor import (  # ty: ignore[unresolved-import]
    SemanticModelExecutor,
    MetricExecutor,
)

# Type checking imports to avoid circular imports
if TYPE_CHECKING:
    from seeknal.dag.manifest import Node  # ty: ignore[unresolved-import]


def get_executor(node: "Node", context: ExecutionContext) -> BaseExecutor:
    """
    Get executor instance for a node, with special handling for Python files.

    This is a wrapper around the registry's get_executor that checks
    if the node comes from a Python file (.py extension) and routes
    it to the PythonExecutor instead of the standard YAML executors.

    Args:
        node: Node to get executor for
        context: Execution context

    Returns:
        Executor instance

    Raises:
        ExecutorNotFoundError: If no executor registered for this type

    Example:
        >>> executor = get_executor(node, context)
        >>> result = executor.run()
    """
    # Check if node is from a Python file (has file_path attribute ending in .py)
    # Source nodes are always handled by SourceExecutor (even from Python files)
    # because SourceExecutor has specialized loaders (iceberg, csv, parquet, etc.)
    if hasattr(node, 'file_path') and node.file_path and str(node.file_path).endswith('.py'):
        from seeknal.dag.manifest import NodeType as NT  # ty: ignore[unresolved-import]
        if node.node_type != NT.SOURCE:
            return PythonExecutor(node, context)

    # Use registry for standard YAML executors
    registry = ExecutorRegistry.get_instance()
    return registry.create_executor(node, context)

__all__ = [
    # Core interfaces
    "BaseExecutor",
    "ExecutorResult",
    "ExecutionContext",
    "ExecutionStatus",

    # Registry
    "ExecutorRegistry",
    "register_executor",
    "get_executor",

    # Exceptions
    "ExecutorError",
    "ExecutorNotFoundError",
    "ExecutorExecutionError",
    "ExecutorValidationError",

    # Executors
    "SourceExecutor",
    "TransformExecutor",
    "AggregationExecutor",
    "SecondOrderAggregationExecutor",
    "FeatureGroupExecutor",
    "ModelExecutor",
    "RuleExecutor",
    "ExposureExecutor",
    "PythonExecutor",
    "SemanticModelExecutor",
    "MetricExecutor",
    "ExposureType",
    "FileFormat",

    # Parameter helpers
    "get_param",
    "list_params",
    "has_param",
]

# Version info
__version__ = "2.1.0"
