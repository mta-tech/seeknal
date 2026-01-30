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

from seeknal.workflow.executors.base import (
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

# Import executors to trigger registration via @register_executor decorator
from seeknal.workflow.executors.source_executor import SourceExecutor
from seeknal.workflow.executors.transform_executor import TransformExecutor
from seeknal.workflow.executors.aggregation_executor import AggregationExecutor
from seeknal.workflow.executors.second_order_aggregation_executor import SecondOrderAggregationExecutor
from seeknal.workflow.executors.feature_group_executor import FeatureGroupExecutor
from seeknal.workflow.executors.model_executor import ModelExecutor
from seeknal.workflow.executors.rule_executor import RuleExecutor
from seeknal.workflow.executors.exposure_executor import (
    ExposureExecutor,
    ExposureType,
    FileFormat,
)

# Import PythonExecutor for registration
from seeknal.workflow.executors.python_executor import PythonExecutor

# Type checking imports to avoid circular imports
if TYPE_CHECKING:
    from seeknal.dag.manifest import Node


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
    if hasattr(node, 'file_path') and node.file_path and str(node.file_path).endswith('.py'):
        # Route Python files to PythonExecutor
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
    "ExposureType",
    "FileFormat",
]

# Version info
__version__ = "1.0.0"
