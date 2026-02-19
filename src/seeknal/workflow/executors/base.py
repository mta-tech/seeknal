"""
Base executor interface for Seeknal workflow node execution.

This module defines the abstract interface that all node executors must implement.
It provides the execution result contract, error handling patterns, and the
executor registry for routing node types to their executors.

Design Decisions:
- Executors are CLASSES (not functions) to enable stateful execution contexts
- Each executor handles one node type (source, transform, feature_group, etc.)
- Executors return a standardized ExecutorResult with timing, row counts, status
- Registry pattern enables dynamic executor discovery and registration
- Dry-run mode is handled at the base class level for consistency
- Execution contexts (DuckDB, Spark, external APIs) are passed via ExecutionContext

Key Components:
- BaseExecutor: Abstract base class defining the execution interface
- ExecutorResult: Standardized result dataclass with execution metadata
- ExecutorRegistry: Central registry for routing node types to executors
- ExecutionContext: Encapsulates execution environment (databases, configs, etc.)
- ExecutorError: Base exception for all executor-related errors
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

from seeknal.dag.manifest import Node, NodeType


class ExecutionStatus(Enum):
    """Status of node execution.

    Maps to workflow.runner.ExecutionStatus for consistency,
    but defined here to keep executors independent.
    """
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CACHED = "cached"
    SKIPPED = "skipped"


@dataclass(slots=True)
class ExecutorResult:
    """
    Standardized result of node execution.

    All executors must return this result type to ensure
    consistent reporting and state management across the system.

    Attributes:
        node_id: ID of the executed node
        status: Execution status from ExecutionStatus enum
        duration_seconds: Time taken to execute (in seconds)
        row_count: Number of rows processed/output (0 if not applicable)
        output_path: Optional path to output data (if materialized)
        error_message: Error message if status is FAILED
        metadata: Additional executor-specific metadata (metrics, warnings, etc.)
        is_dry_run: Whether this was a dry-run execution
        created_at: ISO timestamp of execution completion

    Example:
        >>> result = ExecutorResult(
        ...     node_id="transform.clean_users",
        ...     status=ExecutionStatus.SUCCESS,
        ...     duration_seconds=1.5,
        ...     row_count=1000,
        ...     metadata={"engine": "duckdb"}
        ... )
        >>> result.is_success()
        True
    """
    node_id: str
    status: ExecutionStatus
    duration_seconds: float = 0.0
    row_count: int = 0
    output_path: Optional[Path] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_dry_run: bool = False
    created_at: str = field(
        default_factory=lambda: time.time()
    )

    def is_success(self) -> bool:
        """Check if execution was successful."""
        return self.status == ExecutionStatus.SUCCESS

    def is_failed(self) -> bool:
        """Check if execution failed."""
        return self.status == ExecutionStatus.FAILED

    def is_cached(self) -> bool:
        """Check if result was from cache."""
        return self.status == ExecutionStatus.CACHED

    def is_skipped(self) -> bool:
        """Check if execution was skipped."""
        return self.status == ExecutionStatus.SKIPPED

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization.

        Used by runner.py for state persistence and reporting.
        """
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "duration_seconds": self.duration_seconds,
            "row_count": self.row_count,
            "output_path": str(self.output_path) if self.output_path else None,
            "error_message": self.error_message,
            "metadata": self.metadata,
            "is_dry_run": self.is_dry_run,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExecutorResult":
        """Create from dictionary for JSON deserialization."""
        output_path = data.get("output_path")
        return cls(
            node_id=data["node_id"],
            status=ExecutionStatus(data["status"]),
            duration_seconds=data.get("duration_seconds", 0.0),
            row_count=data.get("row_count", 0),
            output_path=Path(output_path) if output_path else None,
            error_message=data.get("error_message"),
            metadata=data.get("metadata", {}),
            is_dry_run=data.get("is_dry_run", False),
            created_at=data.get("created_at", time.time()),
        )


@dataclass(slots=True)
class ExecutionContext:
    """
    Encapsulates the execution environment for node execution.

    This context is passed to all executors and provides access to:
    - Database connections (DuckDB, Spark)
    - File system paths
    - Configuration and parameters
    - State and caching interfaces

    Attributes:
        project_name: Name of the current project
        workspace_path: Path to the workspace directory
        target_path: Path to the target/build directory
        state_dir: Path to the state storage directory
        duckdb_connection: Optional DuckDB connection (if using DuckDB)
        spark_session: Optional Spark session (if using Spark)
        config: Additional configuration dictionary
        dry_run: Whether running in dry-run mode
        verbose: Whether to enable verbose output
        params: Resolved parameters from YAML (with {{ }} expressions)

    Example:
        >>> context = ExecutionContext(
        ...     project_name="my_project",
        ...     workspace_path=Path("~/seeknal"),
        ...     target_path=Path("target"),
        ...     dry_run=False
        ... )
    """
    project_name: str
    workspace_path: Path
    target_path: Path
    state_dir: Path = field(default_factory=lambda: Path("target/state"))
    duckdb_connection: Optional[Any] = None  # duckdb.DuckDBPyConnection
    spark_session: Optional[Any] = None  # pyspark.sql.SparkSession
    config: Dict[str, Any] = field(default_factory=dict)
    dry_run: bool = False
    verbose: bool = False
    materialize_enabled: Optional[bool] = None  # None=use node config
    params: Dict[str, Any] = field(default_factory=dict)  # Resolved parameters
    common_config: Optional[Dict[str, str]] = None  # Flat common config dict

    def get_duckdb_connection(self):
        """Get or create DuckDB connection."""
        if self.duckdb_connection is None:
            import duckdb
            self.duckdb_connection = duckdb.connect(":memory:")
        return self.duckdb_connection

    def get_spark_session(self):
        """Get or create Spark session."""
        if self.spark_session is None:
            from pyspark.sql import SparkSession
            self.spark_session = SparkSession.builder.getOrCreate()
        return self.spark_session

    def get_output_path(self, node: Node) -> Path:
        """Get output path for a node's results."""
        return self.target_path / node.node_type.value / node.name

    def get_cache_path(self, node: Node) -> Path:
        """Get cache path for a node's results.

        Uses target/cache/{kind}/{node_name}.parquet structure.
        """
        cache_dir = self.target_path / "cache" / node.node_type.value
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / f"{node.name}.parquet"


class ExecutorError(Exception):
    """Base exception for all executor-related errors.

    Subclasses provide more specific error types:
    - ExecutorNotFoundError: No executor registered for a node type
    - ExecutorExecutionError: Error during node execution
    - ExecutorValidationError: Node configuration validation failed
    """
    pass


class ExecutorNotFoundError(ExecutorError):
    """Raised when no executor is registered for a node type."""

    def __init__(self, node_type: NodeType):
        self.node_type = node_type
        super().__init__(f"No executor registered for node type: {node_type.value}")


class ExecutorExecutionError(ExecutorError):
    """Raised when node execution fails."""

    def __init__(self, node_id: str, message: str, cause: Optional[Exception] = None):
        self.node_id = node_id
        self.cause = cause
        super().__init__(f"Execution failed for {node_id}: {message}")


class ExecutorValidationError(ExecutorError):
    """Raised when node configuration validation fails."""

    def __init__(self, node_id: str, message: str):
        self.node_id = node_id
        super().__init__(f"Validation failed for {node_id}: {message}")


class BaseExecutor(ABC):
    """
    Abstract base class for all node executors.

    Each node type (source, transform, feature_group, etc.) has its own
    executor subclass that implements the execute() method.

    Design Rationale:
    - Class-based (not function) to enable stateful execution contexts
    - Supports dependency injection via ExecutionContext
    - Provides hooks for validation, pre/post execution
    - Handles dry-run mode consistently at base class level

    Lifecycle:
        1. validate() - Validate node configuration
        2. pre_execute() - Setup (connections, paths, etc.)
        3. execute() - Main execution logic (implemented by subclasses)
        4. post_execute() - Cleanup and result finalization

    Attributes:
        node_type: The NodeType this executor handles
        node: The node being executed
        context: The execution context (databases, paths, config, etc.)

    Example:
        >>> class MyExecutor(BaseExecutor):
        ...     @property
        ...     def node_type(self) -> NodeType:
        ...         return NodeType.TRANSFORM
        ...
        ...     def execute(self) -> ExecutorResult:
        ...         # Implementation here
        ...         return ExecutorResult(...)
    """

    def __init__(self, node: Node, context: ExecutionContext):
        """Initialize the executor.

        Args:
            node: The node to execute
            context: The execution context
        """
        self.node = node
        self.context = context

    @property
    @abstractmethod
    def node_type(self) -> NodeType:
        """
        Return the NodeType this executor handles.

        Used by the registry to route nodes to executors.

        Returns:
            NodeType enum value

        Example:
            >>> @property
            >>> def node_type(self) -> NodeType:
            ...     return NodeType.TRANSFORM
        """
        pass

    def validate(self) -> None:
        """
        Validate the node configuration before execution.

        Implementations should check that required fields are present
        and valid. Raise ExecutorValidationError if validation fails.

        This method is called before execute() and can be overridden
        to provide custom validation logic.

        Raises:
            ExecutorValidationError: If node configuration is invalid

        Example:
            >>> def validate(self) -> None:
            ...     if "transform" not in self.node.config:
            ...         raise ExecutorValidationError(
            ...             self.node.id,
            ...             "Missing required 'transform' field"
            ...         )
        """
        pass

    def pre_execute(self) -> None:
        """
        Perform setup before node execution.

        Implementations can use this to:
        - Create database connections
        - Prepare output directories
        - Load dependencies

        This method is called after validate() and before execute().

        Example:
            >>> def pre_execute(self) -> None:
            ...     output_path = self.context.get_output_path(self.node)
            ...     output_path.mkdir(parents=True, exist_ok=True)
        """
        pass

    @abstractmethod
    def execute(self) -> ExecutorResult:
        """
        Execute the node and return the result.

        This is the main execution method that must be implemented
        by all executor subclasses. It should:
        1. Perform the node's core logic (data processing, validation, etc.)
        2. Measure execution time
        3. Return an ExecutorResult with status, timing, and metadata

        The method should respect context.dry_run - if True, it should
        validate and plan execution but not make changes.

        Returns:
            ExecutorResult with execution outcome

        Raises:
            ExecutorExecutionError: If execution fails

        Example:
            >>> def execute(self) -> ExecutorResult:
            ...     start = time.time()
            ...
            ...     try:
            ...         if self.context.dry_run:
            ...             return ExecutorResult(
            ...                 node_id=self.node.id,
            ...                 status=ExecutionStatus.SUCCESS,
            ...                 duration_seconds=0.0,
            ...                 is_dry_run=True
            ...             )
            ...
            ...         # Actual execution logic
            ...         row_count = self._process_data()
            ...
            ...         return ExecutorResult(
            ...             node_id=self.node.id,
            ...             status=ExecutionStatus.SUCCESS,
            ...             duration_seconds=time.time() - start,
            ...             row_count=row_count
            ...         )
            ...     except Exception as e:
            ...         raise ExecutorExecutionError(self.node.id, str(e), e)
        """
        pass

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Perform cleanup after node execution, including audit execution.

        Runs any audits defined in the node's config `audits:` section.
        On error-severity failure: sets result.status = FAILED.
        On warn-severity failure: adds warning to result.metadata.

        Args:
            result: The result from execute()

        Returns:
            Possibly modified ExecutorResult
        """
        # Run audits if defined
        audit_configs = self.node.config.get("audits", [])
        if audit_configs and result.is_success():
            try:
                from seeknal.workflow.audits import parse_audits, AuditRunner
                audits = parse_audits(self.node.config)
                if audits:
                    conn = self.context.get_duckdb_connection()
                    table_name = f"{self.node.node_type.value}.{self.node.name}"
                    runner = AuditRunner(conn)
                    audit_results = runner.run_audits(audits, table_name)

                    # Store results in metadata
                    result.metadata["audits"] = [r.to_dict() for r in audit_results]

                    # Check for failures
                    for ar in audit_results:
                        if not ar.passed:
                            if ar.severity == "error":
                                result.status = ExecutionStatus.FAILED
                                result.error_message = f"Audit failed: {ar.audit_type} - {ar.message}"
                                break
                            elif ar.severity == "warn":
                                warnings = result.metadata.setdefault("warnings", [])
                                warnings.append(f"Audit warning: {ar.audit_type} - {ar.message}")
            except Exception as e:
                if self.context.verbose:
                    import warnings
                    warnings.warn(f"Audit execution failed: {e}")

        return result

    def run(self) -> ExecutorResult:
        """
        Execute the node with full lifecycle (validate -> pre -> execute -> post).

        This is the main entry point called by the runner. It orchestrates
        the complete execution lifecycle and handles errors consistently.

        Returns:
            ExecutorResult with execution outcome

        Raises:
            ExecutorValidationError: If validation fails
            ExecutorExecutionError: If execution fails

        Example:
            >>> executor = MyExecutor(node, context)
            >>> result = executor.run()
            >>> if result.is_success():
            ...     print(f"Success: {result.row_count} rows")
        """
        # Validate
        try:
            self.validate()
        except ExecutorValidationError:
            raise
        except Exception as e:
            raise ExecutorValidationError(
                self.node.id,
                f"Validation error: {str(e)}"
            ) from e

        # Pre-execution setup
        try:
            self.pre_execute()
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Pre-execution setup failed: {str(e)}",
                e
            ) from e

        # Main execution
        start_time = time.time()
        try:
            result = self.execute()

            # Ensure duration is set
            if result.duration_seconds == 0.0:
                result.duration_seconds = time.time() - start_time

            # Set dry_run flag if not already set
            if not result.is_dry_run:
                result.is_dry_run = self.context.dry_run

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Execution failed: {str(e)}",
                e
            ) from e

        # Post-execution cleanup
        try:
            result = self.post_execute(result)
        except Exception as e:
            # Log warning but don't fail execution
            if self.context.verbose:
                import warnings
                warnings.warn(f"Post-execution cleanup failed: {str(e)}")

        return result


class ExecutorRegistry:
    """
    Central registry for node type executors.

    The registry maps NodeTypes to executor classes, enabling the runner
    to dynamically route nodes to their appropriate executors.

    Design Pattern:
    - Singleton-like registry (global instance)
    - Decorator-based registration (@register_executor)
    - Automatic discovery via __init_subclass__ hook

    Example:
        >>> # Register an executor
        >>> registry = ExecutorRegistry.get_instance()
        >>> registry.register(MyExecutor)
        >>>
        >>> # Get executor for a node
        >>> executor_cls = registry.get_executor(NodeType.TRANSFORM)
        >>> executor = executor_cls(node, context)
        >>> result = executor.run()
    """

    _instance: Optional[ExecutorRegistry] = None
    _executors: Dict[NodeType, Type[BaseExecutor]] = {}

    def __new__(cls) -> "ExecutorRegistry":
        """Singleton pattern to ensure only one registry exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "ExecutorRegistry":
        """Get the global registry instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def register(self, executor_cls: Type[BaseExecutor]) -> None:
        """
        Register an executor class for its node type.

        Args:
            executor_cls: Executor class to register

        Raises:
            ValueError: If executor already registered for this node type

        Example:
            >>> registry = ExecutorRegistry.get_instance()
            >>> registry.register(TransformExecutor)
        """
        # Create a temporary instance to get node_type
        # We need a temporary node and context for this
        # This is a bit awkward - alternative is to make node_type a class attribute
        # See register_executor() decorator for cleaner approach

        # For now, use class-level attribute if defined
        if hasattr(executor_cls, '_node_type'):
            node_type = executor_cls._node_type
        else:
            raise ValueError(
                f"Executor class {executor_cls.__name__} must define "
                "'_node_type' class attribute or override node_type property"
            )

        if node_type in self._executors:
            raise ValueError(
                f"Executor already registered for {node_type.value}: "
                f"{self._executors[node_type].__name__}"
            )

        self._executors[node_type] = executor_cls

    def get_executor(self, node_type: NodeType) -> Type[BaseExecutor]:
        """
        Get executor class for a node type.

        Args:
            node_type: NodeType to get executor for

        Returns:
            Executor class

        Raises:
            ExecutorNotFoundError: If no executor registered for this type

        Example:
            >>> registry = ExecutorRegistry.get_instance()
            >>> executor_cls = registry.get_executor(NodeType.TRANSFORM)
        """
        if node_type not in self._executors:
            raise ExecutorNotFoundError(node_type)

        return self._executors[node_type]

    def create_executor(
        self,
        node: Node,
        context: ExecutionContext
    ) -> BaseExecutor:
        """
        Create executor instance for a node.

        Args:
            node: Node to create executor for
            context: Execution context

        Returns:
            Executor instance

        Raises:
            ExecutorNotFoundError: If no executor registered for this type

        Example:
            >>> registry = ExecutorRegistry.get_instance()
            >>> executor = registry.create_executor(node, context)
            >>> result = executor.run()
        """
        executor_cls = self.get_executor(node.node_type)
        return executor_cls(node, context)

    def list_registered(self) -> List[NodeType]:
        """
        List all registered node types.

        Returns:
            List of registered NodeTypes

        Example:
            >>> registry = ExecutorRegistry.get_instance()
            >>> for node_type in registry.list_registered():
            ...     print(node_type.value)
        """
        return list(self._executors.keys())

    def clear(self) -> None:
        """Clear all registered executors (mainly for testing)."""
        self._executors.clear()


def register_executor(node_type: NodeType):
    """
    Decorator to register an executor class.

    This decorator sets the _node_type class attribute and registers
    the executor with the global registry.

    Example:
        >>> @register_executor(NodeType.TRANSFORM)
        >>> class TransformExecutor(BaseExecutor):
        ...     def execute(self) -> ExecutorResult:
        ...         # Implementation
        ...         pass
    """
    def decorator(executor_cls: Type[BaseExecutor]) -> Type[BaseExecutor]:
        # Set class attribute
        executor_cls._node_type = node_type

        # Register with global registry
        registry = ExecutorRegistry.get_instance()
        registry.register(executor_cls)

        return executor_cls

    return decorator


# Convenience function for getting executor instance
def get_executor(node: Node, context: ExecutionContext) -> BaseExecutor:
    """
    Get executor instance for a node.

    This is a convenience function that wraps the registry.

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
    registry = ExecutorRegistry.get_instance()
    return registry.create_executor(node, context)
