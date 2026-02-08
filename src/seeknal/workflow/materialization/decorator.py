"""
Materialization decorator for Seeknal workflow executors.

This module provides decorators for adding Iceberg materialization
to executor classes without modifying their core logic.

Design Decisions:
- Decorator pattern for separation of concerns
- Per-node configuration from YAML
- Atomic materialization with automatic rollback
- Graceful degradation if materialization fails

Key Components:
- materialize_if_enabled: Main decorator for conditional materialization
- MaterializationMixin: Mixin class with materialization logic
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Callable, Dict, Optional

from seeknal.workflow.executors.base import (
    BaseExecutor,
    ExecutionContext,
    ExecutorResult,
    ExecutionStatus,
)
from seeknal.workflow.materialization.config import (
    MaterializationConfig,
    ConfigurationError,
)
from seeknal.workflow.materialization.profile_loader import ProfileLoader
from seeknal.workflow.materialization.operations import (
    write_to_iceberg,
    validate_schema_compatibility,
    atomic_materialization,
    create_iceberg_table,
    DuckDBIcebergExtension,
    MaterializationAuditor,
    _get_table_schema,
)

logger = logging.getLogger(__name__)


class MaterializationMixin:
    """
    Mixin class providing materialization capabilities.

    This mixin can be used by executors that need materialization support.
    It provides methods for checking configuration and performing materialization.

    Note: This mixin must be applied before BaseExecutor in the MRO.
    Use multiple inheritance: class MyExecutor(MaterializationMixin, BaseExecutor)

    Attributes:
        profile_loader: Profile loader instance
        config: Merged materialization configuration
        auditor: Audit logger
    """

    def __init__(self, node: Any, context: ExecutionContext, *args, **kwargs):
        """
        Initialize mixin with node and context.

        Args:
            node: The node being executed
            context: The execution context
        """
        # Store node and context for later use
        self._materialization_node = node
        self._materialization_context = context

        # Initialize private attributes
        self._materialization_config: Optional[MaterializationConfig] = None
        self._profile_loader: Optional[ProfileLoader] = None
        self._auditor: Optional[MaterializationAuditor] = None

        # Call super().__init__ if there are more base classes
        super().__init__(*args, **kwargs)

    @property
    def _mat_node(self) -> Any:
        """Get the node (for compatibility with BaseExecutor)."""
        # If the class has 'node' attribute from BaseExecutor, use it
        if hasattr(self, 'node'):
            return self.node
        # Otherwise use the stored node
        return self._materialization_node

    @property
    def _mat_context(self) -> ExecutionContext:
        """Get the context (for compatibility with BaseExecutor)."""
        # If the class has 'context' attribute from BaseExecutor, use it
        if hasattr(self, 'context'):
            return self.context
        # Otherwise use the stored context
        return self._materialization_context

    def _load_materialization_config(self) -> Optional[MaterializationConfig]:
        """
        Load materialization configuration for this node.

        Returns:
            MaterializationConfig if enabled, None otherwise

        Raises:
            ConfigurationError: If configuration is invalid
        """
        # Lazy initialization of profile loader
        if self._profile_loader is None:
            self._profile_loader = ProfileLoader()

        # Load profile
        profile_config = self._profile_loader.load_profile()

        # Check if globally enabled
        if not profile_config.enabled:
            return None

        # Get node (handle both direct and BaseExecutor cases)
        node = self._mat_node

        # Check node-specific configuration
        node_config = node.config.get("materialization", {})

        # Merge profile with node config
        merged_config = profile_config.merge_with_node_config(node_config)

        # Check if enabled for this node
        if not merged_config.enabled:
            return None

        # Validate configuration
        merged_config.validate()

        self._materialization_config = merged_config
        return merged_config

    def _setup_catalog(self, con: Any) -> str:
        """
        Setup REST catalog connection in DuckDB.

        Args:
            con: DuckDB connection

        Returns:
            Catalog name

        Raises:
            ConfigurationError: If catalog setup fails
        """
        config = self._materialization_config

        # Interpolate environment variables
        catalog = config.catalog.interpolate_env_vars()

        # Create catalog
        catalog_name = "iceberg_catalog"

        DuckDBIcebergExtension.create_rest_catalog(
            con=con,
            catalog_name=catalog_name,
            uri=catalog.uri,
            warehouse_path=catalog.warehouse,
            bearer_token=catalog.bearer_token,
        )

        return catalog_name

    def _get_or_create_table(
        self,
        con: Any,
        catalog_name: str,
        schema: Dict[str, str],
    ) -> str:
        """
        Get or create Iceberg table.

        Args:
            con: DuckDB connection
            catalog_name: Catalog name
            schema: Table schema

        Returns:
            Fully qualified table name
        """
        config = self._materialization_config
        node = self._mat_node

        # Determine table name
        if config.table:
            table_name = config.table
        else:
            # Use node ID as table name
            # e.g., "source.orders" -> "warehouse.source.orders"
            table_name = f"warehouse.{node.id}"

        # Check if table exists
        try:
            existing_schema = _get_table_schema(con, f"{catalog_name}.{table_name}")

            # Validate schema compatibility
            validate_schema_compatibility(
                con=con,
                table_name=f"{catalog_name}.{table_name}",
                new_schema=schema,
                config=config,
            )

            return f"{catalog_name}.{table_name}"

        except Exception:
            # Table doesn't exist, create it
            return create_iceberg_table(
                con=con,
                catalog_name=catalog_name,
                table_name=table_name,
                schema=schema,
                partition_by=config.partition_by if config.partition_by else None,
                config=config,
            )

    def _infer_schema(self) -> Dict[str, str]:
        """
        Infer schema from node configuration.

        For source nodes, uses the schema from YAML.
        For transform nodes, infers from the DuckDB view.

        Returns:
            Schema mapping (column_name -> type)
        """
        node = self._mat_node

        # Check if schema is defined in node config
        if "schema" in node.config:
            return self._schema_from_yaml()

        # Infer from DuckDB view
        return self._schema_from_duckdb()

    def _schema_from_yaml(self) -> Dict[str, str]:
        """Extract schema from node YAML configuration."""
        node = self._mat_node

        schema_def = node.config.get("schema", [])

        schema = {}
        for col_def in schema_def:
            col_name = col_def.get("name")
            col_type = col_def.get("data_type")

            if not col_name or not col_type:
                logger.warning(f"Invalid schema definition: {col_def}")
                continue

            schema[col_name] = col_type

        return schema

    def _schema_from_duckdb(self) -> Dict[str, str]:
        """Infer schema from DuckDB view."""
        context = self._mat_context
        node = self._mat_node

        con = context.get_duckdb_connection()

        # Get the view name for this node
        node_id = node.id
        if "." in node_id:
            schema_name, view_name = node_id.split(".", 1)
            qualified_view = f"{schema_name}.{view_name}"
        else:
            qualified_view = "loaded_data"

        try:
            query = f"DESCRIBE {qualified_view}"
            results = con.execute(query).fetchall()

            schema = {}
            for row in results:
                col_name, col_type = row[0], row[1]
                schema[col_name] = col_type

            return schema

        except Exception as e:
            logger.error(f"Failed to infer schema from view {qualified_view}: {e}")
            return {}

    def _perform_materialization(self) -> ExecutorResult:
        """
        Perform materialization after node execution.

        Returns:
            ExecutorResult with materialization metadata

        Raises:
            MaterializationOperationError: If materialization fails
        """
        node = self._mat_node
        context = self._mat_context

        if not self._materialization_config:
            return ExecutorResult(
                node_id=node.id,
                status=ExecutionStatus.SUCCESS,
                metadata={"materialization": "skipped (not enabled)"},
            )

        config = self._materialization_config
        con = context.get_duckdb_connection()

        try:
            # Setup auditor
            self._auditor = MaterializationAuditor()

            # Load Iceberg extension
            DuckDBIcebergExtension.load_extension(con)

            # Setup catalog
            catalog_name = self._setup_catalog(con)

            # Infer schema from node
            schema = self._infer_schema()

            # Get or create table
            table_ref = self._get_or_create_table(con, catalog_name, schema)

            # Determine view name
            node_id = node.id
            if "." in node_id:
                schema_name, view_name = node_id.split(".", 1)
                view_ref = f"{schema_name}.{view_name}"
            else:
                view_ref = "loaded_data"

            # Perform atomic write
            with atomic_materialization(
                con=con,
                table_name=table_ref,
                config=config,
                auditor=self._auditor,
            ) as mat_state:
                write_result = write_to_iceberg(
                    con=con,
                    catalog_name=catalog_name,
                    table_name=table_ref,
                    view_name=view_ref,
                    mode=config.default_mode.value,
                    auditor=self._auditor,
                )

                mat_state.snapshot_id = write_result.snapshot_id

            # Add materialization metadata
            metadata = {
                "materialization": "success",
                "table": table_ref,
                "snapshot_id": write_result.snapshot_id,
                "row_count": write_result.row_count,
                "mode": config.default_mode.value,
            }

            logger.info(
                f"Materialization successful: {node.id} -> {table_ref} "
                f"(snapshot {write_result.snapshot_id[:8]})"
            )

            return ExecutorResult(
                node_id=node.id,
                status=ExecutionStatus.SUCCESS,
                metadata=metadata,
            )

        except Exception as e:
            logger.error(f"Materialization failed for {node.id}: {e}")

            # Return result with error status
            # Note: We don't fail the entire node execution if materialization fails
            return ExecutorResult(
                node_id=node.id,
                status=ExecutionStatus.SUCCESS,  # Node succeeded, materialization failed
                metadata={
                    "materialization": "failed",
                    "error": str(e),
                },
            )


def materialize_if_enabled(func: Callable) -> Callable:
    """
    Decorator to add materialization to executor execute method.

    This decorator wraps the execute method to perform materialization
    after the main execution logic. Materialization only occurs if
    enabled in the profile or node configuration.

    Usage:
        @materialize_if_enabled
        def execute(self) -> ExecutorResult:
            # Original execution logic
            result = self._do_execution()
            return result

    Args:
        func: Original execute method

    Returns:
        Wrapped execute method with materialization
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Call original execute method
        result = func(self, *args, **kwargs)

        # Check if executor has materialization capability
        if not isinstance(self, MaterializationMixin):
            return result

        # Check if dry-run mode
        if self.context.dry_run:
            result.metadata["materialization"] = "skipped (dry-run)"
            return result

        # Only materialize on success
        if not result.is_success():
            return result

        # Load materialization configuration
        config = self._load_materialization_config()

        if not config:
            # Materialization not enabled for this node
            return result

        # Perform materialization
        try:
            mat_result = self._perform_materialization()

            # Merge materialization metadata
            if mat_result.metadata:
                result.metadata.update(mat_result.metadata)

            return result

        except Exception as e:
            # Log error but don't fail the entire execution
            logger.error(
                f"Materialization failed for {self.node.id}, "
                f"but node execution succeeded: {e}"
            )

            result.metadata["materialization"] = "failed"
            result.metadata["materialization_error"] = str(e)

            return result

    return wrapper


def _get_table_schema(con: Any, table_name: str) -> Dict[str, str]:
    """
    Get table schema from Iceberg table.

    Args:
        con: DuckDB connection
        table_name: Fully qualified table name

    Returns:
        Schema mapping (column_name -> type)
    """
    try:
        query = f"DESCRIBE {table_name}"
        results = con.execute(query).fetchall()

        schema = {}
        for row in results:
            col_name, col_type = row[0], row[1]
            schema[col_name] = col_type

        return schema

    except Exception:
        # Table doesn't exist
        return {}
