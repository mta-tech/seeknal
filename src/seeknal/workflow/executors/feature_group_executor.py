"""
Feature Group executor for Seeknal workflow node execution.

This executor handles FEATURE_GROUP node types, creating and materializing
feature groups in the feature store. It supports both DuckDB and Spark engines,
with DuckDB being the preferred default for simplicity and performance.

Key Features:
- Creates/updates feature groups from upstream data sources
- Supports entity definitions and materialization configs
- Handles offline/online store materialization
- Performs event-time partitioning
- Returns row counts and execution timing
- Integrates with existing FeatureGroup/FeatureGroupDuckDB APIs

YAML Configuration Fields:
- features: List of feature names (optional, auto-detected if None)
- entity: Entity definition with join_keys
- materialization: Materialization config (event_time_col, offline, online, etc.)
- transform: Optional SQL transformation to apply
- engine: 'duckdb' (default) or 'spark'
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from .base import (
    BaseExecutor,
    ExecutorResult,
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
    register_executor,
)
from ...dag.manifest import Node, NodeType

# Import FeatureGroup implementations
from ...entity import Entity
from ...featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    Materialization as DuckDBMaterialization,
    OfflineStoreDuckDB,
)
from ...featurestore.feature_group import (
    FeatureGroup,
    Materialization as SparkMaterialization,
)

# Import context for project/workspace
from ...context import context as seeknal_context

# Iceberg materialization support
from ...workflow.materialization.yaml_integration import materialize_node_if_enabled

logger = logging.getLogger(__name__)


@register_executor(NodeType.FEATURE_GROUP)
class FeatureGroupExecutor(BaseExecutor):
    """
    Executor for FEATURE_GROUP nodes.

    Creates and materializes feature groups in the feature store.
    Supports both DuckDB (default) and Spark engines.

    Example YAML:
        nodes:
          user_features:
            type: feature_group
            config:
              entity:
                name: user
                join_keys: [user_id]
              materialization:
                event_time_col: event_date
                offline: true
                online: false
              features: null  # Auto-detect from source
              engine: duckdb  # or 'spark'
    """

    @property
    def node_type(self) -> NodeType:
        """Return the node type this executor handles."""
        return NodeType.FEATURE_GROUP

    def validate(self) -> None:
        """
        Validate the feature group node configuration.

        Checks for required fields:
        - entity: Entity definition with name and join_keys
        - materialization: Materialization configuration

        Raises:
            ExecutorValidationError: If configuration is invalid
        """
        config = self.node.config

        # Validate entity
        if "entity" not in config:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required 'entity' field in configuration"
            )

        entity_config = config["entity"]
        # Support both string format and dict format
        # String: entity: customer (name used as join key)
        # Dict: entity: {name: customer, join_keys: [customer_id]}
        if isinstance(entity_config, str):
            # String format - use entity name as join key
            entity_config = {
                "name": entity_config,
                "join_keys": [entity_config + "_id"]  # Default to entity_id pattern
            }
            config["entity"] = entity_config  # Normalize config
        elif not isinstance(entity_config, dict):
            raise ExecutorValidationError(
                self.node.id,
                "'entity' must be a string or a dictionary with 'name' and 'join_keys'"
            )

        if "name" not in entity_config:
            raise ExecutorValidationError(
                self.node.id,
                "Entity must have a 'name' field"
            )

        if "join_keys" not in entity_config or not entity_config["join_keys"]:
            raise ExecutorValidationError(
                self.node.id,
                "Entity must have non-empty 'join_keys' list"
            )

        # Materialization is optional - use defaults if not provided
        if "materialization" not in config:
            # Set default materialization config
            config["materialization"] = {
                "offline": True,
                "online": False
            }

        mat_config = config["materialization"]
        if not isinstance(mat_config, dict):
            raise ExecutorValidationError(
                self.node.id,
                "'materialization' must be a dictionary"
            )

        # Validate engine type
        engine = config.get("engine", "duckdb")
        if engine not in ["duckdb", "spark"]:
            raise ExecutorValidationError(
                self.node.id,
                f"Invalid engine '{engine}'. Must be 'duckdb' or 'spark'"
            )

    def pre_execute(self) -> None:
        """
        Setup before feature group execution.

        Creates the output directory for feature group data if needed.
        """
        output_path = self.context.get_output_path(self.node)
        output_path.mkdir(parents=True, exist_ok=True)

    def execute(self) -> ExecutorResult:
        """
        Execute the feature group node.

        Process:
        1. Load upstream data from dependencies
        2. Parse entity and materialization configs
        3. Create/update feature group
        4. Set features from source data
        5. Materialize to offline/online stores
        6. Return result with row counts and timing

        Returns:
            ExecutorResult with feature counts and execution metadata

        Raises:
            ExecutorExecutionError: If execution fails
        """
        start_time = time.time()

        try:
            # Get configuration
            config = self.node.config
            engine = config.get("engine", "duckdb")

            # Handle dry-run mode
            if self.context.dry_run:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=0.0,
                    is_dry_run=True,
                    metadata={
                        "engine": engine,
                        "dry_run_message": "Feature group would be created and materialized"
                    }
                )

            # Load upstream data
            source_df = self._load_upstream_data()

            # Create feature group based on engine
            if engine == "duckdb":
                result = self._execute_duckdb(source_df, config)
            elif engine == "spark":
                result = self._execute_spark(source_df, config)
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Unsupported engine: {engine}"
                )

            # Set duration
            result.duration_seconds = time.time() - start_time

            return result

        except ExecutorValidationError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Feature group execution failed: {str(e)}",
                e
            ) from e

    def _load_upstream_data(self) -> Union[pd.DataFrame, Any]:
        """
        Load data from upstream source nodes.

        For feature groups, upstream data comes from SOURCE or TRANSFORM nodes.
        This method:
        1. Checks for DuckDB views created by upstream transforms/sources
        2. Falls back to cache files if views aren't available
        3. Falls back to source file references if cache isn't available

        Returns:
            DataFrame with upstream data (Pandas for DuckDB, Spark for Spark engine)

        Raises:
            ExecutorExecutionError: If upstream data cannot be loaded
        """
        config = self.node.config
        engine = config.get("engine", "duckdb")

        # First, try to load from DuckDB views created by upstream nodes
        # This handles the case where upstream SOURCE or TRANSFORM nodes
        # have created views in the context's DuckDB connection
        inputs = config.get("inputs", [])
        if inputs and engine == "duckdb":
            import pandas as pd
            
            con = self.context.get_duckdb_connection()
            
            # Process inputs to find the view to read from
            # Support both list format: [{"ref": "transform.customer_orders"}]
            # and dict format: {"data": "ref:transform.customer_orders"}
            upstream_ref = None
            if isinstance(inputs, list) and len(inputs) > 0:
                # List format - use the first input
                first_input = inputs[0]
                if isinstance(first_input, dict) and "ref" in first_input:
                    upstream_ref = first_input["ref"]
            elif isinstance(inputs, dict):
                # Dict format - use the first value
                first_value = next(iter(inputs.values()), None)
                if first_value and isinstance(first_value, str):
                    if first_value.startswith("ref:"):
                        upstream_ref = first_value[4:]
                    else:
                        upstream_ref = first_value
            
            if upstream_ref:
                # Check if the view exists in DuckDB
                # View naming: {kind}.{name} (e.g., transform.customer_orders)
                try:
                    # Check if view exists
                    if "." in upstream_ref:
                        schema, name = upstream_ref.split(".", 1)
                    else:
                        schema, name = "main", upstream_ref
                    
                    view_exists = con.execute(
                        f"SELECT EXISTS (SELECT * FROM information_schema.views WHERE table_schema = '{schema}' AND table_name = '{name}')"
                    ).fetchone()[0]
                    
                    if view_exists:
                        # Read from the view
                        df = con.execute(f"SELECT * FROM {upstream_ref}").df()
                        logger.info(f"Loaded {len(df)} rows from DuckDB view '{upstream_ref}'")
                        return df
                except Exception as e:
                    logger.warning(f"Failed to load from DuckDB view '{upstream_ref}': {e}")
                    # Continue to try other methods

        # Try to load from cache
        cache_path = self.context.get_cache_path(self.node)

        if cache_path.exists():
            if engine == "duckdb":
                import pandas as pd
                return pd.read_parquet(cache_path)
            elif engine == "spark":
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                return spark.read.parquet(str(cache_path))

        # If no cache, check for a 'source' reference in config
        if "source" in config:
            source_path = Path(config["source"])
            if source_path.exists():
                if engine == "duckdb":
                    import pandas as pd
                    # Support CSV and Parquet
                    if source_path.suffix == ".csv":
                        return pd.read_csv(source_path)
                    elif source_path.suffix in [".parquet", ".pq"]:
                        return pd.read_parquet(source_path)
                elif engine == "spark":
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.getOrCreate()
                    return spark.read.format("parquet").load(str(source_path))

        # No upstream data available - create empty DataFrame
        if engine == "duckdb":
            import pandas as pd
            return pd.DataFrame()
        elif engine == "spark":
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            return spark.createDataFrame([])

    def _execute_duckdb(
        self,
        source_df: pd.DataFrame,
        config: Dict[str, Any]
    ) -> ExecutorResult:
        """
        Execute feature group using DuckDB engine.

        Args:
            source_df: Source Pandas DataFrame
            config: Feature group configuration

        Returns:
            ExecutorResult with execution outcome
        """
        # Parse entity config
        entity_config = config["entity"]
        entity = Entity(
            name=entity_config["name"],
            join_keys=entity_config["join_keys"],
            description=entity_config.get("description")
        )
        # Get or create entity
        entity.get_or_create()

        # Parse materialization config
        mat_config = config["materialization"]
        materialization = DuckDBMaterialization(
            event_time_col=mat_config.get("event_time_col"),
            offline=mat_config.get("offline", True),
            online=mat_config.get("online", False),
            offline_store=OfflineStoreDuckDB(),
        )

        # Create feature group
        fg = FeatureGroupDuckDB(
            name=self.node.name,
            entity=entity,
            materialization=materialization,
            project=self.context.project_name,
            description=config.get("description")
        )

        # Set source data
        if not source_df.empty:
            fg.set_dataframe(source_df)

            # Apply transform if specified
            if "transform" in config and config["transform"]:
                source_df = self._apply_transform_duckdb(source_df, config["transform"])
                fg.set_dataframe(source_df)

            # Set features
            features = config.get("features")  # None means auto-detect
            fg.set_features(features=features)

            # Materialize
            start_date = config.get("feature_start_time")
            end_date = config.get("feature_end_time")
            mode = config.get("mode", "overwrite")

            if start_date:
                start_date = datetime.fromisoformat(start_date) if isinstance(start_date, str) else start_date
            if end_date:
                end_date = datetime.fromisoformat(end_date) if isinstance(end_date, str) else end_date

            fg.write(
                feature_start_time=start_date,
                feature_end_time=end_date,
                mode=mode
            )

            # Create a DuckDB view from the feature data for materialization
            # View name format: feature_group.{node_name}
            view_name = f"feature_group.{self.node.name}"
            try:
                con = self.context.get_duckdb_connection()
                
                # Create schema if it doesn't exist
                con.execute("CREATE SCHEMA IF NOT EXISTS feature_group")
                
                # Create a view directly from the DataFrame
                # We use a unique temp table name to avoid conflicts
                temp_table = f"_temp_fg_{self.node.name}_{id(self)}"
                con.register(temp_table, source_df)
                con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {temp_table}")
                
                logger.info(f"Created view '{view_name}' for materialization ({len(source_df)} rows)")
            except Exception as e:
                # View creation is not critical for feature group execution
                logger.warning(f"Failed to create view '{view_name}': {e}")

            # Write intermediate parquet for ctx.ref() and entity consolidation
            intermediate_dir = self.context.target_path / "intermediate"
            intermediate_dir.mkdir(parents=True, exist_ok=True)
            parquet_path = intermediate_dir / f"{self.node.id.replace('.', '_')}.parquet"
            source_df.to_parquet(parquet_path, index=False)

            # Write metadata sidecar for FeatureFrame detection
            import json as _json
            meta_path = intermediate_dir / f"{self.node.id.replace('.', '_')}_meta.json"
            meta_path.write_text(_json.dumps({
                "entity_name": entity.name,
                "join_keys": entity.join_keys,
                "event_time_col": mat_config.get("event_time_col", "event_time"),
            }))
            logger.info(
                "Wrote intermediate parquet: %s (%d rows)",
                parquet_path, len(source_df)
            )

            row_count = len(source_df)
        else:
            # Empty source - no features to write
            row_count = 0

        # Get output path
        output_path = self.context.get_output_path(self.node)

        return ExecutorResult(
            node_id=self.node.id,
            status=ExecutionStatus.SUCCESS,
            row_count=row_count,
            output_path=output_path,
            metadata={
                "engine": "duckdb",
                "entity": entity.name,
                "join_keys": entity.join_keys,
                "feature_count": len(fg.features) if fg.features else 0,
                "offline": materialization.offline,
                "online": materialization.online,
                "event_time_col": materialization.event_time_col,
                "version": fg.version,
                "project": self.context.project_name,
            }
        )

    def _execute_spark(
        self,
        source_df: Any,  # pyspark.sql.DataFrame
        config: Dict[str, Any]
    ) -> ExecutorResult:
        """
        Execute feature group using Spark engine.

        Args:
            source_df: Source Spark DataFrame
            config: Feature group configuration

        Returns:
            ExecutorResult with execution outcome
        """
        # Parse entity config
        entity_config = config["entity"]
        entity = Entity(
            name=entity_config["name"],
            join_keys=entity_config["join_keys"],
            description=entity_config.get("description")
        )
        entity.get_or_create()

        # Parse materialization config
        mat_config = config["materialization"]
        materialization = SparkMaterialization(
            event_time_col=mat_config.get("event_time_col"),
            offline=mat_config.get("offline", True),
            online=mat_config.get("online", False),
        )

        # Create feature group
        fg = FeatureGroup(
            name=self.node.name,
            materialization=materialization,
            description=config.get("description")
        )
        fg.entity = entity

        # Set source data
        if source_df and source_df.count() > 0:
            # Apply transform if specified
            if "transform" in config and config["transform"]:
                source_df = self._apply_transform_spark(source_df, config["transform"])

            fg.set_dataframe(source_df)

            # Set features
            features = config.get("features")  # None means auto-detect
            fg.set_features(features=features)

            # Materialize
            start_date = config.get("feature_start_time")
            end_date = config.get("feature_end_time")

            if start_date:
                start_date = datetime.fromisoformat(start_date) if isinstance(start_date, str) else start_date
            if end_date:
                end_date = datetime.fromisoformat(end_date) if isinstance(end_date, str) else end_date

            fg.write(
                feature_start_time=start_date,
                feature_end_time=end_date
            )

            row_count = source_df.count()
        else:
            # Empty source - no features to write
            row_count = 0

        # Get output path
        output_path = self.context.get_output_path(self.node)

        return ExecutorResult(
            node_id=self.node.id,
            status=ExecutionStatus.SUCCESS,
            row_count=row_count,
            output_path=output_path,
            metadata={
                "engine": "spark",
                "entity": entity.name,
                "join_keys": entity.join_keys,
                "feature_count": len(fg.features) if fg.features else 0,
                "offline": materialization.offline,
                "online": materialization.online,
                "event_time_col": materialization.event_time_col,
                "version": fg.version,
                "project": self.context.project_name,
            }
        )

    def _apply_transform_duckdb(
        self,
        df: pd.DataFrame,
        transform_sql: str
    ) -> pd.DataFrame:
        """
        Apply SQL transformation to Pandas DataFrame using DuckDB.

        Args:
            df: Source DataFrame
            transform_sql: SQL transformation query

        Returns:
            Transformed DataFrame
        """
        import duckdb

        con = self.context.get_duckdb_connection()
        con.register("source", df)

        # Execute transform
        result_df = con.execute(transform_sql).df()

        # Clean up
        con.unregister("source")

        return result_df

    def _apply_transform_spark(
        self,
        df: Any,  # pyspark.sql.DataFrame
        transform_sql: str
    ) -> Any:  # pyspark.sql.DataFrame
        """
        Apply SQL transformation to Spark DataFrame.

        Args:
            df: Source Spark DataFrame
            transform_sql: SQL transformation query

        Returns:
            Transformed Spark DataFrame
        """
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        df.createOrReplaceTempView("source")

        result_df = spark.sql(transform_sql)

        # Clean up
        spark.catalog.dropTempView("source")

        return result_df

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Cleanup and finalize after feature group execution.

        Adds additional metadata to the result and handles Iceberg materialization
        if enabled in the node configuration.

        Args:
            result: Result from execute()

        Returns:
            Enhanced ExecutorResult
        """
        # Add executor metadata
        result.metadata["executor_version"] = "1.0.0"
        result.metadata["executor_class"] = "FeatureGroupExecutor"

        # Log summary
        if self.context.verbose:
            from ...context import logger
            logger.info(
                f"Feature group '{self.node.name}' executed: "
                f"{result.row_count} rows, "
                f"{result.metadata.get('feature_count', 0)} features, "
                f"engine={result.metadata.get('engine', 'unknown')}"
            )

        # Handle Iceberg materialization if enabled
        if result.status == ExecutionStatus.SUCCESS and not result.is_dry_run:
            try:
                # Get the context's DuckDB connection (has the view)
                con = self.context.get_duckdb_connection()
                mat_result = materialize_node_if_enabled(
                    self.node,
                    source_con=con,
                    enabled_override=self.context.materialize_enabled
                )
                if mat_result:
                    # Materialization was enabled and succeeded
                    result.metadata["materialization"] = {
                        "enabled": True,
                        "success": mat_result.get("success", False),
                        "table": mat_result.get("table"),
                        "row_count": mat_result.get("row_count"),
                        "mode": mat_result.get("mode"),
                        "iceberg_table": mat_result.get("iceberg_table"),
                    }
                    logger.info(
                        f"Materialized node '{self.node.id}' to Iceberg table "
                        f"'{mat_result.get('table')}' ({mat_result.get('row_count')} rows)"
                    )
            except Exception as e:
                # Log materialization error but don't fail the executor
                # Materialization is optional/augmentation, not core functionality
                logger.warning(
                    f"Failed to materialize node '{self.node.id}' to Iceberg: {e}"
                )
                result.metadata["materialization"] = {
                    "enabled": True,
                    "success": False,
                    "error": str(e),
                }

        return result
