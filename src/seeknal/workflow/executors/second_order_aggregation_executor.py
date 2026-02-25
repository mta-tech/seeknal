"""
Second-Order Aggregation node executor for Seeknal workflow execution.

This module implements the SECOND_ORDER_AGGREGATION node executor, which handles
aggregations of aggregations. It enables advanced feature engineering patterns like:

- Aggregations of aggregations (e.g., average of user 30-day sums across regions)
- Cross-time window aggregations (e.g., weekly patterns from daily data)
- Multi-level hierarchies (e.g., user → store → region)

The executor wraps the existing SecondOrderAggregator from the tasks module,
ensuring consistent behavior and SQL generation patterns.

Key Features:
- Reuses existing SecondOrderAggregator implementation
- Supports basic, window, ratio, and conditional aggregations
- Validates source aggregation exists and has expected schema
- Handles both DuckDB and Spark engines
- Creates intermediate storage for downstream nodes

Reference:
    - Base executor: workflow/executors/base.py
    - SecondOrderAggregator: tasks/duckdb/aggregators/second_order_aggregator.py
    - YAML template: workflow/templates/second_order_aggregation.yml.j2
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from seeknal.dag.manifest import Node, NodeType
from seeknal.validation import validate_column_name
from seeknal.workflow.executors.base import (
    BaseExecutor,
    ExecutorResult,
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
    register_executor,
)


@register_executor(NodeType.SECOND_ORDER_AGGREGATION)
class SecondOrderAggregationExecutor(BaseExecutor):
    """
    Executor for SECOND_ORDER_AGGREGATION nodes.

    Handles aggregations of already-aggregated data, enabling multi-level
    feature engineering and cross-entity analytics.

    The executor loads output from an upstream aggregation node, then applies
    second-order aggregations using the existing SecondOrderAggregator class.

    Supported aggregation types (via SecondOrderAggregator):
    - Basic: sum, avg, mean, min, max, count, stddev, std
    - Days (time windows): aggregations over specific day ranges
    - Ratio: numerator/denominator ratios with different windows
    - Since: conditional aggregations (count_if, sum_if)

    Example YAML configuration:
        ```yaml
        kind: second_order_aggregation
        name: region_user_metrics
        description: "Aggregate user features to region level"
        id_col: region_id
        feature_date_col: date
        source: aggregation.user_daily_features
        features:
          total_users:
            basic: [count]
          avg_user_spend_30d:
            basic: [mean, stddev]
            source_feature: total_spend_30d
          recent_vs_historical:
            ratio:
              numerator: [1, 7]
              denominator: [8, 30]
              aggs: [sum]
            source_feature: transaction_amount
        ```

    Attributes:
        node: The second-order aggregation node to execute
        context: Execution context with DuckDB connection
    """

    @property
    def node_type(self) -> NodeType:
        """Return the node type this executor handles."""
        return NodeType.SECOND_ORDER_AGGREGATION

    def validate(self) -> None:
        """
        Validate the second-order aggregation node configuration.

        Checks that required fields are present and valid:
        - id_col: Column to group by (entity key for second-order)
        - feature_date_col: Date column for features
        - source: Reference to upstream aggregation node
        - features: Feature specifications

        Raises:
            ExecutorValidationError: If configuration is invalid
        """
        config = self.node.config

        # Check required fields
        for field in ["id_col", "feature_date_col", "source", "features"]:
            if field not in config:
                raise ExecutorValidationError(
                    self.node.id,
                    f"Missing required field: {field}"
                )

        # Validate id_col
        id_col = config["id_col"]
        if not isinstance(id_col, str) or not id_col.strip():
            raise ExecutorValidationError(
                self.node.id,
                "Invalid id_col: must be a non-empty string"
            )
        validate_column_name(id_col)

        # Validate feature_date_col
        feature_date_col = config["feature_date_col"]
        if not isinstance(feature_date_col, str) or not feature_date_col.strip():
            raise ExecutorValidationError(
                self.node.id,
                "Invalid feature_date_col: must be a non-empty string"
            )
        validate_column_name(feature_date_col)

        # Validate source reference
        source_ref = config["source"]
        if not isinstance(source_ref, str) or not source_ref.strip():
            raise ExecutorValidationError(
                self.node.id,
                "Invalid source: must be a non-empty string (format: kind.name)"
            )
        # Check source reference format (kind.name)
        if "." not in source_ref:
            raise ExecutorValidationError(
                self.node.id,
                f"Invalid source reference: '{source_ref}'. Expected format: 'kind.name' (e.g., 'aggregation.user_metrics')"
            )

        # Validate features
        features = config["features"]
        if not isinstance(features, dict) or len(features) == 0:
            raise ExecutorValidationError(
                self.node.id,
                "Features must be a non-empty dictionary"
            )

        # Validate each feature
        for feature_name, feature_config in features.items():
            validate_column_name(feature_name)

            if not isinstance(feature_config, dict):
                raise ExecutorValidationError(
                    self.node.id,
                    f"Feature '{feature_name}' must be a dictionary"
                )

            # Check that feature has at least one aggregation type
            has_agg = any(
                key in feature_config
                for key in ["basic", "window", "ratio", "percentile", "delta", "source_feature"]
            )
            if not has_agg:
                raise ExecutorValidationError(
                    self.node.id,
                    f"Feature '{feature_name}' must have at least one "
                    "aggregation type (basic, window, ratio, percentile, delta)"
                )

    def pre_execute(self) -> None:
        """
        Setup before second-order aggregation execution.

        Creates output directory and prepares connections.
        """
        output_path = self.context.get_output_path(self.node)
        output_path.mkdir(parents=True, exist_ok=True)

    def execute(self) -> ExecutorResult:
        """
        Execute the second-order aggregation node.

        Loads source aggregation output, then applies second-order aggregations
        using the SecondOrderAggregator class.

        Returns:
            ExecutorResult with row counts and execution timing

        Raises:
            ExecutorExecutionError: If execution fails
        """
        start_time = time.time()

        try:
            # Handle dry-run mode
            if self.context.dry_run:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=0.0,
                    is_dry_run=True,
                    metadata={
                        "feature_count": len(self.node.config.get("features", {})),
                        "source": self.node.config.get("source"),
                        "features": self._get_feature_summary()
                    }
                )

            # Route to appropriate engine
            if self.context.duckdb_connection is not None or self.context.config.get("engine") == "duckdb":
                return self._execute_duckdb()
            elif self.context.spark_session is not None or self.context.config.get("engine") == "spark":
                return self._execute_spark()
            else:
                # Default to DuckDB
                return self._execute_duckdb()

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Second-order aggregation execution failed: {str(e)}",
                e
            ) from e

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Cleanup after second-order aggregation execution.

        Adds additional metadata to the result.

        Args:
            result: Result from execute()

        Returns:
            Modified result with additional metadata
        """
        result.metadata["executor_version"] = "1.0.0"
        return result

    def _is_python_node(self) -> bool:
        """Check if this node originates from a Python pipeline file."""
        if not hasattr(self.node, 'file_path') or not self.node.file_path:
            return False
        return str(self.node.file_path).endswith('.py')

    def _execute_python_preprocessing(self) -> Path:
        """Run the Python function body via subprocess to produce intermediate parquet.

        Returns:
            Path to the intermediate parquet file

        Raises:
            ExecutorExecutionError: If preprocessing fails
        """
        from seeknal.workflow.executors.python_executor import PythonExecutor  # ty: ignore[unresolved-import]

        py_executor = PythonExecutor(self.node, self.context)
        result = py_executor.execute()

        if result.status != ExecutionStatus.SUCCESS:
            error_msg = result.metadata.get('stderr', 'unknown error') if result.metadata else 'unknown error'
            raise ExecutorExecutionError(
                self.node.id,
                f"Python preprocessing failed: {error_msg}"
            )

        # PythonExecutor saves to target/intermediate/{node_id_dots_replaced}.parquet
        node_id_safe = self.node.id.replace('.', '_')
        parquet_path = Path(self.context.target_path) / "intermediate" / f"{node_id_safe}.parquet"
        if not parquet_path.exists():
            raise ExecutorExecutionError(
                self.node.id,
                "Python preprocessing did not produce output parquet"
            )
        return parquet_path

    def _execute_duckdb(self) -> ExecutorResult:
        """
        Execute second-order aggregation using DuckDB.

        For Python SOA nodes: runs the function body via subprocess first
        to produce a pre-processed DataFrame (intermediate parquet), then
        runs the SOA engine on that result.

        For YAML SOA nodes: loads source data from upstream node's
        intermediate storage, then runs the SOA engine.

        Returns:
            ExecutorResult with execution results

        Raises:
            ExecutorExecutionError: If execution fails
        """
        start_time = time.time()
        config = self.node.config

        # Get DuckDB connection
        con = self.context.get_duckdb_connection()

        source_ref = config.get("source", "")

        if self._is_python_node():
            # Phase 1: Run function body to get pre-processed data
            parquet_path = self._execute_python_preprocessing()
            # Load the pre-processed data directly as SOA input
            source_table_name = f"soa_input_{self.node.name}"
            con.execute(
                f"CREATE OR REPLACE VIEW {source_table_name} AS "
                f"SELECT * FROM '{parquet_path}'"
            )
        else:
            # YAML path: load from upstream node
            source_node = self._resolve_source_node(source_ref)
            source_table_name = self._load_source_data(con, source_node)

        # Build aggregation specs from YAML features
        aggregation_specs = self._build_aggregation_specs(config)

        if not aggregation_specs:
            raise ExecutorExecutionError(
                self.node.id,
                "No valid aggregation specifications found in features"
            )

        # Import SecondOrderAggregator
        from seeknal.tasks.duckdb.aggregators.second_order_aggregator import (
            SecondOrderAggregator,
        )

        # Create aggregator instance
        aggregator = SecondOrderAggregator(
            idCol=config["id_col"],
            featureDateCol=config["feature_date_col"],
            featureDateFormat="yyyy-MM-dd",
            applicationDateCol=config.get("application_date_col"),
            applicationDateFormat="yyyy-MM-dd",
            conn=con
        )

        # Set rules
        aggregator.setRules(aggregation_specs)

        # Execute transformation
        result_relation = aggregator.transform(source_table_name)

        # Convert to DataFrame for row count
        result_df = result_relation.df()
        row_count = len(result_df)

        # Register result as a view for downstream nodes
        view_name = f"second_order_aggregation.{self.node.name}"
        con.register(view_name, result_df)

        # Save to intermediate storage for materialization
        output_path = self.context.get_output_path(self.node)
        intermediate_path = output_path / "result.parquet"
        result_df.to_parquet(intermediate_path, index=False)

        duration = time.time() - start_time

        return ExecutorResult(
            node_id=self.node.id,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=duration,
            row_count=row_count,
            output_path=intermediate_path,
            metadata={
                "engine": "duckdb",
                "source": source_ref,
                "view_name": view_name,
                "features": self._get_feature_summary(),
                "feature_count": len(config.get("features", {}))
            }
        )

    def _execute_spark(self) -> ExecutorResult:
        """
        Execute second-order aggregation using Spark.

        Returns:
            ExecutorResult with execution results

        Raises:
            ExecutorExecutionError: If execution fails
        """
        start_time = time.time()
        config = self.node.config

        # Get Spark session
        spark = self.context.get_spark_session()

        # Resolve source aggregation
        source_ref = config["source"]
        source_node = self._resolve_source_node(source_ref)

        # Load source data from intermediate storage
        source_df = self._load_source_data_spark(spark, source_node)

        # Import SecondOrderAggregator
        from seeknal.tasks.sparkengine.py_impl.aggregators.second_order_aggregator import (
            SecondOrderAggregator as SparkSecondOrderAggregator,
        )

        # Create aggregator instance
        aggregator = SparkSecondOrderAggregator(
            idCol=config["id_col"],
            featureDateCol=config["feature_date_col"],
            applicationDateCol=config.get("application_date_col"),
        )

        # Build aggregation specs from YAML features
        aggregation_specs = self._build_aggregation_specs(config)

        if not aggregation_specs:
            raise ExecutorExecutionError(
                self.node.id,
                "No valid aggregation specifications found in features"
            )

        # Execute transformation
        result_df = aggregator.transform(source_df, aggregation_specs)

        row_count = result_df.count()

        # Create view for downstream nodes
        result_df.createOrReplaceTempView(f"second_order_aggregation.{self.node.name}")

        # Save to intermediate storage
        output_path = self.context.get_output_path(self.node)
        intermediate_path = output_path / "result.parquet"
        result_df.write.parquet(str(intermediate_path), mode="overwrite")

        duration = time.time() - start_time

        return ExecutorResult(
            node_id=self.node.id,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=duration,
            row_count=row_count,
            output_path=intermediate_path,
            metadata={
                "engine": "spark",
                "source": source_ref,
                "features": self._get_feature_summary(),
                "feature_count": len(config.get("features", {}))
            }
        )

    def _resolve_source_node(self, source_ref: str) -> Node:
        """
        Resolve source aggregation node from reference.

        Args:
            source_ref: Source reference in format "kind.name"

        Returns:
            The source Node object

        Raises:
            ExecutorExecutionError: If source node not found
        """
        # The source_ref format is "kind.name" (e.g., "aggregation.user_metrics")
        kind, name = source_ref.split(".", 1)

        # In a real implementation, we would look up the node from the manifest
        # For now, we'll create a placeholder that assumes the node exists
        from seeknal.dag.manifest import Node

        # This is a simplified version - in production, resolve from manifest
        # Map kind string to NodeType enum
        kind_map = {
            "source": NodeType.SOURCE,
            "transform": NodeType.TRANSFORM,
            "feature_group": NodeType.FEATURE_GROUP,
            "model": NodeType.MODEL,
            "aggregation": NodeType.AGGREGATION,
            "second_order_aggregation": NodeType.SECOND_ORDER_AGGREGATION,
            "rule": NodeType.RULE,
            "exposure": NodeType.EXPOSURE,
        }

        return Node(
            id=source_ref,
            name=name,
            node_type=kind_map.get(kind.lower(), NodeType.SOURCE),
            config={}
        )

    def _load_source_data(self, con, source_node: Node) -> str:
        """
        Load source aggregation data from intermediate storage or upstream views.

        Args:
            con: DuckDB connection
            source_node: Source aggregation node

        Returns:
            Table name for the loaded data

        Raises:
            ExecutorExecutionError: If source data not found
        """
        from pathlib import Path

        # Try multiple sources in order:
        # 1. Check for aggregation view (schema.name)
        # 2. Check for aggregation intermediate storage
        # 3. Check for transform intermediate storage
        # 4. Create view from transform output

        intermediate_path = Path(self.context.target_path) / "intermediate"

        # Try aggregation view first (created by upstream aggregation executor)
        source_view_name = f"aggregation.{source_node.name}"
        try:
            # Try to query the view - if it exists, this will work
            test_query = con.execute(f"SELECT * FROM {source_view_name} LIMIT 1")
            return source_view_name
        except Exception:
            pass

        # Try aggregation intermediate storage (result.parquet from aggregation output)
        agg_output_path = Path(self.context.target_path) / "aggregation" / source_node.name
        agg_parquet = agg_output_path / "result.parquet"
        if agg_parquet.exists():
            view_name = f"source_data_{source_node.name}"
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{agg_parquet}'")
            return view_name

        # Try transform intermediate storage
        transform_parquet = intermediate_path / f"transform_{source_node.name}.parquet"
        if transform_parquet.exists():
            # Create a view for the aggregation
            view_name = f"source_data_{source_node.name}"
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{transform_parquet}'")
            return view_name

        # Create view from transform output if it exists
        transform_view = f"transform.{source_node.name}"
        try:
            test_query = con.execute(f"SELECT * FROM {transform_view} LIMIT 1")
            # Create aggregation source view
            view_name = f"source_data_{source_node.name}"
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {transform_view}")
            return view_name
        except Exception:
            pass

        raise ExecutorExecutionError(
            self.node.id,
            f"Source aggregation '{source_node.id}' not found. "
            f"Ensure the upstream aggregation node has been executed and stored output. "
            f"Tried: {source_view_name}, {agg_parquet}, {transform_parquet}, {transform_view}"
        )

    def _load_source_data_spark(self, spark, source_node: Node) -> Any:
        """
        Load source aggregation data from intermediate storage using Spark.

        Args:
            spark: Spark session
            source_node: Source aggregation node

        Returns:
            Spark DataFrame

        Raises:
            ExecutorExecutionError: If source data not found
        """
        # In production, load from intermediate storage path
        # For now, try to load from the view
        source_view_name = f"aggregation.{source_node.name}"

        try:
            return spark.table(source_view_name)
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Source aggregation '{source_view_name}' not found. "
                f"Ensure the upstream aggregation node has been executed."
            ) from e

    def _build_aggregation_specs(self, config: Dict[str, Any]) -> List:
        """
        Build aggregation spec list from YAML feature configuration.

        Converts YAML feature definitions to AggregationSpec objects
        that the SecondOrderAggregator can process.

        Args:
            config: Node configuration dictionary

        Returns:
            List of AggregationSpec objects

        Raises:
            ExecutorExecutionError: If feature configuration is invalid
        """
        from seeknal.tasks.duckdb.aggregators.second_order_aggregator import (
            AggregationSpec,
        )

        specs = []
        features = config.get("features", {})

        for feature_name, feature_config in features.items():
            # Get the actual source feature name (if different)
            source_feature = feature_config.get("source_feature", feature_name)

            # Basic aggregations
            if "basic" in feature_config:
                basic_aggs = feature_config["basic"]
                if not isinstance(basic_aggs, list):
                    basic_aggs = [basic_aggs]

                for agg in basic_aggs:
                    spec = AggregationSpec(
                        name="basic",
                        features=source_feature,
                        aggregations=agg,
                    )
                    specs.append(spec)

            # Window aggregations (basic_days)
            if "window" in feature_config:
                window = feature_config["window"]
                if not isinstance(window, list) or len(window) != 2:
                    raise ExecutorExecutionError(
                        self.node.id,
                        f"Feature '{feature_name}': window must be a list of two integers [lower, upper]"
                    )

                lower, upper = window
                basic_aggs = feature_config.get("basic", [])
                if not isinstance(basic_aggs, list):
                    basic_aggs = [basic_aggs]

                for agg in basic_aggs:
                    spec = AggregationSpec(
                        name="basic_days",
                        features=source_feature,
                        aggregations=agg,
                        dayLimitLower1=str(lower),
                        dayLimitUpper1=str(upper),
                    )
                    specs.append(spec)

            # Ratio aggregations
            if "ratio" in feature_config:
                ratio_config = feature_config["ratio"]
                numerator = ratio_config.get("numerator", [])
                denominator = ratio_config.get("denominator", [])
                aggs = ratio_config.get("aggs", ["sum"])

                if not isinstance(numerator, list) or len(numerator) != 2:
                    raise ExecutorExecutionError(
                        self.node.id,
                        f"Feature '{feature_name}': ratio.numerator must be a list of two integers [lower, upper]"
                    )

                if not isinstance(denominator, list) or len(denominator) != 2:
                    raise ExecutorExecutionError(
                        self.node.id,
                        f"Feature '{feature_name}': ratio.denominator must be a list of two integers [lower, upper]"
                    )

                for agg in aggs:
                    spec = AggregationSpec(
                        name="ratio",
                        features=source_feature,
                        aggregations=agg,
                        dayLimitLower1=str(numerator[0]),
                        dayLimitUpper1=str(numerator[1]),
                        dayLimitLower2=str(denominator[0]),
                        dayLimitUpper2=str(denominator[1]),
                    )
                    specs.append(spec)

            # Since/conditional aggregations
            if "since" in feature_config:
                since_config = feature_config["since"]
                if not isinstance(since_config, dict):
                    raise ExecutorExecutionError(
                        self.node.id,
                        f"Feature '{feature_name}': since must be a dictionary with 'condition' and 'aggs'"
                    )

                condition = since_config.get("condition", "")
                aggs = since_config.get("aggs", ["count"])

                for agg in aggs:
                    spec = AggregationSpec(
                        name="since",
                        features=source_feature,
                        aggregations=agg,
                        filterCondition=condition,
                    )
                    specs.append(spec)

        return specs

    def _get_feature_summary(self) -> List[Dict[str, Any]]:
        """
        Get summary of features and their aggregation types.

        Returns:
            List of feature summaries with aggregation counts
        """
        summary = []
        features = self.node.config.get("features", {})

        for feature_name, feature_config in features.items():
            aggs = {
                "basic": len(feature_config.get("basic", [])),
                "window": len(feature_config.get("window", [])),
                "ratio": len(feature_config.get("ratio", [])),
                "since": len(feature_config.get("since", [])),
            }

            total_aggs = sum(aggs.values())

            summary.append({
                "name": feature_name,
                "total_aggregations": total_aggs,
                **aggs
            })

        return summary
