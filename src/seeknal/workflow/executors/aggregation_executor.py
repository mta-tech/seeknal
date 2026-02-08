"""
Aggregation node executor for Seeknal workflow execution.

This module implements the AGGREGATION node executor, which handles time-series
feature aggregation with sliding windows. It supports basic aggregations (count,
sum, avg, min, max), rolling windows, and group-by operations.

The executor generates optimized SQL using DuckDB window functions for rolling
aggregations, following dbt patterns and feature store best practices.

Key Features:
- Basic aggregations: count, sum, avg, min, max, stddev
- Rolling window aggregations with time-based partitions
- Group-by operations with entity keys
- Multiple features with different aggregation types
- DuckDB window functions for optimal performance
- Support for ratio calculations and conditional aggregations

Reference:
    - Base executor: workflow/executors/base.py
    - Aggregation template: workflow/templates/aggregation.yml.j2
    - dbt patterns: https://docs.getdbt.com/reference/dbt-jinja-functions/flags
"""

from __future__ import annotations

import time
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


@register_executor(NodeType.AGGREGATION)
class AggregationExecutor(BaseExecutor):
    """
    Executor for AGGREGATION nodes.

    Handles time-series feature aggregation with sliding windows and
    group-by operations using DuckDB SQL.

    Supported aggregation types:
    - Basic: count, sum, avg, mean, min, max, stddev, std
    - Rolling: time windows (1d, 7d, 30d, 90d, etc.)
    - Ratio: numerator/denominator ratios with windows
    - Since: conditional aggregations (count_if, sum_if)

    Example YAML configuration:
        ```yaml
        kind: aggregation
        name: user_transaction_agg
        id_col: user_id
        feature_date_col: transaction_date
        application_date_col: prediction_date
        features:
          - name: amount
            basic: [sum, avg, max]
            rolling:
              - window: [1, 7]
                aggs: [sum, avg]
          - name: is_active
            since:
              - condition: "is_active == 1"
                aggs: [count, sum]
        ```

    Attributes:
        node: The aggregation node to execute
        context: Execution context with DuckDB connection
    """

    @property
    def node_type(self) -> NodeType:
        """Return the node type this executor handles."""
        return NodeType.AGGREGATION

    def validate(self) -> None:
        """
        Validate the aggregation node configuration.

        Checks that required fields are present and valid:
        - id_col: Column to group by (entity key)
        - feature_date_col: Date column for features
        - application_date_col: Date column for prediction
        - features: List of feature definitions

        Raises:
            ExecutorValidationError: If configuration is invalid
        """
        config = self.node.config

        # Check required fields
        required_fields = ["id_col", "feature_date_col", "application_date_col"]
        for field in required_fields:
            if field not in config:
                raise ExecutorValidationError(
                    self.node.id,
                    f"Missing required field: {field}"
                )
            # Validate column names
            col_name = config[field]
            if not isinstance(col_name, str) or not col_name.strip():
                raise ExecutorValidationError(
                    self.node.id,
                    f"Invalid {field}: must be a non-empty string"
                )
            validate_column_name(col_name)

        # Check features
        if "features" not in config:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required field: features"
            )

        features = config["features"]
        if not isinstance(features, list) or len(features) == 0:
            raise ExecutorValidationError(
                self.node.id,
                "Features must be a non-empty list"
            )

        # Validate each feature
        for idx, feature in enumerate(features):
            if not isinstance(feature, dict):
                raise ExecutorValidationError(
                    self.node.id,
                    f"Feature at index {idx} must be a dictionary"
                )

            if "name" not in feature:
                raise ExecutorValidationError(
                    self.node.id,
                    f"Feature at index {idx} missing 'name' field"
                )

            feature_name = feature["name"]
            validate_column_name(feature_name)

            # Check that feature has at least one aggregation type
            has_agg = any(
                key in feature for key in ["basic", "rolling", "ratio", "since"]
            )
            if not has_agg:
                raise ExecutorValidationError(
                    self.node.id,
                    f"Feature '{feature_name}' must have at least one "
                    "aggregation type (basic, rolling, ratio, since)"
                )

    def pre_execute(self) -> None:
        """
        Setup before aggregation execution.

        Creates output directory and prepares DuckDB connection.
        """
        output_path = self.context.get_output_path(self.node)
        output_path.mkdir(parents=True, exist_ok=True)

    def execute(self) -> ExecutorResult:
        """
        Execute the aggregation node.

        Generates and executes SQL for aggregations using DuckDB.
        Handles basic and rolling window aggregations.

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
                        "aggregation_count": len(self.node.config.get("features", [])),
                        "features": self._get_feature_summary()
                    }
                )

            # Get DuckDB connection
            con = self.context.get_duckdb_connection()

            # Generate and execute aggregation SQL
            sql, row_count = self._execute_aggregation(con)

            duration = time.time() - start_time

            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=duration,
                row_count=row_count,
                metadata={
                    "engine": "duckdb",
                    "sql_generated": True,
                    "features": self._get_feature_summary(),
                    "aggregation_count": len(self.node.config.get("features", []))
                }
            )

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Aggregation execution failed: {str(e)}",
                e
            ) from e

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Cleanup after aggregation execution.

        Adds additional metadata to the result.

        Args:
            result: Result from execute()

        Returns:
            Modified result with additional metadata
        """
        result.metadata["executor_version"] = "1.0.0"
        return result

    def _get_feature_summary(self) -> List[Dict[str, Any]]:
        """
        Get summary of features and their aggregation types.

        Returns:
            List of feature summaries with aggregation counts
        """
        summary = []
        for feature in self.node.config.get("features", []):
            feature_name = feature.get("name", "unknown")

            aggs = {
                "basic": len(feature.get("basic", [])),
                "rolling": len(feature.get("rolling", [])),
                "ratio": len(feature.get("ratio", [])),
                "since": len(feature.get("since", [])),
            }

            total_aggs = sum(aggs.values())

            summary.append({
                "name": feature_name,
                "total_aggregations": total_aggs,
                **aggs
            })

        return summary

    def _execute_aggregation(self, con) -> tuple[str, int]:
        """
        Execute aggregation SQL and return SQL with row count.

        Args:
            con: DuckDB connection

        Returns:
            Tuple of (sql_query, row_count)

        Raises:
            ExecutorExecutionError: If SQL execution fails
        """
        config = self.node.config

        id_col = config["id_col"]
        feature_date_col = config["feature_date_col"]
        application_date_col = config["application_date_col"]
        features = config["features"]
        group_by = config.get("group_by", [])

        try:
            # Load source data from upstream input
            source_ref = config.get("source", "")
            if source_ref:
                source_table_name = self._load_source_data(con, source_ref)
            else:
                # Fallback to loading from intermediate storage
                source_table_name = self._load_from_intermediate_storage(con)

            # Build SQL for all aggregations
            select_clauses, cte_queries = self._build_aggregation_sql(
                id_col=id_col,
                feature_date_col=feature_date_col,
                application_date_col=application_date_col,
                features=features
            )

            # Build group by columns (id_col, application_date_col, plus any additional group_by columns)
            group_by_cols = [id_col, application_date_col] + group_by
            group_by_clause = ", ".join([f'"{col}"' for col in group_by_cols])

            # Build select columns (same as group by plus aggregations)
            select_cols = [f'"{col}"' for col in group_by_cols]

            # Combine all clauses into final query
            if cte_queries:
                # Use CTEs for complex aggregations
                cte_sql = "WITH " + ",\n".join(cte_queries)
                select_clause_newline = ",\n".join(select_clauses)
                select_cols_clause = ",\n".join(select_cols)
                final_sql = f"""
                {cte_sql}
                SELECT
                    {select_cols_clause},
                    {select_clause_newline}
                FROM aggregated_features
                ORDER BY {group_by_clause}
                """
            else:
                # Simple aggregations without CTEs
                select_clause_newline2 = ",\n".join(select_clauses)
                select_cols_clause = ",\n".join(select_cols)
                final_sql = f"""
                SELECT
                    {select_cols_clause},
                    {select_clause_newline2}
                FROM {source_table_name}
                GROUP BY {group_by_clause}
                ORDER BY {group_by_clause}
                """

            # Execute SQL
            result = con.execute(final_sql)
            df = result.df()
            row_count = len(df)

            # Register result as a view for downstream nodes (especially second-order aggregations)
            view_name = f"aggregation.{self.node.name}"
            con.register(view_name, df)

            # Save to intermediate storage for persistence
            output_path = self.context.get_output_path(self.node)
            output_path.mkdir(parents=True, exist_ok=True)
            intermediate_path = output_path / "result.parquet"
            df.to_parquet(intermediate_path, index=False)

            return final_sql, row_count

        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to execute aggregation SQL: {str(e)}",
                e
            ) from e

    def _load_source_data(self, con, source_ref: str) -> str:
        """
        Load source data from upstream node.

        Args:
            con: DuckDB connection
            source_ref: Source reference (e.g., "transform.customer_orders")

        Returns:
            Table/view name for the loaded data

        Raises:
            ExecutorExecutionError: If source data not found
        """
        # The source_ref format is "kind.name" (e.g., "transform.customer_orders")
        kind, name = source_ref.split(".", 1)

        # Try to load from intermediate storage or create view from existing data
        import os
        from pathlib import Path

        intermediate_path = Path(self.context.target_path) / "intermediate"
        parquet_file = intermediate_path / f"{kind}_{name}.parquet"

        if parquet_file.exists():
            # Load from intermediate storage
            view_name = f"source_data"
            con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{parquet_file}'")
            return view_name
        else:
            # Try to use existing view (from transform execution)
            view_name = source_ref.replace(".", "_")
            # Check if view exists
            view_exists = con.execute(
                f"SELECT EXISTS (SELECT * FROM information_schema.views "
                f"WHERE table_name = '{view_name}')"
            ).fetchone()[0]

            if view_exists:
                # Create alias for the view
                con.execute(f"CREATE OR REPLACE VIEW source_data AS SELECT * FROM {view_name}")
                return "source_data"
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Source data '{source_ref}' not found. "
                    f"Ensure the upstream node has been executed."
                )

    def _load_from_intermediate_storage(self, con) -> str:
        """
        Load source data from intermediate storage (when no explicit source).

        Args:
            con: DuckDB connection

        Returns:
            Table/view name for the loaded data

        Raises:
            ExecutorExecutionError: If source data not found
        """
        import os
        from pathlib import Path

        # Look for the most recent transform output
        intermediate_path = Path(self.context.target_path) / "intermediate"
        if not intermediate_path.exists():
            raise ExecutorExecutionError(
                self.node.id,
                "No intermediate storage found. Run upstream transforms first."
            )

        # Find parquet files
        parquet_files = list(intermediate_path.glob("transform_*.parquet"))
        if parquet_files:
            # Use the most recent one
            latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
            con.execute(f"CREATE OR REPLACE VIEW source_data AS SELECT * FROM '{latest_file}'")
            return "source_data"

        raise ExecutorExecutionError(
            self.node.id,
            "No source data found in intermediate storage."
        )

    def _build_aggregation_sql(
        self,
        id_col: str,
        feature_date_col: str,
        application_date_col: str,
        features: List[Dict[str, Any]]
    ) -> tuple[List[str], List[str]]:
        """
        Build SQL SELECT clauses and CTEs for aggregations.

        Args:
            id_col: ID column for grouping
            feature_date_col: Date column for features
            application_date_col: Date column for predictions
            features: List of feature definitions

        Returns:
            Tuple of (select_clauses, cte_queries)
        """
        select_clauses = []
        cte_queries = []

        for feature in features:
            feature_name = feature.get("name", "unknown")

            # Basic aggregations
            if "basic" in feature:
                basic_aggs = feature["basic"]
                column = feature.get("column", feature_name)
                for agg_type in basic_aggs:
                    clause = self._build_basic_aggregation(
                        feature_name=feature_name,
                        agg_type=agg_type,
                        column=column
                    )
                    select_clauses.append(clause)

            # Rolling window aggregations
            if "rolling" in feature:
                rolling_windows = feature["rolling"]
                column = feature.get("column", feature_name)
                for window_config in rolling_windows:
                    windows = window_config.get("window", [])
                    aggs = window_config.get("aggs", [])

                    for window_days in windows:
                        for agg_type in aggs:
                            clause = self._build_rolling_aggregation(
                                feature_name=feature_name,
                                agg_type=agg_type,
                                column=column,
                                window_days=window_days,
                                id_col=id_col,
                                feature_date_col=feature_date_col
                            )
                            select_clauses.append(clause)

            # Ratio aggregations
            if "ratio" in feature:
                column = feature.get("column", feature_name)
                ratio_configs = feature["ratio"]
                for ratio_config in ratio_configs:
                    numerator_range = ratio_config.get("numerator", [])
                    denominator_range = ratio_config.get("denominator", [])
                    aggs = ratio_config.get("aggs", ["sum"])

                    for num_days in numerator_range:
                        for den_days in denominator_range:
                            for agg_type in aggs:
                                clause = self._build_ratio_aggregation(
                                    feature_name=feature_name,
                                    agg_type=agg_type,
                                    column=column,
                                    numerator_days=num_days,
                                    denominator_days=den_days,
                                    id_col=id_col,
                                    feature_date_col=feature_date_col
                                )
                                select_clauses.append(clause)

            # Since/conditional aggregations
            if "since" in feature:
                column = feature.get("column", feature_name)
                since_configs = feature["since"]
                for since_config in since_configs:
                    condition = since_config.get("condition", "")
                    aggs = since_config.get("aggs", ["count"])

                    for agg_type in aggs:
                        clause = self._build_conditional_aggregation(
                            feature_name=feature_name,
                            agg_type=agg_type,
                            column=column,
                            condition=condition
                        )
                        select_clauses.append(clause)

        return select_clauses, cte_queries

    def _build_basic_aggregation(
        self,
        feature_name: str,
        agg_type: str,
        column: str
    ) -> str:
        """
        Build SQL for basic aggregation.

        Args:
            feature_name: Name of the feature (for output column naming)
            agg_type: Aggregation type (sum, avg, min, max, count, stddev)
            column: The actual column name in the source data to aggregate

        Returns:
            SQL SELECT clause
        """
        # Normalize aggregation type
        agg_type = agg_type.lower()
        if agg_type == "mean":
            agg_type = "avg"
        elif agg_type == "std":
            agg_type = "stddev"

        # Map to SQL function
        sql_funcs = {
            "sum": "SUM",
            "avg": "AVG",
            "mean": "AVG",
            "min": "MIN",
            "max": "MAX",
            "count": "COUNT",
            "stddev": "STDDEV",
            "std": "STDDEV",
        }

        if agg_type not in sql_funcs:
            raise ExecutorExecutionError(
                self.node.id,
                f"Unsupported aggregation type: {agg_type}"
            )

        sql_func = sql_funcs[agg_type]

        # Handle count differently (can use any column or *)
        if agg_type == "count":
            col_name = f'"{column}"'
            return f'{sql_func}({col_name}) AS "{feature_name}_{agg_type}"'
        else:
            col_name = f'"{column}"'
            return f'{sql_func}({col_name}) AS "{feature_name}_{agg_type}"'

    def _build_rolling_aggregation(
        self,
        feature_name: str,
        agg_type: str,
        column: str,
        window_days: int,
        id_col: str,
        feature_date_col: str
    ) -> str:
        """
        Build SQL for rolling window aggregation using DuckDB window functions.

        Args:
            feature_name: Name of the feature column
            agg_type: Aggregation type (sum, avg, min, max)
            window_days: Window size in days
            id_col: ID column for partitioning
            feature_date_col: Date column for ordering

        Returns:
            SQL SELECT clause with window function
        """
        # Normalize aggregation type
        agg_type = agg_type.lower()
        if agg_type == "mean":
            agg_type = "avg"

        # Map to SQL function
        sql_funcs = {
            "sum": "SUM",
            "avg": "AVG",
            "min": "MIN",
            "max": "MAX",
        }

        if agg_type not in sql_funcs:
            raise ExecutorExecutionError(
                self.node.id,
                f"Unsupported rolling aggregation type: {agg_type}"
            )

        sql_func = sql_funcs[agg_type]

        # Build window specification
        # Use RANGE BETWEEN with INTERVAL for time-based windows
        window_spec = (
            f'PARTITION BY "{id_col}" '
            f'ORDER BY "{feature_date_col}" '
            f'RANGE BETWEEN INTERVAL {window_days} DAY PRECEDING AND CURRENT ROW'
        )

        col_name = f'"{column}"'
        alias = f"{feature_name}_{agg_type}_{window_days}d"

        return f'{sql_func}({col_name}) OVER ({window_spec}) AS "{alias}"'

    def _build_ratio_aggregation(
        self,
        feature_name: str,
        agg_type: str,
        column: str,
        numerator_days: int,
        denominator_days: int,
        id_col: str,
        feature_date_col: str
    ) -> str:
        """
        Build SQL for ratio aggregation (numerator/denominator with windows).

        Args:
            feature_name: Name of the feature column
            agg_type: Aggregation type (typically sum)
            column: The actual column name in the source data to aggregate
            numerator_days: Window size for numerator
            denominator_days: Window size for denominator
            id_col: ID column for partitioning
            feature_date_col: Date column for ordering

        Returns:
            SQL SELECT clause with ratio calculation
        """
        # Normalize aggregation type
        agg_type = agg_type.lower()

        # Build numerator window
        numerator_window = (
            f'PARTITION BY "{id_col}" '
            f'ORDER BY "{feature_date_col}" '
            f'RANGE BETWEEN INTERVAL {numerator_days} DAY PRECEDING AND CURRENT ROW'
        )

        # Build denominator window
        denominator_window = (
            f'PARTITION BY "{id_col}" '
            f'ORDER BY "{feature_date_col}" '
            f'RANGE BETWEEN INTERVAL {denominator_days} DAY PRECEDING AND CURRENT ROW'
        )

        col_name = f'"{column}"'
        alias = f"{feature_name}_{agg_type}_ratio_{numerator_days}d_{denominator_days}d"

        # Handle division by zero with NULLIF
        return (
            f'{agg_type.upper()}({col_name}) OVER ({numerator_window}) / '
            f'NULLIF({agg_type.upper()}({col_name}) OVER ({denominator_window}), 0) '
            f'AS "{alias}"'
        )

    def _build_conditional_aggregation(
        self,
        feature_name: str,
        agg_type: str,
        column: str,
        condition: str
    ) -> str:
        """
        Build SQL for conditional aggregation (CASE WHEN).

        Args:
            feature_name: Name of the feature column
            agg_type: Aggregation type (count, sum)
            column: The actual column name in the source data to aggregate
            condition: SQL condition for CASE WHEN

        Returns:
            SQL SELECT clause with conditional aggregation
        """
        # Normalize aggregation type
        agg_type = agg_type.lower()

        if agg_type == "count":
            # COUNT with condition
            alias = f"{feature_name}_{agg_type}_if"
            return f'COUNT(CASE WHEN {condition} THEN 1 END) AS "{alias}"'
        elif agg_type == "sum":
            # SUM with condition
            col_name = f'"{column}"'
            alias = f"{feature_name}_{agg_type}_if"
            return f'SUM(CASE WHEN {condition} THEN {col_name} ELSE 0 END) AS "{alias}"'
        else:
            raise ExecutorExecutionError(
                self.node.id,
                f"Unsupported conditional aggregation type: {agg_type}. "
                "Only 'count' and 'sum' are supported."
            )
