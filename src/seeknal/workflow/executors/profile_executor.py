"""
Profile executor for Seeknal workflow node execution.

This module implements the ProfileExecutor class, which executes PROFILE nodes
by computing statistical profiles (row count, avg, stddev, null metrics,
distinct counts, top values, freshness) from upstream data using DuckDB.

Output is a long-format stats parquet with one row per (column, metric) pair,
stored at target/intermediate/profile_<name>.parquet.

Design Decisions:
- Uses DuckDB DESCRIBE for auto-detecting column types
- Builds a single UNION ALL query per column for efficient computation
- Uses approx_quantile for percentiles (T-Digest, O(N), acceptable accuracy)
- CAST(COUNT(*) AS BIGINT) to avoid HUGEINT in parquet output
- Column names validated via validate_column_name() before SQL interpolation
"""

import logging
import re
import time
from pathlib import Path

import duckdb  # ty: ignore[unresolved-import]

from seeknal.dag.manifest import NodeType  # ty: ignore[unresolved-import]
from seeknal.validation import validate_column_name  # ty: ignore[unresolved-import]

from .base import (
    BaseExecutor,
    ExecutorResult,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
    register_executor,
)

logger = logging.getLogger(__name__)

# Type classification sets for DuckDB column types
NUMERIC_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "FLOAT", "DOUBLE", "DECIMAL",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT",
}

TIMESTAMP_TYPES = {
    "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ",
    "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_NS", "DATE",
}

STRING_TYPES = {"VARCHAR", "TEXT", "CHAR", "BPCHAR", "UUID"}

SKIP_TYPES = {"BLOB", "BYTEA"}

# Sanitization pattern for profile names used in file paths
_UNSAFE_PATH_CHARS = re.compile(r'(\.\.[/\\])|[/\\]')
_DOTDOT = re.compile(r'\.\.')


def _sanitize_name(name: str) -> str:
    """Sanitize profile name for safe use in file paths."""
    s = _UNSAFE_PATH_CHARS.sub('', name)
    s = _DOTDOT.sub('', s)
    return s


def _classify_type(duckdb_type: str) -> str:
    """Classify a DuckDB column type into a category.

    Returns one of: 'numeric', 'timestamp', 'string', 'boolean', 'skip'.
    """
    upper = duckdb_type.upper()
    # Handle DECIMAL(p,s) by checking prefix
    if upper.startswith("DECIMAL"):
        return "numeric"
    if upper in NUMERIC_TYPES:
        return "numeric"
    if upper in TIMESTAMP_TYPES:
        return "timestamp"
    if upper in STRING_TYPES:
        return "string"
    if upper == "BOOLEAN":
        return "boolean"
    if upper in SKIP_TYPES:
        return "skip"
    # Default: treat as string
    return "string"


@register_executor(NodeType.PROFILE)
class ProfileExecutor(BaseExecutor):
    """
    Executor for PROFILE nodes in the workflow DAG.

    Computes statistical profiles from upstream data and writes a long-format
    stats parquet table.

    YAML Configuration Example:
        ```yaml
        # Minimal — auto-detect all columns
        kind: profile
        name: products_stats
        inputs:
          - ref: source.products

        # With column filter and params
        kind: profile
        name: products_stats
        inputs:
          - ref: source.products
        profile:
          columns: [price, quantity, category]
          params:
            max_top_values: 10
        ```
    """

    @property
    def node_type(self) -> NodeType:
        return NodeType.PROFILE

    def validate(self) -> None:
        """Validate the PROFILE node configuration."""
        inputs = self.node.config.get("inputs", [])
        if not inputs:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required 'inputs' — profile needs at least one input"
            )

        profile_config = self.node.config.get("profile", {})
        columns = profile_config.get("columns", [])

        if columns:
            if not isinstance(columns, list):
                raise ExecutorValidationError(
                    self.node.id,
                    "'profile.columns' must be a list"
                )
            for col in columns:
                try:
                    validate_column_name(col)
                except Exception as e:
                    raise ExecutorValidationError(
                        self.node.id,
                        f"Invalid column name '{col}': {e}"
                    )

        params = profile_config.get("params", {})
        max_top = params.get("max_top_values", 5)
        if not isinstance(max_top, int) or max_top < 1:
            raise ExecutorValidationError(
                self.node.id,
                f"'profile.params.max_top_values' must be a positive integer, got {max_top}"
            )

    def execute(self) -> ExecutorResult:
        """Execute the PROFILE node using DuckDB.

        1. Load input parquet
        2. DESCRIBE to detect column types
        3. Build UNION ALL stats queries per column
        4. Write long-format parquet

        Returns:
            ExecutorResult with profile metrics
        """
        start_time = time.time()

        try:
            # Handle dry-run mode
            if self.context.dry_run:
                return self._execute_dry_run()

            input_path = self._resolve_input_path()
            if input_path is None:
                raise ExecutorExecutionError(
                    self.node.id,
                    "No input data available for profiling"
                )

            con = duckdb.connect()
            try:
                safe_path = str(input_path).replace("'", "''")
                con.execute(
                    f"CREATE VIEW input_data AS SELECT * FROM read_parquet('{safe_path}')"
                )

                # Get row count
                row_count = con.execute(
                    "SELECT CAST(COUNT(*) AS BIGINT) FROM input_data"
                ).fetchone()[0]

                # Get column info via DESCRIBE
                col_info = con.execute("DESCRIBE input_data").fetchall()

                # Apply column filter if specified
                profile_config = self.node.config.get("profile", {})
                filter_cols = profile_config.get("columns", [])
                max_top_values = profile_config.get("params", {}).get("max_top_values", 5)

                if filter_cols:
                    available = {row[0] for row in col_info}
                    valid_cols = []
                    for col in filter_cols:
                        if col in available:
                            valid_cols.append(col)
                        else:
                            logger.warning(
                                f"Profile '{self.node.name}': column '{col}' not found "
                                f"in input, skipping"
                            )
                    col_info = [row for row in col_info if row[0] in valid_cols]

                # Build and execute stats query
                stats_sql = self._build_stats_sql(col_info, row_count, max_top_values)

                if stats_sql:
                    result_df = con.execute(stats_sql).fetchdf()
                else:
                    # No columns to profile — create empty DataFrame
                    import pandas as pd  # ty: ignore[unresolved-import]
                    result_df = pd.DataFrame(
                        columns=["column_name", "metric", "value", "detail"]
                    )

                # Write output parquet
                output_path = self._get_output_path()
                output_path.parent.mkdir(parents=True, exist_ok=True)
                result_df.to_parquet(str(output_path), index=False)

                metric_count = len(result_df)
                col_count = len(set(result_df["column_name"])) if metric_count > 0 else 0

                logger.info(
                    f"Profile '{self.node.name}': {metric_count} metrics "
                    f"across {col_count} columns ({row_count} input rows)"
                )

            finally:
                con.close()

            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                row_count=row_count,
                output_path=output_path,
                metadata={
                    "profile_name": self.node.name,
                    "input_rows": row_count,
                    "metric_count": metric_count,
                    "columns_profiled": col_count,
                },
            )

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Profile execution failed: {str(e)}",
                e,
            ) from e

    def _execute_dry_run(self) -> ExecutorResult:
        """Return a dry-run result without computing stats."""
        return ExecutorResult(
            node_id=self.node.id,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=0.0,
            row_count=0,
            metadata={
                "dry_run": True,
                "profile_name": self.node.name,
                "message": "Dry-run: profile not computed",
            },
            is_dry_run=True,
        )

    def _resolve_input_path(self) -> Path | None:
        """Resolve the first input reference to an intermediate parquet path."""
        inputs = self.node.config.get("inputs", [])
        if not inputs:
            return None

        first_input = inputs[0]
        if isinstance(first_input, dict) and "ref" in first_input:
            ref = first_input["ref"]
        elif isinstance(first_input, str):
            ref = first_input
        else:
            return None

        # Try intermediate path: target/intermediate/{kind}_{name}.parquet
        ref_path = ref.replace(".", "_")
        intermediate = self.context.target_path / "intermediate" / f"{ref_path}.parquet"
        if intermediate.exists():
            return intermediate

        # Try cache path: target/cache/{kind}/{name}.parquet
        parts = ref.split(".")
        if len(parts) == 2:
            kind, name = parts
            cache = self.context.target_path / "cache" / kind / f"{name}.parquet"
            if cache.exists():
                return cache

        return None

    def _get_output_path(self) -> Path:
        """Get the output parquet path for this profile."""
        safe_name = _sanitize_name(self.node.name)
        return self.context.target_path / "intermediate" / f"profile_{safe_name}.parquet"

    def _build_stats_sql(
        self,
        col_info: list[tuple],
        row_count: int,
        max_top_values: int,
    ) -> str | None:
        """Build UNION ALL SQL for all metrics across all columns.

        Args:
            col_info: List of (name, type, ...) tuples from DESCRIBE
            row_count: Total row count (for empty-table handling)
            max_top_values: Max number of top values for string columns

        Returns:
            SQL string, or None if no columns to profile
        """
        parts: list[str] = []

        # Table-level: row_count
        parts.append(
            "SELECT '_table_' AS column_name, 'row_count' AS metric, "
            f"CAST(CAST(COUNT(*) AS BIGINT) AS VARCHAR) AS value, "
            "NULL AS detail FROM input_data"
        )

        for row in col_info:
            col_name = row[0]
            col_type = row[1]
            category = _classify_type(col_type)

            if category == "skip":
                logger.warning(
                    f"Profile '{self.node.name}': skipping BLOB/binary column '{col_name}'"
                )
                # Only basic null metrics for skipped types
                parts.extend(self._null_metrics_sql(col_name))
                continue

            # Universal metrics: null_count, null_percent, distinct_count
            parts.extend(self._null_metrics_sql(col_name))
            parts.append(
                f"SELECT '{col_name}', 'distinct_count', "
                f"CAST(CAST(COUNT(DISTINCT \"{col_name}\") AS BIGINT) AS VARCHAR), "
                f"NULL FROM input_data"
            )

            if category == "numeric":
                parts.extend(self._numeric_metrics_sql(col_name))
            elif category == "timestamp":
                parts.extend(self._timestamp_metrics_sql(col_name))
            elif category in ("string", "boolean"):
                parts.extend(self._top_values_sql(col_name, max_top_values))

        if not parts:
            return None

        return "\nUNION ALL\n".join(parts)

    def _null_metrics_sql(self, col: str) -> list[str]:
        """Generate SQL fragments for null_count and null_percent."""
        return [
            f"SELECT '{col}', 'null_count', "
            f"CAST(CAST(SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) AS BIGINT) AS VARCHAR), "
            f"NULL FROM input_data",

            f"SELECT '{col}', 'null_percent', "
            f"CAST(CASE WHEN COUNT(*) = 0 THEN 0 "
            f"ELSE 100.0 * SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) / COUNT(*) "
            f"END AS VARCHAR), "
            f"NULL FROM input_data",
        ]

    def _numeric_metrics_sql(self, col: str) -> list[str]:
        """Generate SQL fragments for numeric column metrics."""
        return [
            f"SELECT '{col}', 'min', CAST(MIN(\"{col}\") AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'max', CAST(MAX(\"{col}\") AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'avg', CAST(AVG(\"{col}\") AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'sum', CAST(CAST(SUM(\"{col}\") AS DOUBLE) AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'stddev', CAST(STDDEV(\"{col}\") AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'p25', CAST(approx_quantile(\"{col}\", 0.25) AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'p50', CAST(approx_quantile(\"{col}\", 0.5) AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'p75', CAST(approx_quantile(\"{col}\", 0.75) AS VARCHAR), NULL FROM input_data",
        ]

    def _timestamp_metrics_sql(self, col: str) -> list[str]:
        """Generate SQL fragments for timestamp column metrics."""
        return [
            f"SELECT '{col}', 'min', CAST(MIN(\"{col}\") AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'max', CAST(MAX(\"{col}\") AS VARCHAR), NULL FROM input_data",
            f"SELECT '{col}', 'freshness_hours', "
            f"CAST(CASE WHEN MAX(\"{col}\") IS NULL THEN NULL "
            f"ELSE epoch(CURRENT_TIMESTAMP - CAST(MAX(\"{col}\") AS TIMESTAMP)) / 3600.0 "
            f"END AS VARCHAR), NULL FROM input_data",
        ]

    def _top_values_sql(self, col: str, max_top: int) -> list[str]:
        """Generate SQL fragment for top_values metric."""
        # Wrap in subquery to avoid LIMIT-in-UNION-ALL parse error
        return [
            f"SELECT * FROM ("
            f"SELECT '{col}' AS column_name, 'top_values' AS metric, "
            f"CAST(NULL AS VARCHAR) AS value, "
            f"CAST(("
            f"  SELECT list(json_object("
            f"    'value', CAST(val AS VARCHAR), "
            f"    'count', CAST(cnt AS BIGINT), "
            f"    'percent', ROUND(pct, 2)"
            f"  )) FROM ("
            f"    SELECT \"{col}\" AS val, "
            f"    CAST(COUNT(*) AS BIGINT) AS cnt, "
            f"    100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS pct "
            f"    FROM input_data "
            f"    WHERE \"{col}\" IS NOT NULL "
            f"    GROUP BY \"{col}\" "
            f"    ORDER BY cnt DESC "
            f"    LIMIT {int(max_top)}"
            f"  )"
            f") AS VARCHAR) AS detail "
            f"LIMIT 1)",
        ]

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """Add execution context info to result metadata."""
        result.metadata["project_name"] = self.context.project_name
        result.metadata["workspace_path"] = str(self.context.workspace_path)
        return result
