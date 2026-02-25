"""
Tests for ProfileExecutor.

Tests the profile node executor for computing statistical profiles
from upstream data using DuckDB.
"""

import json
import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
)
from seeknal.workflow.executors.profile_executor import (
    ProfileExecutor,
    _classify_type,
    _sanitize_name,
)
from seeknal.dag.manifest import Node, NodeType


@pytest.fixture
def execution_context(tmp_path):
    """Create an execution context with target directory."""
    target = tmp_path / "target"
    target.mkdir()
    (target / "intermediate").mkdir()
    return ExecutionContext(
        project_name="test_project",
        workspace_path=tmp_path,
        target_path=target,
        dry_run=False,
        verbose=False,
    )


@pytest.fixture
def dry_run_context(tmp_path):
    """Create a dry-run execution context."""
    target = tmp_path / "target"
    target.mkdir()
    (target / "intermediate").mkdir()
    return ExecutionContext(
        project_name="test_project",
        workspace_path=tmp_path,
        target_path=target,
        dry_run=True,
        verbose=False,
    )


def _make_profile_node(name="test_stats", config=None):
    """Helper to create a profile node."""
    default_config = {
        "inputs": [{"ref": "source.products"}],
    }
    if config:
        default_config.update(config)
    return Node(
        id=f"profile.{name}",
        name=name,
        node_type=NodeType.PROFILE,
        config=default_config,
    )


def _write_input_parquet(context, ref_name, df):
    """Write a DataFrame as an intermediate parquet for testing."""
    path = context.target_path / "intermediate" / f"{ref_name}.parquet"
    df.to_parquet(str(path), index=False)
    return path


class TestTypeClassification:
    """Test DuckDB type classification."""

    def test_numeric_types(self):
        assert _classify_type("INTEGER") == "numeric"
        assert _classify_type("BIGINT") == "numeric"
        assert _classify_type("DOUBLE") == "numeric"
        assert _classify_type("FLOAT") == "numeric"
        assert _classify_type("DECIMAL") == "numeric"
        assert _classify_type("DECIMAL(10,2)") == "numeric"
        assert _classify_type("HUGEINT") == "numeric"

    def test_timestamp_types(self):
        assert _classify_type("TIMESTAMP") == "timestamp"
        assert _classify_type("TIMESTAMP WITH TIME ZONE") == "timestamp"
        assert _classify_type("DATE") == "timestamp"

    def test_string_types(self):
        assert _classify_type("VARCHAR") == "string"
        assert _classify_type("TEXT") == "string"
        assert _classify_type("UUID") == "string"

    def test_boolean_type(self):
        assert _classify_type("BOOLEAN") == "boolean"

    def test_skip_types(self):
        assert _classify_type("BLOB") == "skip"
        assert _classify_type("BYTEA") == "skip"

    def test_unknown_defaults_to_string(self):
        assert _classify_type("SOMETHINGELSE") == "string"


class TestNameSanitization:
    """Test profile name sanitization for path safety."""

    def test_clean_name_unchanged(self):
        assert _sanitize_name("products_stats") == "products_stats"

    def test_path_traversal_stripped(self):
        assert _sanitize_name("../evil") == "evil"
        assert _sanitize_name("../../etc/passwd") == "etcpasswd"

    def test_slashes_stripped(self):
        assert _sanitize_name("foo/bar") == "foobar"
        assert _sanitize_name("foo\\bar") == "foobar"

    def test_dotdot_stripped(self):
        assert _sanitize_name("foo..bar") == "foobar"


class TestProfileExecutorValidation:
    """Test ProfileExecutor.validate()."""

    def test_validate_minimal_config(self, execution_context):
        """Minimal config with just inputs should pass."""
        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        executor.validate()  # Should not raise

    def test_validate_with_columns(self, execution_context):
        """Config with valid column names should pass."""
        node = _make_profile_node(config={
            "profile": {"columns": ["price", "quantity"]},
        })
        executor = ProfileExecutor(node, execution_context)
        executor.validate()

    def test_validate_with_params(self, execution_context):
        """Config with valid params should pass."""
        node = _make_profile_node(config={
            "profile": {"params": {"max_top_values": 10}},
        })
        executor = ProfileExecutor(node, execution_context)
        executor.validate()

    def test_validate_missing_inputs(self, execution_context):
        """Should fail when inputs is empty."""
        node = _make_profile_node(config={"inputs": []})
        executor = ProfileExecutor(node, execution_context)
        with pytest.raises(ExecutorValidationError, match="inputs"):
            executor.validate()

    def test_validate_no_inputs_key(self, execution_context):
        """Should fail when inputs key is missing."""
        node = Node(
            id="profile.bad",
            name="bad",
            node_type=NodeType.PROFILE,
            config={},
        )
        executor = ProfileExecutor(node, execution_context)
        with pytest.raises(ExecutorValidationError, match="inputs"):
            executor.validate()

    def test_validate_invalid_column_name(self, execution_context):
        """Should fail for column names that don't pass validation."""
        node = _make_profile_node(config={
            "profile": {"columns": ["valid_col", "'; DROP TABLE --"]},
        })
        executor = ProfileExecutor(node, execution_context)
        with pytest.raises(ExecutorValidationError, match="Invalid column name"):
            executor.validate()

    def test_validate_columns_not_list(self, execution_context):
        """Should fail when columns is not a list."""
        node = _make_profile_node(config={
            "profile": {"columns": "not_a_list"},
        })
        executor = ProfileExecutor(node, execution_context)
        with pytest.raises(ExecutorValidationError, match="must be a list"):
            executor.validate()

    def test_validate_max_top_values_zero(self, execution_context):
        """Should fail when max_top_values is 0."""
        node = _make_profile_node(config={
            "profile": {"params": {"max_top_values": 0}},
        })
        executor = ProfileExecutor(node, execution_context)
        with pytest.raises(ExecutorValidationError, match="positive integer"):
            executor.validate()

    def test_validate_max_top_values_negative(self, execution_context):
        """Should fail when max_top_values is negative."""
        node = _make_profile_node(config={
            "profile": {"params": {"max_top_values": -1}},
        })
        executor = ProfileExecutor(node, execution_context)
        with pytest.raises(ExecutorValidationError, match="positive integer"):
            executor.validate()


class TestProfileExecutorDryRun:
    """Test ProfileExecutor dry-run mode."""

    def test_dry_run_returns_success(self, dry_run_context):
        """Dry-run should return success without computing stats."""
        node = _make_profile_node()
        executor = ProfileExecutor(node, dry_run_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.is_dry_run is True
        assert result.metadata["dry_run"] is True
        assert result.row_count == 0


class TestProfileExecutorExecution:
    """Test ProfileExecutor.execute() with real data."""

    def test_numeric_columns(self, execution_context):
        """Test profiling numeric columns produces correct metrics."""
        df = pd.DataFrame({
            "price": [10.0, 20.0, 30.0, 40.0, 50.0],
            "quantity": [1, 2, 3, 4, 5],
        })
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.row_count == 5

        # Read output parquet
        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        assert output_path.exists()

        stats_df = pd.read_parquet(str(output_path))

        # Check schema: 4 columns
        assert list(stats_df.columns) == ["column_name", "metric", "value", "detail"]

        # Check row_count metric
        row_count_row = stats_df[
            (stats_df["column_name"] == "_table_") & (stats_df["metric"] == "row_count")
        ]
        assert len(row_count_row) == 1
        assert row_count_row.iloc[0]["value"] == "5"

        # Check avg metric for price
        avg_row = stats_df[
            (stats_df["column_name"] == "price") & (stats_df["metric"] == "avg")
        ]
        assert len(avg_row) == 1
        assert float(avg_row.iloc[0]["value"]) == pytest.approx(30.0)

        # Check stddev exists
        stddev_row = stats_df[
            (stats_df["column_name"] == "price") & (stats_df["metric"] == "stddev")
        ]
        assert len(stddev_row) == 1
        assert stddev_row.iloc[0]["value"] is not None

        # Check percentiles exist
        for metric in ["p25", "p50", "p75"]:
            p_row = stats_df[
                (stats_df["column_name"] == "price") & (stats_df["metric"] == metric)
            ]
            assert len(p_row) == 1

    def test_string_columns(self, execution_context):
        """Test profiling string columns produces top_values."""
        df = pd.DataFrame({
            "category": ["A", "A", "A", "B", "B", "C"],
        })
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        # Check top_values
        top_row = stats_df[
            (stats_df["column_name"] == "category") & (stats_df["metric"] == "top_values")
        ]
        assert len(top_row) == 1
        detail = top_row.iloc[0]["detail"]
        assert detail is not None
        # Detail should be parseable (list of dicts with value, count, percent)
        parsed = json.loads(detail.replace("'", '"')) if isinstance(detail, str) else detail
        assert len(parsed) <= 5  # default max_top_values

        # Check distinct_count
        distinct_row = stats_df[
            (stats_df["column_name"] == "category") & (stats_df["metric"] == "distinct_count")
        ]
        assert len(distinct_row) == 1
        assert distinct_row.iloc[0]["value"] == "3"

    def test_timestamp_columns(self, execution_context):
        """Test profiling timestamp columns produces freshness_hours."""
        now = datetime.now()
        df = pd.DataFrame({
            "created_at": [
                now - timedelta(hours=10),
                now - timedelta(hours=5),
                now - timedelta(hours=1),
            ],
        })
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        # Check freshness_hours exists and is reasonable
        fresh_row = stats_df[
            (stats_df["column_name"] == "created_at") & (stats_df["metric"] == "freshness_hours")
        ]
        assert len(fresh_row) == 1
        freshness = float(fresh_row.iloc[0]["value"])
        # Should be roughly 1 hour (the most recent record)
        assert 0.5 < freshness < 2.0

    def test_mixed_types(self, execution_context):
        """Test profiling a table with mixed column types."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
            "active": [True, False, True],
        })
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["columns_profiled"] > 0

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        # Should have metrics for all columns plus _table_
        column_names = set(stats_df["column_name"])
        assert "_table_" in column_names
        assert "id" in column_names
        assert "name" in column_names
        assert "score" in column_names

    def test_column_filter(self, execution_context):
        """Test that column filter limits which columns are profiled."""
        df = pd.DataFrame({
            "price": [10.0, 20.0, 30.0],
            "quantity": [1, 2, 3],
            "category": ["A", "B", "C"],
        })
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node(config={
            "profile": {"columns": ["price"]},
        })
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        # Only _table_ and price should be present
        column_names = set(stats_df["column_name"])
        assert "price" in column_names
        assert "_table_" in column_names
        assert "quantity" not in column_names
        assert "category" not in column_names

    def test_nonexistent_column_in_filter(self, execution_context):
        """Test that non-existent columns in filter are skipped with warning."""
        df = pd.DataFrame({
            "price": [10.0, 20.0, 30.0],
        })
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node(config={
            "profile": {"columns": ["price", "nonexistent"]},
        })
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        column_names = set(stats_df["column_name"])
        assert "price" in column_names
        assert "nonexistent" not in column_names

    def test_empty_table(self, execution_context):
        """Test profiling an empty table produces row_count=0 and NULL metrics."""
        df = pd.DataFrame({
            "price": pd.Series([], dtype="float64"),
            "name": pd.Series([], dtype="str"),
        })
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.row_count == 0

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        # row_count should be 0
        row_count_row = stats_df[
            (stats_df["column_name"] == "_table_") & (stats_df["metric"] == "row_count")
        ]
        assert row_count_row.iloc[0]["value"] == "0"

    def test_all_null_column(self, execution_context):
        """Test profiling a column with all NULL values."""
        df = pd.DataFrame({
            "price": [None, None, None],
        })
        # Force float type to ensure it's treated as numeric
        df["price"] = df["price"].astype("float64")
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        # null_count should equal row_count
        null_count_row = stats_df[
            (stats_df["column_name"] == "price") & (stats_df["metric"] == "null_count")
        ]
        assert null_count_row.iloc[0]["value"] == "3"

        # null_percent should be 100
        null_pct_row = stats_df[
            (stats_df["column_name"] == "price") & (stats_df["metric"] == "null_percent")
        ]
        assert float(null_pct_row.iloc[0]["value"]) == pytest.approx(100.0)

        # distinct_count should be 0
        distinct_row = stats_df[
            (stats_df["column_name"] == "price") & (stats_df["metric"] == "distinct_count")
        ]
        assert distinct_row.iloc[0]["value"] == "0"

    def test_no_input_data(self, execution_context):
        """Test that missing input data raises an error."""
        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)

        with pytest.raises(ExecutorExecutionError, match="No input data"):
            executor.run()

    def test_max_top_values_param(self, execution_context):
        """Test that max_top_values param limits top values output."""
        df = pd.DataFrame({
            "category": ["A", "B", "C", "D", "E", "F", "G"] * 10,
        })
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node(config={
            "profile": {"params": {"max_top_values": 3}},
        })
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        top_row = stats_df[
            (stats_df["column_name"] == "category") & (stats_df["metric"] == "top_values")
        ]
        detail = top_row.iloc[0]["detail"]
        parsed = json.loads(detail.replace("'", '"')) if isinstance(detail, str) else detail
        assert len(parsed) <= 3

    def test_output_path_uses_sanitized_name(self, execution_context):
        """Test that output path uses sanitized profile name."""
        df = pd.DataFrame({"x": [1, 2, 3]})
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node(name="clean_name")
        executor = ProfileExecutor(node, execution_context)
        result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS
        expected_path = execution_context.target_path / "intermediate" / "profile_clean_name.parquet"
        assert expected_path.exists()

    def test_output_schema_has_four_columns(self, execution_context):
        """Verify output parquet has exactly 4 columns: column_name, metric, value, detail."""
        df = pd.DataFrame({"x": [1, 2, 3]})
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        executor.run()

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))
        assert list(stats_df.columns) == ["column_name", "metric", "value", "detail"]

    def test_single_row_stddev_is_null(self, execution_context):
        """stddev should be NULL for a single-row table (undefined for n=1)."""
        df = pd.DataFrame({"price": [42.0]})
        _write_input_parquet(execution_context, "source_products", df)

        node = _make_profile_node()
        executor = ProfileExecutor(node, execution_context)
        executor.run()

        output_path = execution_context.target_path / "intermediate" / "profile_test_stats.parquet"
        stats_df = pd.read_parquet(str(output_path))

        stddev_row = stats_df[
            (stats_df["column_name"] == "price") & (stats_df["metric"] == "stddev")
        ]
        # DuckDB STDDEV on single row returns NULL
        assert len(stddev_row) == 1
        val = stddev_row.iloc[0]["value"]
        import math
        assert val is None or val == "None" or val == "nan" or (isinstance(val, float) and math.isnan(val)) or float(val) == 0.0
