"""
Comprehensive tests for DuckDB transformers and aggregators with real data.
"""

import pytest
import pandas as pd
import pyarrow as pa
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb.transformers import (
    SQL,
    ColumnRenamed,
    AddColumnByExpr,
    FilterByExpr,
    SelectColumns,
    DropCols,
)
from seeknal.tasks.duckdb.aggregators import (
    DuckDBAggregator,
    FunctionAggregator,
    ExpressionAggregator,
)


class TestSimpleTransformers:
    """Test simple transformers with real data."""

    @pytest.fixture
    def sample_data(self):
        """Create sample PyArrow Table for testing."""
        data = pd.DataFrame(
            {
                "user_id": ["A", "B", "C", "D", "E"],
                "amount": [100, 200, 150, 250, 180],
                "status": ["active", "inactive", "active", "active", "inactive"],
                "date_col": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
            }
        )
        return pa.Table.from_pandas(data)

    def test_sql_transformer(self, sample_data):
        """Test SQL transformer."""
        result = (
            DuckDBTask(name="test_sql")
            .add_input(dataframe=sample_data)
            .add_sql('SELECT user_id, amount FROM __THIS__ WHERE amount > 150')
            .transform()
        )

        assert len(result) == 3  # 3 rows with amount > 150 (B:200, D:250, E:180)
        assert result.num_columns == 2  # user_id and amount only
        print("✓ SQL transformer works correctly")

    def test_add_new_column(self, sample_data):
        """Test AddColumnByExpr transformer."""
        result = (
            DuckDBTask(name="test_add_column")
            .add_input(dataframe=sample_data)
            .add_new_column("amount * 1.1", "adjusted_amount")
            .transform()
        )

        assert "adjusted_amount" in result.column_names
        assert "amount" in result.column_names

        # Verify calculation
        df = result.to_pandas()
        # Use approximate comparison for floating point (handle Decimal type)
        import decimal
        val = df["adjusted_amount"].iloc[0]
        if isinstance(val, decimal.Decimal):
            val = float(val)
        assert abs(val - 110.0) < 0.01
        print("✓ AddColumnByExpr transformer works correctly")

    def test_filter_by_expr(self, sample_data):
        """Test FilterByExpr transformer."""
        result = (
            DuckDBTask(name="test_filter")
            .add_input(dataframe=sample_data)
            .add_filter_by_expr("status = 'active' AND amount > 100")
            .transform()
        )

        assert len(result) == 2  # C and D match both conditions
        df = result.to_pandas()
        assert all(df["status"] == "active")
        assert all(df["amount"] > 100)
        print("✓ FilterByExpr transformer works correctly")

    def test_select_columns(self, sample_data):
        """Test SelectColumns transformer."""
        result = (
            DuckDBTask(name="test_select")
            .add_input(dataframe=sample_data)
            .select_columns(["user_id", "amount"])
            .transform()
        )

        assert result.column_names == ["user_id", "amount"]
        assert len(result) == 5  # All rows preserved
        print("✓ SelectColumns transformer works correctly")

    def test_drop_columns(self, sample_data):
        """Test DropCols transformer."""
        result = (
            DuckDBTask(name="test_drop")
            .add_input(dataframe=sample_data)
            .drop_columns(["status"])
            .transform()
        )

        assert "status" not in result.column_names
        assert "user_id" in result.column_names
        assert "amount" in result.column_names
        print("✓ DropCols transformer works correctly")

    def test_chain_multiple_transformers(self, sample_data):
        """Test chaining multiple transformers."""
        result = (
            DuckDBTask(name="test_chain")
            .add_input(dataframe=sample_data)
            .add_filter_by_expr("amount > 100")
            .add_new_column("amount * 2", "doubled")
            .select_columns(["user_id", "amount", "doubled"])
            .transform()
        )

        assert len(result) == 4  # After filtering (B:200, C:150, D:250, E:180)
        assert "doubled" in result.column_names
        assert "status" not in result.column_names

        df = result.to_pandas()
        assert df["doubled"].iloc[0] == 200 * 2
        print("✓ Chaining transformers works correctly")


class TestAggregators:
    """Test aggregators with real data."""

    @pytest.fixture
    def agg_data(self):
        """Create sample data for aggregation tests."""
        data = pd.DataFrame(
            {
                "user_id": ["A", "A", "A", "B", "B", "B", "C", "C"],
                "amount": [100, 200, 150, 250, 180, 220, 300, 350],
                "status": [1, 1, 0, 1, 0, 1, 1, 0],
            }
        )
        return pa.Table.from_pandas(data)

    def test_simple_aggregation(self, agg_data):
        """Test FunctionAggregator with simple sum."""
        aggregator = DuckDBAggregator(
            group_by_cols=["user_id"],
            aggregators=[
                FunctionAggregator(inputCol="amount", outputCol="total_amount", accumulatorFunction="sum"),
                FunctionAggregator(inputCol="amount", outputCol="avg_amount", accumulatorFunction="avg"),
                FunctionAggregator(inputCol="amount", outputCol="count_rows", accumulatorFunction="count"),
            ],
        )

        result = (
            DuckDBTask(name="test_agg").add_input(dataframe=agg_data).add_stage(aggregator=aggregator).transform()
        )

        assert len(result) == 3  # 3 unique user_ids
        df = result.to_pandas()

        # Verify user A's aggregations
        user_a = df[df["user_id"] == "A"].iloc[0]
        assert user_a["total_amount"] == 450  # 100 + 200 + 150
        assert user_a["count_rows"] == 3
        print("✓ Simple aggregation works correctly")

    def test_expression_aggregator(self, agg_data):
        """Test ExpressionAggregator with custom SQL."""
        aggregator = DuckDBAggregator(
            group_by_cols=["user_id"],
            aggregators=[
                ExpressionAggregator(
                    expression="SUM(CASE WHEN status = 1 THEN amount ELSE 0 END)", outputCol="active_sum"
                ),
            ],
        )

        result = (
            DuckDBTask(name="test_expr_agg").add_input(dataframe=agg_data).add_stage(aggregator=aggregator).transform()
        )

        assert len(result) == 3
        df = result.to_pandas()

        # User A has status=1 for first 2 rows (100 + 200)
        user_a = df[df["user_id"] == "A"].iloc[0]
        assert user_a["active_sum"] == 300
        print("✓ Expression aggregator works correctly")

    def test_multiple_aggregators(self, agg_data):
        """Test multiple aggregation functions together."""
        aggregator = DuckDBAggregator(
            group_by_cols=["user_id"],
            aggregators=[
                FunctionAggregator(inputCol="amount", outputCol="sum_amount", accumulatorFunction="sum"),
                FunctionAggregator(inputCol="amount", outputCol="min_amount", accumulatorFunction="min"),
                FunctionAggregator(inputCol="amount", outputCol="max_amount", accumulatorFunction="max"),
                FunctionAggregator(inputCol="amount", outputCol="avg_amount", accumulatorFunction="avg"),
                ExpressionAggregator(expression="COUNT(*)", outputCol="row_count"),
            ],
        )

        result = (
            DuckDBTask(name="test_multi_agg").add_input(dataframe=agg_data).add_stage(aggregator=aggregator).transform()
        )

        df = result.to_pandas()
        user_a = df[df["user_id"] == "A"].iloc[0]

        assert user_a["sum_amount"] == 450
        assert user_a["min_amount"] == 100
        assert user_a["max_amount"] == 200
        assert user_a["row_count"] == 3
        print("✓ Multiple aggregators work correctly")


class TestRealParquetData:
    """Test with real parquet files from the project."""

    @pytest.fixture
    def parquet_path(self):
        """Path to real parquet file."""
        return "src/tests/data/poi_sample.parquet/part-00000-9590699e-c6c2-4709-b2e4-9b37e7d544d6-c000.parquet"

    def test_read_parquet(self, parquet_path):
        """Test reading parquet file."""
        result = DuckDBTask(name="test_parquet").add_input(path=parquet_path).transform()

        assert len(result) > 0
        assert result.num_columns > 0
        print(f"✓ Successfully read parquet with {len(result)} rows and {result.num_columns} columns")

    def test_parquet_with_transformations(self, parquet_path):
        """Test parquet with transformations."""
        result = (
            DuckDBTask(name="test_parquet_transform")
            .add_input(path=parquet_path)
            .add_sql("SELECT * FROM __THIS__ LIMIT 10")
            .transform()
        )

        assert len(result) == 10
        print("✓ Parquet transformations work correctly")


class TestComplexPipelines:
    """Test complex multi-stage pipelines."""

    @pytest.fixture
    def complex_data(self):
        """Create larger dataset for complex tests."""
        import numpy as np

        np.random.seed(42)
        data = pd.DataFrame(
            {
                "user_id": np.random.choice(["A", "B", "C", "D", "E"], 100),
                "transaction_amount": np.random.randint(10, 500, 100),
                "category": np.random.choice(["food", "travel", "entertainment", "bills"], 100),
                "date": pd.date_range("2024-01-01", periods=100),
            }
        )
        return pa.Table.from_pandas(data)

    def test_filter_transform_aggregate_pipeline(self, complex_data):
        """Test complete pipeline: filter -> transform -> aggregate."""
        aggregator = DuckDBAggregator(
            group_by_cols=["user_id", "category"],
            aggregators=[
                FunctionAggregator(inputCol="transaction_amount", outputCol="total_spent", accumulatorFunction="sum"),
                FunctionAggregator(inputCol="transaction_amount", outputCol="num_transactions", accumulatorFunction="count"),
            ],
        )

        result = (
            DuckDBTask(name="test_complex_pipeline")
            .add_input(dataframe=complex_data)
            .add_filter_by_expr("transaction_amount > 100")
            .add_new_column("transaction_amount * 0.9", "after_discount")
            .add_stage(aggregator=aggregator)
            .add_filter_by_expr("total_spent > 1000")
            .transform()
        )

        assert len(result) > 0
        df = result.to_pandas()
        assert all(df["total_spent"] > 1000)  # Filter applied
        print(f"✓ Complex pipeline works correctly, returned {len(result)} rows")

    def test_return_as_pandas(self, complex_data):
        """Test returning pandas DataFrame instead of PyArrow."""
        result = (
            DuckDBTask(name="test_pandas")
            .add_input(dataframe=complex_data)
            .add_sql("SELECT * FROM __THIS__ LIMIT 10")
            .transform(params={"return_as_pandas": True})
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 10
        print("✓ Return as pandas DataFrame works correctly")


def get_sample_data():
    """Create sample PyArrow Table for testing."""
    data = pd.DataFrame(
        {
            "user_id": ["A", "B", "C", "D", "E"],
            "amount": [100, 200, 150, 250, 180],
            "status": ["active", "inactive", "active", "active", "inactive"],
            "date_col": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"],
        }
    )
    return pa.Table.from_pandas(data)


def get_agg_data():
    """Create sample data for aggregation tests."""
    data = pd.DataFrame(
        {
            "user_id": ["A", "A", "A", "B", "B", "B", "C", "C"],
            "amount": [100, 200, 150, 250, 180, 220, 300, 350],
            "status": [1, 1, 0, 1, 0, 1, 1, 0],
        }
    )
    return pa.Table.from_pandas(data)


def get_complex_data():
    """Create larger dataset for complex tests."""
    import numpy as np

    np.random.seed(42)
    data = pd.DataFrame(
        {
            "user_id": np.random.choice(["A", "B", "C", "D", "E"], 100),
            "transaction_amount": np.random.randint(10, 500, 100),
            "category": np.random.choice(["food", "travel", "entertainment", "bills"], 100),
            "date": pd.date_range("2024-01-01", periods=100),
        }
    )
    return pa.Table.from_pandas(data)


def run_all_tests():
    """Run all tests manually without pytest."""
    print("\n" + "=" * 60)
    print("RUNNING DUCKDB TRANSFORMER AND AGGREGATOR TESTS")
    print("=" * 60 + "\n")

    # Create test instances
    test_simple = TestSimpleTransformers()
    test_agg = TestAggregators()
    test_real = TestRealParquetData()
    test_complex = TestComplexPipelines()

    # Setup fixtures
    sample_data = get_sample_data()
    agg_data = get_agg_data()
    complex_data = get_complex_data()

    # Run simple transformer tests
    print("Testing Simple Transformers...")
    test_simple.test_sql_transformer(sample_data)
    test_simple.test_add_new_column(sample_data)
    test_simple.test_filter_by_expr(sample_data)
    test_simple.test_select_columns(sample_data)
    test_simple.test_drop_columns(sample_data)
    test_simple.test_chain_multiple_transformers(sample_data)
    print("\n✓ All simple transformer tests passed!\n")

    # Run aggregator tests
    print("Testing Aggregators...")
    test_agg.test_simple_aggregation(agg_data)
    test_agg.test_expression_aggregator(agg_data)
    test_agg.test_multiple_aggregators(agg_data)
    print("\n✓ All aggregator tests passed!\n")

    # Run real data tests
    parquet_path = "src/tests/data/poi_sample.parquet/part-00000-9590699e-c6c2-4709-b2e4-9b37e7d544d6-c000.parquet"
    if Path(parquet_path).exists():
        print("Testing with Real Parquet Data...")
        test_real.test_read_parquet(parquet_path)
        test_real.test_parquet_with_transformations(parquet_path)
        print("\n✓ All real data tests passed!\n")
    else:
        print(f"⚠ Skipping real data tests (file not found: {parquet_path})\n")

    # Run complex pipeline tests
    print("Testing Complex Pipelines...")
    test_complex.test_filter_transform_aggregate_pipeline(complex_data)
    test_complex.test_return_as_pandas(complex_data)
    print("\n✓ All complex pipeline tests passed!\n")

    print("=" * 60)
    print("ALL TESTS PASSED SUCCESSFULLY! ✓")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run_all_tests()
