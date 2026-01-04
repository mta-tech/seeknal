"""
Tests for quickstart DuckDB example validation.

This module validates that the quickstart_duckdb.py example runs successfully
and produces expected outputs. It tests the DuckDB workflow demonstrated in
the getting started guide.
"""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pytest

from seeknal.tasks.duckdb import DuckDBTask


# Path to quickstart files relative to project root
QUICKSTART_DIR = os.path.join(
    os.path.dirname(__file__), "..", "..", "examples", "quickstart"
)
QUICKSTART_SCRIPT = os.path.join(QUICKSTART_DIR, "quickstart_duckdb.py")
SAMPLE_DATA_PATH = os.path.join(QUICKSTART_DIR, "sample_data.csv")


@pytest.fixture(scope="module")
def sample_data():
    """Load sample data CSV for testing."""
    if not os.path.exists(SAMPLE_DATA_PATH):
        pytest.skip(f"Sample data not found at {SAMPLE_DATA_PATH}")
    return pd.read_csv(SAMPLE_DATA_PATH)


@pytest.fixture(scope="module")
def sample_data_arrow(sample_data):
    """Convert sample data to PyArrow table."""
    return pa.Table.from_pandas(sample_data)


class TestQuickstartFilesExist:
    """Tests for quickstart file existence."""

    def test_quickstart_script_exists(self):
        """Verify the quickstart_duckdb.py script exists."""
        assert os.path.exists(QUICKSTART_SCRIPT), (
            f"Quickstart script not found at {QUICKSTART_SCRIPT}"
        )

    def test_sample_data_exists(self):
        """Verify the sample_data.csv file exists."""
        assert os.path.exists(SAMPLE_DATA_PATH), (
            f"Sample data not found at {SAMPLE_DATA_PATH}"
        )

    def test_quickstart_script_is_valid_python(self):
        """Verify the quickstart script is valid Python."""
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", QUICKSTART_SCRIPT],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"Script has syntax errors: {result.stderr}"
        )


class TestDuckDBTaskBasics:
    """Tests for basic DuckDB task functionality used in quickstart."""

    def test_duckdb_task_creation(self):
        """Verify DuckDBTask can be created."""
        task = DuckDBTask()
        assert task is not None
        assert task.kind == "duckdb"

    def test_duckdb_task_is_not_spark(self):
        """Verify DuckDBTask is not a Spark job."""
        task = DuckDBTask()
        assert task.is_spark_job is False

    def test_duckdb_task_add_input_with_arrow(self, sample_data_arrow):
        """Verify DuckDBTask can accept PyArrow input."""
        task = DuckDBTask()
        task.add_input(dataframe=sample_data_arrow)
        assert "dataframe" in task.input

    def test_duckdb_task_add_sql(self, sample_data_arrow):
        """Verify DuckDBTask can accept SQL queries."""
        task = DuckDBTask()
        task.add_input(dataframe=sample_data_arrow)
        task.add_sql("SELECT * FROM __THIS__ LIMIT 10")
        assert len(task.sqls) > 0


class TestDuckDBTransformation:
    """Tests for DuckDB data transformation."""

    def test_simple_select(self, sample_data_arrow):
        """Verify simple SELECT transformation works."""
        task = DuckDBTask()
        task.add_input(dataframe=sample_data_arrow)
        task.add_sql("SELECT user_id, action_type FROM __THIS__ LIMIT 5")
        result = task.transform()

        assert result is not None
        assert len(result) == 5

    def test_aggregation_query(self, sample_data_arrow):
        """Verify aggregation queries work (as used in quickstart)."""
        task = DuckDBTask()
        task.add_input(dataframe=sample_data_arrow)
        task.add_sql("""
            SELECT
                user_id,
                COUNT(*) as event_count,
                SUM(purchase_amount) as total_revenue
            FROM __THIS__
            GROUP BY user_id
        """)
        result = task.transform()

        assert result is not None
        result_df = result.to_pandas()
        assert "user_id" in result_df.columns
        assert "event_count" in result_df.columns
        assert "total_revenue" in result_df.columns

    def test_quickstart_feature_engineering_sql(self, sample_data_arrow):
        """Verify the quickstart feature engineering SQL works."""
        task = DuckDBTask()
        task.add_input(dataframe=sample_data_arrow)

        # Use the same SQL as in quickstart_duckdb.py
        user_features_sql = """
        SELECT
            user_id,
            COUNT(*) as total_events,
            COUNT(CASE WHEN action_type = 'purchase' THEN 1 END) as total_purchases,
            COUNT(CASE WHEN action_type = 'view' THEN 1 END) as total_views,
            COUNT(CASE WHEN action_type = 'add_to_cart' THEN 1 END) as total_cart_adds,
            SUM(purchase_amount) as total_revenue,
            AVG(purchase_amount) as avg_purchase_amount,
            MAX(purchase_amount) as max_purchase_amount,
            AVG(session_duration) as avg_session_duration,
            SUM(items_viewed) as total_items_viewed,
            AVG(items_viewed) as avg_items_per_session,
            AVG(cart_value) as avg_cart_value,
            MAX(cart_value) as max_cart_value,
            COUNT(CASE WHEN device_type = 'mobile' THEN 1 END) as mobile_sessions,
            COUNT(CASE WHEN device_type = 'desktop' THEN 1 END) as desktop_sessions,
            COUNT(CASE WHEN device_type = 'tablet' THEN 1 END) as tablet_sessions,
            MODE(product_category) as favorite_category,
            COUNT(DISTINCT product_category) as categories_explored,
            MIN(event_time) as first_event_time,
            MAX(event_time) as last_event_time
        FROM __THIS__
        GROUP BY user_id
        ORDER BY total_revenue DESC
        """

        task.add_sql(user_features_sql)
        result = task.transform()

        assert result is not None
        result_df = result.to_pandas()

        # Verify expected columns exist
        expected_columns = [
            "user_id", "total_events", "total_purchases", "total_views",
            "total_cart_adds", "total_revenue", "avg_purchase_amount",
            "max_purchase_amount", "avg_session_duration", "total_items_viewed",
            "avg_items_per_session", "avg_cart_value", "max_cart_value",
            "mobile_sessions", "desktop_sessions", "tablet_sessions",
            "favorite_category", "categories_explored", "first_event_time",
            "last_event_time"
        ]
        for col in expected_columns:
            assert col in result_df.columns, f"Missing column: {col}"

    def test_chained_transformations(self, sample_data_arrow):
        """Verify chained SQL transformations work."""
        task = DuckDBTask()
        task.add_input(dataframe=sample_data_arrow)
        task.add_sql("SELECT user_id, action_type, purchase_amount FROM __THIS__")
        task.add_sql("SELECT user_id, SUM(purchase_amount) as total FROM __THIS__ GROUP BY user_id")
        result = task.transform()

        assert result is not None
        result_df = result.to_pandas()
        assert "user_id" in result_df.columns
        assert "total" in result_df.columns


class TestFeatureOutputQuality:
    """Tests for feature output quality from quickstart example."""

    @pytest.fixture
    def user_features(self, sample_data_arrow):
        """Generate user features using quickstart SQL."""
        task = DuckDBTask()
        task.add_input(dataframe=sample_data_arrow)
        task.add_sql("""
            SELECT
                user_id,
                COUNT(*) as total_events,
                COUNT(CASE WHEN action_type = 'purchase' THEN 1 END) as total_purchases,
                SUM(purchase_amount) as total_revenue,
                AVG(session_duration) as avg_session_duration,
                COUNT(DISTINCT product_category) as categories_explored
            FROM __THIS__
            GROUP BY user_id
        """)
        result = task.transform()
        return result.to_pandas()

    def test_user_id_is_unique(self, user_features):
        """Verify each user_id appears exactly once in output."""
        assert user_features["user_id"].is_unique, (
            "user_id should be unique after aggregation"
        )

    def test_total_events_positive(self, user_features):
        """Verify all users have at least one event."""
        assert (user_features["total_events"] > 0).all(), (
            "All users should have at least one event"
        )

    def test_total_revenue_non_negative(self, user_features):
        """Verify revenue is non-negative."""
        assert (user_features["total_revenue"] >= 0).all(), (
            "Total revenue should be non-negative"
        )

    def test_avg_session_duration_positive(self, user_features):
        """Verify average session duration is positive."""
        assert (user_features["avg_session_duration"] > 0).all(), (
            "Average session duration should be positive"
        )

    def test_categories_explored_positive(self, user_features):
        """Verify all users explored at least one category."""
        assert (user_features["categories_explored"] > 0).all(), (
            "All users should have explored at least one category"
        )


class TestQuickstartScriptExecution:
    """Tests for running the quickstart script end-to-end."""

    @pytest.fixture
    def temp_quickstart_dir(self):
        """Create a temporary copy of quickstart directory for testing."""
        temp_dir = tempfile.mkdtemp()
        temp_quickstart = os.path.join(temp_dir, "quickstart")

        # Copy quickstart files to temp directory
        shutil.copytree(QUICKSTART_DIR, temp_quickstart)

        yield temp_quickstart

        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_quickstart_script_runs_successfully(self, temp_quickstart_dir):
        """Verify the quickstart script runs without errors."""
        script_path = os.path.join(temp_quickstart_dir, "quickstart_duckdb.py")

        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            cwd=temp_quickstart_dir,
            timeout=120,
        )

        assert result.returncode == 0, (
            f"Quickstart script failed with error:\n{result.stderr}"
        )

    def test_quickstart_creates_output_directory(self, temp_quickstart_dir):
        """Verify the quickstart script creates output directory."""
        script_path = os.path.join(temp_quickstart_dir, "quickstart_duckdb.py")
        output_dir = os.path.join(temp_quickstart_dir, "output")

        subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            cwd=temp_quickstart_dir,
            timeout=120,
        )

        assert os.path.exists(output_dir), (
            "Output directory was not created"
        )

    def test_quickstart_creates_parquet_output(self, temp_quickstart_dir):
        """Verify the quickstart script creates Parquet output."""
        script_path = os.path.join(temp_quickstart_dir, "quickstart_duckdb.py")
        parquet_path = os.path.join(temp_quickstart_dir, "output", "user_features.parquet")

        subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            cwd=temp_quickstart_dir,
            timeout=120,
        )

        assert os.path.exists(parquet_path), (
            "Parquet output file was not created"
        )

        # Verify the parquet file is readable
        df = pd.read_parquet(parquet_path)
        assert len(df) > 0, "Parquet file is empty"

    def test_quickstart_creates_csv_output(self, temp_quickstart_dir):
        """Verify the quickstart script creates CSV output."""
        script_path = os.path.join(temp_quickstart_dir, "quickstart_duckdb.py")
        csv_path = os.path.join(temp_quickstart_dir, "output", "user_features.csv")

        subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            cwd=temp_quickstart_dir,
            timeout=120,
        )

        assert os.path.exists(csv_path), (
            "CSV output file was not created"
        )

        # Verify the CSV file is readable
        df = pd.read_csv(csv_path)
        assert len(df) > 0, "CSV file is empty"

    def test_quickstart_output_has_expected_columns(self, temp_quickstart_dir):
        """Verify the output files have expected feature columns."""
        script_path = os.path.join(temp_quickstart_dir, "quickstart_duckdb.py")
        parquet_path = os.path.join(temp_quickstart_dir, "output", "user_features.parquet")

        subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            cwd=temp_quickstart_dir,
            timeout=120,
        )

        df = pd.read_parquet(parquet_path)

        # Check for key feature columns from quickstart
        expected_columns = [
            "user_id", "total_events", "total_purchases",
            "total_revenue", "favorite_category"
        ]
        for col in expected_columns:
            assert col in df.columns, f"Missing expected column: {col}"

    def test_quickstart_output_contains_users(self, temp_quickstart_dir):
        """Verify the output contains user-level aggregations."""
        script_path = os.path.join(temp_quickstart_dir, "quickstart_duckdb.py")
        parquet_path = os.path.join(temp_quickstart_dir, "output", "user_features.parquet")

        subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            cwd=temp_quickstart_dir,
            timeout=120,
        )

        df = pd.read_parquet(parquet_path)

        # Verify we have a reasonable number of users
        assert len(df) >= 10, (
            f"Expected at least 10 users, got {len(df)}"
        )

        # Verify user_id is unique
        assert df["user_id"].is_unique, (
            "user_id should be unique in aggregated output"
        )


class TestSecondaryAnalysis:
    """Tests for secondary analysis demonstrated in quickstart."""

    def test_conversion_analysis(self, sample_data_arrow):
        """Verify the conversion analysis query works."""
        # First create user features
        features_task = DuckDBTask()
        features_task.add_input(dataframe=sample_data_arrow)
        features_task.add_sql("""
            SELECT
                user_id,
                COUNT(CASE WHEN action_type = 'purchase' THEN 1 END) as total_purchases,
                SUM(purchase_amount) as total_revenue,
                AVG(session_duration) as avg_session_duration
            FROM __THIS__
            GROUP BY user_id
        """)
        features_arrow = features_task.transform()

        # Then run the analysis query from quickstart
        analysis_task = DuckDBTask()
        analysis_task.add_input(dataframe=features_arrow)
        analysis_task.add_sql("""
            SELECT
                CASE
                    WHEN total_purchases = 0 THEN 'No Purchases'
                    WHEN total_purchases = 1 THEN '1 Purchase'
                    WHEN total_purchases <= 3 THEN '2-3 Purchases'
                    ELSE '4+ Purchases'
                END as purchase_segment,
                COUNT(*) as user_count,
                ROUND(AVG(total_revenue), 2) as avg_revenue,
                ROUND(AVG(avg_session_duration), 2) as avg_session_mins
            FROM __THIS__
            GROUP BY 1
            ORDER BY user_count DESC
        """)
        result = analysis_task.transform()
        result_df = result.to_pandas()

        # Verify analysis output
        assert "purchase_segment" in result_df.columns
        assert "user_count" in result_df.columns
        assert "avg_revenue" in result_df.columns
        assert len(result_df) > 0


class TestQuickstartImports:
    """Tests for verifying quickstart imports work correctly."""

    def test_pandas_import(self):
        """Verify pandas is available."""
        import pandas
        assert pandas is not None

    def test_pyarrow_import(self):
        """Verify pyarrow is available."""
        import pyarrow
        assert pyarrow is not None

    def test_duckdb_task_import(self):
        """Verify DuckDBTask can be imported."""
        from seeknal.tasks.duckdb import DuckDBTask
        assert DuckDBTask is not None

    def test_pathlib_import(self):
        """Verify pathlib is available."""
        from pathlib import Path
        assert Path is not None
