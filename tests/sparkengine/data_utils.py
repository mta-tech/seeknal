"""Test data utilities for Spark engine tests."""

import os
import shutil
import stat
import tempfile

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType, FloatType
)
import pytest


@pytest.fixture(scope="module")
def spark_session():
    """Create a local Spark session for testing."""
    # Create secure temp directory with restricted permissions (owner only)
    warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-test_")
    os.chmod(warehouse_dir, stat.S_IRWXU)  # 0o700 - owner read/write/execute only

    # Get the project root directory (src/seeknal parent)
    from pathlib import Path
    project_root = Path(__file__).parent.parent.parent.resolve()
    src_dir = str(project_root / "src")

    # Set PYTHONPATH so Spark workers can find seeknal module
    old_pythonpath = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = f"{src_dir}:{old_pythonpath}"

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.python.worker.reuse", "false") \
        .getOrCreate()

    # Also add to sys.path for the driver process
    import sys
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    yield spark
    spark.stop()
    spark._jvm.System.clearProperty("spark.driver.port")

    # Cleanup temp directory
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)


def create_sample_dataframe(spark: SparkSession, rows: int = 100) -> DataFrame:
    """Create a sample DataFrame with various data types.

    Args:
        spark: SparkSession
        rows: Number of rows to generate

    Returns:
        DataFrame with columns: id, name, value, timestamp, score
    """
    data = [
        (i, f"user_{i}", i * 10.5, f"2024-01-{i % 28 + 1:02d}", 0.1 + (i % 100) / 100)
        for i in range(rows)
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("timestamp", StringType(), False),
        StructField("score", FloatType(), False),
    ])

    return spark.createDataFrame(data, schema)


def create_null_dataframe(spark: SparkSession) -> DataFrame:
    """Create a DataFrame with null values for edge case testing.

    Args:
        spark: SparkSession

    Returns:
        DataFrame with various null combinations
    """
    data = [
        (1, "a", 10.0, None),
        (2, None, 20.0, 0.5),
        (3, "c", None, 0.7),
        (4, "d", 40.0, 0.9),
        (5, None, None, None),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("score", FloatType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_join_left_dataframe(spark: SparkSession) -> DataFrame:
    """Create left DataFrame for join testing."""
    data = [(1, "alice"), (2, "bob"), (3, "charlie"), (4, "david")]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)


def create_join_right_dataframe(spark: SparkSession) -> DataFrame:
    """Create right DataFrame for join testing."""
    data = [(1, 100), (2, 200), (3, 300), (5, 500)]
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("score", IntegerType(), False),
    ])
    return spark.createDataFrame(data, schema)
