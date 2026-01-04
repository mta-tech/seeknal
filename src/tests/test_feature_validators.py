"""
Unit tests for the feature validation framework.

This module contains comprehensive tests for all validator classes and the
ValidationRunner in the seeknal.feature_validation module.
"""

from datetime import datetime, timedelta

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from seeknal.feature_validation.models import (
    ValidationMode,
    ValidationResult,
    ValidationSummary,
)
from seeknal.feature_validation.validators import (
    BaseValidator,
    CustomValidator,
    FreshnessValidator,
    NullValidator,
    RangeValidator,
    UniquenessValidator,
    ValidationException,
    ValidationRunner,
)


# =============================================================================
# Test Data Helpers
# =============================================================================


def create_test_df_with_nulls(spark, null_percentage: float = 0.2) -> DataFrame:
    """
    Create a test DataFrame with a specified percentage of null values.

    Args:
        spark: SparkSession fixture.
        null_percentage: Percentage of rows to have null values (0.0 to 1.0).

    Returns:
        DataFrame with 'id', 'name', and 'value' columns where some rows have nulls.
    """
    # Create 10 rows total
    total_rows = 10
    null_count = int(total_rows * null_percentage)
    non_null_count = total_rows - null_count

    data = []
    # Add non-null rows
    for i in range(non_null_count):
        data.append((i + 1, f"name_{i}", float(i * 10)))

    # Add rows with null values
    for i in range(null_count):
        data.append((non_null_count + i + 1, None, None))

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_test_df_with_range_violations(spark, out_of_range_count: int = 2) -> DataFrame:
    """
    Create a test DataFrame with values outside a range.

    Args:
        spark: SparkSession fixture.
        out_of_range_count: Number of rows with out-of-range values.

    Returns:
        DataFrame with 'id' and 'age' columns where some rows have invalid ages.
    """
    data = [
        (1, 25),   # valid
        (2, 30),   # valid
        (3, 45),   # valid
        (4, 50),   # valid
        (5, 55),   # valid
        (6, 60),   # valid
        (7, 65),   # valid
        (8, 70),   # valid
    ]

    # Add out-of-range values
    for i in range(out_of_range_count):
        if i % 2 == 0:
            data.append((len(data) + 1, -5))  # below minimum
        else:
            data.append((len(data) + 1, 150))  # above maximum

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("age", IntegerType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_test_df_with_duplicates(spark, duplicate_count: int = 2) -> DataFrame:
    """
    Create a test DataFrame with duplicate values.

    Args:
        spark: SparkSession fixture.
        duplicate_count: Number of duplicate rows to add.

    Returns:
        DataFrame with 'user_id', 'timestamp', and 'value' columns.
    """
    data = [
        ("user1", "2024-01-01", 100),
        ("user2", "2024-01-01", 200),
        ("user3", "2024-01-01", 300),
        ("user4", "2024-01-01", 400),
    ]

    # Add duplicates
    for i in range(duplicate_count):
        data.append(("user1", "2024-01-01", 100))  # duplicate of first row

    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("value", IntegerType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_test_df_with_timestamps(
    spark,
    fresh_count: int = 5,
    stale_count: int = 2,
    reference_time: datetime = None,
    max_age_hours: int = 24,
) -> DataFrame:
    """
    Create a test DataFrame with timestamp values for freshness testing.

    Args:
        spark: SparkSession fixture.
        fresh_count: Number of rows with fresh timestamps.
        stale_count: Number of rows with stale timestamps.
        reference_time: Reference time for freshness calculation.
        max_age_hours: Maximum age in hours for "fresh" data.

    Returns:
        DataFrame with 'id' and 'event_time' columns.
    """
    if reference_time is None:
        reference_time = datetime.now()

    data = []

    # Add fresh timestamps (within max_age)
    for i in range(fresh_count):
        fresh_time = reference_time - timedelta(hours=i)
        data.append((i + 1, fresh_time))

    # Add stale timestamps (older than max_age)
    for i in range(stale_count):
        stale_time = reference_time - timedelta(hours=max_age_hours + 48 + i)
        data.append((fresh_count + i + 1, stale_time))

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("event_time", TimestampType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_empty_df(spark) -> DataFrame:
    """
    Create an empty test DataFrame.

    Args:
        spark: SparkSession fixture.

    Returns:
        Empty DataFrame with 'id', 'name', and 'value' columns.
    """
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
    ])

    return spark.createDataFrame([], schema)


# =============================================================================
# NullValidator Tests
# =============================================================================


def test_null_validator_detects_nulls(spark):
    """Test that NullValidator correctly identifies null values in columns."""
    # Create DataFrame with null values
    df = create_test_df_with_nulls(spark, null_percentage=0.2)

    # Create validator for columns with nulls (strict mode - no nulls allowed)
    validator = NullValidator(columns=["name", "value"], max_null_percentage=0.0)

    # Validate
    result = validator.validate(df)

    # Assert validation failed due to nulls
    assert result.passed is False
    assert result.failure_count > 0
    assert "NullValidator" in result.validator_name
    assert result.details is not None
    assert "column_null_counts" in result.details
