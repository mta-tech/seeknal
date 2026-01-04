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


class TestNullValidator:
    """Comprehensive tests for the NullValidator class."""

    # -------------------------------------------------------------------------
    # Basic Null Detection Tests
    # -------------------------------------------------------------------------

    def test_detects_nulls_in_single_column(self, spark):
        """Test that NullValidator correctly identifies null values in a single column."""
        # Create DataFrame with null values
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator for a single column with nulls (strict mode - no nulls allowed)
        validator = NullValidator(columns=["name"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed due to nulls
        assert result.passed is False
        assert result.failure_count > 0
        assert result.validator_name == "NullValidator"
        assert result.details is not None
        assert "column_null_counts" in result.details
        assert "name" in result.details["column_null_counts"]

    def test_detects_nulls_in_multiple_columns(self, spark):
        """Test that NullValidator correctly identifies null values in multiple columns."""
        # Create DataFrame with null values
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator for multiple columns with nulls (strict mode - no nulls allowed)
        validator = NullValidator(columns=["name", "value"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed due to nulls
        assert result.passed is False
        assert result.failure_count > 0
        assert "NullValidator" in result.validator_name
        assert result.details is not None
        assert "column_null_counts" in result.details
        # Check both columns are reported
        assert "name" in result.details["column_null_counts"]
        assert "value" in result.details["column_null_counts"]

    def test_passes_when_no_nulls(self, spark):
        """Test that NullValidator passes when there are no null values."""
        # Create DataFrame with no nulls
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        # Create validator for columns (strict mode - no nulls allowed)
        validator = NullValidator(columns=["name", "value"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert result.validator_name == "NullValidator"
        assert "passed" in result.message.lower()

    def test_detects_nulls_only_in_specified_columns(self, spark):
        """Test that NullValidator only checks specified columns."""
        # Create DataFrame with nulls in 'name' and 'value' columns, but not 'id'
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator for 'id' column only (which has no nulls)
        validator = NullValidator(columns=["id"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (id column has no nulls)
        assert result.passed is True
        assert result.failure_count == 0

    # -------------------------------------------------------------------------
    # Threshold Tests
    # -------------------------------------------------------------------------

    def test_passes_with_nulls_below_threshold(self, spark):
        """Test that NullValidator passes when null percentage is below the threshold."""
        # Create DataFrame with 20% nulls
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator with threshold higher than actual null percentage
        validator = NullValidator(columns=["name"], max_null_percentage=0.3)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (20% nulls is within 30% threshold)
        assert result.passed is True

    def test_fails_with_nulls_above_threshold(self, spark):
        """Test that NullValidator fails when null percentage exceeds threshold."""
        # Create DataFrame with 30% nulls
        df = create_test_df_with_nulls(spark, null_percentage=0.3)

        # Create validator with threshold lower than actual null percentage
        validator = NullValidator(columns=["name"], max_null_percentage=0.2)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (30% nulls exceeds 20% threshold)
        assert result.passed is False
        assert "columns_exceeding_threshold" in result.details
        assert "name" in result.details["columns_exceeding_threshold"]

    def test_passes_with_nulls_exactly_at_threshold(self, spark):
        """Test that NullValidator passes when null percentage is exactly at threshold."""
        # Create DataFrame with exactly 20% nulls
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator with threshold exactly at null percentage
        validator = NullValidator(columns=["name"], max_null_percentage=0.2)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (exactly at threshold)
        assert result.passed is True

    def test_threshold_zero_is_strict_mode(self, spark):
        """Test that a threshold of 0.0 enforces strict mode (no nulls allowed)."""
        # Create DataFrame with minimal nulls (10%)
        df = create_test_df_with_nulls(spark, null_percentage=0.1)

        # Create validator with strict mode (threshold = 0.0)
        validator = NullValidator(columns=["name"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (any nulls fail strict mode)
        assert result.passed is False

    def test_threshold_100_percent_allows_all_nulls(self, spark):
        """Test that a threshold of 1.0 allows 100% nulls."""
        # Create DataFrame with all nulls
        df = create_test_df_with_nulls(spark, null_percentage=1.0)

        # Create validator allowing 100% nulls
        validator = NullValidator(columns=["name"], max_null_percentage=1.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True

    def test_per_column_threshold_evaluation(self, spark):
        """Test that each column is evaluated independently against the threshold."""
        # Create custom DataFrame where one column exceeds threshold
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("col_a", StringType(), True),
            StructField("col_b", StringType(), True),
        ])
        # col_a has 10% nulls (1/10), col_b has 30% nulls (3/10)
        data = [
            (1, "a", "b"),
            (2, "a", "b"),
            (3, "a", "b"),
            (4, "a", None),
            (5, "a", None),
            (6, "a", None),
            (7, "a", "b"),
            (8, "a", "b"),
            (9, "a", "b"),
            (10, None, "b"),
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with 20% threshold
        validator = NullValidator(columns=["col_a", "col_b"], max_null_percentage=0.2)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (col_b exceeds threshold)
        assert result.passed is False
        assert "col_b" in result.details["columns_exceeding_threshold"]
        # col_a should not be in exceeding list (10% < 20%)
        assert "col_a" not in result.details["columns_exceeding_threshold"]

    # -------------------------------------------------------------------------
    # Edge Cases
    # -------------------------------------------------------------------------

    def test_empty_dataframe(self, spark):
        """Test that NullValidator handles empty DataFrames gracefully."""
        # Create empty DataFrame
        df = create_empty_df(spark)

        # Create validator
        validator = NullValidator(columns=["name", "value"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (empty DataFrame has no nulls by definition)
        assert result.passed is True
        assert result.total_count == 0
        assert "empty" in result.message.lower()

    def test_all_null_column(self, spark):
        """Test NullValidator with a column containing all null values."""
        # Create DataFrame with all nulls
        df = create_test_df_with_nulls(spark, null_percentage=1.0)

        # Create validator with strict mode
        validator = NullValidator(columns=["name"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.details["column_null_percentages"]["name"] == 1.0

    def test_missing_column_raises_error(self, spark):
        """Test that NullValidator raises an error for missing columns."""
        # Create DataFrame
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator with a non-existent column
        validator = NullValidator(columns=["nonexistent_column"], max_null_percentage=0.0)

        # Assert that validation raises ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate(df)

        assert "not found" in str(exc_info.value).lower()

    def test_partial_missing_columns(self, spark):
        """Test that NullValidator raises error when some columns are missing."""
        # Create DataFrame
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator with one valid and one invalid column
        validator = NullValidator(columns=["name", "nonexistent"], max_null_percentage=0.0)

        # Assert that validation raises ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate(df)

        assert "nonexistent" in str(exc_info.value)

    def test_single_row_with_null(self, spark):
        """Test NullValidator with a single row containing a null."""
        # Create single-row DataFrame with null
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        df = spark.createDataFrame([(1, None)], schema)

        # Create validator
        validator = NullValidator(columns=["name"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1
        assert result.total_count == 1

    def test_single_row_without_null(self, spark):
        """Test NullValidator with a single row without null."""
        # Create single-row DataFrame without null
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        df = spark.createDataFrame([(1, "test")], schema)

        # Create validator
        validator = NullValidator(columns=["name"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert result.total_count == 1

    def test_large_null_count(self, spark):
        """Test NullValidator with a larger dataset and high null percentage."""
        # Create DataFrame with many rows
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ])
        # Create 100 rows, 80% with nulls
        data = [(i, None if i < 80 else f"val_{i}") for i in range(100)]
        df = spark.createDataFrame(data, schema)

        # Create validator with 50% threshold
        validator = NullValidator(columns=["value"], max_null_percentage=0.5)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (80% > 50%)
        assert result.passed is False
        assert result.details["column_null_counts"]["value"] == 80
        assert result.details["column_null_percentages"]["value"] == 0.8

    # -------------------------------------------------------------------------
    # Input Validation Tests
    # -------------------------------------------------------------------------

    def test_empty_columns_list_raises_error(self, spark):
        """Test that NullValidator raises error when columns list is empty."""
        with pytest.raises(ValueError) as exc_info:
            NullValidator(columns=[], max_null_percentage=0.0)

        assert "empty" in str(exc_info.value).lower()

    def test_invalid_threshold_negative_raises_error(self, spark):
        """Test that NullValidator raises error for negative threshold."""
        with pytest.raises(ValueError) as exc_info:
            NullValidator(columns=["col"], max_null_percentage=-0.1)

        assert "max_null_percentage" in str(exc_info.value)

    def test_invalid_threshold_above_one_raises_error(self, spark):
        """Test that NullValidator raises error for threshold above 1.0."""
        with pytest.raises(ValueError) as exc_info:
            NullValidator(columns=["col"], max_null_percentage=1.5)

        assert "max_null_percentage" in str(exc_info.value)

    def test_boundary_threshold_values(self, spark):
        """Test that NullValidator accepts boundary threshold values (0.0 and 1.0)."""
        # Should not raise
        validator_zero = NullValidator(columns=["col"], max_null_percentage=0.0)
        validator_one = NullValidator(columns=["col"], max_null_percentage=1.0)

        assert validator_zero.max_null_percentage == 0.0
        assert validator_one.max_null_percentage == 1.0

    # -------------------------------------------------------------------------
    # Result Details Tests
    # -------------------------------------------------------------------------

    def test_result_contains_all_expected_fields(self, spark):
        """Test that the validation result contains all expected fields."""
        # Create DataFrame with nulls
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator
        validator = NullValidator(columns=["name", "value"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Check result has all expected fields
        assert result.validator_name == "NullValidator"
        assert isinstance(result.passed, bool)
        assert isinstance(result.failure_count, int)
        assert isinstance(result.total_count, int)
        assert isinstance(result.message, str)
        assert isinstance(result.details, dict)

        # Check details has all expected keys
        assert "columns" in result.details
        assert "max_null_percentage" in result.details
        assert "column_null_counts" in result.details
        assert "column_null_percentages" in result.details
        assert "columns_exceeding_threshold" in result.details

    def test_result_message_clarity_on_pass(self, spark):
        """Test that the validation message is clear on pass."""
        # Create DataFrame without nulls
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        # Create validator
        validator = NullValidator(columns=["name"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is True
        assert "passed" in result.message.lower()
        assert "name" in result.message or str(["name"]) in result.message

    def test_result_message_clarity_on_fail(self, spark):
        """Test that the validation message is clear on failure."""
        # Create DataFrame with nulls
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Create validator
        validator = NullValidator(columns=["name"], max_null_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is False
        assert "failed" in result.message.lower()

    def test_validator_name_property(self, spark):
        """Test that the validator name property works correctly."""
        validator = NullValidator(columns=["col"], max_null_percentage=0.0)
        assert validator.name == "NullValidator"


# Legacy test for backward compatibility
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
