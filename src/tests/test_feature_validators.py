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


# =============================================================================
# RangeValidator Tests
# =============================================================================


class TestRangeValidator:
    """Comprehensive tests for the RangeValidator class."""

    # -------------------------------------------------------------------------
    # Basic Range Detection Tests
    # -------------------------------------------------------------------------

    def test_detects_values_below_minimum(self, spark):
        """Test that RangeValidator correctly identifies values below minimum."""
        # Create DataFrame with out-of-range values (1 below min)
        df = create_test_df_with_range_violations(spark, out_of_range_count=1)

        # Create validator with range 0-100
        validator = RangeValidator(column="age", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation failed due to values below minimum
        assert result.passed is False
        assert result.failure_count > 0
        assert result.validator_name == "RangeValidator"
        assert result.details is not None
        assert "out_of_range_count" in result.details
        assert result.details["out_of_range_count"] > 0

    def test_detects_values_above_maximum(self, spark):
        """Test that RangeValidator correctly identifies values above maximum."""
        # Create DataFrame with out-of-range values (2 = 1 below + 1 above)
        df = create_test_df_with_range_violations(spark, out_of_range_count=2)

        # Create validator with range 0-100
        validator = RangeValidator(column="age", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation failed due to values above maximum
        assert result.passed is False
        assert result.failure_count == 2
        assert "failed" in result.message.lower()

    def test_passes_when_all_values_in_range(self, spark):
        """Test that RangeValidator passes when all values are within range."""
        # Create DataFrame with no out-of-range values
        df = create_test_df_with_range_violations(spark, out_of_range_count=0)

        # Create validator with range 0-100
        validator = RangeValidator(column="age", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert result.validator_name == "RangeValidator"
        assert "passed" in result.message.lower()

    def test_values_at_exact_minimum_boundary(self, spark):
        """Test that values exactly at minimum are considered valid (inclusive)."""
        # Create DataFrame with value exactly at minimum
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, 0), (2, 50), (3, 100)]  # 0 is exactly at min boundary
        df = spark.createDataFrame(data, schema)

        # Create validator with range 0-100
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (boundary values are inclusive)
        assert result.passed is True
        assert result.failure_count == 0

    def test_values_at_exact_maximum_boundary(self, spark):
        """Test that values exactly at maximum are considered valid (inclusive)."""
        # Create DataFrame with value exactly at maximum
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, 0), (2, 50), (3, 100)]  # 100 is exactly at max boundary
        df = spark.createDataFrame(data, schema)

        # Create validator with range 0-100
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (boundary values are inclusive)
        assert result.passed is True
        assert result.failure_count == 0

    def test_values_just_outside_minimum_boundary(self, spark):
        """Test that values just below minimum are detected."""
        # Create DataFrame with value just below minimum
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, -1), (2, 50), (3, 100)]  # -1 is just below min of 0
        df = spark.createDataFrame(data, schema)

        # Create validator with range 0-100
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1

    def test_values_just_outside_maximum_boundary(self, spark):
        """Test that values just above maximum are detected."""
        # Create DataFrame with value just above maximum
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, 0), (2, 50), (3, 101)]  # 101 is just above max of 100
        df = spark.createDataFrame(data, schema)

        # Create validator with range 0-100
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1

    # -------------------------------------------------------------------------
    # Minimum-Only and Maximum-Only Bound Tests
    # -------------------------------------------------------------------------

    def test_only_min_val_specified_passes(self, spark):
        """Test that validation passes when only min_val is specified and all values are valid."""
        # Create DataFrame with all positive values
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, 0), (2, 50), (3, 100), (4, 1000)]  # All >= 0
        df = spark.createDataFrame(data, schema)

        # Create validator with only min_val
        validator = RangeValidator(column="value", min_val=0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert ">= 0" in result.message

    def test_only_min_val_specified_fails(self, spark):
        """Test that validation fails when only min_val is specified and some values are invalid."""
        # Create DataFrame with some negative values
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, -5), (2, 50), (3, 100)]  # -5 is below min of 0
        df = spark.createDataFrame(data, schema)

        # Create validator with only min_val
        validator = RangeValidator(column="value", min_val=0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1
        assert result.details["min_val"] == 0
        assert result.details["max_val"] is None

    def test_only_max_val_specified_passes(self, spark):
        """Test that validation passes when only max_val is specified and all values are valid."""
        # Create DataFrame with all values <= 100
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, -100), (2, 0), (3, 50), (4, 100)]  # All <= 100
        df = spark.createDataFrame(data, schema)

        # Create validator with only max_val
        validator = RangeValidator(column="value", max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert "<= 100" in result.message

    def test_only_max_val_specified_fails(self, spark):
        """Test that validation fails when only max_val is specified and some values are invalid."""
        # Create DataFrame with some values above max
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, 0), (2, 50), (3, 150)]  # 150 is above max of 100
        df = spark.createDataFrame(data, schema)

        # Create validator with only max_val
        validator = RangeValidator(column="value", max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1
        assert result.details["min_val"] is None
        assert result.details["max_val"] == 100

    # -------------------------------------------------------------------------
    # Data Type Tests
    # -------------------------------------------------------------------------

    def test_with_float_values(self, spark):
        """Test RangeValidator with float/double values."""
        # Create DataFrame with float values
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("price", DoubleType(), True),
        ])
        data = [(1, 0.0), (2, 49.99), (3, 100.0), (4, 100.01)]  # 100.01 is above max
        df = spark.createDataFrame(data, schema)

        # Create validator with range 0.0-100.0
        validator = RangeValidator(column="price", min_val=0.0, max_val=100.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed due to 100.01
        assert result.passed is False
        assert result.failure_count == 1

    def test_with_negative_range(self, spark):
        """Test RangeValidator with negative range values."""
        # Create DataFrame with values in negative range
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("temperature", IntegerType(), True),
        ])
        data = [(1, -50), (2, -25), (3, 0), (4, -100)]  # -100 is below min of -50
        df = spark.createDataFrame(data, schema)

        # Create validator with negative range
        validator = RangeValidator(column="temperature", min_val=-50, max_val=0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1

    def test_with_floating_point_precision(self, spark):
        """Test RangeValidator handles floating point precision correctly."""
        # Create DataFrame with precise float values
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", DoubleType(), True),
        ])
        data = [(1, 0.1 + 0.2), (2, 0.3)]  # Both should be ~0.3
        df = spark.createDataFrame(data, schema)

        # Create validator with range that includes 0.3
        validator = RangeValidator(column="value", min_val=0.0, max_val=0.5)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (both values are within range)
        assert result.passed is True

    # -------------------------------------------------------------------------
    # Null Value Handling Tests
    # -------------------------------------------------------------------------

    def test_null_values_excluded_from_check(self, spark):
        """Test that null values are excluded from range check."""
        # Create DataFrame with null values
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, 50), (2, None), (3, 75)]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (null values are excluded)
        assert result.passed is True
        assert result.details["null_count"] == 1
        assert result.details["non_null_count"] == 2

    def test_all_null_column_passes(self, spark):
        """Test that column with all null values passes validation."""
        # Create DataFrame with all null values
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, None), (2, None), (3, None)]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (no non-null values to check)
        assert result.passed is True
        assert result.details["null_count"] == 3
        assert result.details["non_null_count"] == 0

    def test_null_values_with_out_of_range(self, spark):
        """Test that null values don't affect detection of out-of-range values."""
        # Create DataFrame with nulls and out-of-range values
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, None), (2, 150), (3, None), (4, 50)]  # 150 is out of range
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (150 is out of range)
        assert result.passed is False
        assert result.failure_count == 1
        assert result.details["null_count"] == 2
        assert result.details["non_null_count"] == 2

    # -------------------------------------------------------------------------
    # Edge Cases
    # -------------------------------------------------------------------------

    def test_empty_dataframe(self, spark):
        """Test that RangeValidator handles empty DataFrames gracefully."""
        # Create empty DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        df = spark.createDataFrame([], schema)

        # Create validator
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (empty DataFrame)
        assert result.passed is True
        assert result.total_count == 0
        assert "empty" in result.message.lower()

    def test_missing_column_raises_error(self, spark):
        """Test that RangeValidator raises an error for missing columns."""
        # Create DataFrame
        df = create_test_df_with_range_violations(spark, out_of_range_count=0)

        # Create validator with a non-existent column
        validator = RangeValidator(column="nonexistent_column", min_val=0, max_val=100)

        # Assert that validation raises ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate(df)

        assert "not found" in str(exc_info.value).lower()

    def test_single_row_in_range(self, spark):
        """Test RangeValidator with a single row in range."""
        # Create single-row DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        df = spark.createDataFrame([(1, 50)], schema)

        # Create validator
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.total_count == 1
        assert result.failure_count == 0

    def test_single_row_out_of_range(self, spark):
        """Test RangeValidator with a single row out of range."""
        # Create single-row DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        df = spark.createDataFrame([(1, 150)], schema)

        # Create validator
        validator = RangeValidator(column="value", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.total_count == 1
        assert result.failure_count == 1

    def test_same_min_and_max_value(self, spark):
        """Test RangeValidator when min equals max (only one valid value)."""
        # Create DataFrame with exact match and non-match
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, 50), (2, 50), (3, 51)]  # 51 is out of range
        df = spark.createDataFrame(data, schema)

        # Create validator with min == max
        validator = RangeValidator(column="value", min_val=50, max_val=50)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (51 is not exactly 50)
        assert result.passed is False
        assert result.failure_count == 1

    def test_same_min_and_max_value_all_match(self, spark):
        """Test RangeValidator passes when all values equal the single allowed value."""
        # Create DataFrame where all values match the single allowed value
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [(1, 50), (2, 50), (3, 50)]
        df = spark.createDataFrame(data, schema)

        # Create validator with min == max
        validator = RangeValidator(column="value", min_val=50, max_val=50)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0

    def test_large_dataset(self, spark):
        """Test RangeValidator with a larger dataset."""
        # Create DataFrame with many rows
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        # 100 rows, 10 out of range (values 91-100 are out of range for max=90)
        data = [(i, i) for i in range(1, 101)]  # Values 1-100
        df = spark.createDataFrame(data, schema)

        # Create validator with range 1-90
        validator = RangeValidator(column="value", min_val=1, max_val=90)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (10 values > 90)
        assert result.passed is False
        assert result.failure_count == 10
        assert result.total_count == 100

    # -------------------------------------------------------------------------
    # Input Validation Tests
    # -------------------------------------------------------------------------

    def test_empty_column_raises_error(self, spark):
        """Test that RangeValidator raises error when column is empty."""
        with pytest.raises(ValueError) as exc_info:
            RangeValidator(column="", min_val=0, max_val=100)

        assert "empty" in str(exc_info.value).lower()

    def test_both_bounds_none_raises_error(self, spark):
        """Test that RangeValidator raises error when both min and max are None."""
        with pytest.raises(ValueError) as exc_info:
            RangeValidator(column="value", min_val=None, max_val=None)

        assert "at least one" in str(exc_info.value).lower()

    def test_min_greater_than_max_raises_error(self, spark):
        """Test that RangeValidator raises error when min > max."""
        with pytest.raises(ValueError) as exc_info:
            RangeValidator(column="value", min_val=100, max_val=50)

        assert "cannot be greater than" in str(exc_info.value).lower()

    def test_min_equal_max_is_valid(self, spark):
        """Test that RangeValidator accepts min == max."""
        # Should not raise
        validator = RangeValidator(column="value", min_val=50, max_val=50)
        assert validator.min_val == 50
        assert validator.max_val == 50

    # -------------------------------------------------------------------------
    # Result Details Tests
    # -------------------------------------------------------------------------

    def test_result_contains_all_expected_fields(self, spark):
        """Test that the validation result contains all expected fields."""
        # Create DataFrame with out-of-range values
        df = create_test_df_with_range_violations(spark, out_of_range_count=2)

        # Create validator
        validator = RangeValidator(column="age", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Check result has all expected fields
        assert result.validator_name == "RangeValidator"
        assert isinstance(result.passed, bool)
        assert isinstance(result.failure_count, int)
        assert isinstance(result.total_count, int)
        assert isinstance(result.message, str)
        assert isinstance(result.details, dict)

        # Check details has all expected keys
        assert "column" in result.details
        assert "min_val" in result.details
        assert "max_val" in result.details
        assert "out_of_range_count" in result.details
        assert "null_count" in result.details
        assert "non_null_count" in result.details

    def test_result_message_clarity_on_pass(self, spark):
        """Test that the validation message is clear on pass."""
        # Create DataFrame without out-of-range values
        df = create_test_df_with_range_violations(spark, out_of_range_count=0)

        # Create validator
        validator = RangeValidator(column="age", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is True
        assert "passed" in result.message.lower()
        assert "age" in result.message
        assert "[0, 100]" in result.message

    def test_result_message_clarity_on_fail(self, spark):
        """Test that the validation message is clear on failure."""
        # Create DataFrame with out-of-range values
        df = create_test_df_with_range_violations(spark, out_of_range_count=2)

        # Create validator
        validator = RangeValidator(column="age", min_val=0, max_val=100)

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is False
        assert "failed" in result.message.lower()
        assert "age" in result.message

    def test_validator_name_property(self, spark):
        """Test that the validator name property works correctly."""
        validator = RangeValidator(column="col", min_val=0, max_val=100)
        assert validator.name == "RangeValidator"

    def test_column_property(self, spark):
        """Test that the column property is set correctly."""
        validator = RangeValidator(column="test_col", min_val=0, max_val=100)
        assert validator.column == "test_col"

    def test_min_val_property(self, spark):
        """Test that the min_val property is set correctly."""
        validator = RangeValidator(column="col", min_val=10, max_val=100)
        assert validator.min_val == 10

    def test_max_val_property(self, spark):
        """Test that the max_val property is set correctly."""
        validator = RangeValidator(column="col", min_val=0, max_val=99)
        assert validator.max_val == 99


# Legacy test for backward compatibility
def test_range_validator_detects_violations(spark):
    """Test that RangeValidator correctly identifies out-of-range values."""
    # Create DataFrame with out-of-range values
    df = create_test_df_with_range_violations(spark, out_of_range_count=2)

    # Create validator with range 0-100
    validator = RangeValidator(column="age", min_val=0, max_val=100)

    # Validate
    result = validator.validate(df)

    # Assert validation failed due to out-of-range values
    assert result.passed is False
    assert result.failure_count > 0
    assert "RangeValidator" in result.validator_name
    assert result.details is not None
    assert "out_of_range_count" in result.details


# =============================================================================
# UniquenessValidator Tests
# =============================================================================


class TestUniquenessValidator:
    """Comprehensive tests for the UniquenessValidator class."""

    # -------------------------------------------------------------------------
    # Basic Duplicate Detection Tests (Single Column)
    # -------------------------------------------------------------------------

    def test_detects_duplicates_in_single_column(self, spark):
        """Test that UniquenessValidator correctly identifies duplicates in a single column."""
        # Create DataFrame with duplicates
        df = create_test_df_with_duplicates(spark, duplicate_count=2)

        # Create validator for a single column (strict mode - no duplicates allowed)
        validator = UniquenessValidator(columns=["user_id"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed due to duplicates
        assert result.passed is False
        assert result.failure_count > 0
        assert result.validator_name == "UniquenessValidator"
        assert result.details is not None
        assert "duplicate_row_count" in result.details
        assert result.details["duplicate_row_count"] > 0

    def test_passes_when_no_duplicates_single_column(self, spark):
        """Test that UniquenessValidator passes when there are no duplicates in a single column."""
        # Create DataFrame without duplicates
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        data = [(1, "alice"), (2, "bob"), (3, "charlie")]
        df = spark.createDataFrame(data, schema)

        # Create validator for single column (strict mode)
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert result.validator_name == "UniquenessValidator"
        assert "passed" in result.message.lower()
        assert result.details["duplicate_row_count"] == 0

    def test_detects_all_duplicates_single_column(self, spark):
        """Test that UniquenessValidator correctly counts all duplicate rows."""
        # Create DataFrame where all rows have the same value
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("category", StringType(), True),
        ])
        data = [(1, "A"), (2, "A"), (3, "A"), (4, "A"), (5, "A")]  # 5 rows, 1 unique = 4 duplicates
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = UniquenessValidator(columns=["category"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 4  # 5 - 1 = 4 duplicate rows
        assert result.details["unique_count"] == 1
        assert result.details["duplicate_row_count"] == 4

    # -------------------------------------------------------------------------
    # Multi-Column (Composite Key) Tests
    # -------------------------------------------------------------------------

    def test_detects_duplicates_in_multiple_columns(self, spark):
        """Test that UniquenessValidator correctly identifies duplicates in multiple columns."""
        # Create DataFrame with duplicates on composite key
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [
            ("user1", "2024-01-01", 100),
            ("user1", "2024-01-02", 200),  # unique combination
            ("user2", "2024-01-01", 300),
            ("user1", "2024-01-01", 150),  # duplicate of first row (same user_id + timestamp)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator for composite key (user_id + timestamp)
        validator = UniquenessValidator(
            columns=["user_id", "timestamp"],
            max_duplicate_percentage=0.0
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed due to duplicates
        assert result.passed is False
        assert result.failure_count == 1  # 4 total rows, 3 unique combinations = 1 duplicate
        assert result.details["columns"] == ["user_id", "timestamp"]
        assert result.details["unique_count"] == 3
        assert result.details["duplicate_row_count"] == 1

    def test_passes_when_no_duplicates_multi_column(self, spark):
        """Test that UniquenessValidator passes when composite key is unique."""
        # Create DataFrame where composite key is unique
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("value", IntegerType(), True),
        ])
        data = [
            ("user1", "2024-01-01", 100),
            ("user1", "2024-01-02", 200),
            ("user2", "2024-01-01", 300),
            ("user2", "2024-01-02", 400),
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator for composite key
        validator = UniquenessValidator(
            columns=["user_id", "timestamp"],
            max_duplicate_percentage=0.0
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert result.details["unique_count"] == 4
        assert result.details["duplicate_row_count"] == 0
        assert "columns" in result.message

    def test_single_column_unique_but_composite_has_duplicates(self, spark):
        """Test that single column can be unique while composite key has duplicates."""
        # Create DataFrame where each column alone is unique, but combination isn't
        schema = StructType([
            StructField("col_a", StringType(), True),
            StructField("col_b", StringType(), True),
        ])
        data = [
            ("A", "1"),
            ("B", "2"),
            ("A", "1"),  # duplicate combination of first row
        ]
        df = spark.createDataFrame(data, schema)

        # Check single column - should fail
        validator_single = UniquenessValidator(columns=["col_a"], max_duplicate_percentage=0.0)
        result_single = validator_single.validate(df)
        assert result_single.passed is False  # "A" appears twice

        # Check composite - should also fail
        validator_composite = UniquenessValidator(
            columns=["col_a", "col_b"],
            max_duplicate_percentage=0.0
        )
        result_composite = validator_composite.validate(df)
        assert result_composite.passed is False

    def test_composite_key_unique_but_single_columns_have_duplicates(self, spark):
        """Test that composite key can be unique while individual columns have duplicates."""
        # Create DataFrame where individual columns have duplicates but combination is unique
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("date", StringType(), True),
        ])
        data = [
            ("user1", "2024-01-01"),
            ("user1", "2024-01-02"),  # user1 appears twice but dates differ
            ("user2", "2024-01-01"),  # date appears twice but users differ
        ]
        df = spark.createDataFrame(data, schema)

        # Single column check for user_id - should fail (duplicates)
        validator_user = UniquenessValidator(columns=["user_id"], max_duplicate_percentage=0.0)
        result_user = validator_user.validate(df)
        assert result_user.passed is False

        # Single column check for date - should fail (duplicates)
        validator_date = UniquenessValidator(columns=["date"], max_duplicate_percentage=0.0)
        result_date = validator_date.validate(df)
        assert result_date.passed is False

        # Composite key check - should pass (all combinations unique)
        validator_composite = UniquenessValidator(
            columns=["user_id", "date"],
            max_duplicate_percentage=0.0
        )
        result_composite = validator_composite.validate(df)
        assert result_composite.passed is True
        assert result_composite.failure_count == 0

    def test_three_column_composite_key(self, spark):
        """Test UniquenessValidator with a three-column composite key."""
        # Create DataFrame with three-column composite key
        schema = StructType([
            StructField("region", StringType(), True),
            StructField("product", StringType(), True),
            StructField("date", StringType(), True),
            StructField("sales", IntegerType(), True),
        ])
        data = [
            ("US", "A", "2024-01-01", 100),
            ("US", "A", "2024-01-02", 150),
            ("US", "B", "2024-01-01", 200),
            ("EU", "A", "2024-01-01", 300),
            ("US", "A", "2024-01-01", 120),  # duplicate of first row
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator for three-column composite key
        validator = UniquenessValidator(
            columns=["region", "product", "date"],
            max_duplicate_percentage=0.0
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1  # 5 total, 4 unique = 1 duplicate
        assert len(result.details["columns"]) == 3

    # -------------------------------------------------------------------------
    # Threshold Tests
    # -------------------------------------------------------------------------

    def test_passes_with_duplicates_below_threshold(self, spark):
        """Test that UniquenessValidator passes when duplicate percentage is below threshold."""
        # Create DataFrame with 20% duplicates (1 out of 5)
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ])
        data = [
            (1, "A"),
            (2, "B"),
            (3, "C"),
            (4, "D"),
            (1, "A"),  # duplicate (1 duplicate out of 5 = 20%)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with threshold higher than actual duplicate percentage
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=0.3)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (20% duplicates is within 30% threshold)
        assert result.passed is True
        assert result.failure_count == 1
        assert "passed" in result.message.lower()
        assert "within threshold" in result.message.lower()

    def test_fails_with_duplicates_above_threshold(self, spark):
        """Test that UniquenessValidator fails when duplicate percentage exceeds threshold."""
        # Create DataFrame with 40% duplicates (2 out of 5)
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ])
        data = [
            (1, "A"),
            (2, "B"),
            (3, "C"),
            (1, "A"),  # duplicate
            (1, "A"),  # another duplicate (2 duplicates out of 5 = 40%)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with threshold lower than actual duplicate percentage
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=0.2)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (40% duplicates exceeds 20% threshold)
        assert result.passed is False
        assert result.failure_count == 2
        assert "failed" in result.message.lower()
        assert "exceed" in result.message.lower()

    def test_passes_with_duplicates_exactly_at_threshold(self, spark):
        """Test that UniquenessValidator passes when duplicate percentage is exactly at threshold."""
        # Create DataFrame with exactly 20% duplicates (1 out of 5)
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ])
        data = [
            (1, "A"),
            (2, "B"),
            (3, "C"),
            (4, "D"),
            (1, "A"),  # duplicate (1/5 = 20%)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with threshold exactly at duplicate percentage
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=0.2)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (exactly at threshold)
        assert result.passed is True

    def test_threshold_zero_is_strict_mode(self, spark):
        """Test that a threshold of 0.0 enforces strict mode (no duplicates allowed)."""
        # Create DataFrame with minimal duplicates (one duplicate)
        df = create_test_df_with_duplicates(spark, duplicate_count=1)

        # Create validator with strict mode (threshold = 0.0)
        validator = UniquenessValidator(columns=["user_id"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (any duplicates fail strict mode)
        assert result.passed is False

    def test_threshold_100_percent_allows_all_duplicates(self, spark):
        """Test that a threshold of 1.0 allows 100% duplicates."""
        # Create DataFrame where all rows are duplicates
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True),
        ])
        data = [(1, "A"), (1, "A"), (1, "A"), (1, "A")]  # All same
        df = spark.createDataFrame(data, schema)

        # Create validator allowing 100% duplicates
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=1.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True

    # -------------------------------------------------------------------------
    # Null Value Handling Tests
    # -------------------------------------------------------------------------

    def test_null_values_counted_as_group(self, spark):
        """Test that null values are treated as equal (grouped together)."""
        # Create DataFrame with null values
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("category", StringType(), True),
        ])
        data = [
            (1, "A"),
            (2, None),
            (3, None),  # Two nulls = 1 duplicate
            (4, "B"),
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = UniquenessValidator(columns=["category"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (nulls are treated as equal, so there's a duplicate)
        assert result.passed is False
        assert result.failure_count == 1  # 4 rows, 3 unique (A, null, B) = 1 duplicate

    def test_null_in_composite_key(self, spark):
        """Test null values in composite key columns."""
        # Create DataFrame with nulls in composite key
        schema = StructType([
            StructField("col_a", StringType(), True),
            StructField("col_b", StringType(), True),
        ])
        data = [
            ("A", "1"),
            ("A", None),
            ("A", None),  # duplicate of previous row
            (None, "1"),
            (None, "1"),  # duplicate of previous row
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator for composite key
        validator = UniquenessValidator(
            columns=["col_a", "col_b"],
            max_duplicate_percentage=0.0
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 2  # 5 rows, 3 unique = 2 duplicates

    # -------------------------------------------------------------------------
    # Edge Cases
    # -------------------------------------------------------------------------

    def test_empty_dataframe(self, spark):
        """Test that UniquenessValidator handles empty DataFrames gracefully."""
        # Create empty DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        df = spark.createDataFrame([], schema)

        # Create validator
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed (empty DataFrame has no duplicates)
        assert result.passed is True
        assert result.total_count == 0
        assert "empty" in result.message.lower()

    def test_single_row_dataframe(self, spark):
        """Test UniquenessValidator with a single row (always unique)."""
        # Create single-row DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        df = spark.createDataFrame([(1, "alice")], schema)

        # Create validator
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert result.total_count == 1
        assert result.details["unique_count"] == 1

    def test_two_rows_all_duplicates(self, spark):
        """Test UniquenessValidator with two identical rows."""
        # Create DataFrame with two identical rows
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        df = spark.createDataFrame([(1, "alice"), (1, "alice")], schema)

        # Create validator
        validator = UniquenessValidator(columns=["id", "name"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1  # 2 rows, 1 unique = 1 duplicate
        assert result.details["duplicate_percentage"] == 0.5

    def test_missing_column_raises_error(self, spark):
        """Test that UniquenessValidator raises an error for missing columns."""
        # Create DataFrame
        df = create_test_df_with_duplicates(spark, duplicate_count=0)

        # Create validator with a non-existent column
        validator = UniquenessValidator(columns=["nonexistent_column"], max_duplicate_percentage=0.0)

        # Assert that validation raises ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate(df)

        assert "not found" in str(exc_info.value).lower()

    def test_partial_missing_columns(self, spark):
        """Test that UniquenessValidator raises error when some columns are missing."""
        # Create DataFrame
        df = create_test_df_with_duplicates(spark, duplicate_count=0)

        # Create validator with one valid and one invalid column
        validator = UniquenessValidator(
            columns=["user_id", "nonexistent"],
            max_duplicate_percentage=0.0
        )

        # Assert that validation raises ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate(df)

        assert "nonexistent" in str(exc_info.value)

    def test_large_dataset(self, spark):
        """Test UniquenessValidator with a larger dataset."""
        # Create DataFrame with many rows
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", IntegerType(), True),
        ])
        # 100 rows, ids 1-90 are unique, ids 91-100 are duplicates of 1-10
        data = [(i, i * 10) for i in range(1, 91)]  # 90 unique rows
        data.extend([(i, i * 10) for i in range(1, 11)])  # 10 duplicate rows
        df = spark.createDataFrame(data, schema)

        # Create validator with 5% threshold
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=0.05)

        # Validate
        result = validator.validate(df)

        # Assert validation failed (10/100 = 10% > 5%)
        assert result.passed is False
        assert result.failure_count == 10
        assert result.total_count == 100

    # -------------------------------------------------------------------------
    # Input Validation Tests
    # -------------------------------------------------------------------------

    def test_empty_columns_list_raises_error(self, spark):
        """Test that UniquenessValidator raises error when columns list is empty."""
        with pytest.raises(ValueError) as exc_info:
            UniquenessValidator(columns=[], max_duplicate_percentage=0.0)

        assert "empty" in str(exc_info.value).lower()

    def test_invalid_threshold_negative_raises_error(self, spark):
        """Test that UniquenessValidator raises error for negative threshold."""
        with pytest.raises(ValueError) as exc_info:
            UniquenessValidator(columns=["col"], max_duplicate_percentage=-0.1)

        assert "max_duplicate_percentage" in str(exc_info.value)

    def test_invalid_threshold_above_one_raises_error(self, spark):
        """Test that UniquenessValidator raises error for threshold above 1.0."""
        with pytest.raises(ValueError) as exc_info:
            UniquenessValidator(columns=["col"], max_duplicate_percentage=1.5)

        assert "max_duplicate_percentage" in str(exc_info.value)

    def test_boundary_threshold_values(self, spark):
        """Test that UniquenessValidator accepts boundary threshold values (0.0 and 1.0)."""
        # Should not raise
        validator_zero = UniquenessValidator(columns=["col"], max_duplicate_percentage=0.0)
        validator_one = UniquenessValidator(columns=["col"], max_duplicate_percentage=1.0)

        assert validator_zero.max_duplicate_percentage == 0.0
        assert validator_one.max_duplicate_percentage == 1.0

    # -------------------------------------------------------------------------
    # Result Details Tests
    # -------------------------------------------------------------------------

    def test_result_contains_all_expected_fields(self, spark):
        """Test that the validation result contains all expected fields."""
        # Create DataFrame with duplicates
        df = create_test_df_with_duplicates(spark, duplicate_count=2)

        # Create validator
        validator = UniquenessValidator(columns=["user_id", "timestamp"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Check result has all expected fields
        assert result.validator_name == "UniquenessValidator"
        assert isinstance(result.passed, bool)
        assert isinstance(result.failure_count, int)
        assert isinstance(result.total_count, int)
        assert isinstance(result.message, str)
        assert isinstance(result.details, dict)

        # Check details has all expected keys
        assert "columns" in result.details
        assert "max_duplicate_percentage" in result.details
        assert "unique_count" in result.details
        assert "duplicate_row_count" in result.details
        assert "duplicate_percentage" in result.details

    def test_result_message_clarity_on_pass_no_duplicates(self, spark):
        """Test that the validation message is clear when passing with no duplicates."""
        # Create DataFrame without duplicates
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        data = [(1, "alice"), (2, "bob"), (3, "charlie")]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = UniquenessValidator(columns=["id"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is True
        assert "passed" in result.message.lower()
        assert "unique" in result.message.lower()

    def test_result_message_clarity_on_pass_within_threshold(self, spark):
        """Test that the validation message is clear when passing within threshold."""
        # Create DataFrame with some duplicates
        df = create_test_df_with_duplicates(spark, duplicate_count=1)

        # Create validator with high threshold
        validator = UniquenessValidator(columns=["user_id"], max_duplicate_percentage=0.5)

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is True
        assert "passed" in result.message.lower()
        assert "within threshold" in result.message.lower()

    def test_result_message_clarity_on_fail(self, spark):
        """Test that the validation message is clear on failure."""
        # Create DataFrame with duplicates
        df = create_test_df_with_duplicates(spark, duplicate_count=2)

        # Create validator
        validator = UniquenessValidator(columns=["user_id"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is False
        assert "failed" in result.message.lower()

    def test_validator_name_property(self, spark):
        """Test that the validator name property works correctly."""
        validator = UniquenessValidator(columns=["col"], max_duplicate_percentage=0.0)
        assert validator.name == "UniquenessValidator"

    def test_columns_property(self, spark):
        """Test that the columns property is set correctly."""
        validator = UniquenessValidator(columns=["col1", "col2"], max_duplicate_percentage=0.0)
        assert validator.columns == ["col1", "col2"]

    def test_max_duplicate_percentage_property(self, spark):
        """Test that the max_duplicate_percentage property is set correctly."""
        validator = UniquenessValidator(columns=["col"], max_duplicate_percentage=0.15)
        assert validator.max_duplicate_percentage == 0.15

    def test_message_shows_single_column_format(self, spark):
        """Test that message uses singular format for single column."""
        # Create DataFrame
        df = create_test_df_with_duplicates(spark, duplicate_count=0)

        # Create validator for single column
        validator = UniquenessValidator(columns=["user_id"], max_duplicate_percentage=0.0)

        # Validate
        result = validator.validate(df)

        # Check message uses singular format
        assert "column 'user_id'" in result.message

    def test_message_shows_multiple_columns_format(self, spark):
        """Test that message uses plural format for multiple columns."""
        # Create DataFrame
        df = create_test_df_with_duplicates(spark, duplicate_count=0)

        # Create validator for multiple columns
        validator = UniquenessValidator(
            columns=["user_id", "timestamp"],
            max_duplicate_percentage=0.0
        )

        # Validate
        result = validator.validate(df)

        # Check message uses plural format
        assert "columns" in result.message


# Legacy test for backward compatibility
def test_uniqueness_validator_detects_duplicates(spark):
    """Test that UniquenessValidator correctly identifies duplicate values."""
    # Create DataFrame with duplicates
    df = create_test_df_with_duplicates(spark, duplicate_count=2)

    # Create validator for columns with duplicates (strict mode - no duplicates allowed)
    validator = UniquenessValidator(columns=["user_id", "timestamp"], max_duplicate_percentage=0.0)

    # Validate
    result = validator.validate(df)

    # Assert validation failed due to duplicates
    assert result.passed is False
    assert result.failure_count > 0
    assert "UniquenessValidator" in result.validator_name
    assert result.details is not None
    assert "duplicate_row_count" in result.details


# =============================================================================
# FreshnessValidator Tests
# =============================================================================


class TestFreshnessValidator:
    """Comprehensive tests for the FreshnessValidator class."""

    # -------------------------------------------------------------------------
    # Basic Freshness Detection Tests
    # -------------------------------------------------------------------------

    def test_detects_stale_timestamps(self, spark):
        """Test that FreshnessValidator correctly identifies stale timestamps."""
        # Create reference time
        reference_time = datetime.now()

        # Create DataFrame with some stale timestamps
        df = create_test_df_with_timestamps(
            spark,
            fresh_count=5,
            stale_count=2,
            reference_time=reference_time,
            max_age_hours=24,
        )

        # Create validator with 24 hour max age
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed due to stale timestamps
        assert result.passed is False
        assert result.failure_count == 2
        assert result.validator_name == "FreshnessValidator"
        assert result.details is not None
        assert "stale_count" in result.details
        assert result.details["stale_count"] == 2

    def test_passes_when_all_timestamps_fresh(self, spark):
        """Test that FreshnessValidator passes when all timestamps are fresh."""
        # Create reference time
        reference_time = datetime.now()

        # Create DataFrame with only fresh timestamps
        df = create_test_df_with_timestamps(
            spark,
            fresh_count=10,
            stale_count=0,
            reference_time=reference_time,
            max_age_hours=24,
        )

        # Create validator with 24 hour max age
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.failure_count == 0
        assert result.validator_name == "FreshnessValidator"
        assert "passed" in result.message.lower()
        assert result.details["stale_count"] == 0

    def test_detects_all_stale_timestamps(self, spark):
        """Test that FreshnessValidator correctly counts all stale timestamps."""
        # Create reference time
        reference_time = datetime.now()

        # Create DataFrame with all stale timestamps
        df = create_test_df_with_timestamps(
            spark,
            fresh_count=0,
            stale_count=5,
            reference_time=reference_time,
            max_age_hours=24,
        )

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 5
        assert result.details["stale_count"] == 5
        assert result.details["non_null_count"] == 5

    # -------------------------------------------------------------------------
    # Boundary Tests
    # -------------------------------------------------------------------------

    def test_timestamp_exactly_at_cutoff(self, spark):
        """Test that timestamp exactly at cutoff is considered fresh (boundary case)."""
        # Create reference time
        reference_time = datetime(2024, 6, 1, 12, 0, 0)
        max_age = timedelta(hours=24)
        cutoff_time = reference_time - max_age  # June 1, 12:00 - 24h = May 31, 12:00

        # Create DataFrame with timestamp exactly at cutoff
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        # Timestamp at cutoff is NOT stale (cutoff is exclusive for stale, inclusive for fresh)
        data = [(1, cutoff_time)]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=max_age,
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed (at cutoff is still fresh - it's not < cutoff)
        # Based on implementation: stale = timestamp < cutoff
        assert result.passed is True
        assert result.failure_count == 0

    def test_timestamp_just_before_cutoff(self, spark):
        """Test that timestamp just before cutoff is considered stale."""
        # Create reference time
        reference_time = datetime(2024, 6, 1, 12, 0, 0)
        max_age = timedelta(hours=24)
        cutoff_time = reference_time - max_age  # May 31, 12:00

        # Create DataFrame with timestamp just before cutoff (1 second)
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        stale_time = cutoff_time - timedelta(seconds=1)
        data = [(1, stale_time)]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=max_age,
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed (just before cutoff is stale)
        assert result.passed is False
        assert result.failure_count == 1

    def test_timestamp_just_after_cutoff(self, spark):
        """Test that timestamp just after cutoff is considered fresh."""
        # Create reference time
        reference_time = datetime(2024, 6, 1, 12, 0, 0)
        max_age = timedelta(hours=24)
        cutoff_time = reference_time - max_age  # May 31, 12:00

        # Create DataFrame with timestamp just after cutoff (1 second)
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        fresh_time = cutoff_time + timedelta(seconds=1)
        data = [(1, fresh_time)]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=max_age,
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed (just after cutoff is fresh)
        assert result.passed is True
        assert result.failure_count == 0

    # -------------------------------------------------------------------------
    # Reference Time Tests
    # -------------------------------------------------------------------------

    def test_custom_reference_time(self, spark):
        """Test that FreshnessValidator correctly uses custom reference time."""
        # Create a specific reference time
        reference_time = datetime(2024, 6, 1, 12, 0, 0)
        max_age = timedelta(days=7)

        # Create DataFrame with known timestamps
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [
            (1, datetime(2024, 5, 30, 12, 0, 0)),  # 2 days old (fresh)
            (2, datetime(2024, 5, 25, 12, 0, 0)),  # 7 days old (fresh, at boundary)
            (3, datetime(2024, 5, 20, 12, 0, 0)),  # 12 days old (stale)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with custom reference time
        validator = FreshnessValidator(
            column="event_time",
            max_age=max_age,
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed (1 stale record)
        assert result.passed is False
        assert result.failure_count == 1
        assert result.details["reference_time"] == reference_time.isoformat()

    def test_default_reference_time_uses_current_time(self, spark):
        """Test that FreshnessValidator uses current time when reference_time is None."""
        # Create DataFrame with recent timestamps
        current_time = datetime.now()
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [
            (1, current_time - timedelta(hours=1)),  # 1 hour ago
            (2, current_time - timedelta(hours=2)),  # 2 hours ago
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator without reference_time
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),  # 24 hours
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed (all within 24 hours)
        assert result.passed is True
        assert result.failure_count == 0
        assert "reference_time" in result.details

    # -------------------------------------------------------------------------
    # Max Age Unit Tests
    # -------------------------------------------------------------------------

    def test_max_age_in_seconds(self, spark):
        """Test FreshnessValidator with max_age in seconds."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [
            (1, reference_time - timedelta(seconds=30)),  # 30 seconds old (fresh)
            (2, reference_time - timedelta(seconds=90)),  # 90 seconds old (stale)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with 60 second max age
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(seconds=60),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed (1 stale)
        assert result.passed is False
        assert result.failure_count == 1

    def test_max_age_in_minutes(self, spark):
        """Test FreshnessValidator with max_age in minutes."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [
            (1, reference_time - timedelta(minutes=30)),  # 30 mins old (fresh)
            (2, reference_time - timedelta(minutes=90)),  # 90 mins old (stale)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with 60 minute max age
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(minutes=60),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed (1 stale)
        assert result.passed is False
        assert result.failure_count == 1

    def test_max_age_in_hours(self, spark):
        """Test FreshnessValidator with max_age in hours."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [
            (1, reference_time - timedelta(hours=12)),  # 12 hours old (fresh)
            (2, reference_time - timedelta(hours=48)),  # 48 hours old (stale)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with 24 hour max age
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed (1 stale)
        assert result.passed is False
        assert result.failure_count == 1

    def test_max_age_in_days(self, spark):
        """Test FreshnessValidator with max_age in days."""
        reference_time = datetime(2024, 6, 15, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [
            (1, datetime(2024, 6, 10, 12, 0, 0)),  # 5 days old (fresh)
            (2, datetime(2024, 6, 1, 12, 0, 0)),   # 14 days old (stale)
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator with 7 day max age
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(days=7),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed (1 stale)
        assert result.passed is False
        assert result.failure_count == 1

    # -------------------------------------------------------------------------
    # Null Value Handling Tests
    # -------------------------------------------------------------------------

    def test_null_values_excluded_from_check(self, spark):
        """Test that null values are excluded from freshness check."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [
            (1, reference_time - timedelta(hours=1)),  # fresh
            (2, None),  # null
            (3, reference_time - timedelta(hours=2)),  # fresh
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed (null excluded, other values are fresh)
        assert result.passed is True
        assert result.details["null_count"] == 1
        assert result.details["non_null_count"] == 2

    def test_all_null_column_passes(self, spark):
        """Test that column with all null values passes validation."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [(1, None), (2, None), (3, None)]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed (no non-null values to check)
        assert result.passed is True
        assert result.details["null_count"] == 3
        assert result.details["non_null_count"] == 0
        assert "null" in result.message.lower()

    def test_null_values_with_stale_records(self, spark):
        """Test that null values don't affect detection of stale records."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        data = [
            (1, None),
            (2, reference_time - timedelta(days=30)),  # stale
            (3, None),
            (4, reference_time - timedelta(hours=1)),  # fresh
        ]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed (1 stale record)
        assert result.passed is False
        assert result.failure_count == 1
        assert result.details["null_count"] == 2
        assert result.details["non_null_count"] == 2
        assert result.details["stale_count"] == 1

    # -------------------------------------------------------------------------
    # Edge Cases
    # -------------------------------------------------------------------------

    def test_empty_dataframe(self, spark):
        """Test that FreshnessValidator handles empty DataFrames gracefully."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        df = spark.createDataFrame([], schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed (empty DataFrame)
        assert result.passed is True
        assert result.total_count == 0
        assert "empty" in result.message.lower()

    def test_single_fresh_row(self, spark):
        """Test FreshnessValidator with a single fresh row."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        df = spark.createDataFrame([(1, reference_time - timedelta(hours=1))], schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed
        assert result.passed is True
        assert result.total_count == 1
        assert result.failure_count == 0

    def test_single_stale_row(self, spark):
        """Test FreshnessValidator with a single stale row."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        df = spark.createDataFrame([(1, reference_time - timedelta(days=7))], schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.total_count == 1
        assert result.failure_count == 1

    def test_missing_column_raises_error(self, spark):
        """Test that FreshnessValidator raises an error for missing columns."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        df = create_test_df_with_timestamps(
            spark,
            fresh_count=5,
            stale_count=0,
            reference_time=reference_time,
        )

        # Create validator with a non-existent column
        validator = FreshnessValidator(
            column="nonexistent_column",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Assert that validation raises ValueError
        with pytest.raises(ValueError) as exc_info:
            validator.validate(df)

        assert "not found" in str(exc_info.value).lower()

    def test_large_dataset(self, spark):
        """Test FreshnessValidator with a larger dataset."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        # Create 100 rows: 80 fresh, 20 stale
        data = []
        for i in range(80):
            data.append((i, reference_time - timedelta(hours=i % 24)))  # All within 24 hours
        for i in range(20):
            data.append((80 + i, reference_time - timedelta(days=2 + i)))  # All stale

        df = spark.createDataFrame(data, schema)

        # Create validator with 24 hour max age
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed (20 stale records)
        assert result.passed is False
        assert result.failure_count == 20
        assert result.total_count == 100

    def test_very_old_timestamp(self, spark):
        """Test FreshnessValidator with a very old timestamp."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        # Timestamp from year 2000
        very_old = datetime(2000, 1, 1, 0, 0, 0)
        data = [(1, very_old)]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(days=365),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation failed
        assert result.passed is False
        assert result.failure_count == 1

    def test_future_timestamp(self, spark):
        """Test FreshnessValidator with a future timestamp."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        # Timestamp in the future
        future_time = reference_time + timedelta(days=1)
        data = [(1, future_time)]
        df = spark.createDataFrame(data, schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Assert validation passed (future timestamps are fresh by definition)
        assert result.passed is True
        assert result.failure_count == 0

    # -------------------------------------------------------------------------
    # Input Validation Tests
    # -------------------------------------------------------------------------

    def test_empty_column_raises_error(self, spark):
        """Test that FreshnessValidator raises error when column is empty."""
        with pytest.raises(ValueError) as exc_info:
            FreshnessValidator(column="", max_age=timedelta(hours=24))

        assert "empty" in str(exc_info.value).lower()

    def test_non_timedelta_max_age_raises_error(self, spark):
        """Test that FreshnessValidator raises error when max_age is not timedelta."""
        with pytest.raises(ValueError) as exc_info:
            FreshnessValidator(column="event_time", max_age=24)  # Integer, not timedelta

        assert "timedelta" in str(exc_info.value).lower()

    def test_zero_max_age_raises_error(self, spark):
        """Test that FreshnessValidator raises error for zero max_age."""
        with pytest.raises(ValueError) as exc_info:
            FreshnessValidator(column="event_time", max_age=timedelta(seconds=0))

        assert "positive" in str(exc_info.value).lower()

    def test_negative_max_age_raises_error(self, spark):
        """Test that FreshnessValidator raises error for negative max_age."""
        with pytest.raises(ValueError) as exc_info:
            FreshnessValidator(column="event_time", max_age=timedelta(hours=-1))

        assert "positive" in str(exc_info.value).lower()

    def test_valid_small_max_age(self, spark):
        """Test that FreshnessValidator accepts very small positive max_age."""
        # Should not raise
        validator = FreshnessValidator(column="event_time", max_age=timedelta(seconds=1))
        assert validator.max_age.total_seconds() == 1

    def test_valid_large_max_age(self, spark):
        """Test that FreshnessValidator accepts large max_age values."""
        # Should not raise
        validator = FreshnessValidator(column="event_time", max_age=timedelta(days=365))
        assert validator.max_age.total_seconds() == 365 * 24 * 60 * 60

    # -------------------------------------------------------------------------
    # Result Details Tests
    # -------------------------------------------------------------------------

    def test_result_contains_all_expected_fields(self, spark):
        """Test that the validation result contains all expected fields."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        df = create_test_df_with_timestamps(
            spark,
            fresh_count=5,
            stale_count=2,
            reference_time=reference_time,
        )

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Check result has all expected fields
        assert result.validator_name == "FreshnessValidator"
        assert isinstance(result.passed, bool)
        assert isinstance(result.failure_count, int)
        assert isinstance(result.total_count, int)
        assert isinstance(result.message, str)
        assert isinstance(result.details, dict)

        # Check details has all expected keys
        assert "column" in result.details
        assert "max_age_seconds" in result.details
        assert "stale_count" in result.details
        assert "null_count" in result.details
        assert "non_null_count" in result.details
        assert "reference_time" in result.details
        assert "cutoff_time" in result.details

    def test_result_message_clarity_on_pass(self, spark):
        """Test that the validation message is clear on pass."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        df = create_test_df_with_timestamps(
            spark,
            fresh_count=5,
            stale_count=0,
            reference_time=reference_time,
        )

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is True
        assert "passed" in result.message.lower()
        assert "event_time" in result.message

    def test_result_message_clarity_on_fail(self, spark):
        """Test that the validation message is clear on failure."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        df = create_test_df_with_timestamps(
            spark,
            fresh_count=3,
            stale_count=2,
            reference_time=reference_time,
        )

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Check message is clear
        assert result.passed is False
        assert "failed" in result.message.lower()
        assert "event_time" in result.message
        assert "2" in result.message  # Should mention stale count

    def test_result_message_includes_max_age_unit(self, spark):
        """Test that the validation message includes the max_age in appropriate units."""
        reference_time = datetime(2024, 6, 1, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        df = spark.createDataFrame([(1, reference_time - timedelta(hours=1))], schema)

        # Test with hours
        validator_hours = FreshnessValidator(
            column="event_time",
            max_age=timedelta(hours=24),
            reference_time=reference_time,
        )
        result_hours = validator_hours.validate(df)
        assert "24 hours" in result_hours.message

        # Test with days
        validator_days = FreshnessValidator(
            column="event_time",
            max_age=timedelta(days=7),
            reference_time=reference_time,
        )
        result_days = validator_days.validate(df)
        assert "7 days" in result_days.message

        # Test with minutes
        validator_mins = FreshnessValidator(
            column="event_time",
            max_age=timedelta(minutes=30),
            reference_time=reference_time,
        )
        result_mins = validator_mins.validate(df)
        assert "30 minutes" in result_mins.message

    def test_validator_name_property(self, spark):
        """Test that the validator name property works correctly."""
        validator = FreshnessValidator(column="col", max_age=timedelta(hours=24))
        assert validator.name == "FreshnessValidator"

    def test_column_property(self, spark):
        """Test that the column property is set correctly."""
        validator = FreshnessValidator(column="test_col", max_age=timedelta(hours=24))
        assert validator.column == "test_col"

    def test_max_age_property(self, spark):
        """Test that the max_age property is set correctly."""
        max_age = timedelta(hours=48)
        validator = FreshnessValidator(column="col", max_age=max_age)
        assert validator.max_age == max_age

    def test_reference_time_property(self, spark):
        """Test that the reference_time property is set correctly."""
        ref_time = datetime(2024, 6, 1, 12, 0, 0)
        validator = FreshnessValidator(
            column="col",
            max_age=timedelta(hours=24),
            reference_time=ref_time,
        )
        assert validator.reference_time == ref_time

    def test_reference_time_none_by_default(self, spark):
        """Test that reference_time is None by default."""
        validator = FreshnessValidator(column="col", max_age=timedelta(hours=24))
        assert validator.reference_time is None

    def test_cutoff_time_calculation(self, spark):
        """Test that cutoff time is correctly calculated."""
        reference_time = datetime(2024, 6, 15, 12, 0, 0)
        max_age = timedelta(days=7)
        expected_cutoff = datetime(2024, 6, 8, 12, 0, 0)

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("event_time", TimestampType(), True),
        ])
        df = spark.createDataFrame([(1, reference_time - timedelta(hours=1))], schema)

        # Create validator
        validator = FreshnessValidator(
            column="event_time",
            max_age=max_age,
            reference_time=reference_time,
        )

        # Validate
        result = validator.validate(df)

        # Check cutoff time in details
        assert result.details["cutoff_time"] == expected_cutoff.isoformat()


# Legacy test for backward compatibility
def test_freshness_validator_detects_stale_data(spark):
    """Test that FreshnessValidator correctly identifies stale timestamps."""
    # Create reference time
    reference_time = datetime.now()

    # Create DataFrame with stale timestamps
    df = create_test_df_with_timestamps(
        spark,
        fresh_count=3,
        stale_count=2,
        reference_time=reference_time,
        max_age_hours=24,
    )

    # Create validator with 24 hour max age
    validator = FreshnessValidator(
        column="event_time",
        max_age=timedelta(hours=24),
        reference_time=reference_time,
    )

    # Validate
    result = validator.validate(df)

    # Assert validation failed due to stale timestamps
    assert result.passed is False
    assert result.failure_count == 2
    assert "FreshnessValidator" in result.validator_name
    assert result.details is not None
    assert "stale_count" in result.details


# =============================================================================
# CustomValidator and ValidationRunner Tests
# =============================================================================


class TestCustomValidatorAndRunner:
    """Comprehensive tests for the CustomValidator and ValidationRunner classes."""

    # -------------------------------------------------------------------------
    # CustomValidator Initialization Tests
    # -------------------------------------------------------------------------

    def test_custom_validator_initialization_with_function(self, spark):
        """Test that CustomValidator correctly initializes with a function."""
        def my_validator(df):
            return True

        validator = CustomValidator(func=my_validator, name="my_validator")

        assert validator.name == "my_validator"
        assert validator.func is my_validator
        assert validator.description is None

    def test_custom_validator_initialization_with_lambda(self, spark):
        """Test that CustomValidator correctly initializes with a lambda."""
        validator = CustomValidator(
            func=lambda df: df.count() > 0,
            name="non_empty_check",
        )

        assert validator.name == "non_empty_check"
        assert callable(validator.func)

    def test_custom_validator_initialization_with_description(self, spark):
        """Test that CustomValidator correctly stores description."""
        validator = CustomValidator(
            func=lambda df: True,
            name="test_validator",
            description="Checks that data meets requirements",
        )

        assert validator.description == "Checks that data meets requirements"

    def test_custom_validator_initialization_default_name(self, spark):
        """Test that CustomValidator uses default name if not provided."""
        validator = CustomValidator(func=lambda df: True)

        assert validator.name == "CustomValidator"

    def test_custom_validator_rejects_non_callable(self, spark):
        """Test that CustomValidator raises error for non-callable func."""
        with pytest.raises(ValueError) as excinfo:
            CustomValidator(func="not a function", name="test")

        assert "callable" in str(excinfo.value).lower()

    def test_custom_validator_rejects_none_func(self, spark):
        """Test that CustomValidator raises error for None func."""
        with pytest.raises(ValueError) as excinfo:
            CustomValidator(func=None, name="test")

        assert "callable" in str(excinfo.value).lower()

    # -------------------------------------------------------------------------
    # CustomValidator Boolean Return Tests
    # -------------------------------------------------------------------------

    def test_custom_validator_boolean_true_return(self, spark):
        """Test that CustomValidator handles boolean True return correctly."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        validator = CustomValidator(
            func=lambda df: True,
            name="always_pass",
        )

        result = validator.validate(df)

        assert result.passed is True
        assert result.validator_name == "always_pass"
        assert "passed" in result.message.lower()

    def test_custom_validator_boolean_false_return(self, spark):
        """Test that CustomValidator handles boolean False return correctly."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        validator = CustomValidator(
            func=lambda df: False,
            name="always_fail",
        )

        result = validator.validate(df)

        assert result.passed is False
        assert result.validator_name == "always_fail"
        assert "failed" in result.message.lower()

    def test_custom_validator_boolean_with_description_in_message(self, spark):
        """Test that CustomValidator includes description in message."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        validator = CustomValidator(
            func=lambda df: True,
            name="test",
            description="All values are positive",
        )

        result = validator.validate(df)

        assert result.passed is True
        assert "All values are positive" in result.message

    def test_custom_validator_boolean_failure_count(self, spark):
        """Test that CustomValidator sets correct failure_count for boolean return."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        # When True, failure_count should be 0
        validator_pass = CustomValidator(func=lambda df: True, name="pass")
        result_pass = validator_pass.validate(df)
        assert result_pass.failure_count == 0

        # When False, failure_count should be total_count
        validator_fail = CustomValidator(func=lambda df: False, name="fail")
        result_fail = validator_fail.validate(df)
        assert result_fail.failure_count == result_fail.total_count

    # -------------------------------------------------------------------------
    # CustomValidator ValidationResult Return Tests
    # -------------------------------------------------------------------------

    def test_custom_validator_validation_result_return(self, spark):
        """Test that CustomValidator handles ValidationResult return correctly."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def custom_check(df):
            count = df.count()
            return ValidationResult(
                validator_name="custom_check",
                passed=count > 0,
                failure_count=0,
                total_count=count,
                message=f"Found {count} rows",
                details={"row_count": count},
            )

        validator = CustomValidator(func=custom_check, name="custom_check")

        result = validator.validate(df)

        assert result.passed is True
        assert result.validator_name == "custom_check"
        assert "Found" in result.message
        assert "row_count" in result.details

    def test_custom_validator_validation_result_preserves_details(self, spark):
        """Test that CustomValidator preserves ValidationResult details."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def custom_check(df):
            return ValidationResult(
                validator_name="detailed_check",
                passed=True,
                failure_count=0,
                total_count=df.count(),
                message="Custom message",
                details={
                    "custom_key": "custom_value",
                    "nested": {"key": "value"},
                },
            )

        validator = CustomValidator(func=custom_check, name="detailed_check")

        result = validator.validate(df)

        assert result.details["custom_key"] == "custom_value"
        assert result.details["nested"]["key"] == "value"

    def test_custom_validator_sets_validator_name_if_missing(self, spark):
        """Test that CustomValidator sets validator_name if not in result."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def custom_check(df):
            return ValidationResult(
                validator_name="",
                passed=True,
                failure_count=0,
                total_count=df.count(),
                message="Check passed",
            )

        validator = CustomValidator(func=custom_check, name="my_validator")

        result = validator.validate(df)

        assert result.validator_name == "my_validator"

    # -------------------------------------------------------------------------
    # CustomValidator Exception Handling Tests
    # -------------------------------------------------------------------------

    def test_custom_validator_handles_exception_gracefully(self, spark):
        """Test that CustomValidator handles exceptions in user function."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def failing_check(df):
            raise RuntimeError("Something went wrong")

        validator = CustomValidator(func=failing_check, name="failing_check")

        result = validator.validate(df)

        assert result.passed is False
        assert "exception" in result.message.lower()
        assert "Something went wrong" in result.message
        assert "error" in result.details
        assert result.details["error_type"] == "RuntimeError"

    def test_custom_validator_handles_value_error(self, spark):
        """Test that CustomValidator handles ValueError in user function."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def failing_check(df):
            raise ValueError("Invalid value")

        validator = CustomValidator(func=failing_check, name="value_error_check")

        result = validator.validate(df)

        assert result.passed is False
        assert result.details["error_type"] == "ValueError"
        assert "Invalid value" in result.details["error"]

    def test_custom_validator_raises_type_error_for_invalid_return(self, spark):
        """Test that CustomValidator raises TypeError for invalid return type."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def invalid_return_check(df):
            return "invalid"  # Neither bool nor ValidationResult

        validator = CustomValidator(func=invalid_return_check, name="invalid")

        with pytest.raises(TypeError) as excinfo:
            validator.validate(df)

        assert "bool or ValidationResult" in str(excinfo.value)

    # -------------------------------------------------------------------------
    # CustomValidator with Real Validation Logic Tests
    # -------------------------------------------------------------------------

    def test_custom_validator_with_null_check_logic(self, spark):
        """Test CustomValidator with custom null check logic."""
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        def check_no_nulls(df):
            from pyspark.sql import functions as F
            null_count = df.filter(F.col("name").isNull()).count()
            return null_count == 0

        validator = CustomValidator(
            func=check_no_nulls,
            name="null_check",
            description="Checks that name column has no nulls",
        )

        result = validator.validate(df)

        # Should fail because there are nulls
        assert result.passed is False

    def test_custom_validator_with_row_count_logic(self, spark):
        """Test CustomValidator with row count validation logic."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def check_minimum_rows(df):
            return df.count() >= 5

        validator = CustomValidator(
            func=check_minimum_rows,
            name="min_rows_check",
            description="Ensures at least 5 rows exist",
        )

        result = validator.validate(df)

        assert result.passed is True

    def test_custom_validator_with_empty_dataframe(self, spark):
        """Test CustomValidator with empty DataFrame."""
        df = create_empty_df(spark)

        validator = CustomValidator(
            func=lambda df: df.count() > 0,
            name="non_empty_check",
        )

        result = validator.validate(df)

        assert result.passed is False
        assert result.total_count == 0

    # -------------------------------------------------------------------------
    # CustomValidator Details Tests
    # -------------------------------------------------------------------------

    def test_custom_validator_details_include_function_name(self, spark):
        """Test that CustomValidator details include function name."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def my_named_function(df):
            return True

        validator = CustomValidator(func=my_named_function, name="test")

        result = validator.validate(df)

        assert "function_name" in result.details
        assert result.details["function_name"] == "my_named_function"

    def test_custom_validator_details_include_lambda_marker(self, spark):
        """Test that CustomValidator details indicate lambda for anonymous function."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        validator = CustomValidator(func=lambda df: True, name="test")

        result = validator.validate(df)

        assert "function_name" in result.details
        assert result.details["function_name"] == "<lambda>"

    def test_custom_validator_details_include_description(self, spark):
        """Test that CustomValidator details include description when provided."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        validator = CustomValidator(
            func=lambda df: True,
            name="test",
            description="My custom check",
        )

        result = validator.validate(df)

        assert "description" in result.details
        assert result.details["description"] == "My custom check"

    # -------------------------------------------------------------------------
    # ValidationRunner Initialization Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_initialization_with_validators(self, spark):
        """Test ValidationRunner initializes correctly with validators."""
        validators = [
            NullValidator(columns=["name"]),
            RangeValidator(column="id", min_val=0),
        ]

        runner = ValidationRunner(validators=validators, mode=ValidationMode.WARN)

        assert len(runner.validators) == 2
        assert runner.mode == ValidationMode.WARN

    def test_validation_runner_initialization_without_validators(self, spark):
        """Test ValidationRunner initializes correctly without validators."""
        runner = ValidationRunner()

        assert len(runner.validators) == 0
        assert runner.mode == ValidationMode.FAIL  # Default mode

    def test_validation_runner_initialization_with_feature_group_name(self, spark):
        """Test ValidationRunner stores feature group name."""
        runner = ValidationRunner(
            validators=[],
            feature_group_name="my_feature_group",
        )

        assert runner.feature_group_name == "my_feature_group"

    def test_validation_runner_mode_conversion_from_string(self, spark):
        """Test ValidationRunner converts string mode to enum."""
        runner = ValidationRunner(validators=[], mode="warn")

        assert runner.mode == ValidationMode.WARN

    # -------------------------------------------------------------------------
    # ValidationRunner.add_validator Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_add_validator(self, spark):
        """Test ValidationRunner.add_validator adds validators."""
        runner = ValidationRunner()

        runner.add_validator(NullValidator(columns=["name"]))
        runner.add_validator(RangeValidator(column="id", min_val=0))

        assert len(runner.validators) == 2

    def test_validation_runner_add_validator_chaining(self, spark):
        """Test ValidationRunner.add_validator returns self for chaining."""
        runner = ValidationRunner()

        result = runner.add_validator(NullValidator(columns=["name"]))

        assert result is runner

    def test_validation_runner_add_validator_chain_multiple(self, spark):
        """Test ValidationRunner.add_validator can be chained multiple times."""
        runner = (
            ValidationRunner()
            .add_validator(NullValidator(columns=["name"]))
            .add_validator(RangeValidator(column="id", min_val=0))
        )

        assert len(runner.validators) == 2

    # -------------------------------------------------------------------------
    # ValidationRunner.run with No Validators Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_run_no_validators(self, spark, caplog):
        """Test ValidationRunner.run with no validators logs warning."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)
        runner = ValidationRunner(validators=[])

        import logging
        with caplog.at_level(logging.WARNING):
            summary = runner.run(df)

        assert summary.total_validators == 0
        assert "No validators configured" in caplog.text

    def test_validation_runner_run_no_validators_returns_empty_summary(self, spark):
        """Test ValidationRunner.run with no validators returns empty summary."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)
        runner = ValidationRunner(validators=[])

        summary = runner.run(df)

        assert summary.passed is True
        assert summary.total_validators == 0
        assert summary.passed_count == 0
        assert summary.failed_count == 0

    # -------------------------------------------------------------------------
    # ValidationRunner.run with Passing Validators Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_run_all_pass(self, spark):
        """Test ValidationRunner.run when all validators pass."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"]),
                NullValidator(columns=["id"]),
            ],
            mode=ValidationMode.FAIL,
        )

        summary = runner.run(df)

        assert summary.passed is True
        assert summary.total_validators == 2
        assert summary.passed_count == 2
        assert summary.failed_count == 0

    def test_validation_runner_run_returns_all_results(self, spark):
        """Test ValidationRunner.run returns results from all validators."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"]),
                RangeValidator(column="id", min_val=0),
            ],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        assert len(summary.results) == 2
        validator_names = [r.validator_name for r in summary.results]
        assert "NullValidator" in validator_names
        assert "RangeValidator" in validator_names

    # -------------------------------------------------------------------------
    # ValidationRunner.run with Failing Validators in WARN Mode Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_run_warn_mode_continues_on_failure(self, spark):
        """Test ValidationRunner continues to next validator in WARN mode."""
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"], max_null_percentage=0.0),  # Will fail
                NullValidator(columns=["id"], max_null_percentage=0.0),  # Will pass
            ],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        # Both validators should run
        assert summary.total_validators == 2
        assert summary.passed_count == 1
        assert summary.failed_count == 1
        assert summary.passed is False  # Overall failed

    def test_validation_runner_run_warn_mode_collects_all_failures(self, spark):
        """Test ValidationRunner collects all failures in WARN mode."""
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"], max_null_percentage=0.0),  # Will fail
                NullValidator(columns=["value"], max_null_percentage=0.0),  # Will fail
            ],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        assert summary.failed_count == 2
        assert len(summary.results) == 2
        assert all(not r.passed for r in summary.results)

    # -------------------------------------------------------------------------
    # ValidationRunner.run with Failing Validators in FAIL Mode Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_run_fail_mode_raises_exception(self, spark):
        """Test ValidationRunner raises exception on failure in FAIL mode."""
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"], max_null_percentage=0.0),
            ],
            mode=ValidationMode.FAIL,
        )

        with pytest.raises(ValidationException) as excinfo:
            runner.run(df)

        assert "Validation failed" in str(excinfo.value)
        assert excinfo.value.result is not None
        assert excinfo.value.result.passed is False

    def test_validation_runner_run_fail_mode_stops_at_first_failure(self, spark):
        """Test ValidationRunner stops at first failure in FAIL mode."""
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        # Track which validators run
        execution_log = []

        def first_validator(df):
            execution_log.append("first")
            return False

        def second_validator(df):
            execution_log.append("second")
            return True

        runner = ValidationRunner(
            validators=[
                CustomValidator(func=first_validator, name="first"),
                CustomValidator(func=second_validator, name="second"),
            ],
            mode=ValidationMode.FAIL,
        )

        with pytest.raises(ValidationException):
            runner.run(df)

        # Only first validator should have run
        assert execution_log == ["first"]

    def test_validation_runner_run_fail_mode_exception_contains_result(self, spark):
        """Test ValidationRunner exception contains the failed result."""
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"], max_null_percentage=0.0),
            ],
            mode=ValidationMode.FAIL,
        )

        with pytest.raises(ValidationException) as excinfo:
            runner.run(df)

        assert excinfo.value.result.validator_name == "NullValidator"
        assert excinfo.value.result.failure_count > 0

    # -------------------------------------------------------------------------
    # ValidationRunner.run with Empty DataFrame Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_run_empty_dataframe(self, spark, caplog):
        """Test ValidationRunner handles empty DataFrame."""
        df = create_empty_df(spark)

        runner = ValidationRunner(
            validators=[NullValidator(columns=["name"])],
            mode=ValidationMode.WARN,
        )

        import logging
        with caplog.at_level(logging.WARNING):
            summary = runner.run(df)

        assert "empty" in caplog.text.lower()
        assert summary.total_validators == 1

    def test_validation_runner_run_empty_dataframe_validators_still_run(self, spark):
        """Test ValidationRunner runs validators even on empty DataFrame."""
        df = create_empty_df(spark)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"]),
                RangeValidator(column="id", min_val=0),
            ],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        assert summary.total_validators == 2
        assert len(summary.results) == 2

    # -------------------------------------------------------------------------
    # ValidationRunner.run Exception Handling Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_handles_validator_exception_warn_mode(self, spark):
        """Test ValidationRunner handles validator exceptions in WARN mode."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        def raising_validator(df):
            raise RuntimeError("Unexpected error")

        runner = ValidationRunner(
            validators=[
                CustomValidator(func=raising_validator, name="raising"),
                NullValidator(columns=["name"]),  # This should still run
            ],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        # Both validators should be in results
        assert summary.total_validators == 2
        assert summary.failed_count == 1  # The raising one fails
        assert summary.passed_count == 1  # NullValidator passes

    def test_validation_runner_handles_validator_exception_fail_mode(self, spark):
        """Test ValidationRunner raises for validator exceptions in FAIL mode."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        # Create a validator that will raise in validate method
        class RaisingValidator(BaseValidator):
            @property
            def name(self):
                return "RaisingValidator"

            def validate(self, df):
                raise RuntimeError("Validator error")

        runner = ValidationRunner(
            validators=[RaisingValidator()],
            mode=ValidationMode.FAIL,
        )

        with pytest.raises(ValidationException) as excinfo:
            runner.run(df)

        assert "exception" in str(excinfo.value).lower()

    # -------------------------------------------------------------------------
    # ValidationRunner Summary Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_summary_has_feature_group_name(self, spark):
        """Test ValidationRunner summary includes feature group name."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        runner = ValidationRunner(
            validators=[NullValidator(columns=["name"])],
            feature_group_name="test_feature_group",
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        assert summary.feature_group_name == "test_feature_group"

    def test_validation_runner_summary_passed_property(self, spark):
        """Test ValidationRunner summary passed property is correct."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"]),
                NullValidator(columns=["id"]),
            ],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        assert summary.passed is True

    def test_validation_runner_summary_failed_property(self, spark):
        """Test ValidationRunner summary passed is False when any fails."""
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"], max_null_percentage=0.0),  # Fails
                NullValidator(columns=["id"]),  # Passes
            ],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        assert summary.passed is False

    # -------------------------------------------------------------------------
    # ValidationRunner with Custom Validators Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_with_custom_validator(self, spark):
        """Test ValidationRunner works with CustomValidator."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        custom_v = CustomValidator(
            func=lambda df: df.count() > 0,
            name="non_empty_check",
        )

        runner = ValidationRunner(
            validators=[custom_v],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        assert summary.passed is True
        assert summary.results[0].validator_name == "non_empty_check"

    def test_validation_runner_with_mixed_validators(self, spark):
        """Test ValidationRunner with mix of built-in and custom validators."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"]),
                CustomValidator(func=lambda df: True, name="custom"),
                RangeValidator(column="id", min_val=0),
            ],
            mode=ValidationMode.WARN,
        )

        summary = runner.run(df)

        assert summary.total_validators == 3
        assert summary.passed_count == 3
        validator_names = [r.validator_name for r in summary.results]
        assert "NullValidator" in validator_names
        assert "custom" in validator_names
        assert "RangeValidator" in validator_names

    # -------------------------------------------------------------------------
    # ValidationRunner Logging Tests
    # -------------------------------------------------------------------------

    def test_validation_runner_logs_passed_validation(self, spark, caplog):
        """Test ValidationRunner logs passed validations."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        runner = ValidationRunner(
            validators=[NullValidator(columns=["name"])],
            mode=ValidationMode.WARN,
        )

        import logging
        with caplog.at_level(logging.INFO):
            runner.run(df)

        assert "PASSED" in caplog.text

    def test_validation_runner_logs_failed_validation(self, spark, caplog):
        """Test ValidationRunner logs failed validations."""
        df = create_test_df_with_nulls(spark, null_percentage=0.2)

        runner = ValidationRunner(
            validators=[NullValidator(columns=["name"], max_null_percentage=0.0)],
            mode=ValidationMode.WARN,
        )

        import logging
        with caplog.at_level(logging.WARNING):
            runner.run(df)

        assert "FAILED" in caplog.text

    def test_validation_runner_logs_summary(self, spark, caplog):
        """Test ValidationRunner logs summary at the end."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        runner = ValidationRunner(
            validators=[
                NullValidator(columns=["name"]),
                NullValidator(columns=["id"]),
            ],
            mode=ValidationMode.WARN,
        )

        import logging
        with caplog.at_level(logging.INFO):
            runner.run(df)

        # Should log "2/2 validators passed"
        assert "2/2" in caplog.text or "validators passed" in caplog.text

    def test_validation_runner_logs_with_feature_group_prefix(self, spark, caplog):
        """Test ValidationRunner logs include feature group name prefix."""
        df = create_test_df_with_nulls(spark, null_percentage=0.0)

        runner = ValidationRunner(
            validators=[NullValidator(columns=["name"])],
            feature_group_name="my_feature_group",
            mode=ValidationMode.WARN,
        )

        import logging
        with caplog.at_level(logging.INFO):
            runner.run(df)

        assert "my_feature_group" in caplog.text

    # -------------------------------------------------------------------------
    # ValidationException Tests
    # -------------------------------------------------------------------------

    def test_validation_exception_initialization(self, spark):
        """Test ValidationException initializes correctly."""
        result = ValidationResult(
            validator_name="test",
            passed=False,
            failure_count=5,
            total_count=10,
            message="Test failed",
        )

        exception = ValidationException("Validation failed", result=result)

        assert exception.message == "Validation failed"
        assert exception.result is result
        assert str(exception) == "Validation failed"

    def test_validation_exception_without_result(self, spark):
        """Test ValidationException works without result."""
        exception = ValidationException("Simple error")

        assert exception.message == "Simple error"
        assert exception.result is None


# Legacy tests for backward compatibility
def test_custom_validator_simple_usage(spark):
    """Test basic CustomValidator usage pattern."""
    df = create_test_df_with_nulls(spark, null_percentage=0.0)

    validator = CustomValidator(
        func=lambda df: df.count() > 0,
        name="non_empty_check",
    )

    result = validator.validate(df)

    assert result.passed is True
    assert result.validator_name == "non_empty_check"


def test_validation_runner_simple_usage(spark):
    """Test basic ValidationRunner usage pattern."""
    df = create_test_df_with_nulls(spark, null_percentage=0.0)

    runner = ValidationRunner(
        validators=[NullValidator(columns=["name"])],
        mode=ValidationMode.WARN,
    )

    summary = runner.run(df)

    assert summary.passed is True
    assert summary.total_validators == 1
