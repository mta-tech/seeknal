"""
Integration tests for feature validation in FeatureGroup.

This module contains integration tests for the FeatureGroup.validate() method
and its integration with the validation framework.
"""

from datetime import datetime, timedelta
import logging

import pytest
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from seeknal.featurestore.feature_group import FeatureGroup
from seeknal.feature_validation.models import (
    ValidationConfig,
    ValidationMode,
    ValidationResult,
    ValidationSummary,
    ValidatorConfig,
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


def create_feature_df_with_nulls(spark, null_count: int = 2) -> DataFrame:
    """
    Create a test DataFrame with null values suitable for FeatureGroup testing.

    Args:
        spark: SparkSession fixture.
        null_count: Number of rows with null values.

    Returns:
        DataFrame with feature-like columns including nulls.
    """
    data = []
    total_rows = 10

    # Add valid rows
    for i in range(total_rows - null_count):
        data.append((
            f"user_{i}",
            i + 20,  # age
            float(i * 1000),  # income
            f"category_{i % 3}",  # category
        ))

    # Add rows with null values
    for i in range(null_count):
        data.append((
            f"user_{total_rows - null_count + i}",
            None,  # null age
            None,  # null income
            None,  # null category
        ))

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("age", IntegerType(), True),
        StructField("income", DoubleType(), True),
        StructField("category", StringType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_feature_df_with_range_issues(spark) -> DataFrame:
    """
    Create a test DataFrame with values outside expected ranges.

    Returns:
        DataFrame with age values outside valid range [0, 120].
    """
    data = [
        ("user_1", 25, 50000.0),
        ("user_2", 35, 65000.0),
        ("user_3", -5, 30000.0),   # Invalid: negative age
        ("user_4", 45, 75000.0),
        ("user_5", 150, 85000.0),  # Invalid: age > 120
        ("user_6", 55, 55000.0),
    ]

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("age", IntegerType(), True),
        StructField("income", DoubleType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_feature_df_with_duplicates(spark) -> DataFrame:
    """
    Create a test DataFrame with duplicate user_ids.

    Returns:
        DataFrame with duplicate records for uniqueness testing.
    """
    data = [
        ("user_1", 25, 50000.0),
        ("user_2", 35, 65000.0),
        ("user_1", 26, 52000.0),  # Duplicate user_id
        ("user_3", 45, 75000.0),
        ("user_2", 36, 67000.0),  # Duplicate user_id
    ]

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("age", IntegerType(), True),
        StructField("income", DoubleType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_feature_df_with_timestamps(
    spark,
    reference_time: datetime = None,
    stale_count: int = 0,
) -> DataFrame:
    """
    Create a test DataFrame with timestamp column for freshness testing.

    Args:
        spark: SparkSession fixture.
        reference_time: Reference time for freshness calculation.
        stale_count: Number of stale records to include.

    Returns:
        DataFrame with event_time column for freshness validation.
    """
    if reference_time is None:
        reference_time = datetime.now()

    data = [
        ("user_1", reference_time - timedelta(hours=1)),  # Fresh
        ("user_2", reference_time - timedelta(hours=2)),  # Fresh
        ("user_3", reference_time - timedelta(hours=3)),  # Fresh
    ]

    # Add stale records
    for i in range(stale_count):
        stale_time = reference_time - timedelta(days=10 + i)
        data.append((f"user_stale_{i}", stale_time))

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("event_time", TimestampType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_clean_feature_df(spark) -> DataFrame:
    """
    Create a clean test DataFrame with no validation issues.

    Returns:
        DataFrame with valid data for all validators.
    """
    data = [
        ("user_1", 25, 50000.0, "A"),
        ("user_2", 35, 65000.0, "B"),
        ("user_3", 45, 75000.0, "C"),
        ("user_4", 55, 85000.0, "A"),
        ("user_5", 65, 95000.0, "B"),
    ]

    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("age", IntegerType(), True),
        StructField("income", DoubleType(), True),
        StructField("category", StringType(), True),
    ])

    return spark.createDataFrame(data, schema)


# =============================================================================
# Test Class: FeatureGroup.validate() Method
# =============================================================================


class TestFeatureGroupValidateMethod:
    """Tests for FeatureGroup.validate() method."""

    def test_validate_executes_single_validator(self, spark):
        """Test that FeatureGroup.validate() executes a single validator."""
        # Create DataFrame with null values
        df = create_feature_df_with_nulls(spark, null_count=2)

        # Create FeatureGroup with DataFrame source
        fg = FeatureGroup(name="test_fg_single_validator")
        fg.source = df

        # Create validator
        validators = [NullValidator(columns=["age"])]

        # Run validation in WARN mode
        summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Verify results
        assert isinstance(summary, ValidationSummary)
        assert summary.total_validators == 1
        assert len(summary.results) == 1
        assert summary.results[0].validator_name == "NullValidator"

    def test_validate_executes_multiple_validators(self, spark):
        """Test that FeatureGroup.validate() executes multiple validators in sequence."""
        # Create DataFrame with multiple issues
        df = create_feature_df_with_nulls(spark, null_count=2)

        # Create FeatureGroup
        fg = FeatureGroup(name="test_fg_multi_validator")
        fg.source = df

        # Create multiple validators
        validators = [
            NullValidator(columns=["age"]),
            NullValidator(columns=["income"]),
            NullValidator(columns=["category"]),
        ]

        # Run validation in WARN mode
        summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Verify all validators ran
        assert summary.total_validators == 3
        assert len(summary.results) == 3

        # Verify validators ran in order
        assert summary.results[0].validator_name == "NullValidator"
        assert summary.results[1].validator_name == "NullValidator"
        assert summary.results[2].validator_name == "NullValidator"

    def test_validate_returns_validation_summary(self, spark):
        """Test that FeatureGroup.validate() returns a ValidationSummary."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="test_fg_summary")
        fg.source = df

        validators = [NullValidator(columns=["age"])]
        summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Verify summary structure
        assert isinstance(summary, ValidationSummary)
        assert summary.feature_group_name == "test_fg_summary"
        assert isinstance(summary.passed, bool)
        assert isinstance(summary.total_validators, int)
        assert isinstance(summary.passed_count, int)
        assert isinstance(summary.failed_count, int)
        assert isinstance(summary.results, list)

    def test_validate_passes_with_clean_data(self, spark):
        """Test that validation passes with clean data."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="test_fg_clean")
        fg.source = df

        validators = [
            NullValidator(columns=["age", "income", "category"]),
            RangeValidator(column="age", min_val=0, max_val=120),
            UniquenessValidator(columns=["user_id"]),
        ]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        assert summary.passed is True
        assert summary.passed_count == 3
        assert summary.failed_count == 0

    def test_validate_fails_with_null_values(self, spark):
        """Test that validation fails when null values exceed threshold."""
        df = create_feature_df_with_nulls(spark, null_count=3)

        fg = FeatureGroup(name="test_fg_nulls")
        fg.source = df

        validators = [NullValidator(columns=["age"], max_null_percentage=0.0)]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        assert summary.passed is False
        assert summary.failed_count == 1
        assert summary.results[0].passed is False

    def test_validate_fails_with_range_violations(self, spark):
        """Test that validation fails when values are out of range."""
        df = create_feature_df_with_range_issues(spark)

        fg = FeatureGroup(name="test_fg_range")
        fg.source = df

        validators = [RangeValidator(column="age", min_val=0, max_val=120)]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        assert summary.passed is False
        assert summary.results[0].passed is False
        assert summary.results[0].failure_count == 2  # -5 and 150

    def test_validate_fails_with_duplicates(self, spark):
        """Test that validation fails when duplicates exist."""
        df = create_feature_df_with_duplicates(spark)

        fg = FeatureGroup(name="test_fg_duplicates")
        fg.source = df

        validators = [UniquenessValidator(columns=["user_id"])]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        assert summary.passed is False
        assert summary.results[0].passed is False
        assert summary.results[0].failure_count == 2  # 2 duplicate rows

    def test_validate_with_custom_validator(self, spark):
        """Test that FeatureGroup.validate() works with custom validators."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="test_fg_custom")
        fg.source = df

        # Custom validator that checks minimum row count
        def check_min_rows(df: DataFrame) -> bool:
            return df.count() >= 5

        validators = [
            CustomValidator(func=check_min_rows, name="min_rows_check"),
        ]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        assert summary.passed is True
        assert summary.results[0].validator_name == "min_rows_check"


class TestValidationModes:
    """Tests for validation mode behavior in FeatureGroup.validate()."""

    def test_warn_mode_continues_on_failure(self, spark):
        """Test that WARN mode continues executing validators after failure."""
        df = create_feature_df_with_nulls(spark, null_count=3)

        fg = FeatureGroup(name="test_fg_warn")
        fg.source = df

        # First validator will fail, second should still run
        validators = [
            NullValidator(columns=["age"], max_null_percentage=0.0),
            NullValidator(columns=["income"], max_null_percentage=0.0),
        ]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Both validators should have run
        assert summary.total_validators == 2
        assert summary.failed_count == 2

    def test_fail_mode_raises_exception_on_failure(self, spark):
        """Test that FAIL mode raises exception on first validation failure."""
        df = create_feature_df_with_nulls(spark, null_count=3)

        fg = FeatureGroup(name="test_fg_fail")
        fg.source = df

        validators = [
            NullValidator(columns=["age"], max_null_percentage=0.0),
            NullValidator(columns=["income"], max_null_percentage=0.0),
        ]

        # Should raise ValidationException on first failure
        with pytest.raises(ValidationException) as exc_info:
            fg.validate(validators, mode=ValidationMode.FAIL)

        assert "Validation failed" in str(exc_info.value.message)

    def test_fail_mode_stops_on_first_failure(self, spark):
        """Test that FAIL mode stops execution after first failure."""
        df = create_feature_df_with_nulls(spark, null_count=3)

        fg = FeatureGroup(name="test_fg_fail_stop")
        fg.source = df

        # Create a validator that will track if it was called
        called = {"second_validator": False}

        def second_check(df: DataFrame) -> bool:
            called["second_validator"] = True
            return True

        validators = [
            NullValidator(columns=["age"], max_null_percentage=0.0),  # Will fail
            CustomValidator(func=second_check, name="second"),  # Should not run
        ]

        try:
            fg.validate(validators, mode=ValidationMode.FAIL)
        except ValidationException:
            pass

        # Second validator should not have been called
        assert called["second_validator"] is False

    def test_mode_string_conversion(self, spark):
        """Test that mode string is properly converted to ValidationMode."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="test_fg_mode_str")
        fg.source = df

        validators = [NullValidator(columns=["age"])]

        # Test with string modes
        summary_warn = fg.validate(validators, mode="warn")
        assert summary_warn.passed is True

        summary_fail = fg.validate(validators, mode="fail")
        assert summary_fail.passed is True


class TestValidationLogging:
    """Tests for validation result logging."""

    def test_validation_results_logged(self, spark, caplog):
        """Test that validation results are logged."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="test_fg_logging")
        fg.source = df

        validators = [NullValidator(columns=["age"])]

        with caplog.at_level(logging.INFO):
            summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Check that validation completion is logged
        assert any("validation complete" in record.message.lower() for record in caplog.records)

    def test_validation_failure_logged_as_warning(self, spark, caplog):
        """Test that validation failures are logged as warnings."""
        df = create_feature_df_with_nulls(spark, null_count=3)

        fg = FeatureGroup(name="test_fg_warning_log")
        fg.source = df

        validators = [NullValidator(columns=["age"], max_null_percentage=0.0)]

        with caplog.at_level(logging.WARNING):
            summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Check that failure is logged as warning
        assert any("failed" in record.message.lower() for record in caplog.records)

    def test_feature_group_name_in_logs(self, spark, caplog):
        """Test that feature group name appears in validation logs."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="my_feature_group_for_logging")
        fg.source = df

        validators = [NullValidator(columns=["age"])]

        with caplog.at_level(logging.INFO):
            summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Check that feature group name appears in logs
        assert any("my_feature_group_for_logging" in record.message for record in caplog.records)


class TestValidationWithMixedValidators:
    """Tests for validation with multiple validator types."""

    def test_mixed_validators_all_pass(self, spark):
        """Test that multiple validator types work together when all pass."""
        reference_time = datetime.now()

        # Create DataFrame with timestamps
        data = [
            ("user_1", 25, 50000.0, reference_time - timedelta(hours=1)),
            ("user_2", 35, 65000.0, reference_time - timedelta(hours=2)),
            ("user_3", 45, 75000.0, reference_time - timedelta(hours=3)),
        ]

        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("income", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ])

        df = spark.createDataFrame(data, schema)

        fg = FeatureGroup(name="test_fg_mixed")
        fg.source = df

        validators = [
            NullValidator(columns=["age", "income"]),
            RangeValidator(column="age", min_val=0, max_val=120),
            UniquenessValidator(columns=["user_id"]),
            FreshnessValidator(
                column="event_time",
                max_age=timedelta(days=1),
                reference_time=reference_time,
            ),
        ]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        assert summary.passed is True
        assert summary.total_validators == 4
        assert summary.passed_count == 4
        assert summary.failed_count == 0

    def test_mixed_validators_partial_failures(self, spark):
        """Test multiple validators with some passing and some failing."""
        reference_time = datetime.now()

        # Create DataFrame with some issues
        data = [
            ("user_1", 25, 50000.0, reference_time - timedelta(hours=1)),
            ("user_2", 35, None, reference_time - timedelta(hours=2)),  # Null income
            ("user_3", 150, 75000.0, reference_time - timedelta(hours=3)),  # Invalid age
        ]

        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("income", DoubleType(), True),
            StructField("event_time", TimestampType(), True),
        ])

        df = spark.createDataFrame(data, schema)

        fg = FeatureGroup(name="test_fg_mixed_partial")
        fg.source = df

        validators = [
            NullValidator(columns=["income"], max_null_percentage=0.0),  # Fail
            RangeValidator(column="age", min_val=0, max_val=120),  # Fail
            UniquenessValidator(columns=["user_id"]),  # Pass
        ]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        assert summary.passed is False
        assert summary.total_validators == 3
        assert summary.passed_count == 1
        assert summary.failed_count == 2


class TestValidationEdgeCases:
    """Tests for edge cases in FeatureGroup.validate()."""

    def test_validate_with_empty_dataframe(self, spark):
        """Test validation with empty DataFrame."""
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("age", IntegerType(), True),
        ])

        df = spark.createDataFrame([], schema)

        fg = FeatureGroup(name="test_fg_empty")
        fg.source = df

        validators = [NullValidator(columns=["age"])]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Empty DataFrame should pass (no nulls to find)
        assert summary.passed is True

    def test_validate_without_source_raises_error(self, spark):
        """Test that validation without source raises ValueError."""
        fg = FeatureGroup(name="test_fg_no_source")
        # Don't set source

        validators = [NullValidator(columns=["age"])]

        with pytest.raises(ValueError) as exc_info:
            fg.validate(validators, mode=ValidationMode.WARN)

        assert "Source is not set" in str(exc_info.value)

    def test_validate_with_empty_validators_list(self, spark):
        """Test validation with empty validators list."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="test_fg_no_validators")
        fg.source = df

        validators = []

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        # Should pass with no validators
        assert summary.passed is True
        assert summary.total_validators == 0

    def test_validate_with_missing_column(self, spark):
        """Test validation when validator references non-existent column."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="test_fg_missing_col")
        fg.source = df

        validators = [NullValidator(columns=["nonexistent_column"])]

        # Should raise ValidationException in FAIL mode
        with pytest.raises(ValidationException) as exc_info:
            fg.validate(validators, mode=ValidationMode.FAIL)

        assert "exception" in str(exc_info.value.message).lower()


class TestValidationConfigIntegration:
    """Tests for ValidationConfig integration with FeatureGroup."""

    def test_set_validation_config(self, spark):
        """Test that set_validation_config() works."""
        fg = FeatureGroup(name="test_fg_config")

        config = ValidationConfig(
            mode=ValidationMode.WARN,
            validators=[
                ValidatorConfig(validator_type="null", columns=["age"]),
            ],
        )

        result = fg.set_validation_config(config)

        # Should return self for chaining
        assert result is fg
        assert fg.validation_config is not None
        assert fg.validation_config.mode == ValidationMode.WARN
        assert len(fg.validation_config.validators) == 1

    def test_validation_config_fluent_api(self, spark):
        """Test fluent API with set_validation_config()."""
        df = create_clean_feature_df(spark)

        config = ValidationConfig(
            mode=ValidationMode.WARN,
            validators=[],
        )

        # Chain set_dataframe and set_validation_config
        fg = (
            FeatureGroup(name="test_fg_fluent")
            .set_dataframe(df)
            .set_validation_config(config)
        )

        assert fg.source is df
        assert fg.validation_config is config


class TestValidationResultDetails:
    """Tests for ValidationResult details in FeatureGroup.validate()."""

    def test_result_contains_failure_count(self, spark):
        """Test that validation results contain accurate failure count."""
        df = create_feature_df_with_nulls(spark, null_count=3)

        fg = FeatureGroup(name="test_fg_failure_count")
        fg.source = df

        validators = [NullValidator(columns=["age"], max_null_percentage=0.0)]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        result = summary.results[0]
        assert result.failure_count == 3
        assert result.total_count == 10

    def test_result_contains_validator_name(self, spark):
        """Test that validation results contain validator name."""
        df = create_clean_feature_df(spark)

        fg = FeatureGroup(name="test_fg_validator_name")
        fg.source = df

        validators = [
            NullValidator(columns=["age"]),
            RangeValidator(column="income", min_val=0, max_val=1000000),
        ]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        assert summary.results[0].validator_name == "NullValidator"
        assert summary.results[1].validator_name == "RangeValidator"

    def test_result_contains_details(self, spark):
        """Test that validation results contain detailed information."""
        df = create_feature_df_with_range_issues(spark)

        fg = FeatureGroup(name="test_fg_details")
        fg.source = df

        validators = [RangeValidator(column="age", min_val=0, max_val=120)]

        summary = fg.validate(validators, mode=ValidationMode.WARN)

        result = summary.results[0]
        assert result.details is not None
        assert "column" in result.details
        assert "min_val" in result.details
        assert "max_val" in result.details
        assert result.details["column"] == "age"
