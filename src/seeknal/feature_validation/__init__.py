"""
Feature Validation Framework for Seeknal Feature Store.

This module provides a comprehensive validation framework for feature groups,
enabling data quality checks before materialization. The framework supports
built-in validators (null, range, uniqueness, freshness) and custom validation
functions.

Classes:
    Validators:
        BaseValidator: Abstract base class for all validators.
        NullValidator: Validates null/missing values in DataFrame columns.
        RangeValidator: Validates numeric values are within specified bounds.
        UniquenessValidator: Validates uniqueness of values in columns.
        FreshnessValidator: Validates timestamp recency in columns.
        CustomValidator: Wrapper for user-defined validation functions.

    Execution:
        ValidationRunner: Executes multiple validators in sequence.
        ValidationException: Exception raised when validation fails in FAIL mode.

    Models:
        ValidationMode: Enum for validation execution mode (WARN or FAIL).
        ValidationResult: Result of a single validation operation.
        ValidationConfig: Configuration for validation execution.
        ValidatorConfig: Configuration for a single validator.
        ValidationSummary: Summary of all validation results.

Example:
    Basic usage with built-in validators::

        from seeknal.feature_validation import (
            NullValidator,
            RangeValidator,
            ValidationRunner,
            ValidationMode,
        )

        # Create validators
        null_validator = NullValidator(
            columns=["user_id", "email"],
            max_null_percentage=0.0  # No nulls allowed
        )

        range_validator = RangeValidator(
            column="age",
            min_val=0,
            max_val=120
        )

        # Create runner and execute validations
        runner = ValidationRunner(
            validators=[null_validator, range_validator],
            mode=ValidationMode.WARN
        )
        summary = runner.run(df)

        # Check results
        if not summary.passed:
            print(f"Validation failed: {summary.failed_count} validators failed")

    Using custom validators::

        from seeknal.feature_validation import CustomValidator

        # Define custom validation function
        def check_positive_values(df):
            from pyspark.sql import functions as F
            return df.filter(F.col("amount") < 0).count() == 0

        validator = CustomValidator(
            func=check_positive_values,
            name="positive_amount_check",
            description="Ensures all amounts are non-negative"
        )
        result = validator.validate(df)

    Using configuration objects::

        from seeknal.feature_validation import (
            ValidationConfig,
            ValidatorConfig,
            ValidationMode,
        )

        config = ValidationConfig(
            mode=ValidationMode.FAIL,
            validators=[
                ValidatorConfig(
                    validator_type="null",
                    columns=["user_id"],
                    params={"max_null_percentage": 0.0}
                ),
                ValidatorConfig(
                    validator_type="range",
                    columns=["score"],
                    params={"min_val": 0, "max_val": 100}
                ),
            ]
        )
"""

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

__all__ = [
    # Models
    "ValidationResult",
    "ValidationConfig",
    "ValidationMode",
    "ValidationSummary",
    "ValidatorConfig",
    # Validators
    "BaseValidator",
    "NullValidator",
    "RangeValidator",
    "UniquenessValidator",
    "FreshnessValidator",
    "CustomValidator",
    # Execution
    "ValidationRunner",
    "ValidationException",
]
