"""
Feature Validation Framework for Seeknal Feature Store.

This module provides a comprehensive validation framework for feature groups,
enabling data quality checks before materialization. The framework supports
built-in validators (null, range, uniqueness, freshness) and custom validation
functions.

Usage:
    from seeknal.feature_validation import (
        ValidationResult,
        ValidationConfig,
        ValidationMode,
        ValidationSummary,
    )

    # Create a validation configuration
    config = ValidationConfig(
        mode=ValidationMode.WARN,
        validators=[
            ValidatorConfig(validator_type="null", columns=["age"]),
        ]
    )

    # Check validation results
    result = ValidationResult(
        validator_name="NullValidator",
        passed=True,
        total_count=100,
        message="All null checks passed"
    )
"""

from seeknal.feature_validation.models import (
    ValidationConfig,
    ValidationMode,
    ValidationResult,
    ValidationSummary,
    ValidatorConfig,
)

__all__ = [
    "ValidationResult",
    "ValidationConfig",
    "ValidationMode",
    "ValidationSummary",
    "ValidatorConfig",
]
