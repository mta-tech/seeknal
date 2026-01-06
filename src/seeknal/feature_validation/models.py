"""
Pydantic models for feature validation configuration and results.

This module provides data models for configuring validators and capturing
validation results in the Seeknal feature validation framework.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ValidationMode(str, Enum):
    """
    Validation execution mode.

    Attributes:
        WARN: Log validation failures but continue execution.
        FAIL: Raise exception and halt on first validation failure.
    """

    WARN = "warn"
    FAIL = "fail"


class ValidationResult(BaseModel):
    """
    Result of a single validation operation.

    This model captures the outcome of running a validator against a DataFrame,
    including pass/fail status, counts, and detailed information about any issues.

    Attributes:
        validator_name (str): Name of the validator that produced this result.
        passed (bool): Whether the validation passed (True) or failed (False).
        failure_count (int): Number of records that failed validation.
        total_count (int): Total number of records validated.
        message (str): Human-readable message describing the validation outcome.
        details (dict, optional): Additional details about the validation
            (e.g., column names, thresholds, specific values).
        timestamp (datetime): When the validation was performed.

    Example:
        >>> result = ValidationResult(
        ...     validator_name="NullValidator",
        ...     passed=False,
        ...     failure_count=10,
        ...     total_count=100,
        ...     message="Column 'age' has 10% null values, exceeds threshold of 5%",
        ...     details={"column": "age", "null_percentage": 0.10, "threshold": 0.05}
        ... )
    """

    validator_name: str
    passed: bool
    failure_count: int = 0
    total_count: int = 0
    message: str
    details: Optional[Dict[str, Any]] = None
    timestamp: datetime = Field(default_factory=datetime.now)

    model_config = {
        "use_enum_values": True
    }


class ValidatorConfig(BaseModel):
    """
    Configuration for a single validator.

    This model defines the configuration for a validator instance,
    including its type, target columns, and specific parameters.

    Attributes:
        validator_type (str): Type of validator (e.g., "null", "range", "uniqueness", "freshness").
        columns (list, optional): List of column names to validate.
        params (dict, optional): Additional parameters for the validator.

    Example:
        >>> config = ValidatorConfig(
        ...     validator_type="null",
        ...     columns=["age", "name"],
        ...     params={"max_null_percentage": 0.05}
        ... )
    """

    validator_type: str
    columns: Optional[List[str]] = None
    params: Optional[Dict[str, Any]] = None

    model_config = {
        "use_enum_values": True
    }


class ValidationConfig(BaseModel):
    """
    Configuration for validation execution.

    This model defines the overall validation configuration including
    the execution mode and list of validators to run.

    Attributes:
        mode (ValidationMode): Execution mode - "warn" continues on failures,
            "fail" halts on first failure. Default is "fail".
        validators (list): List of validator configurations to execute.
        enabled (bool): Whether validation is enabled. Default is True.

    Example:
        >>> config = ValidationConfig(
        ...     mode=ValidationMode.WARN,
        ...     validators=[
        ...         ValidatorConfig(validator_type="null", columns=["age"]),
        ...         ValidatorConfig(validator_type="range", columns=["score"],
        ...                         params={"min_val": 0, "max_val": 100})
        ...     ]
        ... )
    """

    mode: ValidationMode = ValidationMode.FAIL
    validators: List[ValidatorConfig] = Field(default_factory=list)
    enabled: bool = True

    model_config = {
        "use_enum_values": True
    }


class ValidationSummary(BaseModel):
    """
    Summary of all validation results for a feature group.

    This model aggregates results from multiple validators into a single
    summary with overall pass/fail status.

    Attributes:
        feature_group_name (str, optional): Name of the feature group validated.
        passed (bool): Whether all validations passed.
        total_validators (int): Total number of validators executed.
        passed_count (int): Number of validators that passed.
        failed_count (int): Number of validators that failed.
        results (list): List of individual ValidationResult objects.
        timestamp (datetime): When the validation summary was created.

    Example:
        >>> summary = ValidationSummary(
        ...     feature_group_name="user_features",
        ...     passed=False,
        ...     total_validators=3,
        ...     passed_count=2,
        ...     failed_count=1,
        ...     results=[result1, result2, result3]
        ... )
    """

    feature_group_name: Optional[str] = None
    passed: bool = True
    total_validators: int = 0
    passed_count: int = 0
    failed_count: int = 0
    results: List[ValidationResult] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)

    model_config = {
        "use_enum_values": True
    }

    def add_result(self, result: ValidationResult) -> None:
        """
        Add a validation result to the summary and update counts.

        Args:
            result (ValidationResult): The validation result to add.
        """
        self.results.append(result)
        self.total_validators += 1
        if result.passed:
            self.passed_count += 1
        else:
            self.failed_count += 1
            self.passed = False
