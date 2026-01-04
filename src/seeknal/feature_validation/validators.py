"""
Base validator classes and validation execution engine for feature validation.

This module provides the abstract base class for all validators and the
ValidationRunner class for executing multiple validators in sequence.

Usage:
    from seeknal.feature_validation.validators import BaseValidator, ValidationRunner
    from seeknal.feature_validation.models import ValidationResult, ValidationMode

    # Create custom validator by extending BaseValidator
    class MyValidator(BaseValidator):
        def __init__(self, column: str):
            self.column = column
            self._name = "MyValidator"

        @property
        def name(self) -> str:
            return self._name

        def validate(self, df: DataFrame) -> ValidationResult:
            # Validation logic here
            return ValidationResult(
                validator_name=self.name,
                passed=True,
                message="Validation passed"
            )

    # Use ValidationRunner to execute multiple validators
    runner = ValidationRunner(validators=[MyValidator("col1")], mode=ValidationMode.FAIL)
    summary = runner.run(df)
"""

from abc import ABC, abstractmethod
from typing import List, Optional

from pyspark.sql import DataFrame

from ..context import logger
from .models import ValidationMode, ValidationResult, ValidationSummary


class ValidationException(Exception):
    """
    Exception raised when validation fails in FAIL mode.

    This exception is raised by ValidationRunner when a validator fails
    and the mode is set to FAIL.

    Attributes:
        message (str): Description of the validation failure.
        result (ValidationResult): The validation result that caused the failure.
    """

    def __init__(self, message: str, result: Optional[ValidationResult] = None):
        """
        Initialize ValidationException.

        Args:
            message (str): Description of the validation failure.
            result (ValidationResult, optional): The validation result that caused the failure.
        """
        super().__init__(message)
        self.message = message
        self.result = result


class BaseValidator(ABC):
    """
    Abstract base class for all validators.

    All validators must inherit from this class and implement the
    `validate` method. Validators are designed to be stateless and
    reusable across multiple DataFrames.

    Attributes:
        name (str): Name of the validator (abstract property).

    Example:
        >>> class NullValidator(BaseValidator):
        ...     def __init__(self, columns: List[str]):
        ...         self.columns = columns
        ...         self._name = "NullValidator"
        ...
        ...     @property
        ...     def name(self) -> str:
        ...         return self._name
        ...
        ...     def validate(self, df: DataFrame) -> ValidationResult:
        ...         # Implementation here
        ...         pass
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Return the name of the validator.

        This property should return a human-readable name for the validator,
        which will be used in logging and validation results.

        Returns:
            str: The name of the validator.
        """
        pass

    @abstractmethod
    def validate(self, df: DataFrame) -> ValidationResult:
        """
        Validate the given DataFrame.

        This method must be implemented by all concrete validator classes.
        It should perform the validation logic and return a ValidationResult
        with the outcome.

        Args:
            df (DataFrame): The PySpark DataFrame to validate.

        Returns:
            ValidationResult: The result of the validation, including
                pass/fail status, counts, and messages.

        Raises:
            ValueError: If required columns are missing from the DataFrame.
        """
        pass

    def _check_columns_exist(self, df: DataFrame, columns: List[str]) -> None:
        """
        Check that all specified columns exist in the DataFrame.

        This helper method should be called before performing validation
        to ensure the required columns are present.

        Args:
            df (DataFrame): The PySpark DataFrame to check.
            columns (List[str]): List of column names to verify.

        Raises:
            ValueError: If any column is not found in the DataFrame.
        """
        df_columns = set(df.columns)
        missing_columns = [col for col in columns if col not in df_columns]
        if missing_columns:
            raise ValueError(
                f"Column(s) not found in DataFrame: {missing_columns}. "
                f"Available columns: {list(df_columns)}"
            )

    def _get_total_count(self, df: DataFrame) -> int:
        """
        Get the total row count of the DataFrame.

        Uses efficient Spark count operation.

        Args:
            df (DataFrame): The PySpark DataFrame to count.

        Returns:
            int: The total number of rows in the DataFrame.
        """
        return df.count()


class ValidationRunner:
    """
    Executes multiple validators in sequence on a DataFrame.

    The ValidationRunner manages the execution of validators and handles
    the validation mode (WARN or FAIL). In WARN mode, all validators are
    executed even if some fail. In FAIL mode, execution stops at the first
    failure.

    Attributes:
        validators (List[BaseValidator]): List of validators to execute.
        mode (ValidationMode): Execution mode (WARN or FAIL).
        feature_group_name (str, optional): Name of the feature group being validated.

    Example:
        >>> from seeknal.feature_validation.validators import ValidationRunner
        >>> from seeknal.feature_validation.models import ValidationMode
        >>>
        >>> # Create validators
        >>> validators = [NullValidator(["col1"]), RangeValidator("col2", 0, 100)]
        >>>
        >>> # Run in WARN mode (continues on failures)
        >>> runner = ValidationRunner(validators, mode=ValidationMode.WARN)
        >>> summary = runner.run(df)
        >>> print(f"Passed: {summary.passed}, Failed: {summary.failed_count}")
        >>>
        >>> # Run in FAIL mode (stops on first failure)
        >>> runner = ValidationRunner(validators, mode=ValidationMode.FAIL)
        >>> try:
        ...     summary = runner.run(df)
        ... except ValidationException as e:
        ...     print(f"Validation failed: {e.message}")
    """

    def __init__(
        self,
        validators: Optional[List[BaseValidator]] = None,
        mode: ValidationMode = ValidationMode.FAIL,
        feature_group_name: Optional[str] = None,
    ):
        """
        Initialize ValidationRunner.

        Args:
            validators (List[BaseValidator], optional): List of validators to execute.
                Defaults to empty list.
            mode (ValidationMode, optional): Execution mode. Defaults to FAIL.
            feature_group_name (str, optional): Name of the feature group being validated.
                Used in logging and summary.
        """
        self.validators = validators or []
        self.mode = mode if isinstance(mode, ValidationMode) else ValidationMode(mode)
        self.feature_group_name = feature_group_name

    def add_validator(self, validator: BaseValidator) -> "ValidationRunner":
        """
        Add a validator to the runner.

        Args:
            validator (BaseValidator): The validator to add.

        Returns:
            ValidationRunner: Self for method chaining.
        """
        self.validators.append(validator)
        return self

    def run(self, df: DataFrame) -> ValidationSummary:
        """
        Execute all validators on the given DataFrame.

        Runs each validator in sequence and collects results. The behavior
        on failure depends on the mode:
        - WARN: Log failures but continue to next validator
        - FAIL: Raise ValidationException on first failure

        Args:
            df (DataFrame): The PySpark DataFrame to validate.

        Returns:
            ValidationSummary: Summary of all validation results.

        Raises:
            ValidationException: If mode is FAIL and any validator fails.
        """
        summary = ValidationSummary(feature_group_name=self.feature_group_name)

        if not self.validators:
            logger.warning("No validators configured. Skipping validation.")
            return summary

        # Handle empty DataFrame
        row_count = df.count()
        if row_count == 0:
            logger.warning("DataFrame is empty. Validation may produce unexpected results.")

        for validator in self.validators:
            try:
                result = validator.validate(df)
                summary.add_result(result)

                # Log the result
                self._log_result(result)

                # Handle failure based on mode
                if not result.passed:
                    if self.mode == ValidationMode.FAIL:
                        raise ValidationException(
                            f"Validation failed: {result.message}",
                            result=result,
                        )

            except ValidationException:
                # Re-raise ValidationException as-is
                raise
            except Exception as e:
                # Handle unexpected errors in validators
                error_result = ValidationResult(
                    validator_name=validator.name,
                    passed=False,
                    failure_count=0,
                    total_count=0,
                    message=f"Validator raised exception: {str(e)}",
                    details={"error": str(e), "error_type": type(e).__name__},
                )
                summary.add_result(error_result)
                self._log_result(error_result)

                if self.mode == ValidationMode.FAIL:
                    raise ValidationException(
                        f"Validator '{validator.name}' raised exception: {str(e)}",
                        result=error_result,
                    )

        # Log summary
        self._log_summary(summary)

        return summary

    def _log_result(self, result: ValidationResult) -> None:
        """
        Log a validation result.

        Args:
            result (ValidationResult): The result to log.
        """
        fg_prefix = f"[{self.feature_group_name}] " if self.feature_group_name else ""
        if result.passed:
            logger.info(
                f"{fg_prefix}Validation PASSED: {result.validator_name} - {result.message}"
            )
        else:
            logger.warning(
                f"{fg_prefix}Validation FAILED: {result.validator_name} - {result.message}"
            )
            if result.details:
                logger.debug(f"{fg_prefix}Failure details: {result.details}")

    def _log_summary(self, summary: ValidationSummary) -> None:
        """
        Log validation summary.

        Args:
            summary (ValidationSummary): The summary to log.
        """
        fg_prefix = f"[{self.feature_group_name}] " if self.feature_group_name else ""
        status = "PASSED" if summary.passed else "FAILED"
        logger.info(
            f"{fg_prefix}Validation {status}: "
            f"{summary.passed_count}/{summary.total_validators} validators passed"
        )
