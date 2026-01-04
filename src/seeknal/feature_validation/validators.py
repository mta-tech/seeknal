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
from datetime import datetime, timedelta
from typing import List, Optional, Union

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType, DateType

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


class NullValidator(BaseValidator):
    """
    Validator for detecting null/missing values in DataFrame columns.

    This validator checks specified columns for null values and compares
    the null percentage against a configurable threshold. It can validate
    multiple columns in a single pass.

    Attributes:
        columns (List[str]): List of column names to check for nulls.
        max_null_percentage (float): Maximum allowed percentage of null values
            (between 0.0 and 1.0). Default is 0.0 (no nulls allowed).

    Example:
        >>> # Check multiple columns, allow up to 5% nulls
        >>> validator = NullValidator(
        ...     columns=["age", "name", "email"],
        ...     max_null_percentage=0.05
        ... )
        >>> result = validator.validate(df)
        >>> if not result.passed:
        ...     print(f"Found {result.failure_count} null values")

        >>> # Strict mode: no nulls allowed (default)
        >>> validator = NullValidator(columns=["user_id"])
        >>> result = validator.validate(df)
    """

    def __init__(
        self,
        columns: List[str],
        max_null_percentage: float = 0.0,
    ):
        """
        Initialize NullValidator.

        Args:
            columns (List[str]): List of column names to check for null values.
            max_null_percentage (float, optional): Maximum allowed percentage of
                null values as a decimal (0.0 to 1.0). For example, 0.05 allows
                up to 5% null values. Default is 0.0 (no nulls allowed).

        Raises:
            ValueError: If columns list is empty or max_null_percentage is not
                between 0.0 and 1.0.
        """
        if not columns:
            raise ValueError("columns list cannot be empty")
        if not 0.0 <= max_null_percentage <= 1.0:
            raise ValueError(
                f"max_null_percentage must be between 0.0 and 1.0, got {max_null_percentage}"
            )

        self.columns = columns
        self.max_null_percentage = max_null_percentage
        self._name = "NullValidator"

    @property
    def name(self) -> str:
        """Return the name of this validator."""
        return self._name

    def validate(self, df: DataFrame) -> ValidationResult:
        """
        Validate the DataFrame for null values in specified columns.

        Counts null values across all specified columns and compares
        the percentage against the configured threshold.

        Args:
            df (DataFrame): The PySpark DataFrame to validate.

        Returns:
            ValidationResult: The result containing pass/fail status,
                null count, total count, and detailed information about
                which columns have nulls.

        Raises:
            ValueError: If any specified column is not found in the DataFrame.
        """
        # Check that all columns exist
        self._check_columns_exist(df, self.columns)

        # Get total row count
        total_count = self._get_total_count(df)

        # Handle empty DataFrame
        if total_count == 0:
            return ValidationResult(
                validator_name=self.name,
                passed=True,
                failure_count=0,
                total_count=0,
                message="DataFrame is empty, no null values to check",
                details={
                    "columns": self.columns,
                    "max_null_percentage": self.max_null_percentage,
                },
            )

        # Count nulls for each column
        column_null_counts = {}
        total_null_count = 0

        for col_name in self.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            column_null_counts[col_name] = null_count
            total_null_count += null_count

        # Calculate overall null percentage across all column checks
        # Total possible = rows * columns being checked
        total_checks = total_count * len(self.columns)
        null_percentage = total_null_count / total_checks if total_checks > 0 else 0.0

        # Calculate per-column percentages for details
        column_null_percentages = {
            col: count / total_count
            for col, count in column_null_counts.items()
        }

        # Find columns that exceed threshold
        columns_exceeding_threshold = [
            col for col, pct in column_null_percentages.items()
            if pct > self.max_null_percentage
        ]

        # Determine pass/fail
        passed = len(columns_exceeding_threshold) == 0

        # Build message
        if passed:
            message = (
                f"Null check passed for columns {self.columns}. "
                f"All columns within {self.max_null_percentage:.1%} threshold."
            )
        else:
            failing_details = ", ".join(
                f"{col}: {column_null_percentages[col]:.1%}"
                for col in columns_exceeding_threshold
            )
            message = (
                f"Null check failed. Columns exceeding {self.max_null_percentage:.1%} "
                f"threshold: {failing_details}"
            )

        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            failure_count=total_null_count,
            total_count=total_count,
            message=message,
            details={
                "columns": self.columns,
                "max_null_percentage": self.max_null_percentage,
                "column_null_counts": column_null_counts,
                "column_null_percentages": column_null_percentages,
                "columns_exceeding_threshold": columns_exceeding_threshold,
            },
        )


class RangeValidator(BaseValidator):
    """
    Validator for checking numeric values are within specified bounds.

    This validator checks that values in a specified column fall within
    a minimum and maximum range (inclusive). At least one of min_val
    or max_val must be provided.

    Attributes:
        column (str): Name of the column to validate.
        min_val (float, optional): Minimum allowed value (inclusive).
        max_val (float, optional): Maximum allowed value (inclusive).

    Example:
        >>> # Check age is between 0 and 120
        >>> validator = RangeValidator(
        ...     column="age",
        ...     min_val=0,
        ...     max_val=120
        ... )
        >>> result = validator.validate(df)
        >>> if not result.passed:
        ...     print(f"Found {result.failure_count} values out of range")

        >>> # Check only minimum bound
        >>> validator = RangeValidator(column="price", min_val=0)

        >>> # Check only maximum bound
        >>> validator = RangeValidator(column="percentage", max_val=100)
    """

    def __init__(
        self,
        column: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ):
        """
        Initialize RangeValidator.

        Args:
            column (str): Name of the column to validate.
            min_val (float, optional): Minimum allowed value (inclusive).
                If None, no lower bound is enforced.
            max_val (float, optional): Maximum allowed value (inclusive).
                If None, no upper bound is enforced.

        Raises:
            ValueError: If column is empty, both min_val and max_val are None,
                or min_val is greater than max_val.
        """
        if not column:
            raise ValueError("column cannot be empty")
        if min_val is None and max_val is None:
            raise ValueError("At least one of min_val or max_val must be provided")
        if min_val is not None and max_val is not None and min_val > max_val:
            raise ValueError(
                f"min_val ({min_val}) cannot be greater than max_val ({max_val})"
            )

        self.column = column
        self.min_val = min_val
        self.max_val = max_val
        self._name = "RangeValidator"

    @property
    def name(self) -> str:
        """Return the name of this validator."""
        return self._name

    def validate(self, df: DataFrame) -> ValidationResult:
        """
        Validate the DataFrame for values within the specified range.

        Checks that all non-null values in the specified column fall within
        the configured min/max bounds (inclusive). Null values are excluded
        from the range check but reported in details.

        Args:
            df (DataFrame): The PySpark DataFrame to validate.

        Returns:
            ValidationResult: The result containing pass/fail status,
                count of out-of-range values, total count, and detailed
                information about the violations.

        Raises:
            ValueError: If the specified column is not found in the DataFrame.
        """
        # Check that column exists
        self._check_columns_exist(df, [self.column])

        # Get total row count
        total_count = self._get_total_count(df)

        # Handle empty DataFrame
        if total_count == 0:
            return ValidationResult(
                validator_name=self.name,
                passed=True,
                failure_count=0,
                total_count=0,
                message="DataFrame is empty, no values to check",
                details={
                    "column": self.column,
                    "min_val": self.min_val,
                    "max_val": self.max_val,
                },
            )

        # Count null values (excluded from range check)
        null_count = df.filter(F.col(self.column).isNull()).count()
        non_null_count = total_count - null_count

        # Build range violation condition
        # Only check non-null values
        conditions = []
        if self.min_val is not None:
            conditions.append(F.col(self.column) < self.min_val)
        if self.max_val is not None:
            conditions.append(F.col(self.column) > self.max_val)

        # Combine conditions with OR (value is out of range if either condition is true)
        if len(conditions) == 1:
            out_of_range_condition = conditions[0]
        else:
            out_of_range_condition = conditions[0] | conditions[1]

        # Count values outside the range (excluding nulls)
        out_of_range_count = df.filter(
            F.col(self.column).isNotNull() & out_of_range_condition
        ).count()

        # Determine pass/fail
        passed = out_of_range_count == 0

        # Build range description for messages
        if self.min_val is not None and self.max_val is not None:
            range_desc = f"[{self.min_val}, {self.max_val}]"
        elif self.min_val is not None:
            range_desc = f">= {self.min_val}"
        else:
            range_desc = f"<= {self.max_val}"

        # Build message
        if passed:
            message = (
                f"Range check passed for column '{self.column}'. "
                f"All {non_null_count} non-null values within range {range_desc}."
            )
        else:
            percentage = (out_of_range_count / non_null_count * 100) if non_null_count > 0 else 0
            message = (
                f"Range check failed for column '{self.column}'. "
                f"{out_of_range_count} values ({percentage:.2f}%) outside range {range_desc}."
            )

        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            failure_count=out_of_range_count,
            total_count=total_count,
            message=message,
            details={
                "column": self.column,
                "min_val": self.min_val,
                "max_val": self.max_val,
                "out_of_range_count": out_of_range_count,
                "null_count": null_count,
                "non_null_count": non_null_count,
            },
        )


class UniquenessValidator(BaseValidator):
    """
    Validator for detecting duplicate values in DataFrame columns.

    This validator checks for duplicate records based on specified columns.
    It can check uniqueness on a single column or a combination of columns
    (composite key). The validator counts duplicate rows and can optionally
    allow a certain percentage of duplicates.

    Attributes:
        columns (List[str]): List of column names to check for uniqueness.
            If multiple columns are specified, the combination is checked.
        max_duplicate_percentage (float): Maximum allowed percentage of duplicate
            rows (between 0.0 and 1.0). Default is 0.0 (no duplicates allowed).

    Example:
        >>> # Check single column uniqueness
        >>> validator = UniquenessValidator(columns=["user_id"])
        >>> result = validator.validate(df)
        >>> if not result.passed:
        ...     print(f"Found {result.failure_count} duplicate rows")

        >>> # Check composite key uniqueness
        >>> validator = UniquenessValidator(
        ...     columns=["user_id", "timestamp"],
        ...     max_duplicate_percentage=0.01  # Allow up to 1% duplicates
        ... )
        >>> result = validator.validate(df)

        >>> # Allow some duplicates (e.g., for slowly changing dimensions)
        >>> validator = UniquenessValidator(
        ...     columns=["product_id"],
        ...     max_duplicate_percentage=0.05
        ... )
    """

    def __init__(
        self,
        columns: List[str],
        max_duplicate_percentage: float = 0.0,
    ):
        """
        Initialize UniquenessValidator.

        Args:
            columns (List[str]): List of column names to check for uniqueness.
                If multiple columns are provided, checks uniqueness of the
                combination (composite key).
            max_duplicate_percentage (float, optional): Maximum allowed percentage
                of duplicate rows as a decimal (0.0 to 1.0). For example, 0.05
                allows up to 5% duplicate rows. Default is 0.0 (no duplicates allowed).

        Raises:
            ValueError: If columns list is empty or max_duplicate_percentage is not
                between 0.0 and 1.0.
        """
        if not columns:
            raise ValueError("columns list cannot be empty")
        if not 0.0 <= max_duplicate_percentage <= 1.0:
            raise ValueError(
                f"max_duplicate_percentage must be between 0.0 and 1.0, got {max_duplicate_percentage}"
            )

        self.columns = columns
        self.max_duplicate_percentage = max_duplicate_percentage
        self._name = "UniquenessValidator"

    @property
    def name(self) -> str:
        """Return the name of this validator."""
        return self._name

    def validate(self, df: DataFrame) -> ValidationResult:
        """
        Validate the DataFrame for duplicate values in specified columns.

        Counts duplicate rows based on the specified columns and compares
        the percentage against the configured threshold. A row is considered
        a duplicate if there are multiple rows with the same values for all
        specified columns.

        Args:
            df (DataFrame): The PySpark DataFrame to validate.

        Returns:
            ValidationResult: The result containing pass/fail status,
                duplicate count, total count, and detailed information about
                the duplicates.

        Raises:
            ValueError: If any specified column is not found in the DataFrame.
        """
        # Check that all columns exist
        self._check_columns_exist(df, self.columns)

        # Get total row count
        total_count = self._get_total_count(df)

        # Handle empty DataFrame
        if total_count == 0:
            return ValidationResult(
                validator_name=self.name,
                passed=True,
                failure_count=0,
                total_count=0,
                message="DataFrame is empty, no duplicates to check",
                details={
                    "columns": self.columns,
                    "max_duplicate_percentage": self.max_duplicate_percentage,
                },
            )

        # Count unique combinations of the specified columns
        unique_count = df.select(self.columns).distinct().count()

        # Calculate duplicate rows
        # Duplicate rows = total rows - unique combinations
        duplicate_row_count = total_count - unique_count

        # Calculate duplicate percentage
        duplicate_percentage = duplicate_row_count / total_count if total_count > 0 else 0.0

        # Determine pass/fail based on threshold
        passed = duplicate_percentage <= self.max_duplicate_percentage

        # Build column description for messages
        if len(self.columns) == 1:
            col_desc = f"column '{self.columns[0]}'"
        else:
            col_desc = f"columns {self.columns}"

        # Build message
        if passed:
            if duplicate_row_count == 0:
                message = (
                    f"Uniqueness check passed for {col_desc}. "
                    f"All {total_count} rows are unique."
                )
            else:
                message = (
                    f"Uniqueness check passed for {col_desc}. "
                    f"{duplicate_row_count} duplicate rows ({duplicate_percentage:.2%}) "
                    f"within threshold of {self.max_duplicate_percentage:.2%}."
                )
        else:
            message = (
                f"Uniqueness check failed for {col_desc}. "
                f"{duplicate_row_count} duplicate rows ({duplicate_percentage:.2%}) "
                f"exceed threshold of {self.max_duplicate_percentage:.2%}."
            )

        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            failure_count=duplicate_row_count,
            total_count=total_count,
            message=message,
            details={
                "columns": self.columns,
                "max_duplicate_percentage": self.max_duplicate_percentage,
                "unique_count": unique_count,
                "duplicate_row_count": duplicate_row_count,
                "duplicate_percentage": duplicate_percentage,
            },
        )


class FreshnessValidator(BaseValidator):
    """
    Validator for checking timestamp recency in DataFrame columns.

    This validator checks that timestamp values in a specified column
    are within a maximum age from the current time. It is useful for
    ensuring that feature data is fresh and not stale.

    Attributes:
        column (str): Name of the timestamp column to validate.
        max_age (timedelta): Maximum allowed age of timestamp values.
        reference_time (datetime, optional): Reference time to compare against.
            If None, the current time at validation is used.

    Example:
        >>> from datetime import timedelta
        >>>
        >>> # Check timestamps are within last 24 hours
        >>> validator = FreshnessValidator(
        ...     column="event_time",
        ...     max_age=timedelta(hours=24)
        ... )
        >>> result = validator.validate(df)
        >>> if not result.passed:
        ...     print(f"Found {result.failure_count} stale records")
        >>>
        >>> # Check with custom reference time
        >>> validator = FreshnessValidator(
        ...     column="updated_at",
        ...     max_age=timedelta(days=7),
        ...     reference_time=datetime(2024, 1, 1, 12, 0, 0)
        ... )
        >>>
        >>> # Check for very recent data (last hour)
        >>> validator = FreshnessValidator(
        ...     column="created_at",
        ...     max_age=timedelta(hours=1)
        ... )
    """

    def __init__(
        self,
        column: str,
        max_age: timedelta,
        reference_time: Optional[datetime] = None,
    ):
        """
        Initialize FreshnessValidator.

        Args:
            column (str): Name of the timestamp column to validate.
            max_age (timedelta): Maximum allowed age of timestamp values.
                Values older than (reference_time - max_age) will fail validation.
            reference_time (datetime, optional): Reference time to compare against.
                If None, uses the current time when validate() is called.

        Raises:
            ValueError: If column is empty or max_age is not a positive timedelta.
        """
        if not column:
            raise ValueError("column cannot be empty")
        if not isinstance(max_age, timedelta):
            raise ValueError(
                f"max_age must be a timedelta instance, got {type(max_age).__name__}"
            )
        if max_age.total_seconds() <= 0:
            raise ValueError("max_age must be a positive timedelta")

        self.column = column
        self.max_age = max_age
        self.reference_time = reference_time
        self._name = "FreshnessValidator"

    @property
    def name(self) -> str:
        """Return the name of this validator."""
        return self._name

    def validate(self, df: DataFrame) -> ValidationResult:
        """
        Validate the DataFrame for timestamp freshness.

        Checks that all timestamp values in the specified column are
        within the configured max_age from the reference time. Null
        values are counted separately and excluded from the freshness check.

        Args:
            df (DataFrame): The PySpark DataFrame to validate.

        Returns:
            ValidationResult: The result containing pass/fail status,
                count of stale records, total count, and detailed information
                about the freshness violations.

        Raises:
            ValueError: If the specified column is not found in the DataFrame.
        """
        # Check that column exists
        self._check_columns_exist(df, [self.column])

        # Get total row count
        total_count = self._get_total_count(df)

        # Handle empty DataFrame
        if total_count == 0:
            return ValidationResult(
                validator_name=self.name,
                passed=True,
                failure_count=0,
                total_count=0,
                message="DataFrame is empty, no timestamps to check",
                details={
                    "column": self.column,
                    "max_age_seconds": self.max_age.total_seconds(),
                },
            )

        # Determine reference time
        ref_time = self.reference_time if self.reference_time else datetime.now()
        cutoff_time = ref_time - self.max_age

        # Count null values (excluded from freshness check)
        null_count = df.filter(F.col(self.column).isNull()).count()
        non_null_count = total_count - null_count

        # Handle case where all values are null
        if non_null_count == 0:
            return ValidationResult(
                validator_name=self.name,
                passed=True,
                failure_count=0,
                total_count=total_count,
                message=f"All values in column '{self.column}' are null, no freshness check performed",
                details={
                    "column": self.column,
                    "max_age_seconds": self.max_age.total_seconds(),
                    "null_count": null_count,
                    "reference_time": ref_time.isoformat(),
                    "cutoff_time": cutoff_time.isoformat(),
                },
            )

        # Count stale records (timestamps before cutoff)
        stale_count = df.filter(
            F.col(self.column).isNotNull() & (F.col(self.column) < F.lit(cutoff_time))
        ).count()

        # Determine pass/fail
        passed = stale_count == 0

        # Format max_age for human-readable message
        max_age_str = self._format_timedelta(self.max_age)

        # Build message
        if passed:
            message = (
                f"Freshness check passed for column '{self.column}'. "
                f"All {non_null_count} timestamps are within {max_age_str}."
            )
        else:
            stale_percentage = (stale_count / non_null_count * 100) if non_null_count > 0 else 0
            message = (
                f"Freshness check failed for column '{self.column}'. "
                f"{stale_count} records ({stale_percentage:.2f}%) are older than {max_age_str}."
            )

        return ValidationResult(
            validator_name=self.name,
            passed=passed,
            failure_count=stale_count,
            total_count=total_count,
            message=message,
            details={
                "column": self.column,
                "max_age_seconds": self.max_age.total_seconds(),
                "stale_count": stale_count,
                "null_count": null_count,
                "non_null_count": non_null_count,
                "reference_time": ref_time.isoformat(),
                "cutoff_time": cutoff_time.isoformat(),
            },
        )

    def _format_timedelta(self, td: timedelta) -> str:
        """
        Format a timedelta as a human-readable string.

        Args:
            td (timedelta): The timedelta to format.

        Returns:
            str: A human-readable representation (e.g., "2 days", "3 hours").
        """
        total_seconds = int(td.total_seconds())

        if total_seconds < 60:
            return f"{total_seconds} seconds"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            return f"{minutes} minute{'s' if minutes != 1 else ''}"
        elif total_seconds < 86400:
            hours = total_seconds // 3600
            return f"{hours} hour{'s' if hours != 1 else ''}"
        else:
            days = total_seconds // 86400
            return f"{days} day{'s' if days != 1 else ''}"
