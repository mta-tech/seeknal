"""
Rule executor for Seeknal workflow node execution.

This module implements the RuleExecutor class, which executes RULE nodes
by integrating with the existing feature_validation framework. It supports
multiple rule types (null, range, uniqueness, freshness) and returns
standardized execution results with validation metrics.

Design Decisions:
- Reuses existing validators from feature_validation.validators
- Executes rules against resolved input data (from upstream nodes)
- Returns ExecutorResult with validation metrics and pass/fail status
- Supports both row-level and aggregate rules
- Handles dry-run mode gracefully

Key Components:
- RuleExecutor: Main executor class for RULE nodes
- Rule type mapping: Maps YAML rule types to validator classes
- Result aggregation: Converts ValidationResult to ExecutorResult
"""

import time
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession

from seeknal.dag.manifest import Node, NodeType
from seeknal.feature_validation.validators import (
    BaseValidator,
    NullValidator,
    RangeValidator,
    UniquenessValidator,
    FreshnessValidator,
    CustomValidator,
    ValidationRunner,
)
from seeknal.feature_validation.models import ValidationMode, ValidationResult

from .base import (
    BaseExecutor,
    ExecutorResult,
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
    register_executor,
)


@register_executor(NodeType.RULE)
class RuleExecutor(BaseExecutor):
    """
    Executor for RULE nodes in the workflow DAG.

    This executor loads data from upstream nodes, applies validation rules,
    and returns pass/fail results with detailed metrics.

    YAML Configuration Example:
        ```yaml
        kind: rule
        name: user_data_quality
        description: Validate user data quality
        rule:
          type: null
          columns: [user_id, email, name]
          params:
            max_null_percentage: 0.05
        inputs:
          - ref: source.users
        ```

    Supported Rule Types:
        - null: Check for null/missing values
        - range: Check numeric values are within bounds
        - uniqueness: Check for duplicate values
        - freshness: Check timestamp recency
        - custom: User-defined validation function

    Attributes:
        node (Node): The RULE node to execute
        context (ExecutionContext): Execution environment

    Example:
        >>> executor = RuleExecutor(node, context)
        >>> result = executor.run()
        >>> if result.is_success():
        ...     print(f"All validations passed: {result.metadata['summary']}")
    """

    @property
    def node_type(self) -> NodeType:
        """Return the NodeType this executor handles."""
        return NodeType.RULE

    def validate(self) -> None:
        """
        Validate the RULE node configuration.

        Checks that required fields are present and valid:
        - 'rule' field exists in node.config
        - 'type' is specified in rule config
        - 'columns' is specified for applicable rule types
        - 'params' are valid for the rule type

        Raises:
            ExecutorValidationError: If configuration is invalid
        """
        rule_config = self.node.config.get("rule")

        if not rule_config:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required 'rule' configuration"
            )

        if not isinstance(rule_config, dict):
            raise ExecutorValidationError(
                self.node.id,
                f"'rule' must be a dictionary, got {type(rule_config).__name__}"
            )

        rule_type = rule_config.get("type")

        if not rule_type:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required 'type' field in rule configuration"
            )

        valid_rule_types = ["null", "range", "uniqueness", "freshness", "custom"]
        if rule_type not in valid_rule_types:
            raise ExecutorValidationError(
                self.node.id,
                f"Invalid rule type '{rule_type}'. Must be one of: {valid_rule_types}"
            )

        # Validate rule type specific requirements
        if rule_type in ["null", "uniqueness"]:
            columns = rule_config.get("columns")
            if not columns or not isinstance(columns, list):
                raise ExecutorValidationError(
                    self.node.id,
                    f"Rule type '{rule_type}' requires 'columns' list"
                )

        elif rule_type == "range":
            column = rule_config.get("column") or rule_config.get("columns", [None])[0]
            if not column:
                raise ExecutorValidationError(
                    self.node.id,
                    "Rule type 'range' requires 'column' or 'columns' field"
                )

            params = rule_config.get("params", {})
            if "min_val" not in params and "max_val" not in params:
                raise ExecutorValidationError(
                    self.node.id,
                    "Rule type 'range' requires 'min_val' or 'max_val' in params"
                )

        elif rule_type == "freshness":
            column = rule_config.get("column")
            if not column:
                raise ExecutorValidationError(
                    self.node.id,
                    "Rule type 'freshness' requires 'column' field"
                )

            params = rule_config.get("params", {})
            if "max_age" not in params:
                raise ExecutorValidationError(
                    self.node.id,
                    "Rule type 'freshness' requires 'max_age' in params"
                )

    def execute(self) -> ExecutorResult:
        """
        Execute the RULE node.

        This method:
        1. Loads input data from upstream nodes
        2. Creates validators based on rule configuration
        3. Runs validators against the data
        4. Aggregates results into ExecutorResult

        Returns:
            ExecutorResult with validation metrics and status

        Raises:
            ExecutorExecutionError: If execution fails
        """
        start_time = time.time()

        try:
            # Handle dry-run mode
            if self.context.dry_run:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=0.0,
                    row_count=0,
                    metadata={
                        "dry_run": True,
                        "rule_type": self.node.config.get("rule", {}).get("type"),
                        "message": "Dry-run: validation not executed"
                    },
                    is_dry_run=True
                )

            # Load input data from upstream nodes
            df = self._load_input_data()

            if df is None:
                raise ExecutorExecutionError(
                    self.node.id,
                    "No input data available for validation"
                )

            row_count = df.count()

            # Handle empty DataFrame
            if row_count == 0:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=time.time() - start_time,
                    row_count=0,
                    metadata={
                        "warning": "Input DataFrame is empty, no validation performed",
                        "rule_type": self.node.config.get("rule", {}).get("type")
                    }
                )

            # Create validators from rule configuration
            validators = self._create_validators()

            if not validators:
                raise ExecutorExecutionError(
                    self.node.id,
                    "No validators could be created from rule configuration"
                )

            # Execute validations
            validation_mode = self._get_validation_mode()
            runner = ValidationRunner(
                validators=validators,
                mode=validation_mode,
                feature_group_name=self.node.name
            )

            summary = runner.run(df)

            # Convert validation summary to executor result
            return self._build_result(summary, row_count, start_time)

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Rule execution failed: {str(e)}",
                e
            ) from e

    def _load_input_data(self) -> Optional[DataFrame]:
        """
        Load input data from upstream nodes.

        This method resolves data from upstream nodes (sources, transforms, etc.)
        that are specified in the node's inputs.

        Returns:
            DataFrame with input data, or None if no inputs

        Note:
            This is a simplified implementation. A full implementation would:
            - Resolve refs from node.inputs
            - Load data from cache or execute upstream nodes
            - Handle multiple input sources
        """
        # Get inputs from node config
        inputs = self.node.config.get("inputs", [])

        if not inputs:
            # No inputs specified - this might be a standalone rule
            # In a full implementation, we might load from a default source
            return None

        # For now, try to load from the first input
        # In a full implementation, this would resolve refs and load data
        # from upstream nodes or cache
        first_input = inputs[0]

        if isinstance(first_input, dict) and "ref" in first_input:
            # This is a reference to an upstream node
            # In a full implementation, we would:
            # 1. Resolve the ref to get the upstream node
            # 2. Load data from the upstream node's output
            # 3. Merge data from multiple inputs if needed

            # For now, return None to indicate data should be resolved elsewhere
            return None

        return None

    def _create_validators(self) -> List[BaseValidator]:
        """
        Create validator instances from rule configuration.

        Returns:
            List of BaseValidator instances

        Raises:
            ExecutorExecutionError: If validator creation fails
        """
        rule_config = self.node.config.get("rule", {})
        rule_type = rule_config.get("type")

        try:
            if rule_type == "null":
                return self._create_null_validator(rule_config)
            elif rule_type == "range":
                return self._create_range_validator(rule_config)
            elif rule_type == "uniqueness":
                return self._create_uniqueness_validator(rule_config)
            elif rule_type == "freshness":
                return self._create_freshness_validator(rule_config)
            elif rule_type == "custom":
                return self._create_custom_validator(rule_config)
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Unsupported rule type: {rule_type}"
                )

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Failed to create validator: {str(e)}",
                e
            ) from e

    def _create_null_validator(self, rule_config: Dict[str, Any]) -> List[NullValidator]:
        """Create NullValidator from rule configuration."""
        columns = rule_config.get("columns", [])
        params = rule_config.get("params", {})

        max_null_percentage = params.get("max_null_percentage", 0.0)

        return [
            NullValidator(
                columns=columns,
                max_null_percentage=max_null_percentage
            )
        ]

    def _create_range_validator(self, rule_config: Dict[str, Any]) -> List[RangeValidator]:
        """Create RangeValidator from rule configuration."""
        # Support both 'column' and 'columns' (for consistency)
        column = rule_config.get("column")
        if not column:
            columns = rule_config.get("columns", [])
            if columns:
                column = columns[0]

        if not column:
            raise ExecutorExecutionError(
                self.node.id,
                "Range validator requires 'column' or 'columns'"
            )

        params = rule_config.get("params", {})
        min_val = params.get("min_val")
        max_val = params.get("max_val")

        return [
            RangeValidator(
                column=column,
                min_val=min_val,
                max_val=max_val
            )
        ]

    def _create_uniqueness_validator(
        self,
        rule_config: Dict[str, Any]
    ) -> List[UniquenessValidator]:
        """Create UniquenessValidator from rule configuration."""
        columns = rule_config.get("columns", [])
        params = rule_config.get("params", {})

        max_duplicate_percentage = params.get("max_duplicate_percentage", 0.0)

        return [
            UniquenessValidator(
                columns=columns,
                max_duplicate_percentage=max_duplicate_percentage
            )
        ]

    def _create_freshness_validator(
        self,
        rule_config: Dict[str, Any]
    ) -> List[FreshnessValidator]:
        """Create FreshnessValidator from rule configuration."""
        column = rule_config.get("column")
        params = rule_config.get("params", {})

        max_age_str = params.get("max_age")
        if not max_age_str:
            raise ExecutorExecutionError(
                self.node.id,
                "Freshness validator requires 'max_age' in params"
            )

        # Parse max_age (support various formats)
        max_age = self._parse_timedelta(max_age_str)

        reference_time = params.get("reference_time")  # Optional

        return [
            FreshnessValidator(
                column=column,
                max_age=max_age,
                reference_time=reference_time
            )
        ]

    def _create_custom_validator(self, rule_config: Dict[str, Any]) -> List[CustomValidator]:
        """
        Create CustomValidator from rule configuration.

        For custom rules, the 'params' should contain a 'func' key
        with a callable validation function.

        Note:
            This is a placeholder for future functionality.
            In a full implementation, this would load user-defined
            validation functions from Python modules.
        """
        params = rule_config.get("params", {})

        # For now, return an empty list as custom validators
        # require more complex function loading infrastructure
        raise ExecutorExecutionError(
            self.node.id,
            "Custom validators are not yet supported in workflow execution"
        )

    def _parse_timedelta(self, value: Union[str, int, timedelta]) -> timedelta:
        """
        Parse a timedelta from various formats.

        Args:
            value: Timedelta value as string, int (seconds), or timedelta

        Returns:
            timedelta object

        Raises:
            ExecutorExecutionError: If parsing fails
        """
        if isinstance(value, timedelta):
            return value

        if isinstance(value, (int, float)):
            return timedelta(seconds=value)

        if isinstance(value, str):
            # Parse string format like "1h", "30m", "2d"
            try:
                # Simple parsing for common formats
                value = value.strip().lower()

                if value.endswith("s"):
                    return timedelta(seconds=int(value[:-1]))
                elif value.endswith("m"):
                    return timedelta(minutes=int(value[:-1]))
                elif value.endswith("h"):
                    return timedelta(hours=int(value[:-1]))
                elif value.endswith("d"):
                    return timedelta(days=int(value[:-1]))
                else:
                    # Try to parse as integer seconds
                    return timedelta(seconds=int(value))

            except (ValueError, IndexError) as e:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Failed to parse timedelta '{value}': {str(e)}"
                ) from e

        raise ExecutorExecutionError(
            self.node.id,
            f"Unsupported timedelta format: {type(value).__name__}"
        )

    def _get_validation_mode(self) -> ValidationMode:
        """
        Get the validation mode from node configuration.

        Returns:
            ValidationMode (WARN or FAIL)

        Note:
            Default is FAIL - halt on first validation failure.
            Can be overridden in rule config with 'mode' field.
        """
        rule_config = self.node.config.get("rule", {})
        mode_str = rule_config.get("mode", "fail").lower()

        if mode_str == "warn":
            return ValidationMode.WARN
        else:
            return ValidationMode.FAIL

    def _build_result(
        self,
        summary: "ValidationSummary",
        row_count: int,
        start_time: float
    ) -> ExecutorResult:
        """
        Build ExecutorResult from ValidationSummary.

        Args:
            summary: Validation summary from ValidationRunner
            row_count: Total row count of validated data
            start_time: Execution start time

        Returns:
            ExecutorResult with aggregated metrics
        """
        # Determine status based on validation results
        if summary.passed:
            status = ExecutionStatus.SUCCESS
            error_message = None
        else:
            status = ExecutionStatus.FAILED
            # Build error message from failed validations
            failed_results = [r for r in summary.results if not r.passed]
            error_messages = [
                f"{r.validator_name}: {r.message}"
                for r in failed_results
            ]
            error_message = "; ".join(error_messages)

        # Build metadata with validation details
        metadata = {
            "rule_type": self.node.config.get("rule", {}).get("type"),
            "total_validators": summary.total_validators,
            "passed_validators": summary.passed_count,
            "failed_validators": summary.failed_count,
            "validation_mode": self._get_validation_mode().value,
            "results": [
                {
                    "validator": r.validator_name,
                    "passed": r.passed,
                    "message": r.message,
                    "failure_count": r.failure_count,
                    "total_count": r.total_count,
                }
                for r in summary.results
            ]
        }

        return ExecutorResult(
            node_id=self.node.id,
            status=status,
            duration_seconds=time.time() - start_time,
            row_count=row_count,
            error_message=error_message,
            metadata=metadata
        )

    def post_execute(self, result: ExecutorResult) -> ExecutorResult:
        """
        Post-execution cleanup and metadata enhancement.

        Adds additional metadata to the result including
        validation summary details and timing information.

        Args:
            result: The result from execute()

        Returns:
            Enhanced ExecutorResult
        """
        # Add execution context info
        result.metadata["project_name"] = self.context.project_name
        result.metadata["workspace_path"] = str(self.context.workspace_path)

        return result
