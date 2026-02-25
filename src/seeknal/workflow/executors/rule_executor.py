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

import logging
import re
import time
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


# --- Profile check expression parser ---

@dataclass(frozen=True)
class ThresholdExpr:
    """A parsed threshold expression for profile check evaluation."""

    operator: str  # ">", ">=", "<", "<=", "=", "!=", "between"
    value: float
    high: float | None = None  # Only for "between"

    def evaluate(self, actual: float) -> bool:
        """Return True if the threshold condition is met (i.e. violation)."""
        match self.operator:
            case ">":
                return actual > self.value
            case ">=":
                return actual >= self.value
            case "<":
                return actual < self.value
            case "<=":
                return actual <= self.value
            case "=":
                return actual == self.value
            case "!=":
                return actual != self.value
            case "between":
                assert self.high is not None
                return self.value <= actual <= self.high
        return False


_BETWEEN_RE = re.compile(
    r"^\s*between\s+([-+]?\d+(?:\.\d+)?)\s+and\s+([-+]?\d+(?:\.\d+)?)\s*$", re.I
)
_COMPARE_RE = re.compile(
    r"^\s*(>=|<=|!=|>|<|=)\s*([-+]?\d+(?:\.\d+)?)\s*$"
)


def parse_threshold(expr: str) -> ThresholdExpr:
    """Parse a threshold expression string into a ThresholdExpr.

    Supported formats:
        "> 5", ">= 10", "< 100", "<= 0", "= 0", "!= 3"
        "between 10 and 500"

    Raises:
        ValueError: If the expression doesn't match any supported format
    """
    m = _BETWEEN_RE.match(expr)
    if m:
        return ThresholdExpr("between", float(m.group(1)), float(m.group(2)))
    m = _COMPARE_RE.match(expr)
    if m:
        return ThresholdExpr(m.group(1), float(m.group(2)))
    raise ValueError(f"Invalid threshold expression: {expr!r}")

from seeknal.dag.manifest import Node, NodeType  # ty: ignore[unresolved-import]
from seeknal.feature_validation.validators import (  # ty: ignore[unresolved-import]
    BaseValidator,
    NullValidator,
    RangeValidator,
    UniquenessValidator,
    FreshnessValidator,
    CustomValidator,
    ValidationRunner,
)
from seeknal.feature_validation.models import ValidationMode, ValidationResult  # ty: ignore[unresolved-import]

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
        # Single check
        kind: rule
        name: user_data_quality
        rule:
          type: null
          columns: [user_id, email, name]
          params:
            max_null_percentage: 0.05
        inputs:
          - ref: source.users

        # Multiple checks in one rule
        kind: rule
        name: user_data_quality
        rule:
          - type: null
            columns: [user_id, email]
            params:
              max_null_percentage: 0.0
          - type: range
            column: age
            params:
              min_val: 0
              max_val: 150
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

    def _normalize_rule_config(self) -> List[Dict[str, Any]]:
        """
        Normalize rule config to a list of rule check dicts.

        Supports both single-rule dict and multi-rule list:
            rule:               # single
              type: range
              column: price
            rule:               # multiple
              - type: range
                column: price
              - type: "null"
                columns: [price]

        Returns:
            List of rule check dicts
        """
        rule_config = self.node.config.get("rule")
        if isinstance(rule_config, dict):
            return [rule_config]
        if isinstance(rule_config, list):
            return rule_config
        return []

    def validate(self) -> None:
        """
        Validate the RULE node configuration.

        Checks that required fields are present and valid:
        - 'rule' field exists in node.config
        - 'type' is specified in each rule check
        - 'columns' is specified for applicable rule types
        - 'params' are valid for the rule type

        Supports both single-rule dict and multi-rule list.

        Raises:
            ExecutorValidationError: If configuration is invalid
        """
        rule_config = self.node.config.get("rule")

        if not rule_config:
            raise ExecutorValidationError(
                self.node.id,
                "Missing required 'rule' configuration"
            )

        if not isinstance(rule_config, (dict, list)):
            raise ExecutorValidationError(
                self.node.id,
                f"'rule' must be a dictionary or list, got {type(rule_config).__name__}"
            )

        checks = self._normalize_rule_config()
        if not checks:
            raise ExecutorValidationError(
                self.node.id,
                "Empty 'rule' configuration"
            )

        for idx, check in enumerate(checks):
            label = f"rule[{idx}]" if len(checks) > 1 else "rule"

            if not isinstance(check, dict):
                raise ExecutorValidationError(
                    self.node.id,
                    f"{label}: each rule check must be a dictionary, got {type(check).__name__}"
                )

            self._validate_single_check(check, label)

    def _validate_single_check(self, check: Dict[str, Any], label: str) -> None:
        """Validate a single rule check dict."""
        rule_type = check.get("type")

        if not rule_type:
            raise ExecutorValidationError(
                self.node.id,
                f"{label}: missing required 'type' field"
            )

        valid_rule_types = ["null", "range", "uniqueness", "freshness", "custom", "profile_check", "sql_assertion"]
        if rule_type not in valid_rule_types:
            raise ExecutorValidationError(
                self.node.id,
                f"{label}: invalid rule type '{rule_type}'. Must be one of: {valid_rule_types}"
            )

        if rule_type in ["null", "uniqueness"]:
            columns = check.get("columns")
            if not columns or not isinstance(columns, list):
                raise ExecutorValidationError(
                    self.node.id,
                    f"{label}: rule type '{rule_type}' requires 'columns' list"
                )

        elif rule_type == "range":
            column = check.get("column") or check.get("columns", [None])[0]
            if not column:
                raise ExecutorValidationError(
                    self.node.id,
                    f"{label}: rule type 'range' requires 'column' or 'columns' field"
                )

            params = check.get("params", {})
            if "min_val" not in params and "max_val" not in params:
                raise ExecutorValidationError(
                    self.node.id,
                    f"{label}: rule type 'range' requires 'min_val' or 'max_val' in params"
                )

        elif rule_type == "freshness":
            column = check.get("column")
            if not column:
                raise ExecutorValidationError(
                    self.node.id,
                    f"{label}: rule type 'freshness' requires 'column' field"
                )

            params = check.get("params", {})
            if "max_age" not in params:
                raise ExecutorValidationError(
                    self.node.id,
                    f"{label}: rule type 'freshness' requires 'max_age' in params"
                )

        elif rule_type == "profile_check":
            checks_list = check.get("checks")
            if not checks_list or not isinstance(checks_list, list):
                raise ExecutorValidationError(
                    self.node.id,
                    f"{label}: rule type 'profile_check' requires a 'checks' list"
                )
            for ci, c in enumerate(checks_list):
                if not isinstance(c, dict):
                    raise ExecutorValidationError(
                        self.node.id,
                        f"{label}.checks[{ci}]: each check must be a dictionary"
                    )
                if "metric" not in c:
                    raise ExecutorValidationError(
                        self.node.id,
                        f"{label}.checks[{ci}]: missing required 'metric' field"
                    )
                if "warn" not in c and "fail" not in c:
                    raise ExecutorValidationError(
                        self.node.id,
                        f"{label}.checks[{ci}]: requires at least one of 'warn' or 'fail'"
                    )
                # Validate threshold expressions parse correctly
                for level in ("warn", "fail"):
                    expr_str = c.get(level)
                    if expr_str:
                        try:
                            parse_threshold(expr_str)
                        except ValueError as e:
                            raise ExecutorValidationError(
                                self.node.id,
                                f"{label}.checks[{ci}].{level}: {e}"
                            )

        elif rule_type == "sql_assertion":
            sql = check.get("sql")
            if not sql or not isinstance(sql, str):
                raise ExecutorValidationError(
                    self.node.id,
                    f"{label}: rule type 'sql_assertion' requires a 'sql' string"
                )

    def execute(self) -> ExecutorResult:
        """
        Execute the RULE node using DuckDB-native validation.

        This method:
        1. Loads input data from upstream intermediate parquets
        2. Runs SQL-based validation queries via DuckDB
        3. Returns pass/fail results with violation counts

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

            # Load input data via DuckDB
            import duckdb  # ty: ignore[unresolved-import]

            input_path = self._resolve_input_path()
            if input_path is None:
                raise ExecutorExecutionError(
                    self.node.id,
                    "No input data available for validation"
                )

            con = duckdb.connect()
            con.execute(
                f"CREATE VIEW input_data AS SELECT * FROM read_parquet('{input_path}')"
            )

            # Get row count
            row_count = con.execute("SELECT COUNT(*) FROM input_data").fetchone()[0]

            if row_count == 0:
                con.close()
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=time.time() - start_time,
                    row_count=0,
                    metadata={
                        "warning": "Input data is empty, no validation performed",
                        "rule_type": self.node.config.get("rule", {}).get("type"),
                    },
                )

            # Run DuckDB-native validation for each rule check
            checks = self._normalize_rule_config()
            severity = self.node.config.get("params", {}).get("severity", "error")
            error_msg_override = self.node.config.get("params", {}).get("error_message")
            all_results: List[Dict[str, Any]] = []
            total_violations = 0
            all_passed = True

            for idx, rule_config in enumerate(checks):
                rule_type = rule_config.get("type")
                violations = 0
                message = ""
                passed = True

                if rule_type == "null":
                    columns = rule_config.get("columns", [])
                    max_pct = rule_config.get("params", {}).get("max_null_percentage", 0.0)
                    null_conditions = " OR ".join(f'"{c}" IS NULL' for c in columns)
                    violations = con.execute(
                        f"SELECT COUNT(*) FROM input_data WHERE {null_conditions}"
                    ).fetchone()[0]
                    pct = violations / row_count if row_count > 0 else 0.0
                    passed = pct <= max_pct
                    message = f"Null check: {violations}/{row_count} rows ({pct:.1%}) have nulls in {columns}"

                elif rule_type == "range":
                    column = rule_config.get("column") or rule_config.get("columns", [None])[0]
                    params = rule_config.get("params", {})
                    conditions = []
                    if "min_val" in params:
                        conditions.append(f'"{column}" < {params["min_val"]}')
                    if "max_val" in params:
                        conditions.append(f'"{column}" > {params["max_val"]}')
                    where = " OR ".join(conditions) if conditions else "FALSE"
                    violations = con.execute(
                        f"SELECT COUNT(*) FROM input_data WHERE {where}"
                    ).fetchone()[0]
                    passed = violations == 0
                    message = f"Range check on {column}: {violations}/{row_count} rows out of range"

                elif rule_type == "uniqueness":
                    columns = rule_config.get("columns", [])
                    col_list = ", ".join(f'"{c}"' for c in columns)
                    dup_count = con.execute(
                        f"SELECT COUNT(*) FROM ("
                        f"  SELECT {col_list}, COUNT(*) as cnt FROM input_data "
                        f"  GROUP BY {col_list} HAVING cnt > 1"
                        f")"
                    ).fetchone()[0]
                    violations = dup_count
                    passed = violations == 0
                    message = f"Uniqueness check on {columns}: {violations} duplicate groups found"

                elif rule_type == "freshness":
                    column = rule_config.get("column")
                    max_age = rule_config.get("params", {}).get("max_age", "24h")
                    td = self._parse_timedelta(max_age)
                    seconds = int(td.total_seconds())
                    violations = con.execute(
                        f"SELECT COUNT(*) FROM input_data "
                        f"WHERE CAST(\"{column}\" AS TIMESTAMP) < NOW() - INTERVAL '{seconds}' SECOND"
                    ).fetchone()[0]
                    passed = violations == 0
                    message = f"Freshness check on {column}: {violations}/{row_count} rows older than {max_age}"

                elif rule_type == "profile_check":
                    profile_results = self._execute_profile_check(
                        con, rule_config, input_path
                    )
                    con.close()
                    return profile_results

                elif rule_type == "sql_assertion":
                    assertion_results = self._execute_sql_assertion(
                        con, rule_config
                    )
                    con.close()
                    return assertion_results

                else:
                    con.close()
                    raise ExecutorExecutionError(
                        self.node.id,
                        f"Unsupported rule type: {rule_type}. "
                        f"Must be one of: null, range, uniqueness, freshness, profile_check"
                    )

                total_violations += violations
                if not passed:
                    all_passed = False

                check_result = {
                    "rule_type": rule_type,
                    "passed": passed,
                    "violations": violations,
                    "message": message,
                }
                all_results.append(check_result)

                if passed:
                    logger.info(f"Rule check [{idx}] PASSED: {message}")
                else:
                    logger.info(f"Rule check [{idx}] FAILED: {message}")

            con.close()

            # Build combined metadata
            # For single-check backward compat, flatten result into top level
            if len(all_results) == 1:
                r = all_results[0]
                metadata = {
                    "rule_type": r["rule_type"],
                    "passed": r["passed"],
                    "violations": r["violations"],
                    "message": r["message"],
                    "severity": severity,
                }
            else:
                failed = [r for r in all_results if not r["passed"]]
                metadata = {
                    "checks": len(all_results),
                    "passed": all_passed,
                    "total_violations": total_violations,
                    "failed_checks": len(failed),
                    "results": all_results,
                    "severity": severity,
                }

            if all_passed:
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=time.time() - start_time,
                    row_count=row_count,
                    metadata=metadata,
                )
            else:
                failed_msgs = [r["message"] for r in all_results if not r["passed"]]
                error_msg = error_msg_override or "; ".join(failed_msgs)
                if severity == "warn":
                    logger.warning(f"Rule WARNING: {error_msg} ({total_violations} violations)")
                    return ExecutorResult(
                        node_id=self.node.id,
                        status=ExecutionStatus.SUCCESS,
                        duration_seconds=time.time() - start_time,
                        row_count=row_count,
                        metadata=metadata,
                    )
                else:
                    raise ExecutorExecutionError(
                        self.node.id,
                        f"Validation failed: {error_msg} ({total_violations} violations)"
                    )

        except ExecutorExecutionError:
            raise
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"Rule execution failed: {str(e)}",
                e
            ) from e

    def _resolve_input_path(self) -> Optional[Path]:
        """
        Resolve the first input reference to an intermediate parquet path.

        Returns:
            Path to intermediate parquet, or None if not found
        """
        inputs = self.node.config.get("inputs", [])
        if not inputs:
            return None

        first_input = inputs[0]
        if isinstance(first_input, dict) and "ref" in first_input:
            ref = first_input["ref"]
        elif isinstance(first_input, str):
            ref = first_input
        else:
            return None

        # Try intermediate path: target/intermediate/{kind}_{name}.parquet
        ref_path = ref.replace(".", "_")
        intermediate = self.context.target_path / "intermediate" / f"{ref_path}.parquet"
        if intermediate.exists():
            return intermediate

        # Try cache path: target/cache/{kind}/{name}.parquet
        parts = ref.split(".")
        if len(parts) == 2:
            kind, name = parts
            cache = self.context.target_path / "cache" / kind / f"{name}.parquet"
            if cache.exists():
                return cache

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
        summary: Any,
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

    def _execute_profile_check(
        self,
        con: Any,
        rule_config: Dict[str, Any],
        input_path: Path,
    ) -> ExecutorResult:
        """Execute profile_check rule type against a profile parquet.

        Reads the profile stats parquet, evaluates each check's threshold
        expression against the corresponding metric value, and returns
        an aggregated pass/warn/fail result.

        Args:
            con: DuckDB connection (already has input_data view)
            rule_config: The rule check dict with type=profile_check
            input_path: Path to the profile parquet

        Returns:
            ExecutorResult with check evaluation details
        """
        start_time = time.time()
        checks_list = rule_config.get("checks", [])
        severity = self.node.config.get("params", {}).get("severity", "error")

        check_results: List[Dict[str, Any]] = []
        has_fail = False
        has_warn = False

        for ci, check in enumerate(checks_list):
            metric = check["metric"]
            column = check.get("column", "_table_")
            warn_expr_str = check.get("warn")
            fail_expr_str = check.get("fail")

            # Look up metric value from profile parquet
            row = con.execute(
                "SELECT value FROM input_data "
                "WHERE column_name = ? AND metric = ?",
                [column, metric],
            ).fetchone()

            if row is None:
                # Metric not found in profile
                check_results.append({
                    "check_index": ci,
                    "column": column,
                    "metric": metric,
                    "status": "error",
                    "message": f"Metric '{metric}' not found for column '{column}'",
                })
                has_fail = True
                continue

            raw_value = row[0]

            # Handle NULL metric values
            if raw_value is None:
                status = "pass"
                message = f"{column}.{metric} = NULL"
                if fail_expr_str:
                    status = "fail"
                    message = f"{column}.{metric} = NULL (fail threshold active)"
                    has_fail = True
                elif warn_expr_str:
                    status = "warn"
                    message = f"{column}.{metric} = NULL (warn threshold active)"
                    has_warn = True
                check_results.append({
                    "check_index": ci,
                    "column": column,
                    "metric": metric,
                    "value": None,
                    "status": status,
                    "message": message,
                })
                continue

            # Cast to float for comparison
            try:
                actual = float(raw_value)
            except (ValueError, TypeError) as e:
                check_results.append({
                    "check_index": ci,
                    "column": column,
                    "metric": metric,
                    "value": raw_value,
                    "status": "error",
                    "message": f"Cannot cast metric value to number: {e}",
                })
                has_fail = True
                continue

            # Evaluate fail threshold first (takes precedence)
            status = "pass"
            message = f"{column}.{metric} = {actual}"

            if fail_expr_str:
                fail_expr = parse_threshold(fail_expr_str)
                if fail_expr.evaluate(actual):
                    status = "fail"
                    message = f"{column}.{metric} = {actual} (fails: {fail_expr_str})"
                    has_fail = True

            if status == "pass" and warn_expr_str:
                warn_expr = parse_threshold(warn_expr_str)
                if warn_expr.evaluate(actual):
                    status = "warn"
                    message = f"{column}.{metric} = {actual} (warns: {warn_expr_str})"
                    has_warn = True

            check_results.append({
                "check_index": ci,
                "column": column,
                "metric": metric,
                "value": actual,
                "status": status,
                "message": message,
            })

            if status == "pass":
                logger.info(f"Profile check [{ci}] PASS: {message}")
            elif status == "warn":
                logger.warning(f"Profile check [{ci}] WARN: {message}")
            else:
                logger.info(f"Profile check [{ci}] FAIL: {message}")

        # Aggregate results
        passed_count = sum(1 for r in check_results if r["status"] == "pass")
        warn_count = sum(1 for r in check_results if r["status"] == "warn")
        fail_count = sum(1 for r in check_results if r["status"] in ("fail", "error"))

        metadata = {
            "rule_type": "profile_check",
            "checks": len(check_results),
            "passed": passed_count,
            "warned": warn_count,
            "failed": fail_count,
            "results": check_results,
            "severity": severity,
        }

        if has_fail:
            failed_msgs = [r["message"] for r in check_results if r["status"] in ("fail", "error")]
            error_msg = "; ".join(failed_msgs)
            if severity == "warn":
                logger.warning(f"Profile check WARNING: {error_msg}")
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=time.time() - start_time,
                    row_count=0,
                    metadata=metadata,
                )
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"Profile check failed: {error_msg}"
                )

        if has_warn:
            warn_msgs = [r["message"] for r in check_results if r["status"] == "warn"]
            logger.warning(f"Profile check warnings: {'; '.join(warn_msgs)}")

        return ExecutorResult(
            node_id=self.node.id,
            status=ExecutionStatus.SUCCESS,
            duration_seconds=time.time() - start_time,
            row_count=0,
            metadata=metadata,
        )

    def _register_input_views(self, con: "Any") -> None:
        """Register all input refs as named DuckDB views.

        Each input ``ref`` is converted to a view name by replacing dots
        with underscores (e.g. ``source.orders`` -> ``source_orders``).
        The first input is also aliased as ``input_data`` for backward
        compatibility with other rule types.
        """
        inputs = self.node.config.get("inputs", [])
        for idx, inp in enumerate(inputs):
            if isinstance(inp, dict) and "ref" in inp:
                ref = inp["ref"]
            elif isinstance(inp, str):
                ref = inp
            else:
                continue

            ref_path = ref.replace(".", "_")
            intermediate = self.context.target_path / "intermediate" / f"{ref_path}.parquet"
            if not intermediate.exists():
                parts = ref.split(".")
                if len(parts) == 2:
                    kind, name = parts
                    cache = self.context.target_path / "cache" / kind / f"{name}.parquet"
                    if cache.exists():
                        intermediate = cache
                    else:
                        continue
                else:
                    continue

            view_name = ref_path  # e.g. "source_orders"
            con.execute(
                f"CREATE OR REPLACE VIEW \"{view_name}\" AS "
                f"SELECT * FROM read_parquet('{intermediate}')"
            )

    def _execute_sql_assertion(
        self,
        con: "Any",
        rule_config: Dict[str, Any],
    ) -> ExecutorResult:
        """Execute sql_assertion rule type.

        Runs the user's SQL query. If the query returns any rows, those
        are treated as violations (dbt-style test pattern).

        All input refs are registered as named views before the query runs.
        """
        start_time = time.time()
        severity = self.node.config.get("params", {}).get("severity", "error")
        error_msg_override = self.node.config.get("params", {}).get("error_message")
        sql = rule_config["sql"]

        # Register all inputs as named views
        self._register_input_views(con)

        try:
            result_rel = con.execute(sql)
            violations_df = result_rel.fetchall()
            violation_count = len(violations_df)
        except Exception as e:
            raise ExecutorExecutionError(
                self.node.id,
                f"sql_assertion query failed: {e}"
            ) from e

        passed = violation_count == 0
        message = (
            f"SQL assertion passed (0 violations)"
            if passed
            else f"SQL assertion failed: {violation_count} violation row(s) returned"
        )

        metadata = {
            "rule_type": "sql_assertion",
            "passed": passed,
            "violations": violation_count,
            "message": message,
            "severity": severity,
            "sql": sql,
        }

        if passed:
            logger.info(f"SQL assertion PASSED: {message}")
            return ExecutorResult(
                node_id=self.node.id,
                status=ExecutionStatus.SUCCESS,
                duration_seconds=time.time() - start_time,
                row_count=violation_count,
                metadata=metadata,
            )
        else:
            error_msg = error_msg_override or message
            if severity == "warn":
                logger.warning(f"SQL assertion WARNING: {error_msg}")
                return ExecutorResult(
                    node_id=self.node.id,
                    status=ExecutionStatus.SUCCESS,
                    duration_seconds=time.time() - start_time,
                    row_count=violation_count,
                    metadata=metadata,
                )
            else:
                raise ExecutorExecutionError(
                    self.node.id,
                    f"SQL assertion failed: {error_msg} ({violation_count} violations)"
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
