"""
Tests for profile_check rule type in RuleExecutor.

Tests cover:
- ThresholdExpr and parse_threshold expression parsing
- profile_check validation in RuleExecutor
- profile_check execution: pass, warn, fail scenarios
- NULL metric handling
- Table-level checks (no column specified)
- Multiple checks with mixed results
- Integration: source -> profile -> rule end-to-end
"""

import pytest  # ty: ignore[unresolved-import]
from pathlib import Path

import pandas as pd  # ty: ignore[unresolved-import]

from seeknal.workflow.executors.rule_executor import (  # ty: ignore[unresolved-import]
    ThresholdExpr,
    parse_threshold,
    RuleExecutor,
)
from seeknal.workflow.executors.base import (  # ty: ignore[unresolved-import]
    ExecutionContext,
    ExecutionStatus,
    ExecutorValidationError,
    ExecutorExecutionError,
)
from seeknal.dag.manifest import Node, NodeType  # ty: ignore[unresolved-import]


# ──────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_target(tmp_path):
    """Create a target directory with intermediate subdirectory."""
    intermediate = tmp_path / "intermediate"
    intermediate.mkdir(parents=True)
    return tmp_path


def _make_context(target_path: Path, dry_run: bool = False) -> ExecutionContext:
    """Create a minimal ExecutionContext for testing."""
    return ExecutionContext(
        project_name="test_project",
        workspace_path=target_path.parent,
        target_path=target_path,
        dry_run=dry_run,
    )


def _make_node(name: str, config: dict) -> Node:
    """Create a RULE node with the given config."""
    return Node(
        id=f"rule.{name}",
        name=name,
        node_type=NodeType.RULE,
        config=config,
    )


def _write_profile_parquet(target_path: Path, profile_name: str, rows: list[dict]):
    """Write a profile stats parquet file."""
    df = pd.DataFrame(rows)
    out = target_path / "intermediate" / f"profile_{profile_name}.parquet"
    df.to_parquet(str(out), index=False)
    return out


# ──────────────────────────────────────────────────────────────────────────
# ThresholdExpr & parse_threshold tests
# ──────────────────────────────────────────────────────────────────────────

class TestParseThreshold:
    """Test expression parsing for all operators."""

    def test_greater_than(self):
        expr = parse_threshold("> 5")
        assert expr.operator == ">"
        assert expr.value == 5.0
        assert expr.high is None

    def test_greater_equal(self):
        expr = parse_threshold(">= 10.5")
        assert expr.operator == ">="
        assert expr.value == 10.5

    def test_less_than(self):
        expr = parse_threshold("< 100")
        assert expr.operator == "<"
        assert expr.value == 100.0

    def test_less_equal(self):
        expr = parse_threshold("<= 0")
        assert expr.operator == "<="
        assert expr.value == 0.0

    def test_equal(self):
        expr = parse_threshold("= 0")
        assert expr.operator == "="
        assert expr.value == 0.0

    def test_not_equal(self):
        expr = parse_threshold("!= 3")
        assert expr.operator == "!="
        assert expr.value == 3.0

    def test_between(self):
        expr = parse_threshold("between 10 and 500")
        assert expr.operator == "between"
        assert expr.value == 10.0
        assert expr.high == 500.0

    def test_between_case_insensitive(self):
        expr = parse_threshold("BETWEEN 1.5 AND 99.9")
        assert expr.operator == "between"
        assert expr.value == 1.5
        assert expr.high == 99.9

    def test_negative_values(self):
        expr = parse_threshold("> -5")
        assert expr.value == -5.0

    def test_whitespace_tolerance(self):
        expr = parse_threshold("  >=   42  ")
        assert expr.operator == ">="
        assert expr.value == 42.0

    def test_invalid_expression_raises(self):
        with pytest.raises(ValueError, match="Invalid threshold expression"):
            parse_threshold("around 5")

    def test_empty_expression_raises(self):
        with pytest.raises(ValueError, match="Invalid threshold expression"):
            parse_threshold("")

    def test_missing_value_raises(self):
        with pytest.raises(ValueError, match="Invalid threshold expression"):
            parse_threshold("> ")


class TestThresholdExprEvaluate:
    """Test threshold expression evaluation."""

    def test_greater_than_true(self):
        assert ThresholdExpr(">", 5.0).evaluate(10.0) is True

    def test_greater_than_false(self):
        assert ThresholdExpr(">", 5.0).evaluate(3.0) is False

    def test_greater_than_equal_boundary(self):
        assert ThresholdExpr(">", 5.0).evaluate(5.0) is False

    def test_equal_true(self):
        assert ThresholdExpr("=", 0.0).evaluate(0.0) is True

    def test_equal_false(self):
        assert ThresholdExpr("=", 0.0).evaluate(1.0) is False

    def test_not_equal_true(self):
        assert ThresholdExpr("!=", 3.0).evaluate(5.0) is True

    def test_between_inside(self):
        assert ThresholdExpr("between", 10.0, 500.0).evaluate(250.0) is True

    def test_between_outside(self):
        assert ThresholdExpr("between", 10.0, 500.0).evaluate(501.0) is False

    def test_between_boundary_low(self):
        assert ThresholdExpr("between", 10.0, 500.0).evaluate(10.0) is True

    def test_between_boundary_high(self):
        assert ThresholdExpr("between", 10.0, 500.0).evaluate(500.0) is True

    def test_less_than_true(self):
        assert ThresholdExpr("<", 100.0).evaluate(50.0) is True

    def test_less_equal_boundary(self):
        assert ThresholdExpr("<=", 0.0).evaluate(0.0) is True


# ──────────────────────────────────────────────────────────────────────────
# Validation tests
# ──────────────────────────────────────────────────────────────────────────

class TestProfileCheckValidation:
    """Test RuleExecutor validation for profile_check type."""

    def test_valid_profile_check(self, tmp_target):
        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"metric": "row_count", "fail": "= 0"},
                    {"column": "price", "metric": "avg", "warn": "> 500"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        executor.validate()  # Should not raise

    def test_missing_checks_list(self, tmp_target):
        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {"type": "profile_check"},
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="requires a 'checks' list"):
            executor.validate()

    def test_empty_checks_list(self, tmp_target):
        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {"type": "profile_check", "checks": []},
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="requires a 'checks' list"):
            executor.validate()

    def test_check_missing_metric(self, tmp_target):
        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [{"fail": "= 0"}],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="missing required 'metric'"):
            executor.validate()

    def test_check_missing_warn_and_fail(self, tmp_target):
        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [{"metric": "row_count"}],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="at least one of 'warn' or 'fail'"):
            executor.validate()

    def test_invalid_threshold_expression(self, tmp_target):
        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [{"metric": "row_count", "fail": "around 5"}],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="Invalid threshold expression"):
            executor.validate()

    def test_check_not_dict(self, tmp_target):
        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": ["not_a_dict"],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorValidationError, match="each check must be a dictionary"):
            executor.validate()


# ──────────────────────────────────────────────────────────────────────────
# Execution tests
# ──────────────────────────────────────────────────────────────────────────

class TestProfileCheckExecution:
    """Test profile_check execution: pass, warn, fail scenarios."""

    def _make_profile_data(self):
        """Standard profile data for testing."""
        return [
            {"column_name": "_table_", "metric": "row_count", "value": "1000", "detail": None},
            {"column_name": "price", "metric": "avg", "value": "49.99", "detail": None},
            {"column_name": "price", "metric": "null_percent", "value": "2.5", "detail": None},
            {"column_name": "price", "metric": "stddev", "value": "15.3", "detail": None},
            {"column_name": "price", "metric": "distinct_count", "value": "200", "detail": None},
            {"column_name": "category", "metric": "distinct_count", "value": "5", "detail": None},
        ]

    def test_all_checks_pass(self, tmp_target):
        """All checks pass — result is SUCCESS."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"metric": "row_count", "fail": "= 0"},
                    {"column": "price", "metric": "avg", "warn": "> 500"},
                    {"column": "category", "metric": "distinct_count", "fail": "< 2"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        result = executor.execute()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["rule_type"] == "profile_check"
        assert result.metadata["passed"] == 3
        assert result.metadata["warned"] == 0
        assert result.metadata["failed"] == 0

    def test_warn_threshold_triggered(self, tmp_target):
        """Warn threshold triggered — result is SUCCESS with warning metadata."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    # avg is 49.99, so "> 40" triggers warn
                    {"column": "price", "metric": "avg", "warn": "> 40"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        result = executor.execute()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["warned"] == 1
        assert result.metadata["failed"] == 0
        assert result.metadata["results"][0]["status"] == "warn"

    def test_fail_threshold_raises(self, tmp_target):
        """Fail threshold triggered — raises ExecutorExecutionError."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    # row_count is 1000, "< 5000" is True → fail
                    {"metric": "row_count", "fail": "< 5000"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="Profile check failed"):
            executor.execute()

    def test_fail_equal_zero(self, tmp_target):
        """Fail when row_count = 0."""
        data = [
            {"column_name": "_table_", "metric": "row_count", "value": "0", "detail": None},
        ]
        _write_profile_parquet(tmp_target, "stats", data)

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"metric": "row_count", "fail": "= 0"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="Profile check failed"):
            executor.execute()

    def test_between_check_pass(self, tmp_target):
        """Between check passes when value is in range."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    # avg is 49.99, between 10 and 500 → True → warn triggered
                    {"column": "price", "metric": "avg", "warn": "between 10 and 500"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        result = executor.execute()
        assert result.metadata["warned"] == 1

    def test_between_check_outside(self, tmp_target):
        """Between check doesn't trigger when value is outside range."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    # avg is 49.99, between 100 and 500 → False → pass
                    {"column": "price", "metric": "avg", "warn": "between 100 and 500"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        result = executor.execute()
        assert result.metadata["passed"] == 1
        assert result.metadata["warned"] == 0

    def test_table_level_check_no_column(self, tmp_target):
        """Table-level check (no column specified) defaults to _table_."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    # No column → defaults to _table_
                    {"metric": "row_count", "warn": "> 500"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        result = executor.execute()
        # row_count is 1000, > 500 is True → warn
        assert result.metadata["warned"] == 1

    def test_null_metric_value_fail(self, tmp_target):
        """NULL metric value with fail threshold → FAIL."""
        data = [
            {"column_name": "price", "metric": "avg", "value": None, "detail": None},
        ]
        _write_profile_parquet(tmp_target, "stats", data)

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"column": "price", "metric": "avg", "fail": "> 500"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="NULL"):
            executor.execute()

    def test_null_metric_value_warn(self, tmp_target):
        """NULL metric value with warn threshold only → WARN (SUCCESS with warning)."""
        data = [
            {"column_name": "price", "metric": "avg", "value": None, "detail": None},
        ]
        _write_profile_parquet(tmp_target, "stats", data)

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"column": "price", "metric": "avg", "warn": "> 500"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        result = executor.execute()
        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["warned"] == 1

    def test_metric_not_found(self, tmp_target):
        """Metric not found in profile → treated as error/fail."""
        data = [
            {"column_name": "_table_", "metric": "row_count", "value": "100", "detail": None},
        ]
        _write_profile_parquet(tmp_target, "stats", data)

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"column": "nonexistent", "metric": "avg", "fail": "> 0"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="not found"):
            executor.execute()

    def test_multiple_checks_mixed_results(self, tmp_target):
        """Multiple checks: some pass, some warn — overall SUCCESS."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    # row_count=1000, != 0 → True → warn triggered
                    {"metric": "row_count", "warn": "!= 0"},
                    # distinct_count=5, < 2 → False → pass
                    {"column": "category", "metric": "distinct_count", "fail": "< 2"},
                    # null_percent=2.5, > 1 → True → warn triggered
                    {"column": "price", "metric": "null_percent", "warn": "> 1"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        result = executor.execute()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["passed"] == 1
        assert result.metadata["warned"] == 2
        assert result.metadata["failed"] == 0

    def test_fail_takes_precedence_over_warn(self, tmp_target):
        """When both warn and fail match, fail takes precedence."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    # avg=49.99: fail "> 40" matches, warn "> 30" also matches
                    # fail should take precedence
                    {
                        "column": "price",
                        "metric": "avg",
                        "warn": "> 30",
                        "fail": "> 40",
                    },
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="Profile check failed"):
            executor.execute()

    def test_severity_warn_downgrades_fail_to_success(self, tmp_target):
        """When severity=warn, fails become warnings (SUCCESS)."""
        _write_profile_parquet(tmp_target, "stats", self._make_profile_data())

        node = _make_node("quality", {
            "inputs": [{"ref": "profile.stats"}],
            "params": {"severity": "warn"},
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"metric": "row_count", "fail": "< 5000"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        result = executor.execute()
        # severity=warn downgrades fail to success
        assert result.status == ExecutionStatus.SUCCESS

    def test_no_input_data_raises(self, tmp_target):
        """No input data available — raises ExecutorExecutionError."""
        node = _make_node("quality", {
            "inputs": [{"ref": "profile.nonexistent"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"metric": "row_count", "fail": "= 0"},
                ],
            },
        })
        ctx = _make_context(tmp_target)
        executor = RuleExecutor(node, ctx)
        with pytest.raises(ExecutorExecutionError, match="No input data"):
            executor.execute()


# ──────────────────────────────────────────────────────────────────────────
# Integration test: source -> profile -> rule
# ──────────────────────────────────────────────────────────────────────────

class TestProfileCheckIntegration:
    """End-to-end integration test: profile -> rule pipeline."""

    def test_end_to_end_profile_to_rule(self, tmp_target):
        """Create real data, run ProfileExecutor, then run profile_check rule."""
        from seeknal.workflow.executors.profile_executor import ProfileExecutor  # ty: ignore[unresolved-import]

        # Step 1: Write source data
        source_data = pd.DataFrame({
            "product_id": range(1, 101),
            "price": [10.0 + i * 0.5 for i in range(100)],
            "category": ["A"] * 40 + ["B"] * 30 + ["C"] * 20 + ["D"] * 10,
            "name": [f"Product_{i}" for i in range(1, 101)],
        })
        source_path = tmp_target / "intermediate" / "source_products.parquet"
        source_data.to_parquet(str(source_path), index=False)

        # Step 2: Run ProfileExecutor
        profile_node = Node(
            id="profile.products_stats",
            name="products_stats",
            node_type=NodeType.PROFILE,
            config={
                "inputs": [{"ref": "source.products"}],
            },
        )
        profile_ctx = _make_context(tmp_target)
        profile_executor = ProfileExecutor(profile_node, profile_ctx)
        profile_executor.validate()
        profile_result = profile_executor.execute()
        assert profile_result.status == ExecutionStatus.SUCCESS

        # Step 3: Run profile_check rule against profile output
        rule_node = _make_node("products_quality", {
            "inputs": [{"ref": "profile.products_stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    # row_count should be 100
                    {"metric": "row_count", "fail": "= 0"},
                    # avg price should be reasonable
                    {"column": "price", "metric": "avg", "warn": "> 1000"},
                    # null_percent should be 0
                    {"column": "price", "metric": "null_percent", "fail": "> 0"},
                    # at least 2 distinct categories
                    {"column": "category", "metric": "distinct_count", "fail": "< 2"},
                ],
            },
        })
        rule_ctx = _make_context(tmp_target)
        rule_executor = RuleExecutor(rule_node, rule_ctx)
        rule_executor.validate()
        result = rule_executor.execute()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.metadata["passed"] == 4
        assert result.metadata["warned"] == 0
        assert result.metadata["failed"] == 0

    def test_end_to_end_with_failing_check(self, tmp_target):
        """Integration test where a profile check fails."""
        from seeknal.workflow.executors.profile_executor import ProfileExecutor  # ty: ignore[unresolved-import]

        # Create source data with some nulls
        source_data = pd.DataFrame({
            "product_id": range(1, 51),
            "price": [None] * 25 + [10.0] * 25,  # 50% null
        })
        source_path = tmp_target / "intermediate" / "source_products.parquet"
        source_data.to_parquet(str(source_path), index=False)

        # Run ProfileExecutor
        profile_node = Node(
            id="profile.products_stats",
            name="products_stats",
            node_type=NodeType.PROFILE,
            config={"inputs": [{"ref": "source.products"}]},
        )
        profile_ctx = _make_context(tmp_target)
        profile_executor = ProfileExecutor(profile_node, profile_ctx)
        profile_result = profile_executor.execute()
        assert profile_result.status == ExecutionStatus.SUCCESS

        # Run rule — null_percent should be ~50%, fail > 20 → FAIL
        rule_node = _make_node("products_quality", {
            "inputs": [{"ref": "profile.products_stats"}],
            "rule": {
                "type": "profile_check",
                "checks": [
                    {"column": "price", "metric": "null_percent", "fail": "> 20"},
                ],
            },
        })
        rule_ctx = _make_context(tmp_target)
        rule_executor = RuleExecutor(rule_node, rule_ctx)
        with pytest.raises(ExecutorExecutionError, match="Profile check failed"):
            rule_executor.execute()
