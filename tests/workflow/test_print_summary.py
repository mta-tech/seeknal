"""Tests for print_summary data quality warning visibility."""

import pytest

from seeknal.workflow.executors.base import ExecutionStatus
from seeknal.workflow.runner import ExecutionSummary, NodeResult, print_summary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_result(node_id, metadata=None):
    return NodeResult(
        node_id=node_id,
        status=ExecutionStatus.SUCCESS,
        duration=0.01,
        row_count=0,
        metadata=metadata or {},
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSummaryWarningCounts:
    """Verify that rule warnings appear in the execution summary."""

    def test_regular_rule_all_passed(self, capsys):
        """Standard rules that pass show only 'passed' count."""
        summary = ExecutionSummary(
            total_nodes=2, changed_nodes=2, successful_nodes=2,
            total_duration=0.1,
            results=[
                _make_result("rule.not_null", {
                    "rule_type": "null", "passed": True,
                    "violations": 0, "severity": "error",
                }),
                _make_result("rule.range_check", {
                    "rule_type": "range", "passed": True,
                    "violations": 0, "severity": "error",
                }),
            ],
        )
        print_summary(summary)
        out = capsys.readouterr().out
        assert "2 passed" in out
        assert "warning" not in out.lower()

    def test_regular_rule_warn_severity(self, capsys):
        """A rule with severity=warn that fails shows as warning."""
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, successful_nodes=1,
            total_duration=0.1,
            results=[
                _make_result("rule.price_check", {
                    "rule_type": "range", "passed": False,
                    "violations": 2, "severity": "warn",
                    "message": "price out of range",
                }),
            ],
        )
        print_summary(summary)
        out = capsys.readouterr().out
        assert "1 warning" in out

    def test_profile_check_with_warnings_shows_in_summary(self, capsys):
        """profile_check with warned > 0 must count as warning, not passed."""
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, successful_nodes=1,
            total_duration=0.1,
            results=[
                _make_result("rule.products_quality", {
                    "rule_type": "profile_check",
                    "checks": 4,
                    "passed": 3,
                    "warned": 1,
                    "failed": 0,
                    "severity": "warn",
                    "results": [
                        {"status": "pass", "message": "row_count = 6"},
                        {"status": "warn", "message": "price.avg = 85.58 (warns: > 50)"},
                        {"status": "pass", "message": "price.null_percent = 0.0"},
                        {"status": "pass", "message": "category.distinct_count = 5"},
                    ],
                }),
            ],
        )
        print_summary(summary)
        out = capsys.readouterr().out
        assert "1 warning" in out
        # Must NOT count as passed
        assert "1 passed" not in out

    def test_profile_check_no_warnings_counts_as_passed(self, capsys):
        """profile_check with warned=0 counts as passed."""
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, successful_nodes=1,
            total_duration=0.1,
            results=[
                _make_result("rule.products_quality", {
                    "rule_type": "profile_check",
                    "checks": 3,
                    "passed": 3,
                    "warned": 0,
                    "failed": 0,
                    "severity": "warn",
                    "results": [
                        {"status": "pass", "message": "row_count = 6"},
                        {"status": "pass", "message": "price.null_percent = 0.0"},
                        {"status": "pass", "message": "category.distinct_count = 5"},
                    ],
                }),
            ],
        )
        print_summary(summary)
        out = capsys.readouterr().out
        assert "1 passed" in out
        assert "warning" not in out.lower()

    def test_mixed_rules_and_profile_check(self, capsys):
        """Mixed: 2 regular passed + 1 profile_check with warnings."""
        summary = ExecutionSummary(
            total_nodes=3, changed_nodes=3, successful_nodes=3,
            total_duration=0.1,
            results=[
                _make_result("rule.not_null", {
                    "rule_type": "null", "passed": True,
                    "violations": 0, "severity": "error",
                }),
                _make_result("rule.range_check", {
                    "rule_type": "range", "passed": True,
                    "violations": 0, "severity": "error",
                }),
                _make_result("rule.products_quality", {
                    "rule_type": "profile_check",
                    "checks": 2,
                    "passed": 1,
                    "warned": 1,
                    "failed": 0,
                    "severity": "warn",
                    "results": [
                        {"status": "pass", "message": "row_count = 6"},
                        {"status": "warn", "message": "price.avg = 85.58 (warns: > 50)"},
                    ],
                }),
            ],
        )
        print_summary(summary)
        out = capsys.readouterr().out
        assert "2 passed" in out
        assert "1 warning" in out


class TestSummaryWarningDetails:
    """Verify that warning details are shown after the summary box."""

    def test_profile_check_warnings_shown_in_detail(self, capsys):
        """profile_check warnings must appear in the detail section."""
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, successful_nodes=1,
            total_duration=0.1,
            results=[
                _make_result("rule.products_quality", {
                    "rule_type": "profile_check",
                    "checks": 2,
                    "passed": 1,
                    "warned": 1,
                    "failed": 0,
                    "severity": "warn",
                    "results": [
                        {"status": "pass", "message": "row_count = 6"},
                        {"status": "warn", "message": "price.avg = 85.58 (warns: > 50)"},
                    ],
                }),
            ],
        )
        print_summary(summary)
        out = capsys.readouterr().out
        assert "Data quality warnings:" in out
        assert "rule.products_quality: price.avg = 85.58" in out

    def test_regular_warn_rule_shown_in_detail(self, capsys):
        """Regular rule with severity=warn shows message in detail."""
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, successful_nodes=1,
            total_duration=0.1,
            results=[
                _make_result("rule.price_check", {
                    "rule_type": "range", "passed": False,
                    "violations": 2, "severity": "warn",
                    "message": "2 rows out of range",
                }),
            ],
        )
        print_summary(summary)
        out = capsys.readouterr().out
        assert "Data quality warnings:" in out
        assert "rule.price_check: 2 rows out of range" in out

    def test_no_warnings_no_detail(self, capsys):
        """No warnings -> no detail section."""
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, successful_nodes=1,
            total_duration=0.1,
            results=[
                _make_result("rule.not_null", {
                    "rule_type": "null", "passed": True,
                    "violations": 0, "severity": "error",
                }),
            ],
        )
        print_summary(summary)
        out = capsys.readouterr().out
        assert "Data quality warnings:" not in out
