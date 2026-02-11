"""
Tests for Prefect integration.

Tests the Prefect flow wrappers and deployment utilities.
Note: These tests mock subprocess calls since Prefect may not be installed.
"""
import subprocess
from unittest.mock import Mock, patch
from datetime import datetime

import pytest

from seeknal.workflow.prefect_integration import (
    PrefectRunResult,
    run_seeknal_node,
    seeknal_run_flow,
    seeknal_backfill_flow,
    create_prefect_deployment,
    PREFECT_AVAILABLE,
)


class TestPrefectRunResult:
    """Test PrefectRunResult dataclass."""

    def test_create_success_result(self):
        """Test creating a successful result."""
        result = PrefectRunResult(
            success=True,
            exit_code=0,
            stdout="Done!",
            stderr="",
            duration_seconds=5.0,
        )
        assert result.success is True
        assert result.exit_code == 0
        assert result.stdout == "Done!"

    def test_create_failure_result(self):
        """Test creating a failed result."""
        result = PrefectRunResult(
            success=False,
            exit_code=1,
            stdout="",
            stderr="Error!",
            duration_seconds=2.0,
        )
        assert result.success is False
        assert result.exit_code == 1
        assert result.stderr == "Error!"


class TestRunSeeknalNode:
    """Test the run_seeknal_node Prefect task."""

    @patch("seeknal.workflow.prefect_integration.subprocess.run")
    def test_successful_node_run(self, mock_run):
        """Test successful node execution."""
        mock_run.return_value = Mock(
            returncode=0,
            stdout="Node completed",
            stderr="",
        )

        result = run_seeknal_node("/project", "my_node", parallel=True)

        assert result.success is True
        assert result.exit_code == 0
        mock_run.assert_called_once()

    @patch("seeknal.workflow.prefect_integration.subprocess.run")
    def test_failed_node_run(self, mock_run):
        """Test failed node execution."""
        mock_run.return_value = Mock(
            returncode=1,
            stdout="",
            stderr="Node failed",
        )

        result = run_seeknal_node("/project", "my_node", parallel=True)

        assert result.success is False
        assert result.exit_code == 1
        assert result.stderr == "Node failed"

    @patch("seeknal.workflow.prefect_integration.subprocess.run")
    def test_timeout_handling(self, mock_run):
        """Test timeout is handled correctly."""
        from subprocess import TimeoutExpired

        mock_run.side_effect = TimeoutExpired("cmd", 3600)

        result = run_seeknal_node("/project", "my_node", parallel=True)

        assert result.success is False
        assert result.exit_code == -1
        assert "timed out" in result.stderr.lower()


class TestSeeknalRunFlow:
    """Test the seeknal_run_flow Prefect flow."""

    def test_flow_requires_prefect(self):
        """Test flow raises helpful error without Prefect."""
        if not PREFECT_AVAILABLE:
            with pytest.raises(ImportError, match="Prefect is not installed"):
                seeknal_run_flow(project_path="/project")

    @patch("seeknal.workflow.prefect_integration.subprocess.run")
    def test_full_pipeline_execution(self, mock_run):
        """Test running full pipeline."""
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        mock_run.return_value = Mock(
            returncode=0,
            stdout="Pipeline complete",
            stderr="",
        )

        results = seeknal_run_flow("/project", parallel=True)

        assert "pipeline" in results
        assert results["pipeline"].success is True

    @patch("seeknal.workflow.prefect_integration.subprocess.run")
    def test_specific_node_execution(self, mock_run):
        """Test running specific nodes."""
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        mock_run.return_value = Mock(
            returncode=0,
            stdout="",
            stderr="",
        )

        results = seeknal_run_flow(
            "/project",
            parallel=True,
            node_ids=["node1", "node2"],
        )

        assert len(results) == 2
        assert "node1" in results
        assert "node2" in results
        assert all(r.success for r in results.values())


class TestSeeknalBackfillFlow:
    """Test the seeknal_backfill_flow Prefect flow."""

    def test_backfill_requires_prefect(self):
        """Test backfill raises helpful error without Prefect."""
        if not PREFECT_AVAILABLE:
            with pytest.raises(ImportError, match="Prefect is not installed"):
                seeknal_backfill_flow(
                    project_path="/project",
                    node_id="my_node",
                    start_date="2024-01-01",
                    end_date="2024-01-31",
                )

    @patch("seeknal.workflow.prefect_integration.subprocess.run")
    def test_backfill_with_intervals(self, mock_run):
        """Test backfill processes multiple intervals."""
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        mock_run.return_value = Mock(
            returncode=0,
            stdout="",
            stderr="",
        )

        results = seeknal_backfill_flow(
            project_path="/project",
            node_id="my_node",
            start_date="2024-01-01",
            end_date="2024-01-03",  # 3 days = 3 intervals
            schedule="@daily",
        )

        # Should have results for each day
        assert len(results) == 3
        assert all(isinstance(k, str) for k in results.keys())


class TestCreatePrefectDeployment:
    """Test Prefect deployment creation."""

    def test_deployment_requires_prefect(self):
        """Test deployment creation raises helpful error without Prefect."""
        if not PREFECT_AVAILABLE:
            with pytest.raises(ImportError, match="Prefect is not installed"):
                create_prefect_deployment(
                    flow_name="test-flow",
                    project_path="/project",
                )


@pytest.mark.skipif(not PREFECT_AVAILABLE, reason="Prefect not installed")
class TestPrefectIntegrationEndToEnd:
    """End-to-end tests with actual Prefect (if installed)."""

    def test_flow_decorator_preserves_function(self):
        """Test @flow decorator preserves original function behavior."""
        # The function should still be callable
        assert callable(seeknal_run_flow)

    def test_task_decorator_preserves_function(self):
        """Test @task decorator preserves original function behavior."""
        # The function should still be callable
        assert callable(run_seeknal_node)

    def test_exports_available(self):
        """Test expected exports are available."""
        from seeknal.workflow import prefect_integration

        assert hasattr(prefect_integration, "seeknal_run_flow")
        assert hasattr(prefect_integration, "seeknal_backfill_flow")
        assert hasattr(prefect_integration, "PrefectRunResult")
        assert hasattr(prefect_integration, "run_seeknal_node")


class TestIntervalIntegration:
    """Test integration with interval tracking for backfill."""

    @patch("seeknal.workflow.prefect_integration.create_interval_calculator")
    @patch("seeknal.workflow.prefect_integration.subprocess.run")
    def test_backfill_uses_interval_calculator(self, mock_run, mock_calc):
        """Test backfill uses interval calculator for scheduling."""
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        # Mock interval calculator
        from seeknal.workflow.intervals import Interval, IntervalSet

        mock_calc.return_value.get_pending_intervals.return_value = [
            Interval(
                start=datetime(2024, 1, 1, 0, 0),
                end=datetime(2024, 1, 1, 23, 59, 59),
            ),
            Interval(
                start=datetime(2024, 1, 2, 0, 0),
                end=datetime(2024, 1, 2, 23, 59, 59),
            ),
        ]

        mock_run.return_value = Mock(
            returncode=0,
            stdout="",
            stderr="",
        )

        results = seeknal_backfill_flow(
            project_path="/project",
            node_id="my_node",
            start_date="2024-01-01",
            end_date="2024-01-02",
            schedule="@daily",
        )

        # Should have 2 intervals processed
        assert len(results) == 2
