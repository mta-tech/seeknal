"""
Tests for deep Prefect integration.

Tests the SeeknalPrefectFlow class, layer-based execution,
state sync, consolidation, and artifact generation.
Note: These tests mock the DAGRunner since Prefect may not be installed.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from unittest.mock import MagicMock, Mock, patch, PropertyMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _FakeNodeState:
    """Minimal stub for NodeState in run_state.nodes."""
    def __init__(self, status="success"):
        self.status = status
        self.last_watermark = None
        self.pg_last_watermark = None
        self.fingerprint = ""


class _FakeRunState:
    """Minimal stub for RunState."""
    def __init__(self, nodes=None):
        self.nodes = nodes or {}
        self.run_id = "test-run-001"
        self.schema_version = "1.0"
        self.seeknal_version = "2.4.0"
        self.last_run = ""
        self.config = {}


class _FakeEdge:
    def __init__(self, from_node, to_node):
        self.from_node = from_node
        self.to_node = to_node


class _FakeNodeType:
    def __init__(self, value):
        self.value = value


class _FakeNode:
    def __init__(self, node_id, name, node_type_str="source"):
        self.id = node_id
        self.name = name
        self.node_type = _FakeNodeType(node_type_str)
        self.config = {}
        self.tags = []
        self.file_path = ""


class _FakeManifest:
    """Minimal stub for Manifest."""
    def __init__(self, nodes_dict, edges_list):
        self.nodes = nodes_dict
        self.edges = [_FakeEdge(f, t) for f, t in edges_list]
        self.project = "test-project"

    def get_node(self, node_id):
        return self.nodes.get(node_id)

    def get_upstream_nodes(self, node_id):
        return {e.from_node for e in self.edges if e.to_node == node_id}

    def get_downstream_nodes(self, node_id):
        return {e.to_node for e in self.edges if e.from_node == node_id}


def _make_runner(nodes_dict, edges, run_state=None, cached_nodes=None):
    """Create a mock DAGRunner with specified nodes and edges."""
    manifest = _FakeManifest(nodes_dict, edges)

    runner = MagicMock()
    runner.manifest = manifest
    runner.run_state = run_state or _FakeRunState()
    runner.state_path = Path("/tmp/test_state.json")
    runner.target_path = Path("/tmp/test_target")
    runner._full_refresh = False

    # Implement _get_topological_layers using Kahn's algorithm
    def _get_topological_layers():
        in_degree = {nid: 0 for nid in manifest.nodes}
        downstream_adj = {nid: set() for nid in manifest.nodes}
        for edge in manifest.edges:
            if edge.to_node in in_degree and edge.from_node in in_degree:
                in_degree[edge.to_node] += 1
            if edge.from_node in downstream_adj and edge.to_node in downstream_adj:
                downstream_adj[edge.from_node].add(edge.to_node)

        layers = []
        remaining = set(manifest.nodes.keys())
        while remaining:
            layer = sorted([nid for nid in remaining if in_degree[nid] == 0])
            if not layer:
                raise ValueError("DAG contains cycles")
            layers.append(layer)
            remaining -= set(layer)
            for nid in layer:
                for ds in downstream_adj.get(nid, set()):
                    if ds in in_degree:
                        in_degree[ds] -= 1
        return layers

    runner._get_topological_layers = _get_topological_layers

    # Implement _is_cached
    _cached = set(cached_nodes or [])
    runner._is_cached = lambda nid: nid in _cached

    return runner


# ---------------------------------------------------------------------------
# Tests: Module-level imports and PREFECT_AVAILABLE
# ---------------------------------------------------------------------------

class TestPrefectAvailability:
    """Test optional dependency handling."""

    def test_import_succeeds(self):
        """Module imports without error regardless of Prefect presence."""
        from seeknal.workflow.prefect_integration import PREFECT_AVAILABLE
        assert isinstance(PREFECT_AVAILABLE, bool)

    def test_require_prefect_raises_when_missing(self):
        """_require_prefect raises ImportError with install instructions."""
        from seeknal.workflow.prefect_integration import _require_prefect, PREFECT_AVAILABLE
        if not PREFECT_AVAILABLE:
            with pytest.raises(ImportError, match="pip install seeknal\\[prefect\\]"):
                _require_prefect()

    def test_noop_decorators_when_missing(self):
        """No-op decorators don't crash when Prefect is absent."""
        from seeknal.workflow.prefect_integration import PREFECT_AVAILABLE
        if not PREFECT_AVAILABLE:
            from seeknal.workflow.prefect_integration import flow, task
            # Decorators should return functions unchanged
            @flow(name="test")
            def my_flow():
                return 42
            assert my_flow() == 42

            @task(name="test")
            def my_task():
                return 99
            assert my_task() == 99

    def test_exports_available(self):
        """Expected exports are available."""
        from seeknal.workflow import prefect_integration
        assert hasattr(prefect_integration, "SeeknalPrefectFlow")
        assert hasattr(prefect_integration, "PrefectNodeResult")
        assert hasattr(prefect_integration, "run_node_task")
        assert hasattr(prefect_integration, "seeknal_backfill_flow")
        assert hasattr(prefect_integration, "create_pipeline_flow")


# ---------------------------------------------------------------------------
# Tests: PrefectNodeResult
# ---------------------------------------------------------------------------

class TestPrefectNodeResult:
    """Test the PrefectNodeResult dataclass."""

    def test_success_result(self):
        from seeknal.workflow.prefect_integration import PrefectNodeResult
        r = PrefectNodeResult(
            node_id="source.users",
            status="success",
            duration=1.5,
            row_count=1000,
        )
        assert r.status == "success"
        assert r.row_count == 1000

    def test_failed_result(self):
        from seeknal.workflow.prefect_integration import PrefectNodeResult
        r = PrefectNodeResult(
            node_id="transform.bad",
            status="failed",
            error_message="SQL error",
        )
        assert r.status == "failed"
        assert r.error_message == "SQL error"


# ---------------------------------------------------------------------------
# Tests: run_node_task
# ---------------------------------------------------------------------------

class TestRunNodeTask:
    """Test the Prefect task that executes a single node."""

    def test_successful_execution(self):
        """run_node_task returns success when _execute_node succeeds."""
        from seeknal.workflow.prefect_integration import run_node_task
        from seeknal.workflow.runner import ExecutionStatus, NodeResult

        mock_runner = MagicMock()
        mock_runner._execute_node.return_value = NodeResult(
            node_id="source.users",
            status=ExecutionStatus.SUCCESS,
            duration=2.5,
            row_count=500,
        )

        # Call the underlying function (not the Prefect-decorated version)
        result = run_node_task.fn(mock_runner, "source.users")

        assert result.node_id == "source.users"
        assert result.status == "success"
        assert result.duration == 2.5
        assert result.row_count == 500
        mock_runner._execute_node.assert_called_once_with("source.users")

    def test_failed_execution(self):
        """run_node_task returns failure when _execute_node fails."""
        from seeknal.workflow.prefect_integration import run_node_task
        from seeknal.workflow.runner import ExecutionStatus, NodeResult

        mock_runner = MagicMock()
        mock_runner._execute_node.return_value = NodeResult(
            node_id="transform.bad",
            status=ExecutionStatus.FAILED,
            error_message="SQL syntax error",
        )

        result = run_node_task.fn(mock_runner, "transform.bad")

        assert result.status == "failed"
        assert result.error_message == "SQL syntax error"

    def test_exception_handling(self):
        """run_node_task catches exceptions from _execute_node."""
        from seeknal.workflow.prefect_integration import run_node_task

        mock_runner = MagicMock()
        mock_runner._execute_node.side_effect = RuntimeError("Connection lost")

        result = run_node_task.fn(mock_runner, "source.users")

        assert result.status == "failed"
        assert "Connection lost" in result.error_message


# ---------------------------------------------------------------------------
# Tests: RUNNING state recovery
# ---------------------------------------------------------------------------

class TestRunningStateRecovery:
    """Test crash recovery for RUNNING nodes."""

    def test_running_nodes_reset_to_failed(self):
        """Nodes stuck in RUNNING are reset to FAILED."""
        from seeknal.workflow.prefect_integration import _recover_running_nodes

        state = _FakeRunState(nodes={
            "source.a": _FakeNodeState("success"),
            "transform.b": _FakeNodeState("running"),
            "transform.c": _FakeNodeState("running"),
        })

        log = MagicMock()
        _recover_running_nodes(state, log)

        assert state.nodes["source.a"].status == "success"
        assert state.nodes["transform.b"].status == "failed"
        assert state.nodes["transform.c"].status == "failed"
        assert log.warning.call_count == 2

    def test_no_running_nodes_is_noop(self):
        """No warnings when no RUNNING nodes exist."""
        from seeknal.workflow.prefect_integration import _recover_running_nodes

        state = _FakeRunState(nodes={
            "source.a": _FakeNodeState("success"),
            "transform.b": _FakeNodeState("failed"),
        })

        log = MagicMock()
        _recover_running_nodes(state, log)

        assert log.warning.call_count == 0


# ---------------------------------------------------------------------------
# Tests: _get_nodes_to_run
# ---------------------------------------------------------------------------

class TestGetNodesToRun:
    """Test node selection logic."""

    def test_full_refresh_returns_all(self):
        """Full refresh runs every node."""
        from seeknal.workflow.prefect_integration import _get_nodes_to_run

        runner = _make_runner(
            {"a": _FakeNode("a", "a"), "b": _FakeNode("b", "b")},
            [],
            cached_nodes={"a"},
        )

        result = _get_nodes_to_run(runner, full_refresh=True)
        assert result == {"a", "b"}

    def test_skip_cached_nodes(self):
        """Cached nodes are skipped when not full_refresh."""
        from seeknal.workflow.prefect_integration import _get_nodes_to_run

        runner = _make_runner(
            {"a": _FakeNode("a", "a"), "b": _FakeNode("b", "b"), "c": _FakeNode("c", "c")},
            [],
            cached_nodes={"a"},
        )

        result = _get_nodes_to_run(runner, full_refresh=False)
        assert result == {"b", "c"}


# ---------------------------------------------------------------------------
# Tests: _batch_update_state
# ---------------------------------------------------------------------------

class TestBatchUpdateState:
    """Test per-layer state updates."""

    def test_updates_all_nodes_in_layer(self, tmp_path):
        """All results from a layer are written in a single batch."""
        from seeknal.workflow.prefect_integration import _batch_update_state, PrefectNodeResult
        from seeknal.workflow.state import RunState

        # Use a real RunState for this test
        runner = MagicMock()
        runner.run_state = RunState()
        runner.state_path = tmp_path / "run_state.json"

        layer_results = {
            "a": PrefectNodeResult("a", "success", duration=1.0, row_count=100),
            "b": PrefectNodeResult("b", "failed", error_message="error"),
        }

        _batch_update_state(runner, layer_results)

        # State should be updated
        assert "a" in runner.run_state.nodes
        assert "b" in runner.run_state.nodes
        assert runner.run_state.nodes["a"].status == "success"
        assert runner.run_state.nodes["b"].status == "failed"
        # State file should have been written
        assert runner.state_path.exists()


# ---------------------------------------------------------------------------
# Tests: _run_consolidation
# ---------------------------------------------------------------------------

class TestRunConsolidation:
    """Test entity consolidation in Prefect context."""

    def test_skips_when_no_fg_nodes(self):
        """Consolidation is skipped when no feature_group nodes exist."""
        from seeknal.workflow.prefect_integration import _run_consolidation, PrefectNodeResult

        runner = _make_runner(
            {"source.a": _FakeNode("source.a", "a", "source")},
            [],
        )

        log = MagicMock()
        _run_consolidation(runner, [], log)
        # No error, no consolidation attempted

    def test_skips_when_no_fg_succeeded(self):
        """Consolidation is skipped when no FG nodes succeeded."""
        from seeknal.workflow.prefect_integration import _run_consolidation, PrefectNodeResult

        runner = _make_runner(
            {"feature_group.users": _FakeNode("feature_group.users", "users", "feature_group")},
            [],
        )

        results = [PrefectNodeResult("feature_group.users", "failed")]
        log = MagicMock()
        _run_consolidation(runner, results, log)
        log.info.assert_any_call("No FG nodes succeeded — skipping consolidation")

    @patch("seeknal.workflow.consolidation.consolidator.EntityConsolidator")
    def test_runs_consolidation_on_success(self, mock_consolidator_cls):
        """Consolidation runs when FG nodes succeed."""
        from seeknal.workflow.prefect_integration import _run_consolidation, PrefectNodeResult

        runner = _make_runner(
            {"feature_group.users": _FakeNode("feature_group.users", "users", "feature_group")},
            [],
        )

        mock_consolidator = MagicMock()
        mock_consolidator.consolidate_all.return_value = []
        mock_consolidator_cls.return_value = mock_consolidator

        results = [PrefectNodeResult("feature_group.users", "success")]
        log = MagicMock()
        _run_consolidation(runner, results, log)

    @patch("seeknal.workflow.consolidation.consolidator.EntityConsolidator")
    def test_never_fails_the_flow(self, mock_consolidator_cls):
        """Consolidation errors are caught — never raises."""
        from seeknal.workflow.prefect_integration import _run_consolidation, PrefectNodeResult

        mock_consolidator_cls.side_effect = RuntimeError("boom")

        runner = _make_runner(
            {"feature_group.users": _FakeNode("feature_group.users", "users", "feature_group")},
            [],
        )

        results = [PrefectNodeResult("feature_group.users", "success")]
        log = MagicMock()
        # Should NOT raise
        _run_consolidation(runner, results, log)
        log.warning.assert_called()


# ---------------------------------------------------------------------------
# Tests: _create_results_artifact
# ---------------------------------------------------------------------------

class TestCreateResultsArtifact:
    """Test Prefect markdown artifact creation."""

    def test_creates_markdown_artifact(self):
        """Artifact is created with correct content."""
        from seeknal.workflow.prefect_integration import (
            _create_results_artifact, PrefectNodeResult, PREFECT_AVAILABLE,
        )
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        with patch("prefect.artifacts.create_markdown_artifact") as mock_create:
            runner = MagicMock()
            runner.manifest.project = "test-project"
            runner.run_state.run_id = "test-001"

            results = [
                PrefectNodeResult("source.a", "success", duration=1.0, row_count=100),
                PrefectNodeResult("transform.b", "failed", error_message="SQL error"),
            ]

            _create_results_artifact(results, runner)

            mock_create.assert_called_once()
            call_kwargs = mock_create.call_args[1]
            assert "seeknal-run-test-001" in call_kwargs["key"]
            assert "source.a" in call_kwargs["markdown"]
            assert "SQL error" in call_kwargs["markdown"]

    def test_noop_without_prefect(self):
        """No crash when Prefect is not installed."""
        from seeknal.workflow.prefect_integration import _create_results_artifact, PREFECT_AVAILABLE
        if PREFECT_AVAILABLE:
            pytest.skip("Test for when Prefect is NOT installed")

        # Should not raise
        _create_results_artifact([], MagicMock())


# ---------------------------------------------------------------------------
# Tests: SeeknalPrefectFlow
# ---------------------------------------------------------------------------

class TestSeeknalPrefectFlow:
    """Test the main SeeknalPrefectFlow class."""

    def test_requires_prefect(self):
        """SeeknalPrefectFlow raises ImportError without Prefect."""
        from seeknal.workflow.prefect_integration import SeeknalPrefectFlow, PREFECT_AVAILABLE
        if PREFECT_AVAILABLE:
            pytest.skip("Test for when Prefect is NOT installed")

        with pytest.raises(ImportError, match="pip install seeknal\\[prefect\\]"):
            SeeknalPrefectFlow(project_path=Path("/tmp/test"))

    @pytest.mark.skipif(
        not __import__("seeknal.workflow.prefect_integration", fromlist=["PREFECT_AVAILABLE"]).PREFECT_AVAILABLE,
        reason="Prefect not installed"
    )
    def test_init_stores_config(self):
        """SeeknalPrefectFlow stores configuration correctly."""
        from seeknal.workflow.prefect_integration import SeeknalPrefectFlow

        spf = SeeknalPrefectFlow(
            project_path=Path("/tmp/test"),
            max_workers=4,
            continue_on_error=True,
            env="staging",
            full_refresh=True,
            params={"key": "val"},
        )
        assert spf.max_workers == 4
        assert spf.continue_on_error is True
        assert spf.env == "staging"
        assert spf.full_refresh is True
        assert spf.params == {"key": "val"}

    @pytest.mark.skipif(
        not __import__("seeknal.workflow.prefect_integration", fromlist=["PREFECT_AVAILABLE"]).PREFECT_AVAILABLE,
        reason="Prefect not installed"
    )
    @patch("seeknal.workflow.prefect_integration.SeeknalPrefectFlow._build_runner")
    def test_build_flow_returns_callable(self, mock_build):
        """_build_flow returns a callable flow function."""
        from seeknal.workflow.prefect_integration import SeeknalPrefectFlow

        spf = SeeknalPrefectFlow(project_path=Path("/tmp/test"))
        flow_fn = spf._build_flow()
        assert callable(flow_fn)

    @pytest.mark.skipif(
        not __import__("seeknal.workflow.prefect_integration", fromlist=["PREFECT_AVAILABLE"]).PREFECT_AVAILABLE,
        reason="Prefect not installed"
    )
    def test_generate_prefect_yaml(self):
        """generate_prefect_yaml produces valid YAML."""
        import yaml
        from seeknal.workflow.prefect_integration import SeeknalPrefectFlow

        spf = SeeknalPrefectFlow(
            project_path=Path("/tmp/my-project"),
            max_workers=4,
        )

        yaml_str = spf.generate_prefect_yaml()
        parsed = yaml.safe_load(yaml_str)

        assert "deployments" in parsed
        deploy = parsed["deployments"][0]
        assert "seeknal-my-project" == deploy["name"]
        assert deploy["parameters"]["max_workers"] == 4


# ---------------------------------------------------------------------------
# Tests: Layer-based execution integration
# ---------------------------------------------------------------------------

class TestLayerExecution:
    """Test the _execute_pipeline logic with mock runner."""

    @patch("seeknal.workflow.state.save_state")
    @patch("seeknal.workflow.state.update_node_state")
    @patch("seeknal.workflow.prefect_integration._create_results_artifact")
    @patch("seeknal.workflow.prefect_integration.run_node_task")
    def test_executes_layers_in_order(self, mock_task, mock_artifact, mock_update, mock_save):
        """Nodes execute in correct topological layer order."""
        from seeknal.workflow.prefect_integration import (
            _execute_pipeline, SeeknalPrefectFlow, PrefectNodeResult,
            PREFECT_AVAILABLE,
        )
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        # DAG: a -> b -> c
        nodes = {
            "a": _FakeNode("a", "a"),
            "b": _FakeNode("b", "b"),
            "c": _FakeNode("c", "c"),
        }
        edges = [("a", "b"), ("b", "c")]
        runner = _make_runner(nodes, edges)

        # Mock direct task call to track call order
        call_order = []

        def fake_call(r, nid):
            call_order.append(nid)
            return PrefectNodeResult(nid, "success", 1.0, 100)

        mock_task.side_effect = fake_call

        builder = MagicMock(spec=SeeknalPrefectFlow)
        builder._build_runner.return_value = (runner, Path("/tmp/target"))
        builder.project_path = Path("/tmp/test")

        results = _execute_pipeline(builder, max_workers=4, continue_on_error=False, full_refresh=True)

        # a runs first, then b, then c
        assert call_order == ["a", "b", "c"]
        assert len(results) == 3

    @patch("seeknal.workflow.state.save_state")
    @patch("seeknal.workflow.state.update_node_state")
    @patch("seeknal.workflow.prefect_integration._create_results_artifact")
    @patch("seeknal.workflow.prefect_integration.run_node_task")
    def test_parallel_nodes_in_same_layer(self, mock_task, mock_artifact, mock_update, mock_save):
        """Independent nodes execute in the same layer."""
        from seeknal.workflow.prefect_integration import (
            _execute_pipeline, SeeknalPrefectFlow, PrefectNodeResult,
            PREFECT_AVAILABLE,
        )
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        # DAG: a, b (independent) -> c
        nodes = {
            "a": _FakeNode("a", "a"),
            "b": _FakeNode("b", "b"),
            "c": _FakeNode("c", "c"),
        }
        edges = [("a", "c"), ("b", "c")]
        runner = _make_runner(nodes, edges)

        def fake_call(r, nid):
            return PrefectNodeResult(nid, "success", 1.0, 100)

        mock_task.side_effect = fake_call

        builder = MagicMock(spec=SeeknalPrefectFlow)
        builder._build_runner.return_value = (runner, Path("/tmp/target"))
        builder.project_path = Path("/tmp/test")

        results = _execute_pipeline(builder, max_workers=4, continue_on_error=False, full_refresh=True)

        # All 3 nodes should have results
        result_ids = {r.node_id for r in results}
        assert result_ids == {"a", "b", "c"}

    @patch("seeknal.workflow.state.save_state")
    @patch("seeknal.workflow.state.update_node_state")
    @patch("seeknal.workflow.prefect_integration._create_results_artifact")
    @patch("seeknal.workflow.prefect_integration.run_node_task")
    def test_skips_cached_nodes(self, mock_task, mock_artifact, mock_update, mock_save):
        """Cached nodes are skipped and recorded as cached."""
        from seeknal.workflow.prefect_integration import (
            _execute_pipeline, SeeknalPrefectFlow, PrefectNodeResult,
            PREFECT_AVAILABLE,
        )
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        nodes = {
            "a": _FakeNode("a", "a"),
            "b": _FakeNode("b", "b"),
        }
        runner = _make_runner(nodes, [], cached_nodes={"a"})

        called = []

        def fake_call(r, nid):
            called.append(nid)
            return PrefectNodeResult(nid, "success", 1.0, 100)

        mock_task.side_effect = fake_call

        builder = MagicMock(spec=SeeknalPrefectFlow)
        builder._build_runner.return_value = (runner, Path("/tmp/target"))
        builder.project_path = Path("/tmp/test")

        results = _execute_pipeline(builder, max_workers=4, continue_on_error=False, full_refresh=False)

        # Only "b" should be called; "a" is cached
        assert called == ["b"]
        cached_results = [r for r in results if r.status == "cached"]
        assert len(cached_results) == 1
        assert cached_results[0].node_id == "a"

    @patch("seeknal.workflow.state.save_state")
    @patch("seeknal.workflow.state.update_node_state")
    @patch("seeknal.workflow.prefect_integration._create_results_artifact")
    @patch("seeknal.workflow.prefect_integration.run_node_task")
    def test_failure_skips_downstream(self, mock_task, mock_artifact, mock_update, mock_save):
        """When a node fails, downstream nodes are skipped."""
        from seeknal.workflow.prefect_integration import (
            _execute_pipeline, SeeknalPrefectFlow, PrefectNodeResult,
            PREFECT_AVAILABLE,
        )
        if not PREFECT_AVAILABLE:
            pytest.skip("Prefect not installed")

        # DAG: a -> b -> c
        nodes = {
            "a": _FakeNode("a", "a"),
            "b": _FakeNode("b", "b"),
            "c": _FakeNode("c", "c"),
        }
        edges = [("a", "b"), ("b", "c")]
        runner = _make_runner(nodes, edges)

        def fake_call(r, nid):
            if nid == "a":
                return PrefectNodeResult(nid, "failed", error_message="boom")
            return PrefectNodeResult(nid, "success", 1.0, 100)

        mock_task.side_effect = fake_call

        builder = MagicMock(spec=SeeknalPrefectFlow)
        builder._build_runner.return_value = (runner, Path("/tmp/target"))
        builder.project_path = Path("/tmp/test")

        # continue_on_error=False: stops after first failure
        results = _execute_pipeline(builder, max_workers=4, continue_on_error=False, full_refresh=True)

        # Only "a" was executed (failed), b and c never called
        executed = [r for r in results if r.status in ("success", "failed")]
        assert len(executed) == 1
        assert executed[0].node_id == "a"


# ---------------------------------------------------------------------------
# Tests: Backfill flow
# ---------------------------------------------------------------------------

class TestBackfillFlow:
    """Test the backfill Prefect flow."""

    def test_backfill_requires_prefect(self):
        """Backfill raises helpful error without Prefect."""
        from seeknal.workflow.prefect_integration import seeknal_backfill_flow, PREFECT_AVAILABLE
        if PREFECT_AVAILABLE:
            pytest.skip("Test for when Prefect is NOT installed")

        with pytest.raises(ImportError, match="pip install seeknal\\[prefect\\]"):
            seeknal_backfill_flow(
                project_path="/project",
                node_id="my_node",
                start_date="2024-01-01",
                end_date="2024-01-31",
            )


# ---------------------------------------------------------------------------
# Tests: Exposure schedule validation
# ---------------------------------------------------------------------------

class TestExposureScheduleValidation:
    """Test schedule field validation in exposure YAML."""

    def test_valid_schedule(self):
        """Valid schedule with cron field passes validation."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": {"cron": "0 8 * * MON"},
        }
        result = _validate_report_exposure(data, Path("test.yml"))
        assert result["schedule"]["cron"] == "0 8 * * MON"

    def test_schedule_with_timezone(self):
        """Schedule with timezone is preserved."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": {"cron": "0 8 * * MON", "timezone": "US/Eastern"},
        }
        result = _validate_report_exposure(data, Path("test.yml"))
        assert result["schedule"]["timezone"] == "US/Eastern"

    def test_schedule_without_cron_fails(self):
        """Schedule without cron field fails validation."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": {"timezone": "UTC"},
        }
        with pytest.raises(ValueError, match="schedule.cron is required"):
            _validate_report_exposure(data, Path("test.yml"))

    def test_invalid_cron_format(self):
        """Invalid cron (wrong number of fields) fails validation."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": {"cron": "0 8 *"},  # Only 3 fields
        }
        with pytest.raises(ValueError, match="5 fields"):
            _validate_report_exposure(data, Path("test.yml"))

    def test_schedule_not_dict_fails(self):
        """Schedule as a string fails validation."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
            "schedule": "0 8 * * MON",
        }
        with pytest.raises(ValueError, match="schedule must be a mapping"):
            _validate_report_exposure(data, Path("test.yml"))

    def test_no_schedule_still_works(self):
        """Exposures without schedule continue to work."""
        from seeknal.ask.report.exposure import _validate_report_exposure

        data = {
            "kind": "exposure",
            "type": "report",
            "name": "test",
            "sections": [{"title": "Test"}],
        }
        result = _validate_report_exposure(data, Path("test.yml"))
        assert "schedule" not in result or result.get("schedule") is None
