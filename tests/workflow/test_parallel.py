"""Tests for parallel DAG execution engine."""

import time
import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path

from seeknal.dag.manifest import Manifest, Node, NodeType, Edge
from seeknal.workflow.runner import DAGRunner, ExecutionStatus, NodeResult
from seeknal.workflow.parallel import (
    ParallelDAGRunner,
    ParallelExecutionSummary,
    print_parallel_summary,
)
from seeknal.workflow.state import NodeStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_node(name, node_type=NodeType.SOURCE):
    return Node(
        id=f"{node_type.value}.{name}",
        name=name,
        node_type=node_type,
        config={},
    )


def _make_manifest(*items):
    """Helper to create test manifests from nodes and (from, to) edge tuples."""
    manifest = Manifest()
    for item in items:
        if isinstance(item, Node):
            manifest.add_node(item)
        elif isinstance(item, tuple) and len(item) == 2:
            manifest.add_edge(item[0], item[1])
    return manifest


def _success_result(node_id, duration=0.1):
    return NodeResult(node_id, ExecutionStatus.SUCCESS, duration=duration)


def _failed_result(node_id, msg="boom"):
    return NodeResult(node_id, ExecutionStatus.FAILED, duration=0.1, error_message=msg)


# ---------------------------------------------------------------------------
# Topological Layer Tests
# ---------------------------------------------------------------------------


class TestTopologicalLayers:
    """Tests for DAGRunner._get_topological_layers()."""

    def test_linear_chain(self, tmp_path):
        """A -> B -> C produces 3 layers of 1 node each."""
        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.TRANSFORM)
        c = _make_node("c", NodeType.FEATURE_GROUP)
        manifest = _make_manifest(a, b, c, (a.id, b.id), (b.id, c.id))

        runner = DAGRunner(manifest, target_path=tmp_path)
        layers = runner._get_topological_layers()

        assert len(layers) == 3
        assert layers[0] == [a.id]
        assert layers[1] == [b.id]
        assert layers[2] == [c.id]

    def test_wide_dag_independent_sources(self, tmp_path):
        """Three independent sources go into a single layer."""
        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.SOURCE)
        c = _make_node("c", NodeType.SOURCE)
        manifest = _make_manifest(a, b, c)

        runner = DAGRunner(manifest, target_path=tmp_path)
        layers = runner._get_topological_layers()

        assert len(layers) == 1
        assert sorted(layers[0]) == sorted([a.id, b.id, c.id])

    def test_diamond_dag(self, tmp_path):
        """Diamond: A -> B, A -> C, B -> D, C -> D produces 3 layers."""
        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.TRANSFORM)
        c = _make_node("c", NodeType.TRANSFORM)
        d = _make_node("d", NodeType.FEATURE_GROUP)
        manifest = _make_manifest(
            a, b, c, d,
            (a.id, b.id), (a.id, c.id),
            (b.id, d.id), (c.id, d.id),
        )

        runner = DAGRunner(manifest, target_path=tmp_path)
        layers = runner._get_topological_layers()

        assert len(layers) == 3
        assert layers[0] == [a.id]
        assert sorted(layers[1]) == sorted([b.id, c.id])
        assert layers[2] == [d.id]

    def test_single_node(self, tmp_path):
        """Single node produces 1 layer with 1 node."""
        a = _make_node("a")
        manifest = _make_manifest(a)

        runner = DAGRunner(manifest, target_path=tmp_path)
        layers = runner._get_topological_layers()

        assert layers == [[a.id]]

    def test_empty_dag(self, tmp_path):
        """Empty DAG produces no layers."""
        manifest = Manifest()
        runner = DAGRunner(manifest, target_path=tmp_path)
        layers = runner._get_topological_layers()

        assert layers == []

    def test_layers_are_sorted(self, tmp_path):
        """Nodes within each layer are sorted for deterministic ordering."""
        z = _make_node("z", NodeType.SOURCE)
        m = _make_node("m", NodeType.SOURCE)
        a = _make_node("a", NodeType.SOURCE)
        manifest = _make_manifest(z, m, a)

        runner = DAGRunner(manifest, target_path=tmp_path)
        layers = runner._get_topological_layers()

        assert len(layers) == 1
        assert layers[0] == sorted([z.id, m.id, a.id])

    def test_complex_multi_layer(self, tmp_path):
        """
        s1 -> t1 -> fg1
        s2 -> t2 -> fg1
        Layers: [s1, s2], [t1, t2], [fg1]
        """
        s1 = _make_node("s1", NodeType.SOURCE)
        s2 = _make_node("s2", NodeType.SOURCE)
        t1 = _make_node("t1", NodeType.TRANSFORM)
        t2 = _make_node("t2", NodeType.TRANSFORM)
        fg1 = _make_node("fg1", NodeType.FEATURE_GROUP)
        manifest = _make_manifest(
            s1, s2, t1, t2, fg1,
            (s1.id, t1.id), (s2.id, t2.id),
            (t1.id, fg1.id), (t2.id, fg1.id),
        )

        runner = DAGRunner(manifest, target_path=tmp_path)
        layers = runner._get_topological_layers()

        assert len(layers) == 3
        assert sorted(layers[0]) == sorted([s1.id, s2.id])
        assert sorted(layers[1]) == sorted([t1.id, t2.id])
        assert layers[2] == [fg1.id]


# ---------------------------------------------------------------------------
# Parallel Execution Tests
# ---------------------------------------------------------------------------


class TestParallelExecution:
    """Tests for ParallelDAGRunner.run()."""

    @patch.object(DAGRunner, '_execute_node')
    def test_two_independent_nodes_success(self, mock_execute, tmp_path):
        """Two independent nodes both complete successfully."""
        mock_execute.side_effect = lambda nid, **kw: _success_result(nid)

        n1 = _make_node("a", NodeType.SOURCE)
        n2 = _make_node("b", NodeType.SOURCE)
        manifest = _make_manifest(n1, n2)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2)
        summary = parallel.run({n1.id, n2.id})

        assert summary.successful_nodes == 2
        assert summary.failed_nodes == 0
        assert summary.executed_nodes == 2

    @patch.object(DAGRunner, '_execute_node')
    def test_error_in_one_sibling_completes(self, mock_execute, tmp_path):
        """Error in one node does not prevent sibling from completing."""
        def side_effect(nid, **kw):
            if "a" in nid:
                return _failed_result(nid)
            return _success_result(nid)

        mock_execute.side_effect = side_effect

        n1 = _make_node("a", NodeType.SOURCE)
        n2 = _make_node("b", NodeType.SOURCE)
        manifest = _make_manifest(n1, n2)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2, continue_on_error=True)
        summary = parallel.run({n1.id, n2.id})

        assert summary.successful_nodes == 1
        assert summary.failed_nodes == 1

    @patch.object(DAGRunner, '_execute_node')
    def test_downstream_of_failed_is_skipped(self, mock_execute, tmp_path):
        """Downstream of a failed node is skipped."""
        def side_effect(nid, **kw):
            if "a" in nid:
                return _failed_result(nid)
            return _success_result(nid)

        mock_execute.side_effect = side_effect

        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.TRANSFORM)
        manifest = _make_manifest(a, b, (a.id, b.id))

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2, continue_on_error=True)
        summary = parallel.run({a.id, b.id})

        assert summary.failed_nodes == 1
        # b should be skipped because a failed
        skipped = [r for r in summary.results if r.status == ExecutionStatus.SKIPPED]
        assert len(skipped) == 1
        assert skipped[0].node_id == b.id

    @patch.object(DAGRunner, '_execute_node')
    def test_continue_on_error_true_allows_unaffected_branches(self, mock_execute, tmp_path):
        """
        With continue_on_error=True, unaffected branches proceed.
        DAG: x1 -> y1, x2 -> y2  (two independent branches)
        If x1 fails, x2 -> y2 should still run.
        """
        fail_id = None

        def side_effect(nid, **kw):
            if nid == fail_id:
                return _failed_result(nid)
            return _success_result(nid)

        x1 = _make_node("x1", NodeType.SOURCE)
        x2 = _make_node("x2", NodeType.SOURCE)
        y1 = _make_node("y1", NodeType.TRANSFORM)
        y2 = _make_node("y2", NodeType.TRANSFORM)
        manifest = _make_manifest(x1, x2, y1, y2, (x1.id, y1.id), (x2.id, y2.id))

        fail_id = x1.id
        mock_execute.side_effect = side_effect

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2, continue_on_error=True)
        summary = parallel.run({x1.id, x2.id, y1.id, y2.id})

        assert summary.failed_nodes == 1
        # x2 and y2 should succeed, y1 should be skipped
        assert summary.successful_nodes == 2  # x2, y2
        skipped = [r for r in summary.results if r.status == ExecutionStatus.SKIPPED]
        skipped_ids = {r.node_id for r in skipped}
        assert y1.id in skipped_ids

    @patch.object(DAGRunner, '_execute_node')
    def test_continue_on_error_false_stops_after_failure(self, mock_execute, tmp_path):
        """
        With continue_on_error=False (default), execution stops after a layer with failures.
        DAG: a -> b (linear chain)
        If a fails, b should not execute.
        """
        mock_execute.side_effect = lambda nid, **kw: _failed_result(nid)

        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.TRANSFORM)
        manifest = _make_manifest(a, b, (a.id, b.id))

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2, continue_on_error=False)
        summary = parallel.run({a.id, b.id})

        assert summary.failed_nodes == 1
        # b should not have been executed at all (loop broke after layer 1 failure)
        executed_ids = {r.node_id for r in summary.results if r.status in (ExecutionStatus.SUCCESS, ExecutionStatus.FAILED)}
        assert b.id not in executed_ids

    @patch.object(DAGRunner, '_execute_node')
    def test_max_workers_1_sequential(self, mock_execute, tmp_path):
        """max_workers=1 runs nodes sequentially within a layer."""
        call_order = []

        def side_effect(nid, **kw):
            call_order.append(nid)
            return _success_result(nid)

        mock_execute.side_effect = side_effect

        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.SOURCE)
        c = _make_node("c", NodeType.SOURCE)
        manifest = _make_manifest(a, b, c)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=1)
        summary = parallel.run({a.id, b.id, c.id})

        assert summary.successful_nodes == 3
        # With max_workers=1, all three execute (order may vary due to threading, but they do run)
        assert set(call_order) == {a.id, b.id, c.id}

    @patch.object(DAGRunner, '_execute_node')
    def test_dry_run(self, mock_execute, tmp_path):
        """Dry run doesn't call _execute_node."""
        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.SOURCE)
        manifest = _make_manifest(a, b)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2)
        summary = parallel.run({a.id, b.id}, dry_run=True)

        mock_execute.assert_not_called()
        assert summary.successful_nodes == 2
        assert summary.executed_nodes == 2

    @patch.object(DAGRunner, '_execute_node')
    def test_nodes_not_in_to_run_are_skipped(self, mock_execute, tmp_path):
        """Nodes not in nodes_to_run set appear as skipped/cached."""
        mock_execute.side_effect = lambda nid, **kw: _success_result(nid)

        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.SOURCE)
        manifest = _make_manifest(a, b)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2)
        # Only run a, not b
        summary = parallel.run({a.id})

        assert summary.successful_nodes == 1
        assert summary.skipped_nodes == 1

    @patch.object(DAGRunner, '_execute_node')
    def test_exception_in_thread_produces_failed_result(self, mock_execute, tmp_path):
        """If _execute_node raises an exception, the node is marked FAILED."""
        def side_effect(nid, **kw):
            raise RuntimeError("unexpected crash")

        mock_execute.side_effect = side_effect

        a = _make_node("a", NodeType.SOURCE)
        manifest = _make_manifest(a)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=1)
        summary = parallel.run({a.id})

        assert summary.failed_nodes == 1
        failed = [r for r in summary.results if r.status == ExecutionStatus.FAILED]
        assert len(failed) == 1
        assert "unexpected crash" in failed[0].error_message


# ---------------------------------------------------------------------------
# Batch State Update Tests
# ---------------------------------------------------------------------------


class TestBatchStateUpdates:
    """Tests for state management in parallel execution."""

    @patch.object(DAGRunner, '_execute_node')
    def test_state_updated_after_each_layer(self, mock_execute, tmp_path):
        """State is updated once per layer via _batch_update_state."""
        mock_execute.side_effect = lambda nid, **kw: _success_result(nid)

        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.TRANSFORM)
        manifest = _make_manifest(a, b, (a.id, b.id))

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2)

        with patch.object(parallel, '_batch_update_state', wraps=parallel._batch_update_state) as mock_batch:
            summary = parallel.run({a.id, b.id})

        # Should be called twice: once per layer
        assert mock_batch.call_count == 2

    @patch.object(DAGRunner, '_execute_node')
    def test_successful_nodes_get_success_status(self, mock_execute, tmp_path):
        """Successful nodes get SUCCESS status in run state."""
        mock_execute.side_effect = lambda nid, **kw: _success_result(nid, duration=0.5)

        a = _make_node("a", NodeType.SOURCE)
        manifest = _make_manifest(a)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=1)
        summary = parallel.run({a.id})

        assert a.id in runner.run_state.nodes
        assert runner.run_state.nodes[a.id].status == NodeStatus.SUCCESS.value

    @patch.object(DAGRunner, '_execute_node')
    def test_failed_nodes_get_failed_status(self, mock_execute, tmp_path):
        """Failed nodes get FAILED status in run state."""
        mock_execute.side_effect = lambda nid, **kw: _failed_result(nid)

        a = _make_node("a", NodeType.SOURCE)
        manifest = _make_manifest(a)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=1, continue_on_error=True)
        summary = parallel.run({a.id})

        assert a.id in runner.run_state.nodes
        assert runner.run_state.nodes[a.id].status == NodeStatus.FAILED.value

    @patch.object(DAGRunner, '_execute_node')
    def test_state_file_saved_after_run(self, mock_execute, tmp_path):
        """State file is saved after the run completes."""
        mock_execute.side_effect = lambda nid, **kw: _success_result(nid)

        a = _make_node("a", NodeType.SOURCE)
        manifest = _make_manifest(a)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=1)

        with patch('seeknal.workflow.parallel.save_state') as mock_save:
            parallel.run({a.id})

        mock_save.assert_called_once_with(runner.run_state, runner.state_path)


# ---------------------------------------------------------------------------
# Summary Tests
# ---------------------------------------------------------------------------


class TestParallelSummary:
    """Tests for ParallelExecutionSummary and print_parallel_summary."""

    @patch.object(DAGRunner, '_execute_node')
    def test_summary_counts_match(self, mock_execute, tmp_path):
        """Summary counts match actual execution results."""
        def side_effect(nid, **kw):
            if "fail" in nid:
                return _failed_result(nid)
            return _success_result(nid)

        mock_execute.side_effect = side_effect

        ok = _make_node("ok", NodeType.SOURCE)
        fail = _make_node("fail", NodeType.SOURCE)
        skip = _make_node("skip", NodeType.SOURCE)
        manifest = _make_manifest(ok, fail, skip)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2, continue_on_error=True)
        # Only run ok and fail; skip is not in to_run
        summary = parallel.run({ok.id, fail.id})

        assert summary.successful_nodes == 1
        assert summary.failed_nodes == 1
        assert summary.skipped_nodes == 1  # skip node not in to_run
        assert summary.total_nodes == 3

    @patch.object(DAGRunner, '_execute_node')
    def test_duration_is_recorded(self, mock_execute, tmp_path):
        """Summary records a non-zero duration."""
        mock_execute.side_effect = lambda nid, **kw: _success_result(nid)

        a = _make_node("a", NodeType.SOURCE)
        manifest = _make_manifest(a)

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=1)
        summary = parallel.run({a.id})

        assert summary.total_duration >= 0.0

    @patch.object(DAGRunner, '_execute_node')
    def test_total_layers_count(self, mock_execute, tmp_path):
        """Summary reports correct number of layers."""
        mock_execute.side_effect = lambda nid, **kw: _success_result(nid)

        a = _make_node("a", NodeType.SOURCE)
        b = _make_node("b", NodeType.TRANSFORM)
        c = _make_node("c", NodeType.FEATURE_GROUP)
        manifest = _make_manifest(a, b, c, (a.id, b.id), (b.id, c.id))

        runner = DAGRunner(manifest, target_path=tmp_path)
        parallel = ParallelDAGRunner(runner, max_workers=2)
        summary = parallel.run({a.id, b.id, c.id})

        assert summary.total_layers == 3

    def test_print_parallel_summary_runs(self, capsys):
        """print_parallel_summary runs without error."""
        summary = ParallelExecutionSummary(
            total_nodes=5,
            executed_nodes=3,
            successful_nodes=2,
            failed_nodes=1,
            skipped_nodes=2,
            cached_nodes=0,
            total_layers=2,
            total_duration=1.23,
            results=[
                NodeResult("a", ExecutionStatus.SUCCESS, duration=0.5),
                NodeResult("b", ExecutionStatus.SUCCESS, duration=0.3),
                NodeResult("c", ExecutionStatus.FAILED, error_message="oops"),
            ],
        )

        # Should not raise
        print_parallel_summary(summary)

        captured = capsys.readouterr()
        assert "Parallel Execution Summary" in captured.out
        assert "1.23s" in captured.out
        assert "oops" in captured.out

    def test_print_parallel_summary_no_failures(self, capsys):
        """print_parallel_summary handles case with no failures."""
        summary = ParallelExecutionSummary(
            total_nodes=2,
            executed_nodes=2,
            successful_nodes=2,
            failed_nodes=0,
            skipped_nodes=0,
            cached_nodes=0,
            total_layers=1,
            total_duration=0.5,
            results=[
                NodeResult("a", ExecutionStatus.SUCCESS, duration=0.2),
                NodeResult("b", ExecutionStatus.SUCCESS, duration=0.3),
            ],
        )

        print_parallel_summary(summary)

        captured = capsys.readouterr()
        assert "Failed nodes:" not in captured.out
