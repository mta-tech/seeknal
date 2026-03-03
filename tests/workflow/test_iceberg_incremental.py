"""
Integration tests for Iceberg incremental runs.

These tests verify the full feature end-to-end, covering scenarios
that span runner detection → executor → state persistence.

Unit-level tests live in:
- test_iceberg_metadata.py (snapshot query utility)
- test_iceberg_runner.py (cache invalidation + edge cases)
- test_iceberg_source_executor.py (partition pruning + SQL)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from unittest.mock import patch

from seeknal.dag.manifest import NodeType
from seeknal.workflow.runner import DAGRunner
from seeknal.workflow.state import NodeFingerprint, NodeState, NodeStatus, RunState


# ---------------------------------------------------------------------------
# Stubs (same pattern as test_iceberg_runner.py)
# ---------------------------------------------------------------------------

@dataclass
class _Edge:
    from_node: str
    to_node: str


@dataclass
class _StubNode:
    id: str
    name: str
    node_type: NodeType
    config: Dict[str, Any] = field(default_factory=dict)
    file_path: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    columns: Dict[str, str] = field(default_factory=dict)


@dataclass
class _StubManifest:
    nodes: Dict[str, _StubNode] = field(default_factory=dict)
    edges: List[_Edge] = field(default_factory=list)

    def get_node(self, node_id: str) -> Optional[_StubNode]:
        return self.nodes.get(node_id)

    def get_downstream_nodes(self, node_id: str) -> Set[str]:
        return {e.to_node for e in self.edges if e.from_node == node_id}

    def get_upstream_nodes(self, node_id: str) -> Set[str]:
        return {e.from_node for e in self.edges if e.to_node == node_id}

    def detect_cycles(self):
        return False, []


_FP = NodeFingerprint(content_hash="aaa", schema_hash="bbb", upstream_hash="ccc", config_hash="ddd")
_SNAP_A = "1111111111111111"
_SNAP_B = "2222222222222222"


def _make_runner(manifest: _StubManifest, run_state: RunState, tmp_path: Path) -> DAGRunner:
    runner = DAGRunner.__new__(DAGRunner)
    runner.manifest = manifest
    runner.old_manifest = None
    runner.target_path = tmp_path
    runner.state_path = tmp_path / "run_state.json"
    runner.exec_context = None
    runner.diff = None
    runner.run_state = run_state
    return runner


def _make_cache(tmp_path: Path, node_type: str, node_name: str) -> None:
    cache_file = tmp_path / "cache" / node_type / f"{node_name}.parquet"
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    cache_file.touch()


# ---------------------------------------------------------------------------
# Test 1: Downstream invalidation via DAG edges
# ---------------------------------------------------------------------------

class TestDownstreamInvalidation:
    """When an Iceberg source's snapshot changes, downstream nodes must also re-run."""

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_iceberg_change_cascades_to_downstream(self, mock_snap, tmp_path):
        """
        DAG: source.orders → transform.cleaned → transform.enriched
        Change snapshot on source.orders → all three nodes should be in to_run.
        """
        mock_snap.return_value = (_SNAP_B, "2026-02-01T00:00:00+00:00")

        src = _StubNode(
            id="source.orders", name="orders", node_type=NodeType.SOURCE,
            config={"source": "iceberg", "table": "atlas.ns.orders",
                    "params": {"catalog_uri": "http://lake:8181"}},
        )
        t1 = _StubNode(
            id="transform.cleaned", name="cleaned", node_type=NodeType.TRANSFORM,
            config={"engine": "duckdb"},
        )
        t2 = _StubNode(
            id="transform.enriched", name="enriched", node_type=NodeType.TRANSFORM,
            config={"engine": "duckdb"},
        )

        manifest = _StubManifest(
            nodes={
                "source.orders": src,
                "transform.cleaned": t1,
                "transform.enriched": t2,
            },
            edges=[
                _Edge(from_node="source.orders", to_node="transform.cleaned"),
                _Edge(from_node="transform.cleaned", to_node="transform.enriched"),
            ],
        )

        run_state = RunState()
        # All nodes were previously successful with matching fingerprints
        for nid in manifest.nodes:
            run_state.nodes[nid] = NodeState(
                hash="h", last_run="2026-01-01", status=NodeStatus.SUCCESS.value,
                fingerprint=_FP,
                iceberg_snapshot_id=_SNAP_A if nid == "source.orders" else None,
            )
            _make_cache(tmp_path, manifest.nodes[nid].node_type.value, manifest.nodes[nid].name)

        runner = _make_runner(manifest, run_state, tmp_path)

        with patch("seeknal.workflow.runner.compute_dag_fingerprints",
                    return_value={nid: _FP for nid in manifest.nodes}):
            to_run, skip_reasons = runner._get_nodes_to_run()

        # Source detected as changed via Iceberg snapshot
        assert "source.orders" in to_run
        assert skip_reasons["source.orders"] == "iceberg data changed"

        # Downstream nodes must also be in to_run (cascade)
        assert "transform.cleaned" in to_run
        assert "transform.enriched" in to_run

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_unchanged_snapshot_no_cascade(self, mock_snap, tmp_path):
        """Same snapshot → source cached, downstream also cached."""
        mock_snap.return_value = (_SNAP_A, "2026-01-01T00:00:00+00:00")

        src = _StubNode(
            id="source.orders", name="orders", node_type=NodeType.SOURCE,
            config={"source": "iceberg", "table": "atlas.ns.orders",
                    "params": {"catalog_uri": "http://lake:8181"}},
        )
        t1 = _StubNode(
            id="transform.cleaned", name="cleaned", node_type=NodeType.TRANSFORM,
            config={"engine": "duckdb"},
        )

        manifest = _StubManifest(
            nodes={"source.orders": src, "transform.cleaned": t1},
            edges=[_Edge(from_node="source.orders", to_node="transform.cleaned")],
        )

        run_state = RunState()
        for nid in manifest.nodes:
            run_state.nodes[nid] = NodeState(
                hash="h", last_run="2026-01-01", status=NodeStatus.SUCCESS.value,
                fingerprint=_FP,
                iceberg_snapshot_id=_SNAP_A if nid == "source.orders" else None,
            )
            _make_cache(tmp_path, manifest.nodes[nid].node_type.value, manifest.nodes[nid].name)

        runner = _make_runner(manifest, run_state, tmp_path)

        with patch("seeknal.workflow.runner.compute_dag_fingerprints",
                    return_value={nid: _FP for nid in manifest.nodes}):
            to_run, _ = runner._get_nodes_to_run()

        assert "source.orders" not in to_run
        assert "transform.cleaned" not in to_run


# ---------------------------------------------------------------------------
# Test 2: Multi-run lifecycle (first → incremental → full)
# ---------------------------------------------------------------------------

class TestMultiRunLifecycle:
    """Simulates the Iceberg source through multiple pipeline runs."""

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_first_run_then_cached(self, mock_snap, tmp_path):
        """
        Run 1: No stored state → node runs, snapshot stored.
        Run 2: Same snapshot → node is NOT in to_run (cached).
        """
        node_id = "source.ice"
        node = _StubNode(
            id=node_id, name="ice", node_type=NodeType.SOURCE,
            config={"source": "iceberg", "table": "atlas.ns.tbl",
                    "params": {"catalog_uri": "http://lake:8181"}},
        )
        manifest = _StubManifest(nodes={node_id: node})

        # --- Run 1: empty state ---
        mock_snap.return_value = (_SNAP_A, "2026-01-01T00:00:00+00:00")
        run_state = RunState()
        runner = _make_runner(manifest, run_state, tmp_path)

        with patch("seeknal.workflow.runner.compute_dag_fingerprints",
                    return_value={node_id: _FP}):
            to_run_1, reasons_1 = runner._fingerprint_based_detection()

        # First run: node should execute (new node)
        assert node_id in to_run_1
        assert reasons_1[node_id] == "new node"

        # Simulate post-execution: store snapshot + fingerprint
        run_state.nodes[node_id] = NodeState(
            hash="h", last_run="2026-01-01", status=NodeStatus.SUCCESS.value,
            fingerprint=_FP, iceberg_snapshot_id=_SNAP_A,
            last_watermark="2026-01-01T23:59:59",
        )
        _make_cache(tmp_path, "source", "ice")

        # --- Run 2: same snapshot ---
        runner2 = _make_runner(manifest, run_state, tmp_path)
        with patch("seeknal.workflow.runner.compute_dag_fingerprints",
                    return_value={node_id: _FP}):
            to_run_2, _ = runner2._fingerprint_based_detection()

        assert node_id not in to_run_2

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_snapshot_change_triggers_rerun(self, mock_snap, tmp_path):
        """
        Stored snapshot_A, current snapshot_B → node must re-run.
        """
        node_id = "source.ice"
        node = _StubNode(
            id=node_id, name="ice", node_type=NodeType.SOURCE,
            config={"source": "iceberg", "table": "atlas.ns.tbl",
                    "params": {"catalog_uri": "http://lake:8181"}},
        )
        manifest = _StubManifest(nodes={node_id: node})

        run_state = RunState()
        run_state.nodes[node_id] = NodeState(
            hash="h", last_run="2026-01-01", status=NodeStatus.SUCCESS.value,
            fingerprint=_FP, iceberg_snapshot_id=_SNAP_A,
        )
        _make_cache(tmp_path, "source", "ice")

        mock_snap.return_value = (_SNAP_B, "2026-02-01T00:00:00+00:00")
        runner = _make_runner(manifest, run_state, tmp_path)

        with patch("seeknal.workflow.runner.compute_dag_fingerprints",
                    return_value={node_id: _FP}):
            to_run, reasons = runner._fingerprint_based_detection()

        assert node_id in to_run
        assert reasons[node_id] == "iceberg data changed"
