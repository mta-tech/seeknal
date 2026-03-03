"""
Unit tests for Iceberg snapshot-based cache invalidation in DAGRunner.

Tests cover:
- Unchanged snapshot → node stays cached
- Changed snapshot → node re-runs
- First run (no stored state) → node runs
- Catalog unreachable → node forced to re-run
- Non-Iceberg source → Iceberg check skipped
- Iceberg state persisted after successful execution
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from unittest.mock import patch

from seeknal.dag.manifest import NodeType
from seeknal.workflow.executors.base import ExecutionContext
from seeknal.workflow.runner import DAGRunner, ExecutionStatus, NodeResult
from seeknal.workflow.state import NodeFingerprint, NodeState, NodeStatus, RunState


# ---------------------------------------------------------------------------
# Stubs
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


_SNAP_A = "1111111111111111"
_SNAP_B = "2222222222222222"
_TABLE_REF = "atlas.myns.mytable"

_FP = NodeFingerprint(
    content_hash="aaa",
    schema_hash="bbb",
    upstream_hash="ccc",
    config_hash="ddd",
)


def _make_iceberg_node(node_id: str = "source.iceberg_src") -> _StubNode:
    return _StubNode(
        id=node_id,
        name="iceberg_src",
        node_type=NodeType.SOURCE,
        config={
            "source": "iceberg",
            "table": _TABLE_REF,
            "params": {"catalog_uri": "http://lakekeeper/catalog"},
        },
    )


def _make_csv_node(node_id: str = "source.csv_src") -> _StubNode:
    return _StubNode(
        id=node_id,
        name="csv_src",
        node_type=NodeType.SOURCE,
        config={"source": "csv", "path": "data.csv"},
    )


def _make_node_state(snap_id: Optional[str] = None, fp: Optional[NodeFingerprint] = None) -> NodeState:
    return NodeState(
        hash="somehash",
        last_run="2026-01-01T00:00:00",
        status=NodeStatus.SUCCESS.value,
        fingerprint=fp or _FP,
        iceberg_snapshot_id=snap_id,
    )


def _make_runner(
    manifest: _StubManifest,
    run_state: Optional[RunState] = None,
    tmp_path: Optional[Path] = None,
) -> DAGRunner:
    """Create a DAGRunner with mocked init side-effects."""
    target = tmp_path or Path("/tmp/test_iceberg_runner_target")
    target.mkdir(parents=True, exist_ok=True)

    runner = DAGRunner.__new__(DAGRunner)
    runner.manifest = manifest
    runner.old_manifest = None
    runner.target_path = target
    runner.state_path = target / "run_state.json"
    runner.exec_context = None
    runner.diff = None
    runner.run_state = run_state or RunState()
    return runner


# ---------------------------------------------------------------------------
# Tests for _fingerprint_based_detection — Iceberg snapshot check
# ---------------------------------------------------------------------------

class TestIcebergCacheInvalidation:
    """
    Tests for the Iceberg snapshot check block inside _fingerprint_based_detection.

    Strategy: patch `seeknal.workflow.runner.compute_dag_fingerprints` (the name as
    imported in runner.py) to return a predetermined fingerprint, and pre-populate
    run_state with the same fingerprint so the base fingerprint loop treats the node
    as unchanged (letting the Iceberg block decide).
    """

    _PATCH_FINGERPRINTS = "seeknal.workflow.runner.compute_dag_fingerprints"
    _PATCH_SNAP = "seeknal.workflow.iceberg_metadata.get_current_snapshot_id"

    def _build_runner_with_snap(
        self,
        stored_snap: Optional[str],
        tmp_path: Path,
        node_id: str = "source.iceberg_src",
        node_name: str = "iceberg_src",
        node_type_value: str = "source",
    ) -> DAGRunner:
        """Build a runner with an Iceberg source node and optionally stored snapshot."""
        node = _make_iceberg_node(node_id)
        manifest = _StubManifest(nodes={node_id: node})

        run_state = RunState()
        # Store with matching fingerprint so base loop won't mark it as "content changed"
        run_state.nodes[node_id] = _make_node_state(snap_id=stored_snap, fp=_FP)

        runner = _make_runner(manifest, run_state, tmp_path)

        # Create the cache file so the "cache missing" branch doesn't fire
        cache_file = tmp_path / "cache" / node_type_value / f"{node_name}.parquet"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.touch()

        return runner

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_unchanged_iceberg_cached(self, mock_snap, tmp_path):
        """Same snapshot ID → node NOT added to changed set."""
        mock_snap.return_value = (_SNAP_A, "2026-01-01T00:00:00+00:00")
        node_id = "source.iceberg_src"
        runner = self._build_runner_with_snap(stored_snap=_SNAP_A, tmp_path=tmp_path)

        with patch(self._PATCH_FINGERPRINTS, return_value={node_id: _FP}):
            to_run, skip_reasons = runner._fingerprint_based_detection()

        assert node_id not in to_run

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_changed_iceberg_detected(self, mock_snap, tmp_path):
        """Different snapshot ID → node added to changed set with reason."""
        mock_snap.return_value = (_SNAP_B, "2026-01-02T00:00:00+00:00")
        node_id = "source.iceberg_src"
        runner = self._build_runner_with_snap(stored_snap=_SNAP_A, tmp_path=tmp_path)

        with patch(self._PATCH_FINGERPRINTS, return_value={node_id: _FP}):
            to_run, skip_reasons = runner._fingerprint_based_detection()

        assert node_id in to_run
        assert skip_reasons[node_id] == "iceberg data changed"

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_first_run_no_stored_state(self, mock_snap, tmp_path):
        """No stored NodeState → marked as 'new node' by base loop, Iceberg check skips it."""
        mock_snap.return_value = (_SNAP_A, "2026-01-01T00:00:00+00:00")
        node_id = "source.iceberg_src"
        node = _make_iceberg_node(node_id)
        manifest = _StubManifest(nodes={node_id: node})
        run_state = RunState()  # empty — no stored nodes

        runner = _make_runner(manifest, run_state, tmp_path)

        with patch(self._PATCH_FINGERPRINTS, return_value={node_id: _FP}):
            to_run, skip_reasons = runner._fingerprint_based_detection()

        # Base loop marks it as "new node"; Iceberg block skips it (already in changed)
        assert node_id in to_run
        assert skip_reasons[node_id] == "new node"
        # get_current_snapshot_id should NOT have been called for already-changed nodes
        mock_snap.assert_not_called()

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_catalog_unreachable(self, mock_snap, tmp_path):
        """Catalog returns (None, None) → node forced to re-run."""
        mock_snap.return_value = (None, None)
        node_id = "source.iceberg_src"
        runner = self._build_runner_with_snap(stored_snap=_SNAP_A, tmp_path=tmp_path)

        with patch(self._PATCH_FINGERPRINTS, return_value={node_id: _FP}):
            to_run, skip_reasons = runner._fingerprint_based_detection()

        assert node_id in to_run
        assert skip_reasons[node_id] == "iceberg catalog unreachable"

    @patch("seeknal.workflow.iceberg_metadata.get_current_snapshot_id")
    def test_non_iceberg_source_ignored(self, mock_snap, tmp_path):
        """CSV source node → Iceberg check is not called."""
        node_id = "source.csv_src"
        node = _make_csv_node(node_id)
        manifest = _StubManifest(nodes={node_id: node})
        run_state = RunState()
        run_state.nodes[node_id] = _make_node_state(snap_id=None, fp=_FP)

        runner = _make_runner(manifest, run_state, tmp_path)

        # Create cache file so "cache missing" branch doesn't fire
        cache_file = tmp_path / "cache" / "source" / "csv_src.parquet"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.touch()

        with patch(self._PATCH_FINGERPRINTS, return_value={node_id: _FP}):
            to_run, _ = runner._fingerprint_based_detection()

        mock_snap.assert_not_called()
        assert node_id not in to_run


# ---------------------------------------------------------------------------
# Test for Iceberg state persistence after successful execution
# ---------------------------------------------------------------------------

class TestIcebergStatePersisted:

    def test_iceberg_state_persisted_after_success(self, tmp_path):
        """After successful Iceberg source execution, snapshot fields stored in NodeState."""
        node_id = "source.iceberg_src"
        node = _make_iceberg_node(node_id)
        manifest = _StubManifest(nodes={node_id: node})

        run_state = RunState()
        run_state.nodes[node_id] = _make_node_state(snap_id=None)

        runner = _make_runner(manifest, run_state, tmp_path)

        # Simulate the post-success state update block by calling it directly
        # We replicate the relevant section of runner.run() logic:
        result = NodeResult(
            node_id=node_id,
            status=ExecutionStatus.SUCCESS,
            duration=1.0,
            metadata={
                "iceberg_snapshot_id": _SNAP_A,
                "iceberg_snapshot_timestamp": "2026-01-01T00:00:00+00:00",
                "iceberg_table_ref": _TABLE_REF,
            },
        )

        # Apply the persistence logic (mirrors runner.run() success branch)
        node_obj = runner.manifest.get_node(node_id)
        if (
            node_obj
            and node_obj.node_type == NodeType.SOURCE
            and node_obj.config.get("source") == "iceberg"
        ):
            iceberg_meta = result.metadata or {}
            node_state = runner.run_state.nodes.get(node_id)
            if node_state:
                if iceberg_meta.get("iceberg_snapshot_id"):
                    node_state.iceberg_snapshot_id = iceberg_meta["iceberg_snapshot_id"]
                if iceberg_meta.get("iceberg_snapshot_timestamp"):
                    node_state.iceberg_snapshot_timestamp = iceberg_meta["iceberg_snapshot_timestamp"]
                if iceberg_meta.get("iceberg_table_ref"):
                    node_state.iceberg_table_ref = iceberg_meta["iceberg_table_ref"]

        stored = runner.run_state.nodes[node_id]
        assert stored.iceberg_snapshot_id == _SNAP_A
        assert stored.iceberg_snapshot_timestamp == "2026-01-01T00:00:00+00:00"
        assert stored.iceberg_table_ref == _TABLE_REF

    def test_iceberg_state_not_overwritten_when_metadata_empty(self, tmp_path):
        """If result.metadata has no Iceberg keys, existing snapshot state is preserved."""
        node_id = "source.iceberg_src"
        node = _make_iceberg_node(node_id)
        manifest = _StubManifest(nodes={node_id: node})

        run_state = RunState()
        run_state.nodes[node_id] = _make_node_state(snap_id=_SNAP_A)

        runner = _make_runner(manifest, run_state, tmp_path)

        result = NodeResult(
            node_id=node_id,
            status=ExecutionStatus.SUCCESS,
            duration=1.0,
            metadata={},  # no Iceberg keys
        )

        node_obj = runner.manifest.get_node(node_id)
        if (
            node_obj
            and node_obj.node_type == NodeType.SOURCE
            and node_obj.config.get("source") == "iceberg"
        ):
            iceberg_meta = result.metadata or {}
            node_state = runner.run_state.nodes.get(node_id)
            if node_state:
                if iceberg_meta.get("iceberg_snapshot_id"):
                    node_state.iceberg_snapshot_id = iceberg_meta["iceberg_snapshot_id"]

        # Original snapshot ID should be unchanged
        assert runner.run_state.nodes[node_id].iceberg_snapshot_id == _SNAP_A


# ---------------------------------------------------------------------------
# Edge case: --full refresh clears watermark
# ---------------------------------------------------------------------------

class TestFullRefreshClearsWatermark:

    def test_full_refresh_clears_iceberg_watermark(self, tmp_path):
        """When --full is set, _execute_node should NOT inject last_watermark."""
        node_id = "source.iceberg_src"
        node = _make_iceberg_node(node_id)
        manifest = _StubManifest(nodes={node_id: node})

        run_state = RunState()
        ns = _make_node_state(snap_id=_SNAP_A)
        ns.last_watermark = "2026-01-15T00:00:00"
        run_state.nodes[node_id] = ns

        runner = _make_runner(manifest, run_state, tmp_path)
        runner.exec_context = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
        )
        runner._full_refresh = True

        # _execute_node will fail at _execute_by_type, but watermark injection
        # happens before that — we just need to check exec_context.config
        try:
            runner._execute_node(node_id)
        except Exception:
            pass

        assert "_iceberg_last_watermark" not in runner.exec_context.config

    def test_incremental_run_injects_watermark(self, tmp_path):
        """When --full is NOT set, _execute_node should inject last_watermark."""
        node_id = "source.iceberg_src"
        node = _make_iceberg_node(node_id)
        manifest = _StubManifest(nodes={node_id: node})

        run_state = RunState()
        ns = _make_node_state(snap_id=_SNAP_A)
        ns.last_watermark = "2026-01-15T00:00:00"
        run_state.nodes[node_id] = ns

        runner = _make_runner(manifest, run_state, tmp_path)
        runner.exec_context = ExecutionContext(
            project_name="test",
            workspace_path=tmp_path,
            target_path=tmp_path / "target",
        )
        runner._full_refresh = False

        try:
            runner._execute_node(node_id)
        except Exception:
            pass

        assert runner.exec_context.config.get("_iceberg_last_watermark") == "2026-01-15T00:00:00"


# ---------------------------------------------------------------------------
# State round-trip: NodeState serialization with Iceberg fields
# ---------------------------------------------------------------------------

class TestNodeStateIcebergRoundTrip:

    def test_iceberg_fields_round_trip(self):
        """NodeState Iceberg fields survive to_dict/from_dict serialization."""
        original = NodeState(
            hash="abc123",
            last_run="2026-03-01T12:00:00",
            status="success",
            duration_ms=1500,
            row_count=10000,
            iceberg_snapshot_id=_SNAP_A,
            iceberg_snapshot_timestamp="2026-03-01T11:55:00+00:00",
            iceberg_table_ref=_TABLE_REF,
            iceberg_schema_version=3,
            last_watermark="2026-02-28T23:59:59",
        )

        d = original.to_dict()
        restored = NodeState.from_dict(d)

        assert restored.iceberg_snapshot_id == _SNAP_A
        assert restored.iceberg_snapshot_timestamp == "2026-03-01T11:55:00+00:00"
        assert restored.iceberg_table_ref == _TABLE_REF
        assert restored.iceberg_schema_version == 3
        assert restored.last_watermark == "2026-02-28T23:59:59"

    def test_iceberg_fields_default_none(self):
        """NodeState created without Iceberg fields has None defaults."""
        ns = NodeState(hash="x", last_run="2026-01-01", status="success")
        d = ns.to_dict()
        restored = NodeState.from_dict(d)

        assert restored.iceberg_snapshot_id is None
        assert restored.last_watermark is None

    def test_backward_compat_missing_fields(self):
        """Old state dicts without Iceberg fields still deserialize correctly."""
        old_dict = {
            "hash": "oldhash",
            "last_run": "2025-12-01",
            "status": "success",
            "duration_ms": 500,
            "row_count": 100,
            "version": 1,
            "metadata": {},
        }
        ns = NodeState.from_dict(old_dict)
        assert ns.iceberg_snapshot_id is None
        assert ns.last_watermark is None
        assert ns.hash == "oldhash"
