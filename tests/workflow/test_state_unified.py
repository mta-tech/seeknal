"""Tests for unified state management system."""

import json
import pytest
from pathlib import Path
from datetime import datetime

from seeknal.workflow.state import (
    RunState,
    NodeState,
    NodeStatus,
    save_state,
    load_state,
    detect_changes,
    find_downstream_nodes,
    get_nodes_to_run,
    update_node_state,
    calculate_node_hash,
    StatePersistenceError,
)


class TestRunStateSerialization:
    """Test RunState to_dict/from_dict roundtrip."""

    def test_empty_state_roundtrip(self):
        state = RunState()
        data = state.to_dict()
        restored = RunState.from_dict(data)
        assert restored.schema_version == "2.0"
        assert restored.nodes == {}

    def test_state_with_nodes_roundtrip(self):
        state = RunState()
        state.nodes["source.users"] = NodeState(
            hash="abc123",
            last_run="2026-01-01T00:00:00",
            status="success",
            duration_ms=500,
            row_count=1000,
        )
        data = state.to_dict()
        restored = RunState.from_dict(data)
        assert "source.users" in restored.nodes
        node = restored.nodes["source.users"]
        assert node.hash == "abc123"
        assert node.status == "success"
        assert node.duration_ms == 500
        assert node.row_count == 1000

    def test_schema_version_is_2_0(self):
        state = RunState()
        data = state.to_dict()
        assert data["schema_version"] == "2.0"

    def test_json_roundtrip(self):
        state = RunState()
        state.nodes["t1"] = NodeState(
            hash="x", last_run="2026-01-01", status="success"
        )
        json_str = state.to_json()
        restored = RunState.from_json(json_str)
        assert restored.nodes["t1"].hash == "x"


class TestBackwardCompatibility:
    """Test migration from old state formats."""

    def test_old_runner_format_migration(self):
        """Old runner.py format: {node_id: {status, duration, row_count, last_run}}."""
        old_data = {
            "source.users": {
                "status": "success",
                "duration": 1.5,
                "row_count": 1000,
                "last_run": 1706000000.0,
            },
            "transform.clean": {
                "status": "success",
                "duration": 0.5,
                "row_count": 900,
                "last_run": 1706000001.0,
            },
        }
        state = RunState.from_dict(old_data)
        assert state.schema_version == "2.0"
        assert "source.users" in state.nodes
        assert state.nodes["source.users"].status == "success"
        assert state.nodes["source.users"].duration_ms == 1500

    def test_old_state_py_format_migration(self):
        """Old state.py format with version: "1.0.0"."""
        old_data = {
            "version": "1.0.0",
            "seeknal_version": "2.0.0",
            "last_run": "2026-01-01T00:00:00",
            "run_id": "20260101_000000",
            "config": {},
            "nodes": {
                "source.users": {
                    "hash": "abc",
                    "last_run": "2026-01-01",
                    "status": "success",
                    "duration_ms": 500,
                    "row_count": 1000,
                    "version": 1,
                    "metadata": {},
                }
            },
        }
        state = RunState.from_dict(old_data)
        assert state.schema_version == "2.0"
        assert state.nodes["source.users"].hash == "abc"


class TestStatePersistence:
    """Test save/load state with file I/O."""

    def test_save_and_load(self, tmp_path):
        state_path = tmp_path / "run_state.json"
        state = RunState()
        state.nodes["t1"] = NodeState(
            hash="abc", last_run="2026-01-01", status="success"
        )
        save_state(state, state_path)

        loaded = load_state(state_path)
        assert loaded is not None
        assert "t1" in loaded.nodes
        assert loaded.nodes["t1"].hash == "abc"

    def test_load_nonexistent_returns_none(self, tmp_path):
        state_path = tmp_path / "nonexistent.json"
        assert load_state(state_path) is None

    def test_atomic_write_creates_backup(self, tmp_path):
        state_path = tmp_path / "run_state.json"
        # First save
        state1 = RunState(run_id="run1")
        save_state(state1, state_path)
        # Second save creates backup
        state2 = RunState(run_id="run2")
        save_state(state2, state_path)
        backup_files = list(tmp_path.glob("run_state.json.bak.*"))
        assert len(backup_files) == 1

    def test_load_migrates_old_runner_state(self, tmp_path):
        """Test that load_state finds and migrates old runner.py state."""
        state_dir = tmp_path / "state"
        state_dir.mkdir()
        old_state = {"source.users": {"status": "success", "duration": 1.0, "row_count": 100, "last_run": 0}}
        with open(state_dir / "execution_state.json", "w") as f:
            json.dump(old_state, f)

        # Load from new path — should find old and migrate
        new_path = tmp_path / "run_state.json"
        loaded = load_state(new_path)
        assert loaded is not None
        assert "source.users" in loaded.nodes
        # Check that new file was created
        assert new_path.exists()

    def test_load_invalid_json_raises(self, tmp_path):
        state_path = tmp_path / "run_state.json"
        state_path.write_text("not valid json")
        with pytest.raises(StatePersistenceError):
            load_state(state_path)


class TestChangeDetection:
    """Test detect_changes and downstream propagation."""

    def test_first_run_all_new(self):
        hashes = {"t1": "abc", "t2": "def"}
        changed, new = detect_changes(hashes, None)
        assert changed == set()
        assert new == {"t1", "t2"}

    def test_detect_hash_change(self):
        stored = RunState()
        stored.nodes["t1"] = NodeState(hash="old", last_run="", status="success")
        changed, new = detect_changes({"t1": "new"}, stored)
        assert changed == {"t1"}
        assert new == set()

    def test_detect_new_node(self):
        stored = RunState()
        stored.nodes["t1"] = NodeState(hash="abc", last_run="", status="success")
        changed, new = detect_changes({"t1": "abc", "t2": "def"}, stored)
        assert changed == set()
        assert new == {"t2"}

    def test_no_changes(self):
        stored = RunState()
        stored.nodes["t1"] = NodeState(hash="abc", last_run="", status="success")
        changed, new = detect_changes({"t1": "abc"}, stored)
        assert changed == set()
        assert new == set()


class TestDownstreamPropagation:
    """Test find_downstream_nodes BFS."""

    def test_linear_chain(self):
        dag = {
            "s1": {"t1"},
            "t1": {"t2"},
            "t2": set(),
        }
        result = find_downstream_nodes(dag, {"s1"})
        assert result == {"s1", "t1", "t2"}

    def test_diamond_dag(self):
        dag = {
            "s1": {"t1", "t2"},
            "t1": {"t3"},
            "t2": {"t3"},
            "t3": set(),
        }
        result = find_downstream_nodes(dag, {"s1"})
        assert result == {"s1", "t1", "t2", "t3"}

    def test_no_downstream(self):
        dag = {"s1": set()}
        result = find_downstream_nodes(dag, {"s1"})
        assert result == {"s1"}


class TestGetNodesToRun:
    """Test combined change detection + downstream."""

    def test_first_run_returns_all(self):
        hashes = {"t1": "a", "t2": "b"}
        dag = {"t1": {"t2"}, "t2": set()}
        result = get_nodes_to_run(hashes, None, dag)
        assert result == {"t1", "t2"}

    def test_no_changes_returns_empty(self):
        stored = RunState()
        stored.nodes["t1"] = NodeState(hash="a", last_run="", status="success")
        stored.nodes["t2"] = NodeState(hash="b", last_run="", status="success")
        dag = {"t1": {"t2"}, "t2": set()}
        result = get_nodes_to_run({"t1": "a", "t2": "b"}, stored, dag)
        assert result == set()

    def test_single_change_propagates(self):
        stored = RunState()
        stored.nodes["t1"] = NodeState(hash="a", last_run="", status="success")
        stored.nodes["t2"] = NodeState(hash="b", last_run="", status="success")
        dag = {"t1": {"t2"}, "t2": set()}
        result = get_nodes_to_run({"t1": "changed", "t2": "b"}, stored, dag)
        assert result == {"t1", "t2"}


class TestUpdateNodeState:
    """Test update_node_state function."""

    def test_create_new_node(self):
        state = RunState()
        update_node_state(state, "t1", status="success", duration_ms=100, row_count=50)
        assert "t1" in state.nodes
        assert state.nodes["t1"].status == "success"
        assert state.nodes["t1"].duration_ms == 100

    def test_update_existing_node(self):
        state = RunState()
        state.nodes["t1"] = NodeState(hash="abc", last_run="", status="pending")
        update_node_state(state, "t1", status="success", duration_ms=200)
        assert state.nodes["t1"].status == "success"
        assert state.nodes["t1"].hash == "abc"  # hash preserved

    def test_update_hash_when_provided(self):
        state = RunState()
        state.nodes["t1"] = NodeState(hash="old", last_run="", status="pending")
        update_node_state(state, "t1", status="success", hash="new")
        assert state.nodes["t1"].hash == "new"


class TestCalculateNodeHash:
    """Test hash calculation for different node types."""

    def test_transform_hash(self):
        data = {"kind": "transform", "transform": "SELECT * FROM t1"}
        h = calculate_node_hash(data, Path("t.yml"))
        assert len(h) == 64

    def test_same_content_same_hash(self):
        data = {"kind": "transform", "transform": "SELECT * FROM t1"}
        h1 = calculate_node_hash(data, Path("t.yml"))
        h2 = calculate_node_hash(data, Path("t.yml"))
        assert h1 == h2

    def test_different_content_different_hash(self):
        d1 = {"kind": "transform", "transform": "SELECT * FROM t1"}
        d2 = {"kind": "transform", "transform": "SELECT * FROM t2"}
        h1 = calculate_node_hash(d1, Path("t.yml"))
        h2 = calculate_node_hash(d2, Path("t.yml"))
        assert h1 != h2

    def test_metadata_excluded(self):
        d1 = {"kind": "transform", "transform": "SELECT 1", "description": "A"}
        d2 = {"kind": "transform", "transform": "SELECT 1", "description": "B"}
        assert calculate_node_hash(d1, Path("t.yml")) == calculate_node_hash(d2, Path("t.yml"))

    def test_source_hash(self):
        data = {"kind": "source", "params": {"path": "data.csv"}}
        h = calculate_node_hash(data, Path("s.yml"))
        assert len(h) == 64
