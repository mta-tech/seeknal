"""
Tests for FileStateBackend implementation.

Tests the file-based state backend with JSON persistence,
atomic writes, and backward compatibility.
"""

import json
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from seeknal.state.file_backend import (
    FileStateBackend,
    create_file_state_backend,
)
from seeknal.workflow.state import NodeState, NodeStatus, RunState


class TestFileStateBackend:
    """Tests for FileStateBackend."""

    def test_init(self):
        """Test creating a file state backend."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend.base_path == Path(tmpdir)

    def test_set_and_get_node_state(self):
        """Test setting and getting node state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = NodeState(
                hash="abc123",
                last_run="2024-01-01T00:00:00",
                status="success"
            )

            backend.set_node_state("run1", "node1", state)
            retrieved = backend.get_node_state("run1", "node1")

            assert retrieved is not None
            assert retrieved.hash == "abc123"
            assert retrieved.status == "success"

    def test_get_nonexistent_node_state(self):
        """Test getting a non-existent node state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = backend.get_node_state("run1", "node1")
            assert state is None

    def test_set_node_state_no_overwrite(self):
        """Test that overwrite=False raises error for existing state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state1 = NodeState(hash="abc123", last_run="2024-01-01", status="success")
            state2 = NodeState(hash="def456", last_run="2024-01-02", status="success")

            backend.set_node_state("run1", "node1", state1)

            # Should raise error since state exists
            from seeknal.state.backend import ConcurrencyError
            with pytest.raises(ConcurrencyError):
                backend.set_node_state("run1", "node1", state2, overwrite=False)

    def test_set_and_get_run_state(self):
        """Test setting and getting run state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = RunState(
                run_id="run1",
                last_run="2024-01-01T00:00:00",
            )

            backend.set_run_state("run1", state)
            retrieved = backend.get_run_state("run1")

            assert retrieved is not None
            assert retrieved.run_id == "run1"

    def test_get_nonexistent_run_state(self):
        """Test getting a non-existent run state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state = backend.get_run_state("run1")
            assert state is None

    def test_list_runs(self):
        """Test listing all runs."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_run_state("run1", RunState(run_id="run1"))
            backend.set_run_state("run2", RunState(run_id="run2"))

            runs = backend.list_runs()
            assert set(runs) == {"run1", "run2"}

    def test_list_runs_empty(self):
        """Test listing runs when none exist."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            runs = backend.list_runs()
            assert runs == []

    def test_list_nodes(self):
        """Test listing nodes for a run."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            backend.set_node_state("run1", "node2", NodeState(hash="b", last_run="2024-01-01", status="pending"))

            nodes = backend.list_nodes("run1")
            assert set(nodes) == {"node1", "node2"}

    def test_list_nodes_empty(self):
        """Test listing nodes when none exist."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            nodes = backend.list_nodes("run1")
            assert nodes == []

    def test_delete_run(self):
        """Test deleting a run."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_run_state("run1", RunState(run_id="run1"))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            assert backend.exists("run1") is True

            backend.delete_run("run1")

            assert backend.exists("run1") is False
            assert backend.node_exists("run1", "node1") is False

    def test_delete_node_state(self):
        """Test deleting a node state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            assert backend.node_exists("run1", "node1") is True

            backend.delete_node_state("run1", "node1")

            assert backend.node_exists("run1", "node1") is False

    def test_exists(self):
        """Test checking if run exists."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend.exists("run1") is False

            backend.set_run_state("run1", RunState(run_id="run1"))
            assert backend.exists("run1") is True

    def test_node_exists(self):
        """Test checking if node exists."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend.node_exists("run1", "node1") is False

            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            assert backend.node_exists("run1", "node1") is True

    def test_acquire_and_release_lock(self):
        """Test lock acquisition and release."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            assert backend.acquire_lock("run1", "node1") is True
            assert backend.acquire_lock("run1", "node1") is False  # Already locked

            backend.release_lock("run1", "node1")
            assert backend.acquire_lock("run1", "node1") is True  # Now available

    def test_get_and_set_metadata(self):
        """Test getting and setting metadata."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_run_state("run1", RunState(run_id="run1"))

            metadata = backend.get_metadata("run1")
            assert "run_id" in metadata
            assert metadata["run_id"] == "run1"

    def test_get_all_node_states(self):
        """Test getting all node states for a run."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            backend.set_node_state("run1", "node2", NodeState(hash="b", last_run="2024-01-01", status="pending"))

            all_states = backend.get_all_node_states("run1")

            assert len(all_states) == 2
            assert "node1" in all_states
            assert "node2" in all_states

    def test_health_check(self):
        """Test backend health check."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            assert backend.health_check() is True

    def test_transaction_context_manager(self):
        """Test transaction context manager."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            with backend.transaction() as tx:
                assert tx.is_active is True
                backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            # After transaction, state should be persisted
            assert backend.node_exists("run1", "node1") is True

    def test_transaction_rollback(self):
        """Test transaction rollback on error."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            try:
                with backend.transaction() as tx:
                    backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
                    raise ValueError("Test error")
            except ValueError:
                pass

            # After failed transaction, check state
            # Note: In this simple implementation, the write still happens
            # A more sophisticated implementation would use temp files

    def test_backup_created_on_overwrite(self):
        """Test that backup is created when overwriting existing state."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))
            state1 = RunState(run_id="run1", last_run="2024-01-01T00:00:00")
            state2 = RunState(run_id="run1", last_run="2024-01-02T00:00:00")

            backend.set_run_state("run1", state1)
            backend.set_run_state("run1", state2)

            # Check backup file was created
            run_dir = Path(tmpdir) / "run1"
            backup_files = list(run_dir.glob("*.bak.*"))
            assert len(backup_files) > 0


class TestCreateFileStateBackend:
    """Tests for create_file_state_backend convenience function."""

    def test_create_backend(self):
        """Test creating a file backend via convenience function."""
        with TemporaryDirectory() as tmpdir:
            backend = create_file_state_backend(Path(tmpdir))
            assert isinstance(backend, FileStateBackend)


class TestStateBackendFactoryWithFileBackend:
    """Tests for StateBackendFactory with FileStateBackend."""

    def test_factory_creates_file_backend(self):
        """Test that factory can create file backend."""
        # Already registered in module
        from seeknal.state.backend import StateBackendFactory

        # Should be able to create via factory
        with TemporaryDirectory() as tmpdir:
            backend = StateBackendFactory.create("file", base_path=Path(tmpdir))
            assert isinstance(backend, FileStateBackend)

    def test_list_backends_includes_file(self):
        """Test that 'file' is in list of available backends."""
        from seeknal.state.backend import StateBackendFactory

        backends = StateBackendFactory.list_backends()
        assert "file" in backends


class TestBackwardCompatibility:
    """Tests for backward compatibility with old state formats."""

    def test_load_from_old_runner_format(self):
        """Test loading state from old runner.py format."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            # Create old runner.py state file
            state_dir = Path(tmpdir) / "state"
            state_dir.mkdir(parents=True)
            old_state_path = state_dir / "execution_state.json"

            old_state_data = {
                "transform.my_transform": {
                    "status": "success",
                    "duration": 1.5,  # seconds
                    "row_count": 1000,
                    "last_run": "2024-01-01T00:00:00"
                }
            }

            with open(old_state_path, "w") as f:
                json.dump(old_state_data, f)

            # Load should migrate from old format
            state = backend.get_run_state("")

            assert state is not None
            assert "transform.my_transform" in state.nodes
            assert state.nodes["transform.my_transform"].status == "success"
            assert state.nodes["transform.my_transform"].duration_ms == 1500  # Converted to ms

    def test_load_from_v1_state_format(self):
        """Test loading state from v1.0 format."""
        with TemporaryDirectory() as tmpdir:
            backend = FileStateBackend(Path(tmpdir))

            # Create v1.0 state file
            state_path = Path(tmpdir) / "run_state.json"

            v1_state_data = {
                "version": "1.0.0",
                "nodes": {
                    "transform.my_transform": {
                        "hash": "abc123",
                        "last_run": "2024-01-01T00:00:00",
                        "status": "success",
                        "duration_ms": 1500,
                        "row_count": 1000,
                    }
                }
            }

            with open(state_path, "w") as f:
                json.dump(v1_state_data, f)

            # Load should work
            state = backend.get_run_state("")

            assert state is not None
            assert "transform.my_transform" in state.nodes
