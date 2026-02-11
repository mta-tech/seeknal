"""
Tests for DatabaseStateBackend implementation.

Tests the database-based state backend with SQLite persistence,
ACID transactions, and automatic schema creation.
"""

import json
import pytest
from pathlib import Path
from tempfile import TemporaryDirectory

from seeknal.state.database_backend import (
    DatabaseStateBackend,
    create_database_state_backend,
)
from seeknal.workflow.state import NodeState, RunState


class TestDatabaseStateBackend:
    """Tests for DatabaseStateBackend."""

    def test_init(self):
        """Test creating a database state backend."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            assert backend.db_path == db_path

            # Check database file exists
            assert db_path.exists()

    def test_set_and_get_node_state(self):
        """Test setting and getting node state."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
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
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            state = backend.get_node_state("run1", "node1")
            assert state is None

    def test_set_node_state_no_overwrite(self):
        """Test that overwrite=False raises error for existing state."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
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
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
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
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            state = backend.get_run_state("run1")
            assert state is None

    def test_list_runs(self):
        """Test listing all runs."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            backend.set_run_state("run1", RunState(run_id="run1"))
            backend.set_run_state("run2", RunState(run_id="run2"))

            runs = backend.list_runs()
            assert set(runs) == {"run1", "run2"}

    def test_list_runs_empty(self):
        """Test listing runs when none exist."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            runs = backend.list_runs()
            assert runs == []

    def test_list_nodes(self):
        """Test listing nodes for a run."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            backend.set_node_state("run1", "node2", NodeState(hash="b", last_run="2024-01-01", status="pending"))

            nodes = backend.list_nodes("run1")
            assert set(nodes) == {"node1", "node2"}

    def test_list_nodes_empty(self):
        """Test listing nodes when none exist."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            nodes = backend.list_nodes("run1")
            assert nodes == []

    def test_delete_run(self):
        """Test deleting a run."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            backend.set_run_state("run1", RunState(run_id="run1"))
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            assert backend.exists("run1") is True

            backend.delete_run("run1")

            assert backend.exists("run1") is False
            assert backend.node_exists("run1", "node1") is False

    def test_delete_node_state(self):
        """Test deleting a node state."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            assert backend.node_exists("run1", "node1") is True

            backend.delete_node_state("run1", "node1")

            assert backend.node_exists("run1", "node1") is False

    def test_exists(self):
        """Test checking if run exists."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            assert backend.exists("run1") is False

            backend.set_run_state("run1", RunState(run_id="run1"))
            assert backend.exists("run1") is True

    def test_node_exists(self):
        """Test checking if node exists."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            assert backend.node_exists("run1", "node1") is False

            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            assert backend.node_exists("run1", "node1") is True

    def test_acquire_and_release_lock(self):
        """Test lock acquisition and release."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)

            assert backend.acquire_lock("run1", "node1") is True
            assert backend.acquire_lock("run1", "node1") is False  # Already locked

            backend.release_lock("run1", "node1")
            assert backend.acquire_lock("run1", "node1") is True  # Now available

    def test_get_and_set_metadata(self):
        """Test getting and setting metadata."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            backend.set_run_state("run1", RunState(run_id="run1"))

            metadata = backend.get_metadata("run1")
            assert "run_id" in metadata
            assert metadata["run_id"] == "run1"

    def test_get_all_node_states(self):
        """Test getting all node states for a run."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            backend.set_node_state("run1", "node2", NodeState(hash="b", last_run="2024-01-01", status="pending"))

            all_states = backend.get_all_node_states("run1")

            assert len(all_states) == 2
            assert "node1" in all_states
            assert "node2" in all_states

    def test_health_check(self):
        """Test backend health check."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)
            assert backend.health_check() is True

    def test_transaction_context_manager(self):
        """Test transaction context manager."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)

            with backend.transaction() as tx:
                assert tx.is_active is True
                backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

            # After transaction, state should be persisted
            assert backend.node_exists("run1", "node1") is True

    def test_transaction_commit(self):
        """Test transaction commit."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)

            tx = backend.begin_transaction()
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            backend.commit_transaction(tx)

            assert backend.node_exists("run1", "node1") is True

    def test_transaction_rollback(self):
        """Test transaction rollback."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)

            tx = backend.begin_transaction()
            backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
            backend.rollback_transaction(tx)

            # After rollback, state should not be persisted
            assert backend.node_exists("run1", "node1") is False

    def test_transaction_rollback_on_error(self):
        """Test transaction rollback on error."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)

            try:
                with backend.transaction() as tx:
                    backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
                    raise ValueError("Test error")
            except ValueError:
                pass

            # After failed transaction, check state
            # With database backend, rollback should work
            assert backend.node_exists("run1", "node1") is False

    def test_run_state_with_nodes(self):
        """Test run state with multiple nodes."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)

            state = RunState(
                run_id="run1",
                last_run="2024-01-01T00:00:00",
            )
            state.nodes["node1"] = NodeState(hash="a", last_run="2024-01-01", status="pending")
            state.nodes["node2"] = NodeState(hash="b", last_run="2024-01-01", status="success")

            backend.set_run_state("run1", state)

            retrieved = backend.get_run_state("run1")
            assert retrieved is not None
            assert len(retrieved.nodes) == 2
            assert "node1" in retrieved.nodes
            assert "node2" in retrieved.nodes

    def test_node_state_with_all_fields(self):
        """Test node state with all optional fields."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)

            from seeknal.workflow.state import NodeFingerprint
            fingerprint = NodeFingerprint(
                content_hash="abc123",
                schema_hash="def456",
                upstream_hash="ghi789",
                config_hash="jkl012"
            )

            state = NodeState(
                hash="xyz789",
                last_run="2024-01-01T12:00:00",
                status="success",
                duration_ms=1500,
                row_count=1000,
                version=2,
                metadata={"error": "none", "warnings": []},
                fingerprint=fingerprint,
                iceberg_snapshot_id="snap123",
                iceberg_snapshot_timestamp="2024-01-01T12:00:00",
                iceberg_table_ref="db.table",
                iceberg_schema_version=1,
                completed_intervals=[("2024-01-01", "2024-01-02")],
                completed_partitions=["p1", "p2"],
                restatement_intervals=[("2024-01-03", "2024-01-04")],
            )

            backend.set_node_state("run1", "node1", state)
            retrieved = backend.get_node_state("run1", "node1")

            assert retrieved is not None
            assert retrieved.hash == "xyz789"
            assert retrieved.duration_ms == 1500
            assert retrieved.row_count == 1000
            assert retrieved.version == 2
            assert retrieved.metadata["error"] == "none"
            assert retrieved.fingerprint is not None
            assert retrieved.fingerprint.content_hash == "abc123"
            assert retrieved.iceberg_snapshot_id == "snap123"
            assert retrieved.iceberg_table_ref == "db.table"
            assert len(retrieved.completed_intervals) == 1
            assert len(retrieved.completed_partitions) == 2
            assert len(retrieved.restatement_intervals) == 1


class TestCreateDatabaseStateBackend:
    """Tests for create_database_state_backend convenience function."""

    def test_create_backend(self):
        """Test creating a database backend via convenience function."""
        with TemporaryDirectory() as tmpdir:
            backend = create_database_state_backend(Path(tmpdir) / "state.db")
            assert isinstance(backend, DatabaseStateBackend)


class TestStateBackendFactoryWithDatabaseBackend:
    """Tests for StateBackendFactory with DatabaseStateBackend."""

    def test_factory_creates_database_backend(self):
        """Test that factory can create database backend."""
        # Already registered in module
        from seeknal.state.backend import StateBackendFactory

        # Should be able to create via factory
        with TemporaryDirectory() as tmpdir:
            backend = StateBackendFactory.create("database", db_path=Path(tmpdir) / "state.db")
            assert isinstance(backend, DatabaseStateBackend)

    def test_factory_creates_sqlite_backend(self):
        """Test that 'sqlite' alias creates database backend."""
        from seeknal.state.backend import StateBackendFactory

        with TemporaryDirectory() as tmpdir:
            backend = StateBackendFactory.create("sqlite", db_path=Path(tmpdir) / "state.db")
            assert isinstance(backend, DatabaseStateBackend)

    def test_list_backends_includes_database(self):
        """Test that 'database' is in list of available backends."""
        from seeknal.state.backend import StateBackendFactory

        backends = StateBackendFactory.list_backends()
        assert "database" in backends
        assert "sqlite" in backends


class TestDatabasePersistence:
    """Tests for database persistence across backend instances."""

    def test_persistence_across_instances(self):
        """Test that state persists across backend instances."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"

            # Create first instance and write state
            backend1 = DatabaseStateBackend(db_path)
            state = NodeState(hash="abc123", last_run="2024-01-01", status="success")
            backend1.set_node_state("run1", "node1", state)
            # Close connection to ensure data is flushed
            backend1.close()

            # Create second instance and verify state
            backend2 = DatabaseStateBackend(db_path)
            retrieved = backend2.get_node_state("run1", "node1")

            assert retrieved is not None
            assert retrieved.hash == "abc123"

    def test_concurrent_write_protection(self):
        """Test that concurrent modifications are protected."""
        with TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "state.db"
            backend = DatabaseStateBackend(db_path)

            state1 = NodeState(hash="abc123", last_run="2024-01-01", status="success")
            state2 = NodeState(hash="def456", last_run="2024-01-02", status="success")

            backend.set_node_state("run1", "node1", state1)

            # Trying to set with overwrite=False should fail
            from seeknal.state.backend import ConcurrencyError
            with pytest.raises(ConcurrencyError):
                backend.set_node_state("run1", "node1", state2, overwrite=False)

            # But overwrite=True should work
            backend.set_node_state("run1", "node1", state2, overwrite=True)
            retrieved = backend.get_node_state("run1", "node1")
            assert retrieved.hash == "def456"
