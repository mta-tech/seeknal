"""
Tests for State Backend Protocol.

Tests the abstract base class and factory functionality.
Concrete backend implementations should have their own test files.
"""

import pytest
from unittest.mock import Mock, patch

from seeknal.state.backend import (
    StateBackend,
    StateBackendError,
    TransactionError,
    ConcurrencyError,
    TransactionState,
    StateBackendFactory,
    create_state_backend,
)
from seeknal.workflow.state import NodeState, NodeStatus, RunState


class MockStateBackend(StateBackend):
    """Mock implementation of StateBackend for testing."""

    def __init__(self):
        self.node_states: dict = {}
        self.run_states: dict = {}
        self.metadata: dict = {}
        self.locks: set = set()
        self._transaction_active = False
        self._transaction_ops = []

    def get_node_state(self, run_id: str, node_id: str) -> NodeState | None:
        key = f"{run_id}:{node_id}"
        return self.node_states.get(key)

    def set_node_state(
        self,
        run_id: str,
        node_id: str,
        state: NodeState,
        overwrite: bool = True
    ) -> None:
        key = f"{run_id}:{node_id}"
        if not overwrite and key in self.node_states:
            raise ConcurrencyError(f"Node state already exists: {key}")
        self.node_states[key] = state

    def get_run_state(self, run_id: str) -> RunState | None:
        return self.run_states.get(run_id)

    def set_run_state(
        self,
        run_id: str,
        state: RunState,
        overwrite: bool = True
    ) -> None:
        if not overwrite and run_id in self.run_states:
            raise ConcurrencyError(f"Run state already exists: {run_id}")
        self.run_states[run_id] = state

    def list_runs(self) -> list[str]:
        return list(self.run_states.keys())

    def delete_run(self, run_id: str) -> None:
        if run_id in self.run_states:
            del self.run_states[run_id]
        # Delete all node states for this run
        keys_to_delete = [k for k in self.node_states.keys() if k.startswith(f"{run_id}:")]
        for key in keys_to_delete:
            del self.node_states[key]

    def list_nodes(self, run_id: str) -> list[str]:
        prefix = f"{run_id}:"
        return [k.split(":")[1] for k in self.node_states.keys() if k.startswith(prefix)]

    def delete_node_state(self, run_id: str, node_id: str) -> None:
        key = f"{run_id}:{node_id}"
        if key in self.node_states:
            del self.node_states[key]

    def exists(self, run_id: str) -> bool:
        return run_id in self.run_states

    def node_exists(self, run_id: str, node_id: str) -> bool:
        key = f"{run_id}:{node_id}"
        return key in self.node_states

    def transaction(self):
        """Return a context manager for transactions."""
        @contextmanager
        def _transaction_context():
            tx_state = self.begin_transaction()
            try:
                yield tx_state
                if tx_state.is_active:
                    self.commit_transaction(tx_state)
            except Exception:
                if tx_state.is_active:
                    self.rollback_transaction(tx_state)
                raise

        return _transaction_context()

    def begin_transaction(self) -> TransactionState:
        self._transaction_active = True
        self._transaction_ops = []
        return TransactionState(
            operations=[],
            is_active=True,
            backend=self
        )

    def commit_transaction(self, tx_state: TransactionState) -> None:
        if not tx_state.is_active:
            raise TransactionError("Transaction is not active")
        self._transaction_active = False
        tx_state.is_active = False

    def rollback_transaction(self, tx_state: TransactionState) -> None:
        self._transaction_active = False
        self._transaction_ops = []
        tx_state.is_active = False

    def acquire_lock(
        self,
        run_id: str,
        node_id: str | None = None,
        timeout_ms: int = 5000
    ) -> bool:
        lock_key = f"{run_id}:{node_id or ''}"
        if lock_key in self.locks:
            return False
        self.locks.add(lock_key)
        return True

    def release_lock(
        self,
        run_id: str,
        node_id: str | None = None
    ) -> None:
        lock_key = f"{run_id}:{node_id or ''}"
        self.locks.discard(lock_key)

    def get_metadata(self, run_id: str) -> dict:
        return self.metadata.get(run_id, {})

    def set_metadata(self, run_id: str, metadata: dict) -> None:
        self.metadata[run_id] = metadata


from contextlib import contextmanager


class TestStateBackend:
    """Tests for StateBackend abstract base class."""

    def test_abstract_methods_defined(self):
        """Test that all abstract methods are defined."""
        abstract_methods = StateBackend.__abstractmethods__

        required_methods = {
            "get_node_state",
            "set_node_state",
            "get_run_state",
            "set_run_state",
            "list_runs",
            "delete_run",
            "list_nodes",
            "delete_node_state",
            "exists",
            "node_exists",
            "transaction",
            "begin_transaction",
            "commit_transaction",
            "rollback_transaction",
            "acquire_lock",
            "release_lock",
            "get_metadata",
            "set_metadata",
        }

        assert abstract_methods == required_methods


class TestMockStateBackend:
    """Tests for the mock backend implementation."""

    def test_set_and_get_node_state(self):
        """Test setting and getting node state."""
        backend = MockStateBackend()
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
        backend = MockStateBackend()
        state = backend.get_node_state("run1", "node1")
        assert state is None

    def test_set_node_state_no_overwrite(self):
        """Test that overwrite=False raises error for existing state."""
        backend = MockStateBackend()
        state1 = NodeState(hash="abc123", last_run="2024-01-01", status="success")
        state2 = NodeState(hash="def456", last_run="2024-01-02", status="success")

        backend.set_node_state("run1", "node1", state1)

        with pytest.raises(ConcurrencyError):
            backend.set_node_state("run1", "node1", state2, overwrite=False)

    def test_set_and_get_run_state(self):
        """Test setting and getting run state."""
        backend = MockStateBackend()
        state = RunState(
            run_id="run1",
            last_run="2024-01-01T00:00:00",
        )

        backend.set_run_state("run1", state)
        retrieved = backend.get_run_state("run1")

        assert retrieved is not None
        assert retrieved.run_id == "run1"

    def test_list_runs(self):
        """Test listing all runs."""
        backend = MockStateBackend()
        backend.set_run_state("run1", RunState(run_id="run1"))
        backend.set_run_state("run2", RunState(run_id="run2"))

        runs = backend.list_runs()
        assert set(runs) == {"run1", "run2"}

    def test_list_nodes(self):
        """Test listing nodes for a run."""
        backend = MockStateBackend()
        backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
        backend.set_node_state("run1", "node2", NodeState(hash="b", last_run="2024-01-01", status="pending"))

        nodes = backend.list_nodes("run1")
        assert set(nodes) == {"node1", "node2"}

    def test_delete_run(self):
        """Test deleting a run."""
        backend = MockStateBackend()
        backend.set_run_state("run1", RunState(run_id="run1"))
        backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

        backend.delete_run("run1")

        assert not backend.exists("run1")
        assert not backend.node_exists("run1", "node1")

    def test_delete_node_state(self):
        """Test deleting a node state."""
        backend = MockStateBackend()
        backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))

        backend.delete_node_state("run1", "node1")

        assert not backend.node_exists("run1", "node1")

    def test_exists(self):
        """Test checking if run exists."""
        backend = MockStateBackend()
        assert not backend.exists("run1")

        backend.set_run_state("run1", RunState(run_id="run1"))
        assert backend.exists("run1")

    def test_node_exists(self):
        """Test checking if node exists."""
        backend = MockStateBackend()
        assert not backend.node_exists("run1", "node1")

        backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
        assert backend.node_exists("run1", "node1")

    def test_acquire_and_release_lock(self):
        """Test lock acquisition and release."""
        backend = MockStateBackend()

        assert backend.acquire_lock("run1", "node1") is True
        assert backend.acquire_lock("run1", "node1") is False  # Already locked

        backend.release_lock("run1", "node1")
        assert backend.acquire_lock("run1", "node1") is True  # Now available

    def test_get_and_set_metadata(self):
        """Test getting and setting metadata."""
        backend = MockStateBackend()
        metadata = {"key1": "value1", "key2": "value2"}

        backend.set_metadata("run1", metadata)
        retrieved = backend.get_metadata("run1")

        assert retrieved == metadata

    def test_get_all_node_states(self):
        """Test getting all node states for a run."""
        backend = MockStateBackend()
        backend.set_node_state("run1", "node1", NodeState(hash="a", last_run="2024-01-01", status="pending"))
        backend.set_node_state("run1", "node2", NodeState(hash="b", last_run="2024-01-01", status="pending"))

        all_states = backend.get_all_node_states("run1")

        assert len(all_states) == 2
        assert "node1" in all_states
        assert "node2" in all_states

    def test_health_check(self):
        """Test backend health check."""
        backend = MockStateBackend()
        assert backend.health_check() is True

    def test_transaction_context_manager(self):
        """Test transaction context manager."""
        backend = MockStateBackend()

        with backend.transaction() as tx:
            assert tx.is_active is True

        # Transaction should be committed after context
        assert backend._transaction_active is False


class TestStateBackendFactory:
    """Tests for StateBackendFactory."""

    def test_register_backend(self):
        """Test registering a backend."""
        StateBackendFactory.register("mock", MockStateBackend)

        assert "mock" in StateBackendFactory.list_backends()

    def test_register_invalid_backend(self):
        """Test that registering non-StateBackend raises error."""
        class NotABackend:
            pass

        with pytest.raises(ValueError, match="must subclass StateBackend"):
            StateBackendFactory.register("invalid", NotABackend)

    def test_create_registered_backend(self):
        """Test creating a registered backend."""
        StateBackendFactory.register("mock", MockStateBackend)

        backend = StateBackendFactory.create("mock")
        assert isinstance(backend, MockStateBackend)

    def test_create_unknown_backend(self):
        """Test creating unknown backend raises error."""
        with pytest.raises(ValueError, match="Unknown backend type"):
            StateBackendFactory.create("unknown")

    def test_list_backends(self):
        """Test listing registered backends."""
        StateBackendFactory.register("mock", MockStateBackend)

        backends = StateBackendFactory.list_backends()
        assert "mock" in backends


class TestCreateStateBackend:
    """Tests for create_state_backend convenience function."""

    def test_create_backend(self):
        """Test creating a backend via convenience function."""
        StateBackendFactory.register("mock", MockStateBackend)

        backend = create_state_backend("mock")
        assert isinstance(backend, StateBackend)


class TestExceptions:
    """Tests for custom exceptions."""

    def test_state_backend_error(self):
        """Test StateBackendError can be raised and caught."""
        with pytest.raises(StateBackendError):
            raise StateBackendError("Test error")

    def test_transaction_error(self):
        """Test TransactionError can be raised and caught."""
        with pytest.raises(TransactionError):
            raise TransactionError("Transaction failed")

    def test_concurrency_error(self):
        """Test ConcurrencyError can be raised and caught."""
        with pytest.raises(ConcurrencyError):
            raise ConcurrencyError("Concurrent modification")
