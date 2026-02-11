"""
State Backend Protocol for Seeknal workflow execution.

This module defines the StateBackend protocol/ABC for unified state persistence,
supporting both file-based and database-based backends with transaction support.

Key features:
- Abstract base class defining the state backend interface
- Transaction support for atomic multi-node updates
- Optimistic locking for concurrent access
- Pluggable backends (file, database, cloud storage)
"""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional, List

from ..workflow.state import NodeState, RunState

logger = logging.getLogger(__name__)


class StateBackendError(Exception):
    """Base exception for state backend errors."""
    pass


class TransactionError(StateBackendError):
    """Exception raised when a transaction fails."""
    pass


class ConcurrencyError(StateBackendError):
    """Exception raised when concurrent modification is detected."""
    pass


@dataclass
class TransactionState:
    """
    State tracking for an active transaction.

    Attributes:
        operations: List of operations performed in the transaction
        is_active: Whether the transaction is currently active
        backend: Reference to the backend that created the transaction
    """
    operations: List[Dict[str, Any]]
    is_active: bool
    backend: "StateBackend"


class StateBackend(ABC):
    """
    Abstract base class for state backends.

    Defines the interface for persisting and retrieving workflow state.
    Implementations can use file storage, databases, or cloud storage.

    All backends must support:
    - Reading/writing node and run state
    - Transaction support for atomic updates
    - Optimistic locking for concurrent access
    """

    @abstractmethod
    def get_node_state(self, run_id: str, node_id: str) -> Optional[NodeState]:
        """
        Get the state for a specific node in a run.

        Args:
            run_id: The run identifier
            node_id: The node identifier

        Returns:
            NodeState if found, None otherwise
        """
        pass

    @abstractmethod
    def set_node_state(
        self,
        run_id: str,
        node_id: str,
        state: NodeState,
        overwrite: bool = True
    ) -> None:
        """
        Set the state for a specific node in a run.

        Args:
            run_id: The run identifier
            node_id: The node identifier
            state: The node state to set
            overwrite: If False, raise error if state already exists

        Raises:
            ConcurrencyError: If state exists and overwrite=False
        """
        pass

    @abstractmethod
    def get_run_state(self, run_id: str) -> Optional[RunState]:
        """
        Get the complete state for a run.

        Args:
            run_id: The run identifier

        Returns:
            RunState if found, None otherwise
        """
        pass

    @abstractmethod
    def set_run_state(
        self,
        run_id: str,
        state: RunState,
        overwrite: bool = True
    ) -> None:
        """
        Set the complete state for a run.

        Args:
            run_id: The run identifier
            state: The run state to set
            overwrite: If False, raise error if state already exists

        Raises:
            ConcurrencyError: If state exists and overwrite=False
        """
        pass

    @abstractmethod
    def list_runs(self) -> List[str]:
        """
        List all available run IDs.

        Returns:
            List of run identifiers
        """
        pass

    @abstractmethod
    def delete_run(self, run_id: str) -> None:
        """
        Delete all state for a run.

        Args:
            run_id: The run identifier to delete
        """
        pass

    @abstractmethod
    def list_nodes(self, run_id: str) -> List[str]:
        """
        List all node IDs for a run.

        Args:
            run_id: The run identifier

        Returns:
            List of node identifiers
        """
        pass

    @abstractmethod
    def delete_node_state(self, run_id: str, node_id: str) -> None:
        """
        Delete state for a specific node.

        Args:
            run_id: The run identifier
            node_id: The node identifier
        """
        pass

    @abstractmethod
    def exists(self, run_id: str) -> bool:
        """
        Check if a run state exists.

        Args:
            run_id: The run identifier

        Returns:
            True if run state exists, False otherwise
        """
        pass

    @abstractmethod
    def node_exists(self, run_id: str, node_id: str) -> bool:
        """
        Check if a node state exists.

        Args:
            run_id: The run identifier
            node_id: The node identifier

        Returns:
            True if node state exists, False otherwise
        """
        pass

    @abstractmethod
    @contextmanager
    def transaction(self) -> Iterator[TransactionState]:
        """
        Context manager for transactional updates.

        Within a transaction, multiple state updates can be performed
        atomically. If an exception occurs, all changes are rolled back.

        Yields:
            TransactionState for tracking operations

        Raises:
            TransactionError: If the transaction fails

        Example:
            >>> with backend.transaction() as tx:
            ...     backend.set_node_state("run1", "node1", state1)
            ...     backend.set_node_state("run1", "node2", state2)
            ...     # Both updates succeed or both fail
        """
        pass

    @abstractmethod
    def begin_transaction(self) -> TransactionState:
        """
        Begin a new transaction.

        Returns:
            TransactionState for the new transaction
        """
        pass

    @abstractmethod
    def commit_transaction(self, tx_state: TransactionState) -> None:
        """
        Commit a transaction.

        Args:
            tx_state: The transaction state to commit

        Raises:
            TransactionError: If commit fails
        """
        pass

    @abstractmethod
    def rollback_transaction(self, tx_state: TransactionState) -> None:
        """
        Rollback a transaction.

        Args:
            tx_state: The transaction state to rollback
        """
        pass

    @abstractmethod
    def acquire_lock(
        self,
        run_id: str,
        node_id: Optional[str] = None,
        timeout_ms: int = 5000
    ) -> bool:
        """
        Acquire a lock for optimistic concurrency control.

        Args:
            run_id: The run identifier
            node_id: Optional node identifier for node-level locks
            timeout_ms: Timeout in milliseconds

        Returns:
            True if lock acquired, False if timeout
        """
        pass

    @abstractmethod
    def release_lock(
        self,
        run_id: str,
        node_id: Optional[str] = None
    ) -> None:
        """
        Release a previously acquired lock.

        Args:
            run_id: The run identifier
            node_id: Optional node identifier for node-level locks
        """
        pass

    @abstractmethod
    def get_metadata(self, run_id: str) -> Dict[str, Any]:
        """
        Get metadata for a run.

        Args:
            run_id: The run identifier

        Returns:
            Dictionary of metadata
        """
        pass

    @abstractmethod
    def set_metadata(
        self,
        run_id: str,
        metadata: Dict[str, Any]
    ) -> None:
        """
        Set metadata for a run.

        Args:
            run_id: The run identifier
            metadata: Metadata dictionary to set
        """
        pass

    def get_all_node_states(self, run_id: str) -> Dict[str, NodeState]:
        """
        Get all node states for a run.

        Default implementation iterates through list_nodes().

        Args:
            run_id: The run identifier

        Returns:
            Dict mapping node_id to NodeState
        """
        node_ids = self.list_nodes(run_id)
        states = {}
        for node_id in node_ids:
            state = self.get_node_state(run_id, node_id)
            if state:
                states[node_id] = state
        return states

    def health_check(self) -> bool:
        """
        Check if the backend is healthy and accessible.

        Returns:
            True if backend is healthy, False otherwise
        """
        try:
            self.list_runs()
            return True
        except Exception as e:
            logger.warning(f"Backend health check failed: {e}")
            return False


class StateBackendFactory:
    """
    Factory for creating state backend instances.

    Supports creating backends from configuration strings or
    direct instantiation.
    """

    _backends: Dict[str, type] = {}

    @classmethod
    def register(cls, name: str, backend_class: type) -> None:
        """
        Register a backend implementation.

        Args:
            name: Name to register the backend under
            backend_class: Backend class (must subclass StateBackend)
        """
        if not issubclass(backend_class, StateBackend):
            raise ValueError(f"{backend_class} must subclass StateBackend")
        cls._backends[name] = backend_class

    @classmethod
    def create(cls, backend_type: str, **kwargs) -> StateBackend:
        """
        Create a backend instance.

        Args:
            backend_type: Type of backend to create (e.g., "file", "database")
            **kwargs: Arguments to pass to backend constructor

        Returns:
            StateBackend instance

        Raises:
            ValueError: If backend_type is not registered
        """
        if backend_type not in cls._backends:
            available = ", ".join(cls._backends.keys())
            raise ValueError(
                f"Unknown backend type: {backend_type}. "
                f"Available: {available}"
            )

        backend_class = cls._backends[backend_type]
        return backend_class(**kwargs)

    @classmethod
    def list_backends(cls) -> List[str]:
        """List all registered backend types."""
        return list(cls._backends.keys())


def create_state_backend(
    backend_type: str,
    **kwargs
) -> StateBackend:
    """
    Convenience function to create a state backend.

    Args:
        backend_type: Type of backend to create
        **kwargs: Arguments to pass to backend constructor

    Returns:
        StateBackend instance

    Examples:
        >>> backend = create_state_backend("file", base_path="/tmp/state")
        >>> backend = create_state_backend("database", connection_string="...")
    """
    return StateBackendFactory.create(backend_type, **kwargs)
