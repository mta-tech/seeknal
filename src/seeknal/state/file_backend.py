"""
File-based state backend implementation.

This module provides a file-based implementation of the StateBackend protocol,
using JSON files for persistence with atomic writes and backup support.

Key features:
- Atomic writes (temp file + rename)
- Automatic backup before overwrites
- Migration from old state formats
- Thread-safe operations via file locking
"""

import json
import logging
import re
import shutil
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set

from .backend import (
    StateBackend,
    StateBackendError,
    TransactionError,
    ConcurrencyError,
    TransactionState,
    StateBackendFactory,
)
from ..workflow.state import NodeState, RunState
from ..utils.path_security import is_insecure_path

logger = logging.getLogger(__name__)


class FileStateBackend(StateBackend):
    """
    File-based implementation of StateBackend.

    Stores state as JSON files with atomic writes and automatic backups.
    Supports migration from old state file formats.

    Attributes:
        base_path: Base directory for state files
        lock: Thread lock for concurrent access control
    """

    def __init__(self, base_path: Path):
        """
        Initialize the file state backend.

        Args:
            base_path: Base directory for storing state files

        Raises:
            ValueError: If the base_path is in an insecure location.
        """
        # Validate base_path is not in an insecure location
        base_path_str = str(base_path)
        if is_insecure_path(base_path_str):
            raise ValueError(
                f"Insecure base path detected: '{base_path_str}'. "
                "State files contain sensitive workflow data and should not be "
                "stored in world-writable directories like /tmp. "
                "Use a secure location such as ~/.seeknal/state or set "
                "SEEKNAL_BASE_CONFIG_PATH environment variable."
            )

        self.base_path = Path(base_path)
        self.lock = threading.Lock()
        self._locks: Dict[str, threading.Lock] = {}
        self._transaction_state: Optional[TransactionState] = None

    def _sanitize_run_id(self, run_id: str) -> str:
        """
        Sanitize a run_id to prevent path traversal attacks.

        Removes dangerous path traversal sequences (.., /, \) from the input
        to prevent escaping the base directory.

        Args:
            run_id: The raw run identifier from user input.

        Returns:
            A sanitized run_id safe for use in file paths.

        Examples:
            >>> backend._sanitize_run_id("normal_run")
            'normal_run'
            >>> backend._sanitize_run_id("../escape")
            'escape'
            >>> backend._sanitize_run_id("run/with/slashes")
            'runwithslashes'
        """
        if not run_id:
            return run_id

        # Remove path traversal sequences and directory separators
        # This handles: "../", "..\\", "/", "\\", and standalone ".."
        sanitized = re.sub(r'\.\.[/\\]', '', run_id)  # Remove ../ or ..\
        sanitized = re.sub(r'[/\\]', '', sanitized)   # Remove remaining slashes
        sanitized = re.sub(r'\.\.', '', sanitized)     # Remove any remaining ..

        return sanitized

    def _sanitize_node_id(self, node_id: str) -> str:
        """
        Sanitize a node_id to prevent path traversal attacks.

        Removes dangerous path traversal sequences (.., /, \) from the input
        to prevent escaping the base directory.

        Args:
            node_id: The raw node identifier from user input.

        Returns:
            A sanitized node_id safe for use in file paths.

        Examples:
            >>> backend._sanitize_node_id("normal_node")
            'normal_node'
            >>> backend._sanitize_node_id("../escape")
            'escape'
            >>> backend._sanitize_node_id("node/with/slashes")
            'nodewithslashes'
        """
        if not node_id:
            return node_id

        # Remove path traversal sequences and directory separators
        # This handles: "../", "..\\", "/", "\\", and standalone ".."
        sanitized = re.sub(r'\.\.[/\\]', '', node_id)  # Remove ../ or ..\
        sanitized = re.sub(r'[/\\]', '', sanitized)    # Remove remaining slashes
        sanitized = re.sub(r'\.\.', '', sanitized)      # Remove any remaining ..

        return sanitized

    def _get_run_state_path(self, run_id: str) -> Path:
        """
        Get the file path for a run's state.

        Sanitizes the run_id to prevent path traversal attacks.

        Args:
            run_id: The run identifier (will be sanitized).

        Returns:
            Path to the run's state file.
        """
        # Sanitize run_id to prevent path traversal
        safe_run_id = self._sanitize_run_id(run_id)
        # Use sanitized run_id as the directory name, with run_state.json inside
        run_dir = self.base_path / safe_run_id
        return run_dir / "run_state.json"

    def _get_node_state_path(self, run_id: str, node_id: str) -> Path:
        """
        Get the file path for a node's state.

        Sanitizes both run_id and node_id to prevent path traversal attacks.

        Args:
            run_id: The run identifier (will be sanitized).
            node_id: The node identifier (will be sanitized).

        Returns:
            Path to the node's state file.
        """
        # Sanitize inputs to prevent path traversal
        safe_run_id = self._sanitize_run_id(run_id)
        safe_node_id = self._sanitize_node_id(node_id)
        # Store node states in run-specific subdirectory
        run_dir = self.base_path / safe_run_id
        nodes_dir = run_dir / "nodes"
        return nodes_dir / f"{safe_node_id}.json"

    def get_node_state(self, run_id: str, node_id: str) -> Optional[NodeState]:
        """Get the state for a specific node in a run."""
        node_path = self._get_node_state_path(run_id, node_id)

        if not node_path.exists():
            return None

        try:
            with open(node_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return NodeState.from_dict(data)
        except Exception as e:
            logger.warning(f"Failed to load node state from {node_path}: {e}")
            return None

    def set_node_state(
        self,
        run_id: str,
        node_id: str,
        state: NodeState,
        overwrite: bool = True
    ) -> None:
        """Set the state for a specific node in a run."""
        node_path = self._get_node_state_path(run_id, node_id)

        # Check for concurrent modification
        if not overwrite and node_path.exists():
            raise ConcurrencyError(f"Node state already exists: {run_id}:{node_id}")

        # Ensure parent directory exists
        node_path.parent.mkdir(parents=True, exist_ok=True)

        # Atomic write
        temp_path = node_path.with_suffix(".tmp")
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, indent=2)
            temp_path.replace(node_path)
        except Exception as e:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            raise StateBackendError(f"Failed to save node state to {node_path}: {e}") from e

    def get_run_state(self, run_id: str) -> Optional[RunState]:
        """Get the complete state for a run."""
        run_path = self._get_run_state_path(run_id)

        if not run_path.exists():
            # Try loading from old format (backward compatibility)
            return self._try_load_old_format(run_id)

        try:
            with open(run_path, "r", encoding="utf-8") as f:
                json_str = f.read()
            return RunState.from_json(json_str)
        except Exception as e:
            logger.warning(f"Failed to load run state from {run_path}: {e}")
            return None

    def set_run_state(
        self,
        run_id: str,
        state: RunState,
        overwrite: bool = True
    ) -> None:
        """Set the complete state for a run."""
        run_path = self._get_run_state_path(run_id)

        # Check for concurrent modification
        if not overwrite and run_path.exists():
            raise ConcurrencyError(f"Run state already exists: {run_id}")

        # Ensure parent directory exists
        run_path.parent.mkdir(parents=True, exist_ok=True)

        # Create backup if existing state exists
        if run_path.exists():
            backup_path = run_path.parent / f"{run_path.name}.bak.{state.run_id}"
            try:
                shutil.copy2(run_path, backup_path)
            except Exception as e:
                logger.warning(f"Failed to create backup: {e}")

        # Atomic write
        temp_path = run_path.with_suffix(".tmp")
        try:
            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(state.to_json())
            temp_path.replace(run_path)
        except Exception as e:
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except Exception:
                    pass
            raise StateBackendError(f"Failed to save run state to {run_path}: {e}") from e

    def list_runs(self) -> List[str]:
        """List all available run IDs."""
        if not self.base_path.exists():
            return []

        run_ids = []
        for item in self.base_path.iterdir():
            if item.is_dir():
                run_state_path = item / "run_state.json"
                if run_state_path.exists():
                    run_ids.append(item.name)
        return sorted(run_ids)

    def delete_run(self, run_id: str) -> None:
        """Delete all state for a run."""
        # Sanitize run_id to prevent path traversal
        safe_run_id = self._sanitize_run_id(run_id)
        run_dir = self.base_path / safe_run_id

        if not run_dir.exists():
            return

        try:
            # Remove entire run directory
            shutil.rmtree(run_dir)
        except Exception as e:
            raise StateBackendError(f"Failed to delete run {run_id}: {e}") from e

    def list_nodes(self, run_id: str) -> List[str]:
        """List all node IDs for a run."""
        # Sanitize run_id to prevent path traversal
        safe_run_id = self._sanitize_run_id(run_id)
        nodes_dir = self.base_path / safe_run_id / "nodes"

        if not nodes_dir.exists():
            return []

        node_ids = []
        for item in nodes_dir.iterdir():
            if item.is_file() and item.suffix == ".json":
                node_ids.append(item.stem)
        return sorted(node_ids)

    def delete_node_state(self, run_id: str, node_id: str) -> None:
        """Delete state for a specific node."""
        node_path = self._get_node_state_path(run_id, node_id)

        if node_path.exists():
            try:
                node_path.unlink()
            except Exception as e:
                raise StateBackendError(f"Failed to delete node state: {e}") from e

    def exists(self, run_id: str) -> bool:
        """Check if a run state exists."""
        run_path = self._get_run_state_path(run_id)
        return run_path.exists()

    def node_exists(self, run_id: str, node_id: str) -> bool:
        """Check if a node state exists."""
        node_path = self._get_node_state_path(run_id, node_id)
        return node_path.exists()

    @contextmanager
    def transaction(self) -> Iterator[TransactionState]:
        """Context manager for transactional updates."""
        tx_state = self.begin_transaction()
        try:
            yield tx_state
            if tx_state.is_active:
                self.commit_transaction(tx_state)
        except Exception:
            if tx_state.is_active:
                self.rollback_transaction(tx_state)
            raise

    def begin_transaction(self) -> TransactionState:
        """Begin a new transaction."""
        with self.lock:
            self._transaction_state = TransactionState(
                operations=[],
                is_active=True,
                backend=self
            )
        return self._transaction_state

    def commit_transaction(self, tx_state: TransactionState) -> None:
        """Commit a transaction."""
        if not tx_state.is_active:
            raise TransactionError("Transaction is not active")

        # In file backend, all writes during transaction are already persisted
        # Just mark as complete
        tx_state.is_active = False
        with self.lock:
            if self._transaction_state == tx_state:
                self._transaction_state = None

    def rollback_transaction(self, tx_state: TransactionState) -> None:
        """Rollback a transaction."""
        # For file backend, rollback would require restoring from backup
        # For now, just mark as inactive
        tx_state.is_active = False
        with self.lock:
            if self._transaction_state == tx_state:
                self._transaction_state = None

    def acquire_lock(
        self,
        run_id: str,
        node_id: Optional[str] = None,
        timeout_ms: int = 5000
    ) -> bool:
        """Acquire a lock for optimistic concurrency control."""
        lock_key = f"{run_id}:{node_id or ''}"

        with self.lock:
            if lock_key in self._locks:
                return False
            self._locks[lock_key] = threading.Lock()
            return True

    def release_lock(
        self,
        run_id: str,
        node_id: Optional[str] = None
    ) -> None:
        """Release a previously acquired lock."""
        lock_key = f"{run_id}:{node_id or ''}"

        with self.lock:
            self._locks.pop(lock_key, None)

    def get_metadata(self, run_id: str) -> Dict[str, Any]:
        """Get metadata for a run."""
        state = self.get_run_state(run_id)
        if state:
            return {
                "schema_version": state.schema_version,
                "seeknal_version": state.seeknal_version,
                "last_run": state.last_run,
                "run_id": state.run_id,
            }
        return {}

    def set_metadata(self, run_id: str, metadata: Dict[str, Any]) -> None:
        """Set metadata for a run."""
        state = self.get_run_state(run_id)
        if state:
            # Update fields that are in metadata
            if "schema_version" in metadata:
                state.schema_version = metadata["schema_version"]
            if "seeknal_version" in metadata:
                state.seeknal_version = metadata["seeknal_version"]
            if "last_run" in metadata:
                state.last_run = metadata["last_run"]
            if "run_id" in metadata:
                state.run_id = metadata["run_id"]

            # Save updated state
            self.set_run_state(run_id, state)

    def _try_load_old_format(self, run_id: str) -> Optional[RunState]:
        """
        Try loading from old state file format for backward compatibility.

        Args:
            run_id: The run identifier

        Returns:
            RunState if old format found, None otherwise
        """
        # Try old runner.py state path
        old_state_path = self.base_path / "state" / "execution_state.json"

        if not old_state_path.exists():
            return None

        try:
            with open(old_state_path, "r", encoding="utf-8") as f:
                json_str = f.read()
            state = RunState.from_json(json_str)

            # Migrate to new format
            logger.info(f"Migrating state from old format for {run_id}")
            self.set_run_state(run_id, state)

            return state
        except Exception as e:
            logger.warning(f"Failed to load old state format: {e}")
            return None


def create_file_state_backend(base_path: Path) -> FileStateBackend:
    """
    Convenience function to create a file state backend.

    Args:
        base_path: Base directory for storing state files

    Returns:
        FileStateBackend instance

    Examples:
        >>> backend = create_file_state_backend(Path("/tmp/state"))
        >>> state = backend.get_run_state("run1")
    """
    return FileStateBackend(base_path=base_path)


# Register the file backend with the factory
StateBackendFactory.register("file", FileStateBackend)  # type: ignore[arg-type]
