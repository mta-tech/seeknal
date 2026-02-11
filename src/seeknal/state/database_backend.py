"""
Database-based state backend implementation.

This module provides a database-based implementation of the StateBackend protocol,
using SQLite for persistence with full transaction support.

Key features:
- SQLite-based persistence with JSON columns
- ACID transactions for atomic multi-node updates
- Automatic schema creation and migration
- Connection pooling for concurrent access
"""

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from .backend import (
    StateBackend,
    StateBackendError,
    TransactionError,
    ConcurrencyError,
    TransactionState,
    StateBackendFactory,
)
from ..workflow.state import NodeState, RunState

logger = logging.getLogger(__name__)


class DatabaseStateBackend(StateBackend):
    """
    Database-based implementation of StateBackend.

    Stores state in SQLite database with JSON columns for complex data.
    Supports full ACID transactions and concurrent access.

    Attributes:
        db_path: Path to SQLite database file
        lock: Thread lock for connection management
        _local: Thread-local storage for connections
    """

    def __init__(self, db_path: Path):
        """
        Initialize the database state backend.

        Args:
            db_path: Path to SQLite database file (created if doesn't exist)
        """
        self.db_path = Path(db_path)
        self.lock = threading.Lock()
        self._local = threading.local()
        self._transaction_state: Optional[TransactionState] = None

        # Ensure parent directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize database schema
        self._init_schema()

    def _get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                isolation_level="DEFERRED"  # Deferred transaction mode
            )
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    def _is_in_transaction(self) -> bool:
        """Check if currently in an active transaction."""
        try:
            conn = self._get_connection()
            return conn.in_transaction
        except AttributeError:
            # Fallback for older SQLite versions
            return False

    def _init_schema(self) -> None:
        """Create database schema if it doesn't exist."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Create run_state table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS run_state (
                run_id TEXT PRIMARY KEY,
                schema_version TEXT DEFAULT '2.0',
                seeknal_version TEXT DEFAULT '2.0.0',
                last_run TEXT,
                config_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create node_state table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS node_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                hash TEXT,
                last_run TEXT,
                status TEXT,
                duration_ms INTEGER DEFAULT 0,
                row_count INTEGER DEFAULT 0,
                version INTEGER DEFAULT 1,
                metadata_json TEXT,
                fingerprint_json TEXT,
                iceberg_snapshot_id TEXT,
                iceberg_snapshot_timestamp TEXT,
                iceberg_table_ref TEXT,
                iceberg_schema_version INTEGER,
                completed_intervals_json TEXT,
                completed_partitions_json TEXT,
                restatement_intervals_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(run_id, node_id)
            )
        """)

        # Create indexes
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_node_state_run_id
            ON node_state(run_id)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_node_state_status
            ON node_state(status)
        """)

        # Create locks table for optimistic concurrency
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS backend_locks (
                lock_key TEXT PRIMARY KEY,
                locked_at TEXT DEFAULT CURRENT_TIMESTAMP,
                locked_by TEXT
            )
        """)

        conn.commit()

    def get_node_state(self, run_id: str, node_id: str) -> Optional[NodeState]:
        """Get the state for a specific node in a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                hash, last_run, status, duration_ms, row_count, version,
                metadata_json, fingerprint_json, iceberg_snapshot_id,
                iceberg_snapshot_timestamp, iceberg_table_ref, iceberg_schema_version,
                completed_intervals_json, completed_partitions_json, restatement_intervals_json
            FROM node_state
            WHERE run_id = ? AND node_id = ?
        """, (run_id, node_id))

        row = cursor.fetchone()
        if row is None:
            return None

        return self._row_to_node_state(row)

    def set_node_state(
        self,
        run_id: str,
        node_id: str,
        state: NodeState,
        overwrite: bool = True
    ) -> None:
        """Set the state for a specific node in a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Check for concurrent modification
        if not overwrite:
            cursor.execute("""
                SELECT 1 FROM node_state WHERE run_id = ? AND node_id = ?
            """, (run_id, node_id))
            if cursor.fetchone() is not None:
                raise ConcurrencyError(f"Node state already exists: {run_id}:{node_id}")

        # Serialize complex fields
        metadata_json = json.dumps(state.metadata) if state.metadata else None
        fingerprint_json = json.dumps(state.fingerprint.to_dict()) if state.fingerprint else None
        completed_intervals_json = json.dumps(state.completed_intervals) if state.completed_intervals else None
        completed_partitions_json = json.dumps(state.completed_partitions) if state.completed_partitions else None
        restatement_intervals_json = json.dumps(state.restatement_intervals) if state.restatement_intervals else None

        # Upsert node state
        cursor.execute("""
            INSERT INTO node_state (
                run_id, node_id, hash, last_run, status, duration_ms, row_count,
                version, metadata_json, fingerprint_json, iceberg_snapshot_id,
                iceberg_snapshot_timestamp, iceberg_table_ref, iceberg_schema_version,
                completed_intervals_json, completed_partitions_json, restatement_intervals_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, node_id) DO UPDATE SET
                hash = excluded.hash,
                last_run = excluded.last_run,
                status = excluded.status,
                duration_ms = excluded.duration_ms,
                row_count = excluded.row_count,
                version = excluded.version,
                metadata_json = excluded.metadata_json,
                fingerprint_json = excluded.fingerprint_json,
                iceberg_snapshot_id = excluded.iceberg_snapshot_id,
                iceberg_snapshot_timestamp = excluded.iceberg_snapshot_timestamp,
                iceberg_table_ref = excluded.iceberg_table_ref,
                iceberg_schema_version = excluded.iceberg_schema_version,
                completed_intervals_json = excluded.completed_intervals_json,
                completed_partitions_json = excluded.completed_partitions_json,
                restatement_intervals_json = excluded.restatement_intervals_json,
                updated_at = CURRENT_TIMESTAMP
        """, (
            run_id, node_id, state.hash, state.last_run, state.status,
            state.duration_ms, state.row_count, state.version, metadata_json,
            fingerprint_json, state.iceberg_snapshot_id, state.iceberg_snapshot_timestamp,
            state.iceberg_table_ref, state.iceberg_schema_version,
            completed_intervals_json, completed_partitions_json, restatement_intervals_json
        ))

        # Only commit if not in a transaction
        if not self._is_in_transaction():
            conn.commit()

    def get_run_state(self, run_id: str) -> Optional[RunState]:
        """Get the complete state for a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT run_id, schema_version, seeknal_version, last_run, config_json
            FROM run_state
            WHERE run_id = ?
        """, (run_id,))

        row = cursor.fetchone()
        if row is None:
            return None

        # Get all node states for this run
        cursor.execute("""
            SELECT node_id, hash, last_run, status, duration_ms, row_count, version,
                   metadata_json, fingerprint_json, iceberg_snapshot_id,
                   iceberg_snapshot_timestamp, iceberg_table_ref, iceberg_schema_version,
                   completed_intervals_json, completed_partitions_json, restatement_intervals_json
            FROM node_state
            WHERE run_id = ?
        """, (run_id,))

        nodes = {}
        for node_row in cursor.fetchall():
            node_id = node_row[0]
            node_state = self._node_row_to_node_state(node_row[1:])
            nodes[node_id] = node_state

        # Build RunState
        config = json.loads(row[4]) if row[4] else {}

        return RunState(
            schema_version=row[1],
            seeknal_version=row[2],
            last_run=row[3],
            run_id=row[0],
            config=config,
            nodes=nodes,
        )

    def set_run_state(
        self,
        run_id: str,
        state: RunState,
        overwrite: bool = True
    ) -> None:
        """Set the complete state for a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        # Check for concurrent modification
        if not overwrite:
            cursor.execute("""
                SELECT 1 FROM run_state WHERE run_id = ?
            """, (run_id,))
            if cursor.fetchone() is not None:
                raise ConcurrencyError(f"Run state already exists: {run_id}")

        # Serialize config
        config_json = json.dumps(state.config) if state.config else None

        # Insert or update run state
        cursor.execute("""
            INSERT INTO run_state (run_id, schema_version, seeknal_version, last_run, config_json)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                schema_version = excluded.schema_version,
                seeknal_version = excluded.seeknal_version,
                last_run = excluded.last_run,
                config_json = excluded.config_json,
                updated_at = CURRENT_TIMESTAMP
        """, (
            state.run_id, state.schema_version, state.seeknal_version,
            state.last_run, config_json
        ))

        # Delete existing node states for this run
        cursor.execute("""
            DELETE FROM node_state WHERE run_id = ?
        """, (run_id,))

        # Insert all node states
        for node_id, node_state in state.nodes.items():
            self.set_node_state(run_id, node_id, node_state, overwrite=True)

        # Only commit if not in a transaction
        if not self._is_in_transaction():
            conn.commit()

    def list_runs(self) -> List[str]:
        """List all available run IDs."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT run_id FROM run_state ORDER BY created_at")
        return [row[0] for row in cursor.fetchall()]

    def delete_run(self, run_id: str) -> None:
        """Delete all state for a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Delete all node states for this run
            cursor.execute("""
                DELETE FROM node_state WHERE run_id = ?
            """, (run_id,))

            # Delete run state
            cursor.execute("""
                DELETE FROM run_state WHERE run_id = ?
            """, (run_id,))

            # Only commit if not in a transaction
            if not self._is_in_transaction():
                conn.commit()
        except Exception as e:
            if not self._is_in_transaction():
                conn.rollback()
            raise StateBackendError(f"Failed to delete run {run_id}: {e}") from e

    def list_nodes(self, run_id: str) -> List[str]:
        """List all node IDs for a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT node_id FROM node_state WHERE run_id = ? ORDER BY node_id
        """, (run_id,))

        return [row[0] for row in cursor.fetchall()]

    def delete_node_state(self, run_id: str, node_id: str) -> None:
        """Delete state for a specific node."""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                DELETE FROM node_state WHERE run_id = ? AND node_id = ?
            """, (run_id, node_id))
            # Only commit if not in a transaction
            if not self._is_in_transaction():
                conn.commit()
        except Exception as e:
            if not self._is_in_transaction():
                conn.rollback()
            raise StateBackendError(f"Failed to delete node state: {e}") from e

    def exists(self, run_id: str) -> bool:
        """Check if a run state exists."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 1 FROM run_state WHERE run_id = ?
        """, (run_id,))

        return cursor.fetchone() is not None

    def node_exists(self, run_id: str, node_id: str) -> bool:
        """Check if a node state exists."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT 1 FROM node_state WHERE run_id = ? AND node_id = ?
        """, (run_id, node_id))

        return cursor.fetchone() is not None

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
            conn = self._get_connection()
            # Only begin if not already in a transaction
            if not self._is_in_transaction():
                conn.execute("BEGIN")
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

        conn = self._get_connection()
        try:
            if self._is_in_transaction():
                conn.commit()
            tx_state.is_active = False
            with self.lock:
                if self._transaction_state == tx_state:
                    self._transaction_state = None
        except Exception as e:
            if self._is_in_transaction():
                conn.rollback()
            raise TransactionError(f"Failed to commit transaction: {e}") from e

    def rollback_transaction(self, tx_state: TransactionState) -> None:
        """Rollback a transaction."""
        conn = self._get_connection()
        try:
            if self._is_in_transaction():
                conn.rollback()
            tx_state.is_active = False
            with self.lock:
                if self._transaction_state == tx_state:
                    self._transaction_state = None
        except Exception as e:
            raise TransactionError(f"Failed to rollback transaction: {e}") from e

    def acquire_lock(
        self,
        run_id: str,
        node_id: Optional[str] = None,
        timeout_ms: int = 5000
    ) -> bool:
        """Acquire a lock for optimistic concurrency control."""
        lock_key = f"{run_id}:{node_id or ''}"
        conn = self._get_connection()
        cursor = conn.cursor()

        # Try to insert lock record
        try:
            cursor.execute("""
                INSERT INTO backend_locks (lock_key, locked_by)
                VALUES (?, ?)
            """, (lock_key, threading.current_thread().name))
            # Only commit if not in a transaction
            if not self._is_in_transaction():
                conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Lock already exists
            return False

    def release_lock(
        self,
        run_id: str,
        node_id: Optional[str] = None
    ) -> None:
        """Release a previously acquired lock."""
        lock_key = f"{run_id}:{node_id or ''}"
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            DELETE FROM backend_locks WHERE lock_key = ?
        """, (lock_key,))
        # Only commit if not in a transaction
        if not self._is_in_transaction():
            conn.commit()

    def get_metadata(self, run_id: str) -> Dict[str, Any]:
        """Get metadata for a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT schema_version, seeknal_version, last_run, run_id, config_json
            FROM run_state
            WHERE run_id = ?
        """, (run_id,))

        row = cursor.fetchone()
        if row is None:
            return {}

        return {
            "schema_version": row[0],
            "seeknal_version": row[1],
            "last_run": row[2],
            "run_id": row[3],
        }

    def set_metadata(self, run_id: str, metadata: Dict[str, Any]) -> None:
        """Set metadata for a run."""
        state = self.get_run_state(run_id)
        if not state:
            raise StateBackendError(f"Run {run_id} not found")

        # Update fields from metadata
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

    def get_all_node_states(self, run_id: str) -> Dict[str, NodeState]:
        """Get all node states for a run."""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT node_id, hash, last_run, status, duration_ms, row_count, version,
                   metadata_json, fingerprint_json, iceberg_snapshot_id,
                   iceberg_snapshot_timestamp, iceberg_table_ref, iceberg_schema_version,
                   completed_intervals_json, completed_partitions_json, restatement_intervals_json
            FROM node_state
            WHERE run_id = ?
        """, (run_id,))

        states = {}
        for row in cursor.fetchall():
            node_id = row[0]
            node_state = self._node_row_to_node_state(row[1:])
            states[node_id] = node_state

        return states

    def health_check(self) -> bool:
        """Check backend health by querying database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return cursor.fetchone()[0] == 1
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")
            return False

    def _row_to_node_state(self, row: sqlite3.Row) -> NodeState:
        """Convert a database row to NodeState."""
        return self._node_row_to_node_state(list(row))

    def _node_row_to_node_state(self, row: list) -> NodeState:
        """Convert a row list to NodeState."""
        (
            hash_val, last_run, status, duration_ms, row_count, version,
            metadata_json, fingerprint_json, iceberg_snapshot_id,
            iceberg_snapshot_timestamp, iceberg_table_ref, iceberg_schema_version,
            completed_intervals_json, completed_partitions_json, restatement_intervals_json
        ) = row

        # Deserialize complex fields
        metadata = json.loads(metadata_json) if metadata_json else {}
        fingerprint = None
        if fingerprint_json:
            from ..workflow.state import NodeFingerprint
            fingerprint = NodeFingerprint.from_dict(json.loads(fingerprint_json))

        completed_intervals = []
        if completed_intervals_json:
            completed_intervals = [
                tuple(interval) if isinstance(interval, list) else interval
                for interval in json.loads(completed_intervals_json)
            ]

        completed_partitions = []
        if completed_partitions_json:
            completed_partitions = json.loads(completed_partitions_json)

        restatement_intervals = []
        if restatement_intervals_json:
            restatement_intervals = [
                tuple(interval) if isinstance(interval, list) else interval
                for interval in json.loads(restatement_intervals_json)
            ]

        return NodeState(
            hash=hash_val or "",
            last_run=last_run or "",
            status=status or "pending",
            duration_ms=duration_ms or 0,
            row_count=row_count or 0,
            version=version or 1,
            metadata=metadata,
            fingerprint=fingerprint,
            iceberg_snapshot_id=iceberg_snapshot_id,
            iceberg_snapshot_timestamp=iceberg_snapshot_timestamp,
            iceberg_table_ref=iceberg_table_ref,
            iceberg_schema_version=iceberg_schema_version,
            completed_intervals=completed_intervals,
            completed_partitions=completed_partitions,
            restatement_intervals=restatement_intervals,
        )

    def close(self) -> None:
        """Close database connection."""
        if hasattr(self._local, 'conn') and self._local.conn:
            conn = self._local.conn
            # Commit any pending transaction before closing
            try:
                if self._is_in_transaction():
                    conn.commit()
            except Exception:
                pass  # Ignore errors during cleanup
            conn.close()
            self._local.conn = None

    def __del__(self):
        """Cleanup on deletion."""
        try:
            self.close()
        except Exception:
            pass


def create_database_state_backend(db_path: Path) -> DatabaseStateBackend:
    """
    Convenience function to create a database state backend.

    Args:
        db_path: Path to SQLite database file

    Returns:
        DatabaseStateBackend instance

    Examples:
        >>> backend = create_database_state_backend(Path("/tmp/state.db"))
        >>> state = backend.get_run_state("run1")
    """
    return DatabaseStateBackend(db_path=db_path)


# Register the database backend with the factory
StateBackendFactory.register("database", DatabaseStateBackend)  # type: ignore[arg-type]
StateBackendFactory.register("sqlite", DatabaseStateBackend)  # type: ignore[arg-type]
