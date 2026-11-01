"""Lakebase sync state discovery source."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from seeknal.serving.discovery.config import DiscoveryError


@dataclass(frozen=True)
class SyncState:
    """Replication state for a Lakebase target table."""

    snapshot_id: int | None
    synced_at: datetime


class LakebaseSyncSource:
    """Reads replication sync state from the Lakebase metadata table."""

    def __init__(self, connect: Callable[[], Any]) -> None:
        self.connect = connect

    def read_state(self) -> dict[str, SyncState]:
        """Query _lakebase_sync_state and return mapping of target_table -> SyncState."""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT target_table, snapshot_id, synced_at FROM _lakebase_sync_state"
                )
                rows = cursor.fetchall()
            finally:
                cursor.close()
        except Exception as exc:
            raise DiscoveryError(f"Failed to query Lakebase sync state: {exc}") from exc

        state: dict[str, SyncState] = {}
        for row in rows:
            target_table = str(row[0]).strip()
            raw_snap = row[1]
            snapshot_id = int(raw_snap) if raw_snap is not None else None

            raw_synced = row[2]
            try:
                if isinstance(raw_synced, str):
                    synced_at = datetime.fromisoformat(raw_synced)
                elif isinstance(raw_synced, datetime):
                    synced_at = raw_synced
                else:
                    raise ValueError(
                        f"Unsupported synced_at type '{type(raw_synced).__name__}' for '{target_table}'"
                    )

                if synced_at.tzinfo is None:
                    synced_at = synced_at.replace(tzinfo=timezone.utc)
                else:
                    synced_at = synced_at.astimezone(timezone.utc)
            except Exception as exc:
                raise DiscoveryError(
                    f"Failed to parse synced_at for target '{target_table}': {exc}"
                ) from exc

            state[target_table] = SyncState(
                snapshot_id=snapshot_id,
                synced_at=synced_at,
            )

        return state
