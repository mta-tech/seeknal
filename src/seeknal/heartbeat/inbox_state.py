"""InboxState — change detection for the heartbeat polling daemon.

Persists per-file checksum + status at ``target/heartbeat/inbox_state.json``.
Atomic writes via tempfile + os.replace.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from seeknal.utils.path_security import is_insecure_path

logger = logging.getLogger("seeknal.heartbeat.inbox_state")

_CHUNK_SIZE = 1024 * 1024  # 1 MiB
_LARGE_FILE_BYTES = 100 * 1024 * 1024  # 100 MiB log threshold
_VALID_STATUSES = {"ok", "quarantined", "failed"}


@dataclass
class InboxFileRecord:
    """Per-file state snapshot keyed by absolute path."""

    path: str
    checksum_sha256: str
    size_bytes: int
    mtime: float
    ingested_at: str
    target_table: str
    last_status: str = "ok"

    def __post_init__(self) -> None:
        if self.last_status not in _VALID_STATUSES:
            raise ValueError(
                f"Invalid last_status '{self.last_status}'. "
                f"Expected one of {sorted(_VALID_STATUSES)}."
            )


def _compute_sha256(path: Path) -> tuple[str, int]:
    """SHA-256 + byte count, streaming the file."""
    hasher = hashlib.sha256()
    size = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(_CHUNK_SIZE)
            if not chunk:
                break
            hasher.update(chunk)
            size += len(chunk)
    if size > _LARGE_FILE_BYTES:
        logger.info(
            "Hashed large file %s (%.1f MB)",
            path,
            size / (1024 * 1024),
        )
    return hasher.hexdigest(), size


def _utcnow_iso() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class InboxState:
    """Persistent record of which inbox files have been seen / ingested."""

    def __init__(self, state_path: Path):
        state_path = Path(state_path).expanduser()
        # Path security check on the state file location
        if is_insecure_path(str(state_path)):
            raise ValueError(
                f"Insecure InboxState path: {state_path!s}. "
                "Use a project-local target/heartbeat/inbox_state.json."
            )
        self._state_path = state_path
        self._records: dict[str, InboxFileRecord] = {}

    @property
    def state_path(self) -> Path:
        return self._state_path

    @classmethod
    def load(cls, state_path: Path) -> "InboxState":
        state = cls(state_path)
        if state._state_path.exists():
            try:
                with open(state._state_path, "r") as fh:
                    raw = json.load(fh)
            except json.JSONDecodeError as exc:
                logger.warning(
                    "InboxState file %s is corrupt (%s); starting empty.",
                    state._state_path,
                    exc,
                )
                return state
            if not isinstance(raw, dict):
                logger.warning(
                    "InboxState file %s did not contain a JSON object; starting empty.",
                    state._state_path,
                )
                return state
            for key, entry in raw.items():
                if not isinstance(entry, dict):
                    continue
                try:
                    state._records[key] = InboxFileRecord(**entry)
                except (TypeError, ValueError) as exc:
                    logger.warning(
                        "Skipping malformed inbox_state entry %s: %s", key, exc
                    )
        return state

    def save(self) -> None:
        """Atomic write: serialize to a temp file in the same dir then os.replace."""
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {key: asdict(rec) for key, rec in self._records.items()}
        directory = str(self._state_path.parent)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".inbox_state.", suffix=".json.tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(payload, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self._state_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except FileNotFoundError:
                pass
            raise

    def get(self, path: Path) -> Optional[InboxFileRecord]:
        return self._records.get(str(path.resolve()))

    def all_records(self) -> list[InboxFileRecord]:
        return list(self._records.values())

    def scan(self, folders: Iterable[Path]) -> list[Path]:
        """Return paths in ``folders`` whose checksum is new or changed.

        Quarantined entries (``last_status == 'quarantined'``) are not re-emitted
        unless their checksum changes (the file was replaced).
        """
        candidates: list[Path] = []
        for folder in folders:
            folder = Path(folder).expanduser()
            if not folder.exists() or not folder.is_dir():
                continue
            for entry in sorted(folder.iterdir()):
                if not entry.is_file():
                    continue
                if entry.name.startswith("."):
                    continue
                candidates.append(entry.resolve())

        new_or_changed: list[Path] = []
        for path in candidates:
            try:
                checksum, size = _compute_sha256(path)
            except OSError as exc:
                logger.warning("Skipping unreadable inbox file %s: %s", path, exc)
                continue
            key = str(path)
            existing = self._records.get(key)
            if existing is None or existing.checksum_sha256 != checksum:
                new_or_changed.append(path)
                # remember size for the change handler so it doesn't re-hash
                self._pending_meta = getattr(self, "_pending_meta", {})
                self._pending_meta[key] = (checksum, size, path.stat().st_mtime)
        return new_or_changed

    def _meta(self, path: Path) -> tuple[str, int, float]:
        key = str(path)
        pending = getattr(self, "_pending_meta", {})
        if key in pending:
            return pending[key]
        checksum, size = _compute_sha256(path)
        return checksum, size, path.stat().st_mtime

    def mark_ingested(
        self,
        path: Path,
        target_table: str,
        status: str = "ok",
    ) -> InboxFileRecord:
        checksum, size, mtime = self._meta(path)
        record = InboxFileRecord(
            path=str(path),
            checksum_sha256=checksum,
            size_bytes=size,
            mtime=mtime,
            ingested_at=_utcnow_iso(),
            target_table=target_table,
            last_status=status,
        )
        self._records[str(path)] = record
        return record

    def mark_quarantined(
        self,
        path: Path,
        reason: str,
        target_table: str = "",
    ) -> InboxFileRecord:
        del reason  # captured by RunRecorder; InboxFileRecord just tracks status
        return self.mark_ingested(path, target_table=target_table, status="quarantined")

    def mark_failed(self, path: Path, target_table: str = "") -> InboxFileRecord:
        return self.mark_ingested(path, target_table=target_table, status="failed")
