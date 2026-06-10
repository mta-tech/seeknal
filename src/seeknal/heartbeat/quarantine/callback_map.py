"""Monotonic base36 token map for Telegram inline-keyboard callback_data.

Telegram limits ``callback_data`` to 64 bytes. The natural approach (UUID
prefix) is collision-prone over months; we use a monotonic 4-char base36
counter that resumes from the persisted max on daemon restart.

Stored at ``target/heartbeat/quarantine/_callback_map.json``.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Optional


def _to_base36(num: int, width: int = 4) -> str:
    digits = "0123456789abcdefghijklmnopqrstuvwxyz"
    if num < 0:
        raise ValueError("counter must be non-negative")
    if num == 0:
        return "0".zfill(width)
    parts: list[str] = []
    while num > 0:
        num, rem = divmod(num, 36)
        parts.append(digits[rem])
    return "".join(reversed(parts)).rjust(width, "0")


def _from_base36(value: str) -> int:
    return int(value, 36)


class CallbackMap:
    """Per-project mapping token ↔ run_id, persisted atomically."""

    def __init__(self, path: Path):
        self._path = Path(path)
        self._lock = threading.Lock()
        self._token_to_run: dict[str, str] = {}
        self._counter = 0
        self._load()

    @property
    def path(self) -> Path:
        return self._path

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            raw = json.loads(self._path.read_text())
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(raw, dict):
            return
        self._token_to_run = {str(k): str(v) for k, v in raw.items()}
        if self._token_to_run:
            try:
                self._counter = max(_from_base36(t) for t in self._token_to_run)
            except ValueError:
                self._counter = len(self._token_to_run)

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=".callback_map.", suffix=".json.tmp", dir=str(self._path.parent)
        )
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(self._token_to_run, fh, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self._path)
        except Exception:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    def allocate(self, run_id: str) -> str:
        with self._lock:
            self._counter += 1
            token = _to_base36(self._counter)
            self._token_to_run[token] = run_id
            self._save()
            return token

    def lookup(self, token: str) -> Optional[str]:
        return self._token_to_run.get(token)

    def remove(self, token: str) -> None:
        with self._lock:
            if token in self._token_to_run:
                del self._token_to_run[token]
                self._save()


__all__ = ["CallbackMap"]
