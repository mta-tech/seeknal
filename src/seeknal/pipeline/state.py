"""Persistent per-node state helpers for Python pipelines."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _default_node_key(node_id: str | None) -> str:
    if not node_id:
        return "adhoc"
    return node_id.replace(".", "_")


@dataclass
class PipelineState:
    """Tiny JSON-backed key-value store scoped to one pipeline node."""

    target_dir: Path
    node_id: str | None = None
    _data: dict[str, Any] = field(default_factory=dict, repr=False)
    _loaded: bool = field(default=False, repr=False)

    @property
    def path(self) -> Path:
        return self.target_dir / "pipeline_state" / f"{_default_node_key(self.node_id)}.json"

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text())
            except Exception:
                self._data = {}
        self._loaded = True

    def get(self, key: str, default: Any = None) -> Any:
        self._ensure_loaded()
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> Any:
        self._ensure_loaded()
        self._data[key] = value
        self.save()
        return value

    def delete(self, key: str) -> None:
        self._ensure_loaded()
        if key in self._data:
            del self._data[key]
            self.save()

    def save(self) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data, indent=2, default=str))
        return self.path

