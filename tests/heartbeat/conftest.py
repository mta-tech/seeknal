"""Shared fixtures for heartbeat tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from seeknal.heartbeat.config import HeartbeatConfig


@pytest.fixture
def temp_project(tmp_path: Path) -> Path:
    """A minimal project directory with HEARTBEAT.md."""
    (tmp_path / "HEARTBEAT.md").write_text(
        """```yaml
interval: 0s
enabled: true
inbox_folders:
  - inbox
```
"""
    )
    (tmp_path / "inbox").mkdir(exist_ok=True)
    return tmp_path


@pytest.fixture
def disabled_project(tmp_path: Path) -> Path:
    (tmp_path / "HEARTBEAT.md").write_text(
        """```yaml
interval: 30s
enabled: false
```
"""
    )
    return tmp_path


@pytest.fixture
def config(temp_project: Path) -> HeartbeatConfig:
    return HeartbeatConfig.load(temp_project)


@pytest.fixture
def stub_profile_loader() -> MagicMock:
    loader = MagicMock()
    loader.load_source_defaults.return_value = {}
    return loader
