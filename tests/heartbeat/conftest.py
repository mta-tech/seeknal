"""Shared fixtures for heartbeat tests."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock

import pytest

from seeknal.heartbeat.config import HeartbeatConfig

_REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def tmp_path(tmp_path: Path) -> Iterator[Path]:
    """Repo-internal replacement for the builtin ``tmp_path``.

    Heartbeat config/inbox/catalog APIs validate paths with ``is_insecure_path``,
    which rejects anything under ``/tmp`` — exactly where the builtin ``tmp_path``
    lives on Linux CI. A unique directory under ``<repo>/.seeknal/test-tmp`` keeps
    per-test isolation while passing the security check.
    """

    secure = _REPO_ROOT / ".seeknal" / "test-tmp" / tmp_path.name
    shutil.rmtree(secure, ignore_errors=True)
    secure.mkdir(parents=True, exist_ok=True)
    yield secure
    shutil.rmtree(secure, ignore_errors=True)


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
