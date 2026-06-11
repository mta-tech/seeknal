"""Shared fixtures for Ask access-policy tests."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterator

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def tmp_path(tmp_path: Path) -> Iterator[Path]:
    """Repo-internal replacement for the builtin ``tmp_path``.

    These tests exercise ``AccessPolicy``/``PendingQueue``, which validate paths
    with ``is_insecure_path`` — and that rejects anything under ``/tmp``, exactly
    where the builtin ``tmp_path`` lives on Linux CI. A unique directory under
    ``<repo>/.seeknal/test-tmp`` keeps per-test isolation while passing the
    security check (mirrors ``_secure_project_dir`` in the atlas apply tests).
    """

    secure = _REPO_ROOT / ".seeknal" / "test-tmp" / tmp_path.name
    shutil.rmtree(secure, ignore_errors=True)
    secure.mkdir(parents=True, exist_ok=True)
    yield secure
    shutil.rmtree(secure, ignore_errors=True)
