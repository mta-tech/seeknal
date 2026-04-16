from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.report_server.storage import LocalFilesystemStorage, TarSafetyError


def test_honest_tarball_roundtrip(test_storage: LocalFilesystemStorage, honest_tarball: Path) -> None:
    slug = "abc123"
    with open(honest_tarball, "rb") as f:
        test_storage.extract_tarball(slug, f)

    mount = test_storage.local_mount_path(slug)
    assert mount is not None
    assert (mount / "index.html").exists()
    assert (mount / "_app" / "asset.js").exists()
    assert (mount / "data.duckdb").exists()


def test_local_mount_path_returns_path(test_storage: LocalFilesystemStorage, honest_tarball: Path) -> None:
    slug = "mnttest"
    with open(honest_tarball, "rb") as f:
        test_storage.extract_tarball(slug, f)

    result = test_storage.local_mount_path(slug)
    assert result is not None
    assert isinstance(result, Path)
    assert result.exists()


def test_local_mount_path_returns_none_for_missing(test_storage: LocalFilesystemStorage) -> None:
    result = test_storage.local_mount_path("doesnotexist")
    assert result is None


def test_rejects_path_traversal_tarball(
    test_storage: LocalFilesystemStorage, malicious_tarball_path_traversal: Path
) -> None:
    with open(malicious_tarball_path_traversal, "rb") as f:
        with pytest.raises((TarSafetyError, Exception)):
            test_storage.extract_tarball("traversal", f)

    assert not (test_storage._root / "assets" / "traversal").exists()


def test_rejects_absolute_path_tarball(
    test_storage: LocalFilesystemStorage, malicious_tarball_absolute: Path
) -> None:
    with open(malicious_tarball_absolute, "rb") as f:
        with pytest.raises((TarSafetyError, Exception)):
            test_storage.extract_tarball("absolute", f)


def test_rejects_symlink_tarball(
    test_storage: LocalFilesystemStorage, malicious_tarball_symlink: Path
) -> None:
    with open(malicious_tarball_symlink, "rb") as f:
        with pytest.raises((TarSafetyError, Exception)):
            test_storage.extract_tarball("symlink", f)


def test_rejects_path_traversal_fallback_sanitizer(
    test_storage: LocalFilesystemStorage,
    malicious_tarball_path_traversal: Path,
    monkeypatch,
) -> None:
    fake_version = (3, 11, 0)
    monkeypatch.setattr(sys, "version_info", fake_version)

    with open(malicious_tarball_path_traversal, "rb") as f:
        with pytest.raises((TarSafetyError, Exception)):
            test_storage.extract_tarball("fallback", f)

    assert not (test_storage._root / "assets" / "fallback").exists()
