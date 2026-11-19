from __future__ import annotations

import io
import sys
import tarfile
from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.report_server.storage import LocalFilesystemStorage, TarSafetyError, _validate_tar_member
from tests.report_server.conftest import _make_tarball


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
        with pytest.raises(TarSafetyError):
            test_storage.extract_tarball("traversal", f)

    assert not (test_storage._root / "assets" / "traversal").exists()


def test_rejects_absolute_path_tarball(
    test_storage: LocalFilesystemStorage, malicious_tarball_absolute: Path
) -> None:
    with open(malicious_tarball_absolute, "rb") as f:
        with pytest.raises(TarSafetyError):
            test_storage.extract_tarball("absolute", f)

    assert not (test_storage._root / "assets" / "absolute").exists()


def test_rejects_symlink_tarball(
    test_storage: LocalFilesystemStorage, malicious_tarball_symlink: Path
) -> None:
    with open(malicious_tarball_symlink, "rb") as f:
        with pytest.raises(TarSafetyError):
            test_storage.extract_tarball("symlink", f)

    assert not (test_storage._root / "assets" / "symlink").exists()


def test_rejects_path_traversal_fallback_sanitizer(
    test_storage: LocalFilesystemStorage,
    malicious_tarball_path_traversal: Path,
    monkeypatch,
) -> None:
    fake_version = (3, 11, 0)
    monkeypatch.setattr(sys, "version_info", fake_version)
    monkeypatch.delattr(tarfile, "data_filter", raising=False)

    with open(malicious_tarball_path_traversal, "rb") as f:
        with pytest.raises(TarSafetyError):
            test_storage.extract_tarball("fallback", f)

    assert not (test_storage._root / "assets" / "fallback").exists()


@pytest.mark.parametrize("use_data_filter", [True, False])
def test_safety_parity_with_and_without_data_filter(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    use_data_filter: bool,
) -> None:
    if not use_data_filter:
        monkeypatch.delattr(tarfile, "data_filter", raising=False)
    else:
        assert hasattr(tarfile, "data_filter")

    # Build malicious tarballs for each required class:
    # 1. absolute (POSIX, Windows drive, UNC)
    # 2. .. component
    # 3. symlink
    # 4. hardlink
    # 5. device / FIFO
    # 6. resolved escape (preceded by a valid member to prove nothing is written when rejected)
    cases: list[tuple[str, Path]] = [
        (
            "abs_posix",
            _make_tarball(
                tmp_path,
                [("innocent.txt", b"ok", "file"), ("/etc/passwd", b"root", "absolute")],
                filename="abs_posix.tar.gz",
            ),
        ),
        (
            "abs_win_drive",
            _make_tarball(
                tmp_path,
                [("innocent.txt", b"ok", "file"), ("C:\\Windows\\win.ini", b"win", "file")],
                filename="abs_win_drive.tar.gz",
            ),
        ),
        (
            "abs_unc",
            _make_tarball(
                tmp_path,
                [("innocent.txt", b"ok", "file"), ("\\\\server\\share\\file.txt", b"unc", "file")],
                filename="abs_unc.tar.gz",
            ),
        ),
        (
            "dotdot",
            _make_tarball(
                tmp_path,
                [("innocent.txt", b"ok", "file"), ("../../etc/passwd", b"root", "file")],
                filename="dotdot.tar.gz",
            ),
        ),
        (
            "symlink",
            _make_tarball(
                tmp_path,
                [("innocent.txt", b"ok", "file"), ("evil_sym", "/etc/passwd", "symlink")],
                filename="symlink.tar.gz",
            ),
        ),
        (
            "hardlink",
            _make_tarball(
                tmp_path,
                [("innocent.txt", b"ok", "file"), ("evil_lnk", "/etc/passwd", "hardlink")],
                filename="hardlink.tar.gz",
            ),
        ),
        (
            "device",
            _make_tarball(
                tmp_path,
                [("innocent.txt", b"ok", "file"), ("dev_null", b"", "chrdev")],
                filename="device.tar.gz",
            ),
        ),
        (
            "fifo",
            _make_tarball(
                tmp_path,
                [("innocent.txt", b"ok", "file"), ("named_pipe", b"", "fifo")],
                filename="fifo.tar.gz",
            ),
        ),
    ]

    for case_name, tarball_path in cases:
        slug = f"parity_{case_name}_{int(use_data_filter)}"
        with open(tarball_path, "rb") as f:
            with pytest.raises(TarSafetyError):
                test_storage.extract_tarball(slug, f)
        assert not (test_storage._root / "assets" / slug).exists(), (
            f"Rejected archive {case_name} (data_filter={use_data_filter}) must not leave files"
        )

    # Resolved target escape via pre-existing symlink inside dest or realpath resolution outside dest:
    escape_tarball = _make_tarball(
        tmp_path,
        [("innocent.txt", b"ok", "file"), ("sub/escaped.txt", b"escape", "file")],
        filename="escape.tar.gz",
    )
    escape_slug = f"parity_escape_{int(use_data_filter)}"
    orig_realpath = Path(test_storage._root).resolve()

    import os as _os

    real_os_realpath = _os.path.realpath

    def _fake_realpath(p):
        p_str = str(p)
        if p_str.endswith("sub/escaped.txt") or p_str.endswith("sub\\escaped.txt"):
            return str(orig_realpath.parent / "outside_escaped.txt")
        return real_os_realpath(p)

    with patch("seeknal.report_server.storage.os.path.realpath", side_effect=_fake_realpath):
        with open(escape_tarball, "rb") as f:
            with pytest.raises(TarSafetyError, match="escapes destination"):
                test_storage.extract_tarball(escape_slug, f)

    assert not (test_storage._root / "assets" / escape_slug).exists()


def test_honest_tarball_extracts_identically_with_and_without_data_filter(
    test_storage: LocalFilesystemStorage,
    honest_tarball: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slug_with = "honest_with_filter"
    with open(honest_tarball, "rb") as f:
        test_storage.extract_tarball(slug_with, f)

    monkeypatch.delattr(tarfile, "data_filter", raising=False)
    slug_without = "honest_without_filter"
    with open(honest_tarball, "rb") as f:
        test_storage.extract_tarball(slug_without, f)

    mount_with = test_storage.local_mount_path(slug_with)
    mount_without = test_storage.local_mount_path(slug_without)
    assert mount_with is not None and mount_without is not None

    files_with = sorted(
        p.relative_to(mount_with) for p in mount_with.rglob("*") if p.is_file()
    )
    files_without = sorted(
        p.relative_to(mount_without) for p in mount_without.rglob("*") if p.is_file()
    )
    assert files_with == files_without
    for rel in files_with:
        assert (mount_with / rel).read_bytes() == (mount_without / rel).read_bytes()

