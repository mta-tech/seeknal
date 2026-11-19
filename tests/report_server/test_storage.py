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


def test_gzip_bomb_rejected_before_extraction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)
    storage = LocalFilesystemStorage(
        tmp_path / "bomb-storage",
        max_unpacked_bytes=5 * 1024 * 1024,  # 5 MiB cap for fast test
    )

    # Create a highly compressible archive (~1000:1 expansion: 10 MiB of zeros compresses to ~10 KB)
    bomb_path = tmp_path / "bomb.tar.gz"
    payload = b"\x00" * (10 * 1024 * 1024)
    with tarfile.open(bomb_path, "w:gz") as tf:
        first = tarfile.TarInfo(name="innocent.txt")
        first_data = b"hello"
        first.size = len(first_data)
        tf.addfile(first, io.BytesIO(first_data))

        bomb_info = tarfile.TarInfo(name="bomb.bin")
        bomb_info.size = len(payload)
        tf.addfile(bomb_info, io.BytesIO(payload))

    assert bomb_path.stat().st_size < 50 * 1024  # ~10 KB compressed (~1000:1 ratio)

    extract_called = False
    orig_extractall = tarfile.TarFile.extractall
    orig_extract = tarfile.TarFile.extract

    def _spy_extractall(self, *args, **kwargs):
        nonlocal extract_called
        extract_called = True
        return orig_extractall(self, *args, **kwargs)

    def _spy_extract(self, *args, **kwargs):
        nonlocal extract_called
        extract_called = True
        return orig_extract(self, *args, **kwargs)

    monkeypatch.setattr(tarfile.TarFile, "extractall", _spy_extractall)
    monkeypatch.setattr(tarfile.TarFile, "extract", _spy_extract)

    with open(bomb_path, "rb") as f:
        with pytest.raises(TarSafetyError, match="Tarball uncompressed size exceeds limit"):
            storage.extract_tarball("bomb_slug", f)

    assert not extract_called, "Nothing must be extracted when uncompressed size cap is exceeded"
    assert not (storage._root / "assets" / "bomb_slug").exists()


def test_many_empty_members_rejected_early_with_bounded_memory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import gzip
    import tracemalloc

    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)
    storage = LocalFilesystemStorage(tmp_path / "many-members-storage")  # default 10,000 cap

    # Build a valid tar.gz with 200,000 empty file members rapidly by repeating a 512-byte tar header
    info = tarfile.TarInfo(name="f.txt")
    info.size = 0
    header_block = info.tobuf()
    assert len(header_block) == 512

    many_tar_gz = tmp_path / "200k.tar.gz"
    with gzip.open(many_tar_gz, "wb", compresslevel=1) as gz:
        chunk = header_block * 2000  # 1 MB per write
        for _ in range(100):  # 100 * 2000 = 200,000 members
            gz.write(chunk)
        gz.write(b"\x00" * 1024)  # EOF blocks

    getmembers_called = False
    orig_getmembers = tarfile.TarFile.getmembers

    def _forbid_getmembers(self, *args, **kwargs):
        nonlocal getmembers_called
        getmembers_called = True
        return orig_getmembers(self, *args, **kwargs)

    monkeypatch.setattr(tarfile.TarFile, "getmembers", _forbid_getmembers)

    tracemalloc.start()
    try:
        with open(many_tar_gz, "rb") as f:
            with pytest.raises(TarSafetyError, match="Tarball has too many members"):
                storage.extract_tarball("many_slug", f)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert not getmembers_called, "getmembers() must not be called on the archive"
    assert peak_bytes < 20 * 1024 * 1024, f"Peak memory {peak_bytes} exceeded 20 MiB bound"
    assert not (storage._root / "assets" / "many_slug").exists()


def test_path_depth_and_name_length_caps_enforced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)
    storage = LocalFilesystemStorage(tmp_path / "depth-len-storage")

    deep_name = "/".join([f"d{i}" for i in range(32)] + ["file.txt"])  # 33 components > 32
    deep_tar = _make_tarball(
        tmp_path,
        [("ok.txt", b"ok", "file"), (deep_name, b"deep", "file")],
        filename="deep.tar.gz",
    )
    with open(deep_tar, "rb") as f:
        with pytest.raises(TarSafetyError, match="Tarball entry path too deep"):
            storage.extract_tarball("deep_slug", f)
    assert not (storage._root / "assets" / "deep_slug").exists()

    long_name = ("a" * 60 + "/") * 17 + "file.txt"  # > 1024 chars, depth 18 <= 32
    assert len(long_name) > 1024
    long_tar = _make_tarball(
        tmp_path,
        [("ok.txt", b"ok", "file"), (long_name, b"long", "file")],
        filename="long.tar.gz",
    )
    with open(long_tar, "rb") as f:
        with pytest.raises(TarSafetyError, match="Tarball entry name too long"):
            storage.extract_tarball("long_slug", f)
    assert not (storage._root / "assets" / "long_slug").exists()


def test_server_config_validates_positive_int_caps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from seeknal.report_server.config import (
        DEFAULT_MAX_MEMBER_NAME_LENGTH,
        DEFAULT_MAX_PATH_DEPTH,
        DEFAULT_MAX_TAR_MEMBERS,
        DEFAULT_MAX_UNPACKED_BYTES,
        DEFAULT_MAX_UPLOAD_BYTES,
        ServerConfig,
    )

    monkeypatch.setattr("seeknal.report_server.config.is_insecure_path", lambda _p: False)
    cfg = ServerConfig(data_dir=tmp_path / "cfg-data")
    assert cfg.max_upload_bytes == DEFAULT_MAX_UPLOAD_BYTES == 100 * 1024 * 1024
    assert cfg.max_tar_members == DEFAULT_MAX_TAR_MEMBERS == 10_000
    assert cfg.max_unpacked_bytes == DEFAULT_MAX_UNPACKED_BYTES == 512 * 1024 * 1024
    assert cfg.max_path_depth == DEFAULT_MAX_PATH_DEPTH == 32
    assert cfg.max_member_name_length == DEFAULT_MAX_MEMBER_NAME_LENGTH == 1024
    cfg.validate()

    for field_name in (
        "max_upload_bytes",
        "max_tar_members",
        "max_unpacked_bytes",
        "max_path_depth",
        "max_member_name_length",
    ):
        for bad_val in (0, -5, True, "100"):
            bad_cfg = ServerConfig(data_dir=tmp_path / "cfg-data", **{field_name: bad_val})  # type: ignore[arg-type]
            with pytest.raises(ValueError, match=field_name):
                bad_cfg.validate()

    monkeypatch.setenv("SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS", "not-an-int")
    with pytest.raises(ValueError, match="SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS"):
        ServerConfig.from_env()

    monkeypatch.setenv("SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS", "0")
    with pytest.raises(ValueError, match="SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS"):
        ServerConfig.from_env()


