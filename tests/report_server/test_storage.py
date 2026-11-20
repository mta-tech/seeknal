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
    cfg = ServerConfig(data_dir=tmp_path / "cfg-data", api_keys=["test-key"])
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
            bad_cfg = ServerConfig(
                data_dir=tmp_path / "cfg-data",
                api_keys=["test-key"],
                **{field_name: bad_val},  # type: ignore[arg-type]
            )
            with pytest.raises(ValueError, match=field_name):
                bad_cfg.validate()

    monkeypatch.setenv("SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS", "not-an-int")
    with pytest.raises(ValueError, match="SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS"):
        ServerConfig.from_env()

    monkeypatch.setenv("SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS", "0")
    with pytest.raises(ValueError, match="SEEKNAL_REPORT_SERVER_MAX_TAR_MEMBERS"):
        ServerConfig.from_env()


def test_rejects_nul_byte_in_member_name(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
) -> None:
    nul_tar = tmp_path / "pax_nul.tar.gz"
    with tarfile.open(nul_tar, "w:gz", format=tarfile.PAX_FORMAT) as tf:
        info = tarfile.TarInfo(name="placeholder.txt")
        info.pax_headers["path"] = "bad\x00name.txt"
        data = b"payload"
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))

    with open(nul_tar, "rb") as f:
        with pytest.raises(TarSafetyError, match="Tarball entry name contains a NUL byte"):
            test_storage.extract_tarball("nul_slug", f)

    assert not (test_storage._root / "assets" / "nul_slug").exists()


def test_fallback_path_normalises_modes_and_cleans_up_on_failure(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import stat

    monkeypatch.delattr(tarfile, "data_filter", raising=False)

    # 1. Verify mode normalisation on success: dir -> 0o755, file -> 0o644
    mode_tar = tmp_path / "weird_modes.tar.gz"
    with tarfile.open(mode_tar, "w:gz") as tf:
        d_info = tarfile.TarInfo(name="d")
        d_info.type = tarfile.DIRTYPE
        d_info.mode = 0o555
        tf.addfile(d_info)

        f_info = tarfile.TarInfo(name="d/f.txt")
        f_data = b"hello"
        f_info.size = len(f_data)
        f_info.mode = 0o4777
        tf.addfile(f_info, io.BytesIO(f_data))

    with open(mode_tar, "rb") as f:
        test_storage.extract_tarball("modes_ok", f)

    mount = test_storage.local_mount_path("modes_ok")
    assert mount is not None
    assert stat.S_IMODE((mount / "d").stat().st_mode) == 0o755
    assert stat.S_IMODE((mount / "d" / "f.txt").stat().st_mode) == 0o644

    # 2. Verify cleanup leaves nothing behind when members d (0755), d/f, d (0555), and a failing member
    # are extracted on the fallback path (e.g. file "a" followed by "a/b" which fails mid-extraction).
    fail_tar = tmp_path / "readonly_dir_then_fail.tar.gz"
    with tarfile.open(fail_tar, "w:gz") as tf:
        d1 = tarfile.TarInfo(name="d")
        d1.type = tarfile.DIRTYPE
        d1.mode = 0o755
        tf.addfile(d1)

        df = tarfile.TarInfo(name="d/f")
        df_data = b"inside"
        df.size = len(df_data)
        df.mode = 0o444
        tf.addfile(df, io.BytesIO(df_data))

        d2 = tarfile.TarInfo(name="d")
        d2.type = tarfile.DIRTYPE
        d2.mode = 0o555
        tf.addfile(d2)

        # Failing sequence during extraction: regular file "a" followed by "a/b"
        a_file = tarfile.TarInfo(name="a")
        a_data = b"blocker"
        a_file.size = len(a_data)
        tf.addfile(a_file, io.BytesIO(a_data))

        ab_file = tarfile.TarInfo(name="a/b")
        ab_data = b"triggers_oserror"
        ab_file.size = len(ab_data)
        tf.addfile(ab_file, io.BytesIO(ab_data))

    with open(fail_tar, "rb") as f:
        with pytest.raises(OSError):
            test_storage.extract_tarball("fallback_cleanup_fail", f)

    assert not (test_storage._root / "assets" / "fallback_cleanup_fail").exists(), (
        "Failed extraction on fallback path must leave nothing behind"
    )


def test_open_asset_rejects_traversal_nul_backslash_absolute_and_resolved_escape(
    test_storage: LocalFilesystemStorage,
    honest_tarball: Path,
    tmp_path: Path,
) -> None:
    slug = "open_asset_guard"
    with open(honest_tarball, "rb") as f:
        test_storage.extract_tarball(slug, f)

    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("TOP_SECRET_MARKER_H1", encoding="utf-8")

    # Honest nested asset succeeds
    with test_storage.open_asset(slug, "_app/asset.js") as fh:
        assert b"console.log" in fh.read()

    # Create a post-extraction symlink inside slug dir pointing to outside secret_file
    mount = test_storage.local_mount_path(slug)
    assert mount is not None
    (mount / "leak_link.txt").symlink_to(secret_file)

    bad_paths = [
        "../secret.txt",
        "../../secret.txt",
        "../../../secret.txt",
        "_app/../../secret.txt",
        str(secret_file),
        "/etc/passwd",
        "C:\\Windows\\win.ini",
        "_app\\asset.js",
        "index.html\x00.txt",
        "leak_link.txt",
        "",
    ]
    for bad in bad_paths:
        with pytest.raises(FileNotFoundError) as exc_info:
            test_storage.open_asset(slug, bad)
        assert bad.strip() == "" or bad not in str(exc_info.value)


def _build_streamed_huge_gnu_longname_tar_gz(
    out_path: Path,
    *,
    claimed_bytes: int = 3 * 1024 * 1024 * 1024,
    streamed_bytes: int = 64 * 1024 * 1024,
) -> Path:
    import gzip

    first = tarfile.TarInfo(name="innocent.txt")
    first_data = b"ok"
    first.size = len(first_data)
    first_block = first.tobuf(tarfile.GNU_FORMAT) + first_data.ljust(512, b"\x00")

    long_info = tarfile.TarInfo(name="././@LongLink")
    long_info.type = tarfile.GNUTYPE_LONGNAME
    long_info.size = claimed_bytes
    long_header = long_info.tobuf(tarfile.GNU_FORMAT)

    zero_chunk = b"\x00" * (1024 * 1024)
    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(first_block)
        gz.write(long_header)
        remaining = streamed_bytes
        while remaining > 0:
            step = min(len(zero_chunk), remaining)
            gz.write(zero_chunk[:step])
            remaining -= step
    return out_path


def _build_streamed_huge_pax_comment_tar_gz(
    out_path: Path,
    *,
    claimed_bytes: int = 3 * 1024 * 1024 * 1024,
    streamed_bytes: int = 64 * 1024 * 1024,
) -> Path:
    import gzip

    first = tarfile.TarInfo(name="innocent.txt")
    first_data = b"ok"
    first.size = len(first_data)
    first_block = first.tobuf(tarfile.PAX_FORMAT) + first_data.ljust(512, b"\x00")

    pax_info = tarfile.TarInfo(name="PaxHeader/huge")
    pax_info.type = tarfile.XHDTYPE
    pax_info.size = claimed_bytes
    pax_header = pax_info.tobuf(tarfile.PAX_FORMAT)

    record_prefix = f"{claimed_bytes} comment=".encode("ascii")
    zero_chunk = b"\x00" * (1024 * 1024)
    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(first_block)
        gz.write(pax_header)
        gz.write(record_prefix)
        remaining = streamed_bytes - len(record_prefix)
        while remaining > 0:
            step = min(len(zero_chunk), remaining)
            gz.write(zero_chunk[:step])
            remaining -= step
    return out_path


def test_huge_gnu_longname_and_pax_headers_rejected_with_bounded_memory(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
) -> None:
    import tracemalloc

    gnu_tar = _build_streamed_huge_gnu_longname_tar_gz(tmp_path / "huge_gnulong.tar.gz")
    pax_tar = _build_streamed_huge_pax_comment_tar_gz(tmp_path / "huge_pax.tar.gz")

    for slug, archive in (("huge_gnu_slug", gnu_tar), ("huge_pax_slug", pax_tar)):
        tracemalloc.start()
        try:
            with open(archive, "rb") as f:
                with pytest.raises(TarSafetyError, match="Tarball entry header too large"):
                    test_storage.extract_tarball(slug, f)
            _, peak_bytes = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        assert peak_bytes < 64 * 1024 * 1024, f"{slug} peak memory {peak_bytes} exceeded 64 MiB"
        assert not (test_storage._root / "assets" / slug).exists()


def test_position_bounded_reader_enforces_position_and_allows_two_pass_rewind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from seeknal.report_server.storage import _PositionBoundedReader

    raw = io.BytesIO(b"0123456789abcdef")
    reader = _PositionBoundedReader(raw, max_position=10)

    assert reader.read(6) == b"012345"
    assert reader.tell() == 6

    # Reading past limit raises BEFORE advancing position
    with pytest.raises(TarSafetyError, match="Tarball uncompressed size exceeds limit"):
        reader.read(5)
    assert reader.tell() == 6

    # Unbounded read (n < 0) raises BEFORE reading
    with pytest.raises(TarSafetyError, match="Tarball uncompressed size exceeds limit"):
        reader.read(-1)
    assert reader.tell() == 6

    # Seeking past limit raises BEFORE seeking
    with pytest.raises(TarSafetyError, match="Tarball uncompressed size exceeds limit"):
        reader.seek(11)
    assert reader.tell() == 6

    # Rewinding to 0 and reading again does not double-count against the position ceiling
    assert reader.seek(0) == 0
    assert reader.read(10) == b"0123456789"

    # End-to-end two-pass extraction near max_unpacked_bytes succeeds without double-counting
    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)
    payload = b"x" * 4000
    tight_storage = LocalFilesystemStorage(
        tmp_path / "tight-pos-storage",
        max_unpacked_bytes=4096,
        max_tar_members=2,
    )
    near_limit_tar = _make_tarball(
        tmp_path,
        [("index.html", payload, "file")],
        filename="near_limit.tar.gz",
    )
    with open(near_limit_tar, "rb") as f:
        tight_storage.extract_tarball("tight_ok", f)
    mount = tight_storage.local_mount_path("tight_ok")
    assert mount is not None
    assert (mount / "index.html").read_bytes() == payload






