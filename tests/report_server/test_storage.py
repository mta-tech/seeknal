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
        DEFAULT_MAX_CONCURRENT_PUBLISHES,
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
    assert cfg.max_concurrent_publishes == DEFAULT_MAX_CONCURRENT_PUBLISHES == 4
    cfg.validate()

    for field_name in (
        "max_upload_bytes",
        "max_tar_members",
        "max_unpacked_bytes",
        "max_path_depth",
        "max_member_name_length",
        "max_concurrent_publishes",
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


def _pax_record(key: str, value: str) -> bytes:
    body = f" {key}={value}\n".encode("utf-8")
    length = len(str(len(body))) + len(body)
    if len(str(length)) + len(body) != length:
        length += 1
    return f"{length}".encode("ascii") + body


def _build_old_gnu_sparse_tar_gz(out_path: Path, *, num_extended_blocks: int = 8) -> Path:
    import gzip

    first = tarfile.TarInfo(name="innocent.txt")
    first_data = b"ok"
    first.size = len(first_data)
    first_block = first.tobuf(tarfile.GNU_FORMAT) + first_data.ljust(512, b"\x00")

    sparse_info = tarfile.TarInfo(name="sparse_old.bin")
    sparse_info.type = tarfile.GNUTYPE_SPARSE
    sparse_info.size = 512
    raw = bytearray(sparse_info.tobuf(tarfile.GNU_FORMAT))
    # Populate 4 inline sparse descriptors at offset 386, set isextended=1 at 482, origsize at 483
    for i in range(4):
        pos = 386 + i * 24
        raw[pos : pos + 12] = tarfile.itn(i * 4096 + 1, 12, tarfile.GNU_FORMAT)
        raw[pos + 12 : pos + 24] = tarfile.itn(512, 12, tarfile.GNU_FORMAT)
    raw[482] = 1
    raw[483:495] = tarfile.itn(1024 * 1024, 12, tarfile.GNU_FORMAT)
    raw[148:156] = b"        "
    chksum = tarfile.calc_chksums(bytes(raw))[0]
    raw[148:156] = f"{chksum:06o}\x00 ".encode("ascii")

    ext_blocks = bytearray()
    for blk_idx in range(num_extended_blocks):
        ext = bytearray(512)
        for j in range(21):
            pos = j * 24
            ext[pos : pos + 12] = tarfile.itn((blk_idx * 21 + j + 5) * 4096, 12, tarfile.GNU_FORMAT)
            ext[pos + 12 : pos + 24] = tarfile.itn(512, 12, tarfile.GNU_FORMAT)
        ext[504] = 1 if blk_idx < num_extended_blocks - 1 else 0
        ext_blocks.extend(ext)

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(first_block)
        gz.write(bytes(raw))
        gz.write(bytes(ext_blocks))
        gz.write(b"x" * 512)
        gz.write(b"\x00" * 1024)
    return out_path


def _build_pax_gnu_sparse_01_tar_gz(out_path: Path) -> Path:
    import gzip

    first = tarfile.TarInfo(name="innocent.txt")
    first_data = b"ok"
    first.size = len(first_data)
    first_block = first.tobuf(tarfile.PAX_FORMAT) + first_data.ljust(512, b"\x00")

    sparse_map = ",".join(f"{i * 1024},512" for i in range(64))
    pax_payload = (
        _pax_record("GNU.sparse.size", "65536")
        + _pax_record("GNU.sparse.numblocks", "64")
        + _pax_record("GNU.sparse.name", "sparse01.bin")
        + _pax_record("GNU.sparse.map", sparse_map)
    )
    pax_info = tarfile.TarInfo(name="PaxHeader/sparse01.bin")
    pax_info.type = tarfile.XHDTYPE
    pax_info.size = len(pax_payload)
    pax_blocks = pax_info.tobuf(tarfile.PAX_FORMAT) + pax_payload.ljust(
        ((len(pax_payload) + 511) // 512) * 512, b"\x00"
    )

    reg_info = tarfile.TarInfo(name="sparse01.bin")
    reg_info.type = tarfile.REGTYPE
    reg_info.size = 512
    reg_blocks = reg_info.tobuf(tarfile.PAX_FORMAT) + (b"y" * 512)

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(first_block)
        gz.write(pax_blocks)
        gz.write(reg_blocks)
        gz.write(b"\x00" * 1024)
    return out_path


def _build_pax_gnu_sparse_10_tar_gz(out_path: Path) -> Path:
    import gzip

    first = tarfile.TarInfo(name="innocent.txt")
    first_data = b"ok"
    first.size = len(first_data)
    first_block = first.tobuf(tarfile.PAX_FORMAT) + first_data.ljust(512, b"\x00")

    pax_payload = (
        _pax_record("GNU.sparse.major", "1")
        + _pax_record("GNU.sparse.minor", "0")
        + _pax_record("GNU.sparse.name", "sparse10.bin")
        + _pax_record("GNU.sparse.realsize", "1048576")
    )
    pax_info = tarfile.TarInfo(name="PaxHeader/sparse10.bin")
    pax_info.type = tarfile.XHDTYPE
    pax_info.size = len(pax_payload)
    pax_blocks = pax_info.tobuf(tarfile.PAX_FORMAT) + pax_payload.ljust(
        ((len(pax_payload) + 511) // 512) * 512, b"\x00"
    )

    # GNU sparse 1.0 data block starts with number of entries followed by offset/numbytes lines
    sparse_map_block = (b"256\n" + (b"1\n1\n" * 256)).ljust(2048, b"\x00")
    reg_info = tarfile.TarInfo(name="sparse10.bin")
    reg_info.type = tarfile.REGTYPE
    reg_info.size = len(sparse_map_block)
    reg_blocks = reg_info.tobuf(tarfile.PAX_FORMAT) + sparse_map_block

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(first_block)
        gz.write(pax_blocks)
        gz.write(reg_blocks)
        gz.write(b"\x00" * 1024)
    return out_path


def _build_pax_global_header_tar_gz(out_path: Path) -> Path:
    import gzip

    pax_payload = _pax_record("path", "global_override.txt")
    g_info = tarfile.TarInfo(name="PaxHeader/GlobalHead")
    g_info.type = tarfile.XGLTYPE
    g_info.size = len(pax_payload)
    g_blocks = g_info.tobuf(tarfile.PAX_FORMAT) + pax_payload.ljust(512, b"\x00")

    reg = tarfile.TarInfo(name="index.html")
    reg_data = b"<html>ok</html>"
    reg.size = len(reg_data)
    reg_blocks = reg.tobuf(tarfile.PAX_FORMAT) + reg_data.ljust(512, b"\x00")

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(g_blocks)
        gz.write(reg_blocks)
        gz.write(b"\x00" * 1024)
    return out_path


def _build_gnu_longlink_tar_gz(out_path: Path) -> Path:
    import gzip

    longlink_payload = b"target_" + (b"a" * 120) + b"\x00"
    k_info = tarfile.TarInfo(name="././@LongLink")
    k_info.type = tarfile.GNUTYPE_LONGLINK
    k_info.size = len(longlink_payload)
    k_blocks = k_info.tobuf(tarfile.GNU_FORMAT) + longlink_payload.ljust(512, b"\x00")

    reg = tarfile.TarInfo(name="index.html")
    reg_data = b"<html>ok</html>"
    reg.size = len(reg_data)
    reg_blocks = reg.tobuf(tarfile.GNU_FORMAT) + reg_data.ljust(512, b"\x00")

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(k_blocks)
        gz.write(reg_blocks)
        gz.write(b"\x00" * 1024)
    return out_path


def _build_pax_unknown_key_tar_gz(out_path: Path, *, key: str = "foo.bar", value: str = "x") -> Path:
    import gzip

    pax_payload = _pax_record(key, value)
    x_info = tarfile.TarInfo(name="PaxHeader/index.html")
    x_info.type = tarfile.XHDTYPE
    x_info.size = len(pax_payload)
    x_blocks = x_info.tobuf(tarfile.PAX_FORMAT) + pax_payload.ljust(512, b"\x00")

    reg = tarfile.TarInfo(name="index.html")
    reg_data = b"<html>ok</html>"
    reg.size = len(reg_data)
    reg_blocks = reg.tobuf(tarfile.PAX_FORMAT) + reg_data.ljust(512, b"\x00")

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(x_blocks)
        gz.write(reg_blocks)
        gz.write(b"\x00" * 1024)
    return out_path


def _build_pax_hdrcharset_tar_gz(out_path: Path) -> Path:
    return _build_pax_unknown_key_tar_gz(out_path, key="hdrcharset", value="BINARY")


def _build_pax_linkpath_tar_gz(out_path: Path) -> Path:
    return _build_pax_unknown_key_tar_gz(out_path, key="linkpath", value="target.html")


def _build_conttype_tar_gz(out_path: Path) -> Path:
    import gzip

    first = tarfile.TarInfo(name="innocent.txt")
    first_data = b"ok"
    first.size = len(first_data)
    first_block = first.tobuf(tarfile.GNU_FORMAT) + first_data.ljust(512, b"\x00")

    c_info = tarfile.TarInfo(name="contiguous.bin")
    c_info.type = tarfile.CONTTYPE
    c_data = b"1234"
    c_info.size = len(c_data)
    c_blocks = c_info.tobuf(tarfile.GNU_FORMAT) + c_data.ljust(512, b"\x00")

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(first_block)
        gz.write(c_blocks)
        gz.write(b"\x00" * 1024)
    return out_path


def _build_chained_extended_headers_tar_gz(out_path: Path, *, count: int = 9) -> Path:
    import gzip

    long_payload = b"chained_long_name.html\x00"
    l_info = tarfile.TarInfo(name="././@LongLink")
    l_info.type = tarfile.GNUTYPE_LONGNAME
    l_info.size = len(long_payload)
    l_block = l_info.tobuf(tarfile.GNU_FORMAT) + long_payload.ljust(512, b"\x00")

    reg = tarfile.TarInfo(name="placeholder.html")
    reg_data = b"<html>chained</html>"
    reg.size = len(reg_data)
    reg_blocks = reg.tobuf(tarfile.GNU_FORMAT) + reg_data.ljust(512, b"\x00")

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        for _ in range(count):
            gz.write(l_block)
        gz.write(reg_blocks)
        gz.write(b"\x00" * 1024)
    return out_path


@pytest.mark.parametrize(
    "case_name,builder,expected_error",
    [
        ("global_pax_g", _build_pax_global_header_tar_gz, "Tarball entry type not allowed"),
        ("gnu_longlink_K", _build_gnu_longlink_tar_gz, "Tarball entry type not allowed"),
        ("pax_unknown_key", _build_pax_unknown_key_tar_gz, "Tarball pax header key not allowed"),
        ("pax_linkpath", _build_pax_linkpath_tar_gz, "Tarball pax header key not allowed"),
        ("pax_hdrcharset", _build_pax_hdrcharset_tar_gz, "Tarball entry has a non-UTF-8 member name"),
        ("pax_gnu_sparse_map", _build_pax_gnu_sparse_01_tar_gz, "Tarball pax header key not allowed"),
        ("old_gnu_sparse_S", _build_old_gnu_sparse_tar_gz, "Tarball entry type not allowed"),
        ("pax_gnu_sparse_10", _build_pax_gnu_sparse_10_tar_gz, "Tarball pax header key not allowed"),
        (
            "chain_of_9_extended_headers",
            lambda p: _build_chained_extended_headers_tar_gz(p, count=9),
            "Tarball entry header too large",
        ),
        (
            "chain_of_300_L_headers",
            lambda p: _build_chained_extended_headers_tar_gz(p, count=300),
            "Tarball entry header too large",
        ),
        ("conttype_7", _build_conttype_tar_gz, "Tarball entry type not allowed"),
    ],
)
def test_header_allowlist_and_chain_caps_rejected_before_parsing_with_bounded_memory(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
    case_name: str,
    builder,
    expected_error: str,
) -> None:
    import tracemalloc

    archive = builder(tmp_path / f"{case_name}.tar.gz")
    slug = f"allowlist_{case_name}"

    tracemalloc.start()
    try:
        with open(archive, "rb") as f:
            with pytest.raises(TarSafetyError, match=expected_error):
                test_storage.extract_tarball(slug, f)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak_bytes < 16 * 1024 * 1024, f"{case_name} peak memory {peak_bytes} exceeded 16 MiB"
    assert not (test_storage._root / "assets" / slug).exists()


def test_chain_of_8_extended_headers_accepted(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
) -> None:
    archive = _build_chained_extended_headers_tar_gz(tmp_path / "chain_8.tar.gz", count=8)
    with open(archive, "rb") as f:
        test_storage.extract_tarball("chain_8_ok", f)

    mount = test_storage.local_mount_path("chain_8_ok")
    assert mount is not None
    assert (mount / "chained_long_name.html").read_bytes() == b"<html>chained</html>"


def test_seeknal_packager_and_gnu_tar_longname_archives_accepted(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
) -> None:
    from seeknal.publish.packager import package_build

    # 1. Archive built by seeknal's own packager (package_build)
    build_dir = tmp_path / "packager_build"
    (build_dir / "_app" / "immutable").mkdir(parents=True)
    (build_dir / "index.html").write_text("<html><body>Packaged</body></html>", encoding="utf-8")
    long_asset_rel = "_app/immutable/" + ("chunk_" * 20) + "entry.js"  # > 100 chars
    (build_dir / long_asset_rel).write_text("console.log('packaged');", encoding="utf-8")

    packaged_tar = package_build(build_dir)
    try:
        with open(packaged_tar, "rb") as f:
            test_storage.extract_tarball("packaged_ok", f)
    finally:
        packaged_tar.unlink(missing_ok=True)

    mount_pkg = test_storage.local_mount_path("packaged_ok")
    assert mount_pkg is not None
    assert (mount_pkg / "index.html").exists()
    assert (mount_pkg / long_asset_rel).read_text(encoding="utf-8") == "console.log('packaged');"

    # 2. GNU tar format archive with a 150-character member name (uses GNUTYPE_LONGNAME 'L')
    gnu_long_name = "assets/" + ("a" * 140) + ".js"
    assert len(gnu_long_name) == 150
    gnu_tar_path = tmp_path / "gnu_150_char.tar.gz"
    with tarfile.open(gnu_tar_path, "w:gz", format=tarfile.GNU_FORMAT) as tf:
        info = tarfile.TarInfo(name=gnu_long_name)
        payload = b"console.log('gnu150');"
        info.size = len(payload)
        tf.addfile(info, io.BytesIO(payload))

    with open(gnu_tar_path, "rb") as f:
        test_storage.extract_tarball("gnu_long_ok", f)

    mount_gnu = test_storage.local_mount_path("gnu_long_ok")
    assert mount_gnu is not None
    assert (mount_gnu / gnu_long_name).read_bytes() == b"console.log('gnu150');"


def test_bsdtar_and_gnu_posix_ignored_pax_keys_accepted_and_dropped(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
) -> None:
    # (a) bsdtar-style archive with SCHILY.xattr.*, LIBARCHIVE.creationtime, and SCHILY.dev/ino keys
    bsdtar_path = tmp_path / "bsdtar_style.tar.gz"
    with tarfile.open(bsdtar_path, "w:gz", format=tarfile.PAX_FORMAT) as tf:
        info = tarfile.TarInfo(name="index.html")
        payload = b"<html><body>bsdtar</body></html>"
        info.size = len(payload)
        info.pax_headers = {
            "SCHILY.xattr.com.apple.provenance": "01020304",
            "SCHILY.acl.access": "user::rw-",
            "LIBARCHIVE.xattr.user.test": "val",
            "LIBARCHIVE.creationtime": "1700000000",
            "SCHILY.dev": "16777220",
            "SCHILY.ino": "1234567",
            "comment": "harmless pax comment",
        }
        tf.addfile(info, io.BytesIO(payload))

    observed_members_bsd: list[tarfile.TarInfo] = []
    orig_stream = test_storage._stream_and_validate_members

    def _recording_stream_bsd(tf, dest_real):
        members = orig_stream(tf, dest_real)
        observed_members_bsd.extend(members)
        return members

    with patch.object(test_storage, "_stream_and_validate_members", side_effect=_recording_stream_bsd):
        with open(bsdtar_path, "rb") as f:
            test_storage.extract_tarball("bsdtar_ok", f)

    mount_bsd = test_storage.local_mount_path("bsdtar_ok")
    assert mount_bsd is not None
    assert (mount_bsd / "index.html").read_bytes() == b"<html><body>bsdtar</body></html>"
    assert len(observed_members_bsd) == 1
    assert observed_members_bsd[0].pax_headers == {}

    # (b) GNU tar --format=posix style archive with atime and ctime
    gnu_posix_path = tmp_path / "gnu_posix_style.tar.gz"
    with tarfile.open(gnu_posix_path, "w:gz", format=tarfile.PAX_FORMAT) as tf:
        info = tarfile.TarInfo(name="report.html")
        payload = b"<html><body>gnu-posix</body></html>"
        info.size = len(payload)
        info.pax_headers = {
            "atime": "1700000000.123",
            "ctime": "1700000000.456",
            "mtime": "1700000000",
        }
        tf.addfile(info, io.BytesIO(payload))

    observed_members_posix: list[tarfile.TarInfo] = []

    def _recording_stream_posix(tf, dest_real):
        members = orig_stream(tf, dest_real)
        observed_members_posix.extend(members)
        return members

    with patch.object(test_storage, "_stream_and_validate_members", side_effect=_recording_stream_posix):
        with open(gnu_posix_path, "rb") as f:
            test_storage.extract_tarball("gnu_posix_ok", f)

    mount_posix = test_storage.local_mount_path("gnu_posix_ok")
    assert mount_posix is not None
    assert (mount_posix / "report.html").read_bytes() == b"<html><body>gnu-posix</body></html>"
    assert len(observed_members_posix) == 1
    assert "atime" not in observed_members_posix[0].pax_headers
    assert "ctime" not in observed_members_posix[0].pax_headers
    assert not hasattr(observed_members_posix[0], "atime")
    assert not hasattr(observed_members_posix[0], "ctime")


def test_200_long_named_members_summing_exactly_to_max_unpacked_bytes_accepted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)
    member_count = 200
    per_member_bytes = 64
    exact_max_unpacked = member_count * per_member_bytes

    storage = LocalFilesystemStorage(
        tmp_path / "near-cap-storage",
        max_tar_members=member_count,
        max_unpacked_bytes=exact_max_unpacked,
    )

    tar_path = tmp_path / "200_long_names.tar.gz"
    expected_names: list[str] = []
    with tarfile.open(tar_path, "w:gz", format=tarfile.PAX_FORMAT) as tf:
        for i in range(member_count):
            long_name = f"_app/immutable/chunks/report_module_{'a' * 90}_{i:04d}.js"
            assert len(long_name) > 100
            expected_names.append(long_name)
            payload = f"{i:04d}".encode("ascii").ljust(per_member_bytes, b"x")
            assert len(payload) == per_member_bytes
            info = tarfile.TarInfo(name=long_name)
            info.size = len(payload)
            tf.addfile(info, io.BytesIO(payload))

    with open(tar_path, "rb") as f:
        storage.extract_tarball("near_cap_200", f)

    mount = storage.local_mount_path("near_cap_200")
    assert mount is not None
    for i, name in enumerate(expected_names):
        p = mount / name
        assert p.is_file()
        assert len(p.read_bytes()) == per_member_bytes


def test_global_pax_header_refused_small_and_over_64k(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
) -> None:
    import gzip

    # Small 'g' global pax header is refused by the header-type allowlist (S1b)
    small_g = _build_pax_global_header_tar_gz(tmp_path / "small_global_g.tar.gz")
    with open(small_g, "rb") as f:
        with pytest.raises(TarSafetyError, match="Tarball entry type not allowed"):
            test_storage.extract_tarball("small_g_slug", f)
    assert not (test_storage._root / "assets" / "small_g_slug").exists()

    # Over-64 KiB 'g' global pax header is also refused before reading its body
    huge_g_path = tmp_path / "huge_global_g.tar.gz"
    g_info = tarfile.TarInfo(name="PaxHeader/GlobalHuge")
    g_info.type = tarfile.XGLTYPE
    g_info.size = 128 * 1024
    with gzip.open(huge_g_path, "wb", compresslevel=1) as gz:
        gz.write(g_info.tobuf(tarfile.PAX_FORMAT))
        gz.write(b"\x00" * (128 * 1024))
        gz.write(b"\x00" * 1024)

    with open(huge_g_path, "rb") as f:
        with pytest.raises(TarSafetyError):
            test_storage.extract_tarball("huge_g_slug", f)
    assert not (test_storage._root / "assets" / "huge_g_slug").exists()


def test_pax_extraction_and_sparse_refusal_without_private_tarfile_regex(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(tarfile, "_header_length_prefix_re", raising=False)
    assert not hasattr(tarfile, "_header_length_prefix_re")

    long_name = "reports/" + ("a" * 129) + "_summary.html"
    assert len(long_name) == 150
    non_ascii_name = "reports/données_financière_日本語.html"

    pax_tar = tmp_path / "no_private_regex_pax.tar.gz"
    with tarfile.open(pax_tar, "w:gz") as tf:
        for arcname, content in (
            (long_name, b"<html>long</html>"),
            (non_ascii_name, "<html>non-ascii</html>".encode("utf-8")),
        ):
            info = tarfile.TarInfo(name=arcname)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))

    with open(pax_tar, "rb") as f:
        test_storage.extract_tarball("no_priv_regex_ok", f)

    mount = test_storage.local_mount_path("no_priv_regex_ok")
    assert mount is not None
    assert (mount / long_name).read_bytes() == b"<html>long</html>"
    assert (mount / non_ascii_name).read_bytes() == b"<html>non-ascii</html>"

    # A GNU.sparse key is still refused when tarfile._header_length_prefix_re is absent
    sparse_tar = _build_pax_gnu_sparse_01_tar_gz(tmp_path / "no_priv_regex_sparse.tar.gz")
    with open(sparse_tar, "rb") as f:
        with pytest.raises(TarSafetyError, match="Tarball pax header key not allowed"):
            test_storage.extract_tarball("no_priv_regex_sparse", f)
    assert not (test_storage._root / "assets" / "no_priv_regex_sparse").exists()


def _build_pax_mtime_tar_gz(out_path: Path, mtime_str: str) -> Path:
    return _build_pax_unknown_key_tar_gz(out_path, key="mtime", value=mtime_str)


def _build_ustar_mtime_tar_gz(out_path: Path, raw_mtime_field: bytes) -> Path:
    import gzip

    info = tarfile.TarInfo(name="index.html")
    data = b"<html>mtime</html>"
    info.size = len(data)
    raw = bytearray(info.tobuf(tarfile.GNU_FORMAT))
    assert len(raw_mtime_field) == 12
    raw[136:148] = raw_mtime_field
    raw[148:156] = b"        "
    chksum = tarfile.calc_chksums(bytes(raw))[0]
    raw[148:156] = f"{chksum:06o}\x00 ".encode("ascii")

    with gzip.open(out_path, "wb", compresslevel=1) as gz:
        gz.write(bytes(raw))
        gz.write(data.ljust(512, b"\x00"))
        gz.write(b"\x00" * 1024)
    return out_path


@pytest.mark.parametrize(
    "bad_mtime",
    [
        "1e400",
        "1234567890123456789012345678901",
        "-1",
        "nan",
        "inf",
        "-inf",
        str((1 << 33) + 1),
    ],
)
def test_pax_out_of_range_and_non_finite_mtimes_refused(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
    bad_mtime: str,
) -> None:
    safe_label = bad_mtime.replace("-", "neg_")
    archive = _build_pax_mtime_tar_gz(tmp_path / f"pax_mtime_{safe_label}.tar.gz", bad_mtime)
    slug = f"mtime_{safe_label}"
    with open(archive, "rb") as f:
        with pytest.raises(TarSafetyError, match="Tarball entry has an invalid mtime"):
            test_storage.extract_tarball(slug, f)
    assert not (test_storage._root / "assets" / slug).exists()


def test_ustar_out_of_range_mtimes_refused_and_normal_float_mtime_accepted(
    test_storage: LocalFilesystemStorage,
    tmp_path: Path,
) -> None:
    # Negative base-256 GNU mtime (-1) and > 2**33 base-256 GNU mtime in the 12-byte header field
    neg_ustar = _build_ustar_mtime_tar_gz(
        tmp_path / "ustar_neg_mtime.tar.gz",
        tarfile.itn(-1, 12, tarfile.GNU_FORMAT),
    )
    over_ustar = _build_ustar_mtime_tar_gz(
        tmp_path / "ustar_over_mtime.tar.gz",
        tarfile.itn((1 << 33) + 100, 12, tarfile.GNU_FORMAT),
    )
    for idx, archive in enumerate((neg_ustar, over_ustar)):
        slug = f"ustar_bad_mtime_{idx}"
        with open(archive, "rb") as f:
            with pytest.raises(TarSafetyError, match="Tarball entry has an invalid mtime"):
                test_storage.extract_tarball(slug, f)
        assert not (test_storage._root / "assets" / slug).exists()

    # Normal float mtime in pax header is accepted
    ok_float_tar = _build_pax_mtime_tar_gz(
        tmp_path / "pax_float_mtime_ok.tar.gz",
        "1700000000.125",
    )
    with open(ok_float_tar, "rb") as f:
        test_storage.extract_tarball("pax_float_mtime_ok", f)
    mount = test_storage.local_mount_path("pax_float_mtime_ok")
    assert mount is not None
    assert (mount / "index.html").read_bytes() == b"<html>ok</html>"






