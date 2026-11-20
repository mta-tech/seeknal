from __future__ import annotations

import gzip
import math
import os
import re
import shutil
import sys
import tarfile as _tarfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO, Optional

from seeknal.report_server.config import (
    DEFAULT_MAX_MEMBER_NAME_LENGTH,
    DEFAULT_MAX_PATH_DEPTH,
    DEFAULT_MAX_TAR_MEMBERS,
    DEFAULT_MAX_UNPACKED_BYTES,
)
from seeknal.utils.path_security import is_insecure_path

MAX_TAR_HEADER_BYTES = 64 * 1024  # 64 KiB cap for any single pax or GNU longname/longlink header
MAX_CONSECUTIVE_EXTENDED_HEADERS = 8
MAX_TAR_MTIME = 1 << 33  # 2**33 seconds
TAR_MEMBER_HEADER_OVERHEAD_BYTES = 4096
TAR_FIXED_HEADER_SLACK_BYTES = 1024 * 1024  # 1 MiB fixed header slack

_ALLOWED_TAR_HEADER_TYPES: frozenset[bytes] = frozenset(
    {
        _tarfile.REGTYPE,
        _tarfile.AREGTYPE,
        _tarfile.DIRTYPE,
        _tarfile.XHDTYPE,
        _tarfile.GNUTYPE_LONGNAME,
    }
)
_EXTENDED_TAR_HEADER_TYPES: frozenset[bytes] = frozenset(
    {
        _tarfile.XHDTYPE,
        _tarfile.GNUTYPE_LONGNAME,
    }
)
_ALLOWED_PAX_HEADER_KEYS: frozenset[str] = frozenset(
    {
        "path",
        "size",
        "mtime",
        "uid",
        "gid",
        "uname",
        "gname",
    }
)
_ALLOWED_PAX_HEADER_RAW_KEYS: frozenset[bytes] = frozenset(
    k.encode("ascii") for k in _ALLOWED_PAX_HEADER_KEYS
)
_IGNORED_PAX_HEADER_KEYS: frozenset[str] = frozenset(
    {
        "atime",
        "ctime",
        "comment",
    }
)
_IGNORED_PAX_HEADER_RAW_KEYS: frozenset[bytes] = frozenset(
    k.encode("ascii") for k in _IGNORED_PAX_HEADER_KEYS
)
_IGNORED_PAX_HEADER_PREFIXES: tuple[str, ...] = ("SCHILY.", "LIBARCHIVE.")
_IGNORED_PAX_HEADER_RAW_PREFIXES: tuple[bytes, ...] = (b"SCHILY.", b"LIBARCHIVE.")
_PAX_LEN_RE = re.compile(rb"([0-9]{1,20}) ")


class TarSafetyError(ValueError):
    pass


def _validate_mtime_value(value: object) -> None:
    if isinstance(value, bool):
        raise TarSafetyError("Tarball entry has an invalid mtime")
    if isinstance(value, (bytes, bytearray)):
        try:
            text = bytes(value).decode("ascii")
        except UnicodeDecodeError:
            raise TarSafetyError("Tarball entry has an invalid mtime") from None
        stripped = text.strip()
        if not stripped:
            raise TarSafetyError("Tarball entry has an invalid mtime")
        try:
            parsed: int | float = int(stripped) if "." not in stripped and "e" not in stripped.lower() else float(stripped)
        except (ValueError, OverflowError):
            raise TarSafetyError("Tarball entry has an invalid mtime") from None
    elif isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            raise TarSafetyError("Tarball entry has an invalid mtime")
        try:
            parsed = int(stripped) if "." not in stripped and "e" not in stripped.lower() else float(stripped)
        except (ValueError, OverflowError):
            raise TarSafetyError("Tarball entry has an invalid mtime") from None
    elif isinstance(value, (int, float)):
        parsed = value
    else:
        raise TarSafetyError("Tarball entry has an invalid mtime")

    if isinstance(parsed, float) and not math.isfinite(parsed):
        raise TarSafetyError("Tarball entry has an invalid mtime")
    if parsed < 0 or parsed > MAX_TAR_MTIME:
        raise TarSafetyError("Tarball entry has an invalid mtime")


class _PositionBoundedReader:
    """File-like wrapper around a decompressed stream that enforces a position ceiling."""

    def __init__(self, raw: BinaryIO, max_position: int) -> None:
        self._raw = raw
        self._max_position = max_position

    def tell(self) -> int:
        return self._raw.tell()

    def seekable(self) -> bool:
        return True

    def read(self, n: int = -1) -> bytes:
        pos = self._raw.tell()
        if n is None or n < 0 or pos + n > self._max_position:
            raise TarSafetyError("Tarball uncompressed size exceeds limit")
        return self._raw.read(n)

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        if whence == os.SEEK_SET:
            target = offset
        elif whence == os.SEEK_CUR:
            target = self._raw.tell() + offset
        else:
            raise TarSafetyError("Tarball uncompressed size exceeds limit")
        if target < 0 or target > self._max_position:
            raise TarSafetyError("Tarball uncompressed size exceeds limit")
        return self._raw.seek(offset, whence)

    def close(self) -> None:
        self._raw.close()


class _BoundedTarInfo(_tarfile.TarInfo):
    """TarInfo subclass enforcing a header-type allowlist, pax key allowlist, 64 KiB header cap, and chain depth cap."""

    def _proc_member(self, tar: _tarfile.TarFile) -> _tarfile.TarInfo:  # type: ignore[override]
        if self.type not in _ALLOWED_TAR_HEADER_TYPES:
            raise TarSafetyError("Tarball entry type not allowed")
        _validate_mtime_value(self.mtime)
        if self.type in _EXTENDED_TAR_HEADER_TYPES:
            chain_depth = getattr(tar, "_extended_header_chain_depth", 0) + 1
            if chain_depth > MAX_CONSECUTIVE_EXTENDED_HEADERS:
                raise TarSafetyError("Tarball entry header too large")
            setattr(tar, "_extended_header_chain_depth", chain_depth)
        else:
            setattr(tar, "_extended_header_chain_depth", 0)
        parent_proc_member = getattr(super(), "_proc_member", None)
        if parent_proc_member is None:
            raise TarSafetyError("Tarball entry type not allowed")
        return parent_proc_member(tar)  # type: ignore[misc]

    def _proc_sparse(self, tar: _tarfile.TarFile) -> _tarfile.TarInfo:  # type: ignore[override]
        raise TarSafetyError("Tarball entry type not allowed")

    def _proc_gnusparse_00(self, next_info: _tarfile.TarInfo, raw_headers: object) -> None:  # type: ignore[override]
        raise TarSafetyError("Tarball entry type not allowed")

    def _proc_gnusparse_01(self, next_info: _tarfile.TarInfo, pax_headers: object) -> None:  # type: ignore[override]
        raise TarSafetyError("Tarball entry type not allowed")

    def _proc_gnusparse_10(self, next_info: _tarfile.TarInfo, pax_headers: object, tar: _tarfile.TarFile) -> None:  # type: ignore[override]
        raise TarSafetyError("Tarball entry type not allowed")

    def _proc_gnulong(self, tar: _tarfile.TarFile) -> _tarfile.TarInfo:  # type: ignore[override]
        if self.size < 0 or self.size > MAX_TAR_HEADER_BYTES:
            raise TarSafetyError("Tarball entry header too large")
        parent_proc_gnulong = getattr(super(), "_proc_gnulong", None)
        if parent_proc_gnulong is None:
            raise TarSafetyError("Tarball entry type not allowed")
        return parent_proc_gnulong(tar)  # type: ignore[misc]

    def _proc_pax(self, tar: _tarfile.TarFile) -> _tarfile.TarInfo:  # type: ignore[override]
        if self.size < 0 or self.size > MAX_TAR_HEADER_BYTES:
            raise TarSafetyError("Tarball entry header too large")
        header_buf = tar.fileobj.read(self._block(self.size))
        raw_headers = self._validate_and_parse_pax_records(header_buf)
        pax_headers = tar.pax_headers.copy()
        for raw_keyword, raw_value in raw_headers:
            keyword = raw_keyword.decode("ascii")
            try:
                value = raw_value.decode("utf-8", "strict")
            except UnicodeDecodeError:
                value = raw_value.decode(tar.encoding, tar.errors)
            if keyword in _ALLOWED_PAX_HEADER_KEYS:
                pax_headers[keyword] = value
        try:
            fromtarfile_fn = getattr(self, "_fromtarfile", None)
            if fromtarfile_fn is not None:
                next_info = fromtarfile_fn(tar, dircheck=False)
            else:
                next_info = self.fromtarfile(tar)
        except _tarfile.HeaderError as exc:
            raise _tarfile.SubsequentHeaderError(str(exc)) from None
        apply_pax_fn = getattr(next_info, "_apply_pax_info", None)
        if apply_pax_fn is not None:
            apply_pax_fn(pax_headers, tar.encoding, tar.errors)
        else:
            next_info.pax_headers = pax_headers.copy()
        next_info.offset = self.offset
        if "size" in pax_headers:
            if next_info.size < 0:
                raise TarSafetyError("Tarball uncompressed size exceeds limit")
            offset = next_info.offset_data
            if next_info.isreg() or next_info.type not in _tarfile.SUPPORTED_TYPES:
                offset += next_info._block(next_info.size)
            tar.offset = offset
        return next_info

    @staticmethod
    def _is_ignored_pax_key(key: str) -> bool:
        return key in _IGNORED_PAX_HEADER_KEYS or key.startswith(_IGNORED_PAX_HEADER_PREFIXES)

    @staticmethod
    def _is_ignored_pax_raw_key(raw_key: bytes) -> bool:
        return raw_key in _IGNORED_PAX_HEADER_RAW_KEYS or raw_key.startswith(
            _IGNORED_PAX_HEADER_RAW_PREFIXES
        )

    @classmethod
    def _validate_and_parse_pax_records(cls, buf: bytes) -> list[tuple[bytes, bytes]]:
        pos = 0
        records: list[tuple[bytes, bytes]] = []
        while len(buf) > pos and buf[pos] != 0x00:
            match = _PAX_LEN_RE.match(buf, pos)
            if not match:
                raise _tarfile.InvalidHeaderError("invalid header")
            try:
                length = int(match.group(1))
            except ValueError:
                raise _tarfile.InvalidHeaderError("invalid header") from None
            if length < 5 or pos + length > len(buf):
                raise _tarfile.InvalidHeaderError("invalid header")
            header_value_end_offset = match.start(1) + length - 1
            keyword_and_value = buf[match.end(1) + 1 : header_value_end_offset]
            raw_keyword, equals, raw_value = keyword_and_value.partition(b"=")
            if not raw_keyword or equals != b"=" or buf[header_value_end_offset] != 0x0A:
                raise _tarfile.InvalidHeaderError("invalid header")
            if raw_keyword == b"hdrcharset":
                raise TarSafetyError("Tarball entry has a non-UTF-8 member name")
            if (
                raw_keyword not in _ALLOWED_PAX_HEADER_RAW_KEYS
                and not cls._is_ignored_pax_raw_key(raw_keyword)
            ):
                raise TarSafetyError("Tarball pax header key not allowed")
            if raw_keyword == b"mtime":
                _validate_mtime_value(raw_value)
            records.append((raw_keyword, raw_value))
            pos += length
        return records


def _validate_tar_member(
    member: _tarfile.TarInfo,
    dest_real: str,
    *,
    max_path_depth: int = DEFAULT_MAX_PATH_DEPTH,
    max_member_name_length: int = DEFAULT_MAX_MEMBER_NAME_LENGTH,
) -> None:
    name = member.name
    if "\x00" in name:
        raise TarSafetyError("Tarball entry name contains a NUL byte")
    if len(name) > max_member_name_length:
        raise TarSafetyError("Tarball entry name too long")

    # Reject absolute paths (POSIX /..., Windows drive C:..., or UNC \\... / //...)
    if (
        os.path.isabs(name)
        or name.startswith(("/", "\\"))
        or (len(name) >= 2 and name[0].isalpha() and name[1] == ":")
    ):
        raise TarSafetyError(f"Tarball entry has absolute path: {name!r}")

    # Reject entries with '..' components (splitting on both POSIX and Windows separators)
    parts = [p for part in name.split("/") for p in part.split("\\")]
    if ".." in parts:
        raise TarSafetyError(f"Tarball entry contains '..': {name!r}")

    depth = sum(1 for p in parts if p and p != ".")
    if depth > max_path_depth:
        raise TarSafetyError("Tarball entry path too deep")

    # Reject symlinks and hardlinks
    if member.issym() or member.islnk():
        kind = "symlink" if member.issym() else "hardlink"
        raise TarSafetyError(f"Tarball entry is a {kind}: {name!r}")

    # Reject devices, FIFOs, or other special files (allow only regular files and directories)
    if (
        member.isdev()
        or member.ischr()
        or member.isblk()
        or member.isfifo()
        or not (member.isreg() or member.isdir())
    ):
        raise TarSafetyError(f"Tarball entry is a special or device file: {name!r}")

    if member.size < 0:
        raise TarSafetyError("Tarball uncompressed size exceeds limit")
    _validate_mtime_value(member.mtime)

    # Verify resolved path stays inside dest
    normalized_rel = name.replace("\\", os.sep)
    target = os.path.realpath(os.path.join(dest_real, normalized_rel))
    if not target.startswith(dest_real + os.sep) and target != dest_real:
        raise TarSafetyError(f"Tarball entry escapes destination: {name!r}")


class Storage(ABC):
    @abstractmethod
    def extract_tarball(self, slug: str, fileobj: BinaryIO) -> str:
        """Extract a gzipped tarball for slug. Returns asset_relpath."""

    @abstractmethod
    def open_asset(self, slug: str, relpath: str) -> BinaryIO:
        """Open an asset file for reading."""

    @abstractmethod
    def local_mount_path(self, slug: str) -> Optional[Path]:
        """Return the filesystem path for slug if FS-backed, else None."""

    @abstractmethod
    def delete(self, slug: str) -> None:
        """Delete all assets for slug."""

    @abstractmethod
    def exists(self, slug: str) -> bool:
        """Return True if assets for slug exist."""


class LocalFilesystemStorage(Storage):
    def __init__(
        self,
        root: Path,
        *,
        max_tar_members: int = DEFAULT_MAX_TAR_MEMBERS,
        max_unpacked_bytes: int = DEFAULT_MAX_UNPACKED_BYTES,
        max_path_depth: int = DEFAULT_MAX_PATH_DEPTH,
        max_member_name_length: int = DEFAULT_MAX_MEMBER_NAME_LENGTH,
    ) -> None:
        if is_insecure_path(str(root)):
            raise ValueError(
                f"Insecure storage root detected: '{root}'. "
                "Use a secure location such as ~/.seeknal/report-server"
            )
        self._root = root
        self._max_tar_members = max_tar_members
        self._max_unpacked_bytes = max_unpacked_bytes
        self._max_path_depth = max_path_depth
        self._max_member_name_length = max_member_name_length
        (self._root / "assets").mkdir(parents=True, exist_ok=True)

    def _slug_dir(self, slug: str) -> Path:
        return self._root / "assets" / slug

    def _cleanup_dest(self, dest: Path) -> None:
        if not dest.exists():
            return
        try:
            os.chmod(dest, 0o755)
        except OSError:
            pass
        for root_dir, dirs, files in os.walk(dest, topdown=True):
            for d in dirs:
                try:
                    os.chmod(os.path.join(root_dir, d), 0o755)
                except OSError:
                    pass
            for f in files:
                try:
                    os.chmod(os.path.join(root_dir, f), 0o644)
                except OSError:
                    pass
        shutil.rmtree(dest, ignore_errors=True)

    def extract_tarball(self, slug: str, fileobj: BinaryIO) -> str:
        dest = self._slug_dir(slug)
        dest.mkdir(parents=True, exist_ok=True)
        try:
            self._safe_extract(fileobj, dest)
            self._rewrite_absolute_paths(dest, slug)  # slug reserved for future basePath
        except Exception:
            self._cleanup_dest(dest)
            raise
        return f"assets/{slug}"

    def _stream_and_validate_members(
        self,
        tf: _tarfile.TarFile,
        dest_real: str,
    ) -> list[_tarfile.TarInfo]:
        members: list[_tarfile.TarInfo] = []
        member_count = 0
        total_unpacked = 0
        for member in tf:
            member_count += 1
            if member_count > self._max_tar_members:
                raise TarSafetyError("Tarball has too many members")
            _validate_tar_member(
                member,
                dest_real,
                max_path_depth=self._max_path_depth,
                max_member_name_length=self._max_member_name_length,
            )
            total_unpacked += member.size
            if total_unpacked > self._max_unpacked_bytes:
                raise TarSafetyError("Tarball uncompressed size exceeds limit")
            members.append(member)
        return members

    def _safe_extract(self, fileobj: BinaryIO, dest: Path) -> None:
        max_position = (
            self._max_unpacked_bytes
            + self._max_tar_members * TAR_MEMBER_HEADER_OVERHEAD_BYTES
            + TAR_FIXED_HEADER_SLACK_BYTES
        )
        with gzip.GzipFile(fileobj=fileobj, mode="rb") as gz_stream:
            reader = _PositionBoundedReader(gz_stream, max_position)
            with _tarfile.open(fileobj=reader, mode="r:", tarinfo=_BoundedTarInfo) as tf:  # type: ignore[arg-type]
                # 1. Explicit version-independent check runs first before any member is extracted.
                dest_real = os.path.realpath(dest)
                members = self._stream_and_validate_members(tf, dest_real)

                # 2. Extract using data_filter when available (3.12+ and backports) as a second layer,
                # otherwise extract the validated members one by one.
                if hasattr(_tarfile, "data_filter"):
                    tf.extractall(dest, members=members, filter="data")  # type: ignore[call-arg]
                else:
                    self._manual_safe_extract(tf, dest, members=members)

    def _manual_safe_extract(
        self,
        tf: _tarfile.TarFile,
        dest: Path,
        members: Optional[list[_tarfile.TarInfo]] = None,
    ) -> None:
        dest_real = os.path.realpath(dest)
        if members is None:
            members = self._stream_and_validate_members(tf, dest_real)
        for member in members:
            member.mode = 0o755 if member.isdir() else 0o644
            tf.extract(member, dest, set_attrs=False)
            target_path = dest / member.name
            try:
                os.chmod(target_path, member.mode)
            except OSError:
                pass

    def _rewrite_absolute_paths(self, dest: Path, slug: str) -> None:
        """Replace __SEEKNAL_SLUG__ placeholder with the actual slug.

        The Seeknal scaffolder writes evidence.config.yaml with
        deployment.basePath = /r/__SEEKNAL_SLUG__. Evidence's SvelteKit build
        then bakes this placeholder into all asset URLs, modulepreload tags,
        SvelteKit runtime base, and chunk references. At upload time, we
        replace the placeholder string with the actual server-allocated slug
        so the published report serves correctly under /r/{slug}/.

        This single substitution replaces what would otherwise be 4+ separate
        path rewrites (asset URLs, manifest paths, SvelteKit base, route
        prefix). All Evidence-build internals stay consistent because they
        all use the same placeholder at build time.
        """
        placeholder = "__SEEKNAL_SLUG__"
        for ext in ("*.html", "*.js", "*.css", "*.json"):
            for filepath in dest.rglob(ext):
                if not filepath.is_file():
                    continue
                try:
                    text = filepath.read_text(encoding="utf-8")
                    if placeholder not in text:
                        continue
                    filepath.write_text(text.replace(placeholder, slug), encoding="utf-8")
                except (UnicodeDecodeError, PermissionError, IsADirectoryError):
                    pass

    def open_asset(self, slug: str, relpath: str) -> BinaryIO:
        if (
            not relpath
            or "\x00" in relpath
            or "\\" in relpath
            or os.path.isabs(relpath)
            or relpath.startswith("/")
            or (len(relpath) >= 2 and relpath[0].isalpha() and relpath[1] == ":")
            or ".." in relpath.split("/")
        ):
            raise FileNotFoundError("Not found")
        slug_dir = self._slug_dir(slug)
        try:
            slug_dir_resolved = slug_dir.resolve()
            candidate = (slug_dir_resolved / relpath).resolve()
        except (OSError, ValueError) as exc:
            raise FileNotFoundError("Not found") from exc
        if not candidate.is_relative_to(slug_dir_resolved) or candidate == slug_dir_resolved:
            raise FileNotFoundError("Not found")
        if not candidate.exists() or candidate.is_dir() or not candidate.is_file():
            raise FileNotFoundError("Not found")
        return open(candidate, "rb")  # noqa: WPS515

    def local_mount_path(self, slug: str) -> Optional[Path]:
        p = self._slug_dir(slug)
        return p if p.exists() else None

    def delete(self, slug: str) -> None:
        shutil.rmtree(self._slug_dir(slug), ignore_errors=True)

    def exists(self, slug: str) -> bool:
        return self._slug_dir(slug).exists()
