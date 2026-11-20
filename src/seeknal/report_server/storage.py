from __future__ import annotations

import gzip
import os
import shutil
import sys
import tarfile
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
TAR_MEMBER_HEADER_OVERHEAD_BYTES = 1536


class TarSafetyError(ValueError):
    pass


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


class _BoundedTarInfo(tarfile.TarInfo):
    """TarInfo subclass that caps pax ('x'/'g') and GNU longname/longlink ('L'/'K') headers before reading."""

    def _proc_gnulong(self, tarfile: tarfile.TarFile) -> tarfile.TarInfo:  # type: ignore[override]
        if self.size < 0 or self.size > MAX_TAR_HEADER_BYTES:
            raise TarSafetyError("Tarball entry header too large")
        return super()._proc_gnulong(tarfile)  # type: ignore[misc]

    def _proc_pax(self, tarfile: tarfile.TarFile) -> tarfile.TarInfo:  # type: ignore[override]
        if self.size < 0 or self.size > MAX_TAR_HEADER_BYTES:
            raise TarSafetyError("Tarball entry header too large")
        return super()._proc_pax(tarfile)  # type: ignore[misc]


def _validate_tar_member(
    member: tarfile.TarInfo,
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
        tf: tarfile.TarFile,
        dest_real: str,
    ) -> list[tarfile.TarInfo]:
        members: list[tarfile.TarInfo] = []
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
        )
        with gzip.GzipFile(fileobj=fileobj, mode="rb") as gz_stream:
            reader = _PositionBoundedReader(gz_stream, max_position)
            with tarfile.open(fileobj=reader, mode="r:", tarinfo=_BoundedTarInfo) as tf:  # type: ignore[arg-type]
                # 1. Explicit version-independent check runs first before any member is extracted.
                dest_real = os.path.realpath(dest)
                members = self._stream_and_validate_members(tf, dest_real)

                # 2. Extract using data_filter when available (3.12+ and backports) as a second layer,
                # otherwise extract the validated members one by one.
                if hasattr(tarfile, "data_filter"):
                    tf.extractall(dest, members=members, filter="data")  # type: ignore[call-arg]
                else:
                    self._manual_safe_extract(tf, dest, members=members)

    def _manual_safe_extract(
        self,
        tf: tarfile.TarFile,
        dest: Path,
        members: Optional[list[tarfile.TarInfo]] = None,
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
