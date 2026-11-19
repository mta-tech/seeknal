from __future__ import annotations

import os
import shutil
import sys
import tarfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO, Optional

from seeknal.utils.path_security import is_insecure_path


class TarSafetyError(ValueError):
    pass


def _validate_tar_member(member: tarfile.TarInfo, dest_real: str) -> None:
    name = member.name
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
    def __init__(self, root: Path) -> None:
        if is_insecure_path(str(root)):
            raise ValueError(
                f"Insecure storage root detected: '{root}'. "
                "Use a secure location such as ~/.seeknal/report-server"
            )
        self._root = root
        (self._root / "assets").mkdir(parents=True, exist_ok=True)

    def _slug_dir(self, slug: str) -> Path:
        return self._root / "assets" / slug

    def extract_tarball(self, slug: str, fileobj: BinaryIO) -> str:
        dest = self._slug_dir(slug)
        dest.mkdir(parents=True, exist_ok=True)
        try:
            self._safe_extract(fileobj, dest)
            self._rewrite_absolute_paths(dest, slug)  # slug reserved for future basePath
        except Exception:
            shutil.rmtree(dest, ignore_errors=True)
            raise
        return f"assets/{slug}"

    def _safe_extract(self, fileobj: BinaryIO, dest: Path) -> None:
        with tarfile.open(fileobj=fileobj, mode="r:gz") as tf:
            # 1. Explicit version-independent check runs first before any member is extracted.
            dest_real = os.path.realpath(dest)
            members = tf.getmembers()
            for member in members:
                _validate_tar_member(member, dest_real)

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
            members = tf.getmembers()
            for member in members:
                _validate_tar_member(member, dest_real)
        for member in members:
            tf.extract(member, dest)

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
        full = self._slug_dir(slug) / relpath.lstrip("/")
        return open(full, "rb")  # noqa: WPS515

    def local_mount_path(self, slug: str) -> Optional[Path]:
        p = self._slug_dir(slug)
        return p if p.exists() else None

    def delete(self, slug: str) -> None:
        shutil.rmtree(self._slug_dir(slug), ignore_errors=True)

    def exists(self, slug: str) -> bool:
        return self._slug_dir(slug).exists()
