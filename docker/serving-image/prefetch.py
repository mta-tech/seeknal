#!/usr/bin/env python3
"""Prefetch and hash-verify Serving wheels and DuckDB extensions for air-gapped builds."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import platform as py_platform
import re
import subprocess
import sys
import tomllib
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_EXT_NAME_RE = re.compile(r"^[a-z0-9_]+$")


class PrefetchError(RuntimeError):
    """Raised when prefetching, lock parsing, or artifact validation fails."""


class HashMismatchError(PrefetchError):
    """Raised when an artifact's SHA-256 digest does not match its lock entry."""


@dataclass(frozen=True)
class ExtensionEntry:
    """Pinned DuckDB extension artifact entry from extensions.lock.json."""

    platform: str
    name: str
    source_url: str
    gz_sha256: str
    raw_sha256: str
    gz_bytes: int | None = None

    def install_relpath(self, duckdb_version: str) -> Path:
        return Path(f"v{duckdb_version}") / self.platform / f"{self.name}.duckdb_extension"

    def vendor_relpath(self, duckdb_version: str) -> Path:
        return Path(f"v{duckdb_version}") / self.platform / f"{self.name}.duckdb_extension.gz"


@dataclass(frozen=True)
class ExtensionsManifest:
    """Parsed extensions.lock.json manifest."""

    duckdb_version: str
    extension_directory: str
    entries: tuple[ExtensionEntry, ...]


@dataclass(frozen=True)
class WheelArtifact:
    """Locked Python distribution artifact from uv.lock."""

    package_name: str
    version: str
    filename: str
    url: str
    sha256: str
    size: int | None = None


def normalize_pep503(name: str) -> str:
    """Normalize a Python distribution name per PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def compute_bytes_sha256(data: bytes) -> str:
    """Return lowercase hex SHA-256 of bytes."""
    return hashlib.sha256(data).hexdigest()


def compute_file_sha256(path: str | Path) -> str:
    """Return lowercase hex SHA-256 of a file on disk."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def detect_duckdb_platform(arch: str | None = None) -> str:
    """Normalize Docker TARGETARCH or machine architecture to DuckDB platform tag."""
    raw = (arch or "").strip().lower()
    if not raw:
        raw = py_platform.machine().strip().lower()

    mapping = {
        "amd64": "linux_amd64",
        "x86_64": "linux_amd64",
        "linux_amd64": "linux_amd64",
        "linux/amd64": "linux_amd64",
        "arm64": "linux_arm64",
        "aarch64": "linux_arm64",
        "linux_arm64": "linux_arm64",
        "linux/arm64": "linux_arm64",
    }
    resolved = mapping.get(raw)
    if resolved is None:
        raise PrefetchError(f"Unsupported DuckDB Linux platform architecture: {raw!r}")
    return resolved


def _validate_sha256_field(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise PrefetchError(f"Missing or non-string {label}")
    cleaned = value.strip().lower()
    if cleaned.startswith("sha256:"):
        cleaned = cleaned[len("sha256:") :]
    if not HEX64_RE.fullmatch(cleaned):
        raise PrefetchError(f"Invalid SHA-256 hex digest for {label}: {value!r}")
    return cleaned


def load_extensions_manifest(lock_path: str | Path) -> ExtensionsManifest:
    """Load and validate docker/serving-image/extensions.lock.json."""
    path = Path(lock_path)
    if not path.is_file():
        raise PrefetchError(f"Extensions lockfile not found: {path}")

    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise PrefetchError(f"Failed to parse extensions lockfile {path}: {exc}") from exc

    if not isinstance(raw, dict):
        raise PrefetchError(f"Extensions lockfile {path} must be a JSON object")

    duckdb_version = raw.get("duckdb_version")
    if not isinstance(duckdb_version, str) or not duckdb_version.strip():
        raise PrefetchError("extensions.lock.json missing non-empty 'duckdb_version'")
    duckdb_version = duckdb_version.strip()

    ext_dir = raw.get("extension_directory")
    if not isinstance(ext_dir, str) or not ext_dir.startswith("/"):
        raise PrefetchError(
            "extensions.lock.json 'extension_directory' must be an absolute path"
        )

    raw_entries = raw.get("extensions")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise PrefetchError("extensions.lock.json must contain a non-empty 'extensions' list")

    entries: list[ExtensionEntry] = []
    seen_keys: set[tuple[str, str]] = set()
    for idx, item in enumerate(raw_entries):
        if not isinstance(item, dict):
            raise PrefetchError(f"extensions[{idx}] must be an object")

        plat = detect_duckdb_platform(str(item.get("platform", "")))
        name = str(item.get("name", "")).strip()
        if not SAFE_EXT_NAME_RE.fullmatch(name):
            raise PrefetchError(f"Invalid extension name at extensions[{idx}]: {name!r}")

        key = (plat, name)
        if key in seen_keys:
            raise PrefetchError(f"Duplicate extension entry for {plat}/{name}")
        seen_keys.add(key)

        source_url = str(item.get("source_url", "")).strip()
        if not source_url.startswith("https://"):
            raise PrefetchError(
                f"Extension source_url for {plat}/{name} must use https://, got {source_url!r}"
            )

        gz_a = item.get("gz_sha256")
        gz_b = item.get("sha256")
        if gz_a is not None and gz_b is not None and str(gz_a).lower() != str(gz_b).lower():
            raise PrefetchError(
                f"Conflicting sha256 and gz_sha256 in extensions.lock.json for {plat}/{name}"
            )
        gz_sha256 = _validate_sha256_field(
            gz_a if gz_a is not None else gz_b,
            f"{plat}/{name}.gz_sha256",
        )

        raw_a = item.get("raw_sha256")
        raw_b = item.get("decompressed_sha256")
        if (
            raw_a is not None
            and raw_b is not None
            and str(raw_a).lower() != str(raw_b).lower()
        ):
            raise PrefetchError(
                f"Conflicting raw_sha256 and decompressed_sha256 for {plat}/{name}"
            )
        raw_sha256 = _validate_sha256_field(
            raw_a if raw_a is not None else raw_b,
            f"{plat}/{name}.raw_sha256",
        )

        gz_bytes_raw = item.get("gz_bytes")
        gz_bytes: int | None = None
        if gz_bytes_raw is not None:
            if isinstance(gz_bytes_raw, bool) or not isinstance(gz_bytes_raw, int) or gz_bytes_raw <= 0:
                raise PrefetchError(f"Invalid gz_bytes for {plat}/{name}: {gz_bytes_raw!r}")
            gz_bytes = gz_bytes_raw

        entries.append(
            ExtensionEntry(
                platform=plat,
                name=name,
                source_url=source_url,
                gz_sha256=gz_sha256,
                raw_sha256=raw_sha256,
                gz_bytes=gz_bytes,
            )
        )

    return ExtensionsManifest(
        duckdb_version=duckdb_version,
        extension_directory=ext_dir,
        entries=tuple(entries),
    )


def default_https_fetcher(url: str, *, timeout: float = 120.0) -> bytes:
    """Download bytes over HTTPS only."""
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https":
        raise PrefetchError(f"Refusing non-HTTPS artifact URL: {url!r}")
    req = urllib.request.Request(url, headers={"User-Agent": "seeknal-serving-prefetch/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 - scheme verified https
        return resp.read()


def verify_and_decompress_extension(gz_bytes: bytes, entry: ExtensionEntry) -> bytes:
    """Verify compressed size/SHA-256, gunzip, and verify decompressed SHA-256."""
    if entry.gz_bytes is not None and len(gz_bytes) != entry.gz_bytes:
        raise HashMismatchError(
            f"Compressed byte length mismatch for {entry.platform}/{entry.name}: "
            f"expected {entry.gz_bytes}, got {len(gz_bytes)}"
        )

    actual_gz_sha = compute_bytes_sha256(gz_bytes)
    if actual_gz_sha != entry.gz_sha256:
        raise HashMismatchError(
            f"Compressed SHA-256 mismatch for {entry.platform}/{entry.name}: "
            f"expected {entry.gz_sha256}, got {actual_gz_sha}"
        )

    try:
        raw_bytes = gzip.decompress(gz_bytes)
    except (OSError, EOFError) as exc:
        raise PrefetchError(
            f"Failed to gunzip extension {entry.platform}/{entry.name}: {exc}"
        ) from exc

    actual_raw_sha = compute_bytes_sha256(raw_bytes)
    if actual_raw_sha != entry.raw_sha256:
        raise HashMismatchError(
            f"Decompressed SHA-256 mismatch for {entry.platform}/{entry.name}: "
            f"expected {entry.raw_sha256}, got {actual_raw_sha}"
        )

    return raw_bytes


def parse_uv_lock_artifacts(
    uv_lock_path: str | Path,
) -> tuple[dict[str, str], dict[str, WheelArtifact], dict[str, dict]]:
    """Parse uv.lock into package versions, filename->artifact map, and raw package map."""
    path = Path(uv_lock_path)
    if not path.is_file():
        raise PrefetchError(f"uv.lock not found: {path}")

    try:
        with open(path, "rb") as handle:
            lock_data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PrefetchError(f"Failed to parse uv.lock at {path}: {exc}") from exc

    versions: dict[str, str] = {}
    artifacts: dict[str, WheelArtifact] = {}
    raw_packages: dict[str, dict] = {}

    for pkg in lock_data.get("package", []):
        name = normalize_pep503(pkg["name"])
        version = str(pkg["version"])
        versions[name] = version
        raw_packages[name] = pkg

        dist_items = list(pkg.get("wheels", []))
        sdist = pkg.get("sdist")
        if isinstance(sdist, dict) and "url" in sdist and "hash" in sdist:
            dist_items.append(sdist)

        for item in dist_items:
            url = str(item.get("url", ""))
            if not url:
                continue
            filename = urllib.parse.unquote(urllib.parse.urlparse(url).path.rsplit("/", 1)[-1])
            sha256 = _validate_sha256_field(item.get("hash"), f"{name}:{filename}")
            size = item.get("size") if isinstance(item.get("size"), int) else None
            art = WheelArtifact(
                package_name=name,
                version=version,
                filename=filename,
                url=url,
                sha256=sha256,
                size=size,
            )
            existing = artifacts.get(filename)
            if existing is not None and existing.sha256 != art.sha256:
                raise PrefetchError(f"Conflicting hashes in uv.lock for {filename}")
            artifacts[filename] = art

    return versions, artifacts, raw_packages


def _is_linux_compatible_marker(marker: str | None) -> bool:
    """Return False for Windows/macOS-only dependency markers in uv.lock."""
    if not marker:
        return True
    lowered = marker.lower()
    if "sys_platform == 'win32'" in lowered or "sys_platform == 'darwin'" in lowered:
        return False
    if "os_name == 'nt'" in lowered:
        return False
    return True


def _wheel_matches_linux_cp311(filename: str, platforms: set[str]) -> bool:
    """Check whether a wheel filename applies to CPython 3.11 on requested Linux arches."""
    if not filename.endswith(".whl"):
        return False
    stem = filename[:-4]
    parts = stem.split("-")
    if len(parts) < 5:
        return False
    py_tag, abi_tag, plat_tag = parts[-3], parts[-2], parts[-1]

    if py_tag in {"py3", "py2.py3"} and abi_tag == "none" and plat_tag == "any":
        return True

    py_ok = "cp311" in py_tag or ("py3" in py_tag and abi_tag == "none")
    abi_ok = abi_tag in {"cp311", "abi3", "none"}
    if not (py_ok and abi_ok):
        return False

    if "manylinux" not in plat_tag and not plat_tag.startswith("linux_"):
        return False
    if "musllinux" in plat_tag and "manylinux" not in plat_tag:
        return False

    want_amd64 = "linux_amd64" in platforms and "x86_64" in plat_tag
    want_arm64 = "linux_arm64" in platforms and "aarch64" in plat_tag
    return want_amd64 or want_arm64


def select_serving_wheels(
    uv_lock_path: str | Path,
    *,
    platforms: Iterable[str] = ("linux_amd64", "linux_arm64"),
) -> list[WheelArtifact]:
    """Select all locked wheels required for the serving group on CPython 3.11 Linux."""
    norm_platforms = {detect_duckdb_platform(p) for p in platforms}
    _, _, raw_packages = parse_uv_lock_artifacts(uv_lock_path)

    root_pkg = raw_packages.get("seeknal")
    if root_pkg is None:
        raise PrefetchError("Root package 'seeknal' not found in uv.lock")

    dev_deps = root_pkg.get("dev-dependencies", {})
    serving_roots = dev_deps.get("serving")
    if not isinstance(serving_roots, list) or not serving_roots:
        raise PrefetchError(
            "Dependency group 'serving' not found under seeknal in uv.lock"
        )

    closure: set[str] = set()
    queue: list[str] = []
    for dep in serving_roots:
        if _is_linux_compatible_marker(dep.get("marker")):
            dep_name = normalize_pep503(dep["name"])
            if dep_name not in closure:
                closure.add(dep_name)
                queue.append(dep_name)

    while queue:
        current = queue.pop(0)
        pkg = raw_packages.get(current)
        if pkg is None:
            raise PrefetchError(f"Package {current!r} referenced in closure missing from uv.lock")
        for dep in pkg.get("dependencies", []):
            if not _is_linux_compatible_marker(dep.get("marker")):
                continue
            dep_name = normalize_pep503(dep["name"])
            if dep_name not in closure:
                closure.add(dep_name)
                queue.append(dep_name)

    selected: list[WheelArtifact] = []
    for pkg_name in sorted(closure):
        pkg = raw_packages[pkg_name]
        version = str(pkg["version"])
        pkg_wheels: list[WheelArtifact] = []
        for wheel in pkg.get("wheels", []):
            url = str(wheel["url"])
            filename = urllib.parse.unquote(urllib.parse.urlparse(url).path.rsplit("/", 1)[-1])
            if not _wheel_matches_linux_cp311(filename, norm_platforms):
                continue
            sha256 = _validate_sha256_field(wheel.get("hash"), f"{pkg_name}:{filename}")
            size = wheel.get("size") if isinstance(wheel.get("size"), int) else None
            pkg_wheels.append(
                WheelArtifact(
                    package_name=pkg_name,
                    version=version,
                    filename=filename,
                    url=url,
                    sha256=sha256,
                    size=size,
                )
            )
        if not pkg_wheels:
            raise PrefetchError(
                f"No compatible CPython 3.11 Linux wheels found in uv.lock for {pkg_name}=={version}"
            )
        selected.extend(pkg_wheels)

    return selected


def verify_wheel_directory(
    wheels_dir: str | Path,
    uv_lock_path: str | Path,
) -> list[Path]:
    """Verify every wheel/sdist in wheels_dir matches uv.lock SHA-256 digests."""
    directory = Path(wheels_dir)
    if not directory.is_dir():
        raise PrefetchError(f"Vendor wheels directory not found: {directory}")

    _, artifacts, _ = parse_uv_lock_artifacts(uv_lock_path)
    files = sorted(
        p for p in directory.iterdir() if p.is_file() and not p.name.startswith(".")
    )
    if not files:
        raise PrefetchError(f"Vendor wheels directory is empty: {directory}")

    verified: list[Path] = []
    for file_path in files:
        expected = artifacts.get(file_path.name)
        if expected is None:
            raise PrefetchError(
                f"Unrecognized artifact in wheels directory not present in uv.lock: {file_path.name}"
            )
        if expected.size is not None:
            actual_size = file_path.stat().st_size
            if actual_size != expected.size:
                raise HashMismatchError(
                    f"Byte size mismatch for {file_path.name}: "
                    f"expected {expected.size}, got {actual_size}"
                )
        actual_sha = compute_file_sha256(file_path)
        if actual_sha != expected.sha256:
            raise HashMismatchError(
                f"SHA-256 mismatch for {file_path.name}: "
                f"expected {expected.sha256}, got {actual_sha}"
            )
        verified.append(file_path)

    return verified


def prefetch_wheels(
    vendor_dir: str | Path,
    *,
    uv_lock_path: str | Path = Path("uv.lock"),
    platforms: Iterable[str] = ("linux_amd64", "linux_arm64"),
    fetcher: Callable[[str], bytes] = default_https_fetcher,
    export_runner: Callable[[Path], None] | None = None,
    build_project_runner: Callable[[Path], None] | None = None,
    build_project_wheel: bool = True,
) -> list[Path]:
    """Prefetch hash-pinned requirements.txt, serving wheels, and local seeknal wheel."""
    vendor = Path(vendor_dir)
    wheels_dir = vendor / "wheels"
    wheels_dir.mkdir(parents=True, exist_ok=True)
    requirements_out = vendor / "requirements.txt"

    if export_runner is not None:
        export_runner(requirements_out)
    else:
        proc = subprocess.run(
            [
                "uv",
                "export",
                "--locked",
                "--only-group",
                "serving",
                "--no-emit-project",
                "--format",
                "requirements-txt",
                "-o",
                str(requirements_out),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise PrefetchError(f"uv export failed:\n{proc.stderr.strip()}")

    if not requirements_out.is_file() or "--hash=sha256:" not in requirements_out.read_text(
        encoding="utf-8"
    ):
        raise PrefetchError(
            f"Exported requirements file {requirements_out} missing required --hash=sha256: pins"
        )

    selected_wheels = select_serving_wheels(uv_lock_path, platforms=platforms)
    written: list[Path] = []
    for art in selected_wheels:
        data = fetcher(art.url)
        if art.size is not None and len(data) != art.size:
            raise HashMismatchError(
                f"Downloaded wheel size mismatch for {art.filename}: "
                f"expected {art.size}, got {len(data)}"
            )
        actual_sha = compute_bytes_sha256(data)
        if actual_sha != art.sha256:
            raise HashMismatchError(
                f"Downloaded wheel SHA-256 mismatch for {art.filename}: "
                f"expected {art.sha256}, got {actual_sha}"
            )
        dest = wheels_dir / art.filename
        dest.write_bytes(data)
        written.append(dest)

    verify_wheel_directory(wheels_dir, uv_lock_path)

    if build_project_wheel:
        project_out = vendor / "project"
        project_out.mkdir(parents=True, exist_ok=True)
        if build_project_runner is not None:
            build_project_runner(project_out)
        else:
            project_root = Path(uv_lock_path).resolve().parent
            docs_cli = project_root / "docs" / "cli"
            created_docs_cli = False
            if not docs_cli.exists():
                docs_cli.mkdir(parents=True, exist_ok=True)
                created_docs_cli = True
            try:
                proc = subprocess.run(
                    ["uv", "build", "--wheel", "--out-dir", str(project_out), str(project_root)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            finally:
                if created_docs_cli:
                    try:
                        docs_cli.rmdir()
                    except OSError:
                        pass
            if proc.returncode != 0:
                raise PrefetchError(f"uv build --wheel failed:\n{proc.stderr.strip()}")

    return written


def prefetch_extensions(
    vendor_dir: str | Path,
    *,
    extensions_lock_path: str | Path = Path("docker/serving-image/extensions.lock.json"),
    platforms: Iterable[str] | None = None,
    fetcher: Callable[[str], bytes] = default_https_fetcher,
) -> list[Path]:
    """Download and verify compressed + decompressed DuckDB extensions into vendor/extensions."""
    manifest = load_extensions_manifest(extensions_lock_path)
    target_platforms = (
        {detect_duckdb_platform(p) for p in platforms}
        if platforms is not None
        else {entry.platform for entry in manifest.entries}
    )

    entries = [e for e in manifest.entries if e.platform in target_platforms]
    if not entries:
        raise PrefetchError(
            f"No extension entries in {extensions_lock_path} match platforms {sorted(target_platforms)}"
        )

    ext_vendor_root = Path(vendor_dir) / "extensions"
    written: list[Path] = []
    for entry in entries:
        gz_bytes = fetcher(entry.source_url)
        verify_and_decompress_extension(gz_bytes, entry)
        dest = ext_vendor_root / entry.vendor_relpath(manifest.duckdb_version)
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(gz_bytes)
        os.chmod(dest, 0o644)
        written.append(dest)

    return written


def verify_vendor_directory(
    vendor_dir: str | Path,
    *,
    uv_lock_path: str | Path = Path("uv.lock"),
    extensions_lock_path: str | Path = Path("docker/serving-image/extensions.lock.json"),
    platform: str | None = None,
) -> None:
    """Fail-closed verification of a prefetched vendor directory before offline install."""
    vendor = Path(vendor_dir)
    reqs = vendor / "requirements.txt"
    if not reqs.is_file() or "--hash=sha256:" not in reqs.read_text(encoding="utf-8"):
        raise PrefetchError(
            f"Vendor requirements file missing or lacks SHA-256 hashes: {reqs}"
        )

    verify_wheel_directory(vendor / "wheels", uv_lock_path)

    manifest = load_extensions_manifest(extensions_lock_path)
    target_platforms = (
        {detect_duckdb_platform(platform)}
        if platform
        else {detect_duckdb_platform()}
    )
    matching = [e for e in manifest.entries if e.platform in target_platforms]
    if not matching:
        raise PrefetchError(f"No extensions found in manifest for {sorted(target_platforms)}")

    for entry in matching:
        gz_path = vendor / "extensions" / entry.vendor_relpath(manifest.duckdb_version)
        if not gz_path.is_file():
            raise PrefetchError(f"Missing prefetched extension archive: {gz_path}")
        verify_and_decompress_extension(gz_path.read_bytes(), entry)


def install_extensions(
    *,
    extensions_lock_path: str | Path = Path("docker/serving-image/extensions.lock.json"),
    extension_directory: str | Path | None = None,
    platform: str | None = None,
    vendor_extensions_dir: str | Path | None = None,
    offline: bool = False,
    fetcher: Callable[[str], bytes] = default_https_fetcher,
) -> list[Path]:
    """Install and SHA-256 verify DuckDB extensions into <extension_directory>/v<ver>/<platform>/."""
    manifest = load_extensions_manifest(extensions_lock_path)
    target_platform = detect_duckdb_platform(platform)
    dest_root = Path(extension_directory or manifest.extension_directory)

    entries = [e for e in manifest.entries if e.platform == target_platform]
    if not entries:
        raise PrefetchError(
            f"No extension entries in {extensions_lock_path} for platform {target_platform!r}"
        )

    vendor_root = Path(vendor_extensions_dir) if vendor_extensions_dir else None
    installed: list[Path] = []

    for entry in entries:
        gz_bytes: bytes | None = None
        if vendor_root is not None:
            candidates = [
                vendor_root / entry.vendor_relpath(manifest.duckdb_version),
                vendor_root / entry.platform / f"{entry.name}.duckdb_extension.gz",
            ]
            for candidate in candidates:
                if candidate.is_file():
                    gz_bytes = candidate.read_bytes()
                    break

        if gz_bytes is None:
            if offline:
                raise PrefetchError(
                    f"Offline mode enabled and prefetched archive missing for "
                    f"{entry.platform}/{entry.name} under {vendor_root}"
                )
            gz_bytes = fetcher(entry.source_url)

        raw_bytes = verify_and_decompress_extension(gz_bytes, entry)
        dest_file = dest_root / entry.install_relpath(manifest.duckdb_version)
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        for parent in (dest_root, dest_root / f"v{manifest.duckdb_version}", dest_file.parent):
            try:
                os.chmod(parent, 0o755)
            except OSError:
                pass

        dest_file.write_bytes(raw_bytes)
        os.chmod(dest_file, 0o644)

        written_sha = compute_file_sha256(dest_file)
        if written_sha != entry.raw_sha256:
            dest_file.unlink(missing_ok=True)
            raise HashMismatchError(
                f"Installed extension SHA-256 verification failed for {dest_file}: "
                f"expected {entry.raw_sha256}, got {written_sha}"
            )
        installed.append(dest_file)

    return installed


def prefetch_all(
    vendor_dir: str | Path,
    *,
    uv_lock_path: str | Path = Path("uv.lock"),
    extensions_lock_path: str | Path = Path("docker/serving-image/extensions.lock.json"),
    platforms: Iterable[str] = ("linux_amd64", "linux_arm64"),
    fetcher: Callable[[str], bytes] = default_https_fetcher,
    export_runner: Callable[[Path], None] | None = None,
    build_project_runner: Callable[[Path], None] | None = None,
    build_project_wheel: bool = True,
) -> tuple[list[Path], list[Path]]:
    """Prefetch both locked Python wheels and pinned DuckDB extensions."""
    wheels = prefetch_wheels(
        vendor_dir,
        uv_lock_path=uv_lock_path,
        platforms=platforms,
        fetcher=fetcher,
        export_runner=export_runner,
        build_project_runner=build_project_runner,
        build_project_wheel=build_project_wheel,
    )
    exts = prefetch_extensions(
        vendor_dir,
        extensions_lock_path=extensions_lock_path,
        platforms=platforms,
        fetcher=fetcher,
    )
    return wheels, exts


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point for prefetching, verifying, and installing serving image artifacts."""
    parser = argparse.ArgumentParser(
        description="Prefetch and verify locked artifacts for atlas-rt-serving."
    )
    subparsers = parser.add_subparsers(dest="command")

    p_prefetch = subparsers.add_parser(
        "prefetch", help="Prefetch wheels and DuckDB extensions into vendor directory"
    )
    p_prefetch.add_argument(
        "--vendor-dir",
        "--output-dir",
        default="docker/serving-image/vendor",
        help="Destination vendor directory (default: docker/serving-image/vendor)",
    )
    p_prefetch.add_argument("--uv-lock", default="uv.lock", help="Path to uv.lock")
    p_prefetch.add_argument(
        "--extensions-lock",
        default="docker/serving-image/extensions.lock.json",
        help="Path to extensions.lock.json",
    )
    p_prefetch.add_argument(
        "--platform",
        action="append",
        dest="platforms",
        help="Target platform(s), e.g. linux_amd64, linux_arm64 (default: both)",
    )
    p_prefetch.add_argument(
        "--no-project-wheel",
        action="store_true",
        help="Skip building local seeknal wheel into vendor/project",
    )

    p_verify = subparsers.add_parser(
        "verify-vendor", help="Verify prefetched vendor directory against lockfiles"
    )
    p_verify.add_argument(
        "--vendor-dir",
        default="docker/serving-image/vendor",
        help="Vendor directory to verify",
    )
    p_verify.add_argument("--uv-lock", default="uv.lock", help="Path to uv.lock")
    p_verify.add_argument(
        "--extensions-lock",
        default="docker/serving-image/extensions.lock.json",
        help="Path to extensions.lock.json",
    )
    p_verify.add_argument("--platform", default="", help="Target platform override")

    p_install = subparsers.add_parser(
        "install-extensions",
        help="Verify and install DuckDB extensions into extension_directory",
    )
    p_install.add_argument(
        "--extensions-lock",
        default="docker/serving-image/extensions.lock.json",
        help="Path to extensions.lock.json",
    )
    p_install.add_argument(
        "--extension-directory",
        default=None,
        help="Override target extension directory (default from extensions.lock.json)",
    )
    p_install.add_argument(
        "--vendor-extensions-dir",
        default=None,
        help="Optional prefetched vendor/extensions directory",
    )
    p_install.add_argument("--platform", default="", help="Target platform override")
    p_install.add_argument(
        "--offline",
        action="store_true",
        help="Fail closed unless extensions exist in --vendor-extensions-dir",
    )

    raw_args = list(sys.argv[1:] if argv is None else argv)
    if not raw_args or (
        raw_args[0].startswith("-")
        and raw_args[0] not in {"-h", "--help"}
    ):
        raw_args = ["prefetch", *raw_args]

    args = parser.parse_args(raw_args)

    try:
        if args.command == "prefetch":
            platforms = args.platforms or ["linux_amd64", "linux_arm64"]
            wheels, exts = prefetch_all(
                args.vendor_dir,
                uv_lock_path=args.uv_lock,
                extensions_lock_path=args.extensions_lock,
                platforms=platforms,
                build_project_wheel=not args.no_project_wheel,
            )
            print(
                f"Prefetched {len(wheels)} wheel(s) and {len(exts)} extension(s) into {args.vendor_dir}"
            )
            return 0

        if args.command == "verify-vendor":
            verify_vendor_directory(
                args.vendor_dir,
                uv_lock_path=args.uv_lock,
                extensions_lock_path=args.extensions_lock,
                platform=args.platform or None,
            )
            print(f"Verified vendor directory: {args.vendor_dir}")
            return 0

        if args.command == "install-extensions":
            installed = install_extensions(
                extensions_lock_path=args.extensions_lock,
                extension_directory=args.extension_directory,
                platform=args.platform or None,
                vendor_extensions_dir=args.vendor_extensions_dir,
                offline=args.offline,
            )
            print(
                f"Installed {len(installed)} verified DuckDB extension(s): "
                + ", ".join(str(p) for p in installed)
            )
            return 0

        parser.print_help()
        return 2
    except PrefetchError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
