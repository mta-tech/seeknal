"""Build and runtime version metadata for Seeknal Serving."""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

import duckdb

EXPECTED_DUCKDB_VERSION = "2.0.0"
DEFAULT_EXTENSION_DIRECTORY = "/opt/seeknal/duckdb_extensions"
DEFAULT_BUILDINFO_PATH = Path("/opt/seeknal/buildinfo.json")
FALLBACK_PACKAGE_VERSION = "2.14.0"
BUILD_INFO_KEYS: tuple[str, ...] = (
    "version",
    "git_sha",
    "build_time",
    "duckdb_version",
    "lock_sha256",
)


def _resolve_package_version() -> str:
    """Resolve installed seeknal package version with deterministic fallback."""
    try:
        resolved = importlib.metadata.version("seeknal").strip()
        if resolved:
            return resolved
    except importlib.metadata.PackageNotFoundError:
        pass
    return FALLBACK_PACKAGE_VERSION


def _resolve_lock_sha256(lock_path: str | Path | None = None) -> str:
    """Compute SHA-256 hex digest of uv.lock when available on disk."""
    if lock_path is not None:
        candidate = Path(lock_path)
    else:
        candidate = Path(__file__).resolve().parents[3] / "uv.lock"

    try:
        if candidate.is_file():
            return hashlib.sha256(candidate.read_bytes()).hexdigest()
    except OSError:
        pass
    return "unknown"


def _read_metadata_file(path: Path) -> dict[str, str]:
    """Read string fields from a build-time JSON metadata file if present."""
    try:
        if not path.is_file():
            return {}
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}

    if not isinstance(raw, dict):
        return {}

    cleaned: dict[str, str] = {}
    for key in BUILD_INFO_KEYS:
        val = raw.get(key)
        if isinstance(val, str) and val.strip():
            cleaned[key] = val.strip()
    return cleaned


def get_build_info(
    buildinfo_path: str | Path | None = None,
    *,
    lock_path: str | Path | None = None,
) -> dict[str, str]:
    """Return Serving build and runtime metadata without invoking git or network.

    Always returns the exact 5 keys:
    ``{"version", "git_sha", "build_time", "duckdb_version", "lock_sha256"}``.
    """
    if buildinfo_path is not None:
        meta_path = Path(buildinfo_path)
    else:
        env_override = os.environ.get("SEEKNAL_BUILDINFO_PATH", "").strip()
        meta_path = Path(env_override) if env_override else DEFAULT_BUILDINFO_PATH

    meta = _read_metadata_file(meta_path)

    return {
        "version": meta.get("version") or _resolve_package_version(),
        "git_sha": meta.get("git_sha") or "unknown",
        "build_time": meta.get("build_time") or "unknown",
        "duckdb_version": meta.get("duckdb_version") or str(duckdb.__version__),
        "lock_sha256": meta.get("lock_sha256") or _resolve_lock_sha256(lock_path),
    }


def verify_installed_matches_source(
    source_dir: str | Path,
    *,
    installed_package_dir: str | Path | None = None,
) -> None:
    """Ensure the installed seeknal package matches copied source files by SHA-256."""
    src_root = Path(source_dir)
    if not src_root.is_dir():
        raise RuntimeError(
            f"Source directory not found when verifying installed seeknal code: {src_root}"
        )

    src_files = sorted(p for p in src_root.rglob("*.py") if p.is_file())
    if not src_files:
        raise RuntimeError(
            f"No Python source files found under {src_root} when verifying installed seeknal code"
        )

    if installed_package_dir is not None:
        installed_root = Path(installed_package_dir)
    else:
        spec = importlib.util.find_spec("seeknal")
        if spec is None or not spec.submodule_search_locations:
            raise RuntimeError(
                "seeknal package is not installed; refusing to record buildinfo git_sha"
            )
        installed_root = Path(next(iter(spec.submodule_search_locations)))

    if not installed_root.is_dir():
        raise RuntimeError(
            f"Installed seeknal directory not found: {installed_root}"
        )

    for src_py in src_files:
        rel = src_py.relative_to(src_root)
        inst_py = installed_root / rel
        if not inst_py.is_file():
            raise RuntimeError(
                f"Installed seeknal missing file {rel} present in copied source {src_root}"
            )
        src_sha = hashlib.sha256(src_py.read_bytes()).hexdigest()
        inst_sha = hashlib.sha256(inst_py.read_bytes()).hexdigest()
        if src_sha != inst_sha:
            raise RuntimeError(
                f"Installed seeknal file {inst_py} SHA-256 ({inst_sha}) does not match "
                f"copied source {src_py} ({src_sha}); refusing to record buildinfo git_sha"
            )


def write_build_info(
    output_path: str | Path = DEFAULT_BUILDINFO_PATH,
    *,
    git_sha: str | None = None,
    build_time: str | None = None,
    lock_path: str | Path | None = None,
    version: str | None = None,
    duckdb_version: str | None = None,
    source_dir: str | Path | None = None,
    installed_package_dir: str | Path | None = None,
) -> dict[str, str]:
    """Write build-time metadata JSON file for inclusion in the Serving container."""
    if source_dir is not None:
        verify_installed_matches_source(
            source_dir, installed_package_dir=installed_package_dir
        )

    resolved_git_sha = (git_sha or "").strip() or "unknown"
    resolved_build_time = (build_time or "").strip() or datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    resolved_version = (version or "").strip() or _resolve_package_version()
    resolved_duckdb_version = (duckdb_version or "").strip() or str(duckdb.__version__)
    resolved_lock_sha256 = _resolve_lock_sha256(lock_path)

    payload: dict[str, str] = {
        "version": resolved_version,
        "git_sha": resolved_git_sha,
        "build_time": resolved_build_time,
        "duckdb_version": resolved_duckdb_version,
        "lock_sha256": resolved_lock_sha256,
    }

    dest = Path(output_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.chmod(dest, 0o644)
    return payload


def format_build_info(info: Mapping[str, str], *, as_json: bool = False) -> str:
    """Format build info as a single-line human summary or JSON string."""
    ordered = {key: str(info[key]) for key in BUILD_INFO_KEYS}
    if as_json:
        return json.dumps(ordered)
    return (
        f"seeknal-serving {ordered['version']} "
        f"(git_sha={ordered['git_sha']}, "
        f"build_time={ordered['build_time']}, "
        f"duckdb_version={ordered['duckdb_version']}, "
        f"lock_sha256={ordered['lock_sha256']})"
    )
