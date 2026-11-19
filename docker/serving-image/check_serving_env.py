#!/usr/bin/env python3
"""Build-time fail-closed verification of the atlas-rt-serving Python & DuckDB environment."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import os
import platform as py_platform
import re
import subprocess
import sys
import tomllib
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import duckdb

from seeknal.serving.buildinfo import EXPECTED_DUCKDB_VERSION

KEY_PACKAGES: tuple[str, ...] = (
    "duckdb",
    "sqlglot",
    "starlette",
    "uvicorn",
    "httpx",
    "psycopg2-binary",
    "pymysql",
    "cryptography",
    "pyyaml",
)

FORBIDDEN_PACKAGES: tuple[str, ...] = (
    "pydantic-ai-slim",
    "pydantic-deep",
    "prefect",
    "temporalio",
    "pyspark",
    "scikit-learn",
    "scipy",
    "openai",
    "anthropic",
    "google-genai",
    "python-telegram-bot",
    "redis",
    "ddgs",
    "ipykernel",
)

SAFE_EXT_NAME_RE = re.compile(r"^[a-z0-9_]+$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
EXTENSION_LOAD_PRIORITY: dict[str, int] = {
    "httpfs": 0,
    "avro": 1,
    "iceberg": 3,
}
EXTENSION_PREREQUISITES: dict[str, tuple[str, ...]] = {
    "iceberg": ("avro",),
}
_UNSET = object()


class EnvCheckError(RuntimeError):
    """Raised when any serving environment verification check fails."""


def normalize_pep503(name: str) -> str:
    """Normalize a Python distribution name per PEP 503."""
    return re.sub(r"[-_.]+", "-", name).lower()


def detect_duckdb_platform(arch: str | None = None) -> str:
    """Resolve current or specified Linux architecture to DuckDB platform identifier."""
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
        raise EnvCheckError(f"Unsupported DuckDB Linux platform: {raw!r}")
    return resolved


def _default_installed_versions() -> dict[str, str]:
    installed: dict[str, str] = {}
    for dist in importlib.metadata.distributions():
        name = dist.metadata["Name"]
        if name:
            installed[normalize_pep503(name)] = str(dist.version)
    return installed


def _load_locked_versions(uv_lock_path: Path) -> dict[str, str]:
    if not uv_lock_path.is_file():
        raise EnvCheckError(f"uv.lock not found at {uv_lock_path}")
    try:
        with open(uv_lock_path, "rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise EnvCheckError(f"Failed to parse uv.lock at {uv_lock_path}: {exc}") from exc

    return {
        normalize_pep503(pkg["name"]): str(pkg["version"])
        for pkg in data.get("package", [])
    }


def _resolve_accel_expected_duckdb_version() -> str:
    try:
        mod = importlib.import_module("seeknal.serving.accel.settings")
    except ImportError as exc:
        raise EnvCheckError(
            f"Failed to import seeknal.serving.accel.settings to verify DuckDB version pin: {exc}"
        ) from exc
    val = getattr(mod, "DEFAULT_EXPECTED_DUCKDB_VERSION", None)
    if not isinstance(val, str) or not val.strip():
        raise EnvCheckError(
            "seeknal.serving.accel.settings is importable but missing DEFAULT_EXPECTED_DUCKDB_VERSION"
        )
    return val.strip()


def _compute_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _default_extension_loader(extension_dir: Path, ext_name: str) -> None:
    """Verify DuckDB can LOAD the baked extension with all remote/auto loading disabled."""
    if not SAFE_EXT_NAME_RE.fullmatch(ext_name):
        raise EnvCheckError(f"Unsafe extension identifier: {ext_name!r}")

    escaped_dir = str(extension_dir).replace("'", "''")
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET autoinstall_known_extensions = false")
        con.execute("SET autoload_known_extensions = false")
        con.execute(
            "SET custom_extension_repository = '/nonexistent_extension_repository'"
        )
        con.execute(f"SET extension_directory = '{escaped_dir}'")
        prereqs = EXTENSION_PREREQUISITES.get(ext_name, ())
        for prereq in prereqs:
            if not SAFE_EXT_NAME_RE.fullmatch(prereq):
                raise EnvCheckError(
                    f"Unsafe prerequisite extension identifier: {prereq!r}"
                )
            con.execute(f"LOAD {prereq}")
        con.execute(f"LOAD {ext_name}")
        for expected_loaded in (*prereqs, ext_name):
            row = con.execute(
                "SELECT loaded FROM duckdb_extensions() WHERE extension_name = ?",
                [expected_loaded],
            ).fetchone()
            if not row or not row[0]:
                raise EnvCheckError(
                    f"Extension {expected_loaded!r} did not report loaded=true after LOAD"
                )
    except EnvCheckError:
        raise
    except Exception as exc:
        raise EnvCheckError(
            f"DuckDB failed to LOAD baked extension {ext_name!r} from {extension_dir}: {exc}"
        ) from exc
    finally:
        con.close()


def _default_pip_check_runner() -> None:
    proc = subprocess.run(
        ["uv", "pip", "check", "--python", sys.executable],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        details = (proc.stderr or proc.stdout or "").strip()
        raise EnvCheckError(f"uv pip check failed:\n{details}")


def check_packages(
    installed_versions: Mapping[str, str],
    locked_versions: Mapping[str, str],
    *,
    key_packages: Sequence[str] = KEY_PACKAGES,
    forbidden_packages: Sequence[str] = FORBIDDEN_PACKAGES,
) -> None:
    """Verify key packages match uv.lock versions and no forbidden packages are installed."""
    norm_installed = {normalize_pep503(k): v for k, v in installed_versions.items()}

    for pkg in key_packages:
        norm = normalize_pep503(pkg)
        if norm not in locked_versions:
            raise EnvCheckError(f"Key package {pkg!r} missing from uv.lock")
        actual = norm_installed.get(norm)
        expected = locked_versions[norm]
        if actual is None:
            raise EnvCheckError(f"Required key package {pkg!r} is not installed")
        if actual != expected:
            raise EnvCheckError(
                f"Version mismatch for key package {pkg!r}: "
                f"installed {actual!r} != uv.lock {expected!r}"
            )

    present_forbidden = [
        pkg
        for pkg in forbidden_packages
        if normalize_pep503(pkg) in norm_installed
    ]
    if present_forbidden:
        raise EnvCheckError(
            "Forbidden non-serving package(s) installed in serving environment: "
            + ", ".join(sorted(present_forbidden))
        )


def check_duckdb_version_pin(
    *,
    runtime_duckdb_version: str | None = None,
    expected_duckdb_version: str = EXPECTED_DUCKDB_VERSION,
    accel_expected_duckdb_version: object = _UNSET,
    locked_duckdb_version: object = _UNSET,
) -> None:
    """Verify duckdb.__version__, buildinfo, accel settings, and uv.lock all agree."""
    actual = str(
        duckdb.__version__ if runtime_duckdb_version is None else runtime_duckdb_version
    )
    if actual != expected_duckdb_version:
        raise EnvCheckError(
            f"DuckDB runtime version mismatch: duckdb.__version__={actual!r} "
            f"!= EXPECTED_DUCKDB_VERSION={expected_duckdb_version!r}"
        )

    accel_ver = (
        _resolve_accel_expected_duckdb_version()
        if accel_expected_duckdb_version is _UNSET
        else accel_expected_duckdb_version
    )
    if accel_ver is not None and str(accel_ver) != expected_duckdb_version:
        raise EnvCheckError(
            "DuckDB version pin disagreement between buildinfo and accel settings: "
            f"EXPECTED_DUCKDB_VERSION={expected_duckdb_version!r} != "
            f"DEFAULT_EXPECTED_DUCKDB_VERSION={accel_ver!r}"
        )

    if locked_duckdb_version is not _UNSET:
        if not isinstance(locked_duckdb_version, str) or not locked_duckdb_version.strip():
            raise EnvCheckError("DuckDB version missing from uv.lock")
        locked_str = locked_duckdb_version.strip()
        if locked_str != expected_duckdb_version:
            raise EnvCheckError(
                "DuckDB version pin disagreement between buildinfo and uv.lock: "
                f"EXPECTED_DUCKDB_VERSION={expected_duckdb_version!r} != "
                f"uv.lock duckdb={locked_str!r}"
            )


def check_baked_extensions(
    extensions_lock_path: str | Path,
    *,
    extension_directory: str | Path | None = None,
    platform: str | None = None,
    expected_duckdb_version: str = EXPECTED_DUCKDB_VERSION,
    extension_loader: Callable[[Path, str], None] = _default_extension_loader,
) -> list[Path]:
    """Verify baked DuckDB extensions exist, match raw_sha256, and LOAD offline."""
    lock_file = Path(extensions_lock_path)
    if not lock_file.is_file():
        raise EnvCheckError(f"Extensions lockfile not found: {lock_file}")

    try:
        manifest = json.loads(lock_file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise EnvCheckError(
            f"Failed to parse extensions lockfile {lock_file}: {exc}"
        ) from exc

    manifest_duckdb_ver = str(manifest.get("duckdb_version", "")).strip()
    if manifest_duckdb_ver != expected_duckdb_version:
        raise EnvCheckError(
            f"extensions.lock.json duckdb_version={manifest_duckdb_ver!r} "
            f"!= EXPECTED_DUCKDB_VERSION={expected_duckdb_version!r}"
        )

    env_ext_dir = os.environ.get("SEEKNAL_EXTENSION_DIRECTORY", "").strip()
    resolved_ext_dir = (
        extension_directory
        or env_ext_dir
        or manifest.get("extension_directory", "")
    )
    ext_root = Path(resolved_ext_dir)
    if not str(ext_root).startswith("/"):
        raise EnvCheckError(
            f"Extension directory must be an absolute path, got: {ext_root}"
        )

    target_platform = detect_duckdb_platform(platform)
    raw_entries = [
        item
        for item in manifest.get("extensions", [])
        if detect_duckdb_platform(str(item.get("platform", ""))) == target_platform
    ]
    if not raw_entries:
        raise EnvCheckError(
            f"No extensions listed in {lock_file} for platform {target_platform!r}"
        )

    entries = [
        item
        for _, item in sorted(
            enumerate(raw_entries),
            key=lambda pair: (
                EXTENSION_LOAD_PRIORITY.get(str(pair[1].get("name", "")).strip(), 2),
                pair[0],
            ),
        )
    ]

    entry_names = {str(item.get("name", "")).strip() for item in entries}
    if "avro" not in entry_names:
        raise EnvCheckError(
            f"extensions.lock.json is missing required 'avro' extension entry for "
            f"platform {target_platform!r} (required by DuckDB {manifest_duckdb_ver} 'iceberg' extension)"
        )

    avro_file = (
        ext_root
        / f"v{manifest_duckdb_ver}"
        / target_platform
        / "avro.duckdb_extension"
    )
    if not avro_file.is_file():
        raise EnvCheckError(
            f"Required DuckDB extension avro.duckdb_extension is missing from "
            f"extension_directory {ext_root} (expected {avro_file})"
        )

    staged_extensions: list[tuple[str, Path]] = []
    for entry in entries:
        name = str(entry.get("name", "")).strip()
        if not SAFE_EXT_NAME_RE.fullmatch(name):
            raise EnvCheckError(f"Invalid extension name in lockfile: {name!r}")

        expected_raw = str(
            entry.get("raw_sha256") or entry.get("decompressed_sha256") or ""
        ).strip().lower()
        if not HEX64_RE.fullmatch(expected_raw):
            raise EnvCheckError(
                f"Invalid or missing raw_sha256 for {target_platform}/{name}"
            )

        ext_file = (
            ext_root
            / f"v{manifest_duckdb_ver}"
            / target_platform
            / f"{name}.duckdb_extension"
        )
        if not ext_file.is_file():
            raise EnvCheckError(f"Baked DuckDB extension missing on disk: {ext_file}")

        actual_raw = _compute_sha256(ext_file)
        if actual_raw != expected_raw:
            raise EnvCheckError(
                f"SHA-256 mismatch for baked extension {ext_file}: "
                f"expected {expected_raw}, got {actual_raw}"
            )
        staged_extensions.append((name, ext_file))

    verified_paths: list[Path] = []
    for name, ext_file in staged_extensions:
        extension_loader(ext_root, name)
        verified_paths.append(ext_file)

    return verified_paths


def verify_serving_environment(
    *,
    uv_lock_path: str | Path = Path("uv.lock"),
    extensions_lock_path: str | Path = Path(
        "docker/serving-image/extensions.lock.json"
    ),
    extension_directory: str | Path | None = None,
    platform: str | None = None,
    installed_versions_provider: Callable[[], Mapping[str, str]] | None = None,
    runtime_duckdb_version: str | None = None,
    expected_duckdb_version: str = EXPECTED_DUCKDB_VERSION,
    accel_expected_duckdb_version: object = _UNSET,
    extension_loader: Callable[[Path, str], None] = _default_extension_loader,
    pip_check_runner: Callable[[], None] = _default_pip_check_runner,
) -> None:
    """Run all fail-closed serving image verification checks."""
    locked_versions = _load_locked_versions(Path(uv_lock_path))
    installed_versions = (
        installed_versions_provider()
        if installed_versions_provider is not None
        else _default_installed_versions()
    )

    check_duckdb_version_pin(
        runtime_duckdb_version=runtime_duckdb_version,
        expected_duckdb_version=expected_duckdb_version,
        accel_expected_duckdb_version=accel_expected_duckdb_version,
        locked_duckdb_version=locked_versions.get("duckdb"),
    )
    check_packages(installed_versions, locked_versions)
    check_baked_extensions(
        extensions_lock_path,
        extension_directory=extension_directory,
        platform=platform,
        expected_duckdb_version=expected_duckdb_version,
        extension_loader=extension_loader,
    )
    pip_check_runner()


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point invoked during docker build."""
    parser = argparse.ArgumentParser(
        description="Verify atlas-rt-serving Python packages and baked DuckDB extensions."
    )
    parser.add_argument("--uv-lock", default="uv.lock", help="Path to uv.lock")
    parser.add_argument(
        "--extensions-lock",
        default="docker/serving-image/extensions.lock.json",
        help="Path to extensions.lock.json",
    )
    parser.add_argument(
        "--extension-directory",
        default=None,
        help="Override DuckDB extension directory",
    )
    parser.add_argument(
        "--platform",
        default="",
        help="Override target platform (e.g. linux_amd64, linux_arm64)",
    )
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))

    try:
        verify_serving_environment(
            uv_lock_path=args.uv_lock,
            extensions_lock_path=args.extensions_lock,
            extension_directory=args.extension_directory,
            platform=args.platform or None,
        )
    except EnvCheckError as exc:
        print(f"SERVING ENV CHECK FAILED: {exc}", file=sys.stderr)
        return 1

    print("serving environment ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
