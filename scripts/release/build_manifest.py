#!/usr/bin/env python3
"""Build and validate an Atlas Runtime release manifest."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from pathlib import Path
from typing import Any, Sequence

# Ensure src/ is on sys.path if seeknal is not already installed/importable
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SRC_PATH = _REPO_ROOT / "src"
if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

import yaml

from seeknal.runtime.release import ManifestError, manifest_sha256, parse_manifest

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_NAME_RE = re.compile(r"^atlas-rt/(\d+)\.(\d+)\.(\d+)$")
_SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")


def get_seeknal_version_from_pyproject(repo_root: Path | None = None) -> str:
    """Read seeknal package version from pyproject.toml."""
    root = repo_root or _REPO_ROOT
    pyproject_path = root / "pyproject.toml"
    if not pyproject_path.is_file():
        pyproject_path = Path("pyproject.toml")
    if not pyproject_path.is_file():
        raise ValueError(f"pyproject.toml not found at {pyproject_path}")

    content = pyproject_path.read_text(encoding="utf-8")
    match = re.search(r'version\s*=\s*"([^"]+)"', content)
    if not match:
        raise ValueError("Could not find version in pyproject.toml")
    return match.group(1)


def compute_file_sha256(path: str | Path) -> str:
    """Return SHA-256 hex digest of file bytes."""
    p = Path(path)
    if not p.is_file():
        # Check relative to repo root
        cand = _REPO_ROOT / str(path)
        if cand.is_file():
            p = cand
        else:
            raise FileNotFoundError(f"File not found: {path}")
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _resolve_file_path(path_str: str) -> Path:
    """Resolve file path relative to cwd or repo root."""
    p = Path(path_str)
    if p.is_file():
        return p
    cand = _REPO_ROOT / path_str
    if cand.is_file():
        return cand
    return p


def build_parser() -> argparse.ArgumentParser:
    """Construct CLI argument parser for build_manifest."""
    parser = argparse.ArgumentParser(
        prog="build_manifest.py",
        description="Atlas Runtime release manifest builder and validator.",
    )
    parser.add_argument(
        "--release",
        required=True,
        help="Release name (e.g. atlas-rt/1.0.0)",
    )
    parser.add_argument(
        "--channel",
        default="stable",
        choices=["stable", "lts", "preview"],
        help="Release channel (default: stable)",
    )
    parser.add_argument(
        "--seeknal-version",
        default=None,
        help="Seeknal package version (read from pyproject.toml if omitted)",
    )
    parser.add_argument(
        "--commit",
        required=True,
        help="40-hex lowercase commit hash",
    )
    parser.add_argument(
        "--lock",
        default="uv.lock",
        help="Path to uv.lock (default: uv.lock)",
    )
    parser.add_argument(
        "--python-version",
        default="3.11.9",
        help="Python version (default: 3.11.9)",
    )
    parser.add_argument(
        "--image",
        action="append",
        default=[],
        help="Image reference NAME=REPO@sha256:DIGEST (repeatable)",
    )
    parser.add_argument(
        "--duckdb-version",
        default="1.4.4",
        help="DuckDB version (default: 1.4.4)",
    )
    parser.add_argument(
        "--extension",
        action="append",
        default=[],
        help="DuckDB extension NAME=HASH (repeatable)",
    )
    parser.add_argument(
        "--contracts-file",
        default="docker/release-contracts.yaml",
        help="Path to contracts YAML file (default: docker/release-contracts.yaml)",
    )
    parser.add_argument(
        "--acu-weights-file",
        default="docker/release-acu-weights.yaml",
        help="Path to ACU weights YAML file (default: docker/release-acu-weights.yaml)",
    )
    parser.add_argument(
        "--licence-scan",
        required=True,
        help="Licence scan result (must be 'clean')",
    )
    parser.add_argument(
        "--out",
        default="manifest.yaml",
        help="Output manifest YAML file path (default: manifest.yaml)",
    )
    return parser


def parse_image_arg(arg: str) -> tuple[str, str]:
    """Parse and validate an --image argument NAME=REPO@sha256:DIGEST or NAME=sha256:DIGEST.

    Refuses tag references or invalid digests.
    Returns (name, digest).
    """
    if "=" not in arg:
        raise ValueError(f"Malformed --image argument '{arg}', expected NAME=REPO@sha256:DIGEST")

    name, ref = arg.split("=", 1)
    name = name.strip()
    ref = ref.strip()

    if not name:
        raise ValueError("Image name cannot be empty")

    if "@" in ref:
        _, digest = ref.split("@", 1)
    elif ref.startswith("sha256:"):
        digest = ref
    else:
        # Tag reference or missing digest
        raise ValueError(f"Tag-only or non-digest image reference refused: '{ref}'")

    if not _DIGEST_RE.match(digest):
        raise ValueError(
            f"Image digest for '{name}' must match '^sha256:[0-9a-f]{{64}}$', got '{digest}'"
        )

    return name, digest


def main(argv: Sequence[str] | None = None) -> int:
    """Main CLI entrypoint. Returns 0 on success, non-zero on error."""
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2

    errors: list[str] = []

    # Validate --release
    if not _NAME_RE.match(args.release):
        errors.append(
            f"--release must match '^atlas-rt/(\\d+)\\.(\\d+)\\.(\\d+)$', got '{args.release}'"
        )

    # Validate --licence-scan
    if args.licence_scan != "clean":
        errors.append(f"--licence-scan must be 'clean', got '{args.licence_scan}'")

    # Validate --commit
    if not _HEX40_RE.match(args.commit):
        errors.append(f"--commit must be 40 hex characters, got '{args.commit}'")

    # Resolve seeknal version
    seeknal_version = args.seeknal_version
    if not seeknal_version:
        try:
            seeknal_version = get_seeknal_version_from_pyproject()
        except Exception as exc:
            errors.append(f"Failed to read seeknal version: {exc}")

    if seeknal_version and not _SEMVER_RE.match(seeknal_version):
        errors.append(f"seeknal version must be semver X.Y.Z, got '{seeknal_version}'")

    # Compute uv.lock SHA256
    try:
        lock_sha256 = compute_file_sha256(args.lock)
    except Exception as exc:
        errors.append(f"Failed to read lock file '{args.lock}': {exc}")
        lock_sha256 = ""

    # Parse images
    images: dict[str, dict[str, str]] = {}
    for img_arg in args.image:
        try:
            img_name, digest = parse_image_arg(img_arg)
            images[img_name] = {"digest": digest}
        except ValueError as exc:
            errors.append(str(exc))

    if "atlas-rt" not in images:
        errors.append("Required image 'atlas-rt' is missing from --image arguments")
    if "atlas-rt-serving" not in images:
        errors.append("Required image 'atlas-rt-serving' is missing from --image arguments")

    # Sort images dictionary for determinism: atlas-rt, atlas-rt-agent, atlas-rt-serving, ...
    sorted_images = {k: images[k] for k in sorted(images)}

    # Parse engines / duckdb
    if not _SEMVER_RE.match(args.duckdb_version):
        errors.append(f"--duckdb-version must be semver X.Y.Z, got '{args.duckdb_version}'")

    extensions: dict[str, str] = {}
    for ext_arg in args.extension:
        if "=" not in ext_arg:
            errors.append(f"Malformed --extension argument '{ext_arg}', expected NAME=HASH")
            continue
        ext_name, ext_hash = ext_arg.split("=", 1)
        ext_name = ext_name.strip()
        ext_hash = ext_hash.strip()
        extensions[ext_name] = ext_hash

    engines: dict[str, Any] = {"duckdb": args.duckdb_version}
    if extensions:
        engines["duckdb_extensions"] = {k: extensions[k] for k in sorted(extensions)}

    # Load contracts file
    contracts_file = _resolve_file_path(args.contracts_file)
    if not contracts_file.is_file():
        errors.append(f"Contracts file not found: {args.contracts_file}")
        contracts_data: dict[str, Any] = {}
    else:
        try:
            contracts_data = yaml.safe_load(contracts_file.read_text(encoding="utf-8")) or {}
            if isinstance(contracts_data, dict) and "contracts" in contracts_data:
                contracts_data = contracts_data["contracts"]
        except Exception as exc:
            errors.append(f"Failed to parse contracts YAML '{args.contracts_file}': {exc}")
            contracts_data = {}

    # Load ACU weights file
    acu_weights_file = _resolve_file_path(args.acu_weights_file)
    if not acu_weights_file.is_file():
        errors.append(f"ACU weights file not found: {args.acu_weights_file}")
        acu_weights_data: dict[str, Any] = {}
    else:
        try:
            acu_weights_data = (
                yaml.safe_load(acu_weights_file.read_text(encoding="utf-8")) or {}
            )
            if isinstance(acu_weights_data, dict) and "acu_weights" in acu_weights_data:
                acu_weights_data = acu_weights_data["acu_weights"]
        except Exception as exc:
            errors.append(f"Failed to parse ACU weights YAML '{args.acu_weights_file}': {exc}")
            acu_weights_data = {}

    if errors:
        for err in errors:
            sys.stderr.write(f"ERROR: {err}\n")
        return 1

    # Serving config carried by builder
    serving_data = {"grant_max_lifetime_seconds": 60}

    # Stable, deterministic manifest mapping
    manifest_dict: dict[str, Any] = {
        "apiVersion": "atlas.io/v1",
        "kind": "RuntimeRelease",
        "metadata": {
            "name": args.release,
            "channel": args.channel,
        },
        "seeknal": {
            "version": seeknal_version,
            "commit": args.commit,
            "lock_sha256": lock_sha256,
        },
        "python": args.python_version,
        "images": sorted_images,
        "engines": engines,
        "contracts": contracts_data,
        "serving": serving_data,
        "acu_weights": acu_weights_data,
        "licence_scan": args.licence_scan,
    }

    # Validate against runtime release schema
    try:
        parse_manifest(manifest_dict)
    except ManifestError as exc:
        sys.stderr.write(f"ERROR: Manifest validation failed:\n{exc}\n")
        return 1

    # Dump byte-stable YAML (insertion-ordered mapping, non-flow format)
    yaml_bytes = yaml.dump(
        manifest_dict,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
    ).encode("utf-8")

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(yaml_bytes)

    # Print manifest SHA-256
    sha = manifest_sha256(out_path)
    print(sha)
    return 0


if __name__ == "__main__":
    sys.exit(main())
