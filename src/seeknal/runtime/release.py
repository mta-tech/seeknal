"""Atlas Runtime release manifest schema, models, and validation."""

from __future__ import annotations

import copy
import hashlib
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml


class ManifestError(ValueError):
    """Raised when a runtime release manifest fails schema validation."""

    def __init__(self, message_or_errors: str | list[str]):
        if isinstance(message_or_errors, list):
            self.errors = list(message_or_errors)
            msg = (
                f"Manifest validation failed with {len(self.errors)} error(s):\n  "
                + "\n  ".join(self.errors)
            )
            super().__init__(msg)
        else:
            self.errors = [message_or_errors]
            super().__init__(message_or_errors)


class ContractMismatch(ValueError):
    """Raised when a runtime release does not satisfy required contract versions."""

    def __init__(self, message_or_unsupported: str | list[tuple[str, str]]):
        if isinstance(message_or_unsupported, list):
            self.unsupported = list(message_or_unsupported)
            pairs = [f"{c}={v}" for c, v in self.unsupported]
            msg = f"Unsupported contracts: {', '.join(pairs)}"
            super().__init__(msg)
        else:
            self.unsupported = []
            super().__init__(message_or_unsupported)


@dataclass(frozen=True)
class ImageRef:
    """Reference to a container image in a runtime release."""

    digest: str
    sbom: str | None = None
    signature: str | None = None
    starrocks: str | None = None


@dataclass(frozen=True)
class RuntimeRelease:
    """Parsed and validated Atlas Runtime release manifest."""

    name: str
    version: tuple[int, int, int]
    channel: str
    images: dict[str, ImageRef]
    contracts: dict[str, tuple[str, ...]]
    acu_weights: dict[str, float]
    grant_max_lifetime_seconds: int | float
    raw: dict[str, Any]


_SEMVER_RE = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
_NAME_RE = re.compile(r"^atlas-rt/(\d+)\.(\d+)\.(\d+)$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_HEX40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_HEX_EXT_RE = re.compile(r"^[0-9a-fA-F]{8,64}$")
_PYTHON_RE = re.compile(r"^3\.\d+\.\d+$")
_OTHER_IMAGE_RE = re.compile(r"^atlas-rt-[a-z0-9-]+$")

_ALLOWED_TOP_KEYS = {
    "apiVersion",
    "kind",
    "metadata",
    "seeknal",
    "python",
    "images",
    "engines",
    "contracts",
    "serving",
    "acu_weights",
    "licence_scan",
}

_ALLOWED_CHANNELS = {"stable", "lts", "preview"}
_ALLOWED_CONTRACTS = {"grant", "run_grant", "engine_api", "lineage", "cube_mode"}
_ALLOWED_GRANT = {"v1", "v2", "v3"}
_ALLOWED_CUBE_MODE = {"refuse", "jwt"}
_REQUIRED_ACU_KEYS = ("job", "session", "warehouse", "sandbox")


def parse_manifest(data: Mapping[str, Any]) -> RuntimeRelease:
    """Validate raw manifest mapping and construct a frozen RuntimeRelease."""
    if not isinstance(data, Mapping):
        raise ManifestError(["root: expected a mapping"])

    errors: list[str] = []

    # 1. Top-level unknown keys
    for key in data:
        if key not in _ALLOWED_TOP_KEYS:
            errors.append(f"{key}: unknown top-level key")

    # 2. apiVersion and kind
    if "apiVersion" not in data:
        errors.append("apiVersion: field is required")
    elif data["apiVersion"] != "atlas.io/v1":
        errors.append(f"apiVersion: expected 'atlas.io/v1', got '{data['apiVersion']}'")

    if "kind" not in data:
        errors.append("kind: field is required")
    elif data["kind"] != "RuntimeRelease":
        errors.append(f"kind: expected 'RuntimeRelease', got '{data['kind']}'")

    # 3. metadata
    meta = data.get("metadata")
    parsed_version: tuple[int, int, int] | None = None
    release_name: str = ""
    release_channel: str = ""

    if "metadata" not in data:
        errors.append("metadata: field is required")
    elif not isinstance(meta, Mapping):
        errors.append("metadata: must be a mapping")
    else:
        for k in meta:
            if k not in ("name", "channel"):
                errors.append(f"metadata.{k}: unknown key")

        if "name" not in meta:
            errors.append("metadata.name: field is required")
        elif not isinstance(meta["name"], str):
            errors.append("metadata.name: must be a string")
        else:
            match = _NAME_RE.match(meta["name"])
            if not match:
                errors.append(
                    f"metadata.name: must match '^atlas-rt/(\\d+)\\.(\\d+)\\.(\\d+)$', got '{meta['name']}'"
                )
            else:
                release_name = meta["name"]
                parsed_version = (
                    int(match.group(1)),
                    int(match.group(2)),
                    int(match.group(3)),
                )

        if "channel" not in meta:
            errors.append("metadata.channel: field is required")
        elif meta["channel"] not in _ALLOWED_CHANNELS:
            errors.append(
                f"metadata.channel: must be one of {sorted(_ALLOWED_CHANNELS)}, got '{meta['channel']}'"
            )
        else:
            release_channel = str(meta["channel"])

    # 4. seeknal
    sk = data.get("seeknal")
    if "seeknal" not in data:
        errors.append("seeknal: field is required")
    elif not isinstance(sk, Mapping):
        errors.append("seeknal: must be a mapping")
    else:
        for k in sk:
            if k not in ("version", "commit", "lock_sha256"):
                errors.append(f"seeknal.{k}: unknown key")

        if "version" not in sk:
            errors.append("seeknal.version: field is required")
        elif not isinstance(sk["version"], str) or not _SEMVER_RE.match(sk["version"]):
            errors.append(
                f"seeknal.version: must be semver X.Y.Z, got '{sk.get('version')}'"
            )

        if "commit" not in sk:
            errors.append("seeknal.commit: field is required")
        elif not isinstance(sk["commit"], str) or not _HEX40_RE.match(sk["commit"]):
            errors.append(
                f"seeknal.commit: must be 40 lowercase hex characters, got '{sk.get('commit')}'"
            )

        if "lock_sha256" not in sk:
            errors.append("seeknal.lock_sha256: field is required")
        elif not isinstance(sk["lock_sha256"], str) or not _HEX64_RE.match(
            sk["lock_sha256"]
        ):
            errors.append(
                f"seeknal.lock_sha256: must be 64 lowercase hex characters, got '{sk.get('lock_sha256')}'"
            )

    # 5. python
    py = data.get("python")
    if "python" not in data:
        errors.append("python: field is required")
    elif not isinstance(py, str) or not _PYTHON_RE.match(py):
        errors.append(f"python: must match '^3\\.\\d+\\.\\d+$', got '{py}'")

    # 6. images
    imgs = data.get("images")
    parsed_images: dict[str, ImageRef] = {}
    if "images" not in data:
        errors.append("images: field is required")
    elif not isinstance(imgs, Mapping):
        errors.append("images: must be a mapping")
    else:
        if "atlas-rt" not in imgs:
            errors.append("images.atlas-rt: required image is missing")
        if "atlas-rt-serving" not in imgs:
            errors.append("images.atlas-rt-serving: required image is missing")

        for img_name, img_data in imgs.items():
            if img_name not in (
                "atlas-rt",
                "atlas-rt-serving",
                "atlas-rt-agent",
                "atlas-rt-sql",
            ):
                if not _OTHER_IMAGE_RE.match(img_name):
                    errors.append(
                        f"images.{img_name}: image key must match '^atlas-rt-[a-z0-9-]+$'"
                    )

            if not isinstance(img_data, Mapping):
                errors.append(f"images.{img_name}: must be a mapping")
                continue

            if "tag" in img_data:
                errors.append(f"images.{img_name}.tag: tags are never accepted")

            allowed_img_fields = {"digest", "sbom", "signature"}
            if img_name == "atlas-rt-sql":
                allowed_img_fields.add("starrocks")

            for f_name in img_data:
                if f_name not in allowed_img_fields and f_name != "tag":
                    errors.append(f"images.{img_name}.{f_name}: unknown field")

            digest_val = img_data.get("digest")
            if "digest" not in img_data:
                errors.append(f"images.{img_name}.digest: field is required")
            elif not isinstance(digest_val, str) or not _DIGEST_RE.match(digest_val):
                errors.append(
                    f"images.{img_name}.digest: must match '^sha256:[0-9a-f]{{64}}$', got '{digest_val}'"
                )

            sbom_val = img_data.get("sbom")
            if "sbom" in img_data:
                if not isinstance(sbom_val, str):
                    errors.append(f"images.{img_name}.sbom: must be a string")

            sig_val = img_data.get("signature")
            if "signature" in img_data:
                if sig_val != "cosign":
                    errors.append(
                        f"images.{img_name}.signature: only 'cosign' is supported, got '{sig_val}'"
                    )

            starrocks_val = img_data.get("starrocks")
            if "starrocks" in img_data:
                if img_name != "atlas-rt-sql":
                    errors.append(
                        f"images.{img_name}.starrocks: only allowed on 'atlas-rt-sql'"
                    )
                elif not isinstance(starrocks_val, str) or not _SEMVER_RE.match(
                    str(starrocks_val)
                ):
                    errors.append(
                        f"images.{img_name}.starrocks: must be semver, got '{starrocks_val}'"
                    )

            if isinstance(digest_val, str) and _DIGEST_RE.match(digest_val):
                parsed_images[img_name] = ImageRef(
                    digest=digest_val,
                    sbom=sbom_val if isinstance(sbom_val, str) else None,
                    signature=sig_val if isinstance(sig_val, str) else None,
                    starrocks=starrocks_val if isinstance(starrocks_val, str) else None,
                )

    # 7. engines
    engines = data.get("engines")
    if "engines" not in data:
        errors.append("engines: field is required")
    elif not isinstance(engines, Mapping):
        errors.append("engines: must be a mapping")
    else:
        for k in engines:
            if k not in ("duckdb", "duckdb_extensions"):
                errors.append(f"engines.{k}: unknown field")

        duckdb_val = engines.get("duckdb")
        if "duckdb" not in engines:
            errors.append("engines.duckdb: field is required")
        elif not isinstance(duckdb_val, str) or not _SEMVER_RE.match(duckdb_val):
            errors.append(f"engines.duckdb: must be semver, got '{duckdb_val}'")

        if "duckdb_extensions" in engines:
            exts = engines["duckdb_extensions"]
            if not isinstance(exts, Mapping):
                errors.append("engines.duckdb_extensions: must be a mapping")
            else:
                for ext_name, ext_hex in exts.items():
                    if not isinstance(ext_hex, str) or not _HEX_EXT_RE.match(ext_hex):
                        errors.append(
                            f"engines.duckdb_extensions.{ext_name}: must be a hex string of 8-64 characters"
                        )

    # 8. contracts
    contracts_data = data.get("contracts")
    parsed_contracts: dict[str, tuple[str, ...]] = {}
    if "contracts" not in data:
        errors.append("contracts: field is required")
    elif not isinstance(contracts_data, Mapping):
        errors.append("contracts: must be a mapping")
    else:
        for c_name, c_val in contracts_data.items():
            if c_name not in _ALLOWED_CONTRACTS:
                errors.append(
                    f"contracts.{c_name}: unknown contract key, must be one of {sorted(_ALLOWED_CONTRACTS)}"
                )
                continue

            if not isinstance(c_val, list) or len(c_val) == 0:
                errors.append(
                    f"contracts.{c_name}: must be a non-empty list of unique strings"
                )
                continue

            if not all(isinstance(x, str) for x in c_val):
                errors.append(f"contracts.{c_name}: all elements must be strings")
                continue

            if len(c_val) != len(set(c_val)):
                errors.append(f"contracts.{c_name}: elements must be unique")
                continue

            if c_name == "grant":
                if not set(c_val).issubset(_ALLOWED_GRANT):
                    errors.append(
                        f"contracts.grant: must be a subset of {sorted(_ALLOWED_GRANT)}, got {c_val}"
                    )
            elif c_name == "cube_mode":
                if not set(c_val).issubset(_ALLOWED_CUBE_MODE):
                    errors.append(
                        f"contracts.cube_mode: must be a subset of {sorted(_ALLOWED_CUBE_MODE)}, got {c_val}"
                    )

            parsed_contracts[c_name] = tuple(c_val)

    # 9. serving
    serving = data.get("serving")
    grant_max_lifetime: int | float = 0
    if "serving" not in data:
        errors.append("serving: field is required")
    elif not isinstance(serving, Mapping):
        errors.append("serving: must be a mapping")
    else:
        for k in serving:
            if k != "grant_max_lifetime_seconds":
                errors.append(f"serving.{k}: unknown field")

        if "grant_max_lifetime_seconds" not in serving:
            errors.append("serving.grant_max_lifetime_seconds: field is required")
        else:
            val = serving["grant_max_lifetime_seconds"]
            if (
                isinstance(val, bool)
                or not isinstance(val, (int, float))
                or not math.isfinite(val)
                or val <= 0
                or val > 60
            ):
                errors.append(
                    f"serving.grant_max_lifetime_seconds: must be a number in (0, 60], got {val}"
                )
            else:
                grant_max_lifetime = val

    # 10. acu_weights
    acu = data.get("acu_weights")
    parsed_acu: dict[str, float] = {}
    if "acu_weights" not in data:
        errors.append("acu_weights: field is required")
    elif not isinstance(acu, Mapping):
        errors.append("acu_weights: must be a mapping")
    else:
        for req_k in _REQUIRED_ACU_KEYS:
            if req_k not in acu:
                errors.append(f"acu_weights.{req_k}: required weight is missing")

        for k, v in acu.items():
            if (
                isinstance(v, bool)
                or not isinstance(v, (int, float))
                or not math.isfinite(v)
                or v <= 0
                or v > 10
            ):
                errors.append(
                    f"acu_weights.{k}: must be a finite positive number up to 10, got {v}"
                )
            else:
                parsed_acu[k] = float(v)

    # 11. licence_scan
    lic = data.get("licence_scan")
    if "licence_scan" not in data:
        errors.append("licence_scan: field is required")
    elif lic != "clean":
        errors.append(f"licence_scan: must be 'clean', got '{lic}'")

    if errors:
        raise ManifestError(errors)

    assert parsed_version is not None

    return RuntimeRelease(
        name=release_name,
        version=parsed_version,
        channel=release_channel,
        images=parsed_images,
        contracts=parsed_contracts,
        acu_weights=parsed_acu,
        grant_max_lifetime_seconds=grant_max_lifetime,
        raw=copy.deepcopy(dict(data)),
    )


def load_manifest(path: str | Path) -> RuntimeRelease:
    """Read and validate a manifest file from disk."""
    p = Path(path)
    if not p.is_file():
        raise ManifestError(f"Manifest file not found: {p}")

    content = p.read_text(encoding="utf-8")
    data = yaml.safe_load(content)
    if not isinstance(data, Mapping):
        raise ManifestError(["root: expected a mapping"])

    return parse_manifest(data)


def manifest_sha256(path: str | Path) -> str:
    """Return the SHA-256 of the manifest file bytes exactly as stored."""
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def supports(release: RuntimeRelease, contract: str, version: str) -> bool:
    """Check if the given release supports a specific contract and version."""
    supported_versions = release.contracts.get(contract, ())
    return version in supported_versions


def require_contracts(release: RuntimeRelease, required: Mapping[str, str]) -> None:
    """Verify that the release supports all required contract versions.

    Raises ContractMismatch naming every unsupported pair if any are missing.
    """
    unsupported: list[tuple[str, str]] = []
    for contract, version in required.items():
        if not supports(release, contract, version):
            unsupported.append((contract, version))

    if unsupported:
        raise ContractMismatch(unsupported)


def image_reference(release: RuntimeRelease, image: str, registry: str) -> str:
    """Return the fully-qualified image reference string.

    Format: <registry>/<image>@<digest>
    Raises KeyError if image is unknown.
    """
    if image not in release.images:
        raise KeyError(f"Image '{image}' not found in release '{release.name}'")

    reg = registry.rstrip("/")
    digest = release.images[image].digest
    return f"{reg}/{image}@{digest}"
