"""Tests for Atlas Runtime release manifest schema, validation, and contracts."""

import math
import tempfile
from pathlib import Path

import pytest
import yaml

from seeknal.runtime.release import (
    ContractMismatch,
    ImageRef,
    ManifestError,
    RuntimeRelease,
    image_reference,
    load_manifest,
    manifest_sha256,
    parse_manifest,
    require_contracts,
    supports,
)


def valid_manifest_dict():
    return {
        "apiVersion": "atlas.io/v1",
        "kind": "RuntimeRelease",
        "metadata": {
            "name": "atlas-rt/1.0.3",
            "channel": "stable",
        },
        "seeknal": {
            "version": "2.15.2",
            "commit": "0123456789abcdef0123456789abcdef01234567",
            "lock_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        },
        "python": "3.11.9",
        "images": {
            "atlas-rt": {
                "digest": f"sha256:{'a' * 64}",
                "sbom": "https://atlas.io/sbom/1.0.3",
                "signature": "cosign",
            },
            "atlas-rt-serving": {
                "digest": f"sha256:{'b' * 64}",
            },
            "atlas-rt-agent": {
                "digest": f"sha256:{'c' * 64}",
            },
            "atlas-rt-sql": {
                "digest": f"sha256:{'d' * 64}",
                "starrocks": "4.0.2",
            },
        },
        "engines": {
            "duckdb": "1.4.4",
            "duckdb_extensions": {
                "iceberg": "0123456789abcdef",
                "httpfs": "abcdef0123456789",
            },
        },
        "contracts": {
            "grant": ["v2", "v3"],
            "run_grant": ["v1"],
            "engine_api": ["v1"],
            "lineage": ["openlineage-1"],
            "cube_mode": ["refuse", "jwt"],
        },
        "serving": {
            "grant_max_lifetime_seconds": 60,
        },
        "acu_weights": {
            "job": 1.0,
            "session": 1.5,
            "warehouse": 2.0,
            "sandbox": 1.0,
        },
        "licence_scan": "clean",
    }


def test_valid_manifest_parse():
    data = valid_manifest_dict()
    release = parse_manifest(data)

    assert isinstance(release, RuntimeRelease)
    assert release.name == "atlas-rt/1.0.3"
    assert release.version == (1, 0, 3)
    assert release.channel == "stable"
    assert release.grant_max_lifetime_seconds == 60
    assert release.acu_weights["job"] == 1.0
    assert release.acu_weights["warehouse"] == 2.0

    assert len(release.images) == 4
    rt = release.images["atlas-rt"]
    assert isinstance(rt, ImageRef)
    assert rt.digest == f"sha256:{'a' * 64}"
    assert rt.sbom == "https://atlas.io/sbom/1.0.3"
    assert rt.signature == "cosign"

    sql = release.images["atlas-rt-sql"]
    assert sql.starrocks == "4.0.2"

    assert release.contracts["grant"] == ("v2", "v3")
    assert release.contracts["cube_mode"] == ("refuse", "jwt")
    assert release.raw["apiVersion"] == "atlas.io/v1"


def test_load_manifest_file_and_sha256():
    manifest_path = Path("src/seeknal/runtime/examples/atlas-rt-1.0.0.yaml")
    release = load_manifest(manifest_path)
    assert release.name == "atlas-rt/1.0.0"
    assert release.version == (1, 0, 0)

    sha = manifest_sha256(manifest_path)
    assert len(sha) == 64
    assert all(c in "0123456789abcdef" for c in sha)


def test_load_manifest_missing_file():
    with pytest.raises(ManifestError, match="not found"):
        load_manifest("nonexistent-manifest.yaml")


# --- Validation Rules: One failing case per rule ---


def test_rule_invalid_root_type():
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(["not", "a", "mapping"])
    assert any("root" in err for err in exc_info.value.errors)


def test_rule_unknown_top_level_key():
    data = valid_manifest_dict()
    data["unknown_field"] = "unexpected"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("unknown_field" in err for err in exc_info.value.errors)


def test_rule_missing_api_version():
    data = valid_manifest_dict()
    del data["apiVersion"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("apiVersion" in err for err in exc_info.value.errors)


def test_rule_invalid_api_version():
    data = valid_manifest_dict()
    data["apiVersion"] = "atlas.io/v2"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("apiVersion" in err for err in exc_info.value.errors)


def test_rule_missing_kind():
    data = valid_manifest_dict()
    del data["kind"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("kind" in err for err in exc_info.value.errors)


def test_rule_invalid_kind():
    data = valid_manifest_dict()
    data["kind"] = "DifferentRelease"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("kind" in err for err in exc_info.value.errors)


def test_rule_missing_metadata():
    data = valid_manifest_dict()
    del data["metadata"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("metadata" in err for err in exc_info.value.errors)


def test_rule_invalid_metadata_name():
    data = valid_manifest_dict()
    data["metadata"]["name"] = "atlas-rt/1.0"  # Missing patch
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("metadata.name" in err for err in exc_info.value.errors)


def test_rule_invalid_metadata_channel():
    data = valid_manifest_dict()
    data["metadata"]["channel"] = "canary"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("metadata.channel" in err for err in exc_info.value.errors)


def test_rule_unknown_metadata_key():
    data = valid_manifest_dict()
    data["metadata"]["extra"] = "val"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("metadata.extra" in err for err in exc_info.value.errors)


def test_rule_missing_seeknal():
    data = valid_manifest_dict()
    del data["seeknal"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("seeknal" in err for err in exc_info.value.errors)


def test_rule_invalid_seeknal_version():
    data = valid_manifest_dict()
    data["seeknal"]["version"] = "2.15"  # Not X.Y.Z
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("seeknal.version" in err for err in exc_info.value.errors)


def test_rule_invalid_seeknal_commit():
    data = valid_manifest_dict()
    data["seeknal"]["commit"] = "012345"  # Short, not 40-hex
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("seeknal.commit" in err for err in exc_info.value.errors)


def test_rule_invalid_seeknal_lock_sha256():
    data = valid_manifest_dict()
    data["seeknal"]["lock_sha256"] = "NOT_HEX_64"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("seeknal.lock_sha256" in err for err in exc_info.value.errors)


def test_rule_invalid_python():
    data = valid_manifest_dict()
    data["python"] = "3.11"  # Needs ^3\.\d+\.\d+$
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("python" in err for err in exc_info.value.errors)


def test_rule_missing_images():
    data = valid_manifest_dict()
    del data["images"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images" in err for err in exc_info.value.errors)


def test_rule_missing_required_atlas_rt_image():
    data = valid_manifest_dict()
    del data["images"]["atlas-rt"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt" in err for err in exc_info.value.errors)


def test_rule_missing_required_atlas_rt_serving_image():
    data = valid_manifest_dict()
    del data["images"]["atlas-rt-serving"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt-serving" in err for err in exc_info.value.errors)


def test_rule_disallowed_image_key():
    data = valid_manifest_dict()
    data["images"]["custom_container"] = {"digest": f"sha256:{'a' * 64}"}
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.custom_container" in err for err in exc_info.value.errors)


def test_rule_allowed_other_image_key():
    data = valid_manifest_dict()
    data["images"]["atlas-rt-worker-gpu"] = {"digest": f"sha256:{'a' * 64}"}
    release = parse_manifest(data)
    assert "atlas-rt-worker-gpu" in release.images


def test_rule_invalid_image_digest():
    data = valid_manifest_dict()
    data["images"]["atlas-rt"]["digest"] = "sha256:too_short"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt.digest" in err for err in exc_info.value.errors)


def test_rule_image_tag_rejected():
    data = valid_manifest_dict()
    data["images"]["atlas-rt"]["tag"] = "latest"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt.tag" in err for err in exc_info.value.errors)


def test_rule_invalid_sbom_type():
    data = valid_manifest_dict()
    data["images"]["atlas-rt"]["sbom"] = 12345
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt.sbom" in err for err in exc_info.value.errors)


def test_rule_invalid_signature():
    data = valid_manifest_dict()
    data["images"]["atlas-rt"]["signature"] = "gpg"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt.signature" in err for err in exc_info.value.errors)


def test_rule_starrocks_on_non_sql_image():
    data = valid_manifest_dict()
    data["images"]["atlas-rt"]["starrocks"] = "4.0.2"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt.starrocks" in err for err in exc_info.value.errors)


def test_rule_invalid_starrocks_version():
    data = valid_manifest_dict()
    data["images"]["atlas-rt-sql"]["starrocks"] = "4.0"  # Not semver
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt-sql.starrocks" in err for err in exc_info.value.errors)


def test_rule_unknown_field_in_image():
    data = valid_manifest_dict()
    data["images"]["atlas-rt"]["extra_attr"] = "val"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("images.atlas-rt.extra_attr" in err for err in exc_info.value.errors)


def test_rule_missing_engines():
    data = valid_manifest_dict()
    del data["engines"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("engines" in err for err in exc_info.value.errors)


def test_rule_invalid_engines_duckdb():
    data = valid_manifest_dict()
    data["engines"]["duckdb"] = "latest"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("engines.duckdb" in err for err in exc_info.value.errors)


def test_rule_invalid_duckdb_extensions():
    data = valid_manifest_dict()
    data["engines"]["duckdb_extensions"]["iceberg"] = "not_hex!"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any(
        "engines.duckdb_extensions.iceberg" in err for err in exc_info.value.errors
    )


def test_rule_duckdb_extensions_too_short():
    data = valid_manifest_dict()
    data["engines"]["duckdb_extensions"]["iceberg"] = "12345"  # < 8 chars
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any(
        "engines.duckdb_extensions.iceberg" in err for err in exc_info.value.errors
    )


def test_rule_missing_contracts():
    data = valid_manifest_dict()
    del data["contracts"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("contracts" in err for err in exc_info.value.errors)


def test_rule_unknown_contract_key():
    data = valid_manifest_dict()
    data["contracts"]["unknown_contract"] = ["v1"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("contracts.unknown_contract" in err for err in exc_info.value.errors)


def test_rule_empty_contract_list():
    data = valid_manifest_dict()
    data["contracts"]["grant"] = []
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("contracts.grant" in err for err in exc_info.value.errors)


def test_rule_non_unique_contract_list():
    data = valid_manifest_dict()
    data["contracts"]["grant"] = ["v2", "v2"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("contracts.grant" in err for err in exc_info.value.errors)


def test_rule_invalid_grant_contract_item():
    data = valid_manifest_dict()
    data["contracts"]["grant"] = ["v4"]  # Only v1, v2, v3 allowed
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("contracts.grant" in err for err in exc_info.value.errors)


def test_rule_invalid_cube_mode_contract_item():
    data = valid_manifest_dict()
    data["contracts"]["cube_mode"] = ["unknown_mode"]  # Only refuse, jwt allowed
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("contracts.cube_mode" in err for err in exc_info.value.errors)


def test_rule_missing_serving_grant_max_lifetime():
    data = valid_manifest_dict()
    del data["serving"]["grant_max_lifetime_seconds"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any(
        "serving.grant_max_lifetime_seconds" in err for err in exc_info.value.errors
    )


def test_rule_serving_grant_max_lifetime_zero():
    data = valid_manifest_dict()
    data["serving"]["grant_max_lifetime_seconds"] = 0
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any(
        "serving.grant_max_lifetime_seconds" in err for err in exc_info.value.errors
    )


def test_rule_serving_grant_max_lifetime_exceeds_60():
    data = valid_manifest_dict()
    data["serving"]["grant_max_lifetime_seconds"] = 61
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any(
        "serving.grant_max_lifetime_seconds" in err for err in exc_info.value.errors
    )


def test_rule_missing_required_acu_weight():
    data = valid_manifest_dict()
    del data["acu_weights"]["sandbox"]
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("acu_weights.sandbox" in err for err in exc_info.value.errors)


def test_rule_invalid_acu_weight_non_positive():
    data = valid_manifest_dict()
    data["acu_weights"]["job"] = 0.0
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("acu_weights.job" in err for err in exc_info.value.errors)


def test_rule_invalid_acu_weight_over_10():
    data = valid_manifest_dict()
    data["acu_weights"]["session"] = 10.5
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("acu_weights.session" in err for err in exc_info.value.errors)


def test_rule_invalid_acu_weight_not_finite():
    data = valid_manifest_dict()
    data["acu_weights"]["warehouse"] = float("inf")
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("acu_weights.warehouse" in err for err in exc_info.value.errors)


def test_rule_invalid_licence_scan():
    data = valid_manifest_dict()
    data["licence_scan"] = "dirty"
    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)
    assert any("licence_scan" in err for err in exc_info.value.errors)


# --- Multi-error collection ---


def test_multi_error_collection():
    data = valid_manifest_dict()
    data["apiVersion"] = "invalid-version"
    data["metadata"]["channel"] = "bad-channel"
    data["licence_scan"] = "failed"
    del data["acu_weights"]["sandbox"]

    with pytest.raises(ManifestError) as exc_info:
        parse_manifest(data)

    errors = exc_info.value.errors
    assert len(errors) >= 4

    error_text = str(exc_info.value)
    assert "apiVersion" in error_text
    assert "metadata.channel" in error_text
    assert "licence_scan" in error_text
    assert "acu_weights.sandbox" in error_text


# --- Contracts checks ---


def test_supports_contract():
    release = parse_manifest(valid_manifest_dict())
    assert supports(release, "grant", "v2") is True
    assert supports(release, "grant", "v3") is True
    assert supports(release, "grant", "v1") is False
    assert supports(release, "unknown_contract", "v1") is False


def test_require_contracts_success():
    release = parse_manifest(valid_manifest_dict())
    # Should not raise
    require_contracts(release, {"grant": "v2", "run_grant": "v1", "cube_mode": "jwt"})


def test_require_contracts_failure():
    release = parse_manifest(valid_manifest_dict())
    with pytest.raises(ContractMismatch) as exc_info:
        require_contracts(release, {"grant": "v1", "cube_mode": "other"})

    assert "grant=v1" in str(exc_info.value)
    assert "cube_mode=other" in str(exc_info.value)
    assert exc_info.value.unsupported == [("grant", "v1"), ("cube_mode", "other")]


# --- Image reference helper ---


def test_image_reference():
    release = parse_manifest(valid_manifest_dict())
    ref = image_reference(release, "atlas-rt", "ghcr.io/mta-tech")
    assert ref == f"ghcr.io/mta-tech/atlas-rt@sha256:{'a' * 64}"

    # Strips trailing slash
    ref_slash = image_reference(release, "atlas-rt", "ghcr.io/mta-tech/")
    assert ref_slash == f"ghcr.io/mta-tech/atlas-rt@sha256:{'a' * 64}"


def test_image_reference_unknown_image():
    release = parse_manifest(valid_manifest_dict())
    with pytest.raises(KeyError, match="unknown-image"):
        image_reference(release, "unknown-image", "ghcr.io/mta-tech")
