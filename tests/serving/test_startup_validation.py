"""Tests for fail-closed startup validation and the validate CLI helper."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.server.config import ServerConfig, ServerConfigError
from seeknal.serving.startup_validation import (
    StartupValidationError,
    ValidationReport,
    main as validate_main,
    run_validate_cli,
    validate_startup,
)


@pytest.fixture
def jwks_file(tmp_path: Path) -> str:
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pn = pub.public_numbers()
    x_b64 = base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    y_b64 = base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": x_b64,
        "y": y_b64,
        "kid": "startup-val-key",
    }
    p = tmp_path / "grants.jwks.json"
    p.write_text(json.dumps({"keys": [jwk]}), encoding="utf-8")
    return str(p)


@pytest.fixture
def base_env() -> dict[str, str]:
    return {
        "TEST_LB_DSN": "postgresql://svc:CANARY_PG_PW_987654@lakebase.internal:5432/atlas",
        "TEST_SR_PW": "CANARY_SR_PASSWORD_112233",
        "TEST_LK_ID": "lk-client-id",
        "TEST_LK_SECRET": "CANARY_LK_OAUTH_SECRET_445566",
        "TEST_S3_KEY": "AKIAIOSFODNN7EXAMPLE",
        "TEST_S3_SECRET": "CANARY_S3_SECRET_KEY_778899",
    }


def _write_static_bundle(
    tmp_path: Path,
    jwks_file: str,
    *,
    placement_data: dict,
    schemas_data: dict,
    connections_data: dict | None = None,
    extra_server_data: dict | None = None,
) -> Path:
    pf = tmp_path / "placement.yaml"
    sf = tmp_path / "schemas.json"
    cf = tmp_path / "serving.yaml"

    pf.write_text(yaml.safe_dump(placement_data), encoding="utf-8")
    sf.write_text(json.dumps(schemas_data), encoding="utf-8")

    if connections_data is None:
        connections_data = {
            "lb": {"kind": "lakebase", "dsn_env": "TEST_LB_DSN"},
            "sr": {
                "kind": "starrocks",
                "accounts": {
                    "interactive": {
                        "host": "sr.internal",
                        "port": 9030,
                        "user": "svc",
                        "password_env": "TEST_SR_PW",
                    }
                },
            },
            "duck": {"kind": "duckdb", "path": ":memory:"},
            "cube": {
                "kind": "cube",
                "host": "cube.internal",
                "port": 15432,
                "dbname": "cube",
                "sql_user": "seeknal",
            },
        }

    srv_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {
            "static": {
                "placement_file": str(pf),
                "schemas_file": str(sf),
            }
        },
        "connections": connections_data,
    }
    if extra_server_data:
        srv_dict.update(extra_server_data)

    cf.write_text(yaml.safe_dump(srv_dict), encoding="utf-8")
    return cf


def _iceberg_direct_conn(tmp_path: Path) -> dict:
    ext_dir = tmp_path / "duck_exts"
    ext_dir.mkdir(exist_ok=True)
    return {
        "kind": "duckdb",
        "profile": "iceberg_direct",
        "extension_directory": str(ext_dir),
        "extensions": ["httpfs", "iceberg"],
        "allowed_s3_prefixes": ["s3://atlas-wh/clinical/"],
        "recycle_seconds": 120,
        "iceberg_catalogs": {
            "lake": {
                "endpoint": "https://lk.internal:8181",
                "warehouse": "main",
                "token_uri": "https://idp.internal/token",
                "client_id_env": "TEST_LK_ID",
                "client_secret_env": "TEST_LK_SECRET",
                "access_delegation": "vended",
                "s3": {
                    "endpoint": "s3.internal:9000",
                    "region": "us-east-1",
                    "key_id_env": "TEST_S3_KEY",
                    "secret_env": "TEST_S3_SECRET",
                    "use_ssl": True,
                },
            }
        },
    }


def test_validate_startup_static_ok(tmp_path: Path, jwks_file: str, base_env: dict[str, str]):
    cfg_path = _write_static_bundle(
        tmp_path,
        jwks_file,
        placement_data={
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
                "cube": {"kind": "cube", "dialect": "postgres"},
            },
            "tables": {
                "claims.claim": {
                    "keys": ["claim_id"],
                    "row_count": 50000,
                    "serve": {"lb": "public.claim", "sr": "lk.claims.claim"},
                },
                "clinical.patients": {
                    "keys": ["PATIENT_ID"],  # case-insensitive key match against patient_id
                    "pii": True,
                    "serve": {"lb": "public.patients"},
                },
            },
            "cubes": {
                "claims_cube": {
                    "engine": "cube",
                    "measures": ["total_amount"],
                    "dimensions": ["province"],
                    "mask_variants": {"province": ["redact"]},
                }
            },
        },
        schemas_data={
            "claims.claim": ["claim_id", "amount", "province"],
            "clinical.patients": ["patient_id", "ssn"],
            "claims_cube": ["province", "total_amount"],
        },
    )

    report = validate_startup(cfg_path, env=base_env)
    assert isinstance(report, ValidationReport)
    assert report.ok is True
    assert report.mode == "static"
    assert report.skipped == ()
    assert "no_static_role_references" in report.notes
    assert report.static_role_references_checked == 0
    assert report.placement is not None
    assert report.placement.tables["clinical.patients"].pii is True

    # Also accepts pre-parsed ServerConfig
    srv_cfg = ServerConfig.from_yaml(cfg_path, env=base_env)
    report2 = validate_startup(srv_cfg, env=base_env)
    assert report2.ok is True


def test_validate_startup_discovery_offline_ok_and_skipped(
    tmp_path: Path, jwks_file: str, base_env: dict[str, str], capsys
):
    cf = tmp_path / "serving_disc.yaml"
    cf.write_text(
        yaml.safe_dump(
            {
                "listen": {"host": "127.0.0.1", "port": 8765},
                "grant_keys": {"jwks_file": jwks_file},
                "placement": {
                    "discovery": {
                        "default_catalog": "lakekeeper",
                        "lakekeeper": {
                            "url": "https://lk.internal:8181",
                            "warehouse": "main",
                        },
                        "engines": {
                            "sr": {"kind": "starrocks", "dialect": "starrocks"},
                            "lb": {"kind": "lakebase", "dialect": "postgres"},
                        },
                        "lakebase": {
                            "engine": "lb",
                            "synced_tables": {
                                "lakekeeper.claims.claim": {
                                    "target": "public.claim",
                                    "primary_key": ["claim_id"],
                                }
                            },
                        },
                    }
                },
                "connections": {
                    "lb": {"kind": "lakebase", "dsn_env": "TEST_LB_DSN"},
                    "sr": {
                        "kind": "starrocks",
                        "accounts": {
                            "interactive": {
                                "host": "sr.internal",
                                "port": 9030,
                                "user": "svc",
                                "password_env": "TEST_SR_PW",
                            }
                        },
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    report = validate_startup(cf, env=base_env)
    assert report.ok is True
    assert report.mode == "discovery"
    assert report.skipped == ("live_catalog_discovery",)
    assert "no_static_role_references" in report.notes

    rc = run_validate_cli(cf, env=base_env)
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out.splitlines()[0] == "ok"
    assert "live_catalog_discovery" in captured.out
    assert captured.err == ""


def test_unknown_engine_refusals(tmp_path: Path, jwks_file: str, base_env: dict[str, str]):
    # 1. Table serve unknown engine + Cube unknown engine + policy.pii_engines unknown engine
    # + placement engine missing from connections + engine kind mismatch + invalid policy order kind
    cfg_path = _write_static_bundle(
        tmp_path,
        jwks_file,
        placement_data={
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "unwired_duck": {"kind": "duckdb", "dialect": "duckdb"},
                "mismatched_sr": {"kind": "starrocks", "dialect": "starrocks"},
            },
            "tables": {
                "claims.claim": {
                    "keys": ["claim_id"],
                    "serve": {"ghost_serve_eng": "public.claim"},
                }
            },
            "cubes": {
                "claims_cube": {
                    "engine": "ghost_cube_eng",
                    "measures": ["total_amount"],
                    "dimensions": ["province"],
                }
            },
            "policy": {
                "pii_engines": ["ghost_pii_eng"],
                "analytics_order": ["starrocks", "clickhouse"],
            },
        },
        schemas_data={
            "claims.claim": ["claim_id"],
            "claims_cube": ["province", "total_amount"],
        },
        connections_data={
            "lb": {"kind": "lakebase", "dsn_env": "TEST_LB_DSN"},
            "mismatched_sr": {"kind": "duckdb", "path": ":memory:"},
        },
    )

    with pytest.raises(StartupValidationError) as exc_info:
        validate_startup(cfg_path, env=base_env)

    assert isinstance(exc_info.value, ServerConfigError)
    joined = "\n".join(exc_info.value.problems)
    assert "ghost_serve_eng" in joined
    assert "ghost_cube_eng" in joined
    assert "ghost_pii_eng" in joined
    assert "unwired_duck" in joined
    assert "mismatched_sr" in joined
    assert "clickhouse" in joined

    # 2. Discovery pii_engines referencing unknown engine
    disc_cfg = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {
            "discovery": {
                "default_catalog": "lakekeeper",
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "pii_engines": ["nonexistent_duck"],
            }
        },
        "connections": {"lb": {"kind": "lakebase", "dsn_env": "TEST_LB_DSN"}},
    }
    with pytest.raises(StartupValidationError, match="nonexistent_duck"):
        validate_startup(disc_cfg, env=base_env)


def test_unknown_table_refusals(tmp_path: Path, jwks_file: str, base_env: dict[str, str]):
    cfg_path = _write_static_bundle(
        tmp_path,
        jwks_file,
        placement_data={
            "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
            "tables": {
                "claims.claim": {"keys": ["claim_id"], "serve": {"lb": "public.claim"}},
                "claims.unschematized": {"keys": [], "serve": {"lb": "public.unschematized"}},
            },
            "policy": {
                "pii_tables": ["claims.ghost_pii_table"],
                "masks": {"claims.ghost_masked_table": {"col_a": "redact"}},
            },
        },
        schemas_data={
            "claims.claim": ["claim_id"],
            "claims.stale_extra_schema_table": ["id", "name"],
        },
    )

    with pytest.raises(StartupValidationError) as exc_info:
        validate_startup(cfg_path, env=base_env)

    joined = "\n".join(exc_info.value.problems)
    assert "claims.stale_extra_schema_table" in joined
    assert "claims.unschematized" in joined
    assert "claims.ghost_pii_table" in joined
    assert "claims.ghost_masked_table" in joined


def test_unknown_column_refusals(tmp_path: Path, jwks_file: str, base_env: dict[str, str]):
    cfg_path = _write_static_bundle(
        tmp_path,
        jwks_file,
        placement_data={
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "cube": {"kind": "cube", "dialect": "postgres"},
            },
            "tables": {
                "claims.claim": {
                    "keys": ["missing_pk_col"],
                    "serve": {"lb": "public.claim"},
                    "masks": {"missing_mask_col": "redact"},
                    "row_filters": [
                        {"column": "missing_filter_col", "op": "eq", "values": ["ON"]}
                    ],
                }
            },
            "cubes": {
                "claims_cube": {
                    "engine": "cube",
                    "measures": ["missing_measure_col"],
                    "dimensions": ["province", "missing_dim_col"],
                    "mask_variants": {"missing_variant_base": ["redact"]},
                }
            },
        },
        schemas_data={
            "claims.claim": ["claim_id", "amount"],
            "claims_cube": ["province", "total_amount"],
        },
    )

    with pytest.raises(StartupValidationError) as exc_info:
        validate_startup(cfg_path, env=base_env)

    joined = "\n".join(exc_info.value.problems)
    assert "missing_pk_col" in joined
    assert "missing_mask_col" in joined
    assert "missing_filter_col" in joined
    assert "missing_measure_col" in joined
    assert "missing_dim_col" in joined
    assert "missing_variant_base" in joined


def test_unknown_role_refusal_and_valid_role_policy(
    tmp_path: Path, jwks_file: str, base_env: dict[str, str]
):
    # 1. Unbacked role references in placement and top-level config are refused
    cfg_bad_roles = _write_static_bundle(
        tmp_path,
        jwks_file,
        placement_data={
            "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
            "tables": {
                "claims.claim": {
                    "keys": ["claim_id"],
                    "serve": {"lb": "public.claim"},
                    "roles": ["ghost_table_role"],
                }
            },
            "policy": {"default_role": "ghost_default_role"},
        },
        schemas_data={"claims.claim": ["claim_id"]},
        extra_server_data={"role_bindings": {"alice": ["ghost_bound_role"]}},
    )

    with pytest.raises(StartupValidationError) as exc_info:
        validate_startup(cfg_bad_roles, env=base_env)

    joined = "\n".join(exc_info.value.problems)
    assert "ghost_table_role" in joined
    assert "ghost_default_role" in joined
    assert "ghost_bound_role" in joined

    # 2. Matching role policy passes and validates table/column references inside role policy
    valid_role_dir = tmp_path / "valid_role"
    valid_role_dir.mkdir()
    cfg_valid_role = _write_static_bundle(
        valid_role_dir,
        jwks_file,
        placement_data={
            "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
            "tables": {
                "claims.claim": {
                    "keys": ["claim_id"],
                    "serve": {"lb": "public.claim"},
                    "roles": ["analyst"],
                }
            },
            "policy": {
                "roles": {
                    "analyst": {
                        "tables": {
                            "claims.claim": {
                                "columns": ["claim_id"],
                                "masks": {"ssn": "redact"},
                            }
                        }
                    }
                }
            },
        },
        schemas_data={"claims.claim": ["claim_id", "ssn"]},
    )
    rep = validate_startup(cfg_valid_role, env=base_env)
    assert rep.ok is True
    assert rep.static_role_references_checked == 1
    assert "no_static_role_references" not in rep.notes


def test_pii_rules_in_startup_validation_two_serves_local_file_and_iceberg_direct(
    tmp_path: Path, jwks_file: str, base_env: dict[str, str]
):
    connections = {
        "lb": {"kind": "lakebase", "dsn_env": "TEST_LB_DSN"},
        "sr": {
            "kind": "starrocks",
            "accounts": {
                "interactive": {
                    "host": "sr.internal",
                    "port": 9030,
                    "user": "svc",
                    "password_env": "TEST_SR_PW",
                }
            },
        },
        "duck_local": {"kind": "duckdb", "path": ":memory:"},
        "duck_ice": _iceberg_direct_conn(tmp_path),
    }

    # 1. Refusal of two serves, duckdb local_file without pii_engines, and duckdb iceberg_direct without pii_engines
    bad_dir = tmp_path / "pii_bad"
    bad_dir.mkdir()
    cfg_bad = _write_static_bundle(
        bad_dir,
        jwks_file,
        placement_data={
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
                "duck_local": {"kind": "duckdb", "dialect": "duckdb"},
                "duck_ice": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "clinical.two_serves": {
                    "keys": ["id"],
                    "pii": True,
                    "serve": {"lb": "public.t1", "sr": "lk.clinical.t1"},
                },
                "clinical.on_local_duck": {
                    "keys": ["id"],
                    "pii": True,
                    "serve": {"duck_local": "main.t2"},
                },
                "clinical.on_iceberg_duck": {
                    "keys": ["id"],
                    "pii": True,
                    "serve": {"duck_ice": "lake.clinical.t3"},
                },
            },
        },
        schemas_data={
            "clinical.two_serves": ["id"],
            "clinical.on_local_duck": ["id"],
            "clinical.on_iceberg_duck": ["id"],
        },
        connections_data=connections,
    )

    with pytest.raises(StartupValidationError) as exc_info:
        validate_startup(cfg_bad, env=base_env)

    joined = "\n".join(exc_info.value.problems)
    assert "clinical.two_serves" in joined
    assert "exactly one serve, got 2" in joined
    assert "clinical.on_local_duck" in joined
    assert "duck_local" in joined
    assert "local_file" in joined
    assert "clinical.on_iceberg_duck" in joined
    assert "duck_ice" in joined
    assert "iceberg_direct" in joined

    # 2. Allowed when both duck_local and duck_ice are explicitly listed in policy.pii_engines
    ok_dir = tmp_path / "pii_ok"
    ok_dir.mkdir()
    cfg_ok = _write_static_bundle(
        ok_dir,
        jwks_file,
        placement_data={
            "engines": {
                "duck_local": {"kind": "duckdb", "dialect": "duckdb"},
                "duck_ice": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "clinical.on_local_duck": {
                    "keys": ["id"],
                    "pii": True,
                    "serve": {"duck_local": "main.t2"},
                },
                "clinical.on_iceberg_duck": {
                    "keys": ["id"],
                    "pii": True,
                    "serve": {"duck_ice": "lake.clinical.t3"},
                },
            },
            "policy": {"pii_engines": ["duck_local", "duck_ice"]},
        },
        schemas_data={
            "clinical.on_local_duck": ["id"],
            "clinical.on_iceberg_duck": ["id"],
        },
        connections_data={
            "duck_local": connections["duck_local"],
            "duck_ice": connections["duck_ice"],
        },
    )

    rep = validate_startup(cfg_ok, env=base_env)
    assert rep.ok is True
    assert rep.placement is not None
    assert rep.placement.policy.pii_engines == ("duck_local", "duck_ice")


def test_cli_exit_codes_and_no_secret_leak(
    tmp_path: Path, jwks_file: str, base_env: dict[str, str], monkeypatch, capsys
):
    # 1. Valid static config -> exit code 0, prints 'ok'
    ok_dir = tmp_path / "cli_ok"
    ok_dir.mkdir()
    cfg_ok = _write_static_bundle(
        ok_dir,
        jwks_file,
        placement_data={
            "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
            "tables": {"claims.claim": {"keys": ["claim_id"], "serve": {"lb": "public.claim"}}},
        },
        schemas_data={"claims.claim": ["claim_id"]},
        connections_data={"lb": {"kind": "lakebase", "dsn_env": "TEST_LB_DSN"}},
    )
    for k, v in base_env.items():
        monkeypatch.setenv(k, v)

    rc_ok = validate_main(["--config", str(cfg_ok)])
    assert rc_ok == 0
    cap_ok = capsys.readouterr()
    assert cap_ok.out.strip() == "ok"
    assert cap_ok.err == ""

    # 2. Invalid config with multiple errors + secrets in env & secrets_env_file + URI in bad name -> exit 2, zero leaks
    bad_dir = tmp_path / "cli_bad"
    bad_dir.mkdir()
    secrets_file = bad_dir / "secrets.env"
    file_canary = "FILE_SECRET_VALUE_889900"
    secrets_file.write_text(f"EXTRA_FILE_SECRET={file_canary}\n", encoding="utf-8")
    secrets_file.chmod(0o600)

    monkeypatch.delenv("MISSING_SR_PW_ENV", raising=False)
    cfg_bad = _write_static_bundle(
        bad_dir,
        jwks_file,
        placement_data={
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "duck": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "clinical.patients": {
                    "keys": ["nonexistent_key"],
                    "pii": True,
                    "serve": {
                        "lb": "public.patients",
                        "duck": "main.patients",
                        "postgresql://admin:INLINE_DSN_SECRET_555@db:5432/prod": "x",
                    },
                }
            },
        },
        schemas_data={
            "clinical.patients": ["id"],
            "stale.unknown_tbl": ["id"],
        },
        connections_data={
            "lb": {"kind": "lakebase", "dsn_env": "TEST_LB_DSN"},
            "duck": {"kind": "duckdb", "path": ":memory:"},
            "sr": {
                "kind": "starrocks",
                "accounts": {
                    "interactive": {
                        "host": "sr.internal",
                        "port": 9030,
                        "user": "svc",
                        "password_env": "MISSING_SR_PW_ENV",
                    }
                },
            },
        },
        extra_server_data={"secrets_env_file": str(secrets_file)},
    )

    rc_bad = run_validate_cli(cfg_bad, env=base_env)
    assert rc_bad == 2
    cap_bad = capsys.readouterr()
    assert cap_bad.out == ""

    err_lines = [line for line in cap_bad.err.strip().splitlines() if line]
    assert len(err_lines) >= 4
    assert all(line.startswith("Error: ") for line in err_lines)

    # Clear actionable problem identifiers present
    assert "MISSING_SR_PW_ENV" in cap_bad.err
    assert "nonexistent_key" in cap_bad.err
    assert "stale.unknown_tbl" in cap_bad.err
    assert "clinical.patients" in cap_bad.err

    # Secrets, DSNs, file secrets, and inline credentials never leak
    for forbidden in (
        "CANARY_PG_PW_987654",
        "CANARY_SR_PASSWORD_112233",
        "CANARY_LK_OAUTH_SECRET_445566",
        "CANARY_S3_SECRET_KEY_778899",
        file_canary,
        "INLINE_DSN_SECRET_555",
        "postgresql://svc:",
        "Traceback",
    ):
        assert forbidden not in cap_bad.err
        assert forbidden not in cap_bad.out


def test_role_policy_unknown_table_and_column_and_discovery_unapproved_duckdb_pii(
    tmp_path: Path, jwks_file: str, base_env: dict[str, str]
):
    role_bad_dir = tmp_path / "role_bad_refs"
    role_bad_dir.mkdir()
    cfg_role_bad = _write_static_bundle(
        role_bad_dir,
        jwks_file,
        placement_data={
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
            },
            "tables": {
                "claims.claim": {
                    "keys": ["claim_id"],
                    "serve": {"lb": "public.claim"},
                    "roles": ["analyst"],
                }
            },
            "cubes": {
                "bad_kind_cube": {
                    "engine": "sr",  # not of kind 'cube'
                    "measures": ["m1"],
                    "dimensions": ["d1"],
                }
            },
            "policy": {
                "small_scan_order": ["lakebase", "snowflake"],
                "row_filters": {
                    "claims.claim": [
                        {"column": "policy_missing_filter_col", "op": "eq", "values": [1]}
                    ]
                },
                "roles": {
                    "analyst": {
                        "tables": {
                            "claims.nonexistent_role_table": {"columns": ["id"]},
                            "claims.claim": {"masks": {"role_missing_mask_col": "redact"}},
                        }
                    }
                },
            },
        },
        schemas_data={
            "claims.claim": ["claim_id"],
            "bad_kind_cube": ["d1", "m1"],
        },
    )

    with pytest.raises(StartupValidationError) as exc_info:
        validate_startup(cfg_role_bad, env=base_env)

    joined = "\n".join(exc_info.value.problems)
    assert "claims.nonexistent_role_table" in joined
    assert "role_missing_mask_col" in joined
    assert "policy_missing_filter_col" in joined
    assert "snowflake" in joined
    assert "bad_kind_cube" in joined

    # Discovery offline PII check: pii_tables declared when only unapproved duckdb engine exists
    disc_duck_unapproved = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {
            "discovery": {
                "default_catalog": "lakekeeper",
                "engines": {
                    "duck_ice": {
                        "kind": "duckdb",
                        "dialect": "duckdb",
                        "profile": "iceberg_direct",
                        "alias": "lake",
                    }
                },
                "pii_tables": ["lakekeeper.clinical.patients"],
            }
        },
        "connections": {"duck_ice": _iceberg_direct_conn(tmp_path)},
    }
    with pytest.raises(StartupValidationError, match="duck_ice.*pii_engines"):
        validate_startup(disc_duck_unapproved, env=base_env)

    # Approved when duck_ice is listed in pii_engines
    disc_duck_approved = {
        **disc_duck_unapproved,
        "placement": {
            "discovery": {
                **disc_duck_unapproved["placement"]["discovery"],
                "pii_engines": ["duck_ice"],
            }
        },
    }
    rep = validate_startup(disc_duck_approved, env=base_env)
    assert rep.ok is True
    assert rep.skipped == ("live_catalog_discovery",)


def test_build_server_and_main_validate_cli_refuse_every_violation_and_accept_valid(
    tmp_path: Path, jwks_file: str, base_env: dict[str, str], monkeypatch, capsys
):
    from unittest.mock import MagicMock

    from seeknal.serving.__main__ import main as serving_cli_main
    from seeknal.serving.server.app import build_server

    for k, v in base_env.items():
        monkeypatch.setenv(k, v)

    all_connections = {
        "lb": {"kind": "lakebase", "dsn_env": "TEST_LB_DSN"},
        "sr": {
            "kind": "starrocks",
            "accounts": {
                "interactive": {
                    "host": "sr.internal",
                    "port": 9030,
                    "user": "svc",
                    "password_env": "TEST_SR_PW",
                }
            },
        },
        "duck_local": {"kind": "duckdb", "path": ":memory:"},
        "duck_ice": _iceberg_direct_conn(tmp_path),
        "cube": {
            "kind": "cube",
            "host": "cube.internal",
            "port": 15432,
            "dbname": "cube",
            "sql_user": "seeknal",
        },
    }

    refusal_cases: list[tuple[str, dict, dict, dict | None, dict | None, str]] = [
        (
            "unknown_engine_serve",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"claims.claim": {"keys": ["id"], "serve": {"missing_eng": "public.claim"}}},
            },
            {"claims.claim": ["id"]},
            all_connections,
            None,
            "missing_eng",
        ),
        (
            "unknown_engine_cube",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"claims.claim": {"keys": ["id"], "serve": {"lb": "public.claim"}}},
                "cubes": {"c1": {"engine": "missing_cube_eng", "measures": ["m"], "dimensions": ["d"]}},
            },
            {"claims.claim": ["id"], "c1": ["d", "m"]},
            all_connections,
            None,
            "missing_cube_eng",
        ),
        (
            "unknown_engine_pii_engines",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"claims.claim": {"keys": ["id"], "serve": {"lb": "public.claim"}}},
                "policy": {"pii_engines": ["missing_pii_eng"]},
            },
            {"claims.claim": ["id"]},
            all_connections,
            None,
            "missing_pii_eng",
        ),
        (
            "missing_connection",
            {
                "engines": {"unwired_eng": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {"claims.claim": {"keys": ["id"], "serve": {"unwired_eng": "claim"}}},
            },
            {"claims.claim": ["id"]},
            {"lb": all_connections["lb"]},
            None,
            "unwired_eng",
        ),
        (
            "engine_kind_mismatch",
            {
                "engines": {"lb": {"kind": "starrocks", "dialect": "starrocks"}},
                "tables": {"claims.claim": {"keys": ["id"], "serve": {"lb": "lk.claim"}}},
            },
            {"claims.claim": ["id"]},
            {"lb": all_connections["lb"]},
            None,
            "kind mismatch",
        ),
        (
            "unknown_table_extra_schema",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"claims.claim": {"keys": ["id"], "serve": {"lb": "public.claim"}}},
            },
            {"claims.claim": ["id"], "claims.extra_ghost_tbl": ["id"]},
            all_connections,
            None,
            "claims.extra_ghost_tbl",
        ),
        (
            "unknown_table_policy_ref",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"claims.claim": {"keys": ["id"], "serve": {"lb": "public.claim"}}},
                "policy": {"pii_tables": ["claims.ghost_policy_tbl"]},
            },
            {"claims.claim": ["id"]},
            all_connections,
            None,
            "claims.ghost_policy_tbl",
        ),
        (
            "unknown_column_keys",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"claims.claim": {"keys": ["missing_key_col"], "serve": {"lb": "public.claim"}}},
            },
            {"claims.claim": ["id"]},
            all_connections,
            None,
            "missing_key_col",
        ),
        (
            "unknown_column_masks",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {
                    "claims.claim": {
                        "keys": ["id"],
                        "serve": {"lb": "public.claim"},
                        "masks": {"missing_mask_col": "redact"},
                    }
                },
            },
            {"claims.claim": ["id"]},
            all_connections,
            None,
            "missing_mask_col",
        ),
        (
            "unknown_column_row_filters",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {
                    "claims.claim": {
                        "keys": ["id"],
                        "serve": {"lb": "public.claim"},
                        "row_filters": [{"column": "missing_rf_col", "op": "eq", "values": [1]}],
                    }
                },
            },
            {"claims.claim": ["id"]},
            all_connections,
            None,
            "missing_rf_col",
        ),
        (
            "unknown_role_in_placement",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {
                    "claims.claim": {
                        "keys": ["id"],
                        "serve": {"lb": "public.claim"},
                        "roles": ["unbacked_analyst_role"],
                    }
                },
            },
            {"claims.claim": ["id"]},
            all_connections,
            None,
            "unbacked_analyst_role",
        ),
        (
            "unknown_role_top_level",
            {
                "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                "tables": {"claims.claim": {"keys": ["id"], "serve": {"lb": "public.claim"}}},
            },
            {"claims.claim": ["id"]},
            all_connections,
            {"default_role": "unbacked_top_role"},
            "unbacked_top_role",
        ),
        (
            "pii_two_serves",
            {
                "engines": {
                    "lb": {"kind": "lakebase", "dialect": "postgres"},
                    "sr": {"kind": "starrocks", "dialect": "starrocks"},
                },
                "tables": {
                    "clinical.patients": {
                        "keys": ["id"],
                        "pii": True,
                        "serve": {"lb": "public.patients", "sr": "lk.clinical.patients"},
                    }
                },
            },
            {"clinical.patients": ["id"]},
            all_connections,
            None,
            "exactly one serve",
        ),
        (
            "pii_duckdb_local_file",
            {
                "engines": {"duck_local": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {
                    "clinical.patients": {
                        "keys": ["id"],
                        "pii": True,
                        "serve": {"duck_local": "main.patients"},
                    }
                },
            },
            {"clinical.patients": ["id"]},
            all_connections,
            None,
            "duck_local",
        ),
        (
            "pii_duckdb_iceberg_direct",
            {
                "engines": {"duck_ice": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {
                    "clinical.patients": {
                        "keys": ["id"],
                        "pii": True,
                        "serve": {"duck_ice": "lake.clinical.patients"},
                    }
                },
            },
            {"clinical.patients": ["id"]},
            all_connections,
            None,
            "duck_ice",
        ),
    ]

    for case_name, pl_data, sch_data, conns, extra_srv, expected_substr in refusal_cases:
        case_dir = tmp_path / f"case_{case_name}"
        case_dir.mkdir()
        cfg_file = _write_static_bundle(
            case_dir,
            jwks_file,
            placement_data=pl_data,
            schemas_data=sch_data,
            connections_data=conns,
            extra_server_data=extra_srv,
        )

        # 1. build_server refuses to start with ServerConfigError
        with pytest.raises(ServerConfigError) as exc_bs:
            build_server(cfg_file, env=base_env)
        assert expected_substr in str(exc_bs.value), f"{case_name}: {exc_bs.value}"

        # 2. `python -m seeknal.serving validate --config X` exits 2 with clear message and no secrets
        capsys.readouterr()
        rc_val = serving_cli_main(["validate", "--config", str(cfg_file)])
        assert rc_val == 2, f"{case_name} validate should exit 2"
        cap_val = capsys.readouterr()
        assert cap_val.out == ""
        assert "Error: " in cap_val.err
        assert expected_substr in cap_val.err
        assert "CANARY_PG_PW_987654" not in cap_val.err
        assert "CANARY_SR_PASSWORD_112233" not in cap_val.err
        assert "Traceback" not in cap_val.err

        # 3. `python -m seeknal.serving --config X` refuses server startup before calling uvicorn.run
        fake_uvicorn = MagicMock()
        rc_srv = serving_cli_main(["--config", str(cfg_file)], run=fake_uvicorn)
        assert rc_srv == 2, f"{case_name} server startup should exit 2"
        assert not fake_uvicorn.called
        cap_srv = capsys.readouterr()
        assert "CANARY_PG_PW_987654" not in cap_srv.err
        assert "Traceback" not in cap_srv.err

    # 4. Valid static config via `python -m seeknal.serving validate --config X` and `-c X` -> exit 0, 'ok'
    valid_static_dir = tmp_path / "valid_static_cli"
    valid_static_dir.mkdir()
    valid_static_cfg = _write_static_bundle(
        valid_static_dir,
        jwks_file,
        placement_data={
            "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
            "tables": {
                "clinical.patients": {
                    "keys": ["id"],
                    "pii": True,
                    "serve": {"lb": "public.patients"},
                }
            },
        },
        schemas_data={"clinical.patients": ["id", "ssn"]},
        connections_data={"lb": all_connections["lb"]},
    )
    capsys.readouterr()
    assert serving_cli_main(["validate", "--config", str(valid_static_cfg)]) == 0
    out_s1 = capsys.readouterr()
    assert out_s1.out.strip() == "ok"
    assert out_s1.err == ""

    assert serving_cli_main(["validate", "-c", str(valid_static_cfg)]) == 0
    out_s2 = capsys.readouterr()
    assert out_s2.out.strip() == "ok"

    # 5. Valid discovery config via `python -m seeknal.serving validate --config X` -> exit 0, 'ok' + skip note
    valid_disc_cfg = tmp_path / "valid_disc_cli.yaml"
    valid_disc_cfg.write_text(
        yaml.safe_dump(
            {
                "listen": {"host": "127.0.0.1", "port": 8765},
                "grant_keys": {"jwks_file": jwks_file},
                "placement": {
                    "discovery": {
                        "default_catalog": "lakekeeper",
                        "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
                        "lakebase": {"engine": "lb"},
                    }
                },
                "connections": {"lb": all_connections["lb"]},
            }
        ),
        encoding="utf-8",
    )
    assert serving_cli_main(["validate", "--config", str(valid_disc_cfg)]) == 0
    out_disc = capsys.readouterr()
    assert out_disc.out.splitlines()[0] == "ok"
    assert "live_catalog_discovery" in out_disc.out
    assert out_disc.err == ""

    # 6. `--version`, `--version --json`, and `--check` continue working alongside `validate`
    assert serving_cli_main(["--version"]) == 0
    assert serving_cli_main(["--version", "--json"]) == 0
    capsys.readouterr()
    assert serving_cli_main(["--config", str(valid_static_cfg), "--check"]) == 0
    assert "config ok" in capsys.readouterr().out


