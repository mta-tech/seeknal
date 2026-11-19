"""Tests for §2.3 storage-class PII placement validation and the §2.6 CBN YAML oracle cases."""

from __future__ import annotations

import base64
import copy
import json
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.__main__ import main as serving_cli_main
from seeknal.serving.lakebase_endpoint import effective_storage_class
from seeknal.serving.server.app import build_server
from seeknal.serving.server.config import ServerConfig, ServerConfigError
from seeknal.serving.startup_validation import (
    StartupValidationError,
    run_validate_cli,
    validate_startup,
)


@pytest.fixture
def jwks_path(tmp_path: Path) -> str:
    priv = ec.generate_private_key(ec.SECP256R1())
    pn = priv.public_key().public_numbers()
    jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii"),
        "y": base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii"),
        "kid": "cbn-kid-1",
    }
    p = tmp_path / "grants.jwks.json"
    p.write_text(json.dumps({"keys": [jwk]}), encoding="utf-8")
    return str(p)


@pytest.fixture
def cbn_files(tmp_path: Path, jwks_path: str) -> dict[str, str]:
    secrets_env = tmp_path / "serving.env"
    secrets_env.write_text(
        "\n".join(
            [
                "CBN_PG_REPORT_DSN_BI=postgresql://seeknal_bi:CANARY_BI_PW_101@pg-report.cbn.internal:5432/cbn_report",
                "CBN_LB_OPS_DSN=postgresql://seeknal_ops:CANARY_LB_PW_202@lb-ops.cbn.internal:40100/ops",
                "LB_OPS_PROJECT_TOKEN=CANARY_LB_API_TOKEN_303",
                "LK_CLIENT_ID=seeknal-serving",
                "LK_CLIENT_SECRET=CANARY_LK_SECRET_404",
                "S3_KEY_ID=AKIAIOSFODNN7EXAMPLE",
                "S3_SECRET=CANARY_S3_SECRET_505",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    secrets_env.chmod(0o600)

    ask_dsn_file = tmp_path / "pg-report-ask.dsn"
    ask_dsn_file.write_text(
        "postgresql://seeknal_ask:CANARY_ASK_PW_606@pg-report.cbn.internal:5432/cbn_report\n",
        encoding="utf-8",
    )
    ask_dsn_file.chmod(0o600)

    pg_ca = tmp_path / "cbn-pg-ca.pem"
    pg_ca.write_text("-----BEGIN CERTIFICATE-----\nFAKEPGCA\n-----END CERTIFICATE-----\n", encoding="utf-8")
    lb_ca = tmp_path / "lakebase-ca.pem"
    lb_ca.write_text("-----BEGIN CERTIFICATE-----\nFAKELBCA\n-----END CERTIFICATE-----\n", encoding="utf-8")

    ext_dir = tmp_path / "duckdb_extensions"
    ext_dir.mkdir(exist_ok=True)

    return {
        "jwks": jwks_path,
        "secrets_env": str(secrets_env),
        "ask_dsn_file": str(ask_dsn_file),
        "pg_ca": str(pg_ca),
        "lb_ca": str(lb_ca),
        "ext_dir": str(ext_dir),
    }


def _make_cbn_connections(cbn_files: dict[str, str]) -> dict:
    return {
        "pg_report": {
            "kind": "postgres",
            "mode": "read_only",
            "accounts": {
                "interactive": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                "bi": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                "report": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                "ask": {"dsn_file": cbn_files["ask_dsn_file"]},
            },
            "pool_max": 12,
            "pool_min": 2,
            "acquire_timeout_seconds": 5,
            "connect_timeout_seconds": 5,
            "session": {
                "statement_timeout_ms": 30000,
                "lock_timeout_ms": 2000,
                "idle_in_transaction_session_timeout_ms": 10000,
                "search_path": "pg_catalog",
                "application_name": "seeknal-serving",
                "startup_options": True,
                "connection_limit": 30,
            },
            "tls": {
                "sslmode": "verify-full",
                "sslrootcert_file": cbn_files["pg_ca"],
            },
        },
        "lb_ops": {
            "kind": "lakebase",
            "dsn_env": "CBN_LB_OPS_DSN",
            "pool_max": 4,
            "session": {
                "statement_timeout_ms": 15000,
                "lock_timeout_ms": 2000,
                "idle_in_transaction_session_timeout_ms": 10000,
                "search_path": "pg_catalog",
            },
            "tls": {
                "sslmode": "verify-full",
                "sslrootcert_file": cbn_files["lb_ca"],
            },
            "lakebase": {
                "project_id": "prj_0123456789abcdefghij",
                "branch_id": "br_0123456789abcdefghij",
                "endpoint_id": "ep_0123456789abcdefghij",
                "endpoint_type": "read_write",
                "cu": 1,
                "cold_start": {
                    "wake": "api",
                    "api_url": "https://lakebase-cp.internal.example",
                    "api_token_env": "LB_OPS_PROJECT_TOKEN",
                    "connect_timeout_seconds": 5,
                    "max_query_wait_seconds": 10,
                    "max_wake_seconds": 60,
                },
            },
        },
        "lake": {
            "kind": "duckdb",
            "profile": "iceberg_direct",
            "extension_directory": cbn_files["ext_dir"],
            "extensions": ["httpfs", "avro", "iceberg"],
            "allowed_s3_prefixes": ["s3://cbn-warehouse/gold/"],
            "recycle_seconds": 240,
            "iceberg_catalogs": {
                "lake": {
                    "endpoint": "https://lakekeeper.cbn.internal.example/catalog",
                    "warehouse": "cbn",
                    "token_uri": "https://kc.cbn.internal.example/realms/cbn/protocol/openid-connect/token",
                    "client_id_env": "LK_CLIENT_ID",
                    "client_secret_env": "LK_CLIENT_SECRET",
                    "access_delegation": "vended",
                    "s3": {
                        "endpoint": "s3.cbn.internal.example:9000",
                        "region": "local",
                        "key_id_env": "S3_KEY_ID",
                        "secret_env": "S3_SECRET",
                        "use_ssl": True,
                    },
                }
            },
        },
    }


def _make_cbn_discovery_config(cbn_files: dict[str, str]) -> dict:
    return {
        "secrets_env_file": cbn_files["secrets_env"],
        "default_catalog": "cbn",
        "grant_keys": {"jwks_file": cbn_files["jwks"]},
        "connections": _make_cbn_connections(cbn_files),
        "placement": {
            "discovery": {
                "default_catalog": "cbn",
                "refresh_interval_seconds": 300,
                "lakekeeper": {
                    "url": "https://lakekeeper.cbn.internal.example/catalog",
                    "warehouse": "cbn",
                    "oauth": {
                        "token_url": "https://kc.cbn.internal.example/realms/cbn/protocol/openid-connect/token",
                        "client_id": "seeknal-serving-discovery",
                        "client_secret_env": "LK_CLIENT_SECRET",
                    },
                    "namespaces": ["rpt"],
                },
                "engines": {
                    "lake": {
                        "kind": "duckdb",
                        "dialect": "duckdb",
                        "profile": "iceberg_direct",
                        "alias": "lake",
                    },
                    "pg_report": {"kind": "postgres", "dialect": "postgres"},
                    "lb_ops": {"kind": "lakebase", "dialect": "postgres"},
                },
                "postgres_sources": {
                    "pg_report": {
                        "account": "interactive",
                        "logical_catalog": "cbn",
                        "freshness": {
                            "sync_state_table": "public._lakebase_sync_state",
                            "freshness_sql": "select max(as_of_date) from stg.load_control",
                        },
                        "schemas": {
                            "rpt": {"include": ["rpt_*"], "exclude": ["rpt_*_stg"]},
                            "src_pii": {"include": ["customer"], "pii": True},
                        },
                        "tables": {
                            "cbn.src_pii.customer": {
                                "access": {
                                    "pattern": "point",
                                    "keys": ["customer_id"],
                                    "filterable": ["customer_id", "msisdn_hash"],
                                    "max_rows": 50,
                                }
                            },
                            "cbn.rpt.rpt_txn_detail": {
                                "access": {
                                    "pattern": "filter",
                                    "filterable": [
                                        "customer_id",
                                        "as_of_date",
                                        "branch_code",
                                    ],
                                    "range_filterable": ["as_of_date"],
                                    "sortable": ["as_of_date", "txn_id"],
                                    "max_rows": 5000,
                                }
                            },
                        },
                    },
                    "lb_ops": {
                        "account": "interactive",
                        "logical_catalog": "ops",
                        "schemas": {
                            "ops": {"include": ["ticket_*", "case_status"]}
                        },
                    },
                },
                "policy": {
                    "require_filter_min_rows": 100000,
                    "max_in_list": 1000,
                    "max_offset": 10000,
                    "guard_mode": "enforce",
                    "pii_engines": [],
                    "pii_object_storage_engines": [],
                },
            }
        },
    }


def test_effective_storage_class_matrix_across_all_engine_kinds(
    cbn_files: dict[str, str],
):
    cfg = ServerConfig.from_dict(_make_cbn_discovery_config(cbn_files), env={})
    assert effective_storage_class(cfg.connections["pg_report"]) == "local"
    assert effective_storage_class(cfg.connections["lb_ops"]) == "object_storage"
    assert effective_storage_class(cfg.connections["lake"]) == "object_storage"

    # Legacy lakebase -> unknown; starrocks / cube omitted -> object_storage; duckdb local_file -> local
    assert effective_storage_class({"kind": "lakebase", "dsn_env": "X"}) == "unknown"
    assert (
        effective_storage_class(
            {"kind": "lakebase", "dsn_env": "X", "storage_class": "local"}
        )
        == "local"
    )
    assert effective_storage_class({"kind": "starrocks"}) == "object_storage"
    assert (
        effective_storage_class({"kind": "starrocks", "storage_class": "local"})
        == "local"
    )
    assert effective_storage_class({"kind": "cube"}) == "object_storage"
    assert (
        effective_storage_class({"kind": "cube", "storage_class": "local"})
        == "local"
    )
    assert effective_storage_class({"kind": "duckdb", "profile": "local_file"}) == "local"
    assert (
        effective_storage_class(
            {"kind": "postgres", "storage_class": "object_storage"}
        )
        == "object_storage"
    )


def test_cbn_yaml_discovery_passes_on_pg_report_and_refuses_when_src_pii_moved_to_lb_ops(
    tmp_path: Path, cbn_files: dict[str, str], capsys
):
    # 1. Valid §2.6 CBN YAML passes validate_startup and CLI validate with exit 0
    valid_cbn = _make_cbn_discovery_config(cbn_files)
    valid_yaml = tmp_path / "cbn_valid.yaml"
    valid_yaml.write_text(yaml.safe_dump(valid_cbn), encoding="utf-8")

    report = validate_startup(valid_yaml, env={})
    assert report.ok is True
    assert report.mode == "discovery"

    assert run_validate_cli(valid_yaml, env={}) == 0
    out_ok = capsys.readouterr()
    assert out_ok.out.splitlines()[0] == "ok"
    assert out_ok.err == ""

    # 2. Moving src_pii under postgres_sources.lb_ops.schemas refuses with exit 2 and §2.3 message
    bad_cbn = copy.deepcopy(valid_cbn)
    pg_src = bad_cbn["placement"]["discovery"]["postgres_sources"]["pg_report"]
    lb_src = bad_cbn["placement"]["discovery"]["postgres_sources"]["lb_ops"]
    moved_src_pii = pg_src["schemas"].pop("src_pii")
    pg_src["tables"].pop("cbn.src_pii.customer")
    lb_src["logical_catalog"] = "cbn"
    lb_src["schemas"]["src_pii"] = moved_src_pii

    bad_yaml = tmp_path / "cbn_pii_on_lb_ops.yaml"
    bad_yaml.write_text(yaml.safe_dump(bad_cbn), encoding="utf-8")

    expected_msg = (
        "table 'cbn.src_pii.customer' is pii: true but its serve engine 'lb_ops' "
        "(kind lakebase) has storage_class object_storage; "
        "CBN policy forbids PII in object storage — serve it from a postgres source "
        "or list the engine in policy.pii_object_storage_engines"
    )

    with pytest.raises(StartupValidationError) as exc_info:
        validate_startup(bad_yaml, env={})
    assert expected_msg in exc_info.value.problems

    capsys.readouterr()
    assert serving_cli_main(["validate", "--config", str(bad_yaml)]) == 2
    cap_err = capsys.readouterr().err
    assert expected_msg in cap_err
    for canary in (
        "CANARY_BI_PW_101",
        "CANARY_LB_PW_202",
        "CANARY_LB_API_TOKEN_303",
        "CANARY_LK_SECRET_404",
        "CANARY_S3_SECRET_505",
        "CANARY_ASK_PW_606",
    ):
        assert canary not in cap_err


def test_cbn_static_placement_pg_report_passes_lb_ops_refuses_and_lake_fails_twice(
    tmp_path: Path, cbn_files: dict[str, str], capsys
):
    schemas_file = tmp_path / "schemas.json"
    schemas_file.write_text(
        json.dumps({"cbn.src_pii.customer": ["customer_id", "msisdn_hash", "full_name"]}),
        encoding="utf-8",
    )

    def _write_cbn_static(name: str, serve_map: dict, policy: dict | None = None) -> Path:
        sub = tmp_path / name
        sub.mkdir(exist_ok=True)
        pl_file = sub / "placement.yaml"
        pl_data = {
            "engines": {
                "pg_report": {"kind": "postgres", "dialect": "postgres"},
                "lb_ops": {"kind": "lakebase", "dialect": "postgres"},
                "lake": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "cbn.src_pii.customer": {
                    "keys": ["customer_id"],
                    "pii": True,
                    "serve": serve_map,
                    "access": {
                        "pattern": "point",
                        "keys": ["customer_id"],
                        "filterable": ["customer_id", "msisdn_hash"],
                        "max_rows": 50,
                    },
                }
            },
            "policy": policy
            or {
                "pii_engines": [],
                "pii_object_storage_engines": [],
            },
        }
        pl_file.write_text(yaml.safe_dump(pl_data), encoding="utf-8")
        srv_file = sub / "serving.yaml"
        srv_file.write_text(
            yaml.safe_dump(
                {
                    "secrets_env_file": cbn_files["secrets_env"],
                    "default_catalog": "cbn",
                    "grant_keys": {"jwks_file": cbn_files["jwks"]},
                    "connections": _make_cbn_connections(cbn_files),
                    "placement": {
                        "static": {
                            "placement_file": str(pl_file),
                            "schemas_file": str(schemas_file),
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        return srv_file

    # 1. On pg_report: passes (storage_class: local, single serve)
    cfg_pg = _write_cbn_static("on_pg_report", {"pg_report": "src_pii.customer"})
    rep_pg = validate_startup(cfg_pg, env={})
    assert rep_pg.ok is True
    capsys.readouterr()
    assert serving_cli_main(["validate", "--config", str(cfg_pg)]) == 0

    # 2. On lb_ops: refuses (exit 2) with exact §2.3 message and refuses build_server
    cfg_lb = _write_cbn_static("on_lb_ops", {"lb_ops": "src_pii.customer"})
    expected_lb_msg = (
        "table 'cbn.src_pii.customer' is pii: true but its serve engine 'lb_ops' "
        "(kind lakebase) has storage_class object_storage; "
        "CBN policy forbids PII in object storage — serve it from a postgres source "
        "or list the engine in policy.pii_object_storage_engines"
    )
    with pytest.raises(StartupValidationError) as exc_lb:
        validate_startup(cfg_lb, env={})
    assert exc_lb.value.problems == (expected_lb_msg,)

    with pytest.raises(ServerConfigError, match="storage_class object_storage"):
        build_server(cfg_lb, env={})

    capsys.readouterr()
    assert serving_cli_main(["validate", "--config", str(cfg_lb)]) == 2
    assert expected_lb_msg in capsys.readouterr().err

    # 3. On lake (duckdb iceberg_direct): fails TWICE — DuckDB pii_engines rule AND object_storage rule!
    cfg_lake = _write_cbn_static("on_lake", {"lake": "lake.src_pii.customer"})
    with pytest.raises(StartupValidationError) as exc_lake:
        validate_startup(cfg_lake, env={})
    assert len(exc_lake.value.problems) == 2
    assert any("policy.pii_engines" in p for p in exc_lake.value.problems)
    assert (
        "table 'cbn.src_pii.customer' is pii: true but its serve engine 'lake' "
        "(kind duckdb) has storage_class object_storage; "
        "CBN policy forbids PII in object storage — serve it from a postgres source "
        "or list the engine in policy.pii_object_storage_engines"
    ) in exc_lake.value.problems

    capsys.readouterr()
    assert serving_cli_main(["validate", "--config", str(cfg_lake)]) == 2
    err_lines = [l for l in capsys.readouterr().err.strip().splitlines() if l]
    assert len(err_lines) == 2


def test_legacy_lakebase_undeclared_starrocks_cube_tokenized_and_explicit_exception(
    tmp_path: Path, cbn_files: dict[str, str]
):
    schemas_file = tmp_path / "schemas.json"
    schemas_file.write_text(
        json.dumps({"clinical.patients": ["id", "ssn"]}),
        encoding="utf-8",
    )

    def _run_static_case(
        case_name: str,
        *,
        engines: dict,
        connections: dict,
        table_spec: dict,
        policy: dict | None = None,
    ):
        d = tmp_path / case_name
        d.mkdir(exist_ok=True)
        pf = d / "placement.yaml"
        pf.write_text(
            yaml.safe_dump(
                {
                    "engines": engines,
                    "tables": {"clinical.patients": table_spec},
                    "policy": policy or {},
                }
            ),
            encoding="utf-8",
        )
        sf = d / "serving.yaml"
        sf.write_text(
            yaml.safe_dump(
                {
                    "secrets_env_file": cbn_files["secrets_env"],
                    "grant_keys": {"jwks_file": cbn_files["jwks"]},
                    "connections": connections,
                    "placement": {
                        "static": {
                            "placement_file": str(pf),
                            "schemas_file": str(schemas_file),
                        }
                    },
                }
            ),
            encoding="utf-8",
        )
        return validate_startup(sf, env={})

    # 1. Legacy lakebase (no lakebase: block, no storage_class) -> storage_class unknown -> refused
    with pytest.raises(StartupValidationError) as exc_leg:
        _run_static_case(
            "legacy_lb_refused",
            engines={"lb": {"kind": "lakebase", "dialect": "postgres"}},
            connections={"lb": {"kind": "lakebase", "dsn_env": "CBN_LB_OPS_DSN"}},
            table_spec={"keys": ["id"], "pii": True, "serve": {"lb": "public.patients"}},
        )
    assert "has storage_class unknown" in str(exc_leg.value)

    # Relabelling legacy lakebase to kind: postgres passes
    rep_pg = _run_static_case(
        "relabelled_postgres",
        engines={"lb": {"kind": "postgres", "dialect": "postgres"}},
        connections={"lb": {"kind": "postgres", "dsn_env": "CBN_LB_OPS_DSN"}},
        table_spec={"keys": ["id"], "pii": True, "serve": {"lb": "public.patients"}},
    )
    assert rep_pg.ok is True

    # Or declaring storage_class: local on legacy lakebase passes
    rep_leg_local = _run_static_case(
        "legacy_lb_declared_local",
        engines={"lb": {"kind": "lakebase", "dialect": "postgres"}},
        connections={
            "lb": {
                "kind": "lakebase",
                "dsn_env": "CBN_LB_OPS_DSN",
                "storage_class": "local",
            }
        },
        table_spec={"keys": ["id"], "pii": True, "serve": {"lb": "public.patients"}},
    )
    assert rep_leg_local.ok is True

    # 2. Undeclared starrocks and undeclared cube -> storage_class object_storage -> refused
    sr_conn = {
        "kind": "starrocks",
        "accounts": {
            "interactive": {
                "host": "sr.internal",
                "port": 9030,
                "user": "svc",
                "password_env": "LK_CLIENT_SECRET",
            }
        },
    }
    with pytest.raises(StartupValidationError) as exc_sr:
        _run_static_case(
            "undeclared_sr",
            engines={"sr": {"kind": "starrocks", "dialect": "starrocks"}},
            connections={"sr": sr_conn},
            table_spec={"keys": ["id"], "pii": True, "serve": {"sr": "lk.clinical.patients"}},
        )
    assert "(kind starrocks) has storage_class object_storage" in str(exc_sr.value)

    cube_conn = {
        "kind": "cube",
        "host": "cube.internal",
        "port": 15432,
        "dbname": "cube",
        "sql_user": "seeknal",
    }
    with pytest.raises(StartupValidationError) as exc_cube:
        _run_static_case(
            "undeclared_cube",
            engines={"cube": {"kind": "cube", "dialect": "postgres"}},
            connections={"cube": cube_conn},
            table_spec={"keys": ["id"], "pii": True, "serve": {"cube": "patients"}},
        )
    assert "(kind cube) has storage_class object_storage" in str(exc_cube.value)

    # 3. tokenized: true table is exempt and allowed on object storage (Lakebase & iceberg_direct)
    conns_cbn = _make_cbn_connections(cbn_files)
    rep_tok_lb = _run_static_case(
        "tokenized_on_lb_ops",
        engines={"lb_ops": {"kind": "lakebase", "dialect": "postgres"}},
        connections={"lb_ops": conns_cbn["lb_ops"]},
        table_spec={
            "keys": ["id"],
            "pii": True,
            "tokenized": True,
            "serve": {"lb_ops": "public.patients"},
        },
    )
    assert rep_tok_lb.ok is True

    rep_tok_lake = _run_static_case(
        "tokenized_on_lake",
        engines={"lake": {"kind": "duckdb", "dialect": "duckdb"}},
        connections={"lake": conns_cbn["lake"]},
        table_spec={
            "keys": ["id"],
            "pii": True,
            "tokenized": True,
            "serve": {"lake": "lake.clinical.patients"},
        },
    )
    assert rep_tok_lake.ok is True

    # 4. Explicit policy.pii_object_storage_engines allows untokenized PII on object storage
    rep_exception = _run_static_case(
        "explicit_pii_object_storage_exception",
        engines={"lb_ops": {"kind": "lakebase", "dialect": "postgres"}},
        connections={"lb_ops": conns_cbn["lb_ops"]},
        table_spec={"keys": ["id"], "pii": True, "serve": {"lb_ops": "public.patients"}},
        policy={"pii_object_storage_engines": ["lb_ops"]},
    )
    assert rep_exception.ok is True


def test_storage_class_forbidden_in_placement_engines_static_and_discovery(
    tmp_path: Path, cbn_files: dict[str, str]
):
    # Static placement.engines.<name>.storage_class refused
    pf = tmp_path / "placement_bad_sc.yaml"
    pf.write_text(
        yaml.safe_dump(
            {
                "engines": {
                    "pg_report": {
                        "kind": "postgres",
                        "dialect": "postgres",
                        "storage_class": "local",
                    }
                },
                "tables": {"t": {"keys": ["id"], "serve": {"pg_report": "public.t"}}},
            }
        ),
        encoding="utf-8",
    )
    sf = tmp_path / "schemas_bad_sc.json"
    sf.write_text(json.dumps({"t": ["id"]}), encoding="utf-8")

    with pytest.raises(StartupValidationError) as exc_stat:
        validate_startup(
            {
                "secrets_env_file": cbn_files["secrets_env"],
                "grant_keys": {"jwks_file": cbn_files["jwks"]},
                "connections": {"pg_report": _make_cbn_connections(cbn_files)["pg_report"]},
                "placement": {
                    "static": {"placement_file": str(pf), "schemas_file": str(sf)}
                },
            },
            env={},
        )
    assert (
        "Engine 'pg_report' cannot declare 'storage_class' in placement; "
        "declare 'storage_class' on connections.pg_report instead"
    ) in exc_stat.value.problems

    # Discovery placement.discovery.engines.<name>.storage_class refused
    disc_cfg = _make_cbn_discovery_config(cbn_files)
    disc_cfg["placement"]["discovery"]["engines"]["pg_report"]["storage_class"] = "local"
    with pytest.raises(StartupValidationError) as exc_disc:
        validate_startup(disc_cfg, env={})
    assert (
        "Engine 'pg_report' cannot declare 'storage_class' in placement; "
        "declare 'storage_class' on connections.pg_report instead"
    ) in exc_disc.value.problems
