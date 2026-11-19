"""Tests for Postgres and Lakebase startup/config validation errors, access metadata checks, and warnings."""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.__main__ import main as serving_cli_main
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
        "kid": "pg-val-kid",
    }
    p = tmp_path / "grants.jwks.json"
    p.write_text(json.dumps({"keys": [jwk]}), encoding="utf-8")
    return str(p)


@pytest.fixture
def base_env() -> dict[str, str]:
    return {
        "PG_REPORT_DSN": "postgresql://seeknal_bi:PLANTED_PG_PW_9090@pg.internal:5432/cbn_report",
        "LB_OPS_DSN": "postgresql://seeknal_ops:PLANTED_LB_PW_8080@lb.internal:40100/ops",
        "LB_API_TOK": "PLANTED_LB_CP_TOK_7070",
    }


def _valid_lb_block() -> dict:
    return {
        "project_id": "prj_0123456789abcdefghij",
        "branch_id": "br_0123456789abcdefghij",
        "endpoint_id": "ep_0123456789abcdefghij",
        "endpoint_type": "read_write",
        "cu": 1,
        "cold_start": {
            "wake": "api",
            "api_url": "https://lakebase-cp.internal.example",
            "api_token_env": "LB_API_TOK",
            "connect_timeout_seconds": 5,
            "max_query_wait_seconds": 10,
            "max_wake_seconds": 60,
        },
    }


def _write_static(
    tmp_path: Path,
    jwks_path: str,
    name: str,
    *,
    placement: dict,
    schemas: dict,
    connections: dict,
) -> Path:
    sub = tmp_path / name
    sub.mkdir(exist_ok=True)
    pf = sub / "placement.yaml"
    sf = sub / "schemas.json"
    cf = sub / "serving.yaml"
    pf.write_text(yaml.safe_dump(placement), encoding="utf-8")
    sf.write_text(json.dumps(schemas), encoding="utf-8")
    cf.write_text(
        yaml.safe_dump(
            {
                "grant_keys": {"jwks_file": jwks_path},
                "connections": connections,
                "placement": {
                    "static": {"placement_file": str(pf), "schemas_file": str(sf)}
                },
            }
        ),
        encoding="utf-8",
    )
    return cf


def test_access_metadata_unknown_columns_invalid_pattern_and_point_without_keys(
    tmp_path: Path, jwks_path: str, base_env: dict[str, str]
):
    pg_conn = {"pg": {"kind": "postgres", "dsn_env": "PG_REPORT_DSN"}}

    # 1. Unknown columns in access.{keys,filterable,range_filterable,prefix_filterable,sortable}
    cf_cols = _write_static(
        tmp_path,
        jwks_path,
        "bad_access_cols",
        placement={
            "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
            "tables": {
                "cbn.rpt.txn": {
                    "keys": ["txn_id"],
                    "serve": {"pg": "rpt.txn"},
                    "access": {
                        "pattern": "filter",
                        "keys": ["missing_acc_key"],
                        "filterable": ["txn_id", "missing_filter_col"],
                        "range_filterable": ["missing_range_col"],
                        "prefix_filterable": ["missing_prefix_col"],
                        "sortable": ["missing_sort_col"],
                    },
                }
            },
        },
        schemas={"cbn.rpt.txn": ["txn_id", "customer_id", "as_of_date"]},
        connections=pg_conn,
    )
    with pytest.raises(StartupValidationError) as exc_cols:
        validate_startup(cf_cols, env=base_env)
    joined = "\n".join(exc_cols.value.problems)
    for bad_col in (
        "missing_acc_key",
        "missing_filter_col",
        "missing_range_col",
        "missing_prefix_col",
        "missing_sort_col",
    ):
        assert bad_col in joined

    # 2. Point table without effective keys (neither access.keys nor table.keys)
    cf_point_nokeys = _write_static(
        tmp_path,
        jwks_path,
        "point_no_keys",
        placement={
            "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
            "tables": {
                "cbn.src_pii.customer": {
                    "keys": [],
                    "serve": {"pg": "src_pii.customer"},
                    "access": {"pattern": "point", "filterable": ["customer_id"]},
                }
            },
        },
        schemas={"cbn.src_pii.customer": ["customer_id", "full_name"]},
        connections=pg_conn,
    )
    with pytest.raises(StartupValidationError) as exc_pk:
        validate_startup(cf_point_nokeys, env=base_env)
    assert any("pattern 'point' requires non-empty 'keys'" in p for p in exc_pk.value.problems)

    # Point table inheriting table.keys passes
    cf_point_inherited = _write_static(
        tmp_path,
        jwks_path,
        "point_inherited_keys",
        placement={
            "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
            "tables": {
                "cbn.src_pii.customer": {
                    "keys": ["customer_id"],
                    "serve": {"pg": "src_pii.customer"},
                    "access": {"pattern": "point", "filterable": ["customer_id"]},
                }
            },
        },
        schemas={"cbn.src_pii.customer": ["customer_id", "full_name"]},
        connections=pg_conn,
    )
    assert validate_startup(cf_point_inherited, env=base_env).ok is True

    # 3. Invalid access.pattern
    cf_bad_pat = _write_static(
        tmp_path,
        jwks_path,
        "bad_access_pattern",
        placement={
            "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
            "tables": {
                "cbn.rpt.txn": {
                    "keys": ["txn_id"],
                    "serve": {"pg": "rpt.txn"},
                    "access": {"pattern": "olap_full_scan"},
                }
            },
        },
        schemas={"cbn.rpt.txn": ["txn_id"]},
        connections=pg_conn,
    )
    with pytest.raises(StartupValidationError) as exc_pat:
        validate_startup(cf_bad_pat, env=base_env)
    assert any("invalid pattern" in p for p in exc_pat.value.problems)


def test_lakebase_bad_endpoint_ids_weak_tls_role_guard_false_and_fixed_storage_class(
    tmp_path: Path, jwks_path: str, base_env: dict[str, str]
):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("FAKECA\n", encoding="utf-8")

    def _check_conn_err(conn_spec: dict, match_substr: str) -> None:
        with pytest.raises(StartupValidationError) as exc_info:
            validate_startup(
                {
                    "grant_keys": {"jwks_file": jwks_path},
                    "connections": {"lb_ops": conn_spec},
                    "placement": {
                        "discovery": {
                            "default_catalog": "ops",
                            "engines": {
                                "lb_ops": {"kind": "lakebase", "dialect": "postgres"}
                            },
                        }
                    },
                },
                env=base_env,
            )
        err_text = str(exc_info.value)
        assert match_substr in err_text
        for secret in base_env.values():
            assert secret not in err_text

    base_lb_conn = {
        "kind": "lakebase",
        "dsn_env": "LB_OPS_DSN",
        "tls": {"sslmode": "verify-full", "sslrootcert_file": str(ca_file)},
        "lakebase": _valid_lb_block(),
    }

    # 1. Bad project_id, branch_id, endpoint_id
    for field_name, bad_id in (
        ("project_id", "prj_SHORT"),
        ("project_id", "prj_0123456789ABCDEFGHIJ"),  # uppercase rejected
        ("branch_id", "prj_0123456789abcdefghij"),  # wrong prefix
        ("endpoint_id", "ep_0123456789abcdefghij_extra"),
    ):
        bad_lb = {**_valid_lb_block(), field_name: bad_id}
        _check_conn_err({**base_lb_conn, "lakebase": bad_lb}, field_name)

    # 2. Weak TLS on Lakebase (sslmode=require, missing tls, missing sslrootcert_file)
    for bad_tls in (
        None,
        {"sslmode": "require", "sslrootcert_file": str(ca_file)},
        {"sslmode": "verify-ca", "sslrootcert_file": str(ca_file)},
        {"sslmode": "verify-full"},
    ):
        spec = dict(base_lb_conn)
        if bad_tls is None:
            spec.pop("tls")
        else:
            spec["tls"] = bad_tls
        _check_conn_err(spec, "verify-full")

    # 3. role_guard: false on Lakebase is refused
    _check_conn_err({**base_lb_conn, "role_guard": False}, "role_guard")

    # 4. Fixed storage_class on Lakebase with block cannot be set
    _check_conn_err({**base_lb_conn, "storage_class": "local"}, "storage_class")


def test_literal_secrets_missing_accounts_and_dsn_file_permissions_with_zero_leakage(
    tmp_path: Path, jwks_path: str, base_env: dict[str, str], capsys
):
    planted_dsn = "postgresql://admin:ULTRA_SECRET_DSN_PW_4455@pg.internal:5432/prod"
    planted_tok = "ULTRA_SECRET_API_TOKEN_6677"

    # 1. Literal dsn / password / api_token keys rejected with zero leakage
    for bad_conn in (
        {"kind": "postgres", "dsn": planted_dsn},
        {"kind": "postgres", "dsn_env": "PG_REPORT_DSN", "password": "ULTRA_SECRET_DSN_PW_4455"},
        {
            "kind": "postgres",
            "accounts": {
                "interactive": {"dsn": planted_dsn},
            },
        },
        {
            "kind": "lakebase",
            "dsn_env": "LB_OPS_DSN",
            "tls": {"sslmode": "verify-full", "sslrootcert_file": "/tmp/ca.pem"},
            "lakebase": {
                **_valid_lb_block(),
                "cold_start": {
                    "wake": "api",
                    "api_url": "https://lakebase-cp.internal.example",
                    "api_token": planted_tok,
                },
            },
        },
    ):
        with pytest.raises(StartupValidationError) as exc_lit:
            validate_startup(
                {
                    "grant_keys": {"jwks_file": jwks_path},
                    "connections": {"pg": bad_conn},
                    "placement": {
                        "discovery": {
                            "default_catalog": "cbn",
                            "engines": {
                                "pg": {"kind": bad_conn["kind"], "dialect": "postgres"}
                            },
                        }
                    },
                },
                env=base_env,
            )
        msg = str(exc_lit.value)
        assert planted_dsn not in msg
        assert "ULTRA_SECRET_DSN_PW_4455" not in msg
        assert planted_tok not in msg

    # 2. Both dsn_env and dsn_file rejected; accounts missing 'interactive' rejected;
    #    postgres_sources referencing unconfigured workload account rejected
    good_dsn_file = tmp_path / "good.dsn"
    good_dsn_file.write_text(planted_dsn + "\n", encoding="utf-8")
    good_dsn_file.chmod(0o600)

    with pytest.raises(StartupValidationError, match="dsn_env.*dsn_file|both"):
        validate_startup(
            {
                "grant_keys": {"jwks_file": jwks_path},
                "connections": {
                    "pg": {
                        "kind": "postgres",
                        "dsn_env": "PG_REPORT_DSN",
                        "dsn_file": str(good_dsn_file),
                    }
                },
                "placement": {
                    "discovery": {
                        "default_catalog": "cbn",
                        "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
                    }
                },
            },
            env=base_env,
        )

    with pytest.raises(StartupValidationError, match="interactive"):
        validate_startup(
            {
                "grant_keys": {"jwks_file": jwks_path},
                "connections": {
                    "pg": {
                        "kind": "postgres",
                        "accounts": {"bi": {"dsn_env": "PG_REPORT_DSN"}},
                    }
                },
                "placement": {
                    "discovery": {
                        "default_catalog": "cbn",
                        "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
                    }
                },
            },
            env=base_env,
        )

    # Discovery postgres_sources.pg.account = 'report' when connection only defines 'interactive'
    with pytest.raises(StartupValidationError, match="account.*report"):
        validate_startup(
            {
                "grant_keys": {"jwks_file": jwks_path},
                "connections": {
                    "pg": {
                        "kind": "postgres",
                        "accounts": {"interactive": {"dsn_env": "PG_REPORT_DSN"}},
                    }
                },
                "placement": {
                    "discovery": {
                        "default_catalog": "cbn",
                        "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
                        "postgres_sources": {
                            "pg": {
                                "account": "report",
                                "schemas": {"rpt": {"include": ["*"]}},
                            }
                        },
                    }
                },
            },
            env=base_env,
        )

    # 3. Missing, world-readable (0644), and unreadable (0000) dsn_file (top-level & accounts)
    missing_dsn_file = tmp_path / "does_not_exist.dsn"
    world_dsn_file = tmp_path / "world_readable.dsn"
    world_dsn_file.write_text(planted_dsn + "\n", encoding="utf-8")
    world_dsn_file.chmod(0o644)

    # Missing top-level and account dsn_file
    cfg_missing_files = tmp_path / "missing_dsn_files.yaml"
    cfg_missing_files.write_text(
        yaml.safe_dump(
            {
                "grant_keys": {"jwks_file": jwks_path},
                "connections": {
                    "pg_top": {"kind": "postgres", "dsn_file": str(missing_dsn_file)},
                    "pg_acc": {
                        "kind": "postgres",
                        "accounts": {
                            "interactive": {"dsn_env": "PG_REPORT_DSN"},
                            "ask": {"dsn_file": str(tmp_path / "missing_ask.dsn")},
                        },
                    },
                },
                "placement": {
                    "discovery": {
                        "default_catalog": "cbn",
                        "engines": {
                            "pg_top": {"kind": "postgres", "dialect": "postgres"},
                            "pg_acc": {"kind": "postgres", "dialect": "postgres"},
                        },
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    capsys.readouterr()
    assert run_validate_cli(cfg_missing_files, env=base_env) == 2
    err_missing = capsys.readouterr().err
    assert "does_not_exist.dsn" in err_missing
    assert "missing_ask.dsn" in err_missing

    # World-readable dsn_file refused and never leaks DSN password
    cfg_world = tmp_path / "world_dsn.yaml"
    cfg_world.write_text(
        yaml.safe_dump(
            {
                "grant_keys": {"jwks_file": jwks_path},
                "connections": {
                    "pg": {"kind": "postgres", "dsn_file": str(world_dsn_file)}
                },
                "placement": {
                    "discovery": {
                        "default_catalog": "cbn",
                        "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    capsys.readouterr()
    assert serving_cli_main(["validate", "--config", str(cfg_world)]) == 2
    err_world = capsys.readouterr().err
    assert "group- or world-readable" in err_world
    assert planted_dsn not in err_world
    assert "ULTRA_SECRET_DSN_PW_4455" not in err_world

    # Unreadable (0000) dsn_file refused when not running as root
    if os.geteuid() != 0:
        unreadable_dsn = tmp_path / "unreadable.dsn"
        unreadable_dsn.write_text(planted_dsn + "\n", encoding="utf-8")
        unreadable_dsn.chmod(0o000)
        try:
            with pytest.raises(StartupValidationError, match="not readable"):
                validate_startup(
                    {
                        "grant_keys": {"jwks_file": jwks_path},
                        "connections": {
                            "pg": {"kind": "postgres", "dsn_file": str(unreadable_dsn)}
                        },
                        "placement": {
                            "discovery": {
                                "default_catalog": "cbn",
                                "engines": {
                                    "pg": {"kind": "postgres", "dialect": "postgres"}
                                },
                            }
                        },
                    },
                    env=base_env,
                )
        finally:
            unreadable_dsn.chmod(0o600)


def test_startup_validation_warnings_collected_and_logged_without_failing_valid_config(
    tmp_path: Path, jwks_path: str, base_env: dict[str, str], caplog
):
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("FAKECA\n", encoding="utf-8")

    # Configure all warning conditions on an otherwise valid static config:
    # 1) Legacy kind: lakebase without lakebase: block ('lb_legacy')
    # 2) Missing session: hardening block on postgres ('pg_writable') and lakebase ('lb_legacy', 'lb_ro_overbudget')
    # 3) role_guard: false on postgres ('pg_writable')
    # 4) read_only Lakebase endpoint ('lb_ro_overbudget')
    # 5) PII table served on writable source ('pg_writable' with mode: read_write)
    # 6) Lakebase pool_max (40) > lakebase.max_connections (max(25, 0.25*100) = 25) when cu=0.25
    ro_lb_block = {
        **_valid_lb_block(),
        "endpoint_type": "read_only",
        "cu": 0.25,
    }
    cf = _write_static(
        tmp_path,
        jwks_path,
        "all_warnings_valid",
        placement={
            "engines": {
                "pg_writable": {"kind": "postgres", "dialect": "postgres"},
                "lb_legacy": {"kind": "lakebase", "dialect": "postgres"},
                "lb_ro_overbudget": {"kind": "lakebase", "dialect": "postgres"},
            },
            "tables": {
                "cbn.src_pii.customer": {
                    "keys": ["customer_id"],
                    "pii": True,
                    "serve": {"pg_writable": "src_pii.customer"},
                },
                "ops.tickets": {
                    "keys": ["id"],
                    "serve": {
                        "lb_legacy": "public.tickets",
                        "lb_ro_overbudget": "public.tickets",
                    },
                },
            },
        },
        schemas={
            "cbn.src_pii.customer": ["customer_id", "full_name"],
            "ops.tickets": ["id", "status"],
        },
        connections={
            "pg_writable": {
                "kind": "postgres",
                "mode": "read_write",
                "dsn_env": "PG_REPORT_DSN",
                "role_guard": False,
            },
            "lb_legacy": {
                "kind": "lakebase",
                "dsn_env": "LB_OPS_DSN",
            },
            "lb_ro_overbudget": {
                "kind": "lakebase",
                "dsn_env": "LB_OPS_DSN",
                "pool_max": 40,
                "tls": {"sslmode": "verify-full", "sslrootcert_file": str(ca_file)},
                "lakebase": ro_lb_block,
            },
        },
    )

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="seeknal.serving.startup_validation"):
        report = validate_startup(cf, env=base_env)

    assert report.ok is True
    joined_warnings = "\n".join(report.warnings)

    # 1. Legacy lakebase deprecation
    assert (
        "connection 'lb_legacy' uses kind: lakebase without a lakebase: endpoint block"
        in joined_warnings
    )
    # 2. Missing session hardening
    assert "connection 'pg_writable' (kind postgres) has no 'session' hardening block" in joined_warnings
    assert "connection 'lb_legacy' (kind lakebase) has no 'session' hardening block" in joined_warnings
    # 3. role_guard: false on postgres
    assert "connection 'pg_writable' has role_guard: false" in joined_warnings
    # 4. read_only Lakebase frozen snapshot
    assert "read_only Lakebase endpoints serve a frozen snapshot" in joined_warnings
    # 5. PII table on writable source
    assert "PII table 'cbn.src_pii.customer' is served on writable source 'pg_writable'" in joined_warnings
    # 6. Pool budget > max_connections
    assert "exceeds lakebase.max_connections" in joined_warnings

    # Logged via logger.warning and contains zero secrets
    assert len(caplog.records) >= 6
    for secret in base_env.values():
        assert secret not in joined_warnings
