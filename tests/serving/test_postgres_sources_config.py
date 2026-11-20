"""Unit tests for PostgreSQL and Lakebase source configuration in server/config.py."""

from __future__ import annotations

import base64
import copy
import json
import logging
import os
import stat

import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving.errors import scrub_error_detail
from seeknal.serving.postgres_session import effective_storage_class
from seeknal.serving.server.config import (
    CubeConnectionConfig,
    DuckDBConnectionConfig,
    LakebaseConnectionConfig,
    PostgresConnectionConfig,
    ServerConfig,
    ServerConfigError,
    StarRocksConnectionConfig,
)


@pytest.fixture
def jwks_file(tmp_path):
    priv = ec.generate_private_key(ec.SECP256R1())
    pn = priv.public_key().public_numbers()
    x_b64 = base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    y_b64 = base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    jwks_path = tmp_path / "grants.jwks.json"
    jwks_path.write_text(
        json.dumps(
            {
                "keys": [
                    {
                        "kty": "EC",
                        "crv": "P-256",
                        "x": x_b64,
                        "y": y_b64,
                        "kid": "pg-cfg-key",
                    }
                ]
            }
        )
    )
    return str(jwks_path)


@pytest.fixture
def base_cfg(jwks_file, tmp_path):
    pf = tmp_path / "placement.yaml"
    pf.write_text("engines: {}\ntables: {}\ncubes: {}\n")
    sf = tmp_path / "schemas.json"
    sf.write_text("{}")
    return {
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {
            "static": {
                "placement_file": str(pf),
                "schemas_file": str(sf),
            }
        },
        "connections": {},
    }


def _valid_lakebase_block(endpoint_type: str = "read_write") -> dict:
    return {
        "project_id": "prj_xxxxxxxxxxxxxxxxxxxx",
        "branch_id": "br_xxxxxxxxxxxxxxxxxxxx",
        "endpoint_id": "ep_xxxxxxxxxxxxxxxxxxxx",
        "endpoint_type": endpoint_type,
        "cu": 1,
        "cold_start": {
            "wake": "api",
            "api_url": "https://lakebase-cp.internal.example",
            "api_token_env": "LB_OPS_PROJECT_TOKEN",
            "connect_timeout_seconds": 5,
            "max_query_wait_seconds": 10,
            "max_wake_seconds": 60,
            "retry_backoff_ms": [500, 1000, 2000, 4000],
        },
    }


def test_postgres_minimal_defaults_and_full_config(base_cfg, tmp_path):
    dsn_file = tmp_path / "pg_ask.dsn"
    dsn_file.write_text("postgresql://ask_role:ask_pw_9988@127.0.0.1:5432/cbn\n")
    os.chmod(dsn_file, stat.S_IRUSR | stat.S_IWUSR)

    d = copy.deepcopy(base_cfg)
    d["cursor_key_env"] = "SEEKNAL_CURSOR_HMAC_KEY"
    d["connections"] = {
        "pg_min": {
            "kind": "postgres",
            "dsn_env": "PG_MIN_DSN",
        },
        "pg_report": {
            "kind": "postgres",
            "mode": "read_only",
            "storage_class": "local",
            "accounts": {
                "interactive": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                "bi": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                "report": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                "ask": {"dsn_file": str(dsn_file)},
            },
            "pool_max": 12,
            "pool_min": 2,
            "connect_timeout_seconds": 5,
            "acquire_timeout_seconds": 5,
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
                "sslrootcert_file": "/etc/seeknal/tls/cbn-pg-ca.pem",
            },
            "freshness": {
                "sync_state_table": "public._lakebase_sync_state",
                "freshness_sql": "SELECT max(as_of_date) FROM stg.load_control",
            },
        },
    }

    cfg = ServerConfig.from_dict(d)
    assert cfg.cursor_key_env == "SEEKNAL_CURSOR_HMAC_KEY"

    pg_min = cfg.connections["pg_min"]
    assert isinstance(pg_min, PostgresConnectionConfig)
    assert pg_min.kind == "postgres"
    assert pg_min.mode == "read_only"
    assert pg_min.read_only is True
    assert pg_min.role_guard is True
    assert pg_min.storage_class is None
    assert effective_storage_class(pg_min) == "local"

    pg_rep = cfg.connections["pg_report"]
    assert isinstance(pg_rep, PostgresConnectionConfig)
    assert set(pg_rep.accounts.keys()) == {"interactive", "bi", "report", "ask"}
    assert pg_rep.accounts["interactive"].dsn_env == "CBN_PG_REPORT_DSN_BI"
    assert pg_rep.accounts["ask"].dsn_file == str(dsn_file)
    assert pg_rep.session is not None
    assert pg_rep.session.connection_limit == 30
    assert pg_rep.tls is not None
    assert pg_rep.tls.sslmode == "verify-full"
    assert pg_rep.freshness is not None
    assert pg_rep.freshness.sync_state_table == "public._lakebase_sync_state"

    ref_vars = cfg.get_referenced_env_vars()
    assert "PG_MIN_DSN" in ref_vars
    assert "CBN_PG_REPORT_DSN_BI" in ref_vars
    assert "SEEKNAL_CURSOR_HMAC_KEY" in ref_vars

    # Verify dsn_file secret was registered for scrubbing
    scrubbed = scrub_error_detail("leaked ask_pw_9988 in error", env={})
    assert "ask_pw_9988" not in scrubbed


def test_legacy_lakebase_warns_once_and_preserves_defaults(base_cfg, caplog):
    d = copy.deepcopy(base_cfg)
    d["connections"] = {
        "lb": {
            "kind": "lakebase",
            "dsn_env": "LAKEBASE_DSN",
        }
    }

    with caplog.at_level(logging.WARNING, logger="seeknal.serving.server.config"):
        cfg = ServerConfig.from_dict(d)

    lb = cfg.connections["lb"]
    assert isinstance(lb, LakebaseConnectionConfig)
    assert lb.is_legacy is True
    assert lb.mode == "read_write"
    assert lb.read_only is False
    assert lb.pool_max == 8
    assert lb.pool_min == 0
    assert effective_storage_class(lb) == "unknown"

    matching_warnings = [
        r.getMessage()
        for r in caplog.records
        if "connection 'lb' uses kind: lakebase without a lakebase: endpoint block"
        in r.getMessage()
    ]
    assert len(matching_warnings) == 1
    assert "relabel it kind: postgres" in matching_warnings[0]


def test_lakebase_with_endpoint_block_tls_and_storage_class_rules(base_cfg):
    # 1. Valid Lakebase with endpoint block
    d = copy.deepcopy(base_cfg)
    d["connections"] = {
        "lb_ops": {
            "kind": "lakebase",
            "dsn_env": "CBN_LB_OPS_DSN",
            "tls": {
                "sslmode": "verify-full",
                "sslrootcert_file": "/etc/seeknal/tls/lakebase-ca.pem",
            },
            "lakebase": _valid_lakebase_block(endpoint_type="read_only"),
        }
    }
    cfg = ServerConfig.from_dict(d)
    lb_ops = cfg.connections["lb_ops"]
    assert isinstance(lb_ops, LakebaseConnectionConfig)
    assert lb_ops.is_legacy is False
    assert lb_ops.mode == "read_only"
    assert lb_ops.read_only is True
    assert effective_storage_class(lb_ops) == "object_storage"
    assert "CBN_LB_OPS_DSN" in cfg.get_referenced_env_vars()
    assert "LB_OPS_PROJECT_TOKEN" in cfg.get_referenced_env_vars()

    # 2. Missing tls on lakebase with block -> refused
    d_no_tls = copy.deepcopy(d)
    del d_no_tls["connections"]["lb_ops"]["tls"]
    with pytest.raises(ServerConfigError, match="verify-full"):
        ServerConfig.from_dict(d_no_tls)

    # 3. Weaker sslmode (require / verify-ca) -> refused
    for weak_mode in ("disable", "prefer", "require", "verify-ca"):
        d_weak = copy.deepcopy(d)
        d_weak["connections"]["lb_ops"]["tls"]["sslmode"] = weak_mode
        with pytest.raises(ServerConfigError, match="verify-full"):
            ServerConfig.from_dict(d_weak)

    # 4. Missing sslrootcert_file -> refused
    d_no_ca = copy.deepcopy(d)
    del d_no_ca["connections"]["lb_ops"]["tls"]["sslrootcert_file"]
    with pytest.raises(ServerConfigError, match="sslrootcert_file"):
        ServerConfig.from_dict(d_no_ca)

    # 5. Setting storage_class on connection or inside lakebase block -> refused
    for sc_val in ("local", "object_storage"):
        d_sc_conn = copy.deepcopy(d)
        d_sc_conn["connections"]["lb_ops"]["storage_class"] = sc_val
        with pytest.raises(ServerConfigError, match="storage_class"):
            ServerConfig.from_dict(d_sc_conn)

        d_sc_lb = copy.deepcopy(d)
        d_sc_lb["connections"]["lb_ops"]["lakebase"]["storage_class"] = sc_val
        with pytest.raises(ServerConfigError, match="storage_class"):
            ServerConfig.from_dict(d_sc_lb)


def test_credential_exclusivity_accounts_and_file_permissions(base_cfg, tmp_path):
    good_file = tmp_path / "good.dsn"
    good_file.write_text("postgresql://u:pw@localhost/db\n")
    os.chmod(good_file, 0o600)

    bad_file = tmp_path / "world_readable.dsn"
    bad_file.write_text("postgresql://u:pw@localhost/db\n")
    os.chmod(bad_file, 0o644)

    # 1. Both dsn_env and dsn_file -> refused
    d1 = copy.deepcopy(base_cfg)
    d1["connections"] = {
        "pg": {"kind": "postgres", "dsn_env": "PG_DSN", "dsn_file": str(good_file)}
    }
    with pytest.raises(ServerConfigError, match="exactly one"):
        ServerConfig.from_dict(d1)

    # 2. Both dsn_env and accounts -> refused
    d2 = copy.deepcopy(base_cfg)
    d2["connections"] = {
        "pg": {
            "kind": "postgres",
            "dsn_env": "PG_DSN",
            "accounts": {"interactive": {"dsn_env": "PG_DSN"}},
        }
    }
    with pytest.raises(ServerConfigError, match="exactly one"):
        ServerConfig.from_dict(d2)

    # 3. Missing all credentials -> refused
    d3 = copy.deepcopy(base_cfg)
    d3["connections"] = {"pg": {"kind": "postgres"}}
    with pytest.raises(ServerConfigError, match="dsn_env"):
        ServerConfig.from_dict(d3)

    # 4. Accounts missing 'interactive' -> refused
    d4 = copy.deepcopy(base_cfg)
    d4["connections"] = {
        "pg": {"kind": "postgres", "accounts": {"bi": {"dsn_env": "PG_BI"}}}
    }
    with pytest.raises(ServerConfigError, match="interactive"):
        ServerConfig.from_dict(d4)

    # 5. Invalid workload name in accounts -> refused
    d5 = copy.deepcopy(base_cfg)
    d5["connections"] = {
        "pg": {
            "kind": "postgres",
            "accounts": {
                "interactive": {"dsn_env": "PG_INT"},
                "superuser_workload": {"dsn_env": "PG_SUP"},
            },
        }
    }
    with pytest.raises(ServerConfigError, match="invalid account"):
        ServerConfig.from_dict(d5)

    # 6. Account with both dsn_env and dsn_file -> refused
    d6 = copy.deepcopy(base_cfg)
    d6["connections"] = {
        "pg": {
            "kind": "postgres",
            "accounts": {
                "interactive": {"dsn_env": "PG_INT", "dsn_file": str(good_file)}
            },
        }
    }
    with pytest.raises(ServerConfigError, match="cannot specify both"):
        ServerConfig.from_dict(d6)

    # 7. Group/world-readable dsn_file (0644) -> refused
    d7 = copy.deepcopy(base_cfg)
    d7["connections"] = {"pg": {"kind": "postgres", "dsn_file": str(bad_file)}}
    with pytest.raises(ServerConfigError, match="group- or world-readable"):
        ServerConfig.from_dict(d7)

    # 8. check_secret_files verifies existence and 0600 mode
    missing_path = str(tmp_path / "does_not_exist.dsn")
    d8 = copy.deepcopy(base_cfg)
    d8["connections"] = {"pg": {"kind": "postgres", "dsn_file": missing_path}}
    cfg8 = ServerConfig.from_dict(d8)
    with pytest.raises(ServerConfigError, match="not found"):
        cfg8.check_secret_files()


@pytest.mark.parametrize(
    "secret_key",
    [
        "dsn",
        "password",
        "passwd",
        "secret",
        "token",
        "api_token",
        "uri",
        "url",
        "connection_string",
    ],
)
def test_literal_secret_keys_rejected_without_echoing_planted_secret(
    base_cfg, secret_key
):
    planted_secret = f"PLANTED_CANARY_VALUE_{secret_key.upper()}_7788"

    # Connection level
    d_conn = copy.deepcopy(base_cfg)
    d_conn["connections"] = {
        "pg": {
            "kind": "postgres",
            "dsn_env": "PG_DSN",
            secret_key: planted_secret,
        }
    }
    with pytest.raises(ServerConfigError) as exc_conn:
        ServerConfig.from_dict(d_conn)
    assert planted_secret not in str(exc_conn.value)

    # Account level
    d_acc = copy.deepcopy(base_cfg)
    d_acc["connections"] = {
        "pg": {
            "kind": "postgres",
            "accounts": {
                "interactive": {
                    "dsn_env": "PG_DSN",
                    secret_key: planted_secret,
                }
            },
        }
    }
    with pytest.raises(ServerConfigError) as exc_acc:
        ServerConfig.from_dict(d_acc)
    assert planted_secret not in str(exc_acc.value)

    # lakebase.cold_start level
    d_cs = copy.deepcopy(base_cfg)
    lb_block = _valid_lakebase_block()
    lb_block["cold_start"][secret_key] = planted_secret
    d_cs["connections"] = {
        "lb_ops": {
            "kind": "lakebase",
            "dsn_env": "LB_DSN",
            "tls": {"sslmode": "verify-full", "sslrootcert_file": "/etc/ca.pem"},
            "lakebase": lb_block,
        }
    }
    with pytest.raises(ServerConfigError) as exc_cs:
        ServerConfig.from_dict(d_cs)
    assert planted_secret not in str(exc_cs.value)


def test_planted_dsn_in_dsn_env_or_dsn_file_never_echoed(base_cfg):
    planted_uri = "postgresql://admin:SUPER_SECRET_DSN_PW_4242@10.20.30.40:5432/cbn"

    for field_name in ("dsn_env", "dsn_file"):
        d = copy.deepcopy(base_cfg)
        d["connections"] = {"pg": {"kind": "postgres", field_name: planted_uri}}
        with pytest.raises(ServerConfigError) as exc_info:
            ServerConfig.from_dict(d)
        assert "SUPER_SECRET_DSN_PW_4242" not in str(exc_info.value)
        assert planted_uri not in str(exc_info.value)


def test_role_guard_config_rules(base_cfg, caplog):
    # 1. role_guard: false allowed on postgres with warning
    d_pg = copy.deepcopy(base_cfg)
    d_pg["connections"] = {
        "pg_legacy": {
            "kind": "postgres",
            "dsn_env": "PG_DSN",
            "role_guard": False,
        }
    }
    with caplog.at_level(logging.WARNING, logger="seeknal.serving.server.config"):
        cfg = ServerConfig.from_dict(d_pg)
    assert cfg.connections["pg_legacy"].role_guard is False
    assert any("role_guard: false" in r.getMessage() for r in caplog.records)

    # 2. role_guard: false refused on lakebase (both legacy and with block)
    d_lb_leg = copy.deepcopy(base_cfg)
    d_lb_leg["connections"] = {
        "lb": {"kind": "lakebase", "dsn_env": "LB_DSN", "role_guard": False}
    }
    with pytest.raises(ServerConfigError, match="role_guard"):
        ServerConfig.from_dict(d_lb_leg)

    d_lb_blk = copy.deepcopy(base_cfg)
    d_lb_blk["connections"] = {
        "lb_ops": {
            "kind": "lakebase",
            "dsn_env": "LB_DSN",
            "role_guard": False,
            "tls": {"sslmode": "verify-full", "sslrootcert_file": "/etc/ca.pem"},
            "lakebase": _valid_lakebase_block(),
        }
    }
    with pytest.raises(ServerConfigError, match="role_guard"):
        ServerConfig.from_dict(d_lb_blk)


def test_storage_class_on_other_engines_and_cursor_key_env(base_cfg, tmp_path):
    ext_dir = tmp_path / "empty_exts"
    ext_dir.mkdir()

    d = copy.deepcopy(base_cfg)
    d["connections"] = {
        "sr": {
            "kind": "starrocks",
            "storage_class": "local",
            "accounts": {"interactive": {"url_env": "SR_URL"}},
        },
        "cube": {
            "kind": "cube",
            "host": "cube-host",
            "port": 15432,
            "dbname": "db",
            "sql_user": "seeknal",
            "storage_class": "object_storage",
        },
        "duck_local": {
            "kind": "duckdb",
            "profile": "local_file",
            "path": ":memory:",
            "storage_class": "local",
        },
    }
    cfg = ServerConfig.from_dict(d)
    assert isinstance(cfg.connections["sr"], StarRocksConnectionConfig)
    assert cfg.connections["sr"].storage_class == "local"
    assert isinstance(cfg.connections["cube"], CubeConnectionConfig)
    assert cfg.connections["cube"].storage_class == "object_storage"
    assert isinstance(cfg.connections["duck_local"], DuckDBConnectionConfig)
    assert cfg.connections["duck_local"].storage_class == "local"

    # DuckDB iceberg_direct rejects storage_class='local'
    d_ice = copy.deepcopy(base_cfg)
    d_ice["connections"] = {
        "lake": {
            "kind": "duckdb",
            "profile": "iceberg_direct",
            "storage_class": "local",
            "extension_directory": str(ext_dir),
            "extensions": ["httpfs", "avro", "iceberg"],
            "allowed_s3_prefixes": ["s3://warehouse/gold/"],
            "recycle_seconds": 120,
            "iceberg_catalogs": {
                "lake": {
                    "endpoint": "https://lk.internal/catalog",
                    "warehouse": "main",
                    "token_uri": "https://kc.internal/token",
                    "client_id_env": "LK_ID",
                    "client_secret_env": "LK_SECRET",
                    "access_delegation": "vended",
                    "s3": {
                        "endpoint": "s3.internal:9000",
                        "region": "local",
                        "key_id_env": "S3_KEY",
                        "secret_env": "S3_SEC",
                        "use_ssl": True,
                    },
                }
            },
        }
    }
    with pytest.raises(ServerConfigError, match="storage_class"):
        ServerConfig.from_dict(d_ice)

    # Invalid cursor_key_env rejected without leaking secret-like strings
    d_bad_cur = copy.deepcopy(base_cfg)
    d_bad_cur["cursor_key_env"] = "secret://PLANTED_CURSOR_SECRET_5511@bad"
    d_bad_cur["connections"] = {"pg": {"kind": "postgres", "dsn_env": "PG_DSN"}}
    with pytest.raises(ServerConfigError) as exc_cur:
        ServerConfig.from_dict(d_bad_cur)
    assert "PLANTED_CURSOR_SECRET_5511" not in str(exc_cur.value)


def test_b_m4_postgres_weak_sslmode_requires_allow_insecure_tls_and_lakebase_forbids_it(
    base_cfg, caplog
):
    for weak_mode in ("disable", "allow", "prefer", "require", "verify-ca"):
        d_refused = copy.deepcopy(base_cfg)
        d_refused["connections"] = {
            "pg": {
                "kind": "postgres",
                "dsn_env": "PG_DSN",
                "tls": {"sslmode": weak_mode},
            }
        }
        with pytest.raises(ServerConfigError, match="allow_insecure_tls"):
            ServerConfig.from_dict(d_refused)

        d_allowed = copy.deepcopy(base_cfg)
        d_allowed["connections"] = {
            "pg": {
                "kind": "postgres",
                "dsn_env": "PG_DSN",
                "tls": {"sslmode": weak_mode, "allow_insecure_tls": True},
            }
        }
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="seeknal.serving.server.config"):
            cfg_ok = ServerConfig.from_dict(d_allowed)
        assert cfg_ok.connections["pg"].tls.sslmode == weak_mode
        assert cfg_ok.connections["pg"].tls.allow_insecure_tls is True
        assert any(
            "allow_insecure_tls: true" in r.getMessage() for r in caplog.records
        )

    # Lakebase with endpoint block ALWAYS forbids allow_insecure_tls: true
    d_lb = copy.deepcopy(base_cfg)
    d_lb["connections"] = {
        "lb_ops": {
            "kind": "lakebase",
            "dsn_env": "LB_DSN",
            "tls": {
                "sslmode": "verify-full",
                "sslrootcert_file": "/etc/ca.pem",
                "allow_insecure_tls": True,
            },
            "lakebase": _valid_lakebase_block(),
        }
    }
    with pytest.raises(ServerConfigError, match="allow_insecure_tls"):
        ServerConfig.from_dict(d_lb)
