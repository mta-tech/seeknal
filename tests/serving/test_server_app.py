"""Tests for Seeknal Serving application assembly and endpoints."""

from __future__ import annotations

import base64
import datetime
import hashlib
import json
import os
import stat
import time
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving.adapters import (
    CubeAdapter,
    DBAPIAdapter,
    EngineConfigError,
    EngineError,
    PostgresPool,
    WorkloadRoutedAdapter,
)
from seeknal.serving.discovery.build import DiscoveryResult
from seeknal.serving.grant_token import sign_grant_token
import seeknal.serving.server.app as app_mod
from seeknal.serving.server.app import ServerHandle, build_server
from seeknal.serving.server.config import ServerConfig, ServerConfigError
from seeknal.serving.service import PreparedQuery


@pytest.fixture
def ec_key_pair():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pn = pub.public_numbers()
    x_b64 = (
        base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    )
    y_b64 = (
        base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii")
    )
    jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": x_b64,
        "y": y_b64,
        "kid": "app-test-key",
    }
    return priv, pub, jwk


@pytest.fixture
def jwks_path(tmp_path, ec_key_pair):
    _, _, jwk = ec_key_pair
    p = tmp_path / "keys.jwks.json"
    p.write_text(json.dumps({"keys": [jwk]}))
    return str(p)


@pytest.fixture
def static_catalog_files(tmp_path):
    pf = tmp_path / "placement.yaml"
    pf.write_text("""
engines:
  duck: {kind: duckdb, dialect: duckdb}
tables:
  users:
    keys: [id]
    row_count: 2
    serve:
      duck: users
cubes: {}
""")
    sf = tmp_path / "schemas.json"
    sf.write_text(json.dumps({"users": ["id", "name"]}))
    return str(pf), str(sf)


def test_static_duckdb_end_to_end_and_readyz(
    ec_key_pair, jwks_path, static_catalog_files
):
    priv, _, _ = ec_key_pair
    pf, sf = static_catalog_files

    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_path},
        "require_grant_typ": True,
        "cube_mode": "refuse",
        "placement": {
            "static": {
                "placement_file": pf,
                "schemas_file": sf,
            }
        },
        "connections": {
            "duck": {"kind": "duckdb", "path": ":memory:"},
        },
    }
    config = ServerConfig.from_dict(cfg_dict)

    handle = build_server(config, connect_factories={"duck": lambda: con})
    assert isinstance(handle, ServerHandle)
    assert handle.refresher is None

    with TestClient(handle.app) as client:
        # 1. /healthz 200
        resp_h = client.get("/healthz")
        assert resp_h.status_code == 200
        assert resp_h.json() == {"status": "ok"}

        # 2. /readyz 200 for static placement
        resp_r = client.get("/readyz")
        assert resp_r.status_code == 200
        assert resp_r.json() == {"ready": True}

        # 3. /v1/query with typ: "grant" succeeds
        sql = "SELECT id, name FROM users ORDER BY id"
        now = time.time()
        claims_grant = {
            "sub": "user@test.org",
            "ws": "analytics",
            "jti": "jti-query-001",
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 60,
            "typ": "grant",
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "max_rows": 50,
            "tables": {
                "users": {
                    "columns": ["id", "name"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        }
        token_grant = sign_grant_token(claims_grant, priv, kid="app-test-key")

        resp_q = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token_grant}"},
            json={"sql": sql},
        )
        assert resp_q.status_code == 200
        data = resp_q.json()
        assert data["columns"] == ["id", "name"]
        assert data["rows"] == [[1, "Alice"], [2, "Bob"]]

        # 4. Same grant without typ fails with 401
        claims_no_typ = dict(claims_grant)
        del claims_no_typ["typ"]
        token_no_typ = sign_grant_token(claims_no_typ, priv, kid="app-test-key")

        resp_bad = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token_no_typ}"},
            json={"sql": sql},
        )
        assert resp_bad.status_code == 401
        err_data = resp_bad.json()
        assert err_data["error"] == "grant_token_error"
        assert "expected 'grant'" in err_data["message"]


def test_discovery_mode_readyz_and_error_handling(ec_key_pair, jwks_path):
    should_fail = False

    def fake_discover():
        if should_fail:
            raise RuntimeError(
                "Database connection password leaked SECRET_PASSWORD_999"
            )
        return DiscoveryResult(
            placement={
                "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {
                    "lakekeeper.users": {
                        "keys": ["id"],
                        "row_count": 10,
                        "serve": {"duck": "users"},
                    }
                },
                "cubes": {},
            },
            schemas={"lakekeeper.users": ["id", "name"]},
            warnings=(),
            discovered_at=datetime.datetime.now(datetime.timezone.utc),
        )

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_path},
        "default_catalog": "lakekeeper",
        "placement": {
            "discovery": {
                "default_catalog": "lakekeeper",
                "refresh_interval_seconds": 60,
            }
        },
        "connections": {
            "duck": {"kind": "duckdb", "path": ":memory:"},
        },
    }
    config = ServerConfig.from_dict(cfg_dict)

    # 1. Before first refresh: /readyz returns 503
    handle = build_server(config, discover_fn=fake_discover)
    assert handle.refresher is not None
    assert handle.refresher.last_success_at is None

    client = TestClient(handle.app)
    resp_before = client.get("/readyz")
    assert resp_before.status_code == 503
    assert resp_before.json() == {
        "ready": False,
        "reason": "catalog_not_loaded",
    }

    # 2. After refresh succeeds: /readyz returns 200
    ok = handle.refresher.refresh_once()
    assert ok is True
    assert handle.refresher.last_success_at is not None

    resp_after = client.get("/readyz")
    assert resp_after.status_code == 200
    assert resp_after.json() == {"ready": True}

    # 3. Failing refresh keeps it 503 and leaks no error text
    should_fail = True
    handle_fail = build_server(config, discover_fn=fake_discover)
    handle_fail.refresher.refresh_once()
    assert handle_fail.refresher.last_success_at is None

    client_fail = TestClient(handle_fail.app)
    resp_fail = client_fail.get("/readyz")
    assert resp_fail.status_code == 503
    assert resp_fail.json() == {
        "ready": False,
        "reason": "catalog_not_loaded",
    }
    assert "SECRET_PASSWORD_999" not in resp_fail.text


def test_starrocks_workload_routing_fake_factories(
    tmp_path, ec_key_pair, jwks_path
):
    pf = tmp_path / "sr_placement.yaml"
    pf.write_text("""
engines:
  sr: {kind: starrocks, dialect: starrocks}
tables:
  users:
    keys: [id]
    row_count: 2
    serve:
      sr: users
cubes: {}
""")
    sf = tmp_path / "sr_schemas.json"
    sf.write_text(json.dumps({"users": ["id", "name"]}))

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_path},
        "placement": {
            "static": {
                "placement_file": str(pf),
                "schemas_file": str(sf),
            }
        },
        "connections": {
            "sr": {
                "kind": "starrocks",
                "accounts": {
                    "interactive": {
                        "host": "sr-fe",
                        "port": 9030,
                        "user": "u1",
                        "password_env": "SR_PW1",
                    },
                    "bi": {
                        "host": "sr-fe",
                        "port": 9030,
                        "user": "u2",
                        "password_env": "SR_PW2",
                    },
                },
            },
        },
    }
    config = ServerConfig.from_dict(cfg_dict)

    fake_conn_interactive = MagicMock()
    fake_conn_bi = MagicMock()
    fake_factory_interactive = MagicMock(return_value=fake_conn_interactive)
    fake_factory_bi = MagicMock(return_value=fake_conn_bi)

    factories = {
        "sr": {
            "interactive": fake_factory_interactive,
            "bi": fake_factory_bi,
        }
    }

    env = {"SR_PW1": "p1", "SR_PW2": "p2"}
    handle = build_server(config, env=env, connect_factories=factories)

    sr_adapter = handle.adapters["sr"]
    assert isinstance(sr_adapter, WorkloadRoutedAdapter)
    assert "interactive" in sr_adapter.by_workload
    assert "bi" in sr_adapter.by_workload

    # Interactive adapter connect callable should be fake_factory_interactive
    adapter_interactive = sr_adapter.by_workload["interactive"]
    assert adapter_interactive._connect == fake_factory_interactive

    adapter_bi = sr_adapter.by_workload["bi"]
    assert adapter_bi._connect == fake_factory_bi


def test_cube_mode_adapters(tmp_path, ec_key_pair, jwks_path):
    pf = tmp_path / "cube_placement.yaml"
    pf.write_text("""
engines:
  cube: {kind: cube, dialect: postgres}
tables: {}
cubes:
  orders_cube:
    engine: cube
    measures: [total]
    dimensions: [region]
""")
    sf = tmp_path / "cube_schemas.json"
    sf.write_text(json.dumps({"orders_cube": ["region", "total"]}))

    fake_cube_conn = MagicMock()
    fake_factory = MagicMock(return_value=fake_cube_conn)

    # 1. Cube in jwt mode builds CubeAdapter
    cfg_jwt = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_path},
        "cube_mode": "jwt",
        "placement": {
            "static": {
                "placement_file": str(pf),
                "schemas_file": str(sf),
            }
        },
        "connections": {
            "cube": {
                "kind": "cube",
                "host": "cube",
                "port": 15432,
                "dbname": "db",
                "sql_user": "seeknal",
                "secret_env": "CUBE_SQL_SECRET",
                "connect_timeout_seconds": 10,
            },
        },
    }
    env = {"CUBE_SQL_SECRET": "cube-super-secret-key"}
    config_jwt = ServerConfig.from_dict(cfg_jwt)
    handle_jwt = build_server(
        config_jwt, env=env, connect_factories={"cube": fake_factory}
    )
    cube_adapter_jwt = handle_jwt.adapters["cube"]
    assert isinstance(cube_adapter_jwt, CubeAdapter)
    assert cube_adapter_jwt._security.sql_user == "seeknal"
    assert cube_adapter_jwt._security.secret == b"cube-super-secret-key"

    # 2. Cube in refuse mode builds DBAPIAdapter
    cfg_refuse = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_path},
        "cube_mode": "refuse",
        "placement": {
            "static": {
                "placement_file": str(pf),
                "schemas_file": str(sf),
            }
        },
        "connections": {
            "cube": {
                "kind": "cube",
                "host": "cube",
                "port": 15432,
                "dbname": "db",
                "sql_user": "seeknal",
                "connect_timeout_seconds": 10,
            },
        },
    }
    config_refuse = ServerConfig.from_dict(cfg_refuse)
    handle_refuse = build_server(
        config_refuse, env=env, connect_factories={"cube": fake_factory}
    )
    cube_adapter_refuse = handle_refuse.adapters["cube"]
    assert isinstance(cube_adapter_refuse, DBAPIAdapter)
    assert cube_adapter_refuse._kind == "cube"


def test_build_server_discovery_with_secrets_env_file(
    tmp_path, ec_key_pair, jwks_path
):
    import stat

    sec_env_file = tmp_path / "workloads.env"
    sec_env_file.write_text(
        "LK_CLIENT_SEC=oauth-sec-123\nCUBE_API_SEC=cube-api-sec-456\n"
    )
    sec_env_file.chmod(stat.S_IRUSR | stat.S_IWUSR)

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_path},
        "secrets_env_file": str(sec_env_file),
        "placement": {
            "discovery": {
                "default_catalog": "lakekeeper",
                "lakekeeper": {
                    "url": "https://lk.internal",
                    "warehouse": "atlas",
                    "oauth": {
                        "token_url": "https://auth.internal/token",
                        "client_id": "seeknal-serving-discovery",
                        "client_secret_env": "LK_CLIENT_SEC",
                    },
                },
                "engines": {
                    "cube": {
                        "kind": "cube",
                        "dialect": "postgres",
                        "url": "http://cube.internal:4000",
                        "api_secret_env": "CUBE_API_SEC",
                    }
                },
            }
        },
        "connections": {
            "cube": {
                "kind": "cube",
                "host": "cube.internal",
                "port": 15432,
                "dbname": "cube",
                "sql_user": "seeknal",
            },
            "duck": {"kind": "duckdb", "path": ":memory:"},
        },
    }
    config = ServerConfig.from_dict(cfg_dict)
    handle = build_server(config, discover_fn=lambda: DiscoveryResult(
        placement={"engines": {}, "tables": {}, "cubes": {}},
        schemas={},
        warnings=(),
        discovered_at=datetime.datetime.now(datetime.timezone.utc),
    ))
    assert handle.refresher is not None
    assert handle.refresher.refresh_once() is True


def test_build_server_fails_early_before_logger_refresher_or_adapters(
    tmp_path, jwks_path, monkeypatch
):
    import seeknal.serving.server.app as app_mod

    pf = tmp_path / "placement.yaml"
    pf.write_text("""
engines:
  duck: {kind: duckdb, dialect: duckdb}
  sr: {kind: starrocks, dialect: starrocks}
tables: {}
cubes: {}
""")
    sf = tmp_path / "schemas.json"
    sf.write_text("{}")
    log_file = tmp_path / "should_not_be_created.log"

    cfg_dict = {
        "grant_keys": {"jwks_file": jwks_path},
        "logging": {"level": "INFO", "format": "json", "file": str(log_file)},
        "placement": {
            "static": {
                "placement_file": str(pf),
                "schemas_file": str(sf),
            }
        },
        "connections": {
            "duck": {"kind": "duckdb", "path": ":memory:"},
        },
    }
    config = ServerConfig.from_dict(cfg_dict)

    def _forbidden(*args, **kwargs):
        raise AssertionError("Should not be called when preflight fails")

    monkeypatch.setattr(app_mod, "configure_logging", _forbidden)
    monkeypatch.setattr(app_mod, "DuckDBAdapter", _forbidden)
    monkeypatch.setattr(app_mod, "PlacementRefresher", _forbidden)

    with pytest.raises(ServerConfigError, match="connections.sr"):
        build_server(config)

    assert not log_file.exists()


def test_build_server_loads_static_artifacts_once(
    tmp_path, jwks_path, static_catalog_files, monkeypatch
):
    import builtins

    pf, sf = static_catalog_files
    real_open = builtins.open
    open_counts = {pf: 0, sf: 0}

    def _counting_open(file, *args, **kwargs):
        path_str = str(file)
        if path_str in open_counts:
            open_counts[path_str] += 1
        return real_open(file, *args, **kwargs)

    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "placement": {
                "static": {
                    "placement_file": pf,
                    "schemas_file": sf,
                }
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )
    # Structural from_dict must not have read static placement/schemas files yet
    assert open_counts[pf] == 0
    assert open_counts[sf] == 0

    monkeypatch.setattr(builtins, "open", _counting_open)
    handle = build_server(cfg, connect_factories={"duck": lambda: duckdb.connect()})
    assert isinstance(handle, ServerHandle)
    assert open_counts[pf] == 1
    assert open_counts[sf] == 1


def test_build_server_invalid_placement_never_leaks_dsn_or_canary(
    tmp_path, jwks_path
):
    dsn = "postgresql://demo:syntheticCanary@host/db"
    pf = tmp_path / "placement.yaml"
    pf.write_text(f"""
engines:
  duck:
    kind: "{dsn}"
    dialect: duckdb
tables: {{}}
cubes: {{}}
""")
    sf = tmp_path / "schemas.json"
    sf.write_text("{}")

    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "placement": {
                "static": {
                    "placement_file": str(pf),
                    "schemas_file": str(sf),
                }
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )

    with pytest.raises(ServerConfigError) as exc_info:
        build_server(cfg)

    msg = str(exc_info.value)
    assert dsn not in msg
    assert "syntheticCanary" not in msg
    assert str(pf) in msg
    assert "engines" in msg


def test_build_server_propagates_compilation_cache_max_entries(
    jwks_path, static_catalog_files
):
    """build_server passes compilation_cache_max_entries to ServingPipeline in both static and discovery modes."""
    pf, sf = static_catalog_files

    # 1. Default capacity 256
    cfg_default = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        }
    )
    h_default = build_server(cfg_default, connect_factories={"duck": lambda: duckdb.connect()})
    assert h_default.pipeline.compilation_cache_info() == {
        "hits": 0,
        "misses": 0,
        "evictions": 0,
        "size": 0,
        "max_entries": 256,
    }

    # 2. Custom capacity 64 and disabled 0
    for cap in (64, 0, 1024):
        cfg_cap = ServerConfig.from_dict(
            {
                "grant_keys": {"jwks_file": jwks_path},
                "compilation_cache_max_entries": cap,
                "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
                "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
            }
        )
        h_cap = build_server(cfg_cap, connect_factories={"duck": lambda: duckdb.connect()})
        assert h_cap.pipeline.compilation_cache_info()["max_entries"] == cap


def test_postgres_accounts_share_one_pool_per_credential_and_harden_session(
    tmp_path, jwks_path, static_catalog_files, monkeypatch
):
    pf, sf = static_catalog_files
    ask_dsn_file = tmp_path / "pg_ask.dsn"
    planted_ask_pw = "PLANTED_ASK_DSN_SECRET_9911"
    planted_bi_pw = "PLANTED_BI_DSN_SECRET_2244"
    ask_dsn_file.write_text(
        f"postgresql://seeknal_ask:{planted_ask_pw}@127.0.0.1:5432/cbn\n"
    )
    os.chmod(ask_dsn_file, stat.S_IRUSR | stat.S_IWUSR)

    env = {
        "CBN_PG_REPORT_DSN_BI": f"postgresql://seeknal_bi:{planted_bi_pw}@127.0.0.1:5432/cbn",
    }

    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "ops": {"enabled": True, "public_readyz": True},
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
                "pg_report": {
                    "kind": "postgres",
                    "mode": "read_only",
                    "accounts": {
                        "interactive": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                        "bi": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                        "report": {"dsn_env": "CBN_PG_REPORT_DSN_BI"},
                        "ask": {"dsn_file": str(ask_dsn_file)},
                    },
                    "pool_max": 6,
                    "session": {
                        "statement_timeout_ms": 30000,
                        "lock_timeout_ms": 2000,
                        "idle_in_transaction_session_timeout_ms": 10000,
                        "search_path": "pg_catalog",
                        "application_name": "seeknal-serving",
                        "startup_options": True,
                    },
                    "tls": {
                        "sslmode": "verify-full",
                        "sslrootcert_file": "/etc/seeknal/tls/cbn-ca.pem",
                    },
                },
            },
        },
        env=env,
    )

    connect_calls: list[tuple[str, dict[str, Any]]] = []
    role_row_holder = [(False, False)]

    class FakePGCursor:
        def __init__(self, conn: Any) -> None:
            self.conn = conn
            self._row: Any = None
            self.description = (("id", 23, None, None, None, None, None),)

        def execute(self, sql: str, params: Any = None) -> None:
            self.conn.statements.append((sql, params))
            s = " ".join(sql.strip().split())
            if "pg_roles" in s:
                self._row = role_row_holder[0]
            elif s == "SHOW default_transaction_read_only":
                self._row = ("on",)
            elif s == "SHOW statement_timeout":
                self._row = ("30s",)
            elif s == "SHOW lock_timeout":
                self._row = ("2s",)
            elif s == "SHOW idle_in_transaction_session_timeout":
                self._row = ("10s",)
            elif s == "SHOW search_path":
                self._row = ("pg_catalog",)
            else:
                self._row = (1,)

        def fetchone(self) -> Any:
            return self._row

        def fetchmany(self, size: int) -> list[tuple]:
            return [(42,)]

        def close(self) -> None:
            pass

    class FakePGConn:
        def __init__(self, dsn: str, kw: dict[str, Any]) -> None:
            self.dsn = dsn
            self.kw = kw
            self.closed = 0
            self.statements: list[tuple[str, Any]] = []

        def cursor(self) -> FakePGCursor:
            return FakePGCursor(self)

        def rollback(self) -> None:
            pass

        def close(self) -> None:
            self.closed = 1

    import psycopg2

    def fake_psycopg2_connect(dsn: str, **kwargs: Any) -> FakePGConn:
        connect_calls.append((dsn, dict(kwargs)))
        return FakePGConn(dsn, kwargs)

    monkeypatch.setattr(psycopg2, "connect", fake_psycopg2_connect)

    handle = build_server(
        cfg,
        env=env,
        connect_factories={"duck": lambda: duckdb.connect()},
    )

    pg_adapter = handle.adapters["pg_report"]
    assert isinstance(pg_adapter, WorkloadRoutedAdapter)
    assert set(pg_adapter.by_workload.keys()) == {
        "interactive",
        "bi",
        "report",
        "ask",
    }

    ad_int = pg_adapter.by_workload["interactive"]
    ad_bi = pg_adapter.by_workload["bi"]
    ad_rep = pg_adapter.by_workload["report"]
    ad_ask = pg_adapter.by_workload["ask"]
    assert isinstance(ad_int, DBAPIAdapter)
    assert isinstance(ad_bi, DBAPIAdapter)
    assert isinstance(ad_rep, DBAPIAdapter)
    assert isinstance(ad_ask, DBAPIAdapter)

    # Same credential shares ONE PostgresPool; distinct credential gets its own pool
    assert isinstance(ad_int.pool, PostgresPool)
    assert ad_int.pool is ad_bi.pool
    assert ad_int.pool is ad_rep.pool
    assert ad_ask.pool is not ad_int.pool

    # Missing workload ('job') raises EngineConfigError
    missing_prep = PreparedQuery(
        engine="pg_report",
        engine_kind="postgres",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        rule="point_lookup",
        reason="test",
        row_limit=5,
        masked_columns=(),
        redacted_sql="SELECT 1",
        subject="u@test.org",
        workspace="ws",
        audit_id="aud-job-1",
        cube_security_context=None,
        workload="job",
    )
    with pytest.raises(EngineConfigError, match="no engine account configured for workload 'job'"):
        pg_adapter.execute(missing_prep, timeout_seconds=5.0)

    # Execute on 'bi' and 'interactive' reuses the SAME pooled connection
    for wl, aid in (("bi", "aud-bi-1"), ("interactive", "aud-int-1")):
        p = PreparedQuery(
            engine="pg_report",
            engine_kind="postgres",
            sql="SELECT id FROM rpt WHERE id = %s",
            params=(42,),
            paramstyle="format",
            rule="point_lookup",
            reason="test",
            row_limit=5,
            masked_columns=(),
            redacted_sql="SELECT id FROM rpt WHERE id = %s",
            subject="u@test.org",
            workspace="ws",
            audit_id=aid,
            cube_security_context=None,
            workload=wl,
        )
        res = pg_adapter.execute(p, timeout_seconds=5.0)
        assert res.rows == [(42,)]

    assert len(connect_calls) == 1
    called_dsn, called_kw = connect_calls[0]
    assert planted_bi_pw in called_dsn
    assert "dsn" not in called_kw
    assert called_kw["application_name"] == "seeknal-serving"
    assert called_kw["sslmode"] == "verify-full"
    assert called_kw["sslrootcert"] == "/etc/seeknal/tls/cbn-ca.pem"
    assert "-c default_transaction_read_only=on" in called_kw["options"]

    # Superuser on 'ask' account is refused by role guard without leaking DSN
    role_row_holder[0] = (True, False)
    ask_prep = PreparedQuery(
        engine="pg_report",
        engine_kind="postgres",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        rule="point_lookup",
        reason="test",
        row_limit=5,
        masked_columns=(),
        redacted_sql="SELECT 1",
        subject="u@test.org",
        workspace="ws",
        audit_id="aud-ask-1",
        cube_security_context=None,
        workload="ask",
    )
    with pytest.raises(EngineError, match="Role guard refused connection") as exc_rg:
        pg_adapter.execute(ask_prep, timeout_seconds=5.0)
    assert planted_ask_pw not in str(exc_rg.value)

    # /readyz never leaks planted DSNs
    role_row_holder[0] = (False, False)
    with TestClient(handle.app) as pub_c, TestClient(handle.ops_app) as ops_c:
        r_ops = ops_c.get("/readyz")
        assert r_ops.status_code == 200
        assert planted_bi_pw not in r_ops.text
        assert planted_ask_pw not in r_ops.text
        assert pub_c.get("/readyz").status_code == 200


def test_discovery_wires_postgres_connects_for_configured_account(
    jwks_path, monkeypatch
):
    captured_postgres_connects: dict[str, Any] = {}

    def fake_discover(
        disc_data,
        *,
        http_client,
        lakebase_connect=None,
        postgres_connects=None,
        env=None,
    ):
        if postgres_connects is not None:
            captured_postgres_connects.update(postgres_connects)
        return DiscoveryResult(
            placement={"engines": {}, "tables": {}, "cubes": {}},
            schemas={},
            warnings=(),
            discovered_at=datetime.datetime.now(datetime.timezone.utc),
        )

    monkeypatch.setattr(app_mod, "discover", fake_discover)

    bi_conn_sentinel = MagicMock(name="bi_conn")
    int_conn_sentinel = MagicMock(name="int_conn")

    cfg_dict = {
        "grant_keys": {"jwks_file": jwks_path},
        "placement": {
            "discovery": {
                "default_catalog": "cbn",
                "refresh_interval_seconds": 60,
                "engines": {
                    "pg_report": {"kind": "postgres", "dialect": "postgres"},
                    "pg_default": {"kind": "postgres", "dialect": "postgres"},
                },
                "postgres_sources": {
                    "pg_report": {
                        "account": "bi",
                        "schemas": {"rpt": {"include": ["*"]}},
                    },
                },
            }
        },
        "connections": {
            "pg_report": {
                "kind": "postgres",
                "role_guard": False,
                "accounts": {
                    "interactive": {"dsn_env": "PG_INT_DSN"},
                    "bi": {"dsn_env": "PG_BI_DSN"},
                },
            },
            "pg_default": {
                "kind": "postgres",
                "role_guard": False,
                "dsn_env": "PG_DEF_DSN",
            },
        },
    }
    env = {
        "PG_INT_DSN": "postgresql://u1:pw1@localhost/db",
        "PG_BI_DSN": "postgresql://u2:pw2@localhost/db",
        "PG_DEF_DSN": "postgresql://u3:pw3@localhost/db",
    }
    cfg = ServerConfig.from_dict(cfg_dict, env=env)
    handle = build_server(
        cfg,
        env=env,
        connect_factories={
            "pg_report": {
                "interactive": lambda: int_conn_sentinel,
                "bi": lambda: bi_conn_sentinel,
            },
            "pg_default": lambda: int_conn_sentinel,
        },
    )
    assert handle.refresher is not None
    assert handle.refresher.refresh_once() is True

    assert "pg_report" in captured_postgres_connects
    assert "pg_default" in captured_postgres_connects
    assert captured_postgres_connects["pg_report"]() is bi_conn_sentinel
    assert captured_postgres_connects["pg_default"]() is int_conn_sentinel


def test_lakebase_cold_start_connector_wiring_and_engine_starting_propagation(
    jwks_path, static_catalog_files, monkeypatch
):
    pf, sf = static_catalog_files
    connector_inits: list[tuple[Any, dict[str, Any]]] = []

    class FakeColdStartConnector:
        def __init__(self, connect_fn: Any, cfg: Any, **kw: Any) -> None:
            self.connect_fn = connect_fn
            self.cfg = cfg
            connector_inits.append((cfg, kw))

        def connect(self, deadline: float | None = None) -> Any:
            assert deadline is not None
            raise app_mod.EngineStartingError()

    monkeypatch.setattr(app_mod, "ColdStartConnector", FakeColdStartConnector)

    env = {
        "CBN_LB_OPS_DSN": "postgresql://lb_role:lb_pw_1122@127.0.0.1:40001/ops",
        "LB_OPS_PROJECT_TOKEN": "lb_proj_token_secret_3344",
    }
    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
                "lb_ops": {
                    "kind": "lakebase",
                    "dsn_env": "CBN_LB_OPS_DSN",
                    "tls": {
                        "sslmode": "verify-full",
                        "sslrootcert_file": "/etc/seeknal/tls/lb-ca.pem",
                    },
                    "lakebase": {
                        "project_id": "prj_xxxxxxxxxxxxxxxxxxxx",
                        "branch_id": "br_xxxxxxxxxxxxxxxxxxxx",
                        "endpoint_id": "ep_xxxxxxxxxxxxxxxxxxxx",
                        "endpoint_type": "read_write",
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
            },
        },
        env=env,
    )

    handle = build_server(
        cfg,
        env=env,
        connect_factories={"duck": lambda: duckdb.connect()},
    )
    assert len(connector_inits) == 1
    passed_lb_cfg, passed_kw = connector_inits[0]
    assert passed_lb_cfg.endpoint_id == "ep_xxxxxxxxxxxxxxxxxxxx"
    assert passed_kw["metrics"] is handle.metrics

    lb_adapter = handle.adapters["lb_ops"]
    prep = PreparedQuery(
        engine="lb_ops",
        engine_kind="lakebase",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        rule="point_lookup",
        reason="test",
        row_limit=5,
        masked_columns=(),
        redacted_sql="SELECT 1",
        subject="u@test.org",
        workspace="ws",
        audit_id="aud-lb-wake",
        cube_security_context=None,
    )
    with pytest.raises(app_mod.EngineStartingError) as exc_start:
        lb_adapter.execute(prep, timeout_seconds=5.0)
    assert exc_start.value.code == "engine_starting"
    assert "lb_proj_token_secret_3344" not in str(exc_start.value)
    assert "lb_pw_1122" not in str(exc_start.value)


