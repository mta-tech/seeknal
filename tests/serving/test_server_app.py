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

    handle = build_server(
        config,
        connect_factories={"duck": lambda: con},
        cursor_key="test-app-cursor-key-32-bytes-valid",
    )
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
    handle = build_server(
        config,
        discover_fn=fake_discover,
        cursor_key="test-app-cursor-key-32-bytes-valid",
    )
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
    handle_fail = build_server(
        config,
        discover_fn=fake_discover,
        cursor_key="test-app-cursor-key-32-bytes-valid",
    )
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

    env = {
        "SR_PW1": "p1",
        "SR_PW2": "p2",
        "SEEKNAL_CURSOR_KEY": "test-app-cursor-key-32-bytes-valid",
    }
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
    env = {
        "CUBE_SQL_SECRET": "cube-super-secret-key",
        "SEEKNAL_CURSOR_KEY": "test-app-cursor-key-32-bytes-valid",
    }
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
    handle = build_server(
        config,
        discover_fn=lambda: DiscoveryResult(
            placement={"engines": {}, "tables": {}, "cubes": {}},
            schemas={},
            warnings=(),
            discovered_at=datetime.datetime.now(datetime.timezone.utc),
        ),
        cursor_key="test-app-cursor-key-32-bytes-valid",
    )
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
    handle = build_server(
        cfg,
        connect_factories={"duck": lambda: duckdb.connect()},
        cursor_key="test-app-cursor-key-32-bytes-valid",
    )
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
    h_default = build_server(
        cfg_default,
        connect_factories={"duck": lambda: duckdb.connect()},
        cursor_key="test-app-cursor-key-32-bytes-valid",
    )
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
        h_cap = build_server(
            cfg_cap,
            connect_factories={"duck": lambda: duckdb.connect()},
            cursor_key="test-app-cursor-key-32-bytes-valid",
        )
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
        "SEEKNAL_CURSOR_KEY": "test-app-cursor-key-32-bytes-valid",
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
    captured_connections: list[Any] = []

    def fake_discover(
        disc_data,
        *,
        http_client,
        lakebase_connect=None,
        postgres_connects=None,
        connections=None,
        env=None,
    ):
        if postgres_connects is not None:
            captured_postgres_connects.update(postgres_connects)
        captured_connections.append(connections)
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
        "SEEKNAL_CURSOR_KEY": "test-app-cursor-key-32-bytes-valid",
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
    assert captured_connections == [cfg.connections]


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
        "SEEKNAL_CURSOR_KEY": "test-app-cursor-key-32-bytes-valid",
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
    assert lb_adapter.storage_class == "object_storage"
    assert lb_adapter.account == "default"
    assert lb_adapter.lakebase_endpoint == {
        "project_id": "prj_xxxxxxxxxxxxxxxxxxxx",
        "branch_id": "br_xxxxxxxxxxxxxxxxxxxx",
        "endpoint_id": "ep_xxxxxxxxxxxxxxxxxxxx",
        "endpoint_type": "read_write",
    }
    assert lb_adapter.last_cold_start is True


def test_runtime_discovery_passes_connections_storage_class_through_build_server_and_refresh(
    jwks_path, monkeypatch
):
    from dataclasses import dataclass
    import seeknal.serving.discovery.build as build_mod
    from tests.serving.fake_postgres_catalog import FakePostgresCatalog

    @dataclass(frozen=True)
    class FlaggedIcebergTable:
        namespace: str
        name: str
        columns: tuple[str, ...]
        keys: tuple[str, ...]
        row_count: int | None
        current_snapshot_id: int | None
        snapshot_ms: int | None
        pii: bool = False

    class FakeLakekeeperSource:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.warnings: list[str] = []

        def discover(self) -> list[FlaggedIcebergTable]:
            return [
                FlaggedIcebergTable(
                    namespace="src_pii",
                    name="customer",
                    columns=("customer_id", "full_name"),
                    keys=("customer_id",),
                    row_count=100,
                    current_snapshot_id=11,
                    snapshot_ms=1777636800000,
                    pii=True,
                )
            ]

    monkeypatch.setattr(build_mod, "LakekeeperSource", FakeLakekeeperSource)

    pg_cat = FakePostgresCatalog()
    pg_cat.add_table(
        "src_pii",
        "customer",
        reltuples=100,
        columns=[("customer_id", "text", True), ("full_name", "text", True)],
        indexes=[
            {
                "index_name": "customer_pkey",
                "amname": "btree",
                "columns": ["customer_id"],
                "is_primary": True,
            }
        ],
    )

    env = {
        "PG_S3_DSN": "postgresql://svc:pw@localhost:5432/cbn",
        "SR_PW": "sr-secret-pw",
        "SEEKNAL_CURSOR_KEY": "test-app-cursor-key-32-bytes-valid",
    }

    # 1. Lakekeeper-flagged PII table on kind: postgres declared storage_class: object_storage
    # must fail closed through build_server + initial/periodic refresh!
    cfg_pg_obj = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "default_catalog": "cbn",
            "placement": {
                "discovery": {
                    "default_catalog": "cbn",
                    "refresh_interval_seconds": 60,
                    "lakekeeper": {
                        "url": "https://lk.internal/catalog",
                        "warehouse": "cbn",
                    },
                    "engines": {
                        "pg_s3": {"kind": "postgres", "dialect": "postgres"},
                    },
                    "postgres_sources": {
                        "pg_s3": {
                            "logical_catalog": "cbn",
                            "schemas": {"src_pii": {"include": ["customer"]}},
                        }
                    },
                }
            },
            "connections": {
                "pg_s3": {
                    "kind": "postgres",
                    "dsn_env": "PG_S3_DSN",
                    "role_guard": False,
                    "storage_class": "object_storage",
                }
            },
        },
        env=env,
    )

    with pytest.raises(ServerConfigError, match="has storage_class object_storage"):
        build_server(
            cfg_pg_obj,
            env=env,
            connect_factories={"pg_s3": pg_cat.connect},
            run_initial_refresh=True,
        )

    handle_pg_obj = build_server(
        cfg_pg_obj,
        env=env,
        connect_factories={"pg_s3": pg_cat.connect},
    )
    assert handle_pg_obj.adapters["pg_s3"].storage_class == "object_storage"
    assert handle_pg_obj.refresher is not None
    assert handle_pg_obj.refresher.refresh_once() is False
    assert handle_pg_obj.refresher.last_success_at is None
    assert "has storage_class object_storage" in str(
        handle_pg_obj.refresher.last_refresh_error[0]
    )

    # 2. StarRocks declared storage_class: local is honoured at build_server + refresh_once()
    cfg_sr_local = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "default_catalog": "cbn",
            "placement": {
                "discovery": {
                    "default_catalog": "cbn",
                    "refresh_interval_seconds": 60,
                    "lakekeeper": {
                        "url": "https://lk.internal/catalog",
                        "warehouse": "cbn",
                    },
                    "engines": {
                        "sr": {"kind": "starrocks", "dialect": "starrocks"},
                    },
                }
            },
            "connections": {
                "sr": {
                    "kind": "starrocks",
                    "storage_class": "local",
                    "accounts": {
                        "interactive": {
                            "host": "sr.internal",
                            "port": 9030,
                            "user": "svc",
                            "password_env": "SR_PW",
                        }
                    },
                }
            },
        },
        env=env,
    )
    handle_sr_local = build_server(
        cfg_sr_local,
        env=env,
        connect_factories={"sr": {"interactive": MagicMock()}},
        run_initial_refresh=True,
    )
    assert handle_sr_local.adapters["sr"].storage_class == "local"
    assert handle_sr_local.refresher is not None
    assert handle_sr_local.refresher.last_success_at is not None
    assert "cbn.src_pii.customer" in handle_sr_local.pipeline.placement.tables
    assert handle_sr_local.pipeline.placement.tables["cbn.src_pii.customer"].pii is True
    assert handle_sr_local.refresher.refresh_once() is True

    # Without storage_class: local, StarRocks defaults to object_storage and refuses Lakekeeper PII at refresh!
    cfg_sr_obj = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "default_catalog": "cbn",
            "placement": {
                "discovery": {
                    "default_catalog": "cbn",
                    "refresh_interval_seconds": 60,
                    "lakekeeper": {
                        "url": "https://lk.internal/catalog",
                        "warehouse": "cbn",
                    },
                    "engines": {
                        "sr": {"kind": "starrocks", "dialect": "starrocks"},
                    },
                }
            },
            "connections": {
                "sr": {
                    "kind": "starrocks",
                    "accounts": {
                        "interactive": {
                            "host": "sr.internal",
                            "port": 9030,
                            "user": "svc",
                            "password_env": "SR_PW",
                        }
                    },
                }
            },
        },
        env=env,
    )
    handle_sr_obj = build_server(
        cfg_sr_obj,
        env=env,
        connect_factories={"sr": {"interactive": MagicMock()}},
    )
    assert handle_sr_obj.refresher is not None
    assert handle_sr_obj.refresher.refresh_once() is False
    assert "has storage_class object_storage" in str(
        handle_sr_obj.refresher.last_refresh_error[0]
    )


def test_b_m2_multi_account_lakebase_shares_single_connector_by_endpoint_id_and_populates_cold_start(
    jwks_path, static_catalog_files, monkeypatch
):
    import httpx
    from tests.serving.fake_postgres_catalog import FakePostgresCatalog

    pf, sf = static_catalog_files
    post_start_calls = 0
    endpoint_awake = False

    def cp_transport(request: httpx.Request) -> httpx.Response:
        nonlocal post_start_calls, endpoint_awake
        if request.method == "POST" and request.url.path.endswith("/start"):
            post_start_calls += 1
            return httpx.Response(202, json={"state": "starting"})
        endpoint_awake = True
        return httpx.Response(200, json={"state": "running"})

    real_httpx_client = httpx.Client

    def _make_cp_client(*_a: Any, **_kw: Any) -> Any:
        return real_httpx_client(transport=httpx.MockTransport(cp_transport))

    monkeypatch.setattr(app_mod.httpx, "Client", _make_cp_client)

    int_cat = FakePostgresCatalog()
    bi_cat = FakePostgresCatalog()

    def connect_int(**kw: Any) -> Any:
        if not endpoint_awake:
            raise ConnectionRefusedError("endpoint suspended")
        return int_cat.connect(**kw)

    def connect_bi(**kw: Any) -> Any:
        if not endpoint_awake:
            raise ConnectionRefusedError("endpoint suspended")
        return bi_cat.connect(**kw)

    env = {
        "LB_INT_DSN": "postgresql://int_role:pw1@127.0.0.1:40100/ops",
        "LB_BI_DSN": "postgresql://bi_role:pw2@127.0.0.1:40100/ops",
        "LB_API_TOK": "cp-token-shared-ep",
        "SEEKNAL_CURSOR_KEY": "test-app-cursor-key-32-bytes-valid",
    }
    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "ops": {"enabled": True, "public_readyz": True, "readyz_cache_seconds": 0},
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
                "lb_ops": {
                    "kind": "lakebase",
                    "accounts": {
                        "interactive": {"dsn_env": "LB_INT_DSN"},
                        "bi": {"dsn_env": "LB_BI_DSN"},
                    },
                    "tls": {
                        "sslmode": "verify-full",
                        "sslrootcert_file": "/etc/seeknal/tls/lb-ca.pem",
                    },
                    "lakebase": {
                        "project_id": "prj_0123456789abcdefghij",
                        "branch_id": "br_0123456789abcdefghij",
                        "endpoint_id": "ep_0123456789abcdefghij",
                        "endpoint_type": "read_write",
                        "cold_start": {
                            "wake": "api",
                            "api_url": "https://lakebase-cp.internal.example",
                            "api_token_env": "LB_API_TOK",
                            "connect_timeout_seconds": 2,
                            "max_query_wait_seconds": 5,
                            "max_wake_seconds": 30,
                            "retry_backoff_ms": [10, 20],
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
        connect_factories={
            "duck": lambda: duckdb.connect(),
            "lb_ops": {"interactive": connect_int, "bi": connect_bi},
        },
    )

    lb_routed = handle.adapters["lb_ops"]
    assert isinstance(lb_routed, WorkloadRoutedAdapter)
    ad_int = lb_routed.by_workload["interactive"]
    ad_bi = lb_routed.by_workload["bi"]
    assert isinstance(ad_int, DBAPIAdapter)
    assert isinstance(ad_bi, DBAPIAdapter)

    # Distinct accounts on same endpoint_id have separate DB pools, but ONE shared ColdStartConnector!
    assert ad_int.pool is not ad_bi.pool
    assert ad_int._cold_start_connector is not None
    assert ad_int._cold_start_connector is ad_bi._cold_start_connector

    # /readyz on public and ops apps NEVER triggers POST /start while endpoint is suspended!
    with TestClient(handle.app) as pub_c, TestClient(handle.ops_app) as ops_c:
        assert pub_c.get("/readyz").status_code == 200
        assert ops_c.get("/readyz").status_code == 200
        assert post_start_calls == 0

        # 1st query wakes the endpoint -> cold_start is True
        prep_bi = PreparedQuery(
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
            audit_id="aud-bi-cold",
            cube_security_context=None,
            workload="bi",
        )
        res_cold = lb_routed.execute(prep_bi, timeout_seconds=5.0)
        assert res_cold.cold_start is True
        assert lb_routed.last_cold_start is True
        assert ad_bi.last_cold_start is True
        assert post_start_calls == 1

        # 2nd query on warm pooled connection -> cold_start is False
        res_warm = lb_routed.execute(prep_bi, timeout_seconds=5.0)
        assert res_warm.cold_start is False
        assert lb_routed.last_cold_start is False
        assert ad_bi.last_cold_start is False
        assert post_start_calls == 1

        # Non-Lakebase DuckDB query -> cold_start is None
        duck_ad = handle.adapters["duck"]
        prep_duck = PreparedQuery(
            engine="duck",
            engine_kind="duckdb",
            sql="SELECT 1",
            params=(),
            paramstyle="qmark",
            rule="point_lookup",
            reason="test",
            row_limit=5,
            masked_columns=(),
            redacted_sql="SELECT 1",
            subject="u@test.org",
            workspace="ws",
            audit_id="aud-duck-1",
            cube_security_context=None,
        )
        res_duck = duck_ad.execute(prep_duck, timeout_seconds=5.0)
        assert res_duck.cold_start is None


def test_b_fix_build_server_wires_cursor_key_env_from_env_mapping_and_pages_select(
    tmp_path, ec_key_pair, jwks_path, monkeypatch
):
    priv, _, _ = ec_key_pair
    monkeypatch.delenv("SEEKNAL_CURSOR_KEY", raising=False)
    monkeypatch.delenv("CUSTOM_SELECT_CURSOR_KEY", raising=False)

    pf = tmp_path / "select_placement.yaml"
    pf.write_text(
        """
engines:
  duck: {kind: duckdb, dialect: duckdb}
tables:
  users:
    keys: [id]
    row_count: 10
    access:
      sortable: [id, name]
    serve:
      duck: users
cubes: {}
""",
        encoding="utf-8",
    )
    sf = tmp_path / "select_schemas.json"
    sf.write_text(json.dumps({"users": ["id", "name"]}), encoding="utf-8")

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
    expected_rows = [[i, f"user-{i:02d}"] for i in range(1, 11)]
    con.executemany("INSERT INTO users VALUES (?, ?)", expected_rows)

    cursor_secret = "custom-env-mapping-only-cursor-key-9911"
    env = {"CUSTOM_SELECT_CURSOR_KEY": cursor_secret}
    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "cursor_key_env": "CUSTOM_SELECT_CURSOR_KEY",
            "placement": {
                "static": {
                    "placement_file": str(pf),
                    "schemas_file": str(sf),
                }
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
            },
        },
        env=env,
    )

    handle = build_server(
        cfg,
        env=env,
        connect_factories={"duck": lambda: con.cursor()},
    )

    collected: list[list[Any]] = []
    after_cursor: str | None = None
    page_num = 0
    with TestClient(handle.app) as client:
        while True:
            page_num += 1
            body_obj: dict[str, Any] = {
                "table": "users",
                "columns": ["id", "name"],
                "order_by": [{"id": "asc"}],
                "limit": 3,
            }
            if after_cursor is not None:
                body_obj["after"] = after_cursor
            raw_body = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
            now = time.time()
            tok = sign_grant_token(
                {
                    "sub": "user@test.org",
                    "ws": "analytics",
                    "jti": f"jti-bfix-page-{page_num}",
                    "iss": "atlas-catalog",
                    "aud": "seeknal-serving",
                    "iat": now,
                    "exp": now + 60.0,
                    "typ": "grant",
                    "api": "select.v1",
                    "sql_dialect": "postgres",
                    "sql_sha256": hashlib.sha256(raw_body).hexdigest(),
                    "max_rows": 50,
                    "tables": {
                        "users": {
                            "columns": ["id", "name"],
                            "masks": {},
                            "row_filters": [],
                            "actions": ["select"],
                        }
                    },
                },
                priv,
                kid="app-test-key",
            )
            resp = client.post(
                "/v1/select",
                headers={
                    "Authorization": f"Bearer {tok}",
                    "Content-Type": "application/json",
                },
                content=raw_body,
            )
            assert resp.status_code == 200, resp.text
            pdata = resp.json()
            if page_num == 1:
                assert pdata["truncated"] is True
                assert isinstance(pdata["next"], str) and pdata["next"]
            collected.extend(pdata["rows"])
            after_cursor = pdata.get("next")
            if not after_cursor:
                break

    assert page_num == 4
    assert collected == expected_rows


def test_b_fix_build_server_refuses_startup_when_cursor_key_missing(
    jwks_path, static_catalog_files, monkeypatch
):
    from seeknal.serving.startup_validation import (
        CursorKeyMissingError,
        validate_startup,
    )

    pf, sf = static_catalog_files
    monkeypatch.delenv("SEEKNAL_CURSOR_KEY", raising=False)
    monkeypatch.delenv("MY_CUSTOM_CURSOR_KEY", raising=False)
    planted_other_secret = "PLANTED_UNRELATED_SECRET_8822"

    # 1. Default env var (SEEKNAL_CURSOR_KEY) missing everywhere -> refuses with code == "cursor_key_missing"
    cfg_default = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        }
    )
    with pytest.raises(ServerConfigError) as exc_def:
        build_server(
            cfg_default,
            env={"OTHER_SECRET": planted_other_secret},
        )
    assert isinstance(exc_def.value, CursorKeyMissingError)
    assert exc_def.value.code == "cursor_key_missing"
    assert "cursor_key_missing" in str(exc_def.value)
    assert "SEEKNAL_CURSOR_KEY" in str(exc_def.value)
    assert planted_other_secret not in str(exc_def.value)

    # 2. Custom cursor_key_env missing everywhere -> refuses in both build_server and validate_startup
    cfg_custom = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "cursor_key_env": "MY_CUSTOM_CURSOR_KEY",
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        }
    )
    with pytest.raises(ServerConfigError) as exc_cust:
        build_server(cfg_custom, env={"OTHER_SECRET": planted_other_secret})
    assert isinstance(exc_cust.value, CursorKeyMissingError)
    assert exc_cust.value.code == "cursor_key_missing"
    assert "cursor_key_missing" in str(exc_cust.value)
    assert "MY_CUSTOM_CURSOR_KEY" in str(exc_cust.value)
    assert planted_other_secret not in str(exc_cust.value)

    with pytest.raises(ServerConfigError) as exc_val:
        validate_startup(cfg_custom, env={"OTHER_SECRET": planted_other_secret})
    assert isinstance(exc_val.value, CursorKeyMissingError)
    assert exc_val.value.code == "cursor_key_missing"
    assert "MY_CUSTOM_CURSOR_KEY" in str(exc_val.value)
    assert planted_other_secret not in str(exc_val.value)


def test_f3_cursor_key_weak_refused_at_startup_and_create_app(
    jwks_path, static_catalog_files, ec_key_pair, monkeypatch
):
    from seeknal.serving.http import create_app
    from seeknal.serving.placement import Placement
    from seeknal.serving.service import ServingPipeline
    from seeknal.serving.startup_validation import (
        CursorKeyWeakError,
        validate_startup,
    )

    pf, sf = static_catalog_files
    monkeypatch.delenv("SEEKNAL_CURSOR_KEY", raising=False)
    monkeypatch.delenv("MY_CUSTOM_CURSOR_KEY", raising=False)

    planted_key_31 = "weak-secret-key-prefix-31bytes!"
    assert len(planted_key_31.encode("utf-8")) == 31
    prefix_8 = planted_key_31[:8]

    valid_key_32 = "valid-secret-key-length-32bytes!"
    assert len(valid_key_32.encode("utf-8")) == 32

    cfg_default = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        }
    )

    # 1. 31-byte key passed directly to build_server -> refused with cursor_key_weak
    with pytest.raises(ServerConfigError) as exc_direct:
        build_server(cfg_default, cursor_key=planted_key_31)
    assert isinstance(exc_direct.value, CursorKeyWeakError)
    assert exc_direct.value.code == "cursor_key_weak"
    assert "cursor_key_weak" in str(exc_direct.value)
    assert "SEEKNAL_CURSOR_KEY" in str(exc_direct.value)
    assert "32" in str(exc_direct.value)
    assert planted_key_31 not in str(exc_direct.value)
    assert prefix_8 not in str(exc_direct.value)

    # 2. 31-byte key in default env var (SEEKNAL_CURSOR_KEY) -> refused at startup and in validate_startup
    with pytest.raises(ServerConfigError) as exc_env:
        build_server(cfg_default, env={"SEEKNAL_CURSOR_KEY": planted_key_31})
    assert isinstance(exc_env.value, CursorKeyWeakError)
    assert exc_env.value.code == "cursor_key_weak"
    assert "SEEKNAL_CURSOR_KEY" in str(exc_env.value)
    assert "32" in str(exc_env.value)
    assert planted_key_31 not in str(exc_env.value)
    assert prefix_8 not in str(exc_env.value)

    with pytest.raises(ServerConfigError) as exc_val:
        validate_startup(cfg_default, env={"SEEKNAL_CURSOR_KEY": planted_key_31}, check_cursor_key=True)
    assert isinstance(exc_val.value, CursorKeyWeakError)
    assert exc_val.value.code == "cursor_key_weak"
    assert "SEEKNAL_CURSOR_KEY" in str(exc_val.value)
    assert "32" in str(exc_val.value)
    assert planted_key_31 not in str(exc_val.value)
    assert prefix_8 not in str(exc_val.value)

    # 3. 31-byte key with custom cursor_key_env -> message names custom env var
    cfg_custom = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "cursor_key_env": "MY_CUSTOM_CURSOR_KEY",
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        }
    )
    with pytest.raises(ServerConfigError) as exc_cust:
        build_server(cfg_custom, env={"MY_CUSTOM_CURSOR_KEY": planted_key_31})
    assert isinstance(exc_cust.value, CursorKeyWeakError)
    assert exc_cust.value.code == "cursor_key_weak"
    assert "MY_CUSTOM_CURSOR_KEY" in str(exc_cust.value)
    assert "32" in str(exc_cust.value)
    assert planted_key_31 not in str(exc_cust.value)
    assert prefix_8 not in str(exc_cust.value)

    # 4. create_app called directly with 31-byte key also refuses
    priv, pub, _ = ec_key_pair
    pipeline = ServingPipeline(
        placement=Placement.from_dict({"engines": {"duck": {"kind": "duckdb"}}, "tables": {}, "cubes": {}}),
        schemas={},
        public_keys={"k1": pub},
    )
    dummy_adapters = {"duck": MagicMock()}

    with pytest.raises(ServerConfigError) as exc_app_str:
        create_app(pipeline, dummy_adapters, cursor_key=planted_key_31)
    assert exc_app_str.value.code == "cursor_key_weak"
    assert "SEEKNAL_CURSOR_KEY" in str(exc_app_str.value)
    assert "32" in str(exc_app_str.value)
    assert planted_key_31 not in str(exc_app_str.value)
    assert prefix_8 not in str(exc_app_str.value)

    with pytest.raises(ServerConfigError) as exc_app_bytes:
        create_app(pipeline, dummy_adapters, cursor_key=planted_key_31.encode("utf-8"))
    assert exc_app_bytes.value.code == "cursor_key_weak"
    assert "SEEKNAL_CURSOR_KEY" in str(exc_app_bytes.value)
    assert "32" in str(exc_app_bytes.value)
    assert planted_key_31 not in str(exc_app_bytes.value)
    assert prefix_8 not in str(exc_app_bytes.value)

    # 5. 32-byte key is accepted at startup and in create_app
    handle_ok = build_server(cfg_default, cursor_key=valid_key_32)
    assert isinstance(handle_ok, ServerHandle)

    handle_env_ok = build_server(cfg_default, env={"SEEKNAL_CURSOR_KEY": valid_key_32})
    assert isinstance(handle_env_ok, ServerHandle)

    app_ok = create_app(pipeline, dummy_adapters, cursor_key=valid_key_32)
    assert app_ok is not None


def test_s1_lone_surrogate_cursor_key_refused_without_leak(
    jwks_path, static_catalog_files, ec_key_pair, monkeypatch
):
    from seeknal.serving.http import create_app
    from seeknal.serving.placement import Placement
    from seeknal.serving.service import ServingPipeline
    from seeknal.serving.startup_validation import (
        CursorKeyWeakError,
        validate_startup,
    )

    pf, sf = static_catalog_files
    monkeypatch.delenv("SEEKNAL_CURSOR_KEY", raising=False)

    surrogate_key = "surr_prefix_1234\ud800surr_suffix_5678_32b"
    slices_8 = [surrogate_key[i:i+8] for i in range(len(surrogate_key) - 7)]

    def _assert_no_surrogate_leak(exc_val: Exception, expected_env_var: str) -> None:
        assert isinstance(exc_val, ServerConfigError)
        assert getattr(exc_val, "code", None) == "cursor_key_weak"
        msg = str(exc_val)
        rep = repr(exc_val)
        args_str = str(exc_val.args)
        assert expected_env_var in msg
        assert "must be valid UTF-8 and at least 32 bytes" in msg
        for sl in slices_8:
            assert sl not in msg
            assert sl not in rep
            assert sl not in args_str
        assert surrogate_key not in msg
        assert surrogate_key not in rep
        assert surrogate_key not in args_str
        assert exc_val.__cause__ is None

    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {"duck": {"kind": "duckdb", "path": ":memory:"}},
        }
    )

    # 1. Explicit argument in build_server
    with pytest.raises(ServerConfigError) as exc_arg:
        build_server(cfg, cursor_key=surrogate_key)
    _assert_no_surrogate_leak(exc_arg.value, "SEEKNAL_CURSOR_KEY")

    # 2. Env mapping in build_server and validate_startup
    with pytest.raises(ServerConfigError) as exc_env:
        build_server(cfg, env={"SEEKNAL_CURSOR_KEY": surrogate_key})
    _assert_no_surrogate_leak(exc_env.value, "SEEKNAL_CURSOR_KEY")

    with pytest.raises(ServerConfigError) as exc_val:
        validate_startup(cfg, env={"SEEKNAL_CURSOR_KEY": surrogate_key}, check_cursor_key=True)
    _assert_no_surrogate_leak(exc_val.value, "SEEKNAL_CURSOR_KEY")

    # 3. os.environ (via non-UTF-8 bytes in os.environb producing surrogateescape)
    surrogate_bytes = b"surr_prefix_1234\x80surr_suffix_5678_32b"
    monkeypatch.setitem(os.environb, b"SEEKNAL_CURSOR_KEY", surrogate_bytes)
    surrogate_os_key = os.environ["SEEKNAL_CURSOR_KEY"]
    slices_8_os = [surrogate_os_key[i:i+8] for i in range(len(surrogate_os_key) - 7)]
    with pytest.raises(ServerConfigError) as exc_os:
        build_server(cfg)
    assert isinstance(exc_os.value, ServerConfigError)
    assert getattr(exc_os.value, "code", None) == "cursor_key_weak"
    for sl in slices_8_os:
        assert sl not in str(exc_os.value)
        assert sl not in repr(exc_os.value)
        assert sl not in str(exc_os.value.args)
    assert surrogate_os_key not in str(exc_os.value)
    assert surrogate_os_key not in repr(exc_os.value)
    assert surrogate_os_key not in str(exc_os.value.args)
    assert exc_os.value.__cause__ is None

    # 4. Explicit argument in create_app
    priv, pub, _ = ec_key_pair
    pipeline = ServingPipeline(
        placement=Placement.from_dict({"engines": {"duck": {"kind": "duckdb"}}, "tables": {}, "cubes": {}}),
        schemas={},
        public_keys={"k1": pub},
    )
    with pytest.raises(ServerConfigError) as exc_app:
        create_app(pipeline, {"duck": MagicMock()}, cursor_key=surrogate_key)
    _assert_no_surrogate_leak(exc_app.value, "SEEKNAL_CURSOR_KEY")


def test_b_l3_build_server_isolates_bad_account_password_on_shared_lakebase_endpoint(
    jwks_path, static_catalog_files, monkeypatch
):
    import concurrent.futures
    import threading
    import httpx
    from tests.serving.fake_postgres_catalog import FakePostgresCatalog

    pf, sf = static_catalog_files
    endpoint_awake = False
    post_entered = threading.Event()
    wake_gate = threading.Event()

    class BadPasswordError(Exception):
        pgcode = "28P01"

    def cp_transport(request: httpx.Request) -> httpx.Response:
        nonlocal endpoint_awake
        if request.method == "POST" and request.url.path.endswith("/start"):
            post_entered.set()
            wake_gate.wait(timeout=2.0)
            return httpx.Response(202, json={"state": "starting"})
        endpoint_awake = True
        return httpx.Response(200, json={"state": "running"})

    real_httpx_client = httpx.Client
    monkeypatch.setattr(
        app_mod.httpx,
        "Client",
        lambda *_a, **_kw: real_httpx_client(
            transport=httpx.MockTransport(cp_transport)
        ),
    )

    int_cat = FakePostgresCatalog()

    def connect_bi(**kw: Any) -> Any:
        if not endpoint_awake:
            raise ConnectionRefusedError("endpoint suspended")
        raise BadPasswordError('FATAL: password authentication failed for user "bi"')

    def connect_int(**kw: Any) -> Any:
        if not endpoint_awake:
            raise ConnectionRefusedError("endpoint suspended")
        return int_cat.connect(**kw)

    env = {
        "LB_BI_DSN": "postgresql://bi:badpw@127.0.0.1:40100/ops",
        "LB_INT_DSN": "postgresql://interactive:goodpw@127.0.0.1:40100/ops",
        "LB_API_TOK": "cp-token-bl3",
        "SEEKNAL_CURSOR_KEY": "test-app-cursor-key-32-bytes-valid",
    }
    cfg = ServerConfig.from_dict(
        {
            "grant_keys": {"jwks_file": jwks_path},
            "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
                "lb_ops": {
                    "kind": "lakebase",
                    "accounts": {
                        "bi": {"dsn_env": "LB_BI_DSN"},
                        "interactive": {"dsn_env": "LB_INT_DSN"},
                    },
                    "tls": {
                        "sslmode": "verify-full",
                        "sslrootcert_file": "/etc/seeknal/tls/lb-ca.pem",
                    },
                    "lakebase": {
                        "project_id": "prj_0123456789abcdefghij",
                        "branch_id": "br_0123456789abcdefghij",
                        "endpoint_id": "ep_0123456789abcdefghij",
                        "endpoint_type": "read_write",
                        "cold_start": {
                            "wake": "api",
                            "api_url": "https://lakebase-cp.internal.example",
                            "api_token_env": "LB_API_TOK",
                            "connect_timeout_seconds": 2,
                            "max_query_wait_seconds": 5,
                            "max_wake_seconds": 30,
                            "retry_backoff_ms": [10, 20],
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
        connect_factories={
            "duck": lambda: duckdb.connect(),
            "lb_ops": {"bi": connect_bi, "interactive": connect_int},
        },
    )
    lb_routed = handle.adapters["lb_ops"]

    prep_bi = PreparedQuery(
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
        audit_id="aud-bl3-bi",
        cube_security_context=None,
        workload="bi",
    )
    prep_int = PreparedQuery(
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
        audit_id="aud-bl3-int",
        cube_security_context=None,
        workload="interactive",
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as ex:
        f_bi = ex.submit(lambda: lb_routed.execute(prep_bi, timeout_seconds=5.0))
        assert post_entered.wait(timeout=2.0)
        f_int = ex.submit(lambda: lb_routed.execute(prep_int, timeout_seconds=5.0))
        time.sleep(0.02)
        wake_gate.set()

        with pytest.raises(EngineError):
            f_bi.result(timeout=5.0)
        res_int = f_int.result(timeout=5.0)
        assert res_int.rows == [(1,)]
        assert res_int.cold_start is True




