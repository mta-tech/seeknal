"""Tests for Seeknal Serving application assembly and endpoints."""

from __future__ import annotations

import base64
import datetime
import hashlib
import json
import time
from unittest.mock import MagicMock

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving.adapters import CubeAdapter, DBAPIAdapter, WorkloadRoutedAdapter
from seeknal.serving.discovery.build import DiscoveryResult
from seeknal.serving.grant_token import sign_grant_token
from seeknal.serving.server.app import ServerHandle, build_server
from seeknal.serving.server.config import ServerConfig


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
  sr: {kind: starrocks, dialect: starrocks}
  cube: {kind: cube, dialect: postgres}
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
    assert resp_before.json() == {"ready": False, "reason": "catalog_not_loaded"}

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
    assert resp_fail.json() == {"ready": False, "reason": "catalog_not_loaded"}
    assert "SECRET_PASSWORD_999" not in resp_fail.text


def test_starrocks_workload_routing_fake_factories(
    ec_key_pair, jwks_path, static_catalog_files
):
    pf, sf = static_catalog_files
    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_path},
        "placement": {
            "static": {
                "placement_file": pf,
                "schemas_file": sf,
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


def test_cube_mode_adapters(ec_key_pair, jwks_path, static_catalog_files):
    pf, sf = static_catalog_files
    fake_cube_conn = MagicMock()
    fake_factory = MagicMock(return_value=fake_cube_conn)

    # 1. Cube in jwt mode builds CubeAdapter
    cfg_jwt = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_path},
        "cube_mode": "jwt",
        "placement": {
            "static": {
                "placement_file": pf,
                "schemas_file": sf,
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
                "placement_file": pf,
                "schemas_file": sf,
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
