"""Tests for grant lifetime cap and server configuration."""

from __future__ import annotations

import hashlib
import time

import duckdb
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import (
    GrantTokenError,
    Placement,
    ServerConfig,
    ServerConfigError,
    ServingPipeline,
    build_server,
    sign_grant_token,
)
from seeknal.serving.adapters import DuckDBAdapter


@pytest.fixture
def ec_key_pair():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


def _make_grant(priv, sql: str, *, lifetime_seconds: float, jti: str = "jti-lifetime-001") -> str:
    now = time.time()
    claims = {
        "typ": "grant",
        "sub": "user@atlas.net",
        "ws": "ws-test",
        "jti": jti,
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + lifetime_seconds,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 100,
        "tables": {
            "users": {
                "columns": ["id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    return sign_grant_token(claims, priv, kid="key-1")


def test_pipeline_library_default_stays_300(ec_key_pair):
    priv, pub = ec_key_pair
    placement = Placement.from_dict({
        "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"duck": "users"}}},
        "cubes": {},
    })
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id"]},
        public_keys={"key-1": pub},
    )
    assert pipeline.max_grant_lifetime_seconds == 300.0

    sql = "SELECT id FROM users"
    # 250s grant is accepted
    tok_250 = _make_grant(priv, sql, lifetime_seconds=250.0, jti="jti-250")
    prep = pipeline.prepare(sql, tok_250)
    assert prep.expires_at is not None

    # 301s grant is refused
    tok_301 = _make_grant(priv, sql, lifetime_seconds=301.0, jti="jti-301")
    with pytest.raises(GrantTokenError) as exc_info:
        pipeline.prepare(sql, tok_301)
    assert "exceeds maximum 300" in str(exc_info.value)


@pytest.mark.parametrize("bad_val", [0, -1, -50.0, float("nan"), float("inf"), 301, 300.1, True, False, "invalid"])
def test_config_validation_rejects_invalid_lifetime(bad_val):
    cfg_dict = {
        "grant_max_lifetime_seconds": bad_val,
        "placement": {
            "static": {
                "placement_file": "/tmp/unused.yaml",
                "schemas_file": "/tmp/unused.json",
            }
        },
        "grant_keys": {
            "jwks_file": "/tmp/unused.jwks",
        },
        "connections": {
            "duck": {
                "kind": "duckdb",
                "path": "/tmp/unused.duckdb",
            }
        },
    }
    with pytest.raises(ServerConfigError) as exc_info:
        ServerConfig.from_dict(cfg_dict, load_keys=False)
    assert "grant_max_lifetime_seconds" in str(exc_info.value)


@pytest.mark.parametrize("bad_val", [0, -1, -100, 10.5, True, False, "invalid"])
def test_config_validation_rejects_invalid_replay_cache_max_entries(bad_val):
    cfg_dict = {
        "replay_cache_max_entries": bad_val,
        "placement": {
            "static": {
                "placement_file": "/tmp/unused.yaml",
                "schemas_file": "/tmp/unused.json",
            }
        },
        "grant_keys": {
            "jwks_file": "/tmp/unused.jwks",
        },
        "connections": {
            "duck": {
                "kind": "duckdb",
                "path": "/tmp/unused.duckdb",
            }
        },
    }
    with pytest.raises(ServerConfigError) as exc_info:
        ServerConfig.from_dict(cfg_dict, load_keys=False)
    assert "replay_cache_max_entries" in str(exc_info.value)


def test_build_server_61s_refused_default_and_accepted_at_120(ec_key_pair, tmp_path):
    priv, pub = ec_key_pair

    # Create dummy static placement & schemas files
    pl_file = tmp_path / "placement.yaml"
    pl_file.write_text(
        "engines:\n  duck:\n    kind: duckdb\n    dialect: duckdb\n"
        "tables:\n  users:\n    keys: [id]\n    row_count: 1\n    serve:\n      duck: users\n"
        "cubes: {}\n",
        encoding="utf-8",
    )
    sc_file = tmp_path / "schemas.json"
    sc_file.write_text('{"users": ["id"]}', encoding="utf-8")

    pub_pem = pub.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("ascii")
    pub_file = tmp_path / "pub.pem"
    pub_file.write_text(pub_pem, encoding="utf-8")

    db_file = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE users (id INT)")
    con.execute("INSERT INTO users VALUES (42)")
    con.close()

    base_config = {
        "grant_keys": {
            "pem_files": {
                "key-1": str(pub_file),
            }
        },
        "placement": {
            "static": {
                "placement_file": str(pl_file),
                "schemas_file": str(sc_file),
            }
        },
        "connections": {
            "duck": {
                "kind": "duckdb",
                "path": str(db_file),
            }
        },
    }

    sql = "SELECT id FROM users"
    token_61s = _make_grant(priv, sql, lifetime_seconds=61.0, jti="jti-61s")

    # 1. Default config: grant_max_lifetime_seconds defaults to 60.0 -> 61s grant refused with 401
    server_default = build_server(
        base_config,
        connect_factories={"duck": lambda: duckdb.connect(str(db_file), read_only=True)},
    )
    # Inject loaded key for mock
    server_default.pipeline.public_keys = {"key-1": pub}
    client_default = TestClient(server_default.app)

    resp_def = client_default.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token_61s}"},
        json={"sql": sql},
    )
    assert resp_def.status_code == 401
    assert resp_def.json()["error"] == "grant_token_error"
    assert "exceeds maximum 60" in resp_def.json()["message"]

    # 2. Config with grant_max_lifetime_seconds: 120 -> 61s grant accepted (200 OK)
    cfg_120 = dict(base_config)
    cfg_120["grant_max_lifetime_seconds"] = 120
    server_120 = build_server(
        cfg_120,
        connect_factories={"duck": lambda: duckdb.connect(str(db_file), read_only=True)},
    )
    server_120.pipeline.public_keys = {"key-1": pub}
    client_120 = TestClient(server_120.app)

    resp_120 = client_120.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token_61s}"},
        json={"sql": sql},
    )
    assert resp_120.status_code == 200
    assert resp_120.json()["rows"] == [[42]]
