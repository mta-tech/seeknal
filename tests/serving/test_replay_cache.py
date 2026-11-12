"""Tests for GrantReplayCache and /v1/query grant replay enforcement."""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import Placement, ServingPipeline, sign_grant_token
from seeknal.serving.adapters import DuckDBAdapter, EngineError
from seeknal.serving.http import (
    GrantReplayCache,
    GrantReplayCacheFull,
    GrantReplayedError,
    create_app,
)
from seeknal.serving.service import PreparedQuery


def test_replay_cache_unit_record_and_replay():
    cache = GrantReplayCache(max_entries=10, leeway_seconds=5.0)
    now = 1000.0

    # 1. First insert succeeds
    cache.check_and_record("jti-1", expires_at=now + 50.0, now=now)

    # 2. Replay before expiry raises GrantReplayedError
    with pytest.raises(GrantReplayedError) as exc_info:
        cache.check_and_record("jti-1", expires_at=now + 50.0, now=now + 10.0)
    assert "jti-1" in str(exc_info.value)

    # 3. Another distinct jti succeeds
    cache.check_and_record("jti-2", expires_at=now + 50.0, now=now + 10.0)


def test_replay_cache_unit_pruning_and_bounded():
    cache = GrantReplayCache(max_entries=3, leeway_seconds=5.0)
    now = 1000.0

    # Insert entry 1 that expires at t=1010 (+ 5 leeway = 1015)
    cache.check_and_record("jti-short", expires_at=1010.0, now=now)
    # Insert entry 2 and 3 that expire much later
    cache.check_and_record("jti-long-1", expires_at=2000.0, now=now)
    cache.check_and_record("jti-long-2", expires_at=2000.0, now=now)

    assert len(cache._cache) == 3

    # At now = 1016, entry 1 is expired.
    # Inserting a new entry prunes entry 1 on insert and succeeds without hitting capacity limit.
    cache.check_and_record("jti-new", expires_at=2000.0, now=1016.0)

    assert "jti-short" not in cache._cache
    assert len(cache._cache) == 3
    assert set(cache._cache.keys()) == {"jti-long-1", "jti-long-2", "jti-new"}


def test_replay_cache_unit_full_raises():
    cache = GrantReplayCache(max_entries=2, leeway_seconds=5.0)
    now = 1000.0

    cache.check_and_record("jti-1", expires_at=2000.0, now=now)
    cache.check_and_record("jti-2", expires_at=2000.0, now=now)

    # Cache is full of active entries; fails closed
    with pytest.raises(GrantReplayCacheFull) as exc_info:
        cache.check_and_record("jti-3", expires_at=2000.0, now=now)
    assert "full" in str(exc_info.value).lower()
    assert "jti-3" not in cache._cache


@pytest.fixture
def ec_key_pair():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


@pytest.fixture
def test_setup(ec_key_pair):
    priv, pub = ec_key_pair
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    placement = Placement.from_dict({
        "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {"users": {"keys": ["id"], "row_count": 2, "serve": {"duck": "users"}}},
        "cubes": {},
    })
    schemas = {"users": ["id", "name"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-1": pub},
    )
    adapter = DuckDBAdapter(lambda: con.cursor())
    return priv, pipeline, adapter


def _make_grant(priv, sql: str, jti: str = "jti-test-001", exp_offset: float = 60.0) -> str:
    now = time.time()
    claims = {
        "sub": "user@atlas.net",
        "ws": "ws-test",
        "jti": jti,
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + exp_offset,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 100,
        "tables": {
            "users": {
                "columns": ["id", "name"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    return sign_grant_token(claims, priv, kid="key-1")


def test_http_same_grant_twice_refused_with_401_grant_replayed(test_setup, caplog):
    priv, pipeline, adapter = test_setup
    app = create_app(pipeline, {"duck": adapter})
    client = TestClient(app)

    sql = "SELECT id, name FROM users"
    token = _make_grant(priv, sql, jti="jti-replay-001")

    # 1. First call succeeds
    resp1 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp1.status_code == 200
    assert resp1.json()["rows"] == [[1, "Alice"], [2, "Bob"]]

    # 2. Second call with exact same grant token returns 401 grant_replayed
    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp2 = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql},
        )
    assert resp2.status_code == 401
    data2 = resp2.json()
    assert data2["error"] == "grant_replayed"
    assert "already been used" in data2["message"]

    # Verify audit log includes audit_id and subject
    assert "audit_id=jti-replay-001" in caplog.text
    assert "subject=user@atlas.net" in caplog.text
    assert "status=401" in caplog.text


def test_http_explain_does_not_consume_grant(test_setup):
    priv, pipeline, adapter = test_setup
    app = create_app(pipeline, {"duck": adapter})
    client = TestClient(app)

    sql = "SELECT id, name FROM users"
    token = _make_grant(priv, sql, jti="jti-explain-001")

    # 1. Multiple explain calls succeed and do not consume grant
    for _ in range(3):
        resp_exp = client.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql},
        )
        assert resp_exp.status_code == 200

    # 2. Query with the same grant still succeeds
    resp_q = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp_q.status_code == 200
    assert resp_q.json()["rows"] == [[1, "Alice"], [2, "Bob"]]

    # 3. Now the grant is consumed; subsequent query fails
    resp_q2 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp_q2.status_code == 401
    assert resp_q2.json()["error"] == "grant_replayed"


def test_http_full_cache_returns_503_replay_cache_full(test_setup, caplog):
    priv, pipeline, adapter = test_setup
    tiny_cache = GrantReplayCache(max_entries=1)
    app = create_app(pipeline, {"duck": adapter}, replay_cache=tiny_cache)
    client = TestClient(app)

    sql = "SELECT id, name FROM users"
    token1 = _make_grant(priv, sql, jti="jti-first")
    token2 = _make_grant(priv, sql, jti="jti-second")

    # 1. First query fills cache (1 entry max)
    resp1 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token1}"},
        json={"sql": sql},
    )
    assert resp1.status_code == 200

    # 2. Second query with distinct jti hits full cache -> 503 replay_cache_full
    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp2 = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token2}"},
            json={"sql": sql},
        )
    assert resp2.status_code == 503
    data2 = resp2.json()
    assert data2["error"] == "replay_cache_full"
    assert "audit_id=jti-second" in caplog.text
    assert "subject=user@atlas.net" in caplog.text
    assert "status=503" in caplog.text


def test_http_grant_consumed_even_on_query_failure(test_setup):
    priv, pipeline, _ = test_setup

    class FailingAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float):
            raise EngineError("Adapter failure")

    app = create_app(pipeline, {"duck": FailingAdapter()})
    client = TestClient(app)

    sql = "SELECT id, name FROM users"
    token = _make_grant(priv, sql, jti="jti-failing-query")

    # 1. Query execution fails with 502 engine_error
    resp1 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp1.status_code == 502

    # 2. Re-submitting the same grant fails with 401 grant_replayed (consumed!)
    resp2 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp2.status_code == 401
    assert resp2.json()["error"] == "grant_replayed"
