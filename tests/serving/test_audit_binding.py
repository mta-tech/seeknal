"""Tests for audit echo: binding claims (publication_id, revision, digest) in Grant, PreparedQuery, and HTTP request logs."""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import (
    DuckDBAdapter,
    EnforcementError,
    Grant,
    Placement,
    ServingPipeline,
    create_app,
    sign_grant_token,
)


def _base_claims(
    sql: str,
    *,
    publication_id: Any = None,
    revision: Any = None,
    digest: Any = None,
    extra_claims: dict[str, Any] | None = None,
) -> dict[str, Any]:
    claims = {
        "sub": "user:analyst",
        "ws": "ws-audit",
        "jti": "aud-binding-001",
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 100,
        "exp": time.time() + 3600.0,
        "tables": {
            "users": {
                "columns": ["id", "name"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    if publication_id is not None:
        claims["publication_id"] = publication_id
    if revision is not None:
        claims["revision"] = revision
    if digest is not None:
        claims["digest"] = digest
    if extra_claims:
        claims.update(extra_claims)
    return claims


def test_binding_claims_kept_in_grant():
    sql = "SELECT id, name FROM users"
    # Test with int revision
    claims1 = _base_claims(
        sql,
        publication_id="pub-claims-v1",
        revision=3,
        digest="sha256:abcdef1234567890",
    )
    grant1 = Grant.from_claims(claims1)
    assert grant1.binding == {
        "publication_id": "pub-claims-v1",
        "revision": 3,
        "digest": "sha256:abcdef1234567890",
    }

    # Test with string revision
    claims2 = _base_claims(
        sql,
        publication_id="pub-claims-v2",
        revision="rev-2026-11-01",
        digest="sha256:9876543210fedcba",
    )
    grant2 = Grant.from_claims(claims2)
    assert grant2.binding["revision"] == "rev-2026-11-01"


def test_absent_binding_claims_fine():
    sql = "SELECT id, name FROM users"
    claims = _base_claims(sql)
    grant = Grant.from_claims(claims)
    assert grant.binding == {}

    # Partial binding claims
    claims_partial = _base_claims(sql, publication_id="pub-only")
    grant_partial = Grant.from_claims(claims_partial)
    assert grant_partial.binding == {"publication_id": "pub-only"}


def test_unknown_claims_ignored():
    sql = "SELECT id, name FROM users"
    claims = _base_claims(
        sql,
        publication_id="pub-1",
        extra_claims={"tenant_name": "acme", "flags": [1, 2, 3], "nested": {"a": "b"}},
    )
    grant = Grant.from_claims(claims)
    assert grant.binding == {"publication_id": "pub-1"}
    assert "tenant_name" not in grant.binding
    assert "flags" not in grant.binding
    assert "nested" not in grant.binding


@pytest.mark.parametrize(
    "bad_claims,expected_match",
    [
        ({"digest": {"alg": "sha256", "val": "abc"}}, "digest claim must be a string"),
        ({"digest": 12345}, "digest claim must be a string"),
        ({"publication_id": {"id": "pub-1"}}, "publication_id claim must be a string"),
        ({"publication_id": 999}, "publication_id claim must be a string"),
        ({"revision": {"rev": 1}}, "revision claim must be an int or string"),
        ({"revision": True}, "revision claim must be an int or string"),
        ({"revision": False}, "revision claim must be an int or string"),
        ({"revision": [1, 2]}, "revision claim must be an int or string"),
        ({"revision": 3.14}, "revision claim must be an int or string"),
    ],
)
def test_wrong_type_binding_claims_refused(bad_claims, expected_match):
    sql = "SELECT id, name FROM users"
    claims = _base_claims(sql, extra_claims=bad_claims)
    with pytest.raises(EnforcementError, match=expected_match):
        Grant.from_claims(claims)


def test_prepared_query_gets_binding():
    sql = "SELECT id, name FROM users"
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    claims = _base_claims(
        sql,
        publication_id="pub-xyz",
        revision=10,
        digest="sha256:digest123",
    )
    claims["iss"] = "atlas-catalog"
    claims["aud"] = "seeknal-serving"
    claims["iat"] = time.time()
    claims["exp"] = time.time() + 60.0

    token = sign_grant_token(claims, priv, kid="k1")

    placement = Placement.from_dict({
        "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 10,
                "serve": {"duck": "users"},
            }
        },
        "cubes": {},
    })
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id", "name"]},
        public_keys={"k1": pub},
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.binding == {
        "publication_id": "pub-xyz",
        "revision": 10,
        "digest": "sha256:digest123",
    }


def test_audit_echo_in_request_log(caplog):
    sql = "SELECT id, name FROM users"
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    placement = Placement.from_dict({
        "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 2,
                "serve": {"duck": "users"},
            }
        },
        "cubes": {},
    })
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id", "name"]},
        public_keys={"k1": pub},
    )
    adapter = DuckDBAdapter(lambda: con)
    app = create_app(pipeline, {"duck": adapter})
    client = TestClient(app)

    # 1. Request with all binding claims present
    claims_full = _base_claims(
        sql,
        publication_id="pub-echo-42",
        revision=7,
        digest="sha256:deadbeefcafe",
    )
    claims_full["iss"] = "atlas-catalog"
    claims_full["aud"] = "seeknal-serving"
    claims_full["iat"] = time.time()
    claims_full["exp"] = time.time() + 60.0

    token_full = sign_grant_token(claims_full, priv, kid="k1")

    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp_q = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token_full}"},
            json={"sql": sql},
        )
        assert resp_q.status_code == 200
        # No extra fields in response
        body = resp_q.json()
        assert "binding" not in body
        assert "publication_id" not in body

        # Check request log line in caplog
        assert "publication_id=pub-echo-42" in caplog.text
        assert "revision=7" in caplog.text
        assert "digest=sha256:deadbeefcafe" in caplog.text

    # Test /v1/explain endpoint as well
    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp_e = client.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {token_full}"},
            json={"sql": sql},
        )
        assert resp_e.status_code == 200
        assert "publication_id=pub-echo-42" in caplog.text
        assert "revision=7" in caplog.text
        assert "digest=sha256:deadbeefcafe" in caplog.text

    # 2. Request without binding claims: log should NOT contain them
    claims_empty = _base_claims(sql)
    claims_empty["jti"] = "aud-empty-002"
    claims_empty["iss"] = "atlas-catalog"
    claims_empty["aud"] = "seeknal-serving"
    claims_empty["iat"] = time.time()
    claims_empty["exp"] = time.time() + 60.0
    token_empty = sign_grant_token(claims_empty, priv, kid="k1")

    with caplog.at_level(logging.INFO):
        caplog.clear()
        resp_empty = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token_empty}"},
            json={"sql": sql},
        )
        assert resp_empty.status_code == 200
        assert "publication_id=" not in caplog.text
        assert "revision=" not in caplog.text
        assert "digest=" not in caplog.text
