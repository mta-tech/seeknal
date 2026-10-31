"""Tests for Seeknal Serving HTTP API."""

from __future__ import annotations

import datetime
from decimal import Decimal
import hashlib
import time

from cryptography.hazmat.primitives.asymmetric import ec
import duckdb
import pytest
from starlette.testclient import TestClient

from seeknal.serving import Placement, ServingPipeline, sign_grant_token
from seeknal.serving.adapters import (
    DuckDBAdapter,
    EngineAdapter,
    EngineError,
    EngineTimeout,
    QueryResult,
)
from seeknal.serving.http import create_app
from seeknal.serving.service import PreparedQuery


@pytest.fixture
def ec_key_pair():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


@pytest.fixture
def serving_setup(ec_key_pair):
    priv, pub = ec_key_pair
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE users (id INT, name VARCHAR, salary INT, role VARCHAR)"
    )
    con.execute(
        "INSERT INTO users VALUES "
        "(1, 'Alice', 100000, 'admin'), "
        "(2, 'Bob', 80000, 'analyst'), "
        "(3, 'Charlie', 50000, 'analyst')"
    )

    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
            "missing_eng": {"kind": "duckdb", "dialect": "duckdb"},
        },
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 3,
                "serve": {
                    "duck": "users",
                    "missing_eng": "users",
                },
            }
        },
        "cubes": {},
    })
    schemas = {"users": ["id", "name", "salary", "role"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-1": pub},
    )
    adapter = DuckDBAdapter(lambda: con)
    # "missing_eng" is in placement but not in adapters, for testing 503
    app = create_app(
        pipeline,
        {"duck": adapter},
        timeout_seconds=5.0,
        max_body_bytes=1000,
    )
    client = TestClient(app)
    return client, priv, con, pipeline


def _make_token(
    priv_key,
    sql: str,
    *,
    max_rows: int = 1000,
    tables: dict | None = None,
    exp_offset: float = 60.0,
    iat_offset: float = 0.0,
    sub: str = "analyst@example.com",
) -> str:
    now = time.time()
    if tables is None:
        tables = {
            "users": {
                "columns": ["id", "salary"],
                "masks": {"name": "redact"},
                "row_filters": [{"column": "role", "op": "eq", "values": ["analyst"]}],
                "actions": ["select"],
            }
        }
    claims = {
        "sub": sub,
        "ws": "ws-test",
        "jti": "jti-http-001",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now + iat_offset,
        "exp": now + exp_offset,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": tables,
    }
    return sign_grant_token(claims, priv_key, kid="key-1")


def test_healthz(serving_setup):
    """Test GET /healthz returns 200 {"status": "ok"} with no auth."""
    client, _, _, _ = serving_setup
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_query_success_with_masks_and_filters(serving_setup):
    """Test successful query execution with masking, row filters, and parameterization."""
    client, priv, _, _ = serving_setup
    sql = "SELECT id, name, salary FROM users WHERE salary >= $1"
    token = _make_token(priv, sql)

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "params": [60000]},
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["columns"] == ["id", "name", "salary"]
    # Alice is filtered out by role == 'analyst'
    # Charlie (salary 50000) is filtered out by salary >= 60000
    # Bob matches: salary 80000 >= 60000, name masked to '****'
    assert data["rows"] == [[2, "****", 80000]]
    assert data["truncated"] is False
    assert data["engine"] == "duck"
    assert data["rule"] is not None
    assert data["reason"] is not None
    assert data["masked_columns"] == ["users.name"]
    assert data["audit_id"] == "jti-http-001"
    assert "salary >= $?" in data["redacted_sql"]
    assert data["elapsed_ms"] > 0


def test_query_json_safe_serialization(ec_key_pair):
    """Test serialization of date/datetime/time, Decimal, bytes, and NaN/inf floats."""
    priv, pub = ec_key_pair

    class FakeSpecialAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            return QueryResult(
                columns=("dt", "d", "t", "dec", "b", "nan_val", "inf_val"),
                rows=[
                    (
                        datetime.datetime(2026, 10, 31, 12, 30, 0),
                        datetime.date(2026, 10, 31),
                        datetime.time(12, 30, 0),
                        Decimal("123.45"),
                        b"hello bytes",
                        float("nan"),
                        float("inf"),
                    )
                ],
                truncated=False,
                engine="special",
                elapsed_ms=12.5,
            )

    placement = Placement.from_dict({
        "engines": {"special": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 1,
                "serve": {"special": "users"},
            }
        },
        "cubes": {},
    })
    schemas = {"users": ["id"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-1": pub},
    )
    app = create_app(pipeline, {"special": FakeSpecialAdapter()})
    client = TestClient(app)

    sql = "SELECT id FROM users"
    token = _make_token(priv, sql, tables={"users": {"columns": ["id"], "masks": {}, "actions": ["select"]}})
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["rows"] == [
        [
            "2026-10-31T12:30:00",
            "2026-10-31",
            "12:30:00",
            "123.45",
            "aGVsbG8gYnl0ZXM=",  # base64 of b"hello bytes"
            None,  # NaN -> null
            None,  # inf -> null
        ]
    ]


def test_explain(serving_setup):
    """Test POST /v1/explain returns AST metadata without executing."""
    client, priv, con, _ = serving_setup
    sql = "SELECT id, name, salary FROM users WHERE salary >= $1"
    token = _make_token(priv, sql)

    resp = client.post(
        "/v1/explain",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "params": [50000]},
    )
    assert resp.status_code == 200
    data = resp.json()

    assert data["engine"] == "duck"
    assert "rule" in data
    assert "reason" in data
    assert "?" in data["sql"]
    assert data["paramstyle"] == "qmark"
    assert data["masked_columns"] == ["users.name"]
    assert data["row_limit"] == 1001
    assert "salary >= $?" in data["redacted_sql"]


# --- Error Status Tests ---


def test_error_401_grant_token_error(serving_setup):
    """Test 401 for missing/malformed auth header, expired token, or signature mismatch."""
    client, priv, _, _ = serving_setup
    sql = "SELECT id FROM users"

    # Missing Authorization header
    r1 = client.post("/v1/query", json={"sql": sql})
    assert r1.status_code == 401
    assert "error" in r1.json()

    # Malformed Authorization header (not Bearer)
    r2 = client.post("/v1/query", headers={"Authorization": "Basic 12345"}, json={"sql": sql})
    assert r2.status_code == 401

    # Malformed token string
    r3 = client.post("/v1/query", headers={"Authorization": "Bearer not-a-token"}, json={"sql": sql})
    assert r3.status_code == 401

    # Expired token
    expired_token = _make_token(priv, sql, exp_offset=-20.0, iat_offset=-100.0)
    r4 = client.post("/v1/query", headers={"Authorization": f"Bearer {expired_token}"}, json={"sql": sql})
    assert r4.status_code == 401


def test_error_403_enforcement_error(serving_setup):
    """Test 403 for policy enforcement failures (e.g., ungranted table)."""
    client, priv, _, _ = serving_setup
    sql = "SELECT id FROM users"
    # Token only grants table 'other_table', not 'users'
    token = _make_token(
        priv,
        sql,
        tables={"other_table": {"columns": ["id"], "masks": {}, "actions": ["select"]}},
    )

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 403
    assert resp.json()["error"] == "enforcement_error"


def test_error_422_routing_error(serving_setup):
    """Test 422 for routing errors (e.g., hint to an unknown engine)."""
    client, priv, _, _ = serving_setup
    sql = "SELECT id FROM users"
    token = _make_token(priv, sql)

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "hint": "nonexistent_engine"},
    )
    assert resp.status_code == 422
    assert resp.json()["error"] == "routing_error"


def test_error_400_bad_request(serving_setup):
    """Test 400 for malformed JSON, invalid types, or missing required fields."""
    client, priv, _, _ = serving_setup
    sql = "SELECT id FROM users"
    token = _make_token(priv, sql)
    headers = {"Authorization": f"Bearer {token}"}

    # Malformed JSON
    r1 = client.post("/v1/query", headers=headers, content="not valid json")
    assert r1.status_code == 400

    # JSON is not an object
    r2 = client.post("/v1/query", headers=headers, json=["not", "an", "object"])
    assert r2.status_code == 400

    # 'sql' missing
    r3 = client.post("/v1/query", headers=headers, json={"params": [1]})
    assert r3.status_code == 400

    # 'sql' not a string
    r4 = client.post("/v1/query", headers=headers, json={"sql": 12345})
    assert r4.status_code == 400

    # 'params' not a list
    r5 = client.post("/v1/query", headers=headers, json={"sql": sql, "params": "not a list"})
    assert r5.status_code == 400

    # 'hint' not a string
    r6 = client.post("/v1/query", headers=headers, json={"sql": sql, "hint": 123})
    assert r6.status_code == 400

    # 'max_staleness_seconds' not a number
    r7 = client.post("/v1/query", headers=headers, json={"sql": sql, "max_staleness_seconds": "invalid"})
    assert r7.status_code == 400


def test_error_400_unknown_field(serving_setup):
    """Test 400 when an unknown field is present in the request body."""
    client, priv, _, _ = serving_setup
    sql = "SELECT id FROM users"
    token = _make_token(priv, sql)

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "unrecognized_extra_key": "some_value"},
    )
    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"
    assert "unrecognized_extra_key" in resp.json()["message"]


def test_error_413_body_too_large(serving_setup):
    """Test 413 when request body exceeds max_body_bytes (configured as 1000 bytes in fixture)."""
    client, priv, _, _ = serving_setup
    sql = "SELECT id FROM users"
    token = _make_token(priv, sql)

    huge_comment = "/* " + ("A" * 2000) + " */"
    large_sql = f"{huge_comment} {sql}"

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": large_sql},
    )
    assert resp.status_code == 413
    assert resp.json()["error"] == "payload_too_large"


def test_error_503_no_adapter(serving_setup):
    """Test 503 when the routed engine has no configured adapter."""
    client, priv, _, _ = serving_setup
    # 'missing_eng' is defined in placement but omitted from adapters mapping
    sql = "SELECT id FROM users"
    token = _make_token(priv, sql)

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "hint": "missing_eng"},
    )
    assert resp.status_code == 503
    assert resp.json()["error"] == "engine_unavailable"


def test_error_504_engine_timeout(ec_key_pair):
    """Test 504 when adapter raises EngineTimeout."""
    priv, pub = ec_key_pair

    class TimeoutAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EngineTimeout("Driver statement timeout")

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
    app = create_app(pipeline, {"duck": TimeoutAdapter()})
    client = TestClient(app)

    sql = "SELECT id FROM users"
    token = _make_token(priv, sql, tables={"users": {"columns": ["id"], "masks": {}, "actions": ["select"]}})

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 504
    assert resp.json()["error"] == "engine_timeout"


def test_error_502_engine_error(ec_key_pair):
    """Test 502 when adapter raises EngineError."""
    priv, pub = ec_key_pair

    class FailingAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EngineError("Underlying engine failed")

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
    app = create_app(pipeline, {"duck": FailingAdapter()})
    client = TestClient(app)

    sql = "SELECT id FROM users"
    token = _make_token(priv, sql, tables={"users": {"columns": ["id"], "masks": {}, "actions": ["select"]}})

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 502
    assert resp.json()["error"] == "engine_error"


def test_error_bodies_never_leak_secrets(serving_setup):
    """Test that error responses never contain the raw SQL literals, param values, or grant token."""
    client, priv, _, _ = serving_setup

    secret_literal = "SECRET_SQL_LITERAL_9999"
    secret_param = "SECRET_PARAM_VALUE_8888"

    # SQL with a deliberate parse/syntax error containing the secret literal
    malformed_sql = f"SELECT id FROM users WHERE name = '{secret_literal}' AND %%%INVALID%%%"
    token = _make_token(priv, malformed_sql)

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": malformed_sql, "params": [secret_param]},
    )

    body_text = resp.text
    assert secret_literal not in body_text
    assert secret_param not in body_text
    assert token not in body_text
