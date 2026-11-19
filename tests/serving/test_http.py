"""Tests for Seeknal Serving HTTP API."""

from __future__ import annotations

import asyncio
import datetime
import hashlib
import json
import time
from decimal import Decimal

import duckdb
import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import Placement, ServingPipeline, sign_cancel_token, sign_grant_token
from seeknal.serving.adapters import (
    DuckDBAdapter,
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
    adapter = DuckDBAdapter(lambda: con.cursor())
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


def test_http_named_operation_parameter_digest_and_compilation_cache(serving_setup):
    """Test /v1/query and /v1/explain with signed named-operation claims, cache hits, and tampering refusal."""
    from seeknal.serving.parameter_binding import parameter_digest

    client, priv, _, pipeline = serving_setup
    sql = "SELECT id, name, salary FROM users WHERE salary >= $1 LIMIT 10"

    def make_named_token(params: list, *, jti: str, include_digest: bool = True) -> str:
        now = time.time()
        claims = {
            "sub": "analyst@example.com",
            "ws": "ws-test",
            "jti": jti,
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 60,
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "operation_id": "orders_by_month",
            "operation_version": 1,
            "max_rows": 10,
            "tables": {
                "users": {
                    "columns": ["id", "salary"],
                    "masks": {"name": "redact"},
                    "row_filters": [{"column": "role", "op": "eq", "values": ["analyst"]}],
                    "actions": ["select"],
                }
            },
        }
        if include_digest:
            claims["params_sha256"] = parameter_digest(params)
        return sign_grant_token(claims, priv, kid="key-1")

    # 1. First request misses compilation cache and populates it
    t1 = make_named_token([60000], jti="jti-named-1")
    r1 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t1}"},
        json={"sql": sql, "params": [60000]},
    )
    assert r1.status_code == 200
    assert r1.json()["rows"] == [[2, "****", 80000]]
    assert pipeline.compilation_cache_info()["misses"] == 1
    assert pipeline.compilation_cache_info()["hits"] == 0
    assert pipeline.compilation_cache_info()["size"] == 1

    # 2. Second request with distinct parameter [40000] hits compilation cache and binds fresh params
    t2 = make_named_token([40000], jti="jti-named-2")
    r2 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t2}"},
        json={"sql": sql, "params": [40000]},
    )
    assert r2.status_code == 200
    assert r2.json()["rows"] == [[2, "****", 80000], [3, "****", 50000]]
    assert pipeline.compilation_cache_info()["hits"] == 1

    # 3. /v1/explain also hits compilation cache and verifies parameter digest
    t3 = make_named_token([75000], jti="jti-named-3")
    r3 = client.post(
        "/v1/explain",
        headers={"Authorization": f"Bearer {t3}"},
        json={"sql": sql, "params": [75000]},
    )
    assert r3.status_code == 200
    assert pipeline.compilation_cache_info()["hits"] == 2

    # 4. Tampered parameter value returns 403 enforcement_error without leaking parameter value
    t4 = make_named_token([60000], jti="jti-named-4")
    r4 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t4}"},
        json={"sql": sql, "params": [987654321]},
    )
    assert r4.status_code == 403
    assert r4.json()["error"] == "enforcement_error"
    assert "987654321" not in r4.text
    assert pipeline.compilation_cache_info()["hits"] == 2  # Failed before cache lookup!

    # 5. Missing params_sha256 on named operation returns 403 enforcement_error
    t5 = make_named_token([60000], jti="jti-named-5", include_digest=False)
    r5 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t5}"},
        json={"sql": sql, "params": [60000]},
    )
    assert r5.status_code == 403
    assert r5.json()["error"] == "enforcement_error"


def test_safe_message_redaction_precision_preserves_61_0s_and_redacts_secret_param(
    ec_key_pair,
):
    """Ensure params=[0] does not corrupt '61.0s' while 'PARAM_SECRET_VALUE' is still redacted."""
    from seeknal.serving.http import _safe_message

    priv, pub = ec_key_pair
    placement = Placement.from_dict(
        {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "users": {
                    "keys": ["id"],
                    "row_count": 1,
                    "serve": {"duck": "users"},
                }
            },
            "cubes": {},
        }
    )
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id", "name"]},
        public_keys={"key-1": pub},
        max_grant_lifetime_seconds=60.0,
    )
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR)")
    try:
        app = create_app(pipeline, {"duck": DuckDBAdapter(lambda: con.cursor())})
        client = TestClient(app)

        sql = "SELECT id FROM users WHERE id >= $1 AND name != $2"
        token_61s = _make_token(
            priv,
            sql,
            exp_offset=61.0,
            iat_offset=0.0,
            tables={"users": {"columns": ["id"], "masks": {}, "actions": ["select"]}},
        )

        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token_61s}"},
            json={"sql": sql, "params": [0, "PARAM_SECRET_VALUE"]},
        )
        assert resp.status_code == 401
        msg = resp.json()["message"]
        assert "61.0s" in msg
        assert "60.0s" in msg
        assert "PARAM_SECRET_VALUE" not in resp.text

        direct_msg = _safe_message(
            "Token lifetime of 61.0s exceeds maximum 60.0s; quoted='0'; secret=PARAM_SECRET_VALUE; lit='LIT_SECRET_VAL'",
            sql="SELECT id FROM users WHERE name = 'LIT_SECRET_VAL'",
            params=[0, "PARAM_SECRET_VALUE"],
        )
        assert "61.0s" in direct_msg
        assert "60.0s" in direct_msg
        assert "'0'" not in direct_msg
        assert "PARAM_SECRET_VALUE" not in direct_msg
        assert "LIT_SECRET_VAL" not in direct_msg
    finally:
        con.close()


def test_c_n1_refuse_mode_never_weakens_record_when_sink_fails_only_full_records(
    ec_key_pair,
    caplog: pytest.LogCaptureFixture,
):
    """Structured and callable sinks failing only full records return 503 with no rows and no minimal fallback."""
    import errno
    import logging
    from seeknal.serving.audit import AuditRecord, AuditSink

    priv, pub = ec_key_pair
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR, salary INT, role VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'SENSITIVE_ROW_CELL', 90000, 'analyst')")

    try:
        placement = Placement.from_dict(
            {
                "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {
                    "users": {
                        "keys": ["id"],
                        "row_count": 1,
                        "serve": {"duck": "users"},
                    }
                },
                "cubes": {},
            }
        )
        pipeline = ServingPipeline(
            placement=placement,
            schemas={"users": ["id", "name", "salary", "role"]},
            public_keys={"key-1": pub},
        )
        adapter = DuckDBAdapter(lambda: con.cursor())
        sql = "SELECT id, salary FROM users"

        # 1) Structured sink that accepts minimal records (tables == [] and subject is None)
        #    but fails full records!
        persisted_structured: list[AuditRecord] = []

        class FullRecordRejectingStructuredSink(AuditSink):
            def verify_writable(self) -> None:
                return None

            def write_record(self, record: AuditRecord | dict) -> None:
                rec = (
                    record
                    if isinstance(record, AuditRecord)
                    else AuditRecord.from_dict(record)
                )
                if rec.tables or rec.subject is not None or rec.sql_sha256 is not None:
                    raise OSError(errno.EMSGSIZE, "Message too long for full record")
                persisted_structured.append(rec)

        app_struct = create_app(
            pipeline,
            {"duck": adapter},
            audit_sink=FullRecordRejectingStructuredSink(),
            audit_on_failure="refuse",
        )
        client_struct = TestClient(app_struct)
        tok1 = _make_token(priv, sql)
        with caplog.at_level(logging.ERROR):
            caplog.clear()
            r_struct = client_struct.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok1}"},
                json={"sql": sql},
            )

        assert r_struct.status_code == 503
        body_struct = r_struct.json()
        assert body_struct["code"] == "audit_unavailable"
        assert "rows" not in body_struct
        assert "90000" not in r_struct.text
        assert persisted_structured == []
        assert any(
            rec.levelno == logging.ERROR
            and "code=audit_unavailable" in rec.getMessage()
            and "detail=" in rec.getMessage()
            and "Message too long" in rec.getMessage()
            and "audit_id=jti-http-001" in rec.getMessage()
            for rec in caplog.records
        )

        # 2) Callable sink that fails full records on both /v1/query (pre-exec) and /v1/explain (_finish)
        persisted_callable: list[dict] = []

        def full_record_rejecting_callable(rec_dict: dict) -> None:
            if (
                rec_dict.get("tables")
                or rec_dict.get("subject") is not None
                or rec_dict.get("sql_sha256") is not None
            ):
                raise RuntimeError("callable broker rejected full audit payload")
            persisted_callable.append(rec_dict)

        app_call = create_app(
            pipeline,
            {"duck": adapter},
            audit_sink=full_record_rejecting_callable,
            audit_on_failure="refuse",
        )
        client_call = TestClient(app_call)
        tok2 = _make_token(priv, sql)
        with caplog.at_level(logging.ERROR):
            caplog.clear()
            r_call_q = client_call.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok2}"},
                json={"sql": sql},
            )
            r_call_exp = client_call.post(
                "/v1/explain",
                headers={"Authorization": f"Bearer {tok2}"},
                json={"sql": sql},
            )

        for resp in (r_call_q, r_call_exp):
            assert resp.status_code == 503
            assert resp.json()["code"] == "audit_unavailable"
            assert "rows" not in resp.json()
        assert persisted_callable == []
        err_msgs = [
            rec.getMessage()
            for rec in caplog.records
            if rec.levelno == logging.ERROR and "code=audit_unavailable" in rec.getMessage()
        ]
        assert len(err_msgs) == 2
        for m in err_msgs:
            assert "detail=RuntimeError: callable broker rejected full audit payload" in m
            assert "audit_id=jti-http-001" in m
    finally:
        con.close()


@pytest.mark.parametrize("route", ["/v1/query", "/v1/explain", "/v1/cancel"])
@pytest.mark.parametrize("chunk_size", [1024, 4096])
def test_chunked_20mb_body_rejected_streaming_on_all_post_routes(
    ec_key_pair, route: str, chunk_size: int
):
    """N2: Chunked 20 MB stream without Content-Length stops reading at max_body_bytes."""
    _, pub = ec_key_pair
    max_cap = 1024
    total_stream_bytes = 20 * 1024 * 1024  # 20 MB

    placement = Placement.from_dict(
        {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "users": {"keys": ["id"], "row_count": 1, "serve": {"duck": "users"}}
            },
            "cubes": {},
        }
    )
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id"]},
        public_keys={"key-1": pub},
    )
    audited: list[dict] = []
    con = duckdb.connect()
    try:
        app = create_app(
            pipeline,
            {"duck": DuckDBAdapter(lambda: con.cursor())},
            max_body_bytes=max_cap,
            audit_sink=audited.append,
        )

        bytes_yielded = 0
        chunk_payload = b"x" * chunk_size
        num_chunks = total_stream_bytes // chunk_size

        async def _counting_20mb_stream():
            nonlocal bytes_yielded
            for _ in range(num_chunks):
                bytes_yielded += len(chunk_payload)
                yield chunk_payload

        async def _run() -> httpx.Response:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as async_client:
                return await async_client.post(
                    route,
                    content=_counting_20mb_stream(),
                )

        resp = asyncio.run(_run())

        assert "content-length" not in resp.request.headers
        assert resp.status_code == 413
        payload = resp.json()
        assert payload["code"] == "payload_too_large"
        assert payload["error"] == "payload_too_large"
        assert 0 < bytes_yielded <= max_cap + chunk_size
        assert bytes_yielded < total_stream_bytes
        assert len(audited) == 1
        assert audited[0]["route"] == route
        assert audited[0]["status"] == 413
        assert audited[0]["code"] == "payload_too_large"
    finally:
        con.close()


def test_chunked_body_within_cap_and_midstream_disconnect(ec_key_pair):
    """N2: Chunked bodies <= max_body_bytes succeed and mid-stream ClientDisconnect returns 499."""
    priv, pub = ec_key_pair
    max_cap = 1024
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE users (id INT, role VARCHAR)")
        con.execute("INSERT INTO users VALUES (1, 'analyst')")
        placement = Placement.from_dict(
            {
                "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {
                    "users": {
                        "keys": ["id"],
                        "row_count": 1,
                        "serve": {"duck": "users"},
                    }
                },
                "cubes": {},
            }
        )
        pipeline = ServingPipeline(
            placement=placement,
            schemas={"users": ["id", "role"]},
            public_keys={"key-1": pub},
        )
        app = create_app(
            pipeline,
            {"duck": DuckDBAdapter(lambda: con.cursor())},
            max_body_bytes=max_cap,
        )

        sql = "SELECT id FROM users"
        tables_claim = {
            "users": {"columns": ["id"], "masks": {}, "actions": ["select"]}
        }

        async def _chunked_bytes(raw: bytes, step: int = 17):
            for idx in range(0, len(raw), step):
                yield raw[idx : idx + step]

        async def _run_valid_chunked() -> None:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as async_client:
                # Exact-boundary 1024-byte chunked body on /v1/query
                base_body = json.dumps({"sql": sql}).encode("utf-8")
                padded_body = base_body + (b" " * (max_cap - len(base_body)))
                assert len(padded_body) == max_cap
                tok_q = sign_grant_token(
                    {
                        "jti": "jti-chunk-q",
                        "sub": "analyst@example.com",
                        "ws": "ws-test",
                        "iat": time.time(),
                        "exp": time.time() + 60,
                        "aud": "seeknal-serving",
                        "iss": "atlas-catalog",
                        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                        "tables": tables_claim,
                        "max_rows": 100,
                    },
                    priv,
                    kid="key-1",
                )
                rq = await async_client.post(
                    "/v1/query",
                    headers={"Authorization": f"Bearer {tok_q}"},
                    content=_chunked_bytes(padded_body, step=64),
                )
                assert "content-length" not in rq.request.headers
                assert rq.status_code == 200
                assert rq.json()["rows"] == [[1]]

                tok_e = sign_grant_token(
                    {
                        "jti": "jti-chunk-e",
                        "sub": "analyst@example.com",
                        "ws": "ws-test",
                        "iat": time.time(),
                        "exp": time.time() + 60,
                        "aud": "seeknal-serving",
                        "iss": "atlas-catalog",
                        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                        "tables": tables_claim,
                        "max_rows": 100,
                    },
                    priv,
                    kid="key-1",
                )
                rexp = await async_client.post(
                    "/v1/explain",
                    headers={"Authorization": f"Bearer {tok_e}"},
                    content=_chunked_bytes(base_body, step=7),
                )
                assert "content-length" not in rexp.request.headers
                assert rexp.status_code == 200

                now_ts = time.time()
                tok_c = sign_cancel_token(
                    {
                        "typ": "cancel",
                        "jti": "jti-chunk-c",
                        "sub": "analyst@example.com",
                        "iat": now_ts,
                        "exp": now_ts + 60,
                        "aud": "seeknal-serving",
                        "iss": "atlas-catalog",
                    },
                    priv,
                    kid="key-1",
                )
                rc = await async_client.post(
                    "/v1/cancel",
                    headers={"Authorization": f"Bearer {tok_c}"},
                    content=_chunked_bytes(b" { } ", step=2),
                )
                assert "content-length" not in rc.request.headers
                assert rc.status_code == 404
                assert rc.json()["code"] == "not_found"

        asyncio.run(_run_valid_chunked())

        async def _send_midstream_disconnect(route: str) -> tuple[int, dict]:
            scope = {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "POST",
                "scheme": "http",
                "path": route,
                "raw_path": route.encode("ascii"),
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 50000),
                "server": ("test", 80),
            }
            sent_first = False
            status_holder: list[int] = []
            body_parts: list[bytes] = []

            async def _receive():
                nonlocal sent_first
                if not sent_first:
                    sent_first = True
                    return {"type": "http.request", "body": b'{"sql":', "more_body": True}
                return {"type": "http.disconnect"}

            async def _send(msg):
                if msg["type"] == "http.response.start":
                    status_holder.append(msg["status"])
                elif msg["type"] == "http.response.body":
                    body_parts.append(msg.get("body", b""))

            await app(scope, _receive, _send)
            return status_holder[0], json.loads(b"".join(body_parts).decode("utf-8"))

        for post_route in ("/v1/query", "/v1/explain", "/v1/cancel"):
            st, body = asyncio.run(_send_midstream_disconnect(post_route))
            assert st == 499
            assert body["code"] == "query_cancelled"
    finally:
        con.close()
