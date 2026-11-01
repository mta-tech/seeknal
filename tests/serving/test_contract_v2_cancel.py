"""Tests for Seeknal Serving contract v2: query cancellation, tokens, and log redaction."""

from __future__ import annotations

import asyncio
import hashlib
import threading
import time
from typing import Any

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.responses import Response
from starlette.testclient import TestClient

from seeknal.serving import (
    DBAPIAdapter,
    DuckDBAdapter,
    EngineError,
    GrantTokenError,
    Placement,
    QueryResult,
    ServingPipeline,
    create_app,
    sign_cancel_token,
    sign_grant_token,
    verify_cancel_token,
    verify_grant_token,
)
from seeknal.serving.service import PreparedQuery


def _base_claims(sql: str = "SELECT 1", **kwargs: Any) -> dict[str, Any]:
    now = time.time()
    claims: dict[str, Any] = {
        "sub": "user@example.com",
        "ws": "ws-cancel",
        "jti": "jti-cancel-01",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 60,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 1000,
        "tables": {
            "users": {
                "columns": ["id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    claims.update(kwargs)
    return claims


def _base_cancel_claims(**kwargs: Any) -> dict[str, Any]:
    now = time.time()
    claims: dict[str, Any] = {
        "typ": "cancel",
        "sub": "user@example.com",
        "jti": "jti-cancel-01",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 30,
    }
    claims.update(kwargs)
    return claims


# -------------------------------------------------------------------------
# 1. Cancel token verification tests
# -------------------------------------------------------------------------


def test_verify_cancel_token_valid():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    claims = _base_cancel_claims()
    token = sign_cancel_token(claims, priv, kid="key-1")

    verified = verify_cancel_token(token, {"key-1": pub})
    assert verified["typ"] == "cancel"
    assert verified["jti"] == "jti-cancel-01"
    assert verified["sub"] == "user@example.com"


@pytest.mark.parametrize("bad_typ", ["JWT", "grant", "refresh", None])
def test_verify_cancel_token_wrong_or_missing_typ(bad_typ: Any):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    claims = _base_cancel_claims()
    if bad_typ is None:
        del claims["typ"]
    else:
        claims["typ"] = bad_typ
    token = sign_grant_token(claims, priv, kid="key-1")

    with pytest.raises(GrantTokenError):
        verify_cancel_token(token, {"key-1": pub})


def test_verify_cancel_token_lifetime_over_60_refused():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    now = time.time()
    # exp - iat = 65 > 60
    claims = _base_cancel_claims(iat=now, exp=now + 65)
    token = sign_cancel_token(claims, priv, kid="key-1")

    with pytest.raises(GrantTokenError) as exc_info:
        verify_cancel_token(token, {"key-1": pub})
    assert "exceeds maximum 60" in str(exc_info.value)


def test_verify_cancel_token_wrong_aud():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    claims = _base_cancel_claims(aud="wrong-audience")
    token = sign_cancel_token(claims, priv, kid="key-1")

    with pytest.raises(GrantTokenError):
        verify_cancel_token(token, {"key-1": pub})


def test_verify_cancel_token_wrong_iss():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    claims = _base_cancel_claims(iss="wrong-issuer")
    token = sign_cancel_token(claims, priv, kid="key-1")

    with pytest.raises(GrantTokenError):
        verify_cancel_token(token, {"key-1": pub})


def test_verify_cancel_token_bad_signature():
    priv1 = ec.generate_private_key(ec.SECP256R1())
    priv2 = ec.generate_private_key(ec.SECP256R1())
    pub2 = priv2.public_key()

    claims = _base_cancel_claims()
    token = sign_cancel_token(claims, priv1, kid="key-1")

    with pytest.raises(GrantTokenError):
        verify_cancel_token(token, {"key-1": pub2})


# -------------------------------------------------------------------------
# 2. Token mutual exclusion
# -------------------------------------------------------------------------


def test_grant_used_as_cancel_token_refused_at_v1_cancel():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pipeline = ServingPipeline(
        placement=Placement.from_dict({
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"duck": "users"}}},
            "cubes": {},
        }),
        schemas={"users": ["id"]},
        public_keys={"key-1": pub},
    )
    app = create_app(pipeline, {"duck": DuckDBAdapter(duckdb.connect)})
    client = TestClient(app)

    # Standard grant token (without typ="cancel")
    grant_token = sign_grant_token(_base_claims(), priv, kid="key-1")

    resp = client.post(
        "/v1/cancel",
        headers={"Authorization": f"Bearer {grant_token}"},
    )
    assert resp.status_code == 401
    assert resp.json()["error"] == "grant_token_error"


def test_cancel_token_used_as_grant_refused_at_v1_query():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pipeline = ServingPipeline(
        placement=Placement.from_dict({
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"duck": "users"}}},
            "cubes": {},
        }),
        schemas={"users": ["id"]},
        public_keys={"key-1": pub},
    )
    app = create_app(pipeline, {"duck": DuckDBAdapter(duckdb.connect)})
    client = TestClient(app)

    cancel_token = sign_cancel_token(_base_cancel_claims(), priv, kid="key-1")

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {cancel_token}"},
        json={"sql": "SELECT 1"},
    )
    assert resp.status_code == 401
    assert resp.json()["error"] == "grant_token_error"


# -------------------------------------------------------------------------
# 3. Running query cancellation via /v1/cancel
# -------------------------------------------------------------------------


def test_running_duckdb_query_cancelled_via_v1_cancel():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT)")
    for i in range(100):
        con.execute(f"INSERT INTO users VALUES ({i})")

    placement = Placement.from_dict({
        "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {"users": {"keys": ["id"], "row_count": 100, "serve": {"duck": "users"}}},
        "cubes": {},
    })
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id"]},
        public_keys={"key-1": pub},
    )
    adapter = DuckDBAdapter(lambda: con)
    app = create_app(pipeline, {"duck": adapter}, timeout_seconds=30.0)
    client = TestClient(app)

    # Cross join of 5 copies of 100-row table = 10 billion rows
    sql = "SELECT count(*) FROM users AS t1, users AS t2, users AS t3, users AS t4, users AS t5"
    jti = "jti-cancel-duck-01"
    sub = "analyst@example.com"
    grant_token = sign_grant_token(
        _base_claims(sql=sql, jti=jti, sub=sub),
        priv,
        kid="key-1",
    )
    cancel_token = sign_cancel_token(
        _base_cancel_claims(jti=jti, sub=sub),
        priv,
        kid="key-1",
    )

    query_status: list[int] = []
    query_body: list[dict] = []

    def run_query():
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {grant_token}"},
            json={"sql": sql},
        )
        query_status.append(resp.status_code)
        query_body.append(resp.json())

    t = threading.Thread(target=run_query)
    t.start()

    # Wait briefly for query to start executing
    time.sleep(0.2)

    cancel_resp = client.post(
        "/v1/cancel",
        headers={"Authorization": f"Bearer {cancel_token}"},
    )
    assert cancel_resp.status_code == 202
    assert cancel_resp.json() == {"cancelled": True, "query_id": jti}

    t.join(timeout=5.0)
    assert len(query_status) == 1
    assert query_status[0] == 499
    assert query_body[0]["error"] == "query_cancelled"


def test_cancel_with_wrong_sub_gives_404():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    query_started = threading.Event()
    query_can_finish = threading.Event()

    class BlockingAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float, on_cancel_handle=None) -> QueryResult:
            query_started.set()
            query_can_finish.wait(5.0)
            return QueryResult(columns=("id",), rows=[(1,)], truncated=False, engine="block", elapsed_ms=1.0)

    placement = Placement.from_dict({
        "engines": {"block": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"block": "users"}}},
        "cubes": {},
    })
    pipeline = ServingPipeline(placement=placement, schemas={"users": ["id"]}, public_keys={"key-1": pub})
    app = create_app(pipeline, {"block": BlockingAdapter()})
    client = TestClient(app)

    sql = "SELECT id FROM users"
    jti = "jti-wrong-sub-01"
    sub_owner = "user-owner@example.com"
    sub_other = "user-other@example.com"

    grant_token = sign_grant_token(_base_claims(sql=sql, jti=jti, sub=sub_owner), priv, kid="key-1")
    cancel_token_wrong_sub = sign_cancel_token(_base_cancel_claims(jti=jti, sub=sub_other), priv, kid="key-1")

    query_res: list[int] = []

    def run_query():
        resp = client.post("/v1/query", headers={"Authorization": f"Bearer {grant_token}"}, json={"sql": sql})
        query_res.append(resp.status_code)

    t = threading.Thread(target=run_query)
    t.start()
    query_started.wait(2.0)

    # Cancel with wrong sub -> 404 not_found
    cancel_resp = client.post(
        "/v1/cancel",
        headers={"Authorization": f"Bearer {cancel_token_wrong_sub}"},
    )
    assert cancel_resp.status_code == 404
    assert cancel_resp.json()["error"] == "not_found"

    # Allow query to finish normally
    query_can_finish.set()
    t.join(timeout=5.0)
    assert query_res == [200]


def test_cancel_unknown_jti_gives_404():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pipeline = ServingPipeline(
        placement=Placement.from_dict({
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"duck": "users"}}},
            "cubes": {},
        }),
        schemas={"users": ["id"]},
        public_keys={"key-1": pub},
    )
    app = create_app(pipeline, {"duck": DuckDBAdapter(duckdb.connect)})
    client = TestClient(app)

    cancel_token = sign_cancel_token(_base_cancel_claims(jti="nonexistent-jti"), priv, kid="key-1")
    resp = client.post("/v1/cancel", headers={"Authorization": f"Bearer {cancel_token}"})
    assert resp.status_code == 404
    assert resp.json()["error"] == "not_found"


def test_duplicate_running_jti_gives_409():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    query1_started = threading.Event()
    query1_release = threading.Event()

    class BlockingAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float, on_cancel_handle=None) -> QueryResult:
            query1_started.set()
            query1_release.wait(5.0)
            return QueryResult(columns=("id",), rows=[(1,)], truncated=False, engine="block", elapsed_ms=1.0)

    placement = Placement.from_dict({
        "engines": {"block": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"block": "users"}}},
        "cubes": {},
    })
    pipeline = ServingPipeline(placement=placement, schemas={"users": ["id"]}, public_keys={"key-1": pub})
    app = create_app(pipeline, {"block": BlockingAdapter()})
    client = TestClient(app)

    sql = "SELECT id FROM users"
    jti = "jti-dup-01"
    token = sign_grant_token(_base_claims(sql=sql, jti=jti), priv, kid="key-1")

    t1 = threading.Thread(target=lambda: client.post("/v1/query", headers={"Authorization": f"Bearer {token}"}, json={"sql": sql}))
    t1.start()
    query1_started.wait(2.0)

    # Second query with the same jti is submitted while the first is running -> 409
    resp_dup = client.post("/v1/query", headers={"Authorization": f"Bearer {token}"}, json={"sql": sql})
    assert resp_dup.status_code == 409
    assert resp_dup.json()["error"] == "duplicate_query_id"

    query1_release.set()
    t1.join(timeout=5.0)


# -------------------------------------------------------------------------
# 4. StarRocks cancel path
# -------------------------------------------------------------------------


class FakeSRCursor:
    def __init__(self, conn: FakeSRConnection) -> None:
        self.conn = conn
        self.description = None

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.conn.statements.append(sql)

    def fetchone(self) -> tuple | None:
        if self.conn.statements and "SELECT CONNECTION_ID()" in self.conn.statements[-1]:
            return (54321,)
        return None

    def fetchmany(self, size: int) -> list[tuple]:
        return []

    def close(self) -> None:
        pass


class FakeSRConnection:
    def __init__(self, pool: list[FakeSRConnection]) -> None:
        self.statements: list[str] = []
        pool.append(self)

    def cursor(self) -> FakeSRCursor:
        return FakeSRCursor(self)

    def close(self) -> None:
        pass


def test_starrocks_cancel_path_records_select_connection_id_and_kill_query():
    connections: list[FakeSRConnection] = []

    def connect():
        return FakeSRConnection(connections)

    adapter = DBAPIAdapter(connect, kind="starrocks")
    prep = PreparedQuery(
        engine="sr",
        engine_kind="starrocks",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        rule="test",
        reason="test",
        row_limit=10,
        masked_columns=(),
        redacted_sql="SELECT 1",
        subject="user@example.com",
        workspace="ws",
        audit_id="audit-sr",
        cube_security_context=None,
    )

    cancel_callable: list[Any] = []

    def handle(cancel_func):
        cancel_callable.append(cancel_func)

    adapter.execute(prep, timeout_seconds=10.0, on_cancel_handle=handle)

    assert len(connections) == 1
    conn1 = connections[0]
    assert conn1.statements[0] == "SELECT CONNECTION_ID()"

    assert len(cancel_callable) == 1
    # Calling the cancel function opens second connection and issues KILL QUERY 54321
    cancel_callable[0]()

    assert len(connections) == 2
    conn2 = connections[1]
    assert conn2.statements == ["KILL QUERY 54321"]


# -------------------------------------------------------------------------
# 5. Client disconnect cancel
# -------------------------------------------------------------------------


def test_disconnect_cancel_calls_cancel_callable_and_finishes():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    cancel_was_called = threading.Event()

    class FakeDisconnectAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float, on_cancel_handle=None) -> QueryResult:
            if on_cancel_handle is not None:
                on_cancel_handle(cancel_was_called.set)
            cancel_was_called.wait(5.0)
            return QueryResult(columns=(), rows=[], truncated=False, engine="disc", elapsed_ms=1.0)

    placement = Placement.from_dict({
        "engines": {"disc": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"disc": "users"}}},
        "cubes": {},
    })
    pipeline = ServingPipeline(placement=placement, schemas={"users": ["id"]}, public_keys={"key-1": pub})
    app = create_app(pipeline, {"disc": FakeDisconnectAdapter()})

    sql = "SELECT id FROM users"
    token = sign_grant_token(_base_claims(sql=sql), priv, kid="key-1")

    class FakeDisconnectRequest:
        def __init__(self) -> None:
            self.headers = {"authorization": f"Bearer {token}"}
            self.checks = 0

        async def body(self) -> bytes:
            return f'{{"sql": "{sql}"}}'.encode("utf-8")

        async def is_disconnected(self) -> bool:
            self.checks += 1
            # Return True on subsequent checks
            return self.checks >= 2

    async def run_disconnect_test():
        req = FakeDisconnectRequest()
        query_route = [r for r in app.routes if getattr(r, "path", None) == "/v1/query"][0]
        resp = await query_route.endpoint(req)
        return resp

    resp = asyncio.run(run_disconnect_test())

    assert cancel_was_called.is_set()
    assert resp.status_code == 499
    assert resp.body == b""


# -------------------------------------------------------------------------
# 6. Log redaction
# -------------------------------------------------------------------------


def test_log_redaction_secret_literal_never_in_logs(caplog):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    secret = "SUPER_SECRET_DATABASE_PASSWORD_99999"

    class FailingDriverAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float, on_cancel_handle=None) -> QueryResult:
            raise EngineError(f"Database auth failure with credential: {secret}")

    placement = Placement.from_dict({
        "engines": {"fail": {"kind": "starrocks", "dialect": "starrocks"}},
        "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"fail": "users"}}},
        "cubes": {},
    })
    pipeline = ServingPipeline(placement=placement, schemas={"users": ["id"]}, public_keys={"key-1": pub})
    app = create_app(pipeline, {"fail": FailingDriverAdapter()})
    client = TestClient(app)

    sql = "SELECT id FROM users"
    token = sign_grant_token(_base_claims(sql=sql), priv, kid="key-1")

    with caplog.at_level("DEBUG"):
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token}"},
            json={"sql": sql},
        )

    assert resp.status_code == 502
    assert secret not in caplog.text
