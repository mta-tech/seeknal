"""Tests for Seeknal Serving typed error catalogue, HTTP error payloads, notices, and redaction."""

from __future__ import annotations

import hashlib
import logging
import pathlib
import threading
import time
from typing import Any

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving.adapters import (
    DuckDBAdapter,
    EngineCancelled,
    EngineConfigError,
    EngineError,
    EnginePoolExhausted,
    EngineTimeout,
    QueryResult,
)
from seeknal.serving.audit import AuditSink, AuditWriteError
from seeknal.serving.enforce import EnforcementError
from seeknal.serving.errors import (
    ERROR_CATALOGUE,
    REQUIRED_ERROR_CODES,
    AuditUnavailableError,
    _sanitize_log_value,
    clear_registered_config_secrets,
    register_config_secrets,
    scrub_error_detail,
)
from seeknal.serving.http import _sanitize_log_value as http_sanitize_log_value
from seeknal.serving.grant_token import sign_cancel_token, sign_grant_token
from seeknal.serving.http import GrantReplayCache, create_app
from seeknal.serving.placement import Placement
from seeknal.serving.router import RoutingError
from seeknal.serving.service import PreparedQuery, ServingPipeline


@pytest.fixture(scope="module")
def ec_keys() -> tuple[ec.EllipticCurvePrivateKey, ec.EllipticCurvePublicKey]:
    priv = ec.generate_private_key(ec.SECP256R1())
    return priv, priv.public_key()


def _make_grant(
    priv: ec.EllipticCurvePrivateKey,
    sql: str,
    *,
    jti: str = "jti-typed-001",
    sub: str = "analyst@atlas.example",
    tables: dict[str, Any] | None = None,
    max_rows: int = 100,
    iat_offset: float = 0.0,
    exp_offset: float = 60.0,
    kid: str = "key-1",
    extra_claims: dict[str, Any] | None = None,
) -> str:
    now = time.time()
    if tables is None:
        tables = {
            "users": {
                "columns": ["id", "name", "salary"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        }
    claims: dict[str, Any] = {
        "sub": sub,
        "ws": "ws-typed",
        "jti": jti,
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now + iat_offset,
        "exp": now + exp_offset,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": tables,
    }
    if extra_claims:
        claims.update(extra_claims)
    return sign_grant_token(claims, priv, kid=kid)


def _assert_typed_error(
    resp: Any,
    expected_code: str,
    *,
    expected_audit_id: str | None = None,
    expected_status: int | None = None,
    expected_category: str | None = None,
    expected_message: str | None = None,
) -> dict[str, Any]:
    spec = ERROR_CATALOGUE[expected_code]
    assert resp.status_code == (
        expected_status if expected_status is not None else spec.status
    )
    body = resp.json()
    assert body["code"] == spec.code
    assert body["error"] == (
        expected_category if expected_category is not None else spec.category
    )
    assert body["message"] == (
        expected_message if expected_message is not None else spec.message
    )
    assert body["next_step"] == spec.next_step
    if expected_audit_id is not None:
        assert body["audit_id"] == expected_audit_id
    else:
        assert "audit_id" not in body
    return body


# ---------------------------------------------------------------------------
# 1. Catalogue invariants & documentation sync
# ---------------------------------------------------------------------------


def test_error_catalogue_contains_all_required_codes_and_is_immutable():
    assert len(REQUIRED_ERROR_CODES) == 16
    for code in REQUIRED_ERROR_CODES:
        assert code in ERROR_CATALOGUE
        spec = ERROR_CATALOGUE[code]
        assert spec.code == code
        assert spec.status >= 400
        assert spec.category
        assert spec.message
        assert spec.next_step

    # Additional HTTP compatibility codes
    assert "not_found" in ERROR_CATALOGUE
    assert "duplicate_query_id" in ERROR_CATALOGUE

    with pytest.raises(TypeError):
        ERROR_CATALOGUE["tamper"] = ERROR_CATALOGUE["internal_error"]  # type: ignore[index]


def test_http_docs_document_all_catalogue_codes():
    doc_path = (
        pathlib.Path(__file__).resolve().parents[2] / "docs" / "serving" / "http.md"
    )
    doc_text = doc_path.read_text(encoding="utf-8")
    for code, spec in ERROR_CATALOGUE.items():
        assert f"`{code}`" in doc_text
        assert str(spec.status) in doc_text
        assert f"`{spec.category}`" in doc_text
    assert "`row_cap_hit`" in doc_text
    assert "`notices`" in doc_text
    assert "`next_step`" in doc_text


# ---------------------------------------------------------------------------
# 2. HTTP reachability for every code in ERROR_CATALOGUE
# ---------------------------------------------------------------------------


@pytest.fixture
def multi_engine_setup(ec_keys):
    priv, pub = ec_keys
    parent = duckdb.connect()
    parent.execute("CREATE TABLE users (id INT, name VARCHAR, salary INT)")
    parent.execute(
        "INSERT INTO users VALUES (1, 'Alice', 90000), (2, 'Bob', 80000), (3, 'Cara', 70000)"
    )

    placement = Placement.from_dict(
        {
            "engines": {
                "duck": {"kind": "duckdb", "dialect": "duckdb"},
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "cube": {"kind": "cube", "dialect": "postgres"},
                "unwired": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "users": {
                    "keys": ["id"],
                    "row_count": 3,
                    "serve": {"duck": "users", "unwired": "users"},
                },
                "stale_metrics": {
                    "keys": ["id"],
                    "row_count": 100,
                    "serve": {
                        "sr": {"name": "lakekeeper.stale_metrics", "max_lag_seconds": 300}
                    },
                },
                "sr_only_events": {
                    "keys": ["id"],
                    "row_count": 1000,
                    "serve": {"sr": "lakekeeper.sr_only_events"},
                },
                "lb_only_accounts": {
                    "keys": ["id"],
                    "row_count": 50,
                    "serve": {"lb": "public.lb_only_accounts"},
                },
                "patients_pii": {
                    "keys": ["id"],
                    "row_count": 50,
                    "serve": {"lb": "public.patients_pii"},
                },
            },
            "cubes": {
                "revenue_cube": {
                    "engine": "cube",
                    "measures": ["total_revenue"],
                    "dimensions": ["region"],
                }
            },
        }
    )
    schemas = {
        "users": ["id", "name", "salary"],
        "stale_metrics": ["id", "val"],
        "sr_only_events": ["id", "event_type"],
        "lb_only_accounts": ["id", "owner"],
        "patients_pii": ["id", "ssn"],
        "revenue_cube": ["region", "total_revenue"],
    }
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-1": pub},
        pii_tables={"patients_pii"},
    )
    duck_adapter = DuckDBAdapter(lambda: parent.cursor())
    try:
        yield priv, pub, pipeline, duck_adapter, parent
    finally:
        parent.close()


def test_code_grant_expired(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    sql = "SELECT id FROM users"
    tok = _make_grant(
        priv, sql, jti="jti-exp-01", iat_offset=-120.0, exp_offset=-30.0
    )
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(resp, "grant_expired", expected_audit_id="jti-exp-01")

    # Also reachable when token passes JWT leeway (e.g. expired 1s ago < 5s leeway) and enforce() checks grant.expires_at
    tok_leeway = _make_grant(
        priv, sql, jti="jti-exp-leeway", iat_offset=-60.0, exp_offset=-1.0
    )
    resp_leeway = client.post(
        "/v1/explain",
        headers={"Authorization": f"Bearer {tok_leeway}"},
        json={"sql": sql},
    )
    _assert_typed_error(
        resp_leeway, "grant_expired", expected_audit_id="jti-exp-leeway"
    )


def test_code_grant_invalid(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))
    sql = "SELECT id FROM users"

    # 1. Missing Authorization header -> no audit_id
    r_missing = client.post("/v1/query", json={"sql": sql})
    _assert_typed_error(r_missing, "grant_invalid", expected_audit_id=None)

    # 2. Malformed token -> no audit_id
    r_malformed = client.post(
        "/v1/query",
        headers={"Authorization": "Bearer not.a.jwt"},
        json={"sql": sql},
    )
    _assert_typed_error(r_malformed, "grant_invalid", expected_audit_id=None)

    # 3. Signed with untrusted key -> no audit_id even if payload carries jti
    rogue_priv = ec.generate_private_key(ec.SECP256R1())
    rogue_tok = _make_grant(rogue_priv, sql, jti="jti-rogue-untrusted")
    r_rogue = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {rogue_tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(r_rogue, "grant_invalid", expected_audit_id=None)

    # 4. Cancel token used on /v1/query -> signature+iss+aud verified, so audit_id IS included
    cancel_tok = sign_cancel_token(
        {
            "typ": "cancel",
            "sub": "analyst@atlas.example",
            "jti": "jti-cancel-misuse",
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": time.time(),
            "exp": time.time() + 30.0,
        },
        priv,
        kid="key-1",
    )
    r_cancel = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {cancel_tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(
        r_cancel, "grant_invalid", expected_audit_id="jti-cancel-misuse"
    )


def test_code_grant_replayed(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    sql = "SELECT id FROM users ORDER BY id"
    tok = _make_grant(priv, sql, jti="jti-replay-typed")
    r1 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    assert r1.status_code == 200

    r2 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(
        r2, "grant_replayed", expected_audit_id="jti-replay-typed"
    )


def test_code_access_denied(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    # Query references ungranted column 'salary'
    sql = "SELECT id, salary FROM users"
    tok = _make_grant(
        priv,
        sql,
        jti="jti-denied-01",
        tables={
            "users": {
                "columns": ["id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(resp, "access_denied", expected_audit_id="jti-denied-01")


def test_code_data_stale(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    sql = "SELECT id, val FROM stale_metrics"
    tok = _make_grant(
        priv,
        sql,
        jti="jti-stale-01",
        tables={
            "stale_metrics": {
                "columns": ["id", "val"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql, "max_staleness_seconds": 30},
    )
    _assert_typed_error(resp, "data_stale", expected_audit_id="jti-stale-01")


def test_code_engine_unavailable(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    # 1. Missing adapter for hinted engine 'unwired'
    sql = "SELECT id FROM users"
    tok = _make_grant(priv, sql, jti="jti-unavail-01")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql, "hint": "unwired"},
    )
    _assert_typed_error(
        resp, "engine_unavailable", expected_audit_id="jti-unavail-01"
    )

    # 2. Adapter raises EnginePoolExhausted -> 503 engine_unavailable
    class PoolExhaustedAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EnginePoolExhausted("connection pool exhausted")

    client_pool = TestClient(create_app(pipeline, {"duck": PoolExhaustedAdapter()}))
    tok2 = _make_grant(priv, sql, jti="jti-unavail-pool")
    resp_pool = client_pool.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok2}"},
        json={"sql": sql},
    )
    _assert_typed_error(
        resp_pool, "engine_unavailable", expected_audit_id="jti-unavail-pool"
    )


def test_code_engine_timeout(multi_engine_setup, caplog):
    priv, _, pipeline, _, _ = multi_engine_setup

    class SlowTimeoutAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EngineTimeout("statement timed out")

    client = TestClient(create_app(pipeline, {"duck": SlowTimeoutAdapter()}))
    sql = "SELECT id FROM users"
    tok = _make_grant(priv, sql, jti="jti-timeout-typed")

    with caplog.at_level(logging.ERROR):
        caplog.clear()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok}"},
            json={"sql": sql},
        )
    _assert_typed_error(
        resp, "engine_timeout", expected_audit_id="jti-timeout-typed"
    )
    assert "code=engine_timeout" in caplog.text
    assert "audit_id=jti-timeout-typed" in caplog.text


def test_code_query_spans_engines(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    # Join non-PII table on 'sr' with non-PII table on 'lb'
    sql = (
        "SELECT e.id, a.owner "
        "FROM sr_only_events e JOIN lb_only_accounts a ON e.id = a.id"
    )
    tok = _make_grant(
        priv,
        sql,
        jti="jti-spans-01",
        tables={
            "sr_only_events": {
                "columns": ["id", "event_type"],
                "masks": {},
                "actions": ["select"],
            },
            "lb_only_accounts": {
                "columns": ["id", "owner"],
                "masks": {},
                "actions": ["select"],
            },
        },
    )
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(
        resp, "query_spans_engines", expected_audit_id="jti-spans-01"
    )

    # Mixing cube and table also produces query_spans_engines
    sql_mix = (
        "SELECT r.region FROM revenue_cube r JOIN users u ON r.region = u.name"
    )
    tok_mix = _make_grant(
        priv,
        sql_mix,
        jti="jti-spans-mix",
        tables={
            "revenue_cube": {
                "columns": ["region", "total_revenue"],
                "masks": {},
                "actions": ["select"],
            },
            "users": {
                "columns": ["id", "name"],
                "masks": {},
                "actions": ["select"],
            },
        },
    )
    resp_mix = client.post(
        "/v1/explain",
        headers={"Authorization": f"Bearer {tok_mix}"},
        json={"sql": sql_mix},
    )
    _assert_typed_error(
        resp_mix, "query_spans_engines", expected_audit_id="jti-spans-mix"
    )


def test_code_pii_cross_engine(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    # Join PII table 'patients_pii' (only on 'lb') with 'sr_only_events' (only on 'sr')
    sql = (
        "SELECT p.id, e.event_type "
        "FROM patients_pii p JOIN sr_only_events e ON p.id = e.id"
    )
    tok = _make_grant(
        priv,
        sql,
        jti="jti-pii-cross-01",
        tables={
            "patients_pii": {
                "columns": ["id"],
                "masks": {"ssn": "redact"},
                "actions": ["select"],
            },
            "sr_only_events": {
                "columns": ["id", "event_type"],
                "masks": {},
                "actions": ["select"],
            },
        },
    )
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(
        resp, "pii_cross_engine", expected_audit_id="jti-pii-cross-01"
    )


def test_code_row_cap_hit_error_and_truncation_notice(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup

    # 1. Default truncation mode: HTTP 200 with truncated=True and notices=["row_cap_hit"]
    client_trunc = TestClient(create_app(pipeline, {"duck": duck_adapter}))
    sql = "SELECT id, name FROM users ORDER BY id"
    tok_trunc = _make_grant(priv, sql, jti="jti-rowcap-notice", max_rows=2)
    r_trunc = client_trunc.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok_trunc}"},
        json={"sql": sql},
    )
    assert r_trunc.status_code == 200
    data_trunc = r_trunc.json()
    assert data_trunc["truncated"] is True
    assert data_trunc["notices"] == ["row_cap_hit"]
    assert len(data_trunc["rows"]) == 2

    # Untruncated query has notices=[]
    tok_full = _make_grant(priv, sql, jti="jti-rowcap-ok", max_rows=10)
    r_full = client_trunc.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok_full}"},
        json={"sql": sql},
    )
    assert r_full.status_code == 200
    assert r_full.json()["truncated"] is False
    assert r_full.json()["notices"] == []

    # 2. Strict error mode (on_row_cap="error"): exceeding row cap returns 422 row_cap_hit
    client_strict = TestClient(
        create_app(pipeline, {"duck": duck_adapter}, on_row_cap="error")
    )
    tok_err = _make_grant(priv, sql, jti="jti-rowcap-err", max_rows=2)
    r_err = client_strict.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok_err}"},
        json={"sql": sql},
    )
    _assert_typed_error(r_err, "row_cap_hit", expected_audit_id="jti-rowcap-err")

    # 3. Invalid non-literal LIMIT in SQL also produces 422 row_cap_hit
    sql_bad_lim = "SELECT id FROM users LIMIT -1"
    tok_bad_lim = _make_grant(priv, sql_bad_lim, jti="jti-rowcap-badlim")
    r_bad_lim = client_trunc.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok_bad_lim}"},
        json={"sql": sql_bad_lim},
    )
    _assert_typed_error(
        r_bad_lim, "row_cap_hit", expected_audit_id="jti-rowcap-badlim"
    )


def test_code_result_too_large(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    sql = "SELECT id, name, salary FROM users ORDER BY id"
    tok = _make_grant(priv, sql, jti="jti-too-large-01")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql, "max_result_bytes": 10},
    )
    body = _assert_typed_error(
        resp, "result_too_large", expected_audit_id="jti-too-large-01"
    )
    assert "rows" not in body


def test_code_invalid_sql(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))

    # 1. Malformed SQL syntax with valid grant token -> 400 invalid_sql with audit_id
    bad_sql = "SELECT FROM WHERE ((( %%%"
    tok = _make_grant(priv, bad_sql, jti="jti-badsql-01")
    r_syntax = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": bad_sql},
    )
    _assert_typed_error(
        r_syntax, "invalid_sql", expected_audit_id="jti-badsql-01"
    )

    # 2. Multi-statement SQL -> 400 invalid_sql with audit_id
    multi_sql = "SELECT id FROM users; SELECT name FROM users"
    tok_multi = _make_grant(priv, multi_sql, jti="jti-badsql-multi")
    r_multi = client.post(
        "/v1/explain",
        headers={"Authorization": f"Bearer {tok_multi}"},
        json={"sql": multi_sql},
    )
    _assert_typed_error(
        r_multi, "invalid_sql", expected_audit_id="jti-badsql-multi"
    )

    # 3. DDL statement -> 400 invalid_sql with audit_id
    ddl_sql = "DROP TABLE users"
    tok_ddl = _make_grant(priv, ddl_sql, jti="jti-badsql-ddl")
    r_ddl = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok_ddl}"},
        json={"sql": ddl_sql},
    )
    _assert_typed_error(r_ddl, "invalid_sql", expected_audit_id="jti-badsql-ddl")

    # 4. Malformed JSON body with unverified token -> 400 invalid_sql without audit_id
    r_json = client.post(
        "/v1/query",
        headers={"Authorization": "Bearer untrusted.token.value"},
        content=b"{not-json",
    )
    _assert_typed_error(r_json, "invalid_sql", expected_audit_id=None)


def test_code_payload_too_large(multi_engine_setup):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup
    client = TestClient(
        create_app(pipeline, {"duck": duck_adapter}, max_body_bytes=64)
    )
    sql = "SELECT id FROM users /* " + ("X" * 200) + " */"
    tok = _make_grant(priv, sql, jti="jti-payload-big")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(resp, "payload_too_large", expected_audit_id=None)


def test_code_query_cancelled(multi_engine_setup):
    priv, _, pipeline, _, _ = multi_engine_setup

    class CancelledAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EngineCancelled("Engine query execution cancelled")

    client = TestClient(create_app(pipeline, {"duck": CancelledAdapter()}))
    sql = "SELECT id FROM users"
    tok = _make_grant(priv, sql, jti="jti-cancelled-01")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(
        resp, "query_cancelled", expected_audit_id="jti-cancelled-01"
    )


def test_code_audit_unavailable_refuses_query_fail_closed(multi_engine_setup):
    priv, _, pipeline, _, _ = multi_engine_setup
    executed_count = 0

    class CountingAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            nonlocal executed_count
            executed_count += 1
            return QueryResult(
                columns=("id",),
                rows=[(1,)],
                truncated=False,
                engine="duck",
                elapsed_ms=1.0,
            )

    def broken_audit_sink(record: Any) -> None:
        raise AuditUnavailableError("audit broker down")

    client = TestClient(
        create_app(
            pipeline,
            {"duck": CountingAdapter()},
            audit_sink=broken_audit_sink,
        )
    )
    sql = "SELECT id FROM users"
    tok = _make_grant(priv, sql, jti="jti-audit-down-01")
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok}"},
        json={"sql": sql},
    )
    _assert_typed_error(
        resp, "audit_unavailable", expected_audit_id="jti-audit-down-01"
    )
    assert executed_count == 0


def test_code_internal_error(multi_engine_setup, caplog):
    priv, _, pipeline, _, _ = multi_engine_setup

    class CrashingAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise RuntimeError("unexpected null pointer in driver C extension")

    client = TestClient(create_app(pipeline, {"duck": CrashingAdapter()}))
    sql = "SELECT id FROM users"
    tok = _make_grant(priv, sql, jti="jti-crash-500")

    with caplog.at_level(logging.ERROR):
        caplog.clear()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok}"},
            json={"sql": sql},
        )
    _assert_typed_error(resp, "internal_error", expected_audit_id="jti-crash-500")
    assert "code=internal_error" in caplog.text
    assert "audit_id=jti-crash-500" in caplog.text


def test_additional_http_codes_not_found_and_duplicate_query_id(multi_engine_setup):
    priv, _, pipeline, _, _ = multi_engine_setup

    started = threading.Event()
    release = threading.Event()

    class HoldAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            started.set()
            release.wait(timeout=5.0)
            return QueryResult(
                columns=("id",),
                rows=[(1,)],
                truncated=False,
                engine="duck",
                elapsed_ms=1.0,
            )

    # 1. 404 not_found on /v1/cancel
    client = TestClient(create_app(pipeline, {"duck": HoldAdapter()}))
    cancel_tok = sign_cancel_token(
        {
            "typ": "cancel",
            "sub": "analyst@atlas.example",
            "jti": "jti-missing-999",
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": time.time(),
            "exp": time.time() + 30.0,
        },
        priv,
        kid="key-1",
    )
    r_404 = client.post(
        "/v1/cancel", headers={"Authorization": f"Bearer {cancel_tok}"}
    )
    _assert_typed_error(r_404, "not_found", expected_audit_id="jti-missing-999")

    # 2. 409 duplicate_query_id when RunningQueries already holds the jti
    # Use a replay cache stub that permits testing the concurrent duplicate guard
    class NoopReplayCache(GrantReplayCache):
        def check_and_record(self, jti: str, expires_at: float, now: float | None = None) -> None:
            return None

    client_dup = TestClient(
        create_app(
            pipeline,
            {"duck": HoldAdapter()},
            replay_cache=NoopReplayCache(),
        )
    )
    sql = "SELECT id FROM users"
    tok_dup = _make_grant(priv, sql, jti="jti-dup-409")

    t = threading.Thread(
        target=lambda: client_dup.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_dup}"},
            json={"sql": sql},
        )
    )
    t.start()
    assert started.wait(timeout=2.0)
    try:
        r_409 = client_dup.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_dup}"},
            json={"sql": sql},
        )
        _assert_typed_error(
            r_409, "duplicate_query_id", expected_audit_id="jti-dup-409"
        )
    finally:
        release.set()
        t.join(timeout=3.0)


# ---------------------------------------------------------------------------
# 3. Zero-leakage guarantees across all error paths
# ---------------------------------------------------------------------------


def test_planted_secrets_dsns_hostnames_literals_params_and_tracebacks_never_leak(
    multi_engine_setup,
):
    priv, _, pipeline, _, _ = multi_engine_setup

    sentinel_sql_lit = "SENTINEL_SQL_LITERAL_847291"
    sentinel_param = "SENTINEL_PARAM_VALUE_938172"
    sentinel_dsn = "postgresql://svc_user:SENTINEL_DB_PASS_112233@db-prod-01.internal.corp:5432/lakebase"
    sentinel_hostname = "db-prod-01.internal.corp"
    sentinel_driver = "psycopg2.OperationalError"
    sentinel_audit = "kafka://audit:SENTINEL_AUDIT_778899@broker-01.internal.corp:9092"
    sentinel_internal = "SENTINEL_TRACEBACK_SECRET_556677"

    forbidden_substrings = (
        sentinel_sql_lit,
        sentinel_param,
        sentinel_dsn,
        "SENTINEL_DB_PASS_112233",
        sentinel_hostname,
        "internal.corp",
        sentinel_driver,
        sentinel_audit,
        "SENTINEL_AUDIT_778899",
        sentinel_internal,
        "Traceback",
    )

    def assert_clean(resp: Any, tok: str) -> None:
        text = resp.text
        assert tok not in text
        for secret in forbidden_substrings:
            assert secret not in text, f"Leaked '{secret}' in response: {text}"
        body = resp.json()
        assert set(body.keys()) <= {"error", "code", "message", "next_step", "audit_id"}
        assert body["code"] in ERROR_CATALOGUE

    # Case A: EngineError carrying raw driver text, DSN, password, and hostname
    class LeakyEngineErrorAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EngineError(f"{sentinel_driver}: connection failed for {sentinel_dsn}")

    sql = f"SELECT id FROM users WHERE name = '{sentinel_sql_lit}' AND salary > $1"
    c1 = TestClient(create_app(pipeline, {"duck": LeakyEngineErrorAdapter()}))
    t1 = _make_grant(priv, sql, jti="jti-leak-eng")
    r1 = c1.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t1}"},
        json={"sql": sql, "params": [sentinel_param]},
    )
    assert r1.status_code == 502
    assert_clean(r1, t1)

    # Case B: EngineTimeout carrying hostname and SQL
    class LeakyTimeoutAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EngineTimeout(f"timeout on {sentinel_hostname} running {sql}")

    c2 = TestClient(create_app(pipeline, {"duck": LeakyTimeoutAdapter()}))
    t2 = _make_grant(priv, sql, jti="jti-leak-timeout")
    r2 = c2.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t2}"},
        json={"sql": sql, "params": [sentinel_param]},
    )
    assert r2.status_code == 504
    assert_clean(r2, t2)

    # Case C: Unhandled RuntimeError with internal secret & stack trace
    class LeakyCrashAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise RuntimeError(f"Crash {sentinel_internal} at {sentinel_dsn}")

    c3 = TestClient(create_app(pipeline, {"duck": LeakyCrashAdapter()}))
    t3 = _make_grant(priv, sql, jti="jti-leak-crash")
    r3 = c3.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t3}"},
        json={"sql": sql, "params": [sentinel_param]},
    )
    assert r3.status_code == 500
    assert_clean(r3, t3)

    # Case D: Broken audit sink leaking broker credentials
    def leaky_sink(rec: Any) -> None:
        raise RuntimeError(f"Failed connecting to {sentinel_audit}")

    c4 = TestClient(
        create_app(pipeline, {"duck": LeakyCrashAdapter()}, audit_sink=leaky_sink)
    )
    t4 = _make_grant(priv, sql, jti="jti-leak-audit")
    r4 = c4.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t4}"},
        json={"sql": sql, "params": [sentinel_param]},
    )
    assert r4.status_code == 503
    assert_clean(r4, t4)

    # Case E: SQL parse error containing secret literal and secret param
    bad_sql = f"SELECT id FROM users WHERE name = '{sentinel_sql_lit}' AND ((("
    t5 = _make_grant(priv, bad_sql, jti="jti-leak-parse")
    r5 = c1.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t5}"},
        json={"sql": bad_sql, "params": [sentinel_param]},
    )
    assert r5.status_code == 400
    assert_clean(r5, t5)

    # Case F: Access denied on ungranted column with secret literal in WHERE
    denied_sql = f"SELECT id, secret_col FROM users WHERE name = '{sentinel_sql_lit}'"
    t6 = _make_grant(priv, denied_sql, jti="jti-leak-denied")
    r6 = c1.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t6}"},
        json={"sql": denied_sql, "params": [sentinel_param]},
    )
    assert r6.status_code == 403
    assert_clean(r6, t6)

    # Case G: Grant token with secret planted in kid / iss
    t7 = _make_grant(
        priv,
        sql,
        jti="jti-leak-kid",
        kid=sentinel_dsn,
        extra_claims={"iss": sentinel_hostname},
    )
    r7 = c1.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {t7}"},
        json={"sql": sql, "params": [sentinel_param]},
    )
    assert r7.status_code == 401
    assert_clean(r7, t7)


# ---------------------------------------------------------------------------
# 4. F-L2: Scrubbed exception detail + audit_id logged at ERROR, never in body
# ---------------------------------------------------------------------------


def test_scrub_error_detail_unit_redacts_all_secret_classes_and_preserves_detail():
    jwt_tok = (
        "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiJhbmFseXN0QGV4YW1wbGUuY29tIn0."
        "c2lnbmF0dXJlX3NlY3JldF94eXot"
    )
    sql = "SELECT id FROM users WHERE name = 'vip_customer_secret_42' AND note = 'O''Brian_77'"
    env = {
        "STARROCKS_SECRET_TOKEN": "env_tok_998877",
        "QUOTE_PASSWORD": "pw'with'quote",
        "LAKEBASE_DSN": "postgresql://svc_user:dsn_extracted_pw_4433@db.internal:5432/prod",
        "SAFE_SHORT_KEY": "abc",  # len < 4 ignored
        "NON_SENSITIVE_VAR": "orders_mart_v2",  # non-sensitive name preserved
    }
    exc = EngineError(
        "backend table orders_mart_v2 missing partition 202611 | "
        f"grant=raw_grant_tok_1122 | jwt={jwt_tok} | auth=Bearer opq_tok_5566== | "
        "uri=postgresql://admin:uri_pass_1234@sr-fe:9030/db | "
        "kv=password=kv_secret_7890 api_key=ak_live_3344 | "
        "env1=env_tok_998877 env2=pw''with''quote env3=dsn_extracted_pw_4433 | "
        "lit='vip_customer_secret_42' esc=O'Brian_77 param=param_secret_6655 quoted_num='987654' dur=61.0s | "
        "probe=my_canary_token_88\r\nnext_line_diag"
    )
    scrubbed = scrub_error_detail(
        exc,
        token="raw_grant_tok_1122",
        sql=sql,
        params=["param_secret_6655", 987654, 0, 2],
        env=env,
    )

    assert scrubbed.startswith("EngineError: ")
    assert "backend table orders_mart_v2 missing partition 202611" in scrubbed
    assert "dur=61.0s" in scrubbed
    assert "\\r\\nnext_line_diag" in scrubbed
    assert "\n" not in scrubbed
    assert "\r" not in scrubbed

    for forbidden in (
        "raw_grant_tok_1122",
        jwt_tok,
        "c2lnbmF0dXJlX3NlY3JldF94eXot",
        "opq_tok_5566",
        "uri_pass_1234",
        "kv_secret_7890",
        "ak_live_3344",
        "env_tok_998877",
        "pw'with'quote",
        "pw''with''quote",
        "dsn_extracted_pw_4433",
        "vip_customer_secret_42",
        "O'Brian_77",
        "O''Brian_77",
        "param_secret_6655",
        "'987654'",
        "my_canary_token_88",
    ):
        assert forbidden not in scrubbed, f"Secret '{forbidden}' leaked in: {scrubbed}"


def test_f_l2_error_logs_include_scrubbed_exc_detail_and_audit_id(
    multi_engine_setup,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup

    useful_detail = "backend table orders_mart_v2 missing partition 202611"
    planted_env_secret = "planted_env_s3cr3t_994411"
    planted_dsn_env_pw = "dsn_env_pw_553311"
    planted_dsn_inline_pw = "dsn_inline_pw_882244"
    planted_kv_pw = "kv_pw_665544"
    planted_jwt = (
        "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9."
        "eyJzdWIiOiJsZWFrZWQtdXNlciIsImp0aSI6ImxrazEifQ."
        "c2lnbmF0dXJlX3NlY3JldF8xMjM-"
    )
    planted_bearer_val = "opaque_bearer_tok_334455"
    planted_sql_lit = "vip_sql_literal_445566"
    planted_param = "secret_param_val_778899"
    planted_canary = "canary_probe_token_1199"

    monkeypatch.setenv("STARROCKS_SECRET_KEY", planted_env_secret)
    monkeypatch.setenv(
        "LAKEBASE_DSN",
        f"postgresql://svc_acct:{planted_dsn_env_pw}@db.internal:5432/prod",
    )

    inline_dsn = (
        f"postgresql://lake_user:{planted_dsn_inline_pw}@sr-fe.internal:9030/analytics"
    )
    sql = f"SELECT id FROM users WHERE name = '{planted_sql_lit}' AND name != $1"
    params = [planted_param]

    rich_secret_suffix = (
        f"dsn={inline_dsn} password={planted_kv_pw} env={planted_env_secret} "
        f"dsn_env_pw={planted_dsn_env_pw} jwt={planted_jwt} "
        f"auth=Bearer {planted_bearer_val} lit='{planted_sql_lit}' "
        f"param={planted_param} probe={planted_canary}\nline2_ctx"
    )

    forbidden_secrets = (
        planted_env_secret,
        planted_dsn_env_pw,
        planted_dsn_inline_pw,
        planted_kv_pw,
        planted_jwt,
        "c2lnbmF0dXJlX3NlY3JldF8xMjM-",
        planted_bearer_val,
        planted_sql_lit,
        planted_param,
        planted_canary,
    )

    def _assert_logged_and_scrubbed(
        resp: Any,
        tok: str,
        *,
        expected_audit_id: str,
        expected_exc_cls: str,
        expected_code: str,
    ) -> None:
        # 1. Response never contains detail, useful backend message, or any secret
        body = resp.json()
        assert "detail" not in body
        assert useful_detail not in resp.text
        assert tok not in resp.text
        for secret in forbidden_secrets:
            assert secret not in resp.text, (
                f"Leaked '{secret}' in HTTP response: {resp.text}"
            )

        # 2. ERROR log record contains audit_id, exception class name, code, and useful detail
        error_records = [
            rec
            for rec in caplog.records
            if rec.levelno == logging.ERROR and rec.name == "seeknal.serving.http"
        ]
        assert error_records, f"Expected ERROR log record, got: {caplog.text}"
        matching = [
            rec.getMessage()
            for rec in error_records
            if f"audit_id={expected_audit_id}" in rec.getMessage()
        ]
        assert matching, (
            f"Expected audit_id={expected_audit_id} in ERROR logs, got: "
            f"{[r.getMessage() for r in error_records]}"
        )
        msg = matching[0]
        assert f"code={expected_code}" in msg
        assert f"error={expected_exc_cls}" in msg
        assert f"detail={expected_exc_cls}:" in msg
        assert useful_detail in msg
        assert "\n" not in msg
        assert "\r" not in msg

        # 3. Planted secrets, DSN passwords, JWT, SQL literal, param, and grant token absent from logs
        assert tok not in caplog.text
        for secret in forbidden_secrets:
            assert secret not in caplog.text, (
                f"Leaked '{secret}' in server logs: {caplog.text}"
            )

    # Case 1: EngineError in /v1/query
    class DetailedEngineErrorAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EngineError(f"{useful_detail}; {rich_secret_suffix}")

    c_eng = TestClient(create_app(pipeline, {"duck": DetailedEngineErrorAdapter()}))
    tok_eng = _make_grant(priv, sql, jti="jti-fl2-eng-err")
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        r_eng = c_eng.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_eng}"},
            json={"sql": sql, "params": params},
        )
    assert r_eng.status_code == 502
    _assert_logged_and_scrubbed(
        r_eng,
        tok_eng,
        expected_audit_id="jti-fl2-eng-err",
        expected_exc_cls="EngineError",
        expected_code="engine_unavailable",
    )

    # Case 2: EngineConfigError in /v1/query
    class DetailedConfigErrorAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise EngineConfigError(f"{useful_detail}; {rich_secret_suffix}")

    c_cfg = TestClient(create_app(pipeline, {"duck": DetailedConfigErrorAdapter()}))
    tok_cfg = _make_grant(priv, sql, jti="jti-fl2-cfg-err")
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        r_cfg = c_cfg.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_cfg}"},
            json={"sql": sql, "params": params},
        )
    assert r_cfg.status_code == 503
    _assert_logged_and_scrubbed(
        r_cfg,
        tok_cfg,
        expected_audit_id="jti-fl2-cfg-err",
        expected_exc_cls="EngineConfigError",
        expected_code="engine_unavailable",
    )

    # Case 3a: RoutingError in prepare branch
    orig_prepare = pipeline.prepare

    def raising_routing_prepare(*args: Any, **kwargs: Any) -> PreparedQuery:
        raise RoutingError(f"{useful_detail}; {rich_secret_suffix}", code="data_stale")

    monkeypatch.setattr(pipeline, "prepare", raising_routing_prepare)
    c_prep = TestClient(create_app(pipeline, {"duck": duck_adapter}))
    tok_route = _make_grant(priv, sql, jti="jti-fl2-route-err")
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        r_route = c_prep.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_route}"},
            json={"sql": sql, "params": params},
        )
    assert r_route.status_code == 422
    _assert_logged_and_scrubbed(
        r_route,
        tok_route,
        expected_audit_id="jti-fl2-route-err",
        expected_exc_cls="RoutingError",
        expected_code="data_stale",
    )

    # Case 3b: EnforcementError in prepare branch
    def raising_enforcement_prepare(*args: Any, **kwargs: Any) -> PreparedQuery:
        raise EnforcementError(
            f"{useful_detail}; {rich_secret_suffix}", code="access_denied"
        )

    monkeypatch.setattr(pipeline, "prepare", raising_enforcement_prepare)
    tok_enf = _make_grant(priv, sql, jti="jti-fl2-enforce-err")
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        r_enf = c_prep.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {tok_enf}"},
            json={"sql": sql, "params": params},
        )
    assert r_enf.status_code == 403
    _assert_logged_and_scrubbed(
        r_enf,
        tok_enf,
        expected_audit_id="jti-fl2-enforce-err",
        expected_exc_cls="EnforcementError",
        expected_code="access_denied",
    )
    monkeypatch.setattr(pipeline, "prepare", orig_prepare)

    # Case 4: Unhandled RuntimeError in /v1/query
    class DetailedRuntimeErrorAdapter:
        def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
            raise RuntimeError(f"{useful_detail}; {rich_secret_suffix}")

    c_rt = TestClient(create_app(pipeline, {"duck": DetailedRuntimeErrorAdapter()}))
    tok_rt = _make_grant(priv, sql, jti="jti-fl2-runtime-err")
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        r_rt = c_rt.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_rt}"},
            json={"sql": sql, "params": params},
        )
    assert r_rt.status_code == 500
    _assert_logged_and_scrubbed(
        r_rt,
        tok_rt,
        expected_audit_id="jti-fl2-runtime-err",
        expected_exc_cls="RuntimeError",
        expected_code="internal_error",
    )

    # Case 5: Audit sink failure in _finish & cancel GrantTokenError
    class BrokenStructuredAuditSink(AuditSink):
        on_failure = "refuse"

        def verify_writable(self) -> None:
            pass

        def write_record(self, rec: Any) -> None:
            raise AuditWriteError(f"{useful_detail}; {rich_secret_suffix}")

        def close(self) -> None:
            pass

    c_aud = TestClient(
        create_app(
            pipeline,
            {"duck": duck_adapter},
            audit_sink=BrokenStructuredAuditSink(),
        )
    )
    tok_aud = _make_grant(priv, sql, jti="jti-fl2-audit-err")
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        r_aud = c_aud.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {tok_aud}"},
            json={"sql": sql, "params": params},
        )
    assert r_aud.status_code == 503
    _assert_logged_and_scrubbed(
        r_aud,
        tok_aud,
        expected_audit_id="jti-fl2-audit-err",
        expected_exc_cls="AuditWriteError",
        expected_code="audit_unavailable",
    )

    # Cancel endpoint GrantTokenError (expired cancel token carries verified audit_id)
    tok_bad_cancel = sign_cancel_token(
        {
            "typ": "cancel",
            "sub": "analyst@atlas.example",
            "jti": "jti-fl2-cancel-err",
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": time.time() - 90.0,
            "exp": time.time() - 45.0,
        },
        priv,
        kid="key-1",
    )
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        r_cancel_err = c_eng.post(
            "/v1/cancel",
            headers={"Authorization": f"Bearer {tok_bad_cancel}"},
        )
    assert r_cancel_err.status_code == 401
    assert "detail" not in r_cancel_err.json()
    assert tok_bad_cancel not in caplog.text
    assert any(
        rec.levelno == logging.ERROR
        and "code=grant_expired" in rec.getMessage()
        and "error=GrantExpiredError" in rec.getMessage()
        and "detail=GrantExpiredError:" in rec.getMessage()
        and "audit_id=jti-fl2-cancel-err" in rec.getMessage()
        for rec in caplog.records
    )


def test_c_n2_log_forging_control_chars_length_cap_and_forged_kid(
    multi_engine_setup,
    caplog: pytest.LogCaptureFixture,
):
    priv, _, pipeline, duck_adapter, _ = multi_engine_setup

    # 1. _sanitize_log_value is exported from errors.py and re-exported in http.py
    assert http_sanitize_log_value is _sanitize_log_value

    raw_controls = (
        "head\r\n\t\x1b[2J\x85\x0b\x0c\x00\x1f\x7f\x9f\u2028\u2029\u200btail"
    )
    sanitized = _sanitize_log_value(raw_controls)
    assert "\\r\\n\\t\\x1b[2J\\x85\\x0b\\x0c\\x00\\x1f\\x7f\\x9f\\u2028\\u2029\\u200b" in sanitized
    assert all(ch.isprintable() for ch in sanitized)

    # Verify every 0x00..0x1f and 0x7f..0x9f byte is escaped
    all_ctrl = "".join(chr(c) for c in [*range(0x00, 0x20), *range(0x7F, 0xA0)])
    escaped_all = _sanitize_log_value(all_ctrl)
    assert all(ch.isprintable() for ch in escaped_all)
    for c in [*range(0x00, 0x20), *range(0x7F, 0xA0)]:
        assert chr(c) not in escaped_all

    # 2. scrub_error_detail escapes non-printables and caps length at 2048 chars
    huge_forged_exc = RuntimeError(
        "prefix\x1b[31m\x85\x0b\x0c\u2028" + ("X" * 5000)
    )
    scrubbed_huge = scrub_error_detail(huge_forged_exc)
    assert len(scrubbed_huge) == 2048
    assert all(ch.isprintable() for ch in scrubbed_huge)
    assert "\\x1b[31m\\x85\\x0b\\x0c\\u2028" in scrubbed_huge

    # 3. Forged kid with ANSI escape, C1 NEL, VT, and fake audit_id on /v1/query and /v1/cancel
    forged_kid = "k\x1b[2J FAKE audit_id=root\x85x\x0bY"
    sql = "SELECT id FROM users"
    bad_grant = _make_grant(priv, sql, jti="jti-unverified", kid=forged_kid)
    bad_cancel = sign_cancel_token(
        {
            "typ": "cancel",
            "sub": "analyst@atlas.example",
            "jti": "jti-unverified-cancel",
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": time.time(),
            "exp": time.time() + 30.0,
        },
        priv,
        kid=forged_kid,
    )

    client = TestClient(create_app(pipeline, {"duck": duck_adapter}))
    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        rq = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {bad_grant}"},
            json={"sql": sql},
        )
        rc = client.post(
            "/v1/cancel",
            headers={"Authorization": f"Bearer {bad_cancel}"},
        )

    assert rq.status_code == 401
    assert rc.status_code == 401
    assert "FAKE" not in rq.text
    assert "FAKE" not in rc.text
    assert "FAKE" not in caplog.text
    assert "audit_id=root" not in caplog.text
    for rec in caplog.records:
        msg = rec.getMessage()
        assert all(ch.isprintable() for ch in msg)


def test_c_n3_scrubber_escaped_quotes_all_keys_duckdb_secret_slash_userinfo_and_config_secrets(
    tmp_path: pathlib.Path,
):
    import json
    import os
    from cryptography.hazmat.primitives import serialization
    from seeknal.serving.server.app import build_server
    from seeknal.serving.server.config import ServerConfig, read_secret_file

    clear_registered_config_secrets()
    try:
        # 1. Quoted values with '', "", \', \" and (?<![A-Za-z0-9]) prefix
        quoted_msg = (
            "p1: password='ab''cd1234' "
            'p2: password="ef""gh5678" '
            r"p3: password='ij\'kl9012' "
            r'p4: password="mn\"op3456" '
            "p5: _password='under_pw_1111' "
            "p6: db_pwd=bare_pwd_2222 "
            "p7: my_sslpassword: 'ssl_pw''3333'"
        )
        scrubbed_q = scrub_error_detail(quoted_msg, env={})
        for leaked in (
            "ab",
            "cd1234",
            "ef",
            "gh5678",
            "ij",
            "kl9012",
            "mn",
            "op3456",
            "under_pw_1111",
            "bare_pwd_2222",
            "ssl_pw",
            "3333",
        ):
            assert leaked not in scrubbed_q, f"Leaked '{leaked}' in: {scrubbed_q}"

        # 2. Every required key name in both '=' and ':' forms, plus _DSN_KV_EXTRACT_RE
        all_keys = (
            "pwd",
            "sslpassword",
            "aws_secret_access_key",
            "aws_access_key_id",
            "access_key",
            "secret_key",
            "key_id",
            "session_token",
            "password",
            "passwd",
            "secret",
            "token",
            "client_secret",
            "api_key",
            "apikey",
            "credential",
            "credentials",
            "private_key",
        )
        for idx, key_name in enumerate(all_keys):
            sec_eq = f"val_eq_{idx}_s3cr3t"
            sec_col = f"val_col_{idx}_s3cr3t"
            sec_env_kv = f"val_env_{idx}''s3cr3t"
            raw = (
                f"_{key_name}='{sec_eq}' | {key_name}: \"{sec_col}\" | "
                f"leaked_from_env=val_env_{idx}'s3cr3t"
            )
            out = scrub_error_detail(
                raw,
                env={"DB_CREDENTIAL_BLOB": f"{key_name}='{sec_env_kv}'"},
            )
            assert sec_eq not in out, f"Failed '=' scrub for {key_name}: {out}"
            assert sec_col not in out, f"Failed ':' scrub for {key_name}: {out}"
            assert f"val_env_{idx}" not in out, (
                f"Failed env KV extract for {key_name}: {out}"
            )

        # 3. DuckDB SECRET '...' form (including '' and \' escapes and _SECRET)
        duck_msg = (
            "Binder Error: CREATE SECRET 'duck_s3_top_secret' failed; "
            r"also _secret 'esc''quote\'val_77' and SECRET 'x'"
        )
        scrubbed_duck = scrub_error_detail(duck_msg, env={})
        assert "SECRET '[REDACTED]'" in scrubbed_duck
        assert "duck_s3_top_secret" not in scrubbed_duck
        assert "esc" not in scrubbed_duck
        assert "val_77" not in scrubbed_duck
        assert "SECRET 'x'" not in scrubbed_duck

        # 4. URL userinfo containing '/' (both inline and extracted password)
        url_msg = (
            "connect failed for postgresql://u:p/ss_inline@other-host.internal:5432/db "
            "and echoed raw env pwd p/ss_9988"
        )
        scrubbed_url = scrub_error_detail(
            url_msg,
            env={"PRIMARY_DSN": "postgresql://u:p/ss_9988@host.internal:5432/db"},
        )
        assert "postgresql://[REDACTED]@other-host.internal:5432/db" in scrubbed_url
        assert "u:p/ss" not in scrubbed_url
        assert "p/ss_inline" not in scrubbed_url
        assert "p/ss_9988" not in scrubbed_url

        # Also bare p/ss without env still redacts userinfo inline
        assert (
            scrub_error_detail("err postgresql://u:p/ss@host", env={})
            == "err postgresql://[REDACTED]@host"
        )

        # 5. extra_secrets parameter on scrub_error_detail
        extra_out = scrub_error_detail(
            "failed auth using custom_extra'secret_42 and custom_extra''secret_42",
            env={},
            extra_secrets=["custom_extra'secret_42"],
        )
        assert "custom_extra" not in extra_out
        assert "secret_42" not in extra_out

        # 6. Secrets from ServerConfig / YAML / harmless env var names / secret files / build_server
        priv = ec.generate_private_key(ec.SECP256R1())
        pub_pem = (
            priv.public_key()
            .public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            .decode("ascii")
        )
        pem_path = tmp_path / "pub.pem"
        pem_path.write_text(pub_pem, encoding="utf-8")

        pw_file = tmp_path / "sr_pw.txt"
        pw_file.write_text("file_secret'pw_5566\n", encoding="utf-8")
        os.chmod(pw_file, 0o600)

        env_file = tmp_path / "secrets.env"
        env_file.write_text("HARMLESS_OPS_VAR=ops_envfile_val_3344\n", encoding="utf-8")
        os.chmod(env_file, 0o600)

        pl_file = tmp_path / "placement.yaml"
        pl_file.write_text(
            "engines:\n"
            "  sr: {kind: starrocks, dialect: starrocks}\n"
            "  lb: {kind: lakebase, dialect: postgres}\n"
            "tables:\n"
            "  t:\n"
            "    keys: [id]\n"
            "    row_count: 1\n"
            "    serve:\n"
            "      sr: t\n"
            "cubes: {}\n",
            encoding="utf-8",
        )
        sc_file = tmp_path / "schemas.json"
        sc_file.write_text(json.dumps({"t": ["id"]}), encoding="utf-8")

        harmless_env = {
            # Notice NONE of these env var names match _SENSITIVE_ENV_NAME_RE!
            "HARMLESS_LB_CONN": "postgresql://app_u:lb_slash/pw'8877@lb-host:5432/db",
            "HARMLESS_SR_BI_INFO": "sr_bi_harmless_val'1122",
        }

        cfg_dict = {
            "grant_keys": {"pem_files": {"k1": str(pem_path)}},
            "secrets_env_file": str(env_file),
            "placement": {
                "static": {
                    "placement_file": str(pl_file),
                    "schemas_file": str(sc_file),
                }
            },
            "connections": {
                "sr": {
                    "kind": "starrocks",
                    "accounts": {
                        "interactive": {
                            "host": "127.0.0.1",
                            "port": 9030,
                            "user": "sr_u",
                            "password_file": str(pw_file),
                        },
                        "bi": {
                            "host": "127.0.0.1",
                            "port": 9030,
                            "user": "sr_bi",
                            "password_env": "HARMLESS_SR_BI_INFO",
                        },
                    },
                },
                "lb": {
                    "kind": "lakebase",
                    "dsn_env": "HARMLESS_LB_CONN",
                },
            },
            "ops": {
                "enabled": False,
                "auth_token_env": "HARMLESS_OPS_VAR",
            },
        }

        build_server(
            cfg_dict,
            env=harmless_env,
            connect_factories={"sr": lambda: None, "lb": lambda: None},
        )

        # Also verify direct read_secret_file & ServerConfig.get_env registration
        direct_sf = tmp_path / "direct_sec.txt"
        direct_sf.write_text("direct_file_val_9090\n", encoding="utf-8")
        os.chmod(direct_sf, 0o600)
        assert read_secret_file(str(direct_sf)) == "direct_file_val_9090"

        cfg_obj = ServerConfig.from_dict(cfg_dict, env=harmless_env)
        assert (
            cfg_obj.get_env("HARMLESS_PLAIN_LATE", {"HARMLESS_PLAIN_LATE": "late_cfg_val_7070"})
            == "late_cfg_val_7070"
        )

        # Now call scrub_error_detail with empty env={}: ALL config/file/harmless-env secrets
        # (and their '', \', and URL-userinfo password forms) MUST be scrubbed!
        config_leak_probe = (
            "err1=file_secret'pw_5566 err1_dq=file_secret''pw_5566 err1_bs=file_secret\\'pw_5566 "
            "err2=ops_envfile_val_3344 "
            "err3=lb_slash/pw'8877 err3_dq=lb_slash/pw''8877 err3_bs=lb_slash/pw\\'8877 "
            "err4=sr_bi_harmless_val'1122 err4_dq=sr_bi_harmless_val''1122 "
            "err5=direct_file_val_9090 err6=late_cfg_val_7070"
        )
        scrubbed_cfg = scrub_error_detail(config_leak_probe, env={})
        for secret_frag in (
            "file_secret",
            "pw_5566",
            "ops_envfile_val_3344",
            "lb_slash/pw",
            "8877",
            "sr_bi_harmless_val",
            "1122",
            "direct_file_val_9090",
            "late_cfg_val_7070",
        ):
            assert secret_frag not in scrubbed_cfg, (
                f"Config secret '{secret_frag}' leaked in: {scrubbed_cfg}"
            )
    finally:
        clear_registered_config_secrets()


def test_redaction_precision_preserves_61_0s_with_params_zero_and_redacts_param_secret(
    ec_keys,
    caplog: pytest.LogCaptureFixture,
):
    priv, pub = ec_keys
    parent = duckdb.connect()
    parent.execute("CREATE TABLE users (id INT, name VARCHAR)")
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
            schemas={"users": ["id", "name"]},
            public_keys={"key-1": pub},
            max_grant_lifetime_seconds=60.0,
        )
        client = TestClient(
            create_app(pipeline, {"duck": DuckDBAdapter(lambda: parent.cursor())})
        )

        sql = "SELECT id FROM users WHERE id >= $1 AND name != $2"
        tok_61s = _make_grant(
            priv,
            sql,
            jti="jti-61s-precision",
            iat_offset=0.0,
            exp_offset=61.0,
            tables={"users": {"columns": ["id"], "masks": {}, "actions": ["select"]}},
        )

        with caplog.at_level(logging.ERROR):
            caplog.clear()
            resp = client.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok_61s}"},
                json={"sql": sql, "params": [0, "PARAM_SECRET_VALUE"]},
            )

        assert resp.status_code == 401
        body = resp.json()
        assert "61.0s" in body["message"]
        assert "60.0s" in body["message"]
        assert "PARAM_SECRET_VALUE" not in resp.text
        assert "61.0s" in caplog.text
        assert "PARAM_SECRET_VALUE" not in caplog.text

        # Unit check on scrub_error_detail: exact quoted '0' IS redacted, bare 61.0s survives,
        # and PARAM_SECRET_VALUE is redacted.
        detail = scrub_error_detail(
            "Token lifetime of 61.0s exceeds maximum 60.0s; bound='0'; p=PARAM_SECRET_VALUE; s='SQL_LIT_SECRET'",
            sql="SELECT id FROM users WHERE name = 'SQL_LIT_SECRET'",
            params=[0, "PARAM_SECRET_VALUE"],
            env={},
        )
        assert "61.0s" in detail
        assert "60.0s" in detail
        assert "'0'" not in detail
        assert "PARAM_SECRET_VALUE" not in detail
        assert "SQL_LIT_SECRET" not in detail
    finally:
        parent.close()


def test_n1_scrubber_linear_time_no_catastrophic_backtracking_and_8kib_cap():
    # Unclosed single/double quotes with runs of backslashes across all quoted regexes
    for n in (32, 64, 1000):
        payload_sq = "x token: '" + ("\\" * n)
        t0 = time.perf_counter()
        out_sq = scrub_error_detail(payload_sq, env={})
        elapsed_sq = time.perf_counter() - t0
        assert elapsed_sq < 0.05, f"n={n} single-quote took {elapsed_sq:.4f}s"
        assert out_sq.startswith("x token: '")

        payload_dq = 'x password="' + ("\\" * n)
        t0 = time.perf_counter()
        scrub_error_detail(
            payload_dq,
            env={"DB_SECRET_CFG": "password='" + ("\\" * n)},
        )
        elapsed_dq = time.perf_counter() - t0
        assert elapsed_dq < 0.05, f"n={n} double-quote/env took {elapsed_dq:.4f}s"

        payload_duck = "CREATE SECRET '" + ("\\" * n)
        t0 = time.perf_counter()
        scrub_error_detail(payload_duck, env={})
        elapsed_duck = time.perf_counter() - t0
        assert elapsed_duck < 0.05, f"n={n} duckdb secret took {elapsed_duck:.4f}s"

    # 24 KB of '://' must scrub in linear time (< 50 ms)
    uri_flood = "://" * 8192
    t0 = time.perf_counter()
    scrub_error_detail(uri_flood, env={"BROKEN_DSN": uri_flood})
    elapsed_uri = time.perf_counter() - t0
    assert elapsed_uri < 0.05, f"24 KB '://' took {elapsed_uri:.4f}s"

    # N6: Quadratic canary/sentinel & delimiter-restart adversarial inputs (< 50 ms each)
    from seeknal.serving.audit import _safe_diag_target, _scrub_secret_patterns
    from seeknal.serving.startup_validation import _scrub as startup_scrub

    n6_payloads = (
        ("a.*4096", "a." * 4096),
        ("a-*4096", "a-" * 4096),
        ("eyJ+.a*4096", "eyJ" + ".a" * 4096),
        ("canar.*1400", "canar." * 1400),
        ("a.*12000", "a." * 12000),
        ("eyJ+.a*10000", "eyJ" + ".a" * 10000),
    )
    for label, payload in n6_payloads:
        t0 = time.perf_counter()
        scrub_error_detail(payload, env={})
        elapsed = time.perf_counter() - t0
        assert elapsed < 0.05, f"scrub_error_detail({label}) took {elapsed:.4f}s"

        t0 = time.perf_counter()
        startup_scrub(payload)
        elapsed_sv = time.perf_counter() - t0
        assert elapsed_sv < 0.05, f"startup_scrub({label}) took {elapsed_sv:.4f}s"

        t0 = time.perf_counter()
        _scrub_secret_patterns(payload)
        _safe_diag_target(payload)
        elapsed_aud = time.perf_counter() - t0
        assert elapsed_aud < 0.05, f"audit scrub({label}) took {elapsed_aud:.4f}s"

    # Also verify canary/sentinel/opaque redaction still works and stays < 50 ms when keyword is present
    t0 = time.perf_counter()
    out_canary = scrub_error_detail(("a." * 3500) + " my_canary_tok.1 and sentinel-val_2", env={})
    assert (time.perf_counter() - t0) < 0.05
    assert "my_canary_tok" not in out_canary
    assert "sentinel-val_2" not in out_canary

    t0 = time.perf_counter()
    out_sv_canary = startup_scrub(("a." * 3500) + " my_opaque_tok.1 and canary-val_2")
    assert (time.perf_counter() - t0) < 0.05
    assert "my_opaque_tok" not in out_sv_canary
    assert "canary-val_2" not in out_sv_canary


def test_n3_known_secrets_replaced_before_and_after_generic_regexes():
    clear_registered_config_secrets()
    try:
        # 1. Space-containing secret in bare KV assignment: password=abc def1234
        register_config_secrets("abc def1234")
        out_space = scrub_error_detail("password=abc def1234", env={})
        assert out_space == "password=[REDACTED]"
        assert "def1234" not in out_space
        assert "abc" not in out_space

        # 2a. URI userinfo containing '@' with registered 'u:p@ss/w0rd'
        clear_registered_config_secrets()
        register_config_secrets("u:p@ss/w0rd")
        out_ui = scrub_error_detail("postgresql://u:p@ss/w0rd@host/db", env={})
        assert out_ui == "postgresql://[REDACTED]@host/db"
        assert "ss/w0rd" not in out_ui
        assert "p@ss" not in out_ui

        # 2b. URI userinfo containing '@' with registered password 'p@ss/w0rd'
        clear_registered_config_secrets()
        register_config_secrets("p@ss/w0rd")
        out_pw = scrub_error_detail("postgresql://u:p@ss/w0rd@host/db", env={})
        assert out_pw == "postgresql://[REDACTED]@host/db"
        assert "ss/w0rd" not in out_pw

        # 2c. URI userinfo containing '@' extracted from a DSN env var
        clear_registered_config_secrets()
        out_dsn = scrub_error_detail(
            "connect failed postgresql://other_u:p@ss/w0rd@host/db and raw p@ss/w0rd",
            env={"PRIMARY_DSN": "postgresql://u:p@ss/w0rd@cfg-host/db"},
        )
        assert out_dsn == "connect failed postgresql://[REDACTED]@host/db and raw [REDACTED]"
        assert "ss/w0rd" not in out_dsn

        # 3. Short alphabetic password ('pass') never corrupts 'password=' prefix
        clear_registered_config_secrets()
        register_config_secrets("pass")
        out_short = scrub_error_detail(
            "auth failed password=other_secret_99 (hint: pass)",
            env={},
        )
        assert out_short == "auth failed password=[REDACTED] (hint: [REDACTED])"
    finally:
        clear_registered_config_secrets()


def test_n4_duckdb_space_separated_secret_tokens_and_azure_account_key():
    duckdb_clauses = (
        "CREATE SECRET s3_sec (TYPE S3, "
        "KEY_ID 'AKIAIOSFODNN7EXAMPLE', "
        "SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', "
        "SESSION_TOKEN 'AQoDYXdzEJr123456==', "
        "TOKEN 'ghp_duck_tok_98765', "
        "ACCOUNT_KEY 'az_duck_acct_key_4455==', "
        "CLIENT_SECRET 'az_client_sec''6677', "
        r"CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountKey=conn_str_sec\'8899==')"
    )
    out_duck = scrub_error_detail(duckdb_clauses, env={})
    for tok_name in (
        "KEY_ID",
        "SECRET",
        "SESSION_TOKEN",
        "TOKEN",
        "ACCOUNT_KEY",
        "CLIENT_SECRET",
        "CONNECTION_STRING",
    ):
        assert f"{tok_name} '[REDACTED]'" in out_duck
    for leaked in (
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI",
        "AQoDYXdzEJr123456",
        "ghp_duck_tok_98765",
        "az_duck_acct_key_4455",
        "az_client_sec",
        "6677",
        "conn_str_sec",
        "8899",
    ):
        assert leaked not in out_duck, f"Leaked '{leaked}' in: {out_duck}"

    # Azure AccountKey=..., account_key=..., connection_string=... (with base64 '=' padding and env KV extract)
    azure_msg = (
        "DefaultEndpointsProtocol=https;AccountName=myacct;"
        "AccountKey=dGhpcyBpcyBhIHNlY3JldCBrZXk==;EndpointSuffix=core.windows.net | "
        "account_key=az_snake_b64_val= | "
        "connection_string='BlobEndpoint=https://x;SharedAccessSignature=sv=2022' | "
        "echoed_raw=env_extracted_az_key_1122=="
    )
    out_az = scrub_error_detail(
        azure_msg,
        env={
            "AZURE_STORAGE_SECRET": (
                "DefaultEndpointsProtocol=https;AccountName=myacct;"
                "AccountKey=env_extracted_az_key_1122==;EndpointSuffix=core.windows.net"
            )
        },
    )
    assert "AccountKey=[REDACTED];EndpointSuffix=core.windows.net" in out_az
    assert "account_key=[REDACTED]" in out_az
    assert "connection_string=[REDACTED]" in out_az
    for leaked in (
        "dGhpcyBpcyBhIHNlY3JldCBrZXk",
        "az_snake_b64_val",
        "SharedAccessSignature",
        "env_extracted_az_key_1122",
    ):
        assert leaked not in out_az, f"Leaked '{leaked}' in: {out_az}"


def test_n5_bound_numeric_string_params_len_ge_4_redacted_at_word_boundaries():
    # 1. Exact unquoted occurrence of 6-digit PIN "482913" and 4-digit PIN "0042"
    assert (
        scrub_error_detail("invalid pin 482913", params=["482913"], env={})
        == "invalid pin [REDACTED]"
    )
    assert (
        scrub_error_detail("invalid pin 0042.", params=["0042"], env={})
        == "invalid pin [REDACTED]."
    )

    # 2. Word boundaries respected: embedded tokens stay intact; bounded forms are redacted
    mixed = (
        "invalid pin 482913 | pin=482913 | (482913) | quoted='482913' | "
        "ref=order_482913_v1 | acct_seq=94829130 | dur=61.0s"
    )
    scrubbed_mixed = scrub_error_detail(mixed, params=["482913"], env={})
    assert "invalid pin [REDACTED]" in scrubbed_mixed
    assert "pin=[REDACTED]" in scrubbed_mixed
    assert "([REDACTED])" in scrubbed_mixed
    assert "quoted=[REDACTED]" in scrubbed_mixed
    assert "ref=order_482913_v1" in scrubbed_mixed
    assert "acct_seq=94829130" in scrubbed_mixed
    assert "dur=61.0s" in scrubbed_mixed

    # Whitespace-padded numeric string param >= 4 chars
    assert (
        scrub_error_detail("pin: 482913, retry", params=[" 482913 "], env={})
        == "pin: [REDACTED], retry"
    )

    # 3. Short values and non-parameter numbers stay intact
    lifetime_msg = "Token lifetime of 61.0s exceeds maximum 60.0s"
    for short_or_other_params in ([0], ["0"], ["61"], ["999"], ["482913"]):
        assert (
            scrub_error_detail(lifetime_msg, params=short_or_other_params, env={})
            == lifetime_msg
        )


def test_n7_unquoted_secrets_preserve_plus_and_non_delimiter_ampersand():
    azure_key = (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
        "+K1SZFPTOtr/KBHBeksoGMGw=="
    )
    azure_kv = f"AccountKey={azure_key};"
    out_azure = scrub_error_detail(azure_kv, env={})
    assert out_azure == "AccountKey=[REDACTED];"
    assert "K1SZFPTOtr" not in out_azure
    assert "GMGw" not in out_azure
    assert "+" not in out_azure

    # Also verify _DSN_KV_EXTRACT_RE extracts full base64 key past '+' from env
    out_azure_env = scrub_error_detail(
        f"raw leaked key {azure_key} in driver msg",
        env={"AZURE_CONN_SECRET": f"DefaultEndpointsProtocol=https;{azure_kv}EndpointSuffix=core.windows.net"},
    )
    assert out_azure_env == "raw leaked key [REDACTED] in driver msg"
    assert "K1SZFPTOtr" not in out_azure_env

    # Unquoted password with '+' and with '&' not followed by \w+=
    assert scrub_error_detail("password=abc+def", env={}) == "password=[REDACTED]"
    assert scrub_error_detail("password=ab&cd", env={}) == "password=[REDACTED]"
    assert (
        scrub_error_detail("password=ab&cd&host=db.internal", env={})
        == "password=[REDACTED]&host=db.internal"
    )

    # URL query with sig/signature and subsequent query parameters
    assert (
        scrub_error_detail("https://blob.example.net/c/f?sig=ab+cd&x=1", env={})
        == "https://blob.example.net/c/f?sig=[REDACTED]&x=1"
    )
    assert (
        scrub_error_detail("https://blob.example.net/c/f?signature=ab+cd/ef==&x=1", env={})
        == "https://blob.example.net/c/f?signature=[REDACTED]&x=1"
    )
