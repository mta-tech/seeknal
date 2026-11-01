"""Tests for Seeknal Serving contract v2: column types and request limits."""

from __future__ import annotations

import hashlib
import time
from typing import Any

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import (
    DuckDBAdapter,
    Placement,
    QueryResult,
    ServingPipeline,
    create_app,
    normalize_column_type,
    sign_grant_token,
)
from seeknal.serving.service import PreparedQuery


def _base_claims(sql: str, *, max_rows: int = 1000, **kwargs: Any) -> dict[str, Any]:
    now = time.time()
    claims: dict[str, Any] = {
        "sub": "analyst@example.com",
        "ws": "ws-analytics",
        "jti": "jti-types-01",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": {
            "users": {
                "columns": ["id", "salary", "active", "ratio", "name", "created_date", "created_at"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    claims.update(kwargs)
    return claims


# -------------------------------------------------------------------------
# 1. Type normalisation tests
# -------------------------------------------------------------------------


def test_duckdb_real_query_column_types():
    con = duckdb.connect()
    con.execute("""
        SELECT
            1::INTEGER AS a_int,
            2::BIGINT AS b_bigint,
            3.14::DOUBLE AS c_double,
            12.34::DECIMAL(10,2) AS d_dec,
            'hello'::VARCHAR AS e_varchar,
            DATE '2026-11-01' AS f_date,
            TIMESTAMP '2026-11-01 12:00:00' AS g_ts,
            true::BOOLEAN AS h_bool
    """)
    desc = con.description
    assert desc is not None
    types = [normalize_column_type("duckdb", d) for d in desc]
    assert types == [
        "integer",
        "bigint",
        "double",
        "decimal(10,2)",
        "varchar",
        "date",
        "timestamp",
        "boolean",
    ]


def test_duckdb_other_types():
    con = duckdb.connect()
    con.execute("""
        SELECT
            1::SMALLINT AS s_int,
            1.5::FLOAT AS f_val,
            TIMESTAMPTZ '2026-11-01 12:00:00+00' AS tz_val,
            TIME '12:00:00' AS t_val,
            '\\x01\\x02'::BLOB AS b_val
    """)
    desc = con.description
    assert desc is not None
    types = [normalize_column_type("duckdb", d) for d in desc]
    assert types == ["smallint", "float", "timestamptz", "time", "binary"]
    # unconstrained decimal without precision/scale
    assert normalize_column_type("duckdb", "DECIMAL") == "decimal"
    assert normalize_column_type("duckdb", "NUMERIC") == "decimal"
    assert normalize_column_type("duckdb", ("dec", "DECIMAL", None, None, None, None, None)) == "decimal"


def test_pymysql_field_type_codes():
    import pymysql.constants.FIELD_TYPE as FT

    # boolean (BIT or TINY with display_size=1)
    assert normalize_column_type("pymysql", FT.BIT) == "boolean"
    assert normalize_column_type("starrocks", ("col", FT.BIT, None, None, None, None, None)) == "boolean"
    assert normalize_column_type("pymysql", ("col", FT.TINY, 1, None, None, None, None)) == "boolean"

    # smallint
    assert normalize_column_type("pymysql", FT.SHORT) == "smallint"
    assert normalize_column_type("pymysql", ("col", FT.TINY, 4, None, None, None, None)) == "smallint"

    # integer
    assert normalize_column_type("pymysql", FT.LONG) == "integer"
    assert normalize_column_type("pymysql", FT.INT24) == "integer"

    # bigint
    assert normalize_column_type("pymysql", FT.LONGLONG) == "bigint"

    # float, double
    assert normalize_column_type("pymysql", FT.FLOAT) == "float"
    assert normalize_column_type("pymysql", FT.DOUBLE) == "double"

    # decimal with precision and scale or unconstrained
    assert normalize_column_type("pymysql", ("dec", FT.DECIMAL, None, None, 10, 2, None)) == "decimal(10,2)"
    assert normalize_column_type("starrocks", ("dec", FT.NEWDECIMAL, None, None, 18, 4, None)) == "decimal(18,4)"
    assert normalize_column_type("pymysql", FT.DECIMAL) == "decimal"
    assert normalize_column_type("pymysql", FT.NEWDECIMAL) == "decimal"
    assert normalize_column_type("pymysql", ("dec", FT.DECIMAL, None, None, None, None, None)) == "decimal"

    # varchar
    assert normalize_column_type("pymysql", FT.VARCHAR) == "varchar"
    assert normalize_column_type("pymysql", FT.VAR_STRING) == "varchar"
    assert normalize_column_type("pymysql", FT.STRING) == "varchar"

    # date, timestamp, time
    assert normalize_column_type("pymysql", FT.DATE) == "date"
    assert normalize_column_type("pymysql", FT.NEWDATE) == "date"
    assert normalize_column_type("pymysql", FT.TIMESTAMP) == "timestamp"
    assert normalize_column_type("pymysql", FT.DATETIME) == "timestamp"
    assert normalize_column_type("pymysql", FT.TIME) == "time"

    # binary
    assert normalize_column_type("pymysql", FT.BLOB) == "binary"
    assert normalize_column_type("pymysql", FT.TINY_BLOB) == "binary"
    assert normalize_column_type("pymysql", FT.MEDIUM_BLOB) == "binary"
    assert normalize_column_type("pymysql", FT.LONG_BLOB) == "binary"

    # json
    assert normalize_column_type("pymysql", FT.JSON) == "json"

    # unknown
    assert normalize_column_type("pymysql", 999) == "unknown"


def test_psycopg2_type_oids():
    # 16, 20, 21, 23, 700, 701, 1700, 25, 1043, 1082, 1114, 1184, 1083, 17, 114, 3802
    assert normalize_column_type("lakebase", 16) == "boolean"
    assert normalize_column_type("cube", 16) == "boolean"
    assert normalize_column_type("psycopg2", 20) == "bigint"
    assert normalize_column_type("psycopg2", 21) == "smallint"
    assert normalize_column_type("psycopg2", 23) == "integer"
    assert normalize_column_type("psycopg2", 700) == "float"
    assert normalize_column_type("psycopg2", 701) == "double"
    assert normalize_column_type("psycopg2", ("num", 1700, None, None, 10, 2, None)) == "decimal(10,2)"
    assert normalize_column_type("psycopg2", 1700) == "decimal"
    assert normalize_column_type("lakebase", ("num", 1700, None, None, None, None, None)) == "decimal"
    assert normalize_column_type("cube", ("num", 1700, None, None, None, None, None)) == "decimal"
    assert normalize_column_type("psycopg2", 25) == "varchar"
    assert normalize_column_type("psycopg2", 1043) == "varchar"
    assert normalize_column_type("psycopg2", 1082) == "date"
    assert normalize_column_type("psycopg2", 1114) == "timestamp"
    assert normalize_column_type("psycopg2", 1184) == "timestamptz"
    assert normalize_column_type("psycopg2", 1083) == "time"
    assert normalize_column_type("psycopg2", 17) == "binary"
    assert normalize_column_type("psycopg2", 114) == "json"
    assert normalize_column_type("psycopg2", 3802) == "json"
    assert normalize_column_type("psycopg2", 99999) == "unknown"


# -------------------------------------------------------------------------
# 2. HTTP responses and request limits
# -------------------------------------------------------------------------


@pytest.fixture
def http_setup():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    con = duckdb.connect()
    con.execute("""
        CREATE TABLE users (
            id INT,
            salary INT,
            active BOOLEAN,
            ratio DOUBLE,
            name VARCHAR,
            created_date DATE,
            created_at TIMESTAMP
        )
    """)
    for i in range(1, 101):
        con.execute(f"""
            INSERT INTO users VALUES (
                {i}, {1000 * i}, {i % 2 == 0}, {i * 0.1}, 'user_{i}',
                DATE '2026-11-01', TIMESTAMP '2026-11-01 10:00:00'
            )
        """)

    placement = Placement.from_dict({
        "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {
            "users": {
                "keys": ["id"],
                "row_count": 100,
                "serve": {"duck": "users"},
            }
        },
        "cubes": {},
    })
    schemas = {
        "users": ["id", "salary", "active", "ratio", "name", "created_date", "created_at"]
    }
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-1": pub},
    )
    adapter = DuckDBAdapter(lambda: con)
    app = create_app(pipeline, {"duck": adapter}, timeout_seconds=10.0)
    client = TestClient(app)
    return client, priv, pipeline


def test_http_column_types_in_response(http_setup):
    client, priv, _ = http_setup
    sql = "SELECT id, salary, active, ratio, name, created_date, created_at FROM users"
    token = sign_grant_token(_base_claims(sql), priv, kid="key-1")

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "columns" in data
    assert "column_types" in data
    assert len(data["columns"]) == len(data["column_types"])
    assert data["column_types"] == [
        "integer",
        "integer",
        "boolean",
        "double",
        "varchar",
        "date",
        "timestamp",
    ]


def test_max_rows_lowers_cap_and_clamped_to_grant(http_setup):
    client, priv, pipeline = http_setup
    sql = "SELECT id, salary FROM users"

    # Grant allows 50 rows
    token = sign_grant_token(_base_claims(sql, max_rows=50), priv, kid="key-1")

    # 1. Request lowers cap to 5 rows
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "max_rows": 5},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["rows"]) == 5
    assert data["truncated"] is True

    # Check /v1/explain row_limit is 5 + 1 = 6
    resp_exp = client.post(
        "/v1/explain",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "max_rows": 5},
    )
    assert resp_exp.status_code == 200
    assert resp_exp.json()["row_limit"] == 6

    # 2. Request asks for 200 rows, but grant is 50: clamped to 50
    resp_clamped = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "max_rows": 200},
    )
    assert resp_clamped.status_code == 200
    data_clamped = resp_clamped.json()
    assert len(data_clamped["rows"]) == 50
    assert data_clamped["truncated"] is True


class RecordingAdapter:
    def __init__(self) -> None:
        self.recorded_timeouts: list[float] = []

    def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
        self.recorded_timeouts.append(timeout_seconds)
        return QueryResult(
            columns=("id",),
            rows=[(1,)],
            truncated=False,
            engine="rec",
            elapsed_ms=1.0,
        )


def test_timeout_s_reaches_adapter_as_minimum():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    placement = Placement.from_dict({
        "engines": {"rec": {"kind": "duckdb", "dialect": "duckdb"}},
        "tables": {"users": {"keys": ["id"], "row_count": 1, "serve": {"rec": "users"}}},
        "cubes": {},
    })
    pipeline = ServingPipeline(placement=placement, schemas={"users": ["id"]}, public_keys={"key-1": pub})
    adapter = RecordingAdapter()
    # Server timeout is 30.0 s
    app = create_app(pipeline, {"rec": adapter}, timeout_seconds=30.0)
    client = TestClient(app)

    sql = "SELECT id FROM users"
    token = sign_grant_token(_base_claims(sql), priv, kid="key-1")

    # Lower than server timeout: 5.5 s
    client.post("/v1/query", headers={"Authorization": f"Bearer {token}"}, json={"sql": sql, "timeout_s": 5.5})
    assert adapter.recorded_timeouts[-1] == 5.5

    # Higher than server timeout: 100.0 s clamped to 30.0 s
    client.post("/v1/query", headers={"Authorization": f"Bearer {token}"}, json={"sql": sql, "timeout_s": 100.0})
    assert adapter.recorded_timeouts[-1] == 30.0


def test_max_result_bytes_413_with_no_rows(http_setup):
    client, priv, _ = http_setup
    sql = "SELECT id, salary, name FROM users"
    token = sign_grant_token(_base_claims(sql, max_rows=100), priv, kid="key-1")

    # Set tiny max_result_bytes
    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, "max_result_bytes": 50},
    )
    assert resp.status_code == 413
    data = resp.json()
    assert data["error"] == "result_too_large"
    assert "rows" not in data


@pytest.mark.parametrize(
    "field,bad_val",
    [
        ("timeout_s", "fast"),
        ("timeout_s", True),
        ("timeout_s", -1.0),
        ("timeout_s", 0),
        ("max_rows", "ten"),
        ("max_rows", True),
        ("max_rows", 12.5),
        ("max_rows", -5),
        ("max_rows", 0),
        ("max_result_bytes", "big"),
        ("max_result_bytes", True),
        ("max_result_bytes", 1024.5),
        ("max_result_bytes", -10),
        ("max_result_bytes", 0),
    ],
)
def test_bad_request_limit_field_types_give_400(http_setup, field: str, bad_val: Any):
    client, priv, _ = http_setup
    sql = "SELECT id, salary FROM users"
    token = sign_grant_token(_base_claims(sql), priv, kid="key-1")

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql, field: bad_val},
    )
    assert resp.status_code == 400
    assert resp.json()["error"] == "bad_request"


def test_nan_and_inf_give_400(http_setup):
    client, priv, _ = http_setup
    sql = "SELECT id, salary FROM users"
    token = sign_grant_token(_base_claims(sql), priv, kid="key-1")

    # Send non-standard JSON literals NaN and Infinity directly in content
    for val_str in ("NaN", "Infinity", "-Infinity"):
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            content=f'{{"sql": "{sql}", "timeout_s": {val_str}}}'.encode("utf-8"),
        )
        assert resp.status_code == 400
        assert resp.json()["error"] == "bad_request"


def test_v1_request_without_new_fields_unchanged(http_setup):
    client, priv, _ = http_setup
    sql = "SELECT id, salary FROM users"
    token = sign_grant_token(_base_claims(sql), priv, kid="key-1")

    resp = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {token}"},
        json={"sql": sql},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "columns" in data
    assert "rows" in data
    assert "truncated" in data
    assert "engine" in data
    assert "column_types" in data
    assert len(data["rows"]) == 100
