"""Tests for signed source dialect in Seeknal Serving."""

from __future__ import annotations

import hashlib
import time

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import (
    EnforcementError,
    Grant,
    Placement,
    ServingPipeline,
    sign_grant_token,
)


@pytest.fixture
def ec_key():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


def _make_token(
    priv_key,
    sql: str,
    tables_grant: dict,
    *,
    max_rows: int = 1000,
    sql_dialect: str | None = None,
) -> str:
    now = time.time()
    claims = {
        "sub": "analyst@atlas.org",
        "ws": "ws-claims",
        "jti": "jti-dialect-001",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": tables_grant,
    }
    if sql_dialect is not None:
        claims["sql_dialect"] = sql_dialect
    return sign_grant_token(claims, priv_key, kid="key-dialect")


def test_unknown_and_non_string_sql_dialect_refused():
    """An unknown sql_dialect value, and a non-string value, are refused."""
    base_claims = {
        "sub": "user:1",
        "ws": "ws:1",
        "jti": "jti:1",
        "sql_sha256": "abc",
        "max_rows": 100,
        "exp": time.time() + 60,
        "tables": {
            "tbl": {
                "columns": ["id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }

    # Unknown string dialect
    with pytest.raises(EnforcementError, match="invalid sql_dialect"):
        Grant.from_claims({**base_claims, "sql_dialect": "oracle"})

    with pytest.raises(EnforcementError, match="invalid sql_dialect"):
        Grant.from_claims({**base_claims, "sql_dialect": "snowflake"})

    # Non-string dialects
    with pytest.raises(EnforcementError, match="invalid sql_dialect"):
        Grant.from_claims({**base_claims, "sql_dialect": 123})

    with pytest.raises(EnforcementError, match="invalid sql_dialect"):
        Grant.from_claims({**base_claims, "sql_dialect": ["postgres"]})

    with pytest.raises(EnforcementError, match="invalid sql_dialect"):
        Grant.from_claims({**base_claims, "sql_dialect": None})


def test_absent_claim_behaves_as_postgres(ec_key):
    """An absent sql_dialect claim behaves as postgres."""
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "tables": {
            "users": {
                "keys": ["user_id"],
                "row_count": 50,
                "serve": {"lb": "users"},
            }
        },
        "cubes": {},
    })
    schemas = {"users": ["user_id", "username"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-dialect": pub},
    )

    sql = "SELECT user_id, username FROM users WHERE user_id = $1"
    token = _make_token(
        priv,
        sql,
        {
            "users": {
                "columns": ["user_id", "username"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )

    prepared = pipeline.prepare(sql, token, params=(1,))
    assert prepared.engine == "lb"
    assert prepared.params == (1,)
    assert "%s" in prepared.sql


def test_starrocks_sql_superset_routed_to_starrocks(ec_key):
    """StarRocks SQL as Superset generates it is routed to StarRocks with masks and filters in StarRocks SQL."""
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
        },
        "tables": {
            "claims.claim": {
                "keys": ["claim_id"],
                "row_count": 50_000_000,
                "serve": {"sr": "lakekeeper.claims.claim"},
            }
        },
        "cubes": {},
    })
    schemas = {"claims.claim": ["claim_id", "province", "created_at", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-dialect": pub},
    )

    sql = (
        "SELECT `province`, DATE_TRUNC('day', `created_at`) AS `__timestamp`, SUM(`amount`) AS `total` "
        "FROM `claims`.`claim` "
        "GROUP BY `province`, DATE_TRUNC('day', `created_at`) "
        "ORDER BY `total` DESC LIMIT 1000"
    )
    token = _make_token(
        priv,
        sql,
        {
            "claims.claim": {
                "columns": ["province", "amount"],
                "masks": {"created_at": "year"},
                "row_filters": [{"column": "province", "op": "eq", "values": ["JB"]}],
                "actions": ["select"],
            }
        },
        sql_dialect="starrocks",
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "sr"
    assert prepared.engine_kind == "starrocks"
    assert prepared.rule == "analytics"
    # Check StarRocks backtick rendering and AST rewriting
    assert "`province`" in prepared.sql
    assert "lakekeeper.claims.claim" in prepared.sql
    assert "province = 'JB'" in prepared.sql
    assert "DATE_TRUNC" in prepared.sql
    assert "LIMIT 1000" in prepared.sql


def test_starrocks_sql_with_default_catalog(ec_key):
    """The same StarRocks statement with default_catalog set qualifies to three-part grant key."""
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
        },
        "tables": {
            "lakekeeper.claims.claim": {
                "keys": ["claim_id"],
                "row_count": 50_000_000,
                "serve": {"sr": "lakekeeper.claims.claim"},
            }
        },
        "cubes": {},
    })
    schemas = {"lakekeeper.claims.claim": ["claim_id", "province", "created_at", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-dialect": pub},
        default_catalog="lakekeeper",
    )

    sql = (
        "SELECT `province`, DATE_TRUNC('day', `created_at`) AS `__timestamp`, SUM(`amount`) AS `total` "
        "FROM `claims`.`claim` "
        "GROUP BY `province`, DATE_TRUNC('day', `created_at`) "
        "ORDER BY `total` DESC LIMIT 1000"
    )
    token = _make_token(
        priv,
        sql,
        {
            "lakekeeper.claims.claim": {
                "columns": ["province", "amount"],
                "masks": {"created_at": "year"},
                "row_filters": [{"column": "province", "op": "eq", "values": ["JB"]}],
                "actions": ["select"],
            }
        },
        sql_dialect="starrocks",
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "sr"
    assert prepared.engine_kind == "starrocks"
    assert "lakekeeper" in prepared.sql


def test_starrocks_point_lookup_routed_to_lakebase_valid_postgres(ec_key):
    """A StarRocks-dialect point lookup routed to Lakebase is rendered as valid Postgres (no backticks)."""
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {
            "lb": {"kind": "lakebase", "dialect": "postgres"},
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
        },
        "tables": {
            "claims.claim": {
                "keys": ["claim_id"],
                "row_count": 1000,
                "serve": {
                    "lb": "public.claim",
                    "sr": "sr_claim",
                },
            }
        },
        "cubes": {},
    })
    schemas = {"claims.claim": ["claim_id", "amount", "province"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-dialect": pub},
    )

    sql = "SELECT `claim_id`, `amount` FROM `claims`.`claim` WHERE `claim_id` = 42"
    token = _make_token(
        priv,
        sql,
        {
            "claims.claim": {
                "columns": ["claim_id", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
        sql_dialect="starrocks",
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "lb"
    assert prepared.engine_kind == "lakebase"
    assert prepared.rule == "point_lookup"
    # Valid Postgres SQL must not contain backticks
    assert "`" not in prepared.sql
    assert "public.claim" in prepared.sql


def test_duckdb_end_to_end_backtick_starrocks_sql(ec_key):
    """An end-to-end DuckDB run of backtick-quoted StarRocks SQL executes and returns masked values."""
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
        },
        "tables": {
            "orders": {
                "keys": ["id"],
                "row_count": 50,
                "serve": {"duck": "inmem_orders"},
            }
        },
        "cubes": {},
    })
    schemas = {"orders": ["id", "customer_name", "amount", "province"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-dialect": pub},
    )

    sql = "SELECT `id`, `customer_name`, `amount` FROM `orders` WHERE `amount` >= 50"
    token = _make_token(
        priv,
        sql,
        {
            "orders": {
                "columns": ["id", "amount"],
                "masks": {"customer_name": "redact"},
                "row_filters": [{"column": "province", "op": "eq", "values": ["JB"]}],
                "actions": ["select"],
            }
        },
        sql_dialect="starrocks",
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "duck"
    assert prepared.engine_kind == "duckdb"

    # Execute in DuckDB
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE inmem_orders (id INT, customer_name VARCHAR, amount INT, province VARCHAR)"
    )
    con.execute(
        "INSERT INTO inmem_orders VALUES "
        "(1, 'Alice', 100, 'JB'), "
        "(2, 'Bob', 30, 'JB'), "
        "(3, 'Charlie', 200, 'JK')"
    )

    rows = con.execute(prepared.sql, prepared.params).fetchall()
    # Row 1 matches: amount 100 >= 50, province == 'JB', customer_name redacted to '****'
    # Row 2 filtered: amount 30 < 50
    # Row 3 filtered: province 'JK' != 'JB'
    assert len(rows) == 1
    assert rows[0] == (1, "****", 100)


def test_load_file_in_starrocks_sql_refused(ec_key):
    """load_file(...) written in StarRocks SQL is refused."""
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
        },
        "tables": {
            "claims.claim": {
                "keys": ["claim_id"],
                "row_count": 50,
                "serve": {"sr": "lakekeeper.claims.claim"},
            }
        },
        "cubes": {},
    })
    schemas = {"claims.claim": ["claim_id"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-dialect": pub},
    )

    sql = "SELECT load_file('/etc/passwd')"
    token = _make_token(
        priv,
        sql,
        {
            "claims.claim": {
                "columns": ["claim_id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
        sql_dialect="starrocks",
    )

    with pytest.raises(EnforcementError, match="function 'load_file' is not permitted"):
        pipeline.prepare(sql, token)


def test_non_empty_params_with_starrocks_dialect_refused(ec_key):
    """Non-empty params with sql_dialect starrocks is refused."""
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
        },
        "tables": {
            "claims.claim": {
                "keys": ["claim_id"],
                "row_count": 50,
                "serve": {"sr": "lakekeeper.claims.claim"},
            }
        },
        "cubes": {},
    })
    schemas = {"claims.claim": ["claim_id", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-dialect": pub},
    )

    sql = "SELECT `claim_id` FROM `claims`.`claim` WHERE `claim_id` = 42"
    token = _make_token(
        priv,
        sql,
        {
            "claims.claim": {
                "columns": ["claim_id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
        sql_dialect="starrocks",
    )

    with pytest.raises(EnforcementError, match="Parameters are not supported"):
        pipeline.prepare(sql, token, params=(42,))
