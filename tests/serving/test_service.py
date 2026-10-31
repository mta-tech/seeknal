"""Tests for Seeknal Serving pipeline: token verification, routing, enforcement, and parameter preparation."""

from __future__ import annotations

import hashlib
import time

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import (
    EnforcementError,
    Placement,
    RoutingError,
    ServingPipeline,
    sign_grant_token,
)


@pytest.fixture
def ec_key():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


@pytest.fixture
def standard_pipeline(ec_key):
    priv, pub = ec_key
    placement_dict = {
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
            "cube": {"kind": "cube", "dialect": "postgres"},
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
        },
        "tables": {
            "claims.claim": {
                "keys": ["claim_id"],
                "row_count": 50_000_000,
                "serve": {
                    "sr": "lakekeeper.claims.claim",
                    "lb": "public.claim",
                    "duck": "claim",
                },
            },
            "ref.province": {
                "keys": ["province_code"],
                "row_count": 38,
                "serve": {
                    "sr": "lakekeeper.ref.province",
                    "lb": "public.province",
                    "duck": "province",
                },
            },
            "users": {
                "keys": ["user_id"],
                "row_count": 50,
                "serve": {
                    "sr": "sr_users",
                    "lb": "lakebase_users",
                    "duck": "duck_users",
                },
            },
        },
        "cubes": {
            "claims_cube": {
                "engine": "cube",
                "measures": ["total_amount", "claim_count"],
                "dimensions": ["province", "idrg_code"],
            }
        },
    }
    schemas = {
        "claims.claim": ["claim_id", "province", "idrg_code", "amount", "note"],
        "ref.province": ["province_code", "province_name"],
        "users": ["user_id", "username", "role"],
        "claims_cube": ["province", "idrg_code", "total_amount", "claim_count"],
    }
    placement = Placement.from_dict(placement_dict)
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-atlas": pub},
    )
    return pipeline, priv


def _make_token(priv_key, sql: str, tables_grant: dict, *, max_rows: int = 1000) -> str:
    now = time.time()
    claims = {
        "sub": "analyst@atlas.org",
        "ws": "ws-claims",
        "jti": "jti-service-001",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": tables_grant,
    }
    return sign_grant_token(claims, priv_key, kid="key-atlas")


def test_routing_and_enforcement_starrocks(standard_pipeline):
    """Test analytics query routing to StarRocks with group by and derived table rewrite."""
    pipeline, priv = standard_pipeline
    sql = "SELECT idrg_code, count(*) AS total_count FROM claims.claim GROUP BY idrg_code"
    token = _make_token(
        priv,
        sql,
        {
            "claims.claim": {
                "columns": ["idrg_code", "claim_id"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "sr"
    assert prepared.engine_kind == "starrocks"
    assert prepared.rule == "analytics"
    assert prepared.paramstyle == "format"
    assert prepared.params == ()
    assert "lakekeeper.claims.claim AS claim" in prepared.sql
    assert "COUNT(*)" in prepared.sql
    assert "LIMIT 1001" in prepared.sql
    assert prepared.subject == "analyst@atlas.org"
    assert prepared.workspace == "ws-claims"
    assert prepared.audit_id == "jti-service-001"


def test_routing_and_enforcement_lakebase_point_lookup(standard_pipeline):
    """Test point lookup query with $1 parameter routing to Lakebase."""
    pipeline, priv = standard_pipeline
    sql = "SELECT claim_id, amount FROM claims.claim WHERE claim_id = $1"
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
    )

    prepared = pipeline.prepare(sql, token, params=(42,))
    assert prepared.engine == "lb"
    assert prepared.engine_kind == "lakebase"
    assert prepared.rule == "point_lookup"
    assert prepared.paramstyle == "format"
    assert prepared.params == (42,)
    assert "%s" in prepared.sql
    assert "$1" not in prepared.sql
    assert "@1" not in prepared.sql
    assert "public.claim AS claim" in prepared.sql


def test_routing_and_enforcement_cube(standard_pipeline):
    """Test semantic query routing to Cube with cube_security_context generation."""
    pipeline, priv = standard_pipeline
    sql = "SELECT province, total_amount FROM claims_cube"
    token = _make_token(
        priv,
        sql,
        {
            "claims_cube": {
                "columns": ["province", "total_amount"],
                "masks": {},
                "row_filters": [{"column": "province", "op": "eq", "values": ["JB"]}],
                "actions": ["select"],
            }
        },
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "cube"
    assert prepared.engine_kind == "cube"
    assert prepared.rule == "semantic"
    assert prepared.paramstyle == "format"
    assert "claims_cube" in prepared.sql
    assert prepared.cube_security_context is not None
    assert prepared.cube_security_context["subject"] == "analyst@atlas.org"
    assert "claims_cube" in prepared.cube_security_context["row_filters"]


def test_hinted_write_refusal(standard_pipeline):
    """Test that a write statement hinted to a non-lakebase engine is refused with RoutingError."""
    pipeline, priv = standard_pipeline
    sql = "INSERT INTO users (user_id) VALUES (1)"
    token = _make_token(
        priv,
        sql,
        {
            "users": {
                "columns": ["user_id"],
                "masks": {},
                "row_filters": [],
                "actions": ["insert"],
            }
        },
    )

    # Hinting to StarRocks must be refused
    with pytest.raises(RoutingError) as exc_info:
        pipeline.prepare(sql, token, hint="sr")
    assert "writes go only to lakebase" in str(exc_info.value).lower()

    # Hinting to DuckDB must also be refused
    with pytest.raises(RoutingError) as exc_info:
        pipeline.prepare(sql, token, hint="duck")
    assert "writes go only to lakebase" in str(exc_info.value).lower()

    # SQL comment hint to StarRocks must also be refused
    sql_with_comment = "/* seeknal:engine=sr */ INSERT INTO users (user_id) VALUES (1)"
    token_comment = _make_token(
        priv,
        sql_with_comment,
        {
            "users": {
                "columns": ["user_id"],
                "masks": {},
                "row_filters": [],
                "actions": ["insert"],
            }
        },
    )
    with pytest.raises(RoutingError) as exc_info:
        pipeline.prepare(sql_with_comment, token_comment)
    assert "writes go only to lakebase" in str(exc_info.value).lower()

    # Hinting to Lakebase succeeds
    prep_lb = pipeline.prepare(sql, token, hint="lb")
    assert prep_lb.engine == "lb"
    assert prep_lb.engine_kind == "lakebase"


def test_parameter_reordering_and_percent_escaping(standard_pipeline):
    """Test parameter reordering ($2 ... $1 ... $2) and escaping literal % as %%."""
    pipeline, priv = standard_pipeline
    sql = (
        "SELECT claim_id, amount FROM claims.claim "
        "WHERE claim_id = $2 AND amount > $1 AND note LIKE 'JB%' AND claim_id = $2"
    )
    token = _make_token(
        priv,
        sql,
        {
            "claims.claim": {
                "columns": ["claim_id", "amount", "note"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )

    # $1 is 100, $2 is 42 -> expected final params: (42, 100, 42)
    prepared = pipeline.prepare(sql, token, params=(100, 42))
    assert prepared.paramstyle == "format"
    assert prepared.params == (42, 100, 42)
    # Literal % escaped to %%
    assert "JB%%" in prepared.sql
    assert "JB%" not in prepared.sql.replace("JB%%", "")
    assert "%s" in prepared.sql
    assert "$1" not in prepared.sql
    assert "$2" not in prepared.sql
    assert "@1" not in prepared.sql
    assert "@2" not in prepared.sql


def test_parameter_count_mismatch(standard_pipeline):
    """Test parameter count mismatches raise EnforcementError."""
    pipeline, priv = standard_pipeline

    # Query uses $1 and $2, but only 1 parameter provided
    sql1 = "SELECT claim_id, amount FROM claims.claim WHERE claim_id = $1 AND amount > $2"
    token1 = _make_token(
        priv,
        sql1,
        {"claims.claim": {"columns": ["claim_id", "amount"], "masks": {}, "actions": ["select"]}},
    )
    with pytest.raises(EnforcementError) as exc_info:
        pipeline.prepare(sql1, token1, params=(42,))
    assert "no matching parameter" in str(exc_info.value).lower()

    # Query uses only $1, but 2 parameters provided (unused parameter)
    sql2 = "SELECT claim_id, amount FROM claims.claim WHERE claim_id = $1"
    token2 = _make_token(
        priv,
        sql2,
        {"claims.claim": {"columns": ["claim_id", "amount"], "masks": {}, "actions": ["select"]}},
    )
    with pytest.raises(EnforcementError) as exc_info:
        pipeline.prepare(sql2, token2, params=(42, 99))
    assert "never used" in str(exc_info.value).lower()

    # Query has no placeholders, but parameters provided
    sql3 = "SELECT claim_id, amount FROM claims.claim"
    token3 = _make_token(
        priv,
        sql3,
        {"claims.claim": {"columns": ["claim_id", "amount"], "masks": {}, "actions": ["select"]}},
    )
    with pytest.raises(EnforcementError) as exc_info:
        pipeline.prepare(sql3, token3, params=(42,))
    assert "no placeholders" in str(exc_info.value).lower()

    # Query has placeholders, but empty params tuple provided
    with pytest.raises(EnforcementError) as exc_info:
        pipeline.prepare(sql2, token2, params=())
    assert "no matching parameter" in str(exc_info.value).lower()


def test_duckdb_end_to_end_execution(ec_key):
    """End-to-end run on DuckDB-only placement that executes prepared SQL and checks masks & filters."""
    priv, pub = ec_key
    duck_placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"}
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
        placement=duck_placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )

    sql = "SELECT id, customer_name, amount FROM orders WHERE amount >= $1"
    now = time.time()
    claims = {
        "sub": "analyst@duck.local",
        "ws": "local-analytics",
        "jti": "jti-duck-001",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 60,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 10,
        "tables": {
            "orders": {
                "columns": ["id", "amount"],
                "masks": {"customer_name": "redact"},
                "row_filters": [{"column": "province", "op": "eq", "values": ["JB"]}],
                "actions": ["select"],
            }
        },
    }
    token = sign_grant_token(claims, priv, kid="k1")

    prepared = pipeline.prepare(sql, token, params=(50,))
    assert prepared.engine == "duck"
    assert prepared.engine_kind == "duckdb"
    assert prepared.paramstyle == "qmark"
    assert prepared.params == (50,)
    assert "?" in prepared.sql
    assert "inmem_orders AS orders" in prepared.sql
    assert prepared.masked_columns == ("orders.customer_name",)

    # Execute in in-memory DuckDB
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE inmem_orders (id INT, customer_name VARCHAR, amount INT, province VARCHAR)"
    )
    con.execute(
        "INSERT INTO inmem_orders VALUES "
        "(1, 'John Doe', 100, 'JB'), "
        "(2, 'Jane Smith', 30, 'JB'), "
        "(3, 'Bob Brown', 200, 'JK')"
    )

    rows = con.execute(prepared.sql, prepared.params).fetchall()
    # Row 1 matches: amount 100 >= 50, province == 'JB', customer_name is redacted to '****'
    # Row 2 filtered out: amount 30 < 50
    # Row 3 filtered out: province 'JK' != 'JB'
    assert len(rows) == 1
    assert rows[0] == (1, "****", 100)
