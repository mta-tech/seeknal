"""Tests for default-catalog canonicalisation in Seeknal Serving."""

from __future__ import annotations

import hashlib
import time

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import (
    EnforcementError,
    Placement,
    ServingPipeline,
    sign_grant_token,
)


@pytest.fixture
def ec_key():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


def _make_token(priv_key, sql: str, tables_grant: dict, *, max_rows: int = 1000) -> str:
    now = time.time()
    claims = {
        "sub": "analyst@atlas.org",
        "ws": "ws-claims",
        "jti": "jti-catalog-001",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": tables_grant,
    }
    return sign_grant_token(claims, priv_key, kid="key-catalog")


def test_two_part_reference_canonicalised_to_three_part(ec_key):
    """A two-part reference is routed and enforced against placement and grant keys written as three-part names."""
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
    schemas = {"lakekeeper.claims.claim": ["claim_id", "province", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-catalog": pub},
        default_catalog="lakekeeper",
    )

    sql = "SELECT claim_id, amount FROM claims.claim WHERE amount > 100"
    token = _make_token(
        priv,
        sql,
        {
            "lakekeeper.claims.claim": {
                "columns": ["claim_id", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "sr"
    assert prepared.engine_kind == "starrocks"
    assert "lakekeeper.claims.claim AS claim" in prepared.sql


def test_one_part_names_untouched(ec_key):
    """One-part names are left alone when default_catalog is configured."""
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
        public_keys={"key-catalog": pub},
        default_catalog="lakekeeper",
    )

    sql = "SELECT user_id, username FROM users"
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

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "lb"
    assert "lakekeeper.users" not in prepared.sql
    assert "users AS users" in prepared.sql


def test_cte_named_like_table_not_qualified(ec_key):
    """A CTE named like a table is not qualified with default_catalog."""
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
    schemas = {"lakekeeper.claims.claim": ["claim_id", "province", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-catalog": pub},
        default_catalog="lakekeeper",
    )

    sql = (
        "WITH claim AS ("
        "SELECT claim_id, amount FROM claims.claim WHERE amount > 50"
        ") SELECT claim_id FROM claim"
    )
    token = _make_token(
        priv,
        sql,
        {
            "lakekeeper.claims.claim": {
                "columns": ["claim_id", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "sr"
    assert "from claim" in prepared.sql.lower()
    assert "lakekeeper.claims.claim" in prepared.sql.lower()


def test_already_three_part_name_untouched(ec_key):
    """An already three-part table name is not re-qualified by default_catalog."""
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
    schemas = {"lakekeeper.claims.claim": ["claim_id", "province", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-catalog": pub},
        default_catalog="other_catalog",
    )

    sql = "SELECT claim_id, amount FROM lakekeeper.claims.claim WHERE amount > 100"
    token = _make_token(
        priv,
        sql,
        {
            "lakekeeper.claims.claim": {
                "columns": ["claim_id", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "sr"
    assert "other_catalog" not in prepared.sql


def test_token_hash_computed_on_original_sql(ec_key):
    """The token hash must match the client's original SQL string."""
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
    schemas = {"lakekeeper.claims.claim": ["claim_id", "province", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-catalog": pub},
        default_catalog="lakekeeper",
    )

    sql = "SELECT claim_id, amount FROM claims.claim"
    # Token hashed on original 2-part SQL: must succeed
    token_valid = _make_token(
        priv,
        sql,
        {
            "lakekeeper.claims.claim": {
                "columns": ["claim_id", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )
    prep = pipeline.prepare(sql, token_valid)
    assert prep.engine == "sr"

    # Token hashed on rewritten 3-part SQL: must be rejected
    rewritten_sql = "SELECT claim_id, amount FROM lakekeeper.claims.claim"
    token_invalid = _make_token(
        priv,
        rewritten_sql,
        {
            "lakekeeper.claims.claim": {
                "columns": ["claim_id", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )
    with pytest.raises(EnforcementError, match="SQL SHA256 mismatch"):
        pipeline.prepare(sql, token_invalid)


def test_default_catalog_none_unchanged(ec_key):
    """When default_catalog is None, two-part table references are unchanged."""
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
    schemas = {"claims.claim": ["claim_id", "province", "amount"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-catalog": pub},
        default_catalog=None,
    )

    sql = "SELECT claim_id, amount FROM claims.claim"
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

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "sr"
    assert "lakekeeper.claims.claim AS claim" in prepared.sql


def test_duckdb_end_to_end_default_catalog_execution(ec_key):
    """End-to-end run on DuckDB placement with physical names claims.claim and logical lakekeeper.claims.claim."""
    priv, pub = ec_key
    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
        },
        "tables": {
            "lakekeeper.claims.claim": {
                "keys": ["claim_id"],
                "row_count": 50,
                "serve": {"duck": "claims.claim"},
            }
        },
        "cubes": {},
    })
    schemas = {"lakekeeper.claims.claim": ["claim_id", "amount", "province"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-catalog": pub},
        default_catalog="lakekeeper",
    )

    sql = "SELECT claim_id, amount FROM claims.claim WHERE amount >= 100"
    token = _make_token(
        priv,
        sql,
        {
            "lakekeeper.claims.claim": {
                "columns": ["claim_id", "amount"],
                "masks": {},
                "row_filters": [{"column": "province", "op": "eq", "values": ["JB"]}],
                "actions": ["select"],
            }
        },
    )

    prepared = pipeline.prepare(sql, token)
    assert prepared.engine == "duck"
    assert prepared.engine_kind == "duckdb"
    assert "claims.claim AS claim" in prepared.sql

    # Execute in DuckDB
    con = duckdb.connect()
    con.execute("CREATE SCHEMA claims")
    con.execute("CREATE TABLE claims.claim (claim_id INT, amount INT, province VARCHAR)")
    con.execute(
        "INSERT INTO claims.claim VALUES "
        "(1, 100, 'JB'), "
        "(2, 50, 'JB'), "
        "(3, 200, 'JK')"
    )

    rows = con.execute(prepared.sql, prepared.params).fetchall()
    # Row 1 matches: amount 100 >= 100 and province == 'JB'
    # Row 2 filtered out by amount >= 100
    # Row 3 filtered out by row filter province == 'JB'
    assert len(rows) == 1
    assert rows[0] == (1, 100)
