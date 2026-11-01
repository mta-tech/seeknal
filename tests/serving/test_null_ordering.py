"""Tests for NULL ordering when transpiling across dialects (item 2)."""

from __future__ import annotations

import hashlib
import time

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import (
    DuckDBAdapter,
    Placement,
    ServingPipeline,
    sign_grant_token,
)


@pytest.fixture
def ec_keys():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


def _make_token(priv, sql: str, sql_dialect: str = "starrocks", tables: dict | None = None) -> str:
    now = time.time()
    if tables is None:
        tables = {
            "items": {
                "columns": ["id", "val"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        }
    claims = {
        "sub": "analyst@example.com",
        "ws": "analytics",
        "jti": f"grant-null-{int(now * 1000)}",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 60,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 1000,
        "sql_dialect": sql_dialect,
        "tables": tables,
    }
    return sign_grant_token(claims, priv, kid="k1")


def test_starrocks_null_ordering_execution_on_duckdb(ec_keys):
    """Execute on real DuckDB: StarRocks-dialect query returns NULLs first for ASC and last for DESC."""
    priv, pub = ec_keys

    con = duckdb.connect()
    con.execute("CREATE TABLE items (id INT, val INT)")
    con.execute("INSERT INTO items VALUES (1, 10), (2, NULL), (3, 20), (4, NULL), (5, 5)")

    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
        },
        "tables": {
            "items": {
                "keys": ["id"],
                "row_count": 5,
                "serve": {"duck": "items"},
            }
        },
        "cubes": {},
    })
    schemas = {"items": ["id", "val"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )
    adapter = DuckDBAdapter(lambda: con)

    # 1. ASC: StarRocks default is NULLs first.
    sql_asc = "SELECT val FROM items ORDER BY val ASC"
    token_asc = _make_token(priv, sql_asc, sql_dialect="starrocks")
    prep_asc = pipeline.prepare(sql_asc, token_asc)

    assert "NULLS FIRST" in prep_asc.sql
    res_asc = adapter.execute(prep_asc, timeout_seconds=5.0)
    vals_asc = [row[0] for row in res_asc.rows]
    # First two values must be None
    assert vals_asc[0] is None
    assert vals_asc[1] is None
    assert vals_asc[2:] == [5, 10, 20]

    # 2. DESC: StarRocks default is NULLs last.
    sql_desc = "SELECT val FROM items ORDER BY val DESC"
    token_desc = _make_token(priv, sql_desc, sql_dialect="starrocks")
    prep_desc = pipeline.prepare(sql_desc, token_desc)

    assert "NULLS LAST" in prep_desc.sql
    res_desc = adapter.execute(prep_desc, timeout_seconds=5.0)
    vals_desc = [row[0] for row in res_desc.rows]
    # Last two values must be None
    assert vals_desc[:3] == [20, 10, 5]
    assert vals_desc[3] is None
    assert vals_desc[4] is None


def test_starrocks_explicit_nulls_preserved_on_duckdb_and_postgres(ec_keys):
    """Explicit NULLS FIRST/LAST are preserved when source is StarRocks."""
    priv, pub = ec_keys

    con = duckdb.connect()
    con.execute("CREATE TABLE items (id INT, val INT)")
    con.execute("INSERT INTO items VALUES (1, 10), (2, NULL), (3, 20), (4, NULL), (5, 5)")

    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "tables": {
            "items": {
                "keys": ["id"],
                "row_count": 5,
                "serve": {"duck": "items", "lb": "items"},
            }
        },
        "cubes": {},
    })
    schemas = {"items": ["id", "val"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )
    adapter = DuckDBAdapter(lambda: con)

    # 1. Explicit ASC NULLS LAST -> preserved on DuckDB
    sql_explicit_asc = "SELECT val FROM items ORDER BY val ASC NULLS LAST"
    token_explicit_asc = _make_token(priv, sql_explicit_asc, sql_dialect="starrocks")
    prep_duck_asc = pipeline.prepare(sql_explicit_asc, token_explicit_asc, hint="duck")
    assert "NULLS LAST" in prep_duck_asc.sql
    res_duck_asc = adapter.execute(prep_duck_asc, timeout_seconds=5.0)
    vals_duck_asc = [row[0] for row in res_duck_asc.rows]
    assert vals_duck_asc[-1] is None
    assert vals_duck_asc[-2] is None

    # Preserved when targeting Postgres
    prep_lb_asc = pipeline.prepare(sql_explicit_asc, token_explicit_asc, hint="lb")
    assert "NULLS LAST" in prep_lb_asc.sql

    # 2. Explicit DESC NULLS FIRST -> preserved on DuckDB
    sql_explicit_desc = "SELECT val FROM items ORDER BY val DESC NULLS FIRST"
    token_explicit_desc = _make_token(priv, sql_explicit_desc, sql_dialect="starrocks")
    prep_duck_desc = pipeline.prepare(sql_explicit_desc, token_explicit_desc, hint="duck")
    assert "NULLS FIRST" in prep_duck_desc.sql
    res_duck_desc = adapter.execute(prep_duck_desc, timeout_seconds=5.0)
    vals_duck_desc = [row[0] for row in res_duck_desc.rows]
    assert vals_duck_desc[0] is None
    assert vals_duck_desc[1] is None

    # Preserved when targeting Postgres
    prep_lb_desc = pipeline.prepare(sql_explicit_desc, token_explicit_desc, hint="lb")
    assert "NULLS FIRST" in prep_lb_desc.sql


def test_other_dialect_pairs_unchanged(ec_keys):
    """Dialect pairs other than (starrocks/mysql -> postgres/duckdb) leave implicit nulls untouched."""
    priv, pub = ec_keys

    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
        },
        "tables": {
            "items": {
                "keys": ["id"],
                "row_count": 5,
                "serve": {"duck": "items", "lb": "items", "sr": "items"},
            }
        },
        "cubes": {},
    })
    schemas = {"items": ["id", "val"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )

    # 1. postgres -> postgres: no NULLS FIRST/LAST added
    sql_pg = "SELECT val FROM items ORDER BY val"
    token_pg = _make_token(priv, sql_pg, sql_dialect="postgres")
    prep_pg_pg = pipeline.prepare(sql_pg, token_pg, hint="lb")
    assert "NULLS FIRST" not in prep_pg_pg.sql
    assert "NULLS LAST" not in prep_pg_pg.sql

    # 2. postgres -> duckdb: no NULLS FIRST/LAST added
    prep_pg_duck = pipeline.prepare(sql_pg, token_pg, hint="duck")
    assert "NULLS FIRST" not in prep_pg_duck.sql
    assert "NULLS LAST" not in prep_pg_duck.sql

    # 3. postgres -> starrocks: no CASE WHEN or NULLS FIRST/LAST added
    prep_pg_sr = pipeline.prepare(sql_pg, token_pg, hint="sr")
    assert "CASE WHEN" not in prep_pg_sr.sql
    assert "NULLS" not in prep_pg_sr.sql

    # 4. duckdb -> postgres: no NULLS FIRST/LAST added
    sql_duck = "SELECT val FROM items ORDER BY val"
    token_duck = _make_token(priv, sql_duck, sql_dialect="duckdb")
    prep_duck_pg = pipeline.prepare(sql_duck, token_duck, hint="lb")
    assert "NULLS FIRST" not in prep_duck_pg.sql
    assert "NULLS LAST" not in prep_duck_pg.sql

    # 5. starrocks -> starrocks: no NULLS FIRST/LAST added
    sql_sr = "SELECT val FROM items ORDER BY val"
    token_sr = _make_token(priv, sql_sr, sql_dialect="starrocks")
    prep_sr_sr = pipeline.prepare(sql_sr, token_sr, hint="sr")
    assert "NULLS" not in prep_sr_sr.sql


def test_mysql_source_dialect_null_ordering(ec_keys):
    """MySQL source dialect also gets source-dialect default made explicit when targeting postgres/duckdb."""
    priv, pub = ec_keys

    placement = Placement.from_dict({
        "engines": {
            "duck": {"kind": "duckdb", "dialect": "duckdb"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "tables": {
            "items": {
                "keys": ["id"],
                "row_count": 5,
                "serve": {"duck": "items", "lb": "items"},
            }
        },
        "cubes": {},
    })
    schemas = {"items": ["id", "val"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )

    # MySQL -> Postgres
    sql_asc = "SELECT val FROM items ORDER BY val ASC"
    token_asc = _make_token(priv, sql_asc, sql_dialect="mysql")
    prep_pg_asc = pipeline.prepare(sql_asc, token_asc, hint="lb")
    assert "NULLS FIRST" in prep_pg_asc.sql

    sql_desc = "SELECT val FROM items ORDER BY val DESC"
    token_desc = _make_token(priv, sql_desc, sql_dialect="mysql")
    prep_pg_desc = pipeline.prepare(sql_desc, token_desc, hint="lb")
    assert "NULLS LAST" in prep_pg_desc.sql

    # MySQL -> DuckDB
    prep_duck_asc = pipeline.prepare(sql_asc, token_asc, hint="duck")
    assert "NULLS FIRST" in prep_duck_asc.sql

    prep_duck_desc = pipeline.prepare(sql_desc, token_desc, hint="duck")
    assert "NULLS LAST" in prep_duck_desc.sql
