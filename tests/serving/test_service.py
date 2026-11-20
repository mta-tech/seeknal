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
    pipeline = ServingPipeline(
        placement=pipeline.placement,
        schemas=pipeline.schemas,
        public_keys=pipeline.public_keys,
        cube_mode="jwt",
    )
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


def _pilot_orders_pipeline(ec_key, *, cache_max_entries: int = 256):
    priv, pub = ec_key
    placement = Placement.from_dict(
        {
            "engines": {
                "sr": {"kind": "starrocks", "dialect": "starrocks"},
                "lb": {"kind": "lakebase", "dialect": "postgres"},
                "duck": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "lakekeeper.analytics.orders": {
                    "keys": ["id"],
                    "row_count": 5_000_000,
                    "serve": {
                        "sr": "lakekeeper.analytics.orders",
                        "lb": "public.orders",
                        "duck": "orders_tbl",
                    },
                }
            },
            "cubes": {},
        }
    )
    schemas = {
        "lakekeeper.analytics.orders": [
            "id",
            "customer_id",
            "region",
            "order_month",
            "total",
        ]
    }
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-atlas": pub},
        default_catalog="lakekeeper",
        compilation_cache_max_entries=cache_max_entries,
    )
    return pipeline, priv


def test_sealed_pilots_order_by_id_and_orders_by_month_with_cache_and_duckdb(
    ec_key, monkeypatch
):
    """Verify both sealed pilot operations (order_by_id v1, orders_by_month v1), cache reuse, and DuckDB execution."""
    import sqlglot
    from seeknal.serving.parameter_binding import parameter_digest

    pipeline, priv = _pilot_orders_pipeline(ec_key)

    sql_point = "SELECT id, customer_id, total FROM lakekeeper.analytics.orders WHERE id = $1 LIMIT 1"
    sql_report = (
        "SELECT region, SUM(total) AS total "
        "FROM lakekeeper.analytics.orders "
        "WHERE order_month = $1 "
        "GROUP BY region ORDER BY region LIMIT 100"
    )

    real_parse = sqlglot.parse
    template_parse_calls = 0

    def counting_parse(sql_str, *args, **kwargs):
        nonlocal template_parse_calls
        if sql_str in (sql_point, sql_report):
            template_parse_calls += 1
        return real_parse(sql_str, *args, **kwargs)

    monkeypatch.setattr(sqlglot, "parse", counting_parse)

    con = duckdb.connect()
    con.execute(
        "CREATE TABLE orders_tbl (id INT, customer_id VARCHAR, region VARCHAR, order_month INT, total INT)"
    )
    con.execute(
        "INSERT INTO orders_tbl VALUES "
        "(10, 'cust-a', 'WEST', 11, 150), "
        "(11, 'cust-b', 'WEST', 11, 250), "
        "(12, 'cust-c', 'EAST', 11, 300), "
        "(13, 'cust-d', 'WEST', 12, 500)"
    )

    def make_named_grant(
        sql: str,
        params: list,
        *,
        op_id: str,
        op_ver: int = 1,
        workload: str,
        max_rows: int,
        jti: str,
        sub: str = "analyst@atlas.org",
        masks: dict | None = None,
        row_filters: list | None = None,
    ) -> str:
        now = time.time()
        claims = {
            "sub": sub,
            "ws": "ws-orders",
            "jti": jti,
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 120,
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "params_sha256": parameter_digest(params),
            "operation_id": op_id,
            "operation_version": op_ver,
            "sql_dialect": "postgres",
            "workload": workload,
            "max_rows": max_rows,
            "tables": {
                "lakekeeper.analytics.orders": {
                    "columns": [
                        c
                        for c in ["id", "customer_id", "region", "order_month", "total"]
                        if not masks or c not in masks
                    ],
                    "masks": masks or {},
                    "row_filters": row_filters or [],
                    "actions": ["select"],
                }
            },
        }
        return sign_grant_token(claims, priv, kid="key-atlas")

    # 1. Pilot 1: order_by_id v1 (point_lookup -> routes to lb by default, or duck with hint)
    tok_p1 = make_named_grant(
        sql_point, [10], op_id="order_by_id", workload="interactive", max_rows=1, jti="j-p1"
    )
    prep_p1 = pipeline.prepare(sql_point, tok_p1, params=[10])
    assert prep_p1.engine == "lb"
    assert prep_p1.rule == "point_lookup"
    assert prep_p1.params == (10,)
    assert prep_p1.row_limit == 2
    assert template_parse_calls == 1
    assert pipeline.compilation_cache_info() == {
        "hits": 0,
        "misses": 1,
        "evictions": 0,
        "size": 1,
        "max_entries": 256,
    }

    # Execute same template with a different parameter value [11] and DuckDB hint -> cache hit!
    tok_p2 = make_named_grant(
        sql_point,
        [11],
        op_id="order_by_id",
        workload="interactive",
        max_rows=1,
        jti="j-p2",
        masks={"customer_id": "redact"},
    )
    prep_p2 = pipeline.prepare(sql_point, tok_p2, params=[11], hint="duck")
    assert prep_p2.engine == "duck"
    assert prep_p2.params == (11,)
    assert template_parse_calls == 1  # No second parse of sql_point!
    assert pipeline.compilation_cache_info()["hits"] == 1
    rows_p2 = con.execute(prep_p2.sql, prep_p2.params).fetchall()
    assert rows_p2 == [(11, "****", 250)]

    # 2. Pilot 2: orders_by_month v1 (report -> routes to sr by default, or duck with hint)
    tok_r1 = make_named_grant(
        sql_report, [11], op_id="orders_by_month", workload="report", max_rows=100, jti="j-r1"
    )
    prep_r1 = pipeline.prepare(sql_report, tok_r1, params=[11], hint="duck")
    assert prep_r1.workload == "report"
    assert prep_r1.params == (11,)
    assert template_parse_calls == 2
    rows_r1 = con.execute(prep_r1.sql, prep_r1.params).fetchall()
    assert rows_r1 == [("EAST", 300), ("WEST", 400)]

    # Second execution of orders_by_month with row filter for a different subject -> cache hit + fresh policy!
    tok_r2 = make_named_grant(
        sql_report,
        [11],
        op_id="orders_by_month",
        workload="report",
        max_rows=100,
        jti="j-r2",
        sub="west-only@atlas.org",
        row_filters=[{"column": "region", "op": "eq", "values": ["WEST"]}],
    )
    prep_r2 = pipeline.prepare(sql_report, tok_r2, params=[11], hint="duck")
    assert prep_r2.subject == "west-only@atlas.org"
    assert template_parse_calls == 2  # Cache hit!
    assert pipeline.compilation_cache_info()["hits"] == 2
    rows_r2 = con.execute(prep_r2.sql, prep_r2.params).fetchall()
    assert rows_r2 == [("WEST", 400)]


def test_tampering_and_missing_digest_rejected_before_cache_lookup(ec_key):
    """SQL or parameter tampering and incomplete operation claims fail before any cache lookup."""
    from seeknal.serving.parameter_binding import parameter_digest

    pipeline, priv = _pilot_orders_pipeline(ec_key)
    sql = "SELECT id, customer_id, total FROM lakekeeper.analytics.orders WHERE id = $1 LIMIT 1"
    now = time.time()

    base_claims = {
        "sub": "analyst@atlas.org",
        "ws": "ws-orders",
        "jti": "j-tamper-1",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "params_sha256": parameter_digest([10]),
        "operation_id": "order_by_id",
        "operation_version": 1,
        "sql_dialect": "postgres",
        "max_rows": 1,
        "tables": {
            "lakekeeper.analytics.orders": {
                "columns": ["id", "customer_id", "total"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }
    token = sign_grant_token(base_claims, priv, kid="key-atlas")

    # 1. Parameter value tampered ([999] instead of signed [10])
    with pytest.raises(EnforcementError, match="parameter SHA256 mismatch") as exc_info:
        pipeline.prepare(sql, token, params=[999])
    assert "999" not in str(exc_info.value)
    assert pipeline.compilation_cache_info() == {
        "hits": 0,
        "misses": 0,
        "evictions": 0,
        "size": 0,
        "max_entries": 256,
    }

    # 2. SQL text tampered
    with pytest.raises(EnforcementError, match="SQL SHA256 mismatch"):
        pipeline.prepare(sql + " ", token, params=[10])
    assert pipeline.compilation_cache_info()["misses"] == 0

    # 3. Downgrade attempt: operation_id + operation_version without params_sha256
    downgraded_claims = dict(base_claims)
    del downgraded_claims["params_sha256"]
    downgraded_token = sign_grant_token(downgraded_claims, priv, kid="key-atlas")
    with pytest.raises(EnforcementError, match="named operation grant requires"):
        pipeline.prepare(sql, downgraded_token, params=[10])
    assert pipeline.compilation_cache_info()["misses"] == 0


def test_legacy_and_oversized_and_invalid_sql_cache_admission_rules(ec_key):
    """Legacy grants, digest-only grants, >32KiB SQL, and invalid/multi-statement SQL do not enter cache."""
    from seeknal.serving.parameter_binding import parameter_digest

    pipeline, priv = _pilot_orders_pipeline(ec_key)
    sql = "SELECT id, total FROM lakekeeper.analytics.orders WHERE id = $1 LIMIT 1"
    now = time.time()
    tables_grant = {
        "lakekeeper.analytics.orders": {
            "columns": ["id", "total"],
            "masks": {},
            "row_filters": [],
            "actions": ["select"],
        }
    }

    # 1. Legacy parameterized query without new claims works and bypasses compilation cache
    tok_legacy = _make_token(priv, sql, tables_grant, max_rows=1)
    prep_legacy = pipeline.prepare(sql, tok_legacy, params=[42])
    assert prep_legacy.params == (42,)
    assert pipeline.compilation_cache_info()["misses"] == 0
    assert pipeline.compilation_cache_info()["size"] == 0

    # 2. Claimed params_sha256 without operation_id/operation_version is verified but not cached
    claims_digest_only = {
        "sub": "analyst@atlas.org",
        "ws": "ws-orders",
        "jti": "j-digest-only",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "params_sha256": parameter_digest([42]),
        "max_rows": 1,
        "tables": tables_grant,
    }
    tok_digest_only = sign_grant_token(claims_digest_only, priv, kid="key-atlas")
    prep_do = pipeline.prepare(sql, tok_digest_only, params=[42])
    assert prep_do.params == (42,)
    assert pipeline.compilation_cache_info()["size"] == 0
    with pytest.raises(EnforcementError, match="parameter SHA256 mismatch"):
        pipeline.prepare(sql, tok_digest_only, params=[43])

    # 3. Named operation with SQL > 32KiB executes but is not admitted to compilation cache
    padding = " " * 33_000
    big_sql = f"SELECT id, total FROM lakekeeper.analytics.orders WHERE id = $1{padding}LIMIT 1"
    assert len(big_sql.encode("utf-8")) > 32_768
    claims_big = {
        **claims_digest_only,
        "sql_sha256": hashlib.sha256(big_sql.encode("utf-8")).hexdigest(),
        "operation_id": "order_by_id",
        "operation_version": 1,
    }
    tok_big = sign_grant_token(claims_big, priv, kid="key-atlas")
    prep_big = pipeline.prepare(big_sql, tok_big, params=[42])
    assert prep_big.params == (42,)
    assert pipeline.compilation_cache_info()["size"] == 0
    assert pipeline.compilation_cache_info()["misses"] == 0

    # 4. Multi-statement SQL with valid named operation claims is rejected and not cached
    multi_sql = "SELECT id FROM lakekeeper.analytics.orders WHERE id = $1; SELECT id FROM lakekeeper.analytics.orders"
    claims_multi = {
        **claims_digest_only,
        "sql_sha256": hashlib.sha256(multi_sql.encode("utf-8")).hexdigest(),
        "operation_id": "order_by_id",
        "operation_version": 1,
    }
    tok_multi = sign_grant_token(claims_multi, priv, kid="key-atlas")
    with pytest.raises(EnforcementError, match="expected exactly one statement"):
        pipeline.prepare(multi_sql, tok_multi, params=[42])
    assert pipeline.compilation_cache_info()["size"] == 0


def test_zero_placeholder_rejected_and_legacy_leading_zero_compatible(ec_key):
    """$0 is rejected while legacy $01 placeholder compatibility is preserved."""
    pipeline, priv = _pilot_orders_pipeline(ec_key)
    tables_grant = {
        "lakekeeper.analytics.orders": {
            "columns": ["id"],
            "masks": {},
            "row_filters": [],
            "actions": ["select"],
        }
    }

    zero_sql = "SELECT id FROM lakekeeper.analytics.orders WHERE id = $0"
    tok_zero = _make_token(priv, zero_sql, tables_grant)
    with pytest.raises(EnforcementError, match=r"Placeholder index must be >= 1, got \$0"):
        pipeline.prepare(zero_sql, tok_zero, params=[1])

    legacy_leading_zero_sql = "SELECT id FROM lakekeeper.analytics.orders WHERE id = $01"
    tok_legacy = _make_token(priv, legacy_leading_zero_sql, tables_grant)
    prep = pipeline.prepare(legacy_leading_zero_sql, tok_legacy, params=[42])
    assert prep.params == (42,)


def test_disabled_cache_bypasses_get_put_and_ast_copy_budget_controls(
    ec_key, monkeypatch
):
    """Verify capacity=0 bypasses cache get/put completely and check AST copy budget across legacy/disabled/miss/hit."""
    from sqlglot import exp
    from seeknal.serving.parameter_binding import parameter_digest

    pipeline_disabled, priv = _pilot_orders_pipeline(ec_key, cache_max_entries=0)
    pipeline_enabled, _ = _pilot_orders_pipeline(ec_key, cache_max_entries=256)

    # Use two-part table name 'analytics.orders' to also exercise default_catalog canonicalization
    sql = "SELECT id, total FROM analytics.orders WHERE id = $1 LIMIT 1"
    now = time.time()
    tables_grant = {
        "lakekeeper.analytics.orders": {
            "columns": ["id", "total"],
            "masks": {},
            "row_filters": [],
            "actions": ["select"],
        }
    }

    def make_tok(params: list, *, named: bool, jti: str) -> str:
        claims = {
            "sub": "analyst@atlas.org",
            "ws": "ws-orders",
            "jti": jti,
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 120,
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "sql_dialect": "postgres",
            "max_rows": 1,
            "tables": tables_grant,
        }
        if named:
            claims["params_sha256"] = parameter_digest(params)
            claims["operation_id"] = "order_by_id"
            claims["operation_version"] = 1
        return sign_grant_token(claims, priv, kid="key-atlas")

    real_copy = exp.Expression.copy
    copy_count = 0

    def counting_copy(self, *args, **kwargs):
        nonlocal copy_count
        copy_count += 1
        return real_copy(self, *args, **kwargs)

    monkeypatch.setattr(exp.Expression, "copy", counting_copy)

    # 1. Legacy path (no named operation claims)
    copy_count = 0
    prep_legacy = pipeline_enabled.prepare(
        sql, make_tok([10], named=False, jti="j-leg"), params=[10]
    )
    legacy_copies = copy_count
    assert prep_legacy.params == (10,)
    assert pipeline_enabled.compilation_cache_info() == {
        "hits": 0,
        "misses": 0,
        "evictions": 0,
        "size": 0,
        "max_entries": 256,
    }

    # 2. Disabled cache mode (compilation_cache_max_entries=0) with valid named operation claims:
    # Must bypass cache get/put completely (misses == 0, hits == 0, size == 0) and match legacy copy budget!
    copy_count = 0
    prep_disabled = pipeline_disabled.prepare(
        sql, make_tok([10], named=True, jti="j-dis"), params=[10]
    )
    disabled_copies = copy_count
    assert prep_disabled.params == (10,)
    assert disabled_copies == legacy_copies
    assert pipeline_disabled.compilation_cache_info() == {
        "hits": 0,
        "misses": 0,
        "evictions": 0,
        "size": 0,
        "max_entries": 0,
    }

    # 3. Enabled cache miss: stores 1 copy in cache.put before canonicalization, no extra pipeline copy!
    copy_count = 0
    prep_miss = pipeline_enabled.prepare(
        sql, make_tok([10], named=True, jti="j-miss"), params=[10]
    )
    miss_copies = copy_count
    assert prep_miss.params == (10,)
    assert miss_copies == legacy_copies + 1
    assert pipeline_enabled.compilation_cache_info() == {
        "hits": 0,
        "misses": 1,
        "evictions": 0,
        "size": 1,
        "max_entries": 256,
    }

    # Verify cached raw AST in _compilation_cache was NOT mutated by default_catalog canonicalization
    cached_key = next(iter(pipeline_enabled._compilation_cache._entries.keys()))
    cached_raw_ast = pipeline_enabled._compilation_cache._entries[cached_key]
    cached_tables = list(cached_raw_ast.find_all(exp.Table))
    assert len(cached_tables) == 1
    assert cached_tables[0].catalog == ""  # Raw two-part table preserved before canonicalization!

    # 4. Enabled cache hit: 1 copy in cache.get + same downstream enforce copies as legacy
    copy_count = 0
    prep_hit = pipeline_enabled.prepare(
        sql, make_tok([20], named=True, jti="j-hit"), params=[20]
    )
    hit_copies = copy_count
    assert prep_hit.params == (20,)
    assert hit_copies == legacy_copies + 1
    assert pipeline_enabled.compilation_cache_info()["hits"] == 1


def test_split_verify_grant_prepare_statement_and_postgres_guardrails(ec_key):
    """Verify verify_grant + prepare_statement split, kind='postgres' enforce bridge, and PreparedQuery fields."""
    import sqlglot
    from seeknal.serving.guardrails import GuardrailError

    priv, pub = ec_key
    placement = Placement.from_dict(
        {
            "engines": {
                "pg_report": {"kind": "postgres", "dialect": "postgres"},
                "lake": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "cbn.rpt.txn_detail": {
                    "row_count": 5_000_000,
                    "serve": {
                        "pg_report": "rpt.txn_detail",
                        "lake": "lake.rpt.txn_detail",
                    },
                    "access": {
                        "pattern": "filter",
                        "keys": ["txn_id"],
                        "filterable": ["customer_id", "as_of_date"],
                        "range_filterable": ["as_of_date"],
                        "sortable": ["as_of_date", "txn_id"],
                        "max_rows": 50,
                    },
                },
                "cbn.src_pii.customer": {
                    "pii": True,
                    "serve": {"pg_report": "src_pii.customer"},
                    "access": {
                        "pattern": "point",
                        "keys": ["customer_id"],
                        "filterable": ["customer_id"],
                        "sortable": ["customer_id"],
                        "max_rows": 25,
                    },
                },
            },
        }
    )
    schemas = {
        "cbn.rpt.txn_detail": ["txn_id", "customer_id", "as_of_date", "amount"],
        "cbn.src_pii.customer": ["customer_id", "full_name"],
    }
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"key-atlas": pub},
        default_catalog="cbn",
    )

    # 1. Filtered query on kind='postgres' via verify_grant + prepare_statement
    sql = "SELECT txn_id, amount FROM rpt.txn_detail WHERE customer_id = $1 ORDER BY as_of_date DESC"
    tok = _make_token(
        priv,
        sql,
        {
            "cbn.rpt.txn_detail": {
                "columns": ["txn_id", "customer_id", "as_of_date", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
        max_rows=500,
    )
    grant = pipeline.verify_grant(tok, sql=sql, params=("C-1001",))
    stmt = sqlglot.parse_one(sql, read="postgres")
    prep = pipeline.prepare_statement(
        stmt,
        grant,
        sql=sql,
        params=("C-1001",),
        max_rows=200,
        api="select.v1",
    )
    assert prep.engine == "pg_report"
    assert prep.engine_kind == "postgres"
    assert prep.rule == "filter"
    assert prep.paramstyle == "format"
    assert prep.params == ("C-1001",)
    assert prep.tables == ("cbn.rpt.txn_detail",)
    assert prep.access_pattern == "filter"
    assert prep.guard == "pass"
    assert prep.filter_columns == ("customer_id",)
    # min(grant=500, request=200, table.access.max_rows=50) == 50 -> row_limit = 51
    assert prep.effective_max_rows == 50
    assert prep.row_limit == 51
    assert prep.api == "select.v1"

    # 2. Row filter in grant must NOT satisfy require_filter when client SQL has no filter
    unfiltered_sql = "SELECT customer_id, full_name FROM src_pii.customer"
    tok_unfiltered = _make_token(
        priv,
        unfiltered_sql,
        {
            "cbn.src_pii.customer": {
                "columns": ["customer_id"],
                "masks": {"full_name": "redact"},
                "row_filters": [
                    {"column": "customer_id", "op": "eq", "values": ["C-1001"]}
                ],
                "actions": ["select"],
            }
        },
    )
    with pytest.raises(GuardrailError) as exc_info:
        pipeline.prepare(unfiltered_sql, tok_unfiltered)
    assert exc_info.value.code == "filter_required"
    assert exc_info.value.audit_id == "jti-service-001"


def test_prepare_and_verify_grant_reject_non_null_api_claim(standard_pipeline):
    """ServingPipeline.prepare and default verify_grant must reject non-null 'api' claims directly."""
    from seeknal.serving.grant_token import GrantTokenError

    pipeline, priv = standard_pipeline
    sql = "SELECT user_id, username FROM users WHERE user_id = 1"
    tables_grant = {
        "users": {
            "columns": ["user_id", "username"],
            "masks": {},
            "row_filters": [],
            "actions": ["select"],
        }
    }

    def _mint(api_val, *, include_api: bool = True, jti: str = "jti-api-check") -> str:
        now = time.time()
        claims = {
            "sub": "analyst@atlas.org",
            "ws": "ws-claims",
            "jti": jti,
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 120,
            "sql_dialect": "postgres",
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "max_rows": 100,
            "tables": tables_grant,
        }
        if include_api:
            claims["api"] = api_val
        return sign_grant_token(claims, priv, kid="key-atlas")

    # 1. Direct pipeline.prepare rejects 'api': 'select.v1' and arbitrary non-null 'api'
    for idx, bad_api in enumerate(("select.v1", "custom.v1", 123)):
        jti = f"jti-api-prep-{idx}"
        tok = _mint(bad_api, jti=jti)
        with pytest.raises(GrantTokenError) as exc_info:
            pipeline.prepare(sql, tok)
        assert exc_info.value.code == "grant_invalid"
        assert exc_info.value.audit_id == jti
        assert exc_info.value.subject == "analyst@atlas.org"

        # Default verify_grant (SQL mode) also rejects non-null 'api'
        with pytest.raises(GrantTokenError) as vg_exc:
            pipeline.verify_grant(tok, sql=sql)
        assert vg_exc.value.code == "grant_invalid"
        assert vg_exc.value.audit_id == jti
        assert vg_exc.value.subject == "analyst@atlas.org"

    # 2. Explicit null 'api': None is permitted for SQL queries
    tok_null_api = _mint(None, jti="jti-api-null")
    prep_ok = pipeline.prepare(sql, tok_null_api)
    assert prep_ok.audit_id == "jti-api-null"
    assert prep_ok.api is None

    # 3. verify_grant with expected_api='select.v1' or allow_api_claim=True accepts select.v1 grant
    tok_select = _mint("select.v1", jti="jti-api-select-ok")
    g_expected = pipeline.verify_grant(
        tok_select, sql=sql, expected_api="select.v1"
    )
    assert g_expected.api == "select.v1"
    assert g_expected.audit_id == "jti-api-select-ok"

    g_allowed = pipeline.verify_grant(
        tok_select, sql=sql, allow_api_claim=True
    )
    assert g_allowed.api == "select.v1"

    # 4. verify_grant with expected_api='select.v1' rejects SQL grant lacking 'api'
    tok_no_api = _mint(None, include_api=False, jti="jti-no-api")
    with pytest.raises(GrantTokenError) as missing_api_exc:
        pipeline.verify_grant(tok_no_api, sql=sql, expected_api="select.v1")
    assert missing_api_exc.value.code == "grant_invalid"
    assert missing_api_exc.value.audit_id == "jti-no-api"


