"""Tests for DuckDB iceberg_direct snapshot_id pin rewrite in Router and ServingPipeline."""

from __future__ import annotations

import hashlib
import time

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import Placement, ServingPipeline, sign_grant_token
from seeknal.serving.placement import MAX_SIGNED_INT64, PlacementError, TableServe
from seeknal.serving.router import Router


def _assert_duckdb_parses_single_select(sql: str) -> None:
    con = duckdb.connect(":memory:")
    try:
        stmts = con.extract_statements(sql)
        assert len(stmts) == 1
        assert stmts[0].type == duckdb.StatementType.SELECT
    finally:
        con.close()


def test_router_rewrite_pinned_snapshot_standard_three_part():
    placement = Placement.from_dict(
        {
            "engines": {"duck_ice": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "lakekeeper.analytics.claims": {
                    "keys": ["claim_id"],
                    "row_count": 500000,
                    "serve": {
                        "duck_ice": {
                            "name": "lake.analytics.claims",
                            "max_lag_seconds": 60.0,
                            "snapshot_id": 8877665544332211,
                        }
                    },
                }
            },
        }
    )
    router = Router(placement)
    decision = router.route(
        "SELECT claim_id, amount FROM lakekeeper.analytics.claims WHERE claim_id = 10"
    )
    assert decision.engine == "duck_ice"
    assert (
        decision.sql
        == "SELECT claim_id, amount FROM lake.analytics.claims AS claims AT (VERSION => 8877665544332211) WHERE claim_id = 10"
    )
    _assert_duckdb_parses_single_select(decision.sql)


def test_router_rewrite_pinned_snapshot_nested_namespaces_and_quoting():
    placement = Placement.from_dict(
        {
            "engines": {"duck_ice": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "lakekeeper.nested.events": {
                    "keys": ["id"],
                    "row_count": 1000,
                    "serve": {
                        "duck_ice": {
                            "name": "lake.ns_level1.ns_level2.events",
                            "snapshot_id": 123456789,
                        }
                    },
                },
                "lakekeeper.quoted.special": {
                    "keys": ["id"],
                    "row_count": 1000,
                    "serve": {
                        "duck_ice": {
                            "name": '"lake"."my-ns"."sub.ns"."my-table"',
                            "snapshot_id": 999888777,
                        }
                    },
                },
            },
        }
    )
    router = Router(placement)

    # Nested namespace without explicit alias
    d1 = router.route("SELECT id FROM lakekeeper.nested.events")
    assert (
        d1.sql
        == "SELECT id FROM lake.ns_level1.ns_level2.events AS events AT (VERSION => 123456789)"
    )
    _assert_duckdb_parses_single_select(d1.sql)

    # Quoted identifiers and explicit alias
    d2 = router.route('SELECT s.id FROM lakekeeper.quoted.special AS s')
    assert (
        d2.sql
        == 'SELECT s.id FROM "lake"."my-ns"."sub.ns"."my-table" AS s AT (VERSION => 999888777)'
    )
    _assert_duckdb_parses_single_select(d2.sql)


def test_router_rewrite_join_mixed_pinned_and_unpinned_tables():
    placement = Placement.from_dict(
        {
            "engines": {"duck_ice": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "lakekeeper.analytics.orders": {
                    "keys": ["order_id"],
                    "row_count": 10000,
                    "serve": {
                        "duck_ice": {
                            "name": "lake.analytics.orders",
                            "snapshot_id": 424242,
                        }
                    },
                },
                "lakekeeper.analytics.customers": {
                    "keys": ["customer_id"],
                    "row_count": 500,
                    "serve": {
                        "duck_ice": {
                            "name": "lake.analytics.customers",
                        }
                    },
                },
            },
        }
    )
    router = Router(placement)
    d = router.route(
        "SELECT o.order_id, c.name FROM lakekeeper.analytics.orders o "
        "JOIN lakekeeper.analytics.customers c ON o.customer_id = c.customer_id"
    )
    assert "lake.analytics.orders AS o AT (VERSION => 424242)" in d.sql
    assert "JOIN lake.analytics.customers AS c" in d.sql
    assert "customers AS c AT (" not in d.sql
    _assert_duckdb_parses_single_select(d.sql)


def test_serving_pipeline_round_trip_guard_passes_with_pinned_snapshot():
    """ServingPipeline.prepare() (including _check_round_trip) succeeds when snapshot_id is pinned and DuckDB parses the rendered SQL."""
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()

    placement = Placement.from_dict(
        {
            "engines": {"duck_ice": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "lakekeeper.analytics.orders": {
                    "keys": ["order_id"],
                    "row_count": 1000,
                    "serve": {
                        "duck_ice": {
                            "name": "lake.analytics.sub_ns.orders",
                            "snapshot_id": 555666777,
                        }
                    },
                }
            },
        }
    )
    schemas = {"lakekeeper.analytics.orders": ["order_id", "customer_id", "total"]}
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
        default_catalog="lakekeeper",
    )

    sql = "SELECT order_id, total FROM analytics.orders WHERE customer_id = $1"
    now = time.time()
    token = sign_grant_token(
        {
            "typ": "grant",
            "sub": "user@example.com",
            "ws": "ws-1",
            "jti": "jti-snap-1",
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 60,
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "max_rows": 100,
            "tables": {
                "lakekeeper.analytics.orders": {
                    "columns": ["order_id", "customer_id", "total"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        },
        priv,
        kid="k1",
    )

    prepared = pipeline.prepare(sql, token, params=(42,), now=now)
    assert prepared.engine == "duck_ice"
    assert (
        "FROM lake.analytics.sub_ns.orders AS orders AT (VERSION => 555666777)"
        in prepared.sql
    )
    assert prepared.params == (42,)
    _assert_duckdb_parses_single_select(prepared.sql)


@pytest.mark.parametrize(
    "bad_snap",
    [
        True,
        False,
        0,
        -1,
        -99999,
        12.34,
        "12345",
        "0x1234",
        "",
        "   ",
        MAX_SIGNED_INT64 + 1,
        1 << 64,
    ],
)
def test_placement_rejects_invalid_snapshot_id(bad_snap):
    with pytest.raises(PlacementError, match="snapshot_id"):
        Placement.from_dict(
            {
                "engines": {"duck_ice": {"kind": "duckdb", "dialect": "duckdb"}},
                "tables": {
                    "t": {
                        "keys": ["id"],
                        "serve": {
                            "duck_ice": {"name": "lake.ns.t", "snapshot_id": bad_snap}
                        },
                    }
                },
            }
        )
    with pytest.raises(PlacementError, match="snapshot_id"):
        TableServe(engine="duck_ice", physical_name="lake.ns.t", snapshot_id=bad_snap)  # type: ignore[arg-type]

