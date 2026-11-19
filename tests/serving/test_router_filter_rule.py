"""Tests for the §4.3 filter routing rule, Postgres-wire preference, and writability."""

from __future__ import annotations

import pytest

from seeknal.serving.placement import Placement
from seeknal.serving.router import PiiCrossEngineError, Router, RoutingError


@pytest.fixture
def mirrored_router() -> Router:
    """Router where lb_ops is declared BEFORE pg_report and lake serves analytical copies."""
    placement = Placement.from_dict(
        {
            "engines": {
                "lb_ops": {"kind": "lakebase", "dialect": "postgres"},
                "pg_report": {"kind": "postgres", "dialect": "postgres"},
                "pg_secondary": {"kind": "postgres", "dialect": "postgres"},
                "lake": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "cbn.rpt.txn_detail": {
                    "keys": ["txn_id"],
                    "row_count": 25_000_000,
                    "serve": {
                        "lb_ops": "ops.txn_detail",
                        "pg_report": "rpt.txn_detail",
                        "pg_secondary": "rpt2.txn_detail",
                        "lake": "lake.rpt.txn_detail",
                    },
                    "access": {
                        "pattern": "filter",
                        "keys": ["txn_id"],
                        "filterable": ["customer_id", "as_of_date", "branch_code"],
                        "range_filterable": ["as_of_date"],
                        "prefix_filterable": ["merchant_code"],
                        "sortable": ["as_of_date", "txn_id"],
                        "max_rows": 5000,
                    },
                },
                "cbn.rpt.branch_dim": {
                    "keys": ["branch_code"],
                    "row_count": 200,
                    "serve": {
                        "pg_report": "rpt.branch_dim",
                        "lake": "lake.rpt.branch_dim",
                    },
                    "access": {
                        "pattern": "scan",
                        "keys": ["branch_code"],
                        "filterable": ["branch_code", "region"],
                    },
                },
                "ops.ticket": {
                    "keys": ["ticket_id"],
                    "row_count": 50_000,
                    "serve": {"lb_ops": "ops.ticket"},
                    "access": {
                        "pattern": "filter",
                        "keys": ["ticket_id"],
                        "filterable": ["assignee"],
                    },
                },
                "cbn.src_pii.customer": {
                    "keys": ["customer_id"],
                    "pii": True,
                    "serve": {"pg_report": "src_pii.customer"},
                    "access": {
                        "pattern": "point",
                        "keys": ["customer_id"],
                        "filterable": ["customer_id", "msisdn_hash"],
                    },
                },
            },
            "policy": {
                "point_lookup_max_keys": 100,
                "analytics_min_rows": 1_000_000,
                "max_in_list": 5,
            },
        }
    )
    return Router(placement)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001'",
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE branch_code IN ('B1', 'B2')",
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE (branch_code = 'B1' OR branch_code = 'B2')",
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE as_of_date >= '2026-10-01'",
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE as_of_date BETWEEN '2026-10-01' AND '2026-10-31'",
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE merchant_code LIKE 'MRC01%'",
    ],
)
def test_filter_rule_routes_to_postgres_over_lakebase_and_placement_order(
    mirrored_router: Router, sql: str
):
    decision = mirrored_router.route(sql)
    # 'pg_report' wins over 'lb_ops' (postgres preferred over lakebase)
    # and over 'pg_secondary' (placement order among postgres engines)
    assert decision.engine == "pg_report"
    assert decision.rule == "filter"
    assert "rpt.txn_detail AS txn_detail" in decision.sql


def test_point_lookup_rule_runs_before_filter_and_prefers_postgres(
    mirrored_router: Router,
):
    decision = mirrored_router.route(
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE txn_id = 999"
    )
    assert decision.engine == "pg_report"
    assert decision.rule == "point_lookup"

    # 1-level plain-projection derived table also routes via point_lookup / filter
    d_derived_pt = mirrored_router.route(
        "SELECT tid, amount FROM (SELECT txn_id AS tid, amount FROM cbn.rpt.txn_detail) v WHERE tid = 999"
    )
    assert d_derived_pt.engine == "pg_report"
    assert d_derived_pt.rule == "point_lookup"

    d_derived_filt = mirrored_router.route(
        "SELECT cid, amount FROM (SELECT customer_id AS cid, amount FROM cbn.rpt.txn_detail) v WHERE cid = 'C-1'"
    )
    assert d_derived_filt.engine == "pg_report"
    assert d_derived_filt.rule == "filter"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT branch_code, SUM(amount) FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001' GROUP BY branch_code",
        "SELECT COUNT(*) FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001'",
        "SELECT DISTINCT branch_code FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001'",
        "SELECT txn_id, SUM(amount) OVER (PARTITION BY customer_id) FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001'",
    ],
)
def test_aggregates_window_and_distinct_on_mirrored_tables_route_to_lake(
    mirrored_router: Router, sql: str
):
    decision = mirrored_router.route(sql)
    assert decision.engine == "lake"
    assert decision.rule == "analytics"


@pytest.mark.parametrize(
    "sql",
    [
        # Unfiltered scan on large mirrored table -> lake
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail",
        # Non-filterable column -> lake
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE amount > 500",
        # Wrapped column (CAST) -> not sargable -> lake
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE CAST(customer_id AS text) = 'C-1'",
        # Range on filterable-only (not range_filterable) -> lake
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE branch_code >= 'B01'",
        # IN list exceeding max_in_list (6 > 5) -> lake
        "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE branch_code IN ('1','2','3','4','5','6')",
    ],
)
def test_non_matching_filters_fall_through_to_analytics_lake(
    mirrored_router: Router, sql: str
):
    decision = mirrored_router.route(sql)
    assert decision.engine == "lake"
    assert decision.rule == "analytics"


def test_same_source_join_and_cross_source_join_rejection(mirrored_router: Router):
    # Same-source join on pg_report with sargable filter routes to pg_report via filter rule
    d_join = mirrored_router.route(
        "SELECT t.txn_id, b.branch_code "
        "FROM cbn.rpt.txn_detail t "
        "JOIN cbn.rpt.branch_dim b ON t.branch_code = b.branch_code "
        "WHERE t.customer_id = 'C-1001'"
    )
    assert d_join.engine == "pg_report"
    assert d_join.rule == "filter"

    # Cross-source join (pg_report/lake <-> lb_ops-only ops.ticket where lb_ops doesn't serve branch_dim)
    with pytest.raises(RoutingError) as exc_info:
        mirrored_router.route(
            "SELECT b.branch_code, k.ticket_id "
            "FROM cbn.rpt.branch_dim b JOIN ops.ticket k ON b.branch_code = k.ticket_id"
        )
    assert exc_info.value.code == "query_spans_engines"

    # Cross-source join involving PII table raises pii_cross_engine first (even with a hint)
    for hint_val in (None, "lb_ops", "pg_report"):
        with pytest.raises(PiiCrossEngineError) as pii_exc:
            mirrored_router.route(
                "SELECT c.customer_id, k.ticket_id "
                "FROM cbn.src_pii.customer c JOIN ops.ticket k ON c.customer_id = k.ticket_id",
                hint=hint_val,
            )
        assert pii_exc.value.code == "pii_cross_engine"


def test_writes_only_route_to_writable_postgres_wire_sources():
    placement = Placement.from_dict(
        {
            "engines": {
                "pg_ro": {"kind": "postgres", "dialect": "postgres", "mode": "read_only"},
                "lb_ro": {"kind": "lakebase", "dialect": "postgres", "endpoint_type": "read_only"},
                "pg_rw": {"kind": "postgres", "dialect": "postgres", "mode": "read_write"},
                "lb_rw": {"kind": "lakebase", "dialect": "postgres"},
                "lake": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "rw_table": {
                    "keys": ["id"],
                    "serve": {
                        "pg_ro": "rpt.rw_table",
                        "lb_ro": "snap.rw_table",
                        "pg_rw": "app.rw_table",
                        "lb_rw": "ops.rw_table",
                        "lake": "lake.rw_table",
                    },
                },
                "ro_only_table": {
                    "keys": ["id"],
                    "serve": {
                        "pg_ro": "rpt.ro_only",
                        "lb_ro": "snap.ro_only",
                        "lake": "lake.ro_only",
                    },
                },
            },
        }
    )
    r = Router(placement)

    # Unhinted write picks writable postgres-wire ('pg_rw' preferred over 'lb_rw', skipping read_only)
    d = r.route("INSERT INTO rw_table (id) VALUES (1)")
    assert d.engine == "pg_rw"
    assert d.rule == "write"

    # Hinted write to lb_rw succeeds
    d_lb = r.route("UPDATE rw_table SET id = 2 WHERE id = 1", hint="lb_rw")
    assert d_lb.engine == "lb_rw"
    assert d_lb.rule == "hint"

    # Hinted write to read-only postgres or read-only lakebase is rejected
    for ro_eng in ("pg_ro", "lb_ro", "lake"):
        with pytest.raises(RoutingError, match="not writable"):
            r.route("DELETE FROM rw_table WHERE id = 1", hint=ro_eng)

    # Write to table served only by read-only engines is rejected before any connection
    with pytest.raises(RoutingError, match="no feasible writable engine"):
        r.route("INSERT INTO ro_only_table (id) VALUES (1)")
