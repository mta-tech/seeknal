"""Tests for table access pattern guardrails (seeknal.serving.guardrails, §4.2)."""

from __future__ import annotations

import hashlib
import time

import pytest
import sqlglot
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving.enforce import Grant
from seeknal.serving.grant_token import sign_grant_token
from seeknal.serving.guardrails import (
    GuardrailError,
    GuardrailResult,
    check_guardrails,
)
from seeknal.serving.http import create_app
from seeknal.serving.metrics import MetricsRegistry
from seeknal.serving.placement import Placement
from seeknal.serving.router import Router
from seeknal.serving.service import ServingPipeline


class FakeGuardMetrics:
    """Minimal metrics recorder for verifying guard_mode='warn' and 'enforce' counters."""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}
        self.warnings: dict[str, int] = {}

    def record_guard_rejection(self, code: str) -> None:
        self.counts[code] = self.counts.get(code, 0) + 1

    def record_guard_warning(self, code: str) -> None:
        self.warnings[code] = self.warnings.get(code, 0) + 1


def _make_placement(*, guard_mode: str = "enforce", max_in_list: int = 1000, max_offset: int = 10000) -> Placement:
    return Placement.from_dict(
        {
            "engines": {
                "pg_report": {"kind": "postgres", "dialect": "postgres"},
                "lake": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "cbn.src_pii.customer": {
                    "pii": True,
                    "serve": {"pg_report": "src_pii.customer"},
                    "access": {
                        "pattern": "point",
                        "keys": ["customer_id"],
                        "filterable": ["customer_id", "msisdn_hash"],
                        "prefix_filterable": ["email_prefix"],
                        "sortable": ["customer_id"],
                        "max_rows": 50,
                    },
                },
                "cbn.src_pii.composite_acct": {
                    "pii": True,
                    "serve": {"pg_report": "src_pii.composite_acct"},
                    "access": {
                        "pattern": "point",
                        "keys": ["bank_code", "acct_no"],
                        "filterable": ["bank_code", "acct_no", "tax_id_hash"],
                        "sortable": ["bank_code", "acct_no"],
                        "max_rows": 50,
                    },
                },
                "cbn.rpt.txn_detail": {
                    "row_count": 10_000_000,
                    "serve": {
                        "pg_report": "rpt.txn_detail",
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
                    "row_count": 120,
                    "serve": {
                        "pg_report": "rpt.branch_dim",
                        "lake": "lake.rpt.branch_dim",
                    },
                    "access": {
                        "pattern": "scan",
                        "keys": ["branch_code"],
                        "filterable": ["branch_code", "region"],
                        "sortable": ["branch_code"],
                        "require_filter": False,
                        "max_rows": 500,
                    },
                },
                "cbn.src_pii.implicit_pii": {
                    "keys": ["person_id"],
                    "pii": True,
                    "serve": {"pg_report": "src_pii.implicit_pii"},
                },
                "cbn.rpt.tokenized_customer": {
                    "keys": ["customer_tok"],
                    "pii": True,
                    "tokenized": True,
                    "row_count": 200,
                    "serve": {
                        "pg_report": "rpt.tokenized_customer",
                        "lake": "lake.rpt.tokenized_customer",
                    },
                },
            },
            "policy": {
                "point_lookup_max_keys": 5,
                "max_in_list": max_in_list,
                "max_offset": max_offset,
                "guard_mode": guard_mode,
            },
        }
    )


def _run_guard(
    sql: str,
    placement: Placement | None = None,
    *,
    hint: str | None = "pg_report",
    grant_max_rows: int = 1000,
    request_max_rows: int | None = None,
    metrics: FakeGuardMetrics | None = None,
    params: tuple | list | None = None,
) -> GuardrailResult:
    from seeknal.serving.features import _bind_feature_params

    pl = placement or _make_placement()
    router = Router(pl)
    stmt = sqlglot.parse_one(sql, read="postgres")
    with _bind_feature_params(params or ()):
        decision = router.decide_statement(stmt, hint=hint)

    class _StubGrant:
        max_rows = grant_max_rows

    return check_guardrails(
        stmt,
        decision=decision,
        placement=pl,
        grant=_StubGrant(),
        request_max_rows=request_max_rows,
        metrics=metrics,
        params=params,
    )


# ==============================================================================
# §4.2.1 & §4.2.2: Sargable conjuncts & require_filter (Accept + Reject)
# ==============================================================================


@pytest.mark.parametrize(
    "sql,expected_cols",
    [
        (
            "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001'",
            ("customer_id",),
        ),
        (
            "SELECT * FROM cbn.rpt.txn_detail WHERE 'C-1001' = customer_id",
            ("customer_id",),
        ),
        (
            "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = $1",
            ("customer_id",),
        ),
        (
            "SELECT * FROM cbn.rpt.txn_detail WHERE branch_code IN ('B01', 'B02', $1)",
            ("branch_code",),
        ),
        (
            "SELECT * FROM cbn.rpt.txn_detail WHERE (branch_code = 'B01' OR branch_code IN ('B02', 'B03'))",
            ("branch_code",),
        ),
        (
            "SELECT * FROM cbn.rpt.txn_detail WHERE as_of_date >= DATE '2026-10-01'",
            ("as_of_date",),
        ),
        (
            "SELECT * FROM cbn.rpt.txn_detail WHERE as_of_date BETWEEN $1 AND $2",
            ("as_of_date",),
        ),
        (
            "SELECT * FROM cbn.rpt.txn_detail WHERE merchant_code LIKE 'MRC01%'",
            ("merchant_code",),
        ),
        (
            r"SELECT * FROM cbn.rpt.txn_detail WHERE merchant_code LIKE 'MRC\_01%'",
            ("merchant_code",),
        ),
    ],
)
def test_filter_table_accepts_valid_sargable_conjuncts(sql: str, expected_cols: tuple[str, ...]):
    res = _run_guard(sql)
    assert res.outcome == "pass"
    assert res.access_pattern == "filter"
    assert res.filter_columns == expected_cols


def test_parameterised_prefix_like_satisfies_guardrails_and_routes_via_filter():
    res = _run_guard(
        "SELECT * FROM cbn.rpt.txn_detail WHERE merchant_code LIKE $1",
        hint=None,
        params=["MRC01%"],
    )
    assert res.outcome == "pass"
    assert res.filter_columns == ("merchant_code",)

    for bad_params in (["%MRC"], ["M_C%"], ["%"], []):
        with pytest.raises(GuardrailError) as exc_info:
            _run_guard(
                "SELECT * FROM cbn.rpt.txn_detail WHERE merchant_code LIKE $1",
                hint="pg_report",
                params=bad_params,
            )
        assert exc_info.value.code == "filter_required"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM cbn.rpt.txn_detail",
        "SELECT * FROM cbn.rpt.txn_detail WHERE CAST(customer_id AS text) = 'C-1'",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id::text = 'C-1'",
        "SELECT * FROM cbn.rpt.txn_detail WHERE lower(customer_id) = 'c-1'",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id IS NULL",
        "SELECT * FROM cbn.rpt.txn_detail WHERE 1 = 1 AND customer_id IS NOT NULL",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = NULL",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' OR true",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' OR branch_code = 'B1'",
        # Unindexed column
        "SELECT * FROM cbn.rpt.txn_detail WHERE amount = 100",
        # Range on filterable-only (not range_filterable) column
        "SELECT * FROM cbn.rpt.txn_detail WHERE branch_code >= 'B01'",
        "SELECT * FROM cbn.rpt.txn_detail WHERE branch_code BETWEEN 'B01' AND 'B09'",
        # Prefix LIKE on filterable-only (not prefix_filterable) column
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id LIKE 'C-10%'",
        # Non-prefix LIKE or ILIKE even on prefix_filterable column
        "SELECT * FROM cbn.rpt.txn_detail WHERE merchant_code LIKE '%MRC'",
        "SELECT * FROM cbn.rpt.txn_detail WHERE merchant_code LIKE 'M_C%'",
        "SELECT * FROM cbn.rpt.txn_detail WHERE merchant_code LIKE '%'",
        "SELECT * FROM cbn.rpt.txn_detail WHERE merchant_code ILIKE 'MRC%'",
        # Self-equality tautology
        "SELECT * FROM cbn.rpt.txn_detail t WHERE t.customer_id = t.customer_id",
    ],
)
def test_filter_table_rejects_missing_or_disqualified_predicates(sql: str):
    metrics = FakeGuardMetrics()
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(sql, metrics=metrics)
    assert exc_info.value.code == "filter_required"
    assert metrics.counts == {"filter_required": 1}


def test_in_list_and_or_eq_respect_max_in_list():
    pl = _make_placement(max_in_list=3)
    # 3 values <= max_in_list=3 -> passes
    res = _run_guard(
        "SELECT * FROM cbn.rpt.txn_detail WHERE branch_code IN ('A', 'B', 'C')",
        pl,
    )
    assert res.outcome == "pass"

    # 4 values > max_in_list=3 -> filter_required
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT * FROM cbn.rpt.txn_detail WHERE branch_code IN ('A', 'B', 'C', 'D')",
            pl,
        )
    assert exc_info.value.code == "filter_required"

    # 4 values via OR -> filter_required
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT * FROM cbn.rpt.txn_detail WHERE (branch_code = 'A' OR branch_code IN ('B', 'C', 'D'))",
            pl,
        )
    assert exc_info.value.code == "filter_required"


def test_point_table_accept_and_reject_rules():
    pl = _make_placement()  # point_lookup_max_keys == 5

    # Primary key equality / bounded IN -> accept
    r1 = _run_guard(
        "SELECT * FROM cbn.src_pii.customer WHERE customer_id IN ('C1', 'C2', 'C3')",
        pl,
        hint=None,
    )
    assert r1.outcome == "pass"
    assert r1.access_pattern == "point"

    # Secondary non-key filterable / prefix_filterable -> accept
    r2 = _run_guard(
        "SELECT * FROM cbn.src_pii.customer WHERE msisdn_hash = $1",
        pl,
        hint=None,
    )
    assert r2.outcome == "pass"
    r3 = _run_guard(
        "SELECT * FROM cbn.src_pii.customer WHERE email_prefix LIKE 'alice%'",
        pl,
        hint=None,
    )
    assert r3.outcome == "pass"

    # Key IN exceeding point_lookup_max_keys (6 > 5) must reject even though customer_id is in filterable
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT * FROM cbn.src_pii.customer WHERE customer_id IN ('1','2','3','4','5','6')",
            pl,
            hint=None,
        )
    assert exc_info.value.code == "filter_required"

    # Composite key: both keys constrained with product 2*2=4 <= 5 -> accept
    rc_ok = _run_guard(
        "SELECT * FROM cbn.src_pii.composite_acct WHERE bank_code IN ('001','002') AND acct_no IN ('A1','A2')",
        pl,
        hint=None,
    )
    assert rc_ok.outcome == "pass"

    # Composite key: only one key constrained -> reject
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT * FROM cbn.src_pii.composite_acct WHERE bank_code = '001'",
            pl,
            hint=None,
        )
    assert exc_info.value.code == "filter_required"

    # Composite key: product 3*2=6 > 5 -> reject
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT * FROM cbn.src_pii.composite_acct WHERE bank_code IN ('1','2','3') AND acct_no IN ('A','B')",
            pl,
            hint=None,
        )
    assert exc_info.value.code == "filter_required"


def test_scan_table_and_implicit_pii_vs_tokenized():
    pl = _make_placement()

    # Scan table (require_filter=False) allows full scan and arbitrary ORDER BY on Postgres
    r_scan = _run_guard(
        "SELECT * FROM cbn.rpt.branch_dim ORDER BY lower(region)",
        pl,
        hint="pg_report",
    )
    assert r_scan.outcome == "pass"
    assert r_scan.access_pattern == "scan"

    # Implicit PII table (pii=True, tokenized=False, access=None) requires filter
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard("SELECT * FROM cbn.src_pii.implicit_pii", pl, hint=None)
    assert exc_info.value.code == "filter_required"

    r_pii_ok = _run_guard(
        "SELECT * FROM cbn.src_pii.implicit_pii WHERE person_id = $1",
        pl,
        hint=None,
    )
    assert r_pii_ok.outcome == "pass"
    assert r_pii_ok.access_pattern == "point"

    # Tokenized table (pii=True, tokenized=True, access=None) is exempt from implicit require_filter
    r_tok = _run_guard(
        "SELECT * FROM cbn.rpt.tokenized_customer",
        pl,
        hint="pg_report",
    )
    assert r_tok.outcome == "pass"


# ==============================================================================
# §4.2.2(c): Same-source equi-join propagation (Accept + Reject)
# ==============================================================================


def test_same_source_equi_join_propagation():
    pl = _make_placement()

    # Accept: filtered scan/dim table equi-joined onto filter table's filterable column
    r_join_ok = _run_guard(
        "SELECT t.txn_id, b.branch_code "
        "FROM cbn.rpt.branch_dim b "
        "JOIN cbn.rpt.txn_detail t ON b.branch_code = t.branch_code "
        "WHERE b.region = 'WEST'",
        pl,
        hint="pg_report",
    )
    assert r_join_ok.outcome == "pass"
    assert "branch_code" in r_join_ok.filter_columns

    # Accept: top-level WHERE equi-join from filtered customer to txn_detail
    r_where_join = _run_guard(
        "SELECT c.customer_id, t.txn_id "
        "FROM cbn.src_pii.customer c, cbn.rpt.txn_detail t "
        "WHERE c.msisdn_hash = 'H-99' AND c.customer_id = t.customer_id",
        pl,
        hint=None,
    )
    assert r_where_join.outcome == "pass"

    # Reject: joining UNFILTERED scan table onto filter table must NOT satisfy require_filter
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT t.txn_id FROM cbn.rpt.branch_dim b "
            "JOIN cbn.rpt.txn_detail t ON b.branch_code = t.branch_code",
            pl,
            hint="pg_report",
        )
    assert exc_info.value.code == "filter_required"

    # Reject: equi-join onto non-filterable column of target table
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT t.txn_id FROM cbn.rpt.branch_dim b "
            "JOIN cbn.rpt.txn_detail t ON b.branch_code = t.unindexed_ref "
            "WHERE b.branch_code = 'B01'",
            pl,
            hint="pg_report",
        )
    assert exc_info.value.code == "filter_required"

    # Reject: LEFT JOIN where only right side has ON filter does not filter left table
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT t.txn_id FROM cbn.rpt.txn_detail t "
            "LEFT JOIN cbn.rpt.branch_dim b ON t.branch_code = b.branch_code AND b.region = 'WEST'",
            pl,
            hint="pg_report",
        )
    assert exc_info.value.code == "filter_required"

    # Accept legitimate equality join propagation from scan table to filter table
    r_legit_eq = _run_guard(
        "SELECT t.* FROM cbn.rpt.branch_dim b "
        "JOIN cbn.rpt.txn_detail t ON b.branch_code = t.branch_code "
        "WHERE b.branch_code = 'X'",
        pl,
        hint="pg_report",
    )
    assert r_legit_eq.outcome == "pass"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT c.* FROM cbn.rpt.branch_dim b JOIN cbn.src_pii.customer c ON c.customer_id = b.branch_code WHERE b.region >= ''",
        "SELECT c.* FROM cbn.rpt.branch_dim b JOIN cbn.src_pii.customer c ON c.customer_id = b.branch_code WHERE b.region LIKE 'a%'",
        "SELECT c.* FROM cbn.rpt.branch_dim b JOIN cbn.rpt.txn_detail t ON b.branch_code = t.branch_code JOIN cbn.src_pii.customer c ON c.customer_id = t.customer_id WHERE b.branch_code >= ''",
        "SELECT p.* FROM cbn.rpt.branch_dim b JOIN cbn.src_pii.implicit_pii p ON p.person_id = b.branch_code WHERE b.branch_code > ''",
        "SELECT c.* FROM cbn.rpt.tokenized_customer tc JOIN cbn.src_pii.customer c ON c.customer_id = tc.customer_tok WHERE tc.customer_tok >= ''",
        "SELECT c.* FROM cbn.rpt.tokenized_customer tc JOIN cbn.src_pii.customer c ON c.customer_id = tc.customer_tok WHERE tc.customer_tok LIKE 'a%'",
    ],
)
def test_join_propagation_rejects_unsargable_range_or_like_from_unguarded_tables(
    sql: str,
):
    pl = _make_placement()
    for hint_val in (None, "pg_report"):
        with pytest.raises(GuardrailError) as exc_info:
            _run_guard(sql, pl, hint=hint_val)
        assert exc_info.value.code == "filter_required"


# ==============================================================================
# §4.2.3: Derived tables (1-level transparent vs 2+ levels rejected)
# ==============================================================================


def test_derived_table_one_level_transparent_and_deeper_rejected():
    pl = _make_placement()

    # Accept: 1-level Superset virtual dataset with column alias resolved for WHERE and ORDER BY
    r_vd = _run_guard(
        "SELECT cid, dt, amount "
        "FROM (SELECT customer_id AS cid, as_of_date AS dt, amount FROM cbn.rpt.txn_detail) virtual_ds "
        "WHERE cid = 'C-1001' "
        "ORDER BY dt DESC",
        pl,
    )
    assert r_vd.outcome == "pass"
    assert r_vd.filter_columns == ("customer_id",)

    # Reject: 1-level derived table wrapping CAST(customer_id AS text) AS cid hides bare column
    with pytest.raises(GuardrailError) as exc_info:
        _run_guard(
            "SELECT cid FROM (SELECT CAST(customer_id AS text) AS cid FROM cbn.rpt.txn_detail) v "
            "WHERE cid = 'C-1001'",
            pl,
        )
    assert exc_info.value.code == "filter_required"

    # Reject: 2 levels of subquery nesting in FROM (even with outer or inner filter)
    for two_level_sql in (
        "SELECT * FROM (SELECT * FROM (SELECT * FROM cbn.rpt.txn_detail) s1) s2 WHERE customer_id = 'C-1001'",
        "SELECT * FROM (SELECT * FROM (SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001') s1) s2",
    ):
        with pytest.raises(GuardrailError) as exc_info:
            _run_guard(two_level_sql, pl)
        assert exc_info.value.code == "filter_required"


# ==============================================================================
# §4.2.4: ORDER BY sortable check (Accept + Reject + GROUP BY exemption)
# ==============================================================================


def test_order_by_sortable_accept_reject_and_group_by_exemption():
    pl = _make_placement()

    # Accept: sortable columns and primary keys
    r_ok = _run_guard(
        "SELECT txn_id, as_of_date FROM cbn.rpt.txn_detail "
        "WHERE customer_id = 'C-1001' ORDER BY as_of_date DESC, txn_id DESC",
        pl,
    )
    assert r_ok.outcome == "pass"

    # Accept: SELECT alias over bare sortable column
    r_alias = _run_guard(
        "SELECT as_of_date AS d FROM cbn.rpt.txn_detail "
        "WHERE customer_id = 'C-1001' ORDER BY d DESC",
        pl,
    )
    assert r_alias.outcome == "pass"

    # Accept: GROUP BY query is exempt from sortable restriction
    r_grp = _run_guard(
        "SELECT branch_code, count(*) AS cnt FROM cbn.rpt.txn_detail "
        "WHERE customer_id = 'C-1001' GROUP BY branch_code ORDER BY cnt DESC",
        pl,
    )
    assert r_grp.outcome == "pass"

    # Reject: unindexed column, expression, ordinal, computed alias, or GROUP BY only inside CTE/subquery
    for bad_order_sql in (
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001' ORDER BY amount DESC",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001' ORDER BY lower(as_of_date)",
        "SELECT txn_id FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001' ORDER BY 1",
        "SELECT lower(branch_code) AS as_of_date FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1001' ORDER BY as_of_date",
        "WITH g AS (SELECT branch_code, count(*) AS cnt FROM cbn.rpt.branch_dim GROUP BY branch_code) "
        "SELECT t.txn_id, t.amount FROM cbn.rpt.txn_detail t WHERE t.customer_id = 'C-1001' ORDER BY t.amount DESC",
        "WITH g AS (SELECT branch_code, count(*) AS cnt FROM cbn.rpt.branch_dim GROUP BY branch_code) "
        "SELECT t.txn_id, t.amount FROM cbn.rpt.txn_detail t JOIN g ON t.branch_code = g.branch_code "
        "WHERE t.customer_id = 'C-1001' ORDER BY t.amount DESC",
        "SELECT t.txn_id, t.amount FROM cbn.rpt.txn_detail t "
        "WHERE t.customer_id = 'C-1001' "
        "AND t.branch_code IN (SELECT branch_code FROM cbn.rpt.branch_dim GROUP BY branch_code) "
        "ORDER BY t.amount DESC",
    ):
        with pytest.raises(GuardrailError) as exc_info:
            _run_guard(bad_order_sql, pl)
        assert exc_info.value.code == "sort_not_indexed"


# ==============================================================================
# §4.2.5: OFFSET limits (Accept + Reject)
# ==============================================================================


def test_offset_accept_and_reject():
    pl = _make_placement(max_offset=10_000)

    # Accept: OFFSET 0 and OFFSET == max_offset
    assert (
        _run_guard(
            "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' LIMIT 50 OFFSET 0",
            pl,
        ).outcome
        == "pass"
    )
    assert (
        _run_guard(
            "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' LIMIT 50 OFFSET 10000",
            pl,
        ).outcome
        == "pass"
    )

    # Reject: OFFSET > max_offset, negative OFFSET, parameterized OFFSET
    for bad_offset_sql in (
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' LIMIT 50 OFFSET 10001",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' LIMIT 50 OFFSET 100000",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' OFFSET -1",
        "SELECT * FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' OFFSET $1",
    ):
        with pytest.raises(GuardrailError) as exc_info:
            _run_guard(bad_offset_sql, pl)
        assert exc_info.value.code == "offset_too_large"


# ==============================================================================
# §4.2.6 & §4.2.8: Row cap across all engines + guard_mode='warn'
# ==============================================================================


def test_effective_max_rows_minimum_across_grant_request_and_all_tables():
    pl = _make_placement()

    # On DuckDB (analytics route): table max_rows (5000) caps grant (10000) and request (8000)
    r_duck = _run_guard(
        "SELECT branch_code, SUM(amount) FROM cbn.rpt.txn_detail GROUP BY branch_code",
        pl,
        hint=None,
        grant_max_rows=10_000,
        request_max_rows=8_000,
    )
    assert r_duck.effective_max_rows == 5000

    # Join of customer (max_rows=50) and txn_detail (max_rows=5000) with grant=200, request=100 -> 50
    r_join = _run_guard(
        "SELECT c.customer_id, t.txn_id "
        "FROM cbn.src_pii.customer c "
        "JOIN cbn.rpt.txn_detail t ON c.customer_id = t.customer_id "
        "WHERE c.customer_id = 'C-1'",
        pl,
        hint=None,
        grant_max_rows=200,
        request_max_rows=100,
    )
    assert r_join.effective_max_rows == 50

    # Request max_rows wins when smallest (20 < 50 < 200)
    r_req = _run_guard(
        "SELECT * FROM cbn.src_pii.customer WHERE customer_id = 'C-1'",
        pl,
        hint=None,
        grant_max_rows=200,
        request_max_rows=20,
    )
    assert r_req.effective_max_rows == 20


def test_guard_mode_warn_records_metric_without_rejecting():
    pl_warn = _make_placement(guard_mode="warn")
    metrics = FakeGuardMetrics()

    res = _run_guard(
        "SELECT * FROM cbn.rpt.txn_detail",
        pl_warn,
        metrics=metrics,
    )
    assert res.outcome == "warn"
    assert len(res.warnings) >= 1
    assert "filter_required" in res.warnings[0]
    assert metrics.counts == {}
    assert metrics.warnings == {"filter_required": 1}

    reg = MetricsRegistry(engines=["pg_report", "lake"])
    _run_guard("SELECT * FROM cbn.rpt.txn_detail", pl_warn, metrics=reg)
    rendered = reg.render()
    assert 'seeknal_serving_guard_warnings_total{code="filter_required"} 1' in rendered
    assert 'seeknal_serving_guard_rejections_total{code="filter_required"}' not in rendered


# ==============================================================================
# HTTP E2E reachability for filter_required, sort_not_indexed, offset_too_large
# ==============================================================================


def test_http_endpoints_return_422_for_all_guardrail_codes():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pl = _make_placement()
    schemas = {
        "cbn.rpt.txn_detail": [
            "txn_id",
            "customer_id",
            "as_of_date",
            "branch_code",
            "merchant_code",
            "amount",
        ]
    }
    pipeline = ServingPipeline(
        placement=pl,
        schemas=schemas,
        public_keys={"k1": pub},
    )

    adapter_calls: list[str] = []

    class _NeverCalledAdapter:
        def execute(self, prepared, *, timeout_seconds: float, **kwargs):
            adapter_calls.append(prepared.sql)
            raise AssertionError("Adapter must never be called when guardrails reject")

        def cancel(self, *args, **kwargs):
            pass

    client = TestClient(
        create_app(
            pipeline,
            {"pg_report": _NeverCalledAdapter(), "lake": _NeverCalledAdapter()},
        )
    )

    cases = [
        (
            "filter_required",
            "SELECT txn_id, amount FROM cbn.rpt.txn_detail",
        ),
        (
            "sort_not_indexed",
            "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' ORDER BY amount DESC",
        ),
        (
            "offset_too_large",
            "SELECT txn_id, amount FROM cbn.rpt.txn_detail WHERE customer_id = 'C-1' LIMIT 10 OFFSET 100000",
        ),
    ]

    for idx, (expected_code, sql) in enumerate(cases):
        now = time.time()
        jti = f"jti-guard-http-{idx}"
        tok = sign_grant_token(
            {
                "sub": "analyst@cbn.example",
                "ws": "ws-cbn",
                "jti": jti,
                "iss": "atlas-catalog",
                "aud": "seeknal-serving",
                "iat": now,
                "exp": now + 60.0,
                "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                "max_rows": 100,
                "tables": {
                    "cbn.rpt.txn_detail": {
                        "columns": ["txn_id", "customer_id", "as_of_date", "amount"],
                        "masks": {},
                        "row_filters": [
                            {"column": "customer_id", "op": "eq", "values": ["C-1"]}
                        ],
                        "actions": ["select"],
                    }
                },
            },
            priv,
            kid="k1",
        )
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok}"},
            json={"sql": sql, "hint": "pg_report"},
        )
        assert resp.status_code == 422
        body = resp.json()
        assert body["code"] == expected_code
        assert body["audit_id"] == jti

    assert adapter_calls == []
