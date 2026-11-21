"""Tests for governed structured select compilation in select_api.py."""

from __future__ import annotations

import hashlib
import time

import pytest
from sqlglot import exp

from seeknal.serving import select_api
from seeknal.serving.enforce import EnforcementError, Grant
from seeknal.serving.keyset import CursorError, encode_cursor
from seeknal.serving.parameter_binding import MAX_SAFE_INTEGER, MIN_SAFE_INTEGER
from seeknal.serving.placement import Placement
from seeknal.serving.select_api import SelectInvalidError, compile_select_request

CURSOR_SECRET = b"compiler-test-cursor-hmac-secret"


@pytest.fixture
def cbn_catalog():
    placement = Placement.from_dict(
        {
            "engines": {
                "lb": {"kind": "lakebase", "dialect": "postgres"},
            },
            "tables": {
                "cbn.rpt.rpt_txn_detail": {
                    "keys": ["txn_id"],
                    "row_count": 500_000,
                    "serve": {"lb": "rpt.rpt_txn_detail"},
                },
                "cbn.rpt.composite_pk": {
                    "keys": ["tenant_id", "seq_no"],
                    "row_count": 1000,
                    "serve": {"lb": "rpt.composite_pk"},
                },
            },
            "cubes": {},
        }
    )
    schemas = {
        "cbn.rpt.rpt_txn_detail": [
            "txn_id",
            "customer_id",
            "as_of_date",
            "branch_code",
            "amount",
            "is_reversed",
            "memo",
            "msisdn",
            "internal_secret",
        ],
        "cbn.rpt.composite_pk": [
            "tenant_id",
            "seq_no",
            "status",
        ],
    }
    return placement, schemas


def _make_grant(
    *,
    columns: list[str] | None = None,
    masks: dict[str, str] | None = None,
) -> Grant:
    if columns is None:
        columns = [
            "txn_id",
            "customer_id",
            "as_of_date",
            "branch_code",
            "amount",
            "is_reversed",
            "memo",
        ]
    if masks is None:
        masks = {"msisdn": "redact"}
    return Grant.from_claims(
        {
            "sub": "user:analyst",
            "ws": "cbn",
            "jti": "jti-comp-1",
            "sql_sha256": hashlib.sha256(b"{}").hexdigest(),
            "max_rows": 1000,
            "exp": time.time() + 300.0,
            "api": "select.v1",
            "sql_dialect": "postgres",
            "tables": {
                "cbn.rpt.rpt_txn_detail": {
                    "columns": columns,
                    "masks": masks,
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        }
    )


def test_section5_design_example_golden_sql(cbn_catalog):
    placement, schemas = cbn_catalog
    payload = {
        "table": "cbn.rpt.rpt_txn_detail",
        "columns": ["txn_id", "as_of_date", "branch_code", "amount"],
        "where": {
            "_and": [
                {"customer_id": {"_eq": "C-1001"}},
                {"as_of_date": {"_gte": "2026-10-01", "_lt": "2026-11-01"}},
                {"_not": {"branch_code": {"_in": ["X01", "X02"]}}},
            ]
        },
        "order_by": [{"as_of_date": "desc"}, {"txn_id": "desc"}],
        "limit": 100,
    }
    compiled = compile_select_request(payload, placement=placement, schemas=schemas)

    assert (
        compiled.sql
        == 'SELECT "txn_id", "as_of_date", "branch_code", "amount" '
        'FROM "cbn"."rpt"."rpt_txn_detail" '
        'WHERE "customer_id" = $1 AND "as_of_date" >= $2 AND "as_of_date" < $3 '
        'AND NOT "branch_code" IN ($4, $5) '
        'ORDER BY "as_of_date" DESC, "txn_id" DESC'
    )
    assert compiled.params == ("C-1001", "2026-10-01", "2026-11-01", "X01", "X02")
    assert compiled.order_spec == (("as_of_date", "desc"), ("txn_id", "desc"))
    assert compiled.limit == 100
    assert list(compiled.statement.find_all(exp.Literal)) == []


@pytest.mark.parametrize(
    ("where_clause", "expected_where_sql", "expected_params"),
    [
        ({"customer_id": {"_eq": "C-1"}}, '"customer_id" = $1', ("C-1",)),
        ({"customer_id": {"_neq": "C-1"}}, '"customer_id" <> $1', ("C-1",)),
        ({"amount": {"_gt": 100}}, '"amount" > $1', (100,)),
        ({"amount": {"_gte": 100}}, '"amount" >= $1', (100,)),
        ({"amount": {"_lt": 500}}, '"amount" < $1', (500,)),
        ({"amount": {"_lte": 500}}, '"amount" <= $1', (500,)),
        (
            {"branch_code": {"_in": ["B1", "B2", "B3"]}},
            '"branch_code" IN ($1, $2, $3)',
            ("B1", "B2", "B3"),
        ),
        (
            {"branch_code": {"_nin": ["B1", "B2"]}},
            'NOT "branch_code" IN ($1, $2)',
            ("B1", "B2"),
        ),
        (
            {"branch_code": {"_like": "JKT%"}},
            '"branch_code" LIKE $1',
            ("JKT%",),
        ),
        (
            {"memo": {"_like": r"A\%B\_C\\D%"}},
            '"memo" LIKE $1',
            (r"A\%B\_C\\D%",),
        ),
        ({"memo": {"_is_null": True}}, '"memo" IS NULL', ()),
        ({"memo": {"_is_null": False}}, 'NOT "memo" IS NULL', ()),
        (
            {"is_reversed": {"_eq": False}},
            '"is_reversed" = $1',
            (False,),
        ),
        (
            {
                "_and": [
                    {
                        "_or": [
                            {"branch_code": {"_eq": "B1"}},
                            {"branch_code": {"_eq": "B2"}},
                        ]
                    },
                    {"amount": {"_gt": 0}},
                ]
            },
            '("branch_code" = $1 OR "branch_code" = $2) AND "amount" > $3',
            ("B1", "B2", 0),
        ),
        (
            {
                "_not": {
                    "_and": [
                        {"branch_code": {"_eq": "B1"}},
                        {"is_reversed": {"_eq": True}},
                    ]
                }
            },
            'NOT ("branch_code" = $1 AND "is_reversed" = $2)',
            ("B1", True),
        ),
    ],
)
def test_golden_sql_all_operators(
    cbn_catalog, where_clause, expected_where_sql, expected_params
):
    placement, schemas = cbn_catalog
    compiled = compile_select_request(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id", "amount"],
            "where": where_clause,
        },
        placement=placement,
        schemas=schemas,
    )
    expected_sql = (
        'SELECT "txn_id", "amount" FROM "cbn"."rpt"."rpt_txn_detail" '
        f'WHERE {expected_where_sql} ORDER BY "txn_id" ASC'
    )
    assert compiled.sql == expected_sql
    assert compiled.params == expected_params
    assert list(compiled.statement.find_all(exp.Literal)) == []


def test_hostile_values_appear_only_in_params(cbn_catalog):
    placement, schemas = cbn_catalog
    injection = r"x'; DROP TABLE cbn.rpt.rpt\_txn\_detail; SELECT pg\_sleep(10); --%"
    compiled = compile_select_request(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "where": {
                "customer_id": {"_eq": "'); DELETE FROM users; --"},
                "memo": {"_like": injection},
            },
        },
        placement=placement,
        schemas=schemas,
    )
    assert "DROP" not in compiled.sql
    assert "DELETE" not in compiled.sql
    assert "pg_sleep" not in compiled.sql
    assert "pg\\_sleep" not in compiled.sql
    assert list(compiled.statement.find_all(exp.Literal)) == []
    assert compiled.params == ("'); DELETE FROM users; --", injection)


@pytest.mark.parametrize(
    "bad_payload",
    [
        {"table": "cbn.rpt.rpt_txn_detail; DROP TABLE x", "columns": ["txn_id"]},
        {"table": "cbn..rpt_txn_detail", "columns": ["txn_id"]},
        {"table": "1cbn.rpt.rpt_txn_detail", "columns": ["txn_id"]},
        {"table": "a.b.c.d", "columns": ["txn_id"]},
        {"table": "cbn.rpt.nonexistent", "columns": ["txn_id"]},
        {"table": "cbn.rpt.rpt_txn_detail", "columns": ["txn_id; --"]},
        {"table": "cbn.rpt.rpt_txn_detail", "columns": ["*"]},
        {"table": "cbn.rpt.rpt_txn_detail", "columns": ["unknown_col"]},
        {"table": "cbn.rpt.rpt_txn_detail", "columns": ["txn_id", "TXN_ID"]},
        {"table": "cbn.rpt.rpt_txn_detail", "columns": []},
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "where": {"amount) OR 1=1 --": {"_eq": 1}},
        },
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "where": {"unknown_col": {"_eq": 1}},
        },
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "order_by": [{"txn_id DESC; --": "asc"}],
        },
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "order_by": [{"unknown_col": "asc"}],
        },
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "unknown_top_key": 1,
        },
    ],
)
def test_hostile_identifiers_and_keys_fail_before_any_ast_exists(
    cbn_catalog, monkeypatch, bad_payload
):
    placement, schemas = cbn_catalog

    def _forbid_ast(*_args, **_kwargs):
        pytest.fail("SQLGlot AST node must not be constructed before validation passes")

    monkeypatch.setattr(select_api.exp, "Select", _forbid_ast)
    monkeypatch.setattr(select_api.exp, "Table", _forbid_ast)
    monkeypatch.setattr(select_api.exp, "Column", _forbid_ast)
    monkeypatch.setattr(select_api.exp, "Identifier", _forbid_ast)

    with pytest.raises(SelectInvalidError) as exc_info:
        compile_select_request(bad_payload, placement=placement, schemas=schemas)
    assert exc_info.value.code == "select_invalid"


def test_default_catalog_resolves_two_part_table(cbn_catalog):
    placement, schemas = cbn_catalog
    compiled = compile_select_request(
        {"table": "rpt.rpt_txn_detail", "columns": ["txn_id"]},
        placement=placement,
        schemas=schemas,
        default_catalog="cbn",
    )
    assert compiled.table == "cbn.rpt.rpt_txn_detail"
    assert 'FROM "cbn"."rpt"."rpt_txn_detail"' in compiled.sql


@pytest.mark.parametrize(
    "bad_like",
    [
        "%",
        "%abc",
        "%abc%",
        "ab%c%",
        "ab_c%",
        "abc",
        r"abc\%",
        r"abc\x%",
        "",
        123,
        None,
        True,
    ],
)
def test_like_rejects_non_prefix_patterns(cbn_catalog, bad_like):
    placement, schemas = cbn_catalog
    with pytest.raises(SelectInvalidError) as exc_info:
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "where": {"branch_code": {"_like": bad_like}},
            },
            placement=placement,
            schemas=schemas,
        )
    assert exc_info.value.code == "select_invalid"


def test_where_depth_and_node_caps_and_scalar_rules(cbn_catalog):
    placement, schemas = cbn_catalog

    # Depth 5 succeeds: _and(1) -> _or(2) -> _not(3) -> _and(4) -> leaf(5)
    depth5 = {
        "_and": [
            {
                "_or": [
                    {
                        "_not": {
                            "_and": [
                                {"customer_id": {"_eq": "C-1"}},
                            ]
                        }
                    }
                ]
            }
        ]
    }
    comp5 = compile_select_request(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "where": depth5,
        },
        placement=placement,
        schemas=schemas,
    )
    assert comp5.params == ("C-1",)

    # Depth 6 fails
    depth6 = {"_or": [depth5]}
    with pytest.raises(SelectInvalidError):
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "where": depth6,
            },
            placement=placement,
            schemas=schemas,
        )

    # 64 nodes succeeds (1 _and + 63 _eq = 64); 65 nodes fails (1 _and + 64 _eq = 65)
    ok_64 = {"_and": [{"amount": {"_gt": i}} for i in range(63)]}
    comp64 = compile_select_request(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "where": ok_64,
        },
        placement=placement,
        schemas=schemas,
    )
    assert len(comp64.params) == 63

    bad_65 = {"_and": [{"amount": {"_gt": i}} for i in range(64)]}
    with pytest.raises(SelectInvalidError):
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "where": bad_65,
            },
            placement=placement,
            schemas=schemas,
        )

    # Floats, unsafe ints, None, lone surrogates, and oversized _in lists rejected
    for bad_scalar in (1.5, 0.0, None, MAX_SAFE_INTEGER + 1, MIN_SAFE_INTEGER - 1, "bad\ud800"):
        with pytest.raises(SelectInvalidError):
            compile_select_request(
                {
                    "table": "cbn.rpt.rpt_txn_detail",
                    "columns": ["txn_id"],
                    "where": {"amount": {"_eq": bad_scalar}},
                },
                placement=placement,
                schemas=schemas,
            )

    with pytest.raises(SelectInvalidError):
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "where": {"amount": {"_in": list(range(1001))}},
            },
            placement=placement,
            schemas=schemas,
        )


def test_order_by_direction_pk_append_and_keyset_compilation(cbn_catalog):
    placement, schemas = cbn_catalog

    # Mixed directions rejected
    with pytest.raises(SelectInvalidError):
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "order_by": [{"as_of_date": "desc"}, {"txn_id": "asc"}],
            },
            placement=placement,
            schemas=schemas,
        )

    # Single order column + auto-appended PK in same direction + cursor tuple comparison
    cursor_desc = encode_cursor(
        "cbn.rpt.rpt_txn_detail",
        [("as_of_date", "desc"), ("txn_id", "desc")],
        ["2026-10-15", 777],
        CURSOR_SECRET,
    )
    comp_desc = compile_select_request(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["amount"],
            "where": {"customer_id": {"_eq": "C-9"}},
            "order_by": [{"as_of_date": "desc"}],
            "after": cursor_desc,
        },
        placement=placement,
        schemas=schemas,
        cursor_key=CURSOR_SECRET,
        include_cursor_columns=True,
    )
    assert comp_desc.columns == ("amount",)
    assert comp_desc.extra_cursor_columns == ("as_of_date", "txn_id")
    assert comp_desc.projected_columns == ("amount", "as_of_date", "txn_id")
    assert (
        comp_desc.sql
        == 'SELECT "amount", "as_of_date", "txn_id" FROM "cbn"."rpt"."rpt_txn_detail" '
        'WHERE "customer_id" = $1 AND ("as_of_date", "txn_id") < ($2, $3) '
        'ORDER BY "as_of_date" DESC, "txn_id" DESC'
    )
    assert comp_desc.params == ("C-9", "2026-10-15", 777)

    # Default ASC single PK order + single-column cursor comparison
    cursor_asc = encode_cursor(
        "cbn.rpt.rpt_txn_detail",
        [("txn_id", "asc")],
        [50],
        CURSOR_SECRET,
    )
    comp_asc = compile_select_request(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id", "amount"],
            "after": cursor_asc,
        },
        placement=placement,
        schemas=schemas,
        cursor_key=CURSOR_SECRET,
    )
    assert (
        comp_asc.sql
        == 'SELECT "txn_id", "amount" FROM "cbn"."rpt"."rpt_txn_detail" '
        'WHERE "txn_id" > $1 ORDER BY "txn_id" ASC'
    )
    assert comp_asc.params == (50,)

    # Tampered cursor raises CursorError
    with pytest.raises(CursorError):
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "after": cursor_asc[:-1] + ("A" if cursor_asc[-1] != "A" else "B"),
            },
            placement=placement,
            schemas=schemas,
            cursor_key=CURSOR_SECRET,
        )


def test_masked_and_ungranted_columns_in_where_or_order_by_raise_access_denied(
    cbn_catalog,
):
    placement, schemas = cbn_catalog
    grant = _make_grant()

    # Masked column in projection is allowed
    comp_proj = compile_select_request(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id", "msisdn"],
            "where": {"customer_id": {"_eq": "C-1"}},
        },
        placement=placement,
        schemas=schemas,
        grant=grant,
    )
    assert comp_proj.columns == ("txn_id", "msisdn")

    # Masked column in where -> 403 access_denied
    with pytest.raises(EnforcementError) as exc_w:
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "where": {"msisdn": {"_eq": "0812"}},
            },
            placement=placement,
            schemas=schemas,
            grant=grant,
        )
    assert exc_w.value.code == "access_denied"

    # Masked column in order_by -> 403 access_denied
    with pytest.raises(EnforcementError) as exc_o:
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "order_by": [{"msisdn": "asc"}],
            },
            placement=placement,
            schemas=schemas,
            grant=grant,
        )
    assert exc_o.value.code == "access_denied"

    # Ungranted schema column in where / columns -> 403 access_denied
    with pytest.raises(EnforcementError) as exc_ug:
        compile_select_request(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "where": {"internal_secret": {"_eq": "topsecret"}},
            },
            placement=placement,
            schemas=schemas,
            grant=grant,
        )
    assert exc_ug.value.code == "access_denied"


def test_reserved_word_and_mixed_case_identifiers_render_quoted():
    placement = Placement.from_dict(
        {
            "engines": {"lb": {"kind": "lakebase", "dialect": "postgres"}},
            "tables": {
                "cbn.rpt.UserEvents": {
                    "keys": ["EventId"],
                    "row_count": 100,
                    "serve": {"lb": "rpt.user_events"},
                }
            },
            "cubes": {},
        }
    )
    schemas = {"cbn.rpt.UserEvents": ["EventId", "user", "CreatedAt"]}
    compiled = compile_select_request(
        {
            "table": "cbn.rpt.UserEvents",
            "columns": ["EventId", "user", "CreatedAt"],
            "where": {"user": {"_eq": "alice"}},
            "order_by": [{"CreatedAt": "desc"}],
        },
        placement=placement,
        schemas=schemas,
    )
    assert (
        compiled.sql
        == 'SELECT "EventId", "user", "CreatedAt" '
        'FROM "cbn"."rpt"."UserEvents" '
        'WHERE "user" = $1 '
        'ORDER BY "CreatedAt" DESC, "EventId" DESC'
    )
    assert all(ident.quoted for ident in compiled.statement.find_all(exp.Identifier))

