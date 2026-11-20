"""End-to-end HTTP tests for governed POST /v1/select and keyset pagination."""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import Placement, ServingPipeline, sign_grant_token
from seeknal.serving.adapters import DuckDBAdapter, EngineError
from seeknal.serving.enforce import EnforcementError
from seeknal.serving.http import create_app
from seeknal.serving.keyset import encode_cursor

CURSOR_KEY = "test-http-cursor-hmac-secret-key"


class EngineStartingError(EngineError):
    code = "engine_starting"

    def __init__(self, message: str = "Endpoint waking", *, retry_after: int | None = None) -> None:
        super().__init__(message)
        if retry_after is not None:
            self.retry_after = retry_after


@pytest.fixture
def ec_keys():
    priv = ec.generate_private_key(ec.SECP256R1())
    return priv, priv.public_key()


@pytest.fixture
def select_env(ec_keys):
    priv, pub = ec_keys
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE rpt_txn_detail ("
        "txn_id INT, customer_id VARCHAR, as_of_date VARCHAR, "
        "branch_code VARCHAR, amount INT, msisdn VARCHAR)"
    )
    con.execute(
        "INSERT INTO rpt_txn_detail VALUES "
        "(101, 'C-1', '2026-10-01', 'JKT', 100, '0811111111'), "
        "(102, 'C-1', '2026-10-02', 'JKT', 200, '0811111112'), "
        "(103, 'C-1', '2026-10-02', 'JKT', 300, '0811111113'), "
        "(104, 'C-1', '2026-10-03', 'JKT', 400, '0811111114'), "
        "(105, 'C-1', '2026-10-03', 'BDG', 500, '0811111115'), "
        "(106, 'C-2', '2026-10-03', 'JKT', 600, '0811111116')"
    )

    placement = Placement.from_dict(
        {
            "engines": {
                "duck": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "cbn.rpt.rpt_txn_detail": {
                    "keys": ["txn_id"],
                    "row_count": 6,
                    "serve": {"duck": "rpt_txn_detail"},
                },
                "cbn.rpt.other_table": {
                    "keys": ["txn_id"],
                    "row_count": 1,
                    "serve": {"duck": "rpt_txn_detail"},
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
            "msisdn",
        ],
        "cbn.rpt.other_table": ["txn_id", "amount"],
    }
    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
        default_catalog="cbn",
    )
    adapter = DuckDBAdapter(lambda: con.cursor())
    app = create_app(
        pipeline,
        {"duck": adapter},
        timeout_seconds=5.0,
        max_body_bytes=4096,
        cursor_key=CURSOR_KEY,
    )
    client = TestClient(app)
    return client, priv, pipeline, adapter, con


def _mint_select_grant(
    priv_key: Any,
    raw_body: bytes,
    *,
    jti: str = "jti-sel-1",
    api: Any = "select.v1",
    include_api: bool = True,
    sql_dialect: str = "postgres",
    params_sha256: str | None = None,
    max_rows: int = 1000,
    tables: dict[str, Any] | None = None,
) -> str:
    now = time.time()
    if tables is None:
        tables = {
            "cbn.rpt.rpt_txn_detail": {
                "columns": ["txn_id", "customer_id", "as_of_date", "branch_code", "amount"],
                "masks": {"msisdn": "redact"},
                "row_filters": [{"column": "branch_code", "op": "eq", "values": ["JKT"]}],
                "actions": ["select"],
            }
        }
    claims: dict[str, Any] = {
        "sub": "analyst@cbn.example",
        "ws": "cbn",
        "jti": jti,
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120.0,
        "sql_sha256": hashlib.sha256(raw_body).hexdigest(),
        "sql_dialect": sql_dialect,
        "max_rows": max_rows,
        "tables": tables,
    }
    if include_api:
        claims["api"] = api
    if params_sha256 is not None:
        claims["params_sha256"] = params_sha256
    return sign_grant_token(claims, priv_key, kid="k1")


def test_select_masks_row_filters_and_multipage_keyset_without_pk_in_columns(select_env):
    client, priv, _, _, _ = select_env

    # Customer C-1 in branch JKT (enforced by row_filter) has 4 rows:
    # (2026-10-03, 104, 400), (2026-10-02, 103, 300), (2026-10-02, 102, 200), (2026-10-01, 101, 100)
    # Note: columns=["amount", "msisdn"] omits both as_of_date and txn_id!
    # Serving must internally project them for cursor generation and strip them from the response.
    collected_amounts: list[int] = []
    after_cursor: str | None = None
    page_num = 0

    while True:
        page_num += 1
        body_obj: dict[str, Any] = {
            "table": "rpt.rpt_txn_detail",
            "columns": ["amount", "msisdn"],
            "where": {"customer_id": {"_eq": "C-1"}},
            "order_by": [{"as_of_date": "desc"}],
            "limit": 2,
        }
        if after_cursor is not None:
            body_obj["after"] = after_cursor

        raw_body = json.dumps(body_obj, separators=(",", ":")).encode("utf-8")
        tok = _mint_select_grant(priv, raw_body, jti=f"jti-page-{page_num}")
        resp = client.post(
            "/v1/select",
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            content=raw_body,
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["columns"] == ["amount", "msisdn"]
        assert len(data["column_types"]) == 2
        assert data["masked_columns"] == ["cbn.rpt.rpt_txn_detail.msisdn"]
        assert data["audit_id"] == f"jti-page-{page_num}"
        assert all(len(r) == 2 and r[1] == "****" for r in data["rows"])

        collected_amounts.extend(r[0] for r in data["rows"])
        after_cursor = data["next"]
        if not data["truncated"]:
            assert after_cursor is None
            assert data["notices"] == []
            break
        assert after_cursor is not None
        assert data["notices"] == ["row_cap_hit"]

    # Wait: on 4 matching rows with limit=2:
    # Page 1: [400, 300] (fetched 3 -> truncated=True, next=(2026-10-02, 103))
    # Page 2: [200, 100] (fetched 2 < 3 -> truncated=False, next=None)
    assert page_num == 2
    assert collected_amounts == [400, 300, 200, 100]


def test_select_body_digest_and_cross_endpoint_api_claim_isolation(select_env):
    client, priv, _, _, _ = select_env

    raw_body = b'{"table":"cbn.rpt.rpt_txn_detail","columns":["txn_id"]}'
    sel_token = _mint_select_grant(priv, raw_body, jti="jti-bind-1")

    # 1. One changed body byte (extra space) -> 401 grant_invalid
    tampered_body = b'{"table": "cbn.rpt.rpt_txn_detail","columns":["txn_id"]}'
    r_tampered = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {sel_token}"},
        content=tampered_body,
    )
    assert r_tampered.status_code == 401
    assert r_tampered.json()["code"] == "grant_invalid"

    # 2. Select grant sent to /v1/query (both with select body and with SQL body) -> 401 grant_invalid
    r_q1 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {sel_token}"},
        content=raw_body,
    )
    assert r_q1.status_code == 401
    assert r_q1.json()["code"] == "grant_invalid"

    sql_body = b'{"sql":"SELECT txn_id FROM cbn.rpt.rpt_txn_detail"}'
    sel_tok_on_sql = _mint_select_grant(priv, sql_body, jti="jti-bind-2")
    for sql_endpoint in ("/v1/query", "/v1/explain"):
        r_cross = client.post(
            sql_endpoint,
            headers={"Authorization": f"Bearer {sel_tok_on_sql}"},
            content=sql_body,
        )
        assert r_cross.status_code == 401
        assert r_cross.json()["code"] == "grant_invalid"

    # 3. Query grant (no api claim) sent to /v1/select -> 401 grant_invalid
    query_token = _mint_select_grant(
        priv, raw_body, jti="jti-bind-3", include_api=False
    )
    r_no_api = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {query_token}"},
        content=raw_body,
    )
    assert r_no_api.status_code == 401
    assert r_no_api.json()["code"] == "grant_invalid"

    # 4. Non-postgres sql_dialect or params_sha256 on select.v1 -> 401 grant_invalid
    bad_dialect_tok = _mint_select_grant(
        priv, raw_body, jti="jti-bind-4", sql_dialect="duckdb"
    )
    r_dial = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {bad_dialect_tok}"},
        content=raw_body,
    )
    assert r_dial.status_code == 401
    assert r_dial.json()["code"] == "grant_invalid"

    bad_ps_tok = _mint_select_grant(
        priv, raw_body, jti="jti-bind-5", params_sha256="a" * 64
    )
    r_ps = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {bad_ps_tok}"},
        content=raw_body,
    )
    assert r_ps.status_code == 401
    assert r_ps.json()["code"] == "grant_invalid"

    # 5. Replay protection: reusing jti on /v1/select -> 401 grant_replayed
    r_ok = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {sel_token}"},
        content=raw_body,
    )
    assert r_ok.status_code == 200
    r_replay = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {sel_token}"},
        content=raw_body,
    )
    assert r_replay.status_code == 401
    assert r_replay.json()["code"] == "grant_replayed"


def test_select_invalid_cursor_invalid_and_masked_column_errors(select_env):
    client, priv, _, _, _ = select_env

    # 1. Invalid JSON, non-object JSON, and duplicate JSON keys at any nesting level -> 400 select_invalid
    for idx, bad_raw in enumerate(
        (
            b"{not-json",
            b"[1,2,3]",
            b'{"table":"cbn.rpt.rpt_txn_detail","table":"cbn.rpt.rpt_txn_detail","columns":["txn_id"]}',
            b'{"table":"cbn.rpt.rpt_txn_detail","columns":["txn_id"],"where":{"customer_id":{"_eq":"C-1"}},"where":{"customer_id":{"_eq":"C-2"}}}',
            b'{"table":"cbn.rpt.rpt_txn_detail","columns":["txn_id"],"where":{"customer_id":{"_eq":"C-1"},"customer_id":{"_eq":"C-2"}}}',
            b'{"table":"cbn.rpt.rpt_txn_detail","columns":["txn_id"],"where":{"customer_id":{"_eq":"C-1","_eq":"C-2"}}}',
            b'{"table":"cbn.rpt.rpt_txn_detail","columns":["txn_id"],"order_by":[{"txn_id":"asc","txn_id":"desc"}]}',
        )
    ):
        tok = _mint_select_grant(priv, bad_raw, jti=f"jti-badjson-{idx}")
        resp = client.post(
            "/v1/select",
            headers={"Authorization": f"Bearer {tok}"},
            content=bad_raw,
        )
        assert resp.status_code == 400
        assert resp.json()["code"] == "select_invalid"

    # 2. Bad cursor (flipped bit, foreign table, mismatched order) -> 400 cursor_invalid
    good_cursor = encode_cursor(
        "cbn.rpt.rpt_txn_detail", [("txn_id", "asc")], [101], CURSOR_KEY
    )
    flipped_cursor = good_cursor[:-1] + ("A" if good_cursor[-1] != "A" else "B")
    foreign_cursor = encode_cursor(
        "cbn.rpt.other_table", [("txn_id", "asc")], [101], CURSOR_KEY
    )
    wrong_dir_cursor = encode_cursor(
        "cbn.rpt.rpt_txn_detail", [("txn_id", "desc")], [101], CURSOR_KEY
    )
    for idx, bad_cur in enumerate((flipped_cursor, foreign_cursor, wrong_dir_cursor)):
        raw = json.dumps(
            {
                "table": "cbn.rpt.rpt_txn_detail",
                "columns": ["txn_id"],
                "after": bad_cur,
            }
        ).encode("utf-8")
        tok = _mint_select_grant(priv, raw, jti=f"jti-badcur-{idx}")
        resp = client.post(
            "/v1/select",
            headers={"Authorization": f"Bearer {tok}"},
            content=raw,
        )
        assert resp.status_code == 400
        assert resp.json()["code"] == "cursor_invalid"

    # 3. Masked column in where or order_by -> 403 access_denied
    for idx, extra in enumerate(
        ({"where": {"msisdn": {"_eq": "0811111111"}}}, {"order_by": [{"msisdn": "asc"}]})
    ):
        raw = json.dumps(
            {"table": "cbn.rpt.rpt_txn_detail", "columns": ["txn_id"], **extra}
        ).encode("utf-8")
        tok = _mint_select_grant(priv, raw, jti=f"jti-mask-{idx}")
        resp = client.post(
            "/v1/select",
            headers={"Authorization": f"Bearer {tok}"},
            content=raw,
        )
        assert resp.status_code == 403
        assert resp.json()["code"] == "access_denied"


def test_select_real_pipeline_guardrails_prefix_like_and_engine_starting(
    ec_keys, monkeypatch
):
    from seeknal.serving.adapters import QueryResult

    priv, pub = ec_keys
    placement = Placement.from_dict(
        {
            "engines": {
                "pg_report": {"kind": "postgres", "dialect": "postgres"},
                "lb_waking": {"kind": "lakebase", "dialect": "postgres"},
                "lake": {"kind": "duckdb", "dialect": "duckdb"},
            },
            "tables": {
                "cbn.rpt.rpt_txn_detail": {
                    "keys": ["txn_id"],
                    "row_count": 5_000_000,
                    "serve": {
                        "pg_report": "rpt.rpt_txn_detail",
                        "lake": "lake.rpt_txn_detail",
                    },
                    "access": {
                        "pattern": "filter",
                        "keys": ["txn_id"],
                        "filterable": ["customer_id", "as_of_date"],
                        "prefix_filterable": ["merchant_code"],
                        "sortable": ["as_of_date", "txn_id"],
                        "max_rows": 500,
                    },
                },
                "cbn.ops.ticket": {
                    "keys": ["ticket_id"],
                    "row_count": 10_000,
                    "serve": {"lb_waking": "ops.ticket"},
                    "access": {
                        "pattern": "filter",
                        "keys": ["ticket_id"],
                        "filterable": ["customer_id"],
                        "sortable": ["ticket_id"],
                    },
                },
            },
            "policy": {"max_offset": 100},
            "cubes": {},
        }
    )
    schemas = {
        "cbn.rpt.rpt_txn_detail": [
            "txn_id",
            "customer_id",
            "as_of_date",
            "merchant_code",
            "amount",
        ],
        "cbn.ops.ticket": ["ticket_id", "customer_id"],
    }

    executed_prepared: list[Any] = []

    class RecordingPgAdapter:
        def execute(self, prepared, *, timeout_seconds: float):
            executed_prepared.append(prepared)
            return QueryResult(
                columns=("txn_id", "merchant_code"),
                column_types=("INT", "TEXT"),
                rows=[(101, "MRC0199")],
                truncated=False,
                elapsed_ms=1.5,
                engine=prepared.engine,
            )

    class NeverLakeAdapter:
        def execute(self, prepared, *, timeout_seconds: float):
            raise AssertionError(
                f"Expected pg_report via filter rule, got lake ({prepared.rule})"
            )

    class WakingAdapter:
        def execute(self, prepared, *, timeout_seconds: float):
            raise EngineStartingError("Lakebase waking", retry_after=7)

    pipeline = ServingPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
        default_catalog="cbn",
    )
    monkeypatch.setenv("SEEKNAL_CURSOR_KEY", "env-cursor-secret")
    app = create_app(
        pipeline,
        {
            "pg_report": RecordingPgAdapter(),
            "lb_waking": WakingAdapter(),
            "lake": NeverLakeAdapter(),
        },
    )
    client = TestClient(app)

    grant_tables = {
        "cbn.rpt.rpt_txn_detail": {
            "columns": [
                "txn_id",
                "customer_id",
                "as_of_date",
                "merchant_code",
                "amount",
            ],
            "masks": {},
            "row_filters": [],
            "actions": ["select"],
        },
        "cbn.ops.ticket": {
            "columns": ["ticket_id", "customer_id"],
            "masks": {},
            "row_filters": [],
            "actions": ["select"],
        },
    }

    # 1. Unfiltered /v1/select on pg_report hits real filter_required (422)
    unfiltered_raw = json.dumps(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "hint": "pg_report",
        }
    ).encode("utf-8")
    tok1 = _mint_select_grant(
        priv, unfiltered_raw, jti="jti-guard-1", tables=grant_tables
    )
    r_guard = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {tok1}"},
        content=unfiltered_raw,
    )
    assert r_guard.status_code == 422
    assert r_guard.json()["code"] == "filter_required"

    # 2. Unindexed order_by on /v1/select hits real sort_not_indexed (422)
    bad_sort_raw = json.dumps(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id", "amount"],
            "where": {"customer_id": {"_eq": "C-1"}},
            "order_by": [{"amount": "desc"}],
        }
    ).encode("utf-8")
    tok_sort = _mint_select_grant(
        priv, bad_sort_raw, jti="jti-guard-sort-1", tables=grant_tables
    )
    r_sort = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {tok_sort}"},
        content=bad_sort_raw,
    )
    assert r_sort.status_code == 422
    assert r_sort.json()["code"] == "sort_not_indexed"

    # 3. _like on non-prefix_filterable column hits filter_required (422)
    bad_like_raw = json.dumps(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "where": {"customer_id": {"_like": "C-1%"}},
            "hint": "pg_report",
        }
    ).encode("utf-8")
    tok_bad_like = _mint_select_grant(
        priv, bad_like_raw, jti="jti-guard-badlike-1", tables=grant_tables
    )
    r_bad_like = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {tok_bad_like}"},
        content=bad_like_raw,
    )
    assert r_bad_like.status_code == 422
    assert r_bad_like.json()["code"] == "filter_required"

    # 4. _like on prefix_filterable column routes via 'filter' to pg_report and passes require_filter
    good_like_raw = json.dumps(
        {
            "table": "rpt.rpt_txn_detail",
            "columns": ["txn_id", "merchant_code"],
            "where": {"merchant_code": {"_like": "MRC01%"}},
            "order_by": [{"as_of_date": "desc"}],
            "limit": 10,
        }
    ).encode("utf-8")
    tok_good_like = _mint_select_grant(
        priv, good_like_raw, jti="jti-guard-goodlike-1", tables=grant_tables
    )
    r_good_like = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {tok_good_like}"},
        content=good_like_raw,
    )
    assert r_good_like.status_code == 200, r_good_like.text
    good_body = r_good_like.json()
    assert good_body["engine"] == "pg_report"
    assert good_body["rule"] == "filter"
    assert good_body["rows"] == [[101, "MRC0199"]]
    assert len(executed_prepared) == 1
    assert executed_prepared[0].filter_columns == ("merchant_code",)

    # 5. Real offset_too_large guardrail (422) on the same pipeline, and offset rejected on /v1/select (400)
    offset_sel_raw = json.dumps(
        {
            "table": "cbn.rpt.rpt_txn_detail",
            "columns": ["txn_id"],
            "where": {"customer_id": {"_eq": "C-1"}},
            "offset": 500,
        }
    ).encode("utf-8")
    tok_off_sel = _mint_select_grant(
        priv, offset_sel_raw, jti="jti-off-sel-1", tables=grant_tables
    )
    r_off_sel = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {tok_off_sel}"},
        content=offset_sel_raw,
    )
    assert r_off_sel.status_code == 400
    assert r_off_sel.json()["code"] == "select_invalid"

    off_sql = "SELECT txn_id FROM cbn.rpt.rpt_txn_detail WHERE customer_id = 'C-1' OFFSET 500"
    tok_off_q = _mint_select_grant(
        priv,
        off_sql.encode("utf-8"),
        jti="jti-off-q-1",
        include_api=False,
        tables=grant_tables,
    )
    r_off_q = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok_off_q}"},
        json={"sql": off_sql},
    )
    assert r_off_q.status_code == 422
    assert r_off_q.json()["code"] == "offset_too_large"

    # 6. Filtered select on lb_waking passes guardrails and surfaces 503 engine_starting + Retry-After
    filtered_raw = json.dumps(
        {
            "table": "cbn.ops.ticket",
            "columns": ["ticket_id"],
            "where": {"customer_id": {"_eq": "C-1"}},
        }
    ).encode("utf-8")
    tok2 = _mint_select_grant(
        priv, filtered_raw, jti="jti-wake-1", tables=grant_tables
    )
    r_wake = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {tok2}"},
        content=filtered_raw,
    )
    assert r_wake.status_code == 503
    assert r_wake.headers.get("Retry-After") == "7"
    body = r_wake.json()
    assert body["code"] == "engine_starting"
    assert body["audit_id"] == "jti-wake-1"


@pytest.mark.parametrize("exc_cls", [TypeError, AttributeError])
def test_select_never_falls_open_on_prepare_statement_error(ec_keys, exc_cls):
    priv, pub = ec_keys
    placement = Placement.from_dict(
        {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "cbn.rpt.rpt_txn_detail": {
                    "keys": ["txn_id"],
                    "row_count": 10,
                    "serve": {"duck": "rpt_txn_detail"},
                }
            },
            "cubes": {},
        }
    )
    schemas = {"cbn.rpt.rpt_txn_detail": ["txn_id", "customer_id", "amount"]}
    adapter_called = False

    class BrokenPipeline(ServingPipeline):
        def prepare_statement(self, *args, **kwargs):
            raise exc_cls("boom inside prepare_statement")

    class SpyAdapter:
        def execute(self, prepared, *, timeout_seconds: float):
            nonlocal adapter_called
            adapter_called = True
            raise AssertionError("Adapter must never run when prepare_statement fails")

    pipeline = BrokenPipeline(
        placement=placement,
        schemas=schemas,
        public_keys={"k1": pub},
    )
    client = TestClient(
        create_app(pipeline, {"duck": SpyAdapter()}, cursor_key=CURSOR_KEY)
    )
    raw = json.dumps(
        {"table": "cbn.rpt.rpt_txn_detail", "columns": ["txn_id"]}
    ).encode("utf-8")
    tok = _mint_select_grant(
        priv,
        raw,
        jti=f"jti-failclosed-{exc_cls.__name__}",
        tables={
            "cbn.rpt.rpt_txn_detail": {
                "columns": ["txn_id", "customer_id", "amount"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    )
    resp = client.post(
        "/v1/select",
        headers={"Authorization": f"Bearer {tok}"},
        content=raw,
    )
    assert resp.status_code == 500
    assert resp.json()["code"] == "internal_error"
    assert adapter_called is False

