"""Tests for merging Lakekeeper tables with Postgres/Lakebase introspected tables
and enforcing the §2.3 storage-class PII placement rule during discovery.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import httpx
import pytest

from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.config import DiscoveryConfigError, DiscoveryError
from tests.serving.fake_postgres_catalog import FakePostgresCatalog


def _make_lakekeeper_client(
    tables_by_ns: dict[str, list[dict]],
) -> httpx.Client:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        v1_idx = path.find("/v1/")
        subpath = path[v1_idx:] if v1_idx != -1 else path
        if subpath == "/v1/config":
            return httpx.Response(200, json={})
        if subpath == "/v1/namespaces":
            return httpx.Response(
                200,
                json={"namespaces": [[ns] for ns in tables_by_ns]},
            )
        for ns, tbl_list in tables_by_ns.items():
            if subpath == f"/v1/namespaces/{ns}/tables":
                return httpx.Response(
                    200,
                    json={"identifiers": [{"name": t["name"]} for t in tbl_list]},
                )
            for t in tbl_list:
                if subpath == f"/v1/namespaces/{ns}/tables/{t['name']}":
                    fields = [
                        {"id": i + 1, "name": col}
                        for i, col in enumerate(t["columns"])
                    ]
                    key_ids = [
                        i + 1
                        for i, col in enumerate(t["columns"])
                        if col in t.get("keys", ())
                    ]
                    snap_id = t.get("snapshot_id", 900)
                    return httpx.Response(
                        200,
                        json={
                            "metadata": {
                                "current-schema-id": 0,
                                "schemas": [
                                    {
                                        "schema-id": 0,
                                        "fields": fields,
                                        "identifier-field-ids": key_ids,
                                    }
                                ],
                                "current-snapshot-id": snap_id,
                                "snapshots": [
                                    {
                                        "snapshot-id": snap_id,
                                        "timestamp-ms": 1795089600000,
                                        "summary": {
                                            "total-records": str(
                                                t.get("row_count", 2_000_000)
                                            )
                                        },
                                    }
                                ],
                            }
                        },
                    )
        return httpx.Response(404)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_merge_identical_columns_mismatch_drop_and_pii_never_merges():
    now_dt = datetime(2026, 11, 19, 12, 5, 0, tzinfo=timezone.utc)

    lk_client = _make_lakekeeper_client(
        {
            "rpt": [
                {
                    "name": "rpt_txn_detail",
                    "columns": ["txn_id", "customer_id", "as_of_date", "amount"],
                    "keys": [],
                    "snapshot_id": 555,
                    "row_count": 5_000_000,
                },
                {
                    "name": "rpt_mismatch",
                    "columns": ["id", "col_a", "col_b"],
                    "keys": ["id"],
                    "snapshot_id": 556,
                    "row_count": 500_000,
                },
                {
                    "name": "rpt_lake_only",
                    "columns": ["dt", "total"],
                    "keys": ["dt"],
                    "snapshot_id": 557,
                    "row_count": 10_000,
                },
            ],
            "src_pii": [
                {
                    "name": "customer",
                    "columns": ["customer_id", "full_name", "msisdn_hash"],
                    "keys": ["customer_id"],
                    "snapshot_id": 558,
                    "row_count": 1_000_000,
                }
            ],
        }
    )

    pg_cat = FakePostgresCatalog()
    # 1. rpt_txn_detail: identical columns -> merges with lake, matching snapshot_id=555 -> lag = 0 + 300 = 300s
    pg_cat.add_table(
        "rpt",
        "rpt_txn_detail",
        reltuples=5_000_000,
        columns=[
            ("txn_id", "text", True),
            ("customer_id", "text", True),
            ("as_of_date", "date", True),
            ("amount", "numeric", True),
        ],
        indexes=[
            {
                "index_name": "rpt_txn_detail_pkey",
                "amname": "btree",
                "columns": ["txn_id"],
                "is_primary": True,
            },
            {
                "index_name": "idx_txn_cust_date",
                "amname": "btree",
                "columns": ["customer_id", "as_of_date", "txn_id"],
            },
        ],
    )
    pg_cat.add_sync_state(
        "rpt.rpt_txn_detail",
        snapshot_id=555,
        synced_at=datetime(2026, 11, 19, 11, 0, 0, tzinfo=timezone.utc),
    )

    # 2. rpt_mismatch: missing col_b on Postgres -> Postgres serve dropped with warning!
    pg_cat.add_table(
        "rpt",
        "rpt_mismatch",
        reltuples=500_000,
        columns=[
            ("id", "text", True),
            ("col_a", "text", True),
        ],
        indexes=[
            {
                "index_name": "rpt_mismatch_pkey",
                "amname": "btree",
                "columns": ["id"],
                "is_primary": True,
            }
        ],
    )

    # 3. src_pii.customer: PII table -> NEVER merges; lake serve dropped, only pg_report serve kept!
    pg_cat.add_table(
        "src_pii",
        "customer",
        reltuples=1_000_000,
        columns=[
            ("customer_id", "text", True),
            ("full_name", "text", True),
            ("msisdn_hash", "text", True),
        ],
        indexes=[
            {
                "index_name": "customer_pkey",
                "amname": "btree",
                "columns": ["customer_id"],
                "is_primary": True,
            }
        ],
    )

    lb_ops_cat = FakePostgresCatalog()
    lb_ops_cat.add_table(
        "ops",
        "case_status",
        reltuples=200,
        columns=[("case_id", "text", True), ("status", "text", True)],
        indexes=[
            {
                "index_name": "case_status_pkey",
                "amname": "btree",
                "columns": ["case_id"],
                "is_primary": True,
            }
        ],
    )

    cfg = {
        "default_catalog": "cbn",
        "refresh_interval_seconds": 300,
        "lakekeeper": {
            "url": "https://lk.cbn.internal/catalog",
            "warehouse": "cbn",
        },
        "engines": {
            "lake": {
                "kind": "duckdb",
                "dialect": "duckdb",
                "profile": "iceberg_direct",
                "alias": "lake",
            },
            "pg_report": {"kind": "postgres", "dialect": "postgres"},
            "lb_ops": {"kind": "lakebase", "dialect": "postgres"},
        },
        "postgres_sources": {
            "pg_report": {
                "account": "interactive",
                "logical_catalog": "cbn",
                "freshness": {
                    "sync_state_table": "public._lakebase_sync_state",
                },
                "schemas": {
                    "rpt": {"include": ["rpt_*"]},
                    "src_pii": {"include": ["customer"], "pii": True},
                },
            },
            "lb_ops": {
                "account": "interactive",
                "logical_catalog": "ops",
                "schemas": {
                    "ops": {"include": ["case_status"]},
                },
            },
        },
        "policy": {
            "require_filter_min_rows": 100_000,
            "max_in_list": 1000,
            "max_offset": 10_000,
            "guard_mode": "enforce",
            "pii_engines": [],
            "pii_object_storage_engines": [],
        },
    }

    res = discover(
        cfg,
        http_client=lk_client,
        postgres_connects={
            "pg_report": pg_cat.connect,
            "lb_ops": lb_ops_cat.connect,
        },
        now=now_dt,
    )

    tables = res.placement["tables"]

    # 1. Merged table has BOTH lake and pg_report serves + Postgres access + keys
    merged = tables["cbn.rpt.rpt_txn_detail"]
    assert set(merged["serve"].keys()) == {"lake", "pg_report"}
    assert merged["serve"]["pg_report"]["name"] == "rpt.rpt_txn_detail"
    assert merged["serve"]["pg_report"]["max_lag_seconds"] == 300.0
    assert merged["keys"] == ["txn_id"]
    assert merged["access"]["pattern"] == "filter"
    assert set(merged["access"]["filterable"]) == {"txn_id", "customer_id"}
    assert set(merged["access"]["sortable"]) == {"txn_id", "customer_id", "as_of_date"}

    # 2. Mismatched columns dropped pg_report serve with warning, kept lake serve
    mismatch = tables["cbn.rpt.rpt_mismatch"]
    assert list(mismatch["serve"].keys()) == ["lake"]
    assert "access" not in mismatch
    assert res.schemas["cbn.rpt.rpt_mismatch"] == ["id", "col_a", "col_b"]
    assert any(
        "cbn.rpt.rpt_mismatch" in w and "dropped 'pg_report' serve" in w
        for w in res.warnings
    )

    # 3. Lake-only table untouched
    assert list(tables["cbn.rpt.rpt_lake_only"]["serve"].keys()) == ["lake"]

    # 4. PII table never merged: lake serve dropped, ONLY pg_report serve kept!
    pii_cust = tables["cbn.src_pii.customer"]
    assert pii_cust["pii"] is True
    assert list(pii_cust["serve"].keys()) == ["pg_report"]
    assert pii_cust["serve"]["pg_report"]["name"] == "src_pii.customer"
    assert pii_cust["access"]["pattern"] == "point"
    assert pii_cust["access"]["max_rows"] == 50

    # 5. Standalone operational Lakebase table
    ops_tbl = tables["ops.ops.case_status"]
    assert list(ops_tbl["serve"].keys()) == ["lb_ops"]
    assert ops_tbl["access"]["pattern"] == "scan"
    assert "max_lag_seconds" not in ops_tbl["serve"]["lb_ops"]

    # 6. Engines retain real kinds ('postgres' and 'lakebase')
    assert res.placement["engines"]["pg_report"]["kind"] == "postgres"
    assert res.placement["engines"]["lb_ops"]["kind"] == "lakebase"


def test_lake_marked_pii_also_drops_lake_serve_and_keeps_postgres_only():
    lk_client = _make_lakekeeper_client(
        {
            "src_pii": [
                {
                    "name": "customer",
                    "columns": ["customer_id", "full_name"],
                    "keys": ["customer_id"],
                }
            ]
        }
    )
    pg_cat = FakePostgresCatalog()
    pg_cat.add_table(
        "src_pii",
        "customer",
        reltuples=500,
        columns=[("customer_id", "text", True), ("full_name", "text", True)],
        indexes=[
            {
                "index_name": "customer_pkey",
                "amname": "btree",
                "columns": ["customer_id"],
                "is_primary": True,
            }
        ],
    )

    cfg = {
        "default_catalog": "cbn",
        "lakekeeper": {"url": "https://lk.test", "warehouse": "cbn"},
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
            "pg_report": {"kind": "postgres", "dialect": "postgres"},
        },
        "postgres_sources": {
            "pg_report": {
                "logical_catalog": "cbn",
                "schemas": {"src_pii": {"pii": False}},
            }
        },
        "pii_tables": ["cbn.src_pii.customer"],
    }

    res = discover(
        cfg,
        http_client=lk_client,
        postgres_connects={"pg_report": pg_cat.connect},
    )
    cust = res.placement["tables"]["cbn.src_pii.customer"]
    assert cust["pii"] is True
    assert list(cust["serve"].keys()) == ["pg_report"]
    assert cust["access"]["pattern"] == "point"
    assert cust["access"]["max_rows"] == 50


def test_storage_class_pii_rule_rejects_object_storage_and_allows_exceptions():
    lb_cat = FakePostgresCatalog()
    lb_cat.add_table(
        "src_pii",
        "customer",
        columns=[("customer_id", "text", True), ("full_name", "text", True)],
        indexes=[
            {
                "index_name": "customer_pkey",
                "amname": "btree",
                "columns": ["customer_id"],
                "is_primary": True,
            }
        ],
    )

    # 1. PII schema on lakebase (object_storage) -> rejected with exact §2.3 message
    cfg_lb_pii = {
        "default_catalog": "cbn",
        "engines": {
            "lb_ops": {"kind": "lakebase", "dialect": "postgres"},
        },
        "postgres_sources": {
            "lb_ops": {
                "logical_catalog": "cbn",
                "schemas": {"src_pii": {"include": ["customer"], "pii": True}},
            }
        },
    }
    with pytest.raises(DiscoveryError) as exc_info:
        discover(cfg_lb_pii, postgres_connects={"lb_ops": lb_cat.connect})
    assert str(exc_info.value) == (
        "table 'cbn.src_pii.customer' is pii: true but its serve engine 'lb_ops' "
        "(kind lakebase) has storage_class object_storage; "
        "CBN policy forbids PII in object storage — serve it from a postgres source "
        "or list the engine in policy.pii_object_storage_engines"
    )

    # 2. Legacy lakebase connection without lakebase: block -> storage_class unknown -> rejected
    with pytest.raises(DiscoveryError, match="has storage_class unknown"):
        discover(
            cfg_lb_pii,
            postgres_connects={"lb_ops": lb_cat.connect},
            connections={"lb_ops": {"kind": "lakebase", "dsn_env": "LB_DSN"}},
        )

    # 3. Postgres source explicitly declared storage_class: object_storage -> rejected
    cfg_pg_obj = {
        "default_catalog": "cbn",
        "engines": {
            "pg_s3": {"kind": "postgres", "dialect": "postgres"},
        },
        "postgres_sources": {
            "pg_s3": {
                "logical_catalog": "cbn",
                "schemas": {"src_pii": {"include": ["customer"], "pii": True}},
            }
        },
    }
    with pytest.raises(DiscoveryError, match="serve engine 'pg_s3' \\(kind postgres\\) has storage_class object_storage"):
        discover(
            cfg_pg_obj,
            postgres_connects={"pg_s3": lb_cat.connect},
            storage_classes={"pg_s3": "object_storage"},
        )

    # 4. Allowed when listed in policy.pii_object_storage_engines
    cfg_lb_allowed = {
        **cfg_lb_pii,
        "policy": {"pii_object_storage_engines": ["lb_ops"]},
    }
    res_allowed = discover(
        cfg_lb_allowed,
        postgres_connects={"lb_ops": lb_cat.connect},
    )
    assert res_allowed.placement["tables"]["cbn.src_pii.customer"]["pii"] is True
    assert res_allowed.placement["policy"]["pii_object_storage_engines"] == ["lb_ops"]

    # 5. Tokenized table (tokenized: true) is exempt on object_storage
    cfg_tok = {
        "default_catalog": "cbn",
        "engines": {"lb_ops": {"kind": "lakebase", "dialect": "postgres"}},
        "postgres_sources": {
            "lb_ops": {
                "logical_catalog": "cbn",
                "schemas": {
                    "src_pii": {
                        "include": ["customer"],
                        "pii": True,
                        "tokenized": True,
                    }
                },
            }
        },
    }
    res_tok = discover(cfg_tok, postgres_connects={"lb_ops": lb_cat.connect})
    assert res_tok.placement["tables"]["cbn.src_pii.customer"]["tokenized"] is True


def test_engine_storage_class_in_discovery_engines_rejected():
    with pytest.raises(
        DiscoveryConfigError,
        match="Engine 'pg_report' cannot declare 'storage_class' in placement.discovery.engines; declare 'storage_class' on connections.pg_report instead",
    ):
        discover(
            {
                "default_catalog": "cbn",
                "engines": {
                    "pg_report": {
                        "kind": "postgres",
                        "dialect": "postgres",
                        "storage_class": "local",
                    }
                },
            }
        )
