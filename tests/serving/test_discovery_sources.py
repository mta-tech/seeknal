"""Tests for discovery sources: Lakekeeper, Lakebase, and Cube."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import httpx
import pytest

from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryConfigError,
    DiscoveryError,
    EngineConfig,
    LakekeeperConfig,
)
from seeknal.serving.discovery.cube import CubeSource
from seeknal.serving.discovery.lakebase import LakebaseSyncSource
from seeknal.serving.discovery.lakekeeper import LakekeeperSource


def test_lakekeeper_config_prefix_with_and_without():
    # 1. With prefix
    def handler_with_prefix(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/v1/config?warehouse=atlas"):
            return httpx.Response(200, json={"overrides": {"prefix": "catalog/v1"}})
        if url.endswith("/v1/catalog/v1/namespaces"):
            return httpx.Response(200, json={"namespaces": [["ns1"]]})
        if url.endswith("/v1/catalog/v1/namespaces/ns1/tables"):
            return httpx.Response(200, json={"identifiers": [{"name": "tbl1"}]})
        if url.endswith("/v1/catalog/v1/namespaces/ns1/tables/tbl1"):
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [
                            {
                                "schema-id": 0,
                                "fields": [{"id": 1, "name": "c1"}],
                                "identifier-field-ids": [1],
                            }
                        ],
                    }
                },
            )
        return httpx.Response(404)

    client1 = httpx.Client(transport=httpx.MockTransport(handler_with_prefix))
    cfg1 = LakekeeperConfig(url="https://lk.test", warehouse="atlas")
    src1 = LakekeeperSource(cfg1, client1)
    tables1 = src1.discover()
    assert len(tables1) == 1
    assert tables1[0].name == "tbl1"
    assert tables1[0].namespace == "ns1"

    # 2. Without prefix
    def handler_no_prefix(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/v1/config?warehouse=atlas"):
            return httpx.Response(200, json={"defaults": {"prefix": ""}})
        if url.endswith("/v1/namespaces"):
            return httpx.Response(200, json={"namespaces": [["ns2"]]})
        if url.endswith("/v1/namespaces/ns2/tables"):
            return httpx.Response(200, json={"identifiers": [{"name": "tbl2"}]})
        if url.endswith("/v1/namespaces/ns2/tables/tbl2"):
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [
                            {
                                "schema-id": 0,
                                "fields": [{"id": 1, "name": "c2"}],
                            }
                        ],
                    }
                },
            )
        return httpx.Response(404)

    client2 = httpx.Client(transport=httpx.MockTransport(handler_no_prefix))
    cfg2 = LakekeeperConfig(url="https://lk.test", warehouse="atlas")
    src2 = LakekeeperSource(cfg2, client2)
    tables2 = src2.discover()
    assert len(tables2) == 1
    assert tables2[0].name == "tbl2"
    assert tables2[0].namespace == "ns2"


def test_lakekeeper_namespace_allowlist_and_multilevel_skipped():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/v1/config" in url:
            return httpx.Response(200, json={})
        if url.endswith("/v1/namespaces"):
            return httpx.Response(
                200,
                json={
                    "namespaces": [
                        ["claims"],
                        ["analytics"],
                        ["ignored_ns"],
                        ["nested", "level"],
                        "single_str",
                        "multi.str",
                    ]
                },
            )
        if "/tables" in url:
            return httpx.Response(200, json={"identifiers": []})
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = LakekeeperConfig(
        url="https://lk.test",
        warehouse="atlas",
        namespaces=("claims", "analytics", "single_str"),
    )
    src = LakekeeperSource(cfg, client)
    src.discover()

    # Verify warnings for multi-level namespaces
    assert any("nested.level" in w for w in src.warnings)
    assert any("multi.str" in w for w in src.warnings)


def test_lakekeeper_paging():
    called_pages = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={})
        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["ns1"]]})
        if path == "/v1/namespaces/ns1/tables":
            page = request.url.params.get("pageToken")
            called_pages.append(page)
            if not page:
                return httpx.Response(
                    200,
                    json={
                        "identifiers": [{"name": "t1"}],
                        "next-page-token": "p2",
                    },
                )
            elif page == "p2":
                return httpx.Response(
                    200,
                    json={
                        "identifiers": [{"name": "t2"}],
                    },
                )
        if path in ("/v1/namespaces/ns1/tables/t1", "/v1/namespaces/ns1/tables/t2"):
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "col"}]}],
                    }
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = LakekeeperConfig(url="https://lk.test", warehouse="atlas")
    src = LakekeeperSource(cfg, client)
    tables = src.discover()
    assert len(tables) == 2
    assert [t.name for t in tables] == ["t1", "t2"]
    assert called_pages == [None, "p2"]


def test_lakekeeper_metadata_parsing():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/v1/config" in url:
            return httpx.Response(200, json={})
        if url.endswith("/v1/namespaces"):
            return httpx.Response(200, json={"namespaces": [["claims"]]})
        if url.endswith("/v1/namespaces/claims/tables"):
            return httpx.Response(200, json={"identifiers": [{"name": "claim"}]})
        if url.endswith("/v1/namespaces/claims/tables/claim"):
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 2,
                        "schemas": [
                            {
                                "schema-id": 1,
                                "fields": [{"id": 1, "name": "old_col"}],
                            },
                            {
                                "schema-id": 2,
                                "fields": [
                                    {"id": 10, "name": "claim_id"},
                                    {"id": 20, "name": "patient_id"},
                                    {"id": 30, "name": "amount"},
                                ],
                                "identifier-field-ids": [10, 20],
                            },
                        ],
                        "current-snapshot-id": 9999,
                        "snapshots": [
                            {
                                "snapshot-id": 9999,
                                "timestamp-ms": 1720000000000,
                                "summary": {"total-records": "500000"},
                            }
                        ],
                    }
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = LakekeeperConfig(url="https://lk.test", warehouse="atlas")
    src = LakekeeperSource(cfg, client)
    tables = src.discover()
    assert len(tables) == 1
    t = tables[0]
    assert t.namespace == "claims"
    assert t.name == "claim"
    assert t.columns == ("claim_id", "patient_id", "amount")
    assert t.keys == ("claim_id", "patient_id")
    assert t.row_count == 500000
    assert t.current_snapshot_id == 9999
    assert t.snapshot_ms == 1720000000000


def test_lakekeeper_broken_metadata_skipped_with_warning():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/v1/config" in url:
            return httpx.Response(200, json={})
        if url.endswith("/v1/namespaces"):
            return httpx.Response(200, json={"namespaces": [["ns"]]})
        if url.endswith("/v1/namespaces/ns/tables"):
            return httpx.Response(
                200,
                json={"identifiers": [{"name": "good"}, {"name": "broken"}]},
            )
        if url.endswith("/v1/namespaces/ns/tables/good"):
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "c"}]}],
                    }
                },
            )
        if url.endswith("/v1/namespaces/ns/tables/broken"):
            return httpx.Response(500, text="Internal Server Error")
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = LakekeeperConfig(url="https://lk.test", warehouse="atlas")
    src = LakekeeperSource(cfg, client)
    tables = src.discover()
    assert len(tables) == 1
    assert tables[0].name == "good"
    assert any("broken" in w for w in src.warnings)


def test_lakekeeper_namespace_failure_raises_discovery_error():
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/v1/config" in url:
            return httpx.Response(200, json={})
        if url.endswith("/v1/namespaces"):
            return httpx.Response(500, text="Server exploded")
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = LakekeeperConfig(url="https://lk.test", warehouse="atlas")
    src = LakekeeperSource(cfg, client)
    with pytest.raises(DiscoveryError, match="Failed to list namespaces"):
        src.discover()


def test_lakekeeper_bearer_token_and_never_leaked_in_errors():
    captured_auth = []
    secret_token = "super-secret-lakekeeper-token-98765"

    def handler(request: httpx.Request) -> httpx.Response:
        auth = request.headers.get("Authorization")
        captured_auth.append(auth)
        raise httpx.RequestError(
            f"Network error while using credentials with token {secret_token}",
            request=request,
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = LakekeeperConfig(
        url="https://lk.test",
        warehouse="atlas",
        token_env="MY_LK_TOKEN",
    )
    src = LakekeeperSource(cfg, client, env={"MY_LK_TOKEN": secret_token})

    with pytest.raises(DiscoveryError) as exc_info:
        src.discover()

    assert captured_auth[0] == f"Bearer {secret_token}"
    error_str = str(exc_info.value)
    assert secret_token not in error_str
    assert "***" in error_str


def test_cube_source_meta_parsing_and_failure():
    secret_token = "secret-cube-token-4321"
    captured_auth = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url.endswith("/cubejs-api/v1/meta"):
            auth = request.headers.get("Authorization")
            captured_auth.append(auth)
            return httpx.Response(
                200,
                json={
                    "cubes": [
                        {
                            "name": "orders",
                            "measures": [
                                {"name": "orders.count"},
                                {"name": "orders.total_amount"},
                            ],
                            "dimensions": [
                                {"name": "orders.status"},
                                {"name": "orders.created_at"},
                            ],
                        },
                        {
                            "name": "revenue_view",
                            "measures": [{"name": "revenue_view.mrr"}],
                            "dimensions": [{"name": "revenue_view.tier"}],
                        },
                    ]
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    eng = EngineConfig(
        name="cube",
        kind="cube",
        dialect="postgres",
        url="http://cube:4000",
        token_env="CUBE_TOKEN",
    )
    src = CubeSource(eng, client, env={"CUBE_TOKEN": secret_token})
    cubes = src.discover()
    assert len(cubes) == 2
    assert captured_auth[0] == secret_token

    c1 = cubes[0]
    assert c1.name == "orders"
    assert c1.measures == ("count", "total_amount")
    assert c1.dimensions == ("status", "created_at")

    c2 = cubes[1]
    assert c2.name == "revenue_view"
    assert c2.measures == ("mrr",)
    assert c2.dimensions == ("tier",)

    # Test failure
    def failing_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=f"Error with {secret_token}")

    failing_client = httpx.Client(transport=httpx.MockTransport(failing_handler))
    failing_src = CubeSource(eng, failing_client, env={"CUBE_TOKEN": secret_token})
    with pytest.raises(DiscoveryError) as exc_info:
        failing_src.discover()

    assert secret_token not in str(exc_info.value)


def test_lakebase_sync_state_and_db_error():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE _lakebase_sync_state (
            target_table TEXT PRIMARY KEY,
            snapshot_id INTEGER,
            synced_at TEXT
        )
        """
    )
    # Insert ISO string naive (interpreted as UTC) and ISO string aware
    cursor.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.claim", 101, "2026-05-01T12:00:00"),
    )
    cursor.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.patient", 202, "2026-05-01T12:00:00+00:00"),
    )
    conn.commit()

    src = LakebaseSyncSource(lambda: conn)
    state = src.read_state()

    assert len(state) == 2
    assert state["public.claim"].snapshot_id == 101
    assert state["public.claim"].synced_at == datetime(
        2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc
    )
    assert state["public.patient"].snapshot_id == 202
    assert state["public.patient"].synced_at == datetime(
        2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc
    )

    # Database error test
    cursor.execute("DROP TABLE _lakebase_sync_state")
    conn.commit()
    with pytest.raises(DiscoveryError, match="Failed to query Lakebase sync state"):
        src.read_state()


def test_lakebase_sync_source_qualified_table_and_identifier_validation():
    from tests.serving.fake_postgres_catalog import FakePostgresCatalog

    cat = FakePostgresCatalog()
    cat.add_sync_state(
        "rpt.daily",
        snapshot_id=777,
        synced_at=datetime(2026, 11, 19, 10, 0, 0, tzinfo=timezone.utc),
    )
    cat.add_sync_state(
        "stg.orders",
        snapshot_id=888,
        synced_at="2026-11-19T11:30:00Z",
        table_name="stg._lakebase_sync_state",
    )

    # Default queries qualified public._lakebase_sync_state
    src_default = LakebaseSyncSource(cat.connect)
    st_default = src_default.read_state()
    assert st_default["rpt.daily"].snapshot_id == 777
    assert any(
        sql == "SELECT target_table, snapshot_id, synced_at FROM public._lakebase_sync_state"
        for sql, _ in cat.executed
    )

    # Custom schema-qualified sync_state_table
    src_custom = LakebaseSyncSource(
        cat.connect, sync_state_table="stg._lakebase_sync_state"
    )
    st_custom = src_custom.read_state()
    assert st_custom["stg.orders"].snapshot_id == 888

    # Invalid sync_state_table identifiers rejected with DiscoveryConfigError
    for bad_tbl in (
        "",
        "Public._lakebase_sync_state",
        "a.b.c",
        "public._lakebase_sync_state; DROP TABLE x",
        "public._lakebase_sync_state\n",
        "1bad.tbl",
    ):
        with pytest.raises(DiscoveryConfigError):
            LakebaseSyncSource(cat.connect, sync_state_table=bad_tbl)


def test_discovery_config_postgres_sources_and_policy_validation():
    # Defaults: lakebase gets public._lakebase_sync_state; postgres gets None
    cfg = DiscoveryConfig.from_dict(
        {
            "default_catalog": "cbn",
            "engines": {
                "pg_report": {"kind": "postgres", "dialect": "postgres"},
                "lb_ops": {"kind": "lakebase", "dialect": "postgres"},
            },
            "postgres_sources": {
                "pg_report": {
                    "schemas": {"rpt": {"include": ["rpt_*"], "exclude": ["*_tmp"]}},
                },
                "lb_ops": {
                    "account": "ask",
                    "logical_catalog": "ops",
                    "schema_map": {"phys_ops": "ops"},
                    "schemas": {"phys_ops": {"tokenized": True}},
                },
            },
            "policy": {
                "require_filter_min_rows": 50_000,
                "max_in_list": 500,
                "max_offset": 5_000,
                "guard_mode": "warn",
                "pii_object_storage_engines": ["lb_ops"],
            },
        }
    )

    pg_src = cfg.postgres_sources["pg_report"]
    assert pg_src.account == "interactive"
    assert pg_src.logical_catalog == "pg_report"
    assert pg_src.freshness is not None
    assert pg_src.freshness.sync_state_table is None

    lb_src = cfg.postgres_sources["lb_ops"]
    assert lb_src.account == "ask"
    assert lb_src.logical_catalog == "ops"
    assert dict(lb_src.schema_map) == {"phys_ops": "ops"}
    assert lb_src.freshness is not None
    assert lb_src.freshness.sync_state_table == "public._lakebase_sync_state"
    assert lb_src.schemas["phys_ops"].tokenized is True

    assert cfg.require_filter_min_rows == 50_000
    assert cfg.max_in_list == 500
    assert cfg.max_offset == 5_000
    assert cfg.guard_mode == "warn"
    assert cfg.pii_object_storage_engines == ("lb_ops",)

    # Reject non-postgres/lakebase engine in postgres_sources
    with pytest.raises(DiscoveryConfigError, match="kind 'postgres' or 'lakebase'"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "cbn",
                "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
                "postgres_sources": {"sr": {"schemas": {"rpt": {}}}},
            }
        )

    # Reject invalid account
    with pytest.raises(DiscoveryConfigError, match="account"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "cbn",
                "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
                "postgres_sources": {
                    "pg": {"account": "superuser", "schemas": {"rpt": {}}}
                },
            }
        )

    # Reject empty schemas
    with pytest.raises(DiscoveryConfigError, match="schemas"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "cbn",
                "engines": {"pg": {"kind": "postgres", "dialect": "postgres"}},
                "postgres_sources": {"pg": {"schemas": {}}},
            }
        )

    # Reject invalid guard_mode
    with pytest.raises(DiscoveryConfigError, match="guard_mode"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "cbn",
                "policy": {"guard_mode": "off"},
            }
        )

