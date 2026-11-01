"""Tests for discovery build: Placement construction, lag calculation, and pipeline integration."""

from __future__ import annotations

import hashlib
import sqlite3
import time
from datetime import datetime, timezone

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import Placement, ServingPipeline, sign_grant_token
from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.config import DiscoveryConfig


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
        "jti": "jti-service-001",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": tables_grant,
    }
    return sign_grant_token(claims, priv_key, kid="key-test")


def test_discovery_build_full_and_starrocks_physical_names():
    def mock_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={})
        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["claims"]]})
        if path == "/v1/namespaces/claims/tables":
            return httpx.Response(200, json={"identifiers": [{"name": "claim"}]})
        if path == "/v1/namespaces/claims/tables/claim":
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [
                            {
                                "schema-id": 0,
                                "fields": [
                                    {"id": 1, "name": "claim_id"},
                                    {"id": 2, "name": "amount"},
                                ],
                                "identifier-field-ids": [1],
                            }
                        ],
                        "current-snapshot-id": 500,
                        "snapshots": [
                            {
                                "snapshot-id": 500,
                                "timestamp-ms": 1700000000000,
                                "summary": {"total-records": "10000000"},
                            }
                        ],
                    }
                },
            )
        if path == "/cubejs-api/v1/meta":
            return httpx.Response(
                200,
                json={
                    "cubes": [
                        {
                            "name": "claims_cube",
                            "measures": [{"name": "claims_cube.total_amount"}],
                            "dimensions": [{"name": "claims_cube.claim_id"}],
                        }
                    ]
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))

    sqlite_conn = sqlite3.connect(":memory:")
    cursor = sqlite_conn.cursor()
    cursor.execute(
        """
        CREATE TABLE _lakebase_sync_state (
            target_table TEXT PRIMARY KEY,
            snapshot_id INTEGER,
            synced_at TEXT
        )
        """
    )
    cursor.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.claim", 500, "2026-05-01T10:00:00+00:00"),
    )
    sqlite_conn.commit()

    cfg_dict = {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 60,
        "lakekeeper": {
            "url": "https://lk.test",
            "warehouse": "atlas",
        },
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks", "catalog": "starrocks_cat"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
            "cube": {"kind": "cube", "dialect": "postgres", "url": "http://cube.test"},
        },
        "lakebase": {
            "engine": "lb",
            "synced_tables": {
                "lakekeeper.claims.claim": {
                    "target": "public.claim",
                    "primary_key": ["claim_id"],
                }
            },
        },
    }

    result = discover(
        cfg_dict,
        http_client=client,
        lakebase_connect=lambda: sqlite_conn,
        now=datetime(2026, 5, 1, 10, 1, 0, tzinfo=timezone.utc),
    )

    # 1. Full build gives valid placement
    placement = Placement.from_dict(result.placement)
    assert "sr" in placement.engines
    assert "lb" in placement.engines
    assert "cube" in placement.engines

    # 2. StarRocks physical names
    table_entry = placement.tables["lakekeeper.claims.claim"]
    assert table_entry.name == "lakekeeper.claims.claim"
    assert table_entry.keys == ("claim_id",)
    assert table_entry.row_count == 10000000

    sr_serve = next(s for s in table_entry.serves if s.engine == "sr")
    assert sr_serve.physical_name == "starrocks_cat.claims.claim"
    assert sr_serve.max_lag_seconds is None

    # 3. Cubes
    assert "claims_cube" in placement.cubes
    cube_entry = placement.cubes["claims_cube"]
    assert cube_entry.measures == ("total_amount",)
    assert cube_entry.dimensions == ("claim_id",)
    assert cube_entry.engine == "cube"


def test_lakebase_lag_calculation_matching_and_differing():
    def mock_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={})
        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["claims"]]})
        if path == "/v1/namespaces/claims/tables":
            return httpx.Response(
                200,
                json={"identifiers": [{"name": "t_match"}, {"name": "t_diff"}]},
            )
        if path == "/v1/namespaces/claims/tables/t_match":
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "id"}]}],
                        "current-snapshot-id": 100,
                    }
                },
            )
        if path == "/v1/namespaces/claims/tables/t_diff":
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [{"schema-id": 0, "fields": [{"id": 1, "name": "id"}]}],
                        "current-snapshot-id": 105,
                    }
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))

    sqlite_conn = sqlite3.connect(":memory:")
    cursor = sqlite_conn.cursor()
    cursor.execute(
        """
        CREATE TABLE _lakebase_sync_state (
            target_table TEXT PRIMARY KEY,
            snapshot_id INTEGER,
            synced_at TEXT
        )
        """
    )
    # t_match has matching snapshot_id=100
    cursor.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.t_match", 100, "2026-05-01T12:00:00+00:00"),
    )
    # t_diff has snapshot_id=100 (differs from Iceberg snapshot 105)
    # synced_at is 12:00:00. Now is 12:01:40 (100 seconds later).
    cursor.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.t_diff", 100, "2026-05-01T12:00:00+00:00"),
    )
    sqlite_conn.commit()

    cfg = {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 60,
        "lakekeeper": {"url": "https://lk.test", "warehouse": "atlas"},
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "lakebase": {
            "engine": "lb",
            "synced_tables": {
                "lakekeeper.claims.t_match": {"target": "public.t_match", "primary_key": ["id"]},
                "lakekeeper.claims.t_diff": {"target": "public.t_diff", "primary_key": ["id"]},
            },
        },
    }

    now_dt = datetime(2026, 5, 1, 12, 1, 40, tzinfo=timezone.utc)
    res = discover(cfg, http_client=client, lakebase_connect=lambda: sqlite_conn, now=now_dt)

    t_match_serves = res.placement["tables"]["lakekeeper.claims.t_match"]["serve"]
    t_diff_serves = res.placement["tables"]["lakekeeper.claims.t_diff"]["serve"]

    # When matching: observed_lag = 0, max_lag = 0 + 60 = 60
    assert t_match_serves["lb"]["max_lag_seconds"] == 60.0

    # When differing: observed_lag = 100s, max_lag = 100 + 60 = 160
    assert t_diff_serves["lb"]["max_lag_seconds"] == 160.0


def test_lakebase_no_state_row_and_key_fallback():
    def mock_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={})
        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["claims"]]})
        if path == "/v1/namespaces/claims/tables":
            return httpx.Response(
                200,
                json={"identifiers": [{"name": "t_no_state"}, {"name": "t_no_serve"}]},
            )
        if path == "/v1/namespaces/claims/tables/t_no_state":
            # Table has no Iceberg identifier-field-ids -> keys fallback to synced_tables primary_key
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [
                            {"schema-id": 0, "fields": [{"id": 1, "name": "k1"}, {"id": 2, "name": "val"}]}
                        ],
                    }
                },
            )
        if path == "/v1/namespaces/claims/tables/t_no_serve":
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

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))

    # Empty lakebase sync state
    sqlite_conn = sqlite3.connect(":memory:")
    cursor = sqlite_conn.cursor()
    cursor.execute(
        """
        CREATE TABLE _lakebase_sync_state (
            target_table TEXT PRIMARY KEY,
            snapshot_id INTEGER,
            synced_at TEXT
        )
        """
    )
    sqlite_conn.commit()

    cfg = {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 60,
        "lakekeeper": {"url": "https://lk.test", "warehouse": "atlas"},
        "engines": {
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "lakebase": {
            "engine": "lb",
            "synced_tables": {
                "lakekeeper.claims.t_no_state": {
                    "target": "public.t_no_state",
                    "primary_key": ["k1"],
                }
            },
        },
    }

    res = discover(cfg, http_client=client, lakebase_connect=lambda: sqlite_conn)

    # 1. No state row: no lakebase serve, plus a warning
    assert any("no state row in _lakebase_sync_state" in w for w in res.warnings)

    # 2. Since only lakebase engine was configured and t_no_state had no state row,
    # neither t_no_state nor t_no_serve had any serve at all!
    # A table with no serve at all is left out with a warning
    assert "lakekeeper.claims.t_no_state" not in res.placement["tables"]
    assert "lakekeeper.claims.t_no_serve" not in res.placement["tables"]
    assert any("has no serve and was omitted" in w for w in res.warnings)


def test_key_fallback_to_primary_key_when_served_on_starrocks():
    def mock_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={})
        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["claims"]]})
        if path == "/v1/namespaces/claims/tables":
            return httpx.Response(200, json={"identifiers": [{"name": "claim"}]})
        if path == "/v1/namespaces/claims/tables/claim":
            # No identifier-field-ids in Iceberg
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [
                            {"schema-id": 0, "fields": [{"id": 1, "name": "claim_id"}]}
                        ],
                    }
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))
    cfg = {
        "default_catalog": "lakekeeper",
        "lakekeeper": {"url": "https://lk.test", "warehouse": "atlas"},
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "lakebase": {
            "engine": "lb",
            "synced_tables": {
                "lakekeeper.claims.claim": {
                    "target": "public.claim",
                    "primary_key": ["claim_id"],
                }
            },
        },
    }

    res = discover(cfg, http_client=client)
    assert res.placement["tables"]["lakekeeper.claims.claim"]["keys"] == ["claim_id"]


def test_result_drives_real_serving_pipeline(ec_key):
    priv, pub = ec_key

    def mock_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={})
        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["claims"]]})
        if path == "/v1/namespaces/claims/tables":
            return httpx.Response(200, json={"identifiers": [{"name": "claim"}]})
        if path == "/v1/namespaces/claims/tables/claim":
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [
                            {
                                "schema-id": 0,
                                "fields": [
                                    {"id": 1, "name": "claim_id"},
                                    {"id": 2, "name": "amount"},
                                    {"id": 3, "name": "patient_id"},
                                ],
                                "identifier-field-ids": [1],
                            }
                        ],
                        "current-snapshot-id": 100,
                        "snapshots": [
                            {
                                "snapshot-id": 100,
                                "summary": {"total-records": "5000000"},
                            }
                        ],
                    }
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))

    sqlite_conn = sqlite3.connect(":memory:")
    cursor = sqlite_conn.cursor()
    cursor.execute(
        """
        CREATE TABLE _lakebase_sync_state (
            target_table TEXT PRIMARY KEY,
            snapshot_id INTEGER,
            synced_at TEXT
        )
        """
    )
    cursor.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.claim", 100, "2026-05-01T12:00:00+00:00"),
    )
    sqlite_conn.commit()

    cfg = {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 60,
        "lakekeeper": {"url": "https://lk.test", "warehouse": "atlas"},
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
        },
        "lakebase": {
            "engine": "lb",
            "synced_tables": {
                "lakekeeper.claims.claim": {
                    "target": "public.claim",
                    "primary_key": ["claim_id"],
                }
            },
        },
    }

    res = discover(
        cfg,
        http_client=client,
        lakebase_connect=lambda: sqlite_conn,
        now=datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
    )

    pipeline = ServingPipeline(
        placement=Placement.from_dict(res.placement),
        schemas=res.schemas,
        public_keys={"key-test": pub},
    )

    grant_tables = {
        "lakekeeper.claims.claim": {
            "columns": ["claim_id", "amount", "patient_id"],
            "masks": {},
            "row_filters": [],
            "actions": ["select"],
        }
    }

    # 1. Analytics query routes to StarRocks
    sql_analytics = "SELECT count(*) FROM lakekeeper.claims.claim"
    token_analytics = _make_token(priv, sql_analytics, grant_tables)
    prep_analytics = pipeline.prepare(sql_analytics, token_analytics)
    assert prep_analytics.engine == "sr"
    assert prep_analytics.rule == "analytics"

    # 2. Key lookup routes to Lakebase
    sql_lookup = "SELECT claim_id, amount FROM lakekeeper.claims.claim WHERE claim_id = 123"
    token_lookup = _make_token(priv, sql_lookup, grant_tables)
    prep_lookup = pipeline.prepare(sql_lookup, token_lookup)
    assert prep_lookup.engine == "lb"
    assert prep_lookup.rule == "point_lookup"

    # 3. Key lookup with max_staleness_seconds below Lakebase lag (lag is 60s)
    # With max_staleness_seconds=30s, Lakebase is rejected for staleness, routes to StarRocks
    prep_stale = pipeline.prepare(
        sql_lookup,
        token_lookup,
        max_staleness_seconds=30.0,
    )
    assert prep_stale.engine == "sr"
