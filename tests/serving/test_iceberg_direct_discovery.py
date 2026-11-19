"""Tests for discovery and placement with DuckDB iceberg_direct engine profile."""

from __future__ import annotations

from datetime import datetime, timezone

import httpx
import pytest

from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.config import DiscoveryConfig, DiscoveryConfigError
from seeknal.serving.placement import Placement
from seeknal.serving.router import Router, RoutingError


def _fake_lakekeeper_transport(
    *,
    snapshot_id: int = 9001,
    snapshot_ms: int = 1777636800000,  # 2026-05-01T12:00:00Z
    row_count: int = 2500000,
) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={"defaults": {"prefix": "wh"}})
        if path == "/v1/wh/namespaces":
            return httpx.Response(200, json={"namespaces": [["analytics"]]})
        if path == "/v1/wh/namespaces/analytics/tables":
            return httpx.Response(
                200,
                json={"identifiers": [{"name": "events"}]},
            )
        if path == "/v1/wh/namespaces/analytics/tables/events":
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [
                            {
                                "schema-id": 0,
                                "fields": [
                                    {"id": 1, "name": "event_id"},
                                    {"id": 2, "name": "event_type"},
                                    {"id": 3, "name": "created_at"},
                                ],
                                "identifier-field-ids": [1],
                            }
                        ],
                        "current-snapshot-id": snapshot_id,
                        "snapshots": [
                            {
                                "snapshot-id": snapshot_id,
                                "timestamp-ms": snapshot_ms,
                                "summary": {"total-records": str(row_count)},
                            }
                        ],
                    }
                },
            )
        return httpx.Response(404)

    return httpx.MockTransport(handler)


def test_discovery_emits_duckdb_iceberg_direct_serve_and_max_lag():
    """Discovery emits <alias>.<namespace>.<table> and max_lag_seconds from current-snapshot timestamp age + refresh_interval."""
    # Snapshot is at 2026-05-01T12:00:00Z (1777636800000 ms).
    # Discovery now is 2026-05-01T12:02:00Z -> snapshot age = 120s.
    # refresh_interval_seconds = 45s -> expected max_lag_seconds = 120 + 45 = 165.0s.
    client = httpx.Client(
        transport=_fake_lakekeeper_transport(
            snapshot_id=777123,
            snapshot_ms=1777636800000,
        )
    )

    cfg_dict = {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 45.0,
        "lakekeeper": {
            "url": "https://lk.internal.test",
            "warehouse": "main_wh",
        },
        "engines": {
            "duck_ice": {
                "kind": "duckdb",
                "dialect": "duckdb",
                "profile": "iceberg_direct",
                "alias": "lake",
            }
        },
    }

    now_dt = datetime(2026, 5, 1, 12, 2, 0, tzinfo=timezone.utc)
    res = discover(cfg_dict, http_client=client, now=now_dt)

    assert "lakekeeper.analytics.events" in res.placement["tables"]
    tbl_entry = res.placement["tables"]["lakekeeper.analytics.events"]
    assert tbl_entry["keys"] == ["event_id"]
    assert tbl_entry["row_count"] == 2500000
    assert "duck_ice" in tbl_entry["serve"]

    duck_serve = tbl_entry["serve"]["duck_ice"]
    assert duck_serve["name"] == "lake.analytics.events"
    assert duck_serve["max_lag_seconds"] == 165.0

    # Placement validates and routes analytics query to duck_ice
    placement = Placement.from_dict(res.placement)
    router = Router(placement)
    decision = router.route("SELECT count(*) FROM lakekeeper.analytics.events")
    assert decision.engine == "duck_ice"
    assert "lake.analytics.events AS events" in decision.sql

    # Staleness rejection when max_staleness_seconds < max_lag_seconds (165.0s)
    with pytest.raises(RoutingError, match="staleness"):
        router.route(
            "SELECT count(*) FROM lakekeeper.analytics.events",
            max_staleness_seconds=100.0,
        )


def test_discovery_duckdb_local_file_does_not_emit_lake_serve():
    """Local-file duckdb engines in discovery are unchanged and do not automatically receive Lakekeeper serves."""
    client = httpx.Client(transport=_fake_lakekeeper_transport())
    cfg_dict = {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 60.0,
        "lakekeeper": {
            "url": "https://lk.internal.test",
            "warehouse": "main_wh",
        },
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks", "catalog": "lk_cat"},
            "duck_local": {"kind": "duckdb", "dialect": "duckdb"},
        },
    }

    res = discover(
        cfg_dict,
        http_client=client,
        now=datetime(2026, 5, 1, 12, 1, 0, tzinfo=timezone.utc),
    )
    serves = res.placement["tables"]["lakekeeper.analytics.events"]["serve"]
    assert "sr" in serves
    assert "duck_local" not in serves


def test_discovery_pin_current_snapshot_option():
    """When pin_current_snapshot is true on an iceberg_direct discovery engine, serve includes snapshot_id."""
    client = httpx.Client(
        transport=_fake_lakekeeper_transport(
            snapshot_id=4455667788,
            snapshot_ms=1777636800000,
        )
    )
    cfg_dict = {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 30.0,
        "lakekeeper": {
            "url": "https://lk.internal.test",
            "warehouse": "main_wh",
        },
        "engines": {
            "duck_ice": {
                "kind": "duckdb",
                "dialect": "duckdb",
                "profile": "iceberg_direct",
                "alias": "lake",
                "pin_current_snapshot": True,
            }
        },
    }
    res = discover(
        cfg_dict,
        http_client=client,
        now=datetime(2026, 5, 1, 12, 0, 30, tzinfo=timezone.utc),
    )
    duck_serve = res.placement["tables"]["lakekeeper.analytics.events"]["serve"]["duck_ice"]
    assert duck_serve["snapshot_id"] == 4455667788

    placement = Placement.from_dict(res.placement)
    router = Router(placement)
    decision = router.route("SELECT event_id FROM lakekeeper.analytics.events")
    assert decision.sql == (
        "SELECT event_id FROM lake.analytics.events AT (VERSION => 4455667788) AS events"
    )


def test_discovery_config_rejects_invalid_duckdb_profile():
    with pytest.raises(DiscoveryConfigError, match="profile"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "lakekeeper",
                "engines": {
                    "duck": {
                        "kind": "duckdb",
                        "dialect": "duckdb",
                        "profile": "unknown_profile",
                    }
                },
            }
        )

    with pytest.raises(DiscoveryConfigError, match="profile"):
        DiscoveryConfig.from_dict(
            {
                "default_catalog": "lakekeeper",
                "engines": {
                    "sr": {
                        "kind": "starrocks",
                        "dialect": "starrocks",
                        "profile": "iceberg_direct",
                    }
                },
            }
        )
