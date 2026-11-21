"""Tests for Cube discovery resilience (Section A) and reserved suffixes (Section B)."""

from __future__ import annotations

import io
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone

import httpx
import pytest
import yaml

from seeknal.serving import Placement, ServingPipeline
from seeknal.serving.discovery import main
from seeknal.serving.discovery.build import discover
from seeknal.serving.discovery.config import DiscoveryError
from seeknal.serving.discovery.cube import CubeSource
from seeknal.serving.discovery.refresh import PlacementRefresher


# ==============================================================================
# Helpers
# ==============================================================================

def _make_base_config():
    return {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 60,
        "lakekeeper": {
            "url": "https://lk.test",
            "warehouse": "atlas",
        },
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks", "catalog": "starrocks_cat"},
            "lb": {"kind": "lakebase", "dialect": "postgres"},
            "cube": {
                "kind": "cube",
                "dialect": "postgres",
                "url": "http://cube.test",
                "token_env": "CUBE_TOKEN",
            },
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


def _lakekeeper_and_lakebase_mock(request: httpx.Request) -> httpx.Response:
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
                    "current-snapshot-id": 100,
                }
            },
        )
    return httpx.Response(404)


def _make_sqlite_lakebase():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE _lakebase_sync_state (
            target_table TEXT PRIMARY KEY,
            snapshot_id INTEGER,
            synced_at TEXT
        )
        """
    )
    cur.execute(
        "INSERT INTO _lakebase_sync_state VALUES (?, ?, ?)",
        ("public.claim", 100, "2026-05-01T10:00:00+00:00"),
    )
    conn.commit()
    return conn


# ==============================================================================
# Section A: Cube Failure Isolation
# ==============================================================================

@pytest.mark.parametrize(
    "cube_status_or_error",
    [
        500,
        502,
        503,
        "connect_error",
        "invalid_json",
    ],
)
def test_cube_failure_isolation_continues_discovery(cube_status_or_error):
    secret_token = "secret-token-xyz-12345"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/cubejs-api/v1/meta":
            if cube_status_or_error == "connect_error":
                raise httpx.ConnectError(f"Connection refused with token {secret_token}", request=request)
            elif cube_status_or_error == "invalid_json":
                return httpx.Response(200, text="not valid json")
            else:
                return httpx.Response(cube_status_or_error, text=f"Cube error with {secret_token}")
        return _lakekeeper_and_lakebase_mock(request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    sqlite_conn = _make_sqlite_lakebase()
    cfg = _make_base_config()

    result = discover(
        cfg,
        http_client=client,
        lakebase_connect=lambda: sqlite_conn,
        env={"CUBE_TOKEN": secret_token},
    )

    # 1. Placement cubes is empty
    assert result.placement["cubes"] == {}

    # 2. No cube appears in schemas
    assert "claims_cube" not in result.schemas
    for name in result.schemas:
        assert not name.endswith("_cube")

    # 3. Warning mentions cube metadata was unavailable and cubes were omitted
    cube_warnings = [
        w for w in result.warnings
        if "Cube metadata was unavailable" in w and "cubes were omitted" in w
    ]
    assert len(cube_warnings) == 1

    # 4. Secret token was never leaked in warnings
    assert secret_token not in cube_warnings[0]
    if cube_status_or_error == "connect_error":
        assert "***" in cube_warnings[0]

    # 5. Lakekeeper / StarRocks / Lakebase tables still present
    assert "lakekeeper.claims.claim" in result.placement["tables"]
    assert "lakekeeper.claims.claim" in result.schemas
    assert result.schemas["lakekeeper.claims.claim"] == ["claim_id", "amount"]

    # 6. Placement validates with Placement.from_dict
    placement = Placement.from_dict(result.placement)
    assert "sr" in placement.engines
    assert "lb" in placement.engines
    assert "cube" in placement.engines
    assert "lakekeeper.claims.claim" in placement.tables
    assert len(placement.cubes) == 0


def test_cube_failure_isolation_placement_refresher():
    """PlacementRefresher succeeds with Cube down, swapping catalog without cubes."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/cubejs-api/v1/meta":
            return httpx.Response(500, text="Cube unavailable")
        return _lakekeeper_and_lakebase_mock(request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    sqlite_conn = _make_sqlite_lakebase()
    cfg = _make_base_config()

    def discover_fn():
        return discover(
            cfg,
            http_client=client,
            lakebase_connect=lambda: sqlite_conn,
            env={"CUBE_TOKEN": "tok"},
        )

    pipeline = ServingPipeline(
        placement=Placement.from_dict(
            {"engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}}, "tables": {}, "cubes": {}}
        ),
        schemas={},
        public_keys={},
    )

    refresher = PlacementRefresher(discover_fn, pipeline, interval_seconds=60)
    success = refresher.refresh_once()

    assert success is True
    assert refresher.last_error is None
    assert refresher.consecutive_failures == 0
    # Cube catalog has no cubes, but has table
    assert "lakekeeper.claims.claim" in pipeline.schemas
    assert len(pipeline.placement.cubes) == 0


def test_cube_failure_isolation_one_shot_cli(tmp_path):
    """CLI exits 0 when Cube is down, writing valid placement and schemas, with warning on stderr."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/cubejs-api/v1/meta":
            return httpx.Response(503, text="Service Unavailable")
        return _lakekeeper_and_lakebase_mock(request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    sqlite_conn = _make_sqlite_lakebase()
    cfg_path = tmp_path / "discovery.yaml"
    placement_out = tmp_path / "placement.yaml"
    schemas_out = tmp_path / "schemas.json"

    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(_make_base_config(), f)

    stderr_buf = io.StringIO()
    old_stderr = sys.stderr
    try:
        sys.stderr = stderr_buf
        exit_code = main(
            [
                "--config", str(cfg_path),
                "--placement-out", str(placement_out),
                "--schemas-out", str(schemas_out),
            ],
            http_client=client,
            lakebase_connect=lambda: sqlite_conn,
            env={"CUBE_TOKEN": "cube-tok-123"},
        )
    finally:
        sys.stderr = old_stderr

    assert exit_code == 0
    stderr_output = stderr_buf.getvalue()
    assert "Warning: Cube metadata was unavailable" in stderr_output
    assert "cubes were omitted" in stderr_output

    with open(placement_out, "r", encoding="utf-8") as f:
        placement_dict = yaml.safe_load(f)
    assert placement_dict["cubes"] == {}
    assert "lakekeeper.claims.claim" in placement_dict["tables"]

    with open(schemas_out, "r", encoding="utf-8") as f:
        schemas_dict = json.load(f)
    assert "lakekeeper.claims.claim" in schemas_dict
    assert len([k for k in schemas_dict if k.endswith("_cube")]) == 0


def test_lakekeeper_failure_still_raises_discovery_error():
    """Lakekeeper failure is NOT swallowed; it still raises DiscoveryError."""
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/v1/namespaces"):
            return httpx.Response(500, text="Lakekeeper down")
        if request.url.path == "/v1/config":
            return httpx.Response(200, json={})
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = _make_base_config()

    with pytest.raises(DiscoveryError, match="Failed to list namespaces"):
        discover(cfg, http_client=client)


# ==============================================================================
# Section B: Reserved Suffixes
# ==============================================================================

def test_reserved_suffixes_omission_and_warnings():
    raw_meta = {
        "cubes": [
            {
                "name": "sales_cube",
                "measures": [
                    {"name": "sales_cube.total_amount"},
                    {"name": "sales_cube.total__null"},
                    {"name": "sales_cube.count__hash"},
                    {"name": "sales_cube.metric__weird"},
                ],
                "dimensions": [
                    {"name": "sales_cube.product"},
                    {"name": "sales_cube.product__redact"},
                    {"name": "sales_cube.product__null"},
                    {"name": "sales_cube.product__last4"},
                    {"name": "sales_cube.order_date"},
                    {"name": "sales_cube.order_date__year"},
                    {"name": "sales_cube.product__hash"},
                    {"name": "sales_cube.x__hash"},
                    {"name": "sales_cube.orphan__last4"},
                    {"name": "sales_cube.orphan__redact"},
                    {"name": "sales_cube.nik__weird"},
                ],
            }
        ]
    }

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/cubejs-api/v1/meta":
            return httpx.Response(200, json=raw_meta)
        return _lakekeeper_and_lakebase_mock(request)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    cfg = _make_base_config()
    sqlite_conn = _make_sqlite_lakebase()

    result = discover(
        cfg,
        http_client=client,
        lakebase_connect=lambda: sqlite_conn,
        env={"CUBE_TOKEN": "token"},
    )

    placement = Placement.from_dict(result.placement)
    cube_entry = placement.cubes["sales_cube"]

    # 1. Valid twins: recorded in mask_variants, excluded from dimensions and schemas
    assert cube_entry.mask_variants == {
        "product": ("redact", "null", "last4"),
        "order_date": ("year",),
    }

    # 2. Ordinary dimensions: only product, order_date, and nik__weird (non-reserved suffix)
    assert cube_entry.dimensions == ("product", "order_date", "nik__weird")

    # 3. Measures: total_amount and metric__weird (non-reserved suffix). total__null and count__hash omitted!
    assert cube_entry.measures == ("total_amount", "metric__weird")

    # 4. Schemas: ordinary dimensions + ordinary measures
    assert result.schemas["sales_cube"] == [
        "product",
        "order_date",
        "nik__weird",
        "total_amount",
        "metric__weird",
    ]

    # 5. Omitted members warning: each omitted member is named as <cube>.<member>
    expected_omitted = [
        "sales_cube.total__null",
        "sales_cube.count__hash",
        "sales_cube.product__hash",
        "sales_cube.x__hash",
        "sales_cube.orphan__last4",
        "sales_cube.orphan__redact",
    ]
    for omitted in expected_omitted:
        assert any(omitted in w for w in result.warnings), f"Expected warning for {omitted}"

    # 6. Valid twins and non-reserved suffix members should NOT have omission warnings
    not_omitted = [
        "sales_cube.product__redact",
        "sales_cube.product__null",
        "sales_cube.product__last4",
        "sales_cube.order_date__year",
        "sales_cube.nik__weird",
        "sales_cube.metric__weird",
    ]
    for member in not_omitted:
        assert not any(f"'{member}' has reserved suffix" in w for w in result.warnings)
