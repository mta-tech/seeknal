"""Tests for catalog refresh: pipeline swap, refresher background thread, and discovery CLI."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time

import httpx
import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric import ec

from seeknal.serving import (
    Placement,
    RoutingError,
    ServingPipeline,
    sign_grant_token,
)
from seeknal.serving.discovery.__main__ import main
from seeknal.serving.discovery.build import DiscoveryResult
from seeknal.serving.discovery.config import DiscoveryError
from seeknal.serving.discovery.refresh import PlacementRefresher


@pytest.fixture
def ec_key():
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    return priv, pub


def _make_token(priv_key, sql: str, tables_grant: dict) -> str:
    now = time.time()
    claims = {
        "sub": "analyst@atlas.org",
        "ws": "ws-test",
        "jti": "jti-001",
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 120,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": 100,
        "tables": tables_grant,
    }
    return sign_grant_token(claims, priv_key, kid="test-key")


def test_update_catalog_atomic_swap(ec_key):
    priv, pub = ec_key

    # Catalog 1: has table claims.t1
    cat1_placement = {
        "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
        "tables": {
            "claims.t1": {
                "keys": ["id"],
                "row_count": 1000,
                "serve": {"sr": "lakekeeper.claims.t1"},
            }
        },
        "cubes": {},
    }
    cat1_schemas = {"claims.t1": ["id", "val"]}

    pipeline = ServingPipeline(
        placement=Placement.from_dict(cat1_placement),
        schemas=cat1_schemas,
        public_keys={"test-key": pub},
    )

    # Query for t1 succeeds
    sql_t1 = "SELECT id FROM claims.t1"
    token_t1 = _make_token(
        priv,
        sql_t1,
        {"claims.t1": {"columns": ["id"], "masks": {}, "row_filters": [], "actions": ["select"]}},
    )
    prep1 = pipeline.prepare(sql_t1, token_t1)
    assert prep1.engine == "sr"

    # Query for t2 fails with RoutingError (unknown table)
    sql_t2 = "SELECT id FROM claims.t2"
    token_t2 = _make_token(
        priv,
        sql_t2,
        {"claims.t2": {"columns": ["id"], "masks": {}, "row_filters": [], "actions": ["select"]}},
    )
    with pytest.raises(RoutingError, match="Unknown table or cube 'claims.t2'"):
        pipeline.prepare(sql_t2, token_t2)

    # Catalog 2: removes claims.t1, adds claims.t2
    cat2_placement = {
        "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
        "tables": {
            "claims.t2": {
                "keys": ["id"],
                "row_count": 2000,
                "serve": {"sr": "lakekeeper.claims.t2"},
            }
        },
        "cubes": {},
    }
    cat2_schemas = {"claims.t2": ["id", "name"]}

    pipeline.update_catalog(Placement.from_dict(cat2_placement), cat2_schemas)

    # Now query for t2 succeeds
    prep2 = pipeline.prepare(sql_t2, token_t2)
    assert prep2.engine == "sr"

    # Query for removed t1 now raises RoutingError
    with pytest.raises(RoutingError, match="Unknown table or cube 'claims.t1'"):
        pipeline.prepare(sql_t1, token_t1)


def test_refresher_refresh_once_success_and_failure(ec_key):
    priv, pub = ec_key
    initial_placement = {
        "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
        "tables": {
            "claims.t1": {
                "keys": ["id"],
                "row_count": 100,
                "serve": {"sr": "lakekeeper.claims.t1"},
            }
        },
        "cubes": {},
    }
    initial_schemas = {"claims.t1": ["id"]}
    pipeline = ServingPipeline(
        placement=Placement.from_dict(initial_placement),
        schemas=initial_schemas,
        public_keys={"test-key": pub},
    )

    # Successful discovery
    mock_clock_time = 1000.0

    def mock_clock():
        return mock_clock_time

    updated_placement = {
        "engines": {"sr": {"kind": "starrocks", "dialect": "starrocks"}},
        "tables": {
            "claims.t2": {
                "keys": ["id"],
                "row_count": 200,
                "serve": {"sr": "lakekeeper.claims.t2"},
            }
        },
        "cubes": {},
    }
    mock_result = DiscoveryResult(
        placement=updated_placement,
        schemas={"claims.t2": ["id"]},
        warnings=(),
        discovered_at=time.time(),
    )

    should_fail = False

    def discover_fn():
        if should_fail:
            raise DiscoveryError("Catalog connection lost")
        return mock_result

    refresher = PlacementRefresher(
        discover_fn,
        pipeline,
        interval_seconds=60.0,
        clock=mock_clock,
    )

    # 1. Success pass
    assert refresher.refresh_once() is True
    assert refresher.last_success_at == 1000.0
    assert refresher.last_error is None
    assert refresher.consecutive_failures == 0
    assert "claims.t2" in pipeline.placement.tables

    # 2. Failure pass
    should_fail = True
    assert refresher.refresh_once() is False
    assert refresher.last_error == "DiscoveryError"
    assert refresher.consecutive_failures == 1
    # Last good catalog kept!
    assert "claims.t2" in pipeline.placement.tables

    # 3. Second failure increments consecutive_failures
    assert refresher.refresh_once() is False
    assert refresher.consecutive_failures == 2


def test_refresher_start_stop():
    count = 0

    def discover_fn():
        nonlocal count
        count += 1
        return DiscoveryResult(
            placement={"engines": {}, "tables": {}, "cubes": {}},
            schemas={},
            warnings=(),
            discovered_at=time.time(),
        )

    class DummyPipeline:
        def update_catalog(self, p, s):
            pass

    refresher = PlacementRefresher(
        discover_fn,
        DummyPipeline(),
        interval_seconds=0.04,
    )

    refresher.start()
    time.sleep(0.12)
    refresher.stop()

    assert count >= 1
    assert refresher._thread is None or not refresher._thread.is_alive()


def test_cli_writes_both_files_and_roundtrips(tmp_path):
    cfg_file = tmp_path / "cfg.yaml"
    placement_out = tmp_path / "placement.yaml"
    schemas_out = tmp_path / "schemas.json"

    cfg_data = {
        "default_catalog": "lakekeeper",
        "lakekeeper": {"url": "https://lk.test", "warehouse": "atlas"},
        "engines": {
            "sr": {"kind": "starrocks", "dialect": "starrocks"},
        },
    }
    with open(cfg_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg_data, f)

    def mock_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/config":
            return httpx.Response(200, json={})
        if path == "/v1/namespaces":
            return httpx.Response(200, json={"namespaces": [["analytics"]]})
        if path == "/v1/namespaces/analytics/tables":
            return httpx.Response(200, json={"identifiers": [{"name": "daily"}]})
        if path == "/v1/namespaces/analytics/tables/daily":
            return httpx.Response(
                200,
                json={
                    "metadata": {
                        "current-schema-id": 0,
                        "schemas": [
                            {"schema-id": 0, "fields": [{"id": 1, "name": "dt"}, {"id": 2, "name": "cnt"}]}
                        ],
                    }
                },
            )
        return httpx.Response(404)

    client = httpx.Client(transport=httpx.MockTransport(mock_handler))

    ret = main(
        [
            "--config",
            str(cfg_file),
            "--placement-out",
            str(placement_out),
            "--schemas-out",
            str(schemas_out),
        ],
        http_client=client,
    )
    assert ret == 0

    assert placement_out.exists()
    assert schemas_out.exists()

    # Round trip placement through Placement.from_yaml
    p = Placement.from_yaml(placement_out)
    assert "lakekeeper.analytics.daily" in p.tables
    assert p.tables["lakekeeper.analytics.daily"].serves[0].physical_name == "lakekeeper.analytics.daily"

    # Verify schemas.json content
    with open(schemas_out, "r", encoding="utf-8") as f:
        schemas = json.load(f)
    assert schemas == {"lakekeeper.analytics.daily": ["dt", "cnt"]}


def test_cli_config_error_clean_exit_2_and_no_token(tmp_path, capsys):
    cfg_file = tmp_path / "bad_cfg.yaml"
    secret_token = "secret-token-do-not-leak-77777"

    # Missing required default_catalog
    with open(cfg_file, "w", encoding="utf-8") as f:
        f.write("refresh_interval_seconds: -10\n")

    ret = main(
        [
            "--config",
            str(cfg_file),
            "--placement-out",
            str(tmp_path / "p.yaml"),
            "--schemas-out",
            str(tmp_path / "s.json"),
        ],
        env={"TOKEN": secret_token},
    )
    assert ret == 2

    captured = capsys.readouterr()
    assert "Error:" in captured.err
    assert "Traceback" not in captured.err
    assert secret_token not in captured.err
