"""Tests for Seeknal Serving operations config, readiness probes, and Prometheus metrics."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving.adapters import (
    CubeAdapter,
    CubeSecurityConfig,
    DBAPIAdapter,
    DuckDBAdapter,
    EngineCancelled,
    EngineTimeout,
    LakebasePool,
    WorkloadRoutedAdapter,
)
from seeknal.serving.__main__ import main as serving_main
from seeknal.serving.audit import AuditRecord, AuditSink, AuditWriteError
from seeknal.serving.buildinfo import BUILD_INFO_KEYS
from seeknal.serving.duckdb_policy import DuckDBHolder
from seeknal.serving.grant_token import (
    GrantExpiredError,
    GrantTokenError,
    sign_cancel_token,
    sign_grant_token,
)
from seeknal.serving.http import GrantReplayedError, create_app
from seeknal.serving.metrics import (
    ALLOWED_GRANT_REJECTION_REASONS,
    ALLOWED_ROUTES,
    DEFAULT_DURATION_BUCKETS,
    PROMETHEUS_CONTENT_TYPE,
    MetricsRegistry,
    classify_grant_rejection,
)
from seeknal.serving.ops import (
    OpsConfig,
    ReadinessProbe,
    build_public_extra_routes,
    create_ops_app,
    make_readyz_handler,
    start_ops_server,
)
from seeknal.serving.placement import Placement
from seeknal.serving.server.app import build_server
from seeknal.serving.server.config import (
    DuckDBConnectionConfig,
    IcebergCatalogConfig,
    IcebergS3Config,
    ServerConfig,
    ServerConfigError,
)
from seeknal.serving.service import PreparedQuery, ServingPipeline


@pytest.fixture(autouse=True)
def _reset_duckdb_holder():
    DuckDBHolder.reset()
    yield
    DuckDBHolder.reset()


def _make_prepared_duckdb(sql: str = "SELECT 1", audit_id: str = "q-1") -> PreparedQuery:
    return PreparedQuery(
        sql=sql,
        params=(),
        paramstyle="qmark",
        engine="duck",
        engine_kind="duckdb",
        rule="test",
        reason="test",
        masked_columns=(),
        row_limit=10,
        audit_id=audit_id,
        subject="user@example.com",
        workspace="ws-test",
        redacted_sql=sql,
        cube_security_context=None,
    )


# ---------------------------------------------------------------------------
# 1. OpsConfig validation & defaults
# ---------------------------------------------------------------------------


def test_ops_config_defaults_and_from_dict():
    absent_cfg = OpsConfig()
    assert absent_cfg.enabled is False
    assert absent_cfg.host == "127.0.0.1"
    assert absent_cfg.port == 9090
    assert absent_cfg.auth_token_env is None
    assert absent_cfg.readyz_timeout_seconds == 2.0
    assert absent_cfg.readyz_cache_seconds == 5.0
    assert absent_cfg.public_metrics is False
    assert absent_cfg.public_buildinfo is False
    assert absent_cfg.public_readyz is True

    present_cfg = OpsConfig.from_dict({})
    assert present_cfg.enabled is True
    assert present_cfg.host == "127.0.0.1"
    assert present_cfg.port == 9090
    assert present_cfg.public_metrics is False
    assert present_cfg.public_buildinfo is False
    assert present_cfg.public_readyz is True

    custom_cfg = OpsConfig.from_dict(
        {
            "enabled": True,
            "host": "127.0.0.1",
            "port": 9102,
            "auth_token_env": "SEEKNAL_OPS_TOKEN_1",
            "readyz_timeout_seconds": 1.5,
            "readyz_cache_seconds": 0.0,
            "public_metrics": False,
            "public_buildinfo": False,
            "public_readyz": False,
        },
        error_cls=ServerConfigError,
    )
    assert custom_cfg.port == 9102
    assert custom_cfg.auth_token_env == "SEEKNAL_OPS_TOKEN_1"
    assert custom_cfg.readyz_timeout_seconds == 1.5
    assert custom_cfg.readyz_cache_seconds == 0.0
    assert custom_cfg.public_readyz is False


def test_ops_config_rejects_unknown_and_literal_secret_keys():
    for bad_key in ("token", "auth_token", "bearer_token", "secret", "password"):
        with pytest.raises(ServerConfigError) as exc_info:
            OpsConfig.from_dict(
                {bad_key: "CANARY_OPS_SECRET_VALUE_999"},
                error_cls=ServerConfigError,
            )
        assert "auth_token_env" in str(exc_info.value)
        assert "CANARY_OPS_SECRET_VALUE_999" not in str(exc_info.value)

    with pytest.raises(ValueError, match="Unknown key"):
        OpsConfig.from_dict({"unexpected_field": 123})

    # Invalid env var names (including accidentally pasted literal tokens) are rejected safely
    for bad_env in ("", "123BAD", "BAD-ENV-NAME", "Bearer secret_canary_xyz"):
        with pytest.raises(ServerConfigError) as exc_info:
            OpsConfig.from_dict(
                {"auth_token_env": bad_env},
                error_cls=ServerConfigError,
            )
        assert "secret_canary_xyz" not in str(exc_info.value)

    with pytest.raises(ValueError):
        OpsConfig.from_dict({"port": 0})
    with pytest.raises(ValueError):
        OpsConfig.from_dict({"port": 65536})
    with pytest.raises(ValueError):
        OpsConfig.from_dict({"port": True})
    with pytest.raises(ValueError):
        OpsConfig.from_dict({"readyz_timeout_seconds": 0})
    with pytest.raises(ValueError):
        OpsConfig.from_dict({"readyz_cache_seconds": -0.1})


# ---------------------------------------------------------------------------
# 2. Label allowlists, histogram buckets, counters, and dynamic gauges
# ---------------------------------------------------------------------------


def test_metrics_label_allowlist_blocks_pii_tokens_tables_and_sql():
    planted_subject = "ceo-alice@secret-tenant.internal"
    planted_token = "eyJhbGciOiJFUzI1NiJ9.SUPER_SECRET_GRANT_PAYLOAD.SIGNATURE_XYZ"
    planted_table = "confidential_executive_payroll"
    planted_sql = "SELECT salary FROM confidential_executive_payroll WHERE ssn = '999-88-7777'"

    # Even if a caller passes suspicious names into engines/adapters or record calls,
    # none of them may ever appear in Prometheus output.
    registry = MetricsRegistry(
        engines=["duck", "lakebase", planted_subject, planted_sql],
        adapters={
            "https://user:secret@host/db": MagicMock(),
        },
    )

    registry.record_request(
        f"/v1/query/{planted_table}",
        engine=planted_table,
        status=planted_token,
        duration_seconds=0.042,
        error_code=planted_sql,
    )
    registry.inc_request(planted_sql, planted_subject, 200)
    registry.observe_duration(planted_token, planted_table, 0.2)
    registry.record_error("/v1/query", planted_subject, planted_token)
    registry.record_grant_rejected(planted_token)
    registry.record_grant_rejected(planted_subject)

    rendered = registry.render()

    for forbidden in (
        planted_subject,
        planted_token,
        planted_table,
        planted_sql,
        "999-88-7777",
        "SUPER_SECRET_GRANT_PAYLOAD",
        "secret@host",
    ):
        assert forbidden not in rendered

    assert 'route="other",engine="none",status="unknown"' in rendered
    assert 'route="other",engine="none",status="200"' in rendered
    assert 'seeknal_serving_errors_total{route="other",engine="none",code="internal_error"} 1' in rendered
    assert 'seeknal_serving_errors_total{route="/v1/query",engine="none",code="internal_error"} 1' in rendered
    assert 'seeknal_serving_grants_rejected_total{reason="other"} 2' in rendered


def test_metrics_histogram_buckets_cumulative_counts_and_counters():
    registry = MetricsRegistry(engines=["duck"])

    assert registry.buckets == (
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        30.0,
    )
    assert registry.buckets == DEFAULT_DURATION_BUCKETS

    registry.record_request("/v1/query", engine="duck", status=200, duration_seconds=0.003)
    registry.record_request("/v1/query", engine="duck", status=200, duration_seconds=0.040)
    registry.record_request(
        "/v1/query",
        engine="duck",
        status=504,
        duration_seconds=1.500,
        error_code="engine_timeout",
    )

    text = registry.render()

    assert 'seeknal_serving_requests_total{route="/v1/query",engine="duck",status="200"} 2' in text
    assert 'seeknal_serving_requests_total{route="/v1/query",engine="duck",status="504"} 1' in text
    assert 'seeknal_serving_errors_total{route="/v1/query",engine="duck",code="engine_timeout"} 1' in text

    assert 'seeknal_serving_request_duration_seconds_bucket{route="/v1/query",engine="duck",le="0.005"} 1' in text
    assert 'seeknal_serving_request_duration_seconds_bucket{route="/v1/query",engine="duck",le="0.025"} 1' in text
    assert 'seeknal_serving_request_duration_seconds_bucket{route="/v1/query",engine="duck",le="0.05"} 2' in text
    assert 'seeknal_serving_request_duration_seconds_bucket{route="/v1/query",engine="duck",le="1.0"} 2' in text
    assert 'seeknal_serving_request_duration_seconds_bucket{route="/v1/query",engine="duck",le="2.5"} 3' in text
    assert 'seeknal_serving_request_duration_seconds_bucket{route="/v1/query",engine="duck",le="30.0"} 3' in text
    assert 'seeknal_serving_request_duration_seconds_bucket{route="/v1/query",engine="duck",le="+Inf"} 3' in text
    assert 'seeknal_serving_request_duration_seconds_sum{route="/v1/query",engine="duck"} 1.543000' in text
    assert 'seeknal_serving_request_duration_seconds_count{route="/v1/query",engine="duck"} 3' in text


def test_metrics_grants_rejected_and_audit_write_failures():
    registry = MetricsRegistry()

    initial = registry.render()
    assert "seeknal_serving_audit_write_failures_total 0" in initial

    for reason in ALLOWED_GRANT_REJECTION_REASONS:
        registry.record_grant_rejected(reason)

    # Also test exception-based classification
    registry.record_grant_rejected(GrantTokenError("Token has expired"))
    registry.record_grant_rejected(GrantTokenError("Invalid token signature"))
    registry.record_grant_rejected(GrantTokenError("Expected 3 segments in token, got 1"))
    registry.record_grant_rejected(GrantReplayedError("Grant 'j1' has already been used"))
    registry.record_grant_rejected(GrantTokenError("Unknown key ID 'k99'"))
    registry.record_grant_rejected(GrantTokenError("Invalid audience 'wrong', expected 'seeknal-serving'"))
    registry.record_grant_rejected(GrantTokenError("Token lifetime of 900.0s exceeds maximum 300s"))

    assert classify_grant_rejection(RuntimeError("unexpected")) == "other"

    registry.record_audit_write_failure()
    registry.record_audit_write_failure(2)

    rendered = registry.render()
    for reason in (
        "expired",
        "invalid_signature",
        "malformed",
        "replayed",
        "untrusted_kid",
        "invalid_claims",
        "lifetime_exceeded",
    ):
        assert f'seeknal_serving_grants_rejected_total{{reason="{reason}"}} 2' in rendered
    assert 'seeknal_serving_grants_rejected_total{reason="other"} 1' in rendered
    assert "seeknal_serving_audit_write_failures_total 3" in rendered


# ---------------------------------------------------------------------------
# 3. DuckDB & Lakebase read-only stats and dynamic gauges
# ---------------------------------------------------------------------------


def test_duckdb_queue_depth_in_flight_and_generation_gauges(tmp_path):
    db_v1 = tmp_path / "v1.duckdb"
    c1 = duckdb.connect(str(db_v1))
    c1.execute("CREATE TABLE t (x INT)")
    c1.execute("INSERT INTO t VALUES (1)")
    c1.close()

    db_v2 = tmp_path / "v2.duckdb"
    c2 = duckdb.connect(str(db_v2))
    c2.execute("CREATE TABLE t (x INT)")
    c2.execute("INSERT INTO t VALUES (2)")
    c2.close()

    active_link = tmp_path / "current.duckdb"
    os.symlink(db_v1, active_link)

    holder = DuckDBHolder(max_idle_seconds=0)
    try:
        cfg = DuckDBConnectionConfig(
            path=str(active_link),
            allowed_directories=(str(tmp_path) + "/",),
            max_concurrent_cursors=1,
        )
        adapter = DuckDBAdapter(config=cfg, holder=holder, max_concurrent_cursors=1)
        registry = MetricsRegistry(adapters={"duck": adapter})

        # Before any query: stats() must NOT open a connection
        st0 = adapter.stats()
        assert st0 == {
            "queue_depth": 0,
            "in_flight_cursors": 0,
            "generation": 0,
            "max_concurrent_cursors": 1,
        }
        assert len(holder._instances_by_dev_ino) == 0

        # Run first query -> generation becomes 1
        res1 = adapter.execute(_make_prepared_duckdb("SELECT x FROM t", "q-gen1"), timeout_seconds=5.0)
        assert res1.rows == [(1,)]
        assert adapter.stats()["generation"] == 1

        # Atomic symlink swap -> generation becomes 2 after next query
        tmp_link = tmp_path / "current.duckdb.tmp"
        os.symlink(db_v2, tmp_link)
        os.replace(tmp_link, active_link)

        res2 = adapter.execute(_make_prepared_duckdb("SELECT x FROM t", "q-gen2"), timeout_seconds=5.0)
        assert res2.rows == [(2,)]
        assert adapter.stats()["generation"] == 2

        # Now test in_flight_cursors and queue_depth under contention
        in_execute = threading.Event()
        release_execute = threading.Event()

        class BlockingCursor:
            description = [("x", "INTEGER", None, None, None, None, None)]

            def execute(self, sql, params=()):
                in_execute.set()
                release_execute.wait(timeout=5.0)

            def fetchmany(self, size):
                return [(1,)]

            def close(self):
                pass

        blocking_adapter = DuckDBAdapter(
            connect=lambda: BlockingCursor(),
            max_concurrent_cursors=1,
        )
        registry_blocking = MetricsRegistry(adapters={"duck_rt": blocking_adapter})

        t_active = threading.Thread(
            target=lambda: blocking_adapter.execute(
                _make_prepared_duckdb("SELECT 1", "q-active"),
                timeout_seconds=5.0,
            ),
            daemon=True,
        )
        t_active.start()
        assert in_execute.wait(timeout=2.0)

        waiter_errors: list[Exception] = []
        cancel_waiter_fn: list = []

        def run_waiter_1():
            try:
                blocking_adapter.execute(
                    _make_prepared_duckdb("SELECT 1", "q-wait1"),
                    timeout_seconds=5.0,
                )
            except Exception as exc:
                waiter_errors.append(exc)

        def run_waiter_timeout():
            try:
                blocking_adapter.execute(
                    _make_prepared_duckdb("SELECT 1", "q-wait-timeout"),
                    timeout_seconds=0.08,
                )
            except Exception as exc:
                waiter_errors.append(exc)

        def run_waiter_cancel():
            try:
                blocking_adapter.execute(
                    _make_prepared_duckdb("SELECT 1", "q-wait-cancel"),
                    timeout_seconds=5.0,
                    on_cancel_handle=cancel_waiter_fn.append,
                )
            except Exception as exc:
                waiter_errors.append(exc)

        tw1 = threading.Thread(target=run_waiter_1, daemon=True)
        tw1.start()

        # Poll briefly until tw1 enters the queue
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and blocking_adapter.stats()["queue_depth"] < 1:
            time.sleep(0.005)

        snap = blocking_adapter.stats()
        assert snap["in_flight_cursors"] == 1
        assert snap["queue_depth"] == 1

        metrics_mid = registry_blocking.render()
        assert 'seeknal_serving_duckdb_in_flight_cursors{engine="duck_rt"} 1' in metrics_mid
        assert 'seeknal_serving_duckdb_queue_depth{engine="duck_rt"} 1' in metrics_mid

        # Timeout and cancel while queued must cleanly decrement queue_depth
        tw_to = threading.Thread(target=run_waiter_timeout, daemon=True)
        tw_to.start()
        tw_to.join(timeout=2.0)

        tw_can = threading.Thread(target=run_waiter_cancel, daemon=True)
        tw_can.start()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and not cancel_waiter_fn:
            time.sleep(0.005)
        cancel_waiter_fn[0]()
        tw_can.join(timeout=2.0)

        assert any(isinstance(e, EngineTimeout) for e in waiter_errors)
        assert any(isinstance(e, EngineCancelled) for e in waiter_errors)
        assert blocking_adapter.stats()["queue_depth"] == 1

        # Release the active query so tw1 completes
        release_execute.set()
        t_active.join(timeout=2.0)
        tw1.join(timeout=2.0)

        final_stats = blocking_adapter.stats()
        assert final_stats["in_flight_cursors"] == 0
        assert final_stats["queue_depth"] == 0

        # Also check generation gauge rendered on file-backed adapter
        file_metrics = registry.render()
        assert 'seeknal_serving_duckdb_generation{engine="duck"} 2' in file_metrics
    finally:
        holder.close_all()


def test_duckdb_iceberg_direct_generation_gauge(tmp_path):
    ext_dir = tmp_path / "ext"
    ext_dir.mkdir(exist_ok=True)
    temp_dir = tmp_path / "tmp_spill"
    temp_dir.mkdir(exist_ok=True)
    standin_db = tmp_path / "standin.duckdb"
    sc = duckdb.connect(str(standin_db))
    sc.execute("CREATE SCHEMA ns")
    sc.execute("CREATE TABLE ns.orders (id INT)")
    sc.execute("INSERT INTO ns.orders VALUES (42)")
    sc.close()

    now_val = [100.0]
    holder = DuckDBHolder(max_idle_seconds=0, clock=lambda: now_val[0])
    try:
        cat = IcebergCatalogConfig(
            alias="lake",
            endpoint="https://lakekeeper.internal.example/catalog",
            warehouse="main_warehouse",
            token_uri="https://idp.internal.example/oauth2/token",
            client_id_env="LK_CID",
            client_secret_env="LK_CSEC",
            scope="lakekeeper:read",
            access_delegation="vended",
            s3=IcebergS3Config(
                endpoint="s3.internal.example:9000",
                region="ap-southeast-1",
                url_style="path",
                key_id_env="S3_KID",
                secret_env="S3_SEC",
                use_ssl=True,
            ),
        )
        cfg = DuckDBConnectionConfig(
            profile="iceberg_direct",
            path=":memory:",
            extension_directory=str(ext_dir),
            extensions=("json", "parquet"),
            iceberg_catalogs={"lake": cat},
            allowed_s3_prefixes=("s3://warehouse-bucket/analytics/",),
            recycle_seconds=60.0,
            token_lifetime_seconds=120.0,
            external_access_after_attach="off",
            memory_limit="256MB",
            threads=2,
            temp_directory=str(temp_dir),
            max_temp_directory_size="512MB",
            max_concurrent_cursors=8,
            secrets_env={
                "LK_CID": "fake_cid",
                "LK_CSEC": "fake_csec",
                "S3_KID": "fake_kid",
                "S3_SEC": "fake_sec",
            },
        )
        adapter = DuckDBAdapter(
            config=cfg,
            holder=holder,
            standin_catalogs={"lake": str(standin_db)},
        )
        assert adapter.stats()["generation"] == 0

        adapter.execute(_make_prepared_duckdb("SELECT id FROM lake.ns.orders", "q-ib-1"), timeout_seconds=5.0)
        assert adapter.stats()["generation"] == 1

        # Advance past recycle_seconds -> next query creates generation 2
        now_val[0] = 200.0
        adapter.execute(_make_prepared_duckdb("SELECT id FROM lake.ns.orders", "q-ib-2"), timeout_seconds=5.0)
        assert adapter.stats()["generation"] == 2
    finally:
        holder.close_all()


def test_lakebase_pool_stats_and_metrics_gauges():
    class FakeConn:
        closed = 0

        def close(self):
            self.closed = 1

    pool = LakebasePool(lambda: FakeConn(), pool_max=4)
    try:
        adapter = DBAPIAdapter(lambda: FakeConn(), kind="lakebase", pool=pool)
        unpooled = DBAPIAdapter(lambda: FakeConn(), kind="starrocks")
        assert adapter.pool is pool
        assert unpooled.pool is None
        assert unpooled.pool_stats() is None

        c1 = pool.acquire()
        c2 = pool.acquire()
        assert adapter.pool_stats() == {
            "in_use": 2,
            "idle": 0,
            "total": 2,
            "pool_max": 4,
        }

        pool.release(c1, success=True)
        assert adapter.pool_stats() == {
            "in_use": 1,
            "idle": 1,
            "total": 2,
            "pool_max": 4,
        }

        registry = MetricsRegistry(adapters={"lakebase": adapter, "sr": unpooled})
        rendered = registry.render()
        assert 'seeknal_serving_lakebase_pool_in_use{engine="lakebase"} 1' in rendered
        assert 'seeknal_serving_lakebase_pool_idle{engine="lakebase"} 1' in rendered
        assert 'engine="sr"' not in rendered

        pool.release(c2, success=True)
    finally:
        pool.close()


# ---------------------------------------------------------------------------
# 4. ReadinessProbe: 200, 503, no DSN/error leak, caching with fake clock
# ---------------------------------------------------------------------------


def test_readyz_200_across_all_adapter_kinds_and_non_destructive_duckdb():
    duck_conn = duckdb.connect()
    duck_conn.execute("CREATE TABLE items (id INT)")
    duck_conn.execute("INSERT INTO items VALUES (7)")

    duck_adapter = DuckDBAdapter(connect=lambda: duck_conn)

    lb_pool = LakebasePool(lambda: MagicMock(), pool_max=2)
    lb_adapter = DBAPIAdapter(lambda: MagicMock(), kind="lakebase", pool=lb_pool)

    sr_routed = WorkloadRoutedAdapter(
        {
            "interactive": DBAPIAdapter(lambda: MagicMock(), kind="starrocks"),
            "bi": DBAPIAdapter(
                lambda: (_ for _ in ()).throw(RuntimeError("bi unused")),
                kind="starrocks",
            ),
        }
    )

    cube_sec = CubeSecurityConfig(secret=b"cube-secret-bytes")
    cube_adapter = CubeAdapter(lambda **kw: MagicMock(), security=cube_sec)

    try:
        probe = ReadinessProbe(
            {
                "duck": duck_adapter,
                "lakebase": lb_adapter,
                "starrocks": sr_routed,
                "cube": cube_adapter,
            },
            timeout_seconds=1.0,
            cache_seconds=0.0,
        )
        ops_app = create_ops_app(readiness=probe)
        client = TestClient(ops_app)

        resp = client.get("/readyz")
        assert resp.status_code == 200
        assert resp.json() == {
            "ready": True,
            "engines": {
                "duck": "ok",
                "lakebase": "ok",
                "starrocks": "ok",
                "cube": "ok",
            },
        }

        # Confirm check_health did NOT close the shared in-memory DuckDB connection
        q_res = duck_adapter.execute(
            _make_prepared_duckdb("SELECT id FROM items", "q-after-readyz"),
            timeout_seconds=2.0,
        )
        assert q_res.rows == [(7,)]
    finally:
        lb_pool.close()
        duck_conn.close()


def test_readyz_503_unreachable_or_timed_out_engine_never_leaks_dsn_or_error():
    secret_dsn = "postgresql://svc_prod:ULTRA_SECRET_PW_42@pg-primary.internal.corp:5432/analytics"

    def broken_connect():
        raise ConnectionError(f"FATAL: could not connect to {secret_dsn}")

    def hung_connect():
        time.sleep(2.0)
        return MagicMock()

    healthy_duck = DuckDBAdapter(connect=lambda: duckdb.connect())
    broken_lb = DBAPIAdapter(broken_connect, kind="lakebase")
    hung_sr = DBAPIAdapter(hung_connect, kind="starrocks")

    probe = ReadinessProbe(
        {
            "duck": healthy_duck,
            "lakebase": broken_lb,
            "starrocks": hung_sr,
        },
        timeout_seconds=0.25,
        cache_seconds=0.0,
    )
    ops_app = create_ops_app(readiness=probe)
    client = TestClient(ops_app)

    start = time.perf_counter()
    resp = client.get("/readyz")
    elapsed = time.perf_counter() - start

    assert resp.status_code == 503
    assert elapsed < 1.0
    body_text = resp.text
    for leaked in (
        secret_dsn,
        "ULTRA_SECRET_PW_42",
        "pg-primary.internal.corp",
        "svc_prod",
        "FATAL",
        "ConnectionError",
    ):
        assert leaked not in body_text

    assert resp.json() == {
        "ready": False,
        "engines": {
            "duck": "ok",
            "lakebase": "unavailable",
            "starrocks": "unavailable",
        },
    }


def test_readyz_discovery_catalog_not_loaded():
    @dataclass
    class FakeRefresher:
        last_success_at: float | None = None

    refresher = FakeRefresher(last_success_at=None)
    duck_adapter = DuckDBAdapter(connect=lambda: duckdb.connect())

    probe = ReadinessProbe(
        {"duck": duck_adapter},
        is_static=False,
        refresher=refresher,
        cache_seconds=0.0,
    )
    ops_app = create_ops_app(readiness=probe)
    client = TestClient(ops_app)

    resp_unloaded = client.get("/readyz")
    assert resp_unloaded.status_code == 503
    assert resp_unloaded.json() == {
        "ready": False,
        "reason": "catalog_not_loaded",
        "engines": {"duck": "ok"},
    }

    refresher.last_success_at = 123.45
    resp_loaded = client.get("/readyz")
    assert resp_loaded.status_code == 200
    assert resp_loaded.json() == {
        "ready": True,
        "engines": {"duck": "ok"},
    }


def test_readyz_caching_with_fake_clock():
    fake_now = [1000.0]
    probe_calls = [0]
    engine_healthy = [True]

    def counting_connect():
        probe_calls[0] += 1
        if not engine_healthy[0]:
            raise RuntimeError("engine went down")
        return MagicMock()

    adapter = DBAPIAdapter(counting_connect, kind="lakebase")
    probe = ReadinessProbe(
        {"lakebase": adapter},
        timeout_seconds=1.0,
        cache_seconds=5.0,
        clock=lambda: fake_now[0],
    )
    ops_app = create_ops_app(readiness=probe)
    client = TestClient(ops_app)

    # t = 1000.0 -> cold cache -> probes once
    r1 = client.get("/readyz")
    assert r1.status_code == 200
    assert probe_calls[0] == 1

    # Engine breaks at t = 1002.0, but cache TTL (5.0s) has not expired
    engine_healthy[0] = False
    fake_now[0] = 1002.0
    r2 = client.get("/readyz")
    assert r2.status_code == 200
    assert probe_calls[0] == 1

    # Advance to t = 1004.99 -> still cached
    fake_now[0] = 1004.99
    r3 = client.get("/readyz")
    assert r3.status_code == 200
    assert probe_calls[0] == 1

    # Advance to t = 1005.0 -> cache expired -> re-probes and sees failure
    fake_now[0] = 1005.0
    r4 = client.get("/readyz")
    assert r4.status_code == 503
    assert r4.json() == {"ready": False, "engines": {"lakebase": "unavailable"}}
    assert probe_calls[0] == 2

    # Failure result is also cached so probes cannot hammer a failing engine
    fake_now[0] = 1007.0
    r5 = client.get("/readyz")
    assert r5.status_code == 503
    assert probe_calls[0] == 2

    # Engine recovers after TTL expires at t = 1010.0
    engine_healthy[0] = True
    fake_now[0] = 1010.0
    r6 = client.get("/readyz")
    assert r6.status_code == 200
    assert probe_calls[0] == 3


# ---------------------------------------------------------------------------
# 5. Ops app bearer auth, /buildinfo, and public port isolation
# ---------------------------------------------------------------------------


def test_ops_app_endpoints_without_auth_and_buildinfo():
    duck_adapter = DuckDBAdapter(connect=lambda: duckdb.connect())
    cfg = OpsConfig.from_dict({})
    assert cfg.host == "127.0.0.1"
    assert cfg.auth_token_env is None

    registry = MetricsRegistry(adapters={"duck": duck_adapter})
    ops_app = create_ops_app(
        ops_config=cfg,
        adapters={"duck": duck_adapter},
        metrics=registry,
    )
    client = TestClient(ops_app)

    rh = client.get("/healthz")
    assert rh.status_code == 200
    assert rh.json() == {"status": "ok"}

    rr = client.get("/readyz")
    assert rr.status_code == 200
    assert rr.json() == {"ready": True, "engines": {"duck": "ok"}}

    rb = client.get("/buildinfo")
    assert rb.status_code == 200
    info = rb.json()
    for required_key in ("version", "git_sha", "build_time", "duckdb_version", "lock_sha256"):
        assert required_key in info
        assert isinstance(info[required_key], str)

    rm = client.get("/metrics")
    assert rm.status_code == 200
    assert rm.headers["content-type"] == PROMETHEUS_CONTENT_TYPE
    assert 'seeknal_serving_requests_total{route="/healthz",engine="none",status="200"} 1' in rm.text
    assert 'seeknal_serving_requests_total{route="/readyz",engine="none",status="200"} 1' in rm.text
    assert 'seeknal_serving_requests_total{route="/buildinfo",engine="none",status="200"} 1' in rm.text


def test_ops_app_bearer_auth_enforced_and_constant_time():
    secret_token = "ops-bearer-secret-9f8e7d6c5b4a"
    cfg = OpsConfig.from_dict({"auth_token_env": "OPS_AUTH_TOKEN"})
    registry = MetricsRegistry()
    ops_app = create_ops_app(
        ops_config=cfg,
        metrics=registry,
        env={"OPS_AUTH_TOKEN": secret_token},
    )
    client = TestClient(ops_app)

    for path in ("/healthz", "/readyz", "/metrics", "/buildinfo"):
        # Missing header -> 401
        r_missing = client.get(path)
        assert r_missing.status_code == 401
        assert r_missing.json()["error"] == "unauthorized"

        # Wrong scheme -> 401
        r_scheme = client.get(path, headers={"Authorization": f"Basic {secret_token}"})
        assert r_scheme.status_code == 401

        # Wrong token -> 401 and never echoes secret or wrong token
        wrong_token = "wrong-token-canary-777"
        r_wrong = client.get(path, headers={"Authorization": f"Bearer {wrong_token}"})
        assert r_wrong.status_code == 401
        assert secret_token not in r_wrong.text
        assert wrong_token not in r_wrong.text

        # Valid token -> 200
        r_ok = client.get(path, headers={"Authorization": f"Bearer {secret_token}"})
        assert r_ok.status_code == 200

    # Unset / empty env var fails closed with 401 even for empty Bearer header
    ops_app_unset = create_ops_app(ops_config=cfg, env={})
    client_unset = TestClient(ops_app_unset)
    assert client_unset.get("/metrics").status_code == 401
    assert client_unset.get("/metrics", headers={"Authorization": "Bearer "}).status_code == 401

    # Metrics must never contain the ops bearer token
    scraped = client.get(
        "/metrics", headers={"Authorization": f"Bearer {secret_token}"}
    ).text
    assert secret_token not in scraped
    assert 'seeknal_serving_errors_total{route="/metrics",engine="none",code="unauthorized"}' in scraped


def test_public_port_has_no_metrics_or_buildinfo_by_default(tmp_path):
    # 1. Verify via create_app + build_public_extra_routes with default OpsConfig()
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pn = pub.public_numbers()
    jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii"),
        "y": base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii"),
        "kid": "k1",
    }
    jwks_file = tmp_path / "keys.jwks.json"
    jwks_file.write_text(json.dumps({"keys": [jwk]}))

    pf = tmp_path / "placement.yaml"
    pf.write_text(
        "engines:\n"
        "  duck: {kind: duckdb, dialect: duckdb}\n"
        "tables:\n"
        "  t:\n"
        "    keys: [id]\n"
        "    row_count: 1\n"
        "    serve:\n"
        "      duck: t\n"
        "cubes: {}\n"
    )
    sf = tmp_path / "schemas.json"
    sf.write_text(json.dumps({"t": ["id"]}))

    duck_adapter = DuckDBAdapter(connect=lambda: duckdb.connect())
    default_ops = OpsConfig()
    assert default_ops.host == "127.0.0.1"
    assert default_ops.public_metrics is False
    assert default_ops.public_buildinfo is False
    assert default_ops.public_readyz is True

    readiness = ReadinessProbe({"duck": duck_adapter})
    metrics = MetricsRegistry(adapters={"duck": duck_adapter})
    pipeline = ServingPipeline(
        placement=Placement(engines={}, tables={}, cubes={}),
        schemas={},
        public_keys={},
    )
    public_app = create_app(
        pipeline=pipeline,
        adapters={"duck": duck_adapter},
        extra_routes=build_public_extra_routes(
            default_ops, readiness=readiness, metrics=metrics
        ),
    )
    pub_client = TestClient(public_app)
    assert pub_client.get("/healthz").status_code == 200
    assert pub_client.get("/readyz").status_code == 200
    assert pub_client.get("/metrics").status_code == 404
    assert pub_client.get("/buildinfo").status_code == 404

    # 2. Also verify build_server public app has no /metrics or /buildinfo by default
    srv_cfg = ServerConfig.from_dict(
        {
            "listen": {"host": "127.0.0.1", "port": 8765},
            "grant_keys": {"jwks_file": str(jwks_file)},
            "placement": {
                "static": {
                    "placement_file": str(pf),
                    "schemas_file": str(sf),
                }
            },
            "connections": {
                "duck": {"kind": "duckdb", "path": ":memory:"},
            },
        }
    )
    handle = build_server(srv_cfg, connect_factories={"duck": lambda: duckdb.connect()})
    assert handle.ops_app is None
    assert handle.metrics is not None
    # Unpacking ServerHandle continues to yield (app, pipeline, refresher)
    unpacked_app, unpacked_pipeline, unpacked_refresher = handle
    assert unpacked_app is handle.app
    assert unpacked_pipeline is handle.pipeline
    assert unpacked_refresher is None

    with TestClient(handle.app) as srv_client:
        assert srv_client.get("/healthz").status_code == 200
        ready_resp = srv_client.get("/readyz")
        assert ready_resp.status_code == 200
        assert ready_resp.json() == {"ready": True}
        assert srv_client.get("/metrics").status_code == 404
        assert srv_client.get("/buildinfo").status_code == 404


# ---------------------------------------------------------------------------
# 6. Full integration: ServerConfig, build_server, HTTP metrics, audit, CLI
# ---------------------------------------------------------------------------


def _setup_test_keys_and_catalog(tmp_path):
    priv = ec.generate_private_key(ec.SECP256R1())
    pub = priv.public_key()
    pn = pub.public_numbers()
    jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": base64.urlsafe_b64encode(pn.x.to_bytes(32, "big")).rstrip(b"=").decode("ascii"),
        "y": base64.urlsafe_b64encode(pn.y.to_bytes(32, "big")).rstrip(b"=").decode("ascii"),
        "kid": "int-key-1",
    }
    jwks_file = tmp_path / "keys.jwks.json"
    jwks_file.write_text(json.dumps({"keys": [jwk]}))

    db_file = tmp_path / "analytics.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE orders (id INT, customer VARCHAR)")
    con.execute("INSERT INTO orders VALUES (1, 'Acme'), (2, 'Globex')")
    con.close()

    pf = tmp_path / "placement.yaml"
    pf.write_text(
        "engines:\n"
        "  duck: {kind: duckdb, dialect: duckdb}\n"
        "  lakebase: {kind: lakebase, dialect: postgres}\n"
        "tables:\n"
        "  orders:\n"
        "    keys: [id]\n"
        "    row_count: 2\n"
        "    serve:\n"
        "      duck: orders\n"
        "cubes: {}\n"
    )
    sf = tmp_path / "schemas.json"
    sf.write_text(json.dumps({"orders": ["id", "customer"]}))
    return priv, str(jwks_file), str(db_file), str(pf), str(sf)


def test_server_config_ops_parsing_and_env_preflight(tmp_path, monkeypatch, capsys):
    _, jwks_file, db_file, pf, sf = _setup_test_keys_and_catalog(tmp_path)

    base = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
        "connections": {
            "duck": {
                "kind": "duckdb",
                "path": db_file,
                "allowed_directories": [str(tmp_path) + "/"],
            },
            "lakebase": {"kind": "lakebase", "dsn_env": "LB_DSN_TEST"},
        },
    }

    # Absent or None -> cfg.ops is None
    cfg_none = ServerConfig.from_dict(base)
    assert cfg_none.ops is None
    cfg_explicit_none = ServerConfig.from_dict({**base, "ops": None})
    assert cfg_explicit_none.ops is None

    # Unknown key in ops -> ServerConfigError
    with pytest.raises(ServerConfigError, match="Unknown key"):
        ServerConfig.from_dict({**base, "ops": {"bad_unknown_key": True}})

    # Literal token key in ops -> ServerConfigError without leaking secret
    with pytest.raises(ServerConfigError) as exc_info:
        ServerConfig.from_dict({**base, "ops": {"auth_token": "SECRET_LITERAL_444"}})
    assert "SECRET_LITERAL_444" not in str(exc_info.value)

    # auth_token_env included in get_referenced_env_vars() and checked by --check
    cfg_with_ops = ServerConfig.from_dict(
        {
            **base,
            "ops": {
                "host": "127.0.0.1",
                "port": 9095,
                "auth_token_env": "OPS_BEARER_ENV_TEST",
            },
        }
    )
    assert cfg_with_ops.ops is not None
    assert cfg_with_ops.ops.enabled is True
    assert "OPS_BEARER_ENV_TEST" in cfg_with_ops.get_referenced_env_vars()

    cfg_yaml = tmp_path / "serving_ops.yaml"
    import yaml

    cfg_yaml.write_text(
        yaml.safe_dump(
            {
                **base,
                "ops": {
                    "host": "127.0.0.1",
                    "port": 9095,
                    "auth_token_env": "OPS_BEARER_ENV_TEST",
                },
            }
        )
    )
    monkeypatch.setenv("LB_DSN_TEST", "postgresql://u:p@localhost:5432/db")
    monkeypatch.delenv("OPS_BEARER_ENV_TEST", raising=False)

    # Missing OPS_BEARER_ENV_TEST fails --check
    assert serving_main(["--config", str(cfg_yaml), "--check"]) == 2
    err_out = capsys.readouterr().err
    assert "OPS_BEARER_ENV_TEST" in err_out

    # Present OPS_BEARER_ENV_TEST passes --check
    monkeypatch.setenv("OPS_BEARER_ENV_TEST", "ops-tok-ok")
    assert serving_main(["--config", str(cfg_yaml), "--check"]) == 0
    assert "config ok" in capsys.readouterr().out


def test_build_server_end_to_end_ops_app_public_flags_caching_and_metrics(tmp_path):
    priv, jwks_file, db_file, pf, sf = _setup_test_keys_and_catalog(tmp_path)

    fake_clock = [500.0]
    lb_healthy = [True]
    lb_probe_count = [0]
    secret_dsn = "postgresql://admin:CANARY_PG_PW_888@internal-lb.corp.local:5432/prod"

    class FakeLBConn:
        closed = 0

        def cursor(self):
            cur = MagicMock()
            return cur

        def rollback(self):
            pass

        def close(self):
            self.closed = 1

    def lb_connect(**kwargs):
        lb_probe_count[0] += 1
        if not lb_healthy[0]:
            raise RuntimeError(f"connection failed for {secret_dsn}")
        return FakeLBConn()

    ops_secret = "ops-token-super-secret-xyz"
    env = {
        "LB_DSN_TEST": secret_dsn,
        "OPS_SECRET_ENV": ops_secret,
    }

    cfg_dict = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
        "connections": {
            "duck": {
                "kind": "duckdb",
                "path": db_file,
                "allowed_directories": [str(tmp_path) + "/"],
            },
            "lakebase": {
                "kind": "lakebase",
                "dsn_env": "LB_DSN_TEST",
                "pool_max": 4,
            },
        },
        "ops": {
            "enabled": True,
            "host": "127.0.0.1",
            "port": 9091,
            "auth_token_env": "OPS_SECRET_ENV",
            "readyz_timeout_seconds": 1.0,
            "readyz_cache_seconds": 10.0,
            "public_metrics": False,
            "public_buildinfo": False,
            "public_readyz": True,
        },
    }

    handle = build_server(
        cfg_dict,
        env=env,
        connect_factories={"lakebase": lb_connect},
        readiness_clock=lambda: fake_clock[0],
    )
    assert handle.ops_app is not None
    assert handle.metrics is not None

    ops_headers = {"Authorization": f"Bearer {ops_secret}"}

    with TestClient(handle.app) as pub_client, TestClient(handle.ops_app) as ops_client:
        # Public port: /healthz and /readyz enabled; /metrics and /buildinfo 404
        assert pub_client.get("/healthz").status_code == 200
        pub_ready = pub_client.get("/readyz")
        assert pub_ready.status_code == 200
        assert pub_ready.json() == {"ready": True}
        assert pub_client.get("/metrics").status_code == 404
        assert pub_client.get("/buildinfo").status_code == 404

        # Ops port requires bearer token
        assert ops_client.get("/metrics").status_code == 401
        assert ops_client.get("/buildinfo").status_code == 401
        assert ops_client.get("/readyz").status_code == 401
        assert ops_client.get("/healthz").status_code == 401

        # Ops /buildinfo returns exact 5 keys from buildinfo.py
        bi_resp = ops_client.get("/buildinfo", headers=ops_headers)
        assert bi_resp.status_code == 200
        assert set(bi_resp.json().keys()) == set(BUILD_INFO_KEYS)

        # Shared readiness cache: ops /readyz within 10s reuses cached probe
        assert lb_probe_count[0] == 1
        fake_clock[0] = 505.0
        ops_ready = ops_client.get("/readyz", headers=ops_headers)
        assert ops_ready.status_code == 200
        assert ops_ready.json() == {
            "ready": True,
            "engines": {"duck": "ok", "lakebase": "ok"},
        }
        assert lb_probe_count[0] == 1

        # Break Lakebase and advance past cache TTL; also evict pooled idle conn so pool connects fresh
        handle.adapters["lakebase"].pool.close()
        lb_healthy[0] = False
        fake_clock[0] = 515.0
        pub_ready_503 = pub_client.get("/readyz")
        assert pub_ready_503.status_code == 503
        assert pub_ready_503.json() == {"ready": False}
        assert "CANARY_PG_PW_888" not in pub_ready_503.text
        assert "internal-lb.corp.local" not in pub_ready_503.text

        ops_ready_503 = ops_client.get("/readyz", headers=ops_headers)
        assert ops_ready_503.status_code == 503
        assert ops_ready_503.json() == {
            "ready": False,
            "engines": {"duck": "ok", "lakebase": "unavailable"},
        }

        # Execute queries, explain, cancel, and error/grant-rejection paths on public port
        planted_subject = "vip-analyst@tenant-secret.org"
        planted_lit = "TOP_SECRET_CUSTOMER_NAME"
        sql = f"SELECT id, customer FROM orders WHERE customer != '{planted_lit}' ORDER BY id"
        now = time.time()
        valid_claims = {
            "sub": planted_subject,
            "ws": "ws-prod",
            "jti": "jti-ops-e2e-1",
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": now,
            "exp": now + 30,
            "typ": "grant",
            "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
            "max_rows": 25,
            "tables": {
                "orders": {
                    "columns": ["id", "customer"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        }
        tok_ok = sign_grant_token(valid_claims, priv, kid="int-key-1")

        # 1) /v1/explain 200
        r_exp = pub_client.post(
            "/v1/explain",
            headers={"Authorization": f"Bearer {tok_ok}"},
            json={"sql": sql},
        )
        assert r_exp.status_code == 200

        # 2) /v1/query 200
        r_q = pub_client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_ok}"},
            json={"sql": sql},
        )
        assert r_q.status_code == 200
        assert len(r_q.json()["rows"]) == 2

        # 3) Replay same grant -> 401 grant_replayed
        r_rep = pub_client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_ok}"},
            json={"sql": sql},
        )
        assert r_rep.status_code == 401
        assert r_rep.json()["code"] == "grant_replayed"

        # 4) Expired grant -> 401 grant_expired
        exp_claims = {
            **valid_claims,
            "jti": "jti-ops-expired",
            "iat": now - 120,
            "exp": now - 60,
        }
        tok_expired = sign_grant_token(exp_claims, priv, kid="int-key-1")
        r_expired = pub_client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_expired}"},
            json={"sql": sql},
        )
        assert r_expired.status_code == 401
        assert r_expired.json()["code"] == "grant_expired"

        # 5) Malformed header -> 401 grant_invalid
        r_mal = pub_client.post("/v1/query", json={"sql": sql})
        assert r_mal.status_code == 401

        # 6) Access denied (ungranted column on placed table) -> 403 access_denied
        bad_col_sql = "SELECT id, customer FROM orders"
        denied_claims = {
            **valid_claims,
            "jti": "jti-ops-denied",
            "sql_sha256": hashlib.sha256(bad_col_sql.encode("utf-8")).hexdigest(),
            "tables": {
                "orders": {
                    "columns": ["id"],
                    "masks": {},
                    "row_filters": [],
                    "actions": ["select"],
                }
            },
        }
        tok_denied = sign_grant_token(denied_claims, priv, kid="int-key-1")
        r_den = pub_client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_denied}"},
            json={"sql": bad_col_sql},
        )
        assert r_den.status_code == 403
        assert r_den.json()["code"] == "access_denied"

        # 6b) Unplaced table -> 422 routing error (table name still never enters /metrics)
        unplaced_sql = "SELECT id FROM secret_ungranted_table"
        unplaced_claims = {
            **valid_claims,
            "jti": "jti-ops-unplaced",
            "sql_sha256": hashlib.sha256(unplaced_sql.encode("utf-8")).hexdigest(),
        }
        tok_unplaced = sign_grant_token(unplaced_claims, priv, kid="int-key-1")
        r_unplaced = pub_client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok_unplaced}"},
            json={"sql": unplaced_sql},
        )
        assert r_unplaced.status_code == 422

        # 7) Cancel non-existent query -> 404 not_found
        cancel_tok = sign_cancel_token(
            {
                "sub": planted_subject,
                "jti": "jti-missing-q",
                "iss": "atlas-catalog",
                "aud": "seeknal-serving",
                "iat": now,
                "exp": now + 30,
            },
            priv,
            kid="int-key-1",
        )
        r_can = pub_client.post(
            "/v1/cancel",
            headers={"Authorization": f"Bearer {cancel_tok}"},
        )
        assert r_can.status_code == 404

        # Scrape /metrics on ops listener and verify series + strict isolation
        m_resp = ops_client.get("/metrics", headers=ops_headers)
        assert m_resp.status_code == 200
        metrics_text = m_resp.text

        for secret_str in (
            planted_subject,
            planted_lit,
            tok_ok,
            tok_expired,
            ops_secret,
            "orders",
            "secret_ungranted_table",
            "CANARY_PG_PW_888",
        ):
            assert secret_str not in metrics_text

        assert 'seeknal_serving_requests_total{route="/v1/query",engine="duck",status="200"} 1' in metrics_text
        assert 'seeknal_serving_requests_total{route="/v1/explain",engine="duck",status="200"} 1' in metrics_text
        assert 'seeknal_serving_requests_total{route="/v1/cancel",engine="none",status="404"} 1' in metrics_text
        assert 'seeknal_serving_requests_total{route="/healthz",engine="none",status="200"} 1' in metrics_text

        assert 'seeknal_serving_errors_total{route="/v1/query",engine="duck",code="grant_replayed"} 1' in metrics_text
        assert 'seeknal_serving_errors_total{route="/v1/query",engine="none",code="grant_expired"} 1' in metrics_text
        assert 'seeknal_serving_errors_total{route="/v1/query",engine="none",code="access_denied"} 1' in metrics_text
        assert 'seeknal_serving_errors_total{route="/v1/cancel",engine="none",code="not_found"} 1' in metrics_text

        assert 'seeknal_serving_grants_rejected_total{reason="replayed"} 1' in metrics_text
        assert 'seeknal_serving_grants_rejected_total{reason="expired"} 1' in metrics_text
        assert 'seeknal_serving_grants_rejected_total{reason="malformed"} 1' in metrics_text

        assert 'seeknal_serving_duckdb_generation{engine="duck"} 1' in metrics_text
        assert 'seeknal_serving_duckdb_in_flight_cursors{engine="duck"} 0' in metrics_text
        assert 'seeknal_serving_duckdb_queue_depth{engine="duck"} 0' in metrics_text
        assert 'seeknal_serving_lakebase_pool_in_use{engine="lakebase"} 0' in metrics_text
        assert 'seeknal_serving_lakebase_pool_idle{engine="lakebase"} 0' in metrics_text


def test_public_opt_in_flags_and_audit_failure_metrics(tmp_path):
    priv, jwks_file, db_file, pf, sf = _setup_test_keys_and_catalog(tmp_path)

    # 1. Verify public_metrics=True, public_buildinfo=True, public_readyz=False on build_server
    cfg_public_opt_in = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
        "connections": {
            "duck": {
                "kind": "duckdb",
                "path": db_file,
                "allowed_directories": [str(tmp_path) + "/"],
            },
            "lakebase": {"kind": "lakebase", "dsn_env": "LB_DSN"},
        },
        "ops": {
            "enabled": False,
            "public_metrics": True,
            "public_buildinfo": True,
            "public_readyz": False,
        },
    }
    handle_opt = build_server(
        cfg_public_opt_in,
        env={"LB_DSN": "postgresql://u:p@localhost/db"},
        connect_factories={"lakebase": lambda: MagicMock()},
    )
    assert handle_opt.ops_app is None
    with TestClient(handle_opt.app) as pub_c:
        assert pub_c.get("/readyz").status_code == 404
        assert pub_c.get("/metrics").status_code == 200
        assert pub_c.get("/buildinfo").status_code == 200
        assert set(pub_c.get("/buildinfo").json().keys()) == set(BUILD_INFO_KEYS)

    # 2. Verify audit_write_failures_total increments on both on_failure="log" and "refuse"
    class FailingAuditSink(AuditSink):
        def __init__(self, on_failure: str) -> None:
            self.on_failure = on_failure

        def verify_writable(self) -> None:
            pass

        def write_record(self, record: AuditRecord) -> None:
            raise AuditWriteError("disk full")

        def close(self) -> None:
            pass

    metrics_reg = MetricsRegistry(allowed_engines=("duck",))
    pipeline = ServingPipeline(
        placement=Placement(engines={}, tables={}, cubes={}),
        schemas={},
        public_keys={},
    )

    app_log = create_app(
        pipeline=pipeline,
        adapters={},
        audit_sink=FailingAuditSink("log"),
        audit_on_failure="log",
        metrics=metrics_reg,
    )
    app_refuse = create_app(
        pipeline=pipeline,
        adapters={},
        audit_sink=FailingAuditSink("refuse"),
        audit_on_failure="refuse",
        metrics=metrics_reg,
    )

    r_log = TestClient(app_log).post("/v1/query", json={"sql": "SELECT 1"})
    assert r_log.status_code == 401  # proceeds despite audit failure in "log" mode

    r_refuse = TestClient(app_refuse).post("/v1/query", json={"sql": "SELECT 1"})
    assert r_refuse.status_code == 503
    assert r_refuse.json()["code"] == "audit_unavailable"

    rendered = metrics_reg.render()
    assert "seeknal_serving_audit_write_failures_total 2" in rendered
    assert 'seeknal_serving_errors_total{route="/v1/query",engine="none",code="audit_unavailable"} 1' in rendered


def test_cli_main_starts_and_stops_ops_listener_and_keeps_version_and_validate(
    tmp_path, monkeypatch, capsys
):
    # 1. --version and --version --json work
    assert serving_main(["--version"]) == 0
    assert "seeknal-serving" in capsys.readouterr().out

    assert serving_main(["--version", "--json"]) == 0
    ver_json = json.loads(capsys.readouterr().out)
    assert set(ver_json.keys()) == set(BUILD_INFO_KEYS)

    # 2. validate subcommand works with ops section present
    _, jwks_file, db_file, pf, sf = _setup_test_keys_and_catalog(tmp_path)
    monkeypatch.setenv("LB_DSN_CLI", "postgresql://u:p@localhost/db")
    monkeypatch.setenv("OPS_TOK_CLI", "cli-ops-secret")

    import yaml

    cfg_path = tmp_path / "serving_cli.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "listen": {"host": "127.0.0.1", "port": 8765},
                "grant_keys": {"jwks_file": jwks_file},
                "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
                "connections": {
                    "duck": {
                        "kind": "duckdb",
                        "path": db_file,
                        "allowed_directories": [str(tmp_path) + "/"],
                    },
                    "lakebase": {"kind": "lakebase", "dsn_env": "LB_DSN_CLI"},
                },
                "ops": {
                    "enabled": True,
                    "host": "127.0.0.1",
                    "port": 9199,
                    "auth_token_env": "OPS_TOK_CLI",
                },
            }
        )
    )

    assert serving_main(["validate", "--config", str(cfg_path)]) == 0
    capsys.readouterr()

    # 3. main() invokes ops_run on (127.0.0.1, 9199) alongside main run
    ops_calls: list[tuple[Any, dict[str, Any]]] = []
    ops_started = threading.Event()

    def fake_ops_run(app, **kw):
        ops_calls.append((app, kw))
        ops_started.set()

    def fake_main_run(app, **kw):
        assert ops_started.wait(timeout=2.0)

    rc = serving_main(
        ["--config", str(cfg_path)],
        run=fake_main_run,
        ops_run=fake_ops_run,
    )
    assert rc == 0
    assert len(ops_calls) == 1
    assert ops_calls[0][1] == {"host": "127.0.0.1", "port": 9199}


# ---------------------------------------------------------------------------
# 7. Sub-task D review regression tests (D-M1..M3, D-L1..L2)
# ---------------------------------------------------------------------------


def test_d_m1_readiness_probe_bypasses_duckdb_slot_and_lakebase_pool():
    duck_query_started = threading.Event()
    release_duck_query = threading.Event()

    class SlotHoldingCursor:
        description = [("x", "INTEGER", None, None, None, None, None)]

        def execute(self, sql, params=()):
            if sql != "SELECT 1":
                duck_query_started.set()
                release_duck_query.wait(timeout=5.0)

        def fetchone(self):
            return (1,)

        def fetchmany(self, size):
            return [(1,)]

        def close(self):
            pass

    duck_adapter = DuckDBAdapter(
        connect=lambda: SlotHoldingCursor(),
        max_concurrent_cursors=1,
    )

    class ProbeAwareLBConn:
        def __init__(self) -> None:
            self.closed = 0
            self.executed: list[str] = []

        def cursor(self):
            conn = self

            class _Cur:
                def execute(self, sql, params=()):
                    conn.executed.append(sql)

                def close(self):
                    pass

            return _Cur()

        def close(self):
            self.closed = 1

    created_lb_conns: list[ProbeAwareLBConn] = []

    def lb_connect(**kwargs):
        c = ProbeAwareLBConn()
        created_lb_conns.append(c)
        return c

    lb_pool = LakebasePool(
        lb_connect,
        pool_max=1,
        acquire_timeout_seconds=5.0,
    )
    lb_adapter = DBAPIAdapter(lb_connect, kind="lakebase", pool=lb_pool)

    # 1) Hold the ONLY DuckDB cursor slot in an active query thread
    duck_thread = threading.Thread(
        target=lambda: duck_adapter.execute(
            _make_prepared_duckdb("SELECT 42", "q-hold-slot"),
            timeout_seconds=5.0,
        ),
        daemon=True,
    )
    duck_thread.start()
    assert duck_query_started.wait(timeout=2.0)
    assert duck_adapter.stats()["in_flight_cursors"] == 1

    # 2) Acquire and hold the ONLY pooled Lakebase connection
    held_lb_conn = lb_pool.acquire()
    assert lb_pool.stats()["in_use"] == 1
    assert lb_pool.stats()["pool_max"] == 1

    try:
        probe = ReadinessProbe(
            {"duck": duck_adapter, "lakebase": lb_adapter},
            timeout_seconds=2.0,
            cache_seconds=0.0,
        )
        ops_app = create_ops_app(readiness=probe)
        client = TestClient(ops_app)

        t0 = time.perf_counter()
        resp = client.get("/readyz")
        elapsed = time.perf_counter() - t0

        assert resp.status_code == 200
        assert elapsed < 0.5
        assert resp.json() == {
            "ready": True,
            "engines": {"duck": "ok", "lakebase": "ok"},
        }

        # Verify neither DuckDB slot gauges nor Lakebase pool counts were disturbed
        assert duck_adapter.stats()["in_flight_cursors"] == 1
        assert duck_adapter.stats()["queue_depth"] == 0
        assert lb_pool.stats()["in_use"] == 1
        assert held_lb_conn.closed == 0
        # The separate direct probe connection was opened and closed cleanly
        assert len(created_lb_conns) == 2
        assert created_lb_conns[1].closed == 1
    finally:
        release_duck_query.set()
        duck_thread.join(timeout=2.0)
        lb_pool.release(held_lb_conn, success=True)
        lb_pool.close()


def test_d_m2_single_in_flight_probe_per_engine_prevents_thread_pileup():
    call_count = 0
    count_lock = threading.Lock()
    first_entered = threading.Event()
    unblock_probe = threading.Event()

    def blocking_connect(**kwargs):
        nonlocal call_count
        with count_lock:
            call_count += 1
        first_entered.set()
        unblock_probe.wait(timeout=5.0)
        return MagicMock()

    adapter = DBAPIAdapter(blocking_connect, kind="lakebase")
    probe = ReadinessProbe(
        {"lakebase": adapter},
        timeout_seconds=0.05,
        cache_seconds=0.0,
    )
    ops_app = create_ops_app(readiness=probe)
    client = TestClient(ops_app)

    try:
        t0 = time.perf_counter()
        responses = [client.get("/readyz") for _ in range(5)]
        total_elapsed = time.perf_counter() - t0

        assert first_entered.is_set()
        assert all(r.status_code == 503 for r in responses)
        assert total_elapsed < 1.0
        with count_lock:
            assert call_count == 1
    finally:
        unblock_probe.set()

    # Wait briefly for the unblocked worker thread's finally block to clear _in_flight
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        with probe._lock:
            if "lakebase" not in probe._in_flight:
                break
        time.sleep(0.005)

    next_resp = client.get("/readyz")
    assert next_resp.status_code == 200
    assert next_resp.json() == {"ready": True, "engines": {"lakebase": "ok"}}
    with count_lock:
        assert call_count == 2


def test_d_m3_ops_listener_bind_failure_raises_before_serving(
    tmp_path, monkeypatch, capsys
):
    import httpx
    import socket
    import yaml

    ops_app = create_ops_app(ops_config=OpsConfig(enabled=True))

    conflict_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conflict_sock.bind(("127.0.0.1", 0))
    conflict_sock.listen(1)
    occupied_port = conflict_sock.getsockname()[1]

    try:
        # 1) Default uvicorn path fails fast before serving when port is occupied
        with pytest.raises(RuntimeError, match="Ops listener failed to bind"):
            start_ops_server(ops_app, host="127.0.0.1", port=occupied_port)

        # 2) Custom ops_run path also fails pre-bind check when port is occupied
        ops_run_called = False

        def never_called_ops_run(app, **kw):
            nonlocal ops_run_called
            ops_run_called = True

        with pytest.raises(RuntimeError, match="Ops listener failed to bind"):
            start_ops_server(
                ops_app,
                host="127.0.0.1",
                port=occupied_port,
                ops_run=never_called_ops_run,
            )
        assert ops_run_called is False

        # 3) CLI main() exits non-zero (2) within < 2.0s and never calls public run()
        _, jwks_file, db_file, pf, sf = _setup_test_keys_and_catalog(tmp_path)
        monkeypatch.setenv("LB_DSN_DM3", "postgresql://u:p@localhost/db")
        monkeypatch.setenv("OPS_TOK_DM3", "ops-tok-dm3")

        cfg_path = tmp_path / "serving_dm3.yaml"
        cfg_path.write_text(
            yaml.safe_dump(
                {
                    "listen": {"host": "127.0.0.1", "port": 8765},
                    "grant_keys": {"jwks_file": jwks_file},
                    "placement": {
                        "static": {"placement_file": pf, "schemas_file": sf}
                    },
                    "connections": {
                        "duck": {
                            "kind": "duckdb",
                            "path": db_file,
                            "allowed_directories": [str(tmp_path) + "/"],
                        },
                        "lakebase": {"kind": "lakebase", "dsn_env": "LB_DSN_DM3"},
                    },
                    "ops": {
                        "enabled": True,
                        "host": "127.0.0.1",
                        "port": occupied_port,
                        "auth_token_env": "OPS_TOK_DM3",
                    },
                }
            )
        )

        public_run_called = False

        def fake_public_run(app, **kw):
            nonlocal public_run_called
            public_run_called = True

        t0 = time.perf_counter()
        rc = serving_main(["--config", str(cfg_path)], run=fake_public_run)
        elapsed = time.perf_counter() - t0

        assert rc == 2
        assert elapsed < 2.0
        assert public_run_called is False
        assert "Error:" in capsys.readouterr().err
    finally:
        conflict_sock.close()

    # 4) Custom ops_run raising an exception on startup surfaces RuntimeError in caller
    def failing_ops_run(app, **kw):
        raise OSError("simulated startup failure")

    with pytest.raises(RuntimeError, match="Ops listener failed to start"):
        start_ops_server(
            ops_app,
            host="127.0.0.1",
            port=0,
            ops_run=failing_ops_run,
        )

    # 5) Free port succeeds, waits until started, and serves immediately
    ops_server, ops_thread = start_ops_server(
        ops_app,
        host="127.0.0.1",
        port=0,
    )
    try:
        assert ops_server is not None
        assert ops_server.started is True
        bound_port = ops_server.config.port
        r = httpx.get(f"http://127.0.0.1:{bound_port}/healthz", timeout=2.0)
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}
    finally:
        ops_server.should_exit = True
        ops_thread.join(timeout=5.0)


def test_d_l1_public_readyz_omits_engines_while_ops_readyz_includes_engines():
    @dataclass
    class FakeRefresher:
        last_success_at: float | None = 1.0

    refresher = FakeRefresher(last_success_at=1.0)
    lb_healthy = [True]

    def lb_connect(**kwargs):
        if not lb_healthy[0]:
            raise ConnectionError("lakebase unreachable")
        return MagicMock()

    adapters = {
        "duck": DuckDBAdapter(connect=lambda: duckdb.connect()),
        "lakebase": DBAPIAdapter(lb_connect, kind="lakebase"),
    }
    readiness = ReadinessProbe(
        adapters,
        is_static=False,
        refresher=refresher,
        cache_seconds=0.0,
    )

    pipeline = ServingPipeline(
        placement=Placement(engines={}, tables={}, cubes={}),
        schemas={},
        public_keys={},
    )
    pub_app = create_app(
        pipeline=pipeline,
        adapters=adapters,
        extra_routes=build_public_extra_routes(
            OpsConfig(public_readyz=True),
            readiness=readiness,
        ),
    )
    ops_app = create_ops_app(
        ops_config=OpsConfig(enabled=True),
        readiness=readiness,
    )

    pub_client = TestClient(pub_app)
    ops_client = TestClient(ops_app)

    # 1) All engines healthy + catalog loaded -> 200
    r_pub_ok = pub_client.get("/readyz")
    assert r_pub_ok.status_code == 200
    assert r_pub_ok.json() == {"ready": True}
    assert "engines" not in r_pub_ok.json()

    r_ops_ok = ops_client.get("/readyz")
    assert r_ops_ok.status_code == 200
    assert r_ops_ok.json() == {
        "ready": True,
        "engines": {"duck": "ok", "lakebase": "ok"},
    }

    # 2) Engine unavailable -> both 503, public omits engines, ops includes engines
    lb_healthy[0] = False
    r_pub_unavail = pub_client.get("/readyz")
    assert r_pub_unavail.status_code == 503
    assert r_pub_unavail.json() == {"ready": False}
    assert "engines" not in r_pub_unavail.json()

    r_ops_unavail = ops_client.get("/readyz")
    assert r_ops_unavail.status_code == 503
    assert r_ops_unavail.json() == {
        "ready": False,
        "engines": {"duck": "ok", "lakebase": "unavailable"},
    }

    # 3) Discovery catalog not loaded -> both 503 with reason, public omits engines
    lb_healthy[0] = True
    refresher.last_success_at = None
    r_pub_cat = pub_client.get("/readyz")
    assert r_pub_cat.status_code == 503
    assert r_pub_cat.json() == {"ready": False, "reason": "catalog_not_loaded"}
    assert "engines" not in r_pub_cat.json()

    r_ops_cat = ops_client.get("/readyz")
    assert r_ops_cat.status_code == 503
    assert r_ops_cat.json() == {
        "ready": False,
        "reason": "catalog_not_loaded",
        "engines": {"duck": "ok", "lakebase": "ok"},
    }

    # Also verify make_readyz_handler directly
    assert readiness.check(include_engines=False) == (
        503,
        {"ready": False, "reason": "catalog_not_loaded"},
    )
    assert callable(make_readyz_handler(readiness, include_engines=False))


def test_d_l2_non_loopback_ops_host_without_auth_token_env_logs_warning(caplog):
    import logging

    # 1) Non-loopback without auth_token_env logs WARNING
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="seeknal.serving.ops"):
        create_ops_app(
            ops_config=OpsConfig(enabled=True, host="0.0.0.0", auth_token_env=None)
        )
    assert any(
        rec.levelname == "WARNING"
        and "Ops listener is bound to non-loopback host '0.0.0.0' without 'ops.auth_token_env' configured"
        in rec.getMessage()
        for rec in caplog.records
    )

    # 2) Loopback hosts without auth_token_env do NOT warn
    for loopback_host in ("127.0.0.1", "::1", "localhost"):
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="seeknal.serving.ops"):
            create_ops_app(
                ops_config=OpsConfig(
                    enabled=True,
                    host=loopback_host,
                    auth_token_env=None,
                )
            )
        assert not caplog.records

    # 3) Non-loopback host WITH auth_token_env configured does NOT warn
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="seeknal.serving.ops"):
        create_ops_app(
            ops_config=OpsConfig(
                enabled=True,
                host="0.0.0.0",
                auth_token_env="SEEKNAL_OPS_TOKEN",
            ),
            env={"SEEKNAL_OPS_TOKEN": "tok-123"},
        )
    assert not caplog.records


def test_c_n5_ops_readyz_reports_audit_sink_not_ready_in_refuse_mode_only(tmp_path):
    _, jwks_file, db_file, pf, sf = _setup_test_keys_and_catalog(tmp_path)
    audit_dir = tmp_path / "ops_audit_dir"
    audit_dir.mkdir()
    audit_file = audit_dir / "audit.jsonl"

    cfg_refuse = {
        "listen": {"host": "127.0.0.1", "port": 8765},
        "grant_keys": {"jwks_file": jwks_file},
        "placement": {"static": {"placement_file": pf, "schemas_file": sf}},
        "connections": {
            "duck": {
                "kind": "duckdb",
                "path": db_file,
                "allowed_directories": [str(tmp_path) + "/"],
            },
            "lakebase": {"kind": "lakebase", "dsn_env": "LB_DSN_N5"},
        },
        "audit": {
            "sink": "file",
            "on_failure": "refuse",
            "path": str(audit_file),
            "max_bytes": 65536,
            "timeout_seconds": 0.2,
        },
        "ops": {
            "enabled": True,
            "host": "127.0.0.1",
            "port": 9098,
            "readyz_timeout_seconds": 0.2,
            "readyz_cache_seconds": 0.0,
        },
    }

    handle = build_server(
        cfg_refuse,
        env={"LB_DSN_N5": "postgresql://u:p@localhost:5432/db"},
        connect_factories={"lakebase": lambda **kw: MagicMock()},
    )
    assert handle.ops_app is not None

    with TestClient(handle.app) as pub_c, TestClient(handle.ops_app) as ops_c:
        assert ops_c.get("/readyz").status_code == 200
        assert ops_c.get("/readyz").json() == {
            "ready": True,
            "engines": {"duck": "ok", "lakebase": "ok"},
        }

        # Replace audit file with symlink so audit sink goes down at runtime
        victim = tmp_path / "n5_victim.txt"
        victim.write_text("safe\n", encoding="utf-8")
        sym = audit_dir / "sym.tmp"
        sym.symlink_to(victim)
        os.replace(sym, audit_file)

        r_ops_down = ops_c.get("/readyz")
        assert r_ops_down.status_code == 503
        assert r_ops_down.json() == {
            "ready": False,
            "reason": "audit_sink",
            "engines": {"duck": "ok", "lakebase": "ok"},
        }

        r_pub_down = pub_c.get("/readyz")
        assert r_pub_down.status_code == 503
        assert r_pub_down.json() == {
            "ready": False,
            "reason": "audit_sink",
        }

    # In on_failure="log" mode, a broken audit sink does NOT mark /readyz not-ready
    class _DownSink(AuditSink):
        def verify_writable(self) -> None:
            raise AuditWriteError("down")

        def write_record(self, record: AuditRecord) -> None:
            raise AuditWriteError("down")

    probe_log = ReadinessProbe(
        {"duck": DuckDBAdapter(connect=lambda: duckdb.connect())},
        audit_sink=_DownSink(),
        audit_on_failure="log",
        cache_seconds=0.0,
    )
    st_log, body_log = probe_log.check()
    assert st_log == 200
    assert body_log == {"ready": True, "engines": {"duck": "ok"}}


# ---------------------------------------------------------------------------
# 8. Multi-source Postgres, Lakebase Cold-Start, Select Route & Readiness "starting"
# ---------------------------------------------------------------------------


def test_run3_postgres_pool_guard_sync_discovery_and_lakebase_metrics():
    from seeknal.serving.adapters import PostgresPool

    assert "/v1/select" in ALLOWED_ROUTES

    class FakeConn:
        closed = 0

        def close(self):
            self.closed = 1

    pg_pool_bi = PostgresPool(lambda: FakeConn(), pool_max=4)
    pg_pool_ask = PostgresPool(lambda: FakeConn(), pool_max=2)
    lb_pool = PostgresPool(lambda: FakeConn(), pool_max=4)

    try:
        c_bi_1 = pg_pool_bi.acquire()
        c_bi_2 = pg_pool_bi.acquire()
        pg_pool_bi.release(c_bi_1, success=True)  # 1 in_use, 1 idle

        c_ask = pg_pool_ask.acquire()  # 1 in_use, 0 idle
        c_lb = lb_pool.acquire()
        lb_pool.release(c_lb, success=True)  # 0 in_use, 1 idle

        pg_routed = WorkloadRoutedAdapter(
            {
                "interactive": DBAPIAdapter(
                    lambda: FakeConn(),
                    kind="postgres",
                    pool=pg_pool_bi,
                    account="interactive",
                ),
                "bi": DBAPIAdapter(
                    lambda: FakeConn(),
                    kind="postgres",
                    pool=pg_pool_bi,
                    account="bi",
                ),
                "ask": DBAPIAdapter(
                    lambda: FakeConn(),
                    kind="postgres",
                    pool=pg_pool_ask,
                    account="ask",
                ),
            }
        )
        lb_single = DBAPIAdapter(
            lambda: FakeConn(),
            kind="lakebase",
            pool=lb_pool,
        )

        registry = MetricsRegistry(
            adapters={"pg_report": pg_routed, "lb_ops": lb_single}
        )

        # /v1/select route allowlist
        registry.record_request(
            "/v1/select",
            engine="pg_report",
            status=200,
            duration_seconds=0.012,
        )

        # Guardrail rejections (both record_guard_rejection and inc_guard_rejection + bounded enum)
        registry.record_guard_rejection("filter_required")
        registry.inc_guard_rejection("filter_required")
        registry.record_guard_rejection("sort_not_indexed")
        registry.record_guard_rejection("offset_too_large")
        registry.record_guard_rejection("PLANTED_GUARD_SECRET_XYZ")

        # Sync lag & discovery refresh
        registry.set_sync_lag("pg_report", 12.5)
        registry.set_sync_lag("lb_ops", 0.0)
        registry.record_discovery_refresh("pg_report", "success")
        registry.record_discovery_refresh("lb_ops", "error")
        registry.record_discovery_refresh("lb_ops", "PLANTED_DISC_SECRET_XYZ")

        # Lakebase cold-start & wake series
        registry.record_lakebase_wake("lb_ops", "ok")
        registry.record_lakebase_wake("lb_ops", "success")
        registry.record_lakebase_wake("lb_ops", "timeout")
        registry.record_lakebase_wake("lb_ops", "PLANTED_WAKE_SECRET_XYZ")
        registry.record_lakebase_connect_retry("lb_ops")
        registry.record_lakebase_connect_retry("lb_ops", 2)
        registry.observe_lakebase_cold_start("lb_ops", 3.2)
        registry.set_lakebase_state("lb_ops", "starting")

        rendered = registry.render()

        assert (
            'seeknal_serving_requests_total{route="/v1/select",engine="pg_report",status="200"} 1'
            in rendered
        )

        # Shared Postgres-wire pool gauges with {engine,kind,account}
        assert (
            'seeknal_serving_postgres_pool_in_use{engine="pg_report",kind="postgres",account="interactive"} 1'
            in rendered
        )
        assert (
            'seeknal_serving_postgres_pool_idle{engine="pg_report",kind="postgres",account="interactive"} 1'
            in rendered
        )
        assert (
            'seeknal_serving_postgres_pool_in_use{engine="pg_report",kind="postgres",account="bi"} 1'
            in rendered
        )
        assert (
            'seeknal_serving_postgres_pool_in_use{engine="pg_report",kind="postgres",account="ask"} 1'
            in rendered
        )
        assert (
            'seeknal_serving_postgres_pool_idle{engine="pg_report",kind="postgres",account="ask"} 0'
            in rendered
        )
        assert (
            'seeknal_serving_postgres_pool_in_use{engine="lb_ops",kind="lakebase",account="default"} 0'
            in rendered
        )
        assert (
            'seeknal_serving_postgres_pool_idle{engine="lb_ops",kind="lakebase",account="default"} 1'
            in rendered
        )

        # Deprecated Lakebase pool duplicates still emitted
        assert 'seeknal_serving_lakebase_pool_in_use{engine="pg_report"} 2' in rendered
        assert 'seeknal_serving_lakebase_pool_idle{engine="pg_report"} 1' in rendered
        assert 'seeknal_serving_lakebase_pool_in_use{engine="lb_ops"} 0' in rendered
        assert 'seeknal_serving_lakebase_pool_idle{engine="lb_ops"} 1' in rendered

        # Guard rejections
        assert 'seeknal_serving_guard_rejections_total{code="filter_required"} 2' in rendered
        assert 'seeknal_serving_guard_rejections_total{code="sort_not_indexed"} 1' in rendered
        assert 'seeknal_serving_guard_rejections_total{code="offset_too_large"} 1' in rendered
        assert 'seeknal_serving_guard_rejections_total{code="other"} 1' in rendered

        # Sync lag & discovery refresh
        assert 'seeknal_serving_sync_lag_seconds{engine="pg_report"} 12.5' in rendered
        assert 'seeknal_serving_sync_lag_seconds{engine="lb_ops"} 0' in rendered
        assert (
            'seeknal_serving_discovery_refresh_total{engine="pg_report",outcome="success"} 1'
            in rendered
        )
        assert (
            'seeknal_serving_discovery_refresh_total{engine="lb_ops",outcome="error"} 2'
            in rendered
        )

        # Lakebase wake, connect retries, cold start histogram, state gauge
        assert 'seeknal_serving_lakebase_wake_total{engine="lb_ops",outcome="ok"} 1' in rendered
        assert 'seeknal_serving_lakebase_wake_total{engine="lb_ops",outcome="success"} 1' in rendered
        assert 'seeknal_serving_lakebase_wake_total{engine="lb_ops",outcome="timeout"} 1' in rendered
        assert 'seeknal_serving_lakebase_wake_total{engine="lb_ops",outcome="error"} 1' in rendered
        assert 'seeknal_serving_lakebase_connect_retries_total{engine="lb_ops"} 3' in rendered
        assert 'seeknal_serving_lakebase_cold_start_seconds_bucket{engine="lb_ops",le="5.0"} 1' in rendered
        assert 'seeknal_serving_lakebase_cold_start_seconds_count{engine="lb_ops"} 1' in rendered
        assert 'seeknal_serving_lakebase_state{engine="lb_ops",state="ok"} 0' in rendered
        assert 'seeknal_serving_lakebase_state{engine="lb_ops",state="starting"} 1' in rendered
        assert 'seeknal_serving_lakebase_state{engine="lb_ops",state="unavailable"} 0' in rendered

        for planted in (
            "PLANTED_GUARD_SECRET_XYZ",
            "PLANTED_DISC_SECRET_XYZ",
            "PLANTED_WAKE_SECRET_XYZ",
        ):
            assert planted not in rendered

        pg_pool_bi.release(c_bi_2, success=True)
        pg_pool_ask.release(c_ask, success=True)
    finally:
        pg_pool_bi.close()
        pg_pool_ask.close()
        lb_pool.close()


def test_readyz_reports_starting_for_lakebase_cold_start_connector():
    from seeknal.serving.adapters import PostgresPool

    class FakeColdStartConnector:
        def __init__(self, state: str = "starting") -> None:
            self.state = state

    connector = FakeColdStartConnector("starting")

    def connect_wrapper(**kwargs):
        if connector.state == "starting":
            raise ConnectionRefusedError("endpoint waking")
        return MagicMock()

    setattr(connect_wrapper, "cold_start_connector", connector)
    pool = PostgresPool(connect_wrapper, pool_max=2)

    try:
        lb_adapter = DBAPIAdapter(connect_wrapper, kind="lakebase", pool=pool)
        pg_adapter = DBAPIAdapter(lambda: MagicMock(), kind="postgres")

        probe = ReadinessProbe(
            {"pg_report": pg_adapter, "lb_ops": lb_adapter},
            timeout_seconds=1.0,
            cache_seconds=5.0,
        )
        ops_app = create_ops_app(readiness=probe)
        client = TestClient(ops_app)

        # 1. While connector.state == "starting", /readyz returns 503 with lb_ops: "starting"
        r_starting = client.get("/readyz")
        assert r_starting.status_code == 503
        assert r_starting.json() == {
            "ready": False,
            "engines": {"pg_report": "ok", "lb_ops": "starting"},
        }

        # 2. Once connector transitions to "ok", /readyz transitions to 200 ok
        connector.state = "ok"
        r_ok = client.get("/readyz")
        assert r_ok.status_code == 200
        assert r_ok.json() == {
            "ready": True,
            "engines": {"pg_report": "ok", "lb_ops": "ok"},
        }
    finally:
        pool.close()




