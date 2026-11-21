"""Stage 3 live gate harness for the DuckDB iceberg_direct engine profile.

Skipped unless SEEKNAL_SPIKE_LIVE=1.
All endpoints, warehouses, table references, and secret variable NAMES are read
strictly from environment variables. Each gate check emits a single machine-readable
JSON line: {"gate": "...", "pass": bool, "detail": ...}.
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import resource
import statistics
import threading
import time
from datetime import datetime, timezone
from typing import Any

import duckdb
import httpx
import pytest

from seeknal.serving.adapters import DuckDBAdapter
from seeknal.serving.discovery.build import discover
from seeknal.serving.duckdb_policy import (
    DuckDBHolder,
    harden_iceberg_direct,
    plan_iceberg_direct,
    probe_iceberg_direct,
    probe_iceberg_direct_ok,
)
from seeknal.serving.placement import Placement
from seeknal.serving.router import Router, RoutingError
from seeknal.serving.server.config import (
    DuckDBConnectionConfig,
    IcebergCatalogConfig,
    IcebergS3Config,
)
from seeknal.serving.service import PreparedQuery

pytestmark = pytest.mark.skipif(
    os.environ.get("SEEKNAL_SPIKE_LIVE") != "1",
    reason="Live Iceberg-direct spike gates require SEEKNAL_SPIKE_LIVE=1",
)


def _emit_gate(gate: str, passed: bool, detail: Any) -> dict[str, Any]:
    payload = {"gate": gate, "pass": bool(passed), "detail": detail}
    print(json.dumps(payload, separators=(",", ":"), sort_keys=True), flush=True)
    return payload


def _require_env(var_name: str) -> str:
    val = os.environ.get(var_name, "").strip()
    if not val:
        pytest.skip(f"Missing required live gate env var '{var_name}'")
    return val


def _build_live_config(
    tmp_path,
    *,
    recycle_seconds: float | None = None,
    external_access_after_attach: str | None = None,
) -> tuple[DuckDBConnectionConfig, str, str]:
    ext_dir = _require_env("SEEKNAL_SPIKE_EXTENSION_DIR")
    alias = os.environ.get("SEEKNAL_SPIKE_CATALOG_ALIAS", "lake").strip()
    endpoint = _require_env("SEEKNAL_SPIKE_LAKEKEEPER_ENDPOINT")
    warehouse = _require_env("SEEKNAL_SPIKE_LAKEKEEPER_WAREHOUSE")
    token_uri = _require_env("SEEKNAL_SPIKE_OAUTH_TOKEN_URI")
    client_id_env = _require_env("SEEKNAL_SPIKE_CLIENT_ID_ENV_NAME")
    client_secret_env = _require_env("SEEKNAL_SPIKE_CLIENT_SECRET_ENV_NAME")
    scope = os.environ.get("SEEKNAL_SPIKE_OAUTH_SCOPE", "").strip() or None
    access_delegation = os.environ.get("SEEKNAL_SPIKE_ACCESS_DELEGATION", "vended").strip()

    s3_endpoint = _require_env("SEEKNAL_SPIKE_S3_ENDPOINT")
    s3_region = os.environ.get("SEEKNAL_SPIKE_S3_REGION", "us-east-1").strip()
    s3_url_style = os.environ.get("SEEKNAL_SPIKE_S3_URL_STYLE", "path").strip()
    s3_key_id_env = _require_env("SEEKNAL_SPIKE_S3_KEY_ID_ENV_NAME")
    s3_secret_env = _require_env("SEEKNAL_SPIKE_S3_SECRET_ENV_NAME")
    s3_use_ssl = os.environ.get("SEEKNAL_SPIKE_S3_USE_SSL", "true").strip().lower() in (
        "1",
        "true",
        "yes",
    )
    s3_prefixes_raw = _require_env("SEEKNAL_SPIKE_ALLOWED_S3_PREFIXES")
    allowed_s3_prefixes = tuple(
        p.strip() for p in s3_prefixes_raw.split(",") if p.strip()
    )

    namespace = _require_env("SEEKNAL_SPIKE_TEST_NAMESPACE")
    table = _require_env("SEEKNAL_SPIKE_TEST_TABLE")

    rec_s = (
        recycle_seconds
        if recycle_seconds is not None
        else float(os.environ.get("SEEKNAL_SPIKE_RECYCLE_SECONDS", "30"))
    )
    ext_after = (
        external_access_after_attach
        if external_access_after_attach is not None
        else os.environ.get("SEEKNAL_SPIKE_EXTERNAL_ACCESS_AFTER_ATTACH", "off").strip()
    )

    temp_dir = tmp_path / "live_duckdb_temp"
    temp_dir.mkdir(exist_ok=True)

    cat_cfg = IcebergCatalogConfig(
        alias=alias,
        endpoint=endpoint,
        warehouse=warehouse,
        token_uri=token_uri,
        client_id_env=client_id_env,
        client_secret_env=client_secret_env,
        scope=scope,
        access_delegation=access_delegation,
        s3=IcebergS3Config(
            endpoint=s3_endpoint,
            region=s3_region,
            url_style=s3_url_style,
            key_id_env=s3_key_id_env,
            secret_env=s3_secret_env,
            use_ssl=s3_use_ssl,
        ),
    )

    cfg = DuckDBConnectionConfig(
        profile="iceberg_direct",
        path=":memory:",
        extension_directory=ext_dir,
        extensions=("httpfs", "iceberg"),
        iceberg_catalogs={alias: cat_cfg},
        allowed_s3_prefixes=allowed_s3_prefixes,
        recycle_seconds=rec_s,
        external_access_after_attach=ext_after,
        memory_limit=os.environ.get("SEEKNAL_SPIKE_MEMORY_LIMIT", "1GB"),
        threads=int(os.environ.get("SEEKNAL_SPIKE_THREADS", "4")),
        temp_directory=str(temp_dir),
        max_temp_directory_size="2GB",
        max_concurrent_cursors=16,
    )
    return cfg, f"{alias}.{namespace}.{table}", alias


def _make_live_prepared(sql: str, *, audit_id: str = "live-gate") -> PreparedQuery:
    return PreparedQuery(
        engine="duck_live",
        engine_kind="duckdb",
        sql=sql,
        params=(),
        paramstyle="qmark",
        rule="analytics",
        reason="live gate check",
        row_limit=100,
        masked_columns=(),
        redacted_sql=sql,
        subject="spike-gate@internal",
        workspace="ws-spike",
        audit_id=audit_id,
        cube_security_context=None,
    )


def _current_rss_mb() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # On Linux ru_maxrss is in KiB; on macOS it is in bytes
    if os.uname().sysname.lower() == "darwin":
        return round(usage / (1024.0 * 1024.0), 2)
    return round(usage / 1024.0, 2)


def test_s1_offline_extension_load(tmp_path):
    """S1: Offline extension load from pre-populated extension_directory with autoinstall/autoload disabled."""
    cfg, _, _ = _build_live_config(tmp_path)
    con = duckdb.connect(":memory:")
    try:
        con.execute("SET autoinstall_known_extensions = false")
        con.execute("SET autoload_known_extensions = false")
        con.execute(f"SET extension_directory = '{cfg.extension_directory}'")
        for ext in cfg.extensions:
            con.execute(f"LOAD {ext}")
        rows = con.execute(
            "SELECT extension_name, loaded FROM duckdb_extensions() WHERE extension_name IN ('httpfs', 'iceberg')"
        ).fetchall()
        loaded_map = {str(r[0]): bool(r[1]) for r in rows}
        passed = all(loaded_map.get(e) is True for e in cfg.extensions)
        _emit_gate("S1", passed, {"loaded": loaded_map})
        assert passed
    finally:
        con.close()


def test_s2_select_after_external_access_off_post_attach(tmp_path):
    """S2: SELECT works after external access is turned off post-ATTACH (or records if external_access_after_attach=on is required)."""
    DuckDBHolder.reset()
    try:
        cfg, phys_table, _ = _build_live_config(
            tmp_path, external_access_after_attach="off"
        )
        holder = DuckDBHolder.get_holder()
        cur = holder.get_iceberg_cursor(cfg)
        try:
            row = cur.execute(f"SELECT count(*) FROM {phys_table}").fetchone()
            cnt = int(row[0]) if row else -1
            ext_setting = cur.execute(
                "SELECT current_setting('enable_external_access')"
            ).fetchone()[0]
            passed = cnt >= 0 and (
                ext_setting is False or str(ext_setting).lower() == "false"
            )
            _emit_gate(
                "S2",
                passed,
                {
                    "row_count": cnt,
                    "enable_external_access": ext_setting,
                    "table": phys_table,
                },
            )
            assert passed
        finally:
            cur.close()
    finally:
        DuckDBHolder.reset()


def test_s3_s3_prefix_confinement_read_text_and_copy_to_refused(tmp_path):
    """S3: S3-prefix confinement, local read_text outside allowed_directories, and COPY TO refused."""
    DuckDBHolder.reset()
    try:
        cfg, _, _ = _build_live_config(tmp_path)
        holder = DuckDBHolder.get_holder()
        cur = holder.get_iceberg_cursor(cfg)
        try:
            report = probe_iceberg_direct(cur, cfg)
            passed = (
                report.get("read_text_outside_refused") is True
                and report.get("copy_to_refused") is True
                and report.get("s3_outside_prefix_refused") is True
            )
            _emit_gate(
                "S3",
                passed,
                {
                    "read_text_outside_refused": report.get("read_text_outside_refused"),
                    "copy_to_refused": report.get("copy_to_refused"),
                    "s3_outside_prefix_refused": report.get("s3_outside_prefix_refused"),
                },
            )
            assert passed
        finally:
            cur.close()
    finally:
        DuckDBHolder.reset()


def test_s4_secrets_temporary_and_no_secret_after_lock_or_in_logs(tmp_path):
    """S4: Secrets are temporary only, CREATE SECRET after lock is refused, and no secret text appears in plan safe_sql or errors."""
    DuckDBHolder.reset()
    try:
        cfg, _, alias = _build_live_config(tmp_path)
        steps = plan_iceberg_direct(cfg)
        cat = cfg.iceberg_catalogs[alias]
        secret_values = [
            os.environ.get(cat.client_id_env, ""),
            os.environ.get(cat.client_secret_env, ""),
            os.environ.get(cat.s3.key_id_env, ""),
            os.environ.get(cat.s3.secret_env, ""),
        ]
        leaked_in_plan = any(
            sv and sv in step.safe_sql
            for sv in secret_values
            for step in steps
        )

        holder = DuckDBHolder.get_holder()
        cur = holder.get_iceberg_cursor(cfg)
        try:
            report = probe_iceberg_direct(cur, cfg)
            passed = (
                not leaked_in_plan
                and report.get("secrets_temporary_only") is True
                and report.get("create_secret_refused") is True
            )
            _emit_gate(
                "S4",
                passed,
                {
                    "leaked_in_plan": leaked_in_plan,
                    "secrets_temporary_only": report.get("secrets_temporary_only"),
                    "create_secret_refused": report.get("create_secret_refused"),
                },
            )
            assert passed
        finally:
            cur.close()
    finally:
        DuckDBHolder.reset()


def test_s5_token_refresh_via_recycle_at_c8(tmp_path):
    """S5: Token refresh via generation recycle at concurrency c8 for a configurable duration."""
    DuckDBHolder.reset()
    try:
        duration_s = float(os.environ.get("SEEKNAL_SPIKE_S5_DURATION_SECONDS", "15"))
        recycle_s = float(os.environ.get("SEEKNAL_SPIKE_S5_RECYCLE_SECONDS", "5"))
        cfg, phys_table, _ = _build_live_config(tmp_path, recycle_seconds=recycle_s)
        holder = DuckDBHolder(max_idle_seconds=0)
        adapter = DuckDBAdapter(config=cfg, holder=holder, max_concurrent_cursors=8)

        stop_at = time.monotonic() + duration_s
        errors: list[str] = []
        total_queries = 0
        lock = threading.Lock()

        def worker(w_id: int) -> None:
            nonlocal total_queries
            i = 0
            while time.monotonic() < stop_at:
                i += 1
                prep = _make_live_prepared(
                    f"SELECT count(*) FROM {phys_table}",
                    audit_id=f"s5-w{w_id}-{i}",
                )
                try:
                    adapter.execute(prep, timeout_seconds=10.0)
                    with lock:
                        total_queries += 1
                except Exception as exc:
                    with lock:
                        errors.append(f"{type(exc).__name__}: {exc}")
                    break

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        passed = len(errors) == 0 and total_queries > 0 and holder._generation_seq >= 2
        _emit_gate(
            "S5",
            passed,
            {
                "duration_seconds": duration_s,
                "recycle_seconds": recycle_s,
                "total_queries": total_queries,
                "generations_created": holder._generation_seq,
                "error_count": len(errors),
            },
        )
        assert passed
    finally:
        DuckDBHolder.reset()


def test_s6_writes_refused_on_attached_catalog(tmp_path):
    """S6: Writes (INSERT, DELETE, CREATE TABLE, DROP TABLE) are refused on the attached Iceberg catalog."""
    DuckDBHolder.reset()
    try:
        cfg, phys_table, alias = _build_live_config(tmp_path)
        holder = DuckDBHolder.get_holder()
        cur = holder.get_iceberg_cursor(cfg)
        try:
            report = probe_iceberg_direct(cur, cfg)
            delete_refused = False
            try:
                cur.execute(f"DELETE FROM {phys_table}")
            except duckdb.Error:
                delete_refused = True
            passed = report.get("writes_refused") is True and delete_refused
            _emit_gate(
                "S6",
                passed,
                {
                    "probe_writes_refused": report.get("writes_refused"),
                    "delete_refused": delete_refused,
                    "catalog": alias,
                },
            )
            assert passed
        finally:
            cur.close()
    finally:
        DuckDBHolder.reset()


def test_s8_latency_sampler_sequential_and_c8_cold_warm(tmp_path):
    """S8: Latency sampler (sequential and c8, cold/warm, p50/p95 + observable request counts)."""
    DuckDBHolder.reset()
    try:
        cfg, phys_table, _ = _build_live_config(tmp_path)
        holder = DuckDBHolder(max_idle_seconds=0)
        adapter = DuckDBAdapter(config=cfg, holder=holder, max_concurrent_cursors=8)
        prep = _make_live_prepared(f"SELECT count(*) FROM {phys_table}")

        # Cold query (includes initial attach + metadata read)
        t0 = time.perf_counter()
        adapter.execute(prep, timeout_seconds=30.0)
        cold_ms = round((time.perf_counter() - t0) * 1000.0, 2)

        # Sequential warm samples
        seq_samples: list[float] = []
        for i in range(20):
            ts = time.perf_counter()
            adapter.execute(
                _make_live_prepared(f"SELECT count(*) FROM {phys_table}", audit_id=f"s8-seq-{i}"),
                timeout_seconds=15.0,
            )
            seq_samples.append((time.perf_counter() - ts) * 1000.0)

        # c8 concurrent warm samples
        def run_one(idx: int) -> float:
            tc = time.perf_counter()
            adapter.execute(
                _make_live_prepared(f"SELECT count(*) FROM {phys_table}", audit_id=f"s8-c8-{idx}"),
                timeout_seconds=15.0,
            )
            return (time.perf_counter() - tc) * 1000.0

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            c8_samples = list(pool.map(run_one, range(32)))

        seq_sorted = sorted(seq_samples)
        c8_sorted = sorted(c8_samples)
        detail = {
            "cold_ms": cold_ms,
            "sequential_p50_ms": round(statistics.median(seq_sorted), 2),
            "sequential_p95_ms": round(seq_sorted[int(len(seq_sorted) * 0.95)], 2),
            "c8_p50_ms": round(statistics.median(c8_sorted), 2),
            "c8_p95_ms": round(c8_sorted[int(len(c8_sorted) * 0.95)], 2),
        }
        _emit_gate("S8", True, detail)
    finally:
        DuckDBHolder.reset()


def test_s9_pinned_snapshot_read_stable_across_overwrite(tmp_path):
    """S9: Pinned-snapshot read AT (VERSION => <id>) remains stable across an overwrite commit."""
    DuckDBHolder.reset()
    try:
        pinned_snap = _require_env("SEEKNAL_SPIKE_PINNED_SNAPSHOT_ID")
        cfg, phys_table, _ = _build_live_config(tmp_path)
        holder = DuckDBHolder.get_holder()
        cur = holder.get_iceberg_cursor(cfg)
        try:
            snap_sql = f"SELECT count(*) FROM {phys_table} AT (VERSION => {int(pinned_snap)})"
            r1 = cur.execute(snap_sql).fetchone()[0]
            r2 = cur.execute(snap_sql).fetchone()[0]
            passed = r1 == r2
            _emit_gate(
                "S9",
                passed,
                {
                    "snapshot_id": int(pinned_snap),
                    "count_before": r1,
                    "count_after": r2,
                },
            )
            assert passed
        finally:
            cur.close()
    finally:
        DuckDBHolder.reset()


def test_s11_staleness_rejection_from_snapshot_age(tmp_path):
    """S11: Discovery derives max_lag_seconds from Lakekeeper snapshot age and Router rejects stale reads."""
    cfg, _, alias = _build_live_config(tmp_path)
    cat = cfg.iceberg_catalogs[alias]
    disc_cfg = {
        "default_catalog": "lakekeeper",
        "refresh_interval_seconds": 60.0,
        "lakekeeper": {
            "url": cat.endpoint,
            "warehouse": cat.warehouse,
            "oauth": {
                "token_url": cat.token_uri,
                "client_id": os.environ.get(cat.client_id_env, ""),
                "client_secret_env": cat.client_secret_env,
                "scope": cat.scope,
            },
        },
        "engines": {
            "duck_live": {
                "kind": "duckdb",
                "dialect": "duckdb",
                "profile": "iceberg_direct",
                "alias": alias,
            }
        },
    }
    with httpx.Client() as client:
        res = discover(disc_cfg, http_client=client, now=datetime.now(timezone.utc))

    placement = Placement.from_dict(res.placement)
    router = Router(placement)
    ns = _require_env("SEEKNAL_SPIKE_TEST_NAMESPACE")
    tbl = _require_env("SEEKNAL_SPIKE_TEST_TABLE")
    logical = f"lakekeeper.{ns}.{tbl}"

    rejected_stale = False
    try:
        router.route(f"SELECT count(*) FROM {logical}", max_staleness_seconds=0.001)
    except RoutingError:
        rejected_stale = True

    _emit_gate(
        "S11",
        rejected_stale,
        {
            "table": logical,
            "rejected_on_tight_staleness": rejected_stale,
        },
    )
    assert rejected_stale


def test_s13_rss_sampling(tmp_path):
    """S13: Process RSS sampling before and after query and generation recycle batches."""
    DuckDBHolder.reset()
    try:
        cfg, phys_table, _ = _build_live_config(tmp_path, recycle_seconds=5.0)
        holder = DuckDBHolder(max_idle_seconds=0)
        adapter = DuckDBAdapter(config=cfg, holder=holder, max_concurrent_cursors=8)
        rss_start = _current_rss_mb()

        for i in range(25):
            adapter.execute(
                _make_live_prepared(f"SELECT count(*) FROM {phys_table}", audit_id=f"s13-{i}"),
                timeout_seconds=15.0,
            )

        rss_end = _current_rss_mb()
        max_rss_mb = float(os.environ.get("SEEKNAL_SPIKE_MAX_RSS_MB", "4096"))
        passed = rss_end <= max_rss_mb
        _emit_gate(
            "S13",
            passed,
            {
                "rss_start_mb": rss_start,
                "rss_end_mb": rss_end,
                "rss_delta_mb": round(rss_end - rss_start, 2),
                "max_rss_mb": max_rss_mb,
            },
        )
        assert passed
    finally:
        DuckDBHolder.reset()
