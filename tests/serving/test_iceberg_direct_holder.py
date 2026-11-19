"""Tests for DuckDBHolder and DuckDBAdapter with iceberg_direct profile: recycle under concurrent load, probe failure recycle, and stage-1 guarantees."""

from __future__ import annotations

import concurrent.futures
import threading
import time

import duckdb
import pytest

from seeknal.serving.adapters import (
    DuckDBAdapter,
    EngineCancelled,
    EngineError,
    EngineTimeout,
    QueryResult,
)
from seeknal.serving.duckdb_policy import (
    DuckDBHolder,
    probe_iceberg_direct,
    probe_iceberg_direct_ok,
)
from seeknal.serving.server.config import (
    DuckDBConnectionConfig,
    IcebergCatalogConfig,
    IcebergS3Config,
)
from seeknal.serving.service import PreparedQuery


class FakeClock:
    def __init__(self, initial: float = 1000.0) -> None:
        self._now = initial
        self._lock = threading.Lock()

    def __call__(self) -> float:
        with self._lock:
            return self._now

    def advance(self, seconds: float) -> float:
        with self._lock:
            self._now += seconds
            return self._now


def _make_prepared(
    *,
    sql: str = "SELECT id, label FROM lake.ns.items ORDER BY id",
    params: tuple = (),
    row_limit: int | None = 20,
    audit_id: str = "audit-iceberg-holder",
) -> PreparedQuery:
    return PreparedQuery(
        engine="duck_ice",
        engine_kind="duckdb",
        sql=sql,
        params=params,
        paramstyle="qmark",
        rule="analytics",
        reason="iceberg_direct holder test",
        row_limit=row_limit,
        masked_columns=(),
        redacted_sql=sql,
        subject="analyst@example.com",
        workspace="ws-iceberg",
        audit_id=audit_id,
        cube_security_context=None,
    )


def _build_standin_cfg(tmp_path, *, recycle_seconds: float = 60.0):
    ext_dir = tmp_path / "ext"
    ext_dir.mkdir(exist_ok=True)
    temp_dir = tmp_path / "tmp_spill"
    temp_dir.mkdir(exist_ok=True)
    cat_dir = tmp_path / "cat"
    cat_dir.mkdir(exist_ok=True)
    cat_file = cat_dir / "lake.duckdb"

    con = duckdb.connect(str(cat_file))
    con.execute("CREATE SCHEMA ns")
    con.execute("CREATE TABLE ns.items (id INT, label VARCHAR)")
    con.execute("INSERT INTO ns.items VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
    con.close()

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
        recycle_seconds=recycle_seconds,
        token_lifetime_seconds=recycle_seconds * 2.0,
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
    return cfg, {"lake": str(cat_file)}


def test_iceberg_direct_generation_recycle_drains_inflight_and_closes_old(tmp_path):
    """When recycle_seconds elapses, new queries go to a new in-memory generation while in-flight queries finish on the old one."""
    clock = FakeClock(1000.0)
    holder = DuckDBHolder(max_idle_seconds=0, clock=clock)
    try:
        cfg, standins = _build_standin_cfg(tmp_path, recycle_seconds=30.0)

        # Acquire cursor 1 on generation 1
        cur1 = holder.get_iceberg_cursor(
            cfg,
            config_id="duck_ice",
            standin_catalogs=standins,
        )
        gen1_key = holder._configured_to_dev_ino["duck_ice"]
        gen1_inst = holder._instances_by_dev_ino[gen1_key]
        assert gen1_inst.in_flight == 1
        assert not gen1_inst.is_retired
        assert not gen1_inst.is_closed

        # Before recycle_seconds elapses, same generation is reused
        clock.advance(15.0)
        cur2 = holder.get_iceberg_cursor(
            cfg,
            config_id="duck_ice",
            standin_catalogs=standins,
        )
        assert holder._configured_to_dev_ino["duck_ice"] == gen1_key
        assert gen1_inst.in_flight == 2
        cur2.close()
        assert gen1_inst.in_flight == 1

        # Advance past recycle_seconds (15 + 20 = 35s >= 30s)
        clock.advance(20.0)
        cur3 = holder.get_iceberg_cursor(
            cfg,
            config_id="duck_ice",
            standin_catalogs=standins,
        )
        gen2_key = holder._configured_to_dev_ino["duck_ice"]
        assert gen2_key != gen1_key
        gen2_inst = holder._instances_by_dev_ino[gen2_key]

        # Old generation is retired but still open because cur1 is in-flight
        assert gen1_inst.is_retired is True
        assert gen1_inst.is_closed is False
        assert gen1_inst.in_flight == 1
        assert gen2_inst.in_flight == 1

        # In-flight cur1 can still execute and finish cleanly
        rows1 = cur1.execute("SELECT id, label FROM lake.ns.items ORDER BY id").fetchall()
        assert rows1 == [(1, "alpha"), (2, "beta"), (3, "gamma")]
        cur1.close()

        # Once drained, old generation closes immediately and is removed
        assert gen1_inst.is_closed is True
        assert gen1_key not in holder._instances_by_dev_ino

        # New generation serves queries and remains open
        rows3 = cur3.execute("SELECT id, label FROM lake.ns.items ORDER BY id").fetchall()
        assert rows3 == [(1, "alpha"), (2, "beta"), (3, "gamma")]
        cur3.close()
        assert gen2_inst.in_flight == 0
        assert gen2_inst.is_closed is False
    finally:
        holder.close_all()


def test_iceberg_direct_recycle_on_probe_failure(tmp_path):
    """When probe fails on an active generation, holder recycles to a new healthy generation."""
    clock = FakeClock(1000.0)
    holder = DuckDBHolder(max_idle_seconds=0, clock=clock)
    try:
        cfg, standins = _build_standin_cfg(tmp_path, recycle_seconds=300.0)
        cur1 = holder.get_iceberg_cursor(
            cfg,
            config_id="duck_ice",
            standin_catalogs=standins,
        )
        gen1_key = holder._configured_to_dev_ino["duck_ice"]
        gen1_inst = holder._instances_by_dev_ino[gen1_key]
        cur1.close()

        # Simulate corruption / probe failure on gen1 by closing its underlying connection
        gen1_inst.connection.close()

        # Next get_iceberg_cursor with probe_on_reuse=True detects probe failure and recycles
        cur2 = holder.get_iceberg_cursor(
            cfg,
            config_id="duck_ice",
            standin_catalogs=standins,
            probe_on_reuse=True,
        )
        gen2_key = holder._configured_to_dev_ino["duck_ice"]
        assert gen2_key != gen1_key
        rows = cur2.execute("SELECT count(*) FROM lake.ns.items").fetchall()
        assert rows == [(3,)]
        cur2.close()
    finally:
        holder.close_all()


def test_iceberg_direct_concurrent_recycle_and_queries_zero_failures(tmp_path):
    """Concurrent recycle + queries across 8 worker threads with fake clock advancing has zero failed queries and zero leaks."""
    clock = FakeClock(5000.0)
    holder = DuckDBHolder(max_idle_seconds=0, clock=clock)
    try:
        cfg, standins = _build_standin_cfg(tmp_path, recycle_seconds=10.0)
        adapter = DuckDBAdapter(
            config=cfg,
            holder=holder,
            standin_catalogs=standins,
            max_concurrent_cursors=8,
        )

        stop_event = threading.Event()
        errors: list[BaseException] = []
        completed_count = 0
        count_lock = threading.Lock()

        def worker(worker_id: int) -> None:
            nonlocal completed_count
            i = 0
            while not stop_event.is_set():
                i += 1
                prep = _make_prepared(
                    sql="SELECT id, label FROM lake.ns.items WHERE id = ? ORDER BY id",
                    params=((i % 3) + 1,),
                    audit_id=f"audit-w{worker_id}-{i}",
                )
                try:
                    res = adapter.execute(prep, timeout_seconds=5.0)
                    assert isinstance(res, QueryResult)
                    assert len(res.rows) == 1
                    with count_lock:
                        completed_count += 1
                except BaseException as exc:
                    with count_lock:
                        errors.append(exc)
                    break

        threads = [threading.Thread(target=worker, args=(w,)) for w in range(8)]
        for t in threads:
            t.start()

        # Trigger 12 generation recycles while 8 threads continuously execute queries,
        # waiting for queries to execute on each recycled generation
        for _ in range(12):
            target = completed_count + 8
            deadline = time.monotonic() + 5.0
            while completed_count < target and time.monotonic() < deadline and not errors:
                time.sleep(0.002)
            clock.advance(15.0)

        target = completed_count + 8
        deadline = time.monotonic() + 5.0
        while completed_count < target and time.monotonic() < deadline and not errors:
            time.sleep(0.002)

        stop_event.set()
        for t in threads:
            t.join(timeout=10.0)

        assert errors == [], f"Unexpected query failures during concurrent recycle: {errors}"
        assert completed_count >= 50
        assert adapter._active_slots == 0

        # Only 1 current generation remains open, and its in_flight count is 0
        open_insts = [
            inst for inst in holder._instances_by_dev_ino.values() if not inst.is_closed
        ]
        assert len(open_insts) == 1
        assert open_insts[0].in_flight == 0
        assert not open_insts[0].is_retired
    finally:
        holder.close_all()


def test_iceberg_direct_stage1_guarantees_cursor_bound_queue_cancel_and_cleanup(tmp_path):
    """All stage-1 guarantees (cursor bound, deadline-aware queue, queued cancel never executes) hold on iceberg_direct."""
    clock = FakeClock(1000.0)
    holder = DuckDBHolder(max_idle_seconds=0, clock=clock)
    try:
        cfg, standins = _build_standin_cfg(tmp_path, recycle_seconds=60.0)
        adapter = DuckDBAdapter(
            config=cfg,
            holder=holder,
            standin_catalogs=standins,
            max_concurrent_cursors=1,
        )

        # Hold the single slot
        slot_acquired = adapter._slot_semaphore.acquire(timeout=1.0)
        assert slot_acquired is True
        try:
            # 1. Queued cancel before slot acquisition never touches DuckDB
            cancel_holders: list = []
            with pytest.raises(EngineCancelled):
                def register_and_cancel(fn):
                    cancel_holders.append(fn)
                    fn()

                adapter.execute(
                    _make_prepared(audit_id="audit-queued-cancel"),
                    timeout_seconds=2.0,
                    on_cancel_handle=register_and_cancel,
                )

            # 2. Deadline-aware queue timeout when slot is busy
            with pytest.raises(EngineTimeout):
                adapter.execute(
                    _make_prepared(audit_id="audit-queued-timeout"),
                    timeout_seconds=0.03,
                )
        finally:
            adapter._slot_semaphore.release()

        # 3. Normal execution after slot release succeeds and cleans up cursor
        res = adapter.execute(_make_prepared(audit_id="audit-ok"), timeout_seconds=5.0)
        assert res.rows == [(1, "alpha"), (2, "beta"), (3, "gamma")]
        assert adapter._active_slots == 0
        inst = next(iter(holder._instances_by_dev_ino.values()))
        assert inst.in_flight == 0
    finally:
        holder.close_all()
