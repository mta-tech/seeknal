"""Tests for DuckDB concurrency, connection reuse, symlink swap, temp directory, and reconciled lifecycle guarantees."""

from __future__ import annotations

import concurrent.futures
import os
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
    harden_connection,
    probe,
)
from seeknal.serving.server.config import DuckDBConnectionConfig
from seeknal.serving.service import PreparedQuery


def _make_prepared(
    *,
    engine: str = "duck",
    sql: str = "SELECT 1",
    params: tuple = (),
    row_limit: int | None = 10,
    audit_id: str = "audit-1",
) -> PreparedQuery:
    return PreparedQuery(
        engine=engine,
        engine_kind="duckdb",
        sql=sql,
        params=params,
        paramstyle="qmark",
        rule="test",
        reason="test",
        row_limit=row_limit,
        masked_columns=(),
        redacted_sql="SELECT ?",
        subject="test@example.com",
        workspace="ws-test",
        audit_id=audit_id,
        cube_security_context=None,
    )


def _active_timer_threads() -> list[threading.Thread]:
    return [
        t
        for t in threading.enumerate()
        if isinstance(t, threading.Timer) and t.is_alive()
    ]


@pytest.fixture(autouse=True)
def reset_holder():
    DuckDBHolder.reset()
    yield
    DuckDBHolder.reset()


def test_duckdb_32_concurrent_queries_succeed(tmp_path):
    """32 queries across 8 threads all succeed on a real in-process DuckDB file."""
    db_file = tmp_path / "concurrent.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE metrics (id INT, value DOUBLE)")
    for i in range(100):
        con.execute("INSERT INTO metrics VALUES (?, ?)", [i, float(i * 1.5)])
    con.close()

    cfg = DuckDBConnectionConfig(
        path=str(db_file),
        allowed_directories=(str(tmp_path) + "/",),
        threads=2,
    )
    adapter = DuckDBAdapter(config=cfg)

    def run_query(idx: int) -> QueryResult:
        prep = _make_prepared(
            sql="SELECT id, value FROM metrics WHERE id = ? ORDER BY id",
            params=(idx,),
            audit_id=f"audit-concurrent-{idx}",
        )
        return adapter.execute(prep, timeout_seconds=10.0)

    # 32 queries across 8 threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(run_query, i % 100) for i in range(32)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == 32
    for r in results:
        assert isinstance(r, QueryResult)
        assert len(r.rows) == 1
        assert r.columns == ("id", "value")


def test_duckdb_hardening_holds_on_cursor_during_concurrent_query(tmp_path):
    """Assert hardening properties hold on a cursor taken while another query is running."""
    db_file = tmp_path / "hardening.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE data (id INT)")
    con.execute("INSERT INTO data VALUES (1), (2), (3)")
    con.close()

    allowed_dir = str(tmp_path) + "/"
    cfg = DuckDBConnectionConfig(
        path=str(db_file),
        allowed_directories=(allowed_dir,),
    )
    adapter = DuckDBAdapter(config=cfg)

    slow_started = threading.Event()
    slow_can_finish = threading.Event()
    slow_error = []

    def slow_worker():
        try:
            holder = DuckDBHolder.get_holder()
            cur = holder.get_cursor(str(db_file), allowed_directories=(allowed_dir,))
            slow_started.set()
            slow_can_finish.wait(timeout=10.0)
            cur.execute("SELECT count(*) FROM data")
            cur.fetchall()
            cur.close()
        except Exception as e:
            slow_error.append(e)

    t = threading.Thread(target=slow_worker)
    t.start()
    assert slow_started.wait(timeout=5.0)

    try:
        # Take a new cursor while slow query is running
        holder = DuckDBHolder.get_holder()
        test_cur = holder.get_cursor(str(db_file), allowed_directories=(allowed_dir,))
        try:
            results = probe(test_cur)
            assert results["external_access_disabled"] is True
            assert results["config_locked"] is True
            assert results["read_text_outside_refused"] is True
            assert results["copy_to_refused"] is True
            assert results["attach_refused"] is True
            assert results["install_refused"] is True
            assert results["load_refused"] is True
            assert results["select_ok"] is True
            assert all(results.values())

            # Read-only check: cannot INSERT or CREATE
            with pytest.raises(duckdb.Error):
                test_cur.execute("INSERT INTO data VALUES (999)")
        finally:
            test_cur.close()
    finally:
        slow_can_finish.set()
        t.join()

    assert not slow_error, f"Slow worker failed: {slow_error}"


def test_duckdb_symlink_swap_during_load(tmp_path):
    """A symlink swap during load is picked up by later queries and in-flight ones finish."""
    v1_file = tmp_path / "v1.duckdb"
    con1 = duckdb.connect(str(v1_file))
    con1.execute("CREATE TABLE version (val VARCHAR)")
    con1.execute("INSERT INTO version VALUES ('version_1')")
    con1.close()

    v2_file = tmp_path / "v2.duckdb"
    con2 = duckdb.connect(str(v2_file))
    con2.execute("CREATE TABLE version (val VARCHAR)")
    con2.execute("INSERT INTO version VALUES ('version_2')")
    con2.close()

    symlink_path = tmp_path / "current.duckdb"
    os.symlink(str(v1_file), str(symlink_path))

    cfg = DuckDBConnectionConfig(
        path=str(symlink_path),
        allowed_directories=(str(tmp_path) + "/",),
    )
    adapter = DuckDBAdapter(config=cfg)

    # First query picks up version_1
    res1 = adapter.execute(_make_prepared(sql="SELECT val FROM version"), timeout_seconds=5.0)
    assert res1.rows == [("version_1",)]

    # Start a slow query on v1
    v1_inflight_started = threading.Event()
    v1_inflight_finish = threading.Event()
    v1_result = []

    def inflight_worker():
        try:
            holder = DuckDBHolder.get_holder()
            cur = holder.get_cursor(str(symlink_path), allowed_directories=(str(tmp_path) + "/",))
            v1_inflight_started.set()
            v1_inflight_finish.wait(timeout=10.0)
            cur.execute("SELECT val FROM version")
            rows = cur.fetchall()
            v1_result.append(rows)
            cur.close()
        except Exception as e:
            v1_result.append(e)

    t = threading.Thread(target=inflight_worker)
    t.start()
    assert v1_inflight_started.wait(timeout=5.0)

    # Repoint symlink atomically to v2
    tmp_link = tmp_path / "tmp_symlink"
    os.symlink(str(v2_file), str(tmp_link))
    os.replace(str(tmp_link), str(symlink_path))

    # New query picks up version_2 immediately without restart
    res2 = adapter.execute(_make_prepared(sql="SELECT val FROM version"), timeout_seconds=5.0)
    assert res2.rows == [("version_2",)]

    # Allow in-flight query on v1 to finish
    v1_inflight_finish.set()
    t.join()

    assert v1_result == [[("version_1",)]]


def test_duckdb_timeout_isolation(tmp_path):
    """Timeout of one query does not kill another concurrent query."""
    db_file = tmp_path / "timeout.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE t (n INT)")
    con.execute("INSERT INTO t VALUES (42)")
    con.close()

    cfg = DuckDBConnectionConfig(
        path=str(db_file),
        allowed_directories=(str(tmp_path) + "/",),
    )
    adapter = DuckDBAdapter(config=cfg)

    slow_prep = _make_prepared(
        sql="WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM t WHERE n < 50000000) SELECT count(*) FROM t CROSS JOIN (SELECT 1 UNION ALL SELECT 2)",
        audit_id="audit-slow",
    )
    fast_prep = _make_prepared(
        sql="SELECT n FROM t",
        audit_id="audit-fast",
    )

    slow_error = []
    fast_result = []

    def run_slow():
        try:
            adapter.execute(slow_prep, timeout_seconds=0.2)
        except Exception as e:
            slow_error.append(e)

    def run_fast():
        time.sleep(0.05)
        try:
            res = adapter.execute(fast_prep, timeout_seconds=5.0)
            fast_result.append(res)
        except Exception as e:
            fast_result.append(e)

    t_slow = threading.Thread(target=run_slow)
    t_fast = threading.Thread(target=run_fast)

    t_slow.start()
    t_fast.start()

    t_slow.join()
    t_fast.join()

    assert len(slow_error) == 1
    assert isinstance(slow_error[0], EngineTimeout)

    assert len(fast_result) == 1
    assert isinstance(fast_result[0], QueryResult)
    assert fast_result[0].rows == [(42,)]


def test_duckdb_temp_directory_validation_and_spill_error(tmp_path):
    """temp_directory validation and unset spill failure behavior."""
    allowed_dir = tmp_path / "data"
    allowed_dir.mkdir()
    allowed_str = str(allowed_dir) + "/"

    db_file = allowed_dir / "spill_test.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE t AS SELECT range AS x FROM range(100)")
    con.close()

    inside_dir = allowed_dir / "spill_inside"
    inside_dir.mkdir()

    outside_dir = tmp_path / "spill_outside"
    outside_dir.mkdir()

    # 1. Validation in harden_connection: inside allowed_directories raises
    con_test = duckdb.connect(str(db_file), read_only=True)
    with pytest.raises(ValueError, match="must not be inside allowed directory"):
        harden_connection(con_test, allowed_directories=[allowed_str], temp_directory=str(inside_dir))
    con_test.close()

    # 2. Validation in harden_connection: relative path raises
    con_test = duckdb.connect(str(db_file), read_only=True)
    with pytest.raises(ValueError, match="must be an absolute path"):
        harden_connection(con_test, allowed_directories=[allowed_str], temp_directory="relative/path")
    con_test.close()

    # 3. Validation in harden_connection: non-existent path raises
    con_test = duckdb.connect(str(db_file), read_only=True)
    with pytest.raises(ValueError, match="does not exist"):
        harden_connection(con_test, allowed_directories=[allowed_str], temp_directory=str(tmp_path / "non_existent"))
    con_test.close()

    # 4. When temp_directory is unset, a query needing spill fails with resource_exhausted style error
    cfg_unset = DuckDBConnectionConfig(
        path=str(db_file),
        allowed_directories=(allowed_str,),
        memory_limit="10MB",
        temp_directory=None,
    )
    adapter = DuckDBAdapter(config=cfg_unset)
    spill_prep = _make_prepared(
        sql="SELECT count(*) FROM (SELECT range, repeat(range::varchar, 1000) FROM range(100000) ORDER BY 2)",
        audit_id="audit-spill",
    )
    with pytest.raises(EngineError) as exc_info:
        adapter.execute(spill_prep, timeout_seconds=10.0)

    # Must be a clear resource_exhausted style error rather than internal error
    assert "resource_exhausted" in str(exc_info.value).lower()
    # Must NOT pass DuckDB's raw text suggesting SET temp_directory
    assert "SET temp_directory" not in str(exc_info.value)


def test_duckdb_50_swaps_under_concurrency(tmp_path):
    """50 swaps back and forth between two files under 8 threads with zero failed queries."""
    file_a = tmp_path / "a.duckdb"
    con_a = duckdb.connect(str(file_a))
    con_a.execute("CREATE TABLE ver (val VARCHAR)")
    con_a.execute("INSERT INTO ver VALUES ('A')")
    con_a.close()

    file_b = tmp_path / "b.duckdb"
    con_b = duckdb.connect(str(file_b))
    con_b.execute("CREATE TABLE ver (val VARCHAR)")
    con_b.execute("INSERT INTO ver VALUES ('B')")
    con_b.close()

    link_path = tmp_path / "active.duckdb"
    os.symlink(str(file_a), str(link_path))

    cfg = DuckDBConnectionConfig(
        path=str(link_path),
        allowed_directories=(str(tmp_path) + "/",),
    )
    adapter = DuckDBAdapter(config=cfg)

    stop_workers = threading.Event()
    worker_errors = []
    queries_run = [0]
    queries_lock = threading.Lock()

    def query_worker(w_id: int):
        prep = _make_prepared(sql="SELECT val FROM ver", audit_id=f"audit-swap-{w_id}")
        while not stop_workers.is_set():
            try:
                res = adapter.execute(prep, timeout_seconds=5.0)
                assert res.rows in ([("A",)], [("B",)])
                with queries_lock:
                    queries_run[0] += 1
            except Exception as e:
                worker_errors.append(e)
                break
            time.sleep(0.001)

    threads = [threading.Thread(target=query_worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()

    # Perform 50 swaps back and forth
    current_target = file_a
    for i in range(50):
        next_target = file_b if current_target == file_a else file_a
        tmp_link = tmp_path / f"tmp_swap_{i}"
        os.symlink(str(next_target), str(tmp_link))
        os.replace(str(tmp_link), str(link_path))
        current_target = next_target
        time.sleep(0.01)

    stop_workers.set()
    for t in threads:
        t.join()

    assert not worker_errors, f"Failed queries during 50 swaps: {worker_errors}"
    assert queries_run[0] > 50, f"Expected >50 queries, ran {queries_run[0]}"


def test_duckdb_holder_closed_state(tmp_path):
    """After close_all() the holder is closed and subsequent query fails cleanly with EngineError."""
    db_file = tmp_path / "closed.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE t (id INT)")
    con.close()

    cfg = DuckDBConnectionConfig(path=str(db_file), allowed_directories=(str(tmp_path) + "/",))
    adapter = DuckDBAdapter(config=cfg)

    # First query succeeds
    res = adapter.execute(_make_prepared(sql="SELECT 1"), timeout_seconds=5.0)
    assert res.rows == [(1,)]

    # Close holder
    DuckDBHolder.get_holder().close_all()

    # Query after close fails cleanly with EngineError (502 class)
    with pytest.raises(EngineError) as exc_info:
        adapter.execute(_make_prepared(sql="SELECT 1"), timeout_seconds=5.0)
    assert "DuckDBHolder is closed" in str(exc_info.value)


def test_duckdb_refuse_different_parameters_on_same_file(tmp_path):
    """Holder refuses opening the same file with different hardening parameters."""
    db_file = tmp_path / "params.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE t (id INT)")
    con.close()

    holder = DuckDBHolder.get_holder()
    cur1 = holder.get_cursor(str(db_file), memory_limit="256MB", threads=2)
    cur1.close()

    # Attempt to open same file with different threads parameter
    with pytest.raises(ValueError, match="already open with different hardening parameters"):
        holder.get_cursor(str(db_file), memory_limit="256MB", threads=4)


def test_duckdb_1000_swaps_fd_lifecycle(tmp_path):
    """After 1000 swaps only the current instance remains open in the holder."""
    file_a = tmp_path / "swap_a.duckdb"
    con_a = duckdb.connect(str(file_a))
    con_a.execute("CREATE TABLE t (id INT)")
    con_a.close()

    file_b = tmp_path / "swap_b.duckdb"
    con_b = duckdb.connect(str(file_b))
    con_b.execute("CREATE TABLE t (id INT)")
    con_b.close()

    link_path = tmp_path / "swap_active.duckdb"
    os.symlink(str(file_a), str(link_path))

    cfg = DuckDBConnectionConfig(
        path=str(link_path),
        allowed_directories=(str(tmp_path) + "/",),
    )
    adapter = DuckDBAdapter(config=cfg)

    current_target = file_a
    for i in range(1000):
        next_target = file_b if current_target == file_a else file_a
        tmp_link = tmp_path / f"tmp_swap1000_{i}"
        os.symlink(str(next_target), str(tmp_link))
        os.replace(str(tmp_link), str(link_path))
        current_target = next_target
        res = adapter.execute(_make_prepared(sql="SELECT 1"), timeout_seconds=5.0)
        assert res.rows == [(1,)]

    holder = DuckDBHolder.get_holder()
    open_instances = [inst for inst in holder._instances_by_dev_ino.values() if not inst.is_closed]
    # Exactly ONE instance is open (the current one)
    assert len(open_instances) == 1
    assert not open_instances[0].is_retired


def test_duckdb_configured_temp_dir_oom_error_message(tmp_path):
    """When temp_directory is configured and OOM occurs, message remediation is accurate."""
    temp_dir = tmp_path / "spill_tmp"
    temp_dir.mkdir()
    allowed_dir = tmp_path / "data"
    allowed_dir.mkdir()

    db_file = allowed_dir / "oom.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE t AS SELECT range AS x FROM range(100)")
    con.close()

    cfg = DuckDBConnectionConfig(
        path=str(db_file),
        allowed_directories=(str(allowed_dir) + "/",),
        memory_limit="10MB",
        temp_directory=str(temp_dir),
        max_temp_directory_size="1KB",
    )
    adapter = DuckDBAdapter(config=cfg)
    spill_prep = _make_prepared(
        sql="SELECT count(*) FROM (SELECT range, repeat(range::varchar, 1000) FROM range(100000) ORDER BY 2)",
        audit_id="audit-spill-cfg",
    )
    with pytest.raises(EngineError) as exc_info:
        adapter.execute(spill_prep, timeout_seconds=10.0)

    err = str(exc_info.value)
    assert "increase memory_limit or max_temp_directory_size" in err
    assert "temp_directory is not configured" not in err


def test_duckdb_missing_target_redacts_path(tmp_path):
    """Missing target errors do not expose the full filesystem path."""
    cfg = DuckDBConnectionConfig(
        path=str(tmp_path / "missing" / "secret_folder" / "secret.duckdb"),
    )
    adapter = DuckDBAdapter(config=cfg)
    with pytest.raises(EngineError) as exc_info:
        adapter.execute(_make_prepared(sql="SELECT 1"), timeout_seconds=5.0)

    err = str(exc_info.value)
    assert "secret_folder" not in err
    assert "secret.duckdb" not in err
    assert "DuckDB database file not found" in err


def test_reconciled_parallel_overlap_up_to_bound_and_queue_beyond(tmp_path):
    """Concurrent queries overlap in parallel up to max_concurrent_cursors and queue beyond it, with no leaked cursors/slots/timers."""
    db_file = tmp_path / "overlap.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE items (id INT)")
    con.execute("INSERT INTO items VALUES (1), (2)")
    con.close()

    holder = DuckDBHolder()
    cfg = DuckDBConnectionConfig(
        path=str(db_file),
        allowed_directories=(str(tmp_path) + "/",),
        max_concurrent_cursors=2,
    )
    adapter = DuckDBAdapter(config=cfg, holder=holder)

    barrier_two = threading.Barrier(2)
    release_first_two = threading.Event()
    third_acquired = threading.Event()
    active_in_exec = 0
    max_active_in_exec = 0
    state_lock = threading.Lock()

    orig_get_cursor = holder.get_cursor

    def wrapped_get_cursor(*args, **kwargs):
        raw_cur = orig_get_cursor(*args, **kwargs)

        class OverlapCursor:
            def execute(self, sql, params=()):
                nonlocal active_in_exec, max_active_in_exec
                with state_lock:
                    active_in_exec += 1
                    if active_in_exec > max_active_in_exec:
                        max_active_in_exec = active_in_exec
                    curr = active_in_exec
                try:
                    if "/*slot_hold*/" in sql:
                        barrier_two.wait(timeout=3.0)
                        release_first_two.wait(timeout=5.0)
                    elif "/*slot_queued*/" in sql:
                        third_acquired.set()
                    return raw_cur.execute(sql, params)
                finally:
                    with state_lock:
                        active_in_exec -= 1

            @property
            def description(self):
                return raw_cur.description

            def fetchmany(self, size):
                return raw_cur.fetchmany(size)

            def commit(self):
                if hasattr(raw_cur, "commit"):
                    raw_cur.commit()

            def interrupt(self):
                raw_cur.interrupt()

            def close(self):
                raw_cur.close()

        return OverlapCursor()

    holder.get_cursor = wrapped_get_cursor  # type: ignore[method-assign]
    baseline_timers = set(_active_timer_threads())

    results: list[QueryResult] = []
    errors: list[BaseException] = []

    def run_hold(i: int):
        try:
            r = adapter.execute(
                _make_prepared(sql="SELECT id FROM items /*slot_hold*/ ORDER BY id", audit_id=f"hold-{i}"),
                timeout_seconds=5.0,
            )
            with state_lock:
                results.append(r)
        except BaseException as exc:
            with state_lock:
                errors.append(exc)

    def run_third():
        try:
            r = adapter.execute(
                _make_prepared(sql="SELECT id FROM items /*slot_queued*/ ORDER BY id", audit_id="queued-3"),
                timeout_seconds=5.0,
            )
            with state_lock:
                results.append(r)
        except BaseException as exc:
            with state_lock:
                errors.append(exc)

    t1 = threading.Thread(target=run_hold, args=(1,))
    t2 = threading.Thread(target=run_hold, args=(2,))
    t1.start()
    t2.start()

    # Wait until both concurrent queries are simultaneously inside execute() (proving parallel overlap)
    for _ in range(100):
        with state_lock:
            if max_active_in_exec == 2:
                break
        time.sleep(0.01)
    assert max_active_in_exec == 2

    # Start 3rd query, which must queue because bound=2 is saturated
    t3 = threading.Thread(target=run_third)
    t3.start()
    time.sleep(0.05)
    assert not third_acquired.is_set()

    # Release first two queries; 3rd acquires slot and completes
    release_first_two.set()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)
    t3.join(timeout=5.0)

    assert errors == []
    assert len(results) == 3
    assert third_acquired.is_set()
    assert max_active_in_exec == 2

    # Zero leaked cursors, slots, or timers
    assert adapter._active_slots == 0
    for inst in holder._instances_by_dev_ino.values():
        assert inst.in_flight == 0
    assert set(_active_timer_threads()) - baseline_timers == set()


def test_reconciled_holder_queued_cancel_and_queue_deadline_budget(tmp_path):
    """On holder path: queued cancel never touches DuckDB, and queue wait counts against total timeout deadline."""
    db_file = tmp_path / "holder_queue.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE items (id INT)")
    con.execute("INSERT INTO items VALUES (1)")
    con.close()

    holder = DuckDBHolder()
    cfg = DuckDBConnectionConfig(
        path=str(db_file),
        allowed_directories=(str(tmp_path) + "/",),
        max_concurrent_cursors=1,
    )
    adapter = DuckDBAdapter(config=cfg, holder=holder)

    q1_in_exec = threading.Event()
    q1_release = threading.Event()
    cursor_calls = 0
    call_lock = threading.Lock()
    orig_get_cursor = holder.get_cursor

    def counting_get_cursor(*args, **kwargs):
        nonlocal cursor_calls
        with call_lock:
            cursor_calls += 1
            c_idx = cursor_calls
        raw_cur = orig_get_cursor(*args, **kwargs)

        class WrappedCur:
            def execute(self, sql, params=()):
                if c_idx == 1:
                    q1_in_exec.set()
                    q1_release.wait(timeout=5.0)
                return raw_cur.execute(sql, params)

            @property
            def description(self):
                return raw_cur.description

            def fetchmany(self, size):
                return raw_cur.fetchmany(size)

            def interrupt(self):
                raw_cur.interrupt()

            def close(self):
                raw_cur.close()

        return WrappedCur()

    holder.get_cursor = counting_get_cursor  # type: ignore[method-assign]
    baseline_timers = set(_active_timer_threads())

    # 1. Start Q1 to hold the single cursor slot
    t1 = threading.Thread(
        target=lambda: adapter.execute(_make_prepared(sql="SELECT id FROM items", audit_id="q1"), timeout_seconds=5.0)
    )
    t1.start()
    assert q1_in_exec.wait(timeout=2.0)
    assert cursor_calls == 1

    # 2. Q2 queues and is cancelled before acquiring a slot -> never calls holder.get_cursor
    q2_cancel_fn: list = []
    q2_registered = threading.Event()
    q2_err: list[BaseException] = []

    def on_cancel_q2(fn):
        q2_cancel_fn.append(fn)
        q2_registered.set()

    def run_q2():
        try:
            adapter.execute(
                _make_prepared(sql="SELECT id FROM items", audit_id="q2"),
                timeout_seconds=5.0,
                on_cancel_handle=on_cancel_q2,
            )
        except BaseException as exc:
            q2_err.append(exc)

    t2 = threading.Thread(target=run_q2)
    t2.start()
    assert q2_registered.wait(timeout=2.0)
    time.sleep(0.02)
    q2_cancel_fn[0]()
    t2.join(timeout=2.0)
    assert len(q2_err) == 1
    assert isinstance(q2_err[0], EngineCancelled)
    assert cursor_calls == 1  # Queued cancel never touched DuckDBHolder!

    # 3. Q3 waits in queue for ~0.10s and then runs a slow query with total timeout 0.16s -> queue wait counts against deadline
    def release_q1_after_delay():
        time.sleep(0.10)
        q1_release.set()

    threading.Thread(target=release_q1_after_delay).start()
    slow_prep = _make_prepared(
        sql="SELECT count(*) FROM range(100000000) t1, range(1000) t2",
        audit_id="q3-budget",
    )
    t0 = time.perf_counter()
    with pytest.raises(EngineTimeout):
        adapter.execute(slow_prep, timeout_seconds=0.16)
    elapsed = time.perf_counter() - t0
    t1.join(timeout=5.0)
    assert elapsed < 0.28

    # Zero leaked cursors, slots, or timers
    assert adapter._active_slots == 0
    for inst in holder._instances_by_dev_ino.values():
        assert inst.in_flight == 0
    assert set(_active_timer_threads()) - baseline_timers == set()


def test_reconciled_cancel_isolation_between_two_concurrent_queries(tmp_path):
    """Cancelling one concurrent query on a shared DuckDBHolder does not affect another concurrent query."""
    db_file = tmp_path / "cancel_iso.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE t (n INT)")
    con.execute("INSERT INTO t VALUES (77)")
    con.close()

    holder = DuckDBHolder()
    cfg = DuckDBConnectionConfig(
        path=str(db_file),
        allowed_directories=(str(tmp_path) + "/",),
        max_concurrent_cursors=4,
    )
    adapter = DuckDBAdapter(config=cfg, holder=holder)
    baseline_timers = set(_active_timer_threads())

    slow_prep = _make_prepared(
        sql="WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM r WHERE n < 50000000) SELECT count(*) FROM r CROSS JOIN (SELECT 1 UNION ALL SELECT 2)",
        audit_id="audit-cancel-slow",
    )
    peer_prep = _make_prepared(sql="SELECT n FROM t", audit_id="audit-peer")

    slow_outcomes: list[BaseException] = []
    peer_outcomes: list[QueryResult] = []
    cancel_fn_box: list = []
    cancel_registered = threading.Event()

    def on_cancel_slow(fn):
        cancel_fn_box.append(fn)
        cancel_registered.set()

    def run_slow():
        try:
            adapter.execute(slow_prep, timeout_seconds=10.0, on_cancel_handle=on_cancel_slow)
        except BaseException as exc:
            slow_outcomes.append(exc)

    t_slow = threading.Thread(target=run_slow)
    t_slow.start()
    assert cancel_registered.wait(timeout=2.0)
    time.sleep(0.05)

    # Trigger cancel on slow query while concurrently running peer query
    def trigger_cancel():
        time.sleep(0.02)
        cancel_fn_box[0]()

    t_cancel = threading.Thread(target=trigger_cancel)
    t_peer = threading.Thread(
        target=lambda: peer_outcomes.append(adapter.execute(peer_prep, timeout_seconds=5.0))
    )
    t_cancel.start()
    t_peer.start()

    t_slow.join(timeout=5.0)
    t_cancel.join(timeout=5.0)
    t_peer.join(timeout=5.0)

    assert len(slow_outcomes) == 1
    assert isinstance(slow_outcomes[0], EngineCancelled)
    assert len(peer_outcomes) == 1
    assert peer_outcomes[0].rows == [(77,)]

    # Zero leaked cursors, slots, or timers
    assert adapter._active_slots == 0
    for inst in holder._instances_by_dev_ino.values():
        assert inst.in_flight == 0
    assert set(_active_timer_threads()) - baseline_timers == set()


def test_reconciled_holder_idle_reaper_closes_idle_generation(tmp_path):
    """DuckDBHolder idle reaper closes idle database instances after max_idle_seconds."""
    db_file = tmp_path / "idle_reap.duckdb"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE t (n INT)")
    con.execute("INSERT INTO t VALUES (1)")
    con.close()

    holder = DuckDBHolder(max_idle_seconds=0.1)
    try:
        cfg = DuckDBConnectionConfig(
            path=str(db_file),
            allowed_directories=(str(tmp_path) + "/",),
        )
        adapter = DuckDBAdapter(config=cfg, holder=holder)
        res = adapter.execute(_make_prepared(sql="SELECT n FROM t"), timeout_seconds=5.0)
        assert res.rows == [(1,)]
        assert len(holder._instances_by_dev_ino) == 1

        # Wait for idle reaper to close the idle instance
        time.sleep(0.3)
        assert len(holder._instances_by_dev_ino) == 0

        # Subsequent query transparently reopens and succeeds
        res2 = adapter.execute(_make_prepared(sql="SELECT n FROM t"), timeout_seconds=5.0)
        assert res2.rows == [(1,)]
    finally:
        holder.close_all()
