"""Focused regression and lifecycle tests for owned DuckDB connections and serialized execution."""

from __future__ import annotations

import hashlib
import logging
import threading
import time
from collections.abc import Callable
from typing import Any, cast

import duckdb
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
from starlette.testclient import TestClient

from seeknal.serving import (
    DuckDBAdapter,
    EngineCancelled,
    EngineError,
    EngineTimeout,
    Placement,
    ServerConfig,
    ServingPipeline,
    build_server,
    create_app,
    sign_cancel_token,
    sign_grant_token,
)
from seeknal.serving.duckdb_policy import DuckDBHolder, connect_hardened, probe
from seeknal.serving.service import PreparedQuery


@pytest.fixture(autouse=True)
def _reset_duckdb_holder():
    DuckDBHolder.reset()
    yield
    DuckDBHolder.reset()


def _make_prepared(
    *,
    sql: str = "SELECT id, name FROM users ORDER BY id",
    params: tuple = (),
    paramstyle: str = "qmark",
    row_limit: int | None = 10,
    audit_id: str = "audit-lifecycle-01",
    subject: str = "analyst@example.com",
) -> PreparedQuery:
    return PreparedQuery(
        engine="duck",
        engine_kind="duckdb",
        sql=sql,
        params=params,
        paramstyle=paramstyle,
        rule="small_scan",
        reason="lifecycle test",
        row_limit=row_limit,
        masked_columns=(),
        redacted_sql="SELECT id, name FROM users ORDER BY id",
        subject=subject,
        workspace="ws-lifecycle",
        audit_id=audit_id,
        cube_security_context=None,
    )


def _make_grant_claims(
    sql: str,
    *,
    jti: str,
    sub: str = "analyst@example.com",
    max_rows: int = 100,
) -> dict[str, Any]:
    now = time.time()
    return {
        "typ": "grant",
        "sub": sub,
        "ws": "ws-lifecycle",
        "jti": jti,
        "iss": "atlas-catalog",
        "aud": "seeknal-serving",
        "iat": now,
        "exp": now + 60.0,
        "sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "max_rows": max_rows,
        "tables": {
            "users": {
                "columns": ["id", "name"],
                "masks": {},
                "row_filters": [],
                "actions": ["select"],
            }
        },
    }


def _active_timer_threads() -> list[threading.Thread]:
    return [
        t
        for t in threading.enumerate()
        if isinstance(t, threading.Timer) and t.is_alive()
    ]


@pytest.fixture
def hardened_duckdb_setup(tmp_path):
    """Create a real on-disk DuckDB file and static server config with locked allowed_directories."""
    allowed_dir = tmp_path / "allowed"
    allowed_dir.mkdir()
    db_file = allowed_dir / "serving.duckdb"

    con = duckdb.connect(str(db_file))
    con.execute("CREATE TABLE users (id INT, name VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
    con.close()

    pl_file = tmp_path / "placement.yaml"
    pl_file.write_text(
        "engines:\n"
        "  duck:\n"
        "    kind: duckdb\n"
        "    dialect: duckdb\n"
        "tables:\n"
        "  users:\n"
        "    keys: [id]\n"
        "    row_count: 2\n"
        "    serve:\n"
        "      duck: users\n"
        "cubes: {}\n",
        encoding="utf-8",
    )
    sc_file = tmp_path / "schemas.json"
    sc_file.write_text('{"users": ["id", "name"]}', encoding="utf-8")

    priv = ec.generate_private_key(ec.SECP256R1())
    pub_pem = (
        priv.public_key()
        .public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode("ascii")
    )
    pub_file = tmp_path / "pub.pem"
    pub_file.write_text(pub_pem, encoding="utf-8")

    allowed_dir_str = str(allowed_dir) + "/"
    cfg_dict = {
        "grant_keys": {"pem_files": {"key-1": str(pub_file)}},
        "placement": {
            "static": {
                "placement_file": str(pl_file),
                "schemas_file": str(sc_file),
            }
        },
        "connections": {
            "duck": {
                "kind": "duckdb",
                "path": str(db_file),
                "allowed_directories": [allowed_dir_str],
                "memory_limit": "256MB",
                "threads": 2,
            }
        },
    }
    return {
        "db_file": str(db_file),
        "allowed_dir": allowed_dir_str,
        "cfg_dict": cfg_dict,
        "priv": priv,
        "pub": priv.public_key(),
    }


def test_retained_connection_reference_sequential_hardened_file_reproducer(
    hardened_duckdb_setup,
):
    """Sequential queries on a real hardened DuckDB file succeed: custom factory connections close per query, and default build_server shares a hardened DuckDBHolder instance with per-query cursors."""
    db_file = hardened_duckdb_setup["db_file"]
    allowed_dir = hardened_duckdb_setup["allowed_dir"]
    cfg_dict = hardened_duckdb_setup["cfg_dict"]
    priv = hardened_duckdb_setup["priv"]

    retained_conns: list[duckdb.DuckDBPyConnection] = []

    def tracking_connect() -> duckdb.DuckDBPyConnection:
        c = connect_hardened(
            db_file,
            read_only=True,
            allowed_directories=[allowed_dir],
            memory_limit="256MB",
            threads=2,
        )
        retained_conns.append(c)
        return c

    handle = build_server(
        ServerConfig.from_dict(cfg_dict),
        connect_factories={"duck": tracking_connect},
        cursor_key="test-duckdb-lifecycle-cursor-key",
    )
    client = TestClient(handle.app)

    sql = "SELECT id, name FROM users ORDER BY id"
    for i in range(4):
        tok = sign_grant_token(
            _make_grant_claims(sql, jti=f"jti-seq-{i}"),
            priv,
            kid="key-1",
        )
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok}"},
            json={"sql": sql},
        )
        assert resp.status_code == 200
        assert resp.json()["rows"] == [[1, "Alice"], [2, "Bob"]]

        # Retained connection must already be closed deterministically without GC
        assert len(retained_conns) == i + 1
        with pytest.raises(duckdb.ConnectionException):
            retained_conns[-1].execute("SELECT 1")

    # Also verify default build_server (holder path) across sequential queries: reuses 1 hardened instance and leaves 0 in-flight cursors
    default_handle = build_server(
        ServerConfig.from_dict(cfg_dict),
        cursor_key="test-duckdb-lifecycle-cursor-key",
    )
    default_client = TestClient(default_handle.app)
    for i in range(3):
        tok = sign_grant_token(
            _make_grant_claims(sql, jti=f"jti-default-seq-{i}"),
            priv,
            kid="key-1",
        )
        resp = default_client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok}"},
            json={"sql": sql},
        )
        assert resp.status_code == 200
        assert resp.json()["rows"] == [[1, "Alice"], [2, "Bob"]]

    # Security hardening remains intact and all per-query cursors were closed
    duck_adapter = default_handle.adapters["duck"]
    assert isinstance(duck_adapter, DuckDBAdapter)
    holder = DuckDBHolder.get_holder()
    assert len(holder._instances_by_dev_ino) == 1
    inst = next(iter(holder._instances_by_dev_ino.values()))
    assert inst.in_flight == 0
    probe_conn = duck_adapter._connect()
    try:
        assert inst.in_flight == 1
        probe_res = probe(probe_conn)
        assert all(probe_res.values())
    finally:
        probe_conn.close()
    assert inst.in_flight == 0


def test_two_concurrent_callers_barrier_real_hardened_readonly_file(
    hardened_duckdb_setup,
):
    """Two concurrent callers synchronize via barrier and both succeed in parallel against a real hardened file on the shared DuckDBHolder."""
    cfg_dict = hardened_duckdb_setup["cfg_dict"]
    priv = hardened_duckdb_setup["priv"]

    handle = build_server(
        ServerConfig.from_dict(cfg_dict),
        cursor_key="test-duckdb-lifecycle-cursor-key",
    )
    client = TestClient(handle.app)
    duck_adapter = handle.adapters["duck"]
    assert isinstance(duck_adapter, DuckDBAdapter)
    holder = DuckDBHolder.get_holder()

    state_lock = threading.Lock()
    active_count = 0
    max_active_count = 0
    opened_cursors = 0
    closed_count = 0
    in_exec_barrier = threading.Barrier(2)

    orig_get_cursor = holder.get_cursor

    def tracking_get_cursor(*args: Any, **kwargs: Any) -> Any:
        nonlocal active_count, max_active_count, opened_cursors
        raw_cur = orig_get_cursor(*args, **kwargs)
        with state_lock:
            opened_cursors += 1
            active_count += 1
            if active_count > max_active_count:
                max_active_count = active_count

        class TrackedCursor:
            def __init__(self) -> None:
                self.closed = False

            def execute(self, *e_args: Any, **e_kwargs: Any) -> Any:
                in_exec_barrier.wait(timeout=5.0)
                return raw_cur.execute(*e_args, **e_kwargs)

            @property
            def description(self) -> Any:
                return raw_cur.description

            def fetchmany(self, size: int) -> Any:
                return raw_cur.fetchmany(size)

            def commit(self) -> None:
                if hasattr(raw_cur, "commit"):
                    raw_cur.commit()

            def interrupt(self) -> None:
                raw_cur.interrupt()

            def close(self) -> None:
                nonlocal active_count, closed_count
                with state_lock:
                    if not self.closed:
                        self.closed = True
                        active_count -= 1
                        closed_count += 1
                raw_cur.close()

        return TrackedCursor()

    holder.get_cursor = tracking_get_cursor  # type: ignore[method-assign]

    barrier = threading.Barrier(2)
    sql = "SELECT id, name FROM users ORDER BY id"
    results: list[tuple[int, dict[str, Any]]] = []
    errors: list[BaseException] = []

    def worker(idx: int) -> None:
        tok = sign_grant_token(
            _make_grant_claims(sql, jti=f"jti-concurrent-{idx}"),
            priv,
            kid="key-1",
        )
        try:
            barrier.wait(timeout=5.0)
            resp = client.post(
                "/v1/query",
                headers={"Authorization": f"Bearer {tok}"},
                json={"sql": sql},
            )
            with state_lock:
                results.append((resp.status_code, resp.json()))
        except BaseException as exc:
            with state_lock:
                errors.append(exc)

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    t1.start()
    t2.start()
    t1.join(timeout=10.0)
    t2.join(timeout=10.0)

    assert errors == []
    assert len(results) == 2
    for status, body in results:
        assert status == 200
        assert body["rows"] == [[1, "Alice"], [2, "Bob"]]

    # Both concurrent queries executed simultaneously on the single shared hardened holder instance
    assert max_active_count == 2
    assert active_count == 0
    assert opened_cursors == 2
    assert closed_count == 2
    assert duck_adapter._active_slots == 0
    assert len(holder._instances_by_dev_ino) == 1
    inst = next(iter(holder._instances_by_dev_ino.values()))
    assert inst.in_flight == 0


def test_factory_error_maps_to_secret_safe_engine_error_and_http_502(
    hardened_duckdb_setup, caplog
):
    """Connection-factory exceptions map to EngineError('Engine connection failed') and HTTP 502 without leaking sentinels."""
    cfg_dict = hardened_duckdb_setup["cfg_dict"]
    priv = hardened_duckdb_setup["priv"]
    sentinel = "SECRET_FACTORY_SENTINEL_DSN_987654"

    def failing_factory():
        raise RuntimeError(f"failed opening duckdb with {sentinel}")

    adapter = DuckDBAdapter(failing_factory)
    prep = _make_prepared()

    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        with pytest.raises(EngineError) as exc_info:
            adapter.execute(prep, timeout_seconds=5.0)

    assert str(exc_info.value) == "Engine connection failed"
    assert exc_info.value.__cause__ is None
    assert sentinel not in str(exc_info.value)
    assert sentinel not in caplog.text

    # Verify HTTP /v1/query maps factory failure to 502 engine_error (not 500)
    handle = build_server(
        ServerConfig.from_dict(cfg_dict),
        connect_factories={"duck": failing_factory},
        cursor_key="test-duckdb-lifecycle-cursor-key",
    )
    client = TestClient(handle.app)
    sql = "SELECT id, name FROM users ORDER BY id"
    tok = sign_grant_token(
        _make_grant_claims(sql, jti="jti-factory-fail-http"),
        priv,
        kid="key-1",
    )

    with caplog.at_level(logging.DEBUG):
        caplog.clear()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok}"},
            json={"sql": sql},
        )

    assert resp.status_code == 502
    assert resp.json() == {
        "error": "engine_error",
        "code": "engine_unavailable",
        "message": "Engine connection failed",
        "next_step": "Retry shortly or contact the platform operator if the outage persists.",
        "audit_id": "jti-factory-fail-http",
    }
    assert sentinel not in resp.text
    assert sentinel not in caplog.text


def test_resource_cleanup_across_all_execution_paths(monkeypatch, caplog):
    """Verify connection close, watchdog join, and lock release on success, error, timeout, cancel, callback, timer-start, and close failures."""
    parent = duckdb.connect()
    parent.execute("CREATE TABLE users (id INT, name VARCHAR)")
    parent.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    events: list[str] = []
    created_wrappers: list[Any] = []
    unclosed_raw_cursors: list[duckdb.DuckDBPyConnection] = []

    class InstrumentedConn:
        def __init__(
            self,
            cur: duckdb.DuckDBPyConnection,
            *,
            fail_execute: Exception | None = None,
            fail_close: Exception | None = None,
            delay_connect_s: float = 0.0,
        ) -> None:
            self._cur = cur
            self._fail_execute = fail_execute
            self._fail_close = fail_close
            self.closed = False
            self.close_attempted = False
            self.interrupted = False
            if delay_connect_s > 0:
                time.sleep(delay_connect_s)

        def execute(self, sql: str, params: tuple = ()) -> Any:
            events.append("execute")
            if self._fail_execute is not None:
                raise self._fail_execute
            return self._cur.execute(sql, params)

        @property
        def description(self) -> Any:
            return self._cur.description

        def fetchmany(self, size: int) -> Any:
            return self._cur.fetchmany(size)

        def commit(self) -> None:
            self._cur.commit()

        def interrupt(self) -> None:
            self.interrupted = True
            events.append("interrupt")
            self._cur.interrupt()

        def close(self) -> None:
            self.close_attempted = True
            events.append("close")
            if self._fail_close is not None:
                unclosed_raw_cursors.append(self._cur)
                raise self._fail_close
            self.closed = True
            self._cur.close()

    next_fail_execute: Exception | None = None
    next_fail_close: Exception | None = None
    next_delay_connect_s: float = 0.0

    def factory() -> InstrumentedConn:
        events.append("connect")
        w = InstrumentedConn(
            parent.cursor(),
            fail_execute=next_fail_execute,
            fail_close=next_fail_close,
            delay_connect_s=next_delay_connect_s,
        )
        created_wrappers.append(w)
        return w

    adapter = DuckDBAdapter(
        cast(Callable[[], duckdb.DuckDBPyConnection], factory)
    )
    baseline_timers = set(_active_timer_threads())

    try:
        # 1. Success path
        events.clear()
        res = adapter.execute(_make_prepared(), timeout_seconds=5.0)
        assert res.rows == [(1, "Alice"), (2, "Bob")]
        assert created_wrappers[-1].closed is True
        assert events == ["connect", "execute", "close"]
        assert set(_active_timer_threads()) - baseline_timers == set()

        # 2. SQL execution error path
        events.clear()
        next_fail_execute = RuntimeError("SQL_EXEC_SECRET_SENTINEL_444")
        try:
            with caplog.at_level(logging.DEBUG):
                caplog.clear()
                with pytest.raises(EngineError) as exc_info:
                    adapter.execute(_make_prepared(), timeout_seconds=5.0)
            assert "SQL_EXEC_SECRET_SENTINEL_444" not in str(exc_info.value)
            assert "SQL_EXEC_SECRET_SENTINEL_444" not in caplog.text
            assert exc_info.value.__cause__ is None
            assert created_wrappers[-1].closed is True
            assert events == ["connect", "execute", "close"]
            assert set(_active_timer_threads()) - baseline_timers == set()
        finally:
            next_fail_execute = None

        # 3. Callback registration error path: does not call factory, leak lock, or map to EngineError
        events.clear()

        class CustomCallbackError(Exception):
            pass

        def bad_on_cancel(_cancel_fn: Callable[[], None]) -> None:
            raise CustomCallbackError("callback registration failed")

        with pytest.raises(CustomCallbackError):
            adapter.execute(
                _make_prepared(),
                timeout_seconds=5.0,
                on_cancel_handle=bad_on_cancel,
            )
        assert events == []

        # 4. Timeout path: watchdog fires, interrupts, joins before close
        events.clear()
        slow_prep = _make_prepared(
            sql="SELECT count(*) FROM range(100000000) t1, range(1000) t2"
        )
        with pytest.raises(EngineTimeout):
            adapter.execute(slow_prep, timeout_seconds=0.1)
        assert created_wrappers[-1].closed is True
        assert created_wrappers[-1].interrupted is True
        assert events.index("interrupt") < events.index("close")
        assert set(_active_timer_threads()) - baseline_timers == set()

        # 5. Active cancel path: cancel handle interrupts active query, closes connection, does not affect next query
        events.clear()
        captured_cancel: list[Callable[[], None]] = []
        with pytest.raises(EngineCancelled):

            def trigger_cancel_soon(cancel_fn: Callable[[], None]) -> None:
                captured_cancel.append(cancel_fn)
                t = threading.Timer(0.08, cancel_fn)
                t.daemon = True
                t.start()

            adapter.execute(
                slow_prep,
                timeout_seconds=10.0,
                on_cancel_handle=trigger_cancel_soon,
            )
        assert created_wrappers[-1].closed is True
        assert set(_active_timer_threads()) - baseline_timers == set()

        # Calling the stale cancel callback after cleanup must not affect a subsequent request
        assert len(captured_cancel) == 1
        captured_cancel[0]()
        res_after_cancel = adapter.execute(_make_prepared(), timeout_seconds=5.0)
        assert res_after_cancel.rows == [(1, "Alice"), (2, "Bob")]

        # 6. Connect returning after deadline closes connection without executing SQL
        events.clear()
        next_delay_connect_s = 0.08
        try:
            with pytest.raises(EngineTimeout):
                adapter.execute(_make_prepared(), timeout_seconds=0.03)
            assert created_wrappers[-1].closed is True
            assert events == ["connect", "close"]
            assert set(_active_timer_threads()) - baseline_timers == set()
        finally:
            next_delay_connect_s = 0.0

        # 7a. Truly unstarted Timer.start failure preserves setup RuntimeError, closes connection, and releases slot
        events.clear()
        real_timer_start = threading.Timer.start

        def failing_timer_start(self: threading.Timer) -> None:
            raise RuntimeError("TIMER_START_SETUP_FAIL_555")

        monkeypatch.setattr(threading.Timer, "start", failing_timer_start)
        try:
            with pytest.raises(RuntimeError, match="TIMER_START_SETUP_FAIL_555"):
                adapter.execute(_make_prepared(), timeout_seconds=5.0)
            assert created_wrappers[-1].closed is True
            assert events == ["connect", "close"]
            assert adapter._active_slots == 0
        finally:
            monkeypatch.setattr(threading.Timer, "start", real_timer_start)

        # 7b. Partially started timer that fails during start still gets cancelled/joined before close and preserves RuntimeError
        events.clear()
        partial_timer_events: list[str] = []

        class PartialStartTimer:
            def __init__(self, interval: float, function: Callable[[], None]) -> None:
                self.interval = interval
                self.function = function
                self.daemon = False
                self._alive = False

            def start(self) -> None:
                self._alive = True
                partial_timer_events.append("timer_partial_start")
                raise RuntimeError("PARTIAL_TIMER_START_FAIL_556")

            def is_alive(self) -> bool:
                return self._alive

            def cancel(self) -> None:
                partial_timer_events.append("timer_cancel")

            def join(self) -> None:
                self._alive = False
                partial_timer_events.append("timer_join")

        monkeypatch.setattr(threading, "Timer", PartialStartTimer)
        try:
            with pytest.raises(RuntimeError, match="PARTIAL_TIMER_START_FAIL_556"):
                adapter.execute(_make_prepared(), timeout_seconds=5.0)
            assert partial_timer_events == [
                "timer_partial_start",
                "timer_cancel",
                "timer_join",
            ]
            assert created_wrappers[-1].closed is True
            assert events == ["connect", "close"]
            assert adapter._active_slots == 0
        finally:
            monkeypatch.undo()

        # 8. Close failure (raising BEFORE actually closing):
        # 8a. Sole close failure raises secret-safe EngineError('Engine connection cleanup failed') after releasing slot
        events.clear()
        next_fail_close = RuntimeError("CLOSE_SECRET_SENTINEL_888")
        try:
            with caplog.at_level(logging.DEBUG):
                caplog.clear()
                with pytest.raises(EngineError) as exc_info:
                    adapter.execute(_make_prepared(), timeout_seconds=5.0)
            assert str(exc_info.value) == "Engine connection cleanup failed"
            assert exc_info.value.__cause__ is None
            assert "CLOSE_SECRET_SENTINEL_888" not in str(exc_info.value)
            assert "CLOSE_SECRET_SENTINEL_888" not in caplog.text
            assert created_wrappers[-1].close_attempted is True
            assert created_wrappers[-1].closed is False
            assert adapter._active_slots == 0

            # 8b. Primary execute failure + close failure preserves primary EngineError('Engine query execution failed')
            next_fail_execute = RuntimeError("PRIMARY_EXEC_FAIL_111")
            with caplog.at_level(logging.DEBUG):
                caplog.clear()
                with pytest.raises(EngineError) as exc_info:
                    adapter.execute(_make_prepared(), timeout_seconds=5.0)
            assert str(exc_info.value) == "Engine query execution failed"
            assert exc_info.value.__cause__ is None
            assert "CLOSE_SECRET_SENTINEL_888" not in str(exc_info.value)
            assert "PRIMARY_EXEC_FAIL_111" not in str(exc_info.value)
            assert "CLOSE_SECRET_SENTINEL_888" not in caplog.text
            assert "PRIMARY_EXEC_FAIL_111" not in caplog.text
            assert adapter._active_slots == 0
        finally:
            next_fail_close = None
            next_fail_execute = None

        # Adapter remains healthy for subsequent queries
        final_res = adapter.execute(_make_prepared(), timeout_seconds=5.0)
        assert final_res.rows == [(1, "Alice"), (2, "Bob")]
    finally:
        for raw_cur in unclosed_raw_cursors:
            try:
                raw_cur.close()
            except Exception:
                pass
        parent.close()


def test_first_wins_terminal_latch_ignores_late_cancel_and_watchdog_in_timer_join(
    monkeypatch,
):
    """Late cancel or watchdog callbacks triggered during Timer.join after query outcome is latched must no-op without interrupting or reclassifying."""
    parent = duckdb.connect()
    parent.execute("CREATE TABLE users (id INT, name VARCHAR)")
    parent.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    created_conns: list[Any] = []
    fail_execute_exc: Exception | None = None
    captured_cancel_handles: list[Callable[[], None]] = []
    join_trigger_mode: str = "watchdog_then_cancel"

    class DeterministicConn:
        def __init__(self, cur: duckdb.DuckDBPyConnection) -> None:
            self._cur = cur
            self.interrupted = False
            self.closed = False

        def execute(self, sql: str, params: tuple = ()) -> Any:
            if fail_execute_exc is not None:
                raise fail_execute_exc
            return self._cur.execute(sql, params)

        @property
        def description(self) -> Any:
            return self._cur.description

        def fetchmany(self, size: int) -> Any:
            return self._cur.fetchmany(size)

        def commit(self) -> None:
            self._cur.commit()

        def interrupt(self) -> None:
            self.interrupted = True
            self._cur.interrupt()

        def close(self) -> None:
            self.closed = True
            self._cur.close()

    class JoinFiringTimer:
        def __init__(self, interval: float, function: Callable[[], None]) -> None:
            self.interval = interval
            self.function = function
            self.daemon = False
            self.started = False
            self.cancelled = False
            self.joined = False

        def start(self) -> None:
            self.started = True

        def cancel(self) -> None:
            self.cancelled = True

        def join(self) -> None:
            self.joined = True
            if join_trigger_mode == "watchdog_then_cancel":
                self.function()
                if captured_cancel_handles:
                    captured_cancel_handles[-1]()
            elif join_trigger_mode == "cancel_then_watchdog":
                if captured_cancel_handles:
                    captured_cancel_handles[-1]()
                self.function()

    def factory() -> DeterministicConn:
        c = DeterministicConn(parent.cursor())
        created_conns.append(c)
        return c

    monkeypatch.setattr(threading, "Timer", JoinFiringTimer)
    adapter = DuckDBAdapter(
        cast(Callable[[], duckdb.DuckDBPyConnection], factory)
    )

    try:
        # 1. After successful query + fetch, Timer.join triggers watchdog then cancel -> still returns success, no interrupt
        join_trigger_mode = "watchdog_then_cancel"
        res_ok1 = adapter.execute(
            _make_prepared(),
            timeout_seconds=5.0,
            on_cancel_handle=captured_cancel_handles.append,
        )
        assert res_ok1.rows == [(1, "Alice"), (2, "Bob")]
        assert created_conns[-1].interrupted is False
        assert created_conns[-1].closed is True

        # 2. After successful query + fetch, Timer.join triggers cancel then watchdog -> still returns success, no interrupt
        join_trigger_mode = "cancel_then_watchdog"
        res_ok2 = adapter.execute(
            _make_prepared(),
            timeout_seconds=5.0,
            on_cancel_handle=captured_cancel_handles.append,
        )
        assert res_ok2.rows == [(1, "Alice"), (2, "Bob")]
        assert created_conns[-1].interrupted is False
        assert created_conns[-1].closed is True

        # 3. After SQL execution error, Timer.join triggers cancel then watchdog -> remains EngineError, not reclassified as EngineCancelled/EngineTimeout
        fail_execute_exc = RuntimeError("DB_FAILURE_BEFORE_LATE_CANCEL")
        join_trigger_mode = "cancel_then_watchdog"
        with pytest.raises(EngineError) as exc_info:
            adapter.execute(
                _make_prepared(),
                timeout_seconds=5.0,
                on_cancel_handle=captured_cancel_handles.append,
            )
        assert not isinstance(exc_info.value, (EngineCancelled, EngineTimeout))
        assert str(exc_info.value) == "Engine query execution failed"
        assert created_conns[-1].interrupted is False
        assert created_conns[-1].closed is True

        # 4. After SQL execution error, Timer.join triggers watchdog then cancel -> remains EngineError, not reclassified as EngineTimeout/EngineCancelled
        join_trigger_mode = "watchdog_then_cancel"
        with pytest.raises(EngineError) as exc_info2:
            adapter.execute(
                _make_prepared(),
                timeout_seconds=5.0,
                on_cancel_handle=captured_cancel_handles.append,
            )
        assert not isinstance(exc_info2.value, (EngineCancelled, EngineTimeout))
        assert str(exc_info2.value) == "Engine query execution failed"
        assert created_conns[-1].interrupted is False
        assert created_conns[-1].closed is True
    finally:
        parent.close()


def test_queued_cancellation_and_queue_timeout_without_factory_call(
    hardened_duckdb_setup,
):
    """Queued requests that cancel or time out never call the factory or strand the lock; deadline is shared across queue + execution."""
    priv = hardened_duckdb_setup["priv"]
    pub = hardened_duckdb_setup["pub"]

    parent = duckdb.connect()
    parent.execute("CREATE TABLE users (id INT, name VARCHAR)")
    parent.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    q1_in_execute = threading.Event()
    q1_release = threading.Event()
    factory_calls = 0
    factory_lock = threading.Lock()

    class ControlledConn:
        def __init__(self, cur: duckdb.DuckDBPyConnection, call_no: int) -> None:
            self._cur = cur
            self._call_no = call_no
            self.closed = False

        def execute(self, sql: str, params: tuple = ()) -> Any:
            if self._call_no == 1:
                q1_in_execute.set()
                q1_release.wait(timeout=5.0)
            return self._cur.execute(sql, params)

        @property
        def description(self) -> Any:
            return self._cur.description

        def fetchmany(self, size: int) -> Any:
            return self._cur.fetchmany(size)

        def commit(self) -> None:
            self._cur.commit()

        def interrupt(self) -> None:
            self._cur.interrupt()

        def close(self) -> None:
            self.closed = True
            self._cur.close()

    def controlled_factory() -> ControlledConn:
        nonlocal factory_calls
        with factory_lock:
            factory_calls += 1
            call_no = factory_calls
        return ControlledConn(parent.cursor(), call_no)

    adapter = DuckDBAdapter(
        cast(Callable[[], duckdb.DuckDBPyConnection], controlled_factory)
    )
    placement = Placement.from_dict(
        {
            "engines": {"duck": {"kind": "duckdb", "dialect": "duckdb"}},
            "tables": {
                "users": {"keys": ["id"], "row_count": 2, "serve": {"duck": "users"}}
            },
            "cubes": {},
        }
    )
    pipeline = ServingPipeline(
        placement=placement,
        schemas={"users": ["id", "name"]},
        public_keys={"key-1": pub},
    )
    app = create_app(pipeline, {"duck": adapter}, timeout_seconds=10.0)
    client = TestClient(app)

    sql = "SELECT id, name FROM users ORDER BY id"

    # Start Request 1, which acquires the lock and blocks inside execute()
    r1_outcomes: list[Any] = []

    def run_r1() -> None:
        tok1 = sign_grant_token(
            _make_grant_claims(sql, jti="jti-q1"), priv, kid="key-1"
        )
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok1}"},
            json={"sql": sql},
        )
        r1_outcomes.append((resp.status_code, resp.json()))

    t1 = threading.Thread(target=run_r1)
    t1.start()
    assert q1_in_execute.wait(timeout=2.0)
    assert factory_calls == 1

    # Request 2 queues behind Request 1 and is cancelled via /v1/cancel while still queued
    r2_registered = threading.Event()
    r2_outcomes: list[Any] = []
    jti_q2 = "jti-q2-queued-cancel"
    sub_q2 = "analyst@example.com"

    def run_r2() -> None:
        tok2 = sign_grant_token(
            _make_grant_claims(sql, jti=jti_q2, sub=sub_q2), priv, kid="key-1"
        )
        r2_registered.set()
        resp = client.post(
            "/v1/query",
            headers={"Authorization": f"Bearer {tok2}"},
            json={"sql": sql},
        )
        r2_outcomes.append((resp.status_code, resp.json()))

    t2 = threading.Thread(target=run_r2)
    t2.start()
    assert r2_registered.wait(timeout=2.0)

    # Poll /v1/cancel briefly until Request 2 is registered in RunningQueries (before acquiring adapter lock)
    cancel_tok2 = sign_cancel_token(
        {
            "typ": "cancel",
            "sub": sub_q2,
            "jti": jti_q2,
            "iss": "atlas-catalog",
            "aud": "seeknal-serving",
            "iat": time.time(),
            "exp": time.time() + 30.0,
        },
        priv,
        kid="key-1",
    )
    cancel_resp = None
    for _ in range(40):
        cancel_resp = client.post(
            "/v1/cancel",
            headers={"Authorization": f"Bearer {cancel_tok2}"},
        )
        if cancel_resp.status_code == 202:
            break
        time.sleep(0.01)
    assert cancel_resp is not None and cancel_resp.status_code == 202

    # Request 2 must finish with 499 query_cancelled WHILE Request 1 is still holding the lock
    t2.join(timeout=2.0)
    assert not t2.is_alive()
    assert r2_outcomes == [
        (
            499,
            {
                "error": "query_cancelled",
                "code": "query_cancelled",
                "message": "Engine query execution cancelled",
                "next_step": "Re-run the query if the cancellation was unintentional.",
                "audit_id": jti_q2,
            },
        )
    ]
    # Factory was NEVER called for Request 2
    assert factory_calls == 1

    # Request 3 queues behind Request 1 with a short timeout and expires in queue without calling factory
    tok3 = sign_grant_token(
        _make_grant_claims(sql, jti="jti-q3-queued-timeout"), priv, kid="key-1"
    )
    resp3 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok3}"},
        json={"sql": sql, "timeout_s": 0.04},
    )
    assert resp3.status_code == 504
    assert resp3.json()["error"] == "engine_timeout"
    assert factory_calls == 1

    # Now release Request 1; it completes normally
    q1_release.set()
    t1.join(timeout=5.0)
    assert len(r1_outcomes) == 1
    assert r1_outcomes[0][0] == 200
    assert r1_outcomes[0][1]["rows"] == [[1, "Alice"], [2, "Bob"]]

    # Subsequent Request 4 succeeds, proving lock was not stranded
    tok4 = sign_grant_token(
        _make_grant_claims(sql, jti="jti-q4-after"), priv, kid="key-1"
    )
    resp4 = client.post(
        "/v1/query",
        headers={"Authorization": f"Bearer {tok4}"},
        json={"sql": sql},
    )
    assert resp4.status_code == 200
    assert resp4.json()["rows"] == [[1, "Alice"], [2, "Bob"]]
    assert factory_calls == 2

    parent.close()


def test_total_timeout_includes_queue_and_execution_without_reset():
    """Total timeout budget covers both queue wait and query execution (watchdog uses remaining budget, not full timeout_seconds)."""
    parent = duckdb.connect()
    parent.execute("CREATE TABLE users (id INT, name VARCHAR)")
    parent.execute("INSERT INTO users VALUES (1, 'Alice')")

    first_started = threading.Event()
    call_count = 0
    call_lock = threading.Lock()

    def timed_factory() -> duckdb.DuckDBPyConnection:
        nonlocal call_count
        with call_lock:
            call_count += 1
            idx = call_count
        cur = parent.cursor()
        if idx == 1:
            first_started.set()
            time.sleep(0.12)
        return cur

    timed_adapter = DuckDBAdapter(timed_factory)

    t1 = threading.Thread(
        target=lambda: timed_adapter.execute(_make_prepared(), timeout_seconds=5.0)
    )
    t1.start()
    assert first_started.wait(timeout=2.0)

    # Second caller has total timeout_seconds=0.18s, spends ~0.12s in queue, leaving ~0.06s for a slow query
    slow_prep = _make_prepared(
        sql="SELECT count(*) FROM range(100000000) t1, range(1000) t2"
    )
    t0 = time.perf_counter()
    with pytest.raises(EngineTimeout):
        timed_adapter.execute(slow_prep, timeout_seconds=0.18)
    elapsed = time.perf_counter() - t0

    t1.join(timeout=5.0)
    # Total elapsed time should be bounded near 0.18s (well under 0.30s if timeout had been reset after queue)
    assert elapsed < 0.28
    parent.close()


def test_fresh_per_query_factory_and_file_reopen_semantics(hardened_duckdb_setup):
    """Verify per-query file reopen semantics: updating the DuckDB file between queries is visible immediately."""
    db_file = hardened_duckdb_setup["db_file"]
    allowed_dir = hardened_duckdb_setup["allowed_dir"]

    adapter = DuckDBAdapter(
        lambda: connect_hardened(
            db_file,
            read_only=True,
            allowed_directories=[allowed_dir],
            memory_limit="256MB",
            threads=2,
        )
    )

    res1 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    assert res1.rows == [(1, "Alice"), (2, "Bob")]

    # Because the read-only connection was closed on query completion, a writer can open the file and insert a row
    writer = duckdb.connect(db_file)
    try:
        writer.execute("INSERT INTO users VALUES (3, 'Charlie')")
    finally:
        writer.close()

    res2 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    assert res2.rows == [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
