"""Tests for Lakebase connection pool: reuse, bounding, acquire timeout, expiration, error discard, and restart."""

from __future__ import annotations

import concurrent.futures
import threading
import time
from typing import Any

import pytest

from seeknal.serving.adapters import (
    DBAPIAdapter,
    EngineCancelled,
    EngineError,
    EnginePoolExhausted,
    EngineTimeout,
    LakebasePool,
    PostgresPool,
    QueryResult,
)
from seeknal.serving.service import PreparedQuery


def _make_prepared(
    *,
    sql: str = "SELECT 1",
    params: tuple = (),
    row_limit: int | None = 10,
    audit_id: str = "audit-lb-1",
) -> PreparedQuery:
    return PreparedQuery(
        engine="lb",
        engine_kind="lakebase",
        sql=sql,
        params=params,
        paramstyle="format",
        rule="test",
        reason="test",
        row_limit=row_limit,
        masked_columns=(),
        redacted_sql="SELECT %s",
        subject="test@example.com",
        workspace="ws-test",
        audit_id=audit_id,
        cube_security_context=None,
    )


STATUS_READY = 1
STATUS_BEGIN = 2
STATUS_IN_TRANSACTION = 2

TRANSACTION_STATUS_IDLE = 0
TRANSACTION_STATUS_ACTIVE = 1
TRANSACTION_STATUS_INTRANS = 2
TRANSACTION_STATUS_INERROR = 3
TRANSACTION_STATUS_UNKNOWN = 4


class FakeCursor:
    def __init__(self, conn: FakeConnection) -> None:
        self.conn = conn
        self.closed = False
        self.description = (("val", 23, None, None, None, None, None),)

    def execute(self, sql: str, params: tuple = ()) -> None:
        if self.conn.closed != 0 or self.conn.is_dead:
            raise Exception("Connection is closed or dead")
        if self.conn.fail_on_begin and "BEGIN" in sql:
            self.conn.is_dead = True
            self.conn.closed = 2
            raise Exception("server closed the connection unexpectedly (simulated restart)")
        if "BEGIN" in sql:
            self.conn.status = STATUS_IN_TRANSACTION
        if self.conn.fail_on_query and "SELECT" in sql:
            raise Exception("syntax error in SQL query")
        if self.conn.timeout_on_query and "SELECT" in sql:
            raise Exception("canceling statement due to statement timeout")
        if self.conn.slow_query_seconds > 0 and "SELECT" in sql:
            time.sleep(self.conn.slow_query_seconds)
            if self.conn.cancelled:
                raise Exception("canceling statement due to user request")

    def fetchmany(self, size: int) -> list[tuple]:
        return [(self.conn.conn_id,)]

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    _id_counter = 0
    _id_lock = threading.Lock()

    def __init__(
        self,
        *,
        fail_on_begin: bool = False,
        fail_on_query: bool = False,
        timeout_on_query: bool = False,
        slow_query_seconds: float = 0.0,
    ) -> None:
        with self._id_lock:
            FakeConnection._id_counter += 1
            self.conn_id = FakeConnection._id_counter

        # Real psycopg2 attributes:
        # closed: 0 if open, 1 or 2 if closed
        self.closed = 0
        # status: STATUS_READY is 1
        self.status = STATUS_READY
        self.is_dead = False
        self.cancelled = False
        self.in_use = False
        self.lock = threading.Lock()
        self.fail_on_begin = fail_on_begin
        self.fail_on_query = fail_on_query
        self.timeout_on_query = timeout_on_query
        self.slow_query_seconds = slow_query_seconds
        self.rolled_back = False
        self.committed = False

    def cursor(self) -> FakeCursor:
        if self.closed != 0 or self.is_dead:
            raise Exception("Cannot create cursor on closed or dead connection")
        with self.lock:
            if self.in_use:
                raise AssertionError(f"Connection {self.conn_id} concurrently used by two queries!")
            self.in_use = True
        return FakeCursor(self)

    def cancel(self) -> None:
        self.cancelled = True

    def rollback(self) -> None:
        with self.lock:
            self.in_use = False
            self.status = STATUS_READY
            self.rolled_back = True

    def commit(self) -> None:
        with self.lock:
            self.in_use = False
            self.status = STATUS_READY
            self.committed = True

    def close(self) -> None:
        with self.lock:
            self.closed = 1
            self.in_use = False

    def get_transaction_status(self) -> int:
        if self.closed != 0 or self.is_dead:
            return TRANSACTION_STATUS_UNKNOWN
        if self.in_use:
            return TRANSACTION_STATUS_INTRANS
        return TRANSACTION_STATUS_IDLE

    def poll(self) -> int:
        if self.closed != 0 or self.is_dead:
            raise Exception("Connection dead in poll()")
        return 0


class FakeDriver:
    def __init__(self) -> None:
        self.connections: list[FakeConnection] = []
        self.lock = threading.Lock()
        self.next_fail_on_begin = False
        self.next_slow_seconds = 0.0

    def connect(self, **kwargs: Any) -> FakeConnection:
        with self.lock:
            conn = FakeConnection(
                fail_on_begin=self.next_fail_on_begin,
                slow_query_seconds=self.next_slow_seconds,
            )
            self.next_fail_on_begin = False
            self.next_slow_seconds = 0.0
            self.connections.append(conn)
            return conn


def test_connection_reuse():
    """Sequential queries reuse the same connection instance."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=4, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    res1 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    res2 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    res3 = adapter.execute(_make_prepared(), timeout_seconds=5.0)

    assert res1.rows == res2.rows == res3.rows
    # Only 1 connection was created by driver
    assert len(driver.connections) == 1
    assert not driver.connections[0].closed


def test_pool_bounding():
    """Concurrent queries are bounded by pool_max."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=3, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    def run_q(i: int):
        return adapter.execute(_make_prepared(audit_id=f"audit-{i}"), timeout_seconds=5.0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(run_q, i) for i in range(12)]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert len(results) == 12
    # At most pool_max (3) connections created
    assert len(driver.connections) <= 3


def test_acquire_timeout_exhaustion():
    """When pool is exhausted, waiting times out with EnginePoolExhausted (503-class error)."""
    driver = FakeDriver()
    # pool_max=1, acquire_timeout=0.2s
    pool = LakebasePool(
        driver.connect,
        pool_max=1,
        pool_min=0,
        acquire_timeout_seconds=0.2,
    )
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    # Hold the single connection
    conn1 = pool.acquire()
    try:
        # A query now attempts to acquire and must time out
        with pytest.raises(EnginePoolExhausted) as exc_info:
            adapter.execute(_make_prepared(), timeout_seconds=5.0)
        assert "Lakebase pooled connection" in str(exc_info.value)
    finally:
        pool.release(conn1, success=True)


def test_discard_after_error():
    """A connection with an error is closed and never reused."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=2, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    # First query succeeds
    res1 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    c1 = driver.connections[0]

    # Second query fails
    c1.fail_on_query = True
    with pytest.raises(EngineError):
        adapter.execute(_make_prepared(), timeout_seconds=5.0)

    # c1 was closed and discarded
    assert c1.closed

    # Third query succeeds on a new connection
    res3 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    assert len(driver.connections) == 2
    c2 = driver.connections[1]
    assert not c2.closed
    assert res1.rows != res3.rows


def test_discard_after_cancel():
    """A connection cancelled by the client is closed and never reused."""
    driver = FakeDriver()
    driver.next_slow_seconds = 0.5
    pool = LakebasePool(driver.connect, pool_max=2, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    def cancel_after_start(on_cancel):
        time.sleep(0.05)
        on_cancel()

    cancel_fn_holder = [None]
    def on_cancel_handle(fn):
        cancel_fn_holder[0] = fn
        threading.Thread(target=cancel_after_start, args=(fn,)).start()

    with pytest.raises(EngineCancelled):
        adapter.execute(_make_prepared(), timeout_seconds=5.0, on_cancel_handle=on_cancel_handle)

    c1 = driver.connections[0]
    assert c1.closed


def test_discard_after_timeout():
    """A connection timed out by statement timeout is closed and never reused."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=2, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    # First query succeeds
    adapter.execute(_make_prepared(), timeout_seconds=5.0)
    c1 = driver.connections[0]

    # Statement timeout happens on query
    c1.timeout_on_query = True
    with pytest.raises(EngineTimeout):
        adapter.execute(_make_prepared(), timeout_seconds=0.1)

    assert c1.closed


def test_idle_and_lifetime_expiry():
    """Idle connections older than max_idle_seconds, or beyond max_lifetime_seconds, are closed and replaced."""
    driver = FakeDriver()
    pool = LakebasePool(
        driver.connect,
        pool_max=2,
        pool_min=0,
        max_idle_seconds=0.1,
        max_lifetime_seconds=0.2,
    )
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    # 1. Query 1
    adapter.execute(_make_prepared(), timeout_seconds=5.0)
    c1 = driver.connections[0]

    # Sleep longer than max_idle_seconds
    time.sleep(0.15)

    # Query 2 triggers idle expiration of c1
    adapter.execute(_make_prepared(), timeout_seconds=5.0)
    assert c1.closed
    assert len(driver.connections) == 2
    c2 = driver.connections[1]
    assert not c2.closed

    # Sleep longer than max_lifetime_seconds
    time.sleep(0.25)
    adapter.execute(_make_prepared(), timeout_seconds=5.0)
    assert c2.closed
    assert len(driver.connections) == 3


def test_dead_connection_replacement():
    """A connection found dead on checkout is replaced transparently once."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=2, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    adapter.execute(_make_prepared(), timeout_seconds=5.0)
    c1 = driver.connections[0]

    # Mark c1 as dead while idle in the pool
    c1.is_dead = True

    # Next query checks out c1, detects it is dead, replaces it transparently, and succeeds
    res = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    assert c1.closed
    assert len(driver.connections) == 2
    c2 = driver.connections[1]
    assert res.rows == [(c2.conn_id,)]


def test_endpoint_restart_transparent_retry():
    """After an endpoint restart, dead pooled connection is detected on setup and transparently retried."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=2, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    # Query 1 opens connection 1
    res1 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    c1 = driver.connections[0]

    # Endpoint restarts: c1 is severed and fails on BEGIN
    c1.fail_on_begin = True

    # Next query should transparently replace c1 and succeed without surfacing error to caller
    res2 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    assert c1.closed
    assert len(driver.connections) == 2
    c2 = driver.connections[1]
    assert res2.rows == [(c2.conn_id,)]


def test_no_concurrent_sharing():
    """No connection is ever used by two queries at the same time."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=4, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    def run_worker(idx: int):
        return adapter.execute(_make_prepared(audit_id=f"concur-{idx}"), timeout_seconds=5.0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_worker, i) for i in range(16)]
        for f in concurrent.futures.as_completed(futures):
            # If any concurrent sharing happened, FakeConnection.cursor() would have raised AssertionError
            f.result()


def test_shutdown_closes_all():
    """Pool shutdown / adapter close closes all open connections."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=4, pool_min=2)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    # Pre-warmed connections
    assert len(driver.connections) == 2
    assert not driver.connections[0].closed
    assert not driver.connections[1].closed

    adapter.close()
    assert driver.connections[0].closed
    assert driver.connections[1].closed


def test_pool_max_one_and_pool_min_zero():
    """pool_max: 1 plus pool_min: 0 works properly."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=1, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    res1 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    res2 = adapter.execute(_make_prepared(), timeout_seconds=5.0)
    assert res1.rows == res2.rows
    assert len(driver.connections) == 1


def test_64_sequential_queries_use_one_connection():
    """64 sequential queries with real psycopg2 attributes use ONE connection."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=8, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    first_res = adapter.execute(_make_prepared(audit_id="seq-0"), timeout_seconds=5.0)
    expected_id = driver.connections[0].conn_id
    assert first_res.rows == [(expected_id,)]

    for i in range(1, 64):
        res = adapter.execute(_make_prepared(audit_id=f"seq-{i}"), timeout_seconds=5.0)
        assert res.rows == [(expected_id,)]

    # Exactly 1 connection was created across all 64 queries
    assert len(driver.connections) == 1
    assert driver.connections[0].status == STATUS_READY
    assert driver.connections[0].closed == 0
    pool.close()


def test_late_cancel_does_not_cancel_subsequent_query():
    """A cancel callback called after a query completes does not cancel a subsequent query."""
    driver = FakeDriver()
    pool = LakebasePool(driver.connect, pool_max=1, pool_min=0)
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    captured_cancel = [None]
    def on_cancel(fn):
        captured_cancel[0] = fn

    # Query 1 finishes cleanly
    res1 = adapter.execute(_make_prepared(audit_id="q1"), timeout_seconds=5.0, on_cancel_handle=on_cancel)
    expected_id = driver.connections[0].conn_id
    assert res1.rows == [(expected_id,)]
    assert captured_cancel[0] is not None

    # Late cancel arriving for Query 1
    captured_cancel[0]()

    # The connection returned to the pool was NOT cancelled
    assert not driver.connections[0].cancelled

    # Query 2 executes on the same connection successfully
    res2 = adapter.execute(_make_prepared(audit_id="q2"), timeout_seconds=5.0)
    assert res2.rows == [(expected_id,)]
    assert not driver.connections[0].cancelled
    pool.close()


def test_background_reaper_cleans_idle_connections():
    """Background reaper daemon thread cleans connections idle longer than max_idle_seconds."""
    driver = FakeDriver()
    pool = LakebasePool(
        driver.connect,
        pool_max=4,
        pool_min=0,
        max_idle_seconds=0.1,
    )
    adapter = DBAPIAdapter(driver.connect, kind="lakebase", pool=pool)

    adapter.execute(_make_prepared(), timeout_seconds=5.0)
    c1 = driver.connections[0]
    assert c1.closed == 0

    # Wait for reaper to run (max_idle_seconds is 0.1s, interval is 0.05s)
    time.sleep(0.3)

    assert c1.closed != 0
    pool.close()


def test_minimal_dbapi_driver_sequential_queries_reuse():
    """5 sequential queries through a minimal PEP 249 DB-API driver reuse 1 connection."""
    class MinimalPEP249Connection:
        def __init__(self, idx: int) -> None:
            self.conn_id = idx
            self.in_transaction = False
            self.closed_flag = False

        def cursor(self):
            if self.closed_flag:
                raise Exception("Cannot open cursor on closed connection")
            self.in_transaction = True
            return MinimalPEP249Cursor(self)

        def commit(self):
            if not self.in_transaction:
                raise Exception("Cannot commit without an active transaction")
            self.in_transaction = False

        def rollback(self):
            if not self.in_transaction:
                raise Exception("Cannot rollback without an active transaction")
            self.in_transaction = False

        def close(self):
            self.closed_flag = True

        @property
        def closed(self) -> bool:
            return self.closed_flag

    class MinimalPEP249Cursor:
        def __init__(self, conn: MinimalPEP249Connection) -> None:
            self.conn = conn
            self.description = [("id", 23, None, None, None, None, None)]

        def execute(self, sql: str, params: tuple = ()):
            if self.conn.closed_flag:
                raise Exception("Cursor execute on closed connection")
            if "FAIL" in sql:
                raise Exception("Query failed")

        def fetchmany(self, size: int):
            return [(self.conn.conn_id,)]

        def close(self):
            pass

    created_conns: list[MinimalPEP249Connection] = []

    def minimal_connect():
        c = MinimalPEP249Connection(len(created_conns) + 1)
        created_conns.append(c)
        return c

    pool = LakebasePool(minimal_connect, pool_max=5, pool_min=0)
    adapter = DBAPIAdapter(minimal_connect, kind="lakebase", pool=pool)

    # 5 sequential queries must use exactly ONE connection
    for i in range(5):
        res = adapter.execute(_make_prepared(audit_id=f"min-{i}"), timeout_seconds=5.0)
        assert res.rows == [(1,)]

    assert len(created_conns) == 1
    assert not created_conns[0].closed_flag

    # An error query discards the connection
    fail_prep = _make_prepared(sql="SELECT FAIL", audit_id="fail-query")
    with pytest.raises(EngineError):
        adapter.execute(fail_prep, timeout_seconds=5.0)

    assert created_conns[0].closed_flag

    # Next query opens a fresh connection and succeeds
    res_next = adapter.execute(_make_prepared(audit_id="min-next"), timeout_seconds=5.0)
    assert res_next.rows == [(2,)]
    assert len(created_conns) == 2
    pool.close()


def test_postgres_pool_alias_and_postgres_kind_reuse():
    """LakebasePool is an alias for PostgresPool and serves kind='postgres'."""
    assert LakebasePool is PostgresPool

    driver = FakeDriver()
    pool = PostgresPool(driver.connect, pool_max=4, pool_min=0)
    adapter = DBAPIAdapter(
        driver.connect,
        kind="postgres",
        pool=pool,
        read_only=True,
    )

    def _pg_prep(audit_id: str) -> PreparedQuery:
        return PreparedQuery(
            engine="pg_report",
            engine_kind="postgres",
            sql="SELECT 1",
            params=(),
            paramstyle="format",
            rule="test",
            reason="test",
            row_limit=10,
            masked_columns=(),
            redacted_sql="SELECT %s",
            subject="test@example.com",
            workspace="ws-test",
            audit_id=audit_id,
            cube_security_context=None,
        )

    r1 = adapter.execute(_pg_prep("pg-1"), timeout_seconds=5.0)
    r2 = adapter.execute(_pg_prep("pg-2"), timeout_seconds=5.0)
    assert r1.rows == r2.rows
    assert len(driver.connections) == 1
    pool.close()

