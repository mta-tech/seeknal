"""Tests for Seeknal Serving engine adapters."""

from __future__ import annotations

import duckdb
import pytest

from seeknal.serving.adapters import (
    DBAPIAdapter,
    DuckDBAdapter,
    EngineAdapter,
    EngineError,
    EngineTimeout,
    QueryResult,
    build_adapter,
)
from seeknal.serving.service import PreparedQuery


def _make_prepared(
    *,
    engine: str = "duck",
    engine_kind: str = "duckdb",
    sql: str = "SELECT 1",
    params: tuple = (),
    paramstyle: str = "qmark",
    row_limit: int | None = 10,
    cube_security_context: dict | None = None,
) -> PreparedQuery:
    return PreparedQuery(
        engine=engine,
        engine_kind=engine_kind,
        sql=sql,
        params=params,
        paramstyle=paramstyle,
        rule="test",
        reason="test",
        row_limit=row_limit,
        masked_columns=(),
        redacted_sql="SELECT ?",
        subject="test@example.com",
        workspace="ws-test",
        audit_id="audit-123",
        cube_security_context=cube_security_context,
    )


class FakeCursor:
    def __init__(
        self,
        connection: FakeConnection,
        rows: list[tuple] | None = None,
        description: tuple | None = None,
    ) -> None:
        self.connection = connection
        self.rows = rows or []
        self.description = description
        self.closed = False
        self.fetch_index = 0

    def execute(self, sql: str, params: tuple = ()) -> None:
        if self.connection.raise_on_execute:
            raise self.connection.raise_on_execute
        self.connection.recorded_statements.append((sql, params))

    def fetchmany(self, size: int) -> list[tuple]:
        if self.fetch_index >= len(self.rows):
            return []
        res = self.rows[self.fetch_index : self.fetch_index + size]
        self.fetch_index += size
        return res

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(
        self,
        rows: list[tuple] | None = None,
        description: tuple | None = None,
    ) -> None:
        self.rows = rows or []
        self.description = description
        self.recorded_statements: list[tuple[str, tuple]] = []
        self.committed = False
        self.closed = False
        self.raise_on_execute: Exception | None = None

    def cursor(self) -> FakeCursor:
        return FakeCursor(self, self.rows, self.description)

    def commit(self) -> None:
        self.committed = True

    def close(self) -> None:
        self.closed = True


# --- DuckDBAdapter Tests ---


def test_duckdb_adapter_rows():
    """Test DuckDBAdapter fetches rows and extracts columns without truncation."""
    con = duckdb.connect()
    con.execute("CREATE TABLE users (id INT, name VARCHAR)")
    con.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

    adapter = DuckDBAdapter(lambda: con)
    prep = _make_prepared(
        engine="duck",
        engine_kind="duckdb",
        sql="SELECT id, name FROM users ORDER BY id",
        params=(),
        paramstyle="qmark",
        row_limit=5,
    )

    result = adapter.execute(prep, timeout_seconds=10.0)
    assert isinstance(result, QueryResult)
    assert result.engine == "duck"
    assert result.columns == ("id", "name")
    assert result.rows == [(1, "Alice"), (2, "Bob")]
    assert result.truncated is False
    assert result.elapsed_ms > 0


def test_duckdb_adapter_truncation():
    """Test DuckDBAdapter truncates when exactly row_limit rows are fetched."""
    con = duckdb.connect()
    con.execute("CREATE TABLE items (id INT)")
    con.execute("INSERT INTO items VALUES (1), (2), (3), (4)")

    adapter = DuckDBAdapter(lambda: con)
    # row_limit is 3 (max_rows 2 + 1). 4 rows exist, so fetchmany(3) returns 3 rows.
    # It should drop the 3rd and set truncated=True.
    prep = _make_prepared(
        engine="duck",
        engine_kind="duckdb",
        sql="SELECT id FROM items ORDER BY id",
        params=(),
        paramstyle="qmark",
        row_limit=3,
    )

    result = adapter.execute(prep, timeout_seconds=10.0)
    assert result.columns == ("id",)
    assert result.rows == [(1,), (2,)]
    assert result.truncated is True


def test_duckdb_adapter_write():
    """Test DuckDBAdapter write query commits and returns no rows."""
    con = duckdb.connect()
    con.execute("CREATE TABLE logs (msg VARCHAR)")

    adapter = DuckDBAdapter(lambda: con)
    prep = _make_prepared(
        engine="duck",
        engine_kind="duckdb",
        sql="INSERT INTO logs VALUES (?)",
        params=("hello",),
        paramstyle="qmark",
        row_limit=None,
    )

    result = adapter.execute(prep, timeout_seconds=10.0)
    assert result.columns == ()
    assert result.rows == []
    assert result.truncated is False

    # Check data persisted
    rows = con.execute("SELECT msg FROM logs").fetchall()
    assert rows == [("hello",)]


def test_duckdb_adapter_timeout():
    """Test DuckDBAdapter raises EngineTimeout on slow queries."""
    con = duckdb.connect()
    adapter = DuckDBAdapter(lambda: con)

    # Large cross join that will take much longer than 0.2 seconds
    prep = _make_prepared(
        engine="duck",
        engine_kind="duckdb",
        sql="SELECT count(*) FROM range(100000000) t1, range(1000) t2",
        params=(),
        paramstyle="qmark",
        row_limit=10,
    )

    with pytest.raises(EngineTimeout):
        adapter.execute(prep, timeout_seconds=0.2)


def test_duckdb_adapter_wrong_paramstyle():
    """Test DuckDBAdapter rejects paramstyle other than 'qmark'."""
    con = duckdb.connect()
    adapter = DuckDBAdapter(lambda: con)
    prep = _make_prepared(
        engine="duck",
        engine_kind="duckdb",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
    )
    with pytest.raises(EngineError, match="paramstyle 'qmark'"):
        adapter.execute(prep, timeout_seconds=5.0)


# --- DBAPIAdapter Tests ---


def test_starrocks_preamble_and_execution():
    """Test StarRocks preamble SET query_timeout = <whole seconds>."""
    fake_conn = FakeConnection(
        rows=[(1, "SR")],
        description=(("id", None), ("name", None)),
    )
    adapter = DBAPIAdapter(lambda: fake_conn, kind="starrocks")

    prep = _make_prepared(
        engine="sr",
        engine_kind="starrocks",
        sql="SELECT id, name FROM sr_table WHERE id = %s",
        params=(1,),
        paramstyle="format",
        row_limit=10,
    )

    result = adapter.execute(prep, timeout_seconds=5.4)
    assert result.columns == ("id", "name")
    assert result.rows == [(1, "SR")]
    assert result.truncated is False
    assert fake_conn.closed is True

    # Preamble: SET query_timeout = 5, followed by query
    assert len(fake_conn.recorded_statements) == 2
    assert fake_conn.recorded_statements[0] == ("SET query_timeout = 5", ())
    assert fake_conn.recorded_statements[1] == (
        "SELECT id, name FROM sr_table WHERE id = %s",
        (1,),
    )


def test_lakebase_select_preamble_order():
    """Test Lakebase SELECT preamble order: BEGIN -> SET TRANSACTION READ ONLY -> SET LOCAL statement_timeout."""
    fake_conn = FakeConnection(
        rows=[(42,)],
        description=(("val", None),),
    )
    adapter = DBAPIAdapter(lambda: fake_conn, kind="lakebase")

    prep = _make_prepared(
        engine="lb",
        engine_kind="lakebase",
        sql="SELECT val FROM lb_table",
        params=(),
        paramstyle="format",
        row_limit=10,
    )

    result = adapter.execute(prep, timeout_seconds=3.0)
    assert result.rows == [(42,)]
    assert fake_conn.closed is True

    # Check exact statement order
    stmts = [s[0] for s in fake_conn.recorded_statements]
    assert stmts == [
        "BEGIN",
        "SET TRANSACTION READ ONLY",
        "SET LOCAL statement_timeout = 3000",
        "SELECT val FROM lb_table",
    ]


def test_lakebase_write_preamble_and_commit():
    """Test Lakebase WRITE statement: BEGIN -> SET LOCAL statement_timeout -> statement -> commit."""
    fake_conn = FakeConnection()
    adapter = DBAPIAdapter(lambda: fake_conn, kind="lakebase")

    prep = _make_prepared(
        engine="lb",
        engine_kind="lakebase",
        sql="INSERT INTO lb_table VALUES (%s)",
        params=(99,),
        paramstyle="format",
        row_limit=None,
    )

    result = adapter.execute(prep, timeout_seconds=2.0)
    assert result.columns == ()
    assert result.rows == []
    assert result.truncated is False
    assert fake_conn.committed is True
    assert fake_conn.closed is True

    stmts = [s[0] for s in fake_conn.recorded_statements]
    assert stmts == [
        "BEGIN",
        "SET LOCAL statement_timeout = 2000",
        "INSERT INTO lb_table VALUES (%s)",
    ]


def test_cube_with_security_context():
    """Test Cube query succeeds when cube_security_context is present."""
    fake_conn = FakeConnection(
        rows=[("JB", 100)],
        description=(("province", None), ("total", None)),
    )
    adapter = DBAPIAdapter(lambda: fake_conn, kind="cube")

    prep = _make_prepared(
        engine="cube",
        engine_kind="cube",
        sql="SELECT province, total FROM cube_claims",
        params=(),
        paramstyle="format",
        row_limit=10,
        cube_security_context={"subject": "analyst@example.com"},
    )

    result = adapter.execute(prep, timeout_seconds=10.0)
    assert result.rows == [("JB", 100)]
    assert fake_conn.closed is True
    # Cube has no preamble
    assert len(fake_conn.recorded_statements) == 1
    assert fake_conn.recorded_statements[0][0] == "SELECT province, total FROM cube_claims"


def test_cube_refusal_without_security_context():
    """Test Cube query is refused with EngineError when cube_security_context is None."""
    called = False

    def connect():
        nonlocal called
        called = True
        return FakeConnection()

    adapter = DBAPIAdapter(connect, kind="cube")
    prep = _make_prepared(
        engine="cube",
        engine_kind="cube",
        sql="SELECT * FROM cube_claims",
        params=(),
        paramstyle="format",
        cube_security_context=None,
    )

    with pytest.raises(EngineError, match="cube_security_context"):
        adapter.execute(prep, timeout_seconds=10.0)

    # Must refuse without even attempting to connect
    assert called is False


def test_dbapi_truncation():
    """Test DBAPIAdapter truncation drops last row and sets truncated=True."""
    fake_conn = FakeConnection(
        rows=[(1,), (2,), (3,)],
        description=(("id", None),),
    )
    adapter = DBAPIAdapter(lambda: fake_conn, kind="starrocks")

    prep = _make_prepared(
        engine="sr",
        engine_kind="starrocks",
        sql="SELECT id FROM t",
        params=(),
        paramstyle="format",
        row_limit=3,
    )

    result = adapter.execute(prep, timeout_seconds=10.0)
    assert result.rows == [(1,), (2,)]
    assert result.truncated is True


def test_dbapi_closing_on_error():
    """Test connection is closed even when an error occurs during execution."""
    fake_conn = FakeConnection()
    fake_conn.raise_on_execute = RuntimeError("internal driver failure")
    adapter = DBAPIAdapter(lambda: fake_conn, kind="lakebase")

    prep = _make_prepared(
        engine="lb",
        engine_kind="lakebase",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        row_limit=5,
    )

    with pytest.raises(EngineError) as exc_info:
        adapter.execute(prep, timeout_seconds=5.0)

    assert fake_conn.closed is True
    # Internal error detail should not be in the exception message
    assert "internal driver failure" not in str(exc_info.value)


def test_dbapi_before_connect():
    """Test before_connect hook is executed before connect."""
    events = []

    def before():
        events.append("before")

    def connect():
        events.append("connect")
        return FakeConnection(rows=[(1,)], description=(("x", None),))

    adapter = DBAPIAdapter(connect, kind="lakebase", before_connect=before)
    prep = _make_prepared(
        engine="lb",
        engine_kind="lakebase",
        sql="SELECT 1",
        params=(),
        paramstyle="format",
        row_limit=5,
    )

    adapter.execute(prep, timeout_seconds=5.0)
    assert events == ["before", "connect"]


def test_dbapi_timeout_mapping():
    """Test driver timeout errors map to EngineTimeout and sanitize messages."""
    # 1. PyMySQL code 1317
    class PyMySQLError(Exception):
        pass

    fake_conn = FakeConnection()
    fake_conn.raise_on_execute = PyMySQLError(1317, "Query execution was interrupted sensitive_data_foo")
    adapter = DBAPIAdapter(lambda: fake_conn, kind="starrocks")
    prep = _make_prepared(engine="sr", engine_kind="starrocks", paramstyle="format")

    with pytest.raises(EngineTimeout) as exc_info:
        adapter.execute(prep, timeout_seconds=5.0)
    assert "sensitive_data_foo" not in str(exc_info.value)

    # 2. Timeout in message
    fake_conn2 = FakeConnection()
    fake_conn2.raise_on_execute = Exception("Statement timeout expired with secret_val")
    adapter2 = DBAPIAdapter(lambda: fake_conn2, kind="starrocks")

    with pytest.raises(EngineTimeout) as exc_info:
        adapter2.execute(prep, timeout_seconds=5.0)
    assert "secret_val" not in str(exc_info.value)

    # 3. Psycopg2 QueryCanceled
    class QueryCanceled(Exception):
        pass

    fake_conn3 = FakeConnection()
    fake_conn3.raise_on_execute = QueryCanceled("canceling statement due to statement timeout: secret_user")
    adapter3 = DBAPIAdapter(lambda: fake_conn3, kind="lakebase")
    prep_lb = _make_prepared(engine="lb", engine_kind="lakebase", paramstyle="format")

    with pytest.raises(EngineTimeout) as exc_info:
        adapter3.execute(prep_lb, timeout_seconds=5.0)
    assert "secret_user" not in str(exc_info.value)

    # 4. Non-timeout driver error maps to EngineError
    fake_conn4 = FakeConnection()
    fake_conn4.raise_on_execute = Exception("table not found: sensitive_table")
    adapter4 = DBAPIAdapter(lambda: fake_conn4, kind="lakebase")

    with pytest.raises(EngineError) as exc_info:
        adapter4.execute(prep_lb, timeout_seconds=5.0)
    assert not isinstance(exc_info.value, EngineTimeout)
    assert "sensitive_table" not in str(exc_info.value)


def test_build_adapter():
    """Test build_adapter factory."""
    duck = build_adapter("duckdb", duckdb.connect)
    assert isinstance(duck, DuckDBAdapter)

    sr = build_adapter("starrocks", FakeConnection)
    assert isinstance(sr, DBAPIAdapter)

    lb = build_adapter("lakebase", FakeConnection)
    assert isinstance(lb, DBAPIAdapter)

    cube = build_adapter("cube", FakeConnection)
    assert isinstance(cube, DBAPIAdapter)

    with pytest.raises(ValueError, match="Unsupported engine kind"):
        build_adapter("oracle", FakeConnection)
