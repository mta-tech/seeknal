"""Engine adapters for Seeknal Serving: DuckDB, StarRocks, Lakebase, and Cube."""

from __future__ import annotations

import inspect
import logging
import re
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import duckdb

from seeknal.serving.service import PreparedQuery

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class QueryResult:
    """Result of an engine query execution."""

    columns: tuple[str, ...]
    rows: list[tuple]
    truncated: bool
    engine: str
    elapsed_ms: float
    column_types: tuple[str, ...] = ()


class EngineError(Exception):
    """Raised when engine query execution fails."""


class EngineTimeout(EngineError):
    """Raised when engine query execution times out."""


class EngineConfigError(EngineError):
    """Raised when engine configuration or account for workload is missing."""


class EngineCancelled(EngineError):
    """Raised when engine query execution is cancelled."""


class EngineAdapter(Protocol):
    """Protocol for engine query execution adapters."""

    def execute(
        self,
        prepared: PreparedQuery,
        *,
        timeout_seconds: float,
        on_cancel_handle: Callable[[Callable[[], None]], None] | None = None,
    ) -> QueryResult:
        ...


def _is_timeout_error(exc: BaseException) -> bool:
    """Check if an exception represents a driver query timeout."""
    # Check psycopg2 QueryCanceled
    if type(exc).__name__ == "QueryCanceled":
        return True
    try:
        import psycopg2.errors

        if isinstance(exc, psycopg2.errors.QueryCanceled):
            return True
    except (ImportError, AttributeError):
        pass

    # Check pymysql error code 1317 or message containing "timeout"
    if hasattr(exc, "args") and exc.args:
        if exc.args[0] == 1317:
            return True
        for arg in exc.args:
            if isinstance(arg, str) and "timeout" in arg.lower():
                return True

    if "timeout" in str(exc).lower():
        return True

    return False


def normalize_column_type(kind: str, description_entry: Any) -> str:
    """Normalize a database column type description entry into standard contract v2 types."""
    if isinstance(description_entry, (tuple, list)):
        type_code = description_entry[1] if len(description_entry) > 1 else None
        display_size = description_entry[2] if len(description_entry) > 2 else None
        precision = description_entry[4] if len(description_entry) > 4 else None
        scale = description_entry[5] if len(description_entry) > 5 else None
    else:
        type_code = description_entry
        display_size = None
        precision = None
        scale = None

    kind_lower = kind.lower()
    if kind_lower == "duckdb":
        type_str = str(type_code).strip().lower()
        dec_match = re.match(r"^(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$", type_str)
        if dec_match:
            return f"decimal({dec_match.group(1)},{dec_match.group(2)})"
        if (type_str.startswith("decimal") or type_str.startswith("numeric")) and precision is not None and scale is not None:
            return f"decimal({precision},{scale})"
        if type_str in ("boolean", "bool"):
            return "boolean"
        if type_str in ("smallint", "tinyint", "int2", "short"):
            return "smallint"
        if type_str in ("integer", "int", "int4", "signed"):
            return "integer"
        if type_str in ("bigint", "int8", "long", "hugeint", "uhugeint", "ubigint"):
            return "bigint"
        if type_str in ("float", "float4", "real"):
            return "float"
        if type_str in ("double", "float8"):
            return "double"
        if type_str in ("varchar", "text", "string", "char", "bpchar"):
            return "varchar"
        if type_str == "date":
            return "date"
        if type_str in ("timestamp with time zone", "timestamptz"):
            return "timestamptz"
        if type_str in ("timestamp", "datetime"):
            return "timestamp"
        if type_str in ("time", "time without time zone"):
            return "time"
        if type_str in ("blob", "bytea", "binary", "varbinary"):
            return "binary"
        if type_str == "json":
            return "json"
        return "unknown"

    elif kind_lower in ("starrocks", "pymysql", "mysql"):
        try:
            code = int(type_code)
        except (ValueError, TypeError):
            return "unknown"

        if code in (0, 246):  # DECIMAL, NEWDECIMAL
            if precision is not None and scale is not None:
                return f"decimal({precision},{scale})"
            return "unknown"
        if code == 16:  # BIT
            return "boolean"
        if code == 1:  # TINY
            if display_size == 1:
                return "boolean"
            return "smallint"
        if code == 2:  # SHORT
            return "smallint"
        if code in (3, 9):  # LONG, INT24
            return "integer"
        if code == 8:  # LONGLONG
            return "bigint"
        if code == 4:  # FLOAT
            return "float"
        if code == 5:  # DOUBLE
            return "double"
        if code in (15, 253, 254):  # VARCHAR, VAR_STRING, STRING
            return "varchar"
        if code in (10, 14):  # DATE, NEWDATE
            return "date"
        if code in (7, 12):  # TIMESTAMP, DATETIME
            return "timestamp"
        if code == 11:  # TIME
            return "time"
        if code in (249, 250, 251, 252):  # TINY_BLOB, MEDIUM_BLOB, LONG_BLOB, BLOB
            return "binary"
        if code == 245:  # JSON
            return "json"
        return "unknown"

    elif kind_lower in ("lakebase", "cube", "psycopg2", "postgres", "postgresql"):
        try:
            oid = int(type_code)
        except (ValueError, TypeError):
            return "unknown"

        if oid == 16:
            return "boolean"
        if oid == 21:
            return "smallint"
        if oid == 23:
            return "integer"
        if oid == 20:
            return "bigint"
        if oid == 700:
            return "float"
        if oid == 701:
            return "double"
        if oid == 1700:
            if precision is not None and scale is not None:
                return f"decimal({precision},{scale})"
            return "unknown"
        if oid in (25, 1043):
            return "varchar"
        if oid == 1082:
            return "date"
        if oid == 1114:
            return "timestamp"
        if oid == 1184:
            return "timestamptz"
        if oid == 1083:
            return "time"
        if oid == 17:
            return "binary"
        if oid in (114, 3802):
            return "json"
        return "unknown"

    return "unknown"


class DuckDBAdapter:
    """Adapter for in-process DuckDB execution."""

    def __init__(self, connect: Callable[[], duckdb.DuckDBPyConnection]) -> None:
        self._connect = connect

    def execute(
        self,
        prepared: PreparedQuery,
        *,
        timeout_seconds: float,
        on_cancel_handle: Callable[[Callable[[], None]], None] | None = None,
    ) -> QueryResult:
        if prepared.paramstyle != "qmark":
            raise EngineError(
                f"DuckDBAdapter requires paramstyle 'qmark', got '{prepared.paramstyle}'"
            )

        start = time.perf_counter()
        con = self._connect()
        cancel_event = threading.Event()

        def cancel_func() -> None:
            cancel_event.set()
            try:
                con.interrupt()
            except Exception:
                pass

        if on_cancel_handle is not None:
            on_cancel_handle(cancel_func)

        timer = threading.Timer(timeout_seconds, con.interrupt)
        timer.start()
        try:
            con.execute(prepared.sql, prepared.params)
            if prepared.row_limit is None:
                con.commit()
                columns: tuple[str, ...] = ()
                column_types: tuple[str, ...] = ()
                rows: list[tuple] = []
                truncated = False
            else:
                columns = (
                    tuple(d[0] for d in con.description)
                    if con.description
                    else ()
                )
                column_types = (
                    tuple(normalize_column_type("duckdb", d) for d in con.description)
                    if con.description
                    else ()
                )
                fetched = con.fetchmany(prepared.row_limit)
                if len(fetched) == prepared.row_limit:
                    rows = [tuple(r) for r in fetched[:-1]]
                    truncated = True
                else:
                    rows = [tuple(r) for r in fetched]
                    truncated = False
        except duckdb.InterruptException as exc:
            if cancel_event.is_set():
                logger.error("engine=%s error=%s audit_id=%s", "duckdb", type(exc).__name__, prepared.audit_id)
                raise EngineCancelled("Engine query execution cancelled") from exc
            logger.warning("engine=%s error=%s audit_id=%s", "duckdb", type(exc).__name__, prepared.audit_id)
            raise EngineTimeout("Engine query timed out") from exc
        except EngineTimeout:
            raise
        except EngineCancelled:
            raise
        except Exception as exc:
            if cancel_event.is_set():
                logger.error("engine=%s error=%s audit_id=%s", "duckdb", type(exc).__name__, prepared.audit_id)
                raise EngineCancelled("Engine query execution cancelled") from exc
            logger.error("engine=%s error=%s audit_id=%s", "duckdb", type(exc).__name__, prepared.audit_id)
            raise EngineError("Engine query execution failed") from exc
        finally:
            timer.cancel()

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return QueryResult(
            columns=columns,
            rows=rows,
            truncated=truncated,
            engine=prepared.engine,
            elapsed_ms=elapsed_ms,
            column_types=column_types,
        )


class DBAPIAdapter:
    """Adapter for DB-API 2.0 drivers (pymysql, psycopg2) for StarRocks, Lakebase, and Cube."""

    def __init__(
        self,
        connect: Callable[[], Any],
        *,
        kind: str,
        before_connect: Callable[[], None] | None = None,
    ) -> None:
        self._connect = connect
        self._kind = kind
        self._before_connect = before_connect

    def execute(
        self,
        prepared: PreparedQuery,
        *,
        timeout_seconds: float,
        on_cancel_handle: Callable[[Callable[[], None]], None] | None = None,
    ) -> QueryResult:
        if prepared.paramstyle != "format":
            raise EngineError(
                f"DBAPIAdapter requires paramstyle 'format', got '{prepared.paramstyle}'"
            )
        if prepared.engine_kind != self._kind:
            raise EngineError(
                f"DBAPIAdapter kind '{self._kind}' does not match prepared engine_kind '{prepared.engine_kind}'"
            )
        if self._kind == "cube" and prepared.cube_security_context is None:
            raise EngineError("Cube engine query requires cube_security_context")

        if self._before_connect is not None:
            try:
                self._before_connect()
            except Exception as exc:
                logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
                if _is_timeout_error(exc):
                    raise EngineTimeout("Engine query timed out") from exc
                raise EngineError("Engine connection preparation failed") from exc

        start = time.perf_counter()
        try:
            conn = self._connect()
        except Exception as exc:
            logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
            if _is_timeout_error(exc):
                raise EngineTimeout("Engine query timed out") from exc
            raise EngineError("Engine connection failed") from exc

        cancel_event = threading.Event()
        cancel_called = False
        cancel_lock = threading.Lock()

        try:
            cursor = conn.cursor()
            try:
                if self._kind == "starrocks":
                    if on_cancel_handle is not None:
                        cursor.execute("SELECT CONNECTION_ID()")
                        if hasattr(cursor, "fetchone"):
                            row = cursor.fetchone()
                        else:
                            fetched_rows = cursor.fetchmany(1)
                            row = fetched_rows[0] if fetched_rows else None
                        if row is None or not isinstance(row[0], int):
                            raise EngineError("Failed to retrieve connection id for StarRocks")
                        conn_id = row[0]

                        def cancel_sr() -> None:
                            nonlocal cancel_called
                            with cancel_lock:
                                if cancel_called:
                                    return
                                cancel_called = True
                            cancel_event.set()
                            try:
                                cancel_conn = self._connect()
                                try:
                                    cancel_cur = cancel_conn.cursor()
                                    try:
                                        cancel_cur.execute(f"KILL QUERY {conn_id}")
                                    finally:
                                        try:
                                            cancel_cur.close()
                                        except Exception:
                                            pass
                                finally:
                                    try:
                                        cancel_conn.close()
                                    except Exception:
                                        pass
                            except Exception:
                                pass

                        on_cancel_handle(cancel_sr)

                    timeout_s = max(1, int(round(timeout_seconds)))
                    cursor.execute(f"SET query_timeout = {timeout_s}")

                elif self._kind == "lakebase":
                    def cancel_lb() -> None:
                        nonlocal cancel_called
                        with cancel_lock:
                            if cancel_called:
                                return
                            cancel_called = True
                        cancel_event.set()
                        try:
                            conn.cancel()
                        except Exception:
                            pass

                    if on_cancel_handle is not None:
                        on_cancel_handle(cancel_lb)

                    timeout_ms = max(1, int(round(timeout_seconds * 1000)))
                    cursor.execute("BEGIN")
                    if prepared.row_limit is not None:
                        cursor.execute("SET TRANSACTION READ ONLY")
                    cursor.execute(f"SET LOCAL statement_timeout = {timeout_ms}")

                cursor.execute(prepared.sql, prepared.params)

                if prepared.row_limit is None:
                    conn.commit()
                    columns: tuple[str, ...] = ()
                    column_types: tuple[str, ...] = ()
                    rows: list[tuple] = []
                    truncated = False
                else:
                    columns = (
                        tuple(d[0] for d in cursor.description)
                        if cursor.description
                        else ()
                    )
                    column_types = (
                        tuple(normalize_column_type(self._kind, d) for d in cursor.description)
                        if cursor.description
                        else ()
                    )
                    fetched = cursor.fetchmany(prepared.row_limit)
                    if len(fetched) == prepared.row_limit:
                        rows = [tuple(r) for r in fetched[:-1]]
                        truncated = True
                    else:
                        rows = [tuple(r) for r in fetched]
                        truncated = False
            finally:
                try:
                    cursor.close()
                except Exception:
                    pass
        except Exception as exc:
            if cancel_event.is_set():
                logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
                raise EngineCancelled("Engine query execution cancelled") from exc
            logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
            if _is_timeout_error(exc):
                raise EngineTimeout("Engine query timed out") from exc
            raise EngineError("Engine query execution failed") from exc
        finally:
            try:
                conn.close()
            except Exception as exc:
                logger.warning("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return QueryResult(
            columns=columns,
            rows=rows,
            truncated=truncated,
            engine=prepared.engine,
            elapsed_ms=elapsed_ms,
            column_types=column_types,
        )


class WorkloadRoutedAdapter:
    """Adapter that delegates to another adapter based on prepared.workload."""

    def __init__(self, by_workload: Mapping[str, EngineAdapter]) -> None:
        self.by_workload = dict(by_workload)

    def execute(
        self,
        prepared: PreparedQuery,
        *,
        timeout_seconds: float,
        on_cancel_handle: Callable[[Callable[[], None]], None] | None = None,
    ) -> QueryResult:
        adapter = self.by_workload.get(prepared.workload)
        if adapter is None:
            raise EngineConfigError(
                f"no engine account configured for workload '{prepared.workload}'"
            )
        sig = inspect.signature(adapter.execute)
        if "on_cancel_handle" in sig.parameters:
            return adapter.execute(
                prepared,
                timeout_seconds=timeout_seconds,
                on_cancel_handle=on_cancel_handle,
            )
        return adapter.execute(prepared, timeout_seconds=timeout_seconds)


def build_workload_adapters(
    kind: str,
    connects: Mapping[str, Callable[[], Any]],
    **kw: Any,
) -> WorkloadRoutedAdapter:
    """Build a WorkloadRoutedAdapter for a given engine kind across workloads."""
    by_workload = {
        w: build_adapter(kind, conn, **kw)
        for w, conn in connects.items()
    }
    return WorkloadRoutedAdapter(by_workload)


def build_adapter(
    kind: str,
    connect: Callable[[], Any],
    **kw: Any,
) -> EngineAdapter:
    """Small factory to build an engine adapter for a given engine kind."""
    if kind == "duckdb":
        return DuckDBAdapter(connect, **kw)
    if kind in ("starrocks", "lakebase", "cube"):
        return DBAPIAdapter(connect, kind=kind, **kw)
    raise ValueError(f"Unsupported engine kind: '{kind}'")
