"""Engine adapters for Seeknal Serving: DuckDB, StarRocks, Lakebase, and Cube."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
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


class EngineError(Exception):
    """Raised when engine query execution fails."""


class EngineTimeout(EngineError):
    """Raised when engine query execution times out."""


class EngineAdapter(Protocol):
    """Protocol for engine query execution adapters."""

    def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
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


class DuckDBAdapter:
    """Adapter for in-process DuckDB execution."""

    def __init__(self, connect: Callable[[], duckdb.DuckDBPyConnection]) -> None:
        self._connect = connect

    def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
        if prepared.paramstyle != "qmark":
            raise EngineError(
                f"DuckDBAdapter requires paramstyle 'qmark', got '{prepared.paramstyle}'"
            )

        start = time.perf_counter()
        con = self._connect()
        timer = threading.Timer(timeout_seconds, con.interrupt)
        timer.start()
        try:
            con.execute(prepared.sql, prepared.params)
            if prepared.row_limit is None:
                con.commit()
                columns: tuple[str, ...] = ()
                rows: list[tuple] = []
                truncated = False
            else:
                columns = (
                    tuple(d[0] for d in con.description)
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
            logger.warning("DuckDB query interrupted/timed out: %s", exc)
            raise EngineTimeout("Engine query timed out") from exc
        except EngineTimeout:
            raise
        except Exception as exc:
            logger.exception("DuckDB query execution failed: %s", exc)
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

    def execute(self, prepared: PreparedQuery, *, timeout_seconds: float) -> QueryResult:
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
                logger.exception("before_connect failed for engine kind '%s': %s", self._kind, exc)
                if _is_timeout_error(exc):
                    raise EngineTimeout("Engine query timed out") from exc
                raise EngineError("Engine connection preparation failed") from exc

        start = time.perf_counter()
        try:
            conn = self._connect()
        except Exception as exc:
            logger.exception("connect failed for engine kind '%s': %s", self._kind, exc)
            if _is_timeout_error(exc):
                raise EngineTimeout("Engine query timed out") from exc
            raise EngineError("Engine connection failed") from exc

        try:
            cursor = conn.cursor()
            try:
                if self._kind == "starrocks":
                    timeout_s = max(1, int(round(timeout_seconds)))
                    cursor.execute(f"SET query_timeout = {timeout_s}")
                elif self._kind == "lakebase":
                    timeout_ms = max(1, int(round(timeout_seconds * 1000)))
                    cursor.execute("BEGIN")
                    if prepared.row_limit is not None:
                        cursor.execute("SET TRANSACTION READ ONLY")
                    cursor.execute(f"SET LOCAL statement_timeout = {timeout_ms}")

                cursor.execute(prepared.sql, prepared.params)

                if prepared.row_limit is None:
                    conn.commit()
                    columns: tuple[str, ...] = ()
                    rows: list[tuple] = []
                    truncated = False
                else:
                    columns = (
                        tuple(d[0] for d in cursor.description)
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
            logger.exception("DBAPI query execution failed for engine kind '%s': %s", self._kind, exc)
            if _is_timeout_error(exc):
                raise EngineTimeout("Engine query timed out") from exc
            raise EngineError("Engine query execution failed") from exc
        finally:
            try:
                conn.close()
            except Exception as exc:
                logger.warning("Error closing engine connection: %s", exc)

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return QueryResult(
            columns=columns,
            rows=rows,
            truncated=truncated,
            engine=prepared.engine,
            elapsed_ms=elapsed_ms,
        )


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
