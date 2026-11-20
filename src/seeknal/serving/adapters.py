"""Engine adapters for Seeknal Serving: DuckDB, StarRocks, Postgres, Lakebase, and Cube."""

from __future__ import annotations

import base64
import hashlib
import hmac
import inspect
import json
import logging
import math
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
    cold_start: bool | None = None


class EngineError(Exception):
    """Raised when engine query execution fails."""


class EngineTimeout(EngineError):
    """Raised when engine query execution times out."""


class EngineConfigError(EngineError):
    """Raised when engine configuration or account for workload is missing."""


class EngineCancelled(EngineError):
    """Raised when engine query execution is cancelled."""


class EnginePoolExhausted(EngineConfigError):
    """Raised when Postgres/Lakebase connection pool is exhausted."""


from seeknal.serving.postgres_session import (  # noqa: E402
    POSTGRES_WIRE_KINDS,
    PostgresAccountConfig,
    PostgresSessionConfig,
    PostgresTLSConfig,
    begin_query_transaction,
    build_connect_kwargs,
    is_postgres_wire,
    verify_and_harden_session,
)

try:
    from seeknal.serving.lakebase_endpoint import (  # noqa: E402
        ColdStartConnector,
        EngineStartingError,
        effective_storage_class,
        role_guard_sql,
        snapshot_freshness,
    )
except ImportError:
    from seeknal.serving.postgres_session import (  # noqa: E402
        ColdStartConnector,
        EngineStartingError,
        effective_storage_class,
        role_guard_sql,
        snapshot_freshness,
    )


@dataclass(frozen=True)
class CubeSecurityConfig:
    """Security configuration for Cube context token generation."""

    secret: bytes
    issuer: str = "seeknal-serving"
    audience: str = "cube-sql"
    ttl_seconds: int = 60
    sql_user: str = "seeknal"

    def __post_init__(self) -> None:
        if not isinstance(self.secret, (bytes, bytearray)):
            raise ValueError("secret must be bytes")
        if not isinstance(self.issuer, str):
            raise ValueError("issuer must be string")
        if not isinstance(self.audience, str):
            raise ValueError("audience must be string")
        if not isinstance(self.sql_user, str):
            raise ValueError("sql_user must be string")
        if isinstance(self.ttl_seconds, bool) or not isinstance(self.ttl_seconds, (int, float)):
            raise ValueError("ttl_seconds must be a number")
        if not (0 < self.ttl_seconds <= 300):
            raise ValueError(f"ttl_seconds must be between 1 and 300, got {self.ttl_seconds}")


def make_cube_context_token(
    prepared: PreparedQuery,
    cfg: CubeSecurityConfig,
    now: float | None = None,
) -> str:
    """Build a compact HS256 JWT containing Cube security context using standard library only."""
    if prepared.cube_security_context is None:
        raise EngineError("Cube engine query requires cube_security_context")

    iat = int(now if now is not None else time.time())
    ttl = int(cfg.ttl_seconds)
    if ttl <= 0 or ttl > 300:
        raise ValueError(f"ttl_seconds must be between 1 and 300, got {ttl}")
    exp = iat + ttl

    header = {"alg": "HS256", "typ": "JWT"}
    claims = {
        "iss": cfg.issuer,
        "aud": cfg.audience,
        "iat": iat,
        "exp": exp,
        "sub": prepared.subject,
        "audit_id": prepared.audit_id,
        "masks": prepared.cube_security_context.get("masks", {}),
        "row_filters": prepared.cube_security_context.get("row_filters", {}),
    }

    header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
    payload_bytes = json.dumps(claims, separators=(",", ":")).encode("utf-8")

    header_b64 = base64.urlsafe_b64encode(header_bytes).rstrip(b"=").decode("ascii")
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b"=").decode("ascii")

    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    signature = hmac.new(cfg.secret, signing_input, hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii")

    return f"{header_b64}.{payload_b64}.{sig_b64}"


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


TIMEOUT_PHRASES: tuple[str, ...] = (
    "statement timeout",
    "query timeout",
    "exceeded time limit",
    "execution was interrupted",
)


def _is_timeout_error(exc: BaseException) -> bool:
    """Check if an exception represents a driver query timeout during execution."""
    # Check psycopg2 QueryCanceled
    if type(exc).__name__ == "QueryCanceled":
        return True
    try:
        import psycopg2.errors

        if isinstance(exc, psycopg2.errors.QueryCanceled):
            return True
    except (ImportError, AttributeError):
        pass

    # Check pymysql error codes 1317 and 3024
    if hasattr(exc, "args") and exc.args:
        if exc.args[0] in (1317, 3024):
            return True
        for arg in exc.args:
            if isinstance(arg, str):
                arg_lower = arg.lower()
                for phrase in TIMEOUT_PHRASES:
                    if phrase in arg_lower:
                        return True

    msg = str(exc).lower()
    for phrase in TIMEOUT_PHRASES:
        if phrase in msg:
            return True

    return False


def normalize_column_type(kind: str, description_entry: Any) -> str:
    """Normalize a database column type description entry into standard contract v2 types."""
    if isinstance(description_entry, (tuple, list)):
        type_code = description_entry[1] if len(description_entry) > 1 else None
        display_size = description_entry[2] if len(description_entry) > 2 else None
        precision = description_entry[4] if len(description_entry) > 4 else None
        scale = description_entry[5] if len(description_entry) > 5 else None
    elif hasattr(description_entry, "type_code"):
        type_code = getattr(description_entry, "type_code", None)
        display_size = getattr(description_entry, "display_size", None)
        precision = getattr(description_entry, "precision", None)
        scale = getattr(description_entry, "scale", None)
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
        if (type_str.startswith("decimal") or type_str.startswith("numeric")):
            if precision is not None and scale is not None:
                return f"decimal({precision},{scale})"
            return "decimal"
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
            return "decimal"
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
                if (precision, scale) == (65535, 65535) or precision == 65535:
                    return "decimal"
                return f"decimal({precision},{scale})"
            return "decimal"
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


_DUCKDB_QUEUE_POLL_SECONDS = 0.005


class _DuckDBRequestState:
    """Per-execute first-wins terminal outcome, cancellation, watchdog, and owned-connection state."""

    def __init__(self) -> None:
        self._state_lock = threading.Lock()
        self._outcome: str | None = None
        self._conn: Any = None
        self._owned_conn: Any = None
        self._closed = False

    @property
    def outcome(self) -> str | None:
        with self._state_lock:
            return self._outcome

    def cancel(self) -> None:
        with self._state_lock:
            if self._outcome is not None:
                return
            self._outcome = "cancel"
            conn = self._conn
            if self._closed or conn is None:
                return
            try:
                conn.interrupt()
            except Exception:
                pass

    def timeout_interrupt(self) -> None:
        with self._state_lock:
            if self._outcome is not None:
                return
            self._outcome = "timeout"
            conn = self._conn
            if self._closed or conn is None:
                return
            try:
                conn.interrupt()
            except Exception:
                pass

    def check_or_expire(self, deadline: float) -> str | None:
        with self._state_lock:
            if self._outcome is not None:
                return self._outcome
            if deadline - time.perf_counter() <= 0:
                self._outcome = "timeout"
                self._conn = None
                return "timeout"
            return None

    def attach_connection(self, conn: Any) -> str | None:
        with self._state_lock:
            self._owned_conn = conn
            if self._outcome is not None:
                return self._outcome
            self._conn = conn
            return None

    def finalize_outcome(self, candidate: str) -> str:
        with self._state_lock:
            if self._outcome is None:
                self._outcome = candidate
            self._conn = None
            return self._outcome

    def disarm(self, default_outcome: str = "error") -> str:
        with self._state_lock:
            if self._outcome is None:
                self._outcome = default_outcome
            self._conn = None
            return self._outcome

    def close_connection(self, audit_id: str) -> Exception | None:
        with self._state_lock:
            if self._closed:
                return None
            self._closed = True
            self._conn = None
            conn = self._owned_conn
            self._owned_conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.warning(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    type(exc).__name__,
                    audit_id,
                )
                return exc
        return None


class DuckDBAdapter:
    """Adapter for in-process DuckDB execution with bounded concurrent cursor slots and owned lifecycle."""

    def __init__(
        self,
        connect: Callable[[], duckdb.DuckDBPyConnection] | None = None,
        *,
        config: Any = None,
        holder: Any = None,
        max_concurrent_cursors: int | None = None,
        env: Mapping[str, str] | None = None,
        standin_catalogs: Mapping[str, str] | None = None,
        probe_on_reuse: bool = True,
    ) -> None:
        self._connect_fn = connect
        self._config = config
        self._holder = holder
        self._env = env
        self._standin_catalogs = standin_catalogs
        self._probe_on_reuse = probe_on_reuse
        if max_concurrent_cursors is not None:
            if (
                isinstance(max_concurrent_cursors, bool)
                or not isinstance(max_concurrent_cursors, int)
                or max_concurrent_cursors <= 0
            ):
                raise ValueError("max_concurrent_cursors must be a positive integer")
            bound = max_concurrent_cursors
        elif config is not None and getattr(config, "max_concurrent_cursors", None) is not None:
            bound = int(config.max_concurrent_cursors)
        elif config is not None or holder is not None:
            bound = 32
        else:
            bound = 1
        self._max_concurrent_cursors = bound
        self._slot_semaphore = threading.BoundedSemaphore(bound)
        self._active_slots = 0
        self._waiting_requests = 0
        self._seen_file_inst_id: int | None = None
        self._file_generation = 0
        self._slots_lock = threading.Lock()
        self.storage_class: str | None = None
        self.account: str | None = None
        self.lakebase_endpoint: dict[str, str] | None = None
        self.last_cold_start: bool | None = None

    def _note_cursor_instance(self, cur: Any) -> None:
        inst = getattr(cur, "_instance", None)
        if inst is None or getattr(inst, "is_closed", False):
            return
        inst_id = id(inst)
        if self._seen_file_inst_id == inst_id:
            return
        with self._slots_lock:
            if self._seen_file_inst_id != inst_id:
                self._seen_file_inst_id = inst_id
                self._file_generation += 1

    def _get_cursor(self) -> Any:
        if self._config is not None:
            from seeknal.serving.duckdb_policy import DuckDBHolder

            holder = self._holder or DuckDBHolder.get_holder()
            profile = getattr(self._config, "profile", "local_file")
            if profile == "iceberg_direct":
                return holder.get_iceberg_cursor(
                    self._config,
                    config_id=f"iceberg_direct:{id(self._config)}",
                    env=self._env,
                    standin_catalogs=self._standin_catalogs,
                    probe_on_reuse=self._probe_on_reuse,
                )
            cur = holder.get_cursor(
                self._config.path,
                allowed_directories=self._config.allowed_directories,
                memory_limit=self._config.memory_limit,
                threads=self._config.threads,
                temp_directory=getattr(self._config, "temp_directory", None),
                max_temp_directory_size=getattr(
                    self._config, "max_temp_directory_size", None
                ),
            )
            self._note_cursor_instance(cur)
            return cur
        if self._connect_fn is not None:
            return self._connect_fn()
        raise EngineError("DuckDBAdapter has neither config nor connect function")

    def _extract_holder_generation(self, holder: Any) -> int:
        if getattr(holder, "_closed", False):
            return 0
        configured_map = getattr(holder, "_configured_to_dev_ino", {})
        instances_map = getattr(holder, "_instances_by_dev_ino", {})

        if self._config is not None:
            profile = getattr(self._config, "profile", "local_file")
            if profile == "iceberg_direct":
                cfg_key = f"iceberg_direct:{id(self._config)}"
                curr_key = configured_map.get(cfg_key)
                if curr_key is not None:
                    curr_inst = instances_map.get(curr_key)
                    if curr_inst is not None and not getattr(curr_inst, "is_closed", False):
                        if (
                            isinstance(curr_key, tuple)
                            and len(curr_key) >= 2
                            and curr_key[0] == -1
                        ):
                            return int(curr_key[1])
                        return max(1, int(getattr(holder, "_generation_seq", 1)))
                return 0

            path = getattr(self._config, "path", None)
            curr_key = configured_map.get(path) if path is not None else None
            curr_inst = instances_map.get(curr_key) if curr_key is not None else None
            if curr_inst is not None and not getattr(curr_inst, "is_closed", False):
                inst_id = id(curr_inst)
                with self._slots_lock:
                    if self._seen_file_inst_id != inst_id:
                        self._seen_file_inst_id = inst_id
                        self._file_generation += 1
                    return self._file_generation
            return 0

        gen_seq = int(getattr(holder, "_generation_seq", 0))
        if gen_seq > 0:
            return gen_seq
        return sum(
            1
            for inst in instances_map.values()
            if not getattr(inst, "is_closed", False)
        )

    def _read_current_generation(self) -> int:
        from seeknal.serving.duckdb_policy import DuckDBHolder

        holder = self._holder
        if holder is None and self._config is not None:
            holder = DuckDBHolder.get_holder()
        if holder is None:
            return 0
        lock = getattr(holder, "_lock", None)
        if lock is not None:
            with lock:
                return self._extract_holder_generation(holder)
        return self._extract_holder_generation(holder)

    def stats(self) -> dict[str, int]:
        """Return read-only concurrency and generation statistics without opening a connection."""
        with self._slots_lock:
            waiting = self._waiting_requests
            active_slots = self._active_slots
        current_generation = self._read_current_generation()
        return {
            "queue_depth": waiting,
            "in_flight_cursors": active_slots,
            "generation": current_generation,
            "max_concurrent_cursors": self._max_concurrent_cursors,
        }

    def check_health(self, timeout_seconds: float = 2.0) -> bool:
        """Non-destructively probe DuckDB with SELECT 1 outside the query slot semaphore."""
        try:
            if (
                isinstance(timeout_seconds, bool)
                or not isinstance(timeout_seconds, (int, float))
                or math.isnan(timeout_seconds)
                or math.isinf(timeout_seconds)
                or timeout_seconds <= 0
            ):
                return False

            deadline = time.perf_counter() + float(timeout_seconds)
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                return False

            cur: Any = None
            timer: threading.Timer | None = None
            try:
                if self._config is not None:
                    if self._holder is not None and getattr(self._holder, "_closed", False):
                        return False
                    profile = getattr(self._config, "profile", "local_file")
                    cfg_path = getattr(self._config, "path", None)
                    if (
                        profile != "iceberg_direct"
                        and isinstance(cfg_path, str)
                        and (cfg_path == ":memory:" or cfg_path.startswith(":memory:"))
                    ):
                        cur = duckdb.connect(":memory:")
                    else:
                        cur = self._get_cursor()
                elif self._connect_fn is not None:
                    raw = self._connect_fn()
                    if raw is None:
                        return False
                    if isinstance(raw, duckdb.DuckDBPyConnection):
                        cur = raw.cursor()
                    else:
                        cur = raw
                else:
                    return False

                if cur is None or not callable(getattr(cur, "execute", None)):
                    return False

                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    return False

                timed_out = threading.Event()

                def _interrupt() -> None:
                    timed_out.set()
                    try:
                        cur.interrupt()
                    except Exception:
                        pass

                timer = threading.Timer(remaining, _interrupt)
                timer.daemon = True
                timer.start()

                cur.execute("SELECT 1")
                if timed_out.is_set():
                    return False
                try:
                    cur.fetchone()
                except Exception:
                    pass
                return not timed_out.is_set()
            finally:
                if timer is not None:
                    try:
                        timer.cancel()
                    except Exception:
                        pass
                if cur is not None and callable(getattr(cur, "close", None)):
                    try:
                        cur.close()
                    except Exception:
                        pass
        except Exception:
            return False

    def _connect(self) -> Any:
        return self._get_cursor()

    def close(self) -> None:
        if self._holder is not None:
            self._holder.close_all()

    @staticmethod
    def _connection_error_message(exc: Exception) -> str:
        msg = str(exc)
        if msg in (
            "DuckDBHolder is closed",
            "DuckDB instance is closed",
            "DuckDB database file not found",
            "DuckDB database file is already open with different hardening parameters",
            "In-place rewrite of active DuckDB file is not supported; use atomic symlink swaps",
            "DuckDB probe conformance check failed",
            "DuckDBAdapter has neither config nor connect function",
        ):
            return f"Engine connection failed: {msg}"
        return "Engine connection failed"

    def _resource_exhausted_message(self) -> str:
        if self._config is not None and getattr(self._config, "temp_directory", None):
            return "resource_exhausted: query exceeded memory limit or temp_directory spill space; increase memory_limit or max_temp_directory_size"
        return "resource_exhausted: query exceeded memory limit and temp_directory is not configured; configure temp_directory to allow spilling"

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
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or math.isnan(timeout_seconds)
            or math.isinf(timeout_seconds)
        ):
            raise EngineError("timeout_seconds must be a finite number")

        start = time.perf_counter()
        deadline = start + float(timeout_seconds)
        req_state = _DuckDBRequestState()

        if on_cancel_handle is not None:
            on_cancel_handle(req_state.cancel)

        pre_fast = req_state.check_or_expire(deadline)
        if pre_fast == "cancel":
            logger.error(
                "engine=%s error=%s audit_id=%s",
                "duckdb",
                "EngineCancelled",
                prepared.audit_id,
            )
            raise EngineCancelled("Engine query execution cancelled")
        if pre_fast == "timeout":
            logger.warning(
                "engine=%s error=%s audit_id=%s",
                "duckdb",
                "EngineTimeout",
                prepared.audit_id,
            )
            raise EngineTimeout("Engine query timed out")

        acquired = self._slot_semaphore.acquire(blocking=False)
        if acquired:
            with self._slots_lock:
                self._active_slots += 1
        else:
            decremented_waiting = False
            with self._slots_lock:
                self._waiting_requests += 1
            try:
                while not acquired:
                    pre_outcome = req_state.check_or_expire(deadline)
                    if pre_outcome == "cancel":
                        logger.error(
                            "engine=%s error=%s audit_id=%s",
                            "duckdb",
                            "EngineCancelled",
                            prepared.audit_id,
                        )
                        raise EngineCancelled("Engine query execution cancelled")
                    if pre_outcome == "timeout":
                        logger.warning(
                            "engine=%s error=%s audit_id=%s",
                            "duckdb",
                            "EngineTimeout",
                            prepared.audit_id,
                        )
                        raise EngineTimeout("Engine query timed out")
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        continue
                    acquired = self._slot_semaphore.acquire(
                        timeout=min(remaining, _DUCKDB_QUEUE_POLL_SECONDS)
                    )
                    if acquired:
                        with self._slots_lock:
                            self._active_slots += 1
                            self._waiting_requests -= 1
                            decremented_waiting = True
            finally:
                if not decremented_waiting:
                    with self._slots_lock:
                        self._waiting_requests -= 1

        timer: threading.Timer | None = None
        timer_start_attempted = False
        completed_ok = False
        try:
            post_lock_outcome = req_state.check_or_expire(deadline)
            if post_lock_outcome == "cancel":
                logger.error(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    "EngineCancelled",
                    prepared.audit_id,
                )
                raise EngineCancelled("Engine query execution cancelled")
            if post_lock_outcome == "timeout":
                logger.warning(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    "EngineTimeout",
                    prepared.audit_id,
                )
                raise EngineTimeout("Engine query timed out")

            try:
                con = self._get_cursor()
            except Exception as exc:
                conn_err_outcome = req_state.finalize_outcome("error")
                if conn_err_outcome == "cancel":
                    logger.error(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        type(exc).__name__,
                        prepared.audit_id,
                    )
                    raise EngineCancelled("Engine query execution cancelled") from None
                logger.error(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    type(exc).__name__,
                    prepared.audit_id,
                )
                raise EngineError(self._connection_error_message(exc)) from None

            if (
                con is None
                or not callable(getattr(con, "execute", None))
                or not callable(getattr(con, "close", None))
            ):
                req_state.finalize_outcome("error")
                if con is not None and callable(getattr(con, "close", None)):
                    try:
                        con.close()
                    except Exception as close_exc:
                        logger.warning(
                            "engine=%s error=%s audit_id=%s",
                            "duckdb",
                            type(close_exc).__name__,
                            prepared.audit_id,
                        )
                logger.error(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    "TypeError",
                    prepared.audit_id,
                )
                raise EngineError("Engine connection failed")

            attach_outcome = req_state.attach_connection(con)
            if attach_outcome == "cancel":
                req_state.disarm("cancel")
                logger.error(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    "EngineCancelled",
                    prepared.audit_id,
                )
                raise EngineCancelled("Engine query execution cancelled")

            post_connect_outcome = req_state.check_or_expire(deadline)
            if post_connect_outcome == "cancel":
                req_state.disarm("cancel")
                logger.error(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    "EngineCancelled",
                    prepared.audit_id,
                )
                raise EngineCancelled("Engine query execution cancelled")
            if post_connect_outcome == "timeout":
                req_state.disarm("timeout")
                logger.warning(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    "EngineTimeout",
                    prepared.audit_id,
                )
                raise EngineTimeout("Engine query timed out")

            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                req_state.disarm("timeout")
                logger.warning(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    "EngineTimeout",
                    prepared.audit_id,
                )
                raise EngineTimeout("Engine query timed out")

            timer = threading.Timer(remaining, req_state.timeout_interrupt)
            timer.daemon = True
            timer_start_attempted = True
            timer.start()

            try:
                pre_exec_outcome = req_state.outcome
                if pre_exec_outcome == "cancel":
                    req_state.disarm("cancel")
                    logger.error(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        "EngineCancelled",
                        prepared.audit_id,
                    )
                    raise EngineCancelled("Engine query execution cancelled")
                if pre_exec_outcome == "timeout":
                    req_state.disarm("timeout")
                    logger.warning(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        "EngineTimeout",
                        prepared.audit_id,
                    )
                    raise EngineTimeout("Engine query timed out")

                con.execute(prepared.sql, prepared.params)

                mid_outcome = req_state.outcome
                if mid_outcome == "cancel":
                    req_state.disarm("cancel")
                    logger.error(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        "EngineCancelled",
                        prepared.audit_id,
                    )
                    raise EngineCancelled("Engine query execution cancelled")
                if mid_outcome == "timeout":
                    req_state.disarm("timeout")
                    logger.warning(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        "EngineTimeout",
                        prepared.audit_id,
                    )
                    raise EngineTimeout("Engine query timed out")

                if prepared.row_limit is None:
                    if hasattr(con, "commit"):
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
                        tuple(
                            normalize_column_type("duckdb", d)
                            for d in con.description
                        )
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

                final_outcome = req_state.finalize_outcome("success")
                if final_outcome == "cancel":
                    logger.error(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        "EngineCancelled",
                        prepared.audit_id,
                    )
                    raise EngineCancelled("Engine query execution cancelled")
                if final_outcome == "timeout":
                    logger.warning(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        "EngineTimeout",
                        prepared.audit_id,
                    )
                    raise EngineTimeout("Engine query timed out")
                completed_ok = True
            except duckdb.InterruptException as exc:
                int_outcome = req_state.finalize_outcome("timeout")
                if int_outcome == "cancel":
                    logger.error(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        type(exc).__name__,
                        prepared.audit_id,
                    )
                    raise EngineCancelled("Engine query execution cancelled") from None
                logger.warning(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    type(exc).__name__,
                    prepared.audit_id,
                )
                raise EngineTimeout("Engine query timed out") from None
            except (duckdb.OutOfMemoryException, duckdb.FatalException) as exc:
                if isinstance(exc, duckdb.FatalException):
                    if self._config is not None:
                        from seeknal.serving.duckdb_policy import DuckDBHolder

                        holder = self._holder or DuckDBHolder.get_holder()
                        if getattr(self._config, "profile", "local_file") == "iceberg_direct":
                            holder.invalidate_path(f"iceberg_direct:{id(self._config)}")
                        else:
                            holder.invalidate_path(self._config.path)
                err_outcome = req_state.finalize_outcome("error")
                if err_outcome == "cancel":
                    logger.error(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        type(exc).__name__,
                        prepared.audit_id,
                    )
                    raise EngineCancelled("Engine query execution cancelled") from None
                if err_outcome == "timeout":
                    logger.warning(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        type(exc).__name__,
                        prepared.audit_id,
                    )
                    raise EngineTimeout("Engine query timed out") from None
                logger.error(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    type(exc).__name__,
                    prepared.audit_id,
                )
                raise EngineError(self._resource_exhausted_message()) from None
            except (EngineTimeout, EngineCancelled, EngineError):
                req_state.disarm("error")
                raise
            except Exception as exc:
                err_outcome = req_state.finalize_outcome("error")
                if err_outcome == "cancel":
                    logger.error(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        type(exc).__name__,
                        prepared.audit_id,
                    )
                    raise EngineCancelled("Engine query execution cancelled") from None
                if err_outcome == "timeout":
                    logger.warning(
                        "engine=%s error=%s audit_id=%s",
                        "duckdb",
                        type(exc).__name__,
                        prepared.audit_id,
                    )
                    raise EngineTimeout("Engine query timed out") from None
                logger.error(
                    "engine=%s error=%s audit_id=%s",
                    "duckdb",
                    type(exc).__name__,
                    prepared.audit_id,
                )
                err_msg = str(exc)
                if (
                    "Out of Memory" in err_msg
                    or "temporary directory" in err_msg
                    or "cannot be offloaded" in err_msg
                ):
                    raise EngineError(self._resource_exhausted_message()) from None
                raise EngineError("Engine query execution failed") from None
        finally:
            req_state.disarm("error")
            if timer is not None:
                try:
                    timer.cancel()
                except Exception:
                    pass
                if (
                    (
                        timer_start_attempted
                        or getattr(timer, "is_alive", lambda: False)()
                    )
                    and hasattr(timer, "join")
                    and threading.current_thread() is not timer
                ):
                    try:
                        timer.join()
                    except Exception:
                        pass
            close_exc = req_state.close_connection(prepared.audit_id)
            with self._slots_lock:
                self._active_slots -= 1
            self._slot_semaphore.release()
            if close_exc is not None and completed_ok:
                raise EngineError("Engine connection cleanup failed") from None

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return QueryResult(
            columns=columns,
            rows=rows,
            truncated=truncated,
            engine=prepared.engine,
            elapsed_ms=elapsed_ms,
            column_types=column_types,
        )


class PooledConnection:
    """Wrapper holding connection creation and last used timestamps."""

    def __init__(self, conn: Any, created_at: float) -> None:
        self.conn = conn
        self.created_at = created_at
        self.last_used_at = created_at


class PostgresPool:
    """Thread-safe connection pool for PostgreSQL-wire (postgres and lakebase) connections."""

    def __init__(
        self,
        connect_fn: Callable[..., Any],
        *,
        pool_max: int = 8,
        pool_min: int = 0,
        connect_timeout_seconds: float = 10.0,
        max_idle_seconds: float = 300.0,
        max_lifetime_seconds: float = 1800.0,
        acquire_timeout_seconds: float = 5.0,
    ) -> None:
        self._connect_fn = connect_fn
        self._pool_max = pool_max
        self._pool_min = pool_min
        self._connect_timeout_seconds = connect_timeout_seconds
        self._max_idle_seconds = max_idle_seconds
        self._max_lifetime_seconds = max_lifetime_seconds
        self._acquire_timeout_seconds = acquire_timeout_seconds

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._idle: list[PooledConnection] = []
        self._in_use: dict[Any, PooledConnection] = {}
        self._total_count = 0
        self._closed = False

        self._reaper_lock = threading.Lock()
        self._reaper_thread: threading.Thread | None = None
        self._stop_reaper = threading.Event()

        if self._pool_min > 0:
            with self._lock:
                for _ in range(self._pool_min):
                    try:
                        conn = self._create_connection()
                        now = time.monotonic()
                        self._idle.append(PooledConnection(conn, now))
                        self._total_count += 1
                    except Exception:
                        break

    def _ensure_reaper_started(self) -> None:
        with self._reaper_lock:
            if self._reaper_thread is None and not self._closed:
                self._stop_reaper.clear()
                self._reaper_thread = threading.Thread(
                    target=self._reaper_loop,
                    daemon=True,
                    name="postgres-reaper",
                )
                self._reaper_thread.start()

    def _reaper_loop(self) -> None:
        interval = max(0.05, min(self._max_idle_seconds / 2.0, 10.0))
        while not self._stop_reaper.wait(timeout=interval):
            if self._closed:
                break
            to_close = []
            now = time.monotonic()
            with self._cond:
                remaining = []
                for pooled in self._idle:
                    if (
                        (now - pooled.last_used_at) > self._max_idle_seconds
                        or (now - pooled.created_at) > self._max_lifetime_seconds
                    ):
                        self._total_count -= 1
                        to_close.append(pooled.conn)
                    else:
                        remaining.append(pooled)
                self._idle = remaining
                if to_close:
                    self._cond.notify_all()
            for c in to_close:
                self._close_conn(c)

    def _create_connection(self, *, deadline: float | None = None) -> Any:
        try:
            sig = inspect.signature(self._connect_fn)
            params = sig.parameters
            call_kwargs: dict[str, Any] = {}
            if "connect_timeout" in params or any(
                p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
            ):
                call_kwargs["connect_timeout"] = self._connect_timeout_seconds
            if deadline is not None and "deadline" in params:
                call_kwargs["deadline"] = deadline
            if call_kwargs:
                return self._connect_fn(**call_kwargs)
            return self._connect_fn()
        except TypeError:
            try:
                return self._connect_fn(connect_timeout=self._connect_timeout_seconds)
            except TypeError:
                return self._connect_fn()

    def _close_conn(self, conn: Any) -> None:
        try:
            conn.close()
        except Exception:
            pass

    def _is_dead(self, pooled: PooledConnection, now: float) -> bool:
        conn = pooled.conn
        # Check closed safely across standard DB-API and psycopg2
        closed = getattr(conn, "closed", None)
        if closed is not None:
            if callable(closed):
                try:
                    if closed():
                        return True
                except Exception:
                    return True
            elif isinstance(closed, bool):
                if closed:
                    return True
            elif isinstance(closed, (int, float)):
                if closed != 0:
                    return True

        if (
            getattr(conn, "is_dead", False)
            or getattr(conn, "broken", False)
            or getattr(conn, "dead", False)
        ):
            return True

        open_attr = getattr(conn, "open", None)
        if open_attr is not None and isinstance(open_attr, bool) and not open_attr:
            return True

        # In psycopg2, TRANSACTION_STATUS_UNKNOWN (4) means broken connection
        if hasattr(conn, "get_transaction_status"):
            try:
                if conn.get_transaction_status() == 4:
                    return True
            except Exception:
                return True

        # Cheap poll validation only when connection has been idle longer than threshold
        idle_time = now - pooled.last_used_at
        if idle_time > 10.0:
            if hasattr(conn, "poll"):
                try:
                    conn.poll()
                except Exception:
                    return True
        return False

    def acquire(self, *, deadline: float | None = None) -> Any:
        self._ensure_reaper_started()
        acq_deadline = time.monotonic() + self._acquire_timeout_seconds
        to_close_expired = []
        with self._cond:
            while True:
                if self._closed:
                    raise EngineError("Connection pool is closed")

                now = time.monotonic()
                while self._idle:
                    pooled = self._idle.pop()
                    if (
                        (now - pooled.created_at) > self._max_lifetime_seconds
                        or (now - pooled.last_used_at) > self._max_idle_seconds
                    ):
                        self._total_count -= 1
                        to_close_expired.append(pooled.conn)
                        continue
                    if self._is_dead(pooled, now):
                        self._total_count -= 1
                        to_close_expired.append(pooled.conn)
                        continue

                    self._in_use[pooled.conn] = pooled
                    for c in to_close_expired:
                        self._close_conn(c)
                    return pooled.conn

                if self._total_count < self._pool_max:
                    self._total_count += 1
                    break

                remaining = acq_deadline - time.monotonic()
                if remaining <= 0:
                    for c in to_close_expired:
                        self._close_conn(c)
                    raise EnginePoolExhausted(
                        f"engine_busy: Timed out after {self._acquire_timeout_seconds}s waiting for a Lakebase pooled connection"
                    )
                self._cond.wait(timeout=remaining)
                if (
                    time.monotonic() >= acq_deadline
                    and not self._idle
                    and self._total_count >= self._pool_max
                ):
                    for c in to_close_expired:
                        self._close_conn(c)
                    raise EnginePoolExhausted(
                        f"engine_busy: Timed out after {self._acquire_timeout_seconds}s waiting for a Lakebase pooled connection"
                    )

        for c in to_close_expired:
            self._close_conn(c)

        try:
            conn = self._create_connection(deadline=deadline)
        except Exception:
            with self._cond:
                self._total_count -= 1
                self._cond.notify()
            raise

        now = time.monotonic()
        with self._cond:
            if self._closed:
                self._total_count -= 1
                self._close_conn(conn)
                raise EngineError("Connection pool is closed")
            self._in_use[conn] = PooledConnection(conn, now)
            return conn

    def release(self, conn: Any, *, success: bool) -> None:
        pooled = None
        with self._cond:
            pooled = self._in_use.pop(conn, None)
            if self._closed:
                self._total_count -= 1
                self._cond.notify()
                self._close_conn(conn)
                return

        if not success:
            with self._cond:
                self._total_count -= 1
                self._cond.notify()
            self._close_conn(conn)
            return

        now = time.monotonic()
        with self._cond:
            if self._closed:
                self._total_count -= 1
                self._cond.notify()
                self._close_conn(conn)
                return

            if pooled is None:
                pooled = PooledConnection(conn, now)
            else:
                pooled.last_used_at = now

            self._idle.append(pooled)
            self._cond.notify()

    def close(self) -> None:
        self._stop_reaper.set()
        to_close = []
        with self._cond:
            self._closed = True
            while self._idle:
                p = self._idle.pop()
                to_close.append(p.conn)
            for c in list(self._in_use.keys()):
                to_close.append(c)
            self._in_use.clear()
            self._total_count = 0
            self._cond.notify_all()
        for c in to_close:
            self._close_conn(c)

    def stats(self) -> dict[str, int]:
        """Return read-only connection pool counts."""
        with self._lock:
            return {
                "in_use": len(self._in_use),
                "idle": len(self._idle),
                "total": self._total_count,
                "pool_max": self._pool_max,
            }


LakebasePool = PostgresPool


class DBAPIAdapter:
    """Adapter for DB-API 2.0 drivers (pymysql, psycopg2) for Postgres, Lakebase, StarRocks, and Cube."""

    def __init__(
        self,
        connect: Callable[..., Any],
        *,
        kind: str,
        before_connect: Callable[[], None] | None = None,
        pool: PostgresPool | None = None,
        read_only: bool = False,
        session_config: PostgresSessionConfig | None = None,
        account: str | None = None,
        lakebase_config: Any = None,
        storage_class: str | None = None,
        lakebase_endpoint: Mapping[str, str] | None = None,
        cold_start_connector: Any = None,
    ) -> None:
        if kind not in ("postgres", "lakebase", "starrocks", "cube"):
            raise ValueError(f"Unsupported DBAPIAdapter kind: '{kind}'")
        self._connect = connect
        self._kind = kind
        self._before_connect = before_connect
        self._pool = pool
        self._read_only = bool(read_only)
        self._session_config = session_config
        self._account = account
        self._lakebase_config = lakebase_config
        self._cold_start_connector = (
            cold_start_connector
            if cold_start_connector is not None
            else getattr(connect, "cold_start_connector", None)
        )
        self.storage_class: str | None = storage_class
        self.lakebase_endpoint: dict[str, str] | None = (
            dict(lakebase_endpoint)
            if lakebase_endpoint is not None
            else (
                {
                    "project_id": str(lakebase_config.project_id),
                    "branch_id": str(lakebase_config.branch_id),
                    "endpoint_id": str(lakebase_config.endpoint_id),
                    "endpoint_type": str(lakebase_config.endpoint_type),
                }
                if (
                    kind == "lakebase"
                    and lakebase_config is not None
                    and hasattr(lakebase_config, "project_id")
                    and hasattr(lakebase_config, "branch_id")
                    and hasattr(lakebase_config, "endpoint_id")
                    and hasattr(lakebase_config, "endpoint_type")
                )
                else None
            )
        )
        self.last_cold_start: bool | None = None

    @property
    def kind(self) -> str:
        return self._kind

    @property
    def pool(self) -> PostgresPool | None:
        """Return the underlying PostgreSQL-wire connection pool, if configured."""
        return self._pool

    @property
    def read_only(self) -> bool:
        return self._read_only

    @property
    def session_config(self) -> PostgresSessionConfig | None:
        return self._session_config

    @property
    def account(self) -> str | None:
        return self._account

    @account.setter
    def account(self, value: str | None) -> None:
        self._account = value

    def pool_stats(self) -> dict[str, int] | None:
        """Return read-only PostgreSQL-wire connection pool counts, or None when unpooled."""
        if self._pool is None:
            return None
        return self._pool.stats()

    def measure_snapshot_freshness(self) -> float | None:
        """Measure read-only Lakebase snapshot freshness in seconds via the adapter."""
        if self._kind != "lakebase" or self._lakebase_config is None:
            return None
        if self._pool is not None:
            conn = self._pool.acquire()
            ok = False
            try:
                lag = snapshot_freshness(conn, self._lakebase_config)
                if hasattr(conn, "rollback") and callable(conn.rollback):
                    conn.rollback()
                ok = True
                return lag
            finally:
                self._pool.release(conn, success=ok)
        conn = self._connect()
        try:
            return snapshot_freshness(conn, self._lakebase_config)
        finally:
            if hasattr(conn, "close") and callable(conn.close):
                try:
                    conn.close()
                except Exception:
                    pass

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()

    def check_health(self, timeout_seconds: float = 2.0) -> bool:
        """Probe engine reachability via a short-lived direct connection outside the query pool."""
        try:
            if (
                isinstance(timeout_seconds, bool)
                or not isinstance(timeout_seconds, (int, float))
                or math.isnan(timeout_seconds)
                or math.isinf(timeout_seconds)
                or timeout_seconds <= 0
            ):
                return False

            if self._before_connect is not None:
                self._before_connect()

            to_int = max(1, math.ceil(float(timeout_seconds)))
            accepts_kw = True
            try:
                sig = inspect.signature(self._connect)
                accepts_kw = "connect_timeout" in sig.parameters or any(
                    p.kind == inspect.Parameter.VAR_KEYWORD
                    for p in sig.parameters.values()
                )
            except (ValueError, TypeError):
                accepts_kw = True

            if accepts_kw:
                try:
                    conn = self._connect(connect_timeout=to_int)
                except TypeError:
                    conn = self._connect()
            else:
                conn = self._connect()

            if conn is None:
                return False

            cur: Any = None
            try:
                if hasattr(conn, "cursor") and callable(conn.cursor):
                    cur = conn.cursor()
                    if (
                        cur is not None
                        and hasattr(cur, "execute")
                        and callable(cur.execute)
                    ):
                        cur.execute("SELECT 1")
                return True
            finally:
                if cur is not None and hasattr(cur, "close") and callable(cur.close):
                    try:
                        cur.close()
                    except Exception:
                        pass
                if hasattr(conn, "close") and callable(conn.close):
                    try:
                        conn.close()
                    except Exception:
                        pass
        except Exception:
            return False

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
        if self._read_only and prepared.row_limit is None:
            raise EngineError("Cannot execute write statement on read-only source")

        if self._before_connect is not None:
            try:
                self._before_connect()
            except Exception as exc:
                logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
                raise EngineError("Engine connection preparation failed") from exc

        start = time.perf_counter()
        conn = None
        has_cs_connector = (
            self._kind == "lakebase" and self._cold_start_connector is not None
        )
        if has_cs_connector:
            reset_fn = getattr(
                self._cold_start_connector, "reset_thread_cold_start", None
            )
            if callable(reset_fn):
                reset_fn()
            query_cold_start: bool | None = False
            self.last_cold_start = False
        else:
            query_cold_start = None
            self.last_cold_start = None
        is_pg_wire = is_postgres_wire(self._kind)
        conn_is_pooled = is_pg_wire and self._pool is not None
        hardened_pg = (
            self._kind == "postgres"
            or self._session_config is not None
            or self._read_only
        )
        req_timeout_ms = max(1, int(round(timeout_seconds * 1000)))
        effective_timeout_ms = (
            min(req_timeout_ms, int(self._session_config.statement_timeout_ms))
            if self._session_config is not None
            else req_timeout_ms
        )
        workload_label = (
            getattr(prepared, "workload", None)
            or self._account
            or "interactive"
        )
        has_row_limit = prepared.row_limit is not None
        query_success = False
        cursor = None
        cancel_event = threading.Event()
        timed_out = threading.Event()
        cancel_called = False
        cancel_lock = threading.Lock()
        finished = False
        cube_timer: threading.Timer | None = None

        if conn_is_pooled:
            query_deadline = time.monotonic() + float(timeout_seconds)
            max_attempts = 2
            for attempt in range(max_attempts):
                try:
                    conn = self._pool.acquire(deadline=query_deadline)
                    if has_cs_connector:
                        consume_fn = getattr(
                            self._cold_start_connector,
                            "consume_thread_cold_start",
                            None,
                        )
                        if callable(consume_fn):
                            woke = consume_fn()
                            if woke is True:
                                query_cold_start = True
                        elif bool(
                            getattr(
                                self._cold_start_connector,
                                "last_cold_start",
                                False,
                            )
                        ):
                            query_cold_start = True
                        self.last_cold_start = query_cold_start
                    cursor = conn.cursor()

                    def cancel_lb() -> None:
                        nonlocal cancel_called
                        with cancel_lock:
                            if finished or cancel_called:
                                return
                            cancel_called = True
                        cancel_event.set()
                        try:
                            conn.cancel()
                        except Exception:
                            pass

                    if on_cancel_handle is not None:
                        on_cancel_handle(cancel_lb)

                    begin_query_transaction(
                        cursor,
                        read_only=self._read_only,
                        timeout_ms=effective_timeout_ms,
                        workload=workload_label,
                        audit_id=prepared.audit_id,
                        hardened=hardened_pg,
                        has_row_limit=has_row_limit,
                    )
                    break
                except EnginePoolExhausted:
                    raise
                except Exception as exc:
                    if cursor is not None:
                        try:
                            cursor.close()
                        except Exception:
                            pass
                        cursor = None
                    if conn is not None:
                        self._pool.release(conn, success=False)
                        conn = None
                    if (
                        isinstance(exc, EngineStartingError)
                        or getattr(exc, "code", None) == "engine_starting"
                    ):
                        if has_cs_connector:
                            self.last_cold_start = True
                        raise
                    if has_cs_connector:
                        consume_fn = getattr(
                            self._cold_start_connector,
                            "consume_thread_cold_start",
                            None,
                        )
                        if callable(consume_fn) and consume_fn() is True:
                            self.last_cold_start = True
                    if isinstance(exc, EngineError) and (
                        "Role guard refused connection" in str(exc)
                        or "Session hardening" in str(exc)
                    ):
                        raise
                    if attempt < max_attempts - 1:
                        continue
                    logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
                    raise EngineError("Engine connection failed") from exc
        else:
            try:
                conn = self._connect()
                if has_cs_connector:
                    consume_fn = getattr(
                        self._cold_start_connector,
                        "consume_thread_cold_start",
                        None,
                    )
                    if callable(consume_fn):
                        query_cold_start = bool(consume_fn())
                    else:
                        query_cold_start = bool(
                            getattr(
                                self._cold_start_connector,
                                "last_cold_start",
                                False,
                            )
                        )
                    self.last_cold_start = query_cold_start
            except Exception as exc:
                if (
                    isinstance(exc, EngineStartingError)
                    or getattr(exc, "code", None) == "engine_starting"
                ):
                    if has_cs_connector:
                        self.last_cold_start = True
                    raise
                if has_cs_connector:
                    consume_fn = getattr(
                        self._cold_start_connector,
                        "consume_thread_cold_start",
                        None,
                    )
                    if callable(consume_fn) and consume_fn() is True:
                        self.last_cold_start = True
                if isinstance(exc, EngineError) and (
                    "Role guard refused connection" in str(exc)
                    or "Session hardening" in str(exc)
                ):
                    raise
                logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
                raise EngineError("Engine connection failed") from exc

        try:
            try:
                if cursor is None:
                    cursor = conn.cursor()
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

                    elif is_pg_wire:
                        def cancel_lb_unpooled() -> None:
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
                            on_cancel_handle(cancel_lb_unpooled)

                        begin_query_transaction(
                            cursor,
                            read_only=self._read_only,
                            timeout_ms=effective_timeout_ms,
                            workload=workload_label,
                            audit_id=prepared.audit_id,
                            hardened=hardened_pg,
                            has_row_limit=has_row_limit,
                        )

                    elif self._kind == "cube":
                        def cancel_cube() -> None:
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
                            on_cancel_handle(cancel_cube)

                        def timeout_cube() -> None:
                            timed_out.set()
                            try:
                                conn.cancel()
                            except Exception:
                                pass

                        cube_timer = threading.Timer(timeout_seconds, timeout_cube)
                        cube_timer.start()

                cursor.execute(prepared.sql, prepared.params)
                if cancel_event.is_set():
                    logger.error("engine=%s error=%s audit_id=%s", self._kind, "EngineCancelled", prepared.audit_id)
                    raise EngineCancelled("Engine query execution cancelled")
                if timed_out.is_set():
                    logger.warning("engine=%s error=%s audit_id=%s", self._kind, "EngineTimeout", prepared.audit_id)
                    raise EngineTimeout("Engine query timed out")

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
                    if conn_is_pooled:
                        if hasattr(conn, "rollback"):
                            conn.rollback()

                query_success = True
            finally:
                if cube_timer is not None:
                    cube_timer.cancel()
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception:
                        pass
        except Exception as exc:
            if cancel_event.is_set():
                logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
                raise EngineCancelled("Engine query execution cancelled") from exc
            if timed_out.is_set():
                logger.warning("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
                raise EngineTimeout("Engine query timed out") from exc
            logger.error("engine=%s error=%s audit_id=%s", self._kind, type(exc).__name__, prepared.audit_id)
            if _is_timeout_error(exc):
                raise EngineTimeout("Engine query timed out") from exc
            if isinstance(exc, (EngineTimeout, EngineCancelled, EngineError)):
                raise
            raise EngineError("Engine query execution failed") from exc
        finally:
            with cancel_lock:
                finished = True
            if conn_is_pooled:
                if conn is not None:
                    self._pool.release(conn, success=query_success)
            else:
                if conn is not None:
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
            cold_start=query_cold_start,
        )


class CubeAdapter:
    """Adapter for Cube SQL API execution with JWT security context."""

    def __init__(
        self,
        connect: Callable[..., Any],
        security: CubeSecurityConfig,
        *,
        connect_timeout_seconds: float = 10,
    ) -> None:
        self._connect = connect
        self._security = security
        self._connect_timeout_seconds = connect_timeout_seconds
        self.storage_class: str | None = None
        self.account: str | None = None
        self.lakebase_endpoint: dict[str, str] | None = None
        self.last_cold_start: bool | None = None

    def execute(
        self,
        prepared: PreparedQuery,
        *,
        timeout_seconds: float,
        on_cancel_handle: Callable[[Callable[[], None]], None] | None = None,
    ) -> QueryResult:
        if prepared.cube_security_context is None:
            raise EngineError("Cube engine query requires cube_security_context")

        token = make_cube_context_token(prepared, self._security)
        start = time.perf_counter()

        timeout_int = max(1, math.ceil(float(self._connect_timeout_seconds)))
        try:
            conn = self._connect(
                user=self._security.sql_user,
                password=token,
                connect_timeout=timeout_int,
            )
        except Exception as exc:
            logger.error("engine=%s error=%s audit_id=%s", "cube", type(exc).__name__, prepared.audit_id)
            raise EngineError("Engine connection failed") from None

        cancel_event = threading.Event()
        timed_out = threading.Event()
        cancel_called = False
        cancel_lock = threading.Lock()

        def cancel_cube() -> None:
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
            on_cancel_handle(cancel_cube)

        def timeout_cube() -> None:
            timed_out.set()
            try:
                conn.cancel()
            except Exception:
                pass

        timer = threading.Timer(timeout_seconds, timeout_cube)
        timer.start()

        try:
            cursor = conn.cursor()
            try:
                cursor.execute(prepared.sql, prepared.params)
                if cancel_event.is_set():
                    logger.error("engine=%s error=%s audit_id=%s", "cube", "EngineCancelled", prepared.audit_id)
                    raise EngineCancelled("Engine query execution cancelled")
                if timed_out.is_set():
                    logger.warning("engine=%s error=%s audit_id=%s", "cube", "EngineTimeout", prepared.audit_id)
                    raise EngineTimeout("Engine query timed out")

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
                        tuple(normalize_column_type("cube", d) for d in cursor.description)
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
                logger.error("engine=%s error=%s audit_id=%s", "cube", type(exc).__name__, prepared.audit_id)
                raise EngineCancelled("Engine query execution cancelled") from None
            if timed_out.is_set():
                logger.warning("engine=%s error=%s audit_id=%s", "cube", type(exc).__name__, prepared.audit_id)
                raise EngineTimeout("Engine query timed out") from None
            logger.error("engine=%s error=%s audit_id=%s", "cube", type(exc).__name__, prepared.audit_id)
            if _is_timeout_error(exc):
                raise EngineTimeout("Engine query timed out") from None
            if isinstance(exc, (EngineTimeout, EngineCancelled, EngineError)):
                raise
            raise EngineError("Engine query execution failed") from None
        finally:
            timer.cancel()
            try:
                conn.close()
            except Exception as exc:
                logger.warning("engine=%s error=%s audit_id=%s", "cube", type(exc).__name__, prepared.audit_id)

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

    def __init__(
        self,
        by_workload: Mapping[str, EngineAdapter],
        *,
        storage_class: str | None = None,
        lakebase_endpoint: Mapping[str, str] | None = None,
    ) -> None:
        self.by_workload = dict(by_workload)
        self._storage_class: str | None = storage_class
        self._lakebase_endpoint: dict[str, str] | None = (
            dict(lakebase_endpoint) if lakebase_endpoint is not None else None
        )
        self.account: str | None = (
            "interactive"
            if "interactive" in self.by_workload
            else (next(iter(self.by_workload)) if self.by_workload else None)
        )
        self.last_cold_start: bool | None = None
        if self._storage_class is not None:
            for sub in self.by_workload.values():
                setattr(sub, "storage_class", self._storage_class)
        if self._lakebase_endpoint is not None:
            for sub in self.by_workload.values():
                setattr(sub, "lakebase_endpoint", dict(self._lakebase_endpoint))

    @property
    def storage_class(self) -> str | None:
        return self._storage_class

    @storage_class.setter
    def storage_class(self, value: str | None) -> None:
        self._storage_class = value
        for sub in self.by_workload.values():
            setattr(sub, "storage_class", value)

    @property
    def lakebase_endpoint(self) -> dict[str, str] | None:
        return self._lakebase_endpoint

    @lakebase_endpoint.setter
    def lakebase_endpoint(self, value: dict[str, str] | None) -> None:
        self._lakebase_endpoint = dict(value) if value is not None else None
        for sub in self.by_workload.values():
            setattr(
                sub,
                "lakebase_endpoint",
                dict(value) if value is not None else None,
            )

    def close(self) -> None:
        seen_ids: set[int] = set()
        for sub in self.by_workload.values():
            if id(sub) in seen_ids:
                continue
            seen_ids.add(id(sub))
            if hasattr(sub, "close") and callable(sub.close):
                sub.close()

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
        self.account = prepared.workload
        sig = inspect.signature(adapter.execute)
        try:
            if "on_cancel_handle" in sig.parameters:
                return adapter.execute(
                    prepared,
                    timeout_seconds=timeout_seconds,
                    on_cancel_handle=on_cancel_handle,
                )
            return adapter.execute(prepared, timeout_seconds=timeout_seconds)
        finally:
            self.last_cold_start = getattr(adapter, "last_cold_start", None)


def build_workload_adapters(
    kind: str,
    connects: Mapping[str, Callable[..., Any]],
    *,
    pools: Mapping[str, PostgresPool] | None = None,
    **kw: Any,
) -> WorkloadRoutedAdapter:
    """Build a WorkloadRoutedAdapter for a given engine kind across workloads."""
    by_workload = {
        w: build_adapter(
            kind,
            conn,
            pool=pools.get(w) if pools is not None else kw.get("pool"),
            account=w,
            **{k: v for k, v in kw.items() if k not in ("pool", "account")},
        )
        for w, conn in connects.items()
    }
    return WorkloadRoutedAdapter(by_workload)


def build_adapter(
    kind: str,
    connect: Callable[..., Any] | None = None,
    *,
    config: Any = None,
    pool: PostgresPool | None = None,
    read_only: bool = False,
    session_config: PostgresSessionConfig | None = None,
    account: str | None = None,
    **kw: Any,
) -> EngineAdapter:
    """Small factory to build an engine adapter for a given engine kind."""
    if kind == "duckdb":
        return DuckDBAdapter(connect, config=config, **kw)
    if kind == "cube":
        if "security" in kw and kw["security"] is not None:
            security = kw.pop("security")
            return CubeAdapter(connect, security=security, **kw)
        return DBAPIAdapter(connect, kind=kind, **kw)
    if kind in ("postgres", "lakebase"):
        return DBAPIAdapter(
            connect,
            kind=kind,
            pool=pool,
            read_only=read_only,
            session_config=session_config,
            account=account,
            **kw,
        )
    if kind == "starrocks":
        return DBAPIAdapter(connect, kind=kind, account=account, **kw)
    raise ValueError(f"Unsupported engine kind: '{kind}'")
