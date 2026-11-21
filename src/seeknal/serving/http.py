"""Serving HTTP API: Starlette application exposing query, explain, and healthz endpoints."""

from __future__ import annotations

import asyncio
import base64
import datetime
import hashlib
import heapq
import hmac
import inspect
import json
import logging
import math
import os
import re
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any

import sqlglot
from sqlglot import exp
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from seeknal.serving.adapters import (
    EngineAdapter,
    EngineCancelled,
    EngineConfigError,
    EngineError,
    EnginePoolExhausted,
    EngineTimeout,
)
from seeknal.serving.audit import (
    ALLOWED_AUDIT_ROUTES,
    AuditManager,
    AuditSink,
    AuditWriteError,
    build_audit_record,
    build_minimal_audit_record,
    emit_audit_record,
    get_audit_runtime_tracker,
)
from seeknal.serving.enforce import (
    EnforcementError,
    Grant,
)
from seeknal.serving.errors import (
    ERROR_CATALOGUE,
    AuditUnavailableError,
    _SQL_SINGLE_QUOTED_RE,
    _is_purely_numeric_value,
    _redact_bound_params,
    _sanitize_log_value,
    build_error_payload,
    get_error_spec,
    register_config_secrets,
    scrub_error_detail,
)
from seeknal.serving.grant_token import (
    GrantTokenError,
    verify_cancel_token,
    verify_grant_token,
)
from seeknal.serving.keyset import CursorError, encode_cursor
from seeknal.serving.metrics import MetricsRegistry
from seeknal.serving.router import RoutingError
from seeknal.serving.select_api import (
    CompiledSelect,
    SelectInvalidError,
    compile_select_request,
    reject_duplicate_json_keys,
)
from seeknal.serving.service import (
    PreparedQuery,
    ServingPipeline,
)

logger = logging.getLogger(__name__)

SQL_LITERAL_PATTERN = re.compile(r"'([^']*)'")
WORKLOAD_ACCOUNT_MISSING_RE = re.compile(
    r"^no engine account configured for workload '[a-zA-Z0-9_-]+'$"
)
SAFE_FIELD_NAME_RE = re.compile(r"^[a-z0-9_]{1,64}$")
SAFE_ENGINE_ERROR_MESSAGES = frozenset(
    {
        "Engine connection failed",
        "Engine query execution failed",
        "Engine connection cleanup failed",
        "Engine connection preparation failed",
        "Engine resource limit exceeded",
    }
)
_MAX_AUDIT_SQL_EXTRACT_BYTES = 64 * 1024
_MAX_AUDIT_TABLES = 64
_MAX_AUDIT_TABLE_NAME_LEN = 256
_LONE_SURROGATE_RE = re.compile(r"[\ud800-\udfff]")


class DuplicateQueryIdError(ValueError):
    """Raised when a query with the same jti is already running."""


class GrantReplayedError(ValueError):
    """Raised when a grant token with the same jti is replayed."""


class GrantReplayCacheFull(RuntimeError):
    """Raised when the grant replay cache has reached maximum capacity."""


class GrantReplayCache:
    """Thread-safe cache mapping grant jti to expiry time with leeway."""

    def __init__(self, max_entries: int = 100_000, leeway_seconds: float = 5.0) -> None:
        self.max_entries = int(max_entries)
        self.leeway_seconds = float(leeway_seconds)
        self._lock = threading.Lock()
        self._cache: dict[str, float] = {}
        self._heap: list[tuple[float, str]] = []

    def check_and_record(
        self, jti: str, expires_at: float, now: float | None = None
    ) -> None:
        current_time = time.time() if now is None else float(now)
        record_expiry = float(expires_at) + self.leeway_seconds

        with self._lock:
            # 1. Prune expired entries from the top of the min-heap
            while self._heap and self._heap[0][0] <= current_time:
                exp, old_jti = heapq.heappop(self._heap)
                if self._cache.get(old_jti) == exp:
                    del self._cache[old_jti]

            # 2. Check if jti is present and not yet expired
            if jti in self._cache:
                if self._cache[jti] > current_time:
                    raise GrantReplayedError(f"Grant '{jti}' has already been used")
                else:
                    del self._cache[jti]

            # 3. Check capacity after pruning (fail closed)
            if len(self._cache) >= self.max_entries:
                raise GrantReplayCacheFull(
                    f"Grant replay cache is full (max {self.max_entries} entries)"
                )

            # 4. Record entry
            self._cache[jti] = record_expiry
            heapq.heappush(self._heap, (record_expiry, jti))


class _RunningQueryEntry:
    def __init__(self, subject: str) -> None:
        self.subject = subject
        self.cancel_func: Callable[[], None] | None = None
        self.cancelled = False


class RunningQueries:
    """Thread-safe registry mapping jti to (subject, cancel callable)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._queries: dict[str, _RunningQueryEntry] = {}

    def register(self, jti: str, subject: str) -> None:
        with self._lock:
            if jti in self._queries:
                raise DuplicateQueryIdError(f"Query '{jti}' is already running")
            self._queries[jti] = _RunningQueryEntry(subject)

    def set_cancel_handle(self, jti: str, cancel_func: Callable[[], None]) -> None:
        with self._lock:
            entry = self._queries.get(jti)
            if entry is None:
                return
            entry.cancel_func = cancel_func
            should_cancel = entry.cancelled
        if should_cancel:
            try:
                cancel_func()
            except Exception:
                pass

    def is_running(self, jti: str, subject: str) -> bool:
        """Check whether a query with matching jti and subject is currently running."""
        with self._lock:
            entry = self._queries.get(jti)
            return entry is not None and entry.subject == subject

    def cancel(self, jti: str, subject: str) -> bool:
        """Cancel query if found and subject matches. Returns True on match, False otherwise."""
        with self._lock:
            entry = self._queries.get(jti)
            if entry is None or entry.subject != subject:
                return False
            entry.cancelled = True
            func = entry.cancel_func
        if func is not None:
            try:
                func()
            except Exception:
                pass
        return True

    def unregister(self, jti: str) -> None:
        with self._lock:
            self._queries.pop(jti, None)


def _make_json_safe(val: Any) -> Any:
    """Recursively convert database values into JSON-safe types."""
    if val is None:
        return None
    if isinstance(val, (datetime.datetime, datetime.date, datetime.time)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return str(val)
    if isinstance(val, bytes):
        return base64.b64encode(val).decode("ascii")
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    if isinstance(val, (list, tuple)):
        return [_make_json_safe(v) for v in val]
    if isinstance(val, dict):
        return {k: _make_json_safe(v) for k, v in val.items()}
    return val


def _safe_message(
    msg_or_exc: Exception | str,
    *,
    token: str | None = None,
    sql: str | None = None,
    params: Any = None,
) -> str:
    """Ensure error message never contains grant token, SQL literals, or parameter values."""
    msg = str(msg_or_exc)
    if "sql parse error" in msg.lower():
        return "SQL parse error"

    if token and token in msg:
        msg = msg.replace(token, "[REDACTED]")

    if sql:
        if sql in msg:
            msg = "SQL query error"
        else:
            sql_literals: list[str] = []
            for m in _SQL_SINGLE_QUOTED_RE.finditer(sql):
                raw_inner = m.group(1)
                full_quoted = m.group(0)
                if full_quoted != "''":
                    sql_literals.append(full_quoted)
                unescaped = raw_inner.replace("''", "'")
                if len(raw_inner) >= 3 and not _is_purely_numeric_value(raw_inner):
                    sql_literals.append(raw_inner)
                if len(unescaped) >= 3 and not _is_purely_numeric_value(unescaped):
                    sql_literals.append(unescaped)
            for lit in sorted(set(sql_literals), key=len, reverse=True):
                if lit in msg:
                    msg = msg.replace(lit, "[REDACTED]")

    msg = _redact_bound_params(msg, params)

    return msg


def _is_audit_unavailable(exc: BaseException) -> bool:
    return (
        isinstance(exc, AuditUnavailableError)
        or getattr(exc, "code", None) == "audit_unavailable"
        or type(exc).__name__ == "AuditUnavailableError"
    )


def _is_engine_starting(exc: BaseException) -> bool:
    return (
        getattr(exc, "code", None) == "engine_starting"
        or type(exc).__name__ in ("EngineStartingError", "EngineStarting")
    )


def _retry_after_header_value(exc: BaseException) -> str:
    raw = getattr(exc, "retry_after", None)
    if raw is None:
        raw = getattr(exc, "retry_after_seconds", None)
    if raw is None or isinstance(raw, bool):
        return "3"
    if isinstance(raw, float):
        if math.isnan(raw) or math.isinf(raw) or raw <= 0:
            return "3"
        return str(max(1, int(math.ceil(raw))))
    if isinstance(raw, int):
        return str(max(1, raw))
    raw_str = str(raw).strip()
    return raw_str if raw_str else "3"


def _reject_json_constant(_val: str) -> Any:
    raise ValueError("Invalid JSON constant")


def _safe_grant_invalid_message(exc: GrantTokenError) -> str | None:
    msg = str(exc)
    if msg == "Missing or invalid 'typ' claim, expected 'grant'":
        return msg
    if msg.startswith("Token lifetime of ") and "exceeds maximum" in msg:
        return msg
    return None


def _extract_verified_grant_claims(
    token: str | None,
    pipeline: ServingPipeline,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    if not token:
        return None, None, None
    try:
        claims = verify_grant_token(
            token,
            pipeline.public_keys,
            audience=pipeline.audience,
            issuer=pipeline.issuer,
            require_grant_typ=pipeline.require_grant_typ,
            max_lifetime_seconds=pipeline.max_grant_lifetime_seconds,
        )
        jti = claims.get("jti")
        sub = claims.get("sub")
        return (
            claims,
            jti if isinstance(jti, str) and jti else None,
            sub if isinstance(sub, str) and sub else None,
        )
    except GrantTokenError as exc:
        jti = getattr(exc, "audit_id", None)
        sub = getattr(exc, "subject", None)
        return (
            None,
            jti if isinstance(jti, str) and jti else None,
            sub if isinstance(sub, str) and sub else None,
        )
    except Exception:
        return None, None, None


def _extract_verified_cancel_claims(
    token: str | None,
    pipeline: ServingPipeline,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    if not token:
        return None, None, None
    try:
        claims = verify_cancel_token(
            token,
            pipeline.public_keys,
            audience=pipeline.audience,
            issuer=pipeline.issuer,
        )
        jti = claims.get("jti")
        sub = claims.get("sub")
        return (
            claims,
            jti if isinstance(jti, str) and jti else None,
            sub if isinstance(sub, str) and sub else None,
        )
    except GrantTokenError as exc:
        jti = getattr(exc, "audit_id", None)
        sub = getattr(exc, "subject", None)
        return (
            None,
            jti if isinstance(jti, str) and jti else None,
            sub if isinstance(sub, str) and sub else None,
        )
    except Exception:
        return None, None, None


def _extract_verified_identity(
    token: str | None,
    pipeline: ServingPipeline,
) -> tuple[str | None, str | None]:
    _, jti, sub = _extract_verified_grant_claims(token, pipeline)
    return jti, sub


def _extract_logical_tables(
    sql: str | None,
    *,
    claims: Mapping[str, Any] | None = None,
    sql_dialect: str = "postgres",
    default_catalog: str | None = None,
    prepared: Any = None,
) -> list[str]:
    if prepared is not None:
        prep_tables = getattr(prepared, "tables", None)
        if prep_tables is not None:
            out_prep: list[str] = []
            seen_prep: set[str] = set()
            for raw_t in prep_tables:
                cleaned = _LONE_SURROGATE_RE.sub("\ufffd", str(raw_t)).strip()
                if not cleaned:
                    continue
                cleaned = cleaned[:_MAX_AUDIT_TABLE_NAME_LEN]
                if cleaned not in seen_prep:
                    seen_prep.add(cleaned)
                    out_prep.append(cleaned)
                    if len(out_prep) >= _MAX_AUDIT_TABLES:
                        break
            return out_prep

    if claims is None and prepared is None:
        return []
    if not sql or not isinstance(sql, str):
        return []
    if len(sql) > _MAX_AUDIT_SQL_EXTRACT_BYTES:
        return []
    if len(sql.encode("utf-8", errors="replace")) > _MAX_AUDIT_SQL_EXTRACT_BYTES:
        return []
    if not sql.strip():
        return []

    try:
        stmts = sqlglot.parse(sql, read=sql_dialect)
        if len(stmts) != 1 or stmts[0] is None:
            return []
        stmt = stmts[0]
        cte_names = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE)}
        tables: list[str] = []
        seen: set[str] = set()
        for tbl in stmt.find_all(exp.Table):
            raw_name = (tbl.name or "").strip()
            raw_db = (tbl.db or "").strip()
            raw_cat = (tbl.catalog or "").strip()
            if not raw_name and not raw_db and not raw_cat:
                continue
            if (raw_name.lower() in cte_names and not raw_db and not raw_cat) or (
                tbl.parts
                and ".".join(p.name for p in tbl.parts).strip().lower() in cte_names
            ):
                continue
            parts: list[str] = []
            if raw_cat:
                parts.append(raw_cat)
            elif default_catalog is not None and raw_db and raw_name:
                parts.append(default_catalog)
            if raw_db:
                parts.append(raw_db)
            if raw_name:
                parts.append(raw_name)
            dotted = ".".join(p for p in parts if p).strip().lower()
            if not dotted:
                continue
            dotted = _LONE_SURROGATE_RE.sub("\ufffd", dotted)[:_MAX_AUDIT_TABLE_NAME_LEN]
            if dotted and dotted not in seen:
                seen.add(dotted)
                tables.append(dotted)
                if len(tables) >= _MAX_AUDIT_TABLES:
                    break
        return tables
    except Exception:
        return []


async def _extract_logical_tables_async(
    sql: str | None,
    *,
    claims: Mapping[str, Any] | None = None,
    sql_dialect: str = "postgres",
    default_catalog: str | None = None,
    prepared: Any = None,
) -> list[str]:
    if prepared is not None and getattr(prepared, "tables", None) is not None:
        return _extract_logical_tables(
            sql,
            claims=claims,
            sql_dialect=sql_dialect,
            default_catalog=default_catalog,
            prepared=prepared,
        )
    if claims is None and prepared is None:
        return []
    if not sql or not isinstance(sql, str):
        return []
    if len(sql) > _MAX_AUDIT_SQL_EXTRACT_BYTES:
        return []
    if len(sql.encode("utf-8", errors="replace")) > _MAX_AUDIT_SQL_EXTRACT_BYTES:
        return []
    if not sql.strip():
        return []
    return await run_in_threadpool(
        _extract_logical_tables,
        sql,
        claims=claims,
        sql_dialect=sql_dialect,
        default_catalog=default_catalog,
        prepared=prepared,
    )


def _error_response(
    code: str,
    *,
    status_code: int | None = None,
    category: str | None = None,
    message: Exception | str | None = None,
    audit_id: str | None = None,
    token: str | None = None,
    sql: str | None = None,
    params: Any = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    spec = get_error_spec(code)
    resolved_status = status_code if status_code is not None else spec.status
    resolved_category = category if category is not None else spec.category
    if message is not None:
        resolved_msg = _safe_message(
            message, token=token, sql=sql, params=params
        )
    else:
        resolved_msg = spec.message
    resolved_msg = _LONE_SURROGATE_RE.sub("\ufffd", resolved_msg)
    safe_audit_id = (
        _LONE_SURROGATE_RE.sub("\ufffd", audit_id)
        if isinstance(audit_id, str) and audit_id
        else None
    )
    _, body = build_error_payload(
        spec.code,
        category=resolved_category,
        message=resolved_msg,
        audit_id=safe_audit_id,
    )
    return JSONResponse(body, status_code=resolved_status, headers=headers)


async def _read_body_bounded(request: Request, max_bytes: int) -> bytes | None:
    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            return None
        if chunk:
            chunks.append(chunk)
    return b"".join(chunks)


def _log_request(
    *,
    audit_id: str | None = None,
    subject: str | None = None,
    engine: str | None = None,
    rule: str | None = None,
    status: int = 200,
    elapsed_ms: float = 0.0,
    row_count: int | str = "-",
    publication_id: str | None = None,
    revision: int | str | None = None,
    digest: str | None = None,
    binding: Mapping[str, Any] | None = None,
    claims: Mapping[str, Any] | None = None,
    workload: str | None = None,
    azp: str | None = None,
    origin: str | None = None,
    act_sub: str | None = None,
    act_typ: str | None = None,
) -> None:
    if binding and isinstance(binding, Mapping):
        if publication_id is None and "publication_id" in binding:
            publication_id = binding["publication_id"]
        if revision is None and "revision" in binding:
            revision = binding["revision"]
        if digest is None and "digest" in binding:
            digest = binding["digest"]
        if workload is None and "workload" in binding:
            workload = binding["workload"]
        if azp is None and "azp" in binding:
            azp = binding["azp"]
        if origin is None and "origin" in binding:
            origin = binding["origin"]
        if act_sub is None and "act_sub" in binding:
            act_sub = binding["act_sub"]
        if act_typ is None and "act_typ" in binding:
            act_typ = binding["act_typ"]

    if claims and isinstance(claims, Mapping):
        if publication_id is None and isinstance(claims.get("publication_id"), str):
            publication_id = claims["publication_id"]
        if revision is None and "revision" in claims:
            c_rev = claims["revision"]
            if (isinstance(c_rev, int) and not isinstance(c_rev, bool)) or isinstance(c_rev, str):
                revision = c_rev
        if digest is None and isinstance(claims.get("digest"), str):
            digest = claims["digest"]
        if workload is None and isinstance(claims.get("workload"), str):
            workload = claims["workload"]
        if azp is None and isinstance(claims.get("azp"), str):
            azp = claims["azp"]
        if origin is None and isinstance(claims.get("origin"), str):
            origin = claims["origin"]
        act = claims.get("act")
        if isinstance(act, Mapping):
            if act_sub is None and isinstance(act.get("sub"), str):
                act_sub = act["sub"]
            if act_typ is None and isinstance(act.get("typ"), str):
                act_typ = act["typ"]

    fmt_parts = [
        "request audit_id=%s subject=%s engine=%s rule=%s status=%d elapsed_ms=%.2f row_count=%s"
    ]
    log_args: list[Any] = [
        _sanitize_log_value(audit_id),
        _sanitize_log_value(subject),
        _sanitize_log_value(engine),
        _sanitize_log_value(rule),
        int(status),
        float(elapsed_ms),
        _sanitize_log_value(row_count),
    ]

    for key_name, val in (
        ("publication_id", publication_id),
        ("revision", revision),
        ("digest", digest),
        ("workload", workload),
        ("azp", azp),
        ("origin", origin),
        ("act_sub", act_sub),
        ("act_typ", act_typ),
    ):
        if val is not None:
            fmt_parts.append(f"{key_name}=%s")
            log_args.append(_sanitize_log_value(val))

    logger.info(" ".join(fmt_parts), *log_args)


def _resolve_adapter_audit_metadata(
    adapter: Any,
    *,
    engine_kind: str | None = None,
    workload: str | None = None,
    result: Any = None,
    include_cold_start: bool = True,
) -> tuple[str | None, str | None, Any, Any]:
    """Resolve (storage_class, account, lakebase_endpoint, cold_start) for audit logging."""
    target = adapter
    has_by_workload = False
    if adapter is not None and hasattr(adapter, "by_workload"):
        by_wl = getattr(adapter, "by_workload", None)
        if isinstance(by_wl, Mapping):
            has_by_workload = True
            target = by_wl.get(workload, adapter)

    storage_class = (
        getattr(result, "storage_class", None)
        or getattr(target, "storage_class", None)
        or getattr(adapter, "storage_class", None)
    )
    if storage_class is None:
        if engine_kind in ("postgres", "duckdb"):
            storage_class = "local"
        elif engine_kind in ("lakebase", "starrocks", "cube"):
            storage_class = "object_storage"

    account = (
        getattr(result, "account", None)
        or getattr(target, "account", None)
        or (workload if has_by_workload else getattr(adapter, "account", None))
    )

    lakebase_endpoint = (
        getattr(result, "lakebase_endpoint", None)
        or getattr(target, "lakebase_endpoint", None)
        or getattr(adapter, "lakebase_endpoint", None)
    )

    cold_start: Any = None
    if include_cold_start:
        cold_start = getattr(result, "cold_start", None)
        if cold_start is None and target is not None:
            cold_start = getattr(target, "last_cold_start", None)
        if cold_start is None and adapter is not None:
            cold_start = getattr(adapter, "last_cold_start", None)

    return storage_class, account, lakebase_endpoint, cold_start


def create_app(
    pipeline: ServingPipeline,
    adapters: Mapping[str, EngineAdapter],
    *,
    timeout_seconds: float = 30,
    max_body_bytes: int = 1_000_000,
    max_result_bytes: int = 64 * 1024 * 1024,
    extra_routes: Sequence[Route] | None = None,
    lifespan: Any = None,
    replay_cache: GrantReplayCache | None = None,
    audit_sink: Any = None,
    audit_on_failure: str | None = None,
    audit_failure_callback: Callable[[], Any] | None = None,
    audit_timeout_seconds: float | None = None,
    audit_max_concurrency: int = 4,
    on_row_cap: str = "truncate",
    metrics: MetricsRegistry | None = None,
    cursor_key: bytes | str | None = None,
    cursor_key_env: str | None = None,
) -> Starlette:
    """Create a Starlette serving application."""
    if replay_cache is None:
        replay_cache = GrantReplayCache()
    running_queries = RunningQueries()

    env_var = cursor_key_env or "SEEKNAL_CURSOR_KEY"
    if cursor_key is not None:
        if isinstance(cursor_key, bool) or not isinstance(cursor_key, (bytes, bytearray, str)):
            from seeknal.serving.server.config import ServerConfigError
            raise ServerConfigError("'cursor_key' must be str, bytes, or None")
        try:
            raw_bytes = (
                cursor_key.encode("utf-8")
                if isinstance(cursor_key, str)
                else bytes(cursor_key)
            )
        except UnicodeEncodeError:
            from seeknal.serving.startup_validation import CursorKeyWeakError
            raise CursorKeyWeakError(
                f"cursor_key_weak: pagination cursor signing key in environment variable '{env_var}' "
                f"must be valid UTF-8 and at least 32 bytes"
            ) from None
        if len(raw_bytes) < 32:
            from seeknal.serving.startup_validation import CursorKeyWeakError
            raise CursorKeyWeakError(
                f"cursor_key_weak: pagination cursor signing key in environment variable '{env_var}' "
                f"must be at least 32 bytes (minimum 32 bytes)"
            )
        if isinstance(cursor_key, str):
            register_config_secrets(cursor_key)
        else:
            try:
                register_config_secrets(raw_bytes.decode("utf-8"))
            except UnicodeDecodeError:
                pass
    elif cursor_key_env:
        env_val = os.environ.get(cursor_key_env)
        if env_val is not None:
            try:
                raw_bytes = env_val.encode("utf-8")
            except UnicodeEncodeError:
                from seeknal.serving.startup_validation import CursorKeyWeakError
                raise CursorKeyWeakError(
                    f"cursor_key_weak: pagination cursor signing key in environment variable '{cursor_key_env}' "
                    f"must be valid UTF-8 and at least 32 bytes"
                ) from None
            if len(raw_bytes) < 32:
                from seeknal.serving.startup_validation import CursorKeyWeakError
                raise CursorKeyWeakError(
                    f"cursor_key_weak: pagination cursor signing key in environment variable '{cursor_key_env}' "
                    f"must be at least 32 bytes (minimum 32 bytes)"
                )

    def _resolve_cursor_key() -> bytes | str | None:
        if cursor_key is not None:
            if isinstance(cursor_key, (bytes, bytearray)):
                return bytes(cursor_key) if len(cursor_key) > 0 else None
            if isinstance(cursor_key, str):
                return cursor_key if cursor_key else None
            return None
        if cursor_key_env:
            env_val = os.environ.get(cursor_key_env)
            if env_val:
                register_config_secrets(env_val)
                return env_val
        return None

    if metrics is not None and adapters:
        metrics.register_adapters(adapters)

    effective_audit_on_failure = (
        audit_on_failure
        if audit_on_failure is not None
        else getattr(audit_sink, "on_failure", "refuse")
    )
    base_failure_cb = (
        audit_failure_callback
        if audit_failure_callback is not None
        else getattr(audit_sink, "on_write_failure", None)
    )

    def _on_audit_write_failure() -> None:
        if metrics is not None:
            metrics.record_audit_write_failure()
        if base_failure_cb is not None:
            try:
                base_failure_cb()
            except Exception:
                pass

    is_structured_sink = isinstance(audit_sink, (AuditSink, AuditManager)) or (
        audit_sink is not None and hasattr(audit_sink, "write_record")
    )
    is_callable_sink = (
        audit_sink is not None and not is_structured_sink and callable(audit_sink)
    )
    audit_tracker = get_audit_runtime_tracker(
        audit_sink,
        timeout_seconds=audit_timeout_seconds,
        max_workers=audit_max_concurrency,
    )

    async def _run_audit_io(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        assert audit_tracker is not None
        return await audit_tracker.run_async(
            fn,
            *args,
            timeout_seconds=audit_timeout_seconds,
            **kwargs,
        )

    async def healthz(request: Request) -> Response:
        start = time.perf_counter()
        resp = JSONResponse({"status": "ok"})
        if metrics is not None:
            metrics.record_request(
                "/healthz",
                status=200,
                duration_seconds=time.perf_counter() - start,
            )
        return resp

    def _noop_failure_cb() -> None:
        return None

    async def _finish(
        response: Response,
        *,
        route: str,
        status: int,
        elapsed_ms: float,
        code: str | None = None,
        audit_id: str | None = None,
        subject: str | None = None,
        engine: str | None = None,
        engine_kind: str | None = None,
        storage_class: str | None = None,
        account: str | None = None,
        lakebase_endpoint: Any = None,
        cold_start: Any = None,
        rule: str | None = None,
        access_pattern: str | None = None,
        guard: str | None = None,
        filter_columns: Sequence[str] | None = None,
        effective_max_rows: int | None = None,
        workload: str | None = None,
        tables: Sequence[str] | None = None,
        row_count: int | None = None,
        truncated: bool | None = None,
        sql: str | None = None,
        sql_sha256: str | None = None,
        api: str | None = None,
        paged: bool | None = None,
        params: Any = None,
        token: str | None = None,
        claims: Mapping[str, Any] | None = None,
        binding: Mapping[str, Any] | None = None,
        prepared: PreparedQuery | None = None,
        adapter: Any = None,
        result: Any = None,
        include_cold_start: bool = True,
        skip_callable_audit: bool = False,
        log_request_line: bool = True,
    ) -> Response:
        if prepared is not None:
            if audit_id is None:
                audit_id = prepared.audit_id
            if subject is None:
                subject = prepared.subject
            if engine is None:
                engine = prepared.engine
            if engine_kind is None:
                engine_kind = prepared.engine_kind
            if rule is None:
                rule = prepared.rule
            if workload is None:
                workload = prepared.workload
            if api is None:
                api = prepared.api
            if access_pattern is None:
                access_pattern = prepared.access_pattern
            if guard is None:
                guard = prepared.guard
            if filter_columns is None:
                filter_columns = prepared.filter_columns
            if effective_max_rows is None:
                effective_max_rows = prepared.effective_max_rows
            if not binding and prepared.binding:
                binding = prepared.binding

        if adapter is None and engine is not None:
            adapter = adapters.get(engine)

        if engine_kind is not None or adapter is not None or result is not None:
            res_sc, res_acc, res_lb, res_cs = _resolve_adapter_audit_metadata(
                adapter,
                engine_kind=engine_kind,
                workload=workload,
                result=result,
                include_cold_start=include_cold_start,
            )
            if storage_class is None:
                storage_class = res_sc
            if account is None:
                account = res_acc
            if lakebase_endpoint is None:
                lakebase_endpoint = res_lb
            if cold_start is None:
                cold_start = res_cs

        if audit_sink is not None and route in ALLOWED_AUDIT_ROUTES:
            if is_structured_sink:
                primary_cb_called = False

                def _primary_failure_cb() -> None:
                    nonlocal primary_cb_called
                    primary_cb_called = True
                    _on_audit_write_failure()

                try:
                    record = build_audit_record(
                        route=route,
                        status=status,
                        code=code if status >= 400 else None,
                        elapsed_ms=elapsed_ms,
                        sql=sql,
                        sql_sha256=sql_sha256,
                        api=api,
                        engine_kind=engine_kind,
                        storage_class=storage_class,
                        account=account,
                        lakebase_endpoint=lakebase_endpoint,
                        cold_start=cold_start,
                        access_pattern=access_pattern,
                        guard=guard,
                        filter_columns=filter_columns,
                        effective_max_rows=effective_max_rows,
                        paged=paged,
                        audit_id=audit_id,
                        subject=subject,
                        workload=workload,
                        engine=engine,
                        rule=rule,
                        tables=tables,
                        row_count=row_count,
                        truncated=truncated,
                        claims=claims,
                        binding=binding,
                    )
                    record.to_json_line()
                except (ValueError, UnicodeError, TypeError) as build_exc:
                    logger.error(
                        "Audit record build/serialize failed, falling back to minimal record "
                        "(route=%s, status=%d) error=%s detail=%s audit_id=%s",
                        route,
                        status,
                        type(build_exc).__name__,
                        scrub_error_detail(
                            build_exc, token=token, sql=sql, params=params
                        ),
                        audit_id or "-",
                    )
                    record = build_minimal_audit_record(
                        route=route,
                        status=status,
                        code=code if status >= 400 else None,
                        elapsed_ms=elapsed_ms,
                        audit_id=audit_id,
                    )

                try:
                    await _run_audit_io(
                        emit_audit_record,
                        audit_sink,
                        record,
                        on_failure="refuse",
                        on_write_failure=_primary_failure_cb,
                    )
                except Exception as exc:
                    if not primary_cb_called:
                        _on_audit_write_failure()
                    if effective_audit_on_failure == "log":
                        logger.error(
                            "Audit write failed (on_failure=log, route=%s, status=%d) "
                            "error=%s detail=%s audit_id=%s",
                            route,
                            status,
                            type(exc).__name__,
                            scrub_error_detail(
                                exc, token=token, sql=sql, params=params
                            ),
                            audit_id or "-",
                        )
                    else:
                        logger.error(
                            "engine=%s code=%s error=%s detail=%s audit_id=%s",
                            engine_kind or "-",
                            "audit_unavailable",
                            type(exc).__name__,
                            scrub_error_detail(
                                exc, token=token, sql=sql, params=params
                            ),
                            audit_id or "-",
                        )
                        _log_request(
                            audit_id=audit_id,
                            subject=subject,
                            engine=engine,
                            rule=rule,
                            status=503,
                            elapsed_ms=elapsed_ms,
                            binding=binding,
                            claims=claims,
                        )
                        if metrics is not None:
                            metrics.record_request(
                                route,
                                status=503,
                                duration_seconds=elapsed_ms / 1000.0,
                                engine=engine,
                                error_code="audit_unavailable",
                            )
                        return _error_response(
                            "audit_unavailable",
                            audit_id=audit_id,
                            token=token,
                            sql=sql,
                            params=params,
                        )
            elif is_callable_sink and not skip_callable_audit:
                try:
                    record = build_audit_record(
                        route=route,
                        status=status,
                        code=code if status >= 400 else None,
                        elapsed_ms=elapsed_ms,
                        sql=sql,
                        sql_sha256=sql_sha256,
                        api=api,
                        engine_kind=engine_kind,
                        storage_class=storage_class,
                        account=account,
                        lakebase_endpoint=lakebase_endpoint,
                        cold_start=cold_start,
                        access_pattern=access_pattern,
                        guard=guard,
                        filter_columns=filter_columns,
                        effective_max_rows=effective_max_rows,
                        paged=paged,
                        audit_id=audit_id,
                        subject=subject,
                        workload=workload,
                        engine=engine,
                        rule=rule,
                        tables=tables,
                        row_count=row_count,
                        truncated=truncated,
                        claims=claims,
                        binding=binding,
                    )
                    record_dict = record.to_dict()
                except (ValueError, UnicodeError, TypeError) as build_exc:
                    logger.error(
                        "Audit record build/serialize failed, falling back to minimal record "
                        "(route=%s, status=%d) error=%s detail=%s audit_id=%s",
                        route,
                        status,
                        type(build_exc).__name__,
                        scrub_error_detail(
                            build_exc, token=token, sql=sql, params=params
                        ),
                        audit_id or "-",
                    )
                    minimal_record = build_minimal_audit_record(
                        route=route,
                        status=status,
                        code=code if status >= 400 else None,
                        elapsed_ms=elapsed_ms,
                        audit_id=audit_id,
                    )
                    record_dict = minimal_record.to_dict()

                try:
                    await _run_audit_io(audit_sink, record_dict)
                except Exception as exc:
                    _on_audit_write_failure()
                    if effective_audit_on_failure == "log":
                        logger.error(
                            "Audit write failed (on_failure=log, route=%s, status=%d) "
                            "error=%s detail=%s audit_id=%s",
                            route,
                            status,
                            type(exc).__name__,
                            scrub_error_detail(
                                exc, token=token, sql=sql, params=params
                            ),
                            audit_id or "-",
                        )
                    else:
                        logger.error(
                            "engine=%s code=%s error=%s detail=%s audit_id=%s",
                            engine_kind or "-",
                            "audit_unavailable",
                            type(exc).__name__,
                            scrub_error_detail(
                                exc, token=token, sql=sql, params=params
                            ),
                            audit_id or "-",
                        )
                        _log_request(
                            audit_id=audit_id,
                            subject=subject,
                            engine=engine,
                            rule=rule,
                            status=503,
                            elapsed_ms=elapsed_ms,
                            binding=binding,
                            claims=claims,
                        )
                        if metrics is not None:
                            metrics.record_request(
                                route,
                                status=503,
                                duration_seconds=elapsed_ms / 1000.0,
                                engine=engine,
                                error_code="audit_unavailable",
                            )
                        return _error_response(
                            "audit_unavailable",
                            audit_id=audit_id,
                            token=token,
                            sql=sql,
                            params=params,
                        )

        if log_request_line:
            _log_request(
                audit_id=audit_id,
                subject=subject,
                engine=engine,
                rule=rule,
                status=status,
                elapsed_ms=elapsed_ms,
                row_count=row_count if row_count is not None else "-",
                binding=binding,
                claims=claims,
            )
        if metrics is not None:
            err_code = (code or "internal_error") if status >= 400 else None
            metrics.record_request(
                route,
                status=status,
                duration_seconds=elapsed_ms / 1000.0,
                engine=engine,
                error_code=err_code,
            )
        return response

    async def _bad_request_with_token(
        token: str,
        req_start: float,
        *,
        route: str,
        custom_message: str | None = None,
        sql: str | None = None,
        params: Any = None,
    ) -> Response:
        claims, audit_id, subject = _extract_verified_grant_claims(token, pipeline)
        sql_dialect = (
            claims["sql_dialect"]
            if claims is not None and isinstance(claims.get("sql_dialect"), str)
            else "postgres"
        )
        tables = await _extract_logical_tables_async(
            sql,
            claims=claims,
            sql_dialect=sql_dialect,
            default_catalog=pipeline.default_catalog,
        )
        elapsed_ms = (time.perf_counter() - req_start) * 1000.0
        err_resp = _error_response(
            "invalid_sql",
            message=custom_message,
            audit_id=audit_id,
            token=token,
            sql=sql,
            params=params,
        )
        return await _finish(
            err_resp,
            route=route,
            status=400,
            code="invalid_sql",
            elapsed_ms=elapsed_ms,
            audit_id=audit_id,
            subject=subject,
            tables=tables,
            sql=sql,
            params=params,
            token=token,
            claims=claims,
        )

    async def _parse_common_request(
        request: Request,
        req_start: float,
        route: str,
    ) -> (
        tuple[
            str,
            str,
            tuple,
            str | None,
            float | None,
            float | None,
            int | None,
            int | None,
        ]
        | Response
    ):
        # 1. Check body size limit before parsing
        content_length_hdr = request.headers.get("content-length")
        if content_length_hdr is not None:
            try:
                if int(content_length_hdr) > max_body_bytes:
                    return await _finish(
                        _error_response("payload_too_large"),
                        route=route,
                        status=413,
                        code="payload_too_large",
                        elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    )
            except ValueError:
                pass

        try:
            raw_body = await _read_body_bounded(request, max_body_bytes)
        except ClientDisconnect:
            return await _finish(
                _error_response("query_cancelled"),
                route=route,
                status=499,
                code="query_cancelled",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                log_request_line=False,
            )

        if raw_body is None:
            return await _finish(
                _error_response("payload_too_large"),
                route=route,
                status=413,
                code="payload_too_large",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        # 2. Check Authorization header
        auth_header = request.headers.get("authorization")
        if not auth_header:
            if metrics is not None:
                metrics.record_grant_rejected("malformed")
            return await _finish(
                _error_response("grant_invalid"),
                route=route,
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
            if metrics is not None:
                metrics.record_grant_rejected("malformed")
            return await _finish(
                _error_response("grant_invalid"),
                route=route,
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        token = parts[1].strip()

        v_claims, v_jti, v_sub = _extract_verified_grant_claims(token, pipeline)
        if v_claims is not None and v_claims.get("api") is not None:
            if metrics is not None:
                metrics.record_grant_rejected("invalid_claims")
            return await _finish(
                _error_response("grant_invalid", audit_id=v_jti, token=token),
                route=route,
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=v_jti,
                subject=v_sub,
                token=token,
                claims=v_claims,
            )

        # 3. Parse JSON body
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            return await _bad_request_with_token(token, req_start, route=route)

        if not isinstance(payload, dict):
            return await _bad_request_with_token(token, req_start, route=route)

        unknown_fields = set(payload.keys()) - {
            "sql",
            "params",
            "hint",
            "max_staleness_seconds",
            "timeout_s",
            "max_rows",
            "max_result_bytes",
        }
        if unknown_fields:
            custom_unknown_msg = None
            if all(
                isinstance(k, str) and SAFE_FIELD_NAME_RE.fullmatch(k)
                for k in unknown_fields
            ):
                custom_unknown_msg = (
                    f"Unknown field(s) in request body: {', '.join(sorted(unknown_fields))}"
                )
            candidate_sql = (
                payload["sql"]
                if isinstance(payload.get("sql"), str) and payload["sql"]
                else None
            )
            return await _bad_request_with_token(
                token,
                req_start,
                route=route,
                custom_message=custom_unknown_msg,
                sql=candidate_sql,
            )

        if (
            "sql" not in payload
            or not isinstance(payload["sql"], str)
            or not payload["sql"].strip()
        ):
            return await _bad_request_with_token(token, req_start, route=route)

        sql = payload["sql"]

        params: tuple = ()
        if "params" in payload:
            raw_params = payload["params"]
            if raw_params is not None:
                if not isinstance(raw_params, list):
                    return await _bad_request_with_token(
                        token, req_start, route=route, sql=sql
                    )
                params = tuple(raw_params)

        hint = payload.get("hint")
        if hint is not None and not isinstance(hint, str):
            return await _bad_request_with_token(
                token, req_start, route=route, sql=sql, params=params
            )

        max_staleness = payload.get("max_staleness_seconds")
        if max_staleness is not None:
            if isinstance(max_staleness, bool) or not isinstance(
                max_staleness, (int, float)
            ):
                return await _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )
            if math.isnan(max_staleness) or math.isinf(max_staleness):
                return await _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )
            max_staleness = float(max_staleness)

        timeout_s = payload.get("timeout_s")
        if timeout_s is not None:
            if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
                return await _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )
            if math.isnan(timeout_s) or math.isinf(timeout_s) or timeout_s <= 0:
                return await _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )
            timeout_s = float(timeout_s)

        max_rows = payload.get("max_rows")
        if max_rows is not None:
            if (
                isinstance(max_rows, bool)
                or not isinstance(max_rows, int)
                or max_rows <= 0
            ):
                return await _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )

        req_max_result_bytes = payload.get("max_result_bytes")
        if req_max_result_bytes is not None:
            if (
                isinstance(req_max_result_bytes, bool)
                or not isinstance(req_max_result_bytes, int)
                or req_max_result_bytes <= 0
            ):
                return await _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )

        return (
            token,
            sql,
            params,
            hint,
            max_staleness,
            timeout_s,
            max_rows,
            req_max_result_bytes,
        )

    async def _handle_prepare_exception(
        exc: Exception,
        *,
        route: str,
        token: str,
        sql: str,
        params: Any,
        req_start: float,
    ) -> Response:
        elapsed = (time.perf_counter() - req_start) * 1000.0
        claims, v_jti, v_sub = _extract_verified_grant_claims(token, pipeline)
        sql_dialect = (
            claims["sql_dialect"]
            if claims is not None and isinstance(claims.get("sql_dialect"), str)
            else "postgres"
        )
        tables = await _extract_logical_tables_async(
            sql,
            claims=claims,
            sql_dialect=sql_dialect,
            default_catalog=pipeline.default_catalog,
        )

        if isinstance(exc, GrantTokenError):
            if metrics is not None:
                metrics.record_grant_rejected(exc)
            code = getattr(exc, "code", None) or "grant_invalid"
            spec = get_error_spec(code)
            audit_id = getattr(exc, "audit_id", None) or v_jti
            subject = getattr(exc, "subject", None) or v_sub
            custom_msg = (
                _safe_grant_invalid_message(exc)
                if spec.code == "grant_invalid"
                else None
            )
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                spec.code,
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                audit_id or "-",
            )
            return await _finish(
                _error_response(
                    spec.code,
                    message=custom_msg,
                    audit_id=audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route=route,
                status=401,
                code=spec.code,
                elapsed_ms=elapsed,
                audit_id=audit_id,
                subject=subject,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
            )

        if isinstance(exc, EnforcementError):
            code = getattr(exc, "code", None) or "access_denied"
            spec = get_error_spec(code)
            if metrics is not None and spec.code == "grant_invalid":
                metrics.record_grant_rejected("invalid_claims")
            audit_id = getattr(exc, "audit_id", None) or v_jti
            subject = getattr(exc, "subject", None) or v_sub
            exc_engine = getattr(exc, "engine", None)
            exc_engine_kind = getattr(exc, "engine_kind", None)
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                exc_engine_kind or "-",
                spec.code,
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                audit_id or "-",
            )
            return await _finish(
                _error_response(
                    spec.code,
                    audit_id=audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route=route,
                status=spec.status,
                code=spec.code,
                elapsed_ms=elapsed,
                audit_id=audit_id,
                subject=subject,
                engine=exc_engine,
                engine_kind=exc_engine_kind,
                rule=getattr(exc, "rule", None),
                access_pattern=getattr(exc, "access_pattern", None),
                guard=getattr(exc, "guard", None),
                filter_columns=getattr(exc, "filter_columns", None),
                effective_max_rows=getattr(exc, "effective_max_rows", None),
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                include_cold_start=False,
            )

        if isinstance(exc, RoutingError):
            code = getattr(exc, "code", None) or "query_spans_engines"
            spec = get_error_spec(code)
            audit_id = getattr(exc, "audit_id", None) or v_jti
            subject = getattr(exc, "subject", None) or v_sub
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                spec.code,
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                audit_id or "-",
            )
            return await _finish(
                _error_response(
                    spec.code,
                    audit_id=audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route=route,
                status=spec.status,
                code=spec.code,
                elapsed_ms=elapsed,
                audit_id=audit_id,
                subject=subject,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
            )

        typed_exc_code = getattr(exc, "code", None)
        if (
            isinstance(typed_exc_code, str)
            and typed_exc_code in ERROR_CATALOGUE
            and typed_exc_code not in ("internal_error", "audit_unavailable")
        ):
            spec = get_error_spec(typed_exc_code)
            audit_id = getattr(exc, "audit_id", None) or v_jti
            subject = getattr(exc, "subject", None) or v_sub
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                spec.code,
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                audit_id or "-",
            )
            return await _finish(
                _error_response(
                    spec.code,
                    audit_id=audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route=route,
                status=spec.status,
                code=spec.code,
                elapsed_ms=elapsed,
                audit_id=audit_id,
                subject=subject,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
            )

        audit_id = getattr(exc, "audit_id", None) or v_jti
        subject = getattr(exc, "subject", None) or v_sub
        if isinstance(exc, UnicodeError):
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                "invalid_sql",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "invalid_sql",
                    audit_id=audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route=route,
                status=400,
                code="invalid_sql",
                elapsed_ms=elapsed,
                audit_id=audit_id,
                subject=subject,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
            )

        if _is_audit_unavailable(exc):
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                "audit_unavailable",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "audit_unavailable",
                    audit_id=audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route=route,
                status=503,
                code="audit_unavailable",
                elapsed_ms=elapsed,
                audit_id=audit_id,
                subject=subject,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
            )

        logger.error(
            "engine=%s code=%s error=%s detail=%s audit_id=%s",
            "-",
            "internal_error",
            type(exc).__name__,
            scrub_error_detail(exc, token=token, sql=sql, params=params),
            audit_id or "-",
        )
        return await _finish(
            _error_response(
                "internal_error",
                audit_id=audit_id,
                token=token,
                sql=sql,
                params=params,
            ),
            route=route,
            status=500,
            code="internal_error",
            elapsed_ms=elapsed,
            audit_id=audit_id,
            subject=subject,
            tables=tables,
            sql=sql,
            params=params,
            token=token,
            claims=claims,
        )

    async def explain(request: Request) -> Response:
        req_start = time.perf_counter()
        parsed = await _parse_common_request(request, req_start, "/v1/explain")
        if isinstance(parsed, Response):
            return parsed

        (
            token,
            sql,
            params,
            hint,
            max_staleness,
            timeout_s,
            max_rows,
            req_max_result_bytes,
        ) = parsed

        try:
            prepared = await run_in_threadpool(
                pipeline.prepare,
                sql,
                token,
                params=params,
                hint=hint,
                max_staleness_seconds=max_staleness,
                max_rows=max_rows,
                metrics=metrics,
            )
        except Exception as exc:
            return await _handle_prepare_exception(
                exc,
                route="/v1/explain",
                token=token,
                sql=sql,
                params=params,
                req_start=req_start,
            )

        claims, _, _ = _extract_verified_grant_claims(token, pipeline)
        sql_dialect = (
            claims["sql_dialect"]
            if claims is not None and isinstance(claims.get("sql_dialect"), str)
            else "postgres"
        )
        tables = await _extract_logical_tables_async(
            sql,
            claims=claims,
            sql_dialect=sql_dialect,
            default_catalog=pipeline.default_catalog,
            prepared=prepared,
        )
        elapsed_ms = (time.perf_counter() - req_start) * 1000.0

        resp = JSONResponse(
            {
                "engine": prepared.engine,
                "rule": prepared.rule,
                "reason": prepared.reason,
                "sql": prepared.sql,
                "paramstyle": prepared.paramstyle,
                "masked_columns": list(prepared.masked_columns),
                "row_limit": prepared.row_limit,
                "redacted_sql": prepared.redacted_sql,
            }
        )
        return await _finish(
            resp,
            route="/v1/explain",
            status=200,
            elapsed_ms=elapsed_ms,
            audit_id=prepared.audit_id,
            subject=prepared.subject,
            engine=prepared.engine,
            engine_kind=prepared.engine_kind,
            rule=prepared.rule,
            workload=prepared.workload,
            tables=tables,
            sql=sql,
            params=params,
            token=token,
            claims=claims,
            binding=prepared.binding,
            prepared=prepared,
            include_cold_start=False,
        )

    async def query(request: Request) -> Response:
        req_start = time.perf_counter()
        parsed = await _parse_common_request(request, req_start, "/v1/query")
        if isinstance(parsed, Response):
            return parsed

        (
            token,
            sql,
            params,
            hint,
            max_staleness,
            timeout_s,
            max_rows,
            req_max_result_bytes,
        ) = parsed

        try:
            prepared = await run_in_threadpool(
                pipeline.prepare,
                sql,
                token,
                params=params,
                hint=hint,
                max_staleness_seconds=max_staleness,
                max_rows=max_rows,
                metrics=metrics,
            )
        except Exception as exc:
            return await _handle_prepare_exception(
                exc,
                route="/v1/query",
                token=token,
                sql=sql,
                params=params,
                req_start=req_start,
            )

        claims, _, _ = _extract_verified_grant_claims(token, pipeline)
        sql_dialect = (
            claims["sql_dialect"]
            if claims is not None and isinstance(claims.get("sql_dialect"), str)
            else "postgres"
        )
        tables = await _extract_logical_tables_async(
            sql,
            claims=claims,
            sql_dialect=sql_dialect,
            default_catalog=pipeline.default_catalog,
            prepared=prepared,
        )

        if prepared.expires_at is None:
            if metrics is not None:
                metrics.record_grant_rejected("invalid_claims")
            return await _finish(
                _error_response(
                    "grant_invalid",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                include_cold_start=False,
            )

        try:
            replay_cache.check_and_record(prepared.audit_id, prepared.expires_at)
        except GrantReplayedError:
            if metrics is not None:
                metrics.record_grant_rejected("replayed")
            return await _finish(
                _error_response(
                    "grant_replayed",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=401,
                code="grant_replayed",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                include_cold_start=False,
            )
        except GrantReplayCacheFull:
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    status_code=503,
                    category="replay_cache_full",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=503,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                include_cold_start=False,
            )

        adapter = adapters.get(prepared.engine)
        if adapter is None:
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=503,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                include_cold_start=False,
            )

        callable_audit_done = False
        if is_callable_sink:
            pre_sc, pre_acc, pre_lb, pre_cs = _resolve_adapter_audit_metadata(
                adapter,
                engine_kind=prepared.engine_kind,
                workload=prepared.workload,
                result=None,
                include_cold_start=True,
            )
            try:
                pre_record = build_audit_record(
                    route="/v1/query",
                    status=200,
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    sql=sql,
                    api=prepared.api,
                    engine_kind=prepared.engine_kind,
                    storage_class=pre_sc,
                    account=pre_acc,
                    lakebase_endpoint=pre_lb,
                    cold_start=pre_cs,
                    access_pattern=prepared.access_pattern,
                    guard=prepared.guard,
                    filter_columns=prepared.filter_columns,
                    effective_max_rows=prepared.effective_max_rows,
                    paged=None,
                    audit_id=prepared.audit_id,
                    subject=prepared.subject,
                    workload=prepared.workload,
                    engine=prepared.engine,
                    rule=prepared.rule,
                    tables=tables,
                    claims=claims,
                    binding=prepared.binding,
                )
                pre_record_dict = pre_record.to_dict()
            except (ValueError, UnicodeError, TypeError) as build_exc:
                logger.error(
                    "Audit record build/serialize failed, falling back to minimal record "
                    "(route=/v1/query, status=200) error=%s detail=%s audit_id=%s",
                    type(build_exc).__name__,
                    scrub_error_detail(
                        build_exc, token=token, sql=sql, params=params
                    ),
                    prepared.audit_id or "-",
                )
                min_pre = build_minimal_audit_record(
                    route="/v1/query",
                    status=200,
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    audit_id=prepared.audit_id,
                )
                pre_record_dict = min_pre.to_dict()

            try:
                await _run_audit_io(audit_sink, pre_record_dict)
                callable_audit_done = True
            except Exception as exc:
                _on_audit_write_failure()
                if effective_audit_on_failure == "log":
                    logger.error(
                        "Audit write failed (on_failure=log, route=/v1/query, status=200) "
                        "error=%s detail=%s audit_id=%s",
                        type(exc).__name__,
                        scrub_error_detail(exc, token=token, sql=sql, params=params),
                        prepared.audit_id or "-",
                    )
                    callable_audit_done = True
                else:
                    elapsed_audit_err = (time.perf_counter() - req_start) * 1000.0
                    logger.error(
                        "engine=%s code=%s error=%s detail=%s audit_id=%s",
                        prepared.engine_kind or "-",
                        "audit_unavailable",
                        type(exc).__name__,
                        scrub_error_detail(exc, token=token, sql=sql, params=params),
                        prepared.audit_id or "-",
                    )
                    _log_request(
                        audit_id=prepared.audit_id,
                        subject=prepared.subject,
                        engine=prepared.engine,
                        rule=prepared.rule,
                        status=503,
                        elapsed_ms=elapsed_audit_err,
                        binding=prepared.binding,
                        claims=claims,
                    )
                    if metrics is not None:
                        metrics.record_request(
                            "/v1/query",
                            status=503,
                            duration_seconds=elapsed_audit_err / 1000.0,
                            engine=prepared.engine,
                            error_code="audit_unavailable",
                        )
                    return _error_response(
                        "audit_unavailable",
                        audit_id=prepared.audit_id,
                        token=token,
                        sql=sql,
                        params=params,
                    )

        effective_timeout = (
            min(timeout_seconds, timeout_s)
            if timeout_s is not None
            else timeout_seconds
        )
        effective_max_result_bytes = (
            min(max_result_bytes, req_max_result_bytes)
            if req_max_result_bytes is not None
            else max_result_bytes
        )

        try:
            running_queries.register(prepared.audit_id, prepared.subject)
        except DuplicateQueryIdError:
            return await _finish(
                _error_response(
                    "duplicate_query_id",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=409,
                code="duplicate_query_id",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                include_cold_start=False,
                skip_callable_audit=callable_audit_done,
            )

        cancel_handle: list[Callable[[], None] | None] = [None]

        def on_cancel(cancel_func: Callable[[], None]) -> None:
            cancel_handle[0] = cancel_func
            running_queries.set_cancel_handle(prepared.audit_id, cancel_func)

        try:
            sig = inspect.signature(adapter.execute)
            if "on_cancel_handle" in sig.parameters:
                query_task = asyncio.create_task(
                    run_in_threadpool(
                        adapter.execute,
                        prepared,
                        timeout_seconds=effective_timeout,
                        on_cancel_handle=on_cancel,
                    )
                )
            else:
                query_task = asyncio.create_task(
                    run_in_threadpool(
                        adapter.execute,
                        prepared,
                        timeout_seconds=effective_timeout,
                    )
                )

            while not query_task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(query_task), timeout=0.25)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        if cancel_handle[0] is not None:
                            try:
                                cancel_handle[0]()
                            except Exception:
                                pass
                        try:
                            await query_task
                        except Exception:
                            pass
                        return await _finish(
                            Response(status_code=499),
                            route="/v1/query",
                            status=499,
                            code="query_cancelled",
                            elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                            tables=tables,
                            sql=sql,
                            params=params,
                            token=token,
                            claims=claims,
                            prepared=prepared,
                            adapter=adapter,
                            skip_callable_audit=callable_audit_done,
                            log_request_line=False,
                        )

            result = await query_task

        except EngineTimeout as exc:
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "engine_timeout",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "engine_timeout",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=504,
                code="engine_timeout",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except EnginePoolExhausted as exc:
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "engine_unavailable",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=503,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except EngineConfigError as exc:
            raw_cfg_msg = str(exc)
            safe_cfg_msg = (
                raw_cfg_msg
                if WORKLOAD_ACCOUNT_MISSING_RE.fullmatch(raw_cfg_msg)
                else None
            )
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "engine_unavailable",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    status_code=503,
                    category="workload_account_missing",
                    message=safe_cfg_msg,
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=503,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except EngineCancelled as exc:
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "query_cancelled",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "query_cancelled",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=499,
                code="query_cancelled",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except EngineError as exc:
            if _is_engine_starting(exc):
                retry_after = _retry_after_header_value(exc)
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    prepared.engine_kind or "-",
                    "engine_starting",
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token, sql=sql, params=params),
                    prepared.audit_id or "-",
                )
                return await _finish(
                    _error_response(
                        "engine_starting",
                        audit_id=prepared.audit_id,
                        token=token,
                        sql=sql,
                        params=params,
                        headers={"Retry-After": retry_after},
                    ),
                    route="/v1/query",
                    status=503,
                    code="engine_starting",
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    tables=tables,
                    sql=sql,
                    params=params,
                    token=token,
                    claims=claims,
                    prepared=prepared,
                    adapter=adapter,
                    skip_callable_audit=callable_audit_done,
                )
            raw_eng_msg = str(exc)
            safe_eng_msg = (
                raw_eng_msg
                if raw_eng_msg in SAFE_ENGINE_ERROR_MESSAGES
                else "Engine query execution failed"
            )
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "engine_unavailable",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    status_code=502,
                    category="engine_error",
                    message=safe_eng_msg,
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=502,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except Exception as exc:
            if _is_engine_starting(exc):
                retry_after = _retry_after_header_value(exc)
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    prepared.engine_kind or "-",
                    "engine_starting",
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token, sql=sql, params=params),
                    prepared.audit_id or "-",
                )
                return await _finish(
                    _error_response(
                        "engine_starting",
                        audit_id=prepared.audit_id,
                        token=token,
                        sql=sql,
                        params=params,
                        headers={"Retry-After": retry_after},
                    ),
                    route="/v1/query",
                    status=503,
                    code="engine_starting",
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    tables=tables,
                    sql=sql,
                    params=params,
                    token=token,
                    claims=claims,
                    prepared=prepared,
                    adapter=adapter,
                    skip_callable_audit=callable_audit_done,
                )
            if _is_audit_unavailable(exc):
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    prepared.engine_kind or "-",
                    "audit_unavailable",
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token, sql=sql, params=params),
                    prepared.audit_id or "-",
                )
                return await _finish(
                    _error_response(
                        "audit_unavailable",
                        audit_id=prepared.audit_id,
                        token=token,
                        sql=sql,
                        params=params,
                    ),
                    route="/v1/query",
                    status=503,
                    code="audit_unavailable",
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    tables=tables,
                    sql=sql,
                    params=params,
                    token=token,
                    claims=claims,
                    prepared=prepared,
                    adapter=adapter,
                    skip_callable_audit=callable_audit_done,
                )

            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "internal_error",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, sql=sql, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "internal_error",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=500,
                code="internal_error",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        finally:
            running_queries.unregister(prepared.audit_id)

        effective_row_cap_mode = (
            on_row_cap
            if on_row_cap != "truncate"
            else getattr(pipeline, "on_row_cap", "truncate")
        )
        if effective_row_cap_mode == "error" and result.truncated:
            return await _finish(
                _error_response(
                    "row_cap_hit",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=422,
                code="row_cap_hit",
                elapsed_ms=result.elapsed_ms,
                tables=tables,
                truncated=True,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                result=result,
                skip_callable_audit=callable_audit_done,
            )

        safe_rows = [[_make_json_safe(cell) for cell in row] for row in result.rows]

        encoded_rows = json.dumps(safe_rows).encode("utf-8")
        if len(encoded_rows) > effective_max_result_bytes:
            return await _finish(
                _error_response(
                    "result_too_large",
                    audit_id=prepared.audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route="/v1/query",
                status=413,
                code="result_too_large",
                elapsed_ms=result.elapsed_ms,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                result=result,
                skip_callable_audit=callable_audit_done,
            )

        col_types = list(result.column_types)
        if len(col_types) < len(result.columns):
            col_types.extend(["unknown"] * (len(result.columns) - len(col_types)))
        elif len(col_types) > len(result.columns):
            col_types = col_types[: len(result.columns)]

        resp = JSONResponse(
            {
                "columns": list(result.columns),
                "column_types": col_types,
                "rows": safe_rows,
                "truncated": result.truncated,
                "notices": ["row_cap_hit"] if result.truncated else [],
                "engine": result.engine,
                "rule": prepared.rule,
                "reason": prepared.reason,
                "masked_columns": list(prepared.masked_columns),
                "audit_id": prepared.audit_id,
                "redacted_sql": prepared.redacted_sql,
                "elapsed_ms": result.elapsed_ms,
            }
        )
        return await _finish(
            resp,
            route="/v1/query",
            status=200,
            elapsed_ms=result.elapsed_ms,
            engine=result.engine,
            tables=tables,
            row_count=len(safe_rows),
            truncated=bool(result.truncated),
            sql=sql,
            params=params,
            token=token,
            claims=claims,
            prepared=prepared,
            adapter=adapter,
            result=result,
            skip_callable_audit=callable_audit_done,
        )

    async def cancel(request: Request) -> Response:
        req_start = time.perf_counter()
        content_length_hdr = request.headers.get("content-length")
        if content_length_hdr is not None:
            try:
                if int(content_length_hdr) > max_body_bytes:
                    return await _finish(
                        _error_response("payload_too_large"),
                        route="/v1/cancel",
                        status=413,
                        code="payload_too_large",
                        elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    )
            except ValueError:
                pass

        try:
            raw_body = await _read_body_bounded(request, max_body_bytes)
        except ClientDisconnect:
            return await _finish(
                _error_response("query_cancelled"),
                route="/v1/cancel",
                status=499,
                code="query_cancelled",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                log_request_line=False,
            )

        if raw_body is None:
            return await _finish(
                _error_response("payload_too_large"),
                route="/v1/cancel",
                status=413,
                code="payload_too_large",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        auth_header = request.headers.get("authorization")
        if not auth_header:
            if metrics is not None:
                metrics.record_grant_rejected("malformed")
            return await _finish(
                _error_response("grant_invalid"),
                route="/v1/cancel",
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
            if metrics is not None:
                metrics.record_grant_rejected("malformed")
            return await _finish(
                _error_response("grant_invalid"),
                route="/v1/cancel",
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        token = parts[1].strip()

        stripped_body = raw_body.strip(b" \t\r\n")
        if stripped_body:
            is_empty_json_obj = (
                len(stripped_body) >= 2
                and stripped_body[0:1] == b"{"
                and stripped_body[-1:] == b"}"
                and stripped_body[1:-1].strip(b" \t\r\n") == b""
            )
            if not is_empty_json_obj:
                c_claims, c_jti, c_sub = _extract_verified_cancel_claims(
                    token, pipeline
                )
                return await _finish(
                    _error_response("invalid_sql", token=token),
                    route="/v1/cancel",
                    status=400,
                    code="invalid_sql",
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    audit_id=c_jti,
                    subject=c_sub,
                    token=token,
                    claims=c_claims,
                )

        try:
            claims = verify_cancel_token(
                token,
                pipeline.public_keys,
                audience=pipeline.audience,
                issuer=pipeline.issuer,
            )
        except GrantTokenError as exc:
            if metrics is not None:
                metrics.record_grant_rejected(exc)
            code = getattr(exc, "code", None) or "grant_invalid"
            spec = get_error_spec(code)
            audit_id = getattr(exc, "audit_id", None)
            subject = getattr(exc, "subject", None)
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                spec.code,
                type(exc).__name__,
                scrub_error_detail(exc, token=token),
                audit_id or "-",
            )
            return await _finish(
                _error_response(spec.code, audit_id=audit_id, token=token),
                route="/v1/cancel",
                status=401,
                code=spec.code,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=audit_id,
                subject=subject,
                token=token,
            )

        jti = claims["jti"]
        sub = claims["sub"]
        if not running_queries.is_running(jti, sub):
            return await _finish(
                _error_response("not_found", audit_id=jti, token=token),
                route="/v1/cancel",
                status=404,
                code="not_found",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=jti,
                subject=sub,
                token=token,
                claims=claims,
            )

        resp = await _finish(
            JSONResponse({"cancelled": True, "query_id": jti}, status_code=202),
            route="/v1/cancel",
            status=202,
            elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            audit_id=jti,
            subject=sub,
            token=token,
            claims=claims,
        )
        if resp.status_code == 202:
            running_queries.cancel(jti, sub)
        return resp

    async def handle_405(request: Request, exc: Exception) -> Response:
        resp_405 = JSONResponse(
            {
                "error": "method_not_allowed",
                "code": "invalid_sql",
                "message": "HTTP method not allowed for this endpoint.",
                "next_step": "Check the HTTP method (use POST for /v1/query, /v1/explain, /v1/cancel) and try again.",
            },
            status_code=405,
        )
        return await _finish(
            resp_405,
            route=request.url.path,
            status=405,
            code="invalid_sql",
            elapsed_ms=0.0,
            log_request_line=request.url.path in ALLOWED_AUDIT_ROUTES,
        )

    async def select(request: Request) -> Response:
        req_start = time.perf_counter()
        route = "/v1/select"

        # 1. Bounded body read
        content_length_hdr = request.headers.get("content-length")
        if content_length_hdr is not None:
            try:
                if int(content_length_hdr) > max_body_bytes:
                    return await _finish(
                        _error_response("payload_too_large"),
                        route=route,
                        status=413,
                        code="payload_too_large",
                        elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    )
            except ValueError:
                pass

        try:
            raw_body = await _read_body_bounded(request, max_body_bytes)
        except ClientDisconnect:
            return await _finish(
                _error_response("query_cancelled"),
                route=route,
                status=499,
                code="query_cancelled",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                log_request_line=False,
            )

        if raw_body is None:
            return await _finish(
                _error_response("payload_too_large"),
                route=route,
                status=413,
                code="payload_too_large",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        body_sha256 = hashlib.sha256(raw_body).hexdigest()

        # 2. Verify Authorization Bearer token
        auth_header = request.headers.get("authorization")
        if not auth_header:
            if metrics is not None:
                metrics.record_grant_rejected("malformed")
            return await _finish(
                _error_response("grant_invalid"),
                route=route,
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                sql_sha256=body_sha256,
            )

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
            if metrics is not None:
                metrics.record_grant_rejected("malformed")
            return await _finish(
                _error_response("grant_invalid"),
                route=route,
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                sql_sha256=body_sha256,
            )

        token = parts[1].strip()

        try:
            claims = verify_grant_token(
                token,
                pipeline.public_keys,
                audience=pipeline.audience,
                issuer=pipeline.issuer,
                require_grant_typ=pipeline.require_grant_typ,
                max_lifetime_seconds=pipeline.max_grant_lifetime_seconds,
            )
        except GrantTokenError as exc:
            if metrics is not None:
                metrics.record_grant_rejected(exc)
            code = getattr(exc, "code", None) or "grant_invalid"
            spec = get_error_spec(code)
            audit_id = getattr(exc, "audit_id", None)
            subject = getattr(exc, "subject", None)
            custom_msg = (
                _safe_grant_invalid_message(exc)
                if spec.code == "grant_invalid"
                else None
            )
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                spec.code,
                type(exc).__name__,
                scrub_error_detail(exc, token=token),
                audit_id or "-",
            )
            return await _finish(
                _error_response(
                    spec.code,
                    message=custom_msg,
                    audit_id=audit_id,
                    token=token,
                ),
                route=route,
                status=401,
                code=spec.code,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=audit_id,
                subject=subject,
                sql_sha256=body_sha256,
                token=token,
            )

        v_jti = claims.get("jti") if isinstance(claims.get("jti"), str) and claims.get("jti") else None
        v_sub = claims.get("sub") if isinstance(claims.get("sub"), str) and claims.get("sub") else None
        claim_api = claims.get("api")
        claim_dialect = claims.get("sql_dialect", "postgres")
        claim_params_sha = claims.get("params_sha256")
        claim_sql_sha = claims.get("sql_sha256")

        # 3. Enforce select.v1 grant contract and exact body SHA-256 binding
        if (
            claim_api != "select.v1"
            or claim_dialect != "postgres"
            or claim_params_sha is not None
            or not isinstance(claim_sql_sha, str)
            or not hmac.compare_digest(body_sha256, claim_sql_sha.lower())
        ):
            if metrics is not None:
                metrics.record_grant_rejected("invalid_claims")
            return await _finish(
                _error_response("grant_invalid", audit_id=v_jti, token=token),
                route=route,
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=v_jti,
                subject=v_sub,
                sql_sha256=body_sha256,
                api=claim_api if isinstance(claim_api, str) else None,
                token=token,
                claims=claims,
            )

        claims_for_grant = (
            {k: v for k, v in claims.items() if k != "params_sha256"}
            if "params_sha256" in claims and claims["params_sha256"] is None
            else claims
        )
        try:
            grant = Grant.from_claims(claims_for_grant)
        except EnforcementError as exc:
            code = getattr(exc, "code", None) or "access_denied"
            spec = get_error_spec(code)
            if metrics is not None and spec.code == "grant_invalid":
                metrics.record_grant_rejected("invalid_claims")
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                spec.code,
                type(exc).__name__,
                scrub_error_detail(exc, token=token),
                v_jti or "-",
            )
            return await _finish(
                _error_response(spec.code, audit_id=v_jti, token=token),
                route=route,
                status=spec.status,
                code=spec.code,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=v_jti,
                subject=v_sub,
                sql_sha256=body_sha256,
                api="select.v1",
                token=token,
                claims=claims,
            )

        # 4. Parse JSON body
        try:
            payload = json.loads(
                raw_body.decode("utf-8", errors="strict"),
                parse_constant=_reject_json_constant,
                object_pairs_hook=reject_duplicate_json_keys,
            )
        except Exception:
            return await _finish(
                _error_response("select_invalid", audit_id=grant.audit_id, token=token),
                route=route,
                status=400,
                code="select_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=grant.audit_id,
                subject=grant.subject,
                workload=grant.workload,
                sql_sha256=body_sha256,
                api="select.v1",
                token=token,
                claims=claims,
                binding=grant.binding,
            )

        if not isinstance(payload, dict):
            return await _finish(
                _error_response("select_invalid", audit_id=grant.audit_id, token=token),
                route=route,
                status=400,
                code="select_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=grant.audit_id,
                subject=grant.subject,
                workload=grant.workload,
                sql_sha256=body_sha256,
                api="select.v1",
                token=token,
                claims=claims,
                binding=grant.binding,
            )

        effective_cursor_key = _resolve_cursor_key()
        if effective_cursor_key is not None:
            try:
                effective_key_bytes = (
                    effective_cursor_key.encode("utf-8")
                    if isinstance(effective_cursor_key, str)
                    else bytes(effective_cursor_key)
                )
            except UnicodeEncodeError:
                effective_key_bytes = b""
            if len(effective_key_bytes) < 32:
                logger.error(
                    "engine=- code=cursor_key_weak error=CursorKeyWeak detail=Pagination cursor signing key is too weak audit_id=%s",
                    grant.audit_id,
                )
                return await _finish(
                    _error_response(
                        "cursor_key_weak",
                        status_code=500,
                        audit_id=grant.audit_id,
                        token=token,
                    ),
                    route=route,
                    status=500,
                    code="cursor_key_weak",
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    audit_id=grant.audit_id,
                    subject=grant.subject,
                    workload=grant.workload,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    token=token,
                    claims=claims,
                    binding=grant.binding,
                )

        compiled_holder: list[CompiledSelect | None] = [None]

        def _compile_and_prepare() -> tuple[CompiledSelect, PreparedQuery]:
            with pipeline._lock:
                placement_snap = pipeline.placement
                schemas_snap = pipeline.schemas
                router_snap = pipeline.router
            comp = compile_select_request(
                payload,
                placement=placement_snap,
                schemas=schemas_snap,
                default_catalog=pipeline.default_catalog,
                grant=grant,
                cursor_key=effective_cursor_key,
                include_cursor_columns=bool(effective_cursor_key),
            )
            compiled_holder[0] = comp
            prep = pipeline.prepare_statement(
                statement=comp.statement.copy(),
                grant=grant,
                sql=raw_body,
                params=comp.params,
                hint=comp.hint,
                max_staleness_seconds=comp.max_staleness_seconds,
                max_rows=comp.limit,
                api="select.v1",
                metrics=metrics,
                _snapshot=(placement_snap, schemas_snap, router_snap),
            )
            return comp, prep

        try:
            compiled, prepared = await run_in_threadpool(_compile_and_prepare)
        except Exception as exc:
            elapsed = (time.perf_counter() - req_start) * 1000.0
            comp_err = compiled_holder[0]
            err_tables = [comp_err.table.lower()] if comp_err is not None else None
            err_paged = comp_err.paged if comp_err is not None else None
            if isinstance(exc, (SelectInvalidError, CursorError, EnforcementError)):
                code = getattr(exc, "code", None) or "access_denied"
                spec = get_error_spec(code)
                if metrics is not None and spec.code == "grant_invalid":
                    metrics.record_grant_rejected("invalid_claims")
                exc_engine = getattr(exc, "engine", None)
                exc_engine_kind = getattr(exc, "engine_kind", None)
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    exc_engine_kind or "-",
                    spec.code,
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token),
                    grant.audit_id or "-",
                )
                return await _finish(
                    _error_response(spec.code, audit_id=grant.audit_id, token=token),
                    route=route,
                    status=spec.status,
                    code=spec.code,
                    elapsed_ms=elapsed,
                    audit_id=grant.audit_id,
                    subject=grant.subject,
                    engine=exc_engine,
                    engine_kind=exc_engine_kind,
                    rule=getattr(exc, "rule", None),
                    access_pattern=getattr(exc, "access_pattern", None),
                    guard=getattr(exc, "guard", None),
                    filter_columns=getattr(exc, "filter_columns", None),
                    effective_max_rows=getattr(exc, "effective_max_rows", None),
                    workload=grant.workload,
                    tables=err_tables,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    paged=err_paged,
                    token=token,
                    claims=claims,
                    binding=grant.binding,
                    include_cold_start=False,
                )
            if isinstance(exc, RoutingError):
                code = getattr(exc, "code", None) or "query_spans_engines"
                spec = get_error_spec(code)
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    "-",
                    spec.code,
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token),
                    grant.audit_id or "-",
                )
                return await _finish(
                    _error_response(spec.code, audit_id=grant.audit_id, token=token),
                    route=route,
                    status=spec.status,
                    code=spec.code,
                    elapsed_ms=elapsed,
                    audit_id=grant.audit_id,
                    subject=grant.subject,
                    workload=grant.workload,
                    tables=err_tables,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    paged=err_paged,
                    token=token,
                    claims=claims,
                    binding=grant.binding,
                )
            typed_code = getattr(exc, "code", None)
            if (
                isinstance(typed_code, str)
                and typed_code in ERROR_CATALOGUE
                and typed_code not in ("internal_error", "audit_unavailable")
            ):
                spec = get_error_spec(typed_code)
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    "-",
                    spec.code,
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token),
                    grant.audit_id or "-",
                )
                return await _finish(
                    _error_response(spec.code, audit_id=grant.audit_id, token=token),
                    route=route,
                    status=spec.status,
                    code=spec.code,
                    elapsed_ms=elapsed,
                    audit_id=grant.audit_id,
                    subject=grant.subject,
                    workload=grant.workload,
                    tables=err_tables,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    paged=err_paged,
                    token=token,
                    claims=claims,
                    binding=grant.binding,
                )
            if _is_audit_unavailable(exc):
                return await _finish(
                    _error_response("audit_unavailable", audit_id=grant.audit_id, token=token),
                    route=route,
                    status=503,
                    code="audit_unavailable",
                    elapsed_ms=elapsed,
                    audit_id=grant.audit_id,
                    subject=grant.subject,
                    workload=grant.workload,
                    tables=err_tables,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    paged=err_paged,
                    token=token,
                    claims=claims,
                    binding=grant.binding,
                )
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                "-",
                "internal_error",
                type(exc).__name__,
                scrub_error_detail(exc, token=token),
                grant.audit_id or "-",
            )
            return await _finish(
                _error_response("internal_error", audit_id=grant.audit_id, token=token),
                route=route,
                status=500,
                code="internal_error",
                elapsed_ms=elapsed,
                audit_id=grant.audit_id,
                subject=grant.subject,
                workload=grant.workload,
                tables=err_tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=err_paged,
                token=token,
                claims=claims,
                binding=grant.binding,
            )

        tables = [compiled.table.lower()]
        params = compiled.params

        # 5. Replay cache check
        try:
            replay_cache.check_and_record(prepared.audit_id, grant.expires_at)
        except GrantReplayedError:
            if metrics is not None:
                metrics.record_grant_rejected("replayed")
            return await _finish(
                _error_response(
                    "grant_replayed",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=401,
                code="grant_replayed",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                include_cold_start=False,
            )
        except GrantReplayCacheFull:
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    status_code=503,
                    category="replay_cache_full",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=503,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                include_cold_start=False,
            )

        adapter = adapters.get(prepared.engine)
        if adapter is None:
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=503,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                include_cold_start=False,
            )

        callable_audit_done = False
        if is_callable_sink and route in ALLOWED_AUDIT_ROUTES:
            pre_sc, pre_acc, pre_lb, pre_cs = _resolve_adapter_audit_metadata(
                adapter,
                engine_kind=prepared.engine_kind,
                workload=prepared.workload,
                result=None,
                include_cold_start=True,
            )
            try:
                pre_record = build_audit_record(
                    route=route,
                    status=200,
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    engine_kind=prepared.engine_kind,
                    storage_class=pre_sc,
                    account=pre_acc,
                    lakebase_endpoint=pre_lb,
                    cold_start=pre_cs,
                    access_pattern=prepared.access_pattern,
                    guard=prepared.guard,
                    filter_columns=prepared.filter_columns,
                    effective_max_rows=prepared.effective_max_rows,
                    paged=compiled.paged,
                    audit_id=prepared.audit_id,
                    subject=prepared.subject,
                    workload=prepared.workload,
                    engine=prepared.engine,
                    rule=prepared.rule,
                    tables=tables,
                    claims=claims,
                    binding=prepared.binding,
                )
                pre_record_dict = pre_record.to_dict()
            except (ValueError, UnicodeError, TypeError):
                min_pre = build_minimal_audit_record(
                    route=route,
                    status=200,
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    audit_id=prepared.audit_id,
                )
                pre_record_dict = min_pre.to_dict()

            try:
                await _run_audit_io(audit_sink, pre_record_dict)
                callable_audit_done = True
            except Exception as exc:
                _on_audit_write_failure()
                if effective_audit_on_failure == "log":
                    callable_audit_done = True
                else:
                    elapsed_audit_err = (time.perf_counter() - req_start) * 1000.0
                    logger.error(
                        "engine=%s code=%s error=%s detail=%s audit_id=%s",
                        prepared.engine_kind or "-",
                        "audit_unavailable",
                        type(exc).__name__,
                        scrub_error_detail(exc, token=token, params=params),
                        prepared.audit_id or "-",
                    )
                    _log_request(
                        audit_id=prepared.audit_id,
                        subject=prepared.subject,
                        engine=prepared.engine,
                        rule=prepared.rule,
                        status=503,
                        elapsed_ms=elapsed_audit_err,
                        binding=prepared.binding,
                        claims=claims,
                    )
                    if metrics is not None:
                        metrics.record_request(
                            route,
                            status=503,
                            duration_seconds=elapsed_audit_err / 1000.0,
                            engine=prepared.engine,
                            error_code="audit_unavailable",
                        )
                    return _error_response(
                        "audit_unavailable",
                        audit_id=prepared.audit_id,
                        token=token,
                        params=params,
                    )

        effective_timeout = (
            min(timeout_seconds, compiled.timeout_s)
            if compiled.timeout_s is not None
            else timeout_seconds
        )
        effective_max_result_bytes = (
            min(max_result_bytes, compiled.max_result_bytes)
            if compiled.max_result_bytes is not None
            else max_result_bytes
        )

        try:
            running_queries.register(prepared.audit_id, prepared.subject)
        except DuplicateQueryIdError:
            return await _finish(
                _error_response(
                    "duplicate_query_id",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=409,
                code="duplicate_query_id",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                include_cold_start=False,
                skip_callable_audit=callable_audit_done,
            )

        cancel_handle: list[Callable[[], None] | None] = [None]

        def on_cancel(cancel_func: Callable[[], None]) -> None:
            cancel_handle[0] = cancel_func
            running_queries.set_cancel_handle(prepared.audit_id, cancel_func)

        try:
            sig = inspect.signature(adapter.execute)
            if "on_cancel_handle" in sig.parameters:
                query_task = asyncio.create_task(
                    run_in_threadpool(
                        adapter.execute,
                        prepared,
                        timeout_seconds=effective_timeout,
                        on_cancel_handle=on_cancel,
                    )
                )
            else:
                query_task = asyncio.create_task(
                    run_in_threadpool(
                        adapter.execute,
                        prepared,
                        timeout_seconds=effective_timeout,
                    )
                )

            while not query_task.done():
                try:
                    await asyncio.wait_for(asyncio.shield(query_task), timeout=0.25)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        if cancel_handle[0] is not None:
                            try:
                                cancel_handle[0]()
                            except Exception:
                                pass
                        try:
                            await query_task
                        except Exception:
                            pass
                        return await _finish(
                            Response(status_code=499),
                            route=route,
                            status=499,
                            code="query_cancelled",
                            elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                            tables=tables,
                            sql_sha256=body_sha256,
                            api="select.v1",
                            paged=compiled.paged,
                            params=params,
                            token=token,
                            claims=claims,
                            prepared=prepared,
                            adapter=adapter,
                            skip_callable_audit=callable_audit_done,
                            log_request_line=False,
                        )

            result = await query_task

        except EngineTimeout as exc:
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "engine_timeout",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "engine_timeout",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=504,
                code="engine_timeout",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except EnginePoolExhausted as exc:
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "engine_unavailable",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=503,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except EngineConfigError as exc:
            raw_cfg_msg = str(exc)
            safe_cfg_msg = (
                raw_cfg_msg
                if WORKLOAD_ACCOUNT_MISSING_RE.fullmatch(raw_cfg_msg)
                else None
            )
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "engine_unavailable",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    status_code=503,
                    category="workload_account_missing",
                    message=safe_cfg_msg,
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=503,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except EngineCancelled as exc:
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "query_cancelled",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "query_cancelled",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=499,
                code="query_cancelled",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except EngineError as exc:
            if _is_engine_starting(exc):
                retry_after = _retry_after_header_value(exc)
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    prepared.engine_kind or "-",
                    "engine_starting",
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token, params=params),
                    prepared.audit_id or "-",
                )
                return await _finish(
                    _error_response(
                        "engine_starting",
                        audit_id=prepared.audit_id,
                        token=token,
                        params=params,
                        headers={"Retry-After": retry_after},
                    ),
                    route=route,
                    status=503,
                    code="engine_starting",
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    tables=tables,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    paged=compiled.paged,
                    params=params,
                    token=token,
                    claims=claims,
                    prepared=prepared,
                    adapter=adapter,
                    skip_callable_audit=callable_audit_done,
                )
            raw_eng_msg = str(exc)
            safe_eng_msg = (
                raw_eng_msg
                if raw_eng_msg in SAFE_ENGINE_ERROR_MESSAGES
                else "Engine query execution failed"
            )
            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "engine_unavailable",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "engine_unavailable",
                    status_code=502,
                    category="engine_error",
                    message=safe_eng_msg,
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=502,
                code="engine_unavailable",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        except Exception as exc:
            if _is_engine_starting(exc):
                retry_after = _retry_after_header_value(exc)
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    prepared.engine_kind or "-",
                    "engine_starting",
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token, params=params),
                    prepared.audit_id or "-",
                )
                return await _finish(
                    _error_response(
                        "engine_starting",
                        audit_id=prepared.audit_id,
                        token=token,
                        params=params,
                        headers={"Retry-After": retry_after},
                    ),
                    route=route,
                    status=503,
                    code="engine_starting",
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    tables=tables,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    paged=compiled.paged,
                    params=params,
                    token=token,
                    claims=claims,
                    prepared=prepared,
                    adapter=adapter,
                    skip_callable_audit=callable_audit_done,
                )
            if _is_audit_unavailable(exc):
                logger.error(
                    "engine=%s code=%s error=%s detail=%s audit_id=%s",
                    prepared.engine_kind or "-",
                    "audit_unavailable",
                    type(exc).__name__,
                    scrub_error_detail(exc, token=token, params=params),
                    prepared.audit_id or "-",
                )
                return await _finish(
                    _error_response(
                        "audit_unavailable",
                        audit_id=prepared.audit_id,
                        token=token,
                        params=params,
                    ),
                    route=route,
                    status=503,
                    code="audit_unavailable",
                    elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    tables=tables,
                    sql_sha256=body_sha256,
                    api="select.v1",
                    paged=compiled.paged,
                    params=params,
                    token=token,
                    claims=claims,
                    prepared=prepared,
                    adapter=adapter,
                    skip_callable_audit=callable_audit_done,
                )

            logger.error(
                "engine=%s code=%s error=%s detail=%s audit_id=%s",
                prepared.engine_kind or "-",
                "internal_error",
                type(exc).__name__,
                scrub_error_detail(exc, token=token, params=params),
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "internal_error",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=500,
                code="internal_error",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                skip_callable_audit=callable_audit_done,
            )
        finally:
            running_queries.unregister(prepared.audit_id)

        effective_row_cap_mode = (
            on_row_cap
            if on_row_cap != "truncate"
            else getattr(pipeline, "on_row_cap", "truncate")
        )
        if (
            effective_row_cap_mode == "error"
            and result.truncated
            and (compiled.limit is None or compiled.limit > grant.max_rows)
        ):
            return await _finish(
                _error_response(
                    "row_cap_hit",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=422,
                code="row_cap_hit",
                elapsed_ms=result.elapsed_ms,
                tables=tables,
                truncated=True,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                result=result,
                skip_callable_audit=callable_audit_done,
            )

        full_safe_rows = [
            [_make_json_safe(cell) for cell in row] for row in result.rows
        ]

        if result.truncated and effective_cursor_key is None:
            logger.error(
                "engine=%s code=cursor_key_missing error=CursorKeyMissing detail=Pagination cursor signing key is not configured audit_id=%s",
                getattr(adapter, "kind", None) or "-",
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "cursor_key_missing",
                    status_code=500,
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=500,
                code="cursor_key_missing",
                elapsed_ms=result.elapsed_ms,
                tables=tables,
                truncated=True,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                result=result,
                skip_callable_audit=callable_audit_done,
            )

        try:
            effective_key_bytes = (
                effective_cursor_key.encode("utf-8")
                if isinstance(effective_cursor_key, str)
                else bytes(effective_cursor_key)
                if effective_cursor_key is not None
                else b""
            )
        except UnicodeEncodeError:
            effective_key_bytes = b""
        if result.truncated and len(effective_key_bytes) < 32:
            logger.error(
                "engine=%s code=cursor_key_weak error=CursorKeyWeak detail=Pagination cursor signing key is too weak audit_id=%s",
                getattr(adapter, "kind", None) or "-",
                prepared.audit_id or "-",
            )
            return await _finish(
                _error_response(
                    "cursor_key_weak",
                    status_code=500,
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=500,
                code="cursor_key_weak",
                elapsed_ms=result.elapsed_ms,
                tables=tables,
                truncated=True,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged,
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                result=result,
                skip_callable_audit=callable_audit_done,
            )

        next_cursor: str | None = None
        if (
            result.truncated
            and full_safe_rows
            and compiled.order_spec
            and effective_cursor_key is not None
        ):
            proj_idx = {
                col.lower(): idx
                for idx, col in enumerate(compiled.projected_columns)
            }
            last_row = full_safe_rows[-1]
            key_values = [
                last_row[proj_idx[col.lower()]] for col, _ in compiled.order_spec
            ]
            next_cursor = encode_cursor(
                compiled.table,
                compiled.order_spec,
                key_values,
                effective_cursor_key,
            )

        user_col_count = len(compiled.columns)
        visible_columns = list(result.columns[:user_col_count])
        col_types = list(result.column_types[:user_col_count])
        if len(col_types) < len(visible_columns):
            col_types.extend(
                ["unknown"] * (len(visible_columns) - len(col_types))
            )
        elif len(col_types) > len(visible_columns):
            col_types = col_types[: len(visible_columns)]

        safe_rows = [row[:user_col_count] for row in full_safe_rows]

        encoded_rows = json.dumps(safe_rows).encode("utf-8")
        if len(encoded_rows) > effective_max_result_bytes:
            return await _finish(
                _error_response(
                    "result_too_large",
                    audit_id=prepared.audit_id,
                    token=token,
                    params=params,
                ),
                route=route,
                status=413,
                code="result_too_large",
                elapsed_ms=result.elapsed_ms,
                tables=tables,
                sql_sha256=body_sha256,
                api="select.v1",
                paged=compiled.paged or (next_cursor is not None),
                params=params,
                token=token,
                claims=claims,
                prepared=prepared,
                adapter=adapter,
                result=result,
                skip_callable_audit=callable_audit_done,
            )

        resp = JSONResponse(
            {
                "columns": visible_columns,
                "column_types": col_types,
                "rows": safe_rows,
                "truncated": result.truncated,
                "notices": ["row_cap_hit"] if result.truncated else [],
                "next": next_cursor,
                "engine": result.engine,
                "rule": prepared.rule,
                "reason": prepared.reason,
                "masked_columns": list(prepared.masked_columns),
                "audit_id": prepared.audit_id,
                "redacted_sql": prepared.redacted_sql,
                "elapsed_ms": result.elapsed_ms,
            }
        )
        return await _finish(
            resp,
            route=route,
            status=200,
            elapsed_ms=result.elapsed_ms,
            engine=result.engine,
            tables=tables,
            row_count=len(safe_rows),
            truncated=bool(result.truncated),
            sql_sha256=body_sha256,
            api="select.v1",
            paged=compiled.paged or (next_cursor is not None),
            params=params,
            token=token,
            claims=claims,
            prepared=prepared,
            adapter=adapter,
            result=result,
            skip_callable_audit=callable_audit_done,
        )

    routes = [
        Route("/healthz", healthz, methods=["GET"]),
        Route("/v1/query", query, methods=["POST"]),
        Route("/v1/select", select, methods=["POST"]),
        Route("/v1/explain", explain, methods=["POST"]),
        Route("/v1/cancel", cancel, methods=["POST"]),
    ]
    if extra_routes:
        routes.extend(extra_routes)

    return Starlette(
        routes=routes,
        lifespan=lifespan,
        exception_handlers={405: handle_405},
    )
