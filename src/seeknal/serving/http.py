"""Serving HTTP API: Starlette application exposing query, explain, and healthz endpoints."""

from __future__ import annotations

import asyncio
import base64
import datetime
import heapq
import inspect
import json
import logging
import math
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
from starlette.requests import Request
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
    AuditManager,
    AuditSink,
    AuditWriteError,
    build_audit_record,
    emit_audit_record,
)
from seeknal.serving.enforce import EnforcementError
from seeknal.serving.errors import (
    AuditUnavailableError,
    build_error_payload,
    get_error_spec,
)
from seeknal.serving.features import _extract_tables
from seeknal.serving.grant_token import (
    GrantTokenError,
    verify_cancel_token,
    verify_grant_token,
)
from seeknal.serving.metrics import MetricsRegistry
from seeknal.serving.router import RoutingError
from seeknal.serving.service import ServingPipeline

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
            for lit in SQL_LITERAL_PATTERN.findall(sql):
                if lit and lit in msg:
                    msg = msg.replace(lit, "[REDACTED]")

    if params and isinstance(params, (list, tuple)):
        for p in params:
            if p is not None:
                p_str = str(p)
                if p_str and p_str in msg:
                    msg = msg.replace(p_str, "[REDACTED]")

    return msg


def _is_audit_unavailable(exc: BaseException) -> bool:
    return (
        isinstance(exc, AuditUnavailableError)
        or getattr(exc, "code", None) == "audit_unavailable"
        or type(exc).__name__ == "AuditUnavailableError"
    )


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
    sql_dialect: str = "postgres",
    default_catalog: str | None = None,
    prepared: Any = None,
) -> list[str]:
    if prepared is not None:
        prep_tables = getattr(prepared, "tables", None)
        if prep_tables is not None:
            return [str(t) for t in prep_tables]
    if not sql or not isinstance(sql, str) or not sql.strip():
        return []
    try:
        stmts = sqlglot.parse(sql, read=sql_dialect)
        if len(stmts) != 1 or stmts[0] is None:
            return []
        stmt = stmts[0]
        if default_catalog is not None:
            cte_names = {cte.alias_or_name.lower() for cte in stmt.find_all(exp.CTE)}
            for tbl in stmt.find_all(exp.Table):
                if (tbl.name.lower() in cte_names and not tbl.db and not tbl.catalog) or (
                    tbl.parts and ".".join(p.name for p in tbl.parts).lower() in cte_names
                ):
                    continue
                if tbl.db and tbl.name and not tbl.catalog:
                    tbl.set("catalog", exp.to_identifier(default_catalog))
        return list(_extract_tables(stmt))
    except Exception:
        return []


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
    _, body = build_error_payload(
        spec.code,
        category=resolved_category,
        message=resolved_msg,
        audit_id=audit_id,
    )
    return JSONResponse(body, status_code=resolved_status)


def _extract_token_claims(token: str | None) -> dict[str, Any]:
    if not token or not isinstance(token, str):
        return {}
    parts = token.split(".")
    if len(parts) != 3:
        return {}
    try:
        payload_b64 = parts[1]
        pad = len(payload_b64) % 4
        if pad:
            payload_b64 += "=" * (4 - pad)
        data = json.loads(base64.urlsafe_b64decode(payload_b64.encode("ascii")).decode("utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


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
    workload: str | None = None,
    azp: str | None = None,
    origin: str | None = None,
    act_sub: str | None = None,
    act_typ: str | None = None,
    token: str | None = None,
) -> None:
    if binding:
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

    if token and isinstance(token, str):
        claims = _extract_token_claims(token)
        if workload is None and "workload" in claims and isinstance(claims["workload"], str):
            workload = claims["workload"]
        if azp is None and "azp" in claims and isinstance(claims["azp"], str):
            azp = claims["azp"]
        if origin is None and "origin" in claims and isinstance(claims["origin"], str):
            origin = claims["origin"]
        act = claims.get("act")
        if isinstance(act, Mapping):
            if act_sub is None and "sub" in act and isinstance(act["sub"], str):
                act_sub = act["sub"]
            if act_typ is None and "typ" in act and isinstance(act["typ"], str):
                act_typ = act["typ"]

    extra_parts = []
    if publication_id is not None:
        extra_parts.append(f"publication_id={publication_id}")
    if revision is not None:
        extra_parts.append(f"revision={revision}")
    if digest is not None:
        extra_parts.append(f"digest={digest}")
    if workload is not None:
        extra_parts.append(f"workload={workload}")
    if azp is not None:
        extra_parts.append(f"azp={azp}")
    if origin is not None:
        extra_parts.append(f"origin={origin}")
    if act_sub is not None:
        extra_parts.append(f"act_sub={act_sub}")
    if act_typ is not None:
        extra_parts.append(f"act_typ={act_typ}")

    extra_str = (" " + " ".join(extra_parts)) if extra_parts else ""

    logger.info(
        "request audit_id=%s subject=%s engine=%s rule=%s status=%d elapsed_ms=%.2f row_count=%s"
        + extra_str,
        audit_id or "-",
        subject or "-",
        engine or "-",
        rule or "-",
        status,
        elapsed_ms,
        str(row_count),
    )


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
    on_row_cap: str = "truncate",
    metrics: MetricsRegistry | None = None,
) -> Starlette:
    """Create a Starlette serving application."""
    if replay_cache is None:
        replay_cache = GrantReplayCache()
    running_queries = RunningQueries()

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

    def _finish(
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
        rule: str | None = None,
        workload: str | None = None,
        tables: Sequence[str] | None = None,
        row_count: int | None = None,
        truncated: bool | None = None,
        sql: str | None = None,
        params: Any = None,
        token: str | None = None,
        log_token: str | None = None,
        claims: Mapping[str, Any] | None = None,
        binding: Mapping[str, Any] | None = None,
        skip_callable_audit: bool = False,
        log_request_line: bool = True,
    ) -> Response:
        if audit_sink is not None:
            record = build_audit_record(
                route=route,
                status=status,
                code=code if status >= 400 else None,
                elapsed_ms=elapsed_ms,
                sql=sql,
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
            if is_structured_sink:
                try:
                    emit_audit_record(
                        audit_sink,
                        record,
                        on_failure=effective_audit_on_failure,
                        on_write_failure=_on_audit_write_failure,
                    )
                except (AuditWriteError, AuditUnavailableError) as exc:
                    logger.error(
                        "engine=%s code=%s error=%s audit_id=%s",
                        engine_kind or "-",
                        "audit_unavailable",
                        type(exc).__name__,
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
                        token=log_token,
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
                    audit_sink(record.to_dict())
                except Exception as exc:
                    _on_audit_write_failure()
                    if effective_audit_on_failure == "log":
                        logger.error(
                            "Audit write failed (on_failure=log, route=%s, audit_id=%s, status=%d): %s",
                            route,
                            audit_id or "-",
                            status,
                            type(exc).__name__,
                        )
                    else:
                        logger.error(
                            "engine=%s code=%s error=%s audit_id=%s",
                            engine_kind or "-",
                            "audit_unavailable",
                            type(exc).__name__,
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
                            token=log_token,
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
                token=log_token,
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

    def _bad_request_with_token(
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
        tables = _extract_logical_tables(
            sql,
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
        return _finish(
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
            log_token=token,
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
                    return _finish(
                        _error_response("payload_too_large"),
                        route=route,
                        status=413,
                        code="payload_too_large",
                        elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                    )
            except ValueError:
                pass

        raw_body = await request.body()
        if len(raw_body) > max_body_bytes:
            return _finish(
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
            return _finish(
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
            return _finish(
                _error_response("grant_invalid"),
                route=route,
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        token = parts[1].strip()

        # 3. Parse JSON body
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            return _bad_request_with_token(token, req_start, route=route)

        if not isinstance(payload, dict):
            return _bad_request_with_token(token, req_start, route=route)

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
            return _bad_request_with_token(
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
            return _bad_request_with_token(token, req_start, route=route)

        sql = payload["sql"]

        params: tuple = ()
        if "params" in payload:
            raw_params = payload["params"]
            if raw_params is not None:
                if not isinstance(raw_params, list):
                    return _bad_request_with_token(
                        token, req_start, route=route, sql=sql
                    )
                params = tuple(raw_params)

        hint = payload.get("hint")
        if hint is not None and not isinstance(hint, str):
            return _bad_request_with_token(
                token, req_start, route=route, sql=sql, params=params
            )

        max_staleness = payload.get("max_staleness_seconds")
        if max_staleness is not None:
            if isinstance(max_staleness, bool) or not isinstance(
                max_staleness, (int, float)
            ):
                return _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )
            if math.isnan(max_staleness) or math.isinf(max_staleness):
                return _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )
            max_staleness = float(max_staleness)

        timeout_s = payload.get("timeout_s")
        if timeout_s is not None:
            if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
                return _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )
            if math.isnan(timeout_s) or math.isinf(timeout_s) or timeout_s <= 0:
                return _bad_request_with_token(
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
                return _bad_request_with_token(
                    token, req_start, route=route, sql=sql, params=params
                )

        req_max_result_bytes = payload.get("max_result_bytes")
        if req_max_result_bytes is not None:
            if (
                isinstance(req_max_result_bytes, bool)
                or not isinstance(req_max_result_bytes, int)
                or req_max_result_bytes <= 0
            ):
                return _bad_request_with_token(
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

    def _handle_prepare_exception(
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
        tables = _extract_logical_tables(
            sql,
            sql_dialect=sql_dialect,
            default_catalog=pipeline.default_catalog,
        )

        if isinstance(exc, GrantTokenError):
            if metrics is not None:
                metrics.record_grant_rejected(exc)
            code = getattr(exc, "code", None) or "grant_invalid"
            audit_id = getattr(exc, "audit_id", None) or v_jti
            subject = getattr(exc, "subject", None) or v_sub
            custom_msg = (
                _safe_grant_invalid_message(exc)
                if code == "grant_invalid"
                else None
            )
            return _finish(
                _error_response(
                    code,
                    message=custom_msg,
                    audit_id=audit_id,
                    token=token,
                    sql=sql,
                    params=params,
                ),
                route=route,
                status=401,
                code=code,
                elapsed_ms=elapsed,
                audit_id=audit_id,
                subject=subject,
                tables=tables,
                sql=sql,
                params=params,
                token=token,
                log_token=token,
                claims=claims,
            )

        if isinstance(exc, EnforcementError):
            code = getattr(exc, "code", None) or "access_denied"
            spec = get_error_spec(code)
            audit_id = getattr(exc, "audit_id", None) or v_jti
            subject = getattr(exc, "subject", None) or v_sub
            return _finish(
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
                log_token=token,
                claims=claims,
            )

        if isinstance(exc, RoutingError):
            code = getattr(exc, "code", None) or "query_spans_engines"
            spec = get_error_spec(code)
            audit_id = getattr(exc, "audit_id", None) or v_jti
            subject = getattr(exc, "subject", None) or v_sub
            return _finish(
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
                log_token=token,
                claims=claims,
            )

        audit_id = getattr(exc, "audit_id", None) or v_jti
        subject = getattr(exc, "subject", None) or v_sub
        if _is_audit_unavailable(exc):
            logger.error(
                "engine=%s code=%s error=%s audit_id=%s",
                "-",
                "audit_unavailable",
                type(exc).__name__,
                audit_id or "-",
            )
            return _finish(
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
                log_token=token,
                claims=claims,
            )

        logger.error(
            "engine=%s code=%s error=%s audit_id=%s",
            "-",
            "internal_error",
            type(exc).__name__,
            audit_id or "-",
        )
        return _finish(
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
            log_token=token,
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
            prepared = pipeline.prepare(
                sql,
                token,
                params=params,
                hint=hint,
                max_staleness_seconds=max_staleness,
                max_rows=max_rows,
            )
        except Exception as exc:
            return _handle_prepare_exception(
                exc,
                route="/v1/explain",
                token=token,
                sql=sql,
                params=params,
                req_start=req_start,
            )

        claims = _extract_token_claims(token)
        sql_dialect = (
            claims["sql_dialect"]
            if isinstance(claims.get("sql_dialect"), str)
            else "postgres"
        )
        tables = _extract_logical_tables(
            sql,
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
        return _finish(
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
            log_token=token,
            claims=claims,
            binding=prepared.binding,
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
            prepared = pipeline.prepare(
                sql,
                token,
                params=params,
                hint=hint,
                max_staleness_seconds=max_staleness,
                max_rows=max_rows,
            )
        except Exception as exc:
            return _handle_prepare_exception(
                exc,
                route="/v1/query",
                token=token,
                sql=sql,
                params=params,
                req_start=req_start,
            )

        claims = _extract_token_claims(token)
        sql_dialect = (
            claims["sql_dialect"]
            if isinstance(claims.get("sql_dialect"), str)
            else "postgres"
        )
        tables = _extract_logical_tables(
            sql,
            sql_dialect=sql_dialect,
            default_catalog=pipeline.default_catalog,
            prepared=prepared,
        )

        if prepared.expires_at is None:
            if metrics is not None:
                metrics.record_grant_rejected("invalid_claims")
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
            )

        try:
            replay_cache.check_and_record(prepared.audit_id, prepared.expires_at)
        except GrantReplayedError:
            if metrics is not None:
                metrics.record_grant_rejected("replayed")
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
            )
        except GrantReplayCacheFull:
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
            )

        adapter = adapters.get(prepared.engine)
        if adapter is None:
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
            )

        callable_audit_done = False
        if is_callable_sink:
            pre_record = build_audit_record(
                route="/v1/query",
                status=200,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                sql=sql,
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                workload=prepared.workload,
                engine=prepared.engine,
                rule=prepared.rule,
                tables=tables,
                claims=claims,
                binding=prepared.binding,
            )
            try:
                audit_sink(pre_record.to_dict())
                callable_audit_done = True
            except Exception as exc:
                _on_audit_write_failure()
                if effective_audit_on_failure == "log":
                    logger.error(
                        "Audit write failed (on_failure=log, route=/v1/query, audit_id=%s, status=200): %s",
                        prepared.audit_id,
                        type(exc).__name__,
                    )
                    callable_audit_done = True
                else:
                    elapsed_audit_err = (time.perf_counter() - req_start) * 1000.0
                    logger.error(
                        "engine=%s code=%s error=%s audit_id=%s",
                        prepared.engine_kind,
                        "audit_unavailable",
                        type(exc).__name__,
                        prepared.audit_id,
                    )
                    _log_request(
                        audit_id=prepared.audit_id,
                        subject=prepared.subject,
                        engine=prepared.engine,
                        rule=prepared.rule,
                        status=503,
                        elapsed_ms=elapsed_audit_err,
                        binding=prepared.binding,
                        token=token,
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
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
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
                        return _finish(
                            Response(status_code=499),
                            route="/v1/query",
                            status=499,
                            code="query_cancelled",
                            elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
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
                            log_token=token,
                            claims=claims,
                            binding=prepared.binding,
                            skip_callable_audit=callable_audit_done,
                            log_request_line=False,
                        )

            result = await query_task

        except EngineTimeout as exc:
            logger.error(
                "engine=%s code=%s error=%s audit_id=%s",
                prepared.engine_kind,
                "engine_timeout",
                type(exc).__name__,
                prepared.audit_id,
            )
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
                skip_callable_audit=callable_audit_done,
            )
        except EnginePoolExhausted as exc:
            logger.error(
                "engine=%s code=%s error=%s audit_id=%s",
                prepared.engine_kind,
                "engine_unavailable",
                type(exc).__name__,
                prepared.audit_id,
            )
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
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
                "engine=%s code=%s error=%s audit_id=%s",
                prepared.engine_kind,
                "engine_unavailable",
                type(exc).__name__,
                prepared.audit_id,
            )
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
                skip_callable_audit=callable_audit_done,
            )
        except EngineCancelled:
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
                skip_callable_audit=callable_audit_done,
            )
        except EngineError as exc:
            raw_eng_msg = str(exc)
            safe_eng_msg = (
                raw_eng_msg
                if raw_eng_msg in SAFE_ENGINE_ERROR_MESSAGES
                else "Engine query execution failed"
            )
            logger.error(
                "engine=%s code=%s error=%s audit_id=%s",
                prepared.engine_kind,
                "engine_unavailable",
                type(exc).__name__,
                prepared.audit_id,
            )
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
                skip_callable_audit=callable_audit_done,
            )
        except Exception as exc:
            if _is_audit_unavailable(exc):
                logger.error(
                    "engine=%s code=%s error=%s audit_id=%s",
                    prepared.engine_kind,
                    "audit_unavailable",
                    type(exc).__name__,
                    prepared.audit_id,
                )
                return _finish(
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
                    log_token=token,
                    claims=claims,
                    binding=prepared.binding,
                    skip_callable_audit=callable_audit_done,
                )

            logger.error(
                "engine=%s code=%s error=%s audit_id=%s",
                prepared.engine_kind,
                "internal_error",
                type(exc).__name__,
                prepared.audit_id,
            )
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
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
            return _finish(
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
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                engine_kind=prepared.engine_kind,
                rule=prepared.rule,
                workload=prepared.workload,
                tables=tables,
                truncated=True,
                sql=sql,
                params=params,
                token=token,
                log_token=token,
                claims=claims,
                binding=prepared.binding,
                skip_callable_audit=callable_audit_done,
            )

        safe_rows = [[_make_json_safe(cell) for cell in row] for row in result.rows]

        encoded_rows = json.dumps(safe_rows).encode("utf-8")
        if len(encoded_rows) > effective_max_result_bytes:
            return _finish(
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
                log_token=token,
                claims=claims,
                binding=prepared.binding,
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
        return _finish(
            resp,
            route="/v1/query",
            status=200,
            elapsed_ms=result.elapsed_ms,
            audit_id=prepared.audit_id,
            subject=prepared.subject,
            engine=result.engine,
            engine_kind=prepared.engine_kind,
            rule=prepared.rule,
            workload=prepared.workload,
            tables=tables,
            row_count=len(safe_rows),
            truncated=bool(result.truncated),
            sql=sql,
            params=params,
            token=token,
            log_token=token,
            claims=claims,
            binding=prepared.binding,
            skip_callable_audit=callable_audit_done,
        )

    async def cancel(request: Request) -> Response:
        req_start = time.perf_counter()
        auth_header = request.headers.get("authorization")
        if not auth_header:
            if metrics is not None:
                metrics.record_grant_rejected("malformed")
            return _finish(
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
            return _finish(
                _error_response("grant_invalid"),
                route="/v1/cancel",
                status=401,
                code="grant_invalid",
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )

        token = parts[1].strip()

        raw_body = await request.body()
        if raw_body.strip():
            c_claims, c_jti, c_sub = _extract_verified_cancel_claims(token, pipeline)
            try:
                body_json = json.loads(raw_body.decode("utf-8"))
            except Exception:
                return _finish(
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
            if not isinstance(body_json, dict) or body_json != {}:
                return _finish(
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
            audit_id = getattr(exc, "audit_id", None)
            subject = getattr(exc, "subject", None)
            return _finish(
                _error_response(code, audit_id=audit_id, token=token),
                route="/v1/cancel",
                status=401,
                code=code,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                audit_id=audit_id,
                subject=subject,
                token=token,
            )

        jti = claims["jti"]
        sub = claims["sub"]
        matched = running_queries.cancel(jti, sub)
        if not matched:
            return _finish(
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

        return _finish(
            JSONResponse({"cancelled": True, "query_id": jti}, status_code=202),
            route="/v1/cancel",
            status=202,
            elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            audit_id=jti,
            subject=sub,
            token=token,
            claims=claims,
        )

    routes = [
        Route("/healthz", healthz, methods=["GET"]),
        Route("/v1/query", query, methods=["POST"]),
        Route("/v1/explain", explain, methods=["POST"]),
        Route("/v1/cancel", cancel, methods=["POST"]),
    ]
    if extra_routes:
        routes.extend(extra_routes)

    return Starlette(routes=routes, lifespan=lifespan)
