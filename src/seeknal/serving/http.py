"""Serving HTTP API: Starlette application exposing query, explain, and healthz endpoints."""

from __future__ import annotations

import asyncio
import base64
import datetime
import inspect
import json
import logging
import math
import re
import threading
import time
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

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
    EngineTimeout,
)
from seeknal.serving.enforce import EnforcementError
from seeknal.serving.grant_token import GrantTokenError, verify_cancel_token
from seeknal.serving.router import RoutingError
from seeknal.serving.service import ServingPipeline

logger = logging.getLogger(__name__)

SQL_LITERAL_PATTERN = re.compile(r"'([^']*)'")


class DuplicateQueryIdError(ValueError):
    """Raised when a query with the same jti is already running."""


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


def _error_response(
    status_code: int,
    category: str,
    message: Exception | str,
    *,
    token: str | None = None,
    sql: str | None = None,
    params: Any = None,
) -> JSONResponse:
    safe_msg = _safe_message(message, token=token, sql=sql, params=params)
    return JSONResponse(
        {"error": category, "message": safe_msg},
        status_code=status_code,
    )


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
) -> None:
    if binding:
        if publication_id is None and "publication_id" in binding:
            publication_id = binding["publication_id"]
        if revision is None and "revision" in binding:
            revision = binding["revision"]
        if digest is None and "digest" in binding:
            digest = binding["digest"]

    extra_parts = []
    if publication_id is not None:
        extra_parts.append(f"publication_id={publication_id}")
    if revision is not None:
        extra_parts.append(f"revision={revision}")
    if digest is not None:
        extra_parts.append(f"digest={digest}")

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
) -> Starlette:
    """Create a Starlette serving application."""
    running_queries = RunningQueries()

    async def healthz(request: Request) -> Response:
        return JSONResponse({"status": "ok"})

    async def _parse_common_request(
        request: Request, req_start: float
    ) -> tuple[str, str, tuple, str | None, float | None, float | None, int | None, int | None] | JSONResponse:
        # 1. Check body size limit before parsing
        content_length_hdr = request.headers.get("content-length")
        if content_length_hdr is not None:
            try:
                if int(content_length_hdr) > max_body_bytes:
                    _log_request(status=413, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                    return _error_response(413, "payload_too_large", "Request body exceeds size limit")
            except ValueError:
                pass

        raw_body = await request.body()
        if len(raw_body) > max_body_bytes:
            _log_request(status=413, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(413, "payload_too_large", "Request body exceeds size limit")

        # 2. Check Authorization header
        auth_header = request.headers.get("authorization")
        if not auth_header:
            _log_request(status=401, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(401, "grant_token_error", "Missing Authorization header")

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
            _log_request(status=401, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(401, "grant_token_error", "Malformed Authorization header")

        token = parts[1].strip()

        # 3. Parse JSON body
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception:
            _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(400, "bad_request", "Invalid JSON body", token=token)

        if not isinstance(payload, dict):
            _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(400, "bad_request", "JSON body must be an object", token=token)

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
            _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(
                400,
                "bad_request",
                f"Unknown field(s) in request body: {', '.join(sorted(unknown_fields))}",
                token=token,
            )

        if "sql" not in payload or not isinstance(payload["sql"], str) or not payload["sql"].strip():
            _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(400, "bad_request", "Field 'sql' must be a non-empty string", token=token)

        sql = payload["sql"]

        params: tuple = ()
        if "params" in payload:
            raw_params = payload["params"]
            if raw_params is not None:
                if not isinstance(raw_params, list):
                    _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                    return _error_response(400, "bad_request", "Field 'params' must be a list", token=token, sql=sql)
                params = tuple(raw_params)

        hint = payload.get("hint")
        if hint is not None and not isinstance(hint, str):
            _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(400, "bad_request", "Field 'hint' must be a string", token=token, sql=sql, params=params)

        max_staleness = payload.get("max_staleness_seconds")
        if max_staleness is not None:
            if isinstance(max_staleness, bool) or not isinstance(max_staleness, (int, float)):
                _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                return _error_response(
                    400,
                    "bad_request",
                    "Field 'max_staleness_seconds' must be a number",
                    token=token,
                    sql=sql,
                    params=params,
                )
            if math.isnan(max_staleness) or math.isinf(max_staleness):
                _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                return _error_response(
                    400,
                    "bad_request",
                    "Field 'max_staleness_seconds' must be a finite number",
                    token=token,
                    sql=sql,
                    params=params,
                )
            max_staleness = float(max_staleness)

        timeout_s = payload.get("timeout_s")
        if timeout_s is not None:
            if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
                _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                return _error_response(
                    400,
                    "bad_request",
                    "Field 'timeout_s' must be a positive number",
                    token=token,
                    sql=sql,
                    params=params,
                )
            if math.isnan(timeout_s) or math.isinf(timeout_s) or timeout_s <= 0:
                _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                return _error_response(
                    400,
                    "bad_request",
                    "Field 'timeout_s' must be a positive finite number",
                    token=token,
                    sql=sql,
                    params=params,
                )
            timeout_s = float(timeout_s)

        max_rows = payload.get("max_rows")
        if max_rows is not None:
            if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows <= 0:
                _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                return _error_response(
                    400,
                    "bad_request",
                    "Field 'max_rows' must be a positive integer",
                    token=token,
                    sql=sql,
                    params=params,
                )

        req_max_result_bytes = payload.get("max_result_bytes")
        if req_max_result_bytes is not None:
            if isinstance(req_max_result_bytes, bool) or not isinstance(req_max_result_bytes, int) or req_max_result_bytes <= 0:
                _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                return _error_response(
                    400,
                    "bad_request",
                    "Field 'max_result_bytes' must be a positive integer",
                    token=token,
                    sql=sql,
                    params=params,
                )

        return token, sql, params, hint, max_staleness, timeout_s, max_rows, req_max_result_bytes

    async def explain(request: Request) -> Response:
        req_start = time.perf_counter()
        parsed = await _parse_common_request(request, req_start)
        if isinstance(parsed, JSONResponse):
            return parsed

        token, sql, params, hint, max_staleness, timeout_s, max_rows, req_max_result_bytes = parsed

        try:
            prepared = pipeline.prepare(
                sql,
                token,
                params=params,
                hint=hint,
                max_staleness_seconds=max_staleness,
                max_rows=max_rows,
            )
        except GrantTokenError as exc:
            _log_request(status=401, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(401, "grant_token_error", exc, token=token, sql=sql, params=params)
        except EnforcementError as exc:
            _log_request(status=403, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(403, "enforcement_error", exc, token=token, sql=sql, params=params)
        except RoutingError as exc:
            _log_request(status=422, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(422, "routing_error", exc, token=token, sql=sql, params=params)
        except Exception as exc:
            logger.error("engine=%s error=%s audit_id=%s", "-", type(exc).__name__, "-")
            _log_request(status=500, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(500, "internal_error", "Internal server error", token=token, sql=sql, params=params)

        elapsed_ms = (time.perf_counter() - req_start) * 1000.0
        _log_request(
            audit_id=prepared.audit_id,
            subject=prepared.subject,
            engine=prepared.engine,
            rule=prepared.rule,
            status=200,
            elapsed_ms=elapsed_ms,
            row_count="-",
            binding=prepared.binding,
        )

        return JSONResponse({
            "engine": prepared.engine,
            "rule": prepared.rule,
            "reason": prepared.reason,
            "sql": prepared.sql,
            "paramstyle": prepared.paramstyle,
            "masked_columns": list(prepared.masked_columns),
            "row_limit": prepared.row_limit,
            "redacted_sql": prepared.redacted_sql,
        })

    async def query(request: Request) -> Response:
        req_start = time.perf_counter()
        parsed = await _parse_common_request(request, req_start)
        if isinstance(parsed, JSONResponse):
            return parsed

        token, sql, params, hint, max_staleness, timeout_s, max_rows, req_max_result_bytes = parsed

        try:
            prepared = pipeline.prepare(
                sql,
                token,
                params=params,
                hint=hint,
                max_staleness_seconds=max_staleness,
                max_rows=max_rows,
            )
        except GrantTokenError as exc:
            _log_request(status=401, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(401, "grant_token_error", exc, token=token, sql=sql, params=params)
        except EnforcementError as exc:
            _log_request(status=403, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(403, "enforcement_error", exc, token=token, sql=sql, params=params)
        except RoutingError as exc:
            _log_request(status=422, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(422, "routing_error", exc, token=token, sql=sql, params=params)
        except Exception as exc:
            logger.error("engine=%s error=%s audit_id=%s", "-", type(exc).__name__, "-")
            _log_request(status=500, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(500, "internal_error", "Internal server error", token=token, sql=sql, params=params)

        adapter = adapters.get(prepared.engine)
        if adapter is None:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=503,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                binding=prepared.binding,
            )
            return _error_response(
                503,
                "engine_unavailable",
                f"No adapter configured for engine '{prepared.engine}'",
                token=token,
                sql=sql,
                params=params,
            )

        effective_timeout = min(timeout_seconds, timeout_s) if timeout_s is not None else timeout_seconds
        effective_max_result_bytes = min(max_result_bytes, req_max_result_bytes) if req_max_result_bytes is not None else max_result_bytes

        try:
            running_queries.register(prepared.audit_id, prepared.subject)
        except DuplicateQueryIdError as exc:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=409,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                binding=prepared.binding,
            )
            return _error_response(409, "duplicate_query_id", exc, token=token, sql=sql, params=params)

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
                        return Response(status_code=499)

            result = await query_task

        except EngineTimeout as exc:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=504,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                binding=prepared.binding,
            )
            return _error_response(504, "engine_timeout", exc, token=token, sql=sql, params=params)
        except EngineConfigError as exc:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=503,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                binding=prepared.binding,
            )
            return _error_response(503, "workload_account_missing", exc, token=token, sql=sql, params=params)
        except EngineCancelled as exc:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=499,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                binding=prepared.binding,
            )
            return _error_response(499, "query_cancelled", exc, token=token, sql=sql, params=params)
        except EngineError as exc:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=502,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                binding=prepared.binding,
            )
            return _error_response(502, "engine_error", exc, token=token, sql=sql, params=params)
        except Exception as exc:
            logger.error("engine=%s error=%s audit_id=%s", prepared.engine_kind, type(exc).__name__, prepared.audit_id)
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=500,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
                binding=prepared.binding,
            )
            return _error_response(500, "internal_error", "Internal server error", token=token, sql=sql, params=params)
        finally:
            running_queries.unregister(prepared.audit_id)

        safe_rows = [[_make_json_safe(cell) for cell in row] for row in result.rows]

        encoded_rows = json.dumps(safe_rows).encode("utf-8")
        if len(encoded_rows) > effective_max_result_bytes:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=413,
                elapsed_ms=result.elapsed_ms,
                binding=prepared.binding,
            )
            return _error_response(413, "result_too_large", "Result size exceeds limit", token=token, sql=sql, params=params)

        _log_request(
            audit_id=prepared.audit_id,
            subject=prepared.subject,
            engine=prepared.engine,
            rule=prepared.rule,
            status=200,
            elapsed_ms=result.elapsed_ms,
            row_count=len(safe_rows),
            binding=prepared.binding,
        )

        col_types = list(result.column_types)
        if len(col_types) < len(result.columns):
            col_types.extend(["unknown"] * (len(result.columns) - len(col_types)))
        elif len(col_types) > len(result.columns):
            col_types = col_types[:len(result.columns)]

        return JSONResponse({
            "columns": list(result.columns),
            "column_types": col_types,
            "rows": safe_rows,
            "truncated": result.truncated,
            "engine": result.engine,
            "rule": prepared.rule,
            "reason": prepared.reason,
            "masked_columns": list(prepared.masked_columns),
            "audit_id": prepared.audit_id,
            "redacted_sql": prepared.redacted_sql,
            "elapsed_ms": result.elapsed_ms,
        })

    async def cancel(request: Request) -> Response:
        req_start = time.perf_counter()
        auth_header = request.headers.get("authorization")
        if not auth_header:
            _log_request(status=401, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(401, "grant_token_error", "Missing Authorization header")

        parts = auth_header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
            _log_request(status=401, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(401, "grant_token_error", "Malformed Authorization header")

        token = parts[1].strip()

        raw_body = await request.body()
        if raw_body.strip():
            try:
                body_json = json.loads(raw_body.decode("utf-8"))
            except Exception:
                _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                return _error_response(400, "bad_request", "Invalid JSON body", token=token)
            if not isinstance(body_json, dict) or body_json != {}:
                _log_request(status=400, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
                return _error_response(400, "bad_request", "Body must be empty or {}", token=token)

        try:
            claims = verify_cancel_token(
                token,
                pipeline.public_keys,
                audience=pipeline.audience,
                issuer=pipeline.issuer,
            )
        except GrantTokenError as exc:
            _log_request(status=401, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(401, "grant_token_error", exc, token=token)

        jti = claims["jti"]
        sub = claims["sub"]
        matched = running_queries.cancel(jti, sub)
        if not matched:
            _log_request(audit_id=jti, subject=sub, status=404, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
            return _error_response(404, "not_found", "Query not found", token=token)

        _log_request(audit_id=jti, subject=sub, status=202, elapsed_ms=(time.perf_counter() - req_start) * 1000.0)
        return JSONResponse({"cancelled": True, "query_id": jti}, status_code=202)

    routes = [
        Route("/healthz", healthz, methods=["GET"]),
        Route("/v1/query", query, methods=["POST"]),
        Route("/v1/explain", explain, methods=["POST"]),
        Route("/v1/cancel", cancel, methods=["POST"]),
    ]

    return Starlette(routes=routes)
