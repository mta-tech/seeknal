"""Serving HTTP API: Starlette application exposing query, explain, and healthz endpoints."""

from __future__ import annotations

import base64
import datetime
import json
import logging
import math
import re
import time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from seeknal.serving.adapters import EngineAdapter, EngineError, EngineTimeout
from seeknal.serving.enforce import EnforcementError
from seeknal.serving.grant_token import GrantTokenError
from seeknal.serving.router import RoutingError
from seeknal.serving.service import ServingPipeline

logger = logging.getLogger(__name__)

SQL_LITERAL_PATTERN = re.compile(r"'([^']*)'")


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
) -> None:
    logger.info(
        "request audit_id=%s subject=%s engine=%s rule=%s status=%d elapsed_ms=%.2f row_count=%s",
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
) -> Starlette:
    """Create a Starlette serving application."""

    async def healthz(request: Request) -> Response:
        return JSONResponse({"status": "ok"})

    async def _parse_common_request(request: Request, req_start: float) -> tuple[str, str, tuple, str | None, float | None] | JSONResponse:
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

        unknown_fields = set(payload.keys()) - {"sql", "params", "hint", "max_staleness_seconds"}
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

        return token, sql, params, hint, max_staleness

    async def explain(request: Request) -> Response:
        req_start = time.perf_counter()
        parsed = await _parse_common_request(request, req_start)
        if isinstance(parsed, JSONResponse):
            return parsed

        token, sql, params, hint, max_staleness = parsed

        try:
            prepared = pipeline.prepare(
                sql,
                token,
                params=params,
                hint=hint,
                max_staleness_seconds=max_staleness,
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
            logger.exception("Unexpected error during explain: %s", exc)
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

        token, sql, params, hint, max_staleness = parsed

        try:
            prepared = pipeline.prepare(
                sql,
                token,
                params=params,
                hint=hint,
                max_staleness_seconds=max_staleness,
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
            logger.exception("Unexpected error during query preparation: %s", exc)
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
            )
            return _error_response(
                503,
                "engine_unavailable",
                f"No adapter configured for engine '{prepared.engine}'",
                token=token,
                sql=sql,
                params=params,
            )

        try:
            result = await run_in_threadpool(
                adapter.execute,
                prepared,
                timeout_seconds=timeout_seconds,
            )
        except EngineTimeout as exc:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=504,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )
            return _error_response(504, "engine_timeout", exc, token=token, sql=sql, params=params)
        except EngineError as exc:
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=502,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )
            return _error_response(502, "engine_error", exc, token=token, sql=sql, params=params)
        except Exception as exc:
            logger.exception("Unexpected error during query execution: %s", exc)
            _log_request(
                audit_id=prepared.audit_id,
                subject=prepared.subject,
                engine=prepared.engine,
                rule=prepared.rule,
                status=500,
                elapsed_ms=(time.perf_counter() - req_start) * 1000.0,
            )
            return _error_response(500, "internal_error", "Internal server error", token=token, sql=sql, params=params)

        safe_rows = [[_make_json_safe(cell) for cell in row] for row in result.rows]

        _log_request(
            audit_id=prepared.audit_id,
            subject=prepared.subject,
            engine=prepared.engine,
            rule=prepared.rule,
            status=200,
            elapsed_ms=result.elapsed_ms,
            row_count=len(safe_rows),
        )

        return JSONResponse({
            "columns": list(result.columns),
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

    routes = [
        Route("/healthz", healthz, methods=["GET"]),
        Route("/v1/query", query, methods=["POST"]),
        Route("/v1/explain", explain, methods=["POST"]),
    ]

    return Starlette(routes=routes)
