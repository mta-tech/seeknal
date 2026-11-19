"""Typed error catalogue and response payload helpers for Seeknal Serving."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any


@dataclass(frozen=True)
class ErrorSpec:
    """Specification for a stable user-facing serving error code."""

    code: str
    status: int
    category: str
    message: str
    next_step: str


class AuditUnavailableError(RuntimeError):
    """Raised when the audit logging destination is unavailable and queries must fail closed."""

    code: str = "audit_unavailable"

    def __init__(
        self,
        message: str = "Audit logging destination is unavailable; query refused.",
        *,
        audit_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = "audit_unavailable"
        self.audit_id = audit_id


REQUIRED_ERROR_CODES: tuple[str, ...] = (
    "grant_expired",
    "grant_invalid",
    "grant_replayed",
    "access_denied",
    "data_stale",
    "engine_unavailable",
    "engine_timeout",
    "query_spans_engines",
    "pii_cross_engine",
    "row_cap_hit",
    "result_too_large",
    "invalid_sql",
    "payload_too_large",
    "query_cancelled",
    "audit_unavailable",
    "internal_error",
)

_CATALOGUE_ENTRIES: tuple[ErrorSpec, ...] = (
    ErrorSpec(
        code="grant_expired",
        status=401,
        category="grant_token_error",
        message="Grant token has expired.",
        next_step="Sign in again or refresh the dashboard to get a new grant.",
    ),
    ErrorSpec(
        code="grant_invalid",
        status=401,
        category="grant_token_error",
        message="Grant token is missing, malformed, or invalid.",
        next_step="Sign in again or request a valid grant token from the catalog.",
    ),
    ErrorSpec(
        code="grant_replayed",
        status=401,
        category="grant_replayed",
        message="Grant token has already been used.",
        next_step="Request a fresh grant token for each query execution.",
    ),
    ErrorSpec(
        code="access_denied",
        status=403,
        category="enforcement_error",
        message="Query violates data access policy or grant permissions.",
        next_step="Request access to the required tables and columns in the catalog.",
    ),
    ErrorSpec(
        code="data_stale",
        status=422,
        category="routing_error",
        message="No engine meets the requested data freshness constraint.",
        next_step="Relax max_staleness_seconds or wait for replication to catch up.",
    ),
    ErrorSpec(
        code="engine_unavailable",
        status=503,
        category="engine_unavailable",
        message="The target execution engine is currently unavailable.",
        next_step="Retry shortly or contact the platform operator if the outage persists.",
    ),
    ErrorSpec(
        code="engine_timeout",
        status=504,
        category="engine_timeout",
        message="Query execution timed out.",
        next_step="Simplify the query, add selective filters, or increase timeout_s.",
    ),
    ErrorSpec(
        code="query_spans_engines",
        status=422,
        category="routing_error",
        message="Query references tables or cubes across incompatible engines.",
        next_step="Query tables served by a single engine or split the query.",
    ),
    ErrorSpec(
        code="pii_cross_engine",
        status=422,
        category="routing_error",
        message="PII table cannot be queried together with tables not served on its engine.",
        next_step="Query the PII table separately on its designated engine.",
    ),
    ErrorSpec(
        code="row_cap_hit",
        status=422,
        category="enforcement_error",
        message="Query exceeded the maximum permitted row cap.",
        next_step="Add a stricter LIMIT or narrow your query filters.",
    ),
    ErrorSpec(
        code="result_too_large",
        status=413,
        category="result_too_large",
        message="Query result size exceeds the maximum byte limit.",
        next_step="Reduce the number of projected columns or rows returned.",
    ),
    ErrorSpec(
        code="invalid_sql",
        status=400,
        category="bad_request",
        message="The SQL statement or request payload is invalid.",
        next_step="Check the SQL syntax and request fields and try again.",
    ),
    ErrorSpec(
        code="payload_too_large",
        status=413,
        category="payload_too_large",
        message="Request body exceeds the maximum size limit.",
        next_step="Shorten the SQL statement or reduce the request payload size.",
    ),
    ErrorSpec(
        code="query_cancelled",
        status=499,
        category="query_cancelled",
        message="Engine query execution cancelled",
        next_step="Re-run the query if the cancellation was unintentional.",
    ),
    ErrorSpec(
        code="audit_unavailable",
        status=503,
        category="audit_unavailable",
        message="Audit logging destination is unavailable; query refused.",
        next_step="Retry later or contact the platform operator to restore the audit sink.",
    ),
    ErrorSpec(
        code="internal_error",
        status=500,
        category="internal_error",
        message="Internal server error",
        next_step="Retry the request or contact support with the audit_id.",
    ),
    ErrorSpec(
        code="not_found",
        status=404,
        category="not_found",
        message="Query not found",
        next_step="Verify the query_id (jti) and that the query is still running.",
    ),
    ErrorSpec(
        code="duplicate_query_id",
        status=409,
        category="duplicate_query_id",
        message="A query with this audit_id is already running.",
        next_step="Wait for the in-flight query to complete or issue a new grant.",
    ),
)

ERROR_CATALOGUE: Mapping[str, ErrorSpec] = MappingProxyType(
    {entry.code: entry for entry in _CATALOGUE_ENTRIES}
)


def get_error_spec(code: str | None) -> ErrorSpec:
    """Return the ErrorSpec for *code*, falling back to ``internal_error``."""
    if isinstance(code, str) and code in ERROR_CATALOGUE:
        return ERROR_CATALOGUE[code]
    return ERROR_CATALOGUE["internal_error"]


def build_error_payload(
    code: str | None,
    *,
    category: str | None = None,
    message: str | None = None,
    audit_id: str | None = None,
) -> tuple[int, dict[str, Any]]:
    """Build ``(http_status, json_body_dict)`` for a catalogue error code."""
    spec = get_error_spec(code)
    payload: dict[str, Any] = {
        "error": category if category is not None else spec.category,
        "code": spec.code,
        "message": message if message is not None else spec.message,
        "next_step": spec.next_step,
    }
    if isinstance(audit_id, str) and audit_id:
        payload["audit_id"] = audit_id
    return spec.status, payload
