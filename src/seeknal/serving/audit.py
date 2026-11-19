"""Durable structured audit logging for Seeknal Serving.

Provides a strictly allowlisted JSON-lines audit record model, pluggable sinks
(file with rotation/retention, stdout, and RFC 5424 syslog over TCP/UDP), and
configurable failure semantics (``refuse`` vs ``log``).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
import datetime
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import socket
import stat
import sys
import threading
import time
from typing import Any, TextIO

from seeknal.serving.errors import AuditUnavailableError, build_error_payload

logger = logging.getLogger(__name__)

ALLOWED_AUDIT_ROUTES: tuple[str, ...] = (
    "/v1/query",
    "/v1/explain",
    "/v1/cancel",
)

ALLOWED_AUDIT_FIELDS: tuple[str, ...] = (
    "timestamp",
    "audit_id",
    "subject",
    "act_sub",
    "act_typ",
    "azp",
    "workload",
    "origin",
    "route",
    "engine",
    "rule",
    "tables",
    "status",
    "code",
    "elapsed_ms",
    "row_count",
    "truncated",
    "publication_id",
    "revision",
    "digest",
    "sql_sha256",
)

ALLOWED_AUDIT_FIELD_SET: frozenset[str] = frozenset(ALLOWED_AUDIT_FIELDS)

FORBIDDEN_AUDIT_KEYS: frozenset[str] = frozenset(
    {
        "sql",
        "raw_sql",
        "redacted_sql",
        "sql_literals",
        "literals",
        "params",
        "parameters",
        "rows",
        "result",
        "results",
        "columns",
        "column_types",
        "token",
        "grant",
        "grant_token",
        "cancel_token",
        "authorization",
        "auth_header",
        "headers",
        "dsn",
        "uri",
        "url",
        "password",
        "passwd",
        "secret",
        "api_key",
        "api_secret",
        "private_key",
        "client_secret",
        "credentials",
    }
)

_ALLOWED_SINKS: frozenset[str] = frozenset({"file", "stdout", "syslog"})
_ALLOWED_ON_FAILURE: frozenset[str] = frozenset({"refuse", "log"})
_ALLOWED_FILE_MODES: frozenset[int] = frozenset({0o600, 0o640})
_ALLOWED_SYSLOG_TRANSPORTS: frozenset[str] = frozenset({"tcp", "udp"})

_KNOWN_AUDIT_CONFIG_KEYS: frozenset[str] = frozenset(
    {
        "sink",
        "on_failure",
        "path",
        "mode",
        "max_bytes",
        "rotate_size_bytes",
        "rotate_interval_seconds",
        "rotate_seconds",
        "max_files",
        "retention_count",
        "max_age_seconds",
        "retention_age_seconds",
        "host",
        "port",
        "transport",
        "protocol",
        "facility",
        "app_name",
        "timeout_seconds",
    }
)

_SHA256_HEX_RE = re.compile(r"^[0-9a-fA-F]{64}$")
_OCTAL_MODE_STR_RE = re.compile(r"^(?:0[oO])?[0-7]{3,4}$")
_SAFE_KEY_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

# Defense-in-depth patterns to prevent tokens, Bearer headers, or DSNs/credentials
# from ever appearing in serialized string fields or diagnostic error messages.
_JWT_COMPACT_RE = re.compile(r"eyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*")
_BEARER_HEADER_RE = re.compile(r"(?i)\b(?:authorization\s*:\s*)?bearer\s+\S+")
_DSN_SCHEME_RE = re.compile(
    r"(?i)\b(?:postgres|postgresql|mysql|mariadb|starrocks|duckdb|sqlite|"
    r"clickhouse|snowflake|bigquery|redshift|mongodb|redis|amqp|mssql|oracle|"
    r"jdbc:[a-z0-9+]+)://\S+"
)
_CREDENTIAL_URI_RE = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^/\s:@]+:[^@\s]+@\S+")
_SUSPICIOUS_DIAGNOSTIC_RE = re.compile(
    r"(://|[^/\s]+:[^@\s]+@|\b(?:secret|password|passwd|token|credential)s?\s*[:=]|\bbearer\s+|eyJ[A-Za-z0-9_-]*\.)",
    re.IGNORECASE,
)


class AuditWriteError(AuditUnavailableError):
    """Raised when an audit destination cannot be verified or written to.

    In ``on_failure == "refuse"`` mode at runtime, HTTP handlers map this
    exception to HTTP 503 ``audit_unavailable`` before returning any rows.
    """

    error_code: str = "audit_unavailable"
    code: str = "audit_unavailable"
    status_code: int = 503

    def __init__(
        self,
        message: str = "Audit log unavailable",
        *,
        audit_id: str | None = None,
    ) -> None:
        super().__init__(_scrub_secret_patterns(str(message)), audit_id=audit_id)
        self.error_code = "audit_unavailable"
        self.code = "audit_unavailable"

    def to_error_dict(self) -> dict[str, Any]:
        """Return a secret-safe HTTP error response body without rows."""
        _, payload = build_error_payload(
            "audit_unavailable",
            message=str(self),
            audit_id=self.audit_id,
        )
        return payload


def _scrub_secret_patterns(text: str) -> str:
    """Redact compact JWTs, Bearer headers, and DSNs/credential URIs from strings."""
    cleaned = _BEARER_HEADER_RE.sub("[REDACTED]", text)
    cleaned = _JWT_COMPACT_RE.sub("[REDACTED]", cleaned)
    cleaned = _CREDENTIAL_URI_RE.sub("[REDACTED]", cleaned)
    cleaned = _DSN_SCHEME_RE.sub("[REDACTED]", cleaned)
    return cleaned


def _safe_diag_target(value: Any, fallback: str = "<redacted>") -> str:
    """Format a path or host safely for diagnostic/startup messages."""
    if not isinstance(value, (str, os.PathLike)):
        return f"<{type(value).__name__}>"
    s = str(value).strip()
    if not s:
        return "<empty>"
    if _SUSPICIOUS_DIAGNOSTIC_RE.search(s):
        return fallback
    return s


def _safe_config_key_label(key: Any) -> str:
    """Format an unknown config key name without echoing suspicious strings."""
    if not isinstance(key, str):
        return f"<{type(key).__name__}>"
    if _SUSPICIOUS_DIAGNOSTIC_RE.search(key) or not _SAFE_KEY_NAME_RE.fullmatch(key):
        return "<redacted-key>"
    return key


def compute_sql_sha256(sql: str | None) -> str | None:
    """Compute lowercase hex SHA-256 of SQL text when non-empty, else ``None``."""
    if isinstance(sql, str) and len(sql) > 0:
        return hashlib.sha256(sql.encode("utf-8")).hexdigest()
    return None


def format_utc_timestamp(
    ts: datetime.datetime | float | int | str | None = None,
) -> str:
    """Normalize a timestamp into a UTC ISO-8601 string ending with ``Z``."""
    if ts is None:
        dt = datetime.datetime.now(datetime.timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")

    if isinstance(ts, bool):
        raise ValueError("timestamp must not be a boolean")

    if isinstance(ts, (int, float)):
        if not math.isfinite(float(ts)):
            raise ValueError("timestamp epoch must be finite")
        dt = datetime.datetime.fromtimestamp(float(ts), tz=datetime.timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")

    if isinstance(ts, datetime.datetime):
        if ts.tzinfo is None:
            dt = ts.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = ts.astimezone(datetime.timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")

    if isinstance(ts, str):
        s = ts.strip()
        if not s:
            raise ValueError("timestamp string must not be empty")
        iso_candidate = s[:-1] + "+00:00" if s.endswith(("Z", "z")) else s
        try:
            parsed = datetime.datetime.fromisoformat(iso_candidate)
        except ValueError as exc:
            raise ValueError("timestamp must be a valid ISO-8601 datetime string") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        else:
            parsed = parsed.astimezone(datetime.timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")

    raise ValueError(f"Unsupported timestamp type: {type(ts).__name__}")


def _normalize_optional_str(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Audit field '{field_name}' must be a string or None")
    return _scrub_secret_patterns(value)


def _normalize_tables(
    tables: Iterable[str] | Mapping[str, Any] | None,
    logical_tables: Iterable[str] | Mapping[str, Any] | None = None,
) -> list[str]:
    source = tables if tables is not None else logical_tables
    if source is None:
        return []
    if isinstance(source, (str, bytes)):
        raise ValueError("Audit field 'tables' must be a sequence of table names, not a string")
    if isinstance(source, Mapping):
        raw_items: Iterable[Any] = source.keys()
    elif isinstance(source, Iterable):
        raw_items = source
    else:
        raise ValueError("Audit field 'tables' must be an iterable of strings")

    normalized: list[str] = []
    for item in raw_items:
        if not isinstance(item, str) or not item.strip():
            raise ValueError("Each entry in audit 'tables' must be a non-empty string")
        normalized.append(_scrub_secret_patterns(item.strip()))
    return normalized


def _normalize_sql_sha256(
    sql_sha256: str | None,
    sql: str | None = None,
) -> str | None:
    if isinstance(sql, str) and len(sql) > 0:
        return compute_sql_sha256(sql)
    if sql_sha256 is None:
        return None
    if not isinstance(sql_sha256, str) or not _SHA256_HEX_RE.fullmatch(sql_sha256):
        raise ValueError(
            "Audit field 'sql_sha256' must be a 64-character hexadecimal SHA-256 digest or None"
        )
    return sql_sha256.lower()


@dataclass(init=False)
class AuditRecord:
    """Strictly allowlisted structured audit record for a single Serving request."""

    timestamp: str
    audit_id: str | None
    subject: str | None
    act_sub: str | None
    act_typ: str | None
    azp: str | None
    workload: str | None
    origin: str | None
    route: str
    engine: str | None
    rule: str | None
    tables: list[str]
    status: int
    code: str | None
    elapsed_ms: float
    row_count: int | None
    truncated: bool | None
    publication_id: str | None
    revision: int | str | None
    digest: str | None
    sql_sha256: str | None

    def __init__(
        self,
        *,
        route: str,
        status: int,
        elapsed_ms: float = 0.0,
        timestamp: datetime.datetime | float | int | str | None = None,
        audit_id: str | None = None,
        subject: str | None = None,
        act_sub: str | None = None,
        act_typ: str | None = None,
        azp: str | None = None,
        workload: str | None = None,
        origin: str | None = None,
        engine: str | None = None,
        rule: str | None = None,
        tables: Sequence[str] | None = None,
        logical_tables: Sequence[str] | None = None,
        code: str | None = None,
        error_code: str | None = None,
        row_count: int | None = None,
        truncated: bool | None = None,
        publication_id: str | None = None,
        revision: int | str | None = None,
        digest: str | None = None,
        sql_sha256: str | None = None,
        sql: str | None = None,
    ) -> None:
        if not isinstance(route, str) or route not in ALLOWED_AUDIT_ROUTES:
            raise ValueError(
                f"Audit field 'route' must be one of {ALLOWED_AUDIT_ROUTES}, got {route!r}"
            )

        if isinstance(status, bool) or not isinstance(status, int) or not (100 <= status <= 599):
            raise ValueError("Audit field 'status' must be an HTTP status integer in 100..599")

        if code is not None and error_code is not None and code != error_code:
            raise ValueError("Conflicting 'code' and 'error_code' supplied to AuditRecord")
        resolved_code_raw = code if code is not None else error_code
        resolved_code = (
            _normalize_optional_str(resolved_code_raw, "code")
            if status >= 400
            else None
        )

        if tables is not None and logical_tables is not None and list(tables) != list(logical_tables):
            raise ValueError("Conflicting 'tables' and 'logical_tables' supplied to AuditRecord")

        if isinstance(elapsed_ms, bool) or not isinstance(elapsed_ms, (int, float)):
            raise ValueError("Audit field 'elapsed_ms' must be a finite non-negative number")
        elapsed_f = float(elapsed_ms)
        if not math.isfinite(elapsed_f) or elapsed_f < 0.0:
            raise ValueError("Audit field 'elapsed_ms' must be a finite non-negative number")
        rounded_elapsed = round(elapsed_f, 2)
        if rounded_elapsed == 0.0:
            rounded_elapsed = 0.0

        if row_count is not None:
            if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
                raise ValueError("Audit field 'row_count' must be a non-negative int or None")

        if truncated is not None and not isinstance(truncated, bool):
            raise ValueError("Audit field 'truncated' must be a bool or None")

        if revision is not None:
            if isinstance(revision, bool) or not isinstance(revision, (int, str)):
                raise ValueError("Audit field 'revision' must be an int, string, or None")
            if isinstance(revision, str):
                revision = _scrub_secret_patterns(revision)

        self.timestamp = format_utc_timestamp(timestamp)
        self.audit_id = _normalize_optional_str(audit_id, "audit_id")
        self.subject = _normalize_optional_str(subject, "subject")
        self.act_sub = _normalize_optional_str(act_sub, "act_sub")
        self.act_typ = _normalize_optional_str(act_typ, "act_typ")
        self.azp = _normalize_optional_str(azp, "azp")
        self.workload = _normalize_optional_str(workload, "workload")
        self.origin = _normalize_optional_str(origin, "origin")
        self.route = route
        self.engine = _normalize_optional_str(engine, "engine")
        self.rule = _normalize_optional_str(rule, "rule")
        self.tables = _normalize_tables(tables, logical_tables)
        self.status = status
        self.code = resolved_code
        self.elapsed_ms = rounded_elapsed
        self.row_count = row_count
        self.truncated = truncated
        self.publication_id = _normalize_optional_str(publication_id, "publication_id")
        self.revision = revision
        self.digest = _normalize_optional_str(digest, "digest")
        self.sql_sha256 = _normalize_sql_sha256(sql_sha256, sql=sql)

    @property
    def error_code(self) -> str | None:
        """Alias for ``code`` (typed error code when ``status >= 400``)."""
        return self.code

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary containing strictly the allowlisted audit fields."""
        out: dict[str, Any] = {}
        for key in ALLOWED_AUDIT_FIELDS:
            val = getattr(self, key)
            if key == "tables":
                out[key] = [_scrub_secret_patterns(str(t)) for t in val]
            elif isinstance(val, str) and key not in ("timestamp", "route", "sql_sha256"):
                out[key] = _scrub_secret_patterns(val)
            else:
                out[key] = val
        return out

    def to_json(self) -> str:
        """Serialize this record to a single compact JSON line (without trailing newline)."""
        return serialize_audit_record(self)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = False,
    ) -> AuditRecord:
        """Validate and construct an ``AuditRecord`` from a mapping.

        When ``strict=False`` (default), any non-allowlisted or forbidden keys
        are stripped (with ``sql`` hashed into ``sql_sha256`` if ``sql_sha256``
        is absent). When ``strict=True``, forbidden or unknown keys raise ``ValueError``.
        """
        if not isinstance(data, Mapping):
            raise ValueError(f"Audit record must be a mapping, got {type(data).__name__}")

        permitted_input_keys = ALLOWED_AUDIT_FIELD_SET | {"logical_tables", "error_code"}
        if strict:
            extra_keys = set(data.keys()) - permitted_input_keys
            if extra_keys:
                safe_names = ", ".join(sorted(_safe_config_key_label(k) for k in extra_keys))
                raise ValueError(f"Forbidden or unknown key(s) in audit record: {safe_names}")

        raw_sql = data.get("sql")
        sql_for_hash = raw_sql if isinstance(raw_sql, str) else None

        raw_row_count = data.get("row_count")
        if raw_row_count == "-":
            raw_row_count = None

        return cls(
            timestamp=data.get("timestamp"),
            audit_id=data.get("audit_id"),
            subject=data.get("subject"),
            act_sub=data.get("act_sub"),
            act_typ=data.get("act_typ"),
            azp=data.get("azp"),
            workload=data.get("workload"),
            origin=data.get("origin"),
            route=data["route"],
            engine=data.get("engine"),
            rule=data.get("rule"),
            tables=data.get("tables"),
            logical_tables=data.get("logical_tables"),
            status=data["status"],
            code=data.get("code"),
            error_code=data.get("error_code"),
            elapsed_ms=data.get("elapsed_ms", 0.0),
            row_count=raw_row_count,
            truncated=data.get("truncated"),
            publication_id=data.get("publication_id"),
            revision=data.get("revision"),
            digest=data.get("digest"),
            sql_sha256=data.get("sql_sha256"),
            sql=sql_for_hash,
        )


def build_audit_record(
    *,
    route: str,
    status: int,
    elapsed_ms: float = 0.0,
    timestamp: datetime.datetime | float | int | str | None = None,
    sql: str | None = None,
    sql_sha256: str | None = None,
    audit_id: str | None = None,
    subject: str | None = None,
    act_sub: str | None = None,
    act_typ: str | None = None,
    azp: str | None = None,
    workload: str | None = None,
    origin: str | None = None,
    engine: str | None = None,
    rule: str | None = None,
    tables: Sequence[str] | None = None,
    logical_tables: Sequence[str] | None = None,
    code: str | None = None,
    error_code: str | None = None,
    row_count: int | str | None = None,
    truncated: bool | None = None,
    publication_id: str | None = None,
    revision: int | str | None = None,
    digest: str | None = None,
    claims: Mapping[str, Any] | None = None,
    binding: Mapping[str, Any] | None = None,
    **_ignored_forbidden_or_extra: Any,
) -> AuditRecord:
    """Build a strictly allowlisted ``AuditRecord`` from request/grant metadata.

    Any extra keyword arguments (such as ``token``, ``grant_token``, ``params``,
    ``rows``, ``authorization``, ``dsn``, ``redacted_sql``) are intentionally
    discarded so sensitive data can never enter an ``AuditRecord``.
    """
    if binding and isinstance(binding, Mapping):
        if publication_id is None and isinstance(binding.get("publication_id"), str):
            publication_id = binding["publication_id"]
        if revision is None and "revision" in binding:
            rev = binding["revision"]
            if (isinstance(rev, int) and not isinstance(rev, bool)) or isinstance(rev, str):
                revision = rev
        if digest is None and isinstance(binding.get("digest"), str):
            digest = binding["digest"]
        if workload is None and isinstance(binding.get("workload"), str):
            workload = binding["workload"]
        if azp is None and isinstance(binding.get("azp"), str):
            azp = binding["azp"]
        if origin is None and isinstance(binding.get("origin"), str):
            origin = binding["origin"]
        if act_sub is None and isinstance(binding.get("act_sub"), str):
            act_sub = binding["act_sub"]
        if act_typ is None and isinstance(binding.get("act_typ"), str):
            act_typ = binding["act_typ"]

    if claims and isinstance(claims, Mapping):
        if audit_id is None and isinstance(claims.get("jti"), str):
            audit_id = claims["jti"]
        if subject is None and isinstance(claims.get("sub"), str):
            subject = claims["sub"]
        act = claims.get("act")
        if isinstance(act, Mapping):
            if act_sub is None and isinstance(act.get("sub"), str):
                act_sub = act["sub"]
            if act_typ is None and isinstance(act.get("typ"), str):
                act_typ = act["typ"]
        if azp is None and isinstance(claims.get("azp"), str):
            azp = claims["azp"]
        if workload is None and isinstance(claims.get("workload"), str):
            workload = claims["workload"]
        if origin is None and isinstance(claims.get("origin"), str):
            origin = claims["origin"]
        if publication_id is None and isinstance(claims.get("publication_id"), str):
            publication_id = claims["publication_id"]
        if revision is None and "revision" in claims:
            c_rev = claims["revision"]
            if (isinstance(c_rev, int) and not isinstance(c_rev, bool)) or isinstance(c_rev, str):
                revision = c_rev
        if digest is None and isinstance(claims.get("digest"), str):
            digest = claims["digest"]

    normalized_row_count: int | None
    if row_count == "-":
        normalized_row_count = None
    else:
        normalized_row_count = row_count  # type: ignore[assignment]

    return AuditRecord(
        timestamp=timestamp,
        audit_id=audit_id,
        subject=subject,
        act_sub=act_sub,
        act_typ=act_typ,
        azp=azp,
        workload=workload,
        origin=origin,
        route=route,
        engine=engine,
        rule=rule,
        tables=tables,
        logical_tables=logical_tables,
        status=status,
        code=code,
        error_code=error_code,
        elapsed_ms=elapsed_ms,
        row_count=normalized_row_count,
        truncated=truncated,
        publication_id=publication_id,
        revision=revision,
        digest=digest,
        sql_sha256=sql_sha256,
        sql=sql,
    )


def serialize_audit_record(record: AuditRecord | Mapping[str, Any]) -> str:
    """Serialize an ``AuditRecord`` or mapping into a single allowlisted JSON line."""
    if isinstance(record, AuditRecord):
        validated = record
    elif isinstance(record, Mapping):
        validated = AuditRecord.from_dict(record, strict=False)
    else:
        raise ValueError(f"Expected AuditRecord or Mapping, got {type(record).__name__}")

    raw_dict = validated.to_dict()
    # Enforce strict allowlist and explicit forbidden-key exclusion at serialization boundary.
    allowlisted_payload = {
        key: raw_dict[key]
        for key in ALLOWED_AUDIT_FIELDS
        if key in raw_dict and key not in FORBIDDEN_AUDIT_KEYS
    }
    if set(allowlisted_payload.keys()) != ALLOWED_AUDIT_FIELD_SET:
        raise ValueError("Serialized audit record is missing required allowlisted keys")

    return json.dumps(allowlisted_payload, separators=(",", ":"), ensure_ascii=False)


# ---------------------------------------------------------------------------
# Configuration Model
# ---------------------------------------------------------------------------


def _parse_file_mode(raw_mode: Any, error_cls: type[Exception]) -> int:
    """Validate file mode, accepting ``0o600``/``0o640`` or ``"0600"``/``"0640"``."""
    if isinstance(raw_mode, bool):
        raise error_cls("audit.mode must be '0600' or '0640', got bool")

    parsed: int | None = None
    if isinstance(raw_mode, int):
        parsed = raw_mode
    elif isinstance(raw_mode, str):
        s = raw_mode.strip()
        if not _OCTAL_MODE_STR_RE.fullmatch(s):
            raise error_cls("audit.mode must be '0600' or '0640'")
        octal_digits = s[2:] if s.lower().startswith("0o") else s
        parsed = int(octal_digits, 8)
    else:
        raise error_cls(
            f"audit.mode must be '0600' or '0640', got {type(raw_mode).__name__}"
        )

    if (parsed & 0o022) != 0:
        raise error_cls(
            f"audit.mode {oct(parsed)} is group- or world-writable; only 0600 or 0640 are permitted"
        )
    if parsed not in _ALLOWED_FILE_MODES:
        raise error_cls(
            f"audit.mode {oct(parsed)} is not allowed; must be 0600 or 0640"
        )
    return parsed


def _parse_optional_positive_int(
    value: Any,
    field_name: str,
    error_cls: type[Exception],
) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise error_cls(f"audit.{field_name} must be a positive integer")
    return value


def _parse_optional_positive_float(
    value: Any,
    field_name: str,
    error_cls: type[Exception],
) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise error_cls(f"audit.{field_name} must be a positive number")
    f_val = float(value)
    if not math.isfinite(f_val) or f_val <= 0.0:
        raise error_cls(f"audit.{field_name} must be a positive finite number")
    return f_val


def _resolve_aliased_option(
    data: Mapping[str, Any],
    primary: str,
    alias: str,
    error_cls: type[Exception],
    default: Any = None,
) -> Any:
    has_primary = primary in data
    has_alias = alias in data
    if has_primary and has_alias:
        raise error_cls(
            f"Cannot specify both '{primary}' and '{alias}' in audit configuration"
        )
    if has_primary:
        return data[primary]
    if has_alias:
        return data[alias]
    return default


@dataclass(frozen=True)
class AuditConfig:
    """Configuration model for Seeknal Serving durable audit logging."""

    sink: str = "file"
    on_failure: str = "refuse"
    # File sink options
    path: str | None = None
    mode: int = 0o600
    max_bytes: int | None = None
    rotate_interval_seconds: float | None = None
    max_files: int | None = None
    max_age_seconds: float | None = None
    # Syslog sink options
    host: str = "127.0.0.1"
    port: int = 514
    transport: str = "udp"
    facility: int = 16
    app_name: str = "seeknal-serving"
    timeout_seconds: float = 2.0

    def __post_init__(self) -> None:
        self._validate(ValueError)

    def _validate(self, error_cls: type[Exception]) -> None:
        if not isinstance(self.sink, str) or self.sink not in _ALLOWED_SINKS:
            raise error_cls(
                f"audit.sink must be one of {sorted(_ALLOWED_SINKS)}, got {_safe_diag_target(self.sink)!r}"
            )

        if not isinstance(self.on_failure, str) or self.on_failure not in _ALLOWED_ON_FAILURE:
            raise error_cls(
                f"audit.on_failure must be 'refuse' or 'log', got {_safe_diag_target(self.on_failure)!r}"
            )

        normalized_mode = _parse_file_mode(self.mode, error_cls)
        object.__setattr__(self, "mode", normalized_mode)

        if self.path is not None:
            if not isinstance(self.path, (str, os.PathLike)):
                raise error_cls("audit.path must be a file path string")
            path_str = str(self.path).strip()
            if not path_str or "\x00" in path_str:
                raise error_cls("audit.path must be a non-empty file path without null bytes")
            object.__setattr__(self, "path", path_str)

        if self.sink == "file" and not self.path:
            raise error_cls("audit.path is required when audit.sink is 'file'")

        if self.max_bytes is not None:
            _parse_optional_positive_int(self.max_bytes, "max_bytes", error_cls)

        if self.rotate_interval_seconds is not None:
            rot_f = _parse_optional_positive_float(
                self.rotate_interval_seconds, "rotate_interval_seconds", error_cls
            )
            object.__setattr__(self, "rotate_interval_seconds", rot_f)

        if self.max_files is not None:
            _parse_optional_positive_int(self.max_files, "max_files", error_cls)

        if self.max_age_seconds is not None:
            age_f = _parse_optional_positive_float(
                self.max_age_seconds, "max_age_seconds", error_cls
            )
            object.__setattr__(self, "max_age_seconds", age_f)

        if not isinstance(self.host, str) or not self.host.strip() or any(
            ch.isspace() for ch in self.host.strip()
        ):
            raise error_cls("audit.host must be a non-empty hostname or IP address")
        object.__setattr__(self, "host", self.host.strip())

        if isinstance(self.port, bool) or not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            raise error_cls("audit.port must be an integer between 1 and 65535")

        if not isinstance(self.transport, str) or self.transport not in _ALLOWED_SYSLOG_TRANSPORTS:
            raise error_cls("audit.transport must be 'tcp' or 'udp'")

        if isinstance(self.facility, bool) or not isinstance(self.facility, int) or not (0 <= self.facility <= 23):
            raise error_cls("audit.facility must be an integer between 0 and 23")

        if (
            not isinstance(self.app_name, str)
            or not self.app_name.strip()
            or len(self.app_name.strip()) > 48
            or any(ord(ch) < 33 or ord(ch) > 126 for ch in self.app_name.strip())
        ):
            raise error_cls(
                "audit.app_name must be 1..48 printable US-ASCII characters without spaces"
            )
        object.__setattr__(self, "app_name", self.app_name.strip())

        timeout_f = _parse_optional_positive_float(
            self.timeout_seconds, "timeout_seconds", error_cls
        )
        assert timeout_f is not None
        object.__setattr__(self, "timeout_seconds", timeout_f)

    @property
    def rotate_size_bytes(self) -> int | None:
        return self.max_bytes

    @property
    def rotate_seconds(self) -> float | None:
        return self.rotate_interval_seconds

    @property
    def retention_count(self) -> int | None:
        return self.max_files

    @property
    def retention_age_seconds(self) -> float | None:
        return self.max_age_seconds

    @property
    def protocol(self) -> str:
        return self.transport

    @classmethod
    def from_dict(
        cls,
        d: Mapping[str, Any],
        *,
        error_cls: type[Exception] = ValueError,
    ) -> AuditConfig:
        """Validate and construct ``AuditConfig`` from a configuration dictionary."""
        if not isinstance(d, Mapping):
            raise error_cls(f"audit config must be a mapping, got {type(d).__name__}")

        unknown_keys = [k for k in d.keys() if k not in _KNOWN_AUDIT_CONFIG_KEYS]
        if unknown_keys:
            formatted = ", ".join(sorted(_safe_config_key_label(k) for k in unknown_keys))
            raise error_cls(f"Unknown key(s) in audit config: {formatted}")

        sink = d.get("sink", "file")
        on_failure = d.get("on_failure", "refuse")
        path = d.get("path")
        raw_mode = d.get("mode", 0o600)

        raw_max_bytes = _resolve_aliased_option(
            d, "max_bytes", "rotate_size_bytes", error_cls
        )
        raw_rotate_interval = _resolve_aliased_option(
            d, "rotate_interval_seconds", "rotate_seconds", error_cls
        )
        raw_max_files = _resolve_aliased_option(
            d, "max_files", "retention_count", error_cls
        )
        raw_max_age = _resolve_aliased_option(
            d, "max_age_seconds", "retention_age_seconds", error_cls
        )
        raw_transport = _resolve_aliased_option(
            d, "transport", "protocol", error_cls, default="udp"
        )

        host = d.get("host", "127.0.0.1")
        port = d.get("port", 514)
        facility = d.get("facility", 16)
        app_name = d.get("app_name", "seeknal-serving")
        timeout_seconds = d.get("timeout_seconds", 2.0)

        # Validate using caller's error_cls before instantiation so ServerConfigError
        # (or custom error_cls) is raised directly for any invalid option.
        if not isinstance(sink, str) or sink not in _ALLOWED_SINKS:
            raise error_cls(
                f"audit.sink must be one of {sorted(_ALLOWED_SINKS)}, got {_safe_diag_target(sink)!r}"
            )
        if not isinstance(on_failure, str) or on_failure not in _ALLOWED_ON_FAILURE:
            raise error_cls(
                f"audit.on_failure must be 'refuse' or 'log', got {_safe_diag_target(on_failure)!r}"
            )

        parsed_mode = _parse_file_mode(raw_mode, error_cls)

        if path is not None:
            if not isinstance(path, (str, os.PathLike)):
                raise error_cls("audit.path must be a file path string")
            path_str = str(path).strip()
            if not path_str or "\x00" in path_str:
                raise error_cls("audit.path must be a non-empty file path")
        else:
            path_str = None

        if sink == "file" and not path_str:
            raise error_cls("audit.path is required when audit.sink is 'file'")

        parsed_max_bytes = _parse_optional_positive_int(
            raw_max_bytes, "max_bytes", error_cls
        )
        parsed_rotate_interval = _parse_optional_positive_float(
            raw_rotate_interval, "rotate_interval_seconds", error_cls
        )
        parsed_max_files = _parse_optional_positive_int(
            raw_max_files, "max_files", error_cls
        )
        parsed_max_age = _parse_optional_positive_float(
            raw_max_age, "max_age_seconds", error_cls
        )

        if not isinstance(host, str) or not host.strip() or any(ch.isspace() for ch in host.strip()):
            raise error_cls("audit.host must be a non-empty hostname or IP address")

        if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
            raise error_cls("audit.port must be an integer between 1 and 65535")

        if not isinstance(raw_transport, str) or raw_transport not in _ALLOWED_SYSLOG_TRANSPORTS:
            raise error_cls("audit.transport must be 'tcp' or 'udp'")

        if isinstance(facility, bool) or not isinstance(facility, int) or not (0 <= facility <= 23):
            raise error_cls("audit.facility must be an integer between 0 and 23")

        if (
            not isinstance(app_name, str)
            or not app_name.strip()
            or len(app_name.strip()) > 48
            or any(ord(ch) < 33 or ord(ch) > 126 for ch in app_name.strip())
        ):
            raise error_cls(
                "audit.app_name must be 1..48 printable US-ASCII characters without spaces"
            )

        parsed_timeout = _parse_optional_positive_float(
            timeout_seconds, "timeout_seconds", error_cls
        )
        assert parsed_timeout is not None

        return cls(
            sink=sink,
            on_failure=on_failure,
            path=path_str,
            mode=parsed_mode,
            max_bytes=parsed_max_bytes,
            rotate_interval_seconds=parsed_rotate_interval,
            max_files=parsed_max_files,
            max_age_seconds=parsed_max_age,
            host=host.strip(),
            port=port,
            transport=raw_transport,
            facility=facility,
            app_name=app_name.strip(),
            timeout_seconds=parsed_timeout,
        )


# ---------------------------------------------------------------------------
# Pluggable Audit Sinks
# ---------------------------------------------------------------------------


class AuditSink(ABC):
    """Abstract base class for pluggable durable audit sinks."""

    @abstractmethod
    def verify_writable(self) -> None:
        """Verify that the destination is writable/reachable at startup.

        Raises ``AuditWriteError`` if the destination cannot be opened or reached.
        """

    @abstractmethod
    def write_record(self, record: AuditRecord | Mapping[str, Any]) -> None:
        """Serialize and durably write a single audit record.

        Raises ``AuditWriteError`` on destination write failure.
        """

    def write(self, record: AuditRecord | Mapping[str, Any]) -> None:
        """Alias for :meth:`write_record`."""
        self.write_record(record)

    def close(self) -> None:
        """Release any open file descriptors or sockets."""


def _write_all_fd(fd: int, data: bytes) -> None:
    """Write all bytes to a file descriptor, handling short writes."""
    view = memoryview(data)
    written = 0
    total = len(data)
    while written < total:
        n = os.write(fd, view[written:])
        if n <= 0:
            raise OSError("Short write (0 bytes) to audit file descriptor")
        written += n


class FileAuditSink(AuditSink):
    """Thread-safe JSON-lines file audit sink with strict mode, rotation, and retention."""

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        mode: int | str = 0o600,
        max_bytes: int | None = None,
        rotate_size_bytes: int | None = None,
        rotate_interval_seconds: float | None = None,
        rotate_seconds: float | None = None,
        max_files: int | None = None,
        retention_count: int | None = None,
        max_age_seconds: float | None = None,
        retention_age_seconds: float | None = None,
        clock: Callable[[], float] = time.time,
        fsync: bool = True,
    ) -> None:
        path_str = str(path).strip()
        if not path_str or "\x00" in path_str:
            raise ValueError("FileAuditSink path must be a non-empty path")

        self._path = Path(path_str)
        self._mode = _parse_file_mode(mode, ValueError)

        eff_max_bytes = max_bytes if max_bytes is not None else rotate_size_bytes
        eff_rotate_interval = (
            rotate_interval_seconds
            if rotate_interval_seconds is not None
            else rotate_seconds
        )
        eff_max_files = max_files if max_files is not None else retention_count
        eff_max_age = (
            max_age_seconds if max_age_seconds is not None else retention_age_seconds
        )

        self._max_bytes = _parse_optional_positive_int(
            eff_max_bytes, "max_bytes", ValueError
        )
        self._rotate_interval_seconds = _parse_optional_positive_float(
            eff_rotate_interval, "rotate_interval_seconds", ValueError
        )
        self._max_files = _parse_optional_positive_int(
            eff_max_files, "max_files", ValueError
        )
        self._max_age_seconds = _parse_optional_positive_float(
            eff_max_age, "max_age_seconds", ValueError
        )

        self._clock = clock
        self._fsync = bool(fsync)
        self._lock = threading.Lock()
        self._fd: int | None = None
        self._opened_at: float | None = None
        self._rotated_pattern = re.compile(
            rf"^{re.escape(self._path.name)}\.(\d{{8}}T\d{{12}}Z)\.(\d{{6,}})$"
        )
        self._rotation_seq: int = self._discover_max_seq()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def mode(self) -> int:
        return self._mode

    def _discover_max_seq(self) -> int:
        parent = self._path.parent
        max_seq = 0
        try:
            if parent.is_dir():
                for entry in os.scandir(parent):
                    m = self._rotated_pattern.match(entry.name)
                    if m:
                        max_seq = max(max_seq, int(m.group(2)))
        except OSError:
            pass
        return max_seq

    def _now(self) -> float:
        return float(self._clock())

    def _open_locked(self, now: float) -> int:
        if self._fd is not None:
            return self._fd

        parent = self._path.parent
        safe_path_label = _safe_diag_target(self._path)
        safe_parent_label = _safe_diag_target(parent)

        try:
            if not parent.exists() or not parent.is_dir():
                raise AuditWriteError(
                    f"Audit file parent directory does not exist: {safe_parent_label}"
                )
            if not os.access(parent, os.W_OK | os.X_OK):
                raise AuditWriteError(
                    f"Audit file parent directory is not writable: {safe_parent_label}"
                )

            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
            if hasattr(os, "O_CLOEXEC"):
                flags |= os.O_CLOEXEC

            fd = os.open(str(self._path), flags, self._mode)
            try:
                st = os.fstat(fd)
                if stat.S_ISDIR(st.st_mode):
                    raise AuditWriteError(
                        f"Audit file path points to a directory: {safe_path_label}"
                    )
                if stat.S_ISREG(st.st_mode):
                    try:
                        os.fchmod(fd, self._mode)
                    except OSError:
                        pass
                    os.chmod(self._path, self._mode)
            except Exception:
                os.close(fd)
                raise
        except AuditWriteError:
            raise
        except OSError as exc:
            reason = exc.strerror or type(exc).__name__
            raise AuditWriteError(
                f"Cannot open audit file {safe_path_label}: {reason}"
            ) from exc

        self._fd = fd
        if self._opened_at is None:
            self._opened_at = now
        return fd

    def _close_locked(self) -> None:
        if self._fd is not None:
            fd = self._fd
            self._fd = None
            try:
                os.close(fd)
            except OSError:
                pass

    def _should_rotate_locked(self, fd: int, incoming_bytes: int, now: float) -> bool:
        if self._max_bytes is None and self._rotate_interval_seconds is None:
            return False

        st = os.fstat(fd)
        current_size = st.st_size
        if current_size <= 0:
            return False

        if self._max_bytes is not None and (current_size + incoming_bytes) > self._max_bytes:
            return True

        if self._rotate_interval_seconds is not None and self._opened_at is not None:
            if (now - self._opened_at) >= self._rotate_interval_seconds:
                return True

        return False

    def _rotate_locked(self, now: float) -> Path:
        safe_path_label = _safe_diag_target(self._path)
        try:
            if self._fd is not None and self._fsync:
                os.fsync(self._fd)
            self._close_locked()

            self._rotation_seq += 1
            clamped_epoch = max(0.0, now)
            ts_tag = datetime.datetime.fromtimestamp(
                clamped_epoch, tz=datetime.timezone.utc
            ).strftime("%Y%m%dT%H%M%S%fZ")
            rotated_name = f"{self._path.name}.{ts_tag}.{self._rotation_seq:06d}"
            rotated_path = self._path.with_name(rotated_name)

            os.replace(self._path, rotated_path)
            os.chmod(rotated_path, self._mode)
            if now >= 0.0:
                try:
                    os.utime(rotated_path, (now, now))
                except OSError:
                    pass

            self._opened_at = now
            self._open_locked(now)
            return rotated_path
        except AuditWriteError:
            raise
        except OSError as exc:
            reason = exc.strerror or type(exc).__name__
            raise AuditWriteError(
                f"Failed to rotate audit file {safe_path_label}: {reason}"
            ) from exc

    def _list_rotated_entries_locked(self) -> list[tuple[str, int, float, Path]]:
        """Return ``(ts_tag, seq, mtime, path)`` sorted oldest-first."""
        parent = self._path.parent
        entries: list[tuple[str, int, float, Path]] = []
        try:
            if not parent.is_dir():
                return entries
            for dentry in os.scandir(parent):
                m = self._rotated_pattern.match(dentry.name)
                if not m:
                    continue
                try:
                    st = dentry.stat(follow_symlinks=False)
                except OSError:
                    continue
                entries.append((m.group(1), int(m.group(2)), st.st_mtime, Path(dentry.path)))
        except OSError:
            return entries

        entries.sort(key=lambda item: (item[2], item[0], item[1]))
        return entries

    def _prune_rotated_locked(self, now: float) -> None:
        if self._max_files is None and self._max_age_seconds is None:
            return

        entries = self._list_rotated_entries_locked()
        if not entries:
            return

        remaining: list[tuple[str, int, float, Path]] = []
        for entry in entries:
            _, _, mtime, rot_path = entry
            if self._max_age_seconds is not None and (now - mtime) > self._max_age_seconds:
                try:
                    os.unlink(rot_path)
                except OSError:
                    pass
            else:
                remaining.append(entry)

        if self._max_files is not None and len(remaining) > self._max_files:
            excess = len(remaining) - self._max_files
            for _, _, _, rot_path in remaining[:excess]:
                try:
                    os.unlink(rot_path)
                except OSError:
                    pass

    def rotated_files(self) -> list[Path]:
        """Return existing rotated audit files ordered from oldest to newest."""
        with self._lock:
            return [item[3] for item in self._list_rotated_entries_locked()]

    def verify_writable(self) -> None:
        with self._lock:
            now = self._now()
            fd = self._open_locked(now)
            try:
                os.write(fd, b"")
            except OSError as exc:
                self._close_locked()
                reason = exc.strerror or type(exc).__name__
                raise AuditWriteError(
                    f"Audit file {_safe_diag_target(self._path)} is not writable: {reason}"
                ) from exc

    def write_record(self, record: AuditRecord | Mapping[str, Any]) -> None:
        line = serialize_audit_record(record)
        payload = (line + "\n").encode("utf-8")

        with self._lock:
            now = self._now()
            fd = self._open_locked(now)
            try:
                if self._should_rotate_locked(fd, len(payload), now):
                    self._rotate_locked(now)
                    assert self._fd is not None
                    fd = self._fd

                _write_all_fd(fd, payload)
                if self._fsync:
                    os.fsync(fd)
            except AuditWriteError:
                raise
            except OSError as exc:
                self._close_locked()
                reason = exc.strerror or type(exc).__name__
                raise AuditWriteError(
                    f"Failed writing to audit file {_safe_diag_target(self._path)}: {reason}"
                ) from exc

            self._prune_rotated_locked(now)

    def close(self) -> None:
        with self._lock:
            self._close_locked()


class StdoutAuditSink(AuditSink):
    """Thread-safe JSON-lines writer to ``sys.stdout`` (or an injected text stream)."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream
        self._lock = threading.Lock()

    def _target_stream(self) -> TextIO:
        stream = self._stream if self._stream is not None else sys.stdout
        if stream is None:
            raise AuditWriteError("Audit stdout stream is None")
        if getattr(stream, "closed", False):
            raise AuditWriteError("Audit stdout stream is closed")
        return stream

    def verify_writable(self) -> None:
        with self._lock:
            stream = self._target_stream()
            writable_fn = getattr(stream, "writable", None)
            if callable(writable_fn):
                try:
                    is_writable = writable_fn()
                except Exception as exc:
                    raise AuditWriteError("Audit stdout stream failed writability check") from exc
                if not is_writable:
                    raise AuditWriteError("Audit stdout stream is not writable")
            if not callable(getattr(stream, "write", None)):
                raise AuditWriteError("Audit stdout stream has no write() method")

    def write_record(self, record: AuditRecord | Mapping[str, Any]) -> None:
        line = serialize_audit_record(record)
        with self._lock:
            stream = self._target_stream()
            try:
                stream.write(line + "\n")
                flush_fn = getattr(stream, "flush", None)
                if callable(flush_fn):
                    flush_fn()
            except AuditWriteError:
                raise
            except Exception as exc:
                raise AuditWriteError(
                    f"Failed writing audit record to stdout sink: {type(exc).__name__}"
                ) from exc


def _sanitize_syslog_token(value: str | None, max_len: int, default: str = "-") -> str:
    if not value:
        return default
    cleaned = "".join(
        ch if (33 <= ord(ch) <= 126) else "_"
        for ch in value.strip()
    )
    if not cleaned:
        return default
    return cleaned[:max_len]


class SyslogAuditSink(AuditSink):
    """Thread-safe RFC 5424 syslog audit sink over TCP (RFC 6587 octet-counting) or UDP."""

    def __init__(
        self,
        *,
        host: str = "127.0.0.1",
        port: int = 514,
        transport: str = "udp",
        facility: int = 16,
        severity: int = 6,
        app_name: str = "seeknal-serving",
        timeout_seconds: float = 2.0,
        hostname: str | None = None,
        procid: str | None = None,
        msgid: str = "audit",
    ) -> None:
        if not isinstance(host, str) or not host.strip():
            raise ValueError("SyslogAuditSink host must be a non-empty string")
        if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
            raise ValueError("SyslogAuditSink port must be 1..65535")
        if transport not in _ALLOWED_SYSLOG_TRANSPORTS:
            raise ValueError("SyslogAuditSink transport must be 'tcp' or 'udp'")
        if isinstance(facility, bool) or not isinstance(facility, int) or not (0 <= facility <= 23):
            raise ValueError("SyslogAuditSink facility must be 0..23")
        if isinstance(severity, bool) or not isinstance(severity, int) or not (0 <= severity <= 7):
            raise ValueError("SyslogAuditSink severity must be 0..7")
        parsed_timeout = _parse_optional_positive_float(
            timeout_seconds, "timeout_seconds", ValueError
        )
        assert parsed_timeout is not None

        self._host = host.strip()
        self._port = port
        self._transport = transport
        self._facility = facility
        self._severity = severity
        self._pri = (self._facility * 8) + self._severity
        self._app_name = _sanitize_syslog_token(app_name, 48, default="seeknal-serving")
        self._timeout_seconds = parsed_timeout

        raw_hostname = hostname
        if raw_hostname is None:
            try:
                raw_hostname = socket.gethostname()
            except Exception:
                raw_hostname = "-"
        self._hostname = _sanitize_syslog_token(raw_hostname, 255, default="-")
        self._procid = _sanitize_syslog_token(
            procid if procid is not None else str(os.getpid()),
            128,
            default="-",
        )
        self._msgid = _sanitize_syslog_token(msgid, 32, default="audit")

        self._lock = threading.Lock()
        self._sock: socket.socket | None = None
        self._udp_addr: tuple[Any, ...] | None = None

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def transport(self) -> str:
        return self._transport

    def format_rfc5424(self, record: AuditRecord | Mapping[str, Any]) -> str:
        """Format an audit record as an RFC 5424 message string."""
        validated = (
            record
            if isinstance(record, AuditRecord)
            else AuditRecord.from_dict(record, strict=False)
        )
        json_str = serialize_audit_record(validated)
        return (
            f"<{self._pri}>1 {validated.timestamp} {self._hostname} "
            f"{self._app_name} {self._procid} {self._msgid} - {json_str}"
        )

    def _close_locked(self) -> None:
        if self._sock is not None:
            sock = self._sock
            self._sock = None
            try:
                sock.close()
            except OSError:
                pass
        self._udp_addr = None

    def _connect_tcp_locked(self) -> socket.socket:
        if self._sock is not None:
            # Check non-blockingly if peer closed the TCP connection (EOF / RST) before reusing.
            if hasattr(socket, "MSG_PEEK"):
                try:
                    self._sock.setblocking(False)
                    peeked = self._sock.recv(1, socket.MSG_PEEK)
                    if peeked == b"":
                        self._close_locked()
                except BlockingIOError:
                    pass
                except OSError:
                    self._close_locked()
                finally:
                    if self._sock is not None:
                        try:
                            self._sock.settimeout(self._timeout_seconds)
                        except OSError:
                            self._close_locked()
            if self._sock is not None:
                return self._sock

        safe_host = _safe_diag_target(self._host)
        try:
            sock = socket.create_connection(
                (self._host, self._port), timeout=self._timeout_seconds
            )
            sock.settimeout(self._timeout_seconds)
            self._sock = sock
            return sock
        except OSError as exc:
            reason = exc.strerror or type(exc).__name__
            raise AuditWriteError(
                f"Audit syslog TCP destination {safe_host}:{self._port} is unreachable: {reason}"
            ) from exc

    def _ensure_udp_locked(self) -> tuple[socket.socket, tuple[Any, ...]]:
        if self._sock is not None and self._udp_addr is not None:
            return self._sock, self._udp_addr

        safe_host = _safe_diag_target(self._host)
        try:
            infos = socket.getaddrinfo(
                self._host, self._port, type=socket.SOCK_DGRAM
            )
            if not infos:
                raise AuditWriteError(
                    f"Cannot resolve audit syslog UDP destination {safe_host}:{self._port}"
                )
            af, socktype, proto, _, sockaddr = infos[0]
            sock = socket.socket(af, socktype, proto)
            sock.settimeout(self._timeout_seconds)
            self._sock = sock
            self._udp_addr = sockaddr
            return sock, sockaddr
        except AuditWriteError:
            raise
        except OSError as exc:
            reason = exc.strerror or type(exc).__name__
            raise AuditWriteError(
                f"Cannot initialize audit syslog UDP socket for {safe_host}:{self._port}: {reason}"
            ) from exc

    def verify_writable(self) -> None:
        with self._lock:
            if self._transport == "tcp":
                self._connect_tcp_locked()
            else:
                self._ensure_udp_locked()

    def write_record(self, record: AuditRecord | Mapping[str, Any]) -> None:
        rfc5424_msg = self.format_rfc5424(record)
        msg_bytes = rfc5424_msg.encode("utf-8")
        safe_host = _safe_diag_target(self._host)

        with self._lock:
            if self._transport == "tcp":
                frame = f"{len(msg_bytes)} ".encode("ascii") + msg_bytes
                had_existing_sock = self._sock is not None
                sock = self._connect_tcp_locked()
                try:
                    sock.settimeout(self._timeout_seconds)
                    sock.sendall(frame)
                except OSError as first_exc:
                    self._close_locked()
                    if not had_existing_sock:
                        reason = first_exc.strerror or type(first_exc).__name__
                        raise AuditWriteError(
                            f"Failed sending audit record over TCP to {safe_host}:{self._port}: {reason}"
                        ) from first_exc
                    # Retry once if a previously open socket went stale.
                    sock = self._connect_tcp_locked()
                    try:
                        sock.settimeout(self._timeout_seconds)
                        sock.sendall(frame)
                    except OSError as retry_exc:
                        self._close_locked()
                        reason = retry_exc.strerror or type(retry_exc).__name__
                        raise AuditWriteError(
                            f"Failed sending audit record over TCP to {safe_host}:{self._port}: {reason}"
                        ) from retry_exc
            else:
                sock, sockaddr = self._ensure_udp_locked()
                try:
                    sock.settimeout(self._timeout_seconds)
                    sock.sendto(msg_bytes, sockaddr)
                except OSError as exc:
                    self._close_locked()
                    reason = exc.strerror or type(exc).__name__
                    raise AuditWriteError(
                        f"Failed sending audit record over UDP to {safe_host}:{self._port}: {reason}"
                    ) from exc

    def close(self) -> None:
        with self._lock:
            self._close_locked()


# ---------------------------------------------------------------------------
# Factory, Manager & Runtime Emission Semantics
# ---------------------------------------------------------------------------


def _invoke_failure_callback(callback: Callable[[], Any] | None) -> None:
    if callback is not None:
        try:
            callback()
        except Exception:
            logger.debug("Audit write failure callback raised an exception", exc_info=True)


def build_audit_sink(
    cfg: AuditConfig,
    *,
    verify: bool = True,
    clock: Callable[[], float] = time.time,
    stream: TextIO | None = None,
    on_write_failure: Callable[[], Any] | None = None,
    fsync: bool = True,
) -> AuditSink:
    """Instantiate an ``AuditSink`` from ``AuditConfig`` and run startup verification.

    When ``verify=True`` (default):
    - If ``cfg.on_failure == "refuse"``, an unwritable or unreachable destination
      raises ``AuditWriteError`` immediately so the server refuses to start.
    - If ``cfg.on_failure == "log"``, startup verification failure is logged loudly
      at ``ERROR`` (and ``on_write_failure`` is invoked if supplied) while returning
      the sink instance.
    """
    if not isinstance(cfg, AuditConfig):
        raise ValueError(f"Expected AuditConfig, got {type(cfg).__name__}")

    sink: AuditSink
    if cfg.sink == "file":
        assert cfg.path is not None
        sink = FileAuditSink(
            cfg.path,
            mode=cfg.mode,
            max_bytes=cfg.max_bytes,
            rotate_interval_seconds=cfg.rotate_interval_seconds,
            max_files=cfg.max_files,
            max_age_seconds=cfg.max_age_seconds,
            clock=clock,
            fsync=fsync,
        )
    elif cfg.sink == "stdout":
        sink = StdoutAuditSink(stream=stream)
    elif cfg.sink == "syslog":
        sink = SyslogAuditSink(
            host=cfg.host,
            port=cfg.port,
            transport=cfg.transport,
            facility=cfg.facility,
            app_name=cfg.app_name,
            timeout_seconds=cfg.timeout_seconds,
        )
    else:
        raise ValueError(f"Unsupported audit sink: {cfg.sink!r}")

    if verify:
        try:
            sink.verify_writable()
        except AuditWriteError as exc:
            if cfg.on_failure == "refuse":
                sink.close()
                raise
            logger.error(
                "Audit sink startup check failed (sink=%s, on_failure=log): %s",
                cfg.sink,
                exc,
            )
            _invoke_failure_callback(on_write_failure)

    return sink


class AuditManager:
    """Coordinates an ``AuditSink`` with ``on_failure`` policy and failure metrics hook."""

    def __init__(
        self,
        sink: AuditSink,
        *,
        on_failure: str = "refuse",
        on_write_failure: Callable[[], Any] | None = None,
    ) -> None:
        if on_failure not in _ALLOWED_ON_FAILURE:
            raise ValueError(f"on_failure must be 'refuse' or 'log', got {on_failure!r}")
        self.sink = sink
        self.on_failure = on_failure
        self.on_write_failure = on_write_failure

    @classmethod
    def from_config(
        cls,
        cfg: AuditConfig,
        *,
        verify: bool = True,
        clock: Callable[[], float] = time.time,
        stream: TextIO | None = None,
        on_write_failure: Callable[[], Any] | None = None,
        fsync: bool = True,
    ) -> AuditManager:
        sink = build_audit_sink(
            cfg,
            verify=verify,
            clock=clock,
            stream=stream,
            on_write_failure=on_write_failure,
            fsync=fsync,
        )
        return cls(
            sink,
            on_failure=cfg.on_failure,
            on_write_failure=on_write_failure,
        )

    def verify_writable(self) -> None:
        try:
            self.sink.verify_writable()
        except AuditWriteError as exc:
            if self.on_failure == "refuse":
                raise
            logger.error(
                "Audit sink verification failed (on_failure=log): %s",
                exc,
            )
            _invoke_failure_callback(self.on_write_failure)

    def emit(
        self,
        record: AuditRecord | Mapping[str, Any],
        *,
        on_failure: str | None = None,
        on_write_failure: Callable[[], Any] | None = None,
    ) -> bool:
        """Emit an audit record before returning the HTTP response.

        Returns ``True`` if written, ``False`` if failed under ``on_failure == "log"``.
        Raises ``AuditWriteError`` if failed under ``on_failure == "refuse"``.
        """
        effective_mode = on_failure if on_failure is not None else self.on_failure
        if effective_mode not in _ALLOWED_ON_FAILURE:
            raise ValueError(f"on_failure must be 'refuse' or 'log', got {effective_mode!r}")

        callback = on_write_failure if on_write_failure is not None else self.on_write_failure

        validated = (
            record
            if isinstance(record, AuditRecord)
            else AuditRecord.from_dict(record, strict=False)
        )

        try:
            self.sink.write_record(validated)
            return True
        except (AuditWriteError, AuditUnavailableError, OSError) as raw_exc:
            if isinstance(raw_exc, AuditWriteError):
                exc = raw_exc
            elif isinstance(raw_exc, AuditUnavailableError):
                exc = AuditWriteError("Audit sink unavailable", audit_id=validated.audit_id)
            else:
                exc = AuditWriteError(
                    f"Audit sink write failed: {raw_exc.strerror or type(raw_exc).__name__}",
                    audit_id=validated.audit_id,
                )
            _invoke_failure_callback(callback)
            if effective_mode == "refuse":
                raise exc from raw_exc
            logger.error(
                "Audit write failed (on_failure=log, route=%s, audit_id=%s, status=%d): %s",
                validated.route,
                validated.audit_id or "-",
                validated.status,
                exc,
            )
            return False

    def write_record(self, record: AuditRecord | Mapping[str, Any]) -> bool:
        """Alias for :meth:`emit`."""
        return self.emit(record)

    def close(self) -> None:
        self.sink.close()


AuditWriter = AuditManager


def emit_audit_record(
    sink_or_manager: AuditSink | AuditManager | None,
    record: AuditRecord | Mapping[str, Any],
    *,
    on_failure: str | None = None,
    on_write_failure: Callable[[], Any] | None = None,
) -> bool:
    """Emit a structured audit record prior to returning an HTTP response.

    - When ``sink_or_manager`` is ``None`` (no ``audit:`` configured), this is a
      no-op returning ``True``.
    - When ``on_failure == "refuse"`` (default), write failures raise
      ``AuditWriteError`` (HTTP 503 ``audit_unavailable``, returning NO rows).
    - When ``on_failure == "log"``, write failures log loudly at ``ERROR``,
      invoke ``on_write_failure()`` if provided, and return ``False`` so the
      request can proceed.
    """
    if sink_or_manager is None:
        return True

    if isinstance(sink_or_manager, AuditManager):
        return sink_or_manager.emit(
            record,
            on_failure=on_failure,
            on_write_failure=on_write_failure,
        )

    if not isinstance(sink_or_manager, AuditSink):
        raise ValueError(
            f"Expected AuditSink, AuditManager, or None, got {type(sink_or_manager).__name__}"
        )

    effective_mode = (
        on_failure
        if on_failure is not None
        else getattr(sink_or_manager, "on_failure", "refuse")
    )
    if effective_mode not in _ALLOWED_ON_FAILURE:
        raise ValueError(f"on_failure must be 'refuse' or 'log', got {effective_mode!r}")

    validated = (
        record
        if isinstance(record, AuditRecord)
        else AuditRecord.from_dict(record, strict=False)
    )

    try:
        sink_or_manager.write_record(validated)
        return True
    except (AuditWriteError, AuditUnavailableError, OSError) as raw_exc:
        if isinstance(raw_exc, AuditWriteError):
            exc = raw_exc
        elif isinstance(raw_exc, AuditUnavailableError):
            exc = AuditWriteError("Audit sink unavailable", audit_id=validated.audit_id)
        else:
            exc = AuditWriteError(
                f"Audit sink write failed: {raw_exc.strerror or type(raw_exc).__name__}",
                audit_id=validated.audit_id,
            )
        _invoke_failure_callback(on_write_failure)
        if effective_mode == "refuse":
            raise exc from raw_exc
        logger.error(
            "Audit write failed (on_failure=log, route=%s, audit_id=%s, status=%d): %s",
            validated.route,
            validated.audit_id or "-",
            validated.status,
            exc,
        )
        return False


def verify_audit_config_offline(
    cfg: AuditConfig,
    *,
    error_cls: type[Exception] = ValueError,
) -> None:
    """Perform side-effect-free offline validation of audit file destination."""
    if cfg.sink != "file" or cfg.on_failure != "refuse":
        return
    assert cfg.path is not None
    target = Path(cfg.path)
    parent = target.parent
    if not parent.exists() or not parent.is_dir():
        raise error_cls(
            f"Audit file parent directory does not exist: {_safe_diag_target(parent)}"
        )
    if not os.access(parent, os.W_OK | os.X_OK):
        raise error_cls(
            f"Audit file parent directory is not writable: {_safe_diag_target(parent)}"
        )
    if target.exists():
        if target.is_dir():
            raise error_cls(
                f"Audit file path points to a directory: {_safe_diag_target(target)}"
            )
        if not os.access(target, os.W_OK):
            raise error_cls(
                f"Audit file is not writable: {_safe_diag_target(target)}"
            )


__all__ = [
    "ALLOWED_AUDIT_FIELDS",
    "ALLOWED_AUDIT_FIELD_SET",
    "ALLOWED_AUDIT_ROUTES",
    "FORBIDDEN_AUDIT_KEYS",
    "AuditConfig",
    "AuditManager",
    "AuditRecord",
    "AuditSink",
    "AuditWriteError",
    "AuditWriter",
    "FileAuditSink",
    "StdoutAuditSink",
    "SyslogAuditSink",
    "build_audit_record",
    "build_audit_sink",
    "compute_sql_sha256",
    "emit_audit_record",
    "format_utc_timestamp",
    "serialize_audit_record",
    "verify_audit_config_offline",
]
