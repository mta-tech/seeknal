"""Typed error catalogue and response payload helpers for Seeknal Serving."""

from __future__ import annotations

import os
import re
import threading
import urllib.parse
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
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


_SECRET_KEY_NAMES: tuple[str, ...] = (
    "aws_secret_access_key",
    "aws_access_key_id",
    "connection_string",
    "client_secret",
    "session_token",
    "account_key",
    "sslpassword",
    "private_key",
    "credentials",
    "accountkey",
    "credential",
    "secret_key",
    "access_key",
    "password",
    "api_key",
    "apikey",
    "passwd",
    "secret",
    "key_id",
    "token",
    "pwd",
)
_SECRET_KEYS_ALT = "|".join(_SECRET_KEY_NAMES)
_QUOTED_OR_BARE_VAL_PAT = (
    r"(?:'(?:''|\\[\s\S]|[^'\\])*'|\"(?:\"\"|\\[\s\S]|[^\"\\])*\"|[^\s,;'\"&+)]+)"
)

_JWT_RE = re.compile(r"\beyJ[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+){2,4}")
_BEARER_RE = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]+=*")
_URI_USERINFO_RE = re.compile(r"://[^@\s/]{0,256}(?:/[^@\s/]{0,256}){0,8}@")
_URI_USERINFO_EXTRACT_RE = re.compile(
    r"://([^@\s/]{0,256}(?:/[^@\s/]{0,256}){0,8})@"
)
_DSN_KV_EQ_RE = re.compile(
    rf"(?is)(?<![A-Za-z0-9])({_SECRET_KEYS_ALT})\s*=\s*{_QUOTED_OR_BARE_VAL_PAT}"
)
_SECRET_COLON_KV_RE = re.compile(
    rf"(?is)(?<![A-Za-z0-9])({_SECRET_KEYS_ALT})(['\"]?)\s*:\s*{_QUOTED_OR_BARE_VAL_PAT}"
)
_DSN_KV_EXTRACT_RE = re.compile(
    rf"(?is)(?<![A-Za-z0-9])(?:{_SECRET_KEYS_ALT})\s*=\s*(?:'((?:''|\\[\s\S]|[^'\\])*)'|\"((?:\"\"|\\[\s\S]|[^\"\\])*)\"|([^\s,;'\"&+)]+))"
)
_DUCKDB_SECRET_RE = re.compile(
    r"(?is)(?<![A-Za-z0-9])(SECRET|KEY_ID|SESSION_TOKEN|TOKEN|ACCOUNT_KEY|CLIENT_SECRET|CONNECTION_STRING)\s+'(?:''|\\[\s\S]|[^'\\])*'"
)
_SQL_SINGLE_QUOTED_RE = re.compile(r"'((?:''|[^'])*)'")
_PURELY_NUMERIC_RE = re.compile(
    r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"
)
_SENSITIVE_ENV_NAME_RE = re.compile(
    r"(?i)(SECRET|PASSWORD|PASSWD|TOKEN|DSN|CREDENTIAL|CANARY|SENTINEL|PRIVATE|KEY)"
)
_CANARY_SENTINEL_RE = re.compile(
    r"(?i)\b[A-Za-z0-9_.-]*(?:canary|sentinel)[A-Za-z0-9_.-]*\b"
)
_LONE_SURROGATE_RE = re.compile(r"[\ud800-\udfff]")
_MAX_RAW_SCRUB_INPUT_CHARS = 8192
_MAX_SCRUBBED_DETAIL_CHARS = 2048

_CONFIG_SECRETS_LOCK = threading.Lock()
_REGISTERED_CONFIG_SECRETS: set[str] = set()


def register_config_secrets(*values: str | None) -> None:
    """Register resolved configuration, secret-file, or env secret values for log scrubbing."""
    to_add = [
        val
        for val in values
        if isinstance(val, str) and len(val) >= 4 and val.strip()
    ]
    if not to_add:
        return
    with _CONFIG_SECRETS_LOCK:
        _REGISTERED_CONFIG_SECRETS.update(to_add)


def clear_registered_config_secrets() -> None:
    """Clear module-level registered config secrets (primarily for test isolation)."""
    with _CONFIG_SECRETS_LOCK:
        _REGISTERED_CONFIG_SECRETS.clear()


def _sanitize_log_value(value: Any, *, default: str = "-") -> str:
    """Sanitize a value for single-line logs, escaping all control and non-printable characters."""
    if value is None:
        return default
    raw = _LONE_SURROGATE_RE.sub("\ufffd", str(value))
    if not raw:
        return default
    out_chars: list[str] = []
    for ch in raw:
        code = ord(ch)
        if ch == "\n":
            out_chars.append("\\n")
        elif ch == "\r":
            out_chars.append("\\r")
        elif ch == "\t":
            out_chars.append("\\t")
        elif code < 0x20 or code == 0x7F or (0x80 <= code <= 0x9F):
            out_chars.append(f"\\x{code:02x}")
        elif not ch.isprintable():
            if code <= 0xFF:
                out_chars.append(f"\\x{code:02x}")
            elif code <= 0xFFFF:
                out_chars.append(f"\\u{code:04x}")
            else:
                out_chars.append(f"\\U{code:08x}")
        else:
            out_chars.append(ch)
    return "".join(out_chars)


def _is_purely_numeric_value(val: Any) -> bool:
    """Return True if *val* is a number (excluding bool) or a purely numeric string."""
    if isinstance(val, bool):
        return False
    if isinstance(val, (int, float, Decimal)):
        return True
    if isinstance(val, str):
        s = val.strip()
        return bool(s and _PURELY_NUMERIC_RE.fullmatch(s))
    return False


def _collect_param_redactions(params: Any) -> list[str]:
    """Collect bound-parameter redaction tokens: exact quoted ``'<p>'`` or non-numeric len >= 3."""
    if isinstance(params, (list, tuple)):
        param_items: list[Any] = list(params)
    elif isinstance(params, Mapping):
        param_items = list(params.values())
    else:
        return []

    tokens: set[str] = set()
    for p in param_items:
        if p is None or isinstance(p, bool):
            continue
        p_str = str(p)
        if not p_str:
            continue
        tokens.add(f"'{p_str}'")
        if isinstance(p, str):
            escaped_p = p.replace("'", "''")
            tokens.add(f"'{escaped_p}'")
            if len(p) >= 3 and not _is_purely_numeric_value(p):
                tokens.add(p)
                if escaped_p != p:
                    tokens.add(escaped_p)
        elif len(p_str) >= 3 and not _is_purely_numeric_value(p):
            tokens.add(p_str)

    return sorted(tokens, key=len, reverse=True)


def _add_quote_escaped_forms(secret: str, candidates: set[str]) -> None:
    if len(secret) < 4 or not secret.strip():
        return
    candidates.add(secret)
    candidates.add(secret.replace("'", "''"))
    candidates.add(secret.replace("'", "\\'"))
    candidates.add(secret.replace('"', '""'))
    candidates.add(secret.replace('"', '\\"'))
    if "\\" in secret:
        candidates.add(secret.replace("\\", "\\\\"))


def _expand_secret_value(val: str, candidates: set[str]) -> None:
    _add_quote_escaped_forms(val, candidates)
    val = val[:_MAX_RAW_SCRUB_INPUT_CHARS]
    if "://" in val:
        after_scheme = val.split("://", 1)[1]
        if "@" in after_scheme:
            greedy_userinfo = after_scheme.rsplit("@", 1)[0]
            _add_quote_escaped_forms(greedy_userinfo, candidates)
            if ":" in greedy_userinfo:
                _, pwd = greedy_userinfo.split(":", 1)
                for candidate_pwd in (pwd, urllib.parse.unquote(pwd)):
                    _add_quote_escaped_forms(candidate_pwd, candidates)
    for uri_m in _URI_USERINFO_EXTRACT_RE.finditer(val):
        userinfo = uri_m.group(1)
        if ":" in userinfo:
            _, pwd = userinfo.split(":", 1)
            for candidate_pwd in (pwd, urllib.parse.unquote(pwd)):
                _add_quote_escaped_forms(candidate_pwd, candidates)
        else:
            _add_quote_escaped_forms(userinfo, candidates)
    for kv_m in _DSN_KV_EXTRACT_RE.finditer(val):
        kv_val = kv_m.group(1) or kv_m.group(2) or kv_m.group(3) or ""
        if kv_val:
            for unesc in (
                kv_val,
                kv_val.replace("''", "'"),
                kv_val.replace("\\'", "'"),
                kv_val.replace('""', '"'),
                kv_val.replace('\\"', '"'),
            ):
                _add_quote_escaped_forms(unesc, candidates)


def _extract_env_secrets(
    env: Mapping[str, str] | None,
    *,
    extra_secrets: Iterable[str] | None = None,
) -> list[str]:
    source = os.environ if env is None else env
    candidates: set[str] = set()

    with _CONFIG_SECRETS_LOCK:
        registered_snapshot = tuple(_REGISTERED_CONFIG_SECRETS)
    for reg_val in registered_snapshot:
        _expand_secret_value(reg_val, candidates)

    if extra_secrets is not None:
        for ext_val in extra_secrets:
            if isinstance(ext_val, str):
                _expand_secret_value(ext_val, candidates)

    for name, val in source.items():
        if not isinstance(name, str) or not isinstance(val, str):
            continue
        if not _SENSITIVE_ENV_NAME_RE.search(name):
            continue
        _expand_secret_value(val, candidates)

    return sorted(candidates, key=len, reverse=True)


def _replace_known_secrets(detail: str, secrets: Sequence[str]) -> str:
    for env_secret in secrets:
        if env_secret not in detail:
            continue
        if env_secret.isalpha() and len(env_secret) <= 6:
            detail = re.sub(
                rf"(?<![A-Za-z0-9]){re.escape(env_secret)}(?![A-Za-z0-9])",
                "[REDACTED]",
                detail,
            )
        else:
            detail = detail.replace(env_secret, "[REDACTED]")
    return detail


def scrub_error_detail(
    exc_or_msg: BaseException | str,
    *,
    token: str | None = None,
    sql: str | None = None,
    params: Any = None,
    env: Mapping[str, str] | None = None,
    extra_secrets: Iterable[str] | None = None,
) -> str:
    """Format *exc_or_msg* for single-line server logs with secrets, DSNs, JWTs, SQL literals, and params scrubbed."""
    if isinstance(exc_or_msg, BaseException):
        detail = f"{type(exc_or_msg).__name__}: {exc_or_msg}"
    else:
        detail = str(exc_or_msg)

    if isinstance(token, str) and token:
        detail = detail.replace(token, "[REDACTED]")
        stripped_tok = token.strip()
        if stripped_tok and stripped_tok != token:
            detail = detail.replace(stripped_tok, "[REDACTED]")

    detail = detail[:_MAX_RAW_SCRUB_INPUT_CHARS]

    env_secrets = _extract_env_secrets(env, extra_secrets=extra_secrets)
    detail = _replace_known_secrets(detail, env_secrets)

    detail = _JWT_RE.sub("[REDACTED_JWT]", detail)
    detail = _BEARER_RE.sub("Bearer [REDACTED]", detail)
    detail = _URI_USERINFO_RE.sub("://[REDACTED]@", detail)
    detail = _DUCKDB_SECRET_RE.sub(r"\1 '[REDACTED]'", detail)
    detail = _DSN_KV_EQ_RE.sub(r"\1=[REDACTED]", detail)
    detail = _SECRET_COLON_KV_RE.sub(r"\1\2: [REDACTED]", detail)

    detail = _replace_known_secrets(detail, env_secrets)

    if isinstance(sql, str) and sql:
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
        if sql in detail:
            detail = detail.replace(sql, "[REDACTED]")
        stripped_sql = sql.strip()
        if stripped_sql and stripped_sql in detail:
            detail = detail.replace(stripped_sql, "[REDACTED]")
        for lit in sorted(set(sql_literals), key=len, reverse=True):
            if lit in detail:
                detail = detail.replace(lit, "[REDACTED]")

    for param_tok in _collect_param_redactions(params):
        if param_tok in detail:
            detail = detail.replace(param_tok, "[REDACTED]")

    detail = _CANARY_SENTINEL_RE.sub("[REDACTED]", detail)
    detail = _sanitize_log_value(detail, default="")
    return detail[:_MAX_SCRUBBED_DETAIL_CHARS]
