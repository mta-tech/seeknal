"""Lakebase-specific configuration, storage class rules, role guard, snapshot freshness, and cold-start wake."""

from __future__ import annotations

import concurrent.futures
import inspect
import logging
import math
import os
import re
import threading
import time
import urllib.parse
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from seeknal.serving.adapters import (
    EngineConfigError,
    EngineError,
    PostgresSessionHardeningError,
)
from seeknal.serving.errors import register_config_secrets

logger = logging.getLogger(__name__)

__all__ = [
    "ColdStartConnector",
    "DEFAULT_DENY_ROLES",
    "EngineStartingError",
    "LakebaseColdStartConfig",
    "LakebaseEndpointConfig",
    "LakebaseFreshnessConfig",
    "check_role_guard",
    "effective_storage_class",
    "parse_lakebase_block",
    "role_guard_sql",
    "snapshot_freshness",
    "validate_deny_roles",
    "validate_lakebase_tls",
]

_PROJECT_ID_RE = re.compile(r"\Aprj_[a-z0-9]{20}\Z")
_BRANCH_ID_RE = re.compile(r"\Abr_[a-z0-9]{20}\Z")
_ENDPOINT_ID_RE = re.compile(r"\Aep_[a-z0-9]{20}\Z")
_ENV_VAR_NAME_RE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_QUALIFIED_TABLE_RE = re.compile(
    r"\A[a-z_][a-z0-9_]{0,62}(?:\.[a-z_][a-z0-9_]{0,62})?\Z"
)
_SAFE_LABEL_RE = re.compile(r"\A[A-Za-z0-9_.-]{1,80}\Z")
_DENY_ROLE_IDENT_RE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_-]{0,62}\Z")

DEFAULT_DENY_ROLES: tuple[str, ...] = (
    "neon_superuser",
    "rds_superuser",
    "cloudsqlsuperuser",
    "azure_pg_admin",
)

_DEFAULT_SYNC_STATE_TABLE = "public._lakebase_sync_state"
_DEFAULT_RETRY_BACKOFF_MS: tuple[int, ...] = (500, 1000, 2000, 4000)

_LITERAL_SECRET_KEYS = frozenset(
    {
        "api_token",
        "token",
        "auth_token",
        "bearer_token",
        "secret",
        "password",
        "passwd",
        "dsn",
        "api_key",
        "apikey",
        "client_secret",
    }
)

_ALLOWED_LAKEBASE_KEYS = frozenset(
    {
        "project_id",
        "branch_id",
        "endpoint_id",
        "endpoint_type",
        "cu",
        "cold_start",
        "freshness",
    }
)

_ALLOWED_COLD_START_KEYS = frozenset(
    {
        "connect_timeout_seconds",
        "wake",
        "api_url",
        "api_token_env",
        "max_query_wait_seconds",
        "max_wake_seconds",
        "retry_backoff_ms",
    }
)

_ALLOWED_FRESHNESS_KEYS = frozenset(
    {
        "sync_state_table",
        "freshness_sql",
    }
)

_TERMINAL_ENDPOINT_ERROR_STATES = frozenset(
    {
        "failed",
        "error",
        "broken",
        "deleted",
    }
)

_FIXED_LAKEBASE_STORAGE_CLASS_MSG = (
    "Lakebase storage_class is fixed to 'object_storage' and cannot be set"
)

_ROLE_GUARD_REFUSED_MSG = (
    "Role guard refused connection: role has superuser, BYPASSRLS, or neon_superuser privileges"
)


def _safe_label(value: Any, *, fallback: str = "<invalid>") -> str:
    if isinstance(value, str) and _SAFE_LABEL_RE.match(value):
        return value
    return fallback


def _safe_unknown_keys(keys: Sequence[Any] | set[Any]) -> list[str]:
    cleaned: list[str] = []
    for k in sorted((str(x) for x in keys)):
        cleaned.append(_safe_label(k, fallback="<redacted_key>"))
    return cleaned


def _validate_positive_finite_float(
    value: Any,
    *,
    field_name: str,
    error_cls: type[Exception] = ValueError,
) -> float:
    if isinstance(value, (bool, str)) or not isinstance(value, (int, float)):
        raise error_cls(f"'{field_name}' must be a positive finite number")
    fval = float(value)
    if not math.isfinite(fval) or fval <= 0.0:
        raise error_cls(f"'{field_name}' must be a positive finite number")
    return fval


def _validate_api_url(
    raw_url: Any,
    *,
    error_cls: type[Exception] = ValueError,
) -> str:
    if not isinstance(raw_url, str) or not raw_url.strip():
        raise error_cls("'cold_start.api_url' must be a non-empty https:// URL")
    clean = raw_url.strip()
    if any(ch.isspace() or ord(ch) < 0x20 for ch in clean):
        raise error_cls("'cold_start.api_url' must not contain whitespace or control characters")

    parsed = urllib.parse.urlsplit(clean)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise error_cls("'cold_start.api_url' must start with https:// and include a host")
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
        raise error_cls("'cold_start.api_url' must not contain credentials or userinfo")
    if parsed.scheme == "http" and parsed.hostname.lower() not in (
        "localhost",
        "127.0.0.1",
        "::1",
    ):
        raise error_cls(
            "'cold_start.api_url' must use https:// (http:// is allowed only for loopback hosts)"
        )
    if parsed.query or parsed.fragment:
        raise error_cls("'cold_start.api_url' must not contain query parameters or fragments")

    normalized_path = parsed.path.rstrip("/")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, normalized_path, "", ""))


def _validate_retry_backoff_ms(
    raw_backoff: Any,
    *,
    error_cls: type[Exception] = ValueError,
) -> tuple[int, ...]:
    if isinstance(raw_backoff, (str, bytes, bytearray, Mapping)) or not isinstance(
        raw_backoff, Sequence
    ):
        raise error_cls("'cold_start.retry_backoff_ms' must be a non-empty sequence of positive integers")
    if len(raw_backoff) == 0:
        raise error_cls("'cold_start.retry_backoff_ms' must not be empty")
    items: list[int] = []
    for item in raw_backoff:
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise error_cls(
                "'cold_start.retry_backoff_ms' entries must be positive integers"
            )
        items.append(int(item))
    return tuple(items)


class EngineStartingError(EngineError):
    """Raised when a Lakebase endpoint wake is in flight and the request wait budget expired."""

    code: str = "engine_starting"
    status_code: int = 503
    retry_after: int = 3

    def __init__(
        self,
        message: str = "The target database endpoint is starting up.",
        *,
        retry_after: int = 3,
        audit_id: str | None = None,
    ) -> None:
        if isinstance(retry_after, bool) or not isinstance(retry_after, (int, float)):
            clean_retry = 3
        elif not math.isfinite(float(retry_after)):
            clean_retry = 3
        else:
            clean_retry = max(1, int(math.ceil(float(retry_after))))
        super().__init__(message)
        self.code = "engine_starting"
        self.status_code = 503
        self.status = 503
        self.retry_after = clean_retry
        self.audit_id = audit_id
        self.headers = {"Retry-After": str(clean_retry)}


@dataclass(frozen=True)
class LakebaseColdStartConfig:
    """Cold-start and control-plane wake configuration for a Lakebase endpoint."""

    connect_timeout_seconds: float = 5.0
    wake: str = "none"
    api_url: str | None = None
    api_token_env: str | None = None
    max_query_wait_seconds: float = 10.0
    max_wake_seconds: float = 60.0
    retry_backoff_ms: tuple[int, ...] = _DEFAULT_RETRY_BACKOFF_MS

    def __post_init__(self) -> None:
        ct = _validate_positive_finite_float(
            self.connect_timeout_seconds,
            field_name="cold_start.connect_timeout_seconds",
        )
        object.__setattr__(self, "connect_timeout_seconds", ct)

        if not isinstance(self.wake, str) or self.wake not in ("api", "none"):
            raise ValueError("'cold_start.wake' must be 'api' or 'none'")

        if self.api_url is not None:
            norm_url = _validate_api_url(self.api_url)
            object.__setattr__(self, "api_url", norm_url)
        elif self.wake == "api":
            raise ValueError("'cold_start.api_url' is required when cold_start.wake == 'api'")

        if self.api_token_env is not None:
            if not isinstance(self.api_token_env, str) or not _ENV_VAR_NAME_RE.match(
                self.api_token_env
            ):
                raise ValueError(
                    "'cold_start.api_token_env' must be a valid environment variable name "
                    "matching ^[A-Za-z_][A-Za-z0-9_]{0,127}$"
                )
        elif self.wake == "api":
            raise ValueError(
                "'cold_start.api_token_env' is required when cold_start.wake == 'api'"
            )

        mqw = _validate_positive_finite_float(
            self.max_query_wait_seconds,
            field_name="cold_start.max_query_wait_seconds",
        )
        object.__setattr__(self, "max_query_wait_seconds", mqw)

        mws = _validate_positive_finite_float(
            self.max_wake_seconds,
            field_name="cold_start.max_wake_seconds",
        )
        object.__setattr__(self, "max_wake_seconds", mws)

        backoff = _validate_retry_backoff_ms(self.retry_backoff_ms)
        object.__setattr__(self, "retry_backoff_ms", backoff)


@dataclass(frozen=True)
class LakebaseFreshnessConfig:
    """Data-level freshness marker configuration for a Lakebase endpoint."""

    sync_state_table: str | None = _DEFAULT_SYNC_STATE_TABLE
    freshness_sql: str | None = None

    def __post_init__(self) -> None:
        if self.sync_state_table is not None:
            if not isinstance(self.sync_state_table, str) or not _QUALIFIED_TABLE_RE.match(
                self.sync_state_table
            ):
                raise ValueError(
                    "'freshness.sync_state_table' must match "
                    "^[a-z_][a-z0-9_]{0,62}(\\.[a-z_][a-z0-9_]{0,62})?$"
                )
        if self.freshness_sql is not None:
            if not isinstance(self.freshness_sql, str) or not self.freshness_sql.strip():
                raise ValueError(
                    "'freshness.freshness_sql' must be None or a non-empty SQL string"
                )
            object.__setattr__(self, "freshness_sql", self.freshness_sql.strip())


@dataclass(frozen=True)
class LakebaseEndpointConfig:
    """Validated Atlas Lakebase endpoint metadata, cold-start, and freshness settings."""

    project_id: str
    branch_id: str
    endpoint_id: str
    endpoint_type: str = "read_write"
    cu: float | None = None
    cold_start: LakebaseColdStartConfig = field(default_factory=LakebaseColdStartConfig)
    freshness: LakebaseFreshnessConfig = field(default_factory=LakebaseFreshnessConfig)
    storage_class: str = "object_storage"

    def __post_init__(self) -> None:
        if not isinstance(self.project_id, str) or not _PROJECT_ID_RE.match(self.project_id):
            raise ValueError(
                "'lakebase.project_id' must match ^prj_[a-z0-9]{20}$"
            )
        if not isinstance(self.branch_id, str) or not _BRANCH_ID_RE.match(self.branch_id):
            raise ValueError(
                "'lakebase.branch_id' must match ^br_[a-z0-9]{20}$"
            )
        if not isinstance(self.endpoint_id, str) or not _ENDPOINT_ID_RE.match(self.endpoint_id):
            raise ValueError(
                "'lakebase.endpoint_id' must match ^ep_[a-z0-9]{20}$"
            )
        if not isinstance(self.endpoint_type, str) or self.endpoint_type not in (
            "read_only",
            "read_write",
        ):
            raise ValueError(
                "'lakebase.endpoint_type' must be 'read_only' or 'read_write'"
            )
        if self.cu is not None:
            cu_f = _validate_positive_finite_float(self.cu, field_name="lakebase.cu")
            object.__setattr__(self, "cu", cu_f)
        if not isinstance(self.cold_start, LakebaseColdStartConfig):
            raise ValueError("'lakebase.cold_start' must be a LakebaseColdStartConfig")
        if not isinstance(self.freshness, LakebaseFreshnessConfig):
            raise ValueError("'lakebase.freshness' must be a LakebaseFreshnessConfig")
        if self.storage_class != "object_storage":
            raise ValueError(_FIXED_LAKEBASE_STORAGE_CLASS_MSG)

    @property
    def max_connections(self) -> int | None:
        """Compute max_connections = max(25, round(cu * 100)) when cu is configured."""
        if self.cu is None:
            return None
        return max(25, int(round(self.cu * 100)))


def parse_lakebase_block(
    raw: Mapping[str, Any],
    *,
    eng_name: str = "lakebase",
    error_cls: type[Exception] = ValueError,
) -> LakebaseEndpointConfig:
    """Parse and validate a ``lakebase:`` endpoint configuration mapping."""
    safe_eng = _safe_label(eng_name)
    if not isinstance(raw, Mapping):
        raise error_cls(f"Lakebase endpoint block for '{safe_eng}' must be a mapping")

    if "storage_class" in raw:
        raise error_cls(_FIXED_LAKEBASE_STORAGE_CLASS_MSG)

    for key in raw.keys():
        if isinstance(key, str) and key.lower() in _LITERAL_SECRET_KEYS:
            raise error_cls(
                f"Literal secret key '{_safe_label(key)}' is forbidden in lakebase block for '{safe_eng}'"
            )

    unknown = set(raw.keys()) - _ALLOWED_LAKEBASE_KEYS
    if unknown:
        raise error_cls(
            f"Unknown key(s) in lakebase block for '{safe_eng}': {_safe_unknown_keys(unknown)}"
        )

    project_id = raw.get("project_id")
    if not isinstance(project_id, str) or not _PROJECT_ID_RE.match(project_id):
        raise error_cls(
            f"Lakebase connection '{safe_eng}' 'project_id' must match ^prj_[a-z0-9]{{20}}$"
        )

    branch_id = raw.get("branch_id")
    if not isinstance(branch_id, str) or not _BRANCH_ID_RE.match(branch_id):
        raise error_cls(
            f"Lakebase connection '{safe_eng}' 'branch_id' must match ^br_[a-z0-9]{{20}}$"
        )

    endpoint_id = raw.get("endpoint_id")
    if not isinstance(endpoint_id, str) or not _ENDPOINT_ID_RE.match(endpoint_id):
        raise error_cls(
            f"Lakebase connection '{safe_eng}' 'endpoint_id' must match ^ep_[a-z0-9]{{20}}$"
        )

    endpoint_type = raw["endpoint_type"] if "endpoint_type" in raw else "read_write"
    if not isinstance(endpoint_type, str) or endpoint_type not in (
        "read_only",
        "read_write",
    ):
        raise error_cls(
            f"Lakebase connection '{safe_eng}' 'endpoint_type' must be 'read_only' or 'read_write'"
        )

    cu_raw = raw.get("cu")
    cu_val: float | None = None
    if cu_raw is not None:
        cu_val = _validate_positive_finite_float(
            cu_raw, field_name="lakebase.cu", error_cls=error_cls
        )

    # Parse cold_start sub-block
    cold_start_raw = raw.get("cold_start")
    if cold_start_raw is None:
        cold_start_cfg = LakebaseColdStartConfig()
    else:
        if not isinstance(cold_start_raw, Mapping):
            raise error_cls(
                f"Lakebase connection '{safe_eng}' 'cold_start' must be a mapping"
            )
        for key in cold_start_raw.keys():
            if isinstance(key, str) and key.lower() in _LITERAL_SECRET_KEYS:
                raise error_cls(
                    f"Literal secret key '{_safe_label(key)}' is forbidden in "
                    f"lakebase.cold_start for '{safe_eng}'; use 'api_token_env'"
                )
        unknown_cs = set(cold_start_raw.keys()) - _ALLOWED_COLD_START_KEYS
        if unknown_cs:
            raise error_cls(
                f"Unknown key(s) in lakebase.cold_start for '{safe_eng}': "
                f"{_safe_unknown_keys(unknown_cs)}"
            )

        ct_sec = _validate_positive_finite_float(
            cold_start_raw.get("connect_timeout_seconds", 5.0),
            field_name="cold_start.connect_timeout_seconds",
            error_cls=error_cls,
        )
        wake_val = cold_start_raw["wake"] if "wake" in cold_start_raw else "api"
        if not isinstance(wake_val, str) or wake_val not in ("api", "none"):
            raise error_cls(
                f"Lakebase connection '{safe_eng}' 'cold_start.wake' must be 'api' or 'none'"
            )

        raw_api_url = cold_start_raw.get("api_url")
        if raw_api_url is not None:
            api_url = _validate_api_url(raw_api_url, error_cls=error_cls)
        elif wake_val == "api":
            raise error_cls(
                f"Lakebase connection '{safe_eng}' requires 'cold_start.api_url' when wake == 'api'"
            )
        else:
            api_url = None

        raw_token_env = cold_start_raw.get("api_token_env")
        if raw_token_env is not None:
            if not isinstance(raw_token_env, str) or not _ENV_VAR_NAME_RE.match(
                raw_token_env
            ):
                raise error_cls(
                    f"Lakebase connection '{safe_eng}' 'cold_start.api_token_env' must match "
                    "^[A-Za-z_][A-Za-z0-9_]{0,127}$"
                )
            api_token_env = raw_token_env
        elif wake_val == "api":
            raise error_cls(
                f"Lakebase connection '{safe_eng}' requires 'cold_start.api_token_env' when wake == 'api'"
            )
        else:
            api_token_env = None

        max_qw = _validate_positive_finite_float(
            cold_start_raw.get("max_query_wait_seconds", 10.0),
            field_name="cold_start.max_query_wait_seconds",
            error_cls=error_cls,
        )
        max_wake = _validate_positive_finite_float(
            cold_start_raw.get("max_wake_seconds", 60.0),
            field_name="cold_start.max_wake_seconds",
            error_cls=error_cls,
        )
        backoff_ms = _validate_retry_backoff_ms(
            cold_start_raw.get("retry_backoff_ms", _DEFAULT_RETRY_BACKOFF_MS),
            error_cls=error_cls,
        )

        cold_start_cfg = LakebaseColdStartConfig(
            connect_timeout_seconds=ct_sec,
            wake=wake_val,
            api_url=api_url,
            api_token_env=api_token_env,
            max_query_wait_seconds=max_qw,
            max_wake_seconds=max_wake,
            retry_backoff_ms=backoff_ms,
        )

    # Parse freshness sub-block
    freshness_raw = raw.get("freshness")
    if freshness_raw is None:
        freshness_cfg = LakebaseFreshnessConfig()
    else:
        if not isinstance(freshness_raw, Mapping):
            raise error_cls(
                f"Lakebase connection '{safe_eng}' 'freshness' must be a mapping"
            )
        unknown_fr = set(freshness_raw.keys()) - _ALLOWED_FRESHNESS_KEYS
        if unknown_fr:
            raise error_cls(
                f"Unknown key(s) in lakebase.freshness for '{safe_eng}': "
                f"{_safe_unknown_keys(unknown_fr)}"
            )

        sync_table = (
            freshness_raw["sync_state_table"]
            if "sync_state_table" in freshness_raw
            else _DEFAULT_SYNC_STATE_TABLE
        )
        if sync_table is not None:
            if not isinstance(sync_table, str) or not _QUALIFIED_TABLE_RE.match(sync_table):
                raise error_cls(
                    f"Lakebase connection '{safe_eng}' 'freshness.sync_state_table' must match "
                    "^[a-z_][a-z0-9_]{0,62}(\\.[a-z_][a-z0-9_]{0,62})?$"
                )

        raw_fsql = freshness_raw.get("freshness_sql")
        if raw_fsql is not None:
            if not isinstance(raw_fsql, str) or not raw_fsql.strip():
                raise error_cls(
                    f"Lakebase connection '{safe_eng}' 'freshness.freshness_sql' must be "
                    "None or a non-empty SQL string"
                )
            freshness_sql: str | None = raw_fsql.strip()
        else:
            freshness_sql = None

        freshness_cfg = LakebaseFreshnessConfig(
            sync_state_table=sync_table,
            freshness_sql=freshness_sql,
        )

    return LakebaseEndpointConfig(
        project_id=project_id,
        branch_id=branch_id,
        endpoint_id=endpoint_id,
        endpoint_type=endpoint_type,
        cu=cu_val,
        cold_start=cold_start_cfg,
        freshness=freshness_cfg,
    )


def validate_lakebase_tls(
    tls_cfg: Any,
    *,
    eng_name: str = "lakebase",
    error_cls: type[Exception] = ValueError,
) -> None:
    """Verify that Lakebase TLS uses ``sslmode='verify-full'`` with a non-empty CA path."""
    safe_eng = _safe_label(eng_name)
    if tls_cfg is None:
        raise error_cls(
            f"Lakebase connection '{safe_eng}' requires a 'tls' block with "
            "sslmode='verify-full' and 'sslrootcert_file'"
        )

    if isinstance(tls_cfg, Mapping):
        for key in tls_cfg.keys():
            if isinstance(key, str) and key.lower() in _LITERAL_SECRET_KEYS:
                raise error_cls(
                    f"Literal secret key '{_safe_label(key)}' is forbidden in tls config for '{safe_eng}'"
                )
        sslmode = tls_cfg.get("sslmode")
        sslrootcert_file = tls_cfg.get("sslrootcert_file")
        allow_insecure_tls = tls_cfg.get("allow_insecure_tls", False)
    elif hasattr(tls_cfg, "sslmode") or hasattr(tls_cfg, "sslrootcert_file"):
        sslmode = getattr(tls_cfg, "sslmode", None)
        sslrootcert_file = getattr(tls_cfg, "sslrootcert_file", None)
        allow_insecure_tls = getattr(tls_cfg, "allow_insecure_tls", False)
    else:
        raise error_cls(
            f"Lakebase connection '{safe_eng}' 'tls' configuration must be a mapping or TLS config object"
        )

    if allow_insecure_tls is True:
        raise error_cls(
            f"Lakebase connection '{safe_eng}' forbids tls.allow_insecure_tls; "
            "sslmode='verify-full' is mandatory"
        )

    if not isinstance(sslmode, str) or sslmode != "verify-full":
        raise error_cls(
            f"Lakebase connection '{safe_eng}' requires tls.sslmode == 'verify-full'"
        )

    if not isinstance(sslrootcert_file, str) or not sslrootcert_file.strip():
        raise error_cls(
            f"Lakebase connection '{safe_eng}' requires a non-empty tls.sslrootcert_file"
        )


def _validate_declared_storage_class(declared: Any, *, kind: str) -> str:
    if not isinstance(declared, str) or declared not in ("local", "object_storage"):
        raise ValueError(
            f"Invalid storage_class '{_safe_label(declared)}' for '{_safe_label(kind)}'; "
            "expected 'local' or 'object_storage'"
        )
    return declared


def effective_storage_class(conn_cfg: Any) -> str:
    """Return effective storage class ('local' | 'object_storage' | 'unknown') for a connection."""
    if isinstance(conn_cfg, LakebaseEndpointConfig):
        return "object_storage"

    if isinstance(conn_cfg, Mapping):
        kind = conn_cfg.get("kind")
        has_storage_key = "storage_class" in conn_cfg and conn_cfg["storage_class"] is not None
        declared = conn_cfg.get("storage_class")
        lb_block = conn_cfg.get("lakebase")
        profile = conn_cfg.get("profile")
    else:
        kind = getattr(conn_cfg, "kind", None)
        lb_block = getattr(conn_cfg, "lakebase", None)
        profile = getattr(conn_cfg, "profile", None)
        cls_attr = getattr(type(conn_cfg), "storage_class", None)
        if isinstance(cls_attr, property):
            declared = None
            has_storage_key = False
        else:
            declared = getattr(conn_cfg, "storage_class", None)
            has_storage_key = declared is not None

    if not isinstance(kind, str) or not kind.strip():
        raise ValueError("Connection configuration missing valid 'kind'")
    kind_norm = kind.strip()

    if kind_norm == "postgres":
        if declared is None:
            return "local"
        return _validate_declared_storage_class(declared, kind=kind_norm)

    if kind_norm == "lakebase":
        if lb_block is not None:
            if has_storage_key or (
                isinstance(lb_block, Mapping) and "storage_class" in lb_block
            ):
                raise ValueError(_FIXED_LAKEBASE_STORAGE_CLASS_MSG)
            return "object_storage"
        if declared is None:
            return "unknown"
        return _validate_declared_storage_class(declared, kind=kind_norm)

    if kind_norm == "duckdb":
        eff_profile = "local_file" if profile is None else profile
        if not isinstance(eff_profile, str):
            raise ValueError("DuckDB connection 'profile' must be a string")
        if eff_profile == "iceberg_direct":
            if declared is None or declared == "object_storage":
                return "object_storage"
            raise ValueError(
                "DuckDB profile 'iceberg_direct' storage_class is fixed to 'object_storage'"
            )
        if eff_profile == "local_file":
            if declared is None:
                return "local"
            return _validate_declared_storage_class(declared, kind=kind_norm)
        raise ValueError(f"Unsupported DuckDB profile '{_safe_label(eff_profile)}'")

    if kind_norm in ("starrocks", "cube"):
        if declared is None:
            return "object_storage"
        return _validate_declared_storage_class(declared, kind=kind_norm)

    raise ValueError(f"Unsupported connection kind '{_safe_label(kind_norm)}'")


def validate_deny_roles(
    deny_roles: Sequence[str],
    *,
    error_cls: type[Exception] = ValueError,
) -> tuple[str, ...]:
    """Validate and normalize ``deny_roles`` to a deduplicated tuple of safe role identifiers."""
    if isinstance(deny_roles, (str, bytes, bytearray, Mapping)) or not isinstance(
        deny_roles, Sequence
    ):
        raise error_cls("'deny_roles' must be a sequence of role name strings")
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in deny_roles:
        if not isinstance(item, str):
            raise error_cls(
                "'deny_roles' entries must be non-empty safe role identifiers"
            )
        role = item.strip()
        if not role or not _DENY_ROLE_IDENT_RE.fullmatch(role):
            raise error_cls(
                "'deny_roles' entries must match ^[A-Za-z_][A-Za-z0-9_-]{0,62}$"
            )
        if role not in seen:
            seen.add(role)
            cleaned.append(role)
    return tuple(cleaned)


def role_guard_sql(
    kind: str,
    *,
    deny_roles: Sequence[str] = DEFAULT_DENY_ROLES,
) -> str:
    """Return the startup/pool role-guard verification SQL for ``kind``."""
    if not isinstance(kind, str):
        raise ValueError("kind must be 'lakebase' or 'postgres'")
    kind_norm = kind.strip()
    if kind_norm not in ("lakebase", "postgres"):
        raise ValueError(
            f"Unsupported connection kind '{_safe_label(kind_norm)}' for role_guard_sql; "
            "expected 'lakebase' or 'postgres'"
        )
    cleaned_deny = validate_deny_roles(deny_roles, error_cls=ValueError)
    if cleaned_deny:
        roles_list_sql = ", ".join(f"'{r}'" for r in cleaned_deny)
        return (
            "SELECT COALESCE(BOOL_OR(r.rolsuper), FALSE) AS rolsuper, "
            "COALESCE(BOOL_OR(r.rolbypassrls), FALSE) AS rolbypassrls, "
            f"COALESCE(BOOL_OR(r.rolname IN ({roles_list_sql})), FALSE) AS is_denied_role "
            "FROM pg_catalog.pg_roles AS r "
            "WHERE r.rolname IN (current_user, session_user) "
            "OR pg_catalog.pg_has_role(current_user, r.oid, 'MEMBER') "
            "OR pg_catalog.pg_has_role(session_user, r.oid, 'MEMBER')"
        )
    return (
        "SELECT COALESCE(BOOL_OR(r.rolsuper), FALSE) AS rolsuper, "
        "COALESCE(BOOL_OR(r.rolbypassrls), FALSE) AS rolbypassrls "
        "FROM pg_catalog.pg_roles AS r "
        "WHERE r.rolname IN (current_user, session_user) "
        "OR pg_catalog.pg_has_role(current_user, r.oid, 'MEMBER') "
        "OR pg_catalog.pg_has_role(session_user, r.oid, 'MEMBER')"
    )


def _safe_close_conn(conn: Any) -> None:
    try:
        close_fn = getattr(conn, "close", None)
        if callable(close_fn):
            close_fn()
    except Exception:
        pass


def check_role_guard(
    conn: Any,
    kind: str,
    *,
    deny_roles: Sequence[str] = DEFAULT_DENY_ROLES,
) -> None:
    """Execute role guard query on ``conn`` and fail closed if privileged flags are set."""
    sql = role_guard_sql(kind, deny_roles=deny_roles)
    row: Any = None
    cursor: Any = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
    except Exception as exc:
        _safe_close_conn(conn)
        raise EngineError("Role guard check failed") from exc
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass

    if row is None or len(row) < 2:
        _safe_close_conn(conn)
        raise PostgresSessionHardeningError(
            "Role guard check returned no role metadata for current_user"
        )

    has_rolsuper = bool(row[0])
    has_bypassrls = bool(row[1])
    has_denied_role = any(bool(v) for v in row[2:])

    if has_rolsuper or has_bypassrls or has_denied_role:
        _safe_close_conn(conn)
        raise PostgresSessionHardeningError(_ROLE_GUARD_REFUSED_MSG)


def _resolve_endpoint_type(cfg: Any) -> str | None:
    if cfg is None:
        return None
    if isinstance(cfg, LakebaseEndpointConfig):
        return cfg.endpoint_type
    if isinstance(cfg, Mapping):
        lb_sub = cfg.get("lakebase")
        if lb_sub is not None:
            resolved = _resolve_endpoint_type(lb_sub)
            if resolved is not None:
                return resolved
        ep_type = cfg.get("endpoint_type")
        return ep_type if isinstance(ep_type, str) else None

    lb_attr = getattr(cfg, "lakebase", None)
    if lb_attr is not None:
        resolved = _resolve_endpoint_type(lb_attr)
        if resolved is not None:
            return resolved
    ep_type = getattr(cfg, "endpoint_type", None)
    return ep_type if isinstance(ep_type, str) else None


def _coerce_utc_datetime(val: datetime | float | int | None) -> datetime:
    if val is None:
        return datetime.now(timezone.utc)
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    if not isinstance(val, bool) and isinstance(val, (int, float)):
        fval = float(val)
        if not math.isfinite(fval):
            raise ValueError("'now' timestamp must be finite")
        return datetime.fromtimestamp(fval, tz=timezone.utc)
    raise TypeError(f"Unsupported 'now' value of type {type(val).__name__}")


def snapshot_freshness(
    conn: Any,
    cfg: Any,
    *,
    now: datetime | float | None = None,
) -> float | None:
    """Measure frozen-snapshot age in seconds for a ``read_only`` Lakebase endpoint."""
    if _resolve_endpoint_type(cfg) != "read_only":
        return None

    now_utc = _coerce_utc_datetime(now)
    cursor: Any = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT pg_catalog.pg_postmaster_start_time()")
        row = cursor.fetchone()
    except Exception as exc:
        raise EngineError("Failed to query postmaster start time") from exc
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass

    if row is None or len(row) == 0 or row[0] is None:
        raise EngineError("Postmaster start time query returned empty result")

    raw_ts = row[0]
    if isinstance(raw_ts, datetime):
        start_utc = (
            raw_ts.replace(tzinfo=timezone.utc)
            if raw_ts.tzinfo is None
            else raw_ts.astimezone(timezone.utc)
        )
    elif isinstance(raw_ts, str):
        try:
            parsed = datetime.fromisoformat(raw_ts)
        except ValueError as exc:
            raise EngineError("Invalid postmaster start timestamp format") from exc
        start_utc = (
            parsed.replace(tzinfo=timezone.utc)
            if parsed.tzinfo is None
            else parsed.astimezone(timezone.utc)
        )
    else:
        raise EngineError("Unsupported postmaster start timestamp type")

    return max(0.0, float((now_utc - start_utc).total_seconds()))


def _extract_endpoint_and_cold_start(
    cfg: Any,
) -> tuple[str | None, str | None, LakebaseColdStartConfig]:
    """Extract (project_id, endpoint_id, LakebaseColdStartConfig) from supported cfg shapes."""
    if isinstance(cfg, LakebaseEndpointConfig):
        return cfg.project_id, cfg.endpoint_id, cfg.cold_start
    if isinstance(cfg, LakebaseColdStartConfig):
        return None, None, cfg
    if isinstance(cfg, Mapping):
        if "lakebase" in cfg and cfg["lakebase"] is not None:
            lb_val = cfg["lakebase"]
            if isinstance(lb_val, Mapping):
                parsed = parse_lakebase_block(lb_val)
                return parsed.project_id, parsed.endpoint_id, parsed.cold_start
            return _extract_endpoint_and_cold_start(lb_val)
        if "project_id" in cfg or "endpoint_id" in cfg:
            parsed = parse_lakebase_block(cfg)
            return parsed.project_id, parsed.endpoint_id, parsed.cold_start

    lb_attr = getattr(cfg, "lakebase", None)
    if lb_attr is not None:
        return _extract_endpoint_and_cold_start(lb_attr)

    cs_attr = getattr(cfg, "cold_start", None)
    proj_id = getattr(cfg, "project_id", None)
    ep_id = getattr(cfg, "endpoint_id", None)
    if isinstance(cs_attr, LakebaseColdStartConfig):
        return proj_id, ep_id, cs_attr

    raise ValueError(
        "ColdStartConnector requires a LakebaseEndpointConfig, LakebaseColdStartConfig, "
        "or connection config with a lakebase block"
    )


def _extract_endpoint_state(payload: Any) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    direct = payload.get("state")
    if isinstance(direct, str) and direct:
        return direct.lower()
    nested = payload.get("endpoint")
    if isinstance(nested, Mapping):
        nested_state = nested.get("state")
        if isinstance(nested_state, str) and nested_state:
            return nested_state.lower()
    return None


def _extract_pgcode(exc: BaseException) -> str | None:
    for candidate in (exc, getattr(exc, "__cause__", None)):
        if candidate is None:
            continue
        code = getattr(candidate, "pgcode", None)
        if isinstance(code, str) and code.strip():
            return code.strip().upper()
    return None


def _classify_non_wakeable_connect_error(exc: BaseException) -> EngineError | None:
    """Return a typed EngineError if ``exc`` must NOT trigger a Lakebase wake, else None."""
    if isinstance(exc, (PostgresSessionHardeningError, EngineConfigError)):
        return exc
    if isinstance(exc, EngineError) and not isinstance(exc, EngineStartingError):
        return exc
    if isinstance(exc, (ValueError, TypeError)):
        return EngineError("Engine connection failed")

    pgcode = _extract_pgcode(exc)
    if pgcode is not None:
        if pgcode.startswith("28"):
            return EngineError("Engine authentication failed")
        if pgcode.startswith(("22", "3D", "3F", "42")):
            return EngineError("Engine connection failed")

    msg_low = str(exc).lower()
    if any(
        m in msg_low
        for m in (
            "password authentication failed",
            "authentication failed",
            "no pg_hba.conf entry",
            "fe_sendauth",
            "not permitted to log in",
        )
    ):
        return EngineError("Engine authentication failed")

    if "role guard" in msg_low:
        return PostgresSessionHardeningError("Role guard refused connection")
    if "session hardening" in msg_low or "show verification" in msg_low:
        return PostgresSessionHardeningError(
            "Session hardening SHOW verification failed"
        )

    if any(
        m in msg_low
        for m in (
            "certificate verify failed",
            "ssl error",
            "sslmode",
            "root certificate file",
            "server common name",
            "hostname mismatch",
        )
    ):
        return EngineError("Engine TLS verification failed")

    mro_names = {cls.__name__ for cls in type(exc).__mro__}
    is_operational_cls = "OperationalError" in mro_names
    is_wakeable_pgcode = pgcode is None or pgcode.startswith("08") or pgcode in (
        "57P01",
        "57P02",
        "57P03",
    )
    if isinstance(exc, (OSError, ConnectionError, TimeoutError)):
        return None
    if is_operational_cls and is_wakeable_pgcode:
        return None
    if is_wakeable_pgcode and any(
        m in msg_low
        for m in (
            "connection refused",
            "connection to server",
            "could not connect",
            "timed out",
            "timeout expired",
            "network is unreachable",
            "no route to host",
            "suspended",
            "starting up",
            "warming up",
            "restarting",
            "server closed the connection",
        )
    ):
        return None

    return EngineError("Engine connection failed")


class _WakeOperation:
    """Single-flight state shared by concurrent callers waiting on one endpoint wake."""

    __slots__ = (
        "active_waiters",
        "connect_kwargs",
        "done",
        "endpoint_woke",
        "finished_at",
        "first_conn",
        "opener_connect_fn",
        "outcome",
        "started_at",
        "terminal_error",
        "wake_deadline",
    )

    def __init__(
        self,
        *,
        started_at: float,
        wake_deadline: float,
        connect_kwargs: dict[str, Any],
        opener_connect_fn: Callable[..., Any],
    ) -> None:
        self.started_at = started_at
        self.wake_deadline = wake_deadline
        self.connect_kwargs = dict(connect_kwargs)
        self.opener_connect_fn = opener_connect_fn
        self.active_waiters: int = 0
        self.done: bool = False
        self.endpoint_woke: bool = False
        self.finished_at: float = float("inf")
        self.outcome: str = "starting"  # 'ok' | 'error' | 'timeout'
        self.first_conn: Any = None
        self.terminal_error: EngineError | None = None


class ColdStartConnector:
    """Single-flight control-plane wake and bounded retry connector for Lakebase endpoints."""

    def __init__(
        self,
        connect_fn: Callable[..., Any],
        cfg: Any,
        metrics: Any = None,
        http_client: httpx.Client | None = None,
        *,
        engine_name: str = "lakebase",
        api_token: str | None = None,
        env: Mapping[str, str] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep_fn: Callable[[float], None] = time.sleep,
        executor: Any = None,
    ) -> None:
        if not callable(connect_fn):
            raise ValueError("connect_fn must be callable")
        project_id, endpoint_id, cold_start = _extract_endpoint_and_cold_start(cfg)
        if cold_start.wake == "api" and (not project_id or not endpoint_id):
            raise ValueError(
                "ColdStartConnector with wake == 'api' requires project_id and endpoint_id"
            )

        self._connect_fn = connect_fn
        self._account_connect_fns: list[Callable[..., Any]] = [connect_fn]
        self._project_id = project_id
        self._endpoint_id = endpoint_id
        self._cold_start = cold_start
        self._metrics = metrics
        self._http_client = http_client
        self._engine_name = _safe_label(engine_name, fallback="lakebase")
        self._explicit_api_token = api_token
        self._env = env
        self._clock = clock
        self._sleep_fn = sleep_fn
        if executor is not None:
            if not callable(executor) and not callable(getattr(executor, "submit", None)):
                raise TypeError("executor must be callable or have a submit method")
        self._executor = executor
        self._last_wake_worker: threading.Thread | None = None
        self._uses_real_clock = (clock is time.monotonic) and (sleep_fn is time.sleep)

        if isinstance(api_token, str) and api_token:
            register_config_secrets(api_token)

        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._state: str = "ok"
        self._last_cold_start: bool = False
        self._active_wake: _WakeOperation | None = None
        self._failed_until: float | None = None
        self._last_no_wake_probe_at: float | None = None
        self._no_wake_probe_in_flight: bool = False
        self._tls = threading.local()

    @property
    def endpoint_id(self) -> str | None:
        return self._endpoint_id

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def last_cold_start(self) -> bool:
        with self._lock:
            return self._last_cold_start

    @property
    def cold_start_config(self) -> LakebaseColdStartConfig:
        return self._cold_start

    def register_connect_fn(
        self,
        connect_fn: Callable[..., Any],
        *,
        preferred: bool = False,
    ) -> None:
        """Register an additional account connect callable sharing this endpoint connector."""
        if not callable(connect_fn):
            return
        with self._lock:
            existing = [
                fn for fn in self._account_connect_fns if fn is not connect_fn
            ]
            if preferred:
                self._connect_fn = connect_fn
                self._account_connect_fns = [connect_fn, *existing]
            else:
                self._account_connect_fns = [*existing, connect_fn]

    def reset_thread_cold_start(self) -> None:
        self._tls.cold_start = None

    def consume_thread_cold_start(self) -> bool | None:
        val = getattr(self._tls, "cold_start", None)
        self._tls.cold_start = None
        return val

    def _emit_state(self, state: str) -> None:
        if self._metrics is None:
            return
        fn = getattr(self._metrics, "set_lakebase_state", None)
        if callable(fn):
            try:
                fn(self._engine_name, state)
            except Exception:
                pass

    def _emit_wake(self, outcome: str) -> None:
        if self._metrics is None:
            return
        fn = getattr(self._metrics, "record_lakebase_wake", None)
        if callable(fn):
            try:
                fn(self._engine_name, outcome)
            except Exception:
                pass

    def _emit_connect_retry(self) -> None:
        if self._metrics is None:
            return
        fn = getattr(self._metrics, "record_lakebase_connect_retry", None)
        if callable(fn):
            try:
                fn(self._engine_name)
            except Exception:
                pass

    def _emit_cold_start_duration(self, duration_seconds: float) -> None:
        if self._metrics is None:
            return
        fn = getattr(self._metrics, "observe_lakebase_cold_start", None)
        if callable(fn):
            try:
                fn(self._engine_name, max(0.0, float(duration_seconds)))
            except Exception:
                pass

    def _resolve_api_token(self) -> str | None:
        if isinstance(self._explicit_api_token, str) and self._explicit_api_token:
            register_config_secrets(self._explicit_api_token)
            return self._explicit_api_token
        env_name = self._cold_start.api_token_env
        if not env_name:
            return None
        source_env = self._env if self._env is not None else os.environ
        token = source_env.get(env_name)
        if isinstance(token, str) and token:
            register_config_secrets(token)
            return token
        return None

    def _wake_sleep(self, seconds: float, wake_deadline: float) -> bool:
        """Sleep up to ``seconds`` bounded by ``wake_deadline`` and wake condition waiters."""
        now = self._clock()
        remaining = wake_deadline - now
        if remaining <= 0.0:
            return False
        delay = min(max(0.0, seconds), remaining)
        if delay > 0.0:
            self._sleep_fn(delay)
        with self._cond:
            self._cond.notify_all()
        return self._clock() < wake_deadline

    def _backoff_seconds(self, attempt_idx: int) -> float:
        seq = self._cold_start.retry_backoff_ms
        ms = seq[attempt_idx] if attempt_idx < len(seq) else seq[-1]
        return float(ms) / 1000.0

    def _compute_retry_after(self, op: _WakeOperation) -> int:
        rem = op.wake_deadline - self._clock()
        if not math.isfinite(rem) or rem <= 0.0:
            return 1
        return max(1, min(5, int(math.ceil(rem))))

    def _wake_via_control_plane(self, wake_deadline: float) -> str:
        """Execute POST .../start and poll GET ... until state == 'running'.

        Returns ``'running'``, ``'error'``, or ``'timeout'``. Never leaks tokens.
        """
        token = self._resolve_api_token()
        api_url = self._cold_start.api_url
        if not token or not api_url or not self._project_id or not self._endpoint_id:
            logger.error(
                "Lakebase wake error engine=%s endpoint=%s reason=missing_wake_credentials",
                self._engine_name,
                self._endpoint_id or "-",
            )
            return "error"

        base_ep_url = (
            f"{api_url}/v1/projects/{self._project_id}/endpoints/{self._endpoint_id}"
        )
        start_url = f"{base_ep_url}/start"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

        own_client: httpx.Client | None = None
        client = self._http_client
        if client is None:
            req_timeout = min(
                self._cold_start.connect_timeout_seconds,
                max(0.5, wake_deadline - self._clock()),
            )
            own_client = httpx.Client(timeout=req_timeout)
            client = own_client

        try:
            try:
                start_resp = client.post(start_url, headers=headers)
            except Exception as exc:
                logger.error(
                    "Lakebase wake error engine=%s endpoint=%s phase=start error=%s",
                    self._engine_name,
                    self._endpoint_id,
                    type(exc).__name__,
                )
                return "error"

            if start_resp.status_code < 200 or start_resp.status_code >= 300:
                logger.error(
                    "Lakebase wake error engine=%s endpoint=%s phase=start status=%d",
                    self._engine_name,
                    self._endpoint_id,
                    int(start_resp.status_code),
                )
                return "error"

            poll_idx = 0
            while True:
                if self._clock() >= wake_deadline:
                    return "timeout"

                try:
                    poll_resp = client.get(base_ep_url, headers=headers)
                except Exception as exc:
                    logger.error(
                        "Lakebase wake error engine=%s endpoint=%s phase=poll error=%s",
                        self._engine_name,
                        self._endpoint_id,
                        type(exc).__name__,
                    )
                    return "error"

                if poll_resp.status_code < 200 or poll_resp.status_code >= 300:
                    logger.error(
                        "Lakebase wake error engine=%s endpoint=%s phase=poll status=%d",
                        self._engine_name,
                        self._endpoint_id,
                        int(poll_resp.status_code),
                    )
                    return "error"

                try:
                    body = poll_resp.json()
                except Exception:
                    logger.error(
                        "Lakebase wake error engine=%s endpoint=%s phase=poll reason=invalid_json",
                        self._engine_name,
                        self._endpoint_id,
                    )
                    return "error"

                ep_state = _extract_endpoint_state(body)
                if ep_state == "running":
                    return "running"
                if ep_state in _TERMINAL_ENDPOINT_ERROR_STATES or ep_state is None:
                    logger.error(
                        "Lakebase wake error engine=%s endpoint=%s phase=poll endpoint_state=%s",
                        self._engine_name,
                        self._endpoint_id,
                        _safe_label(ep_state, fallback="unknown"),
                    )
                    return "error"

                if not self._wake_sleep(self._backoff_seconds(poll_idx), wake_deadline):
                    return "timeout"
                poll_idx += 1
        finally:
            if own_client is not None:
                try:
                    own_client.close()
                except Exception:
                    pass

    def _run_wake(self, op: _WakeOperation) -> None:
        """Background single-flight wake worker."""
        outcome = "timeout"
        opened_conn: Any = None
        try:
            if self._cold_start.wake == "api":
                api_status = self._wake_via_control_plane(op.wake_deadline)
                if api_status != "running":
                    outcome = api_status  # 'error' or 'timeout'
                    return
                op.endpoint_woke = True
            else:
                op.endpoint_woke = True

            retry_idx = 0
            while True:
                if self._clock() >= op.wake_deadline:
                    outcome = "timeout"
                    return

                if self._cold_start.wake == "none" or retry_idx > 0:
                    if retry_idx > 0 or self._cold_start.wake == "none":
                        if retry_idx == 0 and self._cold_start.wake == "none":
                            if not self._wake_sleep(
                                self._backoff_seconds(0), op.wake_deadline
                            ):
                                outcome = "timeout"
                                return
                        self._emit_connect_retry()

                try:
                    opened_conn = op.opener_connect_fn(**op.connect_kwargs)
                    outcome = "ok"
                    return
                except Exception as conn_exc:
                    opened_conn = None
                    non_wakeable = _classify_non_wakeable_connect_error(conn_exc)
                    if non_wakeable is not None:
                        op.terminal_error = non_wakeable
                        outcome = "error"
                        return

                backoff_idx = retry_idx + 1 if self._cold_start.wake == "none" else retry_idx
                if not self._wake_sleep(
                    self._backoff_seconds(backoff_idx), op.wake_deadline
                ):
                    outcome = "timeout"
                    return
                retry_idx += 1
        except Exception as exc:
            logger.error(
                "Lakebase wake unexpected failure engine=%s error=%s",
                self._engine_name,
                type(exc).__name__,
            )
            outcome = "error"
        finally:
            finish_now = self._clock()
            elapsed = max(0.0, finish_now - op.started_at)
            orphan_conn: Any = None
            with self._cond:
                op.finished_at = finish_now
                op.outcome = outcome
                if outcome == "ok":
                    self._state = "ok"
                    self._last_cold_start = True
                    self._failed_until = None
                    self._last_no_wake_probe_at = None
                    if op.active_waiters > 0:
                        op.first_conn = opened_conn
                    else:
                        orphan_conn = opened_conn
                elif op.terminal_error is not None:
                    self._state = "ok"
                    self._failed_until = None
                    self._last_no_wake_probe_at = None
                    orphan_conn = opened_conn
                else:
                    self._state = "unavailable"
                    self._last_no_wake_probe_at = None
                    self._failed_until = (
                        finish_now + self._cold_start.max_wake_seconds
                    )
                    orphan_conn = opened_conn

                op.done = True
                if self._active_wake is op:
                    self._active_wake = None
                new_state = self._state
                self._emit_wake(outcome)
                if outcome == "ok":
                    self._emit_cold_start_duration(elapsed)
                self._emit_state(new_state)
                self._cond.notify_all()

            if orphan_conn is not None:
                _safe_close_conn(orphan_conn)

            if outcome != "ok":
                logger.warning(
                    "Lakebase outage engine=%s endpoint=%s outcome=%s elapsed_s=%.3f",
                    self._engine_name,
                    self._endpoint_id or "-",
                    outcome,
                    elapsed,
                )

    def probe_no_wake(
        self,
        *,
        timeout_seconds: float = 2.0,
        min_interval_seconds: float = 5.0,
        now: float | None = None,
        query: str | None = None,
        probe_query: str | None = None,
    ) -> str:
        """Perform at most one bounded no-wake plain connect per interval when unavailable."""
        effective_query = (
            probe_query
            if probe_query is not None
            else (
                query
                if query is not None
                else getattr(self, "_probe_query", "SELECT 1")
            )
        )
        with self._lock:
            if self._state == "ok":
                return "ok"
            if self._state == "starting" or (
                self._active_wake is not None and not self._active_wake.done
            ):
                return "starting"
            probe_now = float(now) if now is not None else float(self._clock())
            if self._no_wake_probe_in_flight:
                return "unavailable"
            if (
                min_interval_seconds > 0.0
                and self._last_no_wake_probe_at is not None
                and 0.0 <= (probe_now - self._last_no_wake_probe_at) < min_interval_seconds
            ):
                return self._state
            self._last_no_wake_probe_at = probe_now
            self._no_wake_probe_in_flight = True

        try:
            eff_timeout = float(timeout_seconds)
            if math.isinf(eff_timeout):
                raise OverflowError("cannot convert float infinity to integer")
            if math.isnan(eff_timeout):
                raise ValueError("cannot convert float NaN to integer")
            to_param: int | float = (
                int(eff_timeout) if eff_timeout >= 1.0 else eff_timeout
            )
            with self._lock:
                candidate_fns = list(self._account_connect_fns) or [self._connect_fn]
            probe_succeeded = False
            for target_fn in candidate_fns:
                conn: Any = None
                cur: Any = None
                try:
                    def _attempt_target_connect() -> Any:
                        accepts_kw = True
                        try:
                            sig = inspect.signature(target_fn)
                            accepts_kw = "connect_timeout" in sig.parameters or any(
                                p.kind == inspect.Parameter.VAR_KEYWORD
                                for p in sig.parameters.values()
                            )
                        except (ValueError, TypeError):
                            accepts_kw = True

                        if accepts_kw:
                            try:
                                return target_fn(connect_timeout=to_param)
                            except TypeError:
                                try:
                                    return target_fn(connect_timeout=max(1, int(eff_timeout)))
                                except TypeError:
                                    return target_fn()
                        return target_fn()

                    if eff_timeout < 1.0:
                        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                        try:
                            fut = pool.submit(_attempt_target_connect)
                            conn = fut.result(timeout=eff_timeout)
                        except concurrent.futures.TimeoutError:
                            conn = None
                            continue
                        finally:
                            pool.shutdown(wait=False)
                    else:
                        conn = _attempt_target_connect()

                    if conn is None:
                        continue

                    timeout_ms = max(1, int(round(eff_timeout * 1000)))
                    if hasattr(conn, "cursor") and callable(conn.cursor):
                        cur = conn.cursor()
                        if (
                            cur is not None
                            and hasattr(cur, "execute")
                            and callable(cur.execute)
                        ):
                            cur.execute("BEGIN")
                            try:
                                cur.execute(
                                    "SELECT set_config('statement_timeout', %s, true)",
                                    (str(timeout_ms),),
                                )
                                cur.execute(effective_query)
                            finally:
                                try:
                                    cur.execute("ROLLBACK")
                                except Exception:
                                    pass
                    probe_succeeded = True
                    break
                except (NameError, AttributeError, TypeError) as exc:
                    logger.error("Programming error during probe_no_wake: %s", exc, exc_info=True)
                    raise
                except Exception as exc:
                    if _classify_non_wakeable_connect_error(exc) is not None:
                        continue
                    return "unavailable"
                finally:
                    if (
                        cur is not None
                        and hasattr(cur, "close")
                        and callable(cur.close)
                    ):
                        try:
                            cur.close()
                        except Exception:
                            pass
                    if conn is not None:
                        _safe_close_conn(conn)

            if not probe_succeeded:
                return "unavailable"

            with self._lock:
                if self._active_wake is None or self._active_wake.done:
                    self._state = "ok"
                    self._failed_until = None
                    self._last_no_wake_probe_at = None
                    self._emit_state("ok")
                cur_state = self._state
            return cur_state
        finally:
            with self._lock:
                self._no_wake_probe_in_flight = False

    def _get_or_start_wake(
        self,
        connect_kwargs: dict[str, Any],
        opener_connect_fn: Callable[..., Any],
    ) -> _WakeOperation:
        started_new = False
        with self._cond:
            if self._active_wake is not None and not self._active_wake.done:
                return self._active_wake
            now = self._clock()
            if self._failed_until is not None and now < self._failed_until:
                raise EngineError("Engine connection failed")
            op = _WakeOperation(
                started_at=now,
                wake_deadline=now + self._cold_start.max_wake_seconds,
                connect_kwargs=connect_kwargs,
                opener_connect_fn=opener_connect_fn,
            )
            self._active_wake = op
            self._state = "starting"
            self._emit_state("starting")
            started_new = True

        if started_new:
            try:
                if self._executor is not None:
                    if callable(getattr(self._executor, "submit", None)):
                        self._executor.submit(self._run_wake, op)
                    elif callable(self._executor):
                        self._executor(self._run_wake, op)
                else:
                    worker = threading.Thread(
                        target=self._run_wake,
                        args=(op,),
                        daemon=True,
                        name=f"lakebase-wake-{self._engine_name}",
                    )
                    self._last_wake_worker = worker
                    worker.start()
            except Exception as exc:
                finish_now = self._clock()
                with self._cond:
                    op.finished_at = finish_now
                    op.outcome = "error"
                    op.done = True
                    self._state = "unavailable"
                    self._last_no_wake_probe_at = None
                    self._failed_until = finish_now + self._cold_start.max_wake_seconds
                    if self._active_wake is op:
                        self._active_wake = None
                    self._emit_wake("error")
                    self._emit_state("unavailable")
                    self._cond.notify_all()
                raise EngineError(f"Engine connection failed: {exc}") from exc
        return op

    def connect(
        self,
        deadline: float | None = None,
        *,
        connect_fn: Callable[..., Any] | None = None,
        **connect_kwargs: Any,
    ) -> Any:
        """Open a Lakebase connection, coordinating single-flight cold-start wake on failure."""
        eff_connect_fn = connect_fn if connect_fn is not None else self._connect_fn
        call_start = self._clock()
        self._tls.cold_start = False
        query_budget_deadline = call_start + self._cold_start.max_query_wait_seconds
        if deadline is not None:
            wait_deadline = min(query_budget_deadline, float(deadline))
        else:
            wait_deadline = query_budget_deadline

        with self._lock:
            existing_op = (
                self._active_wake
                if (self._active_wake is not None and not self._active_wake.done)
                else None
            )
            in_cooldown = (
                existing_op is None
                and self._failed_until is not None
                and call_start < self._failed_until
            )

        if existing_op is None:
            try:
                conn = eff_connect_fn(**connect_kwargs)
            except Exception as exc:
                non_wakeable = _classify_non_wakeable_connect_error(exc)
                if non_wakeable is not None:
                    raise non_wakeable from None
                if in_cooldown:
                    raise EngineError("Engine connection failed") from None
                self._tls.cold_start = True
                op = self._get_or_start_wake(connect_kwargs, eff_connect_fn)
            else:
                with self._lock:
                    if self._active_wake is None or self._active_wake.done:
                        self._state = "ok"
                        self._last_cold_start = False
                        self._failed_until = None
                        self._last_no_wake_probe_at = None
                        self._emit_state("ok")
                self._tls.cold_start = False
                return conn
        else:
            self._tls.cold_start = True
            op = existing_op

        claimed_conn: Any = None
        orphan_conn: Any = None
        with self._cond:
            op.active_waiters += 1
            try:
                while not op.done:
                    remaining = wait_deadline - self._clock()
                    if remaining <= 0.0:
                        raise EngineStartingError(
                            retry_after=self._compute_retry_after(op)
                        )
                    wait_slice = (
                        remaining if self._uses_real_clock else min(remaining, 0.002)
                    )
                    self._cond.wait(timeout=wait_slice)

                if op.finished_at > wait_deadline:
                    raise EngineStartingError(
                        retry_after=self._compute_retry_after(op)
                    )

                if op.outcome != "ok":
                    if eff_connect_fn is not op.opener_connect_fn and (
                        op.terminal_error is not None or op.endpoint_woke
                    ):
                        pass
                    else:
                        if op.terminal_error is not None:
                            raise op.terminal_error
                        raise EngineError("Engine connection failed")

                if (
                    op.first_conn is not None
                    and op.opener_connect_fn is eff_connect_fn
                    and op.connect_kwargs == connect_kwargs
                ):
                    claimed_conn = op.first_conn
                    op.first_conn = None
            finally:
                op.active_waiters -= 1
                if op.done and op.active_waiters == 0 and op.first_conn is not None:
                    orphan_conn = op.first_conn
                    op.first_conn = None

        if orphan_conn is not None:
            _safe_close_conn(orphan_conn)

        if claimed_conn is not None:
            return claimed_conn

        try:
            conn = eff_connect_fn(**connect_kwargs)
        except Exception as exc:
            non_wakeable = _classify_non_wakeable_connect_error(exc)
            if non_wakeable is not None:
                raise non_wakeable from None
            with self._lock:
                if self._active_wake is None or self._active_wake.done:
                    self._state = "unavailable"
                    self._emit_state("unavailable")
            raise EngineError("Engine connection failed") from None

        with self._lock:
            if self._active_wake is None or self._active_wake.done:
                self._state = "ok"
                self._last_cold_start = True
                self._failed_until = None
                self._last_no_wake_probe_at = None
                self._emit_state("ok")
        return conn
