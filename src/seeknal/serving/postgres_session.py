"""Shared PostgreSQL-wire session hardening, TLS, account, and fallback Lakebase helpers."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

POSTGRES_WIRE_KINDS: frozenset[str] = frozenset({"postgres", "lakebase"})
VALID_POSTGRES_MODES: frozenset[str] = frozenset({"read_only", "read_write"})
VALID_SSLMODES: frozenset[str] = frozenset(
    {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
)

_SEARCH_PATH_ITEM_RE = re.compile(r'^(?:[A-Za-z_][A-Za-z0-9_$]*|"\$user"|\$user)$')
_ENV_VAR_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_LAKEBASE_ID_RE_MAP: dict[str, re.Pattern[str]] = {
    "project_id": re.compile(r"^prj_[a-z0-9]{20}$"),
    "branch_id": re.compile(r"^br_[a-z0-9]{20}$"),
    "endpoint_id": re.compile(r"^ep_[a-z0-9]{20}$"),
}
_QUALIFIED_TABLE_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,62}$"
)
_PG_DURATION_RE = re.compile(
    r"^\s*([+-]?\d+(?:\.\d+)?)\s*(us|ms|s|min|h|d)?\s*$",
    re.IGNORECASE,
)
_FORBIDDEN_LITERAL_SECRET_KEYS: frozenset[str] = frozenset(
    {
        "dsn",
        "password",
        "passwd",
        "secret",
        "token",
        "api_token",
        "uri",
        "url",
        "connection_string",
    }
)


def is_postgres_wire(kind: Any) -> bool:
    """Return True when *kind* uses the shared PostgreSQL wire protocol ('postgres' or 'lakebase')."""
    return isinstance(kind, str) and kind in POSTGRES_WIRE_KINDS


def _validate_positive_int(val: Any, field_name: str, error_cls: type[Exception] = ValueError) -> int:
    if isinstance(val, bool) or not isinstance(val, int) or val <= 0:
        raise error_cls(f"'{field_name}' must be a positive integer")
    return val


def _parse_search_path_parts(search_path: Any, error_cls: type[Exception] = ValueError) -> list[str]:
    if not isinstance(search_path, str) or not search_path.strip():
        raise error_cls("'search_path' must be a non-empty string")
    raw_parts = [p.strip() for p in search_path.split(",")]
    if not raw_parts or any(not p for p in raw_parts):
        raise error_cls("'search_path' contains an empty schema entry")
    for p in raw_parts:
        if not _SEARCH_PATH_ITEM_RE.fullmatch(p):
            raise error_cls("Invalid schema identifier in 'search_path'")
    return raw_parts


@dataclass(frozen=True)
class PostgresSessionConfig:
    """Session-level PostgreSQL hardening parameters."""

    statement_timeout_ms: int = 30000
    lock_timeout_ms: int = 2000
    idle_in_transaction_session_timeout_ms: int = 10000
    search_path: str = "pg_catalog"
    application_name: str = "seeknal-serving"
    startup_options: bool = True
    connection_limit: int | None = None

    def __post_init__(self) -> None:
        _validate_positive_int(self.statement_timeout_ms, "statement_timeout_ms")
        _validate_positive_int(self.lock_timeout_ms, "lock_timeout_ms")
        _validate_positive_int(
            self.idle_in_transaction_session_timeout_ms,
            "idle_in_transaction_session_timeout_ms",
        )
        _parse_search_path_parts(self.search_path)
        if (
            not isinstance(self.application_name, str)
            or not self.application_name.strip()
            or any(ord(ch) < 32 for ch in self.application_name)
        ):
            raise ValueError("'application_name' must be a non-empty printable string")
        if not isinstance(self.startup_options, bool):
            raise ValueError("'startup_options' must be a boolean")
        if self.connection_limit is not None:
            _validate_positive_int(self.connection_limit, "connection_limit")


@dataclass(frozen=True)
class PostgresTLSConfig:
    """TLS verification settings for PostgreSQL-wire connections."""

    sslmode: str = "verify-full"
    sslrootcert_file: str | None = None
    sslcert_file: str | None = None
    sslkey_file: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.sslmode, str) or self.sslmode not in VALID_SSLMODES:
            raise ValueError(
                f"'sslmode' must be one of {sorted(VALID_SSLMODES)}"
            )
        for attr in ("sslrootcert_file", "sslcert_file", "sslkey_file"):
            val = getattr(self, attr)
            if val is not None and (not isinstance(val, str) or not val.strip()):
                raise ValueError(f"'{attr}' must be a non-empty string when set")


@dataclass(frozen=True)
class PostgresAccountConfig:
    """Per-workload PostgreSQL credential reference (dsn_env XOR dsn_file)."""

    dsn_env: str | None = None
    dsn_file: str | None = None

    def __post_init__(self) -> None:
        has_env = self.dsn_env is not None and bool(str(self.dsn_env).strip())
        has_file = self.dsn_file is not None and bool(str(self.dsn_file).strip())
        if has_env and has_file:
            raise ValueError("PostgresAccountConfig cannot set both 'dsn_env' and 'dsn_file'")
        if not has_env and not has_file:
            raise ValueError("PostgresAccountConfig requires either 'dsn_env' or 'dsn_file'")


@dataclass(frozen=True)
class PostgresFreshnessConfig:
    """Data-level freshness marker configuration for PostgreSQL-wire sources."""

    sync_state_table: str | None = None
    freshness_sql: str | None = None


def build_connect_kwargs(
    dsn: str,
    *,
    connect_timeout: float = 10.0,
    session: PostgresSessionConfig | None = None,
    tls: PostgresTLSConfig | None = None,
    read_only: bool = False,
    kind: str = "postgres",
) -> dict[str, Any]:
    """Build keyword arguments for ``psycopg2.connect(dsn, **kwargs)``.

    Note: ``dsn`` is validated to be a non-empty string and is intentionally omitted from
    the returned dictionary so ``psycopg2.connect(dsn, **kwargs)`` receives ``dsn`` once
    positionally and ``kwargs`` never carries secret credentials.
    """
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValueError("Database DSN must be a non-empty string")
    if (
        isinstance(connect_timeout, bool)
        or not isinstance(connect_timeout, (int, float))
        or math.isnan(connect_timeout)
        or math.isinf(connect_timeout)
        or connect_timeout <= 0
    ):
        raise ValueError("connect_timeout must be a positive finite number")

    kwargs: dict[str, Any] = {
        "connect_timeout": max(1, math.ceil(float(connect_timeout))),
    }

    if session is not None:
        kwargs["application_name"] = session.application_name
        if session.startup_options:
            sp_parts = _parse_search_path_parts(session.search_path)
            sp_opt = ",".join(sp_parts)
            opt_parts: list[str] = []
            if read_only:
                opt_parts.append("-c default_transaction_read_only=on")
            opt_parts.append(f"-c statement_timeout={int(session.statement_timeout_ms)}")
            opt_parts.append(f"-c lock_timeout={int(session.lock_timeout_ms)}")
            opt_parts.append(
                f"-c idle_in_transaction_session_timeout={int(session.idle_in_transaction_session_timeout_ms)}"
            )
            opt_parts.append(f"-c search_path={sp_opt}")
            kwargs["options"] = " ".join(opt_parts)
    elif read_only:
        kwargs["options"] = "-c default_transaction_read_only=on"

    if tls is not None:
        kwargs["sslmode"] = tls.sslmode
        if tls.sslrootcert_file:
            kwargs["sslrootcert"] = tls.sslrootcert_file
        if tls.sslcert_file:
            kwargs["sslcert"] = tls.sslcert_file
        if tls.sslkey_file:
            kwargs["sslkey"] = tls.sslkey_file
    elif session is not None and kind == "postgres":
        kwargs["sslmode"] = "verify-full"

    return kwargs


def _fetch_one_row(cursor: Any) -> Any:
    if hasattr(cursor, "fetchone") and callable(cursor.fetchone):
        return cursor.fetchone()
    if hasattr(cursor, "fetchmany") and callable(cursor.fetchmany):
        rows = cursor.fetchmany(1)
        return rows[0] if rows else None
    return None


def _pg_bool_is_true(val: Any) -> bool:
    if val is True:
        return True
    if val is False or val is None:
        return False
    if isinstance(val, int):
        if val == 1:
            return True
        if val == 0:
            return False
        return True
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("t", "true", "1", "on", "yes"):
            return True
        if s in ("f", "false", "0", "off", "no"):
            return False
        raise ValueError("Invalid boolean scalar from PostgreSQL")
    raise ValueError("Invalid boolean scalar from PostgreSQL")


def _parse_pg_duration_ms(raw: Any) -> int | None:
    if isinstance(raw, bool) or raw is None:
        return None
    if isinstance(raw, int):
        return raw
    if not isinstance(raw, str):
        return None
    m = _PG_DURATION_RE.fullmatch(raw)
    if not m:
        return None
    num_str, unit_raw = m.group(1), m.group(2)
    try:
        num = float(num_str)
    except ValueError:
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    unit = (unit_raw or "ms").lower()
    multipliers: dict[str, float] = {
        "us": 0.001,
        "ms": 1.0,
        "s": 1000.0,
        "min": 60_000.0,
        "h": 3_600_000.0,
        "d": 86_400_000.0,
    }
    mult = multipliers.get(unit)
    if mult is None:
        return None
    ms_float = num * mult
    ms_round = round(ms_float)
    if abs(ms_float - ms_round) > 1e-6:
        return None
    return int(ms_round)


def _normalize_search_path_token(tok: str) -> str:
    t = tok.strip()
    if t == '"$user"':
        return "$user"
    return t


def _search_path_matches(actual: Any, expected: str) -> bool:
    if not isinstance(actual, str) or not actual.strip():
        return False
    actual_parts = [_normalize_search_path_token(p) for p in actual.split(",") if p.strip()]
    expected_parts = [_normalize_search_path_token(p) for p in expected.split(",") if p.strip()]
    return bool(actual_parts) and actual_parts == expected_parts


def _close_quietly(obj: Any) -> None:
    if obj is not None and hasattr(obj, "close") and callable(obj.close):
        try:
            obj.close()
        except Exception:
            pass


def verify_and_harden_session(
    conn: Any,
    *,
    kind: str = "postgres",
    read_only: bool = False,
    session: PostgresSessionConfig | None = None,
    role_guard: bool = True,
    is_legacy_lakebase: bool = False,
) -> None:
    """Verify role guard and session hardening on a newly opened connection.

    Fails closed: on any error, privileged role, or ``SHOW`` mismatch, closes ``conn``
    and raises ``EngineError`` without leaking DSNs or driver secrets.
    """
    from seeknal.serving.adapters import EngineError

    if conn is None:
        raise EngineError("Database connection is None")

    should_check_role = bool(role_guard or kind == "lakebase")
    if not should_check_role and session is None:
        return

    cur: Any = None
    ran_sets = False
    try:
        cur = conn.cursor()

        if should_check_role:
            cur.execute(role_guard_sql(kind))
            row = _fetch_one_row(cur)
            if isinstance(row, (tuple, list)):
                if len(row) < 2:
                    raise EngineError("Role guard refused connection")
                for col_val in row:
                    if _pg_bool_is_true(col_val):
                        raise EngineError("Role guard refused connection")
            else:
                if not (is_legacy_lakebase and session is None):
                    raise EngineError("Role guard refused connection")

        if session is not None:
            sp_parts = _parse_search_path_parts(session.search_path, error_cls=EngineError)
            if not session.startup_options:
                if read_only:
                    cur.execute("SET default_transaction_read_only = on")
                cur.execute(f"SET statement_timeout = {int(session.statement_timeout_ms)}")
                cur.execute(f"SET lock_timeout = {int(session.lock_timeout_ms)}")
                cur.execute(
                    f"SET idle_in_transaction_session_timeout = {int(session.idle_in_transaction_session_timeout_ms)}"
                )
                sp_sql = ", ".join(sp_parts)
                cur.execute(f"SET search_path TO {sp_sql}")
                ran_sets = True

            if read_only:
                cur.execute("SHOW default_transaction_read_only")
                ro_row = _fetch_one_row(cur)
                if (
                    not isinstance(ro_row, (tuple, list))
                    or not ro_row
                    or not isinstance(ro_row[0], str)
                    or ro_row[0].strip().lower() != "on"
                ):
                    raise EngineError("Session hardening SHOW verification failed")

            for guc_name, expected_ms in (
                ("statement_timeout", int(session.statement_timeout_ms)),
                ("lock_timeout", int(session.lock_timeout_ms)),
                (
                    "idle_in_transaction_session_timeout",
                    int(session.idle_in_transaction_session_timeout_ms),
                ),
            ):
                cur.execute(f"SHOW {guc_name}")
                guc_row = _fetch_one_row(cur)
                if not isinstance(guc_row, (tuple, list)) or not guc_row:
                    raise EngineError("Session hardening SHOW verification failed")
                actual_ms = _parse_pg_duration_ms(guc_row[0])
                if actual_ms != expected_ms:
                    raise EngineError("Session hardening SHOW verification failed")

            cur.execute("SHOW search_path")
            sp_row = _fetch_one_row(cur)
            if (
                not isinstance(sp_row, (tuple, list))
                or not sp_row
                or not _search_path_matches(sp_row[0], session.search_path)
            ):
                raise EngineError("Session hardening SHOW verification failed")

        _close_quietly(cur)
        cur = None

        # End any implicit PEP 249 transaction opened by SELECT/SET/SHOW so the
        # connection returns to IDLE before entering the pool.
        if not getattr(conn, "autocommit", False):
            if ran_sets:
                if hasattr(conn, "commit") and callable(conn.commit):
                    conn.commit()
            else:
                if hasattr(conn, "rollback") and callable(conn.rollback):
                    try:
                        conn.rollback()
                    except Exception:
                        pass
    except EngineError:
        _close_quietly(cur)
        _close_quietly(conn)
        raise
    except Exception:
        _close_quietly(cur)
        _close_quietly(conn)
        raise EngineError("Session hardening verification failed") from None


def begin_query_transaction(
    cursor: Any,
    *,
    read_only: bool,
    timeout_ms: int,
    workload: str,
    audit_id: str,
    hardened: bool,
    has_row_limit: bool = True,
) -> None:
    """Open a per-query transaction with statement timeout and parameterised application_name."""
    ms = max(1, int(timeout_ms))
    if hardened or read_only:
        cursor.execute("BEGIN READ ONLY" if read_only else "BEGIN")
        cursor.execute(f"SET LOCAL statement_timeout = {ms}")
        cursor.execute(
            "SELECT set_config('application_name', %s, true)",
            (f"seeknal:{workload}:{audit_id}",),
        )
    else:
        cursor.execute("BEGIN")
        if has_row_limit:
            cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute(f"SET LOCAL statement_timeout = {ms}")


# ---------------------------------------------------------------------------
# S1 <-> S2 interface imports with standalone fallback before S2 is merged
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _FallbackLakebaseColdStartConfig:
    """Fallback cold-start configuration when seeknal.serving.lakebase_endpoint is absent."""

    connect_timeout_seconds: float = 5.0
    wake: str = "none"
    api_url: str | None = None
    api_token_env: str | None = None
    max_query_wait_seconds: float = 10.0
    max_wake_seconds: float = 60.0
    retry_backoff_ms: tuple[int, ...] = (500, 1000, 2000, 4000)


@dataclass(frozen=True)
class _FallbackLakebaseEndpointConfig:
    """Fallback Lakebase endpoint metadata configuration when S2 module is absent."""

    project_id: str
    branch_id: str
    endpoint_id: str
    endpoint_type: str = "read_write"
    cu: float | int | None = None
    cold_start: _FallbackLakebaseColdStartConfig = field(
        default_factory=_FallbackLakebaseColdStartConfig
    )
    freshness: PostgresFreshnessConfig = field(
        default_factory=lambda: PostgresFreshnessConfig(
            sync_state_table="public._lakebase_sync_state",
            freshness_sql=None,
        )
    )


def _fallback_role_guard_sql(kind: str) -> str:
    if kind == "lakebase":
        return (
            "SELECT r.rolsuper, r.rolbypassrls, "
            "CASE WHEN EXISTS ("
            "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'neon_superuser'"
            ") THEN pg_catalog.pg_has_role(current_user, 'neon_superuser', 'MEMBER') "
            "ELSE FALSE END AS is_neon_superuser "
            "FROM pg_catalog.pg_roles AS r "
            "WHERE r.rolname = current_user"
        )
    return (
        "SELECT r.rolsuper, r.rolbypassrls "
        "FROM pg_catalog.pg_roles AS r "
        "WHERE r.rolname = current_user"
    )


def _fallback_parse_lakebase_block(
    raw: Any,
    *,
    eng_name: str = "lakebase",
    error_cls: type[Exception] = ValueError,
) -> _FallbackLakebaseEndpointConfig:
    if not isinstance(raw, Mapping):
        raise error_cls(
            f"Lakebase connection '{eng_name}' field 'lakebase' must be a mapping"
        )

    for key in raw.keys():
        k_low = str(key).strip().lower()
        if k_low in _FORBIDDEN_LITERAL_SECRET_KEYS:
            raise error_cls(
                f"Lakebase connection '{eng_name}' forbids literal secret key '{k_low}'"
            )
        if k_low == "storage_class":
            raise error_cls(
                f"Lakebase connection '{eng_name}' cannot set 'storage_class' (fixed to 'object_storage')"
            )

    allowed_lb_keys = {
        "project_id",
        "branch_id",
        "endpoint_id",
        "endpoint_type",
        "cu",
        "cold_start",
        "freshness",
    }
    unknown = set(raw.keys()) - allowed_lb_keys
    if unknown:
        raise error_cls(
            f"Unknown key(s) in lakebase block for '{eng_name}': {sorted(str(k) for k in unknown)}"
        )

    ids: dict[str, str] = {}
    for id_field, pattern in _LAKEBASE_ID_RE_MAP.items():
        val = raw.get(id_field)
        if not isinstance(val, str) or not pattern.fullmatch(val.strip()):
            raise error_cls(
                f"Lakebase connection '{eng_name}' has missing or invalid '{id_field}'"
            )
        ids[id_field] = val.strip()

    endpoint_type = raw.get("endpoint_type", "read_write")
    if (
        not isinstance(endpoint_type, str)
        or endpoint_type.strip() not in VALID_POSTGRES_MODES
    ):
        raise error_cls(
            f"Lakebase connection '{eng_name}' 'endpoint_type' must be 'read_write' or 'read_only'"
        )
    endpoint_type_clean = endpoint_type.strip()

    cu_val = raw.get("cu")
    if cu_val is not None:
        if (
            isinstance(cu_val, bool)
            or not isinstance(cu_val, (int, float))
            or math.isnan(cu_val)
            or math.isinf(cu_val)
            or cu_val <= 0
        ):
            raise error_cls(
                f"Lakebase connection '{eng_name}' 'cu' must be a positive number"
            )

    cs_raw = raw.get("cold_start")
    if cs_raw is None:
        cold_start_cfg = _FallbackLakebaseColdStartConfig()
    else:
        if not isinstance(cs_raw, Mapping):
            raise error_cls(
                f"Lakebase connection '{eng_name}' 'cold_start' must be a mapping"
            )
        for ck in cs_raw.keys():
            ck_low = str(ck).strip().lower()
            if ck_low in _FORBIDDEN_LITERAL_SECRET_KEYS:
                raise error_cls(
                    f"Lakebase connection '{eng_name}' 'cold_start' forbids literal secret key '{ck_low}'"
                )
        allowed_cs = {
            "connect_timeout_seconds",
            "wake",
            "api_url",
            "api_token_env",
            "max_query_wait_seconds",
            "max_wake_seconds",
            "retry_backoff_ms",
        }
        unknown_cs = set(cs_raw.keys()) - allowed_cs
        if unknown_cs:
            raise error_cls(
                f"Unknown key(s) in lakebase.cold_start for '{eng_name}': {sorted(str(k) for k in unknown_cs)}"
            )

        def _pos_float(field_k: str, default_v: float) -> float:
            v = cs_raw.get(field_k, default_v)
            if (
                isinstance(v, (bool, str))
                or not isinstance(v, (int, float))
                or math.isnan(v)
                or math.isinf(v)
                or v <= 0
            ):
                raise error_cls(
                    f"Lakebase connection '{eng_name}' 'cold_start.{field_k}' must be a positive number"
                )
            return float(v)

        cs_ct = _pos_float("connect_timeout_seconds", 5.0)
        cs_mqw = _pos_float("max_query_wait_seconds", 10.0)
        cs_mw = _pos_float("max_wake_seconds", 60.0)

        wake_raw = cs_raw.get("wake", "none")
        if not isinstance(wake_raw, str) or wake_raw.strip() not in ("api", "none"):
            raise error_cls(
                f"Lakebase connection '{eng_name}' 'cold_start.wake' must be 'api' or 'none'"
            )
        wake_clean = wake_raw.strip()

        api_url_raw = cs_raw.get("api_url")
        api_url_clean: str | None = None
        if api_url_raw is not None:
            if (
                not isinstance(api_url_raw, str)
                or not api_url_raw.strip()
                or not (
                    api_url_raw.strip().startswith("https://")
                    or api_url_raw.strip().startswith("http://")
                )
                or "@" in api_url_raw
            ):
                raise error_cls(
                    f"Lakebase connection '{eng_name}' 'cold_start.api_url' must be a valid HTTP(S) URL without credentials"
                )
            api_url_clean = api_url_raw.strip()

        api_tok_raw = cs_raw.get("api_token_env")
        api_tok_clean: str | None = None
        if api_tok_raw is not None:
            if (
                not isinstance(api_tok_raw, str)
                or not _ENV_VAR_NAME_RE.fullmatch(api_tok_raw.strip())
            ):
                raise error_cls(
                    f"Lakebase connection '{eng_name}' 'cold_start.api_token_env' must be a valid environment variable name"
                )
            api_tok_clean = api_tok_raw.strip()

        if wake_clean == "api" and (not api_url_clean or not api_tok_clean):
            raise error_cls(
                f"Lakebase connection '{eng_name}' with 'cold_start.wake: api' requires 'api_url' and 'api_token_env'"
            )

        rb_raw = cs_raw.get("retry_backoff_ms", (500, 1000, 2000, 4000))
        if not isinstance(rb_raw, (list, tuple)) or not rb_raw:
            raise error_cls(
                f"Lakebase connection '{eng_name}' 'cold_start.retry_backoff_ms' must be a non-empty list of positive integers"
            )
        rb_clean: list[int] = []
        for item in rb_raw:
            if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
                raise error_cls(
                    f"Lakebase connection '{eng_name}' 'cold_start.retry_backoff_ms' entries must be positive integers"
                )
            rb_clean.append(item)

        cold_start_cfg = _FallbackLakebaseColdStartConfig(
            connect_timeout_seconds=cs_ct,
            wake=wake_clean,
            api_url=api_url_clean,
            api_token_env=api_tok_clean,
            max_query_wait_seconds=cs_mqw,
            max_wake_seconds=cs_mw,
            retry_backoff_ms=tuple(rb_clean),
        )

    fr_raw = raw.get("freshness")
    if fr_raw is None:
        freshness_cfg = PostgresFreshnessConfig(
            sync_state_table="public._lakebase_sync_state",
            freshness_sql=None,
        )
    else:
        if not isinstance(fr_raw, Mapping):
            raise error_cls(
                f"Lakebase connection '{eng_name}' 'freshness' must be a mapping"
            )
        unknown_fr = set(fr_raw.keys()) - {"sync_state_table", "freshness_sql"}
        if unknown_fr:
            raise error_cls(
                f"Unknown key(s) in lakebase.freshness for '{eng_name}': {sorted(str(k) for k in unknown_fr)}"
            )
        sst = fr_raw.get("sync_state_table", "public._lakebase_sync_state")
        if sst is not None:
            if not isinstance(sst, str) or not _QUALIFIED_TABLE_RE.fullmatch(sst.strip()):
                raise error_cls(
                    f"Lakebase connection '{eng_name}' 'freshness.sync_state_table' must be '<schema>.<table>'"
                )
            sst = sst.strip()
        fsql = fr_raw.get("freshness_sql")
        if fsql is not None:
            if not isinstance(fsql, str) or not fsql.strip():
                raise error_cls(
                    f"Lakebase connection '{eng_name}' 'freshness.freshness_sql' must be a non-empty string or null"
                )
            fsql = fsql.strip()
        freshness_cfg = PostgresFreshnessConfig(
            sync_state_table=sst,
            freshness_sql=fsql,
        )

    return _FallbackLakebaseEndpointConfig(
        project_id=ids["project_id"],
        branch_id=ids["branch_id"],
        endpoint_id=ids["endpoint_id"],
        endpoint_type=endpoint_type_clean,
        cu=cu_val,
        cold_start=cold_start_cfg,
        freshness=freshness_cfg,
    )


def _fallback_effective_storage_class(conn_cfg: Any) -> str:
    kind = getattr(conn_cfg, "kind", None)
    declared = getattr(conn_cfg, "storage_class", None)
    if kind == "postgres":
        return declared if declared in ("local", "object_storage") else "local"
    if kind == "lakebase":
        if getattr(conn_cfg, "lakebase", None) is not None:
            return "object_storage"
        if declared in ("local", "object_storage"):
            return declared
        return "unknown"
    if kind == "duckdb":
        profile = getattr(conn_cfg, "profile", "local_file")
        if profile == "iceberg_direct":
            return "object_storage"
        return declared if declared in ("local", "object_storage") else "local"
    if kind in ("starrocks", "cube"):
        return declared if declared in ("local", "object_storage") else "object_storage"
    return "unknown"


class _FallbackColdStartConnector:
    """Fallback cold-start connector delegating directly to connect_fn when S2 is absent."""

    def __init__(
        self,
        connect_fn: Callable[..., Any],
        cfg: Any,
        metrics: Any = None,
        http_client: Any = None,
        **kwargs: Any,
    ) -> None:
        self._connect_fn = connect_fn
        self._cfg = cfg
        self._metrics = metrics
        self._http_client = http_client

    def connect(self, deadline: float | None = None, **kwargs: Any) -> Any:
        return self._connect_fn(**kwargs)


def _fallback_snapshot_freshness(conn: Any, cfg: Any) -> float | None:
    ep_type = getattr(cfg, "endpoint_type", None)
    if ep_type is None:
        lb_sub = getattr(cfg, "lakebase", None)
        ep_type = getattr(lb_sub, "endpoint_type", None)
    if ep_type != "read_only":
        return None
    cur: Any = None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT EXTRACT(EPOCH FROM (clock_timestamp() - pg_catalog.pg_postmaster_start_time()))"
        )
        row = _fetch_one_row(cur)
        if isinstance(row, (tuple, list)) and row and row[0] is not None:
            return max(0.0, float(row[0]))
        return None
    finally:
        _close_quietly(cur)


def _make_fallback_engine_starting_error() -> type[Exception]:
    from seeknal.serving.adapters import EngineError

    class _FallbackEngineStartingError(EngineError):
        """Raised when a Lakebase endpoint is starting up and query wait budget expired."""

        code: str = "engine_starting"
        status_code: int = 503
        retry_after: int = 3

        def __init__(
            self,
            message: str = "The target database endpoint is starting up.",
            *,
            retry_after: int = 3,
        ) -> None:
            super().__init__(message)
            self.code = "engine_starting"
            self.status_code = 503
            self.retry_after = int(retry_after)

    _FallbackEngineStartingError.__name__ = "EngineStartingError"
    _FallbackEngineStartingError.__qualname__ = "EngineStartingError"
    return _FallbackEngineStartingError


try:
    import seeknal.serving.lakebase_endpoint as _lb_ep
    from seeknal.serving.lakebase_endpoint import (
        ColdStartConnector,
        EngineStartingError,
        effective_storage_class,
        parse_lakebase_block,
        role_guard_sql,
        snapshot_freshness,
    )

    LakebaseEndpointConfig = getattr(
        _lb_ep, "LakebaseEndpointConfig", _FallbackLakebaseEndpointConfig
    )
except ImportError:
    parse_lakebase_block = _fallback_parse_lakebase_block
    effective_storage_class = _fallback_effective_storage_class
    ColdStartConnector = _FallbackColdStartConnector
    EngineStartingError = _make_fallback_engine_starting_error()
    role_guard_sql = _fallback_role_guard_sql
    snapshot_freshness = _fallback_snapshot_freshness
    LakebaseEndpointConfig = _FallbackLakebaseEndpointConfig

__all__ = [
    "ColdStartConnector",
    "EngineStartingError",
    "LakebaseEndpointConfig",
    "POSTGRES_WIRE_KINDS",
    "PostgresAccountConfig",
    "PostgresFreshnessConfig",
    "PostgresSessionConfig",
    "PostgresTLSConfig",
    "VALID_POSTGRES_MODES",
    "VALID_SSLMODES",
    "begin_query_transaction",
    "build_connect_kwargs",
    "effective_storage_class",
    "is_postgres_wire",
    "parse_lakebase_block",
    "role_guard_sql",
    "snapshot_freshness",
    "verify_and_harden_session",
]
