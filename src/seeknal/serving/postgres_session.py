"""Shared PostgreSQL-wire session hardening, TLS, and account helpers."""

from __future__ import annotations

from collections.abc import Sequence
import logging
import math
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

UNFORCED_RLS_OWNER_SQL = (
    "SELECT n.nspname, c.relname "
    "FROM pg_catalog.pg_class AS c "
    "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
    "WHERE c.relkind IN ('r', 'p') "
    "AND c.relrowsecurity = TRUE "
    "AND c.relforcerowsecurity = FALSE "
    "AND n.nspname NOT LIKE 'pg_%' "
    "AND n.nspname <> 'information_schema' "
    "AND ("
    "pg_catalog.pg_has_role(current_user, c.relowner, 'USAGE') "
    "OR pg_catalog.pg_has_role(session_user, c.relowner, 'USAGE')"
    ") "
    "ORDER BY n.nspname, c.relname"
)

POSTGRES_WIRE_KINDS: frozenset[str] = frozenset({"postgres", "lakebase"})
VALID_POSTGRES_MODES: frozenset[str] = frozenset({"read_only", "read_write"})
VALID_SSLMODES: frozenset[str] = frozenset(
    {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
)

_SEARCH_PATH_ITEM_RE = re.compile(r'^(?:[A-Za-z_][A-Za-z0-9_$]*|"\$user"|\$user)$')
_PG_DURATION_RE = re.compile(
    r"^\s*([+-]?\d+(?:\.\d+)?)\s*(us|ms|s|min|h|d)?\s*$",
    re.IGNORECASE,
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
    allow_insecure_tls: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.sslmode, str) or self.sslmode not in VALID_SSLMODES:
            raise ValueError(
                f"'sslmode' must be one of {sorted(VALID_SSLMODES)}"
            )
        if not isinstance(self.allow_insecure_tls, bool):
            raise ValueError("'allow_insecure_tls' must be a boolean")
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
    elif kind == "postgres":
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


def _warn_unforced_rls_owned_tables(cur: Any) -> None:
    fetchall_fn = getattr(cur, "fetchall", None)
    if not callable(fetchall_fn):
        return
    try:
        cur.execute(UNFORCED_RLS_OWNER_SQL)
        rows = fetchall_fn()
    except Exception:
        return
    if not rows or not isinstance(rows, (list, tuple)):
        return
    safe_tables: list[str] = []
    for r in rows:
        if isinstance(r, (tuple, list)) and len(r) >= 2:
            nsp, rel = r[0], r[1]
            if isinstance(nsp, str) and isinstance(rel, str) and nsp and rel:
                safe_tables.append(f"{nsp}.{rel}")
    if safe_tables:
        logger.warning(
            "Serving role owns table(s) with ROW LEVEL SECURITY enabled without "
            "FORCE ROW LEVEL SECURITY (table owner bypasses unforced RLS): %s",
            ", ".join(safe_tables),
        )


def verify_and_harden_session(
    conn: Any,
    *,
    kind: str = "postgres",
    read_only: bool = False,
    session: PostgresSessionConfig | None = None,
    role_guard: bool = True,
    deny_roles: Sequence[str] | None = None,
    is_legacy_lakebase: bool = False,
) -> None:
    """Verify role guard and session hardening on a newly opened connection.

    Fails closed: on any error, privileged role, or ``SHOW`` mismatch, closes ``conn``
    and raises ``EngineError`` without leaking DSNs or driver secrets.
    """
    from seeknal.serving.adapters import EngineError, PostgresSessionHardeningError

    if conn is None:
        raise PostgresSessionHardeningError("Database connection is None")

    should_check_role = bool(role_guard or kind == "lakebase")
    if not should_check_role and session is None:
        return

    eff_deny_roles = DEFAULT_DENY_ROLES if deny_roles is None else deny_roles

    cur: Any = None
    ran_sets = False
    try:
        cur = conn.cursor()

        if should_check_role:
            cur.execute(role_guard_sql(kind, deny_roles=eff_deny_roles))
            row = _fetch_one_row(cur)
            if isinstance(row, (tuple, list)):
                if len(row) < 2:
                    raise PostgresSessionHardeningError("Role guard refused connection")
                for col_val in row:
                    if _pg_bool_is_true(col_val):
                        raise PostgresSessionHardeningError("Role guard refused connection")
                _warn_unforced_rls_owned_tables(cur)
            else:
                if not (is_legacy_lakebase and session is None):
                    raise PostgresSessionHardeningError("Role guard refused connection")

        if session is not None:
            sp_parts = _parse_search_path_parts(
                session.search_path, error_cls=PostgresSessionHardeningError
            )
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
                    raise PostgresSessionHardeningError(
                        "Session hardening SHOW verification failed"
                    )

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
                    raise PostgresSessionHardeningError(
                        "Session hardening SHOW verification failed"
                    )
                actual_ms = _parse_pg_duration_ms(guc_row[0])
                if actual_ms != expected_ms:
                    raise PostgresSessionHardeningError(
                        "Session hardening SHOW verification failed"
                    )

            cur.execute("SHOW search_path")
            sp_row = _fetch_one_row(cur)
            if (
                not isinstance(sp_row, (tuple, list))
                or not sp_row
                or not _search_path_matches(sp_row[0], session.search_path)
            ):
                raise PostgresSessionHardeningError(
                    "Session hardening SHOW verification failed"
                )

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
        raise PostgresSessionHardeningError(
            "Session hardening verification failed"
        ) from None


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
    """Open a per-query transaction with SET TRANSACTION READ ONLY, statement timeout, and parameterised application_name."""
    ms = max(1, int(timeout_ms))
    cursor.execute("BEGIN")
    if read_only or has_row_limit:
        cursor.execute("SET TRANSACTION READ ONLY")
    cursor.execute(f"SET LOCAL statement_timeout = {ms}")
    if hardened or read_only:
        cursor.execute(
            "SELECT set_config('application_name', %s, true)",
            (f"seeknal:{workload}:{audit_id}",),
        )


from seeknal.serving.lakebase_endpoint import (  # noqa: E402
    ColdStartConnector,
    DEFAULT_DENY_ROLES,
    EngineStartingError,
    LakebaseEndpointConfig,
    effective_storage_class,
    parse_lakebase_block,
    role_guard_sql,
    snapshot_freshness,
    validate_deny_roles,
)

__all__ = [
    "ColdStartConnector",
    "DEFAULT_DENY_ROLES",
    "EngineStartingError",
    "LakebaseEndpointConfig",
    "POSTGRES_WIRE_KINDS",
    "PostgresAccountConfig",
    "PostgresFreshnessConfig",
    "PostgresSessionConfig",
    "PostgresTLSConfig",
    "UNFORCED_RLS_OWNER_SQL",
    "VALID_POSTGRES_MODES",
    "VALID_SSLMODES",
    "begin_query_transaction",
    "build_connect_kwargs",
    "effective_storage_class",
    "is_postgres_wire",
    "parse_lakebase_block",
    "role_guard_sql",
    "snapshot_freshness",
    "validate_deny_roles",
    "verify_and_harden_session",
]
