"""Deterministic DB-API 2.0 fake for PostgreSQL / Lakebase catalog & session tests.

Zero sockets, zero real sleeps. Records all connect(**kwargs) and (sql, params)
invocations and supports scripting pg_catalog relations/columns/indexes, SHOW
settings, role_guard queries, _lakebase_sync_state rows, freshness_sql results,
pg_postmaster_start_time(), and scripted OperationalError connection failures.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


class Error(Exception):
    """Base DB-API 2.0 exception."""


class InterfaceError(Error):
    """DB-API 2.0 interface error."""


class DatabaseError(Error):
    """DB-API 2.0 database error."""


class OperationalError(DatabaseError):
    """DB-API 2.0 operational error (e.g., connection refused / endpoint suspended)."""


class ProgrammingError(DatabaseError):
    """DB-API 2.0 programming error (e.g., undefined table, read-only violation)."""


class FakeClock:
    """Deterministic wall + monotonic clock with zero real sleeps."""

    def __init__(
        self,
        initial_dt: datetime | None = None,
        *,
        initial_monotonic: float = 1000.0,
    ) -> None:
        if initial_dt is None:
            self._now = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)
        elif initial_dt.tzinfo is None:
            self._now = initial_dt.replace(tzinfo=timezone.utc)
        else:
            self._now = initial_dt.astimezone(timezone.utc)
        self._monotonic = float(initial_monotonic)
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        """Return deterministic UTC wall clock."""
        return self._now

    def monotonic(self) -> float:
        """Return deterministic monotonic seconds."""
        return self._monotonic

    def advance(self, seconds: float) -> None:
        """Advance both monotonic and wall time without sleeping."""
        delta = float(seconds)
        self._monotonic += delta
        self._now = self._now + timedelta(seconds=delta)

    def sleep(self, seconds: float) -> None:
        """Record requested sleep duration and advance clock with zero real sleep."""
        delta = max(0.0, float(seconds))
        self.sleeps.append(delta)
        self.advance(delta)


@dataclass(frozen=True)
class FakeRelation:
    """Scripted pg_catalog.pg_class + pg_namespace entry."""

    schema: str
    table: str
    relkind: str = "r"  # 'r' | 'p' | 'v' | 'm'
    reltuples: float = 0.0
    has_table_privilege: bool = True


@dataclass(frozen=True)
class FakeColumn:
    """Scripted pg_catalog.pg_attribute entry."""

    schema: str
    table: str
    name: str
    type_str: str = "text"
    not_null: bool = False
    has_column_privilege: bool = True
    attnum: int = 1
    is_dropped: bool = False


@dataclass(frozen=True)
class FakeIndex:
    """Scripted pg_catalog.pg_index + pg_am entry."""

    schema: str
    table: str
    index_name: str
    amname: str = "btree"  # 'btree' | 'hash' | 'brin' | ...
    columns: tuple[str, ...] = ()
    is_primary: bool = False
    is_unique: bool = False
    is_valid: bool = True
    is_ready: bool = True
    is_expression: bool = False
    is_partial: bool = False
    opclasses: tuple[str, ...] = ()
    collations: tuple[str, ...] = ()


@dataclass(frozen=True)
class FakeRoleInfo:
    """Scripted role attributes for pg_roles / pg_has_role checks."""

    rolname: str = "seeknal_role"
    rolsuper: bool = False
    rolbypassrls: bool = False
    neon_superuser: bool = False


@dataclass(frozen=True)
class FakeSyncStateRow:
    """Scripted row in a _lakebase_sync_state table."""

    target_table: str
    snapshot_id: int | None
    synced_at: datetime | str


_SHOW_RE = re.compile(r"^\s*SHOW\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*;?\s*$", re.IGNORECASE)
_SET_RE = re.compile(
    r"^\s*SET\s+(?:(LOCAL|SESSION)\s+)?([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|TO)\s*(.+?)\s*;?\s*$",
    re.IGNORECASE,
)
_SYNC_SELECT_RE = re.compile(
    r"^\s*SELECT\s+target_table\s*,\s*snapshot_id\s*,\s*synced_at\s+FROM\s+([a-zA-Z0-9_.]+)\s*;?\s*$",
    re.IGNORECASE,
)
_OPTIONS_C_RE = re.compile(r"-c\s+([a-zA-Z_][a-zA-Z0-9_]*)=([^\s]+)")


def _make_description(names: Sequence[str]) -> tuple[tuple[Any, ...], ...]:
    return tuple((name, None, None, None, None, None, None) for name in names)


class FakePostgresCatalog:
    """Scriptable in-memory PostgreSQL / Lakebase catalog and DB-API 2.0 factory."""

    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError

    def __init__(self, *, clock: FakeClock | None = None) -> None:
        self.clock = clock or FakeClock()
        self.relations: list[FakeRelation] = []
        self.columns: list[FakeColumn] = []
        self.indexes: list[FakeIndex] = []
        self.show_values: dict[str, str] = {
            "server_version_num": "160004",
            "default_transaction_read_only": "on",
            "statement_timeout": "30s",
            "lock_timeout": "2s",
            "idle_in_transaction_session_timeout": "10s",
            "search_path": "pg_catalog",
            "application_name": "seeknal-serving",
            "max_connections": "100",
        }
        self.apply_connect_options: bool = False
        self.role: FakeRoleInfo = FakeRoleInfo()
        self.session_role: FakeRoleInfo | None = None
        self.member_roles: list[FakeRoleInfo] = []
        self.unforced_rls_owned_tables: list[tuple[str, str]] = []
        self.sync_tables: dict[str, list[FakeSyncStateRow]] = {
            "public._lakebase_sync_state": [],
        }
        self.dropped_sync_tables: set[str] = set()
        self.freshness_results: dict[str, list[tuple[Any, ...]] | Exception] = {}
        self.default_freshness_rows: list[tuple[Any, ...]] | Exception | None = None
        self.postmaster_start_time: datetime = datetime(
            2026, 11, 19, 11, 0, 0, tzinfo=timezone.utc
        )
        self._connect_failures: list[Exception] = []
        self._custom_handlers: list[
            tuple[Callable[[str, Any], bool], Callable[[str, Any], tuple[Sequence[str], list[tuple[Any, ...]]]]]
        ] = []

        # Recorded telemetry
        self.connect_calls: list[dict[str, Any]] = []
        self.executed: list[tuple[str, Any]] = []
        self.connections: list[FakePostgresConnection] = []

    # ------------------------------------------------------------------
    # Scripting helpers
    # ------------------------------------------------------------------
    def add_relation(
        self,
        schema: str,
        table: str,
        *,
        relkind: str = "r",
        reltuples: float = 0.0,
        has_table_privilege: bool = True,
    ) -> FakeRelation:
        rel = FakeRelation(
            schema=schema,
            table=table,
            relkind=relkind,
            reltuples=float(reltuples),
            has_table_privilege=has_table_privilege,
        )
        self.relations.append(rel)
        return rel

    def add_unforced_rls_owned_table(self, schema: str, table: str) -> None:
        self.unforced_rls_owned_tables.append((schema, table))

    def add_column(
        self,
        schema: str,
        table: str,
        name: str,
        type_str: str = "text",
        *,
        not_null: bool = False,
        has_column_privilege: bool = True,
        attnum: int | None = None,
        is_dropped: bool = False,
    ) -> FakeColumn:
        if attnum is None:
            existing = [
                c.attnum for c in self.columns if c.schema == schema and c.table == table
            ]
            attnum = (max(existing) + 1) if existing else 1
        col = FakeColumn(
            schema=schema,
            table=table,
            name=name,
            type_str=type_str,
            not_null=not_null,
            has_column_privilege=has_column_privilege,
            attnum=attnum,
            is_dropped=is_dropped,
        )
        self.columns.append(col)
        return col

    def add_index(
        self,
        schema: str,
        table: str,
        index_name: str,
        *,
        amname: str = "btree",
        columns: Sequence[str] = (),
        is_primary: bool = False,
        is_unique: bool = False,
        is_valid: bool = True,
        is_ready: bool = True,
        is_expression: bool = False,
        is_partial: bool = False,
        opclasses: Sequence[str] = (),
        collations: Sequence[str] = (),
    ) -> FakeIndex:
        idx = FakeIndex(
            schema=schema,
            table=table,
            index_name=index_name,
            amname=amname,
            columns=tuple(columns),
            is_primary=is_primary,
            is_unique=is_primary or is_unique,
            is_valid=is_valid,
            is_ready=is_ready,
            is_expression=is_expression,
            is_partial=is_partial,
            opclasses=tuple(opclasses),
            collations=tuple(collations),
        )
        self.indexes.append(idx)
        return idx

    def add_table(
        self,
        schema: str,
        table: str,
        *,
        relkind: str = "r",
        reltuples: float = 100.0,
        has_table_privilege: bool = True,
        columns: Iterable[FakeColumn | tuple[Any, ...] | Mapping[str, Any]] = (),
        indexes: Iterable[FakeIndex | Mapping[str, Any]] = (),
    ) -> FakeRelation:
        """Convenience helper to script a table along with its columns and indexes."""
        rel = self.add_relation(
            schema,
            table,
            relkind=relkind,
            reltuples=reltuples,
            has_table_privilege=has_table_privilege,
        )
        for pos, col_spec in enumerate(columns, start=1):
            if isinstance(col_spec, FakeColumn):
                self.columns.append(col_spec)
            elif isinstance(col_spec, Mapping):
                self.add_column(
                    schema,
                    table,
                    col_spec["name"],
                    col_spec.get("type_str", "text"),
                    not_null=col_spec.get("not_null", False),
                    has_column_privilege=col_spec.get("has_column_privilege", True),
                    attnum=col_spec.get("attnum", pos),
                    is_dropped=col_spec.get("is_dropped", False),
                )
            elif isinstance(col_spec, tuple):
                # Supports (name,) | (name, type_str) | (name, type_str, not_null) | (name, type_str, not_null, has_priv)
                c_name = col_spec[0]
                c_type = col_spec[1] if len(col_spec) > 1 else "text"
                c_nn = bool(col_spec[2]) if len(col_spec) > 2 else False
                c_priv = bool(col_spec[3]) if len(col_spec) > 3 else True
                self.add_column(
                    schema,
                    table,
                    c_name,
                    c_type,
                    not_null=c_nn,
                    has_column_privilege=c_priv,
                    attnum=pos,
                )
            else:
                raise TypeError(f"Unsupported column spec: {col_spec!r}")

        for idx_spec in indexes:
            if isinstance(idx_spec, FakeIndex):
                self.indexes.append(idx_spec)
            elif isinstance(idx_spec, Mapping):
                self.add_index(
                    schema,
                    table,
                    idx_spec["index_name"],
                    amname=idx_spec.get("amname", "btree"),
                    columns=idx_spec.get("columns", ()),
                    is_primary=idx_spec.get("is_primary", False),
                    is_unique=idx_spec.get("is_unique", False),
                    is_valid=idx_spec.get("is_valid", True),
                    is_ready=idx_spec.get("is_ready", True),
                    is_expression=idx_spec.get("is_expression", False),
                    is_partial=idx_spec.get("is_partial", False),
                    opclasses=idx_spec.get("opclasses", ()),
                    collations=idx_spec.get("collations", ()),
                )
            else:
                raise TypeError(f"Unsupported index spec: {idx_spec!r}")
        return rel

    def set_show(self, name: str, value: str) -> None:
        self.show_values[name.lower()] = str(value)

    def set_role(
        self,
        *,
        rolname: str = "seeknal_role",
        rolsuper: bool = False,
        rolbypassrls: bool = False,
        neon_superuser: bool = False,
        session_role: FakeRoleInfo | None = None,
        member_roles: Sequence[FakeRoleInfo] = (),
    ) -> None:
        self.role = FakeRoleInfo(
            rolname=rolname,
            rolsuper=rolsuper,
            rolbypassrls=rolbypassrls,
            neon_superuser=neon_superuser,
        )
        self.session_role = session_role
        self.member_roles = list(member_roles)

    def add_sync_state(
        self,
        target_table: str,
        snapshot_id: int | None,
        synced_at: datetime | str,
        *,
        table_name: str = "public._lakebase_sync_state",
    ) -> None:
        key = table_name.lower()
        self.dropped_sync_tables.discard(key)
        rows = self.sync_tables.setdefault(key, [])
        rows = [r for r in rows if r.target_table != target_table]
        rows.append(
            FakeSyncStateRow(
                target_table=target_table,
                snapshot_id=snapshot_id,
                synced_at=synced_at,
            )
        )
        self.sync_tables[key] = rows

    def drop_sync_state_table(self, table_name: str = "public._lakebase_sync_state") -> None:
        key = table_name.lower()
        self.sync_tables.pop(key, None)
        self.dropped_sync_tables.add(key)

    def set_freshness_value(self, value: Any, *, sql: str | None = None) -> None:
        self.set_freshness_rows([(value,)], sql=sql)

    def set_freshness_rows(
        self,
        rows: Sequence[Sequence[Any]],
        *,
        sql: str | None = None,
    ) -> None:
        normalized = [tuple(r) for r in rows]
        if sql is None:
            self.default_freshness_rows = normalized
        else:
            self.freshness_results[sql.strip()] = normalized

    def set_freshness_error(
        self,
        exc: Exception,
        *,
        sql: str | None = None,
    ) -> None:
        if sql is None:
            self.default_freshness_rows = exc
        else:
            self.freshness_results[sql.strip()] = exc

    def set_postmaster_start_time(self, dt: datetime) -> None:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        self.postmaster_start_time = dt

    def fail_connects(
        self,
        count: int,
        error: Exception | None = None,
    ) -> None:
        """Script the next `count` connect() calls to fail with OperationalError."""
        for _ in range(count):
            self._connect_failures.append(
                error
                if error is not None
                else OperationalError("connection to server refused: endpoint is suspended")
            )

    def script_connect_failures(self, failures: Sequence[Exception]) -> None:
        self._connect_failures.extend(failures)

    def register_query(
        self,
        matcher: Callable[[str, Any], bool],
        handler: Callable[[str, Any], tuple[Sequence[str], list[tuple[Any, ...]]]],
    ) -> None:
        self._custom_handlers.append((matcher, handler))

    # ------------------------------------------------------------------
    # DB-API connect entry point
    # ------------------------------------------------------------------
    def connect(self, *args: Any, **kwargs: Any) -> FakePostgresConnection:
        call_record = dict(kwargs)
        if args:
            call_record["_args"] = args
        self.connect_calls.append(call_record)

        if self._connect_failures:
            exc = self._connect_failures.pop(0)
            raise exc

        conn = FakePostgresConnection(self, call_record)
        self.connections.append(conn)
        return conn


class FakePostgresConnection:
    """Deterministic in-memory DB-API 2.0 connection."""

    def __init__(
        self,
        catalog: FakePostgresCatalog,
        connect_kwargs: Mapping[str, Any],
    ) -> None:
        self.catalog = catalog
        self.connect_kwargs = dict(connect_kwargs)
        self.closed: bool = False
        self.autocommit: bool = False
        self.in_transaction: bool = False
        self.in_readonly_transaction: bool = False
        self._tx_has_query: bool = False
        self.rollback_count: int = 0
        self.commit_count: int = 0
        self.executed: list[tuple[str, Any]] = []
        self.session_values: dict[str, str] = dict(catalog.show_values)
        self._local_values: dict[str, str] = {}

        if catalog.apply_connect_options:
            if "application_name" in connect_kwargs and connect_kwargs["application_name"] is not None:
                self.session_values["application_name"] = str(connect_kwargs["application_name"])
            opts = connect_kwargs.get("options")
            if isinstance(opts, str):
                for m in _OPTIONS_C_RE.finditer(opts):
                    self.session_values[m.group(1).lower()] = m.group(2)

    def _check_open(self) -> None:
        if self.closed:
            raise InterfaceError("connection already closed")

    def cursor(self) -> FakePostgresCursor:
        self._check_open()
        return FakePostgresCursor(self)

    def commit(self) -> None:
        self._check_open()
        self.commit_count += 1
        self.in_transaction = False
        self.in_readonly_transaction = False
        self._tx_has_query = False
        self._local_values.clear()

    def rollback(self) -> None:
        self._check_open()
        self.rollback_count += 1
        self.in_transaction = False
        self.in_readonly_transaction = False
        self._tx_has_query = False
        self._local_values.clear()

    def close(self) -> None:
        self.closed = True
        self.in_transaction = False
        self.in_readonly_transaction = False
        self._tx_has_query = False
        self._local_values.clear()

    def __enter__(self) -> FakePostgresConnection:
        self._check_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is not None and not self.closed:
            self.rollback()
        elif not self.closed:
            self.commit()


class FakePostgresCursor:
    """Deterministic in-memory DB-API 2.0 cursor."""

    def __init__(self, connection: FakePostgresConnection) -> None:
        self.connection = connection
        self.closed: bool = False
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = -1
        self._rows: list[tuple[Any, ...]] = []
        self._pos: int = 0

    def _check_open(self) -> None:
        if self.closed:
            raise InterfaceError("cursor already closed")
        self.connection._check_open()

    def execute(self, sql: str, params: Any = None) -> None:
        self._check_open()
        self.connection.executed.append((sql, params))
        self.connection.catalog.executed.append((sql, params))
        self._rows = []
        self._pos = 0
        self.description = None
        self.rowcount = -1

        stripped = sql.strip()
        upper = stripped.rstrip(";").strip().upper()
        cat = self.connection.catalog

        # Model psycopg2's implicit BEGIN before any statement when autocommit is False:
        # an implicit transaction (inheriting only session default_transaction_read_only)
        # is already active before explicit BEGIN / BEGIN READ ONLY is evaluated.
        if not self.connection.autocommit and not self.connection.in_transaction:
            self.connection.in_transaction = True
            self.connection._tx_has_query = False
            self.connection.in_readonly_transaction = (
                self.connection.session_values.get(
                    "default_transaction_read_only", "off"
                ).lower()
                == "on"
            )

        # 0. Custom handlers
        for matcher, handler in cat._custom_handlers:
            if matcher(sql, params):
                self.connection._tx_has_query = True
                col_names, rows = handler(sql, params)
                self.description = _make_description(col_names)
                self._rows = list(rows)
                self.rowcount = len(self._rows)
                return

        # 1. Transaction control
        if upper in ("BEGIN", "START TRANSACTION"):
            if self.connection.autocommit and not self.connection.in_transaction:
                self.connection.in_transaction = True
                self.connection._tx_has_query = False
                self.connection.in_readonly_transaction = (
                    self.connection.session_values.get(
                        "default_transaction_read_only", "off"
                    ).lower()
                    == "on"
                )
            self.rowcount = 0
            return

        if upper in ("BEGIN READ ONLY", "START TRANSACTION READ ONLY"):
            # Under psycopg2 (autocommit=False), an implicit BEGIN was already sent,
            # so BEGIN READ ONLY is ignored (WARNING 25001) and does NOT make the tx read-only!
            if self.connection.autocommit and not self.connection.in_transaction:
                self.connection.in_transaction = True
                self.connection._tx_has_query = False
                self.connection.in_readonly_transaction = True
            self.rowcount = 0
            return

        if upper == "SET TRANSACTION READ ONLY":
            if self.connection._tx_has_query:
                raise ProgrammingError(
                    "SET TRANSACTION ISOLATION LEVEL / READ ONLY must be called before any query (SQLSTATE 25P01)"
                )
            self.connection.in_readonly_transaction = True
            self.rowcount = 0
            return

        if upper == "COMMIT":
            self.connection.commit()
            self.rowcount = 0
            return

        if upper == "ROLLBACK":
            self.connection.rollback()
            self.rowcount = 0
            return

        # 2. SHOW commands
        m_show = _SHOW_RE.match(stripped)
        if m_show:
            self.connection._tx_has_query = True
            guc = m_show.group(1).lower()
            if guc == "transaction_read_only":
                is_tx_ro = (
                    self.connection.in_readonly_transaction
                    or self.connection.session_values.get(
                        "default_transaction_read_only", "off"
                    ).lower()
                    == "on"
                )
                val = "on" if is_tx_ro else "off"
            else:
                val = self.connection._local_values.get(
                    guc,
                    self.connection.session_values.get(
                        guc, cat.show_values.get(guc, "")
                    ),
                )
            self.description = _make_description([guc])
            self._rows = [(val,)]
            self.rowcount = 1
            return

        # 3. SET commands & set_config(...)
        if "set_config(" in stripped.lower():
            self.connection._tx_has_query = True
            # e.g. SELECT pg_catalog.set_config('application_name', %s, true)
            if isinstance(params, (list, tuple)) and params:
                self.connection._local_values["application_name"] = str(params[0])
                val = str(params[0])
            else:
                val = ""
            self.description = _make_description(["set_config"])
            self._rows = [(val,)]
            self.rowcount = 1
            return

        m_set = _SET_RE.match(stripped)
        if m_set:
            scope = (m_set.group(1) or "SESSION").upper()
            guc = m_set.group(2).lower()
            raw_val = m_set.group(3).strip().strip("'").strip('"')
            if scope == "LOCAL":
                self.connection._local_values[guc] = raw_val
            else:
                self.connection.session_values[guc] = raw_val
            self.rowcount = 0
            return

        self.connection._tx_has_query = True

        # Enforce read-only rejection on mutating statements
        is_ro = (
            self.connection.in_readonly_transaction
            or self.connection.session_values.get("default_transaction_read_only", "off").lower()
            == "on"
        )
        if is_ro and re.match(
            r"^\s*(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE)\b",
            stripped,
            re.IGNORECASE,
        ):
            raise ProgrammingError(
                "cannot execute write statement in a read-only transaction (SQLSTATE 25006)"
            )

        # 4. Role guard & unforced-RLS ownership queries
        lower_sql = stripped.lower()
        if "relrowsecurity" in lower_sql:
            self.description = _make_description(["nspname", "relname"])
            self._rows = list(sorted(cat.unforced_rls_owned_tables))
            self.rowcount = len(self._rows)
            return

        if "pg_roles" in lower_sql or "rolsuper" in lower_sql or "pg_has_role" in lower_sql:
            candidate_roles: list[FakeRoleInfo] = [cat.role]
            if "session_user" in lower_sql and cat.session_role is not None:
                candidate_roles.append(cat.session_role)
            if "r.oid" in lower_sql and "member" in lower_sql:
                candidate_roles.extend(cat.member_roles)

            eff_super = any(r.rolsuper for r in candidate_roles)
            eff_bypass = any(r.rolbypassrls for r in candidate_roles)
            eff_neon = any(r.neon_superuser for r in candidate_roles)
            has_super = "rolsuper" in lower_sql
            has_bypass = "rolbypassrls" in lower_sql
            in_match = re.search(r"r\.rolname\s+in\s*\(([^)]*)\)", stripped, re.IGNORECASE)
            cols: list[str] = []
            vals: list[Any] = []
            if has_super:
                cols.append("rolsuper")
                vals.append(eff_super)
            if has_bypass:
                cols.append("rolbypassrls")
                vals.append(eff_bypass)
            if in_match is not None:
                denied_set = {
                    m.group(1)
                    for m in re.finditer(r"'([^']+)'", in_match.group(1))
                }
                eff_denied = any(r.rolname in denied_set for r in candidate_roles) or (
                    "neon_superuser" in denied_set and eff_neon
                )
                cols.append("is_denied_role")
                vals.append(eff_denied)
            elif "neon_superuser" in lower_sql:
                cols.append("neon_superuser")
                vals.append(eff_neon)
            if not cols:
                cols = ["rolname", "rolsuper", "rolbypassrls"]
                vals = [cat.role.rolname, eff_super, eff_bypass]
            self.description = _make_description(cols)
            self._rows = [tuple(vals)]
            self.rowcount = 1
            return

        # 5. pg_postmaster_start_time()
        if "pg_postmaster_start_time()" in lower_sql:
            self.description = _make_description(["pg_postmaster_start_time"])
            self._rows = [(cat.postmaster_start_time,)]
            self.rowcount = 1
            return

        # 6. pg_catalog introspection queries (parameterised by schema list)
        if "pg_index" in lower_sql:
            allowed_schemas = self._extract_schema_param(params)
            rows: list[tuple[Any, ...]] = []
            for idx in sorted(
                cat.indexes,
                key=lambda x: (x.schema, x.table, x.index_name),
            ):
                if allowed_schemas is not None and idx.schema not in allowed_schemas:
                    continue
                rel = self._find_relation(idx.schema, idx.table)
                if rel is not None and rel.relkind not in ("r", "p", "v", "m"):
                    continue
                if not idx.is_valid or not idx.is_ready:
                    continue
                rows.append(
                    (
                        idx.schema,
                        idx.table,
                        idx.index_name,
                        idx.amname,
                        list(idx.columns),
                        idx.is_primary,
                        idx.is_unique,
                        idx.is_expression,
                        idx.is_partial,
                        list(idx.opclasses),
                        list(idx.collations),
                    )
                )
            self.description = _make_description(
                [
                    "schema_name",
                    "table_name",
                    "index_name",
                    "amname",
                    "key_columns",
                    "is_primary",
                    "is_unique",
                    "is_expression",
                    "is_partial",
                    "opclasses",
                    "collations",
                ]
            )
            self._rows = rows
            self.rowcount = len(rows)
            return

        if "pg_attribute" in lower_sql:
            allowed_schemas = self._extract_schema_param(params)
            rows = []
            for col in sorted(
                cat.columns,
                key=lambda x: (x.schema, x.table, x.attnum),
            ):
                if allowed_schemas is not None and col.schema not in allowed_schemas:
                    continue
                rel = self._find_relation(col.schema, col.table)
                if rel is not None and rel.relkind not in ("r", "p", "v", "m"):
                    continue
                if col.attnum <= 0 or col.is_dropped or not col.has_column_privilege:
                    continue
                rows.append(
                    (
                        col.schema,
                        col.table,
                        col.name,
                        col.type_str,
                        col.not_null,
                        col.attnum,
                    )
                )
            self.description = _make_description(
                [
                    "schema_name",
                    "table_name",
                    "column_name",
                    "data_type",
                    "not_null",
                    "attnum",
                ]
            )
            self._rows = rows
            self.rowcount = len(rows)
            return

        if "pg_class" in lower_sql and "pg_namespace" in lower_sql:
            allowed_schemas = self._extract_schema_param(params)
            rows = []
            for rel in sorted(cat.relations, key=lambda x: (x.schema, x.table)):
                if allowed_schemas is not None and rel.schema not in allowed_schemas:
                    continue
                if rel.relkind not in ("r", "p", "v", "m"):
                    continue
                if not rel.has_table_privilege:
                    continue
                rows.append(
                    (
                        rel.schema,
                        rel.table,
                        rel.relkind,
                        float(rel.reltuples),
                        rel.has_table_privilege,
                    )
                )
            self.description = _make_description(
                ["schema_name", "table_name", "relkind", "reltuples", "has_select"]
            )
            self._rows = rows
            self.rowcount = len(rows)
            return

        # 7. _lakebase_sync_state queries (qualified and unqualified)
        m_sync = _SYNC_SELECT_RE.match(stripped)
        if m_sync:
            tbl_ref = m_sync.group(1).lower()
            if tbl_ref in cat.dropped_sync_tables:
                raise ProgrammingError(f'relation "{tbl_ref}" does not exist')
            sync_rows: list[FakeSyncStateRow] | None = None
            if tbl_ref in cat.sync_tables:
                sync_rows = cat.sync_tables[tbl_ref]
            elif "." not in tbl_ref and f"public.{tbl_ref}" in cat.sync_tables:
                if f"public.{tbl_ref}" in cat.dropped_sync_tables:
                    raise ProgrammingError(f'relation "{tbl_ref}" does not exist')
                sync_rows = cat.sync_tables[f"public.{tbl_ref}"]
            elif tbl_ref == "public._lakebase_sync_state" and "_lakebase_sync_state" in cat.sync_tables:
                sync_rows = cat.sync_tables["_lakebase_sync_state"]

            if sync_rows is None:
                raise ProgrammingError(f'relation "{tbl_ref}" does not exist')

            self.description = _make_description(
                ["target_table", "snapshot_id", "synced_at"]
            )
            self._rows = [
                (r.target_table, r.snapshot_id, r.synced_at) for r in sync_rows
            ]
            self.rowcount = len(self._rows)
            return

        # 8. SELECT 1 readiness probe
        if upper == "SELECT 1":
            self.description = _make_description(["?column?"])
            self._rows = [(1,)]
            self.rowcount = 1
            return

        # 9. freshness_sql result lookup
        if stripped in cat.freshness_results:
            outcome = cat.freshness_results[stripped]
            if isinstance(outcome, Exception):
                raise outcome
            cols = [f"col_{i}" for i in range(len(outcome[0]))] if outcome else ["value"]
            self.description = _make_description(cols)
            self._rows = list(outcome)
            self.rowcount = len(self._rows)
            return

        if cat.default_freshness_rows is not None:
            outcome = cat.default_freshness_rows
            if isinstance(outcome, Exception):
                raise outcome
            cols = [f"col_{i}" for i in range(len(outcome[0]))] if outcome else ["value"]
            self.description = _make_description(cols)
            self._rows = list(outcome)
            self.rowcount = len(self._rows)
            return

        raise ProgrammingError(f"Unscripted SQL in FakePostgresCatalog: {stripped}")

    def _extract_schema_param(self, params: Any) -> set[str] | None:
        if params is None:
            return None
        if isinstance(params, (list, tuple)) and len(params) == 1:
            inner = params[0]
            if isinstance(inner, (list, tuple, set, frozenset)):
                return {str(s) for s in inner}
        if isinstance(params, Mapping):
            for val in params.values():
                if isinstance(val, (list, tuple, set, frozenset)):
                    return {str(s) for s in val}
        return None

    def _find_relation(self, schema: str, table: str) -> FakeRelation | None:
        for rel in self.connection.catalog.relations:
            if rel.schema == schema and rel.table == table:
                return rel
        return None

    def fetchone(self) -> tuple[Any, ...] | None:
        self._check_open()
        if self.description is None:
            raise ProgrammingError("no results to fetch")
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        self._check_open()
        if self.description is None:
            raise ProgrammingError("no results to fetch")
        end = min(len(self._rows), self._pos + max(0, int(size)))
        batch = self._rows[self._pos : end]
        self._pos = end
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        self._check_open()
        if self.description is None:
            raise ProgrammingError("no results to fetch")
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakePostgresCursor:
        self._check_open()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()
