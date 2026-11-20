"""pg_catalog introspection, access-metadata derivation, and freshness cascade
for Postgres and Lakebase serving sources.
"""

from __future__ import annotations

import fnmatch
import re
import sqlite3
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import sqlglot
from sqlglot import exp

from seeknal.serving.discovery.config import (
    DiscoveryConfigError,
    DiscoveryError,
    PostgresSourceConfig,
    _safe_disc_label,
)
from seeknal.serving.discovery.lakebase import LakebaseSyncSource, SyncState

_PG_IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}\Z")
_PATTERN_OPS = frozenset(
    {"text_pattern_ops", "varchar_pattern_ops", "bpchar_pattern_ops"}
)
_FORBIDDEN_FRESHNESS_NODES: tuple[type[exp.Expression], ...] = tuple(
    cls
    for _name in (
        "Insert",
        "Update",
        "Delete",
        "Merge",
        "Create",
        "Drop",
        "Alter",
        "TruncateTable",
        "Command",
        "Set",
        "Transaction",
        "Commit",
        "Rollback",
        "Copy",
        "Into",
        "Lock",
    )
    if (cls := getattr(exp, _name, None)) is not None
)


def _validate_freshness_sql(freshness_sql: str) -> None:
    """Validate that freshness_sql parses as a single read-only SELECT statement."""
    if not isinstance(freshness_sql, str) or not freshness_sql.strip():
        raise DiscoveryError("freshness_sql must be a single SELECT statement")
    try:
        statements = sqlglot.parse(freshness_sql, read="postgres")
    except Exception as exc:
        raise DiscoveryError(
            "freshness_sql must be a single SELECT statement"
        ) from exc
    if (
        len(statements) != 1
        or statements[0] is None
        or not isinstance(statements[0], exp.Select)
    ):
        raise DiscoveryError("freshness_sql must be a single SELECT statement")
    stmt = statements[0]
    if _FORBIDDEN_FRESHNESS_NODES and any(
        isinstance(node, _FORBIDDEN_FRESHNESS_NODES) for node in stmt.walk()
    ):
        raise DiscoveryError("freshness_sql must be a single SELECT statement")


def _run_freshness_sql(
    conn: Any,
    cursor: Any,
    freshness_sql: str,
    *,
    engine_label: str,
) -> list[tuple[Any, ...]]:
    """Validate and execute freshness_sql inside BEGIN + SET TRANSACTION READ ONLY + rollback."""
    _validate_freshness_sql(freshness_sql)
    try:
        if hasattr(conn, "rollback") and callable(conn.rollback):
            try:
                conn.rollback()
            except Exception:
                pass
        cursor.execute("BEGIN")
        try:
            cursor.execute("SET TRANSACTION READ ONLY")
            cursor.execute(freshness_sql)
            return list(cursor.fetchall())
        finally:
            if hasattr(conn, "rollback") and callable(conn.rollback):
                try:
                    conn.rollback()
                except Exception:
                    pass
            else:
                try:
                    cursor.execute("ROLLBACK")
                except Exception:
                    pass
    except DiscoveryError:
        raise
    except Exception as exc:
        raise DiscoveryError(
            f"freshness_sql failed on source '{engine_label}': "
            f"{type(exc).__name__}"
        ) from exc

RELATIONS_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    c.relkind::text AS relkind,
    c.reltuples::float8 AS reltuples,
    pg_catalog.has_table_privilege(c.oid, 'SELECT') AS has_select
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = ANY(%s)
  AND c.relkind IN ('r', 'p', 'v', 'm')
  AND (
      pg_catalog.has_table_privilege(c.oid, 'SELECT')
      OR pg_catalog.has_any_column_privilege(c.oid, 'SELECT')
  )
ORDER BY n.nspname, c.relname
""".strip()

COLUMNS_SQL = """
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    a.attnotnull AS not_null,
    a.attnum AS attnum
FROM pg_catalog.pg_attribute AS a
JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = ANY(%s)
  AND c.relkind IN ('r', 'p', 'v', 'm')
  AND a.attnum > 0
  AND NOT a.attisdropped
  AND pg_catalog.has_column_privilege(c.oid, a.attnum, 'SELECT')
ORDER BY n.nspname, c.relname, a.attnum
""".strip()

INDEXES_SQL = """
SELECT
    n.nspname AS schema_name,
    t.relname AS table_name,
    ix.relname AS index_name,
    am.amname AS amname,
    ARRAY(
        SELECT a.attname
        FROM pg_catalog.unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
        LEFT JOIN pg_catalog.pg_attribute AS a
          ON a.attrelid = t.oid AND a.attnum = k.attnum
        WHERE k.ord <= i.indnkeyatts
        ORDER BY k.ord
    ) AS key_columns,
    i.indisprimary AS is_primary,
    i.indisunique AS is_unique,
    (
        i.indexprs IS NOT NULL
        OR EXISTS (
            SELECT 1
            FROM pg_catalog.unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
            WHERE k.ord <= i.indnkeyatts AND k.attnum = 0
        )
    ) AS is_expression,
    (i.indpred IS NOT NULL) AS is_partial,
    ARRAY(
        SELECT opc.opcname
        FROM pg_catalog.unnest(i.indclass::oid[]) WITH ORDINALITY AS c_oid(opcoid, ord)
        LEFT JOIN pg_catalog.pg_opclass AS opc ON opc.oid = c_oid.opcoid
        WHERE c_oid.ord <= i.indnkeyatts
        ORDER BY c_oid.ord
    ) AS opclasses,
    ARRAY(
        SELECT COALESCE(col.collname, '')
        FROM pg_catalog.unnest(i.indcollation::oid[]) WITH ORDINALITY AS col_oid(colloid, ord)
        LEFT JOIN pg_catalog.pg_collation AS col ON col.oid = col_oid.colloid
        WHERE col_oid.ord <= i.indnkeyatts
        ORDER BY col_oid.ord
    ) AS collations
FROM pg_catalog.pg_index AS i
JOIN pg_catalog.pg_class AS t ON t.oid = i.indrelid
JOIN pg_catalog.pg_namespace AS n ON n.oid = t.relnamespace
JOIN pg_catalog.pg_class AS ix ON ix.oid = i.indexrelid
JOIN pg_catalog.pg_am AS am ON am.oid = ix.relam
WHERE n.nspname = ANY(%s)
  AND t.relkind IN ('r', 'p', 'v', 'm')
  AND i.indisvalid
  AND i.indisready
ORDER BY n.nspname, t.relname, ix.relname
""".strip()


@dataclass(frozen=True)
class PostgresColumnInfo:
    """Introspected PostgreSQL column metadata."""

    name: str
    data_type: str
    not_null: bool
    attnum: int


@dataclass(frozen=True)
class PostgresIndexInfo:
    """Introspected PostgreSQL valid & ready index metadata."""

    name: str
    amname: str
    columns: tuple[str, ...]
    is_primary: bool
    is_unique: bool
    is_expression: bool
    is_partial: bool
    opclasses: tuple[str, ...] = ()
    collations: tuple[str, ...] = ()


@dataclass(frozen=True)
class PostgresIntrospectedTable:
    """Discovered Postgres/Lakebase table with derived + overridden access & freshness."""

    logical_name: str
    physical_name: str
    physical_schema: str
    logical_schema: str
    table_name: str
    relkind: str
    row_count: int | None
    columns: tuple[str, ...]
    column_details: tuple[PostgresColumnInfo, ...]
    indexes: tuple[PostgresIndexInfo, ...]
    keys: tuple[str, ...]
    access: dict[str, Any]
    pii: bool
    tokenized: bool
    max_lag_seconds: float | None
    sync_state: SyncState | None = None
    freshness_source: str | None = None


@dataclass(frozen=True)
class PostgresIntrospectionResult:
    """Full pg_catalog introspection result for a single Postgres-wire source."""

    engine: str
    engine_kind: str
    server_version_num: int
    default_transaction_read_only: str
    tables: dict[str, PostgresIntrospectedTable]
    schemas: dict[str, list[str]]
    warnings: tuple[str, ...]


def _parse_temporal_marker(value: Any, *, context: str) -> datetime:
    """Convert a datetime, date, or ISO-8601 date/timestamp string to UTC datetime."""
    if value is None or isinstance(value, bool):
        raise DiscoveryError(f"{context} returned invalid value: {type(value).__name__}")
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    elif isinstance(value, str):
        s = value.strip()
        if not s:
            raise DiscoveryError(f"{context} returned an empty string")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            try:
                d = date.fromisoformat(s)
                dt = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
            except ValueError as exc:
                raise DiscoveryError(
                    f"{context} returned non-timestamp value"
                ) from exc
    else:
        raise DiscoveryError(
            f"{context} must return a timestamp or date, got {type(value).__name__}"
        )

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dedup_ordered(items: Collection[str], order_hint: Mapping[str, int]) -> list[str]:
    """Deduplicate preserving explicit priority first, then column attnum order."""
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _derive_index_sets(
    columns: tuple[PostgresColumnInfo, ...],
    indexes: tuple[PostgresIndexInfo, ...],
) -> tuple[list[str], dict[str, list[str]]]:
    """Derive keys and index-backed access column lists per §3."""
    exposed_set = {c.name for c in columns}
    not_null_map = {c.name: c.not_null for c in columns}
    col_order = {c.name: idx for idx, c in enumerate(columns)}

    eligible = [
        ix
        for ix in indexes
        if not ix.is_partial
        and not ix.is_expression
        and len(ix.columns) >= 1
        and ix.columns[0] is not None
    ]

    # 1. keys: PK first, else narrowest unique non-partial non-expression index with all NOT NULL exposed cols
    derived_keys: list[str] = []
    for ix in eligible:
        if ix.is_primary and all(c in exposed_set for c in ix.columns):
            derived_keys = list(ix.columns)
            break

    if not derived_keys:
        unique_candidates = [
            ix
            for ix in eligible
            if ix.is_unique
            and all(c in exposed_set and not_null_map.get(c, False) for c in ix.columns)
        ]
        if unique_candidates:
            unique_candidates.sort(key=lambda ix: (len(ix.columns), ix.name))
            derived_keys = list(unique_candidates[0].columns)

    # 2. filterable: leading column of valid non-partial non-expression btree or hash index + PK/keys
    filterable_raw: list[str] = list(derived_keys)
    leading_btree_or_hash: list[str] = []
    for ix in eligible:
        if ix.amname in ("btree", "hash"):
            lead = ix.columns[0]
            if lead in exposed_set:
                leading_btree_or_hash.append(lead)
    leading_btree_or_hash.sort(key=lambda c: col_order.get(c, 999_999))
    filterable_raw.extend(leading_btree_or_hash)
    filterable = _dedup_ordered(filterable_raw, col_order)

    # 3. range_filterable: leading column of valid non-partial non-expression btree index
    range_raw: list[str] = []
    for ix in eligible:
        if ix.amname == "btree":
            lead = ix.columns[0]
            if lead in exposed_set:
                range_raw.append(lead)
    range_raw.sort(key=lambda c: col_order.get(c, 999_999))
    range_filterable = _dedup_ordered(range_raw, col_order)

    # 4. prefix_filterable: leading btree column with *_pattern_ops or COLLATE "C"
    prefix_raw: list[str] = []
    for ix in eligible:
        if ix.amname == "btree":
            lead = ix.columns[0]
            if lead not in exposed_set:
                continue
            opc0 = ix.opclasses[0] if ix.opclasses else ""
            coll0 = ix.collations[0] if ix.collations else ""
            coll0_clean = str(coll0).strip().strip('"').strip("'")
            if opc0 in _PATTERN_OPS or coll0_clean == "C":
                prefix_raw.append(lead)
    prefix_raw.sort(key=lambda c: col_order.get(c, 999_999))
    prefix_filterable = _dedup_ordered(prefix_raw, col_order)

    # 5. sortable: NOT NULL columns forming a leading prefix of any valid non-partial non-expression btree index + PK/keys
    sortable_raw: list[str] = list(derived_keys)
    btree_prefix_cols: list[str] = []
    for ix in eligible:
        if ix.amname != "btree":
            continue
        for col in ix.columns:
            if col in exposed_set and not_null_map.get(col, False):
                btree_prefix_cols.append(col)
            else:
                break
    btree_prefix_cols.sort(key=lambda c: col_order.get(c, 999_999))
    sortable_raw.extend(btree_prefix_cols)
    sortable = _dedup_ordered(sortable_raw, col_order)

    return derived_keys, {
        "filterable": filterable,
        "range_filterable": range_filterable,
        "prefix_filterable": prefix_filterable,
        "sortable": sortable,
    }


def _is_missing_default_sync_table_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    markers = (
        "does not exist",
        "no such table",
        "undefinedtable",
        "permission denied",
        "insufficientprivilege",
    )
    return any(m in msg for m in markers)


def introspect_postgres_source(
    connect_fn: Callable[[], Any],
    source_cfg: PostgresSourceConfig,
    *,
    engine_kind: str,
    read_only: bool = False,
    endpoint_type: str | None = None,
    require_filter_min_rows: int = 100_000,
    now: datetime | None = None,
    refresh_interval_seconds: float = 60.0,
    iceberg_snapshot_ids: Mapping[str, int | None] | None = None,
    extra_pii_tables: Collection[str] = (),
) -> PostgresIntrospectionResult:
    """Introspect pg_catalog for an allowlisted set of schemas, derive access
    metadata, validate overrides, and compute table freshness lag.
    """
    if engine_kind not in ("postgres", "lakebase"):
        raise DiscoveryConfigError(
            f"Unsupported engine_kind '{engine_kind}' for introspect_postgres_source"
        )

    if now is None:
        now_dt = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now_dt = now.replace(tzinfo=timezone.utc)
    else:
        now_dt = now.astimezone(timezone.utc)

    is_read_only = bool(read_only or endpoint_type == "read_only")
    allowlisted_schemas = list(source_cfg.schemas.keys())
    extra_pii_lower = {t.lower() for t in extra_pii_tables}
    iceberg_snaps_lower = (
        {k.lower(): v for k, v in iceberg_snapshot_ids.items()}
        if iceberg_snapshot_ids is not None
        else {}
    )

    warnings: list[str] = []

    try:
        conn = connect_fn()
    except DiscoveryError:
        raise
    except Exception as exc:
        raise DiscoveryError(
            f"Failed to connect to Postgres source '{_safe_disc_label(source_cfg.engine)}': "
            f"{type(exc).__name__}"
        ) from exc

    try:
        cursor = conn.cursor()
        try:
            # 1. Server facts (SHOW server_version_num & SHOW default_transaction_read_only)
            cursor.execute("SHOW server_version_num")
            ver_row = cursor.fetchone()
            try:
                server_version_num = int(str(ver_row[0]).strip()) if ver_row else 0
            except (TypeError, ValueError):
                server_version_num = 0

            cursor.execute("SHOW default_transaction_read_only")
            ro_row = cursor.fetchone()
            ro_setting = str(ro_row[0]).strip().lower() if ro_row else "off"
            if is_read_only and ro_setting != "on":
                raise DiscoveryError(
                    "Read-only source must have default_transaction_read_only = on"
                )

            # 2. Relations
            cursor.execute(RELATIONS_SQL, (allowlisted_schemas,))
            rel_rows = cursor.fetchall()

            # 3. Columns
            cursor.execute(COLUMNS_SQL, (allowlisted_schemas,))
            col_rows = cursor.fetchall()

            # 4. Indexes
            cursor.execute(INDEXES_SQL, (allowlisted_schemas,))
            idx_rows = cursor.fetchall()

            # 5. Freshness markers
            if source_cfg.freshness is not None:
                sync_state_table = source_cfg.freshness.sync_state_table
                sync_state_explicit = source_cfg.freshness.sync_state_explicit
                freshness_sql = source_cfg.freshness.freshness_sql
            else:
                sync_state_table = (
                    "public._lakebase_sync_state"
                    if engine_kind == "lakebase"
                    else None
                )
                sync_state_explicit = False
                freshness_sql = None

            sync_states: dict[str, SyncState] = {}
            if sync_state_table is not None:
                lb_src = LakebaseSyncSource(
                    lambda: conn,
                    sync_state_table=sync_state_table,
                )
                try:
                    sync_states = lb_src.read_state()
                except DiscoveryError as exc:
                    if not sync_state_explicit and _is_missing_default_sync_table_error(
                        exc
                    ):
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        sync_states = {}
                    else:
                        raise

            freshness_sql_lag: float | None = None
            if freshness_sql is not None:
                fr_rows = _run_freshness_sql(
                    conn,
                    cursor,
                    freshness_sql,
                    engine_label=_safe_disc_label(source_cfg.engine),
                )

                if not fr_rows or len(fr_rows) != 1 or len(fr_rows[0]) != 1:
                    raise DiscoveryError(
                        f"freshness_sql on source '{_safe_disc_label(source_cfg.engine)}' "
                        "must return exactly 1 row and 1 column"
                    )
                if fr_rows[0][0] is None:
                    raise DiscoveryError(
                        f"freshness_sql on source '{_safe_disc_label(source_cfg.engine)}' returned NULL"
                    )
                marker_dt = _parse_temporal_marker(
                    fr_rows[0][0],
                    context=f"freshness_sql on '{_safe_disc_label(source_cfg.engine)}'",
                )
                freshness_sql_lag = (
                    max(0.0, (now_dt - marker_dt).total_seconds())
                    + refresh_interval_seconds
                )

            # Group columns and indexes by (schema, table)
            cols_by_rel: dict[tuple[str, str], list[PostgresColumnInfo]] = {}
            for c_row in col_rows:
                c_schema = str(c_row[0])
                c_table = str(c_row[1])
                c_name = str(c_row[2])
                c_type = str(c_row[3])
                c_not_null = bool(c_row[4])
                c_attnum = int(c_row[5])
                if not _PG_IDENT_RE.match(c_name):
                    continue
                cols_by_rel.setdefault((c_schema, c_table), []).append(
                    PostgresColumnInfo(
                        name=c_name,
                        data_type=c_type,
                        not_null=c_not_null,
                        attnum=c_attnum,
                    )
                )

            idxs_by_rel: dict[tuple[str, str], list[PostgresIndexInfo]] = {}
            for i_row in idx_rows:
                i_schema = str(i_row[0])
                i_table = str(i_row[1])
                i_name = str(i_row[2])
                i_am = str(i_row[3]).lower()
                raw_cols = i_row[4] or ()
                i_cols = tuple(
                    str(c) if c is not None else "" for c in raw_cols
                )
                i_primary = bool(i_row[5])
                i_unique = bool(i_row[6])
                i_expr = bool(i_row[7]) or any(c == "" for c in i_cols)
                i_part = bool(i_row[8])
                i_opcs = tuple(str(o) for o in (i_row[9] or ()))
                i_colls = tuple(str(co) for co in (i_row[10] or ()))
                idxs_by_rel.setdefault((i_schema, i_table), []).append(
                    PostgresIndexInfo(
                        name=i_name,
                        amname=i_am,
                        columns=i_cols,
                        is_primary=i_primary,
                        is_unique=i_unique,
                        is_expression=i_expr,
                        is_partial=i_part,
                        opclasses=i_opcs,
                        collations=i_colls,
                    )
                )

            # Filter relations by schema allowlist, identifier regex, include/exclude globs
            candidate_rels: list[tuple[str, str, str, int | None]] = []
            for r_row in rel_rows:
                phys_schema = str(r_row[0])
                tbl_name = str(r_row[1])
                relkind = str(r_row[2])
                raw_tuples = float(r_row[3]) if r_row[3] is not None else -1.0
                has_sel = bool(r_row[4]) if len(r_row) > 4 else True

                if phys_schema not in source_cfg.schemas:
                    continue
                if relkind not in ("r", "p", "v", "m"):
                    continue
                if not has_sel and (phys_schema, tbl_name) not in cols_by_rel:
                    continue
                if not _PG_IDENT_RE.match(phys_schema) or not _PG_IDENT_RE.match(tbl_name):
                    continue

                sch_cfg = source_cfg.schemas[phys_schema]
                if not any(
                    fnmatch.fnmatchcase(tbl_name, pat) for pat in sch_cfg.include
                ):
                    continue
                if any(fnmatch.fnmatchcase(tbl_name, pat) for pat in sch_cfg.exclude):
                    continue

                exposed_cols = cols_by_rel.get((phys_schema, tbl_name), [])
                if not exposed_cols:
                    continue

                if relkind == "v" or raw_tuples < 0:
                    row_count: int | None = None
                else:
                    row_count = int(raw_tuples)

                candidate_rels.append((phys_schema, tbl_name, relkind, row_count))

            # Step 3 of freshness cascade: read-only Lakebase snapshot age via pg_postmaster_start_time()
            postmaster_lag: float | None = None
            needs_postmaster = (
                engine_kind == "lakebase"
                and endpoint_type == "read_only"
                and freshness_sql_lag is None
                and any(
                    f"{ps}.{tn}" not in sync_states
                    for ps, tn, _rk, _rc in candidate_rels
                )
            )
            if needs_postmaster:
                try:
                    cursor.execute("SELECT pg_catalog.pg_postmaster_start_time()")
                    pm_row = cursor.fetchone()
                except Exception as exc:
                    raise DiscoveryError(
                        f"Failed to read pg_postmaster_start_time() on '{_safe_disc_label(source_cfg.engine)}': "
                        f"{type(exc).__name__}"
                    ) from exc
                if not pm_row or pm_row[0] is None:
                    raise DiscoveryError(
                        f"pg_postmaster_start_time() returned NULL on '{_safe_disc_label(source_cfg.engine)}'"
                    )
                pm_dt = _parse_temporal_marker(
                    pm_row[0],
                    context=f"pg_postmaster_start_time() on '{_safe_disc_label(source_cfg.engine)}'",
                )
                postmaster_lag = (
                    max(0.0, (now_dt - pm_dt).total_seconds())
                    + refresh_interval_seconds
                )

        finally:
            cursor.close()
    finally:
        if not isinstance(conn, sqlite3.Connection):
            try:
                conn.close()
            except Exception:
                pass

    # Build introspected table entries, apply overrides, and check unmatched overrides
    tables_out: dict[str, PostgresIntrospectedTable] = {}
    schemas_out: dict[str, list[str]] = {}
    seen_logical_lower: dict[str, str] = {}

    for phys_schema, tbl_name, relkind, row_count in candidate_rels:
        sch_cfg = source_cfg.schemas[phys_schema]
        log_schema = source_cfg.schema_map.get(phys_schema, phys_schema)
        physical_name = f"{phys_schema}.{tbl_name}"
        logical_name = f"{source_cfg.logical_catalog}.{log_schema}.{tbl_name}"
        logical_lower = logical_name.lower()

        if logical_lower in seen_logical_lower:
            raise DiscoveryError(
                f"Logical table collision on '{logical_name}' in postgres_sources.{_safe_disc_label(source_cfg.engine)}"
            )
        seen_logical_lower[logical_lower] = logical_name

        col_infos = tuple(
            sorted(
                cols_by_rel[(phys_schema, tbl_name)],
                key=lambda c: c.attnum,
            )
        )
        col_names = tuple(c.name for c in col_infos)
        exposed_set = set(col_names)
        not_null_map = {c.name: c.not_null for c in col_infos}
        idx_infos = tuple(idxs_by_rel.get((phys_schema, tbl_name), ()))

        derived_keys, derived_sets = _derive_index_sets(col_infos, idx_infos)

        override = source_cfg.tables.get(logical_name)

        # Effective PII and tokenized flags
        is_pii = sch_cfg.pii or (logical_lower in extra_pii_lower)
        if override is not None and override.pii is not None:
            is_pii = override.pii

        is_tokenized = sch_cfg.tokenized
        if override is not None and override.tokenized is not None:
            is_tokenized = override.tokenized

        ov_access = override.access if override is not None else None

        eff_keys = (
            list(ov_access["keys"])
            if ov_access is not None and "keys" in ov_access
            else list(derived_keys)
        )
        eff_filterable = (
            list(ov_access["filterable"])
            if ov_access is not None and "filterable" in ov_access
            else list(derived_sets["filterable"])
        )
        eff_range = (
            list(ov_access["range_filterable"])
            if ov_access is not None and "range_filterable" in ov_access
            else list(derived_sets["range_filterable"])
        )
        eff_prefix = (
            list(ov_access["prefix_filterable"])
            if ov_access is not None and "prefix_filterable" in ov_access
            else list(derived_sets["prefix_filterable"])
        )
        eff_sortable = (
            list(ov_access["sortable"])
            if ov_access is not None and "sortable" in ov_access
            else list(derived_sets["sortable"])
        )

        # Derived pattern
        if is_pii:
            derived_pattern = "point" if eff_keys else "filter"
        elif (
            relkind != "v"
            and row_count is not None
            and row_count < require_filter_min_rows
        ):
            derived_pattern = "scan"
        else:
            derived_pattern = "filter"

        eff_pattern = (
            str(ov_access["pattern"])
            if ov_access is not None and "pattern" in ov_access
            else derived_pattern
        )

        derived_require_filter = eff_pattern in ("point", "filter")
        eff_require_filter = (
            bool(ov_access["require_filter"])
            if ov_access is not None and "require_filter" in ov_access
            else derived_require_filter
        )

        derived_max_rows: int | None = 50 if is_pii else None
        eff_max_rows = (
            ov_access["max_rows"]
            if ov_access is not None and "max_rows" in ov_access
            else derived_max_rows
        )

        # Validate every column in override/effective access lists exists in exposed columns
        for field_name, col_list in (
            ("keys", eff_keys),
            ("filterable", eff_filterable),
            ("range_filterable", eff_range),
            ("prefix_filterable", eff_prefix),
            ("sortable", eff_sortable),
        ):
            for col_ref in col_list:
                if col_ref not in exposed_set:
                    raise DiscoveryError(
                        f"Table '{logical_name}' access.{field_name} references unknown or unprivileged column '{col_ref}'"
                    )

        if eff_pattern == "point" and not eff_keys:
            raise DiscoveryError(
                f"Table '{logical_name}' has pattern 'point' but empty 'keys'"
            )

        for sort_col in eff_sortable:
            if not not_null_map.get(sort_col, False):
                raise DiscoveryError(
                    f"Table '{logical_name}' access.sortable column '{sort_col}' must be NOT NULL"
                )

        # Warn on declared columns with no supporting index
        if ov_access is not None:
            for field_name in (
                "filterable",
                "range_filterable",
                "prefix_filterable",
                "sortable",
            ):
                if field_name in ov_access:
                    derived_field_set = set(derived_sets[field_name])
                    for col_ref in ov_access[field_name]:
                        if col_ref not in derived_field_set:
                            warnings.append(
                                f"Table '{logical_name}' override access.{field_name} column '{col_ref}' "
                                "is not backed by a valid non-partial non-expression index"
                            )

        access_dict: dict[str, Any] = {
            "pattern": eff_pattern,
            "keys": eff_keys,
            "filterable": eff_filterable,
            "range_filterable": eff_range,
            "prefix_filterable": eff_prefix,
            "sortable": eff_sortable,
            "require_filter": eff_require_filter,
            "max_rows": eff_max_rows,
        }

        # Freshness cascade per table
        tbl_sync_state = sync_states.get(physical_name)
        max_lag: float | None = None
        freshness_src: str | None = None

        if tbl_sync_state is not None:
            freshness_src = "sync_state"
            iceberg_snap = iceberg_snaps_lower.get(logical_lower)
            if (
                tbl_sync_state.snapshot_id is not None
                and iceberg_snap is not None
                and tbl_sync_state.snapshot_id == iceberg_snap
            ):
                obs_lag = 0.0
            else:
                obs_lag = max(0.0, (now_dt - tbl_sync_state.synced_at).total_seconds())
            max_lag = obs_lag + refresh_interval_seconds
        elif freshness_sql_lag is not None:
            freshness_src = "freshness_sql"
            max_lag = freshness_sql_lag
        elif postmaster_lag is not None:
            freshness_src = "postmaster_start_time"
            max_lag = postmaster_lag

        tables_out[logical_name] = PostgresIntrospectedTable(
            logical_name=logical_name,
            physical_name=physical_name,
            physical_schema=phys_schema,
            logical_schema=log_schema,
            table_name=tbl_name,
            relkind=relkind,
            row_count=row_count,
            columns=col_names,
            column_details=col_infos,
            indexes=idx_infos,
            keys=tuple(eff_keys),
            access=access_dict,
            pii=is_pii,
            tokenized=is_tokenized,
            max_lag_seconds=max_lag,
            sync_state=tbl_sync_state,
            freshness_source=freshness_src,
        )
        schemas_out[logical_name] = list(col_names)

    # Check that every table override matched an introspected table
    unmatched_overrides = [
        ov_key for ov_key in source_cfg.tables if ov_key not in tables_out
    ]
    if unmatched_overrides:
        fmt_unmatched = ", ".join(
            f"'{_safe_disc_label(u)}'" for u in unmatched_overrides
        )
        raise DiscoveryError(
            f"Configured table override(s) in postgres_sources.{_safe_disc_label(source_cfg.engine)} "
            f"not found in introspected tables: {fmt_unmatched}"
        )

    return PostgresIntrospectionResult(
        engine=source_cfg.engine,
        engine_kind=engine_kind,
        server_version_num=server_version_num,
        default_transaction_read_only=ro_setting,
        tables=tables_out,
        schemas=schemas_out,
        warnings=tuple(warnings),
    )
