"""Canonical typed schema contract for the PostgreSQL online feature store.

This module is the single source of truth for the shape of an online feature
table. Seeknal is the exclusive writer; the catalog is a read-only consumer.
The two systems couple through this contract, not through a code dependency.

Why this module exists
----------------------
An earlier design derived PostgreSQL column types from ``Entity.join_keys``,
which carries **names only** and therefore cannot produce a typed DDL. It also
left ``ARRAY``/``MAP`` unmapped, and assumed ``CREATE OR REPLACE VIEW`` could
cut over incompatible schema changes -- PostgreSQL forbids that (it may only
append columns, never change existing name/order/type).

This module fixes all three: types are explicit, every logical type has a
declared PostgreSQL mapping, and compatible vs incompatible evolution are
separated (see :meth:`OnlineTableDescriptor.compatible_with`).

Write-direction typing is deliberately NOT the inverse of
``seeknal.connections.postgresql.POSTGRESQL_TO_DUCKDB_TYPES``. That map is the
*read* direction and is lossy on purpose -- it collapses ``JSON``/``JSONB``/
``UUID`` to ``VARCHAR`` so DuckDB can consume them. Inheriting that lossiness on
write would silently degrade stored features, so the mappings below are
independent and preserve native PostgreSQL types.
"""

from __future__ import annotations

import hashlib
import json
import zlib
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from seeknal.validation import validate_column_name, validate_schema_name

# PostgreSQL hard limit on identifier length.
PG_MAX_IDENTIFIER_LENGTH = 63

# Contract version. Bump when the *shape* of the contract changes (columns
# added/removed/retyped here), independent of any feature group's own schema.
WRITER_CONTRACT_VERSION = "1"


class OnlineContractError(ValueError):
    """Raised when a descriptor violates the online-store contract."""


# ---------------------------------------------------------------------------
# Logical -> PostgreSQL type mapping (WRITE direction)
# ---------------------------------------------------------------------------

#: Canonical write-direction mapping. Keys are upper-cased DuckDB/logical type
#: names; values are the PostgreSQL types actually emitted in DDL.
LOGICAL_TO_POSTGRES_TYPES: dict[str, str] = {
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "TINYINT": "SMALLINT",
    "SMALLINT": "SMALLINT",
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
    "BIGINT": "BIGINT",
    "HUGEINT": "NUMERIC",
    "FLOAT": "REAL",
    "REAL": "REAL",
    "DOUBLE": "DOUBLE PRECISION",
    "DECIMAL": "NUMERIC",
    "NUMERIC": "NUMERIC",
    "VARCHAR": "TEXT",
    "TEXT": "TEXT",
    "STRING": "TEXT",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP WITHOUT TIME ZONE",
    "TIMESTAMPTZ": "TIMESTAMPTZ",
    "BLOB": "BYTEA",
    # Preserved natively rather than degraded to TEXT (unlike the read map).
    "UUID": "UUID",
    "JSON": "JSONB",
    "JSONB": "JSONB",
    # Nested types: STRUCT/MAP have no faithful relational form, so they are
    # stored as JSONB. ARRAY is handled separately because element type matters.
    "STRUCT": "JSONB",
    "MAP": "JSONB",
}

#: Logical types that may serve as an entity join key. Deliberately narrow:
#: floating point, nested, and unbounded-precision types make unreliable
#: primary keys and unreliable equality semantics for point lookups.
VALID_KEY_TYPES: frozenset[str] = frozenset(
    {"BOOLEAN", "SMALLINT", "INTEGER", "BIGINT", "TEXT", "UUID", "DATE"}
)


def resolve_pg_type(logical_type: str) -> str:
    """Map a logical/DuckDB type name to the PostgreSQL type emitted in DDL.

    ``ARRAY(<inner>)`` and ``<inner>[]`` map to a native PostgreSQL array of the
    mapped element type, so arrays are not flattened into JSON.

    Args:
        logical_type: Logical type name, e.g. ``"BIGINT"``, ``"ARRAY(DOUBLE)"``.

    Returns:
        The PostgreSQL type string.

    Raises:
        OnlineContractError: If the type has no declared mapping. Unmapped types
            fail loudly rather than defaulting to ``TEXT``; a silent fallback is
            how type drift enters a contract.
    """
    if not logical_type or not logical_type.strip():
        raise OnlineContractError("logical type must be a non-empty string")

    raw = logical_type.strip()
    upper = raw.upper()

    # ARRAY(inner) / inner[]
    inner: str | None = None
    if upper.startswith("ARRAY(") and upper.endswith(")"):
        inner = raw[len("ARRAY(") : -1]
    elif upper.endswith("[]"):
        inner = raw[:-2]
    if inner is not None:
        if not inner.strip():
            raise OnlineContractError(
                f"array type {logical_type!r} must declare an element type"
            )
        return f"{resolve_pg_type(inner)}[]"

    # DECIMAL(p,s) / NUMERIC(p,s) keep their precision.
    base = upper.split("(", 1)[0].strip()
    if base in {"DECIMAL", "NUMERIC"} and "(" in upper:
        return f"NUMERIC{upper[upper.index('('):]}"

    try:
        return LOGICAL_TO_POSTGRES_TYPES[base]
    except KeyError:
        raise OnlineContractError(
            f"no PostgreSQL mapping declared for logical type {logical_type!r}. "
            "Add an explicit mapping to LOGICAL_TO_POSTGRES_TYPES rather than "
            "relying on a fallback."
        ) from None


def safe_identifier(*parts: str) -> str:
    """Join *parts* into a PostgreSQL-safe identifier within the 63-char limit.

    Mirrors the CRC32 truncation scheme already used for feature column names in
    ``seeknal.workflow.consolidation.materializer`` so identifier shortening
    behaves consistently across the codebase.
    """
    full = "__".join(p for p in parts if p)
    if len(full) <= PG_MAX_IDENTIFIER_LENGTH:
        return full
    suffix = format(zlib.crc32(full.encode()) & 0xFFFFFFFF, "08x")
    budget = PG_MAX_IDENTIFIER_LENGTH - len(suffix) - 1
    return f"{full[:budget]}_{suffix}"


# ---------------------------------------------------------------------------
# Column + metadata specification
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnSpec:
    """One column in an online feature table."""

    name: str
    logical_type: str
    nullable: bool = True

    def __post_init__(self) -> None:
        validate_column_name(self.name)
        resolve_pg_type(self.logical_type)  # fail fast on unmapped types

    @property
    def pg_type(self) -> str:
        return resolve_pg_type(self.logical_type)

    def ddl_fragment(self) -> str:
        null_clause = "" if self.nullable else " NOT NULL"
        return f'"{self.name}" {self.pg_type}{null_clause}'

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "logical_type": self.logical_type,
            "pg_type": self.pg_type,
            "nullable": self.nullable,
        }


#: Metadata columns every online table carries. Names are reserved: a feature
#: may not use them.
#:
#: ``computed_at`` and ``source_interval_end`` are deliberately distinct. A row
#: recomputed today over a window that ended last week is fresh by one measure
#: and stale by the other, and only the second is meaningful to a consumer
#: deciding whether to trust a feature value.
METADATA_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("computed_at", "TIMESTAMPTZ", nullable=False),
    ColumnSpec("source_interval_start", "TIMESTAMPTZ", nullable=False),
    ColumnSpec("source_interval_end", "TIMESTAMPTZ", nullable=False),
    ColumnSpec("definition_sha", "TEXT", nullable=False),
    ColumnSpec("schema_sha", "TEXT", nullable=False),
    ColumnSpec("publish_run_id", "TEXT", nullable=False),
    # Retirement (plan task 0.6). NULL => live. Non-NULL => intentionally
    # retired. This is what makes "never serve a hole" satisfiable: a
    # transient absence is forbidden, whereas an intentional retirement is
    # represented explicitly and can be surfaced as retired rather than
    # missing. Rows are NOT physically deleted on retirement.
    ColumnSpec("retired_at", "TIMESTAMPTZ", nullable=True),
)

RESERVED_COLUMN_NAMES: frozenset[str] = frozenset(c.name for c in METADATA_COLUMNS)


# ---------------------------------------------------------------------------
# Table descriptor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OnlineTableDescriptor:
    """Canonical description of one published online feature table.

    Attributes:
        project: Owning project name.
        feature_group: Feature group name.
        version: Monotonic schema version. Incompatible changes increment it.
        entity_keys: Composite primary key columns, in order.
        features: Feature value columns.
        schema: PostgreSQL schema holding online tables.
    """

    project: str
    feature_group: str
    version: int
    entity_keys: tuple[ColumnSpec, ...]
    features: tuple[ColumnSpec, ...]
    schema: str = "feature_online"
    _cached_sha: dict[str, str] = field(
        default_factory=dict, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        validate_schema_name(self.schema)
        if self.version < 1:
            raise OnlineContractError("version must be >= 1")
        if not self.entity_keys:
            raise OnlineContractError(
                "at least one entity key is required: a latest-only online table "
                "is keyed by entity, one row per key"
            )
        if not self.features:
            raise OnlineContractError("at least one feature column is required")

        for key in self.entity_keys:
            if key.nullable:
                raise OnlineContractError(
                    f"entity key {key.name!r} must be NOT NULL (it is part of the "
                    "primary key)"
                )
            base = key.logical_type.upper().split("(", 1)[0]
            resolved = LOGICAL_TO_POSTGRES_TYPES.get(base)
            if base not in VALID_KEY_TYPES and resolved not in {
                LOGICAL_TO_POSTGRES_TYPES[k] for k in VALID_KEY_TYPES
            }:
                raise OnlineContractError(
                    f"entity key {key.name!r} has type {key.logical_type!r}, which is "
                    f"not a valid key type. Allowed: {sorted(VALID_KEY_TYPES)}"
                )

        seen: set[str] = set()
        for col in (*self.entity_keys, *self.features):
            if col.name in RESERVED_COLUMN_NAMES:
                raise OnlineContractError(
                    f"column {col.name!r} collides with a reserved metadata column: "
                    f"{sorted(RESERVED_COLUMN_NAMES)}"
                )
            if col.name in seen:
                raise OnlineContractError(f"duplicate column name {col.name!r}")
            seen.add(col.name)

    # -- naming ------------------------------------------------------------

    @property
    def base_name(self) -> str:
        """Version-independent table name. Readers bind to this."""
        return safe_identifier(f"fg_{self.project}", self.feature_group)

    @property
    def physical_name(self) -> str:
        """Versioned physical table that actually stores rows."""
        return safe_identifier(
            f"fg_{self.project}", self.feature_group, f"v{self.version}"
        )

    @property
    def view_fqn(self) -> str:
        """Stable read target. The catalog binds here and never to a version."""
        return f"{self.schema}.{self.base_name}"

    @property
    def physical_fqn(self) -> str:
        return f"{self.schema}.{self.physical_name}"

    def staging_name(self, publish_run_id: str) -> str:
        """Unique, immutable staging table name for one publication.

        Staging names are **never** reused. Reusing a name that was previously
        dropped out-of-band (via ``postgres_execute``) and then recreated through
        the attached catalog leaves stale catalog metadata, and the subsequent
        ``CREATE TABLE ... AS SELECT`` silently writes **zero rows with no
        error**. Deriving the name from ``publish_run_id`` makes that class of
        bug unreachable by construction.
        """
        if not publish_run_id or not publish_run_id.strip():
            raise OnlineContractError("publish_run_id is required for staging names")
        return safe_identifier(
            "stg", self.base_name, publish_run_id.strip().replace("-", "")[:16]
        )

    def staging_fqn(self, publish_run_id: str) -> str:
        return f"{self.schema}.{self.staging_name(publish_run_id)}"

    # -- columns -----------------------------------------------------------

    @property
    def all_columns(self) -> tuple[ColumnSpec, ...]:
        return (*self.entity_keys, *self.features, *METADATA_COLUMNS)

    @property
    def served_column_names(self) -> tuple[str, ...]:
        """Columns an upsert must update. Omitting any yields a logically mixed
        old/new row even though PostgreSQL row visibility remains atomic."""
        return tuple(c.name for c in (*self.features, *METADATA_COLUMNS))

    # -- hashing -----------------------------------------------------------

    @property
    def schema_sha(self) -> str:
        """Stable hash of the table shape, independent of feature logic.

        Lets a reader detect shape drift without knowing anything about how the
        features were computed.
        """
        if "v" not in self._cached_sha:
            canonical = json.dumps(
                {
                    "contract": WRITER_CONTRACT_VERSION,
                    "schema": self.schema,
                    "base_name": self.base_name,
                    "version": self.version,
                    "entity_keys": [c.to_dict() for c in self.entity_keys],
                    "features": [c.to_dict() for c in self.features],
                    "metadata": [c.to_dict() for c in METADATA_COLUMNS],
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            self._cached_sha["v"] = hashlib.sha256(canonical.encode()).hexdigest()
        return self._cached_sha["v"]

    # -- evolution ---------------------------------------------------------

    def compatible_with(self, other: "OnlineTableDescriptor") -> bool:
        """Whether *self* is a backward-compatible successor of *other*.

        Compatible means the change can be served by replacing the stable view
        in place. PostgreSQL's ``CREATE OR REPLACE VIEW`` may only **append**
        columns -- it cannot change an existing column's name, order, or type --
        so compatibility requires every prior column to survive unchanged and in
        the same order, with any additions being nullable and appended.

        Anything else is incompatible and requires a new version table plus a
        pointer swap.
        """
        if (self.schema, self.base_name) != (other.schema, other.base_name):
            return False
        if [c.to_dict() for c in self.entity_keys] != [
            c.to_dict() for c in other.entity_keys
        ]:
            return False  # key changes are never compatible
        if len(self.features) < len(other.features):
            return False  # removal is never compatible
        for old, new in zip(other.features, self.features):
            if old.to_dict() != new.to_dict():
                return False  # retype/reorder/rename is never compatible
        return all(c.nullable for c in self.features[len(other.features) :])

    # -- DDL ---------------------------------------------------------------

    def create_table_ddl(self) -> str:
        """DDL for the versioned physical table, with a typed composite PK.

        The primary key is explicit. ``CREATE TABLE ... AS SELECT`` creates no
        key and no indexes, which would make both point-lookup latency and
        ``ON CONFLICT`` unavailable.
        """
        cols = ",\n  ".join(c.ddl_fragment() for c in self.all_columns)
        pk = ", ".join(f'"{c.name}"' for c in self.entity_keys)
        return (
            f'CREATE TABLE IF NOT EXISTS {self.physical_fqn} (\n  {cols},\n'
            f"  CONSTRAINT {safe_identifier(self.physical_name, 'pk')} "
            f"PRIMARY KEY ({pk})\n)"
        )

    def create_view_ddl(self) -> str:
        """DDL pointing the stable read target at the current version.

        Columns are listed explicitly: ``SELECT *`` in a serving path silently
        changes shape when the underlying table changes.
        """
        cols = ", ".join(f'"{c.name}"' for c in self.all_columns)
        return (
            f"CREATE OR REPLACE VIEW {self.view_fqn} AS "
            f"SELECT {cols} FROM {self.physical_fqn}"
        )

    def live_view_ddl(self) -> str:
        """Stable read target exposing only non-retired rows.

        Retired rows remain physically present and queryable via
        :meth:`view_fqn`; this projection is what a latest-only point lookup
        should read.
        """
        cols = ", ".join(f'"{c.name}"' for c in self.all_columns)
        return (
            f"CREATE OR REPLACE VIEW {self.view_fqn}__live AS "
            f"SELECT {cols} FROM {self.physical_fqn} WHERE \"retired_at\" IS NULL"
        )

    def upsert_sql(self, source_fqn: str) -> str:
        """Remote-only upsert from *source_fqn* into the physical table.

        Updates **every** served column. References no local relation, so it is
        safe to execute server-side.
        """
        cols = ", ".join(f'"{c.name}"' for c in self.all_columns)
        keys = ", ".join(f'"{c.name}"' for c in self.entity_keys)
        updates = ", ".join(
            f'"{name}" = EXCLUDED."{name}"' for name in self.served_column_names
        )
        return (
            f"INSERT INTO {self.physical_fqn} ({cols}) "
            f"SELECT {cols} FROM {source_fqn} "
            f"ON CONFLICT ({keys}) DO UPDATE SET {updates}"
        )

    # -- serialization -----------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": WRITER_CONTRACT_VERSION,
            "project": self.project,
            "feature_group": self.feature_group,
            "version": self.version,
            "schema": self.schema,
            "base_name": self.base_name,
            "physical_name": self.physical_name,
            "entity_keys": [c.to_dict() for c in self.entity_keys],
            "features": [c.to_dict() for c in self.features],
            "metadata_columns": [c.to_dict() for c in METADATA_COLUMNS],
            "schema_sha": self.schema_sha,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "OnlineTableDescriptor":
        def cols(items: Iterable[Mapping[str, Any]]) -> tuple[ColumnSpec, ...]:
            return tuple(
                ColumnSpec(
                    name=i["name"],
                    logical_type=i["logical_type"],
                    nullable=i.get("nullable", True),
                )
                for i in items
            )

        return cls(
            project=data["project"],
            feature_group=data["feature_group"],
            version=int(data["version"]),
            entity_keys=cols(data["entity_keys"]),
            features=cols(data["features"]),
            schema=data.get("schema", "feature_online"),
        )


__all__ = [
    "PG_MAX_IDENTIFIER_LENGTH",
    "WRITER_CONTRACT_VERSION",
    "OnlineContractError",
    "LOGICAL_TO_POSTGRES_TYPES",
    "VALID_KEY_TYPES",
    "METADATA_COLUMNS",
    "RESERVED_COLUMN_NAMES",
    "ColumnSpec",
    "OnlineTableDescriptor",
    "resolve_pg_type",
    "safe_identifier",
]
