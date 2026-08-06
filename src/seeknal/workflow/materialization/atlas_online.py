"""Atomic Seeknal publication into Atlas' PostgreSQL online serving schema."""

from __future__ import annotations

import json
import re
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any, Callable

from seeknal.workflow.materialization.atlas_online_config import (
    AtlasOnlineMaterializationConfig,
)

ATLAS_ONLINE_SCHEMA = "feature_online"
ATLAS_METADATA_COLUMNS = (
    "computed_at",
    "source_interval_start",
    "source_interval_end",
    "definition_sha",
    "schema_sha",
    "publish_run_id",
    "retired_at",
)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class AtlasOnlineMaterializationError(RuntimeError):
    """Raised when an Atlas online publication cannot be completed atomically."""


@dataclass(frozen=True)
class AtlasOnlineWriteResult:
    success: bool
    row_count: int
    duration_seconds: float
    table: str
    revision: str
    definition_sha: str
    schema_sha: str
    publish_run_id: str
    physical_table: str
    source_interval_start: Any | None
    source_interval_end: Any | None

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        for key in ("source_interval_start", "source_interval_end"):
            timestamp = value[key]
            if hasattr(timestamp, "isoformat"):
                value[key] = timestamp.isoformat()
        return value


def _duckdb_relation(value: str) -> str:
    parts = value.split(".")
    if not parts or any(not _IDENTIFIER_RE.fullmatch(part) for part in parts):
        raise AtlasOnlineMaterializationError("invalid DuckDB source relation")
    return ".".join(f'"{part}"' for part in parts)


def _postgres_type(duckdb_type: str) -> str:
    normalized = duckdb_type.upper()
    if normalized.startswith(("VARCHAR", "CHAR", "TEXT", "UUID", "JSON")):
        return "TEXT"
    if normalized.startswith(("TINYINT", "SMALLINT")):
        return "SMALLINT"
    if normalized.startswith(("INTEGER", "INT", "UINTEGER", "USMALLINT")):
        return "INTEGER"
    if normalized.startswith(("BIGINT", "UBIGINT", "HUGEINT")):
        return "BIGINT"
    if normalized.startswith(("DECIMAL", "NUMERIC")):
        return normalized
    if normalized.startswith(("DOUBLE", "FLOAT", "REAL")):
        return "DOUBLE PRECISION"
    if normalized.startswith("BOOLEAN"):
        return "BOOLEAN"
    if normalized.startswith("TIMESTAMP"):
        return (
            "TIMESTAMPTZ"
            if "WITH TIME ZONE" in normalized or normalized == "TIMESTAMPTZ"
            else "TIMESTAMP"
        )
    if normalized.startswith("DATE"):
        return "DATE"
    if normalized.startswith(("BLOB", "BYTEA")):
        return "BYTEA"
    raise AtlasOnlineMaterializationError(
        f"unsupported DuckDB type for Atlas online serving: {duckdb_type}"
    )


class AtlasOnlineMaterializer:
    """Publish a complete Feature Group generation using a transactional swap."""

    def __init__(
        self,
        pg_config: Any,
        config: AtlasOnlineMaterializationConfig,
        *,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        self.pg_config = pg_config
        self.config = config
        self._connect = connect

    def _connect_postgres(self) -> Any:
        if self._connect is None:
            import psycopg2

            connect = psycopg2.connect
        else:
            connect = self._connect
        return connect(
            host=self.pg_config.host,
            port=self.pg_config.port,
            dbname=self.pg_config.database,
            user=self.pg_config.user,
            password=self.pg_config.password,
            sslmode=self.pg_config.sslmode,
            connect_timeout=self.pg_config.connect_timeout,
        )

    def materialize(self, con: Any, view_name: str) -> AtlasOnlineWriteResult:
        """Copy a DuckDB relation and atomically replace ``<table>__live``."""

        from psycopg2 import sql
        from psycopg2.extras import execute_values

        started = time.monotonic()
        relation = _duckdb_relation(view_name)
        described = con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
        source_columns = [str(row[0]) for row in described]
        if not source_columns:
            raise AtlasOnlineMaterializationError("online source has no columns")
        if len(set(source_columns)) != len(source_columns):
            raise AtlasOnlineMaterializationError("online source has duplicate columns")
        invalid = [
            name for name in source_columns if not _IDENTIFIER_RE.fullmatch(name)
        ]
        if invalid:
            raise AtlasOnlineMaterializationError(
                f"invalid online source column: {invalid[0]!r}"
            )
        reserved = sorted(set(source_columns).intersection(ATLAS_METADATA_COLUMNS))
        if reserved:
            raise AtlasOnlineMaterializationError(
                f"online source uses reserved metadata column: {reserved[0]}"
            )
        missing_keys = [
            key for key in self.config.entity_keys if key not in source_columns
        ]
        if missing_keys:
            raise AtlasOnlineMaterializationError(
                f"online source is missing entity key: {missing_keys[0]}"
            )
        if (
            self.config.event_time_column is not None
            and self.config.event_time_column not in source_columns
        ):
            raise AtlasOnlineMaterializationError(
                "online source is missing configured event_time_column"
            )

        column_types = [_postgres_type(str(row[1])) for row in described]
        source_interval_start = None
        source_interval_end = None
        if self.config.event_time_column:
            event_column = f'"{self.config.event_time_column}"'
            interval = con.execute(
                f"SELECT MIN({event_column}), MAX({event_column}) FROM {relation}"
            ).fetchone()
            if interval:
                source_interval_start, source_interval_end = interval

        staging = f"{self.config.table}__staging_{uuid.uuid4().hex[:12]}"
        live = f"{self.config.table}__live"
        previous = f"{self.config.table}__previous"
        published_at = datetime.now(UTC)
        row_count = 0

        postgres = self._connect_postgres()
        try:
            postgres.autocommit = False
            cursor = postgres.cursor()
            cursor.execute("SET LOCAL ROLE feature_online_writer")
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                (f"{ATLAS_ONLINE_SCHEMA}.{live}",),
            )

            definitions = [
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(data_type))
                for name, data_type in zip(source_columns, column_types, strict=True)
            ]
            definitions.extend(
                [
                    sql.SQL('"computed_at" TIMESTAMPTZ NOT NULL'),
                    sql.SQL('"source_interval_start" TIMESTAMPTZ'),
                    sql.SQL('"source_interval_end" TIMESTAMPTZ'),
                    sql.SQL('"definition_sha" TEXT NOT NULL'),
                    sql.SQL('"schema_sha" TEXT NOT NULL'),
                    sql.SQL('"publish_run_id" TEXT NOT NULL'),
                    sql.SQL('"retired_at" TIMESTAMPTZ'),
                ]
            )
            cursor.execute(
                sql.SQL("CREATE TABLE {}.{} ({})").format(
                    sql.Identifier(ATLAS_ONLINE_SCHEMA),
                    sql.Identifier(staging),
                    sql.SQL(", ").join(definitions),
                )
            )

            source_cursor = con.execute(f"SELECT * FROM {relation}")
            insert_columns = [
                *source_columns,
                *ATLAS_METADATA_COLUMNS,
            ]
            insert_query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                sql.Identifier(ATLAS_ONLINE_SCHEMA),
                sql.Identifier(staging),
                sql.SQL(", ").join(map(sql.Identifier, insert_columns)),
            )
            while True:
                rows = source_cursor.fetchmany(self.config.batch_size)
                if not rows:
                    break
                values = [
                    (
                        *row,
                        published_at,
                        source_interval_start,
                        source_interval_end,
                        self.config.definition_sha,
                        self.config.schema_sha,
                        self.config.publish_run_id,
                        None,
                    )
                    for row in rows
                ]
                execute_values(
                    cursor,
                    insert_query,
                    values,
                    page_size=self.config.batch_size,
                )
                row_count += len(values)

            cursor.execute(
                sql.SQL("ALTER TABLE {}.{} ADD PRIMARY KEY ({})").format(
                    sql.Identifier(ATLAS_ONLINE_SCHEMA),
                    sql.Identifier(staging),
                    sql.SQL(", ").join(map(sql.Identifier, self.config.entity_keys)),
                )
            )
            if self.config.event_time_column:
                cursor.execute(
                    sql.SQL("CREATE INDEX ON {}.{} ({})").format(
                        sql.Identifier(ATLAS_ONLINE_SCHEMA),
                        sql.Identifier(staging),
                        sql.Identifier(self.config.event_time_column),
                    )
                )

            cursor.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(ATLAS_ONLINE_SCHEMA),
                    sql.Identifier(previous),
                )
            )
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                )
                """,
                (ATLAS_ONLINE_SCHEMA, live),
            )
            live_exists = bool(cursor.fetchone()[0])
            if live_exists:
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(ATLAS_ONLINE_SCHEMA),
                        sql.Identifier(live),
                        sql.Identifier(previous),
                    )
                )
            cursor.execute(
                sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                    sql.Identifier(ATLAS_ONLINE_SCHEMA),
                    sql.Identifier(staging),
                    sql.Identifier(live),
                )
            )
            cursor.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier(ATLAS_ONLINE_SCHEMA),
                    sql.Identifier(previous),
                )
            )
            cursor.execute(
                sql.SQL("""
                    INSERT INTO {}._online_publications (
                        table_name, revision, definition_sha, schema_sha,
                        publish_run_id, row_count, entity_keys,
                        event_time_column, ttl_seconds, source_interval_start,
                        source_interval_end, published_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (table_name) DO UPDATE SET
                        revision = EXCLUDED.revision,
                        definition_sha = EXCLUDED.definition_sha,
                        schema_sha = EXCLUDED.schema_sha,
                        publish_run_id = EXCLUDED.publish_run_id,
                        row_count = EXCLUDED.row_count,
                        entity_keys = EXCLUDED.entity_keys,
                        event_time_column = EXCLUDED.event_time_column,
                        ttl_seconds = EXCLUDED.ttl_seconds,
                        source_interval_start = EXCLUDED.source_interval_start,
                        source_interval_end = EXCLUDED.source_interval_end,
                        published_at = EXCLUDED.published_at
                    """).format(sql.Identifier(ATLAS_ONLINE_SCHEMA)),
                (
                    self.config.table,
                    self.config.revision,
                    self.config.definition_sha,
                    self.config.schema_sha,
                    self.config.publish_run_id,
                    row_count,
                    json.dumps(list(self.config.entity_keys)),
                    self.config.event_time_column,
                    self.config.ttl_seconds,
                    source_interval_start,
                    source_interval_end,
                    published_at,
                ),
            )
            postgres.commit()
        except Exception as exc:
            postgres.rollback()
            raise AtlasOnlineMaterializationError(
                f"Atlas online publication failed for {self.config.table}: {exc}"
            ) from exc
        finally:
            postgres.close()

        return AtlasOnlineWriteResult(
            success=True,
            row_count=row_count,
            duration_seconds=time.monotonic() - started,
            table=self.config.table,
            revision=self.config.revision,
            definition_sha=self.config.definition_sha,
            schema_sha=self.config.schema_sha,
            publish_run_id=self.config.publish_run_id,
            physical_table=f"{ATLAS_ONLINE_SCHEMA}.{live}",
            source_interval_start=source_interval_start,
            source_interval_end=source_interval_end,
        )


__all__ = [
    "ATLAS_METADATA_COLUMNS",
    "ATLAS_ONLINE_SCHEMA",
    "AtlasOnlineMaterializationError",
    "AtlasOnlineMaterializer",
    "AtlasOnlineWriteResult",
]
