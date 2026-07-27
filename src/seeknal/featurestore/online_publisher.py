"""Publish feature data to the PostgreSQL online store.

Stays entirely within ADR-005: all PostgreSQL I/O goes through DuckDB's postgres
extension. No psycopg client is used here.

The publication sequence below is not a design sketch -- it is the exact shape
verified against a real PostgreSQL during the Phase 0.2 capability spike:

    BEGIN                                   (DuckDB transaction)
      pg_try_advisory_xact_lock(key)        via postgres_query, transaction-scoped
      INSERT ... ON CONFLICT DO UPDATE      activation, from remote staging
      INSERT INTO _online_publications      ledger, same transaction
    COMMIT

Verified properties: an independent PostgreSQL session cannot take the same
lock while the transaction is open, cannot see the uncommitted activation or
ledger rows, and sees both appear together on commit. ROLLBACK reverts both.

Two mistakes this module encodes defences against
-------------------------------------------------
**Session-level locks do not work here.** ``pg_advisory_lock`` (session-scoped)
acquired through ``postgres_query`` does not protect the activation, because
``postgres_query`` wraps its argument in ``COPY (...) TO STDOUT`` and the
statement ends immediately. Only the ``_xact_`` variant, inside an explicit
transaction, holds.

**Staging names must never be reused.** Dropping a table out-of-band via
``postgres_execute`` and recreating the same name through the attached catalog
leaves stale catalog metadata, and the following ``CREATE TABLE ... AS SELECT``
silently writes **zero rows with no error**. Names are therefore derived from
``publish_run_id`` (see ``OnlineTableDescriptor.staging_name``).
"""

from __future__ import annotations

import logging
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from seeknal.featurestore.online_contract import (
    OnlineContractError,
    OnlineTableDescriptor,
)

logger = logging.getLogger(__name__)

PG_ALIAS = "pg_online"

#: Ledger of terminal online activations. Deliberately separate from execution
#: tracking: a ledger holding only online publications cannot answer "did every
#: upstream node succeed for this interval", and conflating the two deadlocks
#: the publication gate because non-publishing upstream nodes never get rows.
PUBLICATIONS_TABLE = "_online_publications"

#: Completeness record for every DAG node execution, publishing or not. This is
#: what the publication gate reads.
EXECUTIONS_TABLE = "_execution_intervals"


class OnlinePublishError(Exception):
    """Publication failed. The activation transaction was not committed."""


class StagingValidationError(OnlinePublishError):
    """Staged data failed validation, so nothing was activated."""


class UpstreamIncompleteError(OnlinePublishError):
    """An upstream node has no successful execution covering the interval.

    Fail-closed: absence of evidence is treated as failure, not as "nothing to
    wait for". A second-order feature computed over an incomplete window is not
    a missing value, it is a confidently wrong one.
    """


@dataclass
class PublishResult:
    """Outcome of one publication."""

    publish_run_id: str
    table: str
    staged_rows: int = 0
    remote_rows_before: int = 0
    remote_rows_after: int = 0
    distinct_keys: int = 0
    activated: bool = False
    duration_seconds: float = 0.0
    error: str | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def succeeded(self) -> bool:
        return self.activated and self.error is None

    def to_dict(self) -> dict[str, Any]:
        return {
            "publish_run_id": self.publish_run_id,
            "table": self.table,
            "staged_rows": self.staged_rows,
            "remote_rows_before": self.remote_rows_before,
            "remote_rows_after": self.remote_rows_after,
            "distinct_keys": self.distinct_keys,
            "activated": self.activated,
            "succeeded": self.succeeded,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
            "warnings": list(self.warnings),
        }


def _q(literal: str) -> str:
    """Escape a string for embedding in a single-quoted SQL literal."""
    return literal.replace("'", "''")


def advisory_key(table_fqn: str) -> int:
    """Derive a stable 63-bit advisory-lock key from a target table name.

    Keyed by target so publications to different tables proceed concurrently
    while publications to the same table serialize.
    """
    return zlib.crc32(table_fqn.encode()) & 0x7FFFFFFF


class OnlinePublisher:
    """Publishes a DuckDB relation into a PostgreSQL online feature table."""

    def __init__(
        self,
        descriptor: OnlineTableDescriptor,
        libpq_dsn: str,
        *,
        alias: str = PG_ALIAS,
    ) -> None:
        self.d = descriptor
        self.dsn = libpq_dsn
        self.alias = alias

    # -- low-level helpers -------------------------------------------------

    def _remote(self, con: Any, sql: str) -> None:
        """Execute *sql* on the PostgreSQL server. No local relations visible."""
        con.execute(f"CALL postgres_execute('{self.alias}', '{_q(sql)}')")

    def _scalar(self, con: Any, sql: str) -> Any:
        row = con.execute(
            f"SELECT * FROM postgres_query('{self.alias}', '{_q(sql)}')"
        ).fetchone()
        return row[0] if row else None

    def attach(self, con: Any) -> None:
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{self.dsn}' AS {self.alias} (TYPE POSTGRES)")

    def detach(self, con: Any) -> None:
        try:
            con.execute(f"DETACH {self.alias}")
        except Exception:  # pragma: no cover - detach is best effort
            logger.debug("DETACH %s ignored", self.alias)

    # -- schema management -------------------------------------------------

    def ensure_schema(self, con: Any) -> None:
        """Create the schema, target table, stable views, and ledgers."""
        self._remote(con, f"CREATE SCHEMA IF NOT EXISTS {self.d.schema}")
        self._remote(con, self.d.create_table_ddl())
        self._remote(con, self.d.create_view_ddl())
        self._remote(con, self.d.live_view_ddl())
        self._remote(
            con,
            f"CREATE TABLE IF NOT EXISTS {self.d.schema}.{PUBLICATIONS_TABLE} ("
            "  publish_run_id TEXT PRIMARY KEY,"
            "  target_table TEXT NOT NULL,"
            "  idempotency_key TEXT NOT NULL,"
            "  definition_sha TEXT NOT NULL,"
            "  schema_sha TEXT NOT NULL,"
            "  source_interval_start TIMESTAMPTZ NOT NULL,"
            "  source_interval_end TIMESTAMPTZ NOT NULL,"
            "  staged_rows BIGINT NOT NULL,"
            "  activated_rows BIGINT NOT NULL,"
            "  status TEXT NOT NULL,"
            "  activated_at TIMESTAMPTZ NOT NULL DEFAULT now()"
            ")",
        )
        self._remote(
            con,
            f"CREATE TABLE IF NOT EXISTS {self.d.schema}.{EXECUTIONS_TABLE} ("
            "  node_id TEXT NOT NULL,"
            "  interval_start TIMESTAMPTZ NOT NULL,"
            "  interval_end TIMESTAMPTZ NOT NULL,"
            "  status TEXT NOT NULL,"
            "  definition_sha TEXT,"
            "  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),"
            "  PRIMARY KEY (node_id, interval_start, interval_end)"
            ")",
        )

    def record_execution(
        self,
        con: Any,
        node_id: str,
        interval_start: datetime,
        interval_end: datetime,
        status: str = "succeeded",
        definition_sha: str = "",
    ) -> None:
        """Record a node execution. Every node records, publishing or not."""
        self._remote(
            con,
            f"INSERT INTO {self.d.schema}.{EXECUTIONS_TABLE} "
            "(node_id, interval_start, interval_end, status, definition_sha) VALUES ("
            f"'{_q(node_id)}', '{interval_start.isoformat()}', "
            f"'{interval_end.isoformat()}', '{_q(status)}', '{_q(definition_sha)}') "
            "ON CONFLICT (node_id, interval_start, interval_end) DO UPDATE SET "
            "status = EXCLUDED.status, definition_sha = EXCLUDED.definition_sha, "
            "recorded_at = now()"
        )

    # -- gating ------------------------------------------------------------

    def assert_upstream_complete(
        self,
        con: Any,
        upstream_node_ids: list[str],
        interval_start: datetime,
        interval_end: datetime,
    ) -> None:
        """Fail unless every upstream node covers the interval successfully.

        Coverage, not exact equality: an upstream node running on a coarser
        schedule legitimately covers a finer downstream interval, and exact
        matching would block it forever.
        """
        if not upstream_node_ids:
            return
        for node_id in upstream_node_ids:
            covered = self._scalar(
                con,
                f"SELECT count(*) FROM {self.d.schema}.{EXECUTIONS_TABLE} "
                f"WHERE node_id = '{_q(node_id)}' AND status = 'succeeded' "
                f"AND interval_start <= '{interval_start.isoformat()}' "
                f"AND interval_end >= '{interval_end.isoformat()}'",
            )
            if not covered:
                raise UpstreamIncompleteError(
                    f"upstream node {node_id!r} has no successful execution covering "
                    f"[{interval_start.isoformat()}, {interval_end.isoformat()}]. "
                    "Refusing to publish: a derived feature over an incomplete "
                    "window is wrong, not missing."
                )

    # -- staging + validation ---------------------------------------------

    def stage(self, con: Any, view_name: str, publish_run_id: str) -> tuple[str, int]:
        """Bulk-load *view_name* into a uniquely-named remote staging table."""
        staging = self.d.staging_fqn(publish_run_id)
        con.execute(f"CREATE TABLE {self.alias}.{staging} AS SELECT * FROM {view_name}")
        staged = self._scalar(con, f"SELECT count(*) FROM {staging}") or 0
        return staging, int(staged)

    def validate_staging(self, con: Any, staging: str, staged_rows: int) -> int:
        """Validate staged data before anything is activated.

        Returns the distinct entity-key count.
        """
        if staged_rows == 0:
            raise StagingValidationError(
                f"staging table {staging} is empty; refusing to activate. "
                "A zero-row publication would blank the served generation."
            )

        keys = ", ".join(f'"{c.name}"' for c in self.d.entity_keys)

        null_keys = self._scalar(
            con,
            f"SELECT count(*) FROM {staging} WHERE "
            + " OR ".join(f'"{c.name}" IS NULL' for c in self.d.entity_keys),
        )
        if null_keys:
            raise StagingValidationError(
                f"{null_keys} staged row(s) have a NULL entity key"
            )

        distinct = int(self._scalar(con, f"SELECT count(DISTINCT ({keys})) FROM {staging}") or 0)
        if distinct != staged_rows:
            raise StagingValidationError(
                f"staged data contains duplicate entity keys: {staged_rows} rows but "
                f"{distinct} distinct keys. ON CONFLICT would apply them in an "
                "arbitrary order, making the result nondeterministic."
            )
        return distinct

    # -- publication -------------------------------------------------------

    def publish(
        self,
        con: Any,
        view_name: str,
        *,
        publish_run_id: str,
        source_interval_start: datetime,
        source_interval_end: datetime,
        definition_sha: str,
        upstream_node_ids: list[str] | None = None,
        idempotency_key: str | None = None,
        cleanup_staging: bool = True,
    ) -> PublishResult:
        """Stage, validate, then atomically activate and record the publication."""
        started = time.perf_counter()
        result = PublishResult(
            publish_run_id=publish_run_id, table=self.d.physical_fqn
        )
        staging: str | None = None

        try:
            self.ensure_schema(con)

            if upstream_node_ids:
                self.assert_upstream_complete(
                    con, upstream_node_ids, source_interval_start, source_interval_end
                )

            result.remote_rows_before = int(
                self._scalar(con, f"SELECT count(*) FROM {self.d.physical_fqn}") or 0
            )

            staging, staged = self.stage(con, view_name, publish_run_id)
            result.staged_rows = staged
            result.distinct_keys = self.validate_staging(con, staging, staged)

            key = advisory_key(self.d.physical_fqn)
            ik = idempotency_key or publish_run_id

            # --- the verified atomic sequence -----------------------------
            con.execute("BEGIN TRANSACTION")
            try:
                acquired = con.execute(
                    f"SELECT * FROM postgres_query('{self.alias}', "
                    f"'SELECT pg_try_advisory_xact_lock({key}) AS locked')"
                ).fetchone()
                if not acquired or not acquired[0]:
                    raise OnlinePublishError(
                        f"another publication holds the lock for {self.d.physical_fqn}; "
                        "publications to one target must not run concurrently"
                    )

                con.execute(
                    f"INSERT INTO {self.alias}.{self.d.physical_fqn} "
                    f"({', '.join(chr(34) + c.name + chr(34) for c in self.d.all_columns)}) "
                    f"SELECT {', '.join(chr(34) + c.name + chr(34) for c in self.d.all_columns)} "
                    f"FROM {self.alias}.{staging} "
                    f"ON CONFLICT ({', '.join(chr(34) + c.name + chr(34) for c in self.d.entity_keys)}) "
                    "DO UPDATE SET "
                    + ", ".join(
                        f'"{n}" = EXCLUDED."{n}"' for n in self.d.served_column_names
                    )
                )

                con.execute(
                    f"INSERT INTO {self.alias}.{self.d.schema}.{PUBLICATIONS_TABLE} "
                    "(publish_run_id, target_table, idempotency_key, definition_sha, "
                    " schema_sha, source_interval_start, source_interval_end, "
                    " staged_rows, activated_rows, status) VALUES ("
                    f"'{_q(publish_run_id)}', '{_q(self.d.physical_fqn)}', '{_q(ik)}', "
                    f"'{_q(definition_sha)}', '{_q(self.d.schema_sha)}', "
                    f"'{source_interval_start.isoformat()}', "
                    f"'{source_interval_end.isoformat()}', "
                    f"{staged}, {staged}, 'succeeded')"
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            # --------------------------------------------------------------

            result.activated = True

            # Verify from the REMOTE side. The helper's historic row count read
            # the local input view, so a reported count was never evidence that
            # anything was written.
            result.remote_rows_after = int(
                self._scalar(con, f"SELECT count(*) FROM {self.d.physical_fqn}") or 0
            )
            confirmed = int(
                self._scalar(
                    con,
                    f"SELECT count(*) FROM {self.d.physical_fqn} "
                    f"WHERE publish_run_id = '{_q(publish_run_id)}'",
                )
                or 0
            )
            if confirmed != staged:
                raise OnlinePublishError(
                    f"remote verification failed: staged {staged} rows but only "
                    f"{confirmed} carry publish_run_id {publish_run_id!r}"
                )

            ledgered = int(
                self._scalar(
                    con,
                    f"SELECT count(*) FROM {self.d.schema}.{PUBLICATIONS_TABLE} "
                    f"WHERE publish_run_id = '{_q(publish_run_id)}'",
                )
                or 0
            )
            if ledgered != 1:
                raise OnlinePublishError(
                    f"ledger verification failed: expected 1 row for "
                    f"{publish_run_id!r}, found {ledgered}"
                )

        except Exception as exc:
            result.error = f"{type(exc).__name__}: {exc}"
            logger.error("online publication failed: %s", result.error)
            raise
        finally:
            result.duration_seconds = time.perf_counter() - started
            if cleanup_staging and staging:
                try:
                    self._remote(con, f"DROP TABLE IF EXISTS {staging}")
                except Exception:  # pragma: no cover
                    result.warnings.append(f"staging cleanup failed for {staging}")

        return result

    # -- retirement --------------------------------------------------------

    def retire(self, con: Any, key_values: dict[str, Any]) -> int:
        """Mark an entity retired. Rows are never physically deleted.

        Retirement is an explicit state so a consumer can distinguish an
        intentional withdrawal from a transient absence during refresh.
        """
        for c in self.d.entity_keys:
            if c.name not in key_values:
                raise OnlineContractError(f"missing entity key column {c.name!r}")
        predicate = " AND ".join(
            f"\"{c.name}\" = '{_q(str(key_values[c.name]))}'"
            for c in self.d.entity_keys
        )
        self._remote(
            con,
            f"UPDATE {self.d.physical_fqn} SET retired_at = now() "
            f"WHERE {predicate} AND retired_at IS NULL",
        )
        return int(
            self._scalar(
                con,
                f"SELECT count(*) FROM {self.d.physical_fqn} "
                f"WHERE {predicate} AND retired_at IS NOT NULL",
            )
            or 0
        )


__all__ = [
    "EXECUTIONS_TABLE",
    "PUBLICATIONS_TABLE",
    "OnlinePublishError",
    "OnlinePublisher",
    "PublishResult",
    "StagingValidationError",
    "UpstreamIncompleteError",
    "advisory_key",
]
