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
from typing import Any, Sequence

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

#: Read policy per feature group: which roles may read the served values.
#:
#: Keyed by ``base_name`` -- the stable, version-independent name readers bind
#: to -- not by the versioned physical table. A schema version cutover must not
#: drop or reset the access policy, and keying by version would do exactly that:
#: publishing ``__v2`` would leave the policy attached to ``__v1`` and the new
#: version unreadable (or, worse under a fail-open reader, unprotected).
#:
#: The policy is written in the same transaction that activates the values, so
#: data can never become readable before the policy that governs it.
POLICY_TABLE = "_feature_group_policy"


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
    #: Roles the store confirms may read this group, read back after commit
    #: rather than echoed from the request.
    read_roles: tuple[str, ...] = ()

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
            "read_roles": list(self.read_roles),
        }


def _q(literal: str) -> str:
    """Escape a string for embedding in a single-quoted SQL literal."""
    return literal.replace("'", "''")


#: PostgreSQL SQLSTATE for a serialization failure. The DuckDB bridge forces
#: REPEATABLE READ (the server default is READ COMMITTED), so a conflicting
#: concurrent write aborts rather than blocking. The advisory lock prevents
#: contention on one target, but this remains possible as defence-in-depth.
SERIALIZATION_FAILURE = "40001"


def is_serialization_failure(exc: BaseException) -> bool:
    """True if *exc* looks like a PostgreSQL serialization failure.

    Matched on message text because the error surfaces through DuckDB's bridge
    rather than as a typed psycopg exception.
    """
    text = str(exc).lower()
    return SERIALIZATION_FAILURE in text or "could not serialize access" in text


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
        """Attach the target database, idempotently.

        Attaching an alias that is already attached raises. That is unhelpful
        when several publishers share one connection -- which happens normally,
        e.g. when version resolution replaces a descriptor and rebuilds the
        publisher around the same connection. Re-attaching is a no-op, not an
        error, so it is treated as one.
        """
        con.execute("INSTALL postgres; LOAD postgres;")
        try:
            con.execute(f"ATTACH '{self.dsn}' AS {self.alias} (TYPE POSTGRES)")
        except Exception as exc:
            if "already exists" not in str(exc):
                raise
            logger.debug("%s already attached; reusing it", self.alias)

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
        self._swap_views(con)
        self._remote(
            con,
            f"CREATE TABLE IF NOT EXISTS {self.d.schema}.{PUBLICATIONS_TABLE} ("
            "  publish_run_id TEXT PRIMARY KEY,"
            "  target_table TEXT NOT NULL,"
            # UNIQUE, not merely NOT NULL. Without it the column is decorative:
            # two runs computing the same idempotency key both insert, and the
            # ledger records a duplicate publication as if it were distinct
            # work. The constraint is what makes a replay detectable.
            "  idempotency_key TEXT NOT NULL UNIQUE,"
            "  definition_sha TEXT NOT NULL,"
            "  schema_sha TEXT NOT NULL,"
            "  source_interval_start TIMESTAMPTZ NOT NULL,"
            "  source_interval_end TIMESTAMPTZ NOT NULL,"
            "  staged_rows BIGINT NOT NULL,"
            "  activated_rows BIGINT NOT NULL,"
            "  status TEXT NOT NULL,"
            # The stable base name. target_table records the *versioned*
            # physical table, which cannot be mapped back to a base name by
            # stripping "__vN": safe_identifier() truncates long names and
            # appends a CRC32 suffix, so the versioned and base forms of a long
            # feature group diverge unrecoverably. Recording it is the only
            # reliable join key between publication status and read policy.
            "  feature_group TEXT,"
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
        self._remote(
            con,
            f"CREATE TABLE IF NOT EXISTS {self.d.schema}.{POLICY_TABLE} ("
            "  feature_group TEXT PRIMARY KEY,"
            # CHECK, not merely NOT NULL. An empty array is a policy that grants
            # nothing, which under a fail-closed reader is indistinguishable
            # from "no policy" -- so the database refuses to store one rather
            # than leaving an unreadable group looking deliberately configured.
            "  read_roles TEXT[] NOT NULL CHECK (cardinality(read_roles) > 0),"
            "  publish_run_id TEXT NOT NULL,"
            "  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"
            ")",
        )
        # Pre-publication snapshot of the rows each publication touches, so a
        # bad-but-successful publication can be undone. LIKE keeps it in step
        # with the physical table's shape automatically; _rb_run_id records
        # which publication the snapshot protects, so a rollback cannot restore
        # rows belonging to a different generation.
        self._remote(
            con,
            f"CREATE TABLE IF NOT EXISTS {self.d.rollback_fqn} "
            f"(LIKE {self.d.physical_fqn})",
        )
        self._remote(
            con,
            f"ALTER TABLE {self.d.rollback_fqn} "
            "ADD COLUMN IF NOT EXISTS _rb_run_id TEXT",
        )
        # The CREATEs above ran through postgres_execute, out-of-band from the
        # attached catalog, so refresh its metadata before anything writes
        # through it.
        con.execute("CALL pg_clear_cache()")
        # CREATE TABLE IF NOT EXISTS keeps an older definition silently, so a
        # ledger predating the UNIQUE constraint would go on accepting
        # duplicate idempotency keys. Reconcile explicitly.
        self.migrate_schema(con)

    def _swap_views(self, con: Any) -> None:
        """Point the stable views at this descriptor's version.

        ``CREATE OR REPLACE VIEW`` alone is not sufficient. PostgreSQL requires
        the replacement to keep existing columns' names, order and types and may
        only append -- so cutting over from ``__v1`` to a ``__v2`` that inserts a
        column mid-list fails with:

            cannot change name of view column "computed_at" to "b"

        The schema contract documents exactly this and prescribes DROP + CREATE
        for incompatible evolution; this is where that is carried out.

        The DROP and CREATE run in one transaction. PostgreSQL DDL is
        transactional, so concurrent readers see the old view or the new one and
        never a window with no view at all.
        """
        base = self.d.view_fqn
        stmts = [
            f"DROP VIEW IF EXISTS {base}__live",
            f"DROP VIEW IF EXISTS {base}",
            self.d.create_view_ddl(),
            self.d.live_view_ddl(),
        ]
        # postgres_execute sends one string to the server; wrapping the
        # statements in BEGIN/COMMIT keeps the cutover atomic for readers.
        self._remote(con, "BEGIN; " + "; ".join(stmts) + "; COMMIT")

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

    def migrate_schema(self, con: Any) -> list[str]:
        """Apply constraints missing from ledgers created by earlier versions.

        ``CREATE TABLE IF NOT EXISTS`` silently keeps an older definition, so a
        ledger created before ``idempotency_key`` was UNIQUE keeps accepting
        duplicates. Applied idempotently and reported, rather than assumed.
        """
        applied: list[str] = []
        exists = self._scalar(
            con,
            "SELECT count(*) FROM pg_constraint c "
            "JOIN pg_class t ON t.oid = c.conrelid "
            "JOIN pg_namespace n ON n.oid = t.relnamespace "
            f"WHERE n.nspname = '{_q(self.d.schema)}' "
            f"AND t.relname = '{_q(PUBLICATIONS_TABLE)}' AND c.contype = 'u'",
        )
        if not exists:
            try:
                self._remote(
                    con,
                    f"ALTER TABLE {self.d.schema}.{PUBLICATIONS_TABLE} "
                    f"ADD CONSTRAINT {PUBLICATIONS_TABLE}_idem_uq UNIQUE (idempotency_key)",
                )
                con.execute("CALL pg_clear_cache()")
                applied.append("idempotency_key UNIQUE")
            except Exception as exc:
                # Pre-existing duplicates block the constraint. Surface that
                # rather than continuing as though the ledger were sound.
                logger.warning(
                    "could not add idempotency_key UNIQUE (existing duplicates?): %s",
                    exc,
                )

        # feature_group joins publication status to read policy. Ledgers written
        # before the policy table exists lack the column entirely, and
        # CREATE TABLE IF NOT EXISTS will not add it.
        has_column = self._scalar(
            con,
            "SELECT count(*) FROM information_schema.columns "
            f"WHERE table_schema = '{_q(self.d.schema)}' "
            f"AND table_name = '{_q(PUBLICATIONS_TABLE)}' "
            "AND column_name = 'feature_group'",
        )
        if not has_column:
            self._remote(
                con,
                f"ALTER TABLE {self.d.schema}.{PUBLICATIONS_TABLE} "
                "ADD COLUMN IF NOT EXISTS feature_group TEXT",
            )
            con.execute("CALL pg_clear_cache()")
            applied.append("feature_group column")
        return applied

    # -- version resolution ------------------------------------------------

    def resolve_version(self, con: Any) -> int:
        """Return the version this descriptor should publish to.

        Publication previously always targeted ``__v1``: the executor never
        supplied a version and ``from_relation`` defaults to 1, so
        ``compatible_with`` was never exercised at runtime and a feature added
        to an existing group would fail against the old table instead of
        creating ``__v2``.

        Resolution walks existing versions newest-first and returns the first
        whose stored ``schema_sha`` matches this descriptor. If versions exist
        but none match, the shape changed incompatibly and the next version
        number is returned.
        """
        rows = con.execute(
            f"SELECT * FROM postgres_query('{self.alias}', "
            f"'SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = ''{_q(self.d.schema)}'' "
            f"AND table_name LIKE ''{_q(self.d.base_name)}\\_\\_v%'' "
            f"ORDER BY table_name')"
        ).fetchall()

        versions: list[int] = []
        prefix = f"{self.d.base_name}__v"
        for (name,) in rows:
            if name.startswith(prefix):
                suffix = name[len(prefix) :]
                if suffix.isdigit():
                    versions.append(int(suffix))
        if not versions:
            return 1

        for v in sorted(versions, reverse=True):
            sha = self._scalar(
                con,
                f"SELECT schema_sha FROM {self.d.schema}.{self.d.base_name}__v{v} LIMIT 1",
            )
            if sha is None or sha == self.d.schema_sha:
                # No rows yet means the shape is unconstrained by data.
                return v

        nxt = max(versions) + 1
        logger.info(
            "schema of %s changed incompatibly; publishing to __v%d",
            self.d.base_name,
            nxt,
        )
        return nxt

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

    def stage(
        self, con: Any, view_name: str, publish_run_id: str, attempt: int = 0
    ) -> tuple[str, int]:
        """Bulk-load *view_name* into a uniquely-named remote staging table.

        *attempt* is folded into the name so a retry never reuses a staging
        name from a previous attempt. Reuse would be the R16 hazard: the earlier
        attempt's cleanup drops the table, and recreating the same name on the
        same attachment can silently produce a zero-row CTAS.
        """
        discriminator = publish_run_id if attempt == 0 else f"{publish_run_id}a{attempt}"
        staging = self.d.staging_fqn(discriminator)
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
        read_roles: Sequence[str],
        upstream_node_ids: list[str] | None = None,
        idempotency_key: str | None = None,
        cleanup_staging: bool = True,
        attempt: int = 0,
    ) -> PublishResult:
        """Stage, validate, then atomically activate and record the publication.

        A single attempt. Use :meth:`publish_with_retry` to absorb serialization
        failures.

        Args:
            read_roles: Roles permitted to read the served values. Required and
                without a default: the catalog is fail-closed, so publishing
                without a policy yields data nobody can read, and defaulting to
                something permissive would silently expose whatever gets
                published. The policy is written inside the activation
                transaction, so values are never readable before the policy
                governing them exists.
        """
        started = time.perf_counter()
        result = PublishResult(
            publish_run_id=publish_run_id, table=self.d.physical_fqn
        )
        staging: str | None = None

        roles = tuple(str(r).strip() for r in (read_roles or ()) if str(r).strip())
        if not roles:
            raise OnlinePublishError(
                f"publish({self.d.base_name!r}) requires a non-empty read_roles: "
                "the catalog denies reads on any feature group with no policy, so "
                "activating values without one would publish data nobody can read."
            )

        try:
            self.ensure_schema(con)

            if upstream_node_ids:
                self.assert_upstream_complete(
                    con, upstream_node_ids, source_interval_start, source_interval_end
                )

            result.remote_rows_before = int(
                self._scalar(con, f"SELECT count(*) FROM {self.d.physical_fqn}") or 0
            )

            staging, staged = self.stage(con, view_name, publish_run_id, attempt)
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

                # Snapshot the rows this publication is about to overwrite,
                # BEFORE the upsert and inside the same transaction. Taken
                # after the lock so it cannot race another publication, and
                # inside the transaction so a rolled-back publication leaves no
                # stale rollback point behind.
                #
                # Only rows that already exist are captured. Rows the
                # publication newly inserts are absent here by construction,
                # which is exactly how rollback tells "restore the old value"
                # from "delete a row that should not exist".
                cols = ", ".join(f'"{c.name}"' for c in self.d.all_columns)
                key_tuple = ", ".join(f'"{c.name}"' for c in self.d.entity_keys)
                con.execute(f"DELETE FROM {self.alias}.{self.d.rollback_fqn}")
                con.execute(
                    f"INSERT INTO {self.alias}.{self.d.rollback_fqn} "
                    f"({cols}, _rb_run_id) "
                    f"SELECT {cols}, '{_q(publish_run_id)}' "
                    f"FROM {self.alias}.{self.d.physical_fqn} "
                    f"WHERE ({key_tuple}) IN "
                    f"(SELECT {key_tuple} FROM {self.alias}.{staging})"
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

                # The read policy activates in the SAME transaction as the
                # values. Writing it before would expose a window in which the
                # policy names a group with no data; writing it after would
                # expose the far worse window in which values are live and the
                # fail-closed catalog has no policy to check them against.
                roles_sql = "ARRAY[" + ", ".join(f"'{_q(r)}'" for r in roles) + "]"
                # updated_at is supplied explicitly rather than left to its
                # DEFAULT. Writing through the attached catalog, DuckDB sends
                # this row as a full-row COPY -- every column, with NULL for the
                # ones the statement omitted -- so the column DEFAULT never
                # applies and NOT NULL rejects the row:
                #   null value in column "updated_at" violates not-null
                # The ledger insert nearby gets away with omitting activated_at,
                # so this is not a general rule about omitted columns; do not
                # "simplify" this back to relying on the DEFAULT.
                now_literal = datetime.now(timezone.utc).isoformat()
                con.execute(
                    f"INSERT INTO {self.alias}.{self.d.schema}.{POLICY_TABLE} "
                    "(feature_group, read_roles, publish_run_id, updated_at) VALUES ("
                    f"'{_q(self.d.base_name)}', {roles_sql}, '{_q(publish_run_id)}', "
                    f"TIMESTAMPTZ '{now_literal}') "
                    "ON CONFLICT (feature_group) DO UPDATE SET "
                    "read_roles = EXCLUDED.read_roles, "
                    "publish_run_id = EXCLUDED.publish_run_id, "
                    "updated_at = EXCLUDED.updated_at"
                )

                con.execute(
                    f"INSERT INTO {self.alias}.{self.d.schema}.{PUBLICATIONS_TABLE} "
                    "(publish_run_id, target_table, idempotency_key, definition_sha, "
                    " schema_sha, source_interval_start, source_interval_end, "
                    " staged_rows, activated_rows, status, feature_group) VALUES ("
                    f"'{_q(publish_run_id)}', '{_q(self.d.physical_fqn)}', '{_q(ik)}', "
                    f"'{_q(definition_sha)}', '{_q(self.d.schema_sha)}', "
                    f"'{source_interval_start.isoformat()}', "
                    f"'{source_interval_end.isoformat()}', "
                    f"{staged}, {staged}, 'succeeded', '{_q(self.d.base_name)}')"
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
                # The transaction has already COMMITTED at this point. Saying
                # only "verification failed" invites an operator to assume
                # nothing landed and republish, when in fact the activation and
                # ledger are durable. State the committed-but-unverified
                # condition explicitly so the next action is an investigation,
                # not a blind retry.
                raise OnlinePublishError(
                    f"remote verification failed AFTER COMMIT: staged {staged} rows "
                    f"but only {confirmed} carry publish_run_id {publish_run_id!r}. "
                    f"The activation and ledger row for {publish_run_id!r} are "
                    "already committed and may be serving -- inspect "
                    f"{self.d.physical_fqn} before republishing."
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
                    f"ledger verification failed AFTER COMMIT: expected 1 row for "
                    f"{publish_run_id!r}, found {ledgered}. The feature activation "
                    "is already committed; this is a ledger inconsistency, not a "
                    "failed publication."
                )

            # A policy that silently failed to land would leave the values
            # unreadable rather than over-exposed -- fail-closed, so not a leak
            # -- but the group would be dark with no indication why. Confirm it
            # rather than assume the insert took effect.
            policy_roles = self._scalar(
                con,
                f"SELECT array_to_string(read_roles, ',') FROM "
                f"{self.d.schema}.{POLICY_TABLE} "
                f"WHERE feature_group = '{_q(self.d.base_name)}'",
            )
            stored = tuple(r for r in str(policy_roles or "").split(",") if r)
            if set(stored) != set(roles):
                raise OnlinePublishError(
                    f"read policy verification failed AFTER COMMIT for "
                    f"{self.d.base_name!r}: expected roles {sorted(roles)}, store "
                    f"has {sorted(stored)}. The values are already committed and "
                    "will be served to whoever the stored policy allows -- "
                    f"reconcile {self.d.schema}.{POLICY_TABLE} before relying on "
                    "this group's access control."
                )
            result.read_roles = tuple(sorted(roles))

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

    def latest_publication(self, con: Any) -> str | None:
        """publish_run_id of the most recent successful publication, or None."""
        return self._scalar(
            con,
            f"SELECT publish_run_id FROM {self.d.schema}.{PUBLICATIONS_TABLE} "
            f"WHERE target_table = '{_q(self.d.physical_fqn)}' "
            "AND status = 'succeeded' ORDER BY activated_at DESC LIMIT 1",
        )

    def rollback(self, con: Any, publish_run_id: str) -> dict[str, int]:
        """Undo *publish_run_id*, restoring the values it replaced.

        Publication is in place, so a successful-but-wrong run overwrites the
        previous values and the ledger records metadata rather than rows. This
        restores the snapshot taken during that publication.

        Only the most recent successful publication can be rolled back. Deeper
        history is not retained, and rolling back an older run would silently
        discard every publication since -- the snapshot describes one step, not
        a timeline. Refusing is the honest answer; the alternative is restoring
        a state that never existed.

        Rows the publication newly inserted are deleted; rows it overwrote are
        restored. Both happen in one transaction under the same advisory lock
        publication uses, so a rollback cannot interleave with a publication.

        Returns:
            Counts of ``restored`` and ``deleted`` rows.

        Raises:
            OnlinePublishError: *publish_run_id* is not the latest successful
                publication, or the lock could not be taken.
        """
        latest = self.latest_publication(con)
        if latest is None:
            raise OnlinePublishError(
                f"no successful publication is recorded for {self.d.physical_fqn}; "
                "there is nothing to roll back"
            )
        if latest != publish_run_id:
            raise OnlinePublishError(
                f"cannot roll back {publish_run_id!r}: the latest successful "
                f"publication is {latest!r}. Only one generation of pre-publication "
                "state is retained, so rolling back an older run would discard "
                "every publication since it. Roll back the latest run instead, or "
                "republish the intended values."
            )

        cols = ", ".join(f'"{c.name}"' for c in self.d.all_columns)
        key_tuple = ", ".join(f'"{c.name}"' for c in self.d.entity_keys)
        rb = f"{self.alias}.{self.d.rollback_fqn}"
        phys = f"{self.alias}.{self.d.physical_fqn}"
        snapshot_keys = (
            f"SELECT {key_tuple} FROM {rb} WHERE _rb_run_id = '{_q(publish_run_id)}'"
        )

        restored = deleted = 0
        con.execute("BEGIN TRANSACTION")
        try:
            acquired = con.execute(
                f"SELECT * FROM postgres_query('{self.alias}', "
                f"'SELECT pg_try_advisory_xact_lock({advisory_key(self.d.physical_fqn)}) AS locked')"
            ).fetchone()
            if not acquired or not acquired[0]:
                raise OnlinePublishError(
                    f"another operation holds the lock for {self.d.physical_fqn}; "
                    "a rollback must not interleave with a publication"
                )

            deleted = int(
                self._scalar(
                    con,
                    f"SELECT count(*) FROM {self.d.physical_fqn} "
                    f"WHERE publish_run_id = '{_q(publish_run_id)}' "
                    f"AND ({key_tuple}) NOT IN (SELECT {key_tuple} FROM "
                    f"{self.d.rollback_fqn} WHERE _rb_run_id = '{_q(publish_run_id)}')",
                )
                or 0
            )
            restored = int(
                self._scalar(
                    con,
                    f"SELECT count(*) FROM {self.d.rollback_fqn} "
                    f"WHERE _rb_run_id = '{_q(publish_run_id)}'",
                )
                or 0
            )

            # Rows this run created: delete them. They had no prior value, so
            # there is nothing to restore.
            con.execute(
                f"DELETE FROM {phys} WHERE publish_run_id = '{_q(publish_run_id)}' "
                f"AND ({key_tuple}) NOT IN ({snapshot_keys})"
            )
            # Rows it overwrote: replace with the snapshot. Delete-then-insert
            # rather than a multi-column UPDATE ... FROM so every column is
            # restored without enumerating assignments, and so a column added
            # later cannot be silently left at its new value.
            con.execute(f"DELETE FROM {phys} WHERE ({key_tuple}) IN ({snapshot_keys})")
            con.execute(
                f"INSERT INTO {phys} ({cols}) SELECT {cols} FROM {rb} "
                f"WHERE _rb_run_id = '{_q(publish_run_id)}'"
            )
            con.execute(
                f"UPDATE {self.alias}.{self.d.schema}.{PUBLICATIONS_TABLE} "
                "SET status = 'rolled_back' "
                f"WHERE publish_run_id = '{_q(publish_run_id)}'"
            )
            # The snapshot describes a publication that no longer stands. Left
            # in place it would advertise a rollback point that, if applied
            # twice, would delete rows the first rollback correctly restored.
            con.execute(f"DELETE FROM {rb}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        logger.info(
            "rolled back %s on %s: %d restored, %d deleted",
            publish_run_id,
            self.d.physical_fqn,
            restored,
            deleted,
        )
        return {"restored": restored, "deleted": deleted}

    def publish_with_retry(
        self,
        con: Any,
        view_name: str,
        *,
        max_retries: int = 3,
        backoff_seconds: float = 0.5,
        **kwargs: Any,
    ) -> PublishResult:
        """Publish, retrying only on serialization failure.

        The advisory lock serializes publications to one target, but the bridge
        forces REPEATABLE READ, so a conflicting concurrent write elsewhere can
        still abort the transaction with SQLSTATE 40001. That is transient and
        safe to retry -- the failed attempt committed nothing.

        Only serialization failures are retried. Validation errors, an
        incomplete upstream, and remote-verification failures are deterministic:
        retrying them would just fail again more slowly, and retrying a
        verification failure could mask real data loss.

        Each attempt stages under a fresh name (see :meth:`stage`).
        """
        last: Exception | None = None
        for attempt in range(max_retries + 1):
            try:
                return self.publish(con, view_name, attempt=attempt, **kwargs)
            except (StagingValidationError, UpstreamIncompleteError):
                raise  # deterministic - retrying cannot help
            except Exception as exc:
                if not is_serialization_failure(exc):
                    raise
                last = exc
                if attempt < max_retries:
                    delay = backoff_seconds * (2**attempt)
                    logger.warning(
                        "publication hit a serialization failure (attempt %d/%d), "
                        "retrying in %.1fs: %s",
                        attempt + 1,
                        max_retries + 1,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
        raise OnlinePublishError(
            f"publication failed after {max_retries + 1} attempts due to repeated "
            f"serialization failures; last error: {last}"
        )

    # -- retirement --------------------------------------------------------

    def expire_stale(self, con: Any, ttl_days: int) -> int:
        """Retire rows whose data is older than *ttl_days*.

        Age is measured from ``source_interval_end`` -- the end of the window the
        values describe -- not ``computed_at``. A row recomputed minutes ago over
        a month-old window is stale by any definition a consumer cares about, and
        measuring from ``computed_at`` would call it fresh.

        Rows are **retired, not deleted**: they leave the live projection but stay
        queryable for audit. Deleting them would make an intentional expiry
        indistinguishable from a transient absence during refresh, which is the
        distinction Principle 2 depends on.

        Returns:
            The number of rows retired by this call.
        """
        if ttl_days <= 0:
            raise OnlinePublishError(
                f"ttl_days must be positive, got {ttl_days}"
            )

        before = int(
            self._scalar(
                con,
                f"SELECT count(*) FROM {self.d.physical_fqn} WHERE retired_at IS NOT NULL",
            )
            or 0
        )
        self._remote(
            con,
            f"UPDATE {self.d.physical_fqn} SET retired_at = now() "
            f"WHERE retired_at IS NULL "
            f"AND source_interval_end < now() - INTERVAL '{int(ttl_days)} days'",
        )
        after = int(
            self._scalar(
                con,
                f"SELECT count(*) FROM {self.d.physical_fqn} WHERE retired_at IS NOT NULL",
            )
            or 0
        )
        retired = after - before
        if retired:
            logger.info(
                "retired %d row(s) from %s older than %d day(s)",
                retired,
                self.d.physical_fqn,
                ttl_days,
            )
        return retired

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
    "SERIALIZATION_FAILURE",
    "OnlinePublishError",
    "OnlinePublisher",
    "PublishResult",
    "StagingValidationError",
    "UpstreamIncompleteError",
    "advisory_key",
    "is_serialization_failure",
]
