"""
PostgreSQL materialization helper for DuckDB-based pipelines.

Materializes DuckDB query results to PostgreSQL via DuckDB's postgres extension
(binary COPY protocol). No psycopg2 or other PostgreSQL driver is needed --
all operations go through DuckDB.

Supported modes:
- FULL: DROP TABLE + CREATE TABLE AS SELECT (full replacement)
- INCREMENTAL_BY_TIME: DELETE rows in time range + INSERT new rows
- UPSERT_BY_KEY: temp table + DELETE USING matching keys + INSERT from temp
"""

from __future__ import annotations

import logging
import time
from typing import Any

from seeknal.workflow.materialization.operations import WriteResult  # ty: ignore[unresolved-import]
from seeknal.workflow.materialization.pg_config import (  # ty: ignore[unresolved-import]
    PostgresMaterializationConfig,
    PostgresMaterializationMode,
)

logger = logging.getLogger(__name__)


def _mask_password(conn_str: str) -> str:
    """Lazily import and call mask_password from the connections module."""
    from seeknal.connections.postgresql import mask_password  # ty: ignore[unresolved-import]

    return mask_password(conn_str)


class PostgresMaterializationError(Exception):
    """Raised when a PostgreSQL materialization operation fails."""

    pass


class PostgresMaterializationHelper:
    """Materialize DuckDB query results to PostgreSQL via DuckDB's postgres extension.

    Args:
        pg_config: A ``PostgreSQLConfig`` instance (from
            ``seeknal.connections.postgresql``). Must expose a
            ``to_libpq_string()`` method.
        mat_config: A ``PostgresMaterializationConfig`` instance.
    """

    PG_ALIAS = "pg_db"

    def __init__(
        self,
        pg_config: Any,
        mat_config: PostgresMaterializationConfig,
    ) -> None:
        """Store configs. Don't connect yet.

        Args:
            pg_config: PostgreSQL connection configuration (PostgreSQLConfig).
            mat_config: Materialization mode and table configuration.
        """
        self.pg_config = pg_config
        self.mat_config = mat_config

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _split_table(self) -> tuple[str, str]:
        """Split ``mat_config.table`` (``schema.table_name``) into parts.

        Returns:
            Tuple of (schema, table_name).
        """
        parts = self.mat_config.table.split(".", 1)
        return parts[0], parts[1]

    def _qualified_table(self) -> str:
        """Return the fully-qualified table reference through the DuckDB alias.

        Example: ``pg_db.public.orders``
        """
        schema, table_name = self._split_table()
        return f"{self.PG_ALIAS}.{schema}.{table_name}"

    def _attach(self, con: Any) -> None:
        """Install the postgres extension (if needed) and ATTACH the database."""
        libpq = self.pg_config.to_libpq_string()
        logger.info(
            "Attaching PostgreSQL database: %s",
            _mask_password(libpq),
        )
        con.execute("INSTALL postgres; LOAD postgres;")
        con.execute(f"ATTACH '{libpq}' AS {self.PG_ALIAS} (TYPE POSTGRES)")

    def _detach(self, con: Any) -> None:
        """Detach the PostgreSQL database, ignoring errors."""
        try:
            con.execute(f"DETACH {self.PG_ALIAS}")
        except Exception:
            logger.debug("DETACH %s ignored (may already be detached)", self.PG_ALIAS)

    def _row_count(self, con: Any, view_name: str) -> int:
        """Return the number of rows in *view_name*."""
        result = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
        return result[0] if result else 0

    def _table_exists(self, con: Any) -> bool:
        """Check whether the target table already exists in PostgreSQL."""
        schema, table_name = self._split_table()
        try:
            row = con.execute(
                f"SELECT 1 FROM {self.PG_ALIAS}.information_schema.tables "
                f"WHERE table_schema = '{schema}' AND table_name = '{table_name}'"
            ).fetchone()
            return row is not None
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def materialize(self, con: Any, view_name: str) -> WriteResult:
        """Main entry point. Dispatches to the correct mode method.

        Args:
            con: DuckDB connection.
            view_name: Name of the DuckDB view containing the data to write.

        Returns:
            WriteResult with row count and duration.
        """
        mode = self.mat_config.mode
        if mode == PostgresMaterializationMode.FULL:
            return self.materialize_full(con, view_name)
        elif mode == PostgresMaterializationMode.INCREMENTAL_BY_TIME:
            return self.materialize_incremental(con, view_name)
        elif mode == PostgresMaterializationMode.UPSERT_BY_KEY:
            return self.materialize_upsert(con, view_name)
        else:
            raise PostgresMaterializationError(f"Unsupported mode: {mode}")

    def materialize_full(self, con: Any, view_name: str) -> WriteResult:
        """Full table replacement: DROP TABLE (if exists) + CREATE TABLE AS SELECT.

        Args:
            con: DuckDB connection.
            view_name: Name of the DuckDB view containing the data to write.

        Returns:
            WriteResult with row count and duration.
        """
        start = time.time()
        qualified = self._qualified_table()
        cascade = " CASCADE" if self.mat_config.cascade else ""

        self._attach(con)
        try:
            row_count = self._row_count(con, view_name)

            if not self.mat_config.create_table and not self._table_exists(con):
                raise PostgresMaterializationError(
                    f"Table {self.mat_config.table} does not exist and "
                    f"create_table is disabled."
                )

            con.execute(f"DROP TABLE IF EXISTS {qualified}{cascade}")
            con.execute(f"CREATE TABLE {qualified} AS SELECT * FROM {view_name}")

            duration = time.time() - start
            logger.info(
                "Full materialization complete: %s (%d rows, %.2fs)",
                self.mat_config.table,
                row_count,
                duration,
            )
            return WriteResult(
                success=True,
                row_count=row_count,
                duration_seconds=duration,
            )
        except PostgresMaterializationError:
            raise
        except Exception as exc:
            raise PostgresMaterializationError(
                f"Full materialization failed for {self.mat_config.table}: {exc}"
            ) from exc
        finally:
            self._detach(con)

    def materialize_incremental(self, con: Any, view_name: str) -> WriteResult:
        """Incremental by time: DELETE rows in time range (with lookback) + INSERT.

        Args:
            con: DuckDB connection.
            view_name: Name of the DuckDB view containing the data to write.

        Returns:
            WriteResult with row count and duration.
        """
        start = time.time()
        qualified = self._qualified_table()
        time_column = self.mat_config.time_column
        lookback = self.mat_config.lookback

        self._attach(con)
        try:
            # Auto-create if table does not exist
            if not self._table_exists(con):
                if not self.mat_config.create_table:
                    raise PostgresMaterializationError(
                        f"Table {self.mat_config.table} does not exist and "
                        f"create_table is disabled."
                    )
                con.execute(
                    f"CREATE TABLE {qualified} AS SELECT * FROM {view_name} WHERE 1=0"
                )

            # Determine the minimum time in the incoming data
            min_time_row = con.execute(
                f"SELECT MIN({time_column}) FROM {view_name}"
            ).fetchone()
            min_time = min_time_row[0] if min_time_row and min_time_row[0] is not None else None

            if min_time is not None:
                con.execute(
                    f"DELETE FROM {qualified} "
                    f"WHERE {time_column} >= CAST('{min_time}' AS TIMESTAMP) - INTERVAL '{lookback} days'"
                )

            con.execute(f"INSERT INTO {qualified} SELECT * FROM {view_name}")
            row_count = self._row_count(con, view_name)

            duration = time.time() - start
            logger.info(
                "Incremental materialization complete: %s (%d rows, %.2fs)",
                self.mat_config.table,
                row_count,
                duration,
            )
            return WriteResult(
                success=True,
                row_count=row_count,
                duration_seconds=duration,
            )
        except PostgresMaterializationError:
            raise
        except Exception as exc:
            raise PostgresMaterializationError(
                f"Incremental materialization failed for {self.mat_config.table}: {exc}"
            ) from exc
        finally:
            self._detach(con)

    def materialize_upsert(self, con: Any, view_name: str) -> WriteResult:
        """Upsert by key, as a single transactional ``INSERT ... ON CONFLICT``.

        Replaces an earlier ``DELETE ... USING`` + ``INSERT`` implementation that
        ran without an explicit transaction. That sequence left a window in
        which a matched row had been deleted but not yet reinserted, so a
        concurrent reader saw the entity **missing** rather than stale. For a
        latest-only online store that is a wrong answer, not a slow one.

        ``ON CONFLICT DO UPDATE`` removes the window entirely: rows are updated
        in place and are never absent. It requires a unique constraint on the
        conflict target, so auto-created tables now get a primary key --
        ``CREATE TABLE AS SELECT`` creates none, which previously left the
        table without a key or indexes.

        Args:
            con: DuckDB connection.
            view_name: Name of the DuckDB view containing the data to write.

        Returns:
            WriteResult with row count and duration.
        """
        start = time.time()
        qualified = self._qualified_table()
        unique_keys = self.mat_config.unique_keys
        if not unique_keys:
            raise PostgresMaterializationError(
                f"upsert_by_key requires unique_keys for {self.mat_config.table}"
            )

        self._attach(con)
        try:
            columns = [
                d[0] for d in con.execute(f"SELECT * FROM {view_name} LIMIT 0").description
            ]
            missing = [k for k in unique_keys if k not in columns]
            if missing:
                raise PostgresMaterializationError(
                    f"unique_keys {missing} are not present in {view_name}"
                )

            if not self._table_exists(con):
                if not self.mat_config.create_table:
                    raise PostgresMaterializationError(
                        f"Table {self.mat_config.table} does not exist and "
                        f"create_table is disabled."
                    )
                con.execute(
                    f"CREATE TABLE {qualified} AS SELECT * FROM {view_name} WHERE 1=0"
                )
                # ON CONFLICT needs a unique constraint on the conflict target,
                # and a point lookup needs the index. CTAS provides neither.
                schema, table_name = self._split_table()
                key_list = ", ".join(f'"{k}"' for k in unique_keys)
                self._remote_execute(
                    con,
                    f'ALTER TABLE "{schema}"."{table_name}" '
                    f'ADD CONSTRAINT "{table_name}_pk" PRIMARY KEY ({key_list})',
                )
                # The constraint was added out-of-band, so the attached catalog
                # still has the pre-ALTER definition and ON CONFLICT would fail
                # with "not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT".
                # Refresh it before relying on the constraint.
                con.execute("CALL pg_clear_cache()")

            col_list = ", ".join(f'"{c}"' for c in columns)
            key_list = ", ".join(f'"{k}"' for k in unique_keys)
            updates = ", ".join(
                f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in unique_keys
            )
            if not updates:
                raise PostgresMaterializationError(
                    f"{view_name} has no non-key columns to update"
                )

            con.execute("BEGIN TRANSACTION")
            try:
                con.execute(
                    f"INSERT INTO {qualified} ({col_list}) "
                    f"SELECT {col_list} FROM {view_name} "
                    f"ON CONFLICT ({key_list}) DO UPDATE SET {updates}"
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

            # Verify from the remote side. _row_count reads the LOCAL view, so
            # on its own it is not evidence that anything was written.
            row_count = self._row_count(con, view_name)
            remote_total = con.execute(
                f"SELECT count(*) FROM {qualified}"
            ).fetchone()[0]
            if remote_total < row_count:
                raise PostgresMaterializationError(
                    f"remote verification failed for {self.mat_config.table}: "
                    f"staged {row_count} rows but the target holds only "
                    f"{remote_total}"
                )

            duration = time.time() - start
            logger.info(
                "Upsert materialization complete: %s (%d rows, %.2fs)",
                self.mat_config.table,
                row_count,
                duration,
            )
            return WriteResult(
                success=True,
                row_count=row_count,
                duration_seconds=duration,
            )
        except PostgresMaterializationError:
            raise
        except Exception as exc:
            raise PostgresMaterializationError(
                f"Upsert materialization failed for {self.mat_config.table}: {exc}"
            ) from exc
        finally:
            self._detach(con)

    def _remote_execute(self, con: Any, sql: str) -> None:
        """Run *sql* natively on the PostgreSQL server."""
        con.execute(
            f"CALL postgres_execute('{self.PG_ALIAS}', '{sql.replace(chr(39), chr(39) * 2)}')"
        )
