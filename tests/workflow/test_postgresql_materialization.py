"""
Unit tests for PostgreSQL materialization helper.

Tests cover:
- Full, incremental, and upsert materialization modes
- Table name splitting and qualified name construction
- ATTACH / DETACH lifecycle (success and failure paths)
- Auto-create vs. create_table=False behavior
- CASCADE option for DROP TABLE
- Row count and duration in WriteResult
- Temp table cleanup on upsert failure
- Correct SQL sequence verification
- Password masking in log output
- Postgres extension loading
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest  # ty: ignore[unresolved-import]

from seeknal.workflow.materialization.pg_config import (  # ty: ignore[unresolved-import]
    PostgresMaterializationConfig,
    PostgresMaterializationMode,
)
from seeknal.workflow.materialization.postgresql import (  # ty: ignore[unresolved-import]
    PostgresMaterializationError,
    PostgresMaterializationHelper,
)


# ---------------------------------------------------------------------------
# Lightweight stand-in for PostgreSQLConfig so tests don't depend on the
# connections package being resolvable by the type checker.
# ---------------------------------------------------------------------------


@dataclass
class _FakePostgreSQLConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "testdb"
    user: str = "testuser"
    password: str = "testpass"

    def to_libpq_string(self) -> str:
        parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"dbname={self.database}",
            f"user={self.user}",
        ]
        if self.password:
            parts.append(f"password={self.password}")
        return " ".join(parts)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pg_config():
    return _FakePostgreSQLConfig()


@pytest.fixture
def mock_con():
    """A MagicMock DuckDB connection whose execute().fetchone() returns (42,).

    ``description`` is populated because the upsert path introspects the source
    relation's columns to build an explicit ON CONFLICT ... DO UPDATE clause.
    """
    con = MagicMock()
    con.execute.return_value = MagicMock()
    con.execute.return_value.fetchone.return_value = (42,)
    con.execute.return_value.description = [
        ("user_id",),
        ("role_id",),
        ("name",),
        ("updated_at",),
    ]
    return con


@pytest.fixture
def mat_config_full():
    return PostgresMaterializationConfig(
        connection="test_pg",
        table="public.orders",
        mode=PostgresMaterializationMode.FULL,
    )


@pytest.fixture
def mat_config_full_cascade():
    return PostgresMaterializationConfig(
        connection="test_pg",
        table="public.orders",
        mode=PostgresMaterializationMode.FULL,
        cascade=True,
    )


@pytest.fixture
def mat_config_full_no_create():
    return PostgresMaterializationConfig(
        connection="test_pg",
        table="public.orders",
        mode=PostgresMaterializationMode.FULL,
        create_table=False,
    )


@pytest.fixture
def mat_config_incremental():
    return PostgresMaterializationConfig(
        connection="test_pg",
        table="public.events",
        mode=PostgresMaterializationMode.INCREMENTAL_BY_TIME,
        time_column="event_time",
        lookback=3,
    )


@pytest.fixture
def mat_config_upsert():
    return PostgresMaterializationConfig(
        connection="test_pg",
        table="public.users",
        mode=PostgresMaterializationMode.UPSERT_BY_KEY,
        unique_keys=["user_id"],
    )


@pytest.fixture
def mat_config_upsert_multi_key():
    return PostgresMaterializationConfig(
        connection="test_pg",
        table="public.user_roles",
        mode=PostgresMaterializationMode.UPSERT_BY_KEY,
        unique_keys=["user_id", "role_id"],
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sql_calls(mock_con: MagicMock) -> list[str]:
    """Return a flat list of SQL strings passed to ``con.execute(...)``."""
    return [c.args[0] for c in mock_con.execute.call_args_list]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPostgresMaterializationHelper:
    """Tests for PostgresMaterializationHelper."""

    # -- Full mode ---------------------------------------------------------

    def test_materialize_full_basic(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        result = helper.materialize_full(mock_con, "my_view")

        sqls = _sql_calls(mock_con)
        assert any("INSTALL postgres" in s for s in sqls)
        assert any("ATTACH" in s and "TYPE POSTGRES" in s for s in sqls)
        assert any("DROP TABLE IF EXISTS pg_db.public.orders" in s for s in sqls)
        assert any("CREATE TABLE pg_db.public.orders AS SELECT * FROM my_view" in s for s in sqls)
        assert any("DETACH" in s for s in sqls)
        assert result.success is True

    def test_materialize_full_with_cascade(self, pg_config, mock_con, mat_config_full_cascade):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full_cascade)
        helper.materialize_full(mock_con, "v")

        sqls = _sql_calls(mock_con)
        drop_sqls = [s for s in sqls if "DROP TABLE IF EXISTS" in s]
        assert len(drop_sqls) == 1
        assert "CASCADE" in drop_sqls[0]

    def test_materialize_full_without_cascade(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        helper.materialize_full(mock_con, "v")

        sqls = _sql_calls(mock_con)
        drop_sqls = [s for s in sqls if "DROP TABLE IF EXISTS" in s]
        assert len(drop_sqls) == 1
        assert "CASCADE" not in drop_sqls[0]

    def test_materialize_full_auto_create_disabled(
        self, pg_config, mat_config_full_no_create
    ):
        """When create_table=False and the table doesn't exist, raise an error."""
        con = MagicMock()

        def side_effect(sql):
            result = MagicMock()
            # _table_exists query returns None (table not found)
            if "information_schema.tables" in sql:
                result.fetchone.return_value = None
            else:
                result.fetchone.return_value = (0,)
            return result

        con.execute.side_effect = side_effect

        helper = PostgresMaterializationHelper(pg_config, mat_config_full_no_create)
        with pytest.raises(PostgresMaterializationError, match="create_table is disabled"):
            helper.materialize_full(con, "v")

    # -- Incremental mode --------------------------------------------------

    def test_materialize_incremental_basic(self, pg_config, mock_con, mat_config_incremental):
        helper = PostgresMaterializationHelper(pg_config, mat_config_incremental)
        result = helper.materialize_incremental(mock_con, "inc_view")

        sqls = _sql_calls(mock_con)
        assert any("DELETE FROM pg_db.public.events" in s for s in sqls)
        assert any("INSERT INTO pg_db.public.events" in s for s in sqls)
        assert result.success is True

    def test_materialize_incremental_with_lookback(
        self, pg_config, mock_con, mat_config_incremental
    ):
        helper = PostgresMaterializationHelper(pg_config, mat_config_incremental)
        helper.materialize_incremental(mock_con, "inc_view")

        sqls = _sql_calls(mock_con)
        delete_sqls = [s for s in sqls if "DELETE FROM" in s]
        assert len(delete_sqls) == 1
        assert "INTERVAL '3 days'" in delete_sqls[0]
        assert "event_time" in delete_sqls[0]

    def test_materialize_incremental_table_not_exists(self, pg_config, mat_config_incremental):
        """When the table doesn't exist and create_table=True, auto-create it."""
        con = MagicMock()

        def side_effect(sql):
            result = MagicMock()
            if "information_schema.tables" in sql:
                result.fetchone.return_value = None  # table doesn't exist
            elif "MIN(" in sql:
                result.fetchone.return_value = (None,)
            else:
                result.fetchone.return_value = (10,)
            return result

        con.execute.side_effect = side_effect

        helper = PostgresMaterializationHelper(pg_config, mat_config_incremental)
        result = helper.materialize_incremental(con, "inc_view")

        sqls = _sql_calls(con)
        # Should CREATE TABLE ... WHERE 1=0 (empty schema copy)
        create_sqls = [s for s in sqls if "CREATE TABLE pg_db" in s and "WHERE 1=0" in s]
        assert len(create_sqls) == 1
        assert result.success is True

    # -- Upsert mode -------------------------------------------------------

    def test_materialize_upsert_uses_on_conflict_not_delete_insert(
        self, pg_config, mock_con, mat_config_upsert
    ):
        """The DELETE ... USING + INSERT sequence left a window in which a
        matched row was deleted but not yet reinserted, so a concurrent reader
        saw the entity missing rather than stale. ON CONFLICT DO UPDATE removes
        that window: rows are updated in place and are never absent."""
        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        result = helper.materialize_upsert(mock_con, "upsert_view")

        sqls = _sql_calls(mock_con)
        assert any("ON CONFLICT" in s and "DO UPDATE SET" in s for s in sqls)
        assert not any("DELETE FROM" in s for s in sqls), (
            "the delete/insert window must not reappear"
        )
        assert result.success is True

    def test_materialize_upsert_is_transactional(
        self, pg_config, mock_con, mat_config_upsert
    ):
        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        helper.materialize_upsert(mock_con, "upsert_view")
        sqls = _sql_calls(mock_con)
        assert any("BEGIN TRANSACTION" in s for s in sqls)
        assert any(s.strip() == "COMMIT" for s in sqls)

    def test_materialize_upsert_updates_every_non_key_column(
        self, pg_config, mock_con, mat_config_upsert
    ):
        """Omitting a column would produce a logically mixed old/new row even
        though PostgreSQL row visibility remains atomic."""
        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        helper.materialize_upsert(mock_con, "upsert_view")
        (upsert,) = [s for s in _sql_calls(mock_con) if "ON CONFLICT" in s]
        for col in ("role_id", "name", "updated_at"):
            assert f'"{col}" = EXCLUDED."{col}"' in upsert
        assert '"user_id" = EXCLUDED."user_id"' not in upsert  # it is the key

    def test_materialize_upsert_multiple_keys(
        self, pg_config, mock_con, mat_config_upsert_multi_key
    ):
        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert_multi_key)
        helper.materialize_upsert(mock_con, "v")

        (upsert,) = [s for s in _sql_calls(mock_con) if "ON CONFLICT" in s]
        assert 'ON CONFLICT ("user_id", "role_id")' in upsert

    def test_materialize_upsert_requires_unique_keys(self, pg_config, mock_con):
        cfg = PostgresMaterializationConfig(
            connection="test_pg",
            table="public.users",
            mode=PostgresMaterializationMode.UPSERT_BY_KEY,
            unique_keys=[],
        )
        helper = PostgresMaterializationHelper(pg_config, cfg)
        with pytest.raises(PostgresMaterializationError, match="requires unique_keys"):
            helper.materialize_upsert(mock_con, "v")

    def test_materialize_upsert_rejects_keys_absent_from_source(
        self, pg_config, mock_con
    ):
        cfg = PostgresMaterializationConfig(
            connection="test_pg",
            table="public.users",
            mode=PostgresMaterializationMode.UPSERT_BY_KEY,
            unique_keys=["nonexistent"],
        )
        helper = PostgresMaterializationHelper(pg_config, cfg)
        with pytest.raises(PostgresMaterializationError, match="not present in"):
            helper.materialize_upsert(mock_con, "v")

    def test_materialize_upsert_rolls_back_on_failure(self, pg_config, mat_config_upsert):
        """A failed upsert must roll back rather than leave a partial write.

        Replaces an earlier test that asserted a temp table was dropped. The
        implementation no longer stages through a temp table, so the property
        worth guarding is transactional: nothing is left half-applied.
        """
        con = MagicMock()

        def side_effect(sql):
            result = MagicMock()
            result.fetchone.return_value = (1,)
            result.description = [("user_id",), ("name",)]
            if "ON CONFLICT" in sql:
                raise RuntimeError("pg error")
            return result

        con.execute.side_effect = side_effect

        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        with pytest.raises(PostgresMaterializationError):
            helper.materialize_upsert(con, "v")

        sqls = _sql_calls(con)
        assert any(s.strip() == "ROLLBACK" for s in sqls)
        assert not any(s.strip() == "COMMIT" for s in sqls)

    def test_materialize_upsert_verifies_keys_landed_remotely(
        self, pg_config, mat_config_upsert
    ):
        """Verification must check that incoming KEYS are present, not that the
        target's total row count is large enough.

        Comparing total rows against the incoming count is close to vacuous: a
        target already holding a million rows satisfies total >= incoming even
        if the upsert wrote nothing.
        """
        con = MagicMock()

        def side_effect(sql):
            result = MagicMock()
            result.description = [("user_id",), ("name",)]
            if "WHERE EXISTS" in sql:
                result.fetchone.return_value = (7,)  # only 7 keys landed
            elif "SELECT DISTINCT" in sql:
                result.fetchone.return_value = (10,)  # 10 distinct incoming
            else:
                result.fetchone.return_value = (10,)
            return result

        con.execute.side_effect = side_effect

        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        with pytest.raises(
            PostgresMaterializationError, match="remote verification failed"
        ):
            helper.materialize_upsert(con, "v")

    def test_materialize_upsert_passes_when_all_keys_landed(
        self, pg_config, mat_config_upsert
    ):
        con = MagicMock()

        def side_effect(sql):
            result = MagicMock()
            result.description = [("user_id",), ("name",)]
            result.fetchone.return_value = (10,)  # incoming == matched
            return result

        con.execute.side_effect = side_effect

        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        assert helper.materialize_upsert(con, "v").success is True

    def test_verification_is_not_satisfied_by_a_large_pre_existing_table(
        self, pg_config, mat_config_upsert
    ):
        """Regression guard for the weak check this replaced."""
        con = MagicMock()

        def side_effect(sql):
            result = MagicMock()
            result.description = [("user_id",), ("name",)]
            if "WHERE EXISTS" in sql:
                result.fetchone.return_value = (0,)  # nothing actually landed
            elif "SELECT DISTINCT" in sql:
                result.fetchone.return_value = (5,)
            else:
                result.fetchone.return_value = (1_000_000,)  # huge target
            return result

        con.execute.side_effect = side_effect

        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        with pytest.raises(PostgresMaterializationError, match="remote verification"):
            helper.materialize_upsert(con, "v")

    # -- Dispatch -----------------------------------------------------------

    def test_materialize_dispatches_to_full(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        result = helper.materialize(mock_con, "v")

        sqls = _sql_calls(mock_con)
        assert any("DROP TABLE IF EXISTS" in s for s in sqls)
        assert result.success is True

    def test_materialize_dispatches_to_incremental(
        self, pg_config, mock_con, mat_config_incremental
    ):
        helper = PostgresMaterializationHelper(pg_config, mat_config_incremental)
        result = helper.materialize(mock_con, "v")
        assert result.success is True

    def test_materialize_dispatches_to_upsert(self, pg_config, mock_con, mat_config_upsert):
        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        result = helper.materialize(mock_con, "v")
        assert result.success is True

    # -- WriteResult --------------------------------------------------------

    def test_materialize_returns_write_result(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        result = helper.materialize_full(mock_con, "v")

        assert result.success is True
        assert result.row_count == 42
        assert result.duration_seconds > 0
        assert result.error_message is None

    def test_row_count_in_result(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        result = helper.materialize(mock_con, "v")
        assert result.row_count == 42

    def test_duration_in_result(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        result = helper.materialize(mock_con, "v")
        assert isinstance(result.duration_seconds, float)
        assert result.duration_seconds >= 0

    # -- DETACH lifecycle ---------------------------------------------------

    def test_materialize_detach_on_success(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        helper.materialize_full(mock_con, "v")

        sqls = _sql_calls(mock_con)
        assert sqls[-1] == "DETACH pg_db"

    def test_materialize_detach_on_failure(self, pg_config, mat_config_full):
        """DETACH must be called even when the operation fails."""
        con = MagicMock()

        def side_effect(sql):
            result = MagicMock()
            result.fetchone.return_value = (0,)
            if "CREATE TABLE" in sql and "AS SELECT" in sql:
                raise RuntimeError("boom")
            return result

        con.execute.side_effect = side_effect

        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        with pytest.raises(PostgresMaterializationError):
            helper.materialize_full(con, "v")

        sqls = _sql_calls(con)
        assert "DETACH pg_db" in sqls

    # -- Table name ---------------------------------------------------------

    def test_table_name_splitting(self, pg_config, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        schema, table_name = helper._split_table()
        assert schema == "public"
        assert table_name == "orders"

    def test_qualified_table(self, pg_config, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        assert helper._qualified_table() == "pg_db.public.orders"

    # -- ATTACH / extension -------------------------------------------------

    def test_attach_with_correct_libpq_string(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        helper.materialize_full(mock_con, "v")

        sqls = _sql_calls(mock_con)
        attach_sqls = [s for s in sqls if "ATTACH" in s and "TYPE POSTGRES" in s]
        assert len(attach_sqls) == 1
        assert "host=localhost" in attach_sqls[0]
        assert "dbname=testdb" in attach_sqls[0]
        assert "user=testuser" in attach_sqls[0]

    def test_postgres_extension_loaded(self, pg_config, mock_con, mat_config_full):
        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        helper.materialize_full(mock_con, "v")

        sqls = _sql_calls(mock_con)
        assert any("INSTALL postgres" in s and "LOAD postgres" in s for s in sqls)

    # -- Logging / masking --------------------------------------------------

    def test_connection_string_masked_in_logs(self, pg_config, mat_config_full, caplog):
        """Passwords must not appear in log output."""
        con = MagicMock()
        con.execute.return_value.fetchone.return_value = (5,)

        helper = PostgresMaterializationHelper(pg_config, mat_config_full)
        with caplog.at_level(logging.DEBUG):
            helper.materialize_full(con, "v")

        for record in caplog.records:
            assert "testpass" not in record.getMessage()
