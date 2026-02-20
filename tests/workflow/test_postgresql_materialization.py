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
    """A MagicMock DuckDB connection whose execute().fetchone() returns (42,)."""
    con = MagicMock()
    con.execute.return_value = MagicMock()
    con.execute.return_value.fetchone.return_value = (42,)
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

    def test_materialize_upsert_basic(self, pg_config, mock_con, mat_config_upsert):
        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        result = helper.materialize_upsert(mock_con, "upsert_view")

        sqls = _sql_calls(mock_con)
        assert any("CREATE TEMP TABLE _seeknal_upsert" in s for s in sqls)
        assert any(
            "DELETE FROM pg_db.public.users" in s and "USING _seeknal_upsert" in s
            for s in sqls
        )
        assert any(
            "INSERT INTO pg_db.public.users SELECT * FROM _seeknal_upsert" in s for s in sqls
        )
        assert any("DROP TABLE IF EXISTS _seeknal_upsert" in s for s in sqls)
        assert result.success is True

    def test_materialize_upsert_multiple_keys(
        self, pg_config, mock_con, mat_config_upsert_multi_key
    ):
        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert_multi_key)
        helper.materialize_upsert(mock_con, "v")

        sqls = _sql_calls(mock_con)
        delete_sqls = [s for s in sqls if "DELETE FROM" in s]
        assert len(delete_sqls) == 1
        assert "t.user_id = s.user_id" in delete_sqls[0]
        assert "t.role_id = s.role_id" in delete_sqls[0]
        assert " AND " in delete_sqls[0]

    def test_materialize_upsert_cleanup_on_failure(self, pg_config, mat_config_upsert):
        """Temp table should be dropped even when the upsert fails."""
        con = MagicMock()

        def side_effect(sql):
            result = MagicMock()
            result.fetchone.return_value = (1,)
            # Blow up on the DELETE USING step
            if "DELETE FROM" in sql and "USING" in sql:
                raise RuntimeError("pg error")
            return result

        con.execute.side_effect = side_effect

        helper = PostgresMaterializationHelper(pg_config, mat_config_upsert)
        with pytest.raises(PostgresMaterializationError):
            helper.materialize_upsert(con, "v")

        # The cleanup DROP should still have been attempted
        sqls = _sql_calls(con)
        cleanup_drops = [s for s in sqls if "DROP TABLE IF EXISTS _seeknal_upsert" in s]
        assert len(cleanup_drops) >= 1

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
