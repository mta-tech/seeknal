"""
Unit tests for PostgreSQL materialization configuration.

Tests cover:
- All three modes (full, incremental_by_time, upsert_by_key)
- Required field validation (connection, table)
- Table name 2-part format validation
- Mode-specific field requirements
- from_dict parsing with string-to-enum conversion
- Invalid configuration rejection
- SQL injection prevention in table names
"""

import pytest  # ty: ignore[unresolved-import]

from seeknal.workflow.materialization.pg_config import (  # ty: ignore[unresolved-import]
    PostgresConfigError,
    PostgresMaterializationConfig,
    PostgresMaterializationMode,
)


# =============================================================================
# Enum Tests
# =============================================================================


class TestPostgresMaterializationMode:
    """Tests for PostgresMaterializationMode enum."""

    def test_mode_values(self):
        assert PostgresMaterializationMode.FULL.value == "full"
        assert PostgresMaterializationMode.INCREMENTAL_BY_TIME.value == "incremental_by_time"
        assert PostgresMaterializationMode.UPSERT_BY_KEY.value == "upsert_by_key"

    def test_mode_from_string(self):
        assert PostgresMaterializationMode("full") == PostgresMaterializationMode.FULL
        assert (
            PostgresMaterializationMode("incremental_by_time")
            == PostgresMaterializationMode.INCREMENTAL_BY_TIME
        )
        assert (
            PostgresMaterializationMode("upsert_by_key")
            == PostgresMaterializationMode.UPSERT_BY_KEY
        )

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError):
            PostgresMaterializationMode("invalid_mode")


# =============================================================================
# Default Values Tests
# =============================================================================


class TestPostgresMaterializationConfigDefaults:
    """Tests for default values."""

    def test_defaults(self):
        config = PostgresMaterializationConfig()
        assert config.type == "postgresql"
        assert config.connection == ""
        assert config.table == ""
        assert config.mode == PostgresMaterializationMode.FULL
        assert config.time_column is None
        assert config.lookback == 0
        assert config.unique_keys == []
        assert config.create_table is True
        assert config.cascade is False


# =============================================================================
# Validation Tests - Required Fields
# =============================================================================


class TestValidateRequiredFields:
    """Tests for required field validation."""

    def test_missing_connection(self):
        config = PostgresMaterializationConfig(
            connection="",
            table="public.orders",
        )
        with pytest.raises(PostgresConfigError, match="connection"):
            config.validate()

    def test_missing_table(self):
        config = PostgresMaterializationConfig(
            connection="my_pg",
            table="",
        )
        with pytest.raises(PostgresConfigError, match="table"):
            config.validate()


# =============================================================================
# Validation Tests - Table Name Format
# =============================================================================


class TestValidateTableName:
    """Tests for table name 2-part format validation."""

    def test_valid_table_names(self):
        valid_names = [
            "public.orders",
            "my_schema.my_table",
            "analytics.user_events",
            "_private.data_01",
        ]
        for name in valid_names:
            config = PostgresMaterializationConfig(connection="pg", table=name)
            config.validate()  # Should not raise

    def test_single_part_table_name(self):
        config = PostgresMaterializationConfig(connection="pg", table="orders")
        with pytest.raises(PostgresConfigError, match="2-part format"):
            config.validate()

    def test_three_part_table_name(self):
        config = PostgresMaterializationConfig(
            connection="pg", table="db.schema.table"
        )
        with pytest.raises(PostgresConfigError, match="2-part format"):
            config.validate()

    def test_table_name_starting_with_number(self):
        config = PostgresMaterializationConfig(connection="pg", table="public.1orders")
        with pytest.raises(PostgresConfigError, match="2-part format"):
            config.validate()

    def test_table_name_with_special_characters(self):
        config = PostgresMaterializationConfig(connection="pg", table="public.my-table")
        with pytest.raises(PostgresConfigError, match="2-part format"):
            config.validate()

    def test_empty_schema_part(self):
        config = PostgresMaterializationConfig(connection="pg", table=".orders")
        with pytest.raises(PostgresConfigError, match="2-part format"):
            config.validate()

    def test_empty_table_part(self):
        config = PostgresMaterializationConfig(connection="pg", table="public.")
        with pytest.raises(PostgresConfigError, match="2-part format"):
            config.validate()


# =============================================================================
# Validation Tests - SQL Injection Prevention
# =============================================================================


class TestValidateTableNameSQLInjection:
    """Tests for SQL injection prevention in table names."""

    def test_semicolon_injection(self):
        config = PostgresMaterializationConfig(
            connection="pg", table="public.orders; DROP TABLE users"
        )
        with pytest.raises(PostgresConfigError):
            config.validate()

    def test_comment_injection(self):
        config = PostgresMaterializationConfig(
            connection="pg", table="public.orders--drop"
        )
        with pytest.raises(PostgresConfigError):
            config.validate()

    def test_block_comment_injection(self):
        config = PostgresMaterializationConfig(
            connection="pg", table="public.orders/*evil*/"
        )
        with pytest.raises(PostgresConfigError):
            config.validate()


# =============================================================================
# Validation Tests - Full Mode
# =============================================================================


class TestValidateFullMode:
    """Tests for FULL mode validation."""

    def test_full_mode_minimal(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.orders",
            mode=PostgresMaterializationMode.FULL,
        )
        config.validate()  # Should not raise

    def test_full_mode_with_all_options(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.orders",
            mode=PostgresMaterializationMode.FULL,
            create_table=False,
            cascade=True,
        )
        config.validate()  # Should not raise


# =============================================================================
# Validation Tests - Incremental By Time Mode
# =============================================================================


class TestValidateIncrementalByTimeMode:
    """Tests for INCREMENTAL_BY_TIME mode validation."""

    def test_incremental_valid(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.events",
            mode=PostgresMaterializationMode.INCREMENTAL_BY_TIME,
            time_column="created_at",
            lookback=7,
        )
        config.validate()  # Should not raise

    def test_incremental_missing_time_column(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.events",
            mode=PostgresMaterializationMode.INCREMENTAL_BY_TIME,
        )
        with pytest.raises(PostgresConfigError, match="time_column"):
            config.validate()

    def test_incremental_invalid_time_column(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.events",
            mode=PostgresMaterializationMode.INCREMENTAL_BY_TIME,
            time_column="invalid;col",
        )
        with pytest.raises(PostgresConfigError, match="time_column"):
            config.validate()

    def test_incremental_zero_lookback(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.events",
            mode=PostgresMaterializationMode.INCREMENTAL_BY_TIME,
            time_column="created_at",
            lookback=0,
        )
        config.validate()  # Should not raise


# =============================================================================
# Validation Tests - Upsert By Key Mode
# =============================================================================


class TestValidateUpsertByKeyMode:
    """Tests for UPSERT_BY_KEY mode validation."""

    def test_upsert_valid_single_key(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.users",
            mode=PostgresMaterializationMode.UPSERT_BY_KEY,
            unique_keys=["user_id"],
        )
        config.validate()  # Should not raise

    def test_upsert_valid_composite_key(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.events",
            mode=PostgresMaterializationMode.UPSERT_BY_KEY,
            unique_keys=["user_id", "event_date"],
        )
        config.validate()  # Should not raise

    def test_upsert_missing_unique_keys(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.users",
            mode=PostgresMaterializationMode.UPSERT_BY_KEY,
            unique_keys=[],
        )
        with pytest.raises(PostgresConfigError, match="unique_keys"):
            config.validate()

    def test_upsert_invalid_key_name(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.users",
            mode=PostgresMaterializationMode.UPSERT_BY_KEY,
            unique_keys=["user_id", "bad;key"],
        )
        with pytest.raises(PostgresConfigError, match="unique key"):
            config.validate()


# =============================================================================
# Validation Tests - Lookback
# =============================================================================


class TestValidateLookback:
    """Tests for lookback validation."""

    def test_negative_lookback(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.events",
            mode=PostgresMaterializationMode.FULL,
            lookback=-1,
        )
        with pytest.raises(PostgresConfigError, match="lookback"):
            config.validate()

    def test_zero_lookback(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.events",
            mode=PostgresMaterializationMode.FULL,
            lookback=0,
        )
        config.validate()  # Should not raise

    def test_positive_lookback(self):
        config = PostgresMaterializationConfig(
            connection="pg",
            table="public.events",
            mode=PostgresMaterializationMode.FULL,
            lookback=30,
        )
        config.validate()  # Should not raise


# =============================================================================
# from_dict Tests
# =============================================================================


class TestFromDict:
    """Tests for from_dict class method."""

    def test_from_dict_full_mode(self):
        data = {
            "type": "postgresql",
            "connection": "my_pg",
            "table": "public.orders",
            "mode": "full",
            "create_table": True,
            "cascade": False,
        }
        config = PostgresMaterializationConfig.from_dict(data)
        assert config.type == "postgresql"
        assert config.connection == "my_pg"
        assert config.table == "public.orders"
        assert config.mode == PostgresMaterializationMode.FULL
        assert config.create_table is True
        assert config.cascade is False

    def test_from_dict_incremental_mode(self):
        data = {
            "connection": "my_pg",
            "table": "analytics.events",
            "mode": "incremental_by_time",
            "time_column": "event_ts",
            "lookback": 14,
        }
        config = PostgresMaterializationConfig.from_dict(data)
        assert config.mode == PostgresMaterializationMode.INCREMENTAL_BY_TIME
        assert config.time_column == "event_ts"
        assert config.lookback == 14

    def test_from_dict_upsert_mode(self):
        data = {
            "connection": "my_pg",
            "table": "public.users",
            "mode": "upsert_by_key",
            "unique_keys": ["user_id", "region"],
        }
        config = PostgresMaterializationConfig.from_dict(data)
        assert config.mode == PostgresMaterializationMode.UPSERT_BY_KEY
        assert config.unique_keys == ["user_id", "region"]

    def test_from_dict_defaults(self):
        data = {}
        config = PostgresMaterializationConfig.from_dict(data)
        assert config.type == "postgresql"
        assert config.connection == ""
        assert config.table == ""
        assert config.mode == PostgresMaterializationMode.FULL
        assert config.time_column is None
        assert config.lookback == 0
        assert config.unique_keys == []
        assert config.create_table is True
        assert config.cascade is False

    def test_from_dict_invalid_mode(self):
        data = {
            "connection": "my_pg",
            "table": "public.orders",
            "mode": "invalid_mode",
        }
        with pytest.raises(PostgresConfigError, match="Invalid PostgreSQL materialization mode"):
            PostgresMaterializationConfig.from_dict(data)

    def test_from_dict_cascade_true(self):
        data = {
            "connection": "my_pg",
            "table": "public.orders",
            "cascade": True,
        }
        config = PostgresMaterializationConfig.from_dict(data)
        assert config.cascade is True

    def test_from_dict_create_table_false(self):
        data = {
            "connection": "my_pg",
            "table": "public.orders",
            "create_table": False,
        }
        config = PostgresMaterializationConfig.from_dict(data)
        assert config.create_table is False

    def test_from_dict_then_validate(self):
        """Test the full flow: parse from dict, then validate."""
        data = {
            "connection": "warehouse_pg",
            "table": "analytics.daily_metrics",
            "mode": "incremental_by_time",
            "time_column": "metric_date",
            "lookback": 3,
            "create_table": True,
        }
        config = PostgresMaterializationConfig.from_dict(data)
        config.validate()  # Should not raise

    def test_from_dict_then_validate_fails(self):
        """Test parse succeeds but validation catches missing fields."""
        data = {
            "table": "analytics.daily_metrics",
            "mode": "upsert_by_key",
            # Missing: connection, unique_keys
        }
        config = PostgresMaterializationConfig.from_dict(data)
        with pytest.raises(PostgresConfigError, match="connection"):
            config.validate()
