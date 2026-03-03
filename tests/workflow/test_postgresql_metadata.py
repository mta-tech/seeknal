"""
Unit tests for PostgreSQL incremental detection metadata queries.

Tests for the postgresql_metadata module.
"""
import pytest
from unittest.mock import patch, MagicMock

from seeknal.workflow.postgresql_metadata import (
    get_max_watermark,
    resolve_postgresql_connection,
)


class TestGetMaxWatermark:
    """Tests for get_max_watermark function."""

    @patch("seeknal.workflow.postgresql_metadata.duckdb")
    @patch("seeknal.workflow.postgresql_metadata.parse_postgresql_config")
    def test_get_max_watermark_returns_timestamp(
        self, mock_parse_config, mock_duckdb
    ):
        """Test that get_max_watermark returns the MAX value as string."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.to_libpq_string.return_value = "postgresql://user:pass@host:5432/db"
        mock_config.host = "localhost"
        mock_config.port = 5432
        mock_config.database = "test_db"
        mock_parse_config.return_value = mock_config

        # Setup mock connection
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con

        # Track execute calls
        execute_results = [
            None,  # LOAD postgres
            None,  # ATTACH
            ("2026-03-03 10:30:00",),  # SELECT MAX query
        ]

        def execute_side_effect(query):
            result = execute_results.pop(0)
            if result is None:
                return None
            mock_result = MagicMock()
            mock_result.fetchone.return_value = result
            return mock_result

        mock_con.execute.side_effect = execute_side_effect

        # Call function
        connection_params = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_pass",
        }

        result = get_max_watermark(
            connection_params=connection_params,
            schema="public",
            table="events",
            time_column="created_at",
        )

        # Verify result
        assert result == "2026-03-03 10:30:00"

    @patch("seeknal.workflow.postgresql_metadata.duckdb")
    @patch("seeknal.workflow.postgresql_metadata.parse_postgresql_config")
    def test_get_max_watermark_returns_none_on_empty_table(
        self, mock_parse_config, mock_duckdb
    ):
        """Test that get_max_watermark returns None for empty table (MAX returns NULL)."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.to_libpq_string.return_value = "postgresql://user:pass@host:5432/db"
        mock_config.host = "localhost"
        mock_config.port = 5432
        mock_config.database = "test_db"
        mock_parse_config.return_value = mock_config

        # Setup mock connection
        mock_con = MagicMock()
        mock_duckdb.connect.return_value = mock_con

        # Mock the query result - MAX returns NULL for empty table
        execute_results = [
            None,  # LOAD postgres
            None,  # ATTACH
            (None,),  # SELECT MAX returns NULL
        ]

        def execute_side_effect(query):
            result = execute_results.pop(0)
            if result is None:
                return None
            mock_result = MagicMock()
            mock_result.fetchone.return_value = result
            return mock_result

        mock_con.execute.side_effect = execute_side_effect

        # Call function
        connection_params = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
        }

        result = get_max_watermark(
            connection_params=connection_params,
            schema="public",
            table="empty_table",
            time_column="created_at",
        )

        # Verify result is None
        assert result is None

    @patch("seeknal.workflow.postgresql_metadata.duckdb")
    def test_get_max_watermark_returns_none_on_connection_error(self, mock_duckdb):
        """Test that get_max_watermark returns None on connection failure."""
        # Setup mock to raise exception
        mock_duckdb.connect.side_effect = Exception("Connection refused")

        # Call function
        connection_params = {
            "host": "unreachable",
            "port": 5432,
            "database": "test_db",
        }

        result = get_max_watermark(
            connection_params=connection_params,
            schema="public",
            table="events",
            time_column="created_at",
        )

        # Verify result is None (graceful failure)
        assert result is None

    def test_get_max_watermark_validates_column_name(self):
        """Test that get_max_watermark validates column name to prevent SQL injection."""
        connection_params = {"host": "localhost"}

        # Test with invalid column name (SQL injection attempt)
        # The actual exception is InvalidIdentifierError, which is a subclass of ValueError
        from seeknal.exceptions._validation_exceptions import InvalidIdentifierError

        with pytest.raises(InvalidIdentifierError):
            get_max_watermark(
                connection_params=connection_params,
                schema="public",
                table="events",
                time_column="created_at; DROP TABLE events; --",
            )


class TestResolvePostgreSQLConnection:
    """Tests for resolve_postgresql_connection function."""

    def test_resolve_inline_params(self):
        """Test resolving connection from inline params."""
        params = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_pass",
        }

        result = resolve_postgresql_connection(params, profile_loader=None)

        assert result == params

    def test_resolve_with_connection_reference_no_loader(self):
        """Test that connection reference falls back to inline params when no loader."""
        params = {
            "connection": "my_pg",
            "host": "fallback_host",
        }

        result = resolve_postgresql_connection(params, profile_loader=None)

        # Should return inline params since no loader to resolve connection
        assert "host" in result
        assert result["host"] == "fallback_host"

    def test_resolve_excludes_query_param(self):
        """Test that query param is excluded from resolved params."""
        params = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "query": "SELECT * FROM custom_view",
        }

        result = resolve_postgresql_connection(params, profile_loader=None)

        # Query should not be in result
        assert "query" not in result
        assert result["host"] == "localhost"
        assert result["port"] == 5432
