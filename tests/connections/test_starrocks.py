"""Tests for StarRocks connection factory."""
from unittest.mock import MagicMock, patch

import pytest

from seeknal.connections.starrocks import (
    StarRocksConfig,
    _interpolate_env_vars,
    create_starrocks_connection,
    create_starrocks_connection_from_url,
    map_starrocks_type_to_duckdb,
    parse_starrocks_config,
    parse_starrocks_url,
    check_starrocks_connection,
)


class TestInterpolateEnvVars:
    """Test environment variable interpolation."""

    def test_interpolate_braces(self, monkeypatch):
        monkeypatch.setenv("MY_HOST", "db.example.com")
        assert _interpolate_env_vars("${MY_HOST}") == "db.example.com"

    def test_interpolate_dollar(self, monkeypatch):
        monkeypatch.setenv("MY_HOST", "db.example.com")
        assert _interpolate_env_vars("$MY_HOST") == "db.example.com"

    def test_interpolate_multiple(self, monkeypatch):
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "9030")
        result = _interpolate_env_vars("${HOST}:${PORT}")
        assert result == "localhost:9030"

    def test_interpolate_missing_var(self):
        with pytest.raises(ValueError, match="not set"):
            _interpolate_env_vars("${NONEXISTENT_VAR_12345}")

    def test_no_interpolation_needed(self):
        assert _interpolate_env_vars("plain_string") == "plain_string"

    def test_empty_string(self):
        assert _interpolate_env_vars("") == ""


class TestStarRocksConfig:
    """Test StarRocksConfig dataclass."""

    def test_defaults(self):
        config = StarRocksConfig()
        assert config.host == "localhost"
        assert config.port == 9030
        assert config.user == "root"
        assert config.password == ""
        assert config.database == ""

    def test_to_pymysql_kwargs(self):
        config = StarRocksConfig(
            host="sr.example.com",
            port=9030,
            user="admin",
            password="secret",
            database="analytics",
        )
        kwargs = config.to_pymysql_kwargs()
        assert kwargs["host"] == "sr.example.com"
        assert kwargs["port"] == 9030
        assert kwargs["user"] == "admin"
        assert kwargs["password"] == "secret"
        assert kwargs["database"] == "analytics"
        assert kwargs["charset"] == "utf8mb4"

    def test_to_pymysql_kwargs_no_password(self):
        config = StarRocksConfig(host="localhost", user="root")
        kwargs = config.to_pymysql_kwargs()
        assert "password" not in kwargs


class TestParseStarRocksConfig:
    """Test config dict parsing."""

    def test_basic_config(self, monkeypatch):
        config = {
            "host": "sr.local",
            "port": 9030,
            "user": "root",
            "password": "",
            "database": "test_db",
        }
        result = parse_starrocks_config(config)
        assert result.host == "sr.local"
        assert result.database == "test_db"

    def test_config_with_env_vars(self, monkeypatch):
        monkeypatch.setenv("SR_HOST", "sr.prod.com")
        monkeypatch.setenv("SR_USER", "admin")
        monkeypatch.setenv("SR_PASS", "s3cret")
        config = {
            "host": "${SR_HOST}",
            "user": "${SR_USER}",
            "password": "${SR_PASS}",
            "database": "prod_db",
        }
        result = parse_starrocks_config(config)
        assert result.host == "sr.prod.com"
        assert result.user == "admin"
        assert result.password == "s3cret"

    def test_config_defaults(self):
        result = parse_starrocks_config({})
        assert result.host == "localhost"
        assert result.port == 9030
        assert result.user == "root"


class TestParseStarRocksUrl:
    """Test URL parsing."""

    def test_full_url(self):
        url = "starrocks://admin:pass@sr.example.com:9030/analytics"
        result = parse_starrocks_url(url)
        assert result.host == "sr.example.com"
        assert result.port == 9030
        assert result.user == "admin"
        assert result.password == "pass"
        assert result.database == "analytics"

    def test_minimal_url(self):
        url = "starrocks://localhost"
        result = parse_starrocks_url(url)
        assert result.host == "localhost"
        assert result.port == 9030  # default
        assert result.user == "root"  # default

    def test_url_with_env_vars(self, monkeypatch):
        monkeypatch.setenv("SR_PASS", "secret123")
        url = "starrocks://admin:$SR_PASS@host:9030/db"
        result = parse_starrocks_url(url)
        assert result.password == "secret123"


class TestCreateConnection:
    """Test connection creation (mocked)."""

    def test_create_from_config(self):
        mock_pymysql = MagicMock()
        mock_conn = MagicMock()
        mock_pymysql.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"pymysql": mock_pymysql}):
            config = {"host": "localhost", "port": 9030, "user": "root", "database": "test"}
            result = create_starrocks_connection(config)

        assert result == mock_conn
        mock_pymysql.connect.assert_called_once()

    def test_create_from_url(self):
        mock_pymysql = MagicMock()
        mock_conn = MagicMock()
        mock_pymysql.connect.return_value = mock_conn

        with patch.dict("sys.modules", {"pymysql": mock_pymysql}):
            result = create_starrocks_connection_from_url("starrocks://root@localhost:9030/db")

        assert result == mock_conn
        mock_pymysql.connect.assert_called_once()


class TestConnectionTest:
    """Test connection test function."""

    @patch("seeknal.connections.starrocks.create_starrocks_connection")
    def test_success(self, mock_create):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [(1,), ("3.3.0-starrocks",)]
        mock_conn.cursor.return_value = mock_cursor
        mock_create.return_value = mock_conn

        success, msg = check_starrocks_connection({"host": "localhost"})
        assert success is True
        assert "3.3.0-starrocks" in msg

    @patch("seeknal.connections.starrocks.create_starrocks_connection")
    def test_failure(self, mock_create):
        mock_create.side_effect = Exception("Connection refused")

        success, msg = check_starrocks_connection({"host": "bad-host"})
        assert success is False
        assert "Connection refused" in msg


class TestTypeMapping:
    """Test StarRocks to DuckDB type mapping."""

    def test_basic_types(self):
        assert map_starrocks_type_to_duckdb("INT") == "INTEGER"
        assert map_starrocks_type_to_duckdb("BIGINT") == "BIGINT"
        assert map_starrocks_type_to_duckdb("BOOLEAN") == "BOOLEAN"
        assert map_starrocks_type_to_duckdb("VARCHAR") == "VARCHAR"

    def test_decimal_preserves_precision(self):
        assert map_starrocks_type_to_duckdb("DECIMAL(10,2)") == "DECIMAL(10,2)"

    def test_date_time_types(self):
        assert map_starrocks_type_to_duckdb("DATE") == "DATE"
        assert map_starrocks_type_to_duckdb("DATETIME") == "TIMESTAMP"

    def test_complex_types_fallback(self):
        assert map_starrocks_type_to_duckdb("JSON") == "VARCHAR"
        assert map_starrocks_type_to_duckdb("ARRAY") == "VARCHAR"

    def test_unknown_type_fallback(self):
        assert map_starrocks_type_to_duckdb("UNKNOWN_TYPE") == "VARCHAR"

    def test_case_insensitive(self):
        assert map_starrocks_type_to_duckdb("int") == "INTEGER"
        assert map_starrocks_type_to_duckdb("varchar(255)") == "VARCHAR"
