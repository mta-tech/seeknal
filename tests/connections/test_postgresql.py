"""Tests for PostgreSQL connection factory."""
from unittest.mock import MagicMock, patch

import pytest

from seeknal.connections.postgresql import (
    PostgreSQLConfig,
    _interpolate_env_vars,
    build_attach_string,
    check_postgresql_connection,
    map_postgresql_type_to_duckdb,
    mask_password,
    parse_postgresql_config,
    parse_postgresql_url,
)


class TestInterpolateEnvVars:
    """Test environment variable interpolation."""

    def test_interpolate_braces(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "db.example.com")
        assert _interpolate_env_vars("${PG_HOST}") == "db.example.com"

    def test_interpolate_dollar(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "db.example.com")
        assert _interpolate_env_vars("$PG_HOST") == "db.example.com"

    def test_interpolate_multiple(self, monkeypatch):
        monkeypatch.setenv("HOST", "pg.local")
        monkeypatch.setenv("PORT", "5432")
        result = _interpolate_env_vars("${HOST}:${PORT}")
        assert result == "pg.local:5432"

    def test_interpolate_missing_var(self):
        with pytest.raises(ValueError, match="not set"):
            _interpolate_env_vars("${NONEXISTENT_PG_VAR_99999}")

    def test_no_interpolation_needed(self):
        assert _interpolate_env_vars("plain_string") == "plain_string"

    def test_empty_string(self):
        assert _interpolate_env_vars("") == ""


class TestPostgreSQLConfig:
    """Test PostgreSQLConfig dataclass."""

    def test_defaults(self):
        config = PostgreSQLConfig()
        assert config.host == "localhost"
        assert config.port == 5432
        assert config.database == "postgres"
        assert config.user == "postgres"
        assert config.password == ""
        assert config.schema == "public"
        assert config.sslmode == "prefer"
        assert config.connect_timeout == 10

    def test_custom_values(self):
        config = PostgreSQLConfig(
            host="pg.prod.com",
            port=5433,
            database="analytics",
            user="admin",
            password="s3cret",
            schema="warehouse",
            sslmode="require",
            connect_timeout=30,
        )
        assert config.host == "pg.prod.com"
        assert config.port == 5433
        assert config.database == "analytics"
        assert config.user == "admin"
        assert config.password == "s3cret"
        assert config.schema == "warehouse"
        assert config.sslmode == "require"
        assert config.connect_timeout == 30

    def test_to_libpq_string_with_password(self):
        config = PostgreSQLConfig(
            host="pg.example.com",
            port=5432,
            database="mydb",
            user="admin",
            password="secret",
            sslmode="require",
            connect_timeout=15,
        )
        result = config.to_libpq_string()
        assert result == (
            "host=pg.example.com port=5432 dbname=mydb "
            "user=admin password=secret sslmode=require connect_timeout=15"
        )

    def test_to_libpq_string_without_password(self):
        config = PostgreSQLConfig(host="localhost", user="postgres")
        result = config.to_libpq_string()
        assert "password=" not in result
        assert "host=localhost" in result
        assert "user=postgres" in result

    def test_to_libpq_string_default_config(self):
        config = PostgreSQLConfig()
        result = config.to_libpq_string()
        assert result == (
            "host=localhost port=5432 dbname=postgres "
            "user=postgres sslmode=prefer connect_timeout=10"
        )


class TestParsePostgreSQLConfig:
    """Test config dict parsing."""

    def test_basic_config(self):
        config = {
            "host": "pg.local",
            "port": 5432,
            "user": "myuser",
            "password": "mypass",
            "database": "test_db",
        }
        result = parse_postgresql_config(config)
        assert result.host == "pg.local"
        assert result.port == 5432
        assert result.user == "myuser"
        assert result.password == "mypass"
        assert result.database == "test_db"

    def test_config_with_env_vars(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "pg.prod.com")
        monkeypatch.setenv("PG_USER", "admin")
        monkeypatch.setenv("PG_PASS", "s3cret")
        config = {
            "host": "${PG_HOST}",
            "user": "${PG_USER}",
            "password": "${PG_PASS}",
            "database": "prod_db",
        }
        result = parse_postgresql_config(config)
        assert result.host == "pg.prod.com"
        assert result.user == "admin"
        assert result.password == "s3cret"

    def test_config_defaults(self):
        result = parse_postgresql_config({})
        assert result.host == "localhost"
        assert result.port == 5432
        assert result.user == "postgres"
        assert result.database == "postgres"
        assert result.schema == "public"
        assert result.sslmode == "prefer"
        assert result.connect_timeout == 10

    def test_config_with_schema_and_sslmode(self):
        config = {
            "host": "pg.local",
            "schema": "warehouse",
            "sslmode": "require",
            "connect_timeout": 30,
        }
        result = parse_postgresql_config(config)
        assert result.schema == "warehouse"
        assert result.sslmode == "require"
        assert result.connect_timeout == 30

    def test_empty_host_raises(self):
        with pytest.raises(ValueError, match="host cannot be empty"):
            parse_postgresql_config({"host": ""})

    def test_negative_port_raises(self):
        with pytest.raises(ValueError, match="port must be between 1 and 65535"):
            parse_postgresql_config({"port": -1})

    def test_zero_port_raises(self):
        with pytest.raises(ValueError, match="port must be between 1 and 65535"):
            parse_postgresql_config({"port": 0})

    def test_port_too_large_raises(self):
        with pytest.raises(ValueError, match="port must be between 1 and 65535"):
            parse_postgresql_config({"port": 70000})

    def test_negative_connect_timeout_raises(self):
        with pytest.raises(ValueError, match="connect_timeout must be non-negative"):
            parse_postgresql_config({"connect_timeout": -5})


class TestParsePostgreSQLUrl:
    """Test URL parsing."""

    def test_full_url(self):
        url = "postgresql://admin:pass@pg.example.com:5432/analytics"
        result = parse_postgresql_url(url)
        assert result.host == "pg.example.com"
        assert result.port == 5432
        assert result.user == "admin"
        assert result.password == "pass"
        assert result.database == "analytics"

    def test_minimal_url(self):
        url = "postgresql://localhost"
        result = parse_postgresql_url(url)
        assert result.host == "localhost"
        assert result.port == 5432
        assert result.user == "postgres"
        assert result.database == "postgres"

    def test_url_with_env_vars(self, monkeypatch):
        monkeypatch.setenv("PG_PASS", "secret123")
        url = "postgresql://admin:$PG_PASS@host:5432/db"
        result = parse_postgresql_url(url)
        assert result.password == "secret123"

    def test_url_with_query_params(self):
        url = "postgresql://admin:pass@host:5432/db?sslmode=require&connect_timeout=30&schema=warehouse"
        result = parse_postgresql_url(url)
        assert result.sslmode == "require"
        assert result.connect_timeout == 30
        assert result.schema == "warehouse"

    def test_url_without_port(self):
        url = "postgresql://admin:pass@pg.example.com/mydb"
        result = parse_postgresql_url(url)
        assert result.host == "pg.example.com"
        assert result.port == 5432
        assert result.database == "mydb"

    def test_url_with_encoded_password(self):
        url = "postgresql://admin:p%40ss%23word@host:5432/db"
        result = parse_postgresql_url(url)
        assert result.password == "p@ss#word"

    def test_url_without_database(self):
        url = "postgresql://admin:pass@host:5432"
        result = parse_postgresql_url(url)
        assert result.database == "postgres"


class TestBuildAttachString:
    """Test build_attach_string."""

    def test_returns_libpq_string(self):
        config = PostgreSQLConfig(
            host="pg.local",
            port=5432,
            database="mydb",
            user="admin",
            password="secret",
        )
        result = build_attach_string(config)
        assert result == config.to_libpq_string()
        assert "host=pg.local" in result
        assert "dbname=mydb" in result


class TestMaskPassword:
    """Test password masking for safe logging."""

    def test_mask_libpq_format(self):
        conn_str = "host=pg.local port=5432 dbname=mydb user=admin password=s3cret sslmode=prefer"
        result = mask_password(conn_str)
        assert "s3cret" not in result
        assert "password=***" in result
        assert "host=pg.local" in result

    def test_mask_url_format(self):
        url = "postgresql://admin:s3cret@pg.local:5432/mydb"
        result = mask_password(url)
        assert "s3cret" not in result
        assert "admin:***@" in result

    def test_no_password_unchanged(self):
        conn_str = "host=pg.local port=5432 dbname=mydb user=admin sslmode=prefer"
        result = mask_password(conn_str)
        assert result == conn_str

    def test_mask_preserves_other_fields(self):
        conn_str = "host=pg.local port=5432 dbname=mydb user=admin password=secret123 sslmode=require"
        result = mask_password(conn_str)
        assert "host=pg.local" in result
        assert "port=5432" in result
        assert "dbname=mydb" in result
        assert "user=admin" in result
        assert "sslmode=require" in result
        assert "secret123" not in result


class TestCheckPostgreSQLConnection:
    """Test connection checker (mocked)."""

    def test_duckdb_not_installed(self):
        with patch("seeknal.connections.postgresql.importlib") as mock_importlib:
            mock_importlib.import_module.side_effect = ImportError("No module named 'duckdb'")
            success, msg = check_postgresql_connection(PostgreSQLConfig())
        assert success is False
        assert "duckdb is required" in msg

    def test_connection_success(self):
        mock_duckdb = MagicMock()
        mock_db = MagicMock()
        mock_duckdb.connect.return_value = mock_db

        with patch("seeknal.connections.postgresql.importlib") as mock_importlib:
            mock_importlib.import_module.return_value = mock_duckdb
            config = PostgreSQLConfig(host="pg.local", database="mydb")
            success, msg = check_postgresql_connection(config)

        assert success is True
        assert "pg.local" in msg
        assert "mydb" in msg
        mock_db.close.assert_called_once()

    def test_connection_failure(self):
        mock_duckdb = MagicMock()
        mock_db = MagicMock()
        mock_db.execute.side_effect = Exception("Connection refused")
        mock_duckdb.connect.return_value = mock_db

        with patch("seeknal.connections.postgresql.importlib") as mock_importlib:
            mock_importlib.import_module.return_value = mock_duckdb
            success, msg = check_postgresql_connection(PostgreSQLConfig())

        assert success is False
        assert "Connection refused" in msg


class TestTypeMapping:
    """Test PostgreSQL to DuckDB type mapping."""

    def test_basic_types(self):
        assert map_postgresql_type_to_duckdb("INTEGER") == "INTEGER"
        assert map_postgresql_type_to_duckdb("BIGINT") == "BIGINT"
        assert map_postgresql_type_to_duckdb("BOOLEAN") == "BOOLEAN"
        assert map_postgresql_type_to_duckdb("VARCHAR") == "VARCHAR"
        assert map_postgresql_type_to_duckdb("TEXT") == "VARCHAR"

    def test_numeric_preserves_precision(self):
        assert map_postgresql_type_to_duckdb("NUMERIC(10,2)") == "DECIMAL(10,2)"
        assert map_postgresql_type_to_duckdb("DECIMAL(5,3)") == "DECIMAL(5,3)"

    def test_date_time_types(self):
        assert map_postgresql_type_to_duckdb("DATE") == "DATE"
        assert map_postgresql_type_to_duckdb("TIMESTAMP") == "TIMESTAMP"
        assert map_postgresql_type_to_duckdb("TIMESTAMPTZ") == "TIMESTAMPTZ"

    def test_alias_types(self):
        assert map_postgresql_type_to_duckdb("BOOL") == "BOOLEAN"
        assert map_postgresql_type_to_duckdb("INT4") == "INTEGER"
        assert map_postgresql_type_to_duckdb("INT8") == "BIGINT"
        assert map_postgresql_type_to_duckdb("FLOAT4") == "FLOAT"
        assert map_postgresql_type_to_duckdb("FLOAT8") == "DOUBLE"

    def test_serial_types(self):
        assert map_postgresql_type_to_duckdb("SERIAL") == "INTEGER"
        assert map_postgresql_type_to_duckdb("BIGSERIAL") == "BIGINT"

    def test_json_types(self):
        assert map_postgresql_type_to_duckdb("JSON") == "VARCHAR"
        assert map_postgresql_type_to_duckdb("JSONB") == "VARCHAR"

    def test_unknown_type_fallback(self):
        assert map_postgresql_type_to_duckdb("UNKNOWN_TYPE") == "VARCHAR"

    def test_case_insensitive(self):
        assert map_postgresql_type_to_duckdb("integer") == "INTEGER"
        assert map_postgresql_type_to_duckdb("varchar(255)") == "VARCHAR"
        assert map_postgresql_type_to_duckdb("boolean") == "BOOLEAN"
