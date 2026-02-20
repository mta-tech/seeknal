"""Tests for profile loader connection profile features.

Tests cover:
- interpolate_env_vars with ${VAR:default} syntax
- interpolate_env_vars_in_dict recursive interpolation
- load_connection_profile from connections: section
- clear_connection_credentials for password clearing
- Error cases: missing profile, missing section, missing name
"""

import pytest  # ty: ignore[unresolved-import]

from seeknal.workflow.materialization.profile_loader import (  # ty: ignore[unresolved-import]
    ProfileLoader,
    interpolate_env_vars,
    interpolate_env_vars_in_dict,
)
from seeknal.workflow.materialization.config import (  # ty: ignore[unresolved-import]
    ConfigurationError,
)


# =============================================================================
# interpolate_env_vars tests
# =============================================================================


class TestInterpolateEnvVars:
    """Tests for env var interpolation with default value support."""

    def test_braces_no_default(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "db.example.com")
        assert interpolate_env_vars("${PG_HOST}") == "db.example.com"

    def test_bare_dollar(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "db.example.com")
        assert interpolate_env_vars("$PG_HOST") == "db.example.com"

    def test_default_value_when_var_not_set(self):
        result = interpolate_env_vars("${NONEXISTENT_TEST_VAR_99:localhost}")
        assert result == "localhost"

    def test_default_value_ignored_when_var_set(self, monkeypatch):
        monkeypatch.setenv("MY_HOST", "prod.example.com")
        result = interpolate_env_vars("${MY_HOST:localhost}")
        assert result == "prod.example.com"

    def test_default_empty_string(self):
        result = interpolate_env_vars("${NONEXISTENT_TEST_VAR_99:}")
        assert result == ""

    def test_multiple_vars_with_defaults(self, monkeypatch):
        monkeypatch.setenv("HOST", "pg.local")
        result = interpolate_env_vars("${HOST}:${PORT:5432}")
        assert result == "pg.local:5432"

    def test_missing_var_no_default_raises(self):
        with pytest.raises(ValueError, match="not set"):
            interpolate_env_vars("${TOTALLY_MISSING_VAR_12345}")

    def test_bare_dollar_missing_raises(self):
        with pytest.raises(ValueError, match="not set"):
            interpolate_env_vars("$TOTALLY_MISSING_VAR_12345")

    def test_no_interpolation_needed(self):
        assert interpolate_env_vars("plain_string") == "plain_string"

    def test_empty_string(self):
        assert interpolate_env_vars("") == ""

    def test_mixed_literal_and_vars(self, monkeypatch):
        monkeypatch.setenv("DB_NAME", "analytics")
        result = interpolate_env_vars("host=localhost dbname=${DB_NAME}")
        assert result == "host=localhost dbname=analytics"

    def test_default_with_special_chars(self):
        result = interpolate_env_vars("${MISSING_VAR:user@host:5432/db}")
        assert result == "user@host:5432/db"


# =============================================================================
# interpolate_env_vars_in_dict tests
# =============================================================================


class TestInterpolateEnvVarsInDict:
    """Tests for recursive dict interpolation."""

    def test_simple_dict(self, monkeypatch):
        monkeypatch.setenv("PG_HOST", "pg.prod.com")
        data = {"host": "${PG_HOST}", "port": 5432, "database": "mydb"}
        result = interpolate_env_vars_in_dict(data)
        assert result["host"] == "pg.prod.com"
        assert result["port"] == 5432
        assert result["database"] == "mydb"

    def test_nested_dict(self, monkeypatch):
        monkeypatch.setenv("DB_USER", "admin")
        data = {"connection": {"user": "${DB_USER}", "port": 5432}}
        result = interpolate_env_vars_in_dict(data)
        assert result["connection"]["user"] == "admin"
        assert result["connection"]["port"] == 5432

    def test_defaults_in_dict(self):
        data = {
            "host": "${MISSING_HOST:localhost}",
            "port": 5432,
        }
        result = interpolate_env_vars_in_dict(data)
        assert result["host"] == "localhost"

    def test_non_string_values_preserved(self):
        data = {"port": 5432, "enabled": True, "tags": ["a", "b"]}
        result = interpolate_env_vars_in_dict(data)
        assert result == data

    def test_original_dict_not_mutated(self, monkeypatch):
        monkeypatch.setenv("X_VAR", "value")
        data = {"key": "${X_VAR}"}
        result = interpolate_env_vars_in_dict(data)
        assert data["key"] == "${X_VAR}"
        assert result["key"] == "value"


# =============================================================================
# load_connection_profile tests
# =============================================================================


class TestLoadConnectionProfile:
    """Tests for ProfileLoader.load_connection_profile()."""

    def _write_profile(self, tmp_path, content):
        """Helper to write profiles.yml in tmp_path."""
        profile_file = tmp_path / "profiles.yml"
        profile_file.write_text(content)
        return profile_file

    def test_load_basic_profile(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PG_PASSWORD", "s3cret")
        profile_file = self._write_profile(tmp_path, """\
connections:
  my_pg:
    type: postgresql
    host: pg.local
    port: 5432
    user: admin
    password: ${PG_PASSWORD}
    database: analytics
""")
        loader = ProfileLoader(profile_path=profile_file)
        profile = loader.load_connection_profile("my_pg")

        assert profile["type"] == "postgresql"
        assert profile["host"] == "pg.local"
        assert profile["port"] == 5432
        assert profile["user"] == "admin"
        assert profile["password"] == "s3cret"
        assert profile["database"] == "analytics"

    def test_load_profile_with_defaults(self, tmp_path):
        profile_file = self._write_profile(tmp_path, """\
connections:
  dev_pg:
    type: postgresql
    host: ${PG_HOST:localhost}
    port: 5432
    user: ${PG_USER:postgres}
    password: ${PG_PASS:devpass}
    database: dev_db
""")
        loader = ProfileLoader(profile_path=profile_file)
        profile = loader.load_connection_profile("dev_pg")

        assert profile["host"] == "localhost"
        assert profile["user"] == "postgres"
        assert profile["password"] == "devpass"

    def test_env_var_overrides_default(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PG_HOST", "prod.example.com")
        profile_file = self._write_profile(tmp_path, """\
connections:
  my_pg:
    type: postgresql
    host: ${PG_HOST:localhost}
    port: 5432
""")
        loader = ProfileLoader(profile_path=profile_file)
        profile = loader.load_connection_profile("my_pg")

        assert profile["host"] == "prod.example.com"

    def test_profile_not_found_raises(self, tmp_path):
        profile_file = self._write_profile(tmp_path, """\
connections:
  my_pg:
    type: postgresql
    host: localhost
""")
        loader = ProfileLoader(profile_path=profile_file)
        with pytest.raises(ConfigurationError, match="'other_pg' not found"):
            loader.load_connection_profile("other_pg")

    def test_no_connections_section_raises(self, tmp_path):
        profile_file = self._write_profile(tmp_path, """\
materialization:
  enabled: true
""")
        loader = ProfileLoader(profile_path=profile_file)
        with pytest.raises(ConfigurationError, match="No 'connections' section"):
            loader.load_connection_profile("my_pg")

    def test_missing_profile_file_raises(self, tmp_path):
        missing = tmp_path / "nonexistent.yml"
        loader = ProfileLoader(profile_path=missing)
        with pytest.raises(ConfigurationError, match="Profile file not found"):
            loader.load_connection_profile("my_pg")

    def test_available_profiles_in_error(self, tmp_path):
        profile_file = self._write_profile(tmp_path, """\
connections:
  alpha:
    type: postgresql
  beta:
    type: postgresql
""")
        loader = ProfileLoader(profile_path=profile_file)
        with pytest.raises(ConfigurationError, match="alpha.*beta|beta.*alpha"):
            loader.load_connection_profile("gamma")

    def test_multiple_connections(self, tmp_path):
        profile_file = self._write_profile(tmp_path, """\
connections:
  pg_dev:
    type: postgresql
    host: localhost
    port: 5432
  pg_prod:
    type: postgresql
    host: prod.db.com
    port: 5433
""")
        loader = ProfileLoader(profile_path=profile_file)

        dev = loader.load_connection_profile("pg_dev")
        assert dev["host"] == "localhost"
        assert dev["port"] == 5432

        prod = loader.load_connection_profile("pg_prod")
        assert prod["host"] == "prod.db.com"
        assert prod["port"] == 5433

    def test_non_string_values_preserved(self, tmp_path):
        profile_file = self._write_profile(tmp_path, """\
connections:
  my_pg:
    type: postgresql
    host: localhost
    port: 5432
    connect_timeout: 30
    sslmode: require
""")
        loader = ProfileLoader(profile_path=profile_file)
        profile = loader.load_connection_profile("my_pg")

        assert profile["port"] == 5432
        assert profile["connect_timeout"] == 30
        assert profile["sslmode"] == "require"


# =============================================================================
# clear_connection_credentials tests
# =============================================================================


class TestClearConnectionCredentials:
    """Tests for credential clearing."""

    def test_clears_password(self):
        profile = {
            "host": "localhost",
            "password": "s3cret123",
            "user": "admin",
        }
        ProfileLoader.clear_connection_credentials(profile)

        assert "password" not in profile
        assert profile["host"] == "localhost"
        assert profile["user"] == "admin"

    def test_clears_multiple_sensitive_keys(self):
        profile = {
            "host": "localhost",
            "password": "s3cret",
            "bearer_token": "tok123",
            "token": "abc",
        }
        ProfileLoader.clear_connection_credentials(profile)

        assert "password" not in profile
        assert "bearer_token" not in profile
        assert "token" not in profile
        assert profile["host"] == "localhost"

    def test_no_sensitive_keys_unchanged(self):
        profile = {"host": "localhost", "port": 5432}
        ProfileLoader.clear_connection_credentials(profile)
        assert profile == {"host": "localhost", "port": 5432}

    def test_non_string_password_not_cleared(self):
        profile = {"password": 12345, "host": "localhost"}
        ProfileLoader.clear_connection_credentials(profile)
        # Non-string password left as-is
        assert profile["password"] == 12345
