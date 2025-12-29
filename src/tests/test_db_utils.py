"""
Unit tests for the db_utils module.

Tests for SecureDatabaseURL, ParsedDatabaseURL, URL parsing utilities,
and error message sanitization functionality.
"""

import pytest

from seeknal.db_utils import (
    CREDENTIAL_PARAMS,
    DatabaseSecurityError,
    ParsedDatabaseURL,
    SecureDatabaseURL,
    build_database_url,
    extract_credentials,
    mask_url_credentials,
    parse_database_url,
    sanitize_database_exceptions,
    sanitize_error_message,
    with_sanitized_exceptions,
)


class TestSecureDatabaseURL:
    """Tests for the SecureDatabaseURL class."""

    def test_str_masks_auth_token(self):
        """Test that __str__ masks the authToken in the URL."""
        url = SecureDatabaseURL(
            "sqlite+libsql://example.turso.io/?authToken=secret123&secure=true"
        )
        result = str(url)
        assert "authToken=****" in result
        assert "secret123" not in result
        assert "secure=true" in result

    def test_repr_masks_auth_token(self):
        """Test that __repr__ masks the authToken in the URL."""
        url = SecureDatabaseURL(
            "sqlite+libsql://example.turso.io/?authToken=secret123&secure=true"
        )
        result = repr(url)
        assert "SecureDatabaseURL(" in result
        assert "authToken=****" in result
        assert "secret123" not in result

    def test_get_connection_url_returns_full_url(self):
        """Test that get_connection_url() returns the full URL with credentials."""
        original_url = (
            "sqlite+libsql://example.turso.io/?authToken=secret123&secure=true"
        )
        url = SecureDatabaseURL(original_url)
        result = url.get_connection_url()
        assert result == original_url
        assert "authToken=secret123" in result

    def test_url_without_auth_token(self):
        """Test handling of URLs without authToken."""
        original_url = "sqlite:///local.db"
        url = SecureDatabaseURL(original_url)
        assert str(url) == original_url
        assert url.get_connection_url() == original_url

    def test_url_with_empty_auth_token(self):
        """Test handling of URLs with empty authToken."""
        original_url = "sqlite+libsql://example.turso.io/?authToken=&secure=true"
        url = SecureDatabaseURL(original_url)
        result = str(url)
        assert "authToken=****" in result
        assert url.get_connection_url() == original_url

    def test_url_with_special_characters_in_token(self):
        """Test handling of URLs with special characters in authToken."""
        token = "abc123!@#$%^&*()_+-=[]{}|;':,./<>?"
        original_url = f"sqlite+libsql://example.turso.io/?authToken={token}&secure=true"
        url = SecureDatabaseURL(original_url)
        result = str(url)
        assert "authToken=****" in result
        assert token not in result
        assert url.get_connection_url() == original_url

    def test_url_with_multiple_query_params(self):
        """Test handling of URLs with multiple query parameters."""
        url = SecureDatabaseURL(
            "sqlite+libsql://example.turso.io/?param1=value1&authToken=secret&param2=value2"
        )
        result = str(url)
        assert "authToken=****" in result
        assert "param1=value1" in result
        assert "param2=value2" in result
        assert "secret" not in result

    def test_equality(self):
        """Test equality comparison between SecureDatabaseURL instances."""
        url1 = SecureDatabaseURL("sqlite+libsql://example.turso.io/?authToken=secret")
        url2 = SecureDatabaseURL("sqlite+libsql://example.turso.io/?authToken=secret")
        url3 = SecureDatabaseURL("sqlite+libsql://other.turso.io/?authToken=secret")

        assert url1 == url2
        assert url1 != url3

    def test_equality_with_non_secure_url(self):
        """Test equality returns NotImplemented for non-SecureDatabaseURL objects."""
        url = SecureDatabaseURL("sqlite:///local.db")
        assert url != "sqlite:///local.db"
        assert url != 42

    def test_hash(self):
        """Test that SecureDatabaseURL instances are hashable."""
        url1 = SecureDatabaseURL("sqlite+libsql://example.turso.io/?authToken=secret")
        url2 = SecureDatabaseURL("sqlite+libsql://example.turso.io/?authToken=secret")

        # Equal URLs should have the same hash
        assert hash(url1) == hash(url2)

        # Should be usable in sets and dicts
        url_set = {url1, url2}
        assert len(url_set) == 1


class TestParseDatabaseURL:
    """Tests for parse_database_url function and ParsedDatabaseURL class."""

    def test_parse_url_with_auth_token(self):
        """Test parsing URL with authToken parameter."""
        url = "sqlite+libsql://example.turso.io/?authToken=secret&secure=true"
        parsed = parse_database_url(url)

        assert "authToken" not in parsed.base_url
        assert "secure=true" in parsed.base_url
        assert parsed.credentials == {"authToken": "secret"}
        assert parsed.has_credentials() is True

    def test_parse_url_without_credentials(self):
        """Test parsing URL without any credentials."""
        url = "sqlite:///local.db"
        parsed = parse_database_url(url)

        # Note: urlunparse may normalize the URL path
        assert parsed.credentials == {}
        assert parsed.has_credentials() is False

    def test_parse_url_with_multiple_credentials(self):
        """Test parsing URL with multiple credential parameters."""
        url = "postgres://host/?authToken=token1&password=pass123&user=admin"
        parsed = parse_database_url(url)

        assert "authToken" not in parsed.base_url
        assert "password" not in parsed.base_url
        assert "user=admin" in parsed.base_url
        assert parsed.credentials["authToken"] == "token1"
        assert parsed.credentials["password"] == "pass123"

    def test_get_auth_token(self):
        """Test get_auth_token method."""
        url = "sqlite+libsql://example.turso.io/?authToken=mytoken&secure=true"
        parsed = parse_database_url(url)
        assert parsed.get_auth_token() == "mytoken"

    def test_get_auth_token_with_underscore(self):
        """Test get_auth_token with auth_token (underscore variant)."""
        url = "sqlite+libsql://example.turso.io/?auth_token=mytoken&secure=true"
        parsed = parse_database_url(url)
        assert parsed.get_auth_token() == "mytoken"

    def test_get_auth_token_returns_none(self):
        """Test get_auth_token returns None when no token present."""
        url = "sqlite:///local.db"
        parsed = parse_database_url(url)
        assert parsed.get_auth_token() is None

    def test_parsed_url_repr_masks_credentials(self):
        """Test that ParsedDatabaseURL.__repr__ masks credentials."""
        url = "sqlite+libsql://example.turso.io/?authToken=secret&secure=true"
        parsed = parse_database_url(url)
        result = repr(parsed)

        assert "****" in result
        assert "secret" not in result

    def test_credentials_returns_copy(self):
        """Test that credentials property returns a copy."""
        url = "sqlite+libsql://example.turso.io/?authToken=secret"
        parsed = parse_database_url(url)

        creds1 = parsed.credentials
        creds2 = parsed.credentials
        creds1["modified"] = "value"

        assert "modified" not in creds2
        assert "modified" not in parsed.credentials


class TestExtractCredentials:
    """Tests for extract_credentials function."""

    def test_extract_credentials_basic(self):
        """Test basic credential extraction."""
        base_url, creds = extract_credentials(
            "sqlite+libsql://host/?authToken=secret&secure=true"
        )

        assert "authToken" not in base_url
        assert "secure=true" in base_url
        assert creds == {"authToken": "secret"}

    def test_extract_credentials_no_credentials(self):
        """Test extraction when no credentials present."""
        base_url, creds = extract_credentials("sqlite:///local.db")

        # URL path may be normalized by urlunparse
        assert creds == {}


class TestMaskUrlCredentials:
    """Tests for mask_url_credentials function."""

    def test_mask_auth_token(self):
        """Test masking authToken in URL."""
        url = "sqlite+libsql://host/?authToken=secret123&secure=true"
        result = mask_url_credentials(url)

        assert "authToken=****" in result
        assert "secret123" not in result
        assert "secure=true" in result

    def test_mask_multiple_credentials(self):
        """Test masking multiple credential parameters."""
        url = "postgres://host/?authToken=token&password=pass&api_key=key123"
        result = mask_url_credentials(url)

        assert "authToken=****" in result
        assert "password=****" in result
        assert "api_key=****" in result
        assert "token" not in result
        # Note: 'pass' appears in 'password' key name, check actual values are masked
        assert "pass&" not in result  # Original value was 'pass'
        assert "key123" not in result

    def test_mask_preserves_non_credential_params(self):
        """Test that non-credential parameters are preserved."""
        url = "sqlite+libsql://host/?authToken=secret&secure=true&timeout=30"
        result = mask_url_credentials(url)

        assert "secure=true" in result
        assert "timeout=30" in result

    def test_mask_url_without_credentials(self):
        """Test masking URL without any credentials."""
        url = "sqlite:///local.db"
        result = mask_url_credentials(url)
        assert result == url


class TestBuildDatabaseUrl:
    """Tests for build_database_url function."""

    def test_build_turso_url(self):
        """Test building Turso database URL."""
        result = build_database_url(
            turso_database_url="libsql://mydb.turso.io",
            turso_auth_token="secret-token",
        )

        assert isinstance(result, SecureDatabaseURL)
        assert "authToken=****" in str(result)
        assert "secret-token" in result.get_connection_url()
        assert "secure=true" in str(result)

    def test_build_local_sqlite_url(self):
        """Test building local SQLite URL."""
        result = build_database_url(local_db_path="/path/to/db.sqlite")

        assert isinstance(result, SecureDatabaseURL)
        assert str(result) == "sqlite:////path/to/db.sqlite"

    def test_build_default_sqlite_url(self):
        """Test building default SQLite URL."""
        result = build_database_url(default_db_path="/default/path/db.sqlite")

        assert isinstance(result, SecureDatabaseURL)
        assert str(result) == "sqlite:////default/path/db.sqlite"

    def test_build_turso_url_missing_token_raises_error(self):
        """Test that missing auth token raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            build_database_url(turso_database_url="libsql://mydb.turso.io")

        assert "turso_auth_token is required" in str(exc_info.value)

    def test_build_no_config_raises_error(self):
        """Test that no configuration raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            build_database_url()

        assert "No valid database configuration provided" in str(exc_info.value)

    def test_build_turso_url_priority(self):
        """Test that Turso URL takes priority over local paths."""
        result = build_database_url(
            turso_database_url="libsql://mydb.turso.io",
            turso_auth_token="secret-token",
            local_db_path="/local/db.sqlite",
            default_db_path="/default/db.sqlite",
        )

        assert "turso.io" in str(result)


class TestSanitizeErrorMessage:
    """Tests for sanitize_error_message function."""

    def test_sanitize_auth_token(self):
        """Test sanitizing authToken in error message."""
        message = "Connection failed: authToken=secret123"
        result = sanitize_error_message(message)

        assert "authToken=****" in result
        assert "secret123" not in result

    def test_sanitize_multiple_credentials(self):
        """Test sanitizing multiple credential types."""
        message = "Error with authToken=abc password=xyz api_key=123"
        result = sanitize_error_message(message)

        assert "authToken=****" in result
        assert "password=****" in result
        assert "api_key=****" in result
        assert "abc" not in result
        assert "xyz" not in result
        assert "123" not in result

    def test_sanitize_preserves_non_credential_content(self):
        """Test that non-credential content is preserved."""
        message = "Database error on host=example.com with authToken=secret"
        result = sanitize_error_message(message)

        assert "host=example.com" in result
        assert "Database error" in result

    def test_sanitize_non_string_input(self):
        """Test sanitizing non-string input (converts to string)."""
        result = sanitize_error_message(12345)
        assert result == "12345"

    def test_sanitize_url_in_message(self):
        """Test sanitizing URL embedded in error message."""
        message = "Cannot connect to sqlite+libsql://host/?authToken=secret&secure=true"
        result = sanitize_error_message(message)

        assert "authToken=****" in result
        assert "secret" not in result


class TestDatabaseSecurityError:
    """Tests for DatabaseSecurityError exception class."""

    def test_exception_sanitizes_message(self):
        """Test that exception message is sanitized."""
        try:
            raise DatabaseSecurityError("Connection failed: authToken=secret")
        except DatabaseSecurityError as e:
            assert "authToken=****" in str(e)
            assert "secret" not in str(e)

    def test_exception_repr_is_sanitized(self):
        """Test that exception repr is sanitized."""
        exc = DatabaseSecurityError("Error with password=mysecret")
        result = repr(exc)

        assert "password=****" in result
        assert "mysecret" not in result

    def test_exception_sanitized_message_property(self):
        """Test sanitized_message property."""
        exc = DatabaseSecurityError("Error: api_key=secret123")
        assert "api_key=****" in exc.sanitized_message
        assert "secret123" not in exc.sanitized_message

    def test_exception_original_exception_property(self):
        """Test original_exception property."""
        original = ValueError("Original error")
        exc = DatabaseSecurityError("Wrapped error", original_exception=original)

        assert exc.original_exception is original

    def test_exception_inheritance(self):
        """Test that DatabaseSecurityError is an Exception."""
        exc = DatabaseSecurityError("Test error")
        assert isinstance(exc, Exception)


class TestSanitizeDatabaseExceptions:
    """Tests for sanitize_database_exceptions context manager."""

    def test_context_manager_sanitizes_exception(self):
        """Test that context manager sanitizes exception messages."""
        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise ValueError("Error: authToken=secret123")

        assert "authToken=****" in str(exc_info.value)
        assert "secret123" not in str(exc_info.value)

    def test_context_manager_preserves_original_exception(self):
        """Test that original exception is preserved."""
        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise ValueError("Test error with password=test")

        assert exc_info.value.original_exception is not None
        assert isinstance(exc_info.value.original_exception, ValueError)

    def test_context_manager_no_exception(self):
        """Test context manager when no exception is raised."""
        result = None
        with sanitize_database_exceptions():
            result = "success"
        assert result == "success"

    def test_context_manager_custom_exception_types(self):
        """Test context manager with custom exception types."""
        # Should catch ValueError
        with pytest.raises(DatabaseSecurityError):
            with sanitize_database_exceptions(exception_types=(ValueError,)):
                raise ValueError("authToken=secret")

        # Should NOT catch TypeError (not in exception_types)
        with pytest.raises(TypeError):
            with sanitize_database_exceptions(exception_types=(ValueError,)):
                raise TypeError("authToken=secret")


class TestWithSanitizedExceptions:
    """Tests for with_sanitized_exceptions decorator."""

    def test_decorator_sanitizes_exception(self):
        """Test that decorator sanitizes exception messages."""

        @with_sanitized_exceptions()
        def failing_function():
            raise ValueError("Connection failed: authToken=secret")

        with pytest.raises(DatabaseSecurityError) as exc_info:
            failing_function()

        assert "authToken=****" in str(exc_info.value)
        assert "secret" not in str(exc_info.value)

    def test_decorator_returns_value(self):
        """Test that decorator preserves return value."""

        @with_sanitized_exceptions()
        def successful_function():
            return "success"

        result = successful_function()
        assert result == "success"

    def test_decorator_preserves_function_metadata(self):
        """Test that decorator preserves function name and docstring."""

        @with_sanitized_exceptions()
        def my_function():
            """My docstring."""
            pass

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "My docstring."

    def test_decorator_custom_exception_types(self):
        """Test decorator with custom exception types."""

        @with_sanitized_exceptions(exception_types=(ValueError,))
        def function_raises_value_error():
            raise ValueError("password=secret")

        @with_sanitized_exceptions(exception_types=(ValueError,))
        def function_raises_type_error():
            raise TypeError("password=secret")

        # Should catch and sanitize ValueError
        with pytest.raises(DatabaseSecurityError):
            function_raises_value_error()

        # Should NOT catch TypeError
        with pytest.raises(TypeError):
            function_raises_type_error()


class TestCredentialParams:
    """Tests for CREDENTIAL_PARAMS constant."""

    def test_credential_params_contains_expected_values(self):
        """Test that CREDENTIAL_PARAMS contains all expected credential types."""
        expected = {"authToken", "password", "auth_token", "secret", "api_key"}
        assert expected == CREDENTIAL_PARAMS

    def test_credential_params_is_frozenset(self):
        """Test that CREDENTIAL_PARAMS is immutable."""
        assert isinstance(CREDENTIAL_PARAMS, frozenset)


class TestErrorMessageSanitizationIntegration:
    """
    Integration tests for error message sanitization.

    These tests verify that exception messages and stack traces do not contain
    credentials when database connection fails, simulating real-world scenarios.
    """

    def test_sqlalchemy_connection_error_is_sanitized(self):
        """Test that SQLAlchemy connection errors have credentials sanitized."""
        # Simulate a SQLAlchemy-like connection error with credentials in the message
        connection_url = "sqlite+libsql://mydb.turso.io/?authToken=super_secret_token_12345&secure=true"
        error_message = f"(sqlite3.OperationalError) unable to open database file: {connection_url}"

        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise Exception(error_message)

        result = str(exc_info.value)
        assert "super_secret_token_12345" not in result
        assert "authToken=****" in result
        assert "unable to open database file" in result

    def test_connection_refused_error_is_sanitized(self):
        """Test that connection refused errors with credentials are sanitized."""
        error_message = "Connection refused to postgres://user:password=mysecretpass&authToken=abc123"

        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise ConnectionError(error_message)

        result = str(exc_info.value)
        assert "mysecretpass" not in result
        assert "abc123" not in result
        assert "password=****" in result
        assert "authToken=****" in result

    def test_nested_exception_credentials_are_sanitized(self):
        """Test that nested exception messages have credentials sanitized."""

        @with_sanitized_exceptions()
        def inner_function():
            raise ValueError("Inner error: api_key=secret_api_key_xyz")

        @with_sanitized_exceptions()
        def outer_function():
            try:
                inner_function()
            except DatabaseSecurityError as e:
                # Re-raise with additional context containing credentials
                raise RuntimeError(
                    f"Outer error with authToken=outer_secret: {e}"
                ) from None

        with pytest.raises(DatabaseSecurityError) as exc_info:
            outer_function()

        result = str(exc_info.value)
        assert "outer_secret" not in result
        assert "secret_api_key_xyz" not in result
        assert "authToken=****" in result
        assert "api_key=****" in result

    def test_exception_repr_does_not_leak_credentials(self):
        """Test that exception repr() doesn't expose credentials."""
        error_message = "DB error: authToken=my_secret_auth_token&password=db_password"

        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise Exception(error_message)

        exc = exc_info.value
        repr_result = repr(exc)

        assert "my_secret_auth_token" not in repr_result
        assert "db_password" not in repr_result
        assert "****" in repr_result

    def test_traceback_simulation_with_credentials(self):
        """Test that simulated traceback handling sanitizes credentials."""
        import traceback

        def function_that_fails_with_credentials():
            url = "sqlite+libsql://db.turso.io/?authToken=traceback_secret_token"
            raise ValueError(f"Failed to connect to {url}")

        try:
            with sanitize_database_exceptions():
                function_that_fails_with_credentials()
        except DatabaseSecurityError as e:
            # The exception message should be sanitized
            assert "traceback_secret_token" not in str(e)
            assert "authToken=****" in str(e)

            # The original exception is preserved for debugging
            # but should not be exposed in logs
            assert e.original_exception is not None
            # Original exception args are not sanitized, but it's internal
            # The important part is that str(e) is safe to log

    def test_multiple_credential_types_in_error(self):
        """Test sanitization of errors containing multiple credential types."""
        error_message = (
            "Connection failed: "
            "authToken=token123 "
            "password=pass456 "
            "secret=mysecret789 "
            "api_key=apikey101112 "
            "auth_token=authtoken131415"
        )

        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise Exception(error_message)

        result = str(exc_info.value)

        # Verify none of the credential values appear
        assert "token123" not in result
        assert "pass456" not in result
        assert "mysecret789" not in result
        assert "apikey101112" not in result
        assert "authtoken131415" not in result

        # Verify all credential parameters are masked
        assert "authToken=****" in result
        assert "password=****" in result
        assert "secret=****" in result
        assert "api_key=****" in result
        assert "auth_token=****" in result

    def test_decorated_function_preserves_functionality(self):
        """Test that decorated functions work correctly when not raising exceptions."""

        @with_sanitized_exceptions()
        def database_operation(url: str):
            return f"Connected to {url}"

        # Function should work normally when no exception is raised
        result = database_operation("sqlite:///test.db")
        assert result == "Connected to sqlite:///test.db"

    def test_decorated_function_sanitizes_on_failure(self):
        """Test that decorated functions sanitize errors on failure."""

        @with_sanitized_exceptions()
        def failing_database_operation():
            raise ConnectionError(
                "Failed to connect: authToken=decorated_secret_token"
            )

        with pytest.raises(DatabaseSecurityError) as exc_info:
            failing_database_operation()

        assert "decorated_secret_token" not in str(exc_info.value)
        assert "authToken=****" in str(exc_info.value)

    def test_context_manager_with_url_in_exception_args(self):
        """Test that exception args containing URLs are sanitized."""
        url = "libsql://mydb.turso.io/?authToken=args_secret_token&secure=true"

        class CustomDBError(Exception):
            def __init__(self, message, url):
                super().__init__(message, url)
                self.url = url

        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise CustomDBError("Connection failed", url)

        result = str(exc_info.value)
        assert "args_secret_token" not in result

    def test_error_with_special_characters_in_credentials(self):
        """Test sanitization of credentials with special characters."""
        # Credentials often contain special characters
        special_token = "abc!@#$%^&*()_+-=[]{}|;':,./<>?"
        error_message = f"Auth failed: authToken={special_token}"

        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise Exception(error_message)

        result = str(exc_info.value)
        # The special characters should not appear in the sanitized output
        assert "abc!" not in result
        assert "authToken=****" in result

    def test_exception_chaining_preserves_original_type(self):
        """Test that original exception is preserved for debugging."""

        @with_sanitized_exceptions()
        def operation_with_specific_error():
            raise ConnectionRefusedError("Cannot connect: password=secret123")

        with pytest.raises(DatabaseSecurityError) as exc_info:
            operation_with_specific_error()

        # Original exception type should be preserved
        assert exc_info.value.original_exception is not None
        assert isinstance(exc_info.value.original_exception, ConnectionRefusedError)

        # But the exposed message should be sanitized
        assert "secret123" not in str(exc_info.value)

    def test_secure_database_url_in_exception(self):
        """Test that SecureDatabaseURL objects don't leak credentials in exceptions."""
        secure_url = SecureDatabaseURL(
            "sqlite+libsql://db.turso.io/?authToken=secure_url_secret&secure=true"
        )

        # When SecureDatabaseURL is converted to string in an error, it should be masked
        error_message = f"Cannot connect to {secure_url}"

        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise Exception(error_message)

        result = str(exc_info.value)
        # SecureDatabaseURL.__str__ already masks, so double-check it stays masked
        assert "secure_url_secret" not in result
        assert "authToken=****" in result

    def test_empty_credential_values_are_handled(self):
        """Test that empty credential values are properly masked."""
        error_message = "Auth error: authToken=&password="

        with pytest.raises(DatabaseSecurityError) as exc_info:
            with sanitize_database_exceptions():
                raise Exception(error_message)

        result = str(exc_info.value)
        assert "authToken=****" in result
        assert "password=****" in result

    def test_database_security_error_can_be_logged_safely(self):
        """Test that DatabaseSecurityError is safe to log."""
        # Simulate what would happen when logging the exception
        error_with_creds = DatabaseSecurityError(
            "Connection to libsql://db.turso.io/?authToken=logging_secret failed"
        )

        # All string representations should be safe
        str_output = str(error_with_creds)
        repr_output = repr(error_with_creds)
        sanitized_output = error_with_creds.sanitized_message

        for output in [str_output, repr_output, sanitized_output]:
            assert "logging_secret" not in output
            assert "authToken=****" in output
