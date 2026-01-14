"""
Comprehensive unit tests for the SQL identifier and path validation module.

Tests cover:
- Valid cases for all identifier types
- Invalid characters and patterns
- Length limit validation
- SQL injection attempt detection
- Edge cases (empty, None, wrong types)
"""

import pytest

from seeknal.validation import (
    SQL_IDENTIFIER_MAX_LENGTH,
    SQL_IDENTIFIER_PATTERN,
    SQL_VALUE_FORBIDDEN_PATTERNS,
    FILE_PATH_FORBIDDEN_CHARS,
    FILE_PATH_MAX_LENGTH,
    validate_column_name,
    validate_column_names,
    validate_database_name,
    validate_file_path,
    validate_schema_name,
    validate_sql_identifier,
    validate_sql_value,
    validate_table_name,
)
from seeknal.exceptions import InvalidIdentifierError, InvalidPathError


class TestValidateSqlIdentifier:
    """Tests for the base validate_sql_identifier function."""

    # Valid identifier tests
    def test_valid_simple_identifier(self):
        """Test simple valid identifiers."""
        assert validate_sql_identifier("users") == "users"
        assert validate_sql_identifier("Orders") == "Orders"
        assert validate_sql_identifier("MY_TABLE") == "MY_TABLE"

    def test_valid_identifier_with_underscore_prefix(self):
        """Test identifiers starting with underscore."""
        assert validate_sql_identifier("_private") == "_private"
        assert validate_sql_identifier("_") == "_"
        assert validate_sql_identifier("__double") == "__double"

    def test_valid_identifier_with_numbers(self):
        """Test identifiers containing numbers."""
        assert validate_sql_identifier("table1") == "table1"
        assert validate_sql_identifier("user_123") == "user_123"
        assert validate_sql_identifier("data2024") == "data2024"
        assert validate_sql_identifier("a1b2c3") == "a1b2c3"

    def test_valid_mixed_case_identifier(self):
        """Test mixed case identifiers."""
        assert validate_sql_identifier("UserTable") == "UserTable"
        assert validate_sql_identifier("userTable") == "userTable"
        assert validate_sql_identifier("USERTABLE") == "USERTABLE"

    def test_valid_long_identifier_at_limit(self):
        """Test identifier at exactly the maximum length."""
        identifier = "a" * SQL_IDENTIFIER_MAX_LENGTH
        assert validate_sql_identifier(identifier) == identifier

    # Invalid identifier tests - empty and type errors
    def test_empty_identifier_raises_error(self):
        """Test that empty identifier raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier("")
        assert "cannot be empty" in str(exc_info.value)

    def test_none_identifier_raises_error(self):
        """Test that None identifier raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier(None)
        assert "cannot be empty" in str(exc_info.value)

    def test_non_string_identifier_raises_error(self):
        """Test that non-string identifiers raise error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier(123)
        assert "must be a string" in str(exc_info.value)
        assert "int" in str(exc_info.value)

    def test_list_identifier_raises_error(self):
        """Test that list identifiers raise error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier(["table"])
        assert "must be a string" in str(exc_info.value)

    # Invalid identifier tests - length limits
    def test_identifier_exceeds_max_length_raises_error(self):
        """Test that identifier exceeding max length raises error."""
        identifier = "a" * (SQL_IDENTIFIER_MAX_LENGTH + 1)
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier(identifier)
        assert "exceeds maximum length" in str(exc_info.value)
        assert str(SQL_IDENTIFIER_MAX_LENGTH) in str(exc_info.value)

    def test_custom_max_length(self):
        """Test custom max length parameter."""
        # Should pass with default length
        identifier = "a" * 50
        assert validate_sql_identifier(identifier) == identifier

        # Should fail with custom shorter length
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier(identifier, max_length=30)
        assert "exceeds maximum length" in str(exc_info.value)
        assert "30" in str(exc_info.value)

    # Invalid identifier tests - invalid characters
    def test_identifier_starting_with_number_raises_error(self):
        """Test that identifier starting with number raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier("123table")
        assert "invalid characters" in str(exc_info.value)

    def test_identifier_with_space_raises_error(self):
        """Test that identifier with space raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier("my table")
        assert "invalid characters" in str(exc_info.value)

    def test_identifier_with_hyphen_raises_error(self):
        """Test that identifier with hyphen raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier("my-table")
        assert "invalid characters" in str(exc_info.value)

    def test_identifier_with_special_chars_raises_error(self):
        """Test that identifier with special characters raises error."""
        invalid_identifiers = [
            "table@name",
            "table#name",
            "table$name",
            "table%name",
            "table^name",
            "table&name",
            "table*name",
            "table(name",
            "table)name",
            "table+name",
            "table=name",
            "table!name",
        ]
        for identifier in invalid_identifiers:
            with pytest.raises(InvalidIdentifierError):
                validate_sql_identifier(identifier)

    def test_identifier_with_dot_raises_error(self):
        """Test that identifier with dot raises error (for qualified names, validate separately)."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier("schema.table")
        assert "invalid characters" in str(exc_info.value)

    def test_identifier_with_quotes_raises_error(self):
        """Test that identifier with quotes raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("table'name")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier('table"name')
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("table`name")

    # SQL injection attempt tests
    def test_sql_injection_drop_table_raises_error(self):
        """Test that DROP TABLE injection attempt raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users; DROP TABLE users;--")

    def test_sql_injection_union_raises_error(self):
        """Test that UNION injection attempt raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users UNION SELECT * FROM passwords")

    def test_sql_injection_comment_raises_error(self):
        """Test that SQL comment injection attempt raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users--")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users/*comment*/")

    def test_sql_injection_semicolon_raises_error(self):
        """Test that semicolon injection attempt raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users;DELETE FROM users")

    # Custom identifier type in error message
    def test_custom_identifier_type_in_error_message(self):
        """Test that custom identifier type appears in error message."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_identifier("", identifier_type="custom field")
        assert "custom field" in str(exc_info.value)


class TestValidateTableName:
    """Tests for the validate_table_name function."""

    def test_valid_table_names(self):
        """Test valid table names."""
        assert validate_table_name("users") == "users"
        assert validate_table_name("user_accounts") == "user_accounts"
        assert validate_table_name("UserAccounts") == "UserAccounts"
        assert validate_table_name("_metadata") == "_metadata"
        assert validate_table_name("table123") == "table123"

    def test_invalid_table_name_raises_error(self):
        """Test that invalid table name raises error with correct type."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_table_name("invalid-table")
        assert "table name" in str(exc_info.value)

    def test_empty_table_name_raises_error(self):
        """Test that empty table name raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_table_name("")
        assert "table name" in str(exc_info.value)
        assert "cannot be empty" in str(exc_info.value)


class TestValidateColumnName:
    """Tests for the validate_column_name function."""

    def test_valid_column_names(self):
        """Test valid column names."""
        assert validate_column_name("id") == "id"
        assert validate_column_name("user_id") == "user_id"
        assert validate_column_name("firstName") == "firstName"
        assert validate_column_name("_internal") == "_internal"
        assert validate_column_name("col1") == "col1"

    def test_invalid_column_name_raises_error(self):
        """Test that invalid column name raises error with correct type."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_column_name("invalid column")
        assert "column name" in str(exc_info.value)

    def test_empty_column_name_raises_error(self):
        """Test that empty column name raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_column_name("")
        assert "column name" in str(exc_info.value)
        assert "cannot be empty" in str(exc_info.value)


class TestValidateDatabaseName:
    """Tests for the validate_database_name function."""

    def test_valid_database_names(self):
        """Test valid database names."""
        assert validate_database_name("mydb") == "mydb"
        assert validate_database_name("my_database") == "my_database"
        assert validate_database_name("ProductionDB") == "ProductionDB"
        assert validate_database_name("db123") == "db123"

    def test_invalid_database_name_raises_error(self):
        """Test that invalid database name raises error with correct type."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_database_name("invalid-database")
        assert "database name" in str(exc_info.value)

    def test_empty_database_name_raises_error(self):
        """Test that empty database name raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_database_name("")
        assert "database name" in str(exc_info.value)
        assert "cannot be empty" in str(exc_info.value)


class TestValidateSchemaName:
    """Tests for the validate_schema_name function."""

    def test_valid_schema_names(self):
        """Test valid schema names."""
        assert validate_schema_name("public") == "public"
        assert validate_schema_name("my_schema") == "my_schema"
        assert validate_schema_name("ProductionSchema") == "ProductionSchema"
        assert validate_schema_name("schema123") == "schema123"

    def test_invalid_schema_name_raises_error(self):
        """Test that invalid schema name raises error with correct type."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_schema_name("invalid-schema")
        assert "schema name" in str(exc_info.value)

    def test_empty_schema_name_raises_error(self):
        """Test that empty schema name raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_schema_name("")
        assert "schema name" in str(exc_info.value)
        assert "cannot be empty" in str(exc_info.value)


class TestValidateFilePath:
    """Tests for the validate_file_path function."""

    # Valid path tests
    def test_valid_simple_paths(self):
        """Test valid simple file paths."""
        assert validate_file_path("/data/file.parquet") == "/data/file.parquet"
        assert validate_file_path("./relative/path.csv") == "./relative/path.csv"
        assert validate_file_path("file.txt") == "file.txt"

    def test_valid_paths_with_spaces(self):
        """Test valid paths with spaces (spaces are allowed in paths)."""
        assert validate_file_path("/path/to/my file.txt") == "/path/to/my file.txt"

    def test_valid_paths_with_special_chars(self):
        """Test valid paths with allowed special characters."""
        assert validate_file_path("/path/to/file-name.txt") == "/path/to/file-name.txt"
        assert validate_file_path("/path/to/file_name.txt") == "/path/to/file_name.txt"
        assert validate_file_path("/path/to/file.name.txt") == "/path/to/file.name.txt"

    def test_valid_glob_patterns(self):
        """Test valid glob patterns in paths (that don't conflict with SQL injection chars)."""
        # Note: /* patterns are blocked as they conflict with SQL block comments
        # Single * and ? globs are allowed
        assert validate_file_path("/data/file?.txt") == "/data/file?.txt"
        assert validate_file_path("/data/file[0-9].txt") == "/data/file[0-9].txt"

    def test_glob_patterns_with_sql_comment_chars_blocked(self):
        """Test that glob patterns containing /* are blocked (security measure)."""
        # These glob patterns conflict with SQL block comment syntax
        # and are intentionally blocked for security
        with pytest.raises(InvalidPathError):
            validate_file_path("/data/*.parquet")  # Contains /*
        with pytest.raises(InvalidPathError):
            validate_file_path("/data/**/*.csv")  # Contains /*

    def test_valid_path_at_max_length(self):
        """Test path at exactly the maximum length."""
        path = "/" + "a" * (FILE_PATH_MAX_LENGTH - 1)
        assert validate_file_path(path) == path

    # Invalid path tests - empty and type errors
    def test_empty_path_raises_error(self):
        """Test that empty path raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path("")
        assert "cannot be empty" in str(exc_info.value)

    def test_none_path_raises_error(self):
        """Test that None path raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path(None)
        assert "cannot be empty" in str(exc_info.value)

    def test_non_string_path_raises_error(self):
        """Test that non-string path raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path(123)
        assert "must be a string" in str(exc_info.value)

    # Invalid path tests - length limits
    def test_path_exceeds_max_length_raises_error(self):
        """Test that path exceeding max length raises error."""
        path = "/" + "a" * FILE_PATH_MAX_LENGTH
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path(path)
        assert "exceeds maximum length" in str(exc_info.value)

    def test_custom_max_length(self):
        """Test custom max length parameter."""
        path = "/data/file.txt"
        # Should pass with default length
        assert validate_file_path(path) == path

        # Should fail with custom shorter length
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path(path, max_length=10)
        assert "exceeds maximum length" in str(exc_info.value)

    # Invalid path tests - forbidden characters
    def test_path_with_single_quote_raises_error(self):
        """Test that path with single quote raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path("/data/file'name.txt")
        assert "forbidden character" in str(exc_info.value)
        assert "'" in str(exc_info.value)

    def test_path_with_double_quote_raises_error(self):
        """Test that path with double quote raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path('/data/file"name.txt')
        assert "forbidden character" in str(exc_info.value)

    def test_path_with_semicolon_raises_error(self):
        """Test that path with semicolon raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path("/data/file;name.txt")
        assert "forbidden character" in str(exc_info.value)
        assert ";" in str(exc_info.value)

    def test_path_with_sql_comment_raises_error(self):
        """Test that path with SQL comment raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path("/data/file--name.txt")
        assert "forbidden character" in str(exc_info.value)
        assert "--" in str(exc_info.value)

    def test_path_with_block_comment_start_raises_error(self):
        """Test that path with block comment start raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path("/data/file/*name.txt")
        assert "forbidden character" in str(exc_info.value)

    def test_path_with_block_comment_end_raises_error(self):
        """Test that path with block comment end raises error."""
        with pytest.raises(InvalidPathError) as exc_info:
            validate_file_path("/data/file*/name.txt")
        assert "forbidden character" in str(exc_info.value)

    # SQL injection attempts in paths
    def test_sql_injection_in_path_raises_error(self):
        """Test that SQL injection attempts in path raise error."""
        with pytest.raises(InvalidPathError):
            validate_file_path("/data/'; DROP TABLE users; --")
        with pytest.raises(InvalidPathError):
            validate_file_path('/data/file.txt"; DELETE FROM data;')

    # Custom forbidden chars
    def test_custom_forbidden_chars(self):
        """Test custom forbidden characters parameter."""
        # Should pass with default (no @)
        assert validate_file_path("/data/file@name.txt") == "/data/file@name.txt"

        # Should fail with custom forbidden chars including @
        with pytest.raises(InvalidPathError):
            validate_file_path("/data/file@name.txt", forbidden_chars=["@"])


class TestValidateSqlValue:
    """Tests for the validate_sql_value function."""

    # Valid value tests
    def test_valid_simple_values(self):
        """Test valid simple values."""
        assert validate_sql_value("hello") == "hello"
        assert validate_sql_value("user123") == "user123"
        assert validate_sql_value("John Doe") == "John Doe"

    def test_valid_values_with_special_chars(self):
        """Test valid values with allowed special characters."""
        assert validate_sql_value("email@example.com") == "email@example.com"
        assert validate_sql_value("hello-world") == "hello-world"
        assert validate_sql_value("2024-01-01") == "2024-01-01"

    def test_empty_value_allowed(self):
        """Test that empty string is allowed for values."""
        assert validate_sql_value("") == ""

    def test_non_string_values_returned_as_is(self):
        """Test that non-string values are returned as-is."""
        assert validate_sql_value(123) == 123
        assert validate_sql_value(45.67) == 45.67
        assert validate_sql_value(None) is None
        assert validate_sql_value(True) is True

    # Invalid value tests - SQL injection patterns
    def test_sql_comment_pattern_raises_error(self):
        """Test that SQL comment patterns raise error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_value("value--comment")
        assert "forbidden pattern" in str(exc_info.value)
        assert "--" in str(exc_info.value)

    def test_block_comment_pattern_raises_error(self):
        """Test that block comment patterns raise error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("value/*comment*/")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("start/*")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("*/end")

    def test_semicolon_pattern_raises_error(self):
        """Test that semicolon pattern raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_value("value; DROP TABLE users")
        assert "forbidden pattern" in str(exc_info.value)

    def test_union_pattern_raises_error(self):
        """Test that UNION pattern raises error (case insensitive)."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_value("1 UNION SELECT * FROM users")
        assert "UNION" in str(exc_info.value)

        # Test case insensitivity
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("1 union select * from users")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("1 Union Select")

    def test_drop_pattern_raises_error(self):
        """Test that DROP pattern raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("DROP TABLE users")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("drop database")

    def test_delete_pattern_raises_error(self):
        """Test that DELETE pattern raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("DELETE FROM users")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("delete from")

    def test_insert_pattern_raises_error(self):
        """Test that INSERT pattern raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("INSERT INTO users")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("insert into")

    def test_update_pattern_raises_error(self):
        """Test that UPDATE pattern raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("UPDATE users SET")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("update set")

    def test_exec_pattern_raises_error(self):
        """Test that EXEC pattern raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("EXEC sp_executesql")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("exec xp_cmdshell")

    # Custom value type in error message
    def test_custom_value_type_in_error_message(self):
        """Test that custom value type appears in error message."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_sql_value("test; DROP", value_type="filter value")
        assert "filter value" in str(exc_info.value)

    # Custom forbidden patterns
    def test_custom_forbidden_patterns(self):
        """Test custom forbidden patterns parameter."""
        # Should pass with default (no CUSTOM)
        assert validate_sql_value("CUSTOM") == "CUSTOM"

        # Should fail with custom forbidden patterns
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("CUSTOM", forbidden_patterns=["CUSTOM"])


class TestValidateColumnNames:
    """Tests for the validate_column_names function."""

    def test_valid_column_names_list(self):
        """Test valid list of column names."""
        columns = ["id", "name", "email", "created_at"]
        assert validate_column_names(columns) == columns

    def test_single_valid_column_name(self):
        """Test single valid column name in list."""
        columns = ["id"]
        assert validate_column_names(columns) == columns

    def test_empty_list_raises_error(self):
        """Test that empty list raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_column_names([])
        assert "list cannot be empty" in str(exc_info.value)

    def test_list_with_invalid_column_raises_error(self):
        """Test that list with invalid column raises error."""
        with pytest.raises(InvalidIdentifierError) as exc_info:
            validate_column_names(["id", "invalid-column", "name"])
        assert "column name" in str(exc_info.value)

    def test_list_with_empty_string_raises_error(self):
        """Test that list with empty string raises error."""
        with pytest.raises(InvalidIdentifierError):
            validate_column_names(["id", "", "name"])


class TestSqlInjectionAttempts:
    """
    Comprehensive tests for various SQL injection attack patterns.
    These tests ensure the validation module blocks common attack vectors.
    """

    def test_classic_or_injection(self):
        """Test classic OR 1=1 injection pattern."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users OR 1=1")

    def test_union_based_injection(self):
        """Test UNION-based SQL injection."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users UNION SELECT password FROM admins")

    def test_stacked_queries_injection(self):
        """Test stacked queries injection (semicolon)."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users; DELETE FROM users WHERE 1=1")

    def test_comment_based_injection(self):
        """Test comment-based injection patterns."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("admin'--")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("admin/**/")

    def test_quote_escape_injection(self):
        """Test quote escape injection."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users'")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier('users"')

    def test_null_byte_injection(self):
        """Test NULL byte injection (blocked by invalid chars)."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users\x00")

    def test_newline_injection(self):
        """Test newline injection (blocked by invalid chars)."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users\nSELECT")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users\rSELECT")

    def test_tab_injection(self):
        """Test tab injection (blocked by invalid chars)."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("users\tSELECT")

    def test_path_traversal_in_file_path(self):
        """Test path traversal is allowed but SQL injection chars blocked."""
        # Path traversal is not our concern (handled by filesystem)
        assert validate_file_path("../../../etc/passwd") == "../../../etc/passwd"

        # But SQL injection in paths is blocked
        with pytest.raises(InvalidPathError):
            validate_file_path("../../../etc/passwd'; DROP TABLE users;--")

    def test_value_injection_in_where_clause(self):
        """Test SQL injection in WHERE clause values."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("1; DROP TABLE users")
        with pytest.raises(InvalidIdentifierError):
            validate_sql_value("1 OR 1=1 UNION SELECT * FROM passwords")


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_single_character_identifiers(self):
        """Test single character identifiers."""
        assert validate_sql_identifier("a") == "a"
        assert validate_sql_identifier("_") == "_"
        assert validate_sql_identifier("Z") == "Z"

    def test_maximum_length_boundary(self):
        """Test identifiers at boundary conditions."""
        # Exactly at limit - should pass
        assert len(validate_sql_identifier("a" * 128)) == 128

        # One over limit - should fail
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("a" * 129)

    def test_unicode_characters_rejected(self):
        """Test that unicode characters are rejected."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("table\u00e9")  # e with accent
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("\u0442\u0430\u0431\u043b\u0435")  # Russian
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("\u8868")  # Chinese character

    def test_whitespace_variations(self):
        """Test various whitespace characters are rejected."""
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("table name")  # space
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("table\tname")  # tab
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("table\nname")  # newline
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier(" table")  # leading space
        with pytest.raises(InvalidIdentifierError):
            validate_sql_identifier("table ")  # trailing space

    def test_sql_reserved_words_allowed(self):
        """Test that SQL reserved words are allowed as identifiers."""
        # These are technically valid identifiers, just not recommended
        assert validate_sql_identifier("SELECT") == "SELECT"
        assert validate_sql_identifier("TABLE") == "TABLE"
        assert validate_sql_identifier("FROM") == "FROM"
        assert validate_sql_identifier("WHERE") == "WHERE"

    def test_case_sensitivity_in_pattern(self):
        """Test that pattern matching is case-insensitive for letters."""
        assert validate_sql_identifier("abc") == "abc"
        assert validate_sql_identifier("ABC") == "ABC"
        assert validate_sql_identifier("AbC") == "AbC"


class TestConstantsExposed:
    """Tests to verify constants are properly exposed."""

    def test_sql_identifier_pattern_exists(self):
        """Test SQL identifier pattern constant exists and is valid."""
        assert SQL_IDENTIFIER_PATTERN is not None
        assert SQL_IDENTIFIER_PATTERN.match("valid_identifier")
        assert not SQL_IDENTIFIER_PATTERN.match("invalid-identifier")

    def test_sql_identifier_max_length_exists(self):
        """Test SQL identifier max length constant exists."""
        assert SQL_IDENTIFIER_MAX_LENGTH == 128

    def test_file_path_max_length_exists(self):
        """Test file path max length constant exists."""
        assert FILE_PATH_MAX_LENGTH == 4096

    def test_file_path_forbidden_chars_exists(self):
        """Test file path forbidden chars constant exists."""
        assert FILE_PATH_FORBIDDEN_CHARS is not None
        assert "'" in FILE_PATH_FORBIDDEN_CHARS
        assert '"' in FILE_PATH_FORBIDDEN_CHARS
        assert ";" in FILE_PATH_FORBIDDEN_CHARS

    def test_sql_value_forbidden_patterns_exists(self):
        """Test SQL value forbidden patterns constant exists."""
        assert SQL_VALUE_FORBIDDEN_PATTERNS is not None
        assert "UNION" in SQL_VALUE_FORBIDDEN_PATTERNS
        assert "DROP" in SQL_VALUE_FORBIDDEN_PATTERNS
