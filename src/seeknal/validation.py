"""
SQL Identifier and Path Validation Module.

This module provides functions to validate SQL identifiers (table names, column names,
database names) and file paths to prevent SQL injection attacks, DoS through long names,
and schema confusion.
"""

import re
from typing import List, Optional

from .exceptions import InvalidIdentifierError, InvalidPathError


# Constants for SQL identifier validation
SQL_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
SQL_IDENTIFIER_MAX_LENGTH = 128

# Constants for file path validation
FILE_PATH_MAX_LENGTH = 4096
FILE_PATH_FORBIDDEN_CHARS = ["'", '"', ";", "--", "/*", "*/"]

# Constants for SQL value validation (used in WHERE clauses)
SQL_VALUE_FORBIDDEN_PATTERNS = [
    "--",
    "/*",
    "*/",
    ";",
    "UNION",
    "DROP",
    "DELETE",
    "INSERT",
    "UPDATE",
    "EXEC",
]


def validate_sql_identifier(
    identifier: str,
    identifier_type: str = "identifier",
    max_length: int = SQL_IDENTIFIER_MAX_LENGTH,
) -> str:
    """
    Validate a SQL identifier (table name, column name, database name).

    SQL identifiers must:
    - Start with a letter (a-z, A-Z) or underscore (_)
    - Contain only alphanumeric characters (a-z, A-Z, 0-9) and underscores (_)
    - Be no longer than max_length characters (default 128)
    - Not be empty

    Args:
        identifier: The SQL identifier to validate.
        identifier_type: Type of identifier for error messages (e.g., "table name").
        max_length: Maximum allowed length for the identifier.

    Returns:
        The validated identifier (unchanged if valid).

    Raises:
        InvalidIdentifierError: If the identifier is invalid.
    """
    if not identifier:
        raise InvalidIdentifierError(
            f"Invalid {identifier_type}: cannot be empty"
        )

    if not isinstance(identifier, str):
        raise InvalidIdentifierError(
            f"Invalid {identifier_type}: must be a string, got {type(identifier).__name__}"
        )

    if len(identifier) > max_length:
        raise InvalidIdentifierError(
            f"Invalid {identifier_type}: '{identifier[:50]}...' exceeds maximum length of {max_length} characters"
        )

    if not SQL_IDENTIFIER_PATTERN.match(identifier):
        raise InvalidIdentifierError(
            f"Invalid {identifier_type}: '{identifier}' contains invalid characters. "
            f"Must start with a letter or underscore and contain only alphanumeric characters and underscores."
        )

    return identifier


def validate_table_name(table_name: str) -> str:
    """
    Validate a SQL table name.

    Table names must follow SQL identifier rules:
    - Start with a letter or underscore
    - Contain only alphanumeric characters and underscores
    - Be no longer than 128 characters

    Args:
        table_name: The table name to validate.

    Returns:
        The validated table name (unchanged if valid).

    Raises:
        InvalidIdentifierError: If the table name is invalid.
    """
    return validate_sql_identifier(table_name, identifier_type="table name")


def validate_column_name(column_name: str) -> str:
    """
    Validate a SQL column name.

    Column names must follow SQL identifier rules:
    - Start with a letter or underscore
    - Contain only alphanumeric characters and underscores
    - Be no longer than 128 characters

    Args:
        column_name: The column name to validate.

    Returns:
        The validated column name (unchanged if valid).

    Raises:
        InvalidIdentifierError: If the column name is invalid.
    """
    return validate_sql_identifier(column_name, identifier_type="column name")


def validate_database_name(database_name: str) -> str:
    """
    Validate a SQL database name.

    Database names must follow SQL identifier rules:
    - Start with a letter or underscore
    - Contain only alphanumeric characters and underscores
    - Be no longer than 128 characters

    Args:
        database_name: The database name to validate.

    Returns:
        The validated database name (unchanged if valid).

    Raises:
        InvalidIdentifierError: If the database name is invalid.
    """
    return validate_sql_identifier(database_name, identifier_type="database name")


def validate_file_path(
    file_path: str,
    max_length: int = FILE_PATH_MAX_LENGTH,
    forbidden_chars: Optional[List[str]] = None,
) -> str:
    """
    Validate a file path to ensure it doesn't contain SQL injection characters.

    File paths are validated to:
    - Not be empty
    - Not exceed max_length characters (default 4096)
    - Not contain forbidden characters that could be used for SQL injection

    Args:
        file_path: The file path to validate.
        max_length: Maximum allowed length for the path.
        forbidden_chars: List of forbidden character sequences. Defaults to
            common SQL injection characters: ', ", ;, --, /*, */

    Returns:
        The validated file path (unchanged if valid).

    Raises:
        InvalidPathError: If the file path is invalid or contains forbidden characters.
    """
    if forbidden_chars is None:
        forbidden_chars = FILE_PATH_FORBIDDEN_CHARS

    if not file_path:
        raise InvalidPathError("Invalid file path: cannot be empty")

    if not isinstance(file_path, str):
        raise InvalidPathError(
            f"Invalid file path: must be a string, got {type(file_path).__name__}"
        )

    if len(file_path) > max_length:
        raise InvalidPathError(
            f"Invalid file path: '{file_path[:50]}...' exceeds maximum length of {max_length} characters"
        )

    for char in forbidden_chars:
        if char in file_path:
            raise InvalidPathError(
                f"Invalid file path: contains forbidden character sequence '{char}'"
            )

    return file_path


def validate_sql_value(
    value: str,
    value_type: str = "value",
    forbidden_patterns: Optional[List[str]] = None,
) -> str:
    """
    Validate a value used in SQL WHERE clauses to prevent SQL injection.

    This function checks for common SQL injection patterns in values
    that will be used in WHERE clauses or other SQL contexts.

    Args:
        value: The value to validate.
        value_type: Type of value for error messages (e.g., "filter value").
        forbidden_patterns: List of forbidden patterns. Defaults to common
            SQL injection patterns like UNION, DROP, DELETE, etc.

    Returns:
        The validated value (unchanged if valid).

    Raises:
        InvalidIdentifierError: If the value contains forbidden patterns.
    """
    if forbidden_patterns is None:
        forbidden_patterns = SQL_VALUE_FORBIDDEN_PATTERNS

    if not isinstance(value, str):
        # Non-string values are returned as-is (they'll be converted later)
        return value

    if not value:
        # Empty strings are allowed for values
        return value

    # Check for forbidden patterns (case-insensitive)
    value_upper = value.upper()
    for pattern in forbidden_patterns:
        if pattern.upper() in value_upper:
            raise InvalidIdentifierError(
                f"Invalid {value_type}: contains forbidden pattern '{pattern}'"
            )

    return value


def validate_column_names(column_names: List[str]) -> List[str]:
    """
    Validate a list of SQL column names.

    Args:
        column_names: List of column names to validate.

    Returns:
        The validated list of column names (unchanged if all valid).

    Raises:
        InvalidIdentifierError: If any column name is invalid.
    """
    if not column_names:
        raise InvalidIdentifierError("Invalid column names: list cannot be empty")

    for column_name in column_names:
        validate_column_name(column_name)

    return column_names


def validate_schema_name(schema_name: str) -> str:
    """
    Validate a SQL schema name.

    Schema names must follow SQL identifier rules:
    - Start with a letter or underscore
    - Contain only alphanumeric characters and underscores
    - Be no longer than 128 characters

    Args:
        schema_name: The schema name to validate.

    Returns:
        The validated schema name (unchanged if valid).

    Raises:
        InvalidIdentifierError: If the schema name is invalid.
    """
    return validate_sql_identifier(schema_name, identifier_type="schema name")
