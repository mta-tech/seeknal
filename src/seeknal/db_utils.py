"""
Database utilities for secure handling of database connection URLs.

This module provides classes and functions for securely managing database
connection URLs, ensuring that sensitive credentials (such as authToken)
are not exposed in logs, error messages, or debugging output.
"""

import os
import re
from contextlib import contextmanager
from functools import wraps
from typing import Callable, Dict, Optional, Tuple, Type, TypeVar, Union
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


class SecureDatabaseURL:
    """
    A wrapper class for database URLs that masks sensitive credentials
    in string representations.

    This class stores the full database URL internally for actual database
    connections, but overrides __str__ and __repr__ to return masked versions
    that hide the authToken parameter.

    Example:
        >>> url = SecureDatabaseURL("sqlite+libsql://example.com/?authToken=secret123&secure=true")
        >>> str(url)
        'sqlite+libsql://example.com/?authToken=****&secure=true'
        >>> url.get_connection_url()
        'sqlite+libsql://example.com/?authToken=secret123&secure=true'
    """

    # Pattern to match authToken parameter in URL query strings
    _AUTH_TOKEN_PATTERN = re.compile(r'(authToken=)([^&]*)')

    def __init__(self, url: str):
        """
        Initialize the SecureDatabaseURL with a database connection URL.

        Args:
            url: The full database connection URL, potentially containing
                 sensitive credentials like authToken.
        """
        self._url = url

    def _mask_credentials(self, url: str) -> str:
        """
        Mask sensitive credentials in the URL.

        Args:
            url: The URL to mask.

        Returns:
            The URL with sensitive credentials replaced with '****'.
        """
        return self._AUTH_TOKEN_PATTERN.sub(r'\1****', url)

    def __str__(self) -> str:
        """
        Return a string representation with masked credentials.

        Returns:
            The database URL with authToken masked as '****'.
        """
        return self._mask_credentials(self._url)

    def __repr__(self) -> str:
        """
        Return a safe debugging representation with masked credentials.

        Returns:
            A repr-style string showing the class name and masked URL.
        """
        return f"SecureDatabaseURL({self._mask_credentials(self._url)!r})"

    def get_connection_url(self) -> str:
        """
        Get the full database URL for actual connection usage.

        This method should be used when passing the URL to database
        connection functions like SQLAlchemy's create_engine().

        Returns:
            The full database URL with all credentials intact.
        """
        return self._url

    def __eq__(self, other: object) -> bool:
        """
        Compare two SecureDatabaseURL instances for equality.

        Args:
            other: Another object to compare with.

        Returns:
            True if both instances wrap the same URL.
        """
        if not isinstance(other, SecureDatabaseURL):
            return NotImplemented
        return self._url == other._url

    def __hash__(self) -> int:
        """
        Return a hash of the SecureDatabaseURL.

        Returns:
            Hash based on the underlying URL.
        """
        return hash(self._url)


# Credential parameters that should be extracted from URLs
CREDENTIAL_PARAMS = frozenset(['authToken', 'password', 'auth_token', 'secret', 'api_key'])


class ParsedDatabaseURL:
    """
    Represents a parsed database URL with credentials separated from the base URL.

    This class holds both the base URL (without credentials) and the extracted
    credential parameters, allowing credentials to be passed via connect_args
    instead of being embedded in the URL string.

    Attributes:
        base_url: The database URL with credential parameters removed.
        credentials: Dictionary of extracted credential parameters.
        query_params: Dictionary of remaining (non-credential) query parameters.
    """

    def __init__(
        self,
        base_url: str,
        credentials: Dict[str, str],
        query_params: Dict[str, str]
    ):
        """
        Initialize the ParsedDatabaseURL.

        Args:
            base_url: The database URL with credentials removed.
            credentials: Dictionary of extracted credential key-value pairs.
            query_params: Dictionary of remaining query parameters.
        """
        self._base_url = base_url
        self._credentials = credentials
        self._query_params = query_params

    @property
    def base_url(self) -> str:
        """Get the base URL without credentials."""
        return self._base_url

    @property
    def credentials(self) -> Dict[str, str]:
        """Get the extracted credentials as a dictionary."""
        return self._credentials.copy()

    @property
    def query_params(self) -> Dict[str, str]:
        """Get the remaining query parameters."""
        return self._query_params.copy()

    def get_auth_token(self) -> Optional[str]:
        """
        Get the authToken if present in credentials.

        Returns:
            The authToken value or None if not present.
        """
        return self._credentials.get('authToken') or self._credentials.get('auth_token')

    def has_credentials(self) -> bool:
        """
        Check if any credentials were extracted.

        Returns:
            True if credentials were found, False otherwise.
        """
        return bool(self._credentials)

    def __repr__(self) -> str:
        """Return a safe repr with masked credentials."""
        masked_creds = {k: '****' for k in self._credentials}
        return f"ParsedDatabaseURL(base_url={self._base_url!r}, credentials={masked_creds})"


def parse_database_url(url: str) -> ParsedDatabaseURL:
    """
    Parse a database URL and separate credentials from the base URL.

    This function extracts credential parameters (authToken, password, etc.)
    from the URL query string, returning a ParsedDatabaseURL object that
    holds both the base URL (without credentials) and the extracted credentials.

    Args:
        url: The full database connection URL.

    Returns:
        A ParsedDatabaseURL object containing the base URL and credentials.

    Example:
        >>> parsed = parse_database_url("sqlite+libsql://example.com/?authToken=secret&secure=true")
        >>> parsed.base_url
        'sqlite+libsql://example.com/?secure=true'
        >>> parsed.credentials
        {'authToken': 'secret'}
    """
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query, keep_blank_values=True)

    # Separate credentials from other query parameters
    credentials: Dict[str, str] = {}
    remaining_params: Dict[str, str] = {}

    for key, values in query_params.items():
        # parse_qs returns lists, we take the first value
        value = values[0] if values else ''
        if key in CREDENTIAL_PARAMS:
            credentials[key] = value
        else:
            remaining_params[key] = value

    # Reconstruct the URL without credentials
    new_query = urlencode(remaining_params) if remaining_params else ''
    base_url_parts = (
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        new_query,
        parsed.fragment
    )
    base_url = urlunparse(base_url_parts)

    return ParsedDatabaseURL(base_url, credentials, remaining_params)


def extract_credentials(url: str) -> Tuple[str, Dict[str, str]]:
    """
    Extract credentials from a database URL.

    A convenience function that returns just the base URL and credentials
    dictionary without creating a ParsedDatabaseURL object.

    Args:
        url: The full database connection URL.

    Returns:
        A tuple of (base_url, credentials_dict).

    Example:
        >>> base_url, creds = extract_credentials("sqlite+libsql://host/?authToken=secret")
        >>> base_url
        'sqlite+libsql://host/'
        >>> creds
        {'authToken': 'secret'}
    """
    parsed = parse_database_url(url)
    return parsed.base_url, parsed.credentials


def mask_url_credentials(url: str) -> str:
    """
    Mask all credential parameters in a database URL.

    This function replaces the values of known credential parameters
    with '****' for safe logging and display.

    Args:
        url: The database URL to mask.

    Returns:
        The URL with credential values replaced with '****'.

    Example:
        >>> mask_url_credentials("sqlite+libsql://host/?authToken=secret&secure=true")
        'sqlite+libsql://host/?authToken=****&secure=true'
    """
    # Use regex-based masking for readable output (avoids URL encoding of ****)
    result = url
    for param in CREDENTIAL_PARAMS:
        # Match param=value where value continues until & or end of string
        pattern = re.compile(rf'({re.escape(param)}=)([^&]*)')
        result = pattern.sub(r'\1****', result)
    return result


def build_database_url(
    turso_database_url: Optional[str] = None,
    turso_auth_token: Optional[str] = None,
    local_db_path: Optional[str] = None,
    default_db_path: Optional[str] = None,
) -> SecureDatabaseURL:
    """
    Build a secure database URL based on the provided configuration.

    This function encapsulates the database URL construction logic and returns
    a SecureDatabaseURL wrapper that masks credentials in string representations.

    The function supports three modes:
    1. Remote Turso database: When turso_database_url and turso_auth_token are provided
    2. Local SQLite database: When local_db_path is provided
    3. Default SQLite database: When default_db_path is provided as fallback

    Args:
        turso_database_url: The Turso database URL (e.g., 'libsql://db-name.turso.io').
            If provided, turso_auth_token must also be provided.
        turso_auth_token: The authentication token for Turso database.
            Required when turso_database_url is provided.
        local_db_path: Path to a local SQLite database file.
            Used when Turso credentials are not provided.
        default_db_path: Default path to SQLite database file.
            Used as fallback when no other configuration is provided.

    Returns:
        A SecureDatabaseURL instance wrapping the constructed database URL.

    Raises:
        ValueError: If turso_database_url is provided without turso_auth_token,
            or if no valid database configuration is provided.

    Example:
        >>> # Remote Turso database
        >>> url = build_database_url(
        ...     turso_database_url="libsql://mydb.turso.io",
        ...     turso_auth_token="secret-token"
        ... )
        >>> str(url)
        'sqlite+libsql://mydb.turso.io/?authToken=****&secure=true'
        >>> url.get_connection_url()
        'sqlite+libsql://mydb.turso.io/?authToken=secret-token&secure=true'

        >>> # Local SQLite database
        >>> url = build_database_url(local_db_path="/path/to/db.sqlite")
        >>> str(url)
        'sqlite:////path/to/db.sqlite'
    """
    if turso_database_url is not None:
        # Remote Turso database configuration
        if turso_auth_token is None:
            raise ValueError(
                "turso_auth_token is required when turso_database_url is provided"
            )
        db_url = f"sqlite+{turso_database_url}/?authToken={turso_auth_token}&secure=true"
    elif local_db_path is not None:
        # Local SQLite database configuration
        local_db_path = os.path.expanduser(local_db_path)
        local_dir = os.path.dirname(local_db_path)
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)
        db_url = f"sqlite:///{local_db_path}"
    elif default_db_path is not None:
        # Default fallback SQLite database
        default_db_path = os.path.expanduser(default_db_path)
        default_dir = os.path.dirname(default_db_path)
        if default_dir:
            os.makedirs(default_dir, exist_ok=True)
        db_url = f"sqlite:///{default_db_path}"
    else:
        raise ValueError(
            "No valid database configuration provided. "
            "Provide either turso_database_url with turso_auth_token, "
            "local_db_path, or default_db_path."
        )

    return SecureDatabaseURL(db_url)


# Type variable for generic function decorators
F = TypeVar('F', bound=Callable)


class DatabaseSecurityError(Exception):
    """
    Custom exception for database-related errors with sanitized messages.

    This exception automatically sanitizes the error message to remove any
    credential patterns (such as authToken values) before storing the message.
    This ensures that credentials are not exposed in logs, stack traces, or
    error displays.

    Example:
        >>> try:
        ...     raise DatabaseSecurityError("Connection failed: authToken=secret123")
        ... except DatabaseSecurityError as e:
        ...     print(str(e))
        'Connection failed: authToken=****'
    """

    def __init__(
        self,
        message: str,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize the DatabaseSecurityError with a sanitized message.

        Args:
            message: The error message, which will be sanitized to remove
                     credential values.
            original_exception: The original exception that caused this error,
                               if any. Preserved for debugging but not exposed
                               in the string representation.
        """
        self._original_message = message
        self._sanitized_message = sanitize_error_message(message)
        self._original_exception = original_exception
        super().__init__(self._sanitized_message)

    @property
    def sanitized_message(self) -> str:
        """Get the sanitized error message."""
        return self._sanitized_message

    @property
    def original_exception(self) -> Optional[Exception]:
        """Get the original exception that caused this error."""
        return self._original_exception

    def __str__(self) -> str:
        """Return the sanitized error message."""
        return self._sanitized_message

    def __repr__(self) -> str:
        """Return a safe repr with sanitized message."""
        return f"DatabaseSecurityError({self._sanitized_message!r})"


def sanitize_error_message(message: str) -> str:
    """
    Sanitize an error message by masking any credential patterns.

    This function replaces values of known credential parameters (authToken,
    password, etc.) with '****' to prevent credential leakage in error messages,
    logs, or stack traces.

    Args:
        message: The error message to sanitize.

    Returns:
        The message with credential values replaced with '****'.

    Example:
        >>> sanitize_error_message("Failed to connect: authToken=secret123")
        'Failed to connect: authToken=****'
        >>> sanitize_error_message("password=mysecret&user=admin")
        'password=****&user=admin'
    """
    if not isinstance(message, str):
        message = str(message)

    result = message
    for param in CREDENTIAL_PARAMS:
        # Match param=value patterns where value continues until & or end of string
        # Also handles cases where value may be quoted or have special characters
        pattern = re.compile(rf'({re.escape(param)}=)([^&\s\'"]*)')
        result = pattern.sub(r'\1****', result)

    return result


@contextmanager
def sanitize_database_exceptions(
    exception_types: Optional[Tuple[Type[Exception], ...]] = None
):
    """
    Context manager that catches database exceptions and re-raises them with
    sanitized error messages.

    This context manager wraps database operations to ensure that any exceptions
    raised have their error messages sanitized to remove credential information.

    Args:
        exception_types: Tuple of exception types to catch and sanitize.
                        If None, catches all Exception types.

    Yields:
        None

    Raises:
        DatabaseSecurityError: If a database exception is caught, it is re-raised
                              as a DatabaseSecurityError with a sanitized message.

    Example:
        >>> with sanitize_database_exceptions():
        ...     # Database operations that might fail
        ...     raise ValueError("Connection failed: authToken=secret123")
        Traceback (most recent call last):
            ...
        DatabaseSecurityError: Connection failed: authToken=****
    """
    if exception_types is None:
        exception_types = (Exception,)

    try:
        yield
    except exception_types as e:
        # Get the full error message including the exception type
        error_message = str(e)

        # Also check if the exception has args that contain credentials
        if hasattr(e, 'args') and e.args:
            sanitized_args = tuple(
                sanitize_error_message(str(arg)) if isinstance(arg, str) else arg
                for arg in e.args
            )
        else:
            sanitized_args = (sanitize_error_message(error_message),)

        # Re-raise as DatabaseSecurityError with sanitized message
        # Log the original error for debugging
        logger.debug(f"Database Error caught: {e}")
        if hasattr(e, 'orig'):
            logger.debug(f"Original DBAPI error: {e.orig}")

        raise DatabaseSecurityError(
            sanitize_error_message(error_message),
            original_exception=e
        ) from None


def with_sanitized_exceptions(
    exception_types: Optional[Tuple[Type[Exception], ...]] = None
) -> Callable[[F], F]:
    """
    Decorator that wraps a function to sanitize database exceptions.

    This decorator catches exceptions from the wrapped function and re-raises
    them as DatabaseSecurityError with sanitized error messages, ensuring
    credentials are not exposed.

    Args:
        exception_types: Tuple of exception types to catch and sanitize.
                        If None, catches all Exception types.

    Returns:
        A decorator function that wraps the target function.

    Example:
        >>> @with_sanitized_exceptions()
        ... def connect_to_database(url):
        ...     raise ValueError(f"Cannot connect to {url}")
        ...
        >>> connect_to_database("sqlite://host/?authToken=secret")
        Traceback (most recent call last):
            ...
        DatabaseSecurityError: Cannot connect to sqlite://host/?authToken=****
    """
    if exception_types is None:
        exception_types = (Exception,)

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with sanitize_database_exceptions(exception_types):
                return func(*args, **kwargs)
        return wrapper  # type: ignore
    return decorator
