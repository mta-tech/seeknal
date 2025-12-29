class InvalidIdentifierError(Exception):
    """
    Exception raised when a SQL identifier (table name, column name, database name)
    contains invalid characters or exceeds length limits.
    """

    pass


class InvalidPathError(Exception):
    """
    Exception raised when a file path contains potentially dangerous characters
    that could be used for SQL injection.
    """

    pass
