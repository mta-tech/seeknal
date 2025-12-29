__version__ = "1.0.0"

# Export validation functions
from .validation import (
    validate_sql_identifier,
    validate_table_name,
    validate_column_name,
    validate_column_names,
    validate_database_name,
    validate_schema_name,
    validate_file_path,
    validate_sql_value,
)

# Export validation exceptions
from .exceptions import (
    InvalidIdentifierError,
    InvalidPathError,
)

__all__ = [
    "__version__",
    # Validation functions
    "validate_sql_identifier",
    "validate_table_name",
    "validate_column_name",
    "validate_column_names",
    "validate_database_name",
    "validate_schema_name",
    "validate_file_path",
    "validate_sql_value",
    # Validation exceptions
    "InvalidIdentifierError",
    "InvalidPathError",
]
