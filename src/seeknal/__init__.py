"""Seeknal: A feature store and data processing library.

Seeknal provides tools for building, managing, and serving machine learning
features at scale. It offers a unified API for feature engineering pipelines,
offline and online feature stores, and data validation.

Key Features:
    - Feature Store: Manage and serve features for ML models with support
      for both offline (batch) and online (real-time) storage.
    - Data Validation: Comprehensive validation utilities for SQL identifiers,
      table names, column names, and file paths to prevent injection attacks.
    - Entity Management: Define and manage entities as the primary keys for
      feature lookups.
    - Pipeline Tasks: Build data processing pipelines with support for
      Spark, DuckDB, and other execution engines.

Example:
    Basic usage::

        import seeknal
        from seeknal.featurestore import FeatureGroup

        # Define a feature group
        feature_group = FeatureGroup(
            name="user_features",
            entity=user_entity,
        )

For more information, see the documentation at https://seeknal.readthedocs.io/
"""

__version__ = "2.2.0"

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
