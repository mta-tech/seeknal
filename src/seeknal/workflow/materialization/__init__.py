"""
Iceberg materialization for Seeknal YAML pipelines.

This module provides Iceberg table materialization for pipeline nodes,
enabling persistent storage with time travel, incremental updates, and
schema evolution.

Key Features:
- Profile-driven configuration (dbt-like profiles.yml)
- Per-node YAML overrides
- Atomic write operations with rollback
- Schema evolution with conservative defaults
- Snapshot management for time travel
- Security-first design (env vars, TLS, audit logging)

Usage:
    # In profiles.yml
    materialization:
      enabled: true
      catalog:
        uri: ${LAKEKEEPER_URI}
        warehouse: s3://my-bucket/warehouse

    # In node YAML
    kind: source
    name: orders
    materialization:
      enabled: true
      mode: append

Components:
- config: Configuration models and validation
- credentials: Credential management (env vars, keyring)
- profile_loader: Load profiles.yml
- operations: Atomic materialization operations
- decorator: Materialization decorator for executors
"""

from seeknal.workflow.materialization.config import (
    MaterializationConfig,
    CatalogConfig,
    SchemaEvolutionConfig,
    DuckDBConfig,
    MaterializationMode,
    SchemaEvolutionMode,
    CatalogType,
    ConfigurationError,
    CredentialError,
    validate_table_name,
    validate_partition_columns,
)

__all__ = [
    # Config models
    "MaterializationConfig",
    "CatalogConfig",
    "SchemaEvolutionConfig",
    "DuckDBConfig",
    # Enums
    "MaterializationMode",
    "SchemaEvolutionMode",
    "CatalogType",
    # Exceptions
    "ConfigurationError",
    "CredentialError",
    # Validation functions
    "validate_table_name",
    "validate_partition_columns",
]
