"""
Materialization configuration models for Iceberg integration.

This module defines the configuration models for Iceberg materialization,
including profile loading, credential management, and validation.

Design Decisions:
- Pydantic models for type safety and validation
- Environment variable interpolation for security
- Optional system keyring integration for credentials
- Conservative defaults for schema evolution
- Profile-driven with per-node YAML overrides

Security Features:
- Credentials loaded from environment variables (not plaintext)
- Optional system keyring support
- SQL injection prevention for table names
- Path traversal prevention for warehouse paths
- TLS verification for catalog connections

Key Components:
- MaterializationConfig: Main configuration model
- CatalogConfig: Catalog connection configuration
- SchemaEvolutionConfig: Schema evolution settings
- ProfileLoader: Load and validate profiles.yml
- CredentialManager: Handle credentials from env vars or keyring
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum

from seeknal.validation import validate_file_path


class MaterializationMode(str, Enum):
    """Materialization write mode."""
    APPEND = "append"
    OVERWRITE = "overwrite"


class SchemaEvolutionMode(str, Enum):
    """Schema evolution mode."""
    SAFE = "safe"  # Conservative, no automatic changes
    AUTO = "auto"  # Allow safe automatic changes
    STRICT = "strict"  # Manual approval required for any change


class CatalogType(str, Enum):
    """Catalog type."""
    REST = "rest"  # Lakekeeper REST catalog (v1)
    # Future: HIVE, GLUE, UNITY, etc.


# Safe type conversions for schema evolution
SAFE_TYPE_CONVERSIONS = {
    ("integer", "long"): True,
    ("float", "double"): True,
    ("string", "string"): True,
}


class MaterializationError(Exception):
    """Base exception for materialization errors."""
    pass


class ConfigurationError(MaterializationError):
    """Raised when configuration is invalid."""
    pass


class CredentialError(MaterializationError):
    """Raised when credential loading fails."""
    pass


@dataclass
class CatalogConfig:
    """
    Catalog connection configuration.

    Attributes:
        type: Catalog type (rest for Lakekeeper)
        uri: REST catalog endpoint URI (will interpolate env vars)
        warehouse: Warehouse path (S3/GCS/Azure)
        bearer_token: Optional bearer token for authentication
        verify_tls: Whether to verify TLS certificates (default: True)
        connection_timeout: Connection timeout in seconds (default: 10)
        request_timeout: Request timeout in seconds (default: 30)
    """
    type: CatalogType = CatalogType.REST
    uri: str = ""
    warehouse: str = ""
    bearer_token: Optional[str] = None
    verify_tls: bool = True
    connection_timeout: int = 10
    request_timeout: int = 30

    def interpolate_env_vars(self) -> "CatalogConfig":
        """
        Interpolate environment variables in configuration values.

        Supports ${VAR_NAME} syntax for environment variables.

        Returns:
            CatalogConfig with interpolated values

        Raises:
            ConfigurationError: If required environment variable is not set
        """
        def interpolate(value: Optional[str]) -> Optional[str]:
            if not value:
                return value

            # Match ${VAR_NAME} or ${VAR_NAME:default} pattern
            pattern = r'\$\{([A-Z_][A-Z0-9_]*)(?::([^}]*))?\}'

            def replace_env_var(match):
                var_name = match.group(1)
                default_value = match.group(2)
                env_value = os.environ.get(var_name)
                if env_value:
                    return env_value
                if default_value is not None:
                    return default_value
                raise ConfigurationError(
                    f"Required environment variable '{var_name}' is not set"
                )

            return re.sub(pattern, replace_env_var, value)

        return CatalogConfig(
            type=self.type,
            uri=interpolate(self.uri) or "",
            warehouse=interpolate(self.warehouse) or "",
            bearer_token=interpolate(self.bearer_token),
            verify_tls=self.verify_tls,
            connection_timeout=self.connection_timeout,
            request_timeout=self.request_timeout,
        )

    def validate(self) -> None:
        """
        Validate catalog configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.uri:
            raise ConfigurationError("Catalog URI is required")

        if not self.warehouse:
            raise ConfigurationError("Warehouse path is required")

        # Validate URI format
        if self.type == CatalogType.REST:
            if not self.uri.startswith(("http://", "https://")):
                raise ConfigurationError(
                    f"REST catalog URI must start with http:// or https://, got: {self.uri}"
                )

        # Validate warehouse path format
        self._validate_warehouse_path()

        # Validate timeouts
        if self.connection_timeout <= 0:
            raise ConfigurationError("Connection timeout must be positive")
        if self.request_timeout <= 0:
            raise ConfigurationError("Request timeout must be positive")

    def _validate_warehouse_path(self) -> None:
        """Validate warehouse path to prevent path traversal attacks."""
        warehouse = self.warehouse

        # Check for path traversal patterns
        if ".." in warehouse:
            raise ConfigurationError(
                f"Warehouse path cannot contain '..' for security reasons: {warehouse}"
            )

        # Validate S3 path format
        if warehouse.startswith("s3://"):
            # S3 path: s3://bucket-name/path
            path_parts = warehouse[5:].split("/", 1)
            if not path_parts[0]:
                raise ConfigurationError(
                    f"Invalid S3 path (missing bucket name): {warehouse}"
                )
            # Validate bucket name (S3 rules)
            bucket = path_parts[0]
            if not re.match(r'^[a-z0-9][a-z0-9.-]*[a-z0-9]$', bucket):
                raise ConfigurationError(
                    f"Invalid S3 bucket name: {bucket}"
                )

        # Validate GCS path format
        elif warehouse.startswith("gs://"):
            # GCS path: gs://bucket-name/path
            path_parts = warehouse[5:].split("/", 1)
            if not path_parts[0]:
                raise ConfigurationError(
                    f"Invalid GCS path (missing bucket name): {warehouse}"
                )

        # Validate Azure path format
        elif warehouse.startswith("abfs://") or warehouse.startswith("abfss://"):
            # Azure path: abfs://container@storage-account/path
            path_parts = warehouse[8:].split("/", 1)
            if not path_parts[0] or "@" not in path_parts[0]:
                raise ConfigurationError(
                    f"Invalid Azure path: {warehouse}"
                )

        else:
            # Local filesystem path
            try:
                validate_file_path(warehouse)
            except Exception as e:
                raise ConfigurationError(
                    f"Invalid warehouse path: {warehouse}: {e}"
                ) from e


@dataclass
class SchemaEvolutionConfig:
    """
    Schema evolution configuration.

    Attributes:
        mode: Evolution mode (safe, auto, strict)
        allow_column_add: Allow adding new columns
        allow_column_type_change: Allow changing column types
        allow_column_drop: Allow dropping columns (always False for safety)
    """
    mode: SchemaEvolutionMode = SchemaEvolutionMode.SAFE
    allow_column_add: bool = False
    allow_column_type_change: bool = False
    allow_column_drop: bool = False  # Never allow column drops

    def validate_type_conversion(
        self,
        old_type: str,
        new_type: str,
    ) -> bool:
        """
        Validate that a type conversion is safe.

        Args:
            old_type: Source type
            new_type: Target type

        Returns:
            True if conversion is safe

        Raises:
            ConfigurationError: If conversion is unsafe and not allowed
        """
        conversion = (old_type, new_type)

        if conversion in SAFE_TYPE_CONVERSIONS:
            return True

        # Check if unsafe conversions are allowed
        if self.allow_column_type_change:
            import warnings
            warnings.warn(
                f"Unsafe type conversion: {old_type} → {new_type}. "
                f"This may cause data loss or corruption."
            )
            return True

        raise ConfigurationError(
            f"Unsafe type conversion: {old_type} → {new_type}. "
            f"Set schema_evolution.allow_column_type_change: true to proceed."
        )


@dataclass
class DuckDBConfig:
    """
    DuckDB-specific configuration for Iceberg.

    Attributes:
        iceberg_extension: Whether to load iceberg extension
        threads: Number of threads for DuckDB
        memory_limit: DuckDB memory limit (e.g., "1GB")
    """
    iceberg_extension: bool = True
    threads: int = 4
    memory_limit: str = "1GB"


@dataclass
class MaterializationConfig:
    """
    Main materialization configuration.

    This configuration is loaded from profiles.yml and can be overridden
    by per-node YAML configuration.

    Attributes:
        enabled: Global enable/disable (opt-in by default)
        catalog: Catalog connection configuration
        default_mode: Default write mode (append or overwrite)
        duckdb: DuckDB-specific settings
        schema_evolution: Schema evolution settings
        partition_by: Default partition columns (optional)
        table: Default table name format (optional)
    """
    enabled: bool = False
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    default_mode: MaterializationMode = MaterializationMode.APPEND
    duckdb: DuckDBConfig = field(default_factory=DuckDBConfig)
    schema_evolution: SchemaEvolutionConfig = field(default_factory=SchemaEvolutionConfig)
    partition_by: List[str] = field(default_factory=list)
    table: Optional[str] = None

    def merge_with_node_config(
        self,
        node_config: Dict[str, Any],
    ) -> "MaterializationConfig":
        """
        Merge profile config with node-specific YAML config.

        Node config takes precedence over profile defaults.

        Args:
            node_config: Node's materialization section from YAML

        Returns:
            Merged MaterializationConfig
        """
        if not node_config:
            return self

        # Extract node config values
        node_enabled = node_config.get("enabled")
        node_mode = node_config.get("mode")
        node_table = node_config.get("table")
        node_partition_by = node_config.get("partition_by", [])

        # Catalog config (usually not overridden at node level)
        node_catalog = node_config.get("catalog", {})

        # Schema evolution config
        node_schema_evolution = node_config.get("schema_evolution", {})

        # Create merged config
        merged_catalog = CatalogConfig(
            type=CatalogType(node_catalog.get("type", self.catalog.type.value)),
            uri=node_catalog.get("uri", self.catalog.uri),
            warehouse=node_catalog.get("warehouse", self.catalog.warehouse),
            bearer_token=node_catalog.get("bearer_token", self.catalog.bearer_token),
            verify_tls=node_catalog.get("verify_tls", self.catalog.verify_tls),
            connection_timeout=node_catalog.get("connection_timeout", self.catalog.connection_timeout),
            request_timeout=node_catalog.get("request_timeout", self.catalog.request_timeout),
        )

        merged_schema_evolution = SchemaEvolutionConfig(
            mode=SchemaEvolutionMode(node_schema_evolution.get("mode", self.schema_evolution.mode.value)),
            allow_column_add=node_schema_evolution.get("allow_column_add", self.schema_evolution.allow_column_add),
            allow_column_type_change=node_schema_evolution.get("allow_column_type_change", self.schema_evolution.allow_column_type_change),
        )

        return MaterializationConfig(
            enabled=node_enabled if node_enabled is not None else self.enabled,
            catalog=merged_catalog,
            default_mode=MaterializationMode(node_mode) if node_mode else self.default_mode,
            duckdb=self.duckdb,  # DuckDB config typically not overridden
            schema_evolution=merged_schema_evolution,
            partition_by=node_partition_by if node_partition_by else self.partition_by,
            table=node_table if node_table else self.table,
        )

    def validate(self) -> None:
        """
        Validate the complete materialization configuration.

        Raises:
            ConfigurationError: If configuration is invalid
        """
        if not self.enabled:
            return

        # Validate catalog config
        self.catalog.validate()

        # Validate schema evolution config
        if self.schema_evolution.allow_column_drop:
            raise ConfigurationError(
                "Column drops are not allowed for data safety reasons"
            )

        # Validate partition columns format
        for partition_col in self.partition_by:
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', partition_col):
                raise ConfigurationError(
                    f"Invalid partition column name: {partition_col}. "
                    f"Must be a valid SQL identifier."
                )


def validate_table_name(table_name: str) -> str:
    """
    Validate and sanitize Iceberg table name.

    Prevents SQL injection by validating table name format.

    Args:
        table_name: Table name to validate

    Returns:
        Sanitized table name

    Raises:
        ConfigurationError: If table name is invalid
    """
    if not table_name:
        raise ConfigurationError("Table name cannot be empty")

    # Check for SQL injection patterns
    dangerous_patterns = [";", "--", "/*", "*/", "xp_", "sp_"]
    for pattern in dangerous_patterns:
        if pattern in table_name.upper():
            raise ConfigurationError(
                f"Table name contains dangerous pattern: {pattern}"
            )

    # Validate fully qualified name (catalog.namespace.table or namespace.table)
    parts = table_name.split(".")

    if len(parts) > 3:
        raise ConfigurationError(
            f"Table name has too many parts: {table_name}. "
            f"Expected format: catalog.namespace.table or namespace.table"
        )

    # Validate each part
    for part in parts:
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', part):
            raise ConfigurationError(
                f"Invalid table name part: {part}. "
                f"Must be a valid SQL identifier."
            )

    return table_name


def validate_partition_columns(
    partition_columns: List[str],
    schema_columns: List[str],
) -> None:
    """
    Validate partition columns exist in schema.

    Args:
        partition_columns: List of partition column names
        schema_columns: List of available schema columns

    Raises:
        ConfigurationError: If partition columns are invalid
    """
    schema_set = set(schema_columns)

    for col in partition_columns:
        if col not in schema_set:
            raise ConfigurationError(
                f"Partition column '{col}' not found in schema. "
                f"Available columns: {', '.join(schema_columns)}"
            )
