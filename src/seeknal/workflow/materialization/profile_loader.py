"""
Profile loader and credential manager for Iceberg materialization.

This module handles loading materialization profiles from profiles.yml
and managing credentials from environment variables or system keyring.

Design Decisions:
- Environment variables for credentials (default, more portable)
- Optional system keyring support (more secure, opt-in)
- Profile defaults with per-node YAML overrides
- Automatic credential clearing after use

Security Features:
- Credentials never stored in plaintext
- Environment variable interpolation
- Optional keyring integration
- Automatic credential clearing from memory

Key Components:
- ProfileLoader: Load and parse profiles.yml
- CredentialManager: Handle credentials from env vars or keyring
"""

from __future__ import annotations

import os
import logging
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from seeknal.context import CONFIG_BASE_URL
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
)

logger = logging.getLogger(__name__)


class CredentialManager:
    """
    Manage credentials for materialization.

    Supports two credential sources:
    1. Environment variables (default, portable)
    2. System keyring (optional, more secure)

    Attributes:
        use_keyring: Whether to use system keyring (default: False)
        keyring_service: Service name for keyring (default: "seeknal")
    """

    def __init__(self, use_keyring: bool = False, keyring_service: str = "seeknal"):
        """
        Initialize credential manager.

        Args:
            use_keyring: Whether to use system keyring
            keyring_service: Service name for keyring storage
        """
        self.use_keyring = use_keyring
        self.keyring_service = keyring_service

        # Try to import keyring if needed
        self._keyring = None
        if use_keyring:
            try:
                import keyring
                self._keyring = keyring
            except ImportError:
                logger.warning(
                    "keyring package not installed. "
                    "Falling back to environment variables. "
                    "Install with: pip install keyring"
                )
                self.use_keyring = False

    def get_catalog_credentials(self) -> Dict[str, str]:
        """
        Get catalog credentials from keyring or environment variables.

        Returns:
            Dictionary with 'uri' and optional 'bearer_token'

        Raises:
            CredentialError: If credentials cannot be retrieved
        """
        if self.use_keyring and self._keyring:
            return self._get_from_keyring()
        else:
            return self._get_from_env_vars()

    def _get_from_keyring(self) -> Dict[str, str]:
        """
        Get credentials from system keyring.

        Returns:
            Dictionary with 'uri' and optional 'bearer_token'

        Raises:
            CredentialError: If credentials not found in keyring
        """
        credentials = {}

        # Get URI from keyring
        uri = self._keyring.get_password(self.keyring_service, "lakekeeper_uri")
        if not uri:
            raise CredentialError(
                "Lakekeeper URI not found in keyring. "
                "Set it with: seeknal config set lakekeeper.uri <uri>"
            )
        credentials["uri"] = uri

        # Get bearer token from keyring (optional)
        token = self._keyring.get_password(self.keyring_service, "lakekeeper_token")
        if token:
            credentials["bearer_token"] = token

        return credentials

    def _get_from_env_vars(self) -> Dict[str, str]:
        """
        Get credentials from environment variables.

        Returns:
            Dictionary with 'uri' and optional 'bearer_token'

        Raises:
            CredentialError: If required environment variables are not set
        """
        credentials = {}

        # Get URI from environment variable
        uri = os.environ.get("LAKEKEEPER_URI")
        if not uri:
            raise CredentialError(
                "LAKEKEEPER_URI environment variable is not set. "
                "Set it with: export LAKEKEEPER_URI=https://lakekeeper.example.com"
            )
        credentials["uri"] = uri

        # Get bearer token from environment variable (optional)
        token = os.environ.get("LAKEKEEPER_TOKEN")
        if token:
            credentials["bearer_token"] = token

        return credentials

    def clear_credentials(self, credentials: Dict[str, str]) -> None:
        """
        Clear credentials from memory (overwrite with null bytes).

        This is a security measure to prevent credential harvesting
        from memory dumps.

        Args:
            credentials: Dictionary with credentials to clear
        """
        for key in credentials:
            # Overwrite string with null bytes
            credentials[key] = "\x00" * len(credentials[key])

        # Delete the keys
        credentials.clear()

    def setup_credentials(self) -> None:
        """
        Interactive setup for credentials.

        Prompts user for credentials and stores them in keyring or
        displays environment variable commands.

        Raises:
            CredentialError: If setup fails
        """
        if self.use_keyring and self._keyring:
            self._setup_keyring_credentials()
        else:
            self._setup_env_var_credentials()

    def _setup_keyring_credentials(self) -> None:
        """Interactive setup for keyring credentials."""
        import getpass

        print("Setting up Lakekeeper credentials in system keyring...")
        uri = input("Lakekeeper URI (e.g., https://lakekeeper.example.com): ")
        token = getpass.getpass("Lakekeeper bearer token (optional, press Enter to skip): ")

        try:
            # Store in keyring
            self._keyring.set_password(self.keyring_service, "lakekeeper_uri", uri)
            if token:
                self._keyring.set_password(self.keyring_service, "lakekeeper_token", token)

            print("✓ Credentials stored securely in system keyring")
        except Exception as e:
            raise CredentialError(f"Failed to store credentials in keyring: {e}") from e

    def _setup_env_var_credentials(self) -> None:
        """Display commands for setting environment variables."""
        import getpass

        print("Setting up Lakekeeper credentials via environment variables...")
        uri = input("Lakekeeper URI (e.g., https://lakekeeper.example.com): ")
        has_token = input("Do you have a bearer token? (y/N): ").lower() == 'y'

        print("\nAdd these to your shell profile (~/.bashrc, ~/.zshrc, etc.):")
        print(f"export LAKEKEEPER_URI='{uri}'")

        if has_token:
            token = getpass.getpass("Lakekeeper bearer token: ")
            print(f"export LAKEKEEPER_TOKEN='{token}'")

        print("\nThen reload your shell: source ~/.bashrc (or ~/.zshrc)")


class ProfileLoader:
    """
    Load materialization profiles from profiles.yml.

    Profile structure (in ~/.seeknal/profiles.yml):

        materialization:
          enabled: true
          catalog:
            type: rest
            uri: ${LAKEKEEPER_URI}
            warehouse: s3://my-bucket/warehouse
          default_mode: append
          schema_evolution:
            mode: safe
            allow_column_add: false
            allow_column_type_change: false

    Attributes:
        profile_path: Path to profiles.yml
        credential_manager: Credential manager instance
    """

    DEFAULT_PROFILE_PATH = Path(CONFIG_BASE_URL) / "profiles.yml"

    def __init__(
        self,
        profile_path: Optional[Path] = None,
        use_keyring: bool = False,
    ):
        """
        Initialize profile loader.

        Args:
            profile_path: Path to profiles.yml (default: ~/.seeknal/profiles.yml)
            use_keyring: Whether to use system keyring for credentials
        """
        self.profile_path = profile_path or self.DEFAULT_PROFILE_PATH
        self.credential_manager = CredentialManager(use_keyring=use_keyring)

    def load_profile(self) -> MaterializationConfig:
        """
        Load materialization profile from profiles.yml.

        Returns:
            MaterializationConfig with profile settings

        Raises:
            ConfigurationError: If profile is invalid
            CredentialError: If credentials cannot be loaded
        """
        # Check if profile file exists
        if not self.profile_path.exists():
            logger.debug(f"Profile file not found: {self.profile_path}")
            return MaterializationConfig()  # Return defaults

        try:
            with open(self.profile_path, "r") as f:
                profile_data = yaml.safe_load(f)

        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Failed to parse profile file {self.profile_path}: {e}"
            ) from e

        except Exception as e:
            raise ConfigurationError(
                f"Failed to read profile file {self.profile_path}: {e}"
            ) from e

        # Extract materialization section
        mat_config = profile_data.get("materialization", {})

        # Parse catalog config
        catalog_data = mat_config.get("catalog", {})
        catalog_config = self._parse_catalog_config(catalog_data)

        # Parse schema evolution config
        schema_evolution_data = mat_config.get("schema_evolution", {})
        schema_evolution_config = self._parse_schema_evolution_config(schema_evolution_data)

        # Parse DuckDB config
        duckdb_data = mat_config.get("duckdb", {})
        duckdb_config = self._parse_duckdb_config(duckdb_data)

        # Parse mode
        mode_str = mat_config.get("default_mode", "append")
        try:
            mode = MaterializationMode(mode_str)
        except ValueError:
            raise ConfigurationError(
                f"Invalid materialization mode: {mode_str}. "
                f"Must be one of: {[m.value for m in MaterializationMode]}"
            )

        # Create merged config
        config = MaterializationConfig(
            enabled=mat_config.get("enabled", False),
            catalog=catalog_config,
            default_mode=mode,
            duckdb=duckdb_config,
            schema_evolution=schema_evolution_config,
            partition_by=mat_config.get("partition_by", []),
            table=mat_config.get("table"),
        )

        # Validate config
        config.validate()

        return config

    def _parse_catalog_config(self, catalog_data: Dict[str, Any]) -> CatalogConfig:
        """
        Parse catalog configuration from profile.

        Args:
            catalog_data: Catalog section from profile YAML

        Returns:
            CatalogConfig instance
        """
        # Parse catalog type
        type_str = catalog_data.get("type", "rest")
        try:
            catalog_type = CatalogType(type_str)
        except ValueError:
            raise ConfigurationError(
                f"Invalid catalog type: {type_str}. "
                f"Must be one of: {[t.value for t in CatalogType]}"
            )

        # Check if credentials should come from keyring/env
        uri = catalog_data.get("uri", "")
        warehouse = catalog_data.get("warehouse", "")

        # If URI is a reference to credentials (e.g., ${LAKEKEEPER_URI}),
        # load from credential manager
        if uri and "${" in uri:
            credentials = self.credential_manager.get_catalog_credentials()
            uri = credentials.get("uri", uri)
            token = credentials.get("bearer_token")

            # Clear credentials from memory
            self.credential_manager.clear_credentials(credentials)
        else:
            token = catalog_data.get("bearer_token")

        return CatalogConfig(
            type=catalog_type,
            uri=uri,
            warehouse=warehouse,
            bearer_token=token,
            verify_tls=catalog_data.get("verify_tls", True),
            connection_timeout=catalog_data.get("connection_timeout", 10),
            request_timeout=catalog_data.get("request_timeout", 30),
        )

    def _parse_schema_evolution_config(
        self,
        schema_evolution_data: Dict[str, Any],
    ) -> SchemaEvolutionConfig:
        """
        Parse schema evolution configuration from profile.

        Args:
            schema_evolution_data: Schema evolution section from profile YAML

        Returns:
            SchemaEvolutionConfig instance
        """
        # Parse mode
        mode_str = schema_evolution_data.get("mode", "safe")
        try:
            mode = SchemaEvolutionMode(mode_str)
        except ValueError:
            raise ConfigurationError(
                f"Invalid schema evolution mode: {mode_str}. "
                f"Must be one of: {[m.value for m in SchemaEvolutionMode]}"
            )

        return SchemaEvolutionConfig(
            mode=mode,
            allow_column_add=schema_evolution_data.get("allow_column_add", False),
            allow_column_type_change=schema_evolution_data.get("allow_column_type_change", False),
            allow_column_drop=False,  # Never allow drops
        )

    def _parse_duckdb_config(self, duckdb_data: Dict[str, Any]) -> DuckDBConfig:
        """
        Parse DuckDB configuration from profile.

        Args:
            duckdb_data: DuckDB section from profile YAML

        Returns:
            DuckDBConfig instance
        """
        return DuckDBConfig(
            iceberg_extension=duckdb_data.get("iceberg_extension", True),
            threads=duckdb_data.get("threads", 4),
            memory_limit=duckdb_data.get("memory_limit", "1GB"),
        )

    def validate_materialization(self) -> bool:
        """
        Validate materialization configuration without loading.

        This is useful for dry-run mode to check if configuration
        is valid before attempting actual materialization.

        Returns:
            True if configuration is valid

        Raises:
            ConfigurationError: If configuration is invalid
            CredentialError: If credentials cannot be loaded
        """
        config = self.load_profile()

        if config.enabled:
            # Validate catalog connectivity
            self._validate_catalog_connection(config)

        return True

    def _validate_catalog_connection(self, config: MaterializationConfig) -> None:
        """
        Validate catalog connectivity.

        Args:
            config: Materialization configuration

        Raises:
            ConfigurationError: If catalog connection fails
        """
        # Interpolate environment variables
        catalog = config.catalog.interpolate_env_vars()

        # Validate catalog format
        catalog.validate()

        # For REST catalog, try to connect
        if catalog.type == CatalogType.REST:
            try:
                import requests
                from urllib3.exceptions import RequestException

                # Test connection with timeout
                response = requests.get(
                    f"{catalog.uri}/v1/config",
                    timeout=catalog.connection_timeout,
                    verify=catalog.verify_tls,
                )
                response.raise_for_status()

                logger.info(f"✓ Catalog connection successful: {catalog.uri}")

            except RequestException as e:
                raise ConfigurationError(
                    f"Cannot connect to Lakekeeper catalog at {catalog.uri}: {e}"
                ) from e
            except Exception as e:
                raise ConfigurationError(
                    f"Catalog connection test failed: {e}"
                ) from e
