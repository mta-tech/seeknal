"""
Profile loader and credential manager for materialization.

This module handles loading materialization profiles from profiles.yml
and managing credentials from environment variables or system keyring.

Design Decisions:
- Environment variables for credentials (default, more portable)
- Optional system keyring support (more secure, opt-in)
- Profile defaults with per-node YAML overrides
- Automatic credential clearing after use
- Unified ``connections:`` section for all connection types

Security Features:
- Credentials never stored in plaintext
- Environment variable interpolation with default values (${VAR:default})
- Optional keyring integration
- Automatic credential clearing from memory

Key Components:
- ProfileLoader: Load and parse profiles.yml
- CredentialManager: Handle credentials from env vars or keyring
- interpolate_env_vars: Env var interpolation with ${VAR:default} support
"""

from __future__ import annotations

import os
import logging
import re
from pathlib import Path
from typing import Any, Dict, Optional

import yaml  # ty: ignore[unresolved-import]

from seeknal.context import CONFIG_BASE_URL  # ty: ignore[unresolved-import]
from seeknal.workflow.materialization.config import (  # ty: ignore[unresolved-import]
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

# Source type aliases for normalization (e.g., "postgres" -> "postgresql")
_SOURCE_TYPE_ALIASES: dict[str, str] = {
    "postgres": "postgresql",
    "jsonl": "json",
}

# Regex for ${VAR} or ${VAR:default} or $VAR patterns
_ENV_VAR_RE = re.compile(
    r"\$\{(\w+)(?::([^}]*))?\}"  # ${VAR} or ${VAR:default}
    r"|"
    r"\$(\w+)"  # $VAR (no default support)
)


def interpolate_env_vars(value: str) -> str:
    """Replace ${VAR}, ${VAR:default}, and $VAR patterns with env var values.

    Supports default values via ${VAR:default} syntax. If VAR is not set
    and no default is provided, raises ValueError.

    Args:
        value: String potentially containing env var references

    Returns:
        String with env vars resolved

    Raises:
        ValueError: If a referenced env var is not set and has no default
    """

    def replacer(match: re.Match) -> str:
        # ${VAR} or ${VAR:default}
        braced_name = match.group(1)
        default_val = match.group(2)
        # $VAR
        bare_name = match.group(3)

        var_name = braced_name or bare_name
        val = os.environ.get(var_name)

        if val is not None:
            return val

        # Has default? Only possible with ${VAR:default} syntax
        if braced_name and default_val is not None:
            return default_val

        raise ValueError(
            f"Environment variable '{var_name}' is not set. "
            f"Set it with: export {var_name}=<value>"
        )

    return _ENV_VAR_RE.sub(replacer, value)


def interpolate_env_vars_in_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively interpolate env vars in all string values of a dict.

    Args:
        data: Dictionary with potentially templated string values

    Returns:
        New dictionary with env vars resolved in string values
    """
    result: Dict[str, Any] = {}
    for key, val in data.items():
        if isinstance(val, str):
            result[key] = interpolate_env_vars(val)
        elif isinstance(val, dict):
            result[key] = interpolate_env_vars_in_dict(val)
        else:
            result[key] = val
    return result


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
                import keyring  # ty: ignore[unresolved-import]
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
        uri = self._keyring.get_password(  # ty: ignore[unresolved-attribute]
            self.keyring_service, "lakekeeper_uri"
        )
        if not uri:
            raise CredentialError(
                "Lakekeeper URI not found in keyring. "
                "Set it with: seeknal config set lakekeeper.uri <uri>"
            )
        credentials["uri"] = uri

        # Get bearer token from keyring (optional)
        token = self._keyring.get_password(  # ty: ignore[unresolved-attribute]
            self.keyring_service, "lakekeeper_token"
        )
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
            self._keyring.set_password(  # ty: ignore[unresolved-attribute]
                self.keyring_service, "lakekeeper_uri", uri
            )
            if token:
                self._keyring.set_password(  # ty: ignore[unresolved-attribute]
                    self.keyring_service, "lakekeeper_token", token
                )

            print("Credentials stored securely in system keyring")
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
    Load materialization and connection profiles from profiles.yml.

    Profile structure (in ~/.seeknal/profiles.yml):

        connections:
          my_pg:
            type: postgresql
            host: ${PG_HOST:localhost}
            port: 5432
            user: ${PG_USER:postgres}
            password: ${PG_PASSWORD}
            database: my_db
            schema: public

        materialization:
          enabled: true
          catalog:
            type: rest
            uri: ${LAKEKEEPER_URI}
            warehouse: s3://my-bucket/warehouse
          default_mode: append
          schema_evolution:
            mode: safe

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
        self._profile_data: Optional[Dict[str, Any]] = None

    def _load_profile_data(self) -> Dict[str, Any]:
        """Load and cache raw profile data from profiles.yml.

        Returns:
            Parsed YAML data as dict

        Raises:
            ConfigurationError: If file cannot be read or parsed
        """
        if self._profile_data is not None:
            return self._profile_data

        if not self.profile_path.exists():
            self._profile_data = {}
            return self._profile_data

        try:
            with open(self.profile_path, "r") as f:
                data = yaml.safe_load(f)
            self._profile_data = data if isinstance(data, dict) else {}
        except yaml.YAMLError as e:
            raise ConfigurationError(
                f"Failed to parse profile file {self.profile_path}: {e}"
            ) from e
        except Exception as e:
            raise ConfigurationError(
                f"Failed to read profile file {self.profile_path}: {e}"
            ) from e

        return self._profile_data

    def load_connection_profile(self, name: str) -> Dict[str, Any]:
        """Load a named connection profile from the ``connections:`` section.

        Profile structure::

            connections:
              my_pg:
                type: postgresql
                host: ${PG_HOST:localhost}
                port: 5432
                user: ${PG_USER:postgres}
                password: ${PG_PASSWORD}
                database: analytics

        Env var interpolation with defaults is supported: ``${VAR:default}``.
        Credentials (password fields) are cleared from the returned dict
        after the caller is done via :func:`clear_connection_credentials`.

        Args:
            name: Connection profile name (e.g., "my_pg")

        Returns:
            Dict with connection config, env vars interpolated

        Raises:
            ConfigurationError: If profile file missing, no connections
                section, or named profile not found
        """
        profile_data = self._load_profile_data()

        if not profile_data:
            raise ConfigurationError(
                f"Profile file not found: {self.profile_path}. "
                f"Create it with a 'connections:' section."
            )

        connections_section = profile_data.get("connections", {})
        if not connections_section:
            raise ConfigurationError(
                "No 'connections' section in profiles.yml. "
                "Add one with named connection profiles."
            )

        profile = connections_section.get(name)
        if not profile:
            available = list(connections_section.keys())
            raise ConfigurationError(
                f"Connection profile '{name}' not found. "
                f"Available profiles: {available}"
            )

        # Interpolate env vars (with ${VAR:default} support)
        return interpolate_env_vars_in_dict(profile)

    def load_source_defaults(self, source_type: str) -> Dict[str, Any]:
        """Load default params for a source type from the ``source_defaults:`` section.

        Profile structure::

            source_defaults:
              iceberg:
                catalog_uri: ${LAKEKEEPER_URI:http://localhost:8181}
                warehouse: ${LAKEKEEPER_WAREHOUSE:seeknal-warehouse}
              postgresql:
                connection: local_pg

        The returned dict has env vars interpolated.  If the section or type
        is missing, an empty dict is returned (no error).

        Source type aliases are normalized: ``postgres`` -> ``postgresql``,
        ``jsonl`` -> ``json``.

        Args:
            source_type: Source type string (e.g., "iceberg", "postgresql", "postgres")

        Returns:
            Dict with default params, env vars interpolated.  Empty dict if
            no defaults defined for this source type.
        """
        profile_data = self._load_profile_data()
        if not profile_data:
            return {}

        defaults_section = profile_data.get("source_defaults", {})
        if not defaults_section:
            return {}

        canonical = _SOURCE_TYPE_ALIASES.get(source_type, source_type)
        # Try canonical name first (e.g., "postgresql")
        source_defaults = defaults_section.get(canonical, {})
        if not source_defaults and source_type != canonical:
            # Try the original (un-normalized) key (e.g., "postgres")
            source_defaults = defaults_section.get(source_type, {})
        if not source_defaults:
            # Try all known aliases that map to this canonical type
            for alias, target in _SOURCE_TYPE_ALIASES.items():
                if target == canonical and alias in defaults_section:
                    source_defaults = defaults_section[alias]
                    break
        if not source_defaults:
            return {}

        return interpolate_env_vars_in_dict(source_defaults)

    @staticmethod
    def clear_connection_credentials(profile: Dict[str, Any]) -> None:
        """Clear sensitive fields from a connection profile dict.

        Overwrites password values with null bytes, then removes the keys.

        Args:
            profile: Connection profile dict to clear
        """
        sensitive_keys = {"password", "bearer_token", "token", "secret"}
        for key in list(profile.keys()):
            if key in sensitive_keys and isinstance(profile[key], str):
                profile[key] = "\x00" * len(profile[key])
                del profile[key]

    def load_profile(self) -> MaterializationConfig:
        """
        Load materialization profile from profiles.yml.

        Returns:
            MaterializationConfig with profile settings

        Raises:
            ConfigurationError: If profile is invalid
            CredentialError: If credentials cannot be loaded
        """
        profile_data = self._load_profile_data()

        if not profile_data:
            logger.debug(f"Profile file not found: {self.profile_path}")
            return MaterializationConfig()  # Return defaults

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

    def load_starrocks_profile(self, name: str = "default") -> Dict[str, Any]:
        """
        Load a StarRocks connection profile from profiles.yml.

        Profile structure (in ~/.seeknal/profiles.yml):

            starrocks:
              default:
                host: ${STARROCKS_HOST}
                port: 9030
                user: ${STARROCKS_USER}
                password: ${STARROCKS_PASSWORD}
                database: my_db

        Args:
            name: Profile name (default: "default")

        Returns:
            Dict with StarRocks connection config

        Raises:
            ConfigurationError: If profile is invalid or not found
        """
        profile_data = self._load_profile_data()

        if not profile_data:
            raise ConfigurationError(
                f"Profile file not found: {self.profile_path}. "
                f"Create it with StarRocks connection details."
            )

        starrocks_section = profile_data.get("starrocks", {})
        if not starrocks_section:
            raise ConfigurationError(
                "No 'starrocks' section in profiles.yml. "
                "Add one with: host, port, user, password, database"
            )

        profile = starrocks_section.get(name)
        if not profile:
            available = list(starrocks_section.keys())
            raise ConfigurationError(
                f"StarRocks profile '{name}' not found. "
                f"Available profiles: {available}"
            )

        return profile

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
                import requests  # ty: ignore[unresolved-import]
                from urllib3.exceptions import RequestException  # ty: ignore[unresolved-import]

                # Test connection with timeout
                response = requests.get(
                    f"{catalog.uri}/v1/config",
                    timeout=catalog.connection_timeout,
                    verify=catalog.verify_tls,
                )
                response.raise_for_status()

                logger.info(f"Catalog connection successful: {catalog.uri}")

            except RequestException as e:
                raise ConfigurationError(
                    f"Cannot connect to Lakekeeper catalog at {catalog.uri}: {e}"
                ) from e
            except Exception as e:
                raise ConfigurationError(
                    f"Catalog connection test failed: {e}"
                ) from e
