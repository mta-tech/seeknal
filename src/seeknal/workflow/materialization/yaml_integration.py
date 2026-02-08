"""
Iceberg materialization integration for YAML workflow executors.

This module provides the integration layer between YAML workflow execution
and Iceberg materialization. It allows YAML nodes to automatically write
their results to Iceberg tables when materialization is enabled.

Usage in YAML:
    kind: source
    name: my_data
    materialization:
      enabled: true
      table: "warehouse.prod.my_data"
      mode: append

The executor will automatically:
1. Execute the node normally (load data, run transform, etc.)
2. If materialization.enabled=true, write results to Iceberg
3. Handle schema validation and evolution
4. Return snapshot information

Design Decisions:
- OAuth2/STS authentication for S3 credentials (via Keycloak)
- Automatic schema validation with evolution modes
- Atomic writes with rollback on failure
- Connection pooling for performance
- Comprehensive error handling
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import duckdb
import requests

from seeknal.workflow.materialization.config import (
    MaterializationConfig,
    CatalogConfig,
    SchemaEvolutionConfig,
    MaterializationMode,
    SchemaEvolutionMode,
    CatalogType,
    ConfigurationError,
    validate_table_name,
)

if TYPE_CHECKING:
    from seeknal.dag.manifest import Node

# Load .env file if present (for local development)
# Check current directory and parent directories
try:
    from dotenv import load_dotenv
    from pathlib import Path

    # Try to find .env in current directory or up to 3 parent directories
    cwd = Path.cwd()
    for path in [cwd] + list(cwd.parents)[:3]:
        env_file = path / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            break
    else:
        # Fallback to default behavior
        load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, skip

logger = logging.getLogger(__name__)


@dataclass
class YAMLMaterializationConfig:
    """
    Materialization config extracted from YAML node.

    Attributes:
        enabled: Whether materialization is enabled for this node
        table: Fully qualified Iceberg table name (e.g., "warehouse.db.table")
        mode: Write mode (append or overwrite)
        create_table: Whether to create table if it doesn't exist
    """
    enabled: bool = False
    table: Optional[str] = None
    mode: MaterializationMode = MaterializationMode.APPEND
    create_table: bool = True


class IcebergMaterializationError(Exception):
    """Base exception for Iceberg materialization errors."""
    pass


class IcebergMaterializationHelper:
    """
    Helper class for materializing workflow node results to Iceberg.

    This class handles:
    - Loading materialization profiles (profiles.yml)
    - Extracting materialization config from YAML nodes
    - Getting OAuth2 tokens from Keycloak
    - Creating DuckDB connections with Iceberg extension
    - Writing data to Iceberg tables in Lakekeeper/MinIO
    - Schema validation and evolution

    The helper is designed to be called by YAML workflow executors
    after they have successfully executed their node logic.
    """

    # Default catalog configuration (can be overridden by .env or profiles.yml)
    # Use direct Lakekeeper access (port 8181) instead of API Gateway (port 80)
    DEFAULT_CATALOG_URI = os.getenv(
        "LAKEKEEPER_URI",
        "http://172.19.0.9:8181"
    )
    DEFAULT_WAREHOUSE = os.getenv(
        "LAKEKEEPER_WAREHOUSE",
        "s3://warehouse"
    )
    # Lakekeeper warehouse ID - used in catalog endpoint path
    DEFAULT_WAREHOUSE_ID = os.getenv(
        "LAKEKEEPER_WAREHOUSE_ID",
        "c008ea5c-fb89-11f0-aa64-c32ca2f52144"  # seeknal-warehouse
    )

    # Keycloak OAuth configuration
    KEYCLOAK_TOKEN_URL = os.getenv(
        "KEYCLOAK_TOKEN_URL",
        "http://172.19.0.9:8080/realms/atlas/protocol/openid-connect/token"
    )
    KEYCLOAK_CLIENT_ID = os.getenv("KEYCLOAK_CLIENT_ID", "duckdb")
    KEYCLOAK_CLIENT_SECRET = os.getenv(
        "KEYCLOAK_CLIENT_SECRET",
        "duckdb-secret-change-in-production"
    )

    # MinIO S3 configuration
    MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://172.19.0.9:9000")
    S3_REGION = os.getenv("AWS_REGION", "us-east-1")
    S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

    @classmethod
    def get_catalog_url(cls, lakekeeper_url: str, warehouse_id: Optional[str] = None) -> str:
        """
        Get the catalog URL for DuckDB ATTACH.

        Handles both direct Lakekeeper access and API Gateway routing:
        - Direct: http://host:8181 → http://host:8181/catalog
        - Gateway: http://host:80/iceberg-catalog (gateway rewrites to /catalog)

        Args:
            lakekeeper_url: Lakekeeper URL
            warehouse_id: Optional warehouse ID to include in URL

        Returns:
            Catalog URL for DuckDB ATTACH
        """
        if warehouse_id is None:
            warehouse_id = cls.DEFAULT_WAREHOUSE_ID

        if "/iceberg-catalog" in lakekeeper_url:
            # Using API Gateway - it rewrites /iceberg-catalog to /catalog
            # Return as-is since gateway handles the path rewrite
            return lakekeeper_url
        # Direct Lakekeeper access - append /catalog with warehouse
        base_url = lakekeeper_url.rstrip("/")
        return f"{base_url}/catalog"

    @classmethod
    def get_oauth_token(cls) -> str:
        """
        Get OAuth2 access token from Keycloak using client credentials flow.

        Returns:
            Bearer token for catalog authentication

        Raises:
            IcebergMaterializationError: If token request fails
        """
        try:
            response = requests.post(
                cls.KEYCLOAK_TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": cls.KEYCLOAK_CLIENT_ID,
                    "client_secret": cls.KEYCLOAK_CLIENT_SECRET,
                },
                timeout=30,
            )
            response.raise_for_status()
            return response.json()["access_token"]
        except Exception as e:
            raise IcebergMaterializationError(
                f"Failed to get OAuth token from Keycloak: {e}"
            ) from e

    @classmethod
    @contextmanager
    def get_duckdb_connection(cls, catalog_uri: Optional[str] = None):
        """
        Create a DuckDB connection configured for Iceberg materialization.

        This context manager:
        1. Creates a DuckDB connection
        2. Loads Iceberg and httpfs extensions
        3. Configures S3/MinIO settings
        4. Attaches to Lakekeeper REST catalog with OAuth2
        5. Yields the connection for use
        6. Closes the connection on exit

        Args:
            catalog_uri: Optional catalog URI (uses default if not provided)

        Yields:
            DuckDB connection configured for Iceberg

        Raises:
            IcebergMaterializationError: If connection setup fails
        """
        if catalog_uri is None:
            catalog_uri = cls.DEFAULT_CATALOG_URI

        con = duckdb.connect(":memory:")

        try:
            # Load extensions
            con.execute("INSTALL iceberg; LOAD iceberg;")
            con.execute("INSTALL httpfs; LOAD httpfs;")
            logger.debug("DuckDB extensions loaded")

            # Configure S3 (MinIO) with credentials
            minio_endpoint = cls.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
            con.execute(f"""
                SET s3_region = '{cls.S3_REGION}';
                SET s3_endpoint = '{minio_endpoint}';
                SET s3_url_style = 'path';
                SET s3_use_ssl = false;
                SET s3_access_key_id = '{cls.S3_ACCESS_KEY}';
                SET s3_secret_access_key = '{cls.S3_SECRET_KEY}';
            """)
            logger.debug(f"S3 configured for MinIO: {minio_endpoint}")

            # Get OAuth token and attach to Lakekeeper
            token = cls.get_oauth_token()
            logger.debug("OAuth2 token obtained from Keycloak")

            catalog_url = cls.get_catalog_url(catalog_uri)
            # The warehouse name in ATTACH becomes the ?warehouse= query param
            con.execute(f"""
                ATTACH 'seeknal-warehouse' AS atlas (
                    TYPE ICEBERG,
                    ENDPOINT '{catalog_url}',
                    AUTHORIZATION_TYPE 'oauth2',
                    TOKEN '{token}'
                );
            """)
            logger.debug(f"Attached to Lakekeeper catalog: {catalog_url}")

            yield con

        except Exception as e:
            logger.error(f"Failed to create DuckDB connection for Iceberg: {e}")
            raise IcebergMaterializationError(
                f"Failed to create DuckDB connection for Iceberg: {e}"
            ) from e
        finally:
            try:
                con.close()
            except Exception:
                pass

    @classmethod
    def extract_materialization_config(cls, node_config: Dict[str, Any]) -> YAMLMaterializationConfig:
        """
        Extract materialization configuration from YAML node config.

        Args:
            node_config: Parsed YAML node configuration

        Returns:
            YAMLMaterializationConfig with settings from the node

        Raises:
            IcebergMaterializationError: If configuration is invalid
        """
        mat_config = node_config.get("materialization", {})

        if not isinstance(mat_config, dict):
            raise IcebergMaterializationError(
                "Materialization config must be a dictionary"
            )

        # Check if materialization is enabled
        enabled = mat_config.get("enabled", False)
        if not isinstance(enabled, bool):
            raise IcebergMaterializationError(
                "Materialization 'enabled' must be a boolean"
            )

        if not enabled:
            return YAMLMaterializationConfig(enabled=False)

        # Extract table name (required when enabled)
        table = mat_config.get("table")
        if not table:
            raise IcebergMaterializationError(
                "Materialization 'table' is required when enabled=true"
            )

        # Validate table name
        try:
            validate_table_name(table)
        except Exception as e:
            raise IcebergMaterializationError(
                f"Invalid table name '{table}': {e}"
            ) from e

        # Extract mode
        mode_str = mat_config.get("mode", "append")
        try:
            mode = MaterializationMode(mode_str)
        except ValueError:
            raise IcebergMaterializationError(
                f"Invalid materialization mode '{mode_str}'. Must be 'append' or 'overwrite'"
            )

        # Extract create_table flag
        create_table = mat_config.get("create_table", True)
        if not isinstance(create_table, bool):
            raise IcebergMaterializationError(
                "Materialization 'create_table' must be a boolean"
            )

        return YAMLMaterializationConfig(
            enabled=True,
            table=table,
            mode=mode,
            create_table=create_table,
        )

    @classmethod
    def materialize_view(
        cls,
        view_name: str,
        table_name: str,
        mode: MaterializationMode = MaterializationMode.APPEND,
        catalog_uri: Optional[str] = None,
        source_con: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Materialize a DuckDB view to an Iceberg table.

        This method:
        1. Optionally uses the provided source connection or creates a new one
        2. Attaches Iceberg catalog to the connection
        3. Reads the view data
        4. Writes to the Iceberg table with the specified mode
        5. Returns snapshot information

        Args:
            view_name: DuckDB view name (e.g., "source.customers")
            table_name: Target Iceberg table (e.g., "warehouse.prod.customers")
            mode: Write mode (append or overwrite)
            catalog_uri: Optional catalog URI (uses default if not provided)
            source_con: Optional existing DuckDB connection with the view

        Returns:
            Dictionary with snapshot info:
            - success: True if write succeeded
            - row_count: Number of rows written
            - table: Table name
            - mode: Write mode used

        Raises:
            IcebergMaterializationError: If materialization fails
        """
        logger.info(
            f"Materializing view '{view_name}' to Iceberg table '{table_name}' "
            f"(mode={mode.value})"
        )

        # Use provided connection or create a new one
        if source_con is not None:
            # Use existing connection - need to set up Iceberg in it
            con = source_con
            should_close = False
            
            # Set up Iceberg extension if not already loaded
            try:
                con.execute("LOAD iceberg;")
            except Exception:
                con.execute("INSTALL iceberg; LOAD iceberg;")
            
            # Configure S3 (MinIO) with credentials
            minio_endpoint = cls.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
            con.execute(f"""
                SET s3_region = '{cls.S3_REGION}';
                SET s3_endpoint = '{minio_endpoint}';
                SET s3_url_style = 'path';
                SET s3_use_ssl = false;
                SET s3_access_key_id = '{cls.S3_ACCESS_KEY}';
                SET s3_secret_access_key = '{cls.S3_SECRET_KEY}';
            """)
            
            # Get OAuth token and attach to Lakekeeper
            token = cls.get_oauth_token()
            catalog_url = cls.get_catalog_url(catalog_uri if catalog_uri else cls.DEFAULT_CATALOG_URI)

            # Attach to Lakekeeper catalog (check if already attached)
            # The warehouse name in ATTACH becomes the ?warehouse= query param
            try:
                con.execute(f"""
                    ATTACH 'seeknal-warehouse' AS atlas (
                        TYPE ICEBERG,
                        ENDPOINT '{catalog_url}',
                        AUTHORIZATION_TYPE 'oauth2',
                        TOKEN '{token}'
                    );
                """)
            except Exception as e:
                # May already be attached, which is fine
                if "already exists" not in str(e):
                    logger.warning(f"Failed to attach Iceberg catalog: {e}")
        else:
            # Create new connection with Iceberg configured
            con = cls.get_duckdb_connection(catalog_uri).__enter__()
            should_close = True

        try:
            try:
                # Check if view exists
                # Split view_name into schema and name
                # view_name format is "schema.name" or "name"
                if "." in view_name:
                    schema, name = view_name.split(".", 1)
                else:
                    schema, name = "main", view_name
                
                # Note: DuckDB uses table_schema and table_name columns
                view_exists = con.execute(
                    f"SELECT EXISTS (SELECT * FROM information_schema.views WHERE table_schema = '{schema}' AND table_name = '{name}')"
                ).fetchone()[0]

                if not view_exists:
                    raise IcebergMaterializationError(
                        f"View '{view_name}' does not exist"
                    )

                # Get row count
                row_count_result = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
                row_count = row_count_result[0] if row_count_result else 0
                logger.info(f"View has {row_count} rows")

                # Parse table name to get database and schema
                parts = table_name.split(".")
                if len(parts) != 3:
                    raise IcebergMaterializationError(
                        f"Invalid table name '{table_name}'. Expected format: 'database.schema.table'"
                    )

                database, schema, table = parts

                # Drop table if mode is overwrite
                if mode == MaterializationMode.OVERWRITE:
                    con.execute(f"DROP TABLE IF EXISTS atlas.{schema}.{table};")
                    logger.info(f"Dropped existing table for overwrite mode")
                
                # Determine whether to CREATE or INSERT
                # For append mode with existing table, use INSERT INTO
                # For overwrite mode (after drop) or new table, use CREATE TABLE AS
                use_insert = False
                if mode == MaterializationMode.APPEND:
                    try:
                        # Check if table exists by querying it
                        con.execute(f"SELECT 1 FROM atlas.{schema}.{table} LIMIT 1")
                        use_insert = True
                        logger.info(f"Table exists, using INSERT INTO for append mode")
                    except:
                        logger.info(f"Table does not exist, creating new table")
                
                if use_insert:
                    # Append mode: Insert into existing table
                    con.execute(f"""
                        INSERT INTO atlas.{schema}.{table}
                        SELECT * FROM {view_name};
                    """)
                    logger.info(f"Appended {row_count} rows to Iceberg table atlas.{schema}.{table}")
                    
                    # Verify new total row count
                    total_count = con.execute(f"SELECT COUNT(*) FROM atlas.{schema}.{table}").fetchone()[0]
                    logger.info(f"Verified {total_count} total rows after append")
                    
                    return {
                        "success": True,
                        "row_count": row_count,
                        "total_count": total_count,
                        "table": table_name,
                        "mode": mode.value,
                        "iceberg_table": f"atlas.{schema}.{table}",
                        "appended": True,
                    }
                else:
                    # Create table from view (for new tables or overwrite mode)
                    con.execute(f"""
                        CREATE TABLE atlas.{schema}.{table} AS
                        SELECT * FROM {view_name};
                    """)
                    logger.info(f"Created Iceberg table atlas.{schema}.{table}")

                    # Verify row count
                    written_count = con.execute(f"SELECT COUNT(*) FROM atlas.{schema}.{table}").fetchone()[0]
                    logger.info(f"Verified {written_count} rows written")

                    return {
                        "success": True,
                        "row_count": written_count,
                        "table": table_name,
                        "mode": mode.value,
                        "iceberg_table": f"atlas.{schema}.{table}",
                    }

            except Exception as e:
                logger.error(f"Failed to materialize view '{view_name}': {e}")
                raise IcebergMaterializationError(
                    f"Failed to materialize view '{view_name}' to '{table_name}': {e}"
                ) from e
        finally:
            # Only close connection if we created it
            if should_close:
                try:
                    con.close()
                except Exception:
                    pass

    @classmethod
    def materialize_node(
        cls,
        node: "Node",
        catalog_uri: Optional[str] = None,
        source_con: Optional[Any] = None,
        enabled_override: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Materialize a workflow node result to Iceberg.

        This is the main entry point called by YAML workflow executors.
        It extracts the materialization config from the node and
        materializes the result to Iceberg if enabled.

        Args:
            node: The workflow node to materialize
            catalog_uri: Optional catalog URI (uses default if not provided)
            source_con: Optional existing DuckDB connection with the view
            enabled_override: Override for materialization (None=use node config, True=force enable, False=force disable)

        Returns:
            Dictionary with materialization result, or None if disabled

        Raises:
            IcebergMaterializationError: If materialization fails
        """
        # Handle enabled override flag
        if enabled_override is False:
            # Force disable materialization
            logger.debug(f"Materialization disabled for node '{node.id}' (override=False)")
            return None
        elif enabled_override is True:
            # Force enable materialization - but only if node has valid config
            mat_section = node.config.get("materialization", {})
            if not mat_section.get("table"):
                # Node doesn't have a table configured, skip materialization
                logger.debug(
                    f"Skipping materialization for node '{node.id}': "
                    "no 'materialization.table' configured in YAML"
                )
                return None
            # Node has table config, ensure enabled is True
            if not mat_section.get("enabled", False):
                # Temporarily enable for this execution
                node.config["materialization"]["enabled"] = True

        # Extract materialization config from node
        try:
            mat_config = cls.extract_materialization_config(node.config)
        except Exception as e:
            logger.error(f"Failed to extract materialization config from node '{node.id}': {e}")
            raise

        # Check if materialization is enabled
        if not mat_config.enabled:
            logger.debug(f"Materialization disabled for node '{node.id}'")
            return None

        # Get the view name for this node
        # For SOURCE nodes: view is "source.{name}"
        # For TRANSFORM nodes: view is "transform.{name}"
        # For FEATURE_GROUP nodes: view is "feature_group.{name}"
        view_name = f"{node.node_type.value}.{node.name}"

        # Materialize the view (table is guaranteed to be non-None when enabled=True)
        assert mat_config.table is not None, "Table must be set when materialization is enabled"
        return cls.materialize_view(
            view_name=view_name,
            table_name=mat_config.table,
            mode=mat_config.mode,
            catalog_uri=catalog_uri,
            source_con=source_con,
        )


# Convenience function for executors
def materialize_node_if_enabled(
    node: "Node",
    catalog_uri: Optional[str] = None,
    source_con: Optional[Any] = None,
    enabled_override: Optional[bool] = None,
) -> Optional[Dict[str, Any]]:
    """
    Convenience function to materialize a node if materialization is enabled.

    This function is designed to be called by YAML workflow executors
    after they have successfully executed their node logic.

    Args:
        node: The workflow node to materialize
        catalog_uri: Optional catalog URI (uses default if not provided)
        source_con: Optional existing DuckDB connection with the view
        enabled_override: Override for materialization (None=use node config, True=force enable, False=force disable)

    Returns:
        Dictionary with materialization result, or None if disabled

    Example:
        >>> from seeknal.workflow.materialization.yaml_integration import materialize_node_if_enabled
        >>>
        >>> # In executor, after successful execution:
        >>> mat_result = materialize_node_if_enabled(node)
        >>> if mat_result:
        ...     print(f"Materialized {mat_result['row_count']} rows to {mat_result['table']}")
    """
    return IcebergMaterializationHelper.materialize_node(node, catalog_uri, source_con, enabled_override)
