"""
PostgreSQL metadata queries for incremental detection.

This module provides functionality for querying PostgreSQL metadata
to detect data changes via watermark comparison, similar to the
Iceberg snapshot detection pattern.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def get_max_watermark(
    connection_params: Dict[str, Any],
    schema: str,
    table: str,
    time_column: str,
) -> Optional[str]:
    """
    Query the maximum value of a time column from a PostgreSQL table.

    This is used for incremental detection: compare the current MAX(time_column)
    with the stored watermark to detect if new data has arrived.

    Args:
        connection_params: Connection parameters dict (host, port, database, user, password, etc.)
                          Can be from profiles.yml or inline params.
        schema: PostgreSQL schema name (e.g., "public", "bronze")
        table: PostgreSQL table name
        time_column: Column name to query MAX() on (must be a timestamp/date type)

    Returns:
        String representation of the MAX value, or None if:
        - Table is empty (MAX returns NULL)
        - Connection fails
        - Query fails

    Note:
        The watermark is stored as-is from the database (no timezone conversion).
        Comparison is string-based for simplicity.
    """
    import duckdb

    from seeknal.connections.postgresql import (
        parse_postgresql_config,
        mask_password,
    )
    from seeknal.validation import validate_column_name

    # Validate column name to prevent SQL injection
    validate_column_name(time_column)

    try:
        # Parse connection config
        config = parse_postgresql_config(connection_params)
        conn_str = config.to_libpq_string()

        # Create temporary DuckDB connection for the query
        con = duckdb.connect()

        try:
            # Load postgres extension
            try:
                con.execute("LOAD postgres")
            except Exception:
                con.execute("INSTALL postgres; LOAD postgres;")

            # Attach PostgreSQL database with unique alias
            pg_alias = "_pg_watermark_check"
            try:
                con.execute(f"""
                ATTACH '{conn_str}'
                AS {pg_alias} (TYPE postgres, READ_ONLY)
                """)
            except Exception as attach_err:
                # May already be attached
                if "already exists" not in str(attach_err).lower():
                    raise

            # Build qualified table name
            qualified_table = f"{pg_alias}.{schema}.{table}"

            # Query MAX(time_column)
            result = con.execute(
                f'SELECT MAX("{time_column}") FROM {qualified_table}'
            ).fetchone()

            if result and result[0] is not None:
                watermark = str(result[0])
                logger.debug(
                    f"PostgreSQL watermark query: MAX({time_column}) = {watermark} "
                    f"from {config.host}:{config.port}/{config.database}.{schema}.{table}"
                )
                return watermark
            else:
                logger.debug(
                    f"PostgreSQL watermark query returned NULL "
                    f"from {config.host}:{config.port}/{config.database}.{schema}.{table}"
                )
                return None

        finally:
            con.close()

    except Exception as e:
        safe_msg = mask_password(str(e))
        logger.warning(
            f"Failed to query PostgreSQL watermark from "
            f"{connection_params.get('host', 'unknown')}:{connection_params.get('port', 5432)}/"
            f"{connection_params.get('database', 'unknown')}.{schema}.{table}: {safe_msg}"
        )
        return None


def resolve_postgresql_connection(
    params: Dict[str, Any],
    profile_loader: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Resolve PostgreSQL connection parameters from params and profile.

    Supports two modes:
    1. Profile-based: params.connection references a profiles.yml connection
    2. Inline: params contains host, port, database, user, password directly

    Args:
        params: Node params dict, may contain 'connection' key or inline credentials
        profile_loader: Optional ProfileLoader instance for resolving connection references

    Returns:
        Resolved connection parameters dict ready for parse_postgresql_config
    """
    # Check for profile-based connection reference
    connection_name = params.get("connection")

    if connection_name and profile_loader:
        # Load connection from profile
        try:
            connections = profile_loader.load_connections()
            if connection_name in connections:
                conn_config = connections[connection_name]
                # Merge: inline params override profile values
                resolved = {**conn_config, **{k: v for k, v in params.items() if k != "connection"}}
                return resolved
        except Exception as e:
            logger.warning(f"Failed to load connection '{connection_name}' from profile: {e}")

    # Fall back to inline params
    return {k: v for k, v in params.items() if k not in ("connection", "query")}
