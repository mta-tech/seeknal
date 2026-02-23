"""
PostgreSQL connection factory for DuckDB ATTACH.

Builds libpq connection strings for DuckDB's postgres_scanner extension.
No external dependencies (no psycopg2) -- the actual connection is via DuckDB.

Supports env var interpolation for credentials.

PostgreSQL type mapping to DuckDB:
    PostgreSQL      -> DuckDB
    boolean         -> BOOLEAN
    smallint        -> SMALLINT
    integer         -> INTEGER
    bigint          -> BIGINT
    real            -> FLOAT
    double precision-> DOUBLE
    numeric(p,s)    -> DECIMAL(p,s)
    char/varchar    -> VARCHAR
    text            -> VARCHAR
    date            -> DATE
    timestamp       -> TIMESTAMP
    timestamptz     -> TIMESTAMPTZ
    json/jsonb      -> VARCHAR
    uuid            -> VARCHAR
    bytea           -> BLOB
"""
from __future__ import annotations

import importlib
import os
import re
from dataclasses import dataclass
from typing import Any, Dict
from urllib.parse import unquote, urlparse


@dataclass
class PostgreSQLConfig:
    """Parsed PostgreSQL connection configuration."""

    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    schema: str = "public"
    sslmode: str = "prefer"
    connect_timeout: int = 10

    def to_libpq_string(self) -> str:
        """Convert to libpq connection string for DuckDB ATTACH.

        Returns:
            A string like "host=X port=Y dbname=Z user=W password=P sslmode=S connect_timeout=T"
        """
        parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"dbname={self.database}",
            f"user={self.user}",
        ]
        if self.password:
            parts.append(f"password={self.password}")
        parts.append(f"sslmode={self.sslmode}")
        parts.append(f"connect_timeout={self.connect_timeout}")
        return " ".join(parts)


# PostgreSQL -> DuckDB type mapping
POSTGRESQL_TO_DUCKDB_TYPES: Dict[str, str] = {
    "BOOLEAN": "BOOLEAN",
    "BOOL": "BOOLEAN",
    "SMALLINT": "SMALLINT",
    "INT2": "SMALLINT",
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
    "INT4": "INTEGER",
    "BIGINT": "BIGINT",
    "INT8": "BIGINT",
    "REAL": "FLOAT",
    "FLOAT4": "FLOAT",
    "DOUBLE PRECISION": "DOUBLE",
    "FLOAT8": "DOUBLE",
    "CHAR": "VARCHAR",
    "CHARACTER": "VARCHAR",
    "VARCHAR": "VARCHAR",
    "CHARACTER VARYING": "VARCHAR",
    "TEXT": "VARCHAR",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMPTZ",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    "JSON": "VARCHAR",
    "JSONB": "VARCHAR",
    "UUID": "VARCHAR",
    "BYTEA": "BLOB",
    "SERIAL": "INTEGER",
    "BIGSERIAL": "BIGINT",
    "SMALLSERIAL": "SMALLINT",
}


def _interpolate_env_vars(value: str) -> str:
    """Replace ${VAR} and $VAR patterns with environment variable values.

    Args:
        value: String potentially containing env var references

    Returns:
        String with env vars resolved

    Raises:
        ValueError: If a referenced env var is not set
    """

    def replacer(match: re.Match) -> str:
        var_name = match.group(1) or match.group(2)
        val = os.environ.get(var_name)
        if val is None:
            raise ValueError(
                f"Environment variable '{var_name}' is not set. "
                f"Set it with: export {var_name}=<value>"
            )
        return val

    # Match ${VAR} or $VAR (only word characters)
    return re.sub(r"\$\{(\w+)\}|\$(\w+)", replacer, value)


def parse_postgresql_config(config: Dict[str, Any]) -> PostgreSQLConfig:
    """Parse PostgreSQL connection config from profiles.yml section.

    Expected config format:
        type: postgresql
        host: ${PG_HOST}
        port: 5432
        user: ${PG_USER}
        password: ${PG_PASSWORD}
        database: my_db
        schema: public
        sslmode: prefer
        connect_timeout: 10

    Args:
        config: Dict from profiles.yml postgresql section

    Returns:
        PostgreSQLConfig with interpolated values

    Raises:
        ValueError: If port is not a valid positive integer or host is empty
    """
    host = _interpolate_env_vars(str(config.get("host", "localhost")))
    if not host.strip():
        raise ValueError("PostgreSQL host cannot be empty")

    port = int(config.get("port", 5432))
    if port <= 0 or port > 65535:
        raise ValueError(f"PostgreSQL port must be between 1 and 65535, got {port}")

    connect_timeout = int(config.get("connect_timeout", 10))
    if connect_timeout < 0:
        raise ValueError(
            f"PostgreSQL connect_timeout must be non-negative, got {connect_timeout}"
        )

    return PostgreSQLConfig(
        host=host,
        port=port,
        database=_interpolate_env_vars(str(config.get("database", "postgres"))),
        user=_interpolate_env_vars(str(config.get("user", "postgres"))),
        password=_interpolate_env_vars(str(config.get("password", ""))),
        schema=_interpolate_env_vars(str(config.get("schema", "public"))),
        sslmode=_interpolate_env_vars(str(config.get("sslmode", "prefer"))),
        connect_timeout=connect_timeout,
    )


def parse_postgresql_url(url: str) -> PostgreSQLConfig:
    """Parse a PostgreSQL connection URL.

    Format: postgresql://user:pass@host:5432/database?sslmode=require

    Args:
        url: Connection URL string

    Returns:
        PostgreSQLConfig
    """
    url = _interpolate_env_vars(url)
    parsed = urlparse(url)

    # Extract query parameters for sslmode, connect_timeout, schema
    query_params: Dict[str, str] = {}
    if parsed.query:
        for param in parsed.query.split("&"):
            if "=" in param:
                key, val = param.split("=", 1)
                query_params[key] = val

    password = unquote(parsed.password) if parsed.password else ""

    return PostgreSQLConfig(
        host=parsed.hostname or "localhost",
        port=parsed.port or 5432,
        database=parsed.path.lstrip("/") if parsed.path and parsed.path != "/" else "postgres",
        user=parsed.username or "postgres",
        password=password,
        schema=query_params.get("schema", "public"),
        sslmode=query_params.get("sslmode", "prefer"),
        connect_timeout=int(query_params.get("connect_timeout", "10")),
    )


def build_attach_string(config: PostgreSQLConfig) -> str:
    """Build the full libpq connection string suitable for DuckDB ATTACH.

    Args:
        config: PostgreSQLConfig instance

    Returns:
        libpq connection string
    """
    return config.to_libpq_string()


def mask_password(conn_str: str) -> str:
    """Replace password values with '***' for safe logging.

    Handles both libpq format (password=secret) and URL format
    (postgresql://user:secret@host).

    Args:
        conn_str: Connection string that may contain a password

    Returns:
        String with password masked
    """
    # Mask libpq format: password=secret (space-delimited or end of string)
    masked = re.sub(r"(password=)\S+", r"\1***", conn_str)
    # Mask URL format: ://user:password@
    masked = re.sub(r"(://[^:]+:)[^@]+(@)", r"\1***\2", masked)
    return masked


def check_postgresql_connection(config: PostgreSQLConfig) -> tuple[bool, str]:
    """Test PostgreSQL connectivity via DuckDB's postgres extension.

    Args:
        config: PostgreSQLConfig instance

    Returns:
        Tuple of (success, message)
    """
    try:
        duckdb_mod = importlib.import_module("duckdb")
    except ImportError:
        return False, (
            "duckdb is required for PostgreSQL connections. "
            "Install with: pip install duckdb"
        )

    conn_str = config.to_libpq_string()
    try:
        db = duckdb_mod.connect()
        try:
            db.execute("INSTALL postgres; LOAD postgres;")
            db.execute(f"ATTACH '{conn_str}' AS pg_test (TYPE POSTGRES, READ_ONLY)")
            db.execute("SELECT 1")
            return True, f"Connected to PostgreSQL at {config.host}:{config.port}/{config.database}"
        finally:
            db.close()
    except Exception as e:
        return False, f"Connection failed: {e}"


def map_postgresql_type_to_duckdb(pg_type: str) -> str:
    """Map a PostgreSQL column type to DuckDB equivalent.

    Args:
        pg_type: PostgreSQL type string (e.g., "INTEGER", "VARCHAR(255)", "NUMERIC(10,2)")

    Returns:
        DuckDB type string
    """
    # Extract base type (before parentheses)
    base = pg_type.strip().upper().split("(")[0].strip()

    # For NUMERIC/DECIMAL, preserve precision/scale
    if base in ("NUMERIC", "DECIMAL") and "(" in pg_type:
        return "DECIMAL" + pg_type[pg_type.index("("):].upper()

    return POSTGRESQL_TO_DUCKDB_TYPES.get(base, "VARCHAR")
