"""
StarRocks connection factory.

Creates pymysql connections to StarRocks (MySQL-protocol compatible, port 9030).
Supports env var interpolation for credentials.

StarRocks type mapping to DuckDB:
    StarRocks       -> DuckDB
    BOOLEAN         -> BOOLEAN
    TINYINT         -> TINYINT
    SMALLINT        -> SMALLINT
    INT             -> INTEGER
    BIGINT          -> BIGINT
    FLOAT           -> FLOAT
    DOUBLE          -> DOUBLE
    DECIMAL(p,s)    -> DECIMAL(p,s)
    CHAR/VARCHAR    -> VARCHAR
    STRING          -> VARCHAR
    DATE            -> DATE
    DATETIME        -> TIMESTAMP
    JSON            -> VARCHAR
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict
from urllib.parse import urlparse


@dataclass
class StarRocksConfig:
    """Parsed StarRocks connection configuration."""
    host: str = "localhost"
    port: int = 9030
    user: str = "root"
    password: str = ""
    database: str = ""

    def to_pymysql_kwargs(self) -> Dict[str, Any]:
        """Convert to pymysql.connect() keyword arguments."""
        kwargs: Dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "database": self.database,
            "charset": "utf8mb4",
            "connect_timeout": 10,
        }
        if self.password:
            kwargs["password"] = self.password
        return kwargs


# StarRocks -> DuckDB type mapping
STARROCKS_TO_DUCKDB_TYPES: Dict[str, str] = {
    "BOOLEAN": "BOOLEAN",
    "TINYINT": "TINYINT",
    "SMALLINT": "SMALLINT",
    "INT": "INTEGER",
    "BIGINT": "BIGINT",
    "LARGEINT": "HUGEINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "CHAR": "VARCHAR",
    "VARCHAR": "VARCHAR",
    "STRING": "VARCHAR",
    "DATE": "DATE",
    "DATETIME": "TIMESTAMP",
    "JSON": "VARCHAR",
    "ARRAY": "VARCHAR",
    "MAP": "VARCHAR",
    "STRUCT": "VARCHAR",
    "HLL": "VARCHAR",
    "BITMAP": "VARCHAR",
    "PERCENTILE": "VARCHAR",
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


def parse_starrocks_config(config: Dict[str, Any]) -> StarRocksConfig:
    """Parse StarRocks connection config from profiles.yml section.

    Expected config format:
        type: starrocks
        host: ${STARROCKS_HOST}
        port: 9030
        user: ${STARROCKS_USER}
        password: ${STARROCKS_PASSWORD}
        database: my_db

    Args:
        config: Dict from profiles.yml starrocks section

    Returns:
        StarRocksConfig with interpolated values
    """
    return StarRocksConfig(
        host=_interpolate_env_vars(str(config.get("host", "localhost"))),
        port=int(config.get("port", 9030)),
        user=_interpolate_env_vars(str(config.get("user", "root"))),
        password=_interpolate_env_vars(str(config.get("password", ""))),
        database=_interpolate_env_vars(str(config.get("database", ""))),
    )


def parse_starrocks_url(url: str) -> StarRocksConfig:
    """Parse a StarRocks connection URL.

    Format: starrocks://user:pass@host:9030/database

    Args:
        url: Connection URL string

    Returns:
        StarRocksConfig
    """
    url = _interpolate_env_vars(url)
    parsed = urlparse(url)

    return StarRocksConfig(
        host=parsed.hostname or "localhost",
        port=parsed.port or 9030,
        user=parsed.username or "root",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") if parsed.path else "",
    )


def create_starrocks_connection(config: Dict[str, Any]):
    """Create a pymysql connection to StarRocks.

    Args:
        config: Connection config dict (from profiles.yml or parsed URL)

    Returns:
        pymysql.Connection

    Raises:
        ImportError: If pymysql is not installed
        pymysql.Error: If connection fails
    """
    try:
        import pymysql
    except ImportError:
        raise ImportError(
            "pymysql is required for StarRocks connections. "
            "Install with: pip install pymysql"
        )

    sr_config = parse_starrocks_config(config)
    return pymysql.connect(**sr_config.to_pymysql_kwargs())


def create_starrocks_connection_from_url(url: str):
    """Create a pymysql connection from a StarRocks URL.

    Args:
        url: starrocks://user:pass@host:9030/database

    Returns:
        pymysql.Connection
    """
    try:
        import pymysql
    except ImportError:
        raise ImportError(
            "pymysql is required for StarRocks connections. "
            "Install with: pip install pymysql"
        )

    sr_config = parse_starrocks_url(url)
    return pymysql.connect(**sr_config.to_pymysql_kwargs())


def map_starrocks_type_to_duckdb(sr_type: str) -> str:
    """Map a StarRocks column type to DuckDB equivalent.

    Args:
        sr_type: StarRocks type string (e.g., "INT", "VARCHAR(255)", "DECIMAL(10,2)")

    Returns:
        DuckDB type string
    """
    # Extract base type (before parentheses)
    base = sr_type.strip().upper().split("(")[0].strip()

    # For DECIMAL, preserve precision/scale
    if base == "DECIMAL" and "(" in sr_type:
        return sr_type.upper()

    return STARROCKS_TO_DUCKDB_TYPES.get(base, "VARCHAR")


def check_starrocks_connection(config: Dict[str, Any]) -> tuple[bool, str]:
    """Test StarRocks connectivity.

    Args:
        config: Connection config dict

    Returns:
        Tuple of (success, message)
    """
    try:
        conn = create_starrocks_connection(config)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()[0]
            return True, f"Connected to StarRocks {version}"
        finally:
            conn.close()
    except ImportError as e:
        return False, str(e)
    except Exception as e:
        return False, f"Connection failed: {e}"
