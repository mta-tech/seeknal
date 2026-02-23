"""
PostgreSQL materialization configuration models.

This module defines the configuration models for PostgreSQL materialization,
supporting full replacement, incremental-by-time, and upsert-by-key modes.

Design Decisions:
- Dataclasses for consistency with existing materialization config
- No dependencies beyond stdlib + dataclasses
- Table names validated as 2-part format (schema.table_name)
- Mode-specific field validation (time_column, unique_keys)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class PostgresConfigError(Exception):
    """Raised when PostgreSQL materialization configuration is invalid."""

    pass


class PostgresMaterializationMode(str, Enum):
    """PostgreSQL materialization write mode."""

    FULL = "full"
    INCREMENTAL_BY_TIME = "incremental_by_time"
    UPSERT_BY_KEY = "upsert_by_key"


# Regex for valid SQL identifiers (used for table name parts and column names)
_SQL_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Regex for 2-part table name: schema.table_name
_TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclass
class PostgresMaterializationConfig:
    """
    PostgreSQL materialization configuration.

    Attributes:
        type: Always "postgresql"
        connection: Connection profile name from profiles.yml
        table: Target table in schema.table_name format (2-part)
        mode: Write mode (full, incremental_by_time, upsert_by_key)
        time_column: Time column for incremental_by_time mode
        lookback: Lookback days for incremental_by_time mode
        unique_keys: Unique key columns for upsert_by_key mode
        create_table: Auto-create table on first run
        cascade: Use CASCADE in DROP statements (default RESTRICT)
    """

    type: str = "postgresql"
    connection: str = ""
    table: str = ""
    mode: PostgresMaterializationMode = PostgresMaterializationMode.FULL
    time_column: Optional[str] = None
    lookback: int = 0
    unique_keys: List[str] = field(default_factory=list)
    create_table: bool = True
    cascade: bool = False

    def validate(self) -> None:
        """
        Validate the PostgreSQL materialization configuration.

        Raises:
            PostgresConfigError: If configuration is invalid
        """
        # connection must not be empty
        if not self.connection:
            raise PostgresConfigError(
                "PostgreSQL materialization requires a 'connection' profile name"
            )

        # table must be 2-part format: schema.table_name
        if not self.table:
            raise PostgresConfigError(
                "PostgreSQL materialization requires a 'table' name"
            )
        if not _TABLE_NAME_RE.match(self.table):
            raise PostgresConfigError(
                f"Invalid table name '{self.table}'. "
                f"Must be 2-part format: schema.table_name "
                f"(e.g., 'public.orders')"
            )

        # Check for SQL injection patterns in table name
        dangerous_patterns = [";", "--", "/*", "*/", "xp_", "sp_"]
        for pattern in dangerous_patterns:
            if pattern in self.table:
                raise PostgresConfigError(
                    f"Table name contains dangerous pattern: {pattern}"
                )

        # Mode-specific validation
        if self.mode == PostgresMaterializationMode.INCREMENTAL_BY_TIME:
            if not self.time_column:
                raise PostgresConfigError(
                    "Mode 'incremental_by_time' requires 'time_column' to be set"
                )
            if not _SQL_IDENTIFIER_RE.match(self.time_column):
                raise PostgresConfigError(
                    f"Invalid time_column '{self.time_column}'. "
                    f"Must be a valid SQL identifier."
                )

        if self.mode == PostgresMaterializationMode.UPSERT_BY_KEY:
            if not self.unique_keys:
                raise PostgresConfigError(
                    "Mode 'upsert_by_key' requires 'unique_keys' to be a non-empty list"
                )
            for key in self.unique_keys:
                if not _SQL_IDENTIFIER_RE.match(key):
                    raise PostgresConfigError(
                        f"Invalid unique key '{key}'. "
                        f"Must be a valid SQL identifier."
                    )

        # lookback must be >= 0
        if self.lookback < 0:
            raise PostgresConfigError(
                f"'lookback' must be >= 0, got {self.lookback}"
            )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> PostgresMaterializationConfig:
        """
        Parse a dict (from YAML) into a PostgresMaterializationConfig.

        Handles string-to-enum conversion for mode.

        Args:
            data: Dictionary with configuration values

        Returns:
            PostgresMaterializationConfig instance

        Raises:
            PostgresConfigError: If mode value is invalid
        """
        mode_value = data.get("mode", "full")
        try:
            mode = PostgresMaterializationMode(mode_value)
        except ValueError:
            valid_modes = [m.value for m in PostgresMaterializationMode]
            raise PostgresConfigError(
                f"Invalid PostgreSQL materialization mode '{mode_value}'. "
                f"Valid modes: {', '.join(valid_modes)}"
            )

        return cls(
            type=data.get("type", "postgresql"),
            connection=data.get("connection", ""),
            table=data.get("table", ""),
            mode=mode,
            time_column=data.get("time_column"),
            lookback=data.get("lookback", 0),
            unique_keys=data.get("unique_keys", []),
            create_table=data.get("create_table", True),
            cascade=data.get("cascade", False),
        )
