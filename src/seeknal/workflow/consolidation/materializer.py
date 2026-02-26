"""
Entity consolidation materializer.

Handles writing consolidated entity views to external targets:
- Iceberg: Native struct columns via DuckDB iceberg extension
- PostgreSQL: Flattened fgname__feature prefixed columns
"""

import logging
import zlib
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# PostgreSQL identifier limit
PG_MAX_IDENTIFIER_LENGTH = 63


def _truncate_pg_column_name(fg_name: str, feature_name: str) -> str:
    """Build a PostgreSQL-safe column name from fg_name and feature_name.

    If the combined name exceeds 63 characters, truncates and appends
    a CRC32 hash suffix for uniqueness.

    Args:
        fg_name: Feature group name
        feature_name: Feature column name

    Returns:
        Column name safe for PostgreSQL (max 63 chars)
    """
    full_name = f"{fg_name}__{feature_name}"
    if len(full_name) <= PG_MAX_IDENTIFIER_LENGTH:
        return full_name

    # Truncate with hash suffix: {fg[:N]}__{feat[:N]}_{crc32_8hex}
    hash_suffix = format(zlib.crc32(full_name.encode()) & 0xFFFFFFFF, '08x')
    max_part = (PG_MAX_IDENTIFIER_LENGTH - len(hash_suffix) - 3) // 2  # 3 for __ and _
    truncated = f"{fg_name[:max_part]}__{feature_name[:max_part]}_{hash_suffix}"
    logger.warning(
        "Column name '%s' exceeds 63 chars, truncated to '%s'",
        full_name, truncated,
    )
    return truncated


def _build_flatten_select(
    entity_parquet_path: Path,
    join_keys: List[str],
    fg_features: Dict[str, List[str]],
) -> str:
    """Build a SELECT statement that flattens struct columns to prefixed flat columns.

    Args:
        entity_parquet_path: Path to the consolidated parquet
        join_keys: Entity join key column names
        fg_features: Dict of fg_name -> list of feature names

    Returns:
        SQL SELECT statement string
    """
    select_parts = list(join_keys) + ["event_time"]

    for fg_name, features in sorted(fg_features.items()):
        for feat in features:
            col_name = _truncate_pg_column_name(fg_name, feat)
            select_parts.append(f"{fg_name}.{feat} AS {col_name}")

    return (
        f"SELECT {', '.join(select_parts)} "
        f"FROM '{entity_parquet_path}'"
    )


def _cast_hugeint_structs(con: Any, parquet_path: Path) -> List[str]:
    """Detect HUGEINT fields inside struct columns and return CAST expressions.

    DuckDB COUNT(*) and SUM() return HUGEINT which Iceberg doesn't support.
    This scans struct column schemas and generates CAST(... AS BIGINT) where needed.

    Args:
        con: DuckDB connection
        parquet_path: Path to consolidated parquet

    Returns:
        List of warning messages for HUGEINT columns found
    """
    warnings_list = []
    try:
        rows = con.execute(
            f"SELECT column_name, column_type FROM "
            f"(DESCRIBE SELECT * FROM '{parquet_path}')"
        ).fetchall()
        for col_name, col_type in rows:
            if "HUGEINT" in str(col_type).upper():
                warnings_list.append(
                    f"Column '{col_name}' contains HUGEINT type — "
                    f"Iceberg does not support HUGEINT. Consider using "
                    f"CAST(... AS BIGINT) in your transforms."
                )
    except Exception as e:
        logger.warning("Failed to check HUGEINT types: %s", e)

    return warnings_list


class ConsolidationMaterializer:
    """Materializes consolidated entity views to external targets.

    Supports:
    - Iceberg: Native struct columns via DuckDB iceberg extension
    - PostgreSQL: Flattened fg_name__feature_name prefixed columns
    """

    def materialize_iceberg(
        self,
        con: Any,
        entity_name: str,
        parquet_path: Path,
        target_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Write consolidated entity view to Iceberg with native struct columns.

        Args:
            con: DuckDB connection with iceberg extension loaded
            entity_name: Entity name
            parquet_path: Path to consolidated parquet
            target_config: Iceberg target configuration with table_pattern, etc.

        Returns:
            Dict with success status and metadata
        """
        try:
            # Check for HUGEINT warnings
            hugeint_warnings = _cast_hugeint_structs(con, parquet_path)
            for warning in hugeint_warnings:
                logger.warning(warning)

            # Resolve table name from pattern
            table_pattern = target_config.get("table_pattern", "")
            table_name = table_pattern.replace("{entity}", entity_name)
            if not table_name:
                return {"success": False, "error": "No table_pattern configured"}

            mode = target_config.get("mode", "overwrite")

            if mode == "overwrite":
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                con.execute(
                    f"CREATE TABLE {table_name} AS "
                    f"SELECT * FROM '{parquet_path}'"
                )
            else:
                # Try INSERT INTO, create if not exists
                try:
                    con.execute(
                        f"INSERT INTO {table_name} "
                        f"SELECT * FROM '{parquet_path}'"
                    )
                except Exception:
                    con.execute(
                        f"CREATE TABLE {table_name} AS "
                        f"SELECT * FROM '{parquet_path}'"
                    )

            row_count = con.execute(
                f"SELECT COUNT(*) FROM '{parquet_path}'"
            ).fetchone()[0]

            logger.info(
                "Materialized entity '%s' to Iceberg table '%s' (%d rows)",
                entity_name, table_name, row_count,
            )

            return {
                "success": True,
                "table": table_name,
                "row_count": row_count,
                "hugeint_warnings": hugeint_warnings,
            }

        except Exception as e:
            logger.warning(
                "Iceberg materialization failed for entity '%s': %s",
                entity_name, e,
            )
            return {"success": False, "error": str(e)}

    def materialize_postgresql(
        self,
        con: Any,
        entity_name: str,
        parquet_path: Path,
        join_keys: List[str],
        fg_features: Dict[str, List[str]],
        target_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Write consolidated entity view to PostgreSQL with flattened columns.

        Flattens struct columns to {fg_name}__{feature_name} prefixed columns,
        then uses DuckDB's postgres extension to write.

        Args:
            con: DuckDB connection with postgres extension loaded
            entity_name: Entity name
            parquet_path: Path to consolidated parquet
            join_keys: Entity join key columns
            fg_features: Dict of fg_name -> list of feature names
            target_config: PostgreSQL target config with connection, table_pattern, etc.

        Returns:
            Dict with success status and metadata
        """
        try:
            table_pattern = target_config.get("table_pattern", "")
            table_name = table_pattern.replace("{entity}", entity_name)
            if not table_name:
                return {"success": False, "error": "No table_pattern configured"}

            connection_name = target_config.get("connection", "")
            mode = target_config.get("mode", "full")

            # Build flattened SELECT
            flatten_sql = _build_flatten_select(
                parquet_path, join_keys, fg_features,
            )

            if mode == "full":
                # DROP + CREATE pattern
                con.execute(f"DROP TABLE IF EXISTS {connection_name}.{table_name}")
                con.execute(
                    f"CREATE TABLE {connection_name}.{table_name} AS {flatten_sql}"
                )
            else:
                # INSERT INTO
                try:
                    con.execute(
                        f"INSERT INTO {connection_name}.{table_name} {flatten_sql}"
                    )
                except Exception:
                    con.execute(
                        f"CREATE TABLE {connection_name}.{table_name} AS {flatten_sql}"
                    )

            row_count = con.execute(
                f"SELECT COUNT(*) FROM ({flatten_sql}) t"
            ).fetchone()[0]

            logger.info(
                "Materialized entity '%s' to PostgreSQL '%s.%s' (%d rows, flattened)",
                entity_name, connection_name, table_name, row_count,
            )

            return {
                "success": True,
                "table": f"{connection_name}.{table_name}",
                "row_count": row_count,
                "mode": "flattened",
            }

        except Exception as e:
            logger.warning(
                "PostgreSQL materialization failed for entity '%s': %s",
                entity_name, e,
            )
            return {"success": False, "error": str(e)}
