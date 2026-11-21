"""DuckDB acceleration cache loader for Seeknal Serving."""

from __future__ import annotations

from seeknal.serving.accel.engines import (
    DuckDBTarget,
    LakebaseSource,
    check_duckdb_version,
    create_target_factory,
    get_installed_duckdb_version,
)
from seeknal.serving.accel.iceberg_source import (
    AccelIcebergCatalogConfig,
    IcebergCatalogSpec,
    IcebergS3Spec,
    IcebergSetupStep,
    IcebergSource,
    IcebergSourceConfig,
    build_iceberg_table_specs,
    create_iceberg_target_factory,
    execute_iceberg_source_setup,
    load_iceberg_source_config,
    plan_iceberg_count_sql,
    plan_iceberg_load_sql,
    plan_iceberg_snapshot_sql,
    plan_iceberg_source_setup,
)
from seeknal.serving.accel.loader import (
    AccelerationError,
    BadInputError,
    LoadError,
    RowCountMismatchError,
    plan_sql,
    run_load,
    scrub_text,
)
from seeknal.serving.accel.placement import (
    generate_placement,
    generate_schemas,
    write_placement_files,
)
from seeknal.serving.accel.settings import (
    DEFAULT_EXPECTED_DUCKDB_VERSION,
    AccelerationSettings,
    get_expected_duckdb_version,
    get_settings,
)
from seeknal.serving.accel.tables import (
    ColumnSpec,
    TableSpec,
    build_table_specs,
    map_type,
)

__version__ = "1.0.0"

__all__ = [
    "AccelIcebergCatalogConfig",
    "AccelerationError",
    "AccelerationSettings",
    "BadInputError",
    "ColumnSpec",
    "DEFAULT_EXPECTED_DUCKDB_VERSION",
    "DuckDBTarget",
    "IcebergCatalogSpec",
    "IcebergS3Spec",
    "IcebergSetupStep",
    "IcebergSource",
    "IcebergSourceConfig",
    "LakebaseSource",
    "LoadError",
    "RowCountMismatchError",
    "TableSpec",
    "__version__",
    "build_iceberg_table_specs",
    "build_table_specs",
    "check_duckdb_version",
    "create_iceberg_target_factory",
    "create_target_factory",
    "execute_iceberg_source_setup",
    "generate_placement",
    "generate_schemas",
    "get_expected_duckdb_version",
    "get_installed_duckdb_version",
    "get_settings",
    "load_iceberg_source_config",
    "map_type",
    "plan_iceberg_count_sql",
    "plan_iceberg_load_sql",
    "plan_iceberg_snapshot_sql",
    "plan_iceberg_source_setup",
    "plan_sql",
    "run_load",
    "scrub_text",
    "write_placement_files",
]
