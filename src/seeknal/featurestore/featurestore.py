import shutil
from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Any, List, Optional, Union
from abc import ABC, abstractmethod
import pandas as pd
from pydantic import BaseModel
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
import pendulum
from delta.tables import *
import typer
import pyarrow as pa
from tabulate import tabulate
import os
import json
import hashlib
from ..entity import Entity
from ..request import FeatureGroupRequest
from ..context import CONFIG_BASE_URL, logger
from ..tasks.duckdb import DuckDBTask
from ..validation import (
    validate_database_name,
    validate_table_name,
    validate_sql_value,
)
from ..utils.path_security import warn_if_insecure_path


class OfflineStoreEnum(str, Enum):
    """Enumeration of supported offline storage types.

    Attributes:
        HIVE_TABLE: Store features as a Hive table in a database.
        FILE: Store features as files on the filesystem (e.g., Delta format).
        ICEBERG: Store features in Apache Iceberg tables with ACID transactions,
            time travel, and cloud storage compatibility.
    """

    HIVE_TABLE = "hive_table"
    FILE = "file"
    ICEBERG = "iceberg"


class OnlineStoreEnum(str, Enum):
    """Enumeration of supported online storage types.

    Attributes:
        HIVE_TABLE: Store features as a Hive table for online serving.
        FILE: Store features as Parquet files for online serving.
    """

    HIVE_TABLE = "hive_table"
    FILE = "file"


class FileKindEnum(str, Enum):
    """Enumeration of supported file formats for feature storage.

    Attributes:
        DELTA: Delta Lake format, providing ACID transactions and versioning.
    """

    DELTA = "delta"


@dataclass
class FeatureStoreFileOutput:
    """
    Configuration for file-based feature store output.

    Attributes:
        path: The filesystem path for storing feature data. A security warning
            will be logged if this path is in an insecure location (e.g., /tmp).
        kind: The file format to use (default: DELTA).
    """

    path: str
    kind: FileKindEnum = FileKindEnum.DELTA

    def __post_init__(self):
        """Validate the path and warn if it's in an insecure location."""
        warn_if_insecure_path(
            self.path, context="feature store file output", logger=logger
        )

    def to_dict(self):
        """Convert the configuration to a dictionary representation.

        Returns:
            dict: Dictionary with 'path' and 'kind' keys.
        """
        return {"path": self.path, "kind": self.kind.value}


@dataclass
class FeatureStoreHiveTableOutput:
    """Configuration for Hive table-based feature store output.

    Attributes:
        database: The Hive database name where features will be stored.
    """

    database: str

    def to_dict(self):
        """Convert the configuration to a dictionary representation.

        Returns:
            dict: Dictionary with 'database' key.
        """
        return {"database": self.database}


@dataclass
class IcebergStoreOutput:
    """Iceberg storage configuration for feature group materialization.

    This configuration enables storing features in Apache Iceberg tables with
    ACID transactions, time travel, and cloud storage compatibility.

    Args:
        table: Table name within namespace
        catalog: Catalog name from profiles.yml (default: "lakekeeper")
        namespace: Iceberg namespace/database (default: "default")
        warehouse: Optional warehouse path override (s3://, gs://, azure://)
        mode: Write mode - "append" or "overwrite" (default: "append")
    """

    table: str
    catalog: str = "lakekeeper"
    namespace: str = "default"
    warehouse: Optional[str] = None
    mode: str = "append"

    def to_dict(self):
        """Convert the configuration to a dictionary representation.

        Returns:
            dict: Dictionary with all Iceberg configuration keys.
        """
        return {
            "catalog": self.catalog,
            "warehouse": self.warehouse,
            "namespace": self.namespace,
            "table": self.table,
            "mode": self.mode,
        }


@dataclass
class OfflineStore:
    """
    Configuration for offline feature store storage.

    Attributes:
        value: The storage configuration (path for FILE, database for HIVE_TABLE).
            A security warning will be logged if a file path is in an insecure
            location (e.g., /tmp).
        kind: The storage type (FILE or HIVE_TABLE).
        name: Optional name for this offline store configuration.
    """

    value: Optional[
        Union[str, FeatureStoreFileOutput, FeatureStoreHiveTableOutput, IcebergStoreOutput]
    ] = None
    kind: OfflineStoreEnum = OfflineStoreEnum.HIVE_TABLE
    name: Optional[str] = None

    def __post_init__(self):
        # Validate path security for file-based storage
        if self.kind == OfflineStoreEnum.FILE:
            self._validate_path_security()

        # Keep value as-is for type safety - conversion happens in __call__
        # The value can be FeatureStoreFileOutput, FeatureStoreHiveTableOutput,
        # or IcebergStoreOutput - don't convert to dict

    def _validate_path_security(self):
        """Validate and warn if the storage path is insecure."""
        if self.value is None:
            return

        # If value is FeatureStoreFileOutput, it validates in its own __post_init__
        if isinstance(self.value, FeatureStoreFileOutput):
            return

        # Extract path from value
        path = None
        if isinstance(self.value, str):
            # Try to parse as JSON
            try:
                value_dict = json.loads(self.value)
                path = value_dict.get("path")
            except (json.JSONDecodeError, TypeError):
                # Not valid JSON, might be a direct path string
                path = self.value
        elif isinstance(self.value, dict):
            path = self.value.get("path")

        if path:
            warn_if_insecure_path(path, context="offline store", logger=logger)

    def get_or_create(self):
        """Retrieve an existing offline store or create a new one.

        If an offline store with the specified name exists, it is retrieved and
        its configuration is loaded. Otherwise, a new offline store is created
        with the current configuration.

        Returns:
            OfflineStore: The current instance with id populated.

        Note:
            The store name defaults to "default" if not specified.
        """
        if self.name is None:
            name = "default"
        else:
            name = self.name
        offline_store = FeatureGroupRequest.get_offline_store_by_name(self.kind, name)
        if offline_store is None:
            offline_store = FeatureGroupRequest.save_offline_store(
                self.kind, self.value, name
            )
            self.id = offline_store.id
        else:
            self.id = offline_store.id
            self.kind = offline_store.kind
            value_params = json.loads(offline_store.params)
            if self.kind == OfflineStoreEnum.HIVE_TABLE:
                if self.value is not None:
                    self.value = FeatureStoreHiveTableOutput(**value_params)
            elif self.kind == OfflineStoreEnum.FILE:
                if self.value is not None:
                    self.value = FeatureStoreFileOutput(
                        path=value_params["path"],
                        kind=FileKindEnum(value_params["kind"]),
                    )
            elif self.kind == OfflineStoreEnum.ICEBERG:
                if self.value is not None:
                    self.value = IcebergStoreOutput(**value_params)
            else:
                self.value = value_params
        return self

    @staticmethod
    def list():
        """List all registered offline stores.

        Displays a formatted table of all offline stores with their names,
        kinds, and configuration values. If no stores are found, displays
        an appropriate message.

        Returns:
            None: Output is printed to the console.
        """
        offline_stores = FeatureGroupRequest.get_offline_stores()
        if offline_stores:
            offline_stores = [
                {
                    "name": offline_store.name,
                    "kind": offline_store.kind,
                    "value": offline_store.params,
                }
                for offline_store in offline_stores
            ]
            typer.echo(tabulate(offline_stores, headers="keys", tablefmt="github"))
        else:
            typer.echo("No offline stores found.")

    def delete(
        self,
        spark: Optional[SparkSession] = None,
        *args,
        **kwargs,
    ) -> bool:
        """Delete storage for a feature group from the offline store.

        For FILE type: Deletes the directory containing the delta table.
        For HIVE_TABLE type: Drops the Hive table using Spark SQL.

        Args:
            spark: SparkSession instance (required for HIVE_TABLE, optional for FILE).
            **kwargs: Must include 'project' and 'entity' to construct the table name.

        Returns:
            bool: True if deletion was successful or resource didn't exist.

        Raises:
            ValueError: If required kwargs (project, entity) are missing.
        """
        project = kwargs.get("project")
        entity = kwargs.get("entity")

        if project is None or entity is None:
            raise ValueError("Both 'project' and 'entity' are required for deletion")

        # Validate project and entity parameters
        validate_table_name(project)
        validate_table_name(entity)
        table_name = "fg_{}__{}".format(project, entity)

        match self.kind:
            case OfflineStoreEnum.FILE:
                if self.value is None:
                    base_path = CONFIG_BASE_URL
                    path = os.path.join(base_path, "data", table_name)
                else:
                    if isinstance(self.value, FeatureStoreFileOutput):
                        base_path = self.value.path
                    elif isinstance(self.value, dict):
                        base_path = self.value.get("path", CONFIG_BASE_URL)
                    else:
                        base_path = self.value
                    path = os.path.join(base_path, table_name)

                if os.path.exists(path):
                    shutil.rmtree(path)
                    logger.info(f"Deleted offline store files at: {path}")
                else:
                    logger.info(f"Offline store path does not exist: {path}")
                return True

            case OfflineStoreEnum.HIVE_TABLE:
                if spark is None:
                    spark = SparkSession.builder.getOrCreate()

                if self.value is None:
                    database = "seeknal"
                elif isinstance(self.value, FeatureStoreHiveTableOutput):
                    database = self.value.database
                elif isinstance(self.value, dict):
                    database = self.value.get("database", "seeknal")
                else:
                    database = "seeknal"

                # Validate database name before use in SQL
                validate_database_name(database)

                full_table_name = "{}.{}".format(database, table_name)
                if spark.catalog.tableExists(full_table_name):
                    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                    logger.info(f"Dropped Hive table: {full_table_name}")
                else:
                    logger.info(f"Hive table does not exist: {full_table_name}")
                return True

            case OfflineStoreEnum.ICEBERG:
                # Iceberg table deletion
                if self.value is None:
                    logger.warning("Iceberg configuration required for deletion")
                    return False

                # Get configuration
                if isinstance(self.value, IcebergStoreOutput):
                    iceberg_config = self.value
                elif isinstance(self.value, dict):
                    iceberg_config = IcebergStoreOutput(**self.value)
                else:
                    logger.warning(f"Invalid Iceberg configuration type: {type(self.value)}")
                    return False

                # Import Iceberg operations
                from seeknal.workflow.materialization.operations import DuckDBIcebergExtension
                from seeknal.workflow.materialization.profile_loader import ProfileLoader
                from seeknal.workflow.materialization.config import MaterializationConfig, ConfigurationError

                # Load profile for catalog configuration
                profile_loader = ProfileLoader()
                try:
                    profile_config = profile_loader.load_profile()
                except (ConfigurationError, Exception) as e:
                    logger.warning(f"Could not load materialization profile: {e}. Using defaults.")
                    profile_config = MaterializationConfig()

                # Get catalog configuration
                catalog_uri = profile_config.catalog.uri if profile_config.catalog.uri else ""
                warehouse_path = (
                    iceberg_config.warehouse or
                    profile_config.catalog.warehouse
                )
                bearer_token = profile_config.catalog.bearer_token

                # Validate required configuration
                if not catalog_uri or not warehouse_path:
                    logger.warning("Catalog URI and warehouse path required for Iceberg deletion")
                    return False

                # Validate table name
                validate_table_name(iceberg_config.table)
                validate_table_name(iceberg_config.namespace)

                # Create DuckDB connection and drop table
                import duckdb
                con = duckdb.connect(":memory:")

                try:
                    # Load Iceberg extension
                    DuckDBIcebergExtension.load_extension(con)

                    # Setup REST catalog
                    catalog_name = "seeknal_catalog"
                    DuckDBIcebergExtension.create_rest_catalog(
                        con=con,
                        catalog_name=catalog_name,
                        uri=catalog_uri,
                        warehouse_path=warehouse_path,
                        bearer_token=bearer_token,
                    )

                    # Create table reference and drop
                    table_ref = f"{catalog_name}.{iceberg_config.namespace}.{iceberg_config.table}"
                    con.execute(f"DROP TABLE IF EXISTS {table_ref}")
                    logger.info(f"Dropped Iceberg table: {table_ref}")
                    return True

                except Exception as e:
                    logger.error(f"Failed to drop Iceberg table: {e}")
                    return False
                finally:
                    con.close()

            case _:
                logger.warning(f"Unknown offline store kind: {self.kind}")
                return False

    def _write_to_iceberg(
        self,
        result: Optional[DataFrame] = None,
        kwds: dict = None,
    ) -> dict:
        """Write DataFrame to Iceberg table using DuckDB extension.

        This method reuses the existing Iceberg infrastructure from YAML pipelines:
        - ProfileLoader for catalog configuration
        - DuckDBIcebergExtension for catalog setup
        - write_to_iceberg for atomic writes

        Args:
            result: The DataFrame (Spark or Pandas) containing feature data.
            kwds: Keyword arguments including:
                - project (str): Project name (unused, for API compatibility)
                - entity (str): Entity name (unused, for API compatibility)

        Returns:
            dict: Result dictionary with keys:
                - path: Full table reference (catalog.namespace.table)
                - num_rows: Number of rows written
                - storage_type: Always "iceberg"
                - snapshot_id: Iceberg snapshot ID
                - table: Table name within namespace
                - namespace: Iceberg namespace

        Raises:
            ValueError: If Iceberg configuration is invalid or write fails.
            MaterializationOperationError: If Iceberg extension or catalog fails.
        """
        from seeknal.workflow.materialization.operations import (
            DuckDBIcebergExtension,
            write_to_iceberg,
        )
        from seeknal.workflow.materialization.profile_loader import ProfileLoader
        from seeknal.workflow.materialization.config import (
            MaterializationConfig,
            ConfigurationError,
        )

        if kwds is None:
            kwds = {}

        # Get Iceberg configuration from value
        iceberg_config = self.value
        if iceberg_config is None:
            raise ValueError("IcebergStoreOutput configuration required for ICEBERG offline store")

        # Handle dict case (from database deserialization)
        if isinstance(iceberg_config, dict):
            # Create a temporary IcebergStoreOutput from dict
            from dataclasses import dataclass

            @dataclass
            class _TempIcebergConfig:
                catalog: str = "lakekeeper"
                warehouse: Optional[str] = None
                namespace: str = "default"
                table: str = ""
                mode: str = "append"

            iceberg_config = _TempIcebergConfig(**iceberg_config)

        # Load profile for catalog configuration
        profile_loader = ProfileLoader()
        try:
            profile_config = profile_loader.load_profile()
        except (ConfigurationError, Exception) as e:
            logger.warning(f"Could not load materialization profile: {e}. Using defaults.")
            profile_config = MaterializationConfig()

        # Merge profile config with IcebergStoreOutput
        # Profile provides defaults, IcebergStoreOutput can override
        catalog_uri = profile_config.catalog.uri if profile_config.catalog.uri else ""
        warehouse_path = (
            iceberg_config.warehouse or
            profile_config.catalog.warehouse
        )
        bearer_token = profile_config.catalog.bearer_token

        # Validate required configuration
        if not catalog_uri:
            raise ValueError(
                "Catalog URI not configured. Set it in profiles.yml under "
                "materialization.catalog.uri or via LAKEKEEPER_URI environment variable."
            )

        if not warehouse_path:
            raise ValueError(
                "Warehouse path not configured. Set it in profiles.yml under "
                "materialization.catalog.warehouse or in IcebergStoreOutput.warehouse."
            )

        # Validate table name
        validate_table_name(iceberg_config.table)
        validate_table_name(iceberg_config.namespace)

        # Convert Spark DataFrame to Pandas if needed
        if result is not None and hasattr(result, 'toPandas'):
            result = result.toPandas()

        # Create DuckDB connection with Iceberg
        import duckdb
        con = duckdb.connect(":memory:")

        try:
            # Load Iceberg extension
            DuckDBIcebergExtension.load_extension(con)

            # Setup REST catalog
            catalog_name = "seeknal_catalog"
            DuckDBIcebergExtension.create_rest_catalog(
                con=con,
                catalog_name=catalog_name,
                uri=catalog_uri,
                warehouse_path=warehouse_path,
                bearer_token=bearer_token,
            )

            # Create table reference
            namespace = iceberg_config.namespace or "default"
            table_name = iceberg_config.table
            table_ref = f"{catalog_name}.{namespace}.{table_name}"

            # Register DataFrame as view
            if result is not None:
                con.register('features_df', result)
                view_name = 'features_df'
            else:
                raise ValueError("No data provided for writing to Iceberg")

            # Write to Iceberg with atomic commit
            write_mode = iceberg_config.mode or "append"
            write_result = write_to_iceberg(
                con=con,
                catalog_name=catalog_name,
                table_name=f"{namespace}.{table_name}",
                view_name=view_name,
                mode=write_mode,
            )

            # Return result dictionary
            return {
                "path": table_ref,
                "num_rows": write_result.row_count,
                "storage_type": "iceberg",
                "snapshot_id": write_result.snapshot_id,
                "table": table_name,
                "namespace": namespace,
                "catalog": iceberg_config.catalog,
            }

        except Exception as e:
            logger.error(f"Failed to write to Iceberg: {e}")
            raise ValueError(f"Iceberg write failed: {e}") from e
        finally:
            con.close()

    def __call__(
        self,
        result: Optional[DataFrame] = None,
        spark: Optional[SparkSession] = None,
        write=True,
        *args: Any,
        **kwds: Any,
    ) -> Any:
        """Write features to or read features from the offline store.

        This method handles both writing and reading operations for the offline
        store. When writing, it supports overwrite, append, and merge modes with
        optional TTL-based data cleanup.

        Args:
            result: The DataFrame containing feature data to write.
            spark: SparkSession instance. If None, a new session is created.
            write: If True, write data to the store. If False, read from store.
            *args: Additional positional arguments.
            **kwds: Keyword arguments including:
                - project (str): Project name for table naming.
                - entity (str): Entity name for table naming.
                - name (str): Feature name (required for write).
                - start_date (str/datetime): Start date for data range.
                - end_date (str/datetime): End date for data range.
                - mode (str): Write mode ('overwrite', 'append', 'merge').
                - ttl (int): Time-to-live in days for data retention.
                - version (str): Version metadata.
                - latest_watermark (datetime): Latest watermark for TTL cleanup.

        Returns:
            DataFrame: When reading (write=False), returns the filtered DataFrame.
            None: When writing (write=True).

        Raises:
            ValueError: If start_date is missing for write operations or if
                an unsupported mode is specified.
        """
        match self.kind:
            case OfflineStoreEnum.HIVE_TABLE:
                if spark is None:
                    spark = SparkSession.builder.getOrCreate()
                if self.value is None:
                    # use default
                    database = "seeknal"
                else:
                    database = self.value.database

                # Validate database name and table components before use in SQL
                validate_database_name(database)
                project = kwds["project"]
                entity = kwds["entity"]
                validate_table_name(project)
                validate_table_name(entity)
                table_name = "fg_{}__{}".format(project, entity)

                start_date = kwds.get("start_date", None)
                end_date = kwds.get("end_date", None)
                if write:
                    name = kwds.get("name")
                    # Validate name used in WHERE clause / replaceWhere condition
                    validate_sql_value(name, value_type="feature name")
                    mode = kwds.get("mode", "overwrite")
                    ttl = kwds.get("ttl", None)
                    if start_date is None:
                        raise ValueError("start_date is required")
                    if end_date != "none":
                        replace_where = "event_time >= '{}' and event_time <= '{}' and name == '{}'".format(
                            start_date, end_date, name
                        )
                    else:
                        replace_where = "event_time >= '{}' and name == '{}'".format(
                            start_date, name
                        )
                    version = kwds.get("version", None)

                    # create database if not exists
                    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
                    if mode == "overwrite":
                        (
                            result.write.format("delta")
                            .mode("overwrite")
                            .option("replaceWhere", replace_where)
                            .option(
                                "userMetadata",
                                "start_date={},end_date={},version={}".format(
                                    start_date, end_date, version
                                ),
                            )
                            .saveAsTable("{}.{}".format(database, table_name))
                        )
                    elif mode == "append":
                        (
                            result.write.format("delta")
                            .mode("append")
                            .option(
                                "userMetadata",
                                "start_date={},end_date={},version={}".format(
                                    start_date, end_date, version
                                ),
                            )
                            .saveAsTable("{}.{}".format(database, table_name))
                        )
                    elif mode == "merge":
                        if not spark.catalog.tableExists(
                            "{}.{}".format(database, table_name)
                        ):
                            logger.info(
                                "Table {} does not exist, creating table".format(
                                    table_name
                                ))
                            (
                                result.write.format("delta")
                                .option(
                                    "userMetadata",
                                    "start_date={},end_date={},version={}".format(
                                        start_date, end_date, version
                                    ),
                                )
                                .saveAsTable("{}.{}".format(database, table_name))
                            )
                        else:
                            delta_table = DeltaTable.forName(
                                spark, "{}.{}".format(database, table_name)
                            )
                            delta_table.alias("source").merge(
                                result.alias("target"),
                                "source.__pk__ = target.__pk__",
                            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                    else:
                        raise ValueError("mode {} is not supported".format(mode))

                    if ttl is not None:
                        delta_table = DeltaTable.forName(
                            spark, "{}.{}".format(database, table_name)
                        )
                        latest_watermark = kwds.get("latest_watermark")
                        _timestamp = (
                            pendulum.instance(latest_watermark)
                            .add(days=-ttl)
                            .format("YYYY-MM-DD HH:MM:SS")
                        )
                        delta_table.delete("event_time < '{}'".format(_timestamp))
                else:
                    df = spark.read.format("delta").table(
                        "{}.{}".format(database, table_name)
                    )
                    if start_date is not None:
                        if isinstance(start_date, datetime):
                            start_date = pendulum.instance(start_date).format(
                                "YYYY-MM-DD HH:MM:SS"
                            )
                    if end_date is not None:
                        if isinstance(end_date, datetime):
                            end_date = pendulum.instance(end_date).format(
                                "YYYY-MM-DD HH:MM:SS"
                            )

                    if start_date is not None and end_date is not None:
                        df = df.filter(
                            (df.event_time >= start_date) & (df.event_time <= end_date)
                        )
                    elif start_date is not None:
                        df = df.filter(df.event_time >= start_date)
                    elif end_date is not None:
                        df = df.filter(df.event_time <= end_date)
                    return df
            case OfflineStoreEnum.FILE:
                if spark is None:
                    spark = SparkSession.builder.getOrCreate()

                # Validate project and entity parameters used in table name construction
                project = kwds["project"]
                entity = kwds["entity"]
                validate_table_name(project)
                validate_table_name(entity)
                table_name = "fg_{}__{}".format(project, entity)

                if self.value is None:
                    # use default
                    base_path = CONFIG_BASE_URL
                    path = os.path.join(base_path, "data", table_name)
                else:
                    if isinstance(self.value, FeatureStoreFileOutput):
                        base_path = self.value.path
                    else:
                        base_path = self.value["path"]
                    path = os.path.join(base_path, table_name)
                start_date = kwds.get("start_date", None)
                end_date = kwds.get("end_date", None)
                if write:
                    name = kwds.get("name")
                    # Validate name used in WHERE clause / replaceWhere condition
                    validate_sql_value(name, value_type="feature name")
                    mode = kwds.get("mode", "overwrite")
                    ttl = kwds.get("ttl", None)
                    if start_date is None:
                        raise ValueError("start_date is required")
                    if end_date != "none":
                        replace_where = "event_time >= '{}' and event_time <= '{}' and name == '{}'".format(
                            start_date, end_date, name
                        )
                    else:
                        replace_where = "event_time >= '{}' and name == '{}'".format(
                            start_date, name
                        )
                    version = kwds.get("version", None)

                    if mode == "overwrite":
                        (
                            result.write.format("delta")
                            .mode("overwrite")
                            .option("replaceWhere", replace_where)
                            .option(
                                "userMetadata",
                                "start_date={},end_date={},version={}".format(
                                    start_date, end_date, version
                                ),
                            )
                            .save(path)
                        )
                    elif mode == "append":
                        (
                            result.write.format("delta")
                            .mode("append")
                            .option(
                                "userMetadata",
                                "start_date={},end_date={},version={}".format(
                                    start_date, end_date, version
                                ),
                            )
                            .save(path)
                        )
                    elif mode == "merge":
                        if not os.path.exists(path):
                            logger.info("Table is not exists, creating it now")
                            (
                                result.write.format("delta")
                                .option(
                                    "userMetadata",
                                    "start_date={},end_date={},version={}".format(
                                        start_date, end_date, version
                                    ),
                                )
                                .save(path)
                            )
                        else:
                            delta_table = DeltaTable.forPath(spark, path)
                            delta_table.alias("source").merge(
                                result.alias("target"),
                                "source.__pk__ = target.__pk__",
                            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                    else:
                        raise ValueError("mode {} is not supported".format(mode))

                    if ttl is not None:
                        delta_table = DeltaTable.forPath(spark, path)
                        latest_watermark = kwds.get("latest_watermark")
                        _timestamp = (
                            pendulum.instance(latest_watermark)
                            .add(days=-ttl)
                            .format("YYYY-MM-DD HH:MM:SS")
                        )
                        delta_table.delete("event_time < '{}'".format(_timestamp))
                else:
                    df = spark.read.format("delta").load(path)
                    if start_date is not None:
                        if isinstance(start_date, datetime):
                            start_date = pendulum.instance(start_date).format(
                                "YYYY-MM-DD HH:MM:SS"
                            )
                    if end_date is not None:
                        if isinstance(end_date, datetime):
                            end_date = pendulum.instance(end_date).format(
                                "YYYY-MM-DD HH:MM:SS"
                            )

                    if start_date is not None and end_date is not None:
                        df = df.filter(
                            (df.event_time >= start_date) & (df.event_time <= end_date)
                        )
                    elif start_date is not None:
                        df = df.filter(df.event_time >= start_date)
                    elif end_date is not None:
                        df = df.filter(df.event_time <= end_date)
                    return df
            case OfflineStoreEnum.ICEBERG:
                return self._write_to_iceberg(result, kwds)

@dataclass
class OnlineStore:
    """Configuration for online feature store.

    Attributes:
        value: The storage configuration (file path or hive table).
            A security warning will be logged if a file path is in an insecure
            location (e.g., /tmp).
        kind: Type of online store (FILE or HIVE_TABLE).
        name: Optional name for the store.
    """
    value: Optional[
        Union[str, FeatureStoreFileOutput, FeatureStoreHiveTableOutput]
    ] = None
    kind: OnlineStoreEnum = OnlineStoreEnum.FILE
    name: Optional[str] = None

    def __post_init__(self):
        # Validate path security for file-based storage
        if self.kind == OnlineStoreEnum.FILE:
            self._validate_path_security()

        if self.value is not None:
            if hasattr(self.value, 'to_dict'):
                self.value = self.value.to_dict()
            elif not isinstance(self.value, (str, dict)):
                self.value = str(self.value)
        self.kind = self.kind.value

    def _validate_path_security(self):
        """Validate and warn if the storage path is insecure."""
        if self.value is None:
            return

        # If value is FeatureStoreFileOutput, it validates in its own __post_init__
        if isinstance(self.value, FeatureStoreFileOutput):
            return

        # Extract path from value
        path = None
        if isinstance(self.value, str):
            # Try to parse as JSON
            try:
                value_dict = json.loads(self.value)
                path = value_dict.get("path")
            except (json.JSONDecodeError, TypeError):
                # Not valid JSON, might be a direct path string
                path = self.value
        elif isinstance(self.value, dict):
            path = self.value.get("path")

        if path:
            warn_if_insecure_path(path, context="online store", logger=logger)

    def __call__(
        self,
        result: Optional[Union[DataFrame, pd.DataFrame]] = None,
        spark: Optional[SparkSession] = None,
        write=True,
        *args: Any,
        **kwargs: Any,
    ):
        """Write features to or prepare a reader from the online store.

        This method handles writing feature data to the online store and
        returns a DuckDB-based SQL reader for querying the stored data.

        Args:
            result: The DataFrame (Spark or Pandas) containing feature data.
            spark: SparkSession instance. If None, a new session is created.
            write: If True, write data before returning reader. If False,
                only return the reader for existing data.
            *args: Additional positional arguments.
            **kwargs: Keyword arguments including:
                - project (str): Project name for file naming.
                - name (str): Feature name for file naming.

        Returns:
            DuckDBTask: A DuckDB reader configured to query the stored
                Parquet files.

        Note:
            The data is stored in Parquet format under the configured path
            with the naming convention: fs_{project}__{name}
        """
        match self.kind:
            case OnlineStoreEnum.FILE:
                if spark is None:
                    spark = SparkSession.builder.getOrCreate()

                name = kwargs.get("name")
                file_name_complete = "fs_{}__{}".format(kwargs["project"], name)

                if self.value is None:
                    base_path = CONFIG_BASE_URL
                    path = os.path.join(base_path, "data", file_name_complete)
                else:
                    if type(self.value) == str:
                        self.value = json.loads(self.value)
                    base_path = self.value["path"]
                    path = os.path.join(base_path, file_name_complete)
                if write:
                    if isinstance(result, DataFrame):
                        result.write.mode("overwrite").parquet(path)
                    elif isinstance(result, pd.DataFrame):
                        os.mkdir(path)
                        result.to_parquet(os.path.join(path, "file.parquet"))

                sql_reader = DuckDBTask().add_input(
                    path=os.path.join(path, "*.parquet")
                )
                return sql_reader

    def delete(self, *args, **kwargs):
        """Delete feature data from the online store.

        Removes the directory containing the feature data for the specified
        project and feature name.

        Args:
            *args: Additional positional arguments (unused).
            **kwargs: Keyword arguments including:
                - project (str): Project name for file naming.
                - name (str): Feature name for file naming.

        Returns:
            bool: True if the deletion was successful or if the path
                did not exist.
        """
        match self.kind:
            case OnlineStoreEnum.FILE:
                name = kwargs.get("name")
                file_name_complete = "fs_{}__{}".format(kwargs["project"], name)

                if self.value == "null" or self.value is None:
                    base_path = CONFIG_BASE_URL
                    path = os.path.join(base_path, "data", file_name_complete)
                else:
                    base_path = self.value["path"]
                    path = os.path.join(base_path, file_name_complete)
                if os.path.exists(path):
                    shutil.rmtree(path)
                return True


class FillNull(BaseModel):
    """Configuration for filling null values in columns.

    Attributes:
        value: Value to use for filling nulls.
        dataType: Data type for the value (e.g., 'double', 'string').
        columns: Optional list of columns to fill. If None, applies to all columns.
    """
    value: str
    dataType: str
    columns: Optional[List[str]] = None

    def to_dict(self):
        """Convert the configuration to a dictionary representation.

        Returns:
            dict: Dictionary containing non-None values from the model.
        """
        return {k: v for k, v in self.model_dump().items() if v is not None}


class OfflineMaterialization(BaseModel):
    """Configuration for offline materialization.

    Attributes:
        store: The offline store configuration.
        mode: Write mode ('overwrite', 'append', 'merge').
        ttl: Time-to-live in days for data retention.
    """
    store: Optional[OfflineStore] = None
    mode: str = "overwrite"
    ttl: Optional[int] = None


class OnlineMaterialization(BaseModel):
    """Configuration for online materialization.

    Attributes:
        store: The online store configuration.
        ttl: Time-to-live in minutes for online store data.
    """
    store: Optional[OnlineStore] = None
    ttl: Optional[int] = 1440

    model_config = {
        "use_enum_values": True
    }


class Feature(BaseModel):
    """Define a Feature.

    Attributes:
        name: Feature name.
        feature_id: Feature ID (assigned by seeknal).
        description: Feature description.
        data_type: Data type for the feature.
        online_data_type: Data type when stored in online-store.
        created_at: Creation timestamp.
        updated_at: Last update timestamp.
    """
    name: str
    feature_id: Optional[str] = None
    description: Optional[str] = None
    data_type: Optional[str] = None
    online_data_type: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self):
        """Convert the feature definition to a dictionary representation.

        Creates a dictionary suitable for API requests with metadata and
        data type information.

        Returns:
            dict: Dictionary with structure:
                - metadata: dict containing 'name' and optionally 'description'
                - datatype: The feature's data type
                - onlineDatatype: The feature's online store data type
        """
        _dict = {
            "metadata": {"name": self.name},
            "datatype": self.data_type,
            "onlineDatatype": self.online_data_type,
        }
        if self.description is not None:
            _dict["metadata"]["description"] = self.description
        return _dict


@dataclass
class FeatureStore(ABC):
    """Abstract base class for feature stores.

    Attributes:
        name: Feature store name.
        entity: Associated entity.
        id: Feature store ID.
    """
    name: str
    entity: Optional[Entity] = None
    id: Optional[str] = None

    @abstractmethod
    def get_or_create(self):
        """Retrieve an existing feature store or create a new one.

        This abstract method must be implemented by subclasses to handle
        the creation or retrieval of feature store instances.

        Returns:
            FeatureStore: The feature store instance.
        """
        pass
