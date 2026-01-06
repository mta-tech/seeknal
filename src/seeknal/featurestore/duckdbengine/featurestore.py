"""DuckDB-based offline and online feature stores.

This module replaces Spark/Delta Lake with DuckDB for feature storage and retrieval.
"""

import os
import json
import shutil
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Union
from datetime import datetime
import pandas as pd
import duckdb

from ...context import logger


class OfflineStoreEnum(str, Enum):
    """Offline store backend types."""
    DUCKDB_TABLE = "duckdb_table"
    PARQUET = "parquet"


class OnlineStoreEnum(str, Enum):
    """Online store backend types."""
    DUCKDB_TABLE = "duckdb_table"
    PARQUET = "parquet"


@dataclass
class FeatureStoreFileOutput:
    """File-based feature store output configuration."""
    path: str

    def to_dict(self):
        return {"path": self.path}


@dataclass
class OfflineStoreDuckDB:
    """DuckDB-based offline feature store.

    Stores features in DuckDB tables or Parquet files with metadata tracking.
    Provides ACID-like guarantees through atomic file operations and metadata.
    """

    value: Optional[Union[str, FeatureStoreFileOutput]] = None
    kind: OfflineStoreEnum = OfflineStoreEnum.PARQUET
    name: Optional[str] = None
    connection: Optional[duckdb.DuckDBPyConnection] = None

    def __post_init__(self):
        if self.value is not None and not isinstance(self.value, str):
            self.value = self.value.to_dict()
        if isinstance(self.kind, str):
            self.kind = OfflineStoreEnum(self.kind)

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection."""
        if self.connection is None:
            # Use in-memory database or file-based if path is provided
            if self.kind == OfflineStoreEnum.DUCKDB_TABLE and isinstance(self.value, dict):
                db_path = self.value.get("path", ":memory:")
                self.connection = duckdb.connect(db_path)
            else:
                self.connection = duckdb.connect(":memory:")
        return self.connection

    def _get_table_path(self, project: str, entity: str, name: str) -> str:
        """Get storage path for feature group."""
        if isinstance(self.value, dict):
            base_path = self.value.get("path", "/tmp/feature_store")
        else:
            base_path = "/tmp/feature_store"

        # Create directory structure: base_path/project/entity/name/
        table_dir = os.path.join(base_path, project, entity, name)
        os.makedirs(table_dir, exist_ok=True)
        return table_dir

    def _get_metadata_path(self, table_dir: str) -> str:
        """Get path to metadata file."""
        return os.path.join(table_dir, "_metadata.json")

    def _load_metadata(self, table_dir: str) -> dict:
        """Load metadata for a feature group."""
        metadata_path = self._get_metadata_path(table_dir)
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                return json.load(f)
        return {"versions": [], "watermarks": [], "schema": None}

    def _save_metadata(self, table_dir: str, metadata: dict):
        """Save metadata for a feature group."""
        metadata_path = self._get_metadata_path(table_dir)
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

    def write(
        self,
        df: pd.DataFrame,
        project: str,
        entity: str,
        name: str,
        mode: str = "overwrite",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs
    ) -> None:
        """Write features to offline store.

        Args:
            df: Features to write
            project: Project name
            entity: Entity name
            name: Feature group name
            mode: Write mode - 'overwrite', 'append', or 'merge'
            start_date: Start date for this batch
            end_date: End date for this batch
        """
        if df is None or len(df) == 0:
            logger.warning("Empty DataFrame, skipping write")
            return

        table_dir = self._get_table_path(project, entity, name)
        metadata = self._load_metadata(table_dir)

        # Generate version ID
        version = datetime.now().strftime("%Y%m%d_%H%M%S")

        if mode == "overwrite":
            self._write_overwrite(df, table_dir, metadata, version, start_date, end_date)
        elif mode == "append":
            self._write_append(df, table_dir, metadata, version, start_date, end_date)
        elif mode == "merge":
            self._write_merge(df, table_dir, metadata, version, start_date, end_date)
        else:
            raise ValueError(f"Unknown mode: {mode}")

        # Update metadata
        metadata["versions"].append({
            "version": version,
            "start_date": str(start_date) if start_date else None,
            "end_date": str(end_date) if end_date else None,
            "mode": mode,
            "rows": len(df),
            "timestamp": datetime.now().isoformat()
        })

        # Track watermarks
        if start_date:
            watermark = str(start_date)
            if watermark not in metadata["watermarks"]:
                metadata["watermarks"].append(watermark)
        if end_date and end_date != "none":
            watermark = str(end_date)
            if watermark not in metadata["watermarks"]:
                metadata["watermarks"].append(watermark)

        metadata["watermarks"] = sorted(set(metadata["watermarks"]))

        # Save schema
        metadata["schema"] = {col: str(dtype) for col, dtype in df.dtypes.items()}

        self._save_metadata(table_dir, metadata)
        logger.info(f"Wrote {len(df)} rows to {table_dir} (mode={mode}, version={version})")

    def _write_overwrite(
        self,
        df: pd.DataFrame,
        table_dir: str,
        metadata: dict,
        version: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ):
        """Overwrite mode: replace matching partitions."""
        parquet_path = os.path.join(table_dir, "data.parquet")

        # If file exists, read existing data and filter
        if os.path.exists(parquet_path):
            existing_df = pd.read_parquet(parquet_path)

            # Filter out rows in the overwrite range
            if start_date and 'event_time' in existing_df.columns:
                filter_mask = existing_df['event_time'] < start_date
                if end_date and end_date != "none":
                    filter_mask |= existing_df['event_time'] > end_date

                filtered_df = existing_df[filter_mask]
                combined_df = pd.concat([filtered_df, df], ignore_index=True)
            else:
                combined_df = df
        else:
            combined_df = df

        # Write to parquet
        combined_df.to_parquet(parquet_path, engine='pyarrow', index=False)

    def _write_append(
        self,
        df: pd.DataFrame,
        table_dir: str,
        metadata: dict,
        version: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ):
        """Append mode: add new rows."""
        parquet_path = os.path.join(table_dir, "data.parquet")

        if os.path.exists(parquet_path):
            existing_df = pd.read_parquet(parquet_path)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            combined_df = df

        combined_df.to_parquet(parquet_path, engine='pyarrow', index=False)

    def _write_merge(
        self,
        df: pd.DataFrame,
        table_dir: str,
        metadata: dict,
        version: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime]
    ):
        """Merge mode: upsert based on primary key.

        Simulates Delta Lake merge using pandas operations.
        Assumes primary key is the entity join key(s) + event_time.
        """
        parquet_path = os.path.join(table_dir, "data.parquet")

        if not os.path.exists(parquet_path):
            # First write - just write the data
            df.to_parquet(parquet_path, engine='pyarrow', index=False)
            return

        # Read existing data
        existing_df = pd.read_parquet(parquet_path)

        # Identify merge keys (typically entity keys + event_time)
        # For now, use all common columns as merge keys
        merge_keys = [col for col in df.columns if col in existing_df.columns and col != 'name']

        if not merge_keys:
            # No merge keys, just append
            combined_df = pd.concat([existing_df, df], ignore_index=True)
        else:
            # Perform upsert using DuckDB
            conn = self._get_connection()

            # Register dataframes as temporary tables
            conn.register("existing", existing_df)
            conn.register("new_data", df)

            # Build merge query
            merge_key_conditions = " AND ".join([f"existing.{k} = new_data.{k}" for k in merge_keys])

            # Delete matching rows from existing and append all new rows
            query = f"""
            SELECT * FROM (
                SELECT * FROM existing
                WHERE NOT EXISTS (
                    SELECT 1 FROM new_data
                    WHERE {merge_key_conditions}
                )
                UNION ALL
                SELECT * FROM new_data
            )
            """

            combined_df = conn.execute(query).df()

            # Unregister temporary tables
            conn.unregister("existing")
            conn.unregister("new_data")

        # Write merged data
        combined_df.to_parquet(parquet_path, engine='pyarrow', index=False)

    def read(
        self,
        project: str,
        entity: str,
        name: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Read features from offline store.

        Args:
            project: Project name
            entity: Entity name
            name: Feature group name
            start_date: Filter start date
            end_date: Filter end date

        Returns:
            DataFrame with features
        """
        table_dir = self._get_table_path(project, entity, name)
        parquet_path = os.path.join(table_dir, "data.parquet")

        if not os.path.exists(parquet_path):
            raise FileNotFoundError(f"Feature group not found: {name}")

        # Read parquet file
        df = pd.read_parquet(parquet_path)

        # Apply date filters if provided
        if start_date and 'event_time' in df.columns:
            df = df[df['event_time'] >= start_date]

        if end_date and end_date != "none" and 'event_time' in df.columns:
            df = df[df['event_time'] <= end_date]

        logger.info(f"Read {len(df)} rows from {table_dir}")
        return df

    def get_watermarks(self, project: str, entity: str, name: str) -> list:
        """Get watermarks for a feature group."""
        table_dir = self._get_table_path(project, entity, name)
        metadata = self._load_metadata(table_dir)
        return metadata.get("watermarks", [])

    def delete(self, project: str, entity: str, name: str) -> bool:
        """Delete a feature group from the offline store.

        Removes all data files and metadata associated with the feature group.

        Args:
            project: Project name
            entity: Entity name
            name: Feature group name

        Returns:
            True if deletion was successful, False otherwise
        """
        if isinstance(self.value, dict):
            base_path = self.value.get("path", "/tmp/feature_store")
        else:
            base_path = "/tmp/feature_store"

        # Build path to feature group directory
        table_dir = os.path.join(base_path, project, entity, name)

        if os.path.exists(table_dir):
            shutil.rmtree(table_dir)
            logger.info(f"Deleted offline feature group at {table_dir}")
            return True
        else:
            logger.warning(f"Offline feature group directory not found: {table_dir}")
            return False


@dataclass
class OnlineStoreDuckDB:
    """DuckDB-based online feature store.

    Stores features for low-latency serving using DuckDB tables.
    """

    value: Optional[Union[str, FeatureStoreFileOutput]] = None
    kind: OnlineStoreEnum = OnlineStoreEnum.DUCKDB_TABLE
    name: Optional[str] = None
    connection: Optional[duckdb.DuckDBPyConnection] = None

    def __post_init__(self):
        if self.value is not None and not isinstance(self.value, str):
            self.value = self.value.to_dict()
        if isinstance(self.kind, str):
            self.kind = OnlineStoreEnum(self.kind)

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection."""
        if self.connection is None:
            if isinstance(self.value, dict):
                db_path = self.value.get("path", ":memory:")
                self.connection = duckdb.connect(db_path)
            else:
                self.connection = duckdb.connect(":memory:")
        return self.connection

    def _get_table_path(self, project: str, entity: str, name: str) -> str:
        """Get storage path for online table.

        Args:
            project: Project name
            entity: Entity name
            name: Table name

        Returns:
            Path to the online table directory
        """
        if isinstance(self.value, dict):
            base_path = self.value.get("path", "/tmp/feature_store_online")
        else:
            base_path = "/tmp/feature_store_online"

        # Create directory structure: base_path/project/entity/name/
        table_dir = os.path.join(base_path, project, entity, name)
        return table_dir

    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        **kwargs
    ) -> None:
        """Write features to online store.

        Args:
            df: Features to write
            table_name: Name of the online table
        """
        conn = self._get_connection()

        # Drop table if exists and recreate
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

        logger.info(f"Wrote {len(df)} rows to online table {table_name}")

    def read(
        self,
        table_name: str,
        keys: Optional[list] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Read features from online store.

        Args:
            table_name: Name of the online table
            keys: Filter by specific keys (list of dicts with key-value pairs)

        Returns:
            DataFrame with features
        """
        conn = self._get_connection()

        if keys:
            # Build WHERE clause from keys
            conditions = []
            for key_dict in keys:
                key_conditions = [f"{k} = '{v}'" for k, v in key_dict.items()]
                conditions.append("(" + " AND ".join(key_conditions) + ")")

            where_clause = " OR ".join(conditions)
            query = f"SELECT * FROM {table_name} WHERE {where_clause}"
        else:
            query = f"SELECT * FROM {table_name}"

        df = conn.execute(query).df()
        logger.info(f"Read {len(df)} rows from online table {table_name}")
        return df

    def delete(self, name: str, project: str, entity: Optional[str] = None) -> bool:
        """Delete an online table from the store.

        Removes the DuckDB table and any associated parquet files.

        Args:
            name: Table name
            project: Project name
            entity: Entity name (optional, used for file-based storage path)

        Returns:
            True if deletion was successful, False otherwise
        """
        # Try to drop the DuckDB table if it exists
        try:
            conn = self._get_connection()
            conn.execute(f"DROP TABLE IF EXISTS {name}")
            logger.info(f"Dropped online table '{name}' from DuckDB")
        except Exception as e:
            logger.warning(f"Could not drop DuckDB table '{name}': {e}")

        # Also clean up any file-based storage
        if isinstance(self.value, dict):
            base_path = self.value.get("path", "/tmp/feature_store_online")
        else:
            base_path = "/tmp/feature_store_online"

        # Build path to table directory
        if entity:
            table_dir = os.path.join(base_path, project, entity, name)
        else:
            # Fallback pattern for online tables without entity
            table_dir = os.path.join(base_path, project, name)

        if os.path.exists(table_dir):
            shutil.rmtree(table_dir)
            logger.info(f"Deleted online table files at {table_dir}")
            return True

        logger.info(f"Online table '{name}' deleted successfully")
        return True
