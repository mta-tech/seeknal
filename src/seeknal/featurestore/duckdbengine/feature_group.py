"""DuckDB-based Feature Group implementation.

Provides feature group management, historical feature retrieval, and online serving
using DuckDB instead of Spark.
"""

import json
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional, Union, Dict, Any
import pandas as pd
import duckdb

from ...context import context, logger
from ...entity import Entity
from ...request import OnlineTableRequest
from .featurestore import (
    OfflineStoreDuckDB,
    OnlineStoreDuckDB,
    FeatureStoreFileOutput,
    OfflineStoreEnum,
    OnlineStoreEnum,
)


class GetLatestTimeStrategy(str, Enum):
    """Strategy for getting latest features."""
    REQUIRE_ALL = "require_all"
    REQUIRE_ANY = "require_any"


@dataclass
class FillNull:
    """Configuration for filling null values."""
    value: str
    dataType: str


@dataclass
class FeatureLookup:
    """Defines which features to retrieve from a feature group."""
    source: 'FeatureGroupDuckDB'
    features: Optional[List[str]] = None


@dataclass
class Materialization:
    """Materialization configuration for a feature group."""
    event_time_col: Optional[str] = None
    offline: bool = True
    online: bool = False
    offline_store: Optional[OfflineStoreDuckDB] = None
    online_store: Optional[OnlineStoreDuckDB] = None

    def __post_init__(self):
        if self.offline_store is None:
            self.offline_store = OfflineStoreDuckDB()
        if self.online_store is None:
            self.online_store = OnlineStoreDuckDB()


@dataclass
class FeatureGroupDuckDB:
    """DuckDB-based Feature Group.

    A feature group is a collection of related features computed from source data.
    """

    name: str
    entity: Entity
    materialization: Materialization = field(default_factory=Materialization)
    dataframe: Optional[pd.DataFrame] = None
    description: Optional[str] = None
    features: Optional[List[str]] = None

    # Metadata
    project: str = "default"
    offline_watermarks: List[str] = field(default_factory=list)
    online_watermarks: List[str] = field(default_factory=list)
    version: Optional[int] = None

    def set_dataframe(self, dataframe: pd.DataFrame) -> 'FeatureGroupDuckDB':
        """Set the source dataframe for this feature group."""
        self.dataframe = dataframe
        return self

    def set_features(self, features: Optional[List[str]] = None) -> 'FeatureGroupDuckDB':
        """Set which features to include.

        If features is None, all columns except entity keys and event_time are used.
        """
        if features is None and self.dataframe is not None:
            # Auto-detect features
            reserved_cols = self.entity.join_keys.copy()
            if self.materialization.event_time_col:
                reserved_cols.append(self.materialization.event_time_col)

            self.features = [col for col in self.dataframe.columns if col not in reserved_cols]
        else:
            self.features = features

        return self

    def write(
        self,
        feature_start_time: Optional[datetime] = None,
        feature_end_time: Optional[datetime] = None,
        mode: str = "overwrite"
    ) -> None:
        """Materialize features to offline/online stores.

        Args:
            feature_start_time: Start time for this batch
            feature_end_time: End time for this batch
            mode: Write mode - 'overwrite', 'append', or 'merge'
        """
        if self.dataframe is None:
            raise ValueError("Dataframe not set. Use set_dataframe() first.")

        # Prepare DataFrame for write
        df = self.dataframe.copy()

        # Add 'name' column for tracking feature group
        df['name'] = self.name

        # Ensure event_time column exists
        if self.materialization.event_time_col and self.materialization.event_time_col in df.columns:
            # Rename to standard 'event_time' column
            if self.materialization.event_time_col != 'event_time':
                df = df.rename(columns={self.materialization.event_time_col: 'event_time'})

        # Write to offline store
        if self.materialization.offline:
            self.materialization.offline_store.write(
                df=df,
                project=self.project,
                entity=self.entity.name,
                name=self.name,
                mode=mode,
                start_date=feature_start_time,
                end_date=feature_end_time,
            )

            # Update watermarks
            watermarks = self.materialization.offline_store.get_watermarks(
                project=self.project,
                entity=self.entity.name,
                name=self.name
            )
            self.offline_watermarks = watermarks

        logger.info(f"Wrote feature group '{self.name}' with {len(df)} rows")

    def get_or_create(self) -> 'FeatureGroupDuckDB':
        """Get or create this feature group (idempotent operation)."""
        # For simplicity, just load watermarks if they exist
        try:
            watermarks = self.materialization.offline_store.get_watermarks(
                project=self.project,
                entity=self.entity.name,
                name=self.name
            )
            self.offline_watermarks = watermarks
        except FileNotFoundError:
            # Feature group doesn't exist yet
            pass

        return self

    def delete(self) -> bool:
        """Delete this feature group.

        Removes all data files from the offline store.

        Returns:
            True if deletion was successful
        """
        # Delete from offline store
        result = self.materialization.offline_store.delete(
            project=self.project,
            entity=self.entity.name,
            name=self.name
        )
        logger.info(f"Deleted feature group '{self.name}'")
        return result


@dataclass
class HistoricalFeaturesDuckDB:
    """Point-in-time historical feature retrieval using DuckDB.

    Performs point-in-time correct joins to get features as they existed at
    specific points in time.
    """

    lookups: List[FeatureLookup]
    fill_nulls: Optional[List[FillNull]] = None
    spine: Optional[pd.DataFrame] = None
    date_col: Optional[str] = None
    keep_cols: Optional[List[str]] = None
    latest_strategy: Optional[GetLatestTimeStrategy] = None

    def to_dataframe(
        self,
        feature_start_time: Optional[datetime] = None,
        feature_end_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Retrieve historical features as a DataFrame.

        Args:
            feature_start_time: Start time for features
            feature_end_time: End time for features

        Returns:
            DataFrame with features
        """
        if not self.lookups:
            raise ValueError("No feature lookups specified")

        # Read all feature groups
        dfs = []
        for lookup in self.lookups:
            fg = lookup.source
            df = fg.materialization.offline_store.read(
                project=fg.project,
                entity=fg.entity.name,
                name=fg.name,
                start_date=feature_start_time,
                end_date=feature_end_time
            )
            dfs.append(df)

        # Merge all feature groups
        if len(dfs) == 1:
            result = dfs[0]
        else:
            # Join on entity keys + event_time
            result = dfs[0]
            for df in dfs[1:]:
                join_keys = self.lookups[0].source.entity.join_keys + ['event_time']
                result = result.merge(df, on=join_keys, how='outer')

        return result

    def using_spine(
        self,
        spine: pd.DataFrame,
        date_col: str,
        keep_cols: Optional[List[str]] = None
    ) -> 'HistoricalFeaturesDuckDB':
        """Use a spine (entity-date pairs) for point-in-time feature retrieval.

        Args:
            spine: DataFrame with entity keys and application dates
            date_col: Name of the date column in spine
            keep_cols: Columns from spine to keep in the result

        Returns:
            Self for chaining
        """
        self.spine = spine
        self.date_col = date_col
        self.keep_cols = keep_cols or []
        return self

    def using_latest(
        self,
        fetch_strategy: GetLatestTimeStrategy = GetLatestTimeStrategy.REQUIRE_ALL
    ) -> 'HistoricalFeaturesDuckDB':
        """Get the latest available features.

        Args:
            fetch_strategy: Strategy for handling missing features

        Returns:
            Self for chaining
        """
        self.latest_strategy = fetch_strategy
        return self

    def to_dataframe_with_spine(self) -> pd.DataFrame:
        """Retrieve features using point-in-time join with spine.

        For each row in the spine, gets features as they existed at or before
        the application date.
        """
        if self.spine is None:
            raise ValueError("Spine not set. Use using_spine() first.")

        conn = duckdb.connect()

        # Convert spine date column to datetime
        spine = self.spine.copy()
        if self.date_col in spine.columns:
            spine[self.date_col] = pd.to_datetime(spine[self.date_col])

        # Register spine
        conn.register("spine", spine)

        # Process each feature group
        result_dfs = []
        for lookup in self.lookups:
            fg = lookup.source

            # Read feature group
            fg_df = fg.materialization.offline_store.read(
                project=fg.project,
                entity=fg.entity.name,
                name=fg.name
            )

            # Ensure event_time is datetime
            if 'event_time' in fg_df.columns:
                fg_df['event_time'] = pd.to_datetime(fg_df['event_time'])

            # Register feature group
            conn.register("features", fg_df)

            # Build point-in-time join query
            entity_keys = fg.entity.join_keys
            join_conditions = " AND ".join([f"spine.{k} = features.{k}" for k in entity_keys])

            # Point-in-time join: get features where event_time <= app_date
            query = f"""
            SELECT
                spine.*,
                features.* EXCLUDE ({', '.join(entity_keys)}),
                ROW_NUMBER() OVER (
                    PARTITION BY {', '.join([f'spine.{k}' for k in entity_keys])}, spine.{self.date_col}
                    ORDER BY features.event_time DESC
                ) as rn
            FROM spine
            LEFT JOIN features
                ON {join_conditions}
                AND features.event_time <= spine.{self.date_col}
            """

            # Get most recent features
            result_df = conn.execute(f"""
                SELECT * EXCLUDE (rn)
                FROM ({query})
                WHERE rn = 1 OR rn IS NULL
            """).df()

            result_dfs.append(result_df)

        # Merge all feature groups
        if len(result_dfs) == 1:
            result = result_dfs[0]
        else:
            result = result_dfs[0]
            merge_keys = self.lookups[0].source.entity.join_keys + [self.date_col]
            for df in result_dfs[1:]:
                result = result.merge(df, on=merge_keys, how='outer')

        # Keep only requested columns from spine
        if self.keep_cols:
            keep_cols_set = set(self.keep_cols + self.lookups[0].source.entity.join_keys + [self.date_col])
            # Also keep feature columns
            feature_cols = [col for col in result.columns if col not in spine.columns or col in keep_cols_set]
            result = result[[col for col in result.columns if col in keep_cols_set or col in feature_cols]]

        conn.close()
        return result

    def serve(
        self,
        name: Optional[str] = None,
        target: Optional[OnlineStoreDuckDB] = None,
        ttl: Optional[timedelta] = None
    ) -> 'OnlineFeaturesDuckDB':
        """Materialize features to online store for serving.

        Args:
            name: Name for the online table
            target: Target online store
            ttl: Time-to-live for features

        Returns:
            OnlineFeaturesDuckDB instance for serving
        """
        # Get latest features
        df = self.to_dataframe()

        # Generate table name if not provided
        if name is None:
            name = hashlib.md5(str(datetime.now().timestamp()).encode()).hexdigest()

        # Use target store or default
        if target is None:
            target = OnlineStoreDuckDB()

        # Write to online store
        target.write(df=df, table_name=name)

        # Create OnlineFeaturesDuckDB instance
        return OnlineFeaturesDuckDB(
            name=name,
            lookup_key=self.lookups[0].source.entity,
            online_store=target
        )


@dataclass
class OnlineFeaturesDuckDB:
    """Online feature serving using DuckDB.

    Provides low-latency feature lookups for real-time predictions.
    Also known as OnlineTableDuckDB in some contexts.
    """

    name: str
    lookup_key: Entity
    online_store: Optional[OnlineStoreDuckDB] = None
    lookups: Optional[List[FeatureLookup]] = None
    project: str = "default"
    id: Optional[str] = None

    def __post_init__(self):
        if self.online_store is None:
            self.online_store = OnlineStoreDuckDB()

    def get_features(self, keys: List[Union[Entity, Dict[str, Any]]]) -> pd.DataFrame:
        """Get features for specific entity keys.

        Args:
            keys: List of entity instances or key dictionaries

        Returns:
            DataFrame with features for the requested keys
        """
        # Convert Entity instances to dicts
        key_dicts = []
        for key in keys:
            if isinstance(key, Entity):
                # Extract key values from Entity
                key_dict = key.key_values if hasattr(key, 'key_values') else {}
            elif isinstance(key, dict):
                key_dict = key
            else:
                raise ValueError(f"Invalid key type: {type(key)}")

            key_dicts.append(key_dict)

        # Read from online store
        df = self.online_store.read(table_name=self.name, keys=key_dicts)

        return df

    def delete(self) -> bool:
        """Delete this online table.

        Removes all data files from the online store and cleans up metadata
        from the database.

        Returns:
            True if deletion was successful

        Raises:
            Exception: If file deletion fails (metadata cleanup still attempted)
        """
        deletion_success = True

        # Step 1: Delete data files from online store
        try:
            entity_name = self.lookup_key.name if self.lookup_key else None
            self.online_store.delete(
                name=self.name,
                project=self.project,
                entity=entity_name
            )
            logger.info(f"Deleted online table data files for '{self.name}'")
        except Exception as e:
            logger.error(f"Failed to delete online table data files for '{self.name}': {e}")
            deletion_success = False

        # Step 2: Clean up metadata from database
        try:
            if self.id is not None:
                OnlineTableRequest.delete_by_id(self.id)
                logger.info(f"Deleted online table metadata for '{self.name}' (id={self.id})")
        except Exception as e:
            logger.error(f"Failed to delete online table metadata for '{self.name}': {e}")
            deletion_success = False

        if deletion_success:
            logger.info(f"Successfully deleted online table '{self.name}'")
        else:
            logger.warning(f"Partial deletion of online table '{self.name}' - some cleanup may have failed")

        return deletion_success


# Alias for backward compatibility
OnlineTableDuckDB = OnlineFeaturesDuckDB
