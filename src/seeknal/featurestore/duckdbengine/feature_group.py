"""
DuckDB-based Feature Group implementation.

This module provides DuckDB-optimized versions of feature group management
for use in environments where Spark is not available or needed.
"""

import json
import hashlib
import copy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Union

import pandas as pd
import pendulum
import pyarrow as pa

from ...context import context, logger, require_project
from ...entity import Entity
from ...request import FeatureGroupRequest, OnlineTableRequest
from ...tasks.duckdb import DuckDBTask
from ...workspace import require_workspace
from ..featurestore import (
    FeatureStore,
    Feature,
    OfflineMaterialization,
    OnlineMaterialization,
    OfflineStore,
    OnlineStore,
    OnlineStoreEnum,
)


def require_saved(func):
    """Decorator to ensure feature group is saved before method execution."""
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, 'feature_group_id') or self.feature_group_id is None:
            raise ValueError("Feature group is not loaded or saved")
        return func(self, *args, **kwargs)
    return wrapper


def require_set_entity(func):
    """Decorator to ensure entity is set before method execution."""
    def wrapper(self, *args, **kwargs):
        if self.entity is None:
            raise ValueError("Entity is not set")
        return func(self, *args, **kwargs)
    return wrapper


class MaterializationDuckDB:
    """Materialization options for DuckDB feature groups."""

    def __init__(
        self,
        event_time_col: Optional[str] = None,
        date_pattern: Optional[str] = None,
        offline: bool = True,
        online: bool = False,
        offline_materialization: Optional[OfflineMaterialization] = None,
        online_materialization: Optional[OnlineMaterialization] = None,
    ):
        self.event_time_col = event_time_col
        self.date_pattern = date_pattern
        self.offline = offline
        self.online = online
        self.offline_materialization = offline_materialization or OfflineMaterialization()
        self.online_materialization = online_materialization or OnlineMaterialization()


@dataclass
class FeatureGroupDuckDB(FeatureStore):
    """
    DuckDB-based Feature Group implementation.

    This class provides feature group functionality optimized for DuckDB,
    suitable for environments where Spark is not available.

    Args:
        name: Feature group name
        entity: Associated entity
        materialization: Materialization settings for this feature group
        source: Source dataframe or task
        description: Description for this feature group
        features: List of features to register
    """

    name: str
    materialization: MaterializationDuckDB = field(default_factory=MaterializationDuckDB)
    source: Optional[Union[DuckDBTask, pd.DataFrame]] = None
    description: Optional[str] = None
    features: Optional[List[Feature]] = None
    tag: Optional[List[str]] = None

    feature_group_id: Optional[str] = None
    offline_watermarks: List[str] = field(default_factory=list)
    online_watermarks: List[str] = field(default_factory=list)
    version: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def __post_init__(self):
        if self.entity is not None:
            if not hasattr(self.entity, 'entity_id') or self.entity.entity_id is None:
                self.entity.get_or_create()

    @require_workspace
    @require_project
    def get_or_create(self, version=None):
        """
        Get or create this feature group in the database.

        Args:
            version: Optional specific version to load.

        Returns:
            self: The feature group instance.
        """
        feature_group = FeatureGroupRequest.select_by_name(self.name)
        if feature_group is None:
            if self.entity is None:
                raise ValueError("Entity is not set")
            if self.features is None:
                raise ValueError("Features are not set")

            body = {
                "name": self.name,
                "project_id": context.project_id,
                "description": self.description or "",
                "offline": self.materialization.offline,
                "online": self.materialization.online,
                "materialization_params": {},
                "entity_id": self.entity.entity_id,
                "features": [f.to_dict() for f in self.features],
                "flow_id": None,
                "avro_schema": None,
            }

            req = FeatureGroupRequest(body=body)
            (
                self.feature_group_id,
                features,
                version_obj,
                offline_store_id,
                online_store_id,
            ) = req.save()
            self.version = version_obj.version
            self.offline_store_id = offline_store_id
            self.online_store_id = online_store_id
        else:
            logger.warning("Feature group already exists. Loading the feature group.")
            self.feature_group_id = feature_group.id
            self.id = feature_group.id

            if version is None:
                versions = FeatureGroupRequest.select_version_by_feature_group_id(
                    feature_group.id
                )
                self.version = versions[0].version
            else:
                version_obj = FeatureGroupRequest.select_by_feature_group_id_and_version(
                    feature_group.id, version
                )
                if version_obj is None:
                    raise ValueError(f"Version {version} not found.")
                self.version = version

            if feature_group.entity_id is not None:
                from ...request import EntityRequest
                entity = EntityRequest.select_by_id(feature_group.entity_id)
                self.entity = Entity(name=entity.name).get_or_create()

            self.offline_store_id = feature_group.offline_store
            self.online_store_id = feature_group.online_store

            # Load offline store
            if self.offline_store_id:
                _offline_store = FeatureGroupRequest.get_offline_store_by_id(
                    self.offline_store_id
                )
                if _offline_store:
                    from ..featurestore import OfflineStoreEnum, FeatureStoreFileOutput, FileKindEnum
                    self.materialization.offline_materialization.store = OfflineStore(
                        kind=OfflineStoreEnum(_offline_store.kind),
                        name=_offline_store.name
                    )

        return self

    @require_workspace
    @require_saved
    @require_project
    def delete(self):
        """
        Delete this feature group including storage files and metadata.

        Deletes the feature group from both the storage backend (parquet/delta files)
        and the metadata database.

        Returns:
            FeatureGroupRequest: The result of the metadata deletion.

        Raises:
            ValueError: If feature group is not saved or required context is missing.
        """
        offline_store = self.materialization.offline_materialization.store
        if offline_store is not None:
            offline_store.delete(
                name=self.name,
                project=context.project_id,
                entity=self.entity.entity_id
            )

        return FeatureGroupRequest.delete_by_id(self.feature_group_id)


@dataclass
class OnlineFeaturesDuckDB:
    """
    DuckDB-based online features implementation.

    This class provides online feature serving functionality using DuckDB
    for environments where Spark is not available.

    Args:
        lookup_key: Entity to use as lookup key
        dataframe: Source dataframe
        name: Optional name for the online table
        description: Description
        ttl: Time-to-live for the features
        online_store: Online store configuration
    """

    lookup_key: Entity
    dataframe: Optional[pd.DataFrame] = None
    name: Optional[str] = None
    description: str = ""
    ttl: Optional[timedelta] = None
    online_store: Optional[OnlineStore] = None
    tag: Optional[List[str]] = None
    online_watermarks: List[str] = field(default_factory=list)
    online_table_id: Optional[str] = None
    _online_reader: Optional[DuckDBTask] = None

    @require_project
    @require_workspace
    def __post_init__(self):
        if not hasattr(self.lookup_key, 'entity_id') or self.lookup_key.entity_id is None:
            self.lookup_key.get_or_create()

        if self.name is None:
            if self.dataframe is not None:
                column_names = ",".join(list(self.dataframe.columns))
                name = hashlib.md5(column_names.encode()).hexdigest()
            else:
                name = hashlib.md5(str(datetime.now()).encode()).hexdigest()
        else:
            name = self.name

        online_table = OnlineTableRequest.select_by_name(name)
        if online_table is None:
            if self.dataframe is None:
                raise ValueError(
                    "dataframe must be provided if online features have not been saved before."
                )

            if "__pk__" not in self.dataframe.columns:
                self.dataframe["__pk__"] = range(len(self.dataframe))

            if self.online_store is None:
                self._online_reader = DuckDBTask().add_input(
                    dataframe=pa.Table.from_pandas(self.dataframe)
                )
            else:
                self._online_reader = self.online_store(
                    spark=None,
                    result=self.dataframe,
                    write=True,
                    name=name,
                    project=context.project_id,
                )

                # Save online table metadata
                req = OnlineTableRequest(
                    body={
                        "name": name,
                        "description": self.description,
                        "entity": self.lookup_key.entity_id,
                        "online_store": self.online_store,
                        "feature_lookups": None,
                        "ttl": self.ttl,
                        "watermarks": self.online_watermarks,
                    }
                )
                self.online_table_id = req.save()
            self.dataframe = None
        else:
            self.description = online_table.description
            self.online_table_id = online_table.id

            online_store_obj = OnlineTableRequest.get_online_store_by_id(
                online_table.online_store_id
            )
            if online_store_obj:
                self.online_store = OnlineStore(
                    kind=OnlineStoreEnum(online_store_obj.kind),
                    value=online_store_obj.params if online_store_obj.params != "null" else None,
                    name=online_store_obj.name,
                )
                self._online_reader = self.online_store(
                    spark=None,
                    result=None,
                    write=False,
                    name=name,
                    project=context.project_id,
                )

    def delete(self):
        """
        Delete this online feature table including storage files and metadata.

        Deletes the online feature data from the storage backend and removes
        all associated metadata from the database.

        Returns:
            bool: True if deletion was successful.

        Raises:
            ValueError: If online table is not initialized or required context is missing.
        """
        if self.online_store is not None:
            try:
                self.online_store.delete(name=self.name, project=context.project_id)
            except Exception as e:
                logger.error(f"Failed to delete online store files: {e}")

        if self.online_table_id is not None:
            OnlineTableRequest.delete_by_id(self.online_table_id)
            OnlineTableRequest.delete_online_watermarks(self.online_table_id)
            OnlineTableRequest.delete_feature_group_from_online_table(self.online_table_id)

        return True

    def get_features(
        self,
        keys: List[dict],
        filter: Optional[str] = None,
        drop_event_time: bool = True,
    ):
        """
        Get features for the given entity keys.

        Args:
            keys: List of dictionaries with entity key-value pairs.
            filter: Optional SQL filter expression.
            drop_event_time: Whether to drop the event_time column from results.

        Returns:
            list: List of feature records as dictionaries.
        """
        if self._online_reader is None:
            raise ValueError("Online reader not initialized. Must be served first.")

        from ...validation import validate_column_name, validate_sql_value

        keys_str = []
        for key_dict in keys:
            for k, v in key_dict.items():
                validate_column_name(k)
                validate_sql_value(str(v), value_type="key value")
                keys_str.append(f"{k}='{v}'")

        keys_stm = " AND ".join(keys_str)
        if filter is not None:
            keys_stm = keys_stm + " AND " + filter

        query = f"SELECT * FROM __THIS__ WHERE {keys_stm}"
        res = (
            self._online_reader.add_sql(query)
            .transform(params={"return_as_pandas": True})
            .drop("__pk__", axis=1)
        )

        if drop_event_time and "event_time" in res.columns:
            res = res.drop("event_time", axis=1)

        return json.loads(res.to_json(orient="records"))
