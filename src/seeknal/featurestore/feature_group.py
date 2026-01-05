import json
from copy import deepcopy
import copy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import List, Optional, Union
import typer
from functools import reduce
import pendulum
from pydantic import BaseModel
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession, functions as F
import quinn
import mack

from ..context import context, logger, require_project
from ..entity import Entity
from ..flow import Flow, FlowOutput, FlowOutputEnum
from ..request import (
    EntityRequest,
    FeatureGroupRequest,
    FeatureRequest,
    FlowRequest,
    ProjectRequest,
    OnlineTableRequest,
)
from ..tasks.sparkengine import SparkEngineTask
from ..workspace import require_workspace
from .featurestore import *
from .featurestore import FeatureStore, Feature, FillNull
from ..tasks.sparkengine.transformers import (
    PointInTime,
    TableJoinDef,
    JoinTablesByExpr,
    JoinType,
    FilterByExpr,
)
from ..tasks.duckdb import DuckDBTask
from ..validation import validate_column_name, validate_sql_value


class Materialization(BaseModel):
    """
    Materialization options

    Attributes:
        event_time_col (str, optional): Specify which column that contains event time. Default to None.
        date_pattern (str, optional): Date pattern that use in event_time_col. Default to "yyyy-MM-dd".
        offline (bool, optional): Set the feature group should be stored in offline-store. Default to True.
        online (bool, optional): Set the feature group should be stored in online-store. Default to False.
        serving_ttl_days (int, optional): Look back window for features defined at the online-store.
            This parameters determines how long features will live in the online store. The unit is in days.
            Shorter TTLs improve performance and reduce computation. Default to 1.
            For example, if we set TTLs as 1 then only one day data available in online-store
        force_update_online (bool, optional): force to update the data in online-store. This will not consider
            to check whether the data going materialized newer than the data that already stored in online-store.
            Default to False.
        online_write_mode (OnlineWriteModeEnum, optional): Write mode when materialize to online-store.
            Default to "Append"
        schema_version (List[dict], optional): Determine which schema version for the feature group.
            Default to None.
    """

    event_time_col: Optional[str] = None
    date_pattern: Optional[str] = None
    offline: bool = True
    online: bool = False
    offline_materialization: OfflineMaterialization = OfflineMaterialization()
    online_materialization: OnlineMaterialization = OnlineMaterialization()

    #class Config:
    #    use_enum_values = True
    model_config = {
        "use_enum_values": True
    }


def require_saved(func):
    def wrapper(self, *args, **kwargs):
        if not "feature_group_id" in vars(self):
            raise ValueError("Feature group is not loaded or saved")
        else:
            func(self, *args, **kwargs)

    return wrapper


def require_set_source(func):
    def wrapper(self, *args, **kwargs):
        if self.source is None:
            raise ValueError("Source is not set")
        else:
            func(self, *args, **kwargs)

    return wrapper


def require_set_features(func):
    def wrapper(self, *args, **kwargs):
        if self.features is None:
            raise ValueError("Features are not set")
        else:
            func(self, *args, **kwargs)

    return wrapper


def require_set_entity(func):
    def wrapper(self, *args, **kwargs):
        if self.entity is None:
            raise ValueError("Entity is not set")
        else:
            func(self, *args, **kwargs)

    return wrapper


@dataclass
class FeatureGroup(FeatureStore):
    """
    A feature group representing a set of features created from a data source.

    A FeatureGroup is a logical grouping of related features that share the same
    entity and are typically computed from the same data source (Flow or DataFrame).
    It supports both offline and online materialization for feature storage and serving.

    Attributes:
        name: The unique name of the feature group within the project.
        materialization: Configuration for how features are stored and served,
            including offline/online storage settings and TTL configurations.
        source: The data source for computing features. Can be a Flow pipeline
            or a Spark DataFrame. If Flow, output will be set to SPARK_DATAFRAME.
        description: Optional human-readable description of the feature group.
        features: List of Feature objects to register. If None, all columns
            except join_keys and event_time will be registered as features.
        tag: Optional list of tags for categorizing and filtering feature groups.
        feature_group_id: Unique identifier assigned by the system after creation.
        offline_watermarks: List of timestamps indicating when offline data was
            materialized. Used for tracking data freshness.
        online_watermarks: List of timestamps indicating when online data was
            materialized. Used for tracking data freshness.
        version: Schema version number for the feature group.
        created_at: Timestamp when the feature group was created.
        updated_at: Timestamp when the feature group was last updated.
        avro_schema: Avro schema definition for the feature data structure.

    Example:
        >>> from seeknal.featurestore import FeatureGroup, Materialization
        >>> from seeknal.entity import Entity
        >>>
        >>> # Create a feature group from a flow
        >>> fg = FeatureGroup(
        ...     name="user_features",
        ...     materialization=Materialization(event_time_col="event_date"),
        ... )
        >>> fg.entity = Entity(name="user", join_keys=["user_id"])
        >>> fg.set_flow(my_flow).set_features().get_or_create()

    Note:
        A SparkContext must be active before creating a FeatureGroup instance.
        Initialize your Project and Workspace first if you encounter SparkContext errors.
    """

    name: str
    materialization: Materialization = field(default_factory=Materialization)
    source: Optional[Union[Flow, DataFrame]] = None
    description: Optional[str] = None
    features: Optional[List[Feature]] = None
    tag: Optional[List[str]] = None

    feature_group_id: Optional[str] = None
    offline_watermarks: List[str] = field(default_factory=list)
    online_watermarks: List[str] = field(default_factory=list)
    version: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    avro_schema: Optional[dict] = None

    def __post_init__(self):
        """Initialize the feature group and validate dependencies.

        Sets up the JVM gateway for Spark operations and ensures that the source
        (Flow or DataFrame) and entity are properly initialized.

        Raises:
            AttributeError: If SparkContext is not active or properly configured.
        """
        try:
            self._jvm_gateway = SparkContext._active_spark_context._gateway.jvm
        except AttributeError as e:
            raise AttributeError("Failed to initialize SparkContext. Please ensure that SparkContext is properly configured.\nTips: You can load your Project and Workspace first.") from e
        if self.source is not None:
            if isinstance(self.source, Flow):
                if self.source.output is not None:
                    if self.source.output.kind != FlowOutputEnum.SPARK_DATAFRAME:
                        self.source.output = FlowOutput(
                            kind=FlowOutputEnum.SPARK_DATAFRAME
                        )
                if not "flow_id" in vars(self.source):
                    self.source.get_or_create()
        if self.entity is not None:
            if not "entity_id" in vars(self.entity):
                self.entity.get_or_create()

    def set_flow(self, flow: Flow):
        """Set the data flow pipeline as the source for this feature group.

        Configures a Flow as the data source for computing features. The flow's
        output will be automatically set to SPARK_DATAFRAME if not already
        configured. If the flow hasn't been persisted, it will be created.

        Args:
            flow: The Flow pipeline to use for feature computation.

        Returns:
            FeatureGroup: The current instance for method chaining.

        Example:
            >>> fg = FeatureGroup(name="user_features")
            >>> fg.set_flow(my_transformation_flow)
        """
        if flow.output is not None:
            if flow.output.kind != FlowOutputEnum.SPARK_DATAFRAME:
                flow.output = FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)
            if not "flow_id" in vars(flow):
                flow.get_or_create()
        self.source = flow
        return self

    def set_dataframe(self, dataframe: DataFrame):
        """Set a Spark DataFrame as the source for this feature group.

        Configures a pre-computed Spark DataFrame as the data source for
        features. This is useful when features have already been computed
        or when using ad-hoc data that doesn't require a Flow pipeline.

        Args:
            dataframe: The Spark DataFrame containing the feature data.

        Returns:
            FeatureGroup: The current instance for method chaining.

        Example:
            >>> fg = FeatureGroup(name="user_features")
            >>> fg.set_dataframe(my_spark_df)
        """
        self.source = dataframe
        return self

    @staticmethod
    def _parse_avro_schema(schema: dict, exclude_cols: Optional[List[str]] = None):
        """Parse an Avro schema and extract feature definitions.

        Converts an Avro schema into a list of Feature objects by extracting
        field names and data types. Handles various Avro type representations
        including unions, arrays, and nested types.

        Args:
            schema: A dictionary representing the Avro schema with a 'fields' key
                containing the list of field definitions.
            exclude_cols: Optional list of column names to exclude from the
                resulting features. Useful for filtering out entity keys or
                timestamp columns.

        Returns:
            list[Feature]: A list of Feature objects with name, data_type, and
                online_data_type populated from the schema.

        Note:
            Fields that cannot be parsed will be skipped with a warning logged.
        """
        features = []
        for idx, i in enumerate(schema["fields"]):
            if isinstance(i["type"], List):
                if isinstance(i["type"][0], str):
                    data_type = i["type"][0]
                elif isinstance(i["type"][0], dict):
                    data_type = i["type"][0]["type"]
            elif isinstance(i["type"], dict):
                if i["type"]["type"] == "array":
                    data_type = "{}({})".format(i["type"]["type"], i["type"]["items"])
                else:
                    data_type = i["type"]["type"]
            elif isinstance(i["type"], str):
                data_type = i["type"]
            else:
                logger.warning(
                    "Cannot parse {} at {}. Skipping this.".format(i["type"], idx)
                )
                continue

            feature = Feature(
                name=i["name"], data_type=data_type, online_data_type=data_type
            )
            if exclude_cols is not None:
                if i["name"] not in exclude_cols:
                    features.append(feature)
            else:
                features.append(feature)
        return features

    @require_set_source
    @require_set_entity
    def set_features(
        self,
        features: Optional[List[Feature]] = None,
        reference_date: Optional[str] = None,
    ):
        """
        Set features to be used for this feature group. If features set as None,
        then it will use all columns except join_keys and event_time as features

        Args:
            features (Optional[List[Feature]], optional): Specify features. If this None,
                then automatically get features from transformation result. In addition,
                user may tell the feature name and description, then the detail about datatype
                automatically fetch from transformation result. Defaults to None.
            reference_date (Optional[str], optional): Specify date can be used as reference for
                get features from the transformation. Defaults to None.
            validate_with_source (bool, optional): If set as true, it won't validate with
                transformation result. Defaults to True.

        Raises:
            ValueError: If specify features not found from the transformation result

        Returns:
            Populate features of the feature group
        """
        if self.source is None:
            raise ValueError("Source is not set")

        reserved_cols = []
        _features = None
        for z in self.entity.join_keys:
            reserved_cols.append(z)

        if self.materialization.event_time_col is not None:
            reserved_cols.append(self.materialization.event_time_col)
        # if features not known yet, then need to load the data first
        # for getting list of features
        if features is None:
            # logger.info("Using all columns except entity join_key and event_time columns.")
            if isinstance(self.source, Flow):
                res = self.source.run(date=reference_date).drop(*reserved_cols)
            elif isinstance(self.source, DataFrame):
                res = self.source.drop(*reserved_cols)
            else:
                raise ValueError("Source only accepts Flow or DataFrame.")
            _avro_schema = json.loads(
                self._jvm_gateway.za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils.toAvroSchema(
                    res._jdf
                ).toString()
            )

            if _avro_schema is None:
                raise ValueError(
                    f"Feature not found in the source. Please make sure features are available from source."
                )
            _features = self._parse_avro_schema(schema=_avro_schema)
        else:
            feature_names = [f.name for f in features]
            metadata = {}
            if self.features is None:
                for k in features:
                    metadata[k.name] = {
                        "description": k.description,
                        **Feature(name=k.name).model_dump(exclude={"description"}),
                    }
            else:
                for k in features:
                    for f in self.features:
                        if k.name == f.name:
                            metadata[k.name] = f.dict()

            selections = feature_names + reserved_cols
            if isinstance(self.source, Flow):
                res = (
                    self.source.run(date=reference_date)
                    .select(*selections)
                    .drop(*reserved_cols)
                )
            elif isinstance(self.source, DataFrame):
                res = self.source.select(*selections).drop(*reserved_cols)
            else:
                raise ValueError("Source only accepts Flow or DataFrame.")
            _avro_schema = json.loads(
                self._jvm_gateway.za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils.toAvroSchema(
                    res._jdf
                ).toString()
            )
            if _avro_schema is None:
                raise ValueError(
                    "Cannot parse schema from the source. Please make sure features are available from source."
                )
            _features = self._parse_avro_schema(schema=_avro_schema)
            for k in _features:
                k.description = metadata[k.name]["description"]
                k.feature_id = metadata[k.name]["feature_id"]
                k.created_at = metadata[k.name]["created_at"]
                k.updated_at = metadata[k.name]["updated_at"]

        self.features = _features
        self.avro_schema = _avro_schema

        return self

    @require_workspace
    @require_project
    def get_or_create(self, version=None):
        """
        The `get_or_create` function retrieves an existing feature group or creates a new one based on the
        provided parameters.

        Args:
          version: The `version` parameter is an optional argument that specifies the version of the feature
        group to retrieve. If a version is provided, the code will load the feature group with that specific
        version. If no version is provided, the code will load the latest version of the feature group.

        Returns:
          The method `get_or_create` returns the instance of the class `self` after performing some
        operations and updating its attributes.
        """

        materialization_params = self.materialization.model_dump(exclude_none=False)
        materialization_params["offline_materialization"]["store"] = (
            asdict(self.materialization.offline_materialization.store)
            if self.materialization.offline_materialization.store is not None
            else None
        )
        materialization_params["online_materialization"]["store"] = asdict(
            self.materialization.online_materialization.store
        )

        body = {
            "name": self.name,
            "project_id": context.project_id,
            "description": ("" if self.description is None else self.description),
            "offline": self.materialization.offline,
            "online": self.materialization.online,
            "materialization_params": materialization_params,
        }

        feature_group = FeatureGroupRequest.select_by_name(self.name)
        if feature_group is None:
            if self.source is None:
                raise ValueError("source is not set")
            if self.entity is None:
                raise ValueError("Entity is not set")
            if self.features is None:
                raise ValueError("Features are not set")

            if isinstance(self.source, Flow):
                body["flow_id"] = self.source.flow_id
            else:
                body["flow_id"] = None

            req = FeatureGroupRequest(
                body={
                    **body,
                    "entity_id": self.entity.entity_id,
                    "features": [f.to_dict() for f in self.features],
                    "avro_schema": self.avro_schema,
                }
            )

            (
                self.feature_group_id,
                features,
                version_obj,
                offline_store_id,
                online_store_id,
            ) = req.save()
            self.version = version_obj.version
            for i in features:
                for j in self.features:
                    if i["metadata"]["name"] == j.name:
                        j.feature_id = i["feature_id"]
                        j.created_at = pendulum.instance(i["created_at"]).format(
                            "YYYY-MM-DD HH:mm:ss"
                        )
                        j.updated_at = pendulum.instance(i["updated_at"]).format(
                            "YYYY-MM-DD HH:mm:ss"
                        )
                        break
            self.offline_store_id = offline_store_id
            self.online_store_id = online_store_id

        else:
            logger.warning("Feature group already exists. Loading the feature group.")
            if version is None:
                versions = FeatureGroupRequest.select_version_by_feature_group_id(
                    feature_group.id
                )
                self.version = versions[0].version
                version_id = versions[0].id
                self.avro_schema = json.loads(versions[0].avro_schema)
            else:
                version_obj = (
                    FeatureGroupRequest.select_by_feature_group_id_and_version(
                        feature_group.id, version
                    )
                )
                if version_obj is None:
                    raise ValueError(f"Version {version} not found.")
                self.version = version
                version_id = version_obj.id
                self.avro_schema = json.loads(version_obj.avro_schema)

            offline_watermarks = FeatureGroupRequest.select_watermarks_by_version_id(
            feature_group.id, version_id
            )
            if offline_watermarks is not None:
                self.offline_watermarks = list(
                    map(
                        lambda x: pendulum.instance(x.date).format(
                            "YYYY-MM-DD HH:mm:SS"
                        ),
                        offline_watermarks,
                    )
                )
            if feature_group.online_watermark is not None:
                self.online_watermarks = feature_group.online_watermark.split(",")

            if feature_group.flow_id is not None:
                flow = FlowRequest.select_by_id(feature_group.flow_id)
                self.source = Flow(name=flow.name).get_or_create()
            else:
                self.source = None
            entity = EntityRequest.select_by_id(feature_group.entity_id)
            self.entity = Entity(name=entity.name).get_or_create()
            self.feature_group_id = feature_group.id
            self.id = feature_group.id

            features = FeatureRequest.select_by_feature_group_id_and_version(
                feature_group.id, self.version
            )
            self.features = []
            for i in features:
                self.features.append(
                    Feature(
                        name=i.name,
                        feature_id=str(i.id),
                        description=i.description,
                        data_type=i.datatype,
                        online_data_type=i.online_datatype,
                        created_at=pendulum.instance(i.created_at).format(
                            "YYYY-MM-DD HH:mm:ss"
                        ),
                        updated_at=pendulum.instance(i.updated_at).format(
                            "YYYY-MM-DD HH:mm:ss"
                        ),
                    )
                )

            # handle materialization
            for key, value in json.loads(feature_group.materialization_params).items():
                if key == "offline_materialization":
                    for k, v in value.items():
                        setattr(self.materialization.offline_materialization, k, v)
                elif key == "online_materialization":
                    for k, v in value.items():
                        if k == "store":
                            self.materialization.online_materialization.store = (
                                OnlineStore(
                                    kind=OnlineStoreEnum(v["kind"]), value=v["value"]
                                )
                            )
                        else:
                            setattr(self.materialization.online_materialization, k, v)
                else:
                    setattr(self.materialization, key, value)
            self.offline_store_id = feature_group.offline_store
            self.online_store_id = feature_group.online_store

        # handling load offline store object
        _offline_store = FeatureGroupRequest.get_offline_store_by_id(
            self.offline_store_id
        )
        self.materialization.offline_materialization.store = OfflineStore(
            kind=OfflineStoreEnum(_offline_store.kind), name=_offline_store.name
        )
        if _offline_store.params == "null":
            self.materialization.offline_materialization.store.value = None
        else:
            value_params = json.loads(_offline_store.params)
            if _offline_store.kind == "file":
                self.materialization.offline_materialization.store.value = (
                    FeatureStoreFileOutput(
                        path=value_params["path"],
                        kind=FileKindEnum(value_params["kind"]),
                    )
                )
            elif _offline_store.kind == "hive_table":
                self.materialization.offline_materialization.store.value = (
                    FeatureStoreHiveTableOutput(database=value_params["database"])
                )
            else:
                self.materialization.offline_materialization.store.value = value_params

        return self

    def update_materialization(
        self,
        offline: Optional[bool] = None,
        online: Optional[bool] = None,
        offline_materialization: Optional[OfflineMaterialization] = None,
        online_materialization: Optional[OnlineMaterialization] = None,
    ):
        """Update the materialization settings for this feature group.

        Modifies the materialization configuration and persists the changes
        to the feature store backend. This allows changing storage settings,
        TTL values, and enabling/disabling offline or online storage.

        Args:
            offline: Enable or disable offline storage. If None, keeps current setting.
            online: Enable or disable online storage. If None, keeps current setting.
            offline_materialization: New offline materialization configuration.
                If None, keeps current setting.
            online_materialization: New online materialization configuration.
                If None, keeps current setting.

        Returns:
            FeatureGroup: The current instance for method chaining.

        Example:
            >>> fg.update_materialization(
            ...     online=True,
            ...     online_materialization=OnlineMaterialization(ttl=2880)
            ... )
        """
        if offline is not None:
            self.materialization.offline = offline
        if online is not None:
            self.materialization.online = online
        if offline_materialization is not None:
            self.materialization.offline_materialization = offline_materialization
        if online_materialization is not None:
            self.materialization.online_materialization = online_materialization

        materialization_params = self.materialization.dict(exclude_none=False)
        materialization_params["offline_materialization"]["store"] = (
            asdict(self.materialization.offline_materialization.store)
            if self.materialization.offline_materialization.store is not None
            else None
        )
        materialization_params["online_materialization"]["store"] = asdict(
            self.materialization.online_materialization.store
        )

        body = {
            "offline": self.materialization.offline,
            "online": self.materialization.online,
            "materialization_params": materialization_params,
            "feature_group_id": self.feature_group_id,
        }

        req = FeatureGroupRequest(
            body={
                **body,
            }
        )

        req.update_materialization()

        return self

    @require_workspace
    @require_project
    def list_versions(self):
        """
        List all versions of this feature group.

        Returns a list of dictionaries containing version metadata including:
        - version: The version number
        - avro_schema: The Avro schema for this version (as dict)
        - created_at: When the version was created
        - updated_at: When the version was last updated
        - feature_count: Number of features in this version

        Returns:
            List[dict]: A list of version metadata dictionaries, ordered by version
                number descending (latest first). Returns an empty list if the
                feature group has not been saved or has no versions.

        Example:
            >>> fg = FeatureGroup(name="user_features").get_or_create()
            >>> versions = fg.list_versions()
            >>> for v in versions:
            ...     print(f"Version {v['version']}: {v['feature_count']} features")
        """
        if not hasattr(self, 'feature_group_id') or self.feature_group_id is None:
            # Feature group not saved yet, return empty list
            return []

        versions = FeatureGroupRequest.select_version_by_feature_group_id(
            self.feature_group_id
        )

        if versions is None:
            return []

        result = []
        for v in versions:
            # Parse avro_schema from JSON string
            try:
                avro_schema = json.loads(v.avro_schema) if v.avro_schema else None
            except (json.JSONDecodeError, TypeError):
                avro_schema = None

            # Get feature count for this version
            features = FeatureRequest.select_by_feature_group_id_and_version(
                self.feature_group_id, v.version
            )
            feature_count = len(features) if features else 0

            result.append({
                "version": v.version,
                "avro_schema": avro_schema,
                "created_at": pendulum.instance(v.created_at).format("YYYY-MM-DD HH:mm:ss") if v.created_at else None,
                "updated_at": pendulum.instance(v.updated_at).format("YYYY-MM-DD HH:mm:ss") if v.updated_at else None,
                "feature_count": feature_count,
            })

        return result

    @require_workspace
    @require_project
    def get_version(self, version: int) -> Optional[dict]:
        """
        Get metadata for a specific version of this feature group.

        Args:
            version (int): The version number to retrieve.

        Returns:
            Optional[dict]: A dictionary containing version metadata if found,
                None if the version doesn't exist. The dictionary includes:
                - version: The version number
                - avro_schema: The Avro schema for this version (as dict)
                - created_at: When the version was created
                - updated_at: When the version was last updated
                - feature_count: Number of features in this version

        Example:
            >>> fg = FeatureGroup(name="user_features").get_or_create()
            >>> v1 = fg.get_version(1)
            >>> if v1:
            ...     print(f"Version 1 has {v1['feature_count']} features")
        """
        if not hasattr(self, 'feature_group_id') or self.feature_group_id is None:
            # Feature group not saved yet, return None
            return None

        version_obj = FeatureGroupRequest.select_by_feature_group_id_and_version(
            self.feature_group_id, version
        )

        if version_obj is None:
            return None

        # Parse avro_schema from JSON string
        try:
            avro_schema = json.loads(version_obj.avro_schema) if version_obj.avro_schema else None
        except (json.JSONDecodeError, TypeError):
            avro_schema = None

        # Get feature count for this version
        features = FeatureRequest.select_by_feature_group_id_and_version(
            self.feature_group_id, version
        )
        feature_count = len(features) if features else 0

        return {
            "version": version_obj.version,
            "avro_schema": avro_schema,
            "created_at": pendulum.instance(version_obj.created_at).format("YYYY-MM-DD HH:mm:ss") if version_obj.created_at else None,
            "updated_at": pendulum.instance(version_obj.updated_at).format("YYYY-MM-DD HH:mm:ss") if version_obj.updated_at else None,
            "feature_count": feature_count,
        }

    @require_workspace
    @require_project
    def compare_versions(self, from_version: int, to_version: int) -> Optional[dict]:
        """
        Compare schemas between two versions of this feature group.

        Identifies added, removed, and modified features between the two versions
        by comparing their Avro schemas.

        Args:
            from_version (int): The base version number to compare from.
            to_version (int): The target version number to compare to.

        Returns:
            Optional[dict]: A dictionary containing the comparison result if both
                versions exist, None if either version doesn't exist or the feature
                group has not been saved. The dictionary includes:
                - from_version: The base version number
                - to_version: The target version number
                - added: List of field names added in to_version
                - removed: List of field names removed in to_version
                - modified: List of dicts with field name and type changes

        Raises:
            ValueError: If from_version equals to_version.

        Example:
            >>> fg = FeatureGroup(name="user_features").get_or_create()
            >>> diff = fg.compare_versions(1, 2)
            >>> if diff:
            ...     print(f"Added features: {diff['added']}")
            ...     print(f"Removed features: {diff['removed']}")
            ...     print(f"Modified features: {diff['modified']}")
        """
        if from_version == to_version:
            raise ValueError("from_version and to_version must be different")

        if not hasattr(self, 'feature_group_id') or self.feature_group_id is None:
            # Feature group not saved yet, return None
            return None

        # Fetch both versions
        from_version_obj = FeatureGroupRequest.select_by_feature_group_id_and_version(
            self.feature_group_id, from_version
        )
        to_version_obj = FeatureGroupRequest.select_by_feature_group_id_and_version(
            self.feature_group_id, to_version
        )

        # Check if both versions exist
        if from_version_obj is None:
            raise ValueError(f"Version {from_version} not found for this feature group")
        if to_version_obj is None:
            raise ValueError(f"Version {to_version} not found for this feature group")

        # Get avro_schema JSON strings
        from_schema_json = from_version_obj.avro_schema if from_version_obj.avro_schema else "{}"
        to_schema_json = to_version_obj.avro_schema if to_version_obj.avro_schema else "{}"

        # Compare schemas using FeatureGroupRequest.compare_schemas()
        schema_diff = FeatureGroupRequest.compare_schemas(from_schema_json, to_schema_json)

        return {
            "from_version": from_version,
            "to_version": to_version,
            "added": schema_diff.get("added", []),
            "removed": schema_diff.get("removed", []),
            "modified": schema_diff.get("modified", []),
        }

    @require_workspace
    @require_saved
    @require_project
    def delete(self):
        """Delete this feature group and its associated data.

        Removes the feature group from the feature store backend along with
        any data stored in the offline store. This operation is irreversible.

        Returns:
            FeatureGroupRequest: The request object used to perform the deletion.

        Raises:
            ValueError: If the feature group has not been saved (no feature_group_id).

        Note:
            This requires an active workspace and project context. The feature
            group must have been previously saved using get_or_create().
        """
        offline_store = self.materialization.offline_materialization.store
        offline_store.delete(
            name=self.name, project=context.project_id, entity=self.entity.entity_id
        )

        return FeatureGroupRequest.delete_by_id(self.feature_group_id)

    @require_workspace
    @require_saved
    @require_project
    def write(
        self,
        feature_start_time: Optional[datetime] = None,
        feature_end_time: Optional[datetime] = None,
        output_date_pattern: str = "yyyyMMdd",
    ):
        """
        Writes the feature group data to the offline store, using the specified
        feature start and end times and output date pattern.

        Args:
            feature_start_time (Optional[datetime]): The start time for the feature data.
                If None, the current date is used.
            feature_end_time (Optional[datetime]): The end time for the feature data.
                If None, all available data is used.
            output_date_pattern (str): The output date pattern for the feature data.

        Returns:
            None
        """

        date_now = pendulum.now(tz="UTC").format("YYYY-MM-DD")
        if isinstance(self.source, Flow):
            fg_flow = deepcopy(self.source)
            if self.materialization.event_time_col is None:
                add_date_column = SparkEngineTask().add_stage(
                    class_name="tech.mta.seeknal.transformers.AddColumnByExpr",
                    params={"expression": f"'{date_now}'", "outputCol": "__date__"},
                )
                if fg_flow.tasks is not None:
                    fg_flow.tasks.append(add_date_column)
                else:
                    fg_flow.tasks = [add_date_column]
        elif isinstance(self.source, DataFrame):
            if self.materialization.event_time_col is None:
                fg_flow = self.source.withColumn("__date__", F.lit(date_now))
            else:
                fg_flow = self.source
        else:
            raise ValueError("Source only accepts Flow or DataFrame.")

        date_pattern = "yyyy-MM-dd"
        if (
            self.materialization.event_time_col is not None
            and self.materialization.date_pattern is not None
        ):
            date_pattern = self.materialization.date_pattern

        spark = SparkSession.builder.getOrCreate()
        if self.materialization.event_time_col is not None:
            event_time_col = self.materialization.event_time_col
            if isinstance(fg_flow, Flow):
                fg_flow = fg_flow.set_input_date_col(
                    date_col=event_time_col, date_pattern=date_pattern
                )
        else:
            event_time_col = "__date__"
        if isinstance(fg_flow, Flow):
            flow_res = fg_flow.run(
                start_date=feature_start_time, end_date=feature_end_time
            )
        elif isinstance(fg_flow, DataFrame):
            if feature_start_time is not None:
                flow_res = fg_flow.filter(
                    F.col(event_time_col)
                    >= pendulum.instance(feature_start_time).format(
                        date_pattern.upper()
                    )
                )
            elif feature_end_time is not None:
                flow_res = fg_flow.filter(
                    F.col(event_time_col)
                    <= pendulum.instance(feature_end_time).format(date_pattern.upper())
                )
            elif feature_start_time is not None and feature_end_time is not None:
                flow_res = fg_flow.filter(
                    (
                        F.col(event_time_col)
                        >= pendulum.instance(feature_start_time).format(
                            date_pattern.upper()
                        )
                    )
                    & (
                        F.col(event_time_col)
                        <= pendulum.instance(feature_end_time).format(
                            date_pattern.upper()
                        )
                    )
                )
            else:
                flow_res = fg_flow
        flow_res = quinn.snake_case_col_names(flow_res)
        # generate pk given entity join_keys
        flow_res = mack.with_md5_cols(
            flow_res, self.entity.join_keys + [event_time_col], "__pk__"
        )

        # getting the date from the flow result
        if self.materialization.event_time_col is not None:
            _date = (
                SparkEngineTask()
                .add_input(dataframe=flow_res)
                .set_date_col(date_col=event_time_col)
                .get_date_available()
            )
            _date = [
                pendulum.parse(i).format(output_date_pattern.upper()) for i in _date
            ]
            date_available = [
                datetime.fromisoformat(pendulum.parse(i).to_datetime_string())
                for i in _date
            ]
        else:
            date_available = [
                datetime.fromisoformat(pendulum.parse(date_now).to_datetime_string())
            ]

        if self.materialization.offline:
            logger.info("writing to offline-store")

            arr_size = len(self.entity.join_keys) + 1
            arr = spark.sparkContext._gateway.new_array(
                self._jvm_gateway.java.lang.String, arr_size
            )
            for idx, i in enumerate(self.entity.join_keys):
                arr[idx] = i
            arr[arr_size - 1] = "__pk__"

            project_name = ProjectRequest.select_by_id(context.project_id).name
            fs_serialize = (
                self._jvm_gateway.tech.mta.seeknal.connector.serde.FeatureStoreSerDe()
                .setEventTimeCol(event_time_col)
                .setDatePattern(date_pattern)
                .setEntity(self.entity.name)
                .setProject(project_name)
                .setFeatureGroup(self.name)
                .setKeyCols(arr)
                .setSerialize(True)
            )

            res = fs_serialize.transform(flow_res._jdf)
            res_df = DataFrame(res, self._jvm_gateway._wrapped)

            # add watermarks
            version_obj = FeatureGroupRequest.select_by_feature_group_id_and_version(
                self.feature_group_id, self.version
            )
            if version_obj is None:
                raise ValueError(f"Version {self.version} not found.")
            req = FeatureGroupRequest(
                body={
                    "feature_group_id": self.feature_group_id,
                    "feature_group_version_id": version_obj.id,
                }
            )

            req.add_offline_watermarks(date_available)
            offline_watermarks = [
                w.date
                for w in FeatureGroupRequest.select_watermarks_by_version_id(
                    self.feature_group_id, version_obj.id
                )
            ]

            # writing to offline-store
            offline_store = self.materialization.offline_materialization.store
            _start_date = (
                date_now
                if feature_start_time is None
                else pendulum.instance(feature_start_time).format(date_pattern.upper())
            )
            _end_date = (
                "none"
                if feature_end_time is None
                else pendulum.instance(feature_end_time).format(date_pattern.upper())
            )
            offline_store(
                result=res_df,
                name=self.name,
                project=context.project_id,
                entity=self.entity.entity_id,
                mode=self.materialization.offline_materialization.mode,
                start_date=_start_date,
                end_date=_end_date,
                version=self.version,
                latest_watermark=max(offline_watermarks),
                ttl=self.materialization.offline_materialization.ttl,
            )
        if self.materialization.online:
            logger.info("Writing to online-store.")
            if self.materialization.offline:
                hist = HistoricalFeatures(lookups=[FeatureLookup(source=self)])
                hist.using_latest().serve(
                    target=self.materialization.online_materialization.store,
                    ttl=timedelta(days=self.materialization.online_materialization.ttl),
                )
            else:
                flow_res = flow_res.withColumn(
                    "event_time", F.to_timestamp(F.col(event_time_col), date_pattern)
                ).drop(event_time_col)
                if self.materialization.online_materialization.ttl is not None:
                    _timedelta = timedelta(
                        minutes=self.materialization.online_materialization.ttl
                    )
                else:
                    _timedelta = None
                OnlineFeatures(
                    lookup_key=self.entity,
                    lookups=[FeatureLookup(source=self)],
                    ttl=_timedelta,
                    online_store=self.materialization.online_materialization.store,
                    dataframe=flow_res,
                )


class GetLatestTimeStrategy(Enum):
    REQUIRE_ALL = "require_all"
    REQUIRE_ANY = "require_any"


@dataclass
class FeatureLookup:
    """
    A class that represents a feature lookup operation in a feature store.

    Attributes:
        source (FeatureStore): The feature store to perform the lookup on.
        features (Optional[List[str]]): A list of feature names to include in the lookup.
            If None, all features in the store will be included.
        exclude_features (Optional[List[str]]): A list of feature names to exclude from the lookup.
            If None, no features will be excluded.
    """

    source: FeatureStore
    features: Optional[List[str]] = None
    exclude_features: Optional[List[str]] = None


@dataclass
class HistoricalFeatures:
    """
    A class for retrieving historical features from a feature store.

    Attributes:
        lookups (List[FeatureLookup]): A list of FeatureLookup objects representing the features to retrieve.
    """

    lookups: List[FeatureLookup]
    fill_nulls: Optional[List[FillNull]] = None

    @require_project
    @require_workspace
    def __post_init__(self):
        """
        Initializes the HistoricalFeatures object by checking whether objects in lookups are already loaded and loading them if not.
        """
        for i in self.lookups:
            if i.source.id is None:
                i.source.get_or_create()
        self.lookup_key = self.lookups[0].source.entity
        self.offline_store_id = self.lookups[0].source.offline_store_id
        for l in self.lookups:
            if l.source.offline_store_id != self.offline_store_id:
                raise ValueError("All feature sources must have the same offline store")
        self.offline_store = self.lookups[
            0
        ].source.materialization.offline_materialization.store
        if len(self.lookups) > 1:
            for i in self.lookups[1:]:
                if i.source.entity != self.lookup_key:
                    raise ValueError("All feature stores must have the same entity")
        self._jvm_gateway = SparkContext._active_spark_context._gateway.jvm
        self.spark = SparkSession.builder.getOrCreate()
        df = self.offline_store(
            spark=self.spark,
            project=context.project_id,
            entity=self.lookup_key.entity_id,
            write=False,
        )
        df = self._deserialize(df)
        self.flow = (
            SparkEngineTask()
            .add_input(dataframe=df)
            .set_date_col(date_col="event_time", date_pattern="yyyy-MM-dd HH:mm:SS")
        )

    def _deserialize(self, df: DataFrame) -> DataFrame:
        """
        Deserialize a DataFrame by applying a set of transformations defined by the FeatureServing object.

        Args:
            df (pyspark.sql.DataFrame): The DataFrame to be deserialized.

        Returns:
            pyspark.sql.DataFrame: The deserialized DataFrame.
        """
        feature_groups = []
        fg_clazz = self._jvm_gateway.tech.mta.seeknal.params.FeatureGroup
        for i in self.lookups:
            fg_def = {}
            if isinstance(i.source, FeatureGroup):
                fg_def = {
                    "name": i.source.name,
                    "schemaValueString": i.source.avro_schema,
                }
                if i.features is not None:
                    fg_def["features"] = i.features
                if i.exclude_features is not None:
                    fg_def["excludeFeatures"] = i.exclude_features
            else:
                raise ValueError("Only FeatureGroup is supported in HistoricalFeatures")
            feature_groups.append(fg_def)

        arr_fg = self.spark.sparkContext._gateway.new_array(
            fg_clazz, len(feature_groups)
        )
        for i, fg in enumerate(feature_groups):
            if "features" not in fg:
                _java_feature_obj = self._jvm_gateway.scala.Option.apply(None)
            else:
                _java_feature_arr = self.spark.sparkContext._gateway.new_array(
                    self._jvm_gateway.java.lang.String, len(fg["features"])
                )
                for idx, k in enumerate(fg["features"]):
                    _java_feature_arr[idx] = k
                _java_feature_obj = self._jvm_gateway.scala.Option.apply(
                    _java_feature_arr
                )
            if "excludeFeatures" not in fg:
                _java_exclude_feature_obj = self._jvm_gateway.scala.Option.apply(None)
            else:
                _java_exclude_feature_arr = self.spark.sparkContext._gateway.new_array(
                    self._jvm_gateway.java.lang.String, len(fg["excludeFeatures"])
                )
                for idx, l in enumerate(fg["excludeFeatures"]):
                    _java_exclude_feature_arr[idx] = l
                _java_exclude_feature_obj = self._jvm_gateway.scala.Option.apply(
                    _java_exclude_feature_arr
                )

            arr_fg[i] = fg_clazz(
                fg["name"],
                _java_feature_obj,
                _java_exclude_feature_obj,
                self._jvm_gateway.scala.Option.apply(None),
                self._jvm_gateway.scala.Option.apply(None),
                self._jvm_gateway.scala.Option.apply(
                    json.dumps(fg["schemaValueString"])
                ),
            )
        arr_size = len(self.lookup_key.join_keys) + 1
        arr = self.spark.sparkContext._gateway.new_array(
            self._jvm_gateway.java.lang.String, arr_size
        )
        for idx, i in enumerate(self.lookup_key.join_keys):
            arr[idx] = i
        arr[arr_size - 1] = "__pk__"
        project_name = ProjectRequest.select_by_id(context.project_id).name
        fillnull_clazz = self._jvm_gateway.tech.mta.seeknal.params.FillNull
        arr_fillnull = None
        if self.fill_nulls is not None:
            arr_fillnull = self.spark.sparkContext._gateway.new_array(
                fillnull_clazz, len(self.fill_nulls)
            )
            for idx, f in enumerate(self.fill_nulls):
                if f.columns is None:
                    fillnull = fillnull_clazz(
                        f.value, f.dataType, self._jvm_gateway.scala.Option.apply(None)
                    )
                else:
                    arr_col = self.spark.sparkContext._gateway.new_array(
                        self._jvm_gateway.java.lang.String, len(f.columns)
                    )
                    for idx, k in enumerate(f.columns):
                        arr_col[idx] = k
                    fillnull = fillnull_clazz(
                        f.value,
                        f.dataType,
                        self._jvm_gateway.scala.Option.apply(arr_col),
                    )
                arr_fillnull[idx] = fillnull
        fs_deserialize = (
            self._jvm_gateway.tech.mta.seeknal.connector.serde.FeatureStoreSerDe()
            .setEntity(self.lookup_key.name)
            .setProject(project_name)
            .setFeatureGroups(arr_fg)
            .setKeyCols(arr)
            .setSerialize(False)
        )
        if self.fill_nulls is not None:
            fs_deserialize = fs_deserialize.setFillNull(arr_fillnull)
        res = fs_deserialize.transform(df._jdf)
        res_df = DataFrame(res, self._jvm_gateway._wrapped)

        return res_df

    def using_spine(
        self,
        spine: pd.DataFrame,
        date_col: Optional[str] = None,
        offset: int = 0,
        length: Optional[int] = None,
        keep_cols: Optional[List[str]] = None,
    ):
        """
        Adds a spine DataFrame to the feature store serving pipeline.

        Args:
            spine (pd.DataFrame): The spine DataFrame to add to the pipeline.
            date_col (str, optional): The name of the column containing the date to use for point-in-time joins.
                If not provided, point-in-time joins will not be performed.
            offset (int, optional): number of days to use as a reference point for join.
                E.g. offset=3, how='past' means that features dates equal (and older than) to three days before application date will be joined. Defaults to 0.
            length (int, optional): when how is not equal to 'point in time' limit the period of feature dates to join. Defaults to no limit.
            keep_cols (List[str], optional): A list of column names to keep from the spine DataFrame.
                If not provided, none columns will be kept.

        """
        spine_columns = list(spine.keys())
        for i in self.lookup_key.join_keys:
            if i not in spine_columns:
                raise ValueError("Spine DataFrame must contain all join keys")
        spine_df = self.spark.createDataFrame(spine)

        # Validate join_keys before using them in SQL expressions
        for join_key in self.lookup_key.join_keys:
            validate_column_name(join_key)

        if date_col is not None:
            point_in_time = PointInTime(
                spine=spine_df,
                offset=offset,
                length=length,
                feature_date_format="yyyy-MM-dd HH:mm:SS",
                app_date=date_col,
                app_date_format="yyyy-MM-dd",
                col_id=self.lookup_key.join_keys[0],
                spine_col_id=self.lookup_key.join_keys[0],
                keep_cols=keep_cols,
            )
            self.flow.add_stage(transformer=point_in_time)
        else:
            selector = ["a.*"]
            if keep_cols is not None:
                selector += keep_cols
            tables = [
                TableJoinDef(
                    table=spine_df,
                    joinType=JoinType.INNER,
                    alias="b",
                    joinExpression="a.{} = b.{}".format(
                        self.lookup_key.join_keys[0], self.lookup_key.join_keys[0]
                    ),
                )
            ]
            join = JoinTablesByExpr(tables=tables, select_stm=",".join(selector))
            self.flow.add_stage(transformer=join)
        return self

    def _get_offline_watermarks(
        self, fetch_strategy: GetLatestTimeStrategy = GetLatestTimeStrategy.REQUIRE_ANY
    ):
        # select latest time according fetch strategy
        if fetch_strategy == GetLatestTimeStrategy.REQUIRE_ALL:
            offline_watermarks = {}
            for i in self.lookups:
                offline_watermarks[i.source.name] = i.source.offline_watermarks

            watermark_set = list(
                set(reduce(list.__add__, list(offline_watermarks.values())))
            )
            intersect_watermark = []
            feature_group_names = [l.source.name for l in self.lookups]
            for i in watermark_set:
                intersect_watermark.append(i)
                for k in feature_group_names:
                    if i not in offline_watermarks[k]:
                        intersect_watermark.remove(i)
            if intersect_watermark:
                max_intersect_watermark = max(intersect_watermark)
                value = (max_intersect_watermark, max_intersect_watermark)
            else:
                raise ValueError(
                    "No offline watermark records are intersected. Please materialize the feature-group with same time horizon or use fetch_strategy='require_any'"
                )
        elif fetch_strategy == GetLatestTimeStrategy.REQUIRE_ANY:
            offline_watermarks = {}
            for i in self.lookups:
                offline_watermarks[i.source.name] = [max(i.source.offline_watermarks)]

            watermark_set = list(
                set(reduce(list.__add__, list(offline_watermarks.values())))
            )

            if watermark_set:
                # since we want fetch all feature groups then start and end date must
                # cover all feature groups
                value = (min(watermark_set), max(watermark_set))
            else:
                raise ValueError(
                    "Offline watermark records aren't available. Please materialize the feature-group to offline store first."
                )
        return value

    def using_latest(
        self, fetch_strategy: GetLatestTimeStrategy = GetLatestTimeStrategy.REQUIRE_ALL
    ):
        value = self._get_offline_watermarks(fetch_strategy)
        filter_by_expr = FilterByExpr(
            expression=f"event_time BETWEEN '{value[0]}' AND '{value[1]}'"
        )
        self.flow.add_stage(transformer=filter_by_expr)
        return self

    def _filter_by_start_end_time(
        self,
        df: DataFrame,
        feature_start_time: Optional[datetime] = None,
        feature_end_time: Optional[datetime] = None,
    ) -> DataFrame:
        if feature_start_time is not None:
            if isinstance(feature_start_time, datetime):
                feature_start_time = pendulum.instance(feature_start_time).format(
                    "YYYY-MM-DD HH:MM:SS"
                )
        if feature_end_time is not None:
            if isinstance(feature_end_time, datetime):
                feature_end_time = pendulum.instance(feature_end_time).format(
                    "YYYY-MM-DD HH:MM:SS"
                )
        if feature_start_time is not None and feature_end_time is not None:
            df = df.filter(
                (df.event_time >= feature_start_time)
                & (df.event_time <= feature_end_time)
            )
        elif feature_start_time is not None:
            df = df.filter(df.event_time >= feature_start_time)
        elif feature_end_time is not None:
            df = df.filter(df.event_time <= feature_end_time)
        return df

    def to_dataframe(
        self,
        feature_start_time: Optional[datetime] = None,
        feature_end_time: Optional[datetime] = None,
    ) -> DataFrame:
        """
        Returns a pandas DataFrame containing the transformed feature data within the specified time range.

        Args:
            feature_start_time (Optional[datetime]): The start time of the time range to filter the feature data.
            feature_end_time (Optional[datetime]): The end time of the time range to filter the feature data.
        """
        df = self.flow.transform(spark=self.spark)
        return self._filter_by_start_end_time(df, feature_start_time, feature_end_time)

    def serve(
        self,
        feature_start_time: Optional[datetime] = None,
        feature_end_time: Optional[datetime] = None,
        target: Optional[OnlineStore] = None,
        name: Optional[str] = None,
        ttl: Optional[timedelta] = None,
    ):
        df = self.flow.transform(spark=self.spark)
        df = self._filter_by_start_end_time(df, feature_start_time, feature_end_time)
        return OnlineFeatures(
            name=name,
            lookup_key=self.lookup_key,
            lookups=self.lookups,
            dataframe=df,
            ttl=ttl,
            online_store=target,
        )


@dataclass
class OnlineFeatures:

    lookup_key: Entity
    dataframe: Optional[Union[DataFrame, pd.DataFrame]] = None
    name: Optional[str] = None
    description: str = ""
    lookups: Optional[List[FeatureLookup]] = None
    ttl: Optional[timedelta] = None
    online_store: Optional[OnlineStore] = None
    tag: Optional[List[str]] = None
    online_watermarks: List[str] = field(default_factory=list)
    _online_reader: Optional[DuckDBTask] = None

    @require_project
    @require_workspace
    def __post_init__(self, **kwargs):
        if not "entity_id" in vars(self.lookup_key):
            self.lookup_key.get_or_create()

        if self.name is None:
            if self.lookups is not None:
                fg_names = [
                    json.dumps(
                        {
                            "names": i.source.name,
                            "features": i.features,
                            "exclude_features": i.exclude_features,
                        }
                    )
                    for i in self.lookups
                ]
                name = hashlib.md5(",".join(fg_names).encode()).hexdigest()
            else:
                column_names = ",".join(self.dataframe.columns)
                name = hashlib.md5(column_names.encode()).hexdigest()
        else:
            name = self.name

        online_table = OnlineTableRequest.select_by_name(name)
        if online_table is None:
            if self.dataframe is None:
                raise ValueError(
                    "dataframe must be provided if online features have not saved before."
                )
            _features = self.lookup_key.join_keys + ["event_time", "__pk__"]
            if self.lookups is not None:
                for i in self.lookups:
                    i.source.get_or_create()
                    _features.extend([k.name for k in i.source.features])

                # check whether input dataframe columns align with
                # lookups
                if isinstance(self.dataframe, DataFrame):
                    for f in self.dataframe.columns:
                        if f not in _features:
                            raise ValueError(
                                f"Column {f} is not part of any feature lookups that you specified."
                            )
                    if "event_time" not in self.dataframe.columns:
                        raise ValueError(f"Input dataframe must have event_time column")
                elif isinstance(self.dataframe, pd.DataFrame):
                    for f in list(self.dataframe.columns):
                        if f not in _features:
                            raise ValueError(
                                f"Column {f} is not part of any feature lookups that you specified."
                            )
                    if "event_time" not in list(self.dataframe.columns):
                        raise ValueError(f"Input dataframe must have event_time column")
            if "__pk__" not in self.dataframe.columns:
                if isinstance(self.dataframe, DataFrame):
                    self.dataframe = self.dataframe.withColumn(
                        "__pk__", F.monotonically_increasing_id()
                    )
                elif isinstance(self.dataframe, pd.DataFrame):
                    self.dataframe["__pk__"] = range(len(self.dataframe))
            if self.online_store is None:
                pd_df = self.dataframe.toPandas()
                self._online_reader = DuckDBTask().add_input(
                    dataframe=pa.Table.from_pandas(pd_df)
                )
            else:
                if "spark" in kwargs:
                    spark = kwargs["spark"]
                else:
                    spark = SparkSession.builder.getOrCreate()
                self._online_reader = self.online_store(
                    spark=spark,
                    result=self.dataframe,
                    write=True,
                    name=name,
                    project=context.project_id,
                )
                if not self.online_watermarks:
                    query = copy.deepcopy(self._online_reader)
                    distinct_dt_df = query.add_sql(
                        "SELECT distinct event_time FROM __THIS__"
                    ).transform(params={"return_as_pandas": True})
                    self.online_watermarks = [
                        datetime.fromisoformat(
                            pendulum.parse(str(f)).to_datetime_string()
                        )
                        for f in list(distinct_dt_df["event_time"].values)
                    ]
                # save online table
                req = OnlineTableRequest(
                    body={
                        "name": name,
                        "description": self.description,
                        "entity": self.lookup_key.entity_id,
                        "online_store": self.online_store,
                        "feature_lookups": self.lookups,
                        "ttl": self.ttl,
                        "watermarks": self.online_watermarks,
                    }
                )
                self.online_table_id = req.save()
            self.dataframe = None
        else:
            self.description = online_table.description
            self.author = online_table.author
            # create lookups
            fg_ids = OnlineTableRequest.get_feature_group_from_online_table(
                online_table.id
            )
            if fg_ids is None:
                self.lookups = None
            else:
                lookups = []
                for i in fg_ids:
                    fg = FeatureGroupRequest.select_by_id(i.feature_group_id)
                    lookups.append(
                        FeatureLookup(
                            source=FeatureGroup(name=fg.name).get_or_create(),
                            features=i.features,
                            exclude_features=i.exclude_features,
                        )
                    )
                self.lookups = lookups
            online_store = OnlineTableRequest.get_online_store_by_id(
                online_table.online_store_id
            )
            self.online_store = OnlineStore(
                kind=OnlineStoreEnum(online_store.kind),
                value=online_store.params if online_store.params != "null" else None,
                name=online_store.name,
            )

            if online_table.delete_at is not None:
                if online_table.delete_at <= datetime.now():
                    if self.lookups is not None:
                        logger.warning(
                            "Refreshing online_table since it is expired based specified ttl."
                        )
                        dataframe = (
                            HistoricalFeatures(lookups=self.lookups)
                            .using_latest()
                            .to_dataframe()
                        )
                        self._online_reader = self.online_store(
                            result=dataframe,
                            write=True,
                            name=name,
                            project=context.project_id,
                        )
                        query = copy.deepcopy(self._online_reader)
                        distinct_dt_df = query.add_sql(
                            "SELECT distinct event_time FROM __THIS__"
                        ).transform(params={"return_as_pandas": True})
                        self.online_watermarks = [
                            datetime.fromisoformat(
                                pendulum.parse(str(f)).to_datetime_string()
                            )
                            for f in list(distinct_dt_df["event_time"].values)
                        ]
                        # refresh latest watermarks
                        OnlineTableRequest.delete_online_watermarks(online_table.id)
                        OnlineTableRequest.add_online_watermarks(
                            online_table.id, self.online_watermarks
                        )
                        OnlineTableRequest.update_delete_at(online_table.id, self.ttl)

                    else:
                        logger.warning(
                            "Online table is expired. Data in the table might not reflected the most recent."
                        )
                        self._online_reader = self.online_store(
                            spark=None,
                            result=None,
                            write=False,
                            name=name,
                            project=context.project_id,
                        )
                else:
                    self._online_reader = self.online_store(
                        spark=None,
                        result=None,
                        write=False,
                        name=name,
                        project=context.project_id,
                    )
            else:
                self._online_reader = self.online_store(
                    spark=None,
                    result=None,
                    write=False,
                    name=name,
                    project=context.project_id,
                )
            self.online_table_id = online_table.id

    def get_features(
        self,
        keys: Union[List[dict], List[Entity]],
        filter: Optional[str] = None,
        drop_event_time: bool = True,
    ):
        if self._online_reader is None:
            raise ValueError("Must be served first.")
        keys_str = []
        for i in keys:
            if isinstance(i, dict):
                for k, v in i.items():
                    # Validate column name and value to prevent SQL injection
                    validate_column_name(k)
                    validate_sql_value(str(v), value_type="key value")
                    keys_str.append(f"{k}='{v}'")
            elif isinstance(i, Entity):
                if i.name != self.lookup_key.name:
                    raise ValueError("Provided entity is not correct.")
                for k, v in i.key_values.items():
                    # Validate column name and value to prevent SQL injection
                    validate_column_name(k)
                    validate_sql_value(str(v), value_type="key value")
                    keys_str.append(f"{k}='{v}'")
        keys_stm = " AND ".join(keys_str)
        if filter is not None:
            keys_stm = keys_stm + " AND " + filter
        query = "SELECT * FROM __THIS__ WHERE {}".format(keys_stm)
        res = (
            self._online_reader.add_sql(query)
            .transform(params={"return_as_pandas": True})
            .drop("__pk__", axis=1)
        )
        if drop_event_time:
            res = res.drop("event_time", axis=1)
        return json.loads(res.to_json(orient="records"))

    def delete(self):
        try:
            self.online_store.delete(name=self.name, project=context.project_id)
        except Exception as e:
            print(e)
            logger.error("Failed to delete online table.")
        OnlineTableRequest.delete_by_id(self.online_table_id)
        OnlineTableRequest.delete_online_watermarks(self.online_table_id)
        OnlineTableRequest.delete_feature_group_from_online_table(self.online_table_id)
        return True
