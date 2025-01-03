import shutil
from dataclasses import dataclass, asdict, field
from enum import Enum
from typing import Any, Optional, Union
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


class OfflineStoreEnum(str, Enum):
    HIVE_TABLE = "hive_table"
    FILE = "file"


class OnlineStoreEnum(str, Enum):
    HIVE_TABLE = "hive_table"
    FILE = "file"


class FileKindEnum(str, Enum):
    DELTA = "delta"


@dataclass
class FeatureStoreFileOutput:
    path: str
    kind: FileKindEnum = FileKindEnum.DELTA

    def to_dict(self):
        return {"path": self.path, "kind": self.kind.value}


@dataclass
class FeatureStoreHiveTableOutput:
    database: str

    def to_dict(self):
        return {"database": self.database}


@dataclass
class OfflineStore:
    value: Optional[
        Union[str, FeatureStoreFileOutput, FeatureStoreHiveTableOutput]
    ] = None
    kind: OfflineStoreEnum = OfflineStoreEnum.HIVE_TABLE
    name: Optional[str] = None

    def __post_init__(self):
        if self.value is not None:
            self.value = (
                self.value.to_dict() if not isinstance(self.value, str) else self.value
            )
        self.kind = self.kind.value

    def get_or_create(self):
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
            else:
                self.value = value_params
        return self

    @staticmethod
    def list():
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

    def __call__(
        self,
        result: Optional[DataFrame] = None,
        spark: Optional[SparkSession] = None,
        write=True,
        *args: Any,
        **kwds: Any,
    ) -> Any:
        match self.kind:
            case OfflineStoreEnum.HIVE_TABLE:
                if spark is None:
                    spark = SparkSession.builder.getOrCreate()
                if self.value is None:
                    # use default
                    database = "seeknal"
                    table_name = "fg_{}__{}".format(kwds["project"], kwds["entity"])
                else:
                    database = self.value.database
                    table_name = "fg_{}__{}".format(kwds["project"], kwds["entity"])
                start_date = kwds.get("start_date", None)
                end_date = kwds.get("end_date", None)
                if write:
                    name = kwds.get("name")
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
                            raise ValueError(
                                "cannot upsert because table is not exists"
                            )
                        delta_table = DeltaTable.forName(
                            spark, "{}.{}".format(database, table_name)
                        )
                        delta_table.alias("source").merge(
                            result.alias("target"), "source.__pk__ = target.__pk__"
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
                if self.value is None:
                    # use default
                    base_path = CONFIG_BASE_URL
                    table_name = "fg_{}__{}".format(kwds["project"], kwds["entity"])
                    path = os.path.join(base_path, "data", table_name)
                else:
                    if isinstance(self.value, FeatureStoreFileOutput):
                        base_path = self.value.path
                    else:
                        base_path = self.value["path"]
                    table_name = "fg_{}__{}".format(kwds["project"], kwds["entity"])
                    path = os.path.join(base_path, table_name)
                start_date = kwds.get("start_date", None)
                end_date = kwds.get("end_date", None)
                if write:
                    name = kwds.get("name")
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
                            raise ValueError(
                                "cannot upsert because table is not exists"
                            )
                        delta_table = DeltaTable.forPath(spark, path)
                        delta_table.alias("source").merge(
                            result.alias("target"), "source.__pk__ = target.__pk__"
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
            case None:
                return result
        return None

    def delete(
        self, spark: Optional[SparkSession] = None, *args: Any, **kwds: Any
    ) -> Any:
        match self.kind:
            case OfflineStoreEnum.HIVE_TABLE:
                if spark is None:
                    spark = SparkSession.builder.getOrCreate()
                name = kwds.get("name")
                if self.value is None:
                    database = "seeknal"
                else:
                    database = self.value.database
                try:
                    delta_table = DeltaTable.forName(
                        spark,
                        "{}.fg_{}_{}".format(database, kwds["project"], kwds["entity"]),
                    )
                    delta_table.delete(f"name = '{name}'")
                except:
                    logger.error("Table is not a delta table.")
                return True
            case OfflineStoreEnum.FILE:
                if spark is None:
                    spark = SparkSession.builder.getOrCreate()
                name = kwds.get("name")
                if self.value == "null" or self.value is None:
                    base_path = CONFIG_BASE_URL
                    table_name = "fg_{}__{}".format(kwds["project"], kwds["entity"])
                    path = os.path.join(base_path, "data", table_name)
                else:
                    if isinstance(self.value, FeatureStoreFileOutput):
                        base_path = self.value.path
                    else:
                        base_path = self.value["path"]
                    table_name = "fg_{}__{}".format(kwds["project"], kwds["entity"])
                    path = os.path.join(base_path, table_name)
                try:
                    delta_table = DeltaTable.forPath(spark, path)
                    delta_table.delete(f"name = '{name}'")
                except:
                    logger.error("Table not found")
            case None:
                return False
        return False


@dataclass
class OnlineStore:
    value: Optional[
        Union[str, FeatureStoreFileOutput, FeatureStoreHiveTableOutput]
    ] = None
    kind: OnlineStoreEnum = OnlineStoreEnum.FILE
    name: Optional[str] = None

    def __post_init__(self):
        if self.value is not None:
            self.value = (
                self.value.to_dict() if not isinstance(self.value, str) else self.value
            )
        self.kind = self.kind.value

    def __call__(
        self,
        result: Optional[Union[DataFrame, pd.DataFrame]] = None,
        spark: Optional[SparkSession] = None,
        write=True,
        *args: Any,
        **kwargs: Any,
    ):
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
    """
    List columns that want the nulls to be filled
    Attributes:
        value (str): value for filling the nulls
        dataType (str): data type for the correspond value to specific datatype
        columns (List[str], optional): Determines which columns to be filled the nulls
    """

    value: str
    dataType: str
    columns: Optional[List[str]] = None

    def to_dict(self):
        return {k: v for k, v in asdict(self).items() if v is not None}


class OfflineMaterialization(BaseModel):
    store: Optional[OfflineStore] = None
    mode: str = "overwrite"
    ttl: Optional[int] = None


class OnlineMaterialization(BaseModel):
    """
    Online materialization options

    Attributes:
        serving_ttl_days (int, optional): Look back window for features defined at the online-store.
            This parameters determines how long features will live in the online store. The unit is in days.
            Shorter TTLs improve performance and reduce computation. Default to 1.
            For example, if we set TTLs as 1 then only one day data available in online-store
        force_update (bool, optional): force to update the data in online-store without considering
            the data that going materialized is newer than the data that already stored in online-store.
            Default to False.
    """

    store: OnlineStore = OnlineStore()
    ttl: Optional[int] = 1440

    #class Config:
    #    use_enum_values = True
    model_config = {
        "use_enum_values": True
    }


class Feature(BaseModel):
    """
    Define Feature

    Attributes:
        name (str): name of feature
        feature_id (str, optional): feature id. Should not define by user but by seeknal.
        description (str, optional): description of the feature
        data_type (str, optional): data type for the feature. If None, then
            it will expect `set_features()` method to run for getting the correct
            data type.
        online_data_type (str, optional): data type when stored in online-store. If None,
            it will use data_type
        created_at (str, optional): when the feature created
        updated_at (str, optional): when the feature updated
    """

    name: str
    feature_id: Optional[str] = None
    description: Optional[str] = None
    data_type: Optional[str] = None
    online_data_type: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self):
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

    name: str
    entity: Optional[Entity] = None
    id: Optional[str] = None

    @abstractmethod
    def get_or_create(self):
        pass
