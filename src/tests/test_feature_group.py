from datetime import datetime, timedelta

import pytest
from seeknal.entity import Entity
from seeknal.featurestore.feature_group import (
    FeatureGroup,
    Materialization,
    OfflineMaterialization,
    OfflineStore,
    OfflineStoreEnum,
    FeatureStoreFileOutput,
    OnlineStore,
    OnlineStoreEnum,
    HistoricalFeatures,
    FeatureLookup,
    FillNull,
    GetLatestTimeStrategy,
    OnlineFeatures,
)

from seeknal.flow import *
from seeknal.featurestore.featurestore import Feature
from seeknal.project import Project
from seeknal.workspace import Workspace
from pyspark import SparkContext
from pyspark.sql import DataFrame
import pandas as pd

from .conftest import are_equal, file_from_string


@pytest.fixture(scope="module")
def input_data_spark(spark):
    comm_day = spark.read.format("parquet").load("tests/data/feateng_comm_day")
    comm_day.write.saveAsTable("comm_day")


def test_feature_group(spark, input_data_spark):
    # create a flow

    Project(name="my_project").get_or_create()
    flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="comm_day")
    my_flow = Flow(
        input=flow_input, tasks=None, output=FlowOutput(), name="my_flow_for_fg"
    )

    my_fg = FeatureGroup(
        name="comm_day",
        entity=Entity(name="msisdn", join_keys=["msisdn"]).get_or_create(),
        materialization=Materialization(event_time_col="day"),
    ).set_flow(my_flow)
    my_fg.set_features()
    # print(my_fg)
    my_fg.get_or_create()


def test_create_feature_group_without_flow(input_data_spark, spark):
    Project(name="my_project").get_or_create()
    input_df = spark.read.table("comm_day")
    my_fg = FeatureGroup(
        name="comm_day_four",
        entity=Entity(name="msisdn", join_keys=["msisdn"]).get_or_create(),
        materialization=Materialization(event_time_col="day"),
    ).set_dataframe(dataframe=input_df)

    my_fg.set_features()
    my_fg.get_or_create()


def test_write_feature_group_without_flow(input_data_spark, spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day_four").get_or_create()
    my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
        feature_start_time=datetime(2019, 3, 5)
    )

def test_update_feature_group(input_data_spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day").get_or_create()

def test_write_feature_group(input_data_spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day").get_or_create()
    my_fg.write(feature_start_time=datetime(2019, 3, 5))

def test_write_feature_group_without_event_time(input_data_spark):
    Project(name="my_project").get_or_create()
    flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="comm_day")
    my_flow = Flow(
        input=flow_input, tasks=None, output=FlowOutput(), name="my_flow_for_fg_two"
    ).get_or_create()

    my_fg = FeatureGroup(
        name="comm_day_two",
        entity=Entity(name="msisdn", join_keys=["msisdn"]).get_or_create(),
    ).set_flow(my_flow)
    my_fg.set_features()
    my_fg.get_or_create()
    my_fg.write()

def test_write_feature_group_to_path(input_data_spark, spark):
    Project(name="my_project").get_or_create()
    materialization = Materialization(event_time_col="day", 
    offline_materialization=OfflineMaterialization(
    store=OfflineStore(kind=OfflineStoreEnum.FILE, 
                       name="test_offline_store",
                       value=FeatureStoreFileOutput(path="file:///tmp/offline_store")), 
                       mode="overwrite", ttl=None),
    offline=True)
    input_df = spark.read.table("comm_day")
    my_fg = FeatureGroup(
        name="test_write_fg",
        entity=Entity(name="msisdn", join_keys=["msisdn"]).get_or_create(),
        materialization=materialization,
    ).set_dataframe(dataframe=input_df)

    my_fg.set_features()
    my_fg.get_or_create()
    my_fg.set_dataframe(dataframe=input_df).write(
        feature_start_time=datetime(2019, 3, 5)
    )
    my_fg.delete()

def test_load_historical_features(input_data_spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day")
    fs = FeatureLookup(source=my_fg)
    fillnull = FillNull(value="0.0", dataType="double")
    hist = HistoricalFeatures(lookups=[fs], fill_nulls=[fillnull])
    df = hist.to_dataframe(feature_start_time=datetime(2019, 3, 5))
    df.show()


def test_load_with_spine(input_data_spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day")
    fs = FeatureLookup(source=my_fg)

    hist = HistoricalFeatures(lookups=[fs])
    spine_dummy_data = pd.DataFrame(
        [
            {"msisdn": "011ezY2Kjs", "app_date": "2019-03-19", "label": 1},
            {"msisdn": "01ViZtJZCj", "app_date": "2019-03-10", "label": 0},
        ]
    )
    df = hist.using_spine(
        spine=spine_dummy_data, date_col="app_date", keep_cols=["label"]
    ).to_dataframe()
    df.show()


def test_load_with_latest(input_data_spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day")
    my_fg_two = FeatureGroup(name="comm_day_two")
    fs = FeatureLookup(source=my_fg)
    fs_two = FeatureLookup(source=my_fg_two)

    hist = HistoricalFeatures(lookups=[fs, fs_two])
    df = hist.using_latest(
        fetch_strategy=GetLatestTimeStrategy.REQUIRE_ANY
    ).to_dataframe()
    df.show()

def test_delete_feature_group(input_data_spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day").get_or_create()
    my_fg.delete()


def test_serve_features(input_data_spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day_four")
    fs = FeatureLookup(source=my_fg)

    user_one = Entity(name="msisdn").get_or_create().set_key_values("05X5wBWKN3")
    fillnull = FillNull(value="0.0", dataType="double")
    hist = HistoricalFeatures(lookups=[fs], fill_nulls=[fillnull])
    hist = hist.using_latest().serve()
    abc = hist.get_features(keys=[user_one])
    print(abc)

def test_feature_group_and_upsert(spark, input_data_spark):
    # create a flow

    Project(name="my_project").get_or_create()
    flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="comm_day")
    my_flow = Flow(
        input=flow_input, tasks=None, output=FlowOutput(), name="my_flow_for_fg_three"
    )

    my_fg = FeatureGroup(
        name="comm_day_three",
        entity=Entity(name="msisdn", join_keys=["msisdn"]).get_or_create(),
        materialization=Materialization(
            event_time_col="day",
            offline_materialization=OfflineMaterialization(
                store=OfflineStore(kind=OfflineStoreEnum.FILE, name="default"),
                mode="merge",
                ttl=None,
            ),
            online=False,
        ),
    ).set_flow(my_flow)
    my_fg.set_features()
    my_fg.get_or_create()

    my_fg.write(
        feature_start_time=datetime(2019, 3, 6), feature_end_time=datetime(2019, 3, 7)
    )

    my_fg.write(
        feature_start_time=datetime(2019, 3, 7), feature_end_time=datetime(2019, 3, 9)
    )
    print(my_fg.get_or_create().offline_watermarks)
    assert sorted(set(my_fg.get_or_create().offline_watermarks)) == [
        '2019-03-06 00:00:00', '2019-03-07 00:00:00', '2019-03-08 00:00:00',
        '2019-03-09 00:00:00'
    ]

def test_load_offline_store():
    Project(name="my_project").get_or_create()
    my_offline_store = OfflineStore(name="my_offline_store", kind=OfflineStoreEnum.FILE)
    my_offline_store.get_or_create()
    print(my_offline_store)

    OfflineStore.list()

def test_attach_offline_store():
    Project(name="my_project").get_or_create()
    offline_store = OfflineStore(
        name="my_offline_store", kind=OfflineStoreEnum.FILE
    ).get_or_create()
    # return to default offline store
    Workspace().attach_offline_store(offline_store)
    default_offline_store = OfflineStore(
        name="default",
    ).get_or_create()
    Workspace().attach_offline_store(default_offline_store)

def test_update_materialization(input_data_spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day_three").get_or_create()

    my_fg.update_materialization(
        offline_materialization=OfflineMaterialization(mode="merge", ttl=2)
    )

def test_serve_with_path(input_data_spark, spark):

    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day_four").get_or_create()
    my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
        feature_start_time=datetime(2019, 3, 5)
    )    
    fs = FeatureLookup(source=my_fg)

    user_one = Entity(name="msisdn").get_or_create().set_key_values("05X5wBWKN3")
    fillnull = FillNull(value="0.0", dataType="double")
    hist = HistoricalFeatures(lookups=[fs], fill_nulls=[fillnull])
    online_store = OnlineStore(value=FeatureStoreFileOutput(path="/tmp/online_store"))
    online_table = hist.using_latest().serve(
        target=online_store, ttl=timedelta(minutes=1)
    )
    print(online_table.get_features(keys=[user_one]))

def test_load_online_features_from_hist(input_data_spark, spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day_four").get_or_create()
    my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
        feature_start_time=datetime(2019, 3, 5)
    )
    fs = FeatureLookup(source=my_fg)
    online_table = OnlineFeatures(
        lookups=[fs], lookup_key=Entity(name="msisdn").get_or_create()
    )
    abc = online_table.get_features(keys=[{"msisdn": "05X5wBWKN3"}])
    print(abc)

def test_load_online_features(input_data_spark):
    Project(name="my_project").get_or_create()
    online_table = OnlineFeatures(
        name="74b9921a61b3512faae0db2f0b60c3f5",
        lookup_key=Entity(name="msisdn").get_or_create(),
    )
    abc = online_table.get_features(keys=[{"msisdn": "05X5wBWKN3"}])
    print(abc)

def test_create_online_table(input_data_spark, spark):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day_four").get_or_create()
    my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
        feature_start_time=datetime(2019, 3, 5)
    )
    fs = FeatureLookup(source=my_fg)
    user_one = Entity(name="msisdn").get_or_create().set_key_values("05X5wBWKN3")
    fillnull = FillNull(value="0.0", dataType="double")
    hist = HistoricalFeatures(lookups=[fs], fill_nulls=[fillnull])
    online_store = OnlineStore(value=FeatureStoreFileOutput(path="/tmp/online_store"))
    hist.using_latest().serve(
        name="my_online_table", target=online_store
    )
    online_table = OnlineFeatures(
        name="my_online_table",
        lookup_key=Entity(name="msisdn").get_or_create()
    )
    abc = online_table.get_features(keys=[{"msisdn": "05X5wBWKN3"}])
    print(abc)

def test_delete_online_table(input_data_spark):
    Project(name="my_project").get_or_create()
    online_table = OnlineFeatures(
        name="my_online_table",
        lookup_key=Entity(name="msisdn").get_or_create(),
    )
    online_table.delete()

    online_table = OnlineFeatures(
        name="74b9921a61b3512faae0db2f0b60c3f5",
        lookup_key=Entity(name="msisdn").get_or_create(),
    )
    online_table.delete()
