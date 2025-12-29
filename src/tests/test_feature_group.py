from datetime import datetime, timedelta
import logging

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

def test_write_feature_group_to_path(input_data_spark, spark, secure_temp_dir):
    Project(name="my_project").get_or_create()
    offline_store_path = secure_temp_dir.mkdir("offline_store")
    materialization = Materialization(event_time_col="day",
    offline_materialization=OfflineMaterialization(
    store=OfflineStore(kind=OfflineStoreEnum.FILE,
                       name="test_offline_store",
                       value=FeatureStoreFileOutput(path=f"file://{offline_store_path}")),
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

def test_serve_with_path(input_data_spark, spark, secure_temp_dir):

    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day_four").get_or_create()
    my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
        feature_start_time=datetime(2019, 3, 5)
    )
    fs = FeatureLookup(source=my_fg)

    user_one = Entity(name="msisdn").get_or_create().set_key_values("05X5wBWKN3")
    fillnull = FillNull(value="0.0", dataType="double")
    hist = HistoricalFeatures(lookups=[fs], fill_nulls=[fillnull])
    online_store_path = secure_temp_dir.mkdir("online_store")
    online_store = OnlineStore(value=FeatureStoreFileOutput(path=online_store_path))
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

def test_create_online_table(input_data_spark, spark, secure_temp_dir):
    Project(name="my_project").get_or_create()
    my_fg = FeatureGroup(name="comm_day_four").get_or_create()
    my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
        feature_start_time=datetime(2019, 3, 5)
    )
    fs = FeatureLookup(source=my_fg)
    user_one = Entity(name="msisdn").get_or_create().set_key_values("05X5wBWKN3")
    fillnull = FillNull(value="0.0", dataType="double")
    hist = HistoricalFeatures(lookups=[fs], fill_nulls=[fillnull])
    online_store_path = secure_temp_dir.mkdir("online_store")
    online_store = OnlineStore(value=FeatureStoreFileOutput(path=online_store_path))
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


class TestFeatureGroupValidation:
    """Tests for input validation in feature group operations."""

    # Test OnlineFeatures.get_features() column name validation
    def test_get_features_valid_column_name(self, input_data_spark, spark):
        """Test that valid column names are accepted in get_features()."""
        Project(name="my_project").get_or_create()
        my_fg = FeatureGroup(name="comm_day_four").get_or_create()
        my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
            feature_start_time=datetime(2019, 3, 5)
        )
        fs = FeatureLookup(source=my_fg)
        online_table = OnlineFeatures(
            lookups=[fs], lookup_key=Entity(name="msisdn").get_or_create()
        )
        # Valid column name - should not raise
        result = online_table.get_features(keys=[{"msisdn": "05X5wBWKN3"}])
        assert isinstance(result, list)

    def test_get_features_invalid_column_name_with_hyphen(self, input_data_spark, spark):
        """Test that column names with hyphens are rejected in get_features()."""
        from seeknal.exceptions import InvalidIdentifierError

        Project(name="my_project").get_or_create()
        my_fg = FeatureGroup(name="comm_day_four").get_or_create()
        my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
            feature_start_time=datetime(2019, 3, 5)
        )
        fs = FeatureLookup(source=my_fg)
        online_table = OnlineFeatures(
            lookups=[fs], lookup_key=Entity(name="msisdn").get_or_create()
        )
        # Invalid column name with hyphen - should raise InvalidIdentifierError
        with pytest.raises(InvalidIdentifierError) as exc_info:
            online_table.get_features(keys=[{"invalid-column": "value"}])
        assert "column name" in str(exc_info.value)
        assert "invalid characters" in str(exc_info.value)

    def test_get_features_invalid_column_name_with_space(self, input_data_spark, spark):
        """Test that column names with spaces are rejected in get_features()."""
        from seeknal.exceptions import InvalidIdentifierError

        Project(name="my_project").get_or_create()
        my_fg = FeatureGroup(name="comm_day_four").get_or_create()
        my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
            feature_start_time=datetime(2019, 3, 5)
        )
        fs = FeatureLookup(source=my_fg)
        online_table = OnlineFeatures(
            lookups=[fs], lookup_key=Entity(name="msisdn").get_or_create()
        )
        # Invalid column name with space - should raise InvalidIdentifierError
        with pytest.raises(InvalidIdentifierError) as exc_info:
            online_table.get_features(keys=[{"invalid column": "value"}])
        assert "column name" in str(exc_info.value)

    def test_get_features_sql_injection_in_column_name(self, input_data_spark, spark):
        """Test that SQL injection attempts via column names are blocked."""
        from seeknal.exceptions import InvalidIdentifierError

        Project(name="my_project").get_or_create()
        my_fg = FeatureGroup(name="comm_day_four").get_or_create()
        my_fg.set_dataframe(dataframe=spark.read.table("comm_day")).write(
            feature_start_time=datetime(2019, 3, 5)
        )
        fs = FeatureLookup(source=my_fg)
        online_table = OnlineFeatures(
            lookups=[fs], lookup_key=Entity(name="msisdn").get_or_create()
        )
        # SQL injection attempt via column name - should raise InvalidIdentifierError
        with pytest.raises(InvalidIdentifierError):
            online_table.get_features(keys=[{"col'; DROP TABLE --": "value"}])


class TestOfflineStorePathSecurity:
    """Tests for OfflineStore path security validation."""

    def test_file_kind_with_insecure_dict_path_logs_warning(self, caplog):
        """OfflineStore should log a warning for /tmp paths passed as dict."""
        with caplog.at_level(logging.WARNING):
            store = OfflineStore(
                kind=OfflineStoreEnum.FILE,
                value={"path": "/tmp/offline_data", "kind": "delta"}
            )

        assert "Security Warning" in caplog.text
        assert "/tmp/offline_data" in caplog.text
        assert "offline store" in caplog.text
        # Should still create the object
        assert store.value["path"] == "/tmp/offline_data"

    def test_file_kind_with_insecure_json_path_logs_warning(self, caplog):
        """OfflineStore should log a warning for /tmp paths passed as JSON string."""
        import json
        with caplog.at_level(logging.WARNING):
            store = OfflineStore(
                kind=OfflineStoreEnum.FILE,
                value=json.dumps({"path": "/var/tmp/data", "kind": "delta"})
            )

        assert "Security Warning" in caplog.text
        assert "/var/tmp/data" in caplog.text

    def test_file_kind_with_secure_path_no_warning(self, caplog, tmp_path):
        """OfflineStore should not log warning for secure paths."""
        secure_path = str(tmp_path / "offline_data")
        with caplog.at_level(logging.WARNING):
            store = OfflineStore(
                kind=OfflineStoreEnum.FILE,
                value={"path": secure_path, "kind": "delta"}
            )

        assert "Security Warning" not in caplog.text
        assert store.value["path"] == secure_path

    def test_hive_table_kind_no_validation(self, caplog):
        """OfflineStore with HIVE_TABLE kind should not validate paths."""
        with caplog.at_level(logging.WARNING):
            store = OfflineStore(
                kind=OfflineStoreEnum.HIVE_TABLE,
                value={"database": "my_database"}
            )

        # No security warning for HIVE_TABLE kind
        assert "Security Warning" not in caplog.text

    def test_file_kind_with_none_value_no_error(self, caplog):
        """OfflineStore with FILE kind but None value should not error."""
        with caplog.at_level(logging.WARNING):
            store = OfflineStore(kind=OfflineStoreEnum.FILE, value=None)

        # No error, no warning
        assert store.value is None

    def test_file_kind_with_feature_store_file_output_delegates_validation(self, caplog):
        """OfflineStore should delegate validation to FeatureStoreFileOutput."""
        with caplog.at_level(logging.WARNING):
            # FeatureStoreFileOutput validates in its own __post_init__
            store = OfflineStore(
                kind=OfflineStoreEnum.FILE,
                value=FeatureStoreFileOutput(path="/tmp/offline_store")
            )

        # Warning comes from FeatureStoreFileOutput
        assert "Security Warning" in caplog.text
        assert "feature store file output" in caplog.text


class TestOnlineStorePathSecurity:
    """Tests for OnlineStore path security validation."""

    def test_file_kind_with_insecure_dict_path_logs_warning(self, caplog):
        """OnlineStore should log a warning for /tmp paths passed as dict."""
        with caplog.at_level(logging.WARNING):
            store = OnlineStore(
                value={"path": "/tmp/online_data", "kind": "delta"}
            )

        assert "Security Warning" in caplog.text
        assert "/tmp/online_data" in caplog.text
        assert "online store" in caplog.text

    def test_file_kind_with_insecure_json_path_logs_warning(self, caplog):
        """OnlineStore should log a warning for /var/tmp paths passed as JSON string."""
        import json
        with caplog.at_level(logging.WARNING):
            store = OnlineStore(
                value=json.dumps({"path": "/var/tmp/data", "kind": "delta"})
            )

        assert "Security Warning" in caplog.text
        assert "/var/tmp/data" in caplog.text

    def test_file_kind_with_secure_path_no_warning(self, caplog, tmp_path):
        """OnlineStore should not log warning for secure paths."""
        secure_path = str(tmp_path / "online_data")
        with caplog.at_level(logging.WARNING):
            store = OnlineStore(
                value={"path": secure_path, "kind": "delta"}
            )

        assert "Security Warning" not in caplog.text
        assert store.value["path"] == secure_path

    def test_file_kind_with_none_value_no_error(self, caplog):
        """OnlineStore with None value should not error."""
        with caplog.at_level(logging.WARNING):
            store = OnlineStore(value=None)

        # No error, no warning
        assert store.value is None

    def test_file_kind_with_feature_store_file_output_delegates_validation(self, caplog):
        """OnlineStore should delegate validation to FeatureStoreFileOutput."""
        with caplog.at_level(logging.WARNING):
            # FeatureStoreFileOutput validates in its own __post_init__
            store = OnlineStore(
                value=FeatureStoreFileOutput(path="/tmp/online_store")
            )

        # Warning comes from FeatureStoreFileOutput
        assert "Security Warning" in caplog.text
        assert "feature store file output" in caplog.text