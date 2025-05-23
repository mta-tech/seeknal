import pytest
from unittest import mock
import os
import shutil
import json
from datetime import datetime, date
import pendulum

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType
from delta.tables import DeltaTable

from seeknal.featurestore.featurestore import (
    OfflineStore,
    OnlineStore,
    Feature,
    FeatureStoreFileOutput,
    FeatureStoreHiveTableOutput,
    OfflineStoreEnum,
    OnlineStoreEnum,
    FeatureStoreType,
    StoreType, # Assuming this is an alias or will be used
)
from seeknal.api_request.rest_api_request import FeatureGroupRequest # Assuming this is the correct import path
from seeknal.context import SETTINGS, CONFIG_BASE_URL
from seeknal.tasks.duckdb_task import DuckDBTask # For OnlineStore read
from seeknal.utils.utils import camel_to_snake # For default naming if needed

# Default project and entity for testing default paths
TEST_PROJECT_NAME = "test_project"
TEST_ENTITY_NAME = "test_entity"
TEST_FEATURE_GROUP_NAME = "test_feature_group"

# Fixtures
@pytest.fixture(scope="session")
def spark_session():
    """pytest fixture for creating a SparkSession."""
    spark = (
        SparkSession.builder.appName("pytest-spark-session")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.master", "local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-pytest") # For Hive table tests
        .enableHiveSupport() # Enable Hive support for table operations
        .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture
def mock_settings(mocker):
    """Fixture to manage SETTINGS, ensuring a clean state for each test and mocking CONFIG_BASE_URL."""
    original_settings = SETTINGS.copy()
    original_config_base_url = CONFIG_BASE_URL
    
    mocker.patch.dict(SETTINGS, {"project_id": "test_proj_id_settings"}) # Example default setting
    mocker.patch('seeknal.featurestore.featurestore.CONFIG_BASE_URL', 'file:///tmp/seeknal_data_pytest')

    yield SETTINGS

    SETTINGS.clear()
    SETTINGS.update(original_settings)
    # Restore CONFIG_BASE_URL if it was changed by a test (though it's mocked here)
    # This is more for completeness if tests were to modify it directly via the module path
    mocker.patch('seeknal.featurestore.featurestore.CONFIG_BASE_URL', original_config_base_url)


@pytest.fixture
def mock_dataframe(spark_session):
    """Creates a mock Spark DataFrame for testing."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("event_date", DateType(), True),
        StructField("update_ts", TimestampType(), True)
    ])
    data = [
        (1, "record1", date(2023, 1, 1), datetime(2023, 1, 1, 12, 0, 0)),
        (2, "record2", date(2023, 1, 15), datetime(2023, 1, 15, 12, 0, 0)),
        (3, "record3", date(2023, 2, 1), datetime(2023, 2, 1, 12, 0, 0)),
    ]
    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def mock_pandas_dataframe():
    """Creates a mock Pandas DataFrame for testing OnlineStore file writes."""
    try:
        import pandas as pd
        return pd.DataFrame({
            "id": [10, 20],
            "feature_value": ["val1", "val2"]
        })
    except ImportError:
        return None # Indicate pandas is not available


# Mock for FeatureGroupRequest
@pytest.fixture
def mock_fgr_get_offline_store(mocker):
    return mocker.patch.object(FeatureGroupRequest, 'get_offline_store_by_name')

@pytest.fixture
def mock_fgr_save_offline_store(mocker):
    return mocker.patch.object(FeatureGroupRequest, 'save_offline_store')

# Hypothetical mocks for OnlineStore - assuming similar FeatureGroupRequest interaction
@pytest.fixture
def mock_fgr_get_online_store(mocker):
    # This might need to be adapted if OnlineStore uses a different mechanism
    return mocker.patch.object(FeatureGroupRequest, 'get_online_store_by_name') 

@pytest.fixture
def mock_fgr_save_online_store(mocker):
    return mocker.patch.object(FeatureGroupRequest, 'save_online_store')


# Mocks for Spark DataFrame operations
@pytest.fixture
def mock_spark_df_writer(mocker):
    mock_writer = mocker.MagicMock()
    # Fluent interface for writer
    mock_writer.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.options.return_value = mock_writer
    mock_writer.partitionBy.return_value = mock_writer
    mock_writer.saveAsTable.return_value = None 
    mock_writer.save.return_value = None
    return mock_writer

@pytest.fixture
def mock_spark_df_read(mocker):
    mock_reader = mocker.MagicMock()
    mock_reader.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.options.return_value = mock_reader
    mock_reader.table.return_value = mocker.MagicMock(spec=DataFrame) # a mock DataFrame
    mock_reader.load.return_value = mocker.MagicMock(spec=DataFrame) # a mock DataFrame
    return mock_reader


@pytest.fixture
def mock_delta_table(mocker):
    mock_dt = mocker.MagicMock(spec=DeltaTable)
    mock_dt.alias.return_value = mock_dt # For merge
    mock_dt.merge.return_value = mock_dt # For merge
    mock_dt.whenMatchedUpdateAll.return_value = mock_dt # For merge
    mock_dt.whenNotMatchedInsertAll.return_value = mock_dt # For merge
    mock_dt.execute.return_value = None # For merge
    mock_dt.delete.return_value = None # For delete operations
    return mock_dt

@pytest.fixture
def mock_spark_catalog(mocker, spark_session):
    # spark_session.catalog is a real object. We can mock its methods.
    return mocker.patch.object(spark_session.catalog, 'tableExists')


# Test for Feature Class
class TestFeature:
    def test_feature_to_dict_minimal(self):
        """Test Feature.to_dict() with only required fields."""
        feature = Feature(name="test_feature", dtype="int")
        expected = {
            "name": "test_feature",
            "dtype": "int",
            "description": None,
            "keys": None,
            "tags": None,
        }
        assert feature.to_dict() == expected

    def test_feature_to_dict_all_fields(self):
        """Test Feature.to_dict() with all fields populated."""
        feature = Feature(
            name="user_clicks",
            dtype="long",
            description="Total clicks by user.",
            keys=["user_id"],
            tags={"source": "web", "version": "1"},
        )
        expected = {
            "name": "user_clicks",
            "dtype": "long",
            "description": "Total clicks by user.",
            "keys": ["user_id"],
            "tags": {"source": "web", "version": "1"},
        }
        assert feature.to_dict() == expected

# Helper function to get base data path for tests
def get_base_data_path():
    return CONFIG_BASE_URL.replace('file://', '')


# Tests for OfflineStore
class TestOfflineStore:

    def test_offline_store_post_init_default_kind_and_value(self):
        """Test OfflineStore __post_init__ with default kind and None value."""
        store = OfflineStore(name="test_store")
        assert store.kind == OfflineStoreEnum.HIVE_TABLE.value
        assert store.value is None # Default value should be None

    def test_offline_store_post_init_file_kind_feature_store_output(self):
        """Test OfflineStore __post_init__ with FILE kind and FeatureStoreFileOutput."""
        file_output = FeatureStoreFileOutput(path="/my/path", format="delta")
        store = OfflineStore(name="test_store", kind=OfflineStoreEnum.FILE, value=file_output)
        assert store.kind == OfflineStoreEnum.FILE.value
        assert isinstance(store.value, dict)
        assert store.value["path"] == "/my/path"
        assert store.value["format"] == "delta"

    def test_offline_store_post_init_hive_kind_feature_store_output(self):
        """Test OfflineStore __post_init__ with HIVE_TABLE kind and FeatureStoreHiveTableOutput."""
        hive_output = FeatureStoreHiveTableOutput(database="mydb", table="mytable", format="delta")
        store = OfflineStore(name="test_store", kind=OfflineStoreEnum.HIVE_TABLE, value=hive_output)
        assert store.kind == OfflineStoreEnum.HIVE_TABLE.value
        assert isinstance(store.value, dict)
        assert store.value["database"] == "mydb"
        assert store.value["table"] == "mytable"

    def test_offline_store_post_init_value_already_dict(self):
        """Test OfflineStore __post_init__ when value is already a dictionary."""
        val_dict = {"path": "/another/path", "format": "parquet"}
        store = OfflineStore(name="test_store", kind=OfflineStoreEnum.FILE, value=val_dict)
        assert store.kind == OfflineStoreEnum.FILE.value
        assert store.value == val_dict


    def test_get_or_create_store_does_not_exist(self, mock_settings, mock_fgr_get_offline_store, mock_fgr_save_offline_store):
        """Test get_or_create when the offline store does not exist."""
        mock_fgr_get_offline_store.return_value = None
        mock_save_return = mock.MagicMock()
        mock_save_return.id = "new_store_id_123"
        mock_fgr_save_offline_store.return_value = mock_save_return

        store = OfflineStore(name="new_offline_store", kind=OfflineStoreEnum.FILE, value={"path": "/data"})
        store.project_name = TEST_PROJECT_NAME # Set for the call

        result_store = store.get_or_create()

        mock_fgr_get_offline_store.assert_called_once_with(name="new_offline_store", project_name=TEST_PROJECT_NAME)
        expected_params_json = json.dumps({"path": "/data"})
        mock_fgr_save_offline_store.assert_called_once_with(
            name="new_offline_store",
            kind=OfflineStoreEnum.FILE.value,
            project_name=TEST_PROJECT_NAME,
            params=expected_params_json,
            description=None # Assuming description is not set for this test
        )
        assert store.id == "new_store_id_123"
        assert result_store is store

    def test_get_or_create_store_exists(self, mock_settings, mock_fgr_get_offline_store, mock_fgr_save_offline_store):
        """Test get_or_create when the offline store already exists."""
        mock_existing_store_data = mock.MagicMock()
        mock_existing_store_data.id = "existing_id_456"
        mock_existing_store_data.name = "existing_store" # Name from DB
        mock_existing_store_data.kind = OfflineStoreEnum.HIVE_TABLE.value
        mock_existing_store_data.params = json.dumps({"database": "db_from_server", "table": "tbl_from_server"})
        mock_existing_store_data.description = "Existing Description"

        mock_fgr_get_offline_store.return_value = mock_existing_store_data

        store = OfflineStore(name="existing_store") # Initial name
        store.project_name = TEST_PROJECT_NAME

        result_store = store.get_or_create()

        mock_fgr_get_offline_store.assert_called_once_with(name="existing_store", project_name=TEST_PROJECT_NAME)
        mock_fgr_save_offline_store.assert_not_called()
        
        assert store.id == "existing_id_456"
        assert store.name == "existing_store" # Name should be updated if different, though same here
        assert store.kind == OfflineStoreEnum.HIVE_TABLE.value
        assert store.value == {"database": "db_from_server", "table": "tbl_from_server"}
        assert store.description == "Existing Description"
        assert result_store is store

    # --- Tests for OfflineStore.__call__(write=True) ---

    @pytest.mark.parametrize("mode", ["append", "overwrite", "merge"])
    def test_offline_store_call_write_hive_table_default_db(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings, mode):
        """Test OfflineStore write to HIVE_TABLE with default DB and different modes."""
        store = OfflineStore(name=TEST_FEATURE_GROUP_NAME, kind=OfflineStoreEnum.HIVE_TABLE)
        store.project_name = TEST_PROJECT_NAME # Set for default path generation
        store.value = None # Ensure default DB is used

        # Mock DataFrame's write attribute to return our mock_spark_df_writer
        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer):
            # For merge, mock tableExists
            with mock.patch.object(spark_session.catalog, 'tableExists', return_value=(mode == "merge")) as mock_table_exists, \
                 mock.patch('delta.DeltaTable.forName') as mock_delta_table_for_name:

                if mode == "merge":
                    mock_dt_instance = mock_delta_table_for_name.return_value
                    mock_dt_instance.alias.return_value = mock_dt_instance
                    mock_dt_instance.merge.return_value = mock_dt_instance
                    mock_dt_instance.whenMatchedUpdateAll.return_value = mock_dt_instance
                    mock_dt_instance.whenNotMatchedInsertAll.return_value = mock_dt_instance
                
                kwargs = {
                    "dataframe": mock_dataframe,
                    "entity": TEST_ENTITY_NAME, # For partitionBy
                    "mode": mode,
                    "partition_cols": ["event_date"],
                    "primary_keys": ["id"],
                    "start_date": "2023-01-01" # Required for overwrite/merge with replaceWhere
                }
                if mode != "merge": # start_date not strictly required for append if not using replaceWhere
                     del kwargs["start_date"]


                store(**kwargs)

                expected_table_name = f"seeknal.{TEST_PROJECT_NAME}_{TEST_ENTITY_NAME}_{TEST_FEATURE_GROUP_NAME}"
                
                mock_spark_df_writer.format.assert_called_with("delta")
                mock_spark_df_writer.partitionBy.assert_called_with(["event_date"])

                if mode == "append":
                    mock_spark_df_writer.mode.assert_called_with("append")
                    mock_spark_df_writer.saveAsTable.assert_called_with(expected_table_name)
                elif mode == "overwrite":
                    mock_spark_df_writer.mode.assert_called_with("overwrite")
                    # This is the crucial part for partition overwrite
                    # Assuming start_date is a string 'YYYY-MM-DD'
                    # The actual date column type in DataFrame is DateType, so direct comparison should work.
                    expected_replace_where = "event_date >= '2023-01-01' AND event_date <= '2023-02-01'" # Based on mock_dataframe
                    # We need to calculate the max date from the mock_dataframe for the upper bound if start_date is the only input
                    # For simplicity, let's assume an end_date was implicitly derived or would be passed.
                    # For this test, let's assume it has logic to determine max date in df for replaceWhere.
                    # Or, more realistically, it expects an end_date if a range is to be overwritten.
                    # The original code implies replaceWhere uses start_date and end_date if both available,
                    # or just start_date for a single partition, or a range based on df's min/max if start_date is bool.
                    # Given kwarg start_date="2023-01-01", and mock_dataframe has dates up to "2023-02-01"
                    # Let's assume an implicit end_date or max_date_in_df logic for replaceWhere.
                    # For this test, we'll assume the function correctly determines the range.
                    # A more precise test would mock date range calculation if complex.
                    # For now, let's verify option("replaceWhere", ANY) was called.
                    # A better way would be to have a predictable end_date or mock the date range finding.
                    # Let's refine the test to pass an explicit end_date for overwrite.
                    store(dataframe=mock_dataframe, entity=TEST_ENTITY_NAME, mode="overwrite", 
                          partition_cols=["event_date"], primary_keys=["id"],
                          start_date="2023-01-01", end_date="2023-02-01") # Add end_date
                    
                    mock_spark_df_writer.option.assert_any_call("replaceWhere", "event_date >= '2023-01-01' AND event_date <= '2023-02-01'")
                    mock_spark_df_writer.saveAsTable.assert_called_with(expected_table_name)

                elif mode == "merge":
                    mock_table_exists.assert_called_with(expected_table_name)
                    mock_delta_table_for_name.assert_called_with(spark_session, expected_table_name)
                    
                    # Verify merge condition. Based on primary_keys = ["id"]
                    # The string must match exactly "source.id = target.id" or "target.id = source.id"
                    # It depends on the alias used in the actual implementation.
                    # Let's assume 'target' for DeltaTable and 'source' for the new data.
                    merge_condition = mock_dt_instance.merge.call_args[0][1] # Second arg to merge is condition
                    assert "source.id = target.id" in str(merge_condition) or "target.id = source.id" in str(merge_condition)
                    
                    mock_dt_instance.whenMatchedUpdateAll.assert_called_once()
                    mock_dt_instance.whenNotMatchedInsertAll.assert_called_once()
                    mock_dt_instance.execute.assert_called_once()
                    # saveAsTable should not be called directly in merge if table exists
                    mock_spark_df_writer.saveAsTable.assert_not_called()


    def test_offline_store_call_write_hive_table_custom_db(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings):
        """Test OfflineStore write to HIVE_TABLE with custom DB."""
        custom_value = FeatureStoreHiveTableOutput(database="custom_db", table="custom_table", format="delta")
        store = OfflineStore(name="custom_fg", kind=OfflineStoreEnum.HIVE_TABLE, value=custom_value)
        store.project_name = TEST_PROJECT_NAME

        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer):
            store(dataframe=mock_dataframe, entity="custom_entity", mode="append", partition_cols=["event_date"])

        expected_table_name = "custom_db.custom_table"
        mock_spark_df_writer.format.assert_called_with("delta")
        mock_spark_df_writer.mode.assert_called_with("append")
        mock_spark_df_writer.partitionBy.assert_called_with(["event_date"])
        mock_spark_df_writer.saveAsTable.assert_called_with(expected_table_name)

    def test_offline_store_call_write_file_default_path(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings):
        """Test OfflineStore write to FILE with default path."""
        store = OfflineStore(name=TEST_FEATURE_GROUP_NAME, kind=OfflineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME # For default path
        store.value = None # Ensure default path is used

        base_path = get_base_data_path()
        expected_path = os.path.join(base_path, TEST_PROJECT_NAME, TEST_ENTITY_NAME, TEST_FEATURE_GROUP_NAME)
        
        # Clean up path if it exists from previous runs (important for local testing)
        if os.path.exists(expected_path):
            shutil.rmtree(expected_path)

        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer):
            store(dataframe=mock_dataframe, entity=TEST_ENTITY_NAME, mode="append", partition_cols=["event_date"])

        mock_spark_df_writer.format.assert_called_with("delta")
        mock_spark_df_writer.mode.assert_called_with("append")
        mock_spark_df_writer.partitionBy.assert_called_with(["event_date"])
        mock_spark_df_writer.save.assert_called_with(expected_path)
        
        # Verify directory was created by the function (or Spark) - this is more of an integration check
        # For unit test, we trust Spark's save creates it. If store itself creates it, mock os.makedirs.
        # The provided code doesn't show explicit os.makedirs for FILE kind, Spark handles it.

    def test_offline_store_call_write_file_custom_path(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings):
        """Test OfflineStore write to FILE with custom path."""
        custom_value = FeatureStoreFileOutput(path="/tmp/custom_output_path", format="parquet")
        store = OfflineStore(name="custom_fg_file", kind=OfflineStoreEnum.FILE, value=custom_value)
        
        expected_path = "/tmp/custom_output_path"
        if os.path.exists(expected_path):
            shutil.rmtree(expected_path)

        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer):
            store(dataframe=mock_dataframe, entity="custom_entity_file", mode="overwrite", 
                  partition_cols=["event_date"], start_date="2023-01-01", end_date="2023-02-01") # For replaceWhere

        mock_spark_df_writer.format.assert_called_with("parquet") # Format from custom_value
        mock_spark_df_writer.mode.assert_called_with("overwrite")
        mock_spark_df_writer.option.assert_any_call("replaceWhere", "event_date >= '2023-01-01' AND event_date <= '2023-02-01'")
        mock_spark_df_writer.partitionBy.assert_called_with(["event_date"])
        mock_spark_df_writer.save.assert_called_with(expected_path)

    def test_offline_store_call_write_merge_file_path_does_not_exist(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings):
        """Test OfflineStore write with mode='merge' when file path does not exist (falls back to save)."""
        store = OfflineStore(name=TEST_FEATURE_GROUP_NAME, kind=OfflineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME
        store.value = None # Use default path

        base_path = get_base_data_path()
        expected_path = os.path.join(base_path, TEST_PROJECT_NAME, TEST_ENTITY_NAME, TEST_FEATURE_GROUP_NAME)
        
        if os.path.exists(expected_path): # Clean up before test
            shutil.rmtree(expected_path)

        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer), \
             mock.patch('os.path.exists', return_value=False) as mock_os_exists, \
             mock.patch('delta.DeltaTable.forPath') as mock_delta_for_path: # Should not be called

            store(dataframe=mock_dataframe, entity=TEST_ENTITY_NAME, mode="merge", 
                  partition_cols=["event_date"], primary_keys=["id"])
            
            mock_os_exists.assert_called_with(expected_path) # Checked path existence
            mock_delta_for_path.assert_not_called() # DeltaTable.forPath should not be called

            # Should fall back to a normal save (append typically, or could be overwrite if specified)
            # The implementation detail: if merge target doesn't exist, it does a regular save.
            # The mode for this save might be 'append' or 'overwrite'. Let's assume 'append'.
            # The provided code snippet suggests it will try to create the table, often with 'append' or error if schema not set.
            # For a robust merge fallback, it usually means an initial write.
            # The code snippet has: `df.write.format(fmt).mode(mode).save(path)` if mode is not merge or table not exists.
            # This means it would use mode="merge" in save(), which is invalid for df.write.save().
            # This indicates a potential bug or simplification in the provided snippet.
            # A typical fallback is to perform an initial write, often in "overwrite" or "append" mode.
            # Let's assume it defaults to "append" for the initial save in a merge-like scenario.
            # **Correction**: The original code for merge fallback is:
            # `dataframe.write.format(file_format).mode("overwrite").partitionBy(*partition_cols).save(path_str)`
            # So, it should use "overwrite" for the initial save.
            mock_spark_df_writer.format.assert_called_with("delta")
            mock_spark_df_writer.mode.assert_called_with("overwrite") # Fallback to overwrite
            mock_spark_df_writer.partitionBy.assert_called_with(["event_date"])
            mock_spark_df_writer.save.assert_called_with(expected_path)


    def test_offline_store_call_write_merge_file_path_exists(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings, mock_delta_table):
        """Test OfflineStore write with mode='merge' when file path exists."""
        store = OfflineStore(name=TEST_FEATURE_GROUP_NAME, kind=OfflineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME
        store.value = {"path": "/tmp/test_merge_delta_path", "format": "delta"} # Custom path for isolation

        expected_path = "/tmp/test_merge_delta_path"
        
        # Ensure path exists for DeltaTable.forPath
        os.makedirs(expected_path, exist_ok=True) 
        # Create a dummy _delta_log so DeltaTable.forPath doesn't fail instantly if it checks
        os.makedirs(os.path.join(expected_path, "_delta_log"), exist_ok=True)


        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer), \
             mock.patch('os.path.exists', return_value=True) as mock_os_exists, \
             mock.patch('delta.DeltaTable.forPath', return_value=mock_delta_table) as mock_delta_for_path:

            store(dataframe=mock_dataframe, entity=TEST_ENTITY_NAME, mode="merge", 
                  partition_cols=["event_date"], primary_keys=["id"])

            mock_os_exists.assert_called_with(expected_path)
            mock_delta_for_path.assert_called_with(spark_session, expected_path)
            
            merge_condition = mock_delta_table.merge.call_args[0][1]
            assert "source.id = target.id" in str(merge_condition) or "target.id = source.id" in str(merge_condition)
            mock_delta_table.whenMatchedUpdateAll.assert_called_once()
            mock_delta_table.whenNotMatchedInsertAll.assert_called_once()
            mock_delta_table.execute.assert_called_once()
            mock_spark_df_writer.save.assert_not_called() # Should not do a df.save()

        # Clean up
        if os.path.exists(expected_path):
            shutil.rmtree(expected_path)
            
    def test_offline_store_call_write_ttl_logic(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings, mock_delta_table):
        """Test OfflineStore write with TTL for Delta tables/paths."""
        store = OfflineStore(name="ttl_test_fg", kind=OfflineStoreEnum.FILE, 
                             value={"path": "/tmp/ttl_delta", "format": "delta"})
        store.project_name = TEST_PROJECT_NAME

        expected_path = "/tmp/ttl_delta"
        os.makedirs(expected_path, exist_ok=True)
        os.makedirs(os.path.join(expected_path, "_delta_log"), exist_ok=True)


        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer), \
             mock.patch('os.path.exists', return_value=True), \
             mock.patch('delta.DeltaTable.forPath', return_value=mock_delta_table) as mock_delta_for_path:
            
            # Use append mode for simplicity, as TTL is a separate step
            store(dataframe=mock_dataframe, entity=TEST_ENTITY_NAME, mode="append", 
                  ttl="30d", update_ts_col="update_ts") # update_ts from mock_dataframe

            # Verify write happened
            mock_spark_df_writer.save.assert_called_once_with(expected_path)
            
            # Verify DeltaTable was fetched for TTL
            mock_delta_for_path.assert_called_with(spark_session, expected_path)
            
            # Verify delete was called with correct condition
            # Need to mock pendulum.now() for predictable TTL condition
            fixed_now = pendulum.datetime(2023, 3, 15) # Arbitrary fixed "now"
            with mock.patch('pendulum.now', return_value=fixed_now):
                # Re-run the store call within this mock context if TTL calculation is dynamic
                # For this structure, we assume TTL condition is formed correctly based on 'update_ts < current_time - ttl_interval'
                # The exact string depends on pendulum interval math and SQL formatting.
                # Example: if ttl="30d", update_ts_col="update_ts"
                # delete_time = fixed_now.subtract(days=30) -> 2023-02-13
                # condition = "update_ts < '2023-02-13T00:00:00'" (or similar based on actual implementation)
                
                # Re-trigger store call within the pendulum.now mock if the condition is built inside store()
                # However, the mock_delta_table is already configured.
                # Let's assume the delete condition was formed correctly and check the call.
                # We need to ensure the delete was called. The exact condition string is tricky to match
                # without seeing the exact implementation detail of how `date_sub` or timestamp math is done.
                
                # For now, let's verify .delete(ANY) was called.
                # A more robust test would capture the condition and parse/validate it.
                
                # Re-call store to ensure pendulum.now() is used by the TTL logic.
                # This requires resetting relevant mocks if they are call_once sensitive.
                mock_spark_df_writer.reset_mock()
                mock_delta_for_path.reset_mock() # Reset this one specifically for the call count
                mock_delta_table.reset_mock() # Reset the instance of DeltaTable mock

                store(dataframe=mock_dataframe, entity=TEST_ENTITY_NAME, mode="append", 
                      ttl="30d", update_ts_col="update_ts")

            mock_delta_table.delete.assert_called_once()
            delete_condition = mock_delta_table.delete.call_args[0][0] # First arg to delete is condition
            # Expected condition: "update_ts < '2023-02-13T00:00:00+00:00'" (if using ISO format with offset)
            # or "update_ts < '2023-02-13 00:00:00'"
            # This depends heavily on how `subtract_time_from_str` and SQL formatting is done.
            # For now, check if it contains the column name and comparison.
            assert "update_ts <" in delete_condition
            assert "2023-02-13" in delete_condition # Based on fixed_now and 30d TTL

        if os.path.exists(expected_path):
            shutil.rmtree(expected_path)

    def test_offline_store_call_write_invalid_mode(self, mock_dataframe, mock_settings):
        """Test OfflineStore write with an invalid mode raises ValueError."""
        store = OfflineStore(name="test_fg", kind=OfflineStoreEnum.FILE)
        with pytest.raises(ValueError, match="Invalid mode specified"):
            store(dataframe=mock_dataframe, entity="test_entity", mode="unsupported_mode")

    def test_offline_store_call_write_overwrite_no_start_date(self, mock_dataframe, mock_settings):
        """Test OfflineStore write with overwrite mode but no start_date raises ValueError."""
        store = OfflineStore(name="test_fg", kind=OfflineStoreEnum.FILE)
        with pytest.raises(ValueError, match="'start_date' must be provided for overwrite mode with 'replaceWhere'"):
            store(dataframe=mock_dataframe, entity="test_entity", mode="overwrite", partition_cols=["event_date"])

    # --- Tests for OfflineStore.__call__(write=False) ---

    def test_offline_store_call_read_hive_table_default_db(self, spark_session, mock_spark_df_read, mock_settings):
        """Test OfflineStore read from HIVE_TABLE with default DB and no filters."""
        store = OfflineStore(name=TEST_FEATURE_GROUP_NAME, kind=OfflineStoreEnum.HIVE_TABLE)
        store.project_name = TEST_PROJECT_NAME
        store.value = None # Default DB

        expected_table_name = f"seeknal.{TEST_PROJECT_NAME}_{TEST_ENTITY_NAME}_{TEST_FEATURE_GROUP_NAME}"
        
        # Mock SparkSession's read attribute
        with mock.patch.object(spark_session, 'read', mock_spark_df_read):
            result_df = store(write=False, entity=TEST_ENTITY_NAME)

            mock_spark_df_read.format.assert_called_with("delta")
            mock_spark_df_read.table.assert_called_with(expected_table_name)
            assert result_df is mock_spark_df_read.table.return_value # Ensure the mock DF is returned
            # No where clause should be applied
            result_df.where.assert_not_called()
            result_df.filter.assert_not_called()


    def test_offline_store_call_read_hive_table_custom_db(self, spark_session, mock_spark_df_read, mock_settings):
        """Test OfflineStore read from HIVE_TABLE with custom DB."""
        custom_value = FeatureStoreHiveTableOutput(database="custom_read_db", table="custom_read_table")
        store = OfflineStore(name="custom_read_fg", kind=OfflineStoreEnum.HIVE_TABLE, value=custom_value)
        store.project_name = TEST_PROJECT_NAME

        expected_table_name = "custom_read_db.custom_read_table"

        with mock.patch.object(spark_session, 'read', mock_spark_df_read):
            result_df = store(write=False, entity="custom_read_entity")

            mock_spark_df_read.format.assert_called_with("delta") # Default format if not in custom_value
            mock_spark_df_read.table.assert_called_with(expected_table_name)
            assert result_df is mock_spark_df_read.table.return_value

    def test_offline_store_call_read_file_default_path(self, spark_session, mock_spark_df_read, mock_settings):
        """Test OfflineStore read from FILE with default path."""
        store = OfflineStore(name=TEST_FEATURE_GROUP_NAME, kind=OfflineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME
        store.value = None # Default path

        base_path = get_base_data_path()
        expected_path = os.path.join(base_path, TEST_PROJECT_NAME, TEST_ENTITY_NAME, TEST_FEATURE_GROUP_NAME)

        with mock.patch.object(spark_session, 'read', mock_spark_df_read):
            result_df = store(write=False, entity=TEST_ENTITY_NAME)

            mock_spark_df_read.format.assert_called_with("delta")
            mock_spark_df_read.load.assert_called_with(expected_path)
            assert result_df is mock_spark_df_read.load.return_value

    def test_offline_store_call_read_file_custom_path_and_format(self, spark_session, mock_spark_df_read, mock_settings):
        """Test OfflineStore read from FILE with custom path and format."""
        custom_value = FeatureStoreFileOutput(path="/tmp/custom_read_path", format="parquet")
        store = OfflineStore(name="custom_read_file_fg", kind=OfflineStoreEnum.FILE, value=custom_value)

        expected_path = "/tmp/custom_read_path"

        with mock.patch.object(spark_session, 'read', mock_spark_df_read):
            result_df = store(write=False, entity="custom_read_file_entity")
            
            mock_spark_df_read.format.assert_called_with("parquet") # Format from custom_value
            mock_spark_df_read.load.assert_called_with(expected_path)
            assert result_df is mock_spark_df_read.load.return_value

    @pytest.mark.parametrize("start_date, end_date, expected_filter_str_part", [
        ("2023-01-10", None, "date_col >= '2023-01-10'"),
        (datetime(2023, 1, 10, 10, 0, 0), None, "date_col >= '2023-01-10'"), # datetime object
        (None, "2023-01-20", "date_col <= '2023-01-20'"),
        (pendulum.parse("2023-01-10T00:00:00"), pendulum.parse("2023-01-20T23:59:59"), "date_col >= '2023-01-10' AND date_col <= '2023-01-20'"),
        ("2023-01-10", "2023-01-20", "date_col >= '2023-01-10' AND date_col <= '2023-01-20'"),
    ])
    def test_offline_store_call_read_with_date_filters(self, spark_session, mock_spark_df_read, mock_settings, start_date, end_date, expected_filter_str_part):
        """Test OfflineStore read with various date filter combinations."""
        store = OfflineStore(name="filtered_fg", kind=OfflineStoreEnum.FILE) # Kind doesn't matter much for filter logic
        store.project_name = TEST_PROJECT_NAME
        store.value = {"path": "/tmp/filtered_path"} # Dummy path

        # The mock DataFrame returned by read.load() or read.table() needs a filter method
        mock_filtered_df = mock.MagicMock(spec=DataFrame)
        mock_spark_df_read.load.return_value = mock_filtered_df
        mock_spark_df_read.table.return_value = mock_filtered_df # If HIVE was used

        with mock.patch.object(spark_session, 'read', mock_spark_df_read):
            result_df = store(write=False, entity="filtered_entity", 
                                start_date=start_date, end_date=end_date, date_col="date_col")

            assert result_df is mock_filtered_df
            if start_date or end_date:
                mock_filtered_df.filter.assert_called_once()
                actual_filter_str = mock_filtered_df.filter.call_args[0][0]
                assert expected_filter_str_part in actual_filter_str
            else: # Should not happen with these params, but good for completeness
                mock_filtered_df.filter.assert_not_called()

    def test_offline_store_call_read_no_date_col_with_dates(self, spark_session, mock_spark_df_read, mock_settings):
        """Test OfflineStore read raises ValueError if dates provided but no date_col."""
        store = OfflineStore(name="fg", kind=OfflineStoreEnum.FILE)
        with pytest.raises(ValueError, match="'date_col' must be provided if 'start_date' or 'end_date' is used."):
            store(write=False, entity="entity", start_date="2023-01-01")

    # --- Tests for OfflineStore.delete() ---

    def test_offline_store_delete_hive_table_default_db(self, spark_session, mock_settings, mock_delta_table):
        """Test OfflineStore delete from HIVE_TABLE with default DB."""
        store = OfflineStore(name=TEST_FEATURE_GROUP_NAME, kind=OfflineStoreEnum.HIVE_TABLE)
        store.project_name = TEST_PROJECT_NAME
        store.value = None # Default DB

        expected_table_name = f"seeknal.{TEST_PROJECT_NAME}_{TEST_ENTITY_NAME}_{TEST_FEATURE_GROUP_NAME}"
        delete_condition = "id = 123"

        with mock.patch('delta.DeltaTable.forName', return_value=mock_delta_table) as mock_dt_for_name:
            store.delete(entity=TEST_ENTITY_NAME, condition=delete_condition)

            mock_dt_for_name.assert_called_once_with(spark_session, expected_table_name)
            mock_delta_table.delete.assert_called_once_with(delete_condition)

    def test_offline_store_delete_hive_table_custom_db(self, spark_session, mock_settings, mock_delta_table):
        """Test OfflineStore delete from HIVE_TABLE with custom DB."""
        custom_value = FeatureStoreHiveTableOutput(database="del_db", table="del_table")
        store = OfflineStore(name="del_fg", kind=OfflineStoreEnum.HIVE_TABLE, value=custom_value)
        store.project_name = TEST_PROJECT_NAME

        expected_table_name = "del_db.del_table"
        delete_condition = "name = 'test_record'"
        
        with mock.patch('delta.DeltaTable.forName', return_value=mock_delta_table) as mock_dt_for_name:
            store.delete(entity="del_entity", condition=delete_condition) # entity kwarg not used for custom table

            mock_dt_for_name.assert_called_once_with(spark_session, expected_table_name)
            mock_delta_table.delete.assert_called_once_with(delete_condition)

    def test_offline_store_delete_file_default_path(self, spark_session, mock_settings, mock_delta_table):
        """Test OfflineStore delete from FILE with default path."""
        store = OfflineStore(name=TEST_FEATURE_GROUP_NAME, kind=OfflineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME
        store.value = None # Default path

        base_path = get_base_data_path()
        expected_path = os.path.join(base_path, TEST_PROJECT_NAME, TEST_ENTITY_NAME, TEST_FEATURE_GROUP_NAME)
        delete_condition = "event_date < '2023-01-01'"

        with mock.patch('delta.DeltaTable.forPath', return_value=mock_delta_table) as mock_dt_for_path:
            store.delete(entity=TEST_ENTITY_NAME, condition=delete_condition)

            mock_dt_for_path.assert_called_once_with(spark_session, expected_path)
            mock_delta_table.delete.assert_called_once_with(delete_condition)

    def test_offline_store_delete_file_custom_path(self, spark_session, mock_settings, mock_delta_table):
        """Test OfflineStore delete from FILE with custom path."""
        custom_value = FeatureStoreFileOutput(path="/tmp/custom_delete_path", format="delta")
        store = OfflineStore(name="custom_del_file_fg", kind=OfflineStoreEnum.FILE, value=custom_value)

        expected_path = "/tmp/custom_delete_path"
        delete_condition = "status = 'inactive'"
        
        with mock.patch('delta.DeltaTable.forPath', return_value=mock_delta_table) as mock_dt_for_path:
            store.delete(entity="custom_del_entity", condition=delete_condition) # entity not used for custom path

            mock_dt_for_path.assert_called_once_with(spark_session, expected_path)
            mock_delta_table.delete.assert_called_once_with(delete_condition)

    def test_offline_store_delete_non_delta_file_raises_error(self, spark_session, mock_settings):
        """Test OfflineStore delete from a non-Delta FILE raises NotImplementedError."""
        custom_value = FeatureStoreFileOutput(path="/tmp/non_delta_path", format="parquet")
        store = OfflineStore(name="non_delta_fg", kind=OfflineStoreEnum.FILE, value=custom_value)
        
        with pytest.raises(NotImplementedError, match="Delete operation is only supported for Delta format currently."):
            store.delete(entity="some_entity", condition="id=1")

    def test_offline_store_delete_no_condition_raises_error(self, spark_session, mock_settings):
        """Test OfflineStore delete without a condition raises ValueError."""
        store = OfflineStore(name="test_fg", kind=OfflineStoreEnum.FILE, value={"path":"/p", "format":"delta"})
        with pytest.raises(ValueError, match="Condition must be provided for delete operation to prevent accidental full data deletion."):
            store.delete(entity="some_entity")

    def test_offline_store_delete_delta_operation_exception(self, spark_session, mock_settings, mock_delta_table):
        """Test OfflineStore delete handles exceptions during DeltaTable operations."""
        store = OfflineStore(name="test_fg", kind=OfflineStoreEnum.FILE, value={"path":"/p", "format":"delta"})
        store.project_name = TEST_PROJECT_NAME # Needed for path resolution if default
        
        mock_delta_table.delete.side_effect = Exception("Delta operation failed")

        with mock.patch('delta.DeltaTable.forPath', return_value=mock_delta_table) as mock_dt_for_path:
            with pytest.raises(Exception, match="Delta operation failed"):
                store.delete(entity=TEST_ENTITY_NAME, condition="id=1")
            mock_dt_for_path.assert_called_once() # Ensure it attempted to get the table
            mock_delta_table.delete.assert_called_once() # Ensure it attempted to delete


# Tests for OnlineStore
class TestOnlineStore:

    def test_online_store_post_init_default_kind_and_value(self):
        """Test OnlineStore __post_init__ with default kind (FILE) and None value."""
        store = OnlineStore(name="test_online_store")
        assert store.kind == OnlineStoreEnum.FILE.value # Default is FILE
        assert store.value is None

    def test_online_store_post_init_file_kind_feature_store_output(self):
        """Test OnlineStore __post_init__ with FILE kind and FeatureStoreFileOutput."""
        file_output = FeatureStoreFileOutput(path="/online/path", format="parquet") # Format might be ignored for online
        store = OnlineStore(name="test_online_store", kind=OnlineStoreEnum.FILE, value=file_output)
        assert store.kind == OnlineStoreEnum.FILE.value
        assert isinstance(store.value, dict)
        assert store.value["path"] == "/online/path"
        # Format might not be strictly relevant for OnlineStore if it always uses parquet or similar
        assert store.value["format"] == "parquet" 

    def test_online_store_post_init_value_already_dict(self):
        """Test OnlineStore __post_init__ when value is already a dictionary."""
        val_dict = {"path": "/another/online_path", "format": "json"}
        store = OnlineStore(name="test_online_store", kind=OnlineStoreEnum.FILE, value=val_dict)
        assert store.kind == OnlineStoreEnum.FILE.value
        assert store.value == val_dict

    # --- Tests for OnlineStore.get_or_create() ---
    # Assuming OnlineStore uses similar FeatureGroupRequest methods as OfflineStore,
    # but named get_online_store_by_name and save_online_store.

    def test_get_or_create_online_store_does_not_exist(self, mock_settings, mock_fgr_get_online_store, mock_fgr_save_online_store):
        """Test get_or_create for OnlineStore when it does not exist."""
        mock_fgr_get_online_store.return_value = None
        mock_save_return = mock.MagicMock()
        mock_save_return.id = "new_online_store_id_789"
        mock_fgr_save_online_store.return_value = mock_save_return

        store = OnlineStore(name="new_online_store", kind=OnlineStoreEnum.FILE, value={"path": "/online_data"})
        store.project_name = TEST_PROJECT_NAME

        result_store = store.get_or_create()

        mock_fgr_get_online_store.assert_called_once_with(name="new_online_store", project_name=TEST_PROJECT_NAME)
        expected_params_json = json.dumps({"path": "/online_data"}) # Assuming it stores value as params
        mock_fgr_save_online_store.assert_called_once_with(
            name="new_online_store",
            kind=OnlineStoreEnum.FILE.value,
            project_name=TEST_PROJECT_NAME,
            params=expected_params_json,
            description=None
        )
        assert store.id == "new_online_store_id_789"
        assert result_store is store

    def test_get_or_create_online_store_exists(self, mock_settings, mock_fgr_get_online_store, mock_fgr_save_online_store):
        """Test get_or_create for OnlineStore when it already exists."""
        mock_existing_store_data = mock.MagicMock()
        mock_existing_store_data.id = "existing_online_id_012"
        mock_existing_store_data.name = "existing_online_store"
        mock_existing_store_data.kind = OnlineStoreEnum.FILE.value
        mock_existing_store_data.params = json.dumps({"path": "/server/online_path", "format": "parquet"})
        mock_existing_store_data.description = "Existing Online Description"

        mock_fgr_get_online_store.return_value = mock_existing_store_data

        store = OnlineStore(name="existing_online_store")
        store.project_name = TEST_PROJECT_NAME

        result_store = store.get_or_create()

        mock_fgr_get_online_store.assert_called_once_with(name="existing_online_store", project_name=TEST_PROJECT_NAME)
        mock_fgr_save_online_store.assert_not_called()
        
        assert store.id == "existing_online_id_012"
        assert store.name == "existing_online_store"
        assert store.kind == OnlineStoreEnum.FILE.value
        assert store.value == {"path": "/server/online_path", "format": "parquet"}
        assert store.description == "Existing Online Description"
        assert result_store is store

    # --- Tests for OnlineStore.__call__(write=True) ---

    def test_online_store_call_write_file_default_path_spark_df(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings):
        """Test OnlineStore write (Spark DF) to FILE with default path."""
        store = OnlineStore(name=TEST_FEATURE_GROUP_NAME, kind=OnlineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME # For default path
        store.value = None # Ensure default path is used

        base_path = get_base_data_path() # From CONFIG_BASE_URL
        # Online store path structure is typically simpler: project_name/feature_group_name.parquet
        expected_dir = os.path.join(base_path, TEST_PROJECT_NAME, "online_store")
        expected_file_path = os.path.join(expected_dir, f"{TEST_FEATURE_GROUP_NAME}.parquet")

        # Clean up before test
        if os.path.exists(expected_dir):
            shutil.rmtree(expected_dir)

        # Mock Spark DF writer chain, specifically for to_pandas().write.parquet() if that's the path
        # However, the provided code directly uses df.write.parquet(path)
        mock_dataframe.coalesce.return_value = mock_dataframe # Mock coalesce if used

        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer), \
             mock.patch('os.makedirs') as mock_makedirs:
            
            store(dataframe=mock_dataframe, entity=TEST_ENTITY_NAME) # Entity might not be used for online path

            mock_makedirs.assert_called_once_with(expected_dir, exist_ok=True)
            mock_spark_df_writer.mode.assert_called_with("overwrite") # Default mode for online store updates
            mock_spark_df_writer.parquet.assert_called_once_with(expected_file_path)
            # Verify coalesce was called if it's part of the implementation for Spark DFs
            mock_dataframe.coalesce.assert_called_once_with(1)


    def test_online_store_call_write_file_custom_path_spark_df(self, spark_session, mock_dataframe, mock_spark_df_writer, mock_settings):
        """Test OnlineStore write (Spark DF) to FILE with custom path."""
        custom_file_path = "/tmp/custom_online_output.parquet"
        custom_value = FeatureStoreFileOutput(path=custom_file_path, format="parquet") # format might be ignored
        store = OnlineStore(name="custom_online_fg", kind=OnlineStoreEnum.FILE, value=custom_value)
        
        expected_dir = os.path.dirname(custom_file_path)

        if os.path.exists(custom_file_path):
            os.remove(custom_file_path)
        if os.path.exists(expected_dir) and not os.listdir(expected_dir) : # remove dir if empty and it was created by test
             os.rmdir(expected_dir)


        mock_dataframe.coalesce.return_value = mock_dataframe
        with mock.patch.object(mock_dataframe, 'write', mock_spark_df_writer), \
             mock.patch('os.makedirs') as mock_makedirs:

            store(dataframe=mock_dataframe, entity="custom_entity")

            mock_makedirs.assert_called_once_with(expected_dir, exist_ok=True)
            mock_spark_df_writer.mode.assert_called_with("overwrite")
            mock_spark_df_writer.parquet.assert_called_once_with(custom_file_path)
            mock_dataframe.coalesce.assert_called_once_with(1)


    def test_online_store_call_write_file_pandas_df(self, mock_pandas_dataframe, mock_settings):
        """Test OnlineStore write (Pandas DF) to FILE."""
        if mock_pandas_dataframe is None:
            pytest.skip("Pandas not installed, skipping test.")

        store = OnlineStore(name="pandas_online_fg", kind=OnlineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME
        store.value = {"path": "/tmp/pandas_data.parquet"} # Custom path for isolation

        expected_file_path = "/tmp/pandas_data.parquet"
        expected_dir = os.path.dirname(expected_file_path)

        if os.path.exists(expected_file_path):
            os.remove(expected_file_path)

        # Mock os.makedirs and pandas.DataFrame.to_parquet
        with mock.patch('os.makedirs') as mock_makedirs, \
             mock.patch.object(mock_pandas_dataframe, 'to_parquet') as mock_to_parquet:

            store(dataframe=mock_pandas_dataframe, entity="pandas_entity")

            mock_makedirs.assert_called_once_with(expected_dir, exist_ok=True)
            mock_to_parquet.assert_called_once_with(expected_file_path, engine="pyarrow", index=False)
        
        if os.path.exists(expected_file_path): # cleanup
            os.remove(expected_file_path)


    def test_online_store_call_write_unsupported_dataframe_type(self, mock_settings):
        """Test OnlineStore write with an unsupported DataFrame type raises TypeError."""
        store = OnlineStore(name="error_fg", kind=OnlineStoreEnum.FILE)
        unsupported_df = {"data": "not a dataframe"} # Example of unsupported type
        
        with pytest.raises(TypeError, match="Unsupported DataFrame type for OnlineStore"):
            store(dataframe=unsupported_df, entity="error_entity")

    def test_online_store_call_write_non_file_kind_raises_error(self, mock_dataframe, mock_settings):
        """Test OnlineStore write with non-FILE kind raises NotImplementedError."""
        # This assumes OnlineStore might someday have other kinds, though current code is FILE-centric.
        store = OnlineStore(name="other_kind_fg", kind="HYPOTHETICAL_DB_KIND")
        store.value = {"some_config": "value"}

        with pytest.raises(NotImplementedError, match="OnlineStore write is only implemented for FILE kind."):
            store(dataframe=mock_dataframe, entity="some_entity")

    # --- Tests for OnlineStore.__call__(write=False) ---

    def test_online_store_call_read_file_default_path(self, mock_settings):
        """Test OnlineStore read from FILE with default path returns configured DuckDBTask."""
        store = OnlineStore(name=TEST_FEATURE_GROUP_NAME, kind=OnlineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME
        store.value = None # Default path

        base_path = get_base_data_path()
        expected_file_path = os.path.join(base_path, TEST_PROJECT_NAME, "online_store", f"{TEST_FEATURE_GROUP_NAME}.parquet")

        # Mock DuckDBTask to verify its initialization
        with mock.patch('seeknal.featurestore.featurestore.DuckDBTask') as mock_duckdb_task_constructor:
            mock_duckdb_instance = mock.MagicMock(spec=DuckDBTask)
            mock_duckdb_task_constructor.return_value = mock_duckdb_instance
            
            # Mock os.path.exists to simulate file existence
            with mock.patch('os.path.exists', return_value=True) as mock_path_exists:
                task = store(write=False, entity=TEST_ENTITY_NAME) # entity not really used for default online path

                mock_path_exists.assert_called_once_with(expected_file_path)
                mock_duckdb_task_constructor.assert_called_once()
                
                # Check the arguments passed to DuckDBTask constructor
                # The first arg is sql, which should be a SELECT query
                # The second arg is connection, which should be None by default for DuckDBTask
                # Then kwargs like file_path=expected_file_path
                call_args = mock_duckdb_task_constructor.call_args
                assert f"SELECT * FROM read_parquet('{expected_file_path}')" in call_args[0][0] # SQL query
                assert call_args[1]['file_path'] == expected_file_path # file_path kwarg
                assert task is mock_duckdb_instance

    def test_online_store_call_read_file_custom_path(self, mock_settings):
        """Test OnlineStore read from FILE with custom path returns configured DuckDBTask."""
        custom_path = "/tmp/my_online_features.parquet"
        store = OnlineStore(name="custom_online_read", kind=OnlineStoreEnum.FILE, value={"path": custom_path})
        store.project_name = TEST_PROJECT_NAME

        with mock.patch('seeknal.featurestore.featurestore.DuckDBTask') as mock_duckdb_task_constructor, \
             mock.patch('os.path.exists', return_value=True) as mock_path_exists:
            mock_duckdb_instance = mock.MagicMock(spec=DuckDBTask)
            mock_duckdb_task_constructor.return_value = mock_duckdb_instance

            task = store(write=False, entity="some_entity")

            mock_path_exists.assert_called_once_with(custom_path)
            mock_duckdb_task_constructor.assert_called_once()
            call_args = mock_duckdb_task_constructor.call_args
            assert f"SELECT * FROM read_parquet('{custom_path}')" in call_args[0][0]
            assert call_args[1]['file_path'] == custom_path
            assert task is mock_duckdb_instance

    def test_online_store_call_read_file_not_exists(self, mock_settings):
        """Test OnlineStore read from FILE when file does not exist raises FileNotFoundError."""
        store = OnlineStore(name="non_existent_online", kind=OnlineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME
        store.value = {"path": "/tmp/non_existent_file.parquet"}

        with mock.patch('os.path.exists', return_value=False) as mock_path_exists:
            with pytest.raises(FileNotFoundError, match="Online store file not found at /tmp/non_existent_file.parquet"):
                store(write=False, entity="some_entity")
            mock_path_exists.assert_called_once_with("/tmp/non_existent_file.parquet")


    def test_online_store_call_read_non_file_kind_raises_error(self, mock_settings):
        """Test OnlineStore read with non-FILE kind raises NotImplementedError."""
        store = OnlineStore(name="other_kind_online_read", kind="SOME_DB_KIND")
        store.value = {"db_config": "value"}

        with pytest.raises(NotImplementedError, match="OnlineStore read is only implemented for FILE kind."):
            store(write=False, entity="some_entity")

    # --- Tests for OnlineStore.delete() ---

    def test_online_store_delete_file_default_path(self, mock_settings):
        """Test OnlineStore delete for FILE kind with default path."""
        store = OnlineStore(name=TEST_FEATURE_GROUP_NAME, kind=OnlineStoreEnum.FILE)
        store.project_name = TEST_PROJECT_NAME
        store.value = None # Default path

        base_path = get_base_data_path()
        # Default path for online store is <base_path>/<project_name>/online_store/<feature_group_name>.parquet
        expected_file_path = os.path.join(base_path, TEST_PROJECT_NAME, "online_store", f"{TEST_FEATURE_GROUP_NAME}.parquet")

        with mock.patch('os.path.exists') as mock_path_exists, \
             mock.patch('os.remove') as mock_os_remove, \
             mock.patch('shutil.rmtree') as mock_shutil_rmtree: # shutil.rmtree might be used if it's a dir

            # Scenario 1: File exists
            mock_path_exists.return_value = True
            store.delete(entity=TEST_ENTITY_NAME) # entity name might be used in path construction

            mock_path_exists.assert_called_with(expected_file_path)
            # OnlineStore currently removes the specific .parquet file, not the directory.
            mock_os_remove.assert_called_once_with(expected_file_path)
            mock_shutil_rmtree.assert_not_called() # Should not be called for single file removal

            # Scenario 2: File does not exist (should not raise error, just log)
            mock_path_exists.return_value = False
            mock_os_remove.reset_mock() # Reset for the next call
            
            # Mock logger to check for "File not found" message
            with mock.patch('logging.Logger.info') as mock_logger_info:
                store.delete(entity=TEST_ENTITY_NAME)
                mock_os_remove.assert_not_called() # Not called if file doesn't exist
                
                # Check if a "File not found" like message was logged
                # This depends on the exact logging message in the implementation.
                # For now, we assume it logs something like this.
                # Example: "File /path/to/file.parquet not found, skipping deletion."
                # We'll check if logger.info was called. A more specific check would be on the message.
                # Based on the provided OnlineStore.delete, it doesn't log if not found, it just skips.
                # So, no specific log check here, just that it doesn't error.
                pass # No error should be raised.


    def test_online_store_delete_file_custom_path(self, mock_settings):
        """Test OnlineStore delete for FILE kind with a custom path."""
        custom_path = "/tmp/custom_online_store_to_delete.parquet"
        store = OnlineStore(name="custom_delete_online", kind=OnlineStoreEnum.FILE, value={"path": custom_path})
        store.project_name = TEST_PROJECT_NAME

        with mock.patch('os.path.exists') as mock_path_exists, \
             mock.patch('os.remove') as mock_os_remove:

            mock_path_exists.return_value = True
            store.delete(entity="some_entity") # Entity not used for custom path

            mock_path_exists.assert_called_with(custom_path)
            mock_os_remove.assert_called_once_with(custom_path)

    def test_online_store_delete_non_file_kind_raises_error(self, mock_settings):
        """Test OnlineStore delete with non-FILE kind raises NotImplementedError."""
        store = OnlineStore(name="other_kind_delete", kind="SOME_OTHER_KIND")
        store.value = {"config": "value"}

        with pytest.raises(NotImplementedError, match="OnlineStore delete is only implemented for FILE kind."):
            store.delete(entity="some_entity")

    def test_online_store_delete_os_error(self, mock_settings):
        """Test OnlineStore delete handles OSError during file removal."""
        custom_path = "/tmp/online_delete_error.parquet"
        store = OnlineStore(name="os_error_delete", kind=OnlineStoreEnum.FILE, value={"path": custom_path})

        with mock.patch('os.path.exists', return_value=True) as mock_path_exists, \
             mock.patch('os.remove', side_effect=OSError("Permission denied")) as mock_os_remove, \
             mock.patch('logging.Logger.error') as mock_logger_error: # Assuming errors are logged

            with pytest.raises(OSError, match="Permission denied"): # The original error should propagate
                store.delete(entity="some_entity")
            
            mock_os_remove.assert_called_once_with(custom_path)
            # Check if logger.error was called with a message containing the path and error
            # This depends on the actual logging in the implementation.
            # Example: "Error deleting file /tmp/online_delete_error.parquet: Permission denied"
            # For now, let's assume it logs. If not, this part of assert is not needed.
            # The provided snippet does not show logging in delete, so error propagates directly.
            # mock_logger_error.assert_called_once() # Uncomment if logging is implemented


# Tests for OnlineStore will go here
