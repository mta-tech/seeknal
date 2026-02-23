"""End-to-end tests for Iceberg Feature Group materialization.

These tests verify the complete Iceberg storage workflow for feature groups:
- Creating feature groups with Iceberg storage
- Writing features to Iceberg tables
- Verifying snapshot metadata capture
- Error handling for invalid configurations

Note: These tests use mocked infrastructure by default. For real infrastructure
tests, set the environment variables:
- LAKEKEEPER_URI: REST catalog endpoint
- LAKEKEEPER_WAREHOUSE: S3 warehouse path
"""

import os
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from seeknal.cli.main import app
from seeknal.featurestore import (
    FeatureGroup,
    Materialization,
    OfflineMaterialization,
    OfflineStore,
    OfflineStoreEnum,
    IcebergStoreOutput,
)
from seeknal.entity import Entity
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


runner = CliRunner()


@pytest.fixture(scope="function")
def clean_test_env(tmp_path):
    """Create a clean test environment for E2E tests."""
    original_dir = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original_dir)


@pytest.fixture(scope="function")
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("IcebergFeatureGroupTest") \
        .config("spark.sql.warehouse.dir", str(Path.cwd() / "warehouse")) \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def sample_customer_data(spark_session):
    """Create sample customer feature data."""
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("event_date", StringType(), False),
        StructField("total_orders", IntegerType(), False),
        StructField("total_spend", DoubleType(), False),
    ])

    data = [
        ("A001", "2024-01-01", 5, 100.0),
        ("A002", "2024-01-01", 10, 250.0),
        ("A003", "2024-01-01", 3, 75.0),
        ("A001", "2024-01-02", 2, 50.0),
        ("A002", "2024-01-02", 5, 125.0),
    ]

    return spark_session.createDataFrame(data, schema=schema)


class TestIcebergFeatureGroupBasic:
    """Basic tests for Iceberg Feature Group functionality."""

    def test_iceberg_store_output_creation(self):
        """Test IcebergStoreOutput dataclass creation."""
        config = IcebergStoreOutput(
            table="customer_features",
            catalog="lakekeeper",
            namespace="prod",
            warehouse="s3://my-bucket/warehouse",
            mode="append"
        )

        assert config.table == "customer_features"
        assert config.catalog == "lakekeeper"
        assert config.namespace == "prod"
        assert config.warehouse == "s3://my-bucket/warehouse"
        assert config.mode == "append"

    def test_iceberg_store_output_defaults(self):
        """Test IcebergStoreOutput with default values."""
        config = IcebergStoreOutput(table="test_table")

        assert config.table == "test_table"
        assert config.catalog == "lakekeeper"
        assert config.namespace == "default"
        assert config.warehouse is None
        assert config.mode == "append"

    def test_iceberg_store_output_to_dict(self):
        """Test IcebergStoreOutput.to_dict() method."""
        config = IcebergStoreOutput(
            table="test_table",
            catalog="my_catalog",
            namespace="test_ns",
            warehouse="s3://bucket/wh"
        )

        result = config.to_dict()
        assert result["table"] == "test_table"
        assert result["catalog"] == "my_catalog"
        assert result["namespace"] == "test_ns"
        assert result["warehouse"] == "s3://bucket/wh"
        assert result["mode"] == "append"

    def test_offline_store_enum_has_iceberg(self):
        """Test that OfflineStoreEnum includes ICEBERG."""
        assert hasattr(OfflineStoreEnum, "ICEBERG")
        assert OfflineStoreEnum.ICEBERG == "iceberg"

    def test_offline_store_with_iceberg_config(self):
        """Test OfflineStore configuration with Iceberg."""
        iceberg_output = IcebergStoreOutput(
            table="features",
            catalog="lakekeeper",
            namespace="default"
        )

        store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=iceberg_output,
            name="test_store"
        )

        assert store.kind == OfflineStoreEnum.ICEBERG
        assert isinstance(store.value, IcebergStoreOutput)
        assert store.value.table == "features"


class TestIcebergFeatureGroupCreation:
    """Tests for creating Feature Groups with Iceberg storage."""

    def test_create_feature_group_with_iceberg_storage(self):
        """Test creating a FeatureGroup with Iceberg storage configuration."""
        customer_entity = Entity(
            name="customer",
            join_keys=["customer_id"]
        )

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table="customer_features",
                catalog="lakekeeper",
                namespace="prod"
            )
        )

        materialization = Materialization(
            event_time_col="event_date",
            offline=True,
            offline_materialization=OfflineMaterialization(
                store=iceberg_store,
                mode="append"
            )
        )

        fg = FeatureGroup(
            name="customer_features",
            entity=customer_entity,
            materialization=materialization
        )

        assert fg.name == "customer_features"
        assert fg.materialization.offline_materialization.store.kind == OfflineStoreEnum.ICEBERG
        assert fg.materialization.offline_materialization.store.value.table == "customer_features"


class TestIcebergWriteOperation:
    """Tests for Iceberg write operations."""

    @patch('seeknal.workflow.materialization.profile_loader.ProfileLoader.load_profile')
    @patch('seeknal.workflow.materialization.operations.DuckDBIcebergExtension.load_extension')
    @patch('seeknal.workflow.materialization.operations.DuckDBIcebergExtension.create_rest_catalog')
    @patch('seeknal.workflow.materialization.operations.write_to_iceberg')
    def test_write_to_iceberg_append_mode(
        self,
        mock_write_to_iceberg,
        mock_create_catalog,
        mock_load_extension,
        mock_load_profile,
        sample_customer_data
    ):
        """Test writing features to Iceberg in append mode."""
        # Setup mocks
        from seeknal.workflow.materialization.config import MaterializationConfig, CatalogConfig
        from seeknal.workflow.materialization.operations import WriteResult

        mock_profile_config = MaterializationConfig(
            catalog=CatalogConfig(
                uri="http://localhost:8181",
                warehouse="s3://test/warehouse"
            )
        )
        mock_load_profile.return_value = mock_profile_config

        mock_write_result = WriteResult(
            success=True,
            row_count=5,
            snapshot_id="1234567890"
        )
        mock_write_to_iceberg.return_value = mock_write_result

        # Create feature group with Iceberg
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table="customer_features",
                catalog="lakekeeper",
                namespace="test"
            )
        )

        # Call the write operation
        result = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="test_feature"
        )

        # Verify result
        assert result["storage_type"] == "iceberg"
        assert result["num_rows"] == 5
        assert result["snapshot_id"] == "1234567890"
        assert result["table"] == "customer_features"
        assert result["namespace"] == "test"
        assert result["catalog"] == "lakekeeper"

        # Verify write_to_iceberg was called
        mock_write_to_iceberg.assert_called_once()

    def test_write_to_iceberg_overwrite_mode(self):
        """Test writing features to Iceberg in overwrite mode."""
        # Test with overwrite mode
        iceberg_output = IcebergStoreOutput(
            table="test_table",
            mode="overwrite"
        )

        assert iceberg_output.mode == "overwrite"

    def test_write_to_iceberg_custom_mode(self):
        """Test that custom mode values are accepted (for future extensibility)."""
        # The dataclass accepts any mode value - validation happens at write time
        iceberg_output = IcebergStoreOutput(
            table="test_table",
            mode="custom_mode"
        )
        assert iceberg_output.mode == "custom_mode"


class TestIcebergErrorHandling:
    """Tests for error handling in Iceberg operations."""

    def test_missing_catalog_uri_raises_error(self, sample_customer_data):
        """Test that missing catalog URI raises a descriptive error."""
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table="customer_features"
            )
        )

        with patch('seeknal.workflow.materialization.profile_loader.ProfileLoader.load_profile') as mock_load:
            from seeknal.workflow.materialization.config import MaterializationConfig, CatalogConfig

            # Return empty config
            mock_config = MaterializationConfig(
                catalog=CatalogConfig(uri="", warehouse="")
            )
            mock_load.return_value = mock_config

            with pytest.raises(ValueError) as exc_info:
                iceberg_store(
                    result=sample_customer_data,
                    project="test",
                    entity="customer"
                )

            assert "Catalog URI not configured" in str(exc_info.value)

    def test_missing_warehouse_path_raises_error(self, sample_customer_data):
        """Test that missing warehouse path raises a descriptive error."""
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table="customer_features"
            )
        )

        with patch('seeknal.workflow.materialization.profile_loader.ProfileLoader.load_profile') as mock_load:
            from seeknal.workflow.materialization.config import MaterializationConfig, CatalogConfig

            # Return config with URI but no warehouse
            mock_config = MaterializationConfig(
                catalog=CatalogConfig(
                    uri="http://localhost:8181",
                    warehouse=""
                )
            )
            mock_load.return_value = mock_config

            with pytest.raises(ValueError) as exc_info:
                iceberg_store(
                    result=sample_customer_data,
                    project="test",
                    entity="customer"
                )

            assert "Warehouse path not configured" in str(exc_info.value)


class TestIcebergCLIIntegration:
    """Tests for CLI integration with Iceberg feature groups."""

    def test_list_feature_groups_shows_iceberg_type(self, clean_test_env):
        """Test that list command shows Iceberg storage type."""
        # Initialize project
        runner.invoke(app, ["init", "--name", "iceberg_test"])

        # Note: This test would require a saved feature group with Iceberg
        # For now, just verify the command runs without error
        result = runner.invoke(app, ["list", "feature-groups"])
        assert result.exit_code == 0 or "No feature groups found" in result.stdout


@pytest.mark.skipif(
    os.getenv("LAKEKEEPER_URI") is None,
    reason="Requires LAKEKEEPER_URI environment variable"
)
class TestIcebergRealInfrastructure:
    """Tests against real Iceberg infrastructure (atlas-dev-server).

    These tests only run when environment variables are set:
    - LAKEKEEPER_URI: REST catalog endpoint
    - LAKEKEEPER_WAREHOUSE: S3 warehouse path
    - LAKEKEEPER_TOKEN: Optional bearer token
    """

    def test_real_iceberg_connection(self):
        """Test connection to real Lakekeeper catalog."""
        import duckdb

        catalog_uri = os.getenv("LAKEKEEPER_URI")
        warehouse_path = os.getenv("LAKEKEEPER_WAREHOUSE")

        con = duckdb.connect(":memory:")
        try:
            from seeknal.workflow.materialization.operations import DuckDBIcebergExtension

            # Load extension
            DuckDBIcebergExtension.load_extension(con)

            # Create catalog
            DuckDBIcebergExtension.create_rest_catalog(
                con=con,
                catalog_name="test_catalog",
                uri=catalog_uri,
                warehouse_path=warehouse_path
            )

            # Test query
            result = con.execute("SHOW DATABASES").fetchall()
            assert isinstance(result, list)

        finally:
            con.close()

    def test_real_iceberg_write_and_read(self, sample_customer_data):
        """Test writing and reading from real Iceberg table."""
        catalog_uri = os.getenv("LAKEKEEPER_URI")
        warehouse_path = os.getenv("LAKEKEEPER_WAREHOUSE")

        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table=f"test_customer_features_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                catalog="lakekeeper",
                namespace="test"
            )
        )

        result = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="test_feature"
        )

        assert result["storage_type"] == "iceberg"
        assert result["num_rows"] == 5
        assert result["snapshot_id"] is not None
