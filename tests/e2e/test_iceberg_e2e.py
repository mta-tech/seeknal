"""End-to-end tests for Iceberg feature group materialization.

These tests verify complete Iceberg integration workflows including:
- Creating feature groups with Iceberg storage
- Writing features with append and overwrite modes
- Verifying snapshot metadata capture
- Error handling for invalid configurations
- Integration with existing FeatureGroup API

Tests use mock DuckDB/Iceberg for reliability without requiring
actual Lakekeeper catalog instances.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from unittest import mock

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


runner = CliRunner()


@pytest.fixture(scope="function")
def clean_test_env(tmp_path):
    """Create a clean test environment for E2E tests."""
    original_dir = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(original_dir)


@pytest.fixture(scope="function")
def mock_duckdb_iceberg():
    """Mock DuckDB Iceberg operations for testing."""
    with mock.patch("duckdb.connect") as mock_connect:
        # Setup mock connection
        mock_con = mock.MagicMock()
        mock_connect.return_value = mock_con

        # Mock execute to return successful results
        mock_con.execute.return_value.fetchone.return_value = [100]  # row_count

        yield mock_con


class TestIcebergFeatureGroupCreation:
    """E2E tests for creating feature groups with Iceberg storage."""

    @mock.patch("seeknal.workflow.materialization.profile_loader.ProfileLoader")
    def test_create_feature_group_with_iceberg_storage(
        self, mock_profile_loader, clean_test_env
    ):
        """Test creating a feature group with Iceberg offline storage."""
        # Mock profile loader to return valid config
        mock_loader_instance = mock.MagicMock()
        mock_config = mock.MagicMock()
        mock_config.catalog.uri = "https://lakekeeper.example.com"
        mock_config.catalog.warehouse = "s3://test-bucket/warehouse"
        mock_config.catalog.bearer_token = None
        mock_loader_instance.load_profile.return_value = mock_config
        mock_profile_loader.return_value = mock_loader_instance

        # Create entity
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        # Create feature group with Iceberg storage
        fg = FeatureGroup(
            name="customer_features",
            entity=customer_entity,
            materialization=Materialization(
                event_time_col="event_date",
                offline=True,
                offline_materialization=OfflineMaterialization(
                    store=OfflineStore(
                        kind=OfflineStoreEnum.ICEBERG,
                        value=IcebergStoreOutput(
                            catalog="lakekeeper",
                            namespace="prod",
                            table="customer_features",
                            mode="append"
                        )
                    ),
                    mode="append"
                )
            )
        )

        # Verify configuration
        assert fg.materialization.offline_materialization.store.kind == "iceberg"
        assert fg.materialization.offline_materialization.store.value.catalog == "lakekeeper"
        assert fg.materialization.offline_materialization.store.value.namespace == "prod"
        assert fg.materialization.offline_materialization.store.value.table == "customer_features"
        assert fg.materialization.offline_materialization.store.value.mode == "append"

    def test_iceberg_store_output_defaults(self):
        """Test IcebergStoreOutput default values."""
        iceberg_config = IcebergStoreOutput(table="test_table")

        assert iceberg_config.catalog == "lakekeeper"
        assert iceberg_config.namespace == "default"
        assert iceberg_config.mode == "append"
        assert iceberg_config.warehouse is None
        assert iceberg_config.table == "test_table"

    def test_iceberg_store_output_to_dict(self):
        """Test IcebergStoreOutput to_dict conversion."""
        iceberg_config = IcebergStoreOutput(
            catalog="custom_catalog",
            warehouse="s3://my-bucket/warehouse",
            namespace="test_ns",
            table="test_table",
            mode="overwrite"
        )

        config_dict = iceberg_config.to_dict()
        assert config_dict["catalog"] == "custom_catalog"
        assert config_dict["warehouse"] == "s3://my-bucket/warehouse"
        assert config_dict["namespace"] == "test_ns"
        assert config_dict["table"] == "test_table"
        assert config_dict["mode"] == "overwrite"


class TestIcebergWriteOperations:
    """E2E tests for writing features to Iceberg storage."""

    @mock.patch("seeknal.workflow.materialization.profile_loader.ProfileLoader")
    @mock.patch("duckdb.connect")
    def test_write_features_in_append_mode(
        self, mock_connect, mock_profile_loader, mock_duckdb_iceberg
    ):
        """Test writing features to Iceberg in append mode."""
        # Mock profile loader
        mock_loader_instance = mock.MagicMock()
        mock_config = mock.MagicMock()
        mock_config.catalog.uri = "https://lakekeeper.example.com"
        mock_config.catalog.warehouse = "s3://test-bucket/warehouse"
        mock_config.catalog.bearer_token = None
        mock_loader_instance.load_profile.return_value = mock_config
        mock_profile_loader.return_value = mock_loader_instance

        # Setup write_to_iceberg mock (imported inside _write_to_iceberg)
        with mock.patch("seeknal.workflow.materialization.operations.write_to_iceberg") as mock_write:
            mock_result = mock.MagicMock()
            mock_result.row_count = 100
            mock_result.snapshot_id = "1234567890abcdef"
            mock_write.return_value = mock_result

            # Create offline store with Iceberg
            iceberg_store = OfflineStore(
                kind=OfflineStoreEnum.ICEBERG,
                value=IcebergStoreOutput(
                    catalog="lakekeeper",
                    namespace="test",
                    table="customer_features",
                    mode="append"
                )
            )

            # Mock result DataFrame
            mock_result_df = mock.MagicMock()

            # Call write
            result = iceberg_store._write_to_iceberg(
                result=mock_result_df,
                kwds={"project": "test_project", "entity": "customer"}
            )

            # Verify result
            assert result["storage_type"] == "iceberg"
            assert result["num_rows"] == 100
            assert result["snapshot_id"] == "1234567890abcdef"
            assert result["table"] == "customer_features"
            assert result["namespace"] == "test"

            # Verify write_to_iceberg was called
            mock_write.assert_called_once()

    @mock.patch("seeknal.workflow.materialization.profile_loader.ProfileLoader")
    @mock.patch("duckdb.connect")
    def test_write_features_in_overwrite_mode(
        self, mock_connect, mock_profile_loader, mock_duckdb_iceberg
    ):
        """Test writing features to Iceberg in overwrite mode."""
        # Mock profile loader
        mock_loader_instance = mock.MagicMock()
        mock_config = mock.MagicMock()
        mock_config.catalog.uri = "https://lakekeeper.example.com"
        mock_config.catalog.warehouse = "s3://test-bucket/warehouse"
        mock_config.catalog.bearer_token = None
        mock_loader_instance.load_profile.return_value = mock_config
        mock_profile_loader.return_value = mock_loader_instance

        # Setup write_to_iceberg mock (imported inside _write_to_iceberg)
        with mock.patch("seeknal.workflow.materialization.operations.write_to_iceberg") as mock_write:
            mock_result = mock.MagicMock()
            mock_result.row_count = 200
            mock_result.snapshot_id = "fedcba0987654321"
            mock_write.return_value = mock_result

            # Create offline store with Iceberg overwrite mode
            iceberg_store = OfflineStore(
                kind=OfflineStoreEnum.ICEBERG,
                value=IcebergStoreOutput(
                    catalog="lakekeeper",
                    namespace="test",
                    table="customer_features",
                    mode="overwrite"
                )
            )

            # Mock result DataFrame
            mock_result_df = mock.MagicMock()

            # Call write
            result = iceberg_store._write_to_iceberg(
                result=mock_result_df,
                kwds={"project": "test_project", "entity": "customer"}
            )

            # Verify overwrite mode was passed
            assert result["storage_type"] == "iceberg"
            assert result["num_rows"] == 200

            # Verify write_to_iceberg was called with overwrite mode
            call_args = mock_write.call_args
            assert call_args.kwargs["mode"] == "overwrite"


class TestIcebergErrorHandling:
    """E2E tests for error handling in Iceberg operations."""

    def test_iceberg_write_without_catalog_uri(self):
        """Test error when catalog URI is not configured."""
        with mock.patch("seeknal.workflow.materialization.profile_loader.ProfileLoader") as mock_profile_loader:
            # Mock profile loader with no URI
            mock_loader_instance = mock.MagicMock()
            mock_config = mock.MagicMock()
            mock_config.catalog.uri = ""
            mock_config.catalog.warehouse = "s3://test-bucket/warehouse"
            mock_loader_instance.load_profile.return_value = mock_config
            mock_profile_loader.return_value = mock_loader_instance

            # Create offline store with Iceberg
            iceberg_store = OfflineStore(
                kind=OfflineStoreEnum.ICEBERG,
                value=IcebergStoreOutput(
                    catalog="lakekeeper",
                    namespace="test",
                    table="customer_features"
                )
            )

            # Mock result DataFrame
            mock_result_df = mock.MagicMock()

            # Should raise ValueError
            with pytest.raises(ValueError, match="Catalog URI not configured"):
                iceberg_store._write_to_iceberg(
                    result=mock_result_df,
                    kwds={"project": "test_project", "entity": "customer"}
                )

    def test_iceberg_write_without_warehouse_path(self):
        """Test error when warehouse path is not configured."""
        with mock.patch("seeknal.workflow.materialization.profile_loader.ProfileLoader") as mock_profile_loader:
            # Mock profile loader with no warehouse
            mock_loader_instance = mock.MagicMock()
            mock_config = mock.MagicMock()
            mock_config.catalog.uri = "https://lakekeeper.example.com"
            mock_config.catalog.warehouse = ""
            mock_loader_instance.load_profile.return_value = mock_config
            mock_profile_loader.return_value = mock_loader_instance

            # Create offline store with Iceberg (no warehouse override)
            iceberg_store = OfflineStore(
                kind=OfflineStoreEnum.ICEBERG,
                value=IcebergStoreOutput(
                    catalog="lakekeeper",
                    namespace="test",
                    table="customer_features"
                )
            )

            # Mock result DataFrame
            mock_result_df = mock.MagicMock()

            # Should raise ValueError
            with pytest.raises(ValueError, match="Warehouse path not configured"):
                iceberg_store._write_to_iceberg(
                    result=mock_result_df,
                    kwds={"project": "test_project", "entity": "customer"}
                )

    def test_iceberg_write_without_configuration(self):
        """Test error when IcebergStoreOutput is None."""
        # Create offline store with None value
        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=None
        )

        # Mock result DataFrame
        mock_result_df = mock.MagicMock()

        # Should raise ValueError
        with pytest.raises(ValueError, match="IcebergStoreOutput configuration required"):
            iceberg_store._write_to_iceberg(
                result=mock_result_df,
                kwds={"project": "test_project", "entity": "customer"}
            )


class TestIcebergCLIIntegration:
    """E2E tests for Iceberg CLI integration."""

    def test_iceberg_available_in_offline_store_enum(self, clean_test_env):
        """Test that ICEBERG is available as an offline store option."""
        # Verify enum has ICEBERG value
        assert OfflineStoreEnum.ICEBERG == "iceberg"

        # Verify all expected values exist
        assert OfflineStoreEnum.HIVE_TABLE == "hive_table"
        assert OfflineStoreEnum.FILE == "file"

    def test_iceberg_store_output_import(self, clean_test_env):
        """Test that IcebergStoreOutput can be imported."""
        # Test import works
        from seeknal.featurestore import IcebergStoreOutput

        # Test instantiation
        config = IcebergStoreOutput(
            catalog="test_catalog",
            namespace="test_namespace",
            table="test_table"
        )

        assert config.catalog == "test_catalog"
        assert config.namespace == "test_namespace"
        assert config.table == "test_table"


class TestIcebergBackwardCompatibility:
    """E2E tests for backward compatibility with existing storage types."""

    def test_hive_table_still_works(self, clean_test_env):
        """Test that HIVE_TABLE storage still works alongside ICEBERG."""
        # Create entity
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        # Create feature group with HIVE_TABLE storage (existing behavior)
        fg = FeatureGroup(
            name="customer_features",
            entity=customer_entity,
            materialization=Materialization(
                event_time_col="event_date",
                offline=True,
                offline_materialization=OfflineMaterialization(
                    store=OfflineStore(
                        kind=OfflineStoreEnum.HIVE_TABLE,
                        value={"database": "seeknal"}
                    ),
                    mode="append"
                )
            )
        )

        # Verify HIVE_TABLE still works
        assert fg.materialization.offline_materialization.store.kind == "hive_table"

    def test_file_storage_still_works(self, clean_test_env):
        """Test that FILE storage still works alongside ICEBERG."""
        # Create entity
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        # Create feature group with FILE storage (existing behavior)
        fg = FeatureGroup(
            name="customer_features",
            entity=customer_entity,
            materialization=Materialization(
                event_time_col="event_date",
                offline=True,
                offline_materialization=OfflineMaterialization(
                    store=OfflineStore(
                        kind=OfflineStoreEnum.FILE,
                        value={"path": "/tmp/test", "kind": "delta"}
                    ),
                    mode="append"
                )
            )
        )

        # Verify FILE still works
        assert fg.materialization.offline_materialization.store.kind == "file"


class TestIcebergDeleteOperation:
    """E2E tests for deleting Iceberg feature groups."""

    @mock.patch("seeknal.workflow.materialization.profile_loader.ProfileLoader")
    @mock.patch("duckdb.connect")
    def test_delete_iceberg_table(
        self, mock_connect, mock_profile_loader, mock_duckdb_iceberg
    ):
        """Test deleting an Iceberg table."""
        # Mock profile loader
        mock_loader_instance = mock.MagicMock()
        mock_config = mock.MagicMock()
        mock_config.catalog.uri = "https://lakekeeper.example.com"
        mock_config.catalog.warehouse = "s3://test-bucket/warehouse"
        mock_config.catalog.bearer_token = None
        mock_loader_instance.load_profile.return_value = mock_config
        mock_profile_loader.return_value = mock_loader_instance

        # Create offline store with Iceberg
        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                catalog="lakekeeper",
                namespace="test",
                table="customer_features"
            )
        )

        # Call delete
        result = iceberg_store.delete(
            project="test_project",
            entity="customer_entity"
        )

        # Verify delete was attempted
        assert result is True
        # The mock connection should have been used for execute
        assert mock_connect.called

    @mock.patch("seeknal.workflow.materialization.profile_loader.ProfileLoader")
    def test_delete_iceberg_without_configuration(self, mock_profile_loader):
        """Test delete error when IcebergStoreOutput is None."""
        # Create offline store with None value
        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=None
        )

        # Call delete
        result = iceberg_store.delete(
            project="test_project",
            entity="customer_entity"
        )

        # Should return False (failed to delete)
        assert result is False
