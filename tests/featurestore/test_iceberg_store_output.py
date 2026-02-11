"""Unit tests for IcebergStoreOutput model.

Tests cover:
- Dataclass initialization
- Field validation
- Default values
- to_dict() method
"""

import pytest

from seeknal.featurestore.featurestore import IcebergStoreOutput


class TestIcebergStoreOutput:
    """Unit tests for IcebergStoreOutput dataclass."""

    def test_initialization_with_all_fields(self):
        """Test initialization with all fields specified."""
        config = IcebergStoreOutput(
            table="my_table",
            catalog="my_catalog",
            namespace="my_namespace",
            warehouse="s3://my-bucket/warehouse",
            mode="overwrite"
        )

        assert config.table == "my_table"
        assert config.catalog == "my_catalog"
        assert config.namespace == "my_namespace"
        assert config.warehouse == "s3://my-bucket/warehouse"
        assert config.mode == "overwrite"

    def test_initialization_with_required_field_only(self):
        """Test initialization with only the required 'table' field."""
        config = IcebergStoreOutput(table="my_table")

        assert config.table == "my_table"
        # Check defaults
        assert config.catalog == "lakekeeper"
        assert config.namespace == "default"
        assert config.warehouse is None
        assert config.mode == "append"

    def test_default_catalog_value(self):
        """Test that catalog defaults to 'lakekeeper'."""
        config = IcebergStoreOutput(table="test")
        assert config.catalog == "lakekeeper"

    def test_default_namespace_value(self):
        """Test that namespace defaults to 'default'."""
        config = IcebergStoreOutput(table="test")
        assert config.namespace == "default"

    def test_default_warehouse_value(self):
        """Test that warehouse defaults to None."""
        config = IcebergStoreOutput(table="test")
        assert config.warehouse is None

    def test_default_mode_value(self):
        """Test that mode defaults to 'append'."""
        config = IcebergStoreOutput(table="test")
        assert config.mode == "append"

    def test_mode_append(self):
        """Test append mode."""
        config = IcebergStoreOutput(table="test", mode="append")
        assert config.mode == "append"

    def test_mode_overwrite(self):
        """Test overwrite mode."""
        config = IcebergStoreOutput(table="test", mode="overwrite")
        assert config.mode == "overwrite"

    def test_to_dict_with_all_fields(self):
        """Test to_dict() method with all fields."""
        config = IcebergStoreOutput(
            table="my_table",
            catalog="my_catalog",
            namespace="my_ns",
            warehouse="s3://bucket/wh",
            mode="overwrite"
        )

        result = config.to_dict()

        assert result == {
            "catalog": "my_catalog",
            "warehouse": "s3://bucket/wh",
            "namespace": "my_ns",
            "table": "my_table",
            "mode": "overwrite"
        }

    def test_to_dict_with_defaults(self):
        """Test to_dict() method with default values."""
        config = IcebergStoreOutput(table="my_table")

        result = config.to_dict()

        assert result == {
            "catalog": "lakekeeper",
            "warehouse": None,
            "namespace": "default",
            "table": "my_table",
            "mode": "append"
        }

    def test_field_ordering_table_first(self):
        """Test that 'table' is the first field (required parameter)."""
        # This tests that the dataclass is properly defined with
        # required fields before optional fields
        config = IcebergStoreOutput("my_table")
        assert config.table == "my_table"

    def test_warehouse_optional(self):
        """Test that warehouse is optional and can be None."""
        config = IcebergStoreOutput(table="test")
        assert config.warehouse is None

        config2 = IcebergStoreOutput(table="test", warehouse="s3://bucket/wh")
        assert config2.warehouse == "s3://bucket/wh"

    def test_different_catalog_names(self):
        """Test with different catalog names."""
        for catalog_name in ["lakekeeper", "rest", "hive", "glue"]:
            config = IcebergStoreOutput(table="test", catalog=catalog_name)
            assert config.catalog == catalog_name

    def test_different_namespace_values(self):
        """Test with different namespace values."""
        for namespace in ["default", "prod", "staging", "dev"]:
            config = IcebergStoreOutput(table="test", namespace=namespace)
            assert config.namespace == namespace

    def test_warehouse_s3_path(self):
        """Test warehouse with S3 path."""
        config = IcebergStoreOutput(
            table="test",
            warehouse="s3://my-bucket/path/to/warehouse"
        )
        assert config.warehouse == "s3://my-bucket/path/to/warehouse"

    def test_warehouse_gcs_path(self):
        """Test warehouse with GCS path."""
        config = IcebergStoreOutput(
            table="test",
            warehouse="gs://my-bucket/path/to/warehouse"
        )
        assert config.warehouse == "gs://my-bucket/path/to/warehouse"

    def test_warehouse_azure_path(self):
        """Test warehouse with Azure path."""
        config = IcebergStoreOutput(
            table="test",
            warehouse="abfs://container@account.dfs.core.windows.net/path"
        )
        assert config.warehouse == "abfs://container@account.dfs.core.windows.net/path"

    def test_warehouse_local_path(self):
        """Test warehouse with local file path."""
        config = IcebergStoreOutput(
            table="test",
            warehouse="/path/to/local/warehouse"
        )
        assert config.warehouse == "/path/to/local/warehouse"
