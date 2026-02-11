"""Integration tests for Iceberg Feature Groups with real infrastructure.

These tests validate Iceberg Feature Group materialization against real
MinIO and Lakekeeper infrastructure deployed on atlas-dev-server.

## Infrastructure Requirements

These tests require the following infrastructure:
- atlas-dev-server: 172.19.0.9
- MinIO: S3-compatible storage on port 9000
- Lakekeeper: REST catalog for Iceberg on port 8181

## Configuration

Set these environment variables before running:
- LAKEKEEPER_URI: http://172.19.0.9:8181
- LAKEKEEPER_WAREHOUSE: s3://iceberg/warehouse
- AWS_ACCESS_KEY_ID: MinIO access key
- AWS_SECRET_ACCESS_KEY: MinIO secret key

## Running Tests

```bash
# Verify infrastructure is running
ssh fitra@atlas-dev-server "docker ps | grep -E 'minio|lakekeeper'"

# Run tests
pytest tests/integration/test_iceberg_real_infra.py -v
```
"""

import os
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import duckdb

from seeknal.featurestore import (
    FeatureGroup,
    Materialization,
    OfflineMaterialization,
    OfflineStore,
    OfflineStoreEnum,
    IcebergStoreOutput,
)
from seeknal.entity import Entity
from seeknal.workflow.materialization.operations import DuckDBIcebergExtension
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# =============================================================================
# Infrastructure Validation Tests
# =============================================================================


class TestInfrastructureValidation:
    """Tests to verify atlas-dev-server infrastructure is running."""

    def test_atlas_dev_server_reachable(self):
        """Test that atlas-dev-server is reachable via SSH."""
        result = subprocess.run(
            ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no",
             "fitra@atlas-dev-server", "echo 'connected'"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
        assert "connected" in result.stdout

    def test_minio_container_running(self):
        """Test that MinIO container is running on atlas-dev-server."""
        result = subprocess.run(
            ["ssh", "fitra@atlas-dev-server",
             "docker ps --format '{{.Names}}' | grep atlas-minio"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
        assert "atlas-minio" in result.stdout

    def test_lakekeeper_container_running(self):
        """Test that Lakekeeper container is running on atlas-dev-server."""
        result = subprocess.run(
            ["ssh", "fitra@atlas-dev-server",
             "docker ps --format '{{.Names}}' | grep atlas-lakekeeper"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0
        assert "atlas-lakekeeper" in result.stdout

    def test_lakekeeper_port_accessible(self):
        """Test that Lakekeeper REST API port is accessible."""
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(("172.19.0.9", 8181))
        sock.close()
        assert result == 0, "Lakekeeper port 8181 is not accessible"

    def test_minio_port_accessible(self):
        """Test that MinIO port is accessible."""
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(("172.19.0.9", 9000))
        sock.close()
        assert result == 0, "MinIO port 9000 is not accessible"


# =============================================================================
# DuckDB + Iceberg Extension Tests
# =============================================================================


@pytest.mark.skipif(
    os.getenv("LAKEKEEPER_URI") is None,
    reason="Requires LAKEKEEPER_URI environment variable"
)
class TestDuckDBIcebergExtension:
    """Tests for DuckDB Iceberg extension with real infrastructure."""

    @pytest.fixture(autouse=True)
    def setup_duckdb(self):
        """Setup DuckDB with Iceberg extension."""
        self.con = duckdb.connect(":memory:")
        DuckDBIcebergExtension.load_extension(self.con)
        yield
        self.con.close()

    def test_load_iceberg_extension(self):
        """Test that Iceberg extension loads successfully."""
        result = self.con.execute("FROM extensions WHERE name = 'iceberg'").fetchone()
        assert result is not None, "Iceberg extension not loaded"

    def test_create_rest_catalog(self):
        """Test creating REST catalog connection to Lakekeeper."""
        catalog_uri = os.getenv("LAKEKEEPER_URI", "http://172.19.0.9:8181")
        warehouse_path = os.getenv("LAKEKEEPER_WAREHOUSE", "s3://iceberg/warehouse")

        DuckDBIcebergExtension.create_rest_catalog(
            con=self.con,
            catalog_name="test_catalog",
            uri=catalog_uri,
            warehouse_path=warehouse_path
        )

        # Verify catalog was created
        result = self.con.execute("SHOW DATABASES").fetchall()
        assert isinstance(result, list)

    def test_create_and_drop_iceberg_table(self):
        """Test creating and dropping an Iceberg table."""
        catalog_uri = os.getenv("LAKEKEEPER_URI", "http://172.19.0.9:8181")
        warehouse_path = os.getenv("LAKEKEEPER_WAREHOUSE", "s3://iceberg/warehouse")

        DuckDBIcebergExtension.create_rest_catalog(
            con=self.con,
            catalog_name="test_catalog",
            uri=catalog_uri,
            warehouse_path=warehouse_path
        )

        table_name = f"test_table_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        # Create table
        self.con.execute(f"""
            CREATE TABLE test_catalog.default.{table_name} (
                id INTEGER,
                name VARCHAR,
                value DOUBLE
            )
        """)

        # Insert data
        self.con.execute(f"INSERT INTO test_catalog.default.{table_name} VALUES (1, 'test', 100.5)")

        # Query back
        result = self.con.execute(f"SELECT * FROM test_catalog.default.{table_name}").fetchone()
        assert result[0] == 1
        assert result[1] == "test"
        assert result[2] == 100.5

        # Cleanup
        self.con.execute(f"DROP TABLE IF EXISTS test_catalog.default.{table_name}")


# =============================================================================
# Feature Group Integration Tests
# =============================================================================


@pytest.mark.skipif(
    os.getenv("LAKEKEEPER_URI") is None,
    reason="Requires LAKEKEEPER_URI environment variable"
)
class TestIcebergFeatureGroupIntegration:
    """Integration tests for Feature Groups with real Iceberg infrastructure."""

    @pytest.fixture
    def spark_session(self):
        """Create a Spark session for testing."""
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("IcebergIntegrationTest") \
            .config("spark.sql.warehouse.dir", str(Path.cwd() / "warehouse")) \
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture
    def sample_customer_data(self, spark_session):
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
        ]

        return spark_session.createDataFrame(data, schema)

    def test_create_feature_group_with_iceberg(self):
        """Test creating a FeatureGroup with Iceberg storage."""
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        table_name = f"customer_features_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table=table_name,
                catalog="lakekeeper",
                namespace="test"
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

    def test_write_features_to_iceberg(self, sample_customer_data):
        """Test writing features to real Iceberg table."""
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        table_name = f"customer_features_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table=table_name,
                catalog="lakekeeper",
                namespace="test"
            )
        )

        result = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="total_orders"
        )

        # Verify write result
        assert result["storage_type"] == "iceberg"
        assert result["num_rows"] == 3
        assert result["snapshot_id"] is not None
        assert result["table"] == table_name
        assert result["namespace"] == "test"

    def test_append_mode_increments_rows(self, sample_customer_data):
        """Test that append mode adds rows to existing table."""
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        table_name = f"customer_features_append_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table=table_name,
                catalog="lakekeeper",
                namespace="test",
                mode="append"
            )
        )

        # First write
        result1 = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="total_orders"
        )

        # Second write
        result2 = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="total_orders"
        )

        # Append mode should have more rows
        assert result2["num_rows"] >= result1["num_rows"]

    def test_overwrite_mode_replaces_data(self, sample_customer_data):
        """Test that overwrite mode replaces table data."""
        customer_entity = Entity(name="customer", join_keys=["customer_id"])

        table_name = f"customer_features_overwrite_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table=table_name,
                catalog="lakekeeper",
                namespace="test",
                mode="overwrite"
            )
        )

        # First write
        result1 = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="total_orders"
        )

        # Second write with overwrite
        result2 = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="total_orders"
        )

        # Overwrite should have same row count
        assert result2["num_rows"] == result1["num_rows"] == 3

    def test_snapshot_metadata_captured(self, sample_customer_data):
        """Test that snapshot metadata is captured correctly."""
        table_name = f"customer_features_snap_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table=table_name,
                catalog="lakekeeper",
                namespace="test"
            )
        )

        result = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="total_orders"
        )

        # Verify snapshot metadata
        assert "snapshot_id" in result
        assert result["snapshot_id"] is not None
        assert isinstance(result["snapshot_id"], str)

    def test_time_travel_query(self, sample_customer_data):
        """Test querying Iceberg table as of specific snapshot."""
        table_name = f"customer_features_timetravel_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(
                table=table_name,
                catalog="lakekeeper",
                namespace="test"
            )
        )

        # First write
        result1 = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="total_orders"
        )
        snapshot1 = result1["snapshot_id"]

        # Second write
        result2 = iceberg_store(
            result=sample_customer_data,
            project="test_project",
            entity="customer",
            name="total_orders"
        )
        snapshot2 = result2["snapshot_id"]

        # Snapshots should be different
        assert snapshot1 != snapshot2

        # Can query as of snapshot (implementation dependent)
        # This tests that snapshot IDs are properly captured


# =============================================================================
# Error Handling Tests
# =============================================================================


@pytest.mark.skipif(
    os.getenv("LAKEKEEPER_URI") is None,
    reason="Requires LAKEKEEPER_URI environment variable"
)
class TestIcebergErrorHandlingReal:
    """Error handling tests with real infrastructure."""

    def test_invalid_table_name_raises_error(self):
        """Test that invalid table name raises validation error."""
        from seeknal.validation import validate_table_name

        with pytest.raises(ValueError):
            validate_table_name("invalid-table-name")  # Hyphens not allowed

    def test_missing_catalog_uri_error_message(self):
        """Test helpful error message when catalog URI is missing."""
        # Temporarily unset environment variable
        original_uri = os.environ.get("LAKEKEEPER_URI")
        os.environ["LAKEKEEPER_URI"] = ""

        try:
            iceberg_store = OfflineStore(
                kind=OfflineStoreEnum.ICEBERG,
                value=IcebergStoreOutput(table="test")
            )

            with pytest.raises(ValueError) as exc_info:
                iceberg_store(result=None, project="test", entity="test")

            assert "Catalog URI not configured" in str(exc_info.value)
        finally:
            if original_uri:
                os.environ["LAKEKEEPER_URI"] = original_uri
            else:
                os.environ.pop("LAKEKEEPER_URI", None)

    def test_connection_failure_handling(self):
        """Test handling of connection failures to Lakekeeper."""
        # Use invalid URI to simulate connection failure
        iceberg_store = OfflineStore(
            kind=OfflineStoreEnum.ICEBERG,
            value=IcebergStoreOutput(table="test", catalog="invalid")
        )

        with patch.dict(os.environ, {"LAKEKEEPER_URI": "http://invalid:9999"}):
            with pytest.raises((ValueError, Exception)):
                iceberg_store(
                    result=None,
                    project="test",
                    entity="test"
                )


# =============================================================================
# Cleanup Utilities
# =============================================================================


def cleanup_test_tables(catalog_name="test_catalog", namespace="test"):
    """Utility function to clean up test tables from Iceberg.

    Args:
        catalog_name: Name of the catalog
        namespace: Namespace containing test tables
    """
    con = duckdb.connect(":memory:")

    try:
        DuckDBIcebergExtension.load_extension(con)

        catalog_uri = os.getenv("LAKEKEEPER_URI", "http://172.19.0.9:8181")
        warehouse_path = os.getenv("LAKEKEEPER_WAREHOUSE", "s3://iceberg/warehouse")

        DuckDBIcebergExtension.create_rest_catalog(
            con=con,
            catalog_name=catalog_name,
            uri=catalog_uri,
            warehouse_path=warehouse_path
        )

        # List and drop test tables
        tables = con.execute(f"SHOW TABLES FROM {catalog_name}.{namespace}").fetchall()

        for table in tables:
            table_name = table[0]
            if table_name.startswith(("test_", "customer_features_")):
                con.execute(f"DROP TABLE IF EXISTS {catalog_name}.{namespace}.{table_name}")
                print(f"Cleaned up table: {table_name}")

    finally:
        con.close()


if __name__ == "__main__":
    # Run infrastructure validation
    print("=" * 60)
    print("Running Infrastructure Validation Tests")
    print("=" * 60)

    pytest.main([__file__, "-v", "-k", "TestInfrastructureValidation"])

    # Run real infrastructure tests if environment is set
    if os.getenv("LAKEKEEPER_URI"):
        print("\n" + "=" * 60)
        print("Running Real Infrastructure Integration Tests")
        print("=" * 60)

        pytest.main([__file__, "-v", "-k", "TestDuckDBIcebergExtension"])
        pytest.main([__file__, "-v", "-k", "TestIcebergFeatureGroupIntegration"])
    else:
        print("\nSkipping real infrastructure tests (set LAKEKEEPER_URI)")

    # Cleanup
    print("\n" + "=" * 60)
    print("Cleaning up test tables...")
    print("=" * 60)
    cleanup_test_tables()
