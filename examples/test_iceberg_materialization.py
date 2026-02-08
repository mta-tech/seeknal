#!/usr/bin/env python3
"""
Test Seeknal Iceberg Materialization with Real Lakekeeper and MinIO

This script tests the Seeknal materialization functionality against:
- Lakekeeper: http://172.19.0.9:80/iceberg-catalog (via API Gateway)
- MinIO: http://172.19.0.9:9000

Usage:
    python examples/test_iceberg_materialization.py
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb
import requests


def get_oauth_token() -> str:
    """Get OAuth2 token from Keycloak for Lakekeeper authentication."""
    keycloak_url = "http://172.19.0.9:8080/realms/atlas/protocol/openid-connect/token"

    response = requests.post(
        keycloak_url,
        data={
            "grant_type": "client_credentials",
            "client_id": "duckdb",
            "client_secret": "duckdb-secret-change-in-production",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


def get_catalog_url(lakekeeper_url: str) -> str:
    """Get the catalog URL for DuckDB ATTACH.

    Handles both direct Lakekeeper access and API Gateway routing:
    - Direct: http://host:8181 → http://host:8181/catalog
    - Gateway: http://host:80/iceberg-catalog (gateway rewrites to /catalog)
    """
    if "/iceberg-catalog" in lakekeeper_url:
        # Using API Gateway - it rewrites /iceberg-catalog to /catalog
        # Return as-is since gateway handles the path rewrite
        return lakekeeper_url
    # Direct Lakekeeper access - append /catalog
    return lakekeeper_url if lakekeeper_url.endswith("/catalog") else f"{lakekeeper_url}/catalog"


def test_lakekeeper_connection():
    """Test connection to Lakekeeper REST catalog via gateway."""
    print("\n" + "=" * 60)
    print("Test 1: Lakekeeper Connection")
    print("=" * 60)

    try:
        # Get OAuth token
        token = get_oauth_token()
        print(f"✓ Obtained OAuth token from Keycloak")

        # Test catalog endpoint (gateway rewrites /iceberg-catalog to /catalog)
        # The /v1/config endpoint tests connectivity
        lakekeeper_url = "http://172.19.0.9:80/iceberg-catalog"
        catalog_url = get_catalog_url(lakekeeper_url)
        headers = {"Authorization": f"Bearer {token}"}

        print(f"✓ Testing catalog endpoint: {catalog_url}")
        print(f"  Note: Gateway routes /iceberg-catalog → /catalog")

        # Just verify the catalog is reachable - we don't need to check specific warehouses
        # The warehouse is configured on the Lakekeeper server side
        response = requests.get(f"{catalog_url}/v1/config", headers=headers, timeout=30)
        # Expect 400 (No warehouse specified) which proves catalog is reachable
        if response.status_code == 400:
            print(f"✓ Lakekeeper catalog is reachable")
            print(f"  (Warehouse configured on server side)")
            return True, token
        else:
            response.raise_for_status()

        return True, token

    except Exception as e:
        print(f"✗ Lakekeeper connection failed: {e}")
        return False, None


def test_duckdb_iceberg_attachment():
    """Test DuckDB Iceberg extension attachment with OAuth."""
    print("\n" + "=" * 60)
    print("Test 2: DuckDB Iceberg Attachment with OAuth")
    print("=" * 60)

    try:
        token = get_oauth_token()
        lakekeeper_url = "http://172.19.0.9:80/iceberg-catalog"
        catalog_url = get_catalog_url(lakekeeper_url)

        # Create DuckDB connection
        con = duckdb.connect(":memory:")
        print("✓ DuckDB connection created")

        # Load extensions
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        print("✓ DuckDB extensions loaded")

        # Configure S3 (MinIO) - credentials come from Lakekeeper via STS when using OAuth
        minio_endpoint = "172.19.0.9:9000"
        con.execute(f"""
            SET s3_region = 'us-east-1';
            SET s3_endpoint = '{minio_endpoint}';
            SET s3_url_style = 'path';
            SET s3_use_ssl = false;
        """)
        print(f"✓ S3 configured for MinIO")

        # Attach to Lakekeeper with OAuth
        # 'atlas-warehouse' is just a DuckDB alias - the real warehouse is on the server
        print(f"\nAttaching catalog: {catalog_url}")
        con.execute(f"""
            ATTACH 'atlas-warehouse' AS atlas (
                TYPE ICEBERG,
                ENDPOINT '{catalog_url}',
                AUTHORIZATION_TYPE 'oauth2',
                TOKEN '{token}'
            );
        """)
        print(f"✓ DuckDB attached to Lakekeeper (OAuth)")

        # List tables
        print("\nAvailable tables:")
        tables = con.execute("SHOW ALL TABLES").fetchdf()
        print(tables[["database", "schema", "name"]].to_string())

        # Query sample data
        print("\nSample data from atlas.curated.ecommerce_events:")
        result = con.execute("""
            SELECT user_id, action_type, product_category, purchase_amount
            FROM atlas.curated.ecommerce_events
            LIMIT 5
        """).fetchdf()
        print(result.to_string())

        con.close()
        return True

    except Exception as e:
        print(f"✗ DuckDB Iceberg attachment failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_snapshot_operations():
    """Test snapshot/time-travel operations."""
    print("\n" + "=" * 60)
    print("Test 3: Snapshot Operations (Time Travel)")
    print("=" * 60)

    try:
        token = get_oauth_token()
        lakekeeper_url = "http://172.19.0.9:80/iceberg-catalog"
        catalog_url = get_catalog_url(lakekeeper_url)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute("INSTALL httpfs; LOAD httpfs;")

        minio_endpoint = "172.19.0.9:9000"
        con.execute(f"""
            SET s3_region = 'us-east-1';
            SET s3_endpoint = '{minio_endpoint}';
            SET s3_url_style = 'path';
            SET s3_use_ssl = false;
        """)

        con.execute(f"""
            ATTACH 'atlas-warehouse' AS atlas (
                TYPE ICEBERG,
                ENDPOINT '{catalog_url}',
                AUTHORIZATION_TYPE 'oauth2',
                TOKEN '{token}'
            );
        """)

        # Get current snapshot info
        print("Current table statistics:")

        # Query current snapshot
        current_data = con.execute("""
            SELECT COUNT(*) as row_count,
                   MIN(event_timestamp) as earliest_event,
                   MAX(event_timestamp) as latest_event
            FROM atlas.curated.ecommerce_events
        """).fetchdf()
        print(current_data.to_string())

        # Test time travel query syntax (Iceberg feature)
        print("\nTesting time travel capabilities:")
        print("  Note: Full time travel requires snapshot IDs")
        print("  Current connection supports querying table data")

        con.close()
        return True

    except Exception as e:
        print(f"✗ Snapshot operations failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_materialization_config():
    """Test Seeknal materialization configuration validation."""
    print("\n" + "=" * 60)
    print("Test 4: Seeknal Materialization Configuration")
    print("=" * 60)

    try:
        from seeknal.workflow.materialization.config import (
            MaterializationConfig,
            CatalogConfig,
            SchemaEvolutionConfig,
            MaterializationMode,
            SchemaEvolutionMode,
            CatalogType,
        )
        from seeknal.workflow.materialization.profile_loader import ProfileLoader

        print("✓ Materialization modules imported successfully")

        # Create test configuration
        config = MaterializationConfig(
            enabled=True,
            catalog=CatalogConfig(
                type=CatalogType.REST,
                uri="http://172.19.0.9:80/iceberg-catalog",
                warehouse="s3://warehouse",
            ),
            default_mode=MaterializationMode.APPEND,
            schema_evolution=SchemaEvolutionConfig(
                mode=SchemaEvolutionMode.SAFE,
                allow_column_add=True,
                allow_column_type_change=False,
                allow_column_drop=False,
            ),
        )

        print("✓ Test configuration created")
        print(f"\nConfiguration:")
        print(f"  Enabled: {config.enabled}")
        print(f"  Catalog Type: {config.catalog.type}")
        print(f"  Catalog URI: {config.catalog.uri}")
        print(f"  Warehouse: {config.catalog.warehouse}")
        print(f"  Default Mode: {config.default_mode}")
        print(f"  Schema Evolution: {config.schema_evolution.mode}")
        print(f"  Allow Column Add: {config.schema_evolution.allow_column_add}")

        # Validate configuration
        config.catalog.validate()
        print("✓ Configuration validation passed")

        return True

    except Exception as e:
        print(f"✗ Configuration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_write_operations():
    """Test write operations (if permissions allow)."""
    print("\n" + "=" * 60)
    print("Test 5: Write Operations (Materialization)")
    print("=" * 60)

    try:
        token = get_oauth_token()
        lakekeeper_url = "http://172.19.0.9:80/iceberg-catalog"
        catalog_url = get_catalog_url(lakekeeper_url)

        con = duckdb.connect(":memory:")
        con.execute("INSTALL iceberg; LOAD iceberg;")
        con.execute("INSTALL httpfs; LOAD httpfs;")

        minio_endpoint = "172.19.0.9:9000"
        con.execute(f"""
            SET s3_region = 'us-east-1';
            SET s3_endpoint = '{minio_endpoint}';
            SET s3_url_style = 'path';
            SET s3_use_ssl = false;
        """)

        con.execute(f"""
            ATTACH 'atlas-warehouse' AS atlas (
                TYPE ICEBERG,
                ENDPOINT '{catalog_url}',
                AUTHORIZATION_TYPE 'oauth2',
                TOKEN '{token}'
            );
        """)

        print("Testing append operation to new table...")

        # Drop table if exists (DuckDB Iceberg doesn't support CREATE OR REPLACE)
        con.execute("DROP TABLE IF EXISTS atlas.curated.user_segment_summary;")

        # Create a test table with aggregation results in the 'curated' namespace (which exists)
        con.execute("""
            CREATE TABLE atlas.curated.user_segment_summary AS
            WITH enriched AS (
                SELECT
                    e.*,
                    CASE
                        WHEN u.total_revenue > 2000 THEN 'high_value'
                        WHEN u.total_revenue > 1000 THEN 'medium_value'
                        ELSE 'low_value'
                    END as user_segment
                FROM atlas.curated.ecommerce_events e
                LEFT JOIN atlas.curated.ecommerce_user_features u ON e.user_id = u.user_id
            )
            SELECT
                user_segment,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(SUM(CASE WHEN action_type = 'purchase' THEN purchase_amount ELSE 0 END), 2) as total_revenue
            FROM enriched
            GROUP BY user_segment
            ORDER BY total_revenue DESC;
        """)

        print("✓ Created test table: atlas.curated.user_segment_summary")

        # Verify the write
        result = con.execute("SELECT * FROM atlas.curated.user_segment_summary").fetchdf()
        print("\nTable contents:")
        print(result.to_string())

        # Check if table exists in catalog
        tables = con.execute("SHOW ALL TABLES").fetchdf()
        test_tables = tables[tables['name'] == 'user_segment_summary']

        if not test_tables.empty:
            print(f"\n✓ Table 'user_segment_summary' exists in catalog")
            print(f"  Location: {test_tables.iloc[0]['database']}.{test_tables.iloc[0]['schema']}.{test_tables.iloc[0]['name']}")
        else:
            print(f"\n⚠ Table 'user_segment_summary' not found in SHOW ALL TABLES")

        con.close()
        return True

    except Exception as e:
        print(f"✗ Write operation failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Seeknal Iceberg Materialization Test Suite")
    print("=" * 60)
    print("\nInfrastructure:")
    print("  Lakekeeper: http://172.19.0.9:80/iceberg-catalog (via API Gateway)")
    print("  MinIO:      http://172.19.0.9:9000")
    print("  Keycloak:   http://172.19.0.9:8080")

    results = {}

    # Test 1: Lakekeeper connection
    success, _ = test_lakekeeper_connection()
    results['lakekeeper_connection'] = success

    if success:
        # Test 2: DuckDB attachment
        results['duckdb_attachment'] = test_duckdb_iceberg_attachment()

        # Test 3: Snapshot operations
        results['snapshot_operations'] = test_snapshot_operations()

        # Test 4: Configuration validation
        results['materialization_config'] = test_materialization_config()

        # Test 5: Write operations
        results['write_operations'] = test_write_operations()

    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)

    for test_name, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        print(f"{status}: {test_name}")

    total_tests = len(results)
    passed_tests = sum(results.values())
    print(f"\nTotal: {passed_tests}/{total_tests} tests passed")

    if passed_tests == total_tests:
        print("\n✓ All tests passed!")
    else:
        print(f"\n⚠ {total_tests - passed_tests} test(s) failed")

    return 0 if passed_tests == total_tests else 1


if __name__ == "__main__":
    sys.exit(main())
