"""
Tests for PipelineContext.features() and PipelineContext.entity() (Phase 2).

Tests cover:
- ctx.features() with valid feature list → correct flat DataFrame
- ctx.features() with as_of → point-in-time filter
- ctx.features() with invalid FG name → clear error
- ctx.entity() returns all struct columns
- ctx.entity() with as_of → filtered
- FileNotFoundError when no consolidated parquet
"""

from pathlib import Path

import duckdb
import pandas as pd
import pytest

from seeknal.pipeline.context import PipelineContext
from seeknal.workflow.consolidation.catalog import EntityCatalog, FGCatalogEntry


def _create_consolidated_parquet(target_dir: Path, entity_name: str):
    """Helper: create a consolidated parquet with struct columns for tests."""
    store_dir = target_dir / "feature_store" / entity_name
    store_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = store_dir / "features.parquet"

    con = duckdb.connect()
    try:
        con.execute("""
            CREATE TABLE consolidated AS
            SELECT
                customer_id,
                event_time,
                struct_pack(total_orders := total_orders, total_revenue := total_revenue)
                    AS customer_features,
                struct_pack(avg_price := avg_price, num_products := num_products)
                    AS product_features
            FROM (
                VALUES
                    ('C1', TIMESTAMP '2026-01-01', 10, 100.0, 9.99, 5),
                    ('C1', TIMESTAMP '2026-01-15', 15, 150.0, 10.99, 7),
                    ('C2', TIMESTAMP '2026-01-02', 20, 200.0, 19.99, 10),
                    ('C3', TIMESTAMP '2026-01-03', 30, 300.0, 29.99, 15)
            ) AS t(customer_id, event_time, total_orders, total_revenue, avg_price, num_products)
        """)
        con.execute(f"COPY consolidated TO '{parquet_path}' (FORMAT PARQUET)")
    finally:
        con.close()

    # Write catalog
    catalog = EntityCatalog(
        entity_name=entity_name,
        join_keys=["customer_id"],
        feature_groups={
            "customer_features": FGCatalogEntry(
                name="customer_features",
                features=["total_orders", "total_revenue"],
            ),
            "product_features": FGCatalogEntry(
                name="product_features",
                features=["avg_price", "num_products"],
            ),
        },
    )
    catalog.save(store_dir / "_entity_catalog.json")

    return parquet_path


@pytest.fixture
def ctx_with_data(tmp_path):
    """Create a PipelineContext with consolidated data."""
    _create_consolidated_parquet(tmp_path, "customer")
    return PipelineContext(
        project_path=tmp_path,
        target_dir=tmp_path,
        config={},
    )


class TestContextFeatures:
    def test_features_valid_selection(self, ctx_with_data):
        """Select specific features from multiple FGs."""
        df = ctx_with_data.features(
            "customer",
            ["customer_features.total_revenue", "product_features.avg_price"],
        )

        assert "customer_id" in df.columns
        assert "event_time" in df.columns
        assert "customer_features__total_revenue" in df.columns
        assert "product_features__avg_price" in df.columns
        assert len(df) == 4  # All 4 rows

    def test_features_with_as_of_filter(self, ctx_with_data):
        """as_of filter returns latest row per entity key before cutoff."""
        df = ctx_with_data.features(
            "customer",
            ["customer_features.total_orders"],
            as_of="2026-01-10",
        )

        # C1 has two rows (Jan 1, Jan 15), only Jan 1 is <= Jan 10
        # C2 has Jan 2, C3 has Jan 3 — both <= Jan 10
        assert len(df) == 3

        c1_row = df[df["customer_id"] == "C1"]
        assert len(c1_row) == 1
        assert c1_row.iloc[0]["customer_features__total_orders"] == 10

    def test_features_as_of_latest_per_key(self, ctx_with_data):
        """as_of returns the LATEST row per entity key within the window."""
        df = ctx_with_data.features(
            "customer",
            ["customer_features.total_orders"],
            as_of="2026-01-20",
        )

        # C1 has Jan 1 and Jan 15 — both <= Jan 20, should return Jan 15 (latest)
        c1_row = df[df["customer_id"] == "C1"]
        assert len(c1_row) == 1
        assert c1_row.iloc[0]["customer_features__total_orders"] == 15

    def test_features_invalid_format_raises(self, ctx_with_data):
        """Feature reference without dot raises ValueError."""
        with pytest.raises(ValueError, match="Invalid feature reference"):
            ctx_with_data.features("customer", ["total_revenue"])

    def test_features_missing_entity_raises(self, ctx_with_data):
        """Non-existent entity raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="No consolidated features"):
            ctx_with_data.features("nonexistent", ["fg.feat"])


class TestContextEntity:
    def test_entity_returns_all_struct_columns(self, ctx_with_data):
        """entity() returns DataFrame with struct columns."""
        df = ctx_with_data.entity("customer")

        assert "customer_id" in df.columns
        assert "event_time" in df.columns
        assert "customer_features" in df.columns
        assert "product_features" in df.columns
        assert len(df) == 4

    def test_entity_with_as_of_filter(self, ctx_with_data):
        """entity() with as_of returns latest per key."""
        df = ctx_with_data.entity("customer", as_of="2026-01-10")

        # 3 customers, each with 1 latest row <= Jan 10
        assert len(df) == 3

    def test_entity_missing_raises(self, ctx_with_data):
        """Non-existent entity raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="No consolidated features"):
            ctx_with_data.entity("nonexistent")

    def test_entity_no_as_of_returns_all(self, ctx_with_data):
        """Without as_of, all rows are returned."""
        df = ctx_with_data.entity("customer")
        assert len(df) == 4  # All 4 rows including C1's two entries
