"""
Integration tests for FeatureFrame with PipelineContext and FeatureGroupExecutor.

Tests cover:
- ctx.ref("feature_group.X") returns FeatureFrame when metadata sidecar exists
- ctx.ref("source.X") returns plain DataFrame
- ctx.ref("feature_group.X") returns plain DataFrame when no sidecar (backward compat)
- ctx._store_output() for FeatureFrame writes parquet + sidecar
- ctx._store_output() for plain DataFrame writes parquet only
- ctx.features() with spine does PIT join
- ctx.features() without spine preserves current behavior
- ctx.features() with spine but no date_col raises ValueError
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from seeknal.pipeline.context import PipelineContext
from seeknal.pipeline.feature_frame import FeatureFrame


@pytest.fixture
def target_dir(tmp_path):
    """Create a target dir with intermediate directory."""
    intermediate = tmp_path / "intermediate"
    intermediate.mkdir()
    return tmp_path


@pytest.fixture
def ctx(target_dir):
    """PipelineContext pointing at the test target_dir."""
    return PipelineContext(
        project_path=target_dir.parent,
        target_dir=target_dir,
        config={},
    )


class TestCtxRefFeatureFrame:
    def test_ref_returns_feature_frame_with_sidecar(self, ctx, target_dir):
        """ctx.ref('feature_group.X') returns FeatureFrame when _meta.json exists."""
        intermediate = target_dir / "intermediate"

        # Write parquet
        df = pd.DataFrame({
            "customer_id": ["C1", "C2"],
            "total_orders": [10, 20],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
        })
        df.to_parquet(intermediate / "feature_group_cust.parquet", index=False)

        # Write metadata sidecar
        meta = {
            "entity_name": "customer",
            "join_keys": ["customer_id"],
            "event_time_col": "event_time",
        }
        (intermediate / "feature_group_cust_meta.json").write_text(json.dumps(meta))

        result = ctx.ref("feature_group.cust")

        assert isinstance(result, FeatureFrame)
        assert result.entity_name == "customer"
        assert result.join_keys == ["customer_id"]
        assert result.event_time_col == "event_time"
        assert result.shape == (2, 3)

    def test_ref_returns_dataframe_for_source(self, ctx, target_dir):
        """ctx.ref('source.X') returns plain DataFrame."""
        intermediate = target_dir / "intermediate"
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        df.to_parquet(intermediate / "source_test.parquet", index=False)

        result = ctx.ref("source.test")

        assert isinstance(result, pd.DataFrame)
        assert not isinstance(result, FeatureFrame)

    def test_ref_returns_dataframe_for_transform(self, ctx, target_dir):
        """ctx.ref('transform.X') returns plain DataFrame."""
        intermediate = target_dir / "intermediate"
        df = pd.DataFrame({"x": [10]})
        df.to_parquet(intermediate / "transform_test.parquet", index=False)

        result = ctx.ref("transform.test")

        assert isinstance(result, pd.DataFrame)
        assert not isinstance(result, FeatureFrame)

    def test_ref_returns_dataframe_without_sidecar(self, ctx, target_dir):
        """ctx.ref('feature_group.X') returns plain DataFrame when no sidecar."""
        intermediate = target_dir / "intermediate"
        df = pd.DataFrame({"a": [1]})
        df.to_parquet(intermediate / "feature_group_old.parquet", index=False)
        # No _meta.json written

        result = ctx.ref("feature_group.old")

        assert isinstance(result, pd.DataFrame)
        assert not isinstance(result, FeatureFrame)

    def test_ref_caches_feature_frame(self, ctx, target_dir):
        """Second call to ctx.ref() returns cached FeatureFrame."""
        intermediate = target_dir / "intermediate"
        df = pd.DataFrame({
            "customer_id": ["C1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
        })
        df.to_parquet(intermediate / "feature_group_cached.parquet", index=False)
        meta = {"entity_name": "customer", "join_keys": ["customer_id"], "event_time_col": "event_time"}
        (intermediate / "feature_group_cached_meta.json").write_text(json.dumps(meta))

        result1 = ctx.ref("feature_group.cached")
        result2 = ctx.ref("feature_group.cached")

        assert result1 is result2  # Same object from cache

    def test_ref_feature_frame_pit_join_works(self, ctx, target_dir):
        """End-to-end: ctx.ref() → FeatureFrame → .pit_join() → DataFrame."""
        intermediate = target_dir / "intermediate"
        features = pd.DataFrame({
            "customer_id": ["C1", "C1"],
            "total_orders": [5, 10],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-15"]),
        })
        features.to_parquet(intermediate / "feature_group_cust_feat.parquet", index=False)
        meta = {"entity_name": "customer", "join_keys": ["customer_id"], "event_time_col": "event_time"}
        (intermediate / "feature_group_cust_feat_meta.json").write_text(json.dumps(meta))

        ff = ctx.ref("feature_group.cust_feat")
        spine = pd.DataFrame({
            "customer_id": ["C1"],
            "label_date": pd.to_datetime(["2026-01-10"]),
        })
        result = ff.pit_join(spine=spine, date_col="label_date")

        assert isinstance(result, pd.DataFrame)
        assert not isinstance(result, FeatureFrame)
        assert result["total_orders"].iloc[0] == 5  # Jan 1 features


class TestStoreOutputFeatureFrame:
    def test_store_feature_frame_writes_parquet_and_sidecar(self, ctx, target_dir):
        """_store_output() with FeatureFrame writes parquet + sidecar JSON."""
        df = pd.DataFrame({"customer_id": ["C1"], "val": [42]})
        ff = FeatureFrame(df, "customer", ["customer_id"], "event_time")

        output_path = ctx._store_output("feature_group.test_store", ff)

        # Parquet should exist
        assert output_path.exists()
        stored = pd.read_parquet(output_path)
        assert stored.shape == (1, 2)

        # Sidecar should exist
        meta_path = output_path.parent / "feature_group_test_store_meta.json"
        assert meta_path.exists()
        meta = json.loads(meta_path.read_text())
        assert meta["entity_name"] == "customer"
        assert meta["join_keys"] == ["customer_id"]
        assert meta["event_time_col"] == "event_time"

    def test_store_plain_dataframe_no_sidecar(self, ctx, target_dir):
        """_store_output() with plain DataFrame does NOT write sidecar."""
        df = pd.DataFrame({"a": [1, 2]})

        output_path = ctx._store_output("transform.test_plain", df)

        assert output_path.exists()
        meta_path = output_path.parent / "transform_test_plain_meta.json"
        assert not meta_path.exists()

    def test_store_feature_frame_round_trip(self, ctx, target_dir):
        """Store FeatureFrame, then load it back via ref()."""
        df = pd.DataFrame({
            "customer_id": ["C1"],
            "sales": [100],
            "event_time": pd.to_datetime(["2026-01-01"]),
        })
        ff = FeatureFrame(df, "customer", ["customer_id"], "event_time")

        ctx._store_output("feature_group.rt", ff)
        # Clear cache to force reload from disk
        ctx._node_outputs.clear()

        loaded = ctx.ref("feature_group.rt")
        assert isinstance(loaded, FeatureFrame)
        assert loaded.entity_name == "customer"
        assert loaded.shape == (1, 3)

    def test_store_preserves_feature_frame_in_memory(self, ctx):
        """After _store_output(), in-memory cache holds the FeatureFrame."""
        df = pd.DataFrame({"a": [1]})
        ff = FeatureFrame(df, "entity", ["a"], "event_time")

        ctx._store_output("feature_group.mem", ff)

        cached = ctx._node_outputs["feature_group.mem"]
        assert isinstance(cached, FeatureFrame)
        assert cached is ff


class TestCtxFeaturesWithSpine:
    """Tests for ctx.features() with spine PIT join."""

    @pytest.fixture
    def consolidated_ctx(self, target_dir):
        """Context with a consolidated entity parquet."""
        import duckdb

        store_dir = target_dir / "feature_store" / "customer"
        store_dir.mkdir(parents=True, exist_ok=True)
        parquet_path = store_dir / "features.parquet"

        con = duckdb.connect()
        try:
            con.execute("""
                CREATE TABLE consolidated AS
                SELECT
                    customer_id,
                    event_time,
                    struct_pack(total_orders := total_orders) AS customer_features
                FROM (
                    VALUES
                        ('C1', TIMESTAMP '2026-01-01', 5),
                        ('C1', TIMESTAMP '2026-01-15', 10),
                        ('C2', TIMESTAMP '2026-01-01', 3),
                        ('C2', TIMESTAMP '2026-01-15', 7)
                ) AS t(customer_id, event_time, total_orders)
            """)
            con.execute(f"COPY consolidated TO '{parquet_path}' (FORMAT PARQUET)")
        finally:
            con.close()

        # Write catalog
        from seeknal.workflow.consolidation.catalog import EntityCatalog, FGCatalogEntry
        catalog = EntityCatalog(
            entity_name="customer",
            join_keys=["customer_id"],
            feature_groups={
                "customer_features": FGCatalogEntry(
                    name="customer_features",
                    features=["total_orders"],
                ),
            },
        )
        catalog.save(store_dir / "_entity_catalog.json")

        return PipelineContext(
            project_path=target_dir.parent,
            target_dir=target_dir,
            config={},
        )

    def test_features_with_spine_pit_join(self, consolidated_ctx):
        """ctx.features() with spine performs correct PIT join."""
        spine = pd.DataFrame({
            "customer_id": ["C1", "C2"],
            "label_date": pd.to_datetime(["2026-01-10", "2026-01-20"]),
        })
        result = consolidated_ctx.features(
            "customer",
            ["customer_features.total_orders"],
            spine=spine,
            date_col="label_date",
        )

        assert isinstance(result, pd.DataFrame)
        c1 = result[result["customer_id"] == "C1"]
        c2 = result[result["customer_id"] == "C2"]
        assert c1["customer_features__total_orders"].iloc[0] == 5  # Jan 1
        assert c2["customer_features__total_orders"].iloc[0] == 7  # Jan 15

    def test_features_with_spine_and_keep_cols(self, consolidated_ctx):
        """keep_cols filters spine columns in the result."""
        spine = pd.DataFrame({
            "customer_id": ["C1"],
            "churned": [1],
            "label_date": pd.to_datetime(["2026-01-10"]),
        })
        result = consolidated_ctx.features(
            "customer",
            ["customer_features.total_orders"],
            spine=spine,
            date_col="label_date",
            keep_cols=["customer_id", "churned"],
        )

        assert "customer_id" in result.columns
        assert "churned" in result.columns
        assert "customer_features__total_orders" in result.columns

    def test_features_without_spine_preserves_behavior(self, consolidated_ctx):
        """Without spine, ctx.features() returns all rows (current behavior)."""
        result = consolidated_ctx.features(
            "customer",
            ["customer_features.total_orders"],
        )
        assert len(result) == 4  # All 4 rows

    def test_features_with_spine_no_date_col_raises(self, consolidated_ctx):
        """spine without date_col raises ValueError."""
        spine = pd.DataFrame({"customer_id": ["C1"]})
        with pytest.raises(ValueError, match="date_col is required"):
            consolidated_ctx.features(
                "customer",
                ["customer_features.total_orders"],
                spine=spine,
            )

    def test_features_as_of_still_works(self, consolidated_ctx):
        """as_of parameter still works (backward compat)."""
        result = consolidated_ctx.features(
            "customer",
            ["customer_features.total_orders"],
            as_of="2026-01-10",
        )
        # Should only return Jan 1 rows (event_time <= 2026-01-10)
        assert len(result) == 2  # C1 and C2 at Jan 1
