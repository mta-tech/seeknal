"""
Tests for FeatureFrame class.

Tests cover:
- FeatureFrame creation and metadata access
- DataFrame delegation: __getitem__, __len__, shape, columns, head, describe
- .to_df() returns plain DataFrame
- .pit_join() with correct PIT semantics
- .pit_join() with keep_cols filtering
- .pit_join() with no matching features (NULL handling)
- .as_of() returns filtered DataFrame
- .pit_join() with multiple entity keys
"""

import pandas as pd
import pytest

from seeknal.pipeline.feature_frame import FeatureFrame


@pytest.fixture
def sample_features():
    """Feature data with two time points per customer."""
    return pd.DataFrame({
        "customer_id": ["C1", "C1", "C2", "C2"],
        "total_orders": [5, 10, 3, 7],
        "total_revenue": [100.0, 200.0, 50.0, 150.0],
        "event_time": pd.to_datetime([
            "2026-01-01", "2026-01-15", "2026-01-01", "2026-01-15"
        ]),
    })


@pytest.fixture
def sample_spine():
    """Spine with label dates between feature timestamps."""
    return pd.DataFrame({
        "customer_id": ["C1", "C2"],
        "churned": [0, 1],
        "label_date": pd.to_datetime(["2026-01-10", "2026-01-20"]),
    })


@pytest.fixture
def ff(sample_features):
    """FeatureFrame wrapping sample features."""
    return FeatureFrame(
        df=sample_features,
        entity_name="customer",
        join_keys=["customer_id"],
        event_time_col="event_time",
    )


class TestFeatureFrameCreation:
    def test_metadata_access(self, ff):
        assert ff.entity_name == "customer"
        assert ff.join_keys == ["customer_id"]
        assert ff.event_time_col == "event_time"

    def test_shape(self, ff):
        assert ff.shape == (4, 4)

    def test_columns(self, ff):
        assert list(ff.columns) == [
            "customer_id", "total_orders", "total_revenue", "event_time"
        ]

    def test_len(self, ff):
        assert len(ff) == 4

    def test_empty(self, ff):
        assert not ff.empty

    def test_dtypes(self, ff):
        assert "customer_id" in ff.dtypes.index


class TestDataFrameDelegation:
    def test_getitem_single_column(self, ff):
        result = ff["customer_id"]
        assert list(result) == ["C1", "C1", "C2", "C2"]

    def test_getitem_multiple_columns(self, ff):
        result = ff[["customer_id", "total_orders"]]
        assert result.shape == (4, 2)

    def test_head(self, ff):
        result = ff.head(2)
        assert len(result) == 2

    def test_tail(self, ff):
        result = ff.tail(2)
        assert len(result) == 2

    def test_describe(self, ff):
        result = ff.describe()
        assert "total_orders" in result.columns

    def test_contains(self, ff):
        assert "customer_id" in ff

    def test_iter(self, ff):
        cols = list(ff)
        assert "customer_id" in cols

    def test_repr(self, ff):
        r = repr(ff)
        assert "FeatureFrame" in r
        assert "customer" in r

    def test_copy(self, ff):
        ff_copy = ff.copy()
        assert ff_copy.entity_name == ff.entity_name
        assert ff_copy.shape == ff.shape
        # Verify it's a true copy
        ff_copy._df.iloc[0, 1] = 999
        assert ff._df.iloc[0, 1] != 999

    def test_fillna(self, ff):
        result = ff.fillna(0)
        assert isinstance(result, pd.DataFrame)

    def test_merge(self, ff, sample_spine):
        result = ff.merge(sample_spine, on="customer_id")
        assert "churned" in result.columns


class TestToDF:
    def test_returns_plain_dataframe(self, ff):
        result = ff.to_df()
        assert isinstance(result, pd.DataFrame)
        assert not isinstance(result, FeatureFrame)
        assert result.shape == (4, 4)

    def test_is_same_object(self, ff):
        result = ff.to_df()
        assert result is ff._df


class TestPITJoin:
    def test_basic_pit_join(self, ff, sample_spine):
        """C1 at Jan 10 should get Jan 1 features (5 orders).
        C2 at Jan 20 should get Jan 15 features (7 orders)."""
        result = ff.pit_join(spine=sample_spine, date_col="label_date")

        assert isinstance(result, pd.DataFrame)
        assert not isinstance(result, FeatureFrame)

        c1 = result[result["customer_id"] == "C1"]
        c2 = result[result["customer_id"] == "C2"]

        assert len(c1) == 1
        assert len(c2) == 1
        assert c1["total_orders"].iloc[0] == 5  # Jan 1 features (before Jan 10)
        assert c2["total_orders"].iloc[0] == 7  # Jan 15 features (before Jan 20)

    def test_pit_join_with_keep_cols(self, ff, sample_spine):
        result = ff.pit_join(
            spine=sample_spine,
            date_col="label_date",
            keep_cols=["customer_id", "churned"],
        )

        assert "customer_id" in result.columns
        assert "churned" in result.columns
        assert "total_orders" in result.columns  # feature columns kept
        assert "label_date" in result.columns  # date_col always kept

    def test_pit_join_no_matching_features(self):
        """When spine date is before all features, result should have NULLs."""
        features_df = pd.DataFrame({
            "customer_id": ["C1"],
            "total_orders": [10],
            "event_time": pd.to_datetime(["2026-02-01"]),
        })
        spine = pd.DataFrame({
            "customer_id": ["C1"],
            "label_date": pd.to_datetime(["2026-01-01"]),  # Before any features
        })
        ff = FeatureFrame(features_df, "customer", ["customer_id"], "event_time")
        result = ff.pit_join(spine=spine, date_col="label_date")

        assert len(result) == 1
        assert pd.isna(result["total_orders"].iloc[0])

    def test_pit_join_returns_latest_before_date(self):
        """When multiple features exist before the date, pick the latest."""
        features_df = pd.DataFrame({
            "customer_id": ["C1", "C1", "C1"],
            "total_orders": [1, 5, 10],
            "event_time": pd.to_datetime([
                "2026-01-01", "2026-01-10", "2026-01-20"
            ]),
        })
        spine = pd.DataFrame({
            "customer_id": ["C1"],
            "label_date": pd.to_datetime(["2026-01-15"]),
        })
        ff = FeatureFrame(features_df, "customer", ["customer_id"], "event_time")
        result = ff.pit_join(spine=spine, date_col="label_date")

        assert result["total_orders"].iloc[0] == 5  # Jan 10, not Jan 1

    def test_pit_join_multiple_entity_keys(self):
        """PIT join with composite entity key (two columns)."""
        features_df = pd.DataFrame({
            "store_id": ["S1", "S1", "S2"],
            "product_id": ["P1", "P1", "P1"],
            "sales": [100, 200, 300],
            "event_time": pd.to_datetime([
                "2026-01-01", "2026-01-15", "2026-01-10"
            ]),
        })
        spine = pd.DataFrame({
            "store_id": ["S1", "S2"],
            "product_id": ["P1", "P1"],
            "label_date": pd.to_datetime(["2026-01-10", "2026-01-20"]),
        })
        ff = FeatureFrame(
            features_df, "store_product",
            ["store_id", "product_id"], "event_time"
        )
        result = ff.pit_join(spine=spine, date_col="label_date")

        s1 = result[(result["store_id"] == "S1") & (result["product_id"] == "P1")]
        s2 = result[(result["store_id"] == "S2") & (result["product_id"] == "P1")]
        assert s1["sales"].iloc[0] == 100  # Jan 1 (before Jan 10)
        assert s2["sales"].iloc[0] == 300  # Jan 10 (before Jan 20)

    def test_pit_join_preserves_spine_columns(self, ff, sample_spine):
        """Spine columns like 'churned' should appear in the result."""
        result = ff.pit_join(spine=sample_spine, date_col="label_date")
        assert "churned" in result.columns
        assert "label_date" in result.columns
        assert "customer_id" in result.columns


class TestAsOf:
    def test_as_of_filters_correctly(self, ff):
        """as_of('2026-01-10') should return Jan 1 features only."""
        result = ff.as_of("2026-01-10")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # One per customer
        c1 = result[result["customer_id"] == "C1"]
        assert c1["total_orders"].iloc[0] == 5

    def test_as_of_returns_latest_per_entity(self, ff):
        """as_of('2026-02-01') should return Jan 15 features (latest)."""
        result = ff.as_of("2026-02-01")

        assert len(result) == 2
        c1 = result[result["customer_id"] == "C1"]
        assert c1["total_orders"].iloc[0] == 10  # Jan 15 features

    def test_as_of_no_matching_returns_empty(self):
        """as_of before any features should return empty DataFrame."""
        features_df = pd.DataFrame({
            "customer_id": ["C1"],
            "total_orders": [10],
            "event_time": pd.to_datetime(["2026-02-01"]),
        })
        ff = FeatureFrame(features_df, "customer", ["customer_id"], "event_time")
        result = ff.as_of("2026-01-01")
        assert len(result) == 0
