"""Tests for Python @second_order_aggregation decorator with built-in SOA engine.

Tests cover:
- Decorator signature: features= required, source= removed, wrapper(ctx)
- Node metadata population (features, id_col, feature_date_col, etc.)
- Integration with @materialize stacking
- Registry registration
"""

import pytest  # ty: ignore[unresolved-import]
from unittest.mock import MagicMock

from seeknal.pipeline.decorators import (  # ty: ignore[unresolved-import]
    second_order_aggregation,
    materialize,
    get_registered_nodes,
    clear_registry,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    """Clear the registry before and after each test."""
    clear_registry()
    yield
    clear_registry()


class TestSecondOrderAggregationDecorator:
    """Tests for @second_order_aggregation decorator with features= parameter."""

    def test_basic_features_required(self):
        """Decorator accepts features= dict and stores in metadata."""

        @second_order_aggregation(
            name="region_metrics",
            id_col="region",
            feature_date_col="order_date",
            features={
                "daily_amount": {"basic": ["sum", "mean"]},
            },
        )
        def region_metrics(ctx):
            pass

        meta = region_metrics._seeknal_node
        assert meta["kind"] == "second_order_aggregation"
        assert meta["name"] == "region_metrics"
        assert meta["id"] == "second_order_aggregation.region_metrics"
        assert meta["id_col"] == "region"
        assert meta["feature_date_col"] == "order_date"
        assert meta["features"] == {"daily_amount": {"basic": ["sum", "mean"]}}

    def test_features_with_all_aggregation_types(self):
        """Features dict supports basic, window, ratio types."""

        features = {
            "daily_amount": {"basic": ["sum", "mean", "max", "stddev"]},
            "recent_spending": {
                "window": [1, 7],
                "basic": ["sum"],
                "source_feature": "daily_amount",
            },
            "spending_trend": {
                "ratio": {"numerator": [1, 7], "denominator": [8, 14], "aggs": ["sum"]},
                "source_feature": "daily_amount",
            },
        }

        @second_order_aggregation(
            name="region_metrics",
            id_col="region",
            feature_date_col="order_date",
            application_date_col="application_date",
            features=features,
        )
        def region_metrics(ctx):
            pass

        meta = region_metrics._seeknal_node
        assert meta["features"] == features
        assert meta["application_date_col"] == "application_date"

    def test_no_source_in_metadata(self):
        """Decorator does not store 'source' in metadata."""

        @second_order_aggregation(
            name="region_metrics",
            id_col="region",
            feature_date_col="order_date",
            features={"daily_amount": {"basic": ["sum"]}},
        )
        def region_metrics(ctx):
            pass

        meta = region_metrics._seeknal_node
        assert "source" not in meta

    def test_wrapper_signature_ctx_only(self):
        """Wrapper takes only ctx (no df parameter)."""
        call_args = []

        @second_order_aggregation(
            name="region_metrics",
            id_col="region",
            feature_date_col="order_date",
            features={"daily_amount": {"basic": ["sum"]}},
        )
        def region_metrics(ctx):
            call_args.append(ctx)
            return "result"

        # Call with a mock ctx that has _store_output
        mock_ctx = MagicMock()
        result = region_metrics(mock_ctx)
        assert call_args == [mock_ctx]
        assert result == "result"
        mock_ctx._store_output.assert_called_once()

    def test_registered_in_node_registry(self):
        """Decorated function is registered in the node registry."""

        @second_order_aggregation(
            name="region_metrics",
            id_col="region",
            feature_date_col="order_date",
            features={"daily_amount": {"basic": ["sum"]}},
        )
        def region_metrics(ctx):
            pass

        nodes = get_registered_nodes()
        assert "second_order_aggregation.region_metrics" in nodes

    def test_optional_fields_default(self):
        """Optional fields default to None/empty."""

        @second_order_aggregation(
            name="test_node",
            id_col="id",
            feature_date_col="date",
            features={"col": {"basic": ["sum"]}},
        )
        def test_node(ctx):
            pass

        meta = test_node._seeknal_node
        assert meta["application_date_col"] is None
        assert meta["description"] is None
        assert meta["owner"] is None
        assert meta["tags"] == []
        assert meta["materialization"] is None

    def test_with_description_and_tags(self):
        """Decorator accepts description, owner, and tags."""

        @second_order_aggregation(
            name="test_node",
            id_col="id",
            feature_date_col="date",
            features={"col": {"basic": ["sum"]}},
            description="Test description",
            owner="data-team",
            tags=["ml", "features"],
        )
        def test_node(ctx):
            pass

        meta = test_node._seeknal_node
        assert meta["description"] == "Test description"
        assert meta["owner"] == "data-team"
        assert meta["tags"] == ["ml", "features"]

    def test_func_stored_in_metadata(self):
        """The original function is stored in metadata for subprocess execution."""

        @second_order_aggregation(
            name="test_node",
            id_col="id",
            feature_date_col="date",
            features={"col": {"basic": ["sum"]}},
        )
        def test_node(ctx):
            return "test_result"

        meta = test_node._seeknal_node
        assert "func" in meta
        assert callable(meta["func"])

    def test_materialize_stacking(self):
        """@materialize can stack on @second_order_aggregation."""

        @materialize(type="postgresql", connection="pg1", table="public.soa_output")
        @second_order_aggregation(
            name="region_metrics",
            id_col="region",
            feature_date_col="order_date",
            features={"daily_amount": {"basic": ["sum"]}},
        )
        def region_metrics(ctx):
            pass

        assert hasattr(region_metrics, "_seeknal_materializations")
        assert len(region_metrics._seeknal_materializations) == 1
        assert region_metrics._seeknal_materializations[0]["type"] == "postgresql"
