"""Tests for seeknal.ask.agents.tools.query_metric."""

import yaml
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def project_with_semantic(tmp_path):
    """Create a project with semantic models and test data."""
    # Create semantic model YAML
    sm_dir = tmp_path / "seeknal" / "semantic_models"
    sm_dir.mkdir(parents=True)

    sm_yaml = {
        "kind": "semantic_model",
        "name": "orders",
        "model": "ref('transform.orders')",
        "entities": [{"name": "order_id", "type": "primary"}],
        "dimensions": [
            {"name": "region", "type": "categorical"},
            {"name": "ordered_at", "type": "time", "time_granularity": "day"},
        ],
        "measures": [
            {
                "name": "total_revenue",
                "expr": "amount",
                "agg": "sum",
                "description": "Total revenue",
                "aliases": ["revenue", "rev"],
            },
            {
                "name": "order_count",
                "expr": "order_id",
                "agg": "count_distinct",
                "aliases": ["orders"],
            },
        ],
    }
    (sm_dir / "orders.yml").write_text(yaml.safe_dump(sm_yaml))

    # Create test data as parquet
    import duckdb
    import pandas as pd

    intermediate = tmp_path / "target" / "intermediate"
    intermediate.mkdir(parents=True)

    df = pd.DataFrame({
        "order_id": [1, 2, 3, 4, 5],
        "amount": [100.0, 200.0, 150.0, 300.0, 250.0],
        "region": ["US", "US", "EU", "EU", "APAC"],
        "ordered_at": pd.to_datetime([
            "2025-01-01", "2025-01-15", "2025-02-01", "2025-02-15", "2025-03-01",
        ]),
    })
    df.to_parquet(intermediate / "transform_orders.parquet", index=False)

    return tmp_path


@pytest.fixture
def setup_tool_context(project_with_semantic):
    """Set up ToolContext with REPL for the test project."""
    from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
    from seeknal.cli.repl import REPL

    repl = REPL(project_path=project_with_semantic, skip_history=True)
    set_tool_context(ToolContext(
        repl=repl,
        artifact_discovery=MagicMock(),
        project_path=project_with_semantic,
    ))
    return project_with_semantic


class TestQueryMetric:
    def test_single_metric_query(self, setup_tool_context):
        from seeknal.ask.agents.tools.query_metric import query_metric

        result = query_metric.invoke({
            "metrics": "total_revenue",
        })
        assert "total_revenue" in result
        assert "1000" in result or "1,000" in result  # SUM of all amounts

    def test_metric_with_dimension(self, setup_tool_context):
        from seeknal.ask.agents.tools.query_metric import query_metric

        result = query_metric.invoke({
            "metrics": "total_revenue",
            "dimensions": "region",
        })
        assert "region" in result
        assert "US" in result
        assert "EU" in result

    def test_multi_metric_query(self, setup_tool_context):
        from seeknal.ask.agents.tools.query_metric import query_metric

        result = query_metric.invoke({
            "metrics": "total_revenue,order_count",
            "dimensions": "region",
        })
        assert "total_revenue" in result
        assert "order_count" in result

    def test_alias_resolves_to_canonical(self, setup_tool_context):
        """'revenue' alias should resolve to 'total_revenue' metric."""
        from seeknal.ask.agents.tools.query_metric import query_metric

        result = query_metric.invoke({
            "metrics": "revenue",
        })
        assert "total_revenue" in result
        # Should show the canonical name in metadata
        assert "Metrics" in result

    def test_unknown_metric_returns_available(self, setup_tool_context):
        from seeknal.ask.agents.tools.query_metric import query_metric

        result = query_metric.invoke({
            "metrics": "nonexistent_metric",
        })
        assert "Unknown metric" in result
        assert "total_revenue" in result  # Lists available metrics

    def test_no_semantic_models_returns_guidance(self, tmp_path):
        from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
        from seeknal.ask.agents.tools.query_metric import query_metric

        set_tool_context(ToolContext(
            repl=MagicMock(),
            artifact_discovery=MagicMock(),
            project_path=tmp_path,
        ))

        result = query_metric.invoke({"metrics": "revenue"})
        assert "No semantic models found" in result
        assert "seeknal semantic bootstrap" in result

    def test_compiled_sql_in_output(self, setup_tool_context):
        """Output should include the compiled SQL for transparency."""
        from seeknal.ask.agents.tools.query_metric import query_metric

        result = query_metric.invoke({
            "metrics": "total_revenue",
            "dimensions": "region",
        })
        assert "```sql" in result
        assert "SELECT" in result

    def test_filter_applied(self, setup_tool_context):
        from seeknal.ask.agents.tools.query_metric import query_metric

        result = query_metric.invoke({
            "metrics": "total_revenue",
            "dimensions": "region",
            "filters": "region = 'US'",
        })
        assert "Filters" in result
        # Should only show US data
        assert "US" in result


class TestResolveTableNames:
    def test_maps_short_to_prefixed(self):
        from seeknal.ask.agents.tools.query_metric import _resolve_table_names

        sql = "SELECT SUM(amount) FROM orders"
        views = {"transform_orders", "source_customers"}

        result = _resolve_table_names(sql, views)
        assert "transform_orders" in result
        assert "FROM orders" not in result

    def test_preserves_exact_match(self):
        from seeknal.ask.agents.tools.query_metric import _resolve_table_names

        sql = "SELECT * FROM transform_orders"
        views = {"transform_orders"}

        result = _resolve_table_names(sql, views)
        assert "transform_orders" in result

    def test_handles_join(self):
        from seeknal.ask.agents.tools.query_metric import _resolve_table_names

        sql = "SELECT * FROM orders JOIN customers ON orders.cid = customers.id"
        views = {"transform_orders", "source_customers"}

        result = _resolve_table_names(sql, views)
        assert "transform_orders" in result
        assert "source_customers" in result

    def test_no_match_preserves_original(self):
        from seeknal.ask.agents.tools.query_metric import _resolve_table_names

        sql = "SELECT * FROM unknown_table"
        views = {"transform_orders"}

        result = _resolve_table_names(sql, views)
        assert "unknown_table" in result
