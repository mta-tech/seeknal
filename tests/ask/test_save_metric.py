"""Tests for seeknal.ask.agents.tools.save_metric."""

import yaml
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context


@pytest.fixture
def project_dir(tmp_path):
    """Create a minimal project directory."""
    (tmp_path / "seeknal").mkdir()
    return tmp_path


@pytest.fixture
def mock_ctx(project_dir):
    """Set up a ToolContext pointing at the temp project."""
    ctx = ToolContext(
        repl=MagicMock(),
        artifact_discovery=MagicMock(),
        project_path=project_dir,
    )
    set_tool_context(ctx)
    return ctx


class TestSaveMetricPreview:
    """Preview mode (confirmed=False) returns YAML without writing."""

    def test_preview_returns_yaml(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "monthly_revenue",
            "measure": "total_revenue",
            "description": "Monthly revenue metric",
        })
        assert "Preview" in result
        assert "monthly_revenue" in result
        assert "total_revenue" in result
        # No file should be written
        assert not (project_dir / "seeknal" / "metrics" / "monthly_revenue.yml").exists()

    def test_preview_ratio_metric(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "conversion_rate",
            "metric_type": "ratio",
            "numerator": "converted_count",
            "denominator": "total_visits",
            "description": "Conversion rate",
        })
        assert "Preview" in result
        assert "conversion_rate" in result
        assert "numerator" in result
        assert "denominator" in result
        assert not (project_dir / "seeknal" / "metrics" / "conversion_rate.yml").exists()


class TestSaveMetricConfirmed:
    """Confirmed mode writes valid metric YAML to seeknal/metrics/."""

    def test_writes_simple_metric(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "monthly_revenue",
            "measure": "total_revenue",
            "description": "Monthly revenue",
            "confirmed": True,
        })
        assert "Saved metric" in result

        target = project_dir / "seeknal" / "metrics" / "monthly_revenue.yml"
        assert target.exists()

        data = yaml.safe_load(target.read_text())
        assert data["kind"] == "metric"
        assert data["name"] == "monthly_revenue"
        assert data["type"] == "simple"
        assert data["measure"] == "total_revenue"
        assert data["description"] == "Monthly revenue"

    def test_writes_ratio_metric(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "conversion_rate",
            "metric_type": "ratio",
            "numerator": "converted_count",
            "denominator": "total_visits",
            "confirmed": True,
        })
        assert "Saved metric" in result

        target = project_dir / "seeknal" / "metrics" / "conversion_rate.yml"
        data = yaml.safe_load(target.read_text())
        assert data["type"] == "ratio"
        assert data["numerator"] == "converted_count"
        assert data["denominator"] == "total_visits"

    def test_auto_creates_metrics_directory(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.save_metric import save_metric

        # Ensure metrics dir does not exist yet
        assert not (project_dir / "seeknal" / "metrics").exists()

        save_metric.invoke({
            "name": "test_metric",
            "measure": "some_measure",
            "confirmed": True,
        })
        assert (project_dir / "seeknal" / "metrics").is_dir()
        assert (project_dir / "seeknal" / "metrics" / "test_metric.yml").exists()

    def test_written_yaml_parseable_by_metric_from_dict(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.save_metric import save_metric
        from seeknal.workflow.semantic.models import Metric

        save_metric.invoke({
            "name": "total_orders",
            "measure": "order_count",
            "description": "Total order count",
            "confirmed": True,
        })

        target = project_dir / "seeknal" / "metrics" / "total_orders.yml"
        data = yaml.safe_load(target.read_text())
        metric = Metric.from_dict(data)
        assert metric.name == "total_orders"
        assert metric.type.value == "simple"
        assert metric.measure == "order_count"
        assert metric.description == "Total order count"


class TestSaveMetricDuplicateDetection:
    """Duplicate names are detected and rejected."""

    def test_rejects_duplicate_from_separate_metric_file(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.save_metric import save_metric

        # Pre-create an existing metric file
        metrics_dir = project_dir / "seeknal" / "metrics"
        metrics_dir.mkdir(parents=True)
        existing = {
            "kind": "metric",
            "name": "total_revenue",
            "type": "simple",
            "measure": "total_revenue",
        }
        (metrics_dir / "total_revenue.yml").write_text(yaml.safe_dump(existing))

        result = save_metric.invoke({
            "name": "total_revenue",
            "measure": "total_revenue",
            "confirmed": True,
        })
        assert "already exists" in result

    def test_rejects_duplicate_from_inline_metric(self, mock_ctx, project_dir):
        from seeknal.ask.agents.tools.save_metric import save_metric

        # Create a semantic model with an inline metric
        sm_dir = project_dir / "seeknal" / "semantic_models"
        sm_dir.mkdir(parents=True)
        sm_yaml = {
            "kind": "semantic_model",
            "name": "orders",
            "model": "ref('transform.orders')",
            "entities": [{"name": "order_id", "type": "primary"}],
            "measures": [{"name": "rev", "expr": "amount", "agg": "sum"}],
            "metrics": [
                {"name": "monthly_rev", "type": "simple", "measure": "rev"},
            ],
        }
        (sm_dir / "orders.yml").write_text(yaml.safe_dump(sm_yaml))

        result = save_metric.invoke({
            "name": "monthly_rev",
            "measure": "rev",
            "confirmed": True,
        })
        assert "already exists" in result

    def test_rejects_duplicate_from_auto_generated_measure(self, mock_ctx, project_dir):
        """Auto-generated metrics from measures should also block duplicates."""
        from seeknal.ask.agents.tools.save_metric import save_metric

        sm_dir = project_dir / "seeknal" / "semantic_models"
        sm_dir.mkdir(parents=True)
        sm_yaml = {
            "kind": "semantic_model",
            "name": "orders",
            "model": "ref('transform.orders')",
            "entities": [{"name": "order_id", "type": "primary"}],
            "measures": [{"name": "total_revenue", "expr": "amount", "agg": "sum"}],
        }
        (sm_dir / "orders.yml").write_text(yaml.safe_dump(sm_yaml))

        result = save_metric.invoke({
            "name": "total_revenue",
            "measure": "total_revenue",
            "confirmed": True,
        })
        assert "already exists" in result


class TestSaveMetricValidation:
    """Invalid inputs are rejected with helpful errors."""

    def test_empty_name_rejected(self, mock_ctx):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "",
            "measure": "total_revenue",
        })
        assert "Invalid metric name" in result

    def test_invalid_name_rejected(self, mock_ctx):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "123-bad-name!",
            "measure": "total_revenue",
        })
        assert "Invalid metric name" in result

    def test_name_with_path_traversal_rejected(self, mock_ctx):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "../escape",
            "measure": "total_revenue",
        })
        assert "Invalid metric name" in result

    def test_simple_metric_requires_measure(self, mock_ctx):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "my_metric",
            "metric_type": "simple",
        })
        assert "require" in result.lower() and "measure" in result.lower()

    def test_ratio_metric_requires_numerator(self, mock_ctx):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "my_ratio",
            "metric_type": "ratio",
            "denominator": "total_visits",
        })
        assert "numerator" in result.lower()

    def test_ratio_metric_requires_denominator(self, mock_ctx):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "my_ratio",
            "metric_type": "ratio",
            "numerator": "converted",
        })
        assert "denominator" in result.lower()

    def test_unsupported_metric_type_rejected(self, mock_ctx):
        from seeknal.ask.agents.tools.save_metric import save_metric

        result = save_metric.invoke({
            "name": "my_metric",
            "metric_type": "unknown_type",
            "measure": "total_revenue",
        })
        assert "Unsupported" in result
