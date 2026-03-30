"""Tests for seeknal.workflow.semantic.loader."""

import pytest
import yaml


@pytest.fixture
def project_dir(tmp_path):
    """Create a minimal project directory with semantic model YAML."""
    sm_dir = tmp_path / "seeknal" / "semantic_models"
    sm_dir.mkdir(parents=True)
    metrics_dir = tmp_path / "seeknal" / "metrics"
    metrics_dir.mkdir(parents=True)
    return tmp_path


class TestLoadSemanticLayer:
    def test_loads_semantic_models(self, project_dir):
        from seeknal.workflow.semantic.loader import load_semantic_layer

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
                {"name": "total_revenue", "expr": "amount", "agg": "sum",
                 "description": "Total revenue", "aliases": ["revenue", "rev"]},
                {"name": "order_count", "expr": "order_id", "agg": "count_distinct"},
            ],
        }
        (project_dir / "seeknal" / "semantic_models" / "orders.yml").write_text(
            yaml.safe_dump(sm_yaml)
        )

        models, metrics = load_semantic_layer(project_dir)

        assert len(models) == 1
        assert models[0].name == "orders"
        assert len(models[0].measures) == 2
        assert models[0].measures[0].aliases == ["revenue", "rev"]

        # Auto-generated simple metrics from measures
        assert len(metrics) == 2
        assert metrics[0].name == "total_revenue"
        assert metrics[1].name == "order_count"

    def test_loads_separate_metric_files(self, project_dir):
        from seeknal.workflow.semantic.loader import load_semantic_layer

        sm_yaml = {
            "kind": "semantic_model",
            "name": "orders",
            "model": "ref('transform.orders')",
            "entities": [{"name": "order_id", "type": "primary"}],
            "dimensions": [],
            "measures": [
                {"name": "total_revenue", "expr": "amount", "agg": "sum"},
                {"name": "total_orders", "expr": "order_id", "agg": "count_distinct"},
            ],
        }
        (project_dir / "seeknal" / "semantic_models" / "orders.yml").write_text(
            yaml.safe_dump(sm_yaml)
        )

        metric_yaml = {
            "kind": "metric",
            "name": "avg_order_value",
            "type": "ratio",
            "numerator": "total_revenue",
            "denominator": "total_orders",
            "description": "Average order value",
            "aliases": ["AOV"],
        }
        (project_dir / "seeknal" / "metrics" / "aov.yml").write_text(
            yaml.safe_dump(metric_yaml)
        )

        models, metrics = load_semantic_layer(project_dir)

        metric_names = {m.name for m in metrics}
        assert "avg_order_value" in metric_names
        # Check alias preserved
        aov = next(m for m in metrics if m.name == "avg_order_value")
        assert aov.aliases == ["AOV"]

    def test_inline_metrics_take_priority(self, project_dir):
        from seeknal.workflow.semantic.loader import load_semantic_layer

        sm_yaml = {
            "kind": "semantic_model",
            "name": "orders",
            "model": "ref('transform.orders')",
            "entities": [{"name": "order_id", "type": "primary"}],
            "dimensions": [],
            "measures": [
                {"name": "total_revenue", "expr": "amount", "agg": "sum"},
            ],
            "metrics": [
                {"name": "total_revenue", "type": "simple", "measure": "total_revenue",
                 "description": "Inline definition"},
            ],
        }
        (project_dir / "seeknal" / "semantic_models" / "orders.yml").write_text(
            yaml.safe_dump(sm_yaml)
        )

        models, metrics = load_semantic_layer(project_dir)

        # Inline metric takes priority; auto-generated from measure is skipped
        assert len(metrics) == 1
        assert metrics[0].description == "Inline definition"

    def test_auto_generates_simple_metrics_from_measures(self, project_dir):
        from seeknal.workflow.semantic.loader import load_semantic_layer

        sm_yaml = {
            "kind": "semantic_model",
            "name": "orders",
            "model": "ref('transform.orders')",
            "entities": [{"name": "order_id", "type": "primary"}],
            "dimensions": [],
            "measures": [
                {"name": "revenue", "expr": "amount", "agg": "sum",
                 "aliases": ["rev", "total_rev"]},
            ],
        }
        (project_dir / "seeknal" / "semantic_models" / "orders.yml").write_text(
            yaml.safe_dump(sm_yaml)
        )

        _, metrics = load_semantic_layer(project_dir)

        assert len(metrics) == 1
        assert metrics[0].name == "revenue"
        assert metrics[0].type.value == "simple"
        # Aliases should propagate from measure to auto-generated metric
        assert metrics[0].aliases == ["rev", "total_rev"]

    def test_empty_project_returns_empty_lists(self, project_dir):
        from seeknal.workflow.semantic.loader import load_semantic_layer

        models, metrics = load_semantic_layer(project_dir)
        assert models == []
        assert metrics == []

    def test_no_semantic_dirs_returns_empty(self, tmp_path):
        from seeknal.workflow.semantic.loader import load_semantic_layer

        models, metrics = load_semantic_layer(tmp_path)
        assert models == []
        assert metrics == []

    def test_malformed_yaml_skipped_with_warning(self, project_dir):
        from seeknal.workflow.semantic.loader import load_semantic_layer

        (project_dir / "seeknal" / "semantic_models" / "bad.yml").write_text(
            "kind: semantic_model\nname: bad\n  invalid: yaml: here"
        )

        with pytest.warns(UserWarning, match="Skipping malformed"):
            models, metrics = load_semantic_layer(project_dir)

        assert models == []
        assert metrics == []
