"""Tests for seeknal query and deploy-metrics CLI commands."""
import pytest
import yaml
from typer.testing import CliRunner

from seeknal.cli.main import app

runner = CliRunner()


@pytest.fixture
def project_with_semantic(tmp_path):
    """Create a project directory with semantic model and metric YAML files."""
    sm_dir = tmp_path / "seeknal" / "semantic_models"
    sm_dir.mkdir(parents=True)

    (sm_dir / "orders.yml").write_text(yaml.dump({
        "kind": "semantic_model",
        "name": "orders",
        "model": "ref('transform.orders')",
        "default_time_dimension": "ordered_at",
        "entities": [
            {"name": "order_id", "type": "primary"},
            {"name": "customer_id", "type": "foreign"},
        ],
        "dimensions": [
            {"name": "ordered_at", "type": "time", "expr": "ordered_at", "time_granularity": "day"},
            {"name": "region", "type": "categorical"},
        ],
        "measures": [
            {"name": "revenue", "expr": "amount", "agg": "sum"},
            {"name": "order_count", "expr": "1", "agg": "sum"},
        ],
    }))

    metrics_dir = tmp_path / "seeknal" / "metrics"
    metrics_dir.mkdir(parents=True)

    (metrics_dir / "revenue.yml").write_text(
        "kind: metric\nname: total_revenue\ntype: simple\nmeasure: revenue\n"
        "---\n"
        "kind: metric\nname: total_orders\ntype: simple\nmeasure: order_count\n"
    )

    return tmp_path


class TestQueryCommand:
    def test_compile_flag_shows_sql(self, project_with_semantic):
        result = runner.invoke(app, [
            "query",
            "--metrics", "total_revenue",
            "--compile",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "SUM(amount)" in result.output
        assert "total_revenue" in result.output

    def test_compile_with_dimensions(self, project_with_semantic):
        result = runner.invoke(app, [
            "query",
            "--metrics", "total_revenue",
            "--dimensions", "region",
            "--compile",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "region" in result.output
        assert "GROUP BY" in result.output

    def test_compile_with_time_grain(self, project_with_semantic):
        result = runner.invoke(app, [
            "query",
            "--metrics", "total_revenue",
            "--dimensions", "ordered_at__month",
            "--compile",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "date_trunc('month', ordered_at)" in result.output

    def test_compile_with_filter(self, project_with_semantic):
        result = runner.invoke(app, [
            "query",
            "--metrics", "total_revenue",
            "--filter", "region = 'US'",
            "--compile",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "WHERE" in result.output
        assert "region = 'US'" in result.output

    def test_compile_with_order_and_limit(self, project_with_semantic):
        result = runner.invoke(app, [
            "query",
            "--metrics", "total_revenue",
            "--dimensions", "region",
            "--order-by", "-total_revenue",
            "--limit", "10",
            "--compile",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "ORDER BY total_revenue DESC" in result.output
        assert "LIMIT 10" in result.output

    def test_multiple_metrics(self, project_with_semantic):
        result = runner.invoke(app, [
            "query",
            "--metrics", "total_revenue,total_orders",
            "--compile",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "total_revenue" in result.output
        assert "total_orders" in result.output

    def test_unknown_metric_fails(self, project_with_semantic):
        result = runner.invoke(app, [
            "query",
            "--metrics", "nonexistent",
            "--compile",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code != 0

    def test_no_semantic_models_fails(self, tmp_path):
        result = runner.invoke(app, [
            "query",
            "--metrics", "total_revenue",
            "--project-path", str(tmp_path),
        ])
        assert result.exit_code != 0
        assert "No semantic models found" in result.output

    def test_filter_injection_blocked(self, project_with_semantic):
        result = runner.invoke(app, [
            "query",
            "--metrics", "total_revenue",
            "--filter", "1; DROP TABLE users",
            "--compile",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code != 0


class TestDeployMetricsCommand:
    def test_dry_run(self, project_with_semantic):
        result = runner.invoke(app, [
            "deploy-metrics",
            "--connection", "starrocks://root@localhost:9030/test",
            "--dry-run",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "mv_total_revenue" in result.output
        assert "CREATE MATERIALIZED VIEW" in result.output

    def test_dry_run_with_refresh_interval(self, project_with_semantic):
        result = runner.invoke(app, [
            "deploy-metrics",
            "--connection", "starrocks://root@localhost:9030/test",
            "--dry-run",
            "--refresh-interval", "1 HOUR",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "1 HOUR" in result.output

    def test_dry_run_with_drop_existing(self, project_with_semantic):
        result = runner.invoke(app, [
            "deploy-metrics",
            "--connection", "starrocks://root@localhost:9030/test",
            "--dry-run",
            "--drop-existing",
            "--project-path", str(project_with_semantic),
        ])
        assert result.exit_code == 0
        assert "DROP MATERIALIZED VIEW" in result.output

    def test_no_semantic_models_fails(self, tmp_path):
        result = runner.invoke(app, [
            "deploy-metrics",
            "--connection", "starrocks://root@localhost:9030/test",
            "--project-path", str(tmp_path),
        ])
        assert result.exit_code != 0
        assert "No semantic models found" in result.output
