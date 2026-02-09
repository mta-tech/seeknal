"""Tests for semantic layer deployment (StarRocks MV generation)."""
from unittest.mock import MagicMock, patch

import pytest

from seeknal.workflow.semantic.models import Metric, SemanticModel
from seeknal.workflow.semantic.compiler import MetricCompiler
from seeknal.workflow.semantic.deploy import DeployConfig, DeployResult, MetricDeployer

ORDERS_MODEL = SemanticModel.from_dict({
    "name": "orders",
    "model": "ref('transform.orders')",
    "default_time_dimension": "ordered_at",
    "entities": [{"name": "order_id", "type": "primary"}],
    "dimensions": [
        {"name": "ordered_at", "type": "time", "expr": "ordered_at", "time_granularity": "day"},
        {"name": "region", "type": "categorical"},
    ],
    "measures": [
        {"name": "revenue", "expr": "amount", "agg": "sum"},
        {"name": "order_count", "expr": "1", "agg": "sum"},
    ],
})

SIMPLE_METRIC = Metric.from_dict({
    "name": "total_revenue",
    "type": "simple",
    "measure": "revenue",
})

ORDERS_METRIC = Metric.from_dict({
    "name": "total_orders",
    "type": "simple",
    "measure": "order_count",
})


def _build_deployer(conn_config=None):
    compiler = MetricCompiler([ORDERS_MODEL], [SIMPLE_METRIC, ORDERS_METRIC])
    return MetricDeployer(compiler, conn_config or {"host": "localhost"})


class TestGenerateMvDdl:
    def test_basic_ddl(self):
        deployer = _build_deployer()
        ddl = deployer.generate_mv_ddl(SIMPLE_METRIC)
        assert "CREATE MATERIALIZED VIEW IF NOT EXISTS mv_total_revenue" in ddl
        assert "REFRESH ASYNC EVERY(INTERVAL 1 DAY)" in ddl
        assert "SUM(amount)" in ddl

    def test_ddl_with_dimensions(self):
        deployer = _build_deployer()
        ddl = deployer.generate_mv_ddl(SIMPLE_METRIC, dimensions=["region"])
        assert "region" in ddl
        assert "GROUP BY" in ddl

    def test_ddl_with_drop_existing(self):
        deployer = _build_deployer()
        config = DeployConfig(drop_existing=True)
        ddl = deployer.generate_mv_ddl(SIMPLE_METRIC, config)
        assert "DROP MATERIALIZED VIEW IF EXISTS mv_total_revenue" in ddl

    def test_ddl_with_custom_refresh(self):
        deployer = _build_deployer()
        config = DeployConfig(refresh_interval="1 HOUR")
        ddl = deployer.generate_mv_ddl(SIMPLE_METRIC, config)
        assert "EVERY(INTERVAL 1 HOUR)" in ddl

    def test_ddl_with_properties(self):
        deployer = _build_deployer()
        config = DeployConfig(properties={"replication_num": "1"})
        ddl = deployer.generate_mv_ddl(SIMPLE_METRIC, config)
        assert "PROPERTIES" in ddl
        assert "replication_num" in ddl

    def test_ddl_with_force_external_table(self):
        deployer = _build_deployer()
        config = DeployConfig(force_external_table_query_rewrite=True)
        ddl = deployer.generate_mv_ddl(SIMPLE_METRIC, config)
        assert "force_external_table_query_rewrite" in ddl


class TestDeploy:
    def test_dry_run(self):
        deployer = _build_deployer()
        results = deployer.deploy([SIMPLE_METRIC, ORDERS_METRIC], dry_run=True)
        assert len(results) == 2
        assert all(r.success for r in results)
        assert results[0].mv_name == "mv_total_revenue"
        assert results[1].mv_name == "mv_total_orders"
        assert "CREATE MATERIALIZED VIEW" in results[0].ddl

    def test_deploy_executes_ddl(self):
        deployer = _build_deployer({"host": "localhost", "port": 9030})

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch(
            "seeknal.connections.starrocks.create_starrocks_connection"
        ) as mock_create:
            mock_create.return_value = mock_conn
            results = deployer.deploy([SIMPLE_METRIC])

        assert len(results) == 1
        assert results[0].success
        mock_cursor.execute.assert_called()
        mock_conn.close.assert_called()

    def test_deploy_handles_connection_error(self):
        deployer = _build_deployer({"host": "bad-host"})

        with patch(
            "seeknal.connections.starrocks.create_starrocks_connection"
        ) as mock_create:
            mock_create.side_effect = Exception("Connection refused")
            results = deployer.deploy([SIMPLE_METRIC])

        assert len(results) == 1
        assert not results[0].success
        assert "Connection refused" in results[0].error


class TestDeployResult:
    def test_success_result(self):
        r = DeployResult(
            metric_name="total_revenue",
            mv_name="mv_total_revenue",
            ddl="CREATE ...",
            success=True,
        )
        assert r.success
        assert r.error is None

    def test_failure_result(self):
        r = DeployResult(
            metric_name="total_revenue",
            mv_name="mv_total_revenue",
            ddl="",
            success=False,
            error="Connection refused",
        )
        assert not r.success
        assert "Connection refused" in r.error
