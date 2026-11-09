"""Tests for Governed Mode wiring and integration across executors and runners."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.runtime.catalog_io import CatalogIO, NotInRunGrant, get_catalog_io, set_catalog_io
from seeknal.runtime.controller_client import NodeNotAuthorized, RunRevoked
from seeknal.workflow.executors.base import (
    BaseExecutor,
    ExecutionContext,
    ExecutorResult,
)
from seeknal.workflow.executors.source_executor import SourceExecutor
from seeknal.workflow.materialization.dispatcher import MaterializationDispatcher
from seeknal.workflow.materialization.operations import MaterializationOperationError
from seeknal.workflow.parallel import ParallelDAGRunner
from seeknal.workflow.runner import DAGRunner, ExecutionStatus


class FakeConnection:
    """Connection mock that records executed statements and allows query stubs."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def execute(self, sql: str, *args, **kwargs):
        self.calls.append(sql)
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return (0,)


@pytest.fixture
def execution_context(tmp_path: Path):
    return ExecutionContext(
        project_name="test_proj",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
        dry_run=False,
        verbose=False,
    )


@pytest.fixture
def mock_catalog_io():
    cio = MagicMock(spec=CatalogIO)
    cio.check_read = MagicMock()
    cio.check_write = MagicMock()
    cio.attach_iceberg = MagicMock()
    cio.connection = MagicMock()
    cio.node_start = MagicMock()
    return cio


def test_load_iceberg_governed_calls_check_read_and_attach(
    execution_context, mock_catalog_io, monkeypatch
):
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    set_catalog_io(mock_catalog_io)

    node = Node(
        id="source.orders",
        name="orders",
        node_type=NodeType.SOURCE,
        config={"source": "iceberg", "table": "warehouse.raw.orders"},
    )
    executor = SourceExecutor(node, execution_context)
    con = FakeConnection()

    executor._load_iceberg(con, "warehouse.raw.orders", {})

    # Verifies check_read and attach_iceberg were called
    mock_catalog_io.check_read.assert_called_once_with("warehouse.raw.orders")
    mock_catalog_io.attach_iceberg.assert_called_once_with(con, "warehouse")

    # Verifies S3 settings were never executed
    all_sql = " ".join(con.calls).lower()
    assert "set s3_access_key_id" not in all_sql
    assert "set s3_secret_access_key" not in all_sql
    assert "set s3_use_ssl" not in all_sql

    set_catalog_io(None)


def test_load_iceberg_ungoverned_uses_old_code_path(
    execution_context, monkeypatch
):
    monkeypatch.delenv("ATLAS_RUNTIME_MODE", raising=False)
    set_catalog_io(None)

    assert get_catalog_io() is None

    node = Node(
        id="source.orders",
        name="orders",
        node_type=NodeType.SOURCE,
        config={"source": "iceberg", "table": "warehouse.raw.orders"},
    )
    executor = SourceExecutor(node, execution_context)
    con = FakeConnection()

    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("LAKEKEEPER_URI", "http://lakekeeper:8181")

    executor._load_iceberg(con, "warehouse.raw.orders", {})

    # In ungoverned mode, old code sets S3 settings and runs ATTACH
    all_sql = " ".join(con.calls).lower()
    assert "set s3_access_key_id = 'minioadmin'" in all_sql
    assert "set s3_use_ssl = false" in all_sql
    assert any("attach 'seeknal-warehouse' as warehouse" in c.lower() for c in con.calls)


def test_dispatcher_iceberg_write_governed_calls_check_write(
    mock_catalog_io, monkeypatch
):
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    set_catalog_io(mock_catalog_io)

    dispatcher = MaterializationDispatcher()
    con = FakeConnection()
    target_config = {
        "type": "iceberg",
        "table": "warehouse.analytics.monthly_revenue",
        "mode": "append",
    }

    with patch("seeknal.workflow.materialization.operations.write_to_iceberg") as mock_write:
        mock_write.return_value = MagicMock(success=True, row_count=100)
        dispatcher._materialize_iceberg(con, "v_monthly_revenue", target_config)

    mock_catalog_io.check_write.assert_called_once_with("warehouse.analytics.monthly_revenue")
    set_catalog_io(None)


def test_dispatcher_iceberg_write_ungoverned_skips_check_write(monkeypatch):
    monkeypatch.delenv("ATLAS_RUNTIME_MODE", raising=False)
    set_catalog_io(None)

    dispatcher = MaterializationDispatcher()
    con = FakeConnection()
    target_config = {
        "type": "iceberg",
        "table": "warehouse.analytics.monthly_revenue",
        "mode": "append",
    }

    with patch("seeknal.workflow.materialization.operations.write_to_iceberg") as mock_write:
        mock_write.return_value = MagicMock(success=True, row_count=100)
        dispatcher._materialize_iceberg(con, "v_monthly_revenue", target_config)

    # In ungoverned mode, get_catalog_io() is None
    assert get_catalog_io() is None


def test_pyiceberg_advanced_path_refused_in_governed_mode(
    mock_catalog_io, monkeypatch
):
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    set_catalog_io(mock_catalog_io)

    dispatcher = MaterializationDispatcher()
    con = FakeConnection()
    target_config = {
        "type": "iceberg",
        "table": "warehouse.analytics.orders",
        "mode": "upsert",
        "unique_keys": ["order_id"],
    }

    with pytest.raises(MaterializationOperationError, match="PyIceberg advanced mutation modes are not supported"):
        dispatcher._materialize_iceberg(con, "v_orders", target_config)

    set_catalog_io(None)


def test_postgres_source_and_materialization_governed_resolves_connection(
    execution_context, mock_catalog_io, monkeypatch
):
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    set_catalog_io(mock_catalog_io)

    mock_catalog_io.connection.side_effect = lambda name, action: {
        "host": "postgres.internal",
        "port": 5432,
        "database": "governed_db",
        "user": f"{action}_user",
        "password": "secret",
    }

    # 1. Postgres source config resolution
    node = Node(
        id="source.pg_orders",
        name="pg_orders",
        node_type=NodeType.SOURCE,
        config={"source": "postgresql", "connection": "pg_conn"},
    )
    executor = SourceExecutor(node, execution_context)
    pg_config = executor._resolve_postgresql_config({"connection": "pg_conn"})

    mock_catalog_io.connection.assert_called_with("pg_conn", "read")
    assert pg_config.host == "postgres.internal"
    assert pg_config.user == "read_user"

    # 2. Postgres materialization resolution
    dispatcher = MaterializationDispatcher()
    con = FakeConnection()
    target_config = {
        "type": "postgresql",
        "connection": "pg_dest",
        "table": "public.orders_summary",
        "mode": "full",
    }

    with patch("seeknal.workflow.materialization.postgresql.PostgresMaterializationHelper.materialize") as mock_mat:
        mock_mat.return_value = MagicMock(success=True)
        with patch.object(dispatcher, "_ensure_pg_schema"):
            dispatcher._materialize_postgresql(con, "v_orders", target_config)

    mock_catalog_io.connection.assert_called_with("pg_dest", "write")
    set_catalog_io(None)


def test_node_start_run_revoked_fails_node_and_stops_dag_runner(
    execution_context, mock_catalog_io, monkeypatch, tmp_path
):
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    set_catalog_io(mock_catalog_io)

    mock_catalog_io.node_start.side_effect = RunRevoked("Run grant revoked by administrator")

    node1 = Node(
        id="source.n1",
        name="n1",
        node_type=NodeType.SOURCE,
        config={"source": "iceberg", "table": "warehouse.raw.t1"},
    )
    node2 = Node(
        id="source.n2",
        name="n2",
        node_type=NodeType.SOURCE,
        config={"source": "iceberg", "table": "warehouse.raw.t2"},
    )

    manifest = Manifest(project="test")
    manifest.add_node(node1)
    manifest.add_node(node2)

    runner = DAGRunner(
        manifest=manifest,
        target_path=tmp_path / "target",
        exec_context=execution_context,
    )

    # Even with continue_on_error=True, RunRevoked must halt subsequent nodes
    summary = runner.run(continue_on_error=True)

    assert summary.failed_nodes == 1
    assert len(summary.results) == 1
    failed_result = summary.results[0]
    assert failed_result.node_id == "source.n1"
    assert failed_result.status == ExecutionStatus.FAILED
    assert "Run grant revoked" in failed_result.error_message

    # Node 2 was never scheduled
    assert not any(r.node_id == "source.n2" for r in summary.results)

    set_catalog_io(None)


def test_node_start_not_authorized_fails_node_and_stops_parallel_runner(
    execution_context, mock_catalog_io, monkeypatch, tmp_path
):
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")
    set_catalog_io(mock_catalog_io)

    mock_catalog_io.node_start.side_effect = NodeNotAuthorized("Node not authorized in grant")

    node1 = Node(
        id="source.n1",
        name="n1",
        node_type=NodeType.SOURCE,
        config={"source": "iceberg", "table": "warehouse.raw.t1"},
    )
    node2 = Node(
        id="source.n2",
        name="n2",
        node_type=NodeType.SOURCE,
        config={"source": "iceberg", "table": "warehouse.raw.t2"},
    )

    manifest = Manifest(project="test")
    manifest.add_node(node1)
    manifest.add_node(node2)
    # node1 in layer 1, node2 in layer 2
    manifest.add_edge("source.n1", "source.n2")

    runner = DAGRunner(
        manifest=manifest,
        target_path=tmp_path / "target",
        exec_context=execution_context,
    )
    parallel_runner = ParallelDAGRunner(runner, continue_on_error=True)

    summary = parallel_runner.run(nodes_to_run={"source.n1", "source.n2"})

    assert summary.failed_nodes == 1
    assert any("not authorized" in (r.error_message or "").lower() for r in summary.results)
    # n2 should not have run
    assert not any(r.node_id == "source.n2" and r.status == ExecutionStatus.SUCCESS for r in summary.results)

    set_catalog_io(None)
