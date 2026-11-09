"""Tests for OpenLineage event emission from governed runs."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.runtime.catalog_io import (
    CatalogIO,
    GovernedContext,
    get_catalog_io,
    set_catalog_io,
)
from seeknal.runtime.controller_client import RunRevoked
from seeknal.runtime.lineage import (
    COLUMN_LINEAGE_DATASET_FACET_SCHEMA_URL,
    DATASET_VERSION_DATASET_FACET_SCHEMA_URL,
    ERROR_MESSAGE_RUN_FACET_SCHEMA_URL,
    OPENLINEAGE_SCHEMA_URL,
    PRODUCER,
    RUN_NAMESPACE,
    SCHEMA_DATASET_FACET_SCHEMA_URL,
    LineageEmitter,
)
from seeknal.runtime.run_plan import PlanNode, RunPlan, TableAccess
from seeknal.workflow.executors.base import (
    BaseExecutor,
    ExecutionContext,
    ExecutionStatus,
    ExecutorResult,
    register_executor,
)
from seeknal.workflow.parallel import ParallelDAGRunner
from seeknal.workflow.runner import DAGRunner


class RecordingEmitter:
    """In-memory recording emitter for tests."""

    def __init__(self, buffered: int = 0) -> None:
        self.events: list[dict[str, Any]] = []
        self.flush_count: int = 0
        self._buffered: int = buffered

    def emit(self, event: dict[str, Any]) -> None:
        self.events.append(event)

    def flush(self) -> None:
        self.flush_count += 1

    def stats(self) -> dict[str, int]:
        return {
            "sent": len(self.events),
            "buffered": self._buffered,
            "dropped": 0,
            "failed_attempts": 0,
        }


class FailingEmitter:
    """Emitter that always raises on emit for testing failure resilience."""

    def __init__(self) -> None:
        self.flush_count: int = 0

    def emit(self, event: dict[str, Any]) -> None:
        raise RuntimeError("Controller unreachable / transport failure")

    def flush(self) -> None:
        self.flush_count += 1

    def stats(self) -> dict[str, int]:
        return {"sent": 0, "buffered": 1, "dropped": 0, "failed_attempts": 1}


@dataclass
class DummyGrant:
    jti: str = "550e8400-e29b-41d4-a716-446655440000"
    ws: str = "ws-analytics"
    reads: tuple[TableAccess, ...] = ()
    writes: tuple[TableAccess, ...] = ()
    connections: tuple[Any, ...] = ()


def _make_governed_context(
    nodes: list[PlanNode] | None = None,
    grant_jti: str = "550e8400-e29b-41d4-a716-446655440000",
) -> GovernedContext:
    grant = DummyGrant(jti=grant_jti, ws="ws-analytics")
    plan_nodes = tuple(nodes or [])
    plan = RunPlan(reads=(), writes=(), connections=(), nodes=plan_nodes)
    mock_client = MagicMock()

    return GovernedContext(
        grant=grant,  # type: ignore[arg-type]
        grant_file="/dummy/grant.jwt",
        grant_token="dummy-token",
        jwks_file="/dummy/jwks.json",
        public_keys={},
        lakekeeper_token_file="/dummy/token",
        lakekeeper_uri="http://lakekeeper:8181",
        lakekeeper_warehouse="warehouse",
        run_controller_url="http://controller:8080",
        connections_dir="/dummy/connections",
        controller_client=mock_client,
        plan=plan,
        project="test_proj",
    )


class DummyTransformExecutor(BaseExecutor):
    """Test executor for transform nodes."""

    @property
    def node_type(self) -> NodeType:
        return NodeType.TRANSFORM

    def validate(self) -> None:
        pass

    def execute(self) -> ExecutorResult:
        if self.node.config.get("fail_execute"):
            raise ValueError("Intentional test execution failure")
        return ExecutorResult(
            node_id=self.node.id,
            status=ExecutionStatus.SUCCESS,
            row_count=100,
            metadata={
                "columns": [{"name": "order_id", "type": "bigint"}, {"name": "amount", "type": "double"}],
                "snapshot_ids": {"warehouse.analytics.daily_orders": 999988887777},
            },
        )


@pytest.fixture(autouse=True)
def clean_catalog_io():
    """Ensure catalog_io is cleanly reset before and after every test."""
    from seeknal.workflow.executors.base import ExecutorRegistry

    registry = ExecutorRegistry.get_instance()
    orig = registry._executors.get(NodeType.TRANSFORM)
    registry._executors[NodeType.TRANSFORM] = DummyTransformExecutor

    set_catalog_io(None)
    yield
    set_catalog_io(None)

    if orig is not None:
        registry._executors[NodeType.TRANSFORM] = orig
    else:
        registry._executors.pop(NodeType.TRANSFORM, None)


def test_start_and_complete_emitted_for_successful_node(tmp_path, monkeypatch):
    """START and COMPLETE emitted for a successful node, with right runId and datasets."""
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")

    plan_node = PlanNode(
        id="transform.daily_orders",
        kind="transform",
        reads=("warehouse.raw.orders",),
        writes=("warehouse.analytics.daily_orders",),
        connections=(),
    )
    ctx = _make_governed_context(nodes=[plan_node])
    cio = CatalogIO(ctx, project="ecommerce")
    recording_emitter = RecordingEmitter()
    cio.lineage_emitter = recording_emitter
    set_catalog_io(cio)

    node = Node(
        id="transform.daily_orders",
        name="daily_orders",
        node_type=NodeType.TRANSFORM,
        config={
            "transform": "SELECT order_id, amount FROM orders",
            "inputs": {"orders": "ref:source.orders"},
        },
    )
    manifest = Manifest(project="ecommerce")
    manifest.add_node(node)

    exec_ctx = ExecutionContext(
        project_name="ecommerce",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(manifest=manifest, target_path=tmp_path / "target", exec_context=exec_ctx)
    summary = runner.run()

    assert summary.successful_nodes == 1
    assert len(recording_emitter.events) == 2

    # 1. Inspect START event
    start_ev = recording_emitter.events[0]
    assert start_ev["eventType"] == "START"
    expected_run_id = str(uuid.uuid5(RUN_NAMESPACE, f"{ctx.grant.jti}:transform.daily_orders"))
    assert start_ev["run"]["runId"] == expected_run_id
    assert start_ev["job"]["name"] == "transform.daily_orders"
    assert start_ev["job"]["namespace"] == "atlas://ws-analytics/ecommerce"
    assert len(start_ev["inputs"]) == 1
    assert start_ev["inputs"][0]["name"] == "raw.orders"
    assert start_ev["inputs"][0]["namespace"] == "iceberg://lakekeeper:8181/warehouse"
    assert len(start_ev["outputs"]) == 1
    assert start_ev["outputs"][0]["name"] == "analytics.daily_orders"
    assert start_ev["outputs"][0]["namespace"] == "iceberg://lakekeeper:8181/warehouse"

    # 2. Inspect COMPLETE event
    comp_ev = recording_emitter.events[1]
    assert comp_ev["eventType"] == "COMPLETE"
    assert comp_ev["run"]["runId"] == expected_run_id
    assert comp_ev["job"]["name"] == "transform.daily_orders"
    assert len(comp_ev["inputs"]) == 1
    assert len(comp_ev["outputs"]) == 1

    # Output dataset facets: schema, columnLineage, version
    out_ds = comp_ev["outputs"][0]
    assert "facets" in out_ds
    assert "schema" in out_ds["facets"]
    fields = out_ds["facets"]["schema"]["fields"]
    assert {"name": "order_id", "type": "bigint"} in fields
    assert {"name": "amount", "type": "double"} in fields

    assert "version" in out_ds["facets"]
    assert out_ds["facets"]["version"]["datasetVersion"] == "999988887777"

    assert "columnLineage" in out_ds["facets"]
    col_fields = out_ds["facets"]["columnLineage"]["fields"]
    assert "amount" in col_fields
    assert col_fields["amount"]["inputFields"][0]["name"] == "raw.orders"
    assert col_fields["amount"]["inputFields"][0]["field"] == "amount"


def test_fail_emitted_on_executor_error(tmp_path, monkeypatch):
    """FAIL emitted on an executor error with error details."""
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")

    plan_node = PlanNode(
        id="transform.bad_node",
        kind="transform",
        reads=("warehouse.raw.orders",),
        writes=(),
        connections=(),
    )
    ctx = _make_governed_context(nodes=[plan_node])
    cio = CatalogIO(ctx, project="ecommerce")
    recording_emitter = RecordingEmitter()
    cio.lineage_emitter = recording_emitter
    set_catalog_io(cio)

    node = Node(
        id="transform.bad_node",
        name="bad_node",
        node_type=NodeType.TRANSFORM,
        config={"fail_execute": True},
    )
    manifest = Manifest(project="ecommerce")
    manifest.add_node(node)

    exec_ctx = ExecutionContext(
        project_name="ecommerce",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(manifest=manifest, target_path=tmp_path / "target", exec_context=exec_ctx)
    summary = runner.run()

    assert summary.failed_nodes == 1
    assert len(recording_emitter.events) == 2

    start_ev = recording_emitter.events[0]
    assert start_ev["eventType"] == "START"

    fail_ev = recording_emitter.events[1]
    assert fail_ev["eventType"] == "FAIL"
    expected_run_id = str(uuid.uuid5(RUN_NAMESPACE, f"{ctx.grant.jti}:transform.bad_node"))
    assert fail_ev["run"]["runId"] == expected_run_id

    error_facet = fail_ev["run"]["facets"].get("errorMessage")
    assert error_facet is not None
    assert "Intentional test execution failure" in error_facet["message"]
    assert error_facet["programmingLanguage"] == "python"


def test_revoked_node_start_emits_no_start_and_stops_run_with_flush(tmp_path, monkeypatch):
    """A revoked node_start emits no START and stops the run, with flush still called."""
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")

    plan_node = PlanNode(
        id="transform.node1",
        kind="transform",
        reads=(),
        writes=(),
        connections=(),
    )
    ctx = _make_governed_context(nodes=[plan_node])
    cio = CatalogIO(ctx, project="ecommerce")
    recording_emitter = RecordingEmitter()
    cio.lineage_emitter = recording_emitter
    # Make node_start raise RunRevoked
    cio.node_start = MagicMock(side_effect=RunRevoked("Run grant revoked by administrator"))
    set_catalog_io(cio)

    node1 = Node(id="transform.node1", name="node1", node_type=NodeType.TRANSFORM)
    node2 = Node(id="transform.node2", name="node2", node_type=NodeType.TRANSFORM)
    manifest = Manifest(project="ecommerce")
    manifest.add_node(node1)
    manifest.add_node(node2)

    exec_ctx = ExecutionContext(
        project_name="ecommerce",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(manifest=manifest, target_path=tmp_path / "target", exec_context=exec_ctx)
    summary = runner.run(continue_on_error=True)

    assert summary.failed_nodes == 1
    # Stopped immediately; node 2 was never run
    assert not any(r.node_id == "transform.node2" for r in summary.results)

    # Crucial check: NO START was emitted for the revoked node
    assert len(recording_emitter.events) == 0
    # And flush was still called at end of run
    assert recording_emitter.flush_count == 1


def test_emitter_failure_does_not_fail_node(tmp_path, monkeypatch):
    """An emitter failure does not fail the node."""
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")

    plan_node = PlanNode(
        id="transform.daily_orders",
        kind="transform",
        reads=(),
        writes=(),
        connections=(),
    )
    ctx = _make_governed_context(nodes=[plan_node])
    cio = CatalogIO(ctx, project="ecommerce")
    cio.lineage_emitter = FailingEmitter()
    set_catalog_io(cio)

    node = Node(
        id="transform.daily_orders",
        name="daily_orders",
        node_type=NodeType.TRANSFORM,
    )
    manifest = Manifest(project="ecommerce")
    manifest.add_node(node)

    exec_ctx = ExecutionContext(
        project_name="ecommerce",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(manifest=manifest, target_path=tmp_path / "target", exec_context=exec_ctx)
    summary = runner.run()

    # Node succeeds despite emitter errors
    assert summary.successful_nodes == 1
    assert summary.failed_nodes == 0


def test_non_governed_runs_emit_nothing(tmp_path, monkeypatch):
    """Non-governed runs emit nothing and do not flush."""
    monkeypatch.delenv("ATLAS_RUNTIME_MODE", raising=False)
    set_catalog_io(None)

    assert get_catalog_io() is None

    node = Node(
        id="transform.daily_orders",
        name="daily_orders",
        node_type=NodeType.TRANSFORM,
    )
    manifest = Manifest(project="ecommerce")
    manifest.add_node(node)

    exec_ctx = ExecutionContext(
        project_name="ecommerce",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(manifest=manifest, target_path=tmp_path / "target", exec_context=exec_ctx)
    summary = runner.run()

    assert summary.successful_nodes == 1
    # CatalogIO is still None
    assert get_catalog_io() is None


def test_end_of_run_flush_called_exactly_once(tmp_path, monkeypatch, caplog):
    """The end-of-run flush is called exactly once in DAGRunner, with stats and warnings logged."""
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")

    plan_node = PlanNode(
        id="transform.n1",
        kind="transform",
        reads=(),
        writes=(),
        connections=(),
    )
    ctx = _make_governed_context(nodes=[plan_node])
    cio = CatalogIO(ctx, project="ecommerce")
    recording_emitter = RecordingEmitter(buffered=3)
    cio.lineage_emitter = recording_emitter
    set_catalog_io(cio)

    node = Node(id="transform.n1", name="n1", node_type=NodeType.TRANSFORM)
    manifest = Manifest(project="ecommerce")
    manifest.add_node(node)

    exec_ctx = ExecutionContext(
        project_name="ecommerce",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(manifest=manifest, target_path=tmp_path / "target", exec_context=exec_ctx)

    with caplog.at_level(logging.INFO):
        summary = runner.run()

    assert summary.successful_nodes == 1
    assert recording_emitter.flush_count == 1
    # Check stats logged
    assert any("Lineage delivery stats" in rec.message for rec in caplog.records)
    # Check warning logged for buffered events
    assert any("Lineage events remain buffered after flush: 3" in rec.message for rec in caplog.records)


def test_parallel_runner_end_of_run_flush_called_exactly_once(tmp_path, monkeypatch, caplog):
    """The end-of-run flush is called exactly once in ParallelDAGRunner."""
    monkeypatch.setenv("ATLAS_RUNTIME_MODE", "governed")

    plan_node = PlanNode(
        id="transform.p1",
        kind="transform",
        reads=(),
        writes=(),
        connections=(),
    )
    ctx = _make_governed_context(nodes=[plan_node])
    cio = CatalogIO(ctx, project="ecommerce")
    recording_emitter = RecordingEmitter()
    cio.lineage_emitter = recording_emitter
    set_catalog_io(cio)

    node = Node(id="transform.p1", name="p1", node_type=NodeType.TRANSFORM)
    manifest = Manifest(project="ecommerce")
    manifest.add_node(node)

    exec_ctx = ExecutionContext(
        project_name="ecommerce",
        workspace_path=tmp_path,
        target_path=tmp_path / "target",
    )
    runner = DAGRunner(manifest=manifest, target_path=tmp_path / "target", exec_context=exec_ctx)
    parallel_runner = ParallelDAGRunner(runner, max_workers=2)

    with caplog.at_level(logging.INFO):
        summary = parallel_runner.run(nodes_to_run={"transform.p1"})

    assert summary.successful_nodes == 1
    assert recording_emitter.flush_count == 1


def test_lineage_emitter_mock_transport_integration(monkeypatch):
    """End-to-end integration test with LineageEmitter and httpx.MockTransport."""
    captured_requests: list[httpx.Request] = []

    def handle_request(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(201, json={"status": "accepted"})

    transport = httpx.MockTransport(handle_request)
    emitter = LineageEmitter(
        base_url="http://controller.local",
        grant_token="test-secret-token",
        transport=transport,
    )

    plan_node = PlanNode(
        id="transform.orders",
        kind="transform",
        reads=("warehouse.raw.orders",),
        writes=("warehouse.analytics.orders",),
        connections=(),
    )
    ctx = _make_governed_context(nodes=[plan_node])
    cio = CatalogIO(ctx, project="ecommerce")
    cio.lineage_emitter = emitter

    cio.emit_node_event("START", "transform.orders")
    cio.emit_node_event(
        "COMPLETE",
        "transform.orders",
        schema={"id": "bigint", "total": "double"},
        snapshot_ids={"warehouse.analytics.orders": "123456789"},
        sql="SELECT id, total FROM orders",
    )

    assert len(captured_requests) == 2
    req_start = captured_requests[0]
    assert req_start.url == "http://controller.local/api/runtime/v1/lineage"
    assert req_start.headers["authorization"] == "Bearer test-secret-token"

    body_complete = httpx.Response(200, content=captured_requests[1].content).json()
    assert body_complete["eventType"] == "COMPLETE"
    assert body_complete["outputs"][0]["facets"]["version"]["datasetVersion"] == "123456789"
    assert len(body_complete["outputs"][0]["facets"]["schema"]["fields"]) == 2

    # Verify stats
    stats = emitter.stats()
    assert stats["sent"] == 2
    assert stats["buffered"] == 0
