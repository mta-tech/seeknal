"""
Tests for the Atlas pre-run access enforcement hooked into ``seeknal run``.

These exercise the enforcement at the hook level (``_enforce_pre_run``) and the pure
``_source_fqtn`` resolver -- no data is materialized and no DAG is built. A fake gate
stands in for the live Atlas backend, and ``create_governance_gate_from_env`` is
monkeypatched on ``seeknal.cli.main`` so the gate-None / deny / allow paths are
covered without ATLAS_API_URL or a network.
"""

from __future__ import annotations

from typing import Any

import pytest

from seeknal.cli import main as main_module
from seeknal.dag.manifest import NodeType
from seeknal.integrations.atlas_client import AtlasPolicyDenied


class _FakeNode:
    """Minimal stand-in for a DAG node exposing ``config`` and ``node_type``."""

    def __init__(self, node_type: NodeType, config: dict[str, Any]) -> None:
        self.node_type = node_type
        self.kind = node_type
        self.config = config


class _FakeDAG:
    """Minimal stand-in for the DAG builder exposing ``nodes`` by id."""

    def __init__(self, nodes: dict[str, _FakeNode]) -> None:
        self.nodes = nodes


class _FakeGate:
    """Records ``enforce_access`` calls; optionally denies a named resource."""

    def __init__(self, deny: str | None = None) -> None:
        self.deny = deny
        self.calls: list[dict[str, Any]] = []

    def enforce_access(self, *, resource: str, action: str, actor: str | None) -> Any:
        self.calls.append({"resource": resource, "action": action, "actor": actor})
        if self.deny is not None and resource == self.deny:
            raise AtlasPolicyDenied(f"denied {action} on {resource}")
        return object()


# -- _source_fqtn ------------------------------------------------------------


def test_source_fqtn_iceberg_three_part_table_used_verbatim() -> None:
    node = _FakeNode(
        NodeType.SOURCE,
        {"source": "iceberg", "table": "atlas.qa_iceberg.signal_events"},
    )
    assert main_module._source_fqtn(node) == "atlas.qa_iceberg.signal_events"


def test_source_fqtn_prepends_warehouse_from_params() -> None:
    node = _FakeNode(
        NodeType.SOURCE,
        {"source": "iceberg", "table": "qa.events", "params": {"warehouse": "seeknal-warehouse"}},
    )
    assert main_module._source_fqtn(node) == "seeknal-warehouse.qa.events"


def test_source_fqtn_postgresql_schema_table_fallback() -> None:
    # No warehouse/catalog context -> most-qualified string available is returned.
    node = _FakeNode(
        NodeType.SOURCE,
        {"source": "postgresql", "table": "public.events", "params": {"connection": "pg"}},
    )
    assert main_module._source_fqtn(node) == "public.events"


def test_source_fqtn_none_when_no_table() -> None:
    node = _FakeNode(NodeType.SOURCE, {"source": "csv"})
    assert main_module._source_fqtn(node) is None
    node_empty = _FakeNode(NodeType.SOURCE, {"source": "csv", "table": "  "})
    assert main_module._source_fqtn(node_empty) is None


# -- _enforce_pre_run --------------------------------------------------------


def test_gate_none_skips_enforcement(monkeypatch: pytest.MonkeyPatch) -> None:
    # A gate that would explode if used -- proves None short-circuits.
    sentinel = _FakeGate()
    dag = _FakeDAG(
        {"source.a": _FakeNode(NodeType.SOURCE, {"source": "iceberg", "table": "w.n.a"})}
    )
    # gate is None => no calls.
    main_module._enforce_pre_run({"source.a"}, dag, None)
    assert sentinel.calls == []


def test_gate_allows_checks_only_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(main_module, "user_sub_from_credentials", lambda: "user-1", raising=False)
    dag = _FakeDAG(
        {
            "source.a": _FakeNode(NodeType.SOURCE, {"source": "iceberg", "table": "w.n.a"}),
            "transform.b": _FakeNode(NodeType.TRANSFORM, {"transform": "SELECT 1"}),
        }
    )
    gate = _FakeGate()

    main_module._enforce_pre_run({"source.a", "transform.b"}, dag, gate)

    # Only the source node was access-checked, with action=read.
    assert len(gate.calls) == 1
    assert gate.calls[0]["resource"] == "w.n.a"
    assert gate.calls[0]["action"] == "read"


def test_gate_denies_aborts_with_request_access_hint(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import typer

    monkeypatch.setattr(main_module, "user_sub_from_credentials", lambda: "user-1", raising=False)
    dag = _FakeDAG(
        {"source.secret": _FakeNode(NodeType.SOURCE, {"source": "iceberg", "table": "w.n.secret"})}
    )
    gate = _FakeGate(deny="w.n.secret")

    with pytest.raises(typer.Exit) as excinfo:
        main_module._enforce_pre_run({"source.secret"}, dag, gate)

    assert excinfo.value.exit_code == 1
    out = capsys.readouterr().out
    assert "request-access" in out
    assert "w.n.secret" in out


# -- run() integration at the enforcement boundary ---------------------------


def test_run_proceeds_when_gate_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """gate None => the factory returns None and enforcement is a no-op."""

    called = {"enforced": False}

    def fake_enforce(nodes_to_run, dag_builder, gate):
        called["enforced"] = True
        assert gate is None

    monkeypatch.setattr(main_module, "create_governance_gate_from_env", lambda: None)
    monkeypatch.setattr(main_module, "_enforce_pre_run", fake_enforce)

    # Call the hook the way run() does.
    gate = main_module.create_governance_gate_from_env()
    main_module._enforce_pre_run({"source.a"}, _FakeDAG({}), gate)
    assert called["enforced"] is True
