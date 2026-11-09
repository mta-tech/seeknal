"""Run plan manifest compilation and canonical serialization for Atlas Runtime."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from seeknal.dag.manifest import NodeType
from seeknal.workflow.dag import DAGBuilder
from seeknal.workflow.materialization.dispatcher import MaterializationDispatcher


class RunPlanError(Exception):
    """Raised when run plan construction or validation fails."""


@dataclass(frozen=True)
class TableAccess:
    """Table access entry in run plan."""

    table: str
    actions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "actions": list(self.actions),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TableAccess:
        return cls(
            table=data["table"],
            actions=tuple(data.get("actions", ())),
        )


@dataclass(frozen=True)
class ConnectionAccess:
    """Connection access entry in run plan."""

    name: str
    actions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "actions": list(self.actions),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ConnectionAccess:
        return cls(
            name=data["name"],
            actions=tuple(data.get("actions", ())),
        )


@dataclass(frozen=True)
class PlanNode:
    """A node entry in run plan."""

    id: str
    kind: str
    reads: tuple[str, ...]
    writes: tuple[str, ...]
    connections: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind,
            "reads": list(self.reads),
            "writes": list(self.writes),
            "connections": list(self.connections),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PlanNode:
        return cls(
            id=data["id"],
            kind=data["kind"],
            reads=tuple(data.get("reads", ())),
            writes=tuple(data.get("writes", ())),
            connections=tuple(data.get("connections", ())),
        )


@dataclass(frozen=True)
class RunPlan:
    """Compiled run plan manifest."""

    reads: tuple[TableAccess, ...]
    writes: tuple[TableAccess, ...]
    connections: tuple[ConnectionAccess, ...]
    nodes: tuple[PlanNode, ...]

    @property
    def has_python_nodes(self) -> bool:
        """True if any node in the plan requires Python/gVisor execution."""
        return any(n.kind == "python" or n.id.startswith("python.") for n in self.nodes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "reads": [r.to_dict() for r in self.reads],
            "writes": [w.to_dict() for w in self.writes],
            "connections": [c.to_dict() for c in self.connections],
            "nodes": [n.to_dict() for n in self.nodes],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunPlan:
        return cls(
            reads=tuple(TableAccess.from_dict(r) for r in data.get("reads", ())),
            writes=tuple(TableAccess.from_dict(w) for w in data.get("writes", ())),
            connections=tuple(ConnectionAccess.from_dict(c) for c in data.get("connections", ())),
            nodes=tuple(PlanNode.from_dict(n) for n in data.get("nodes", ())),
        )


def canonical_plan_json(plan: RunPlan | dict[str, Any]) -> bytes:
    """Serialize plan to canonical JSON bytes with sorted keys and tight separators."""
    plan_dict = plan.to_dict() if isinstance(plan, RunPlan) else plan
    return json.dumps(
        plan_dict,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def plan_sha256(plan: RunPlan | dict[str, Any]) -> str:
    """Compute sha256 hex digest of the canonical plan JSON."""
    return hashlib.sha256(canonical_plan_json(plan)).hexdigest()


def _validate_iceberg_table(table_str: str, node_id: str, context: str) -> str:
    """Validate that an Iceberg table name is 3-part and normalize to lowercase."""
    cleaned = table_str.strip().lower()
    parts = cleaned.split(".")
    if len(parts) != 3 or not all(parts):
        raise RunPlanError(
            f"Node '{node_id}': Iceberg {context} table must be 3-part format "
            f"'catalog.namespace.table', got '{table_str}'"
        )
    return cleaned


def build_run_plan(
    project_path: str | Path,
    *,
    env: str | None = None,
    profile_path: str | Path | None = None,
) -> RunPlan:
    """Build DAG and walk nodes in topological order to produce a RunPlan."""
    proj = Path(project_path)
    prof = Path(profile_path) if profile_path is not None else None

    builder = DAGBuilder(project_path=proj, profile_path=prof)
    builder.build()
    order = builder.topological_sort()

    all_reads: set[str] = set()
    all_writes: set[str] = set()
    all_connections: dict[str, set[str]] = {}

    plan_nodes: list[PlanNode] = []

    for node_id in order:
        node = builder.nodes[node_id]
        yaml_data = node.yaml_data or {}

        # Determine node kind
        raw_kind = getattr(node.kind, "value", str(node.kind))
        is_python = (
            raw_kind == "python"
            or node.kind == NodeType.PYTHON
            or (
                hasattr(node, "file_path")
                and node.file_path
                and str(node.file_path).endswith(".py")
                and raw_kind != "source"
            )
        )
        plan_kind = "python" if is_python else raw_kind

        node_reads: list[str] = []
        node_writes: list[str] = []
        node_connections: dict[str, set[str]] = {}

        # 1. Sources
        if raw_kind == "source":
            source_type = str(yaml_data.get("source", yaml_data.get("type", ""))).strip().lower()
            if source_type == "iceberg":
                table_raw = yaml_data.get("table", "")
                if not table_raw:
                    raise RunPlanError(f"Node '{node.id}': Iceberg source missing 'table'")
                norm_table = _validate_iceberg_table(str(table_raw), node.id, "source")
                node_reads.append(norm_table)
                all_reads.add(norm_table)
            else:
                params = yaml_data.get("params")
                conn = (
                    params.get("connection")
                    if isinstance(params, dict)
                    else None
                ) or yaml_data.get("connection")
                if conn and str(conn).strip():
                    conn_str = str(conn).strip()
                    node_connections.setdefault(conn_str, set()).add("read")
                    all_connections.setdefault(conn_str, set()).add("read")

        # 2. Materializations (sinks)
        mats = yaml_data.get("materializations")
        if mats is None:
            mat = yaml_data.get("materialization")
            if isinstance(mat, dict):
                mats = [mat]
            elif isinstance(mat, list):
                mats = list(mat)
            else:
                mats = []

        for target in mats:
            if not isinstance(target, dict):
                continue
            if target.get("enabled") is False:
                continue

            target_cfg = dict(target)
            if env:
                target_cfg = MaterializationDispatcher._prefix_target(target_cfg, env)

            target_type = str(target_cfg.get("type", "iceberg")).strip().lower()
            if target_type == "iceberg":
                table_raw = target_cfg.get("table", "")
                if not table_raw:
                    raise RunPlanError(f"Node '{node.id}': Iceberg materialization missing 'table'")
                norm_table = _validate_iceberg_table(str(table_raw), node.id, "sink")
                node_writes.append(norm_table)
                all_writes.add(norm_table)
            elif target_type in ("postgresql", "postgres"):
                conn = target_cfg.get("connection")
                if conn and str(conn).strip():
                    conn_str = str(conn).strip()
                    node_connections.setdefault(conn_str, set()).add("write")
                    all_connections.setdefault(conn_str, set()).add("write")

        plan_node = PlanNode(
            id=node.id,
            kind=plan_kind,
            reads=tuple(sorted(set(node_reads))),
            writes=tuple(sorted(set(node_writes))),
            connections=tuple(sorted(node_connections.keys())),
        )
        plan_nodes.append(plan_node)

    # Compile sorted, deduplicated top-level access lists
    final_reads = tuple(
        TableAccess(table=t, actions=("read",)) for t in sorted(all_reads)
    )
    final_writes = tuple(
        TableAccess(table=t, actions=("write",)) for t in sorted(all_writes)
    )
    final_connections = tuple(
        ConnectionAccess(name=name, actions=tuple(sorted(actions)))
        for name, actions in sorted(all_connections.items())
    )

    return RunPlan(
        reads=final_reads,
        writes=final_writes,
        connections=final_connections,
        nodes=tuple(plan_nodes),
    )
