"""
Tests for DAGRunner entity consolidation integration (Phase 2).

Tests cover:
- _consolidate_entities triggers after FG nodes succeed
- Consolidation skipped when no FG nodes in DAG
- Consolidation skipped on dry-run
- Only entities with changed FGs are consolidated
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from seeknal.workflow.runner import DAGRunner, ExecutionStatus, ExecutionSummary, NodeResult


# ---------------------------------------------------------------------------
# Stubs matching Manifest/Node interface
# ---------------------------------------------------------------------------

class _NodeType(Enum):
    FEATURE_GROUP = "feature_group"
    SOURCE = "source"
    TRANSFORM = "transform"


@dataclass
class _Edge:
    from_node: str
    to_node: str


@dataclass
class _StubNode:
    id: str
    name: str
    node_type: _NodeType
    config: Dict[str, Any] = field(default_factory=dict)
    file_path: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    columns: Dict[str, str] = field(default_factory=dict)


@dataclass
class _StubManifest:
    nodes: Dict[str, _StubNode] = field(default_factory=dict)
    edges: List[_Edge] = field(default_factory=list)

    def get_node(self, node_id: str) -> Optional[_StubNode]:
        return self.nodes.get(node_id)

    def get_downstream_nodes(self, node_id: str) -> Set[str]:
        return {e.to_node for e in self.edges if e.from_node == node_id}

    def get_upstream_nodes(self, node_id: str) -> Set[str]:
        return {e.from_node for e in self.edges if e.to_node == node_id}

    def detect_cycles(self):
        return False, []


def _make_fg_node(name: str, entity_name: str, join_keys: List[str]) -> _StubNode:
    return _StubNode(
        id=f"feature_group.{name}",
        name=name,
        node_type=_NodeType.FEATURE_GROUP,
        config={
            "entity": {"name": entity_name, "join_keys": join_keys},
            "materialization": {"event_time_col": "event_time", "offline": True},
        },
    )


def _write_fg_parquet(target_path: Path, fg_name: str, df: pd.DataFrame) -> Path:
    intermediate = target_path / "intermediate"
    intermediate.mkdir(parents=True, exist_ok=True)
    path = intermediate / f"feature_group_{fg_name}.parquet"
    df.to_parquet(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConsolidateEntities:
    def test_consolidation_runs_after_fg_success(self, tmp_path):
        """After FG nodes succeed, consolidated parquet is created."""
        df_cust = pd.DataFrame({
            "customer_id": ["C1", "C2"],
            "event_time": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "revenue": [100.0, 200.0],
        })
        _write_fg_parquet(tmp_path, "customer_features", df_cust)

        manifest = _StubManifest(
            nodes={
                "feature_group.customer_features": _make_fg_node(
                    "customer_features", "customer", ["customer_id"]
                ),
            }
        )

        runner = DAGRunner(manifest=manifest, target_path=tmp_path)

        # Simulate a successful FG execution
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, successful_nodes=1,
            results=[
                NodeResult(
                    node_id="feature_group.customer_features",
                    status=ExecutionStatus.SUCCESS,
                    duration=1.0, row_count=2,
                ),
            ],
        )

        runner._consolidate_entities(summary)

        # Verify consolidated parquet exists
        consolidated_path = tmp_path / "feature_store" / "customer" / "features.parquet"
        assert consolidated_path.exists()

        catalog_path = tmp_path / "feature_store" / "customer" / "_entity_catalog.json"
        assert catalog_path.exists()

    def test_consolidation_skipped_when_no_fg_nodes(self, tmp_path):
        """With only source/transform nodes, consolidation is skipped."""
        manifest = _StubManifest(
            nodes={
                "source.raw": _StubNode(
                    id="source.raw", name="raw", node_type=_NodeType.SOURCE,
                ),
            }
        )
        runner = DAGRunner(manifest=manifest, target_path=tmp_path)
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, successful_nodes=1,
            results=[
                NodeResult(
                    node_id="source.raw",
                    status=ExecutionStatus.SUCCESS,
                    duration=0.5,
                ),
            ],
        )

        runner._consolidate_entities(summary)

        # No feature_store directory created
        assert not (tmp_path / "feature_store").exists()

    def test_consolidation_skipped_on_dry_run(self, tmp_path):
        """Dry run does not trigger consolidation."""
        manifest = _StubManifest(
            nodes={
                "feature_group.fg1": _make_fg_node("fg1", "customer", ["cid"]),
            }
        )
        runner = DAGRunner(manifest=manifest, target_path=tmp_path)
        summary = ExecutionSummary()

        runner._consolidate_entities(summary, dry_run=True)
        assert not (tmp_path / "feature_store").exists()

    def test_consolidation_skipped_when_no_fg_succeeded(self, tmp_path):
        """If all FG nodes failed, consolidation is skipped."""
        manifest = _StubManifest(
            nodes={
                "feature_group.fg1": _make_fg_node("fg1", "customer", ["cid"]),
            }
        )
        runner = DAGRunner(manifest=manifest, target_path=tmp_path)
        summary = ExecutionSummary(
            total_nodes=1, changed_nodes=1, failed_nodes=1,
            results=[
                NodeResult(
                    node_id="feature_group.fg1",
                    status=ExecutionStatus.FAILED,
                    error_message="test failure",
                ),
            ],
        )

        runner._consolidate_entities(summary)
        assert not (tmp_path / "feature_store").exists()

    def test_consolidation_only_affected_entities(self, tmp_path):
        """Only entities with changed FGs are consolidated."""
        df_cust = pd.DataFrame({
            "customer_id": ["C1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "revenue": [100.0],
        })
        df_prod = pd.DataFrame({
            "product_id": ["P1"],
            "event_time": pd.to_datetime(["2026-01-01"]),
            "price": [9.99],
        })
        _write_fg_parquet(tmp_path, "customer_features", df_cust)
        _write_fg_parquet(tmp_path, "product_features", df_prod)

        manifest = _StubManifest(
            nodes={
                "feature_group.customer_features": _make_fg_node(
                    "customer_features", "customer", ["customer_id"]
                ),
                "feature_group.product_features": _make_fg_node(
                    "product_features", "product", ["product_id"]
                ),
            }
        )
        runner = DAGRunner(manifest=manifest, target_path=tmp_path)

        # Only customer_features succeeded
        summary = ExecutionSummary(
            total_nodes=2, changed_nodes=1, successful_nodes=1,
            results=[
                NodeResult(
                    node_id="feature_group.customer_features",
                    status=ExecutionStatus.SUCCESS,
                    duration=1.0,
                ),
            ],
        )

        runner._consolidate_entities(summary)

        # Customer entity consolidated
        assert (tmp_path / "feature_store" / "customer" / "features.parquet").exists()
        # Product entity NOT consolidated (not in changed_fgs)
        assert not (tmp_path / "feature_store" / "product").exists()
