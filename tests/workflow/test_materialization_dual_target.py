"""Characterisation tests for dual Iceberg + PostgreSQL materialization on a single node.

Pins current behaviour with mocks (no real PostgreSQL or Iceberg connections):
1. YAML parsing/validation of `materializations: [{type: iceberg, ...}, {type: postgresql, ...}]`
   for transform (and source / feature_group / python nodes).
2. Both targets are written from the same DuckDB view in list order during a single run.
3. Supported PostgreSQL modes (`full`, `incremental_by_time`, `upsert_by_key`; not `append`/`overwrite`).
4. Failure handling & `required_failed` semantics:
   - A failed `postgresql` target is best-effort and sets `required_failed=False`, so the node
     itself still finishes with `ExecutionStatus.SUCCESS` (silent at node status level).
   - A failed `iceberg` target with basic `append`/`overwrite` also leaves `required_failed=False`
     (node status `ExecutionStatus.SUCCESS`), whereas a failed `iceberg` target with advanced
     mode (`upsert` or `insert_overwrite`) sets `required_failed=True` and flips node status to
     `ExecutionStatus.FAILED` in `BaseExecutor.run()`.
5. Environment / namespace prefixing differences (`MaterializationDispatcher._prefix_target`).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.dag import DAGBuilder
from seeknal.workflow.executors.base import ExecutionContext, ExecutionStatus, ExecutorResult
from seeknal.workflow.executors.feature_group_executor import FeatureGroupExecutor
from seeknal.workflow.executors.python_executor import PythonExecutor
from seeknal.workflow.executors.transform_executor import TransformExecutor
from seeknal.workflow.materialization.dispatcher import MaterializationDispatcher
from seeknal.workflow.materialization.operations import WriteResult
from seeknal.workflow.materialization.pg_config import (
    PostgresConfigError,
    PostgresMaterializationConfig,
    PostgresMaterializationMode,
)
from seeknal.workflow.validators import validate_schema


def _make_exec_context(tmp_path: Path, *, env_name: str | None = None) -> ExecutionContext:
    target_path = tmp_path / "target"
    target_path.mkdir(parents=True, exist_ok=True)
    return ExecutionContext(
        project_name="dual_mat_test",
        workspace_path=tmp_path,
        target_path=target_path,
        duckdb_connection=duckdb.connect(":memory:"),
        dry_run=False,
        materialize_enabled=True,
        env_name=env_name,
    )


class TestDualTargetYAMLParsingAndValidation:
    """YAML parsing and schema validation for plural `materializations:` with Iceberg + PostgreSQL."""

    def test_transform_yaml_accepts_dual_iceberg_and_postgresql_targets(self, tmp_path: Path):
        sources_dir = tmp_path / "seeknal" / "sources"
        transforms_dir = tmp_path / "seeknal" / "transforms"
        sources_dir.mkdir(parents=True)
        transforms_dir.mkdir(parents=True)

        (sources_dir / "orders.yml").write_text(
            "kind: source\nname: orders\nsource: csv\ntable: orders.csv\n",
            encoding="utf-8",
        )
        transform_yml = transforms_dir / "curated_orders.yml"
        transform_yml.write_text(
            "kind: transform\n"
            "name: curated_orders\n"
            "transform: SELECT order_id, amount FROM input_0\n"
            "inputs:\n"
            "  - ref: source.orders\n"
            "materializations:\n"
            "  - type: iceberg\n"
            "    table: warehouse.curated.orders\n"
            "    mode: append\n"
            "  - type: postgresql\n"
            "    connection: pg_local\n"
            "    table: public.curated_orders\n"
            "    mode: upsert_by_key\n"
            "    unique_keys: [order_id]\n",
            encoding="utf-8",
        )

        builder = DAGBuilder(project_path=tmp_path)
        builder.build()
        assert builder.get_parse_errors() == []

        node = builder.get_node("transform.curated_orders")
        assert node is not None
        validate_schema(node.yaml_data)

        mats = node.yaml_data["materializations"]
        assert len(mats) == 2
        assert mats[0]["type"] == "iceberg"
        assert mats[0]["table"] == "warehouse.curated.orders"
        assert mats[1]["type"] == "postgresql"
        assert mats[1]["table"] == "public.curated_orders"
        assert mats[1]["mode"] == "upsert_by_key"


class TestPostgresMaterializationModes:
    """Characterise supported and unsupported modes in PostgresMaterializationConfig."""

    @pytest.mark.parametrize(
        "mode_dict,expected_mode",
        [
            ({"mode": "full"}, PostgresMaterializationMode.FULL),
            (
                {"mode": "incremental_by_time", "time_column": "created_at", "lookback": 7},
                PostgresMaterializationMode.INCREMENTAL_BY_TIME,
            ),
            (
                {"mode": "upsert_by_key", "unique_keys": ["order_id"]},
                PostgresMaterializationMode.UPSERT_BY_KEY,
            ),
        ],
    )
    def test_postgresql_supported_modes_validate(self, mode_dict, expected_mode):
        cfg = PostgresMaterializationConfig.from_dict(
            {
                "type": "postgresql",
                "connection": "pg_local",
                "table": "public.orders",
                **mode_dict,
            }
        )
        cfg.validate()
        assert cfg.mode == expected_mode

    @pytest.mark.parametrize("unsupported_mode", ["append", "overwrite", "upsert", "insert_overwrite"])
    def test_postgresql_rejects_iceberg_mode_names(self, unsupported_mode: str):
        with pytest.raises(PostgresConfigError, match="Invalid PostgreSQL materialization mode"):
            PostgresMaterializationConfig.from_dict(
                {
                    "type": "postgresql",
                    "connection": "pg_local",
                    "table": "public.orders",
                    "mode": unsupported_mode,
                }
            )


class TestDualTargetExecutionOrderAndFailureSemantics:
    """Both targets are written from the same DuckDB view in order; check failure statuses."""

    def test_transform_executor_dispatches_both_targets_in_order_from_same_view(
        self, tmp_path: Path
    ):
        ctx = _make_exec_context(tmp_path)
        node = Node(
            id="transform.curated_orders",
            name="curated_orders",
            node_type=NodeType.TRANSFORM,
            config={
                "kind": "transform",
                "name": "curated_orders",
                "transform": "SELECT 1 AS order_id, 42.5 AS amount UNION ALL SELECT 2, 99.0",
                "materializations": [
                    {
                        "type": "iceberg",
                        "table": "warehouse.curated.orders",
                        "mode": "append",
                    },
                    {
                        "type": "postgresql",
                        "connection": "pg_local",
                        "table": "public.curated_orders",
                        "mode": "full",
                    },
                ],
            },
        )

        call_order: list[tuple[str, str, int]] = []

        def fake_iceberg(self_disp, con, view_name, target_cfg):
            rows = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            call_order.append(("iceberg", view_name, rows))
            return WriteResult(success=True, row_count=rows, duration_seconds=0.1)

        def fake_pg(self_disp, con, view_name, target_cfg):
            rows = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
            call_order.append(("postgresql", view_name, rows))
            return WriteResult(success=True, row_count=rows, duration_seconds=0.1)

        with (
            patch.object(MaterializationDispatcher, "_materialize_iceberg", fake_iceberg),
            patch.object(MaterializationDispatcher, "_materialize_postgresql", fake_pg),
        ):
            executor = TransformExecutor(node, ctx)
            result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.is_success() is True
        assert result.row_count == 2

        # Dispatched in exact YAML list order from the same DuckDB view `transform.curated_orders`
        assert call_order == [
            ("iceberg", "transform.curated_orders", 2),
            ("postgresql", "transform.curated_orders", 2),
        ]

        mat_meta = result.metadata["materialization"]
        assert mat_meta["enabled"] is True
        assert mat_meta["success"] is True
        assert mat_meta["total"] == 2
        assert mat_meta["succeeded"] == 2
        assert mat_meta["failed"] == 0
        assert mat_meta["required_failed"] is False
        assert [r["type"] for r in mat_meta["results"]] == ["iceberg", "postgresql"]

    def test_failed_postgresql_target_is_silent_at_node_status_level(self, tmp_path: Path):
        """Characterises current behaviour: when PostgreSQL target fails, required_failed is False
        and TransformExecutor.run() still returns ExecutionStatus.SUCCESS."""
        ctx = _make_exec_context(tmp_path)
        node = Node(
            id="transform.curated_orders",
            name="curated_orders",
            node_type=NodeType.TRANSFORM,
            config={
                "kind": "transform",
                "name": "curated_orders",
                "transform": "SELECT 1 AS order_id, 42.5 AS amount",
                "materializations": [
                    {
                        "type": "iceberg",
                        "table": "warehouse.curated.orders",
                        "mode": "append",
                    },
                    {
                        "type": "postgresql",
                        "connection": "pg_local",
                        "table": "public.curated_orders",
                        "mode": "upsert_by_key",
                        "unique_keys": ["order_id"],
                    },
                ],
            },
        )

        with (
            patch.object(
                MaterializationDispatcher,
                "_materialize_iceberg",
                return_value=WriteResult(success=True, row_count=1, duration_seconds=0.05),
            ),
            patch.object(
                MaterializationDispatcher,
                "_materialize_postgresql",
                side_effect=RuntimeError("PostgreSQL connection refused"),
            ),
        ):
            executor = TransformExecutor(node, ctx)
            result = executor.run()

        # Node status remains SUCCESS because required_failed is only set for Iceberg advanced modes
        assert result.status == ExecutionStatus.SUCCESS
        assert result.is_success() is True
        assert result.error_message is None

        mat_meta = result.metadata["materialization"]
        assert mat_meta["success"] is False
        assert mat_meta["total"] == 2
        assert mat_meta["succeeded"] == 1
        assert mat_meta["failed"] == 1
        assert mat_meta["required_failed"] is False
        assert mat_meta["results"][0]["type"] == "iceberg"
        assert mat_meta["results"][0]["success"] is True
        assert mat_meta["results"][1]["type"] == "postgresql"
        assert mat_meta["results"][1]["success"] is False
        assert "PostgreSQL connection refused" in mat_meta["results"][1]["error"]

    def test_failed_iceberg_advanced_mode_fails_node_via_required_failed(self, tmp_path: Path):
        """Contrast with PostgreSQL failure: an Iceberg `upsert` failure sets required_failed=True
        and BaseExecutor.run() flips the node status to ExecutionStatus.FAILED."""
        ctx = _make_exec_context(tmp_path)
        node = Node(
            id="transform.curated_orders",
            name="curated_orders",
            node_type=NodeType.TRANSFORM,
            config={
                "kind": "transform",
                "name": "curated_orders",
                "transform": "SELECT 1 AS order_id, 42.5 AS amount",
                "materializations": [
                    {
                        "type": "iceberg",
                        "table": "warehouse.curated.orders",
                        "mode": "upsert",
                        "unique_keys": ["order_id"],
                    },
                    {
                        "type": "postgresql",
                        "connection": "pg_local",
                        "table": "public.curated_orders",
                        "mode": "full",
                    },
                ],
            },
        )

        with (
            patch.object(
                MaterializationDispatcher,
                "_materialize_iceberg",
                side_effect=RuntimeError("Iceberg upsert commit conflict"),
            ),
            patch.object(
                MaterializationDispatcher,
                "_materialize_postgresql",
                return_value=WriteResult(success=True, row_count=1, duration_seconds=0.05),
            ),
        ):
            executor = TransformExecutor(node, ctx)
            result = executor.run()

        # PostgreSQL still ran (best-effort), but Iceberg upsert failure sets required_failed=True
        # which causes BaseExecutor.run() to mark the node FAILED.
        assert result.status == ExecutionStatus.FAILED
        assert result.is_failed() is True
        assert "Required Iceberg materialization failed" in (result.error_message or "")
        assert result.metadata["materialization"]["required_failed"] is True
        assert result.metadata["materialization"]["succeeded"] == 1
        assert result.metadata["materialization"]["failed"] == 1

    def test_python_and_feature_group_executors_dispatch_dual_targets(self, tmp_path: Path):
        """Both PythonExecutor.post_execute and FeatureGroupExecutor.post_execute dispatch
        dual Iceberg + PostgreSQL targets from their respective DuckDB views."""
        ctx = _make_exec_context(tmp_path)
        targets = [
            {"type": "iceberg", "table": "warehouse.curated.features", "mode": "overwrite"},
            {
                "type": "postgresql",
                "connection": "pg_local",
                "table": "public.features",
                "mode": "full",
            },
        ]

        # 1. PythonExecutor post_execute reads intermediate parquet into `transform.<name>` view
        intermediate_dir = ctx.target_path / "intermediate"
        intermediate_dir.mkdir(parents=True, exist_ok=True)
        py_parquet = intermediate_dir / "python_py_feat.parquet"
        pd.DataFrame({"user_id": [1, 2], "score": [0.8, 0.9]}).to_parquet(py_parquet, index=False)

        py_node = Node(
            id="python.py_feat",
            name="py_feat",
            node_type=NodeType.PYTHON,
            config={"materializations": targets},
        )
        py_exec = PythonExecutor(py_node, ctx)

        dispatched_views: list[tuple[str, str]] = []

        def record_iceberg(self_disp, con, view_name, target_cfg):
            dispatched_views.append(("iceberg", view_name))
            return WriteResult(success=True, row_count=2, duration_seconds=0.01)

        def record_pg_fail(self_disp, con, view_name, target_cfg):
            dispatched_views.append(("postgresql", view_name))
            raise RuntimeError("pg write error")

        with (
            patch.object(MaterializationDispatcher, "_materialize_iceberg", record_iceberg),
            patch.object(MaterializationDispatcher, "_materialize_postgresql", record_pg_fail),
        ):
            py_res = py_exec.post_execute(
                ExecutorResult(node_id=py_node.id, status=ExecutionStatus.SUCCESS, row_count=2)
            )

        assert py_res.status == ExecutionStatus.SUCCESS
        assert py_res.metadata["materialization"]["required_failed"] is False
        assert dispatched_views == [
            ("iceberg", "transform.py_feat"),
            ("postgresql", "transform.py_feat"),
        ]

        # 2. FeatureGroupExecutor post_execute dispatches from `feature_group.<name>` view
        con = ctx.get_duckdb_connection()
        con.execute("CREATE SCHEMA IF NOT EXISTS feature_group")
        con.execute("CREATE OR REPLACE VIEW feature_group.fg_feat AS SELECT 1 AS user_id")

        fg_node = Node(
            id="feature_group.fg_feat",
            name="fg_feat",
            node_type=NodeType.FEATURE_GROUP,
            config={"materializations": targets},
        )
        fg_exec = FeatureGroupExecutor(fg_node, ctx)
        dispatched_views.clear()

        with (
            patch.object(MaterializationDispatcher, "_materialize_iceberg", record_iceberg),
            patch.object(MaterializationDispatcher, "_materialize_postgresql", record_pg_fail),
        ):
            fg_res = fg_exec.post_execute(
                ExecutorResult(node_id=fg_node.id, status=ExecutionStatus.SUCCESS, row_count=1)
            )

        assert fg_res.status == ExecutionStatus.SUCCESS
        assert fg_res.metadata["materialization"]["required_failed"] is False
        assert dispatched_views == [
            ("iceberg", "feature_group.fg_feat"),
            ("postgresql", "feature_group.fg_feat"),
        ]


class TestEnvNamespacePrefixingDifferences:
    """Characterise env_name prefixing differences between PostgreSQL and Iceberg targets."""

    def test_prefix_target_postgresql_vs_iceberg(self, tmp_path: Path):
        ctx = _make_exec_context(tmp_path, env_name="staging")
        dispatcher = MaterializationDispatcher(profile_loader=MagicMock())

        seen_tables: list[tuple[str, str]] = []

        def capture_iceberg(self_disp, con, view_name, target_cfg):
            seen_tables.append(("iceberg", target_cfg["table"]))
            return WriteResult(success=True, row_count=1, duration_seconds=0.01)

        def capture_pg(self_disp, con, view_name, target_cfg):
            seen_tables.append(("postgresql", target_cfg["table"]))
            return WriteResult(success=True, row_count=1, duration_seconds=0.01)

        with (
            patch.object(MaterializationDispatcher, "_materialize_iceberg", capture_iceberg),
            patch.object(MaterializationDispatcher, "_materialize_postgresql", capture_pg),
        ):
            dispatcher.dispatch(
                con=ctx.get_duckdb_connection(),
                view_name="transform.orders",
                targets=[
                    # 3-part Iceberg: catalog.namespace.table -> catalog.{env}_namespace.table
                    {"type": "iceberg", "table": "warehouse.curated.orders"},
                    # 2-part Iceberg: namespace.table -> NOT prefixed (count('.') < 2)
                    {"type": "iceberg", "table": "curated.orders"},
                    # 2-part PostgreSQL: schema.table -> {env}_schema.table
                    {"type": "postgresql", "connection": "pg1", "table": "public.orders"},
                ],
                node_id="transform.orders",
                env_name="staging",
            )

        assert seen_tables == [
            ("iceberg", "warehouse.staging_curated.orders"),
            ("iceberg", "curated.orders"),
            ("postgresql", "staging_public.orders"),
        ]
