"""Tests for dual Iceberg + PostgreSQL materialization on a single node.

Verifies behaviour with mocks (no real PostgreSQL or Iceberg connections):
1. YAML parsing/validation of `materializations: [{type: iceberg, ...}, {type: postgresql, ...}]`
   for transform, source, feature_group, and python nodes, including boolean `required`.
2. Both targets are written from the same DuckDB view in list order during a single run.
3. Supported PostgreSQL modes (`full`, `incremental_by_time`, `upsert_by_key`; not `append`/`overwrite`).
4. Failure handling & `required_failed` semantics:
   - A failed `postgresql` target is required by default (`required_failed=True`) and marks the
     node `ExecutionStatus.FAILED` with an error message naming the target label and failure category.
   - Setting `required: false` on a `postgresql` target opts back into best-effort behaviour
     (`required_failed=False`, node status `ExecutionStatus.SUCCESS` with failure in metadata).
   - A failed `iceberg` target with basic `append`/`overwrite` leaves `required_failed=False`
     (node status `ExecutionStatus.SUCCESS`), whereas an advanced mode (`upsert` or `insert_overwrite`)
     sets `required_failed=True` and marks the node `ExecutionStatus.FAILED`.
5. Secret scrubbing across dispatcher logs, result metadata, and node error messages.
6. CLI-level `seeknal run` (sequential and `--parallel`) exits non-zero when a PostgreSQL target fails.
7. Environment / namespace prefixing differences (`MaterializationDispatcher._prefix_target`).
"""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest
import typer
from typer.testing import CliRunner

from seeknal.cli.main import app
from seeknal.dag.manifest import Node, NodeType
from seeknal.workflow.dag import DAGBuilder
from seeknal.workflow.executors.base import ExecutionContext, ExecutionStatus, ExecutorResult
from seeknal.workflow.executors.feature_group_executor import FeatureGroupExecutor
from seeknal.workflow.executors.python_executor import PythonExecutor
from seeknal.workflow.executors.source_executor import SourceExecutor
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

    def test_postgresql_required_defaults_true_and_validates_boolean(self):
        default_cfg = PostgresMaterializationConfig.from_dict(
            {
                "type": "postgresql",
                "connection": "pg_local",
                "table": "public.orders",
                "mode": "full",
                "extra_unknown_key": "ignored",
            }
        )
        default_cfg.validate()
        assert default_cfg.required is True

        opt_out_cfg = PostgresMaterializationConfig.from_dict(
            {
                "type": "postgresql",
                "connection": "pg_local",
                "table": "public.orders",
                "mode": "full",
                "required": False,
            }
        )
        opt_out_cfg.validate()
        assert opt_out_cfg.required is False

        for invalid_required in ["false", "true", 0, 1, None, ["false"]]:
            with pytest.raises(PostgresConfigError, match="'required' must be a boolean"):
                PostgresMaterializationConfig.from_dict(
                    {
                        "type": "postgresql",
                        "connection": "pg_local",
                        "table": "public.orders",
                        "mode": "full",
                        "required": invalid_required,
                    }
                )
            with pytest.raises(typer.Exit):
                validate_schema(
                    {
                        "kind": "transform",
                        "name": "curated_orders",
                        "materializations": [
                            {
                                "type": "postgresql",
                                "connection": "pg_local",
                                "table": "public.orders",
                                "mode": "full",
                                "required": invalid_required,
                            }
                        ],
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

    def test_failed_postgresql_target_fails_node_by_default_and_attempts_iceberg(
        self, tmp_path: Path
    ):
        """A dual iceberg+postgresql node where postgresql fails ends FAILED with the target
        label, target type, and failure category named, while the iceberg target is still attempted."""
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
                        "type": "postgresql",
                        "connection": "pg_local",
                        "table": "public.curated_orders",
                        "mode": "upsert_by_key",
                        "unique_keys": ["order_id"],
                    },
                    {
                        "type": "iceberg",
                        "table": "warehouse.curated.orders",
                        "mode": "append",
                    },
                ],
            },
        )

        attempted: list[str] = []

        def fake_pg_fail(self_disp, con, view_name, target_cfg):
            attempted.append("postgresql")
            raise RuntimeError("PostgreSQL connection refused")

        def fake_iceberg_ok(self_disp, con, view_name, target_cfg):
            attempted.append("iceberg")
            return WriteResult(success=True, row_count=1, duration_seconds=0.05)

        with (
            patch.object(MaterializationDispatcher, "_materialize_postgresql", fake_pg_fail),
            patch.object(MaterializationDispatcher, "_materialize_iceberg", fake_iceberg_ok),
        ):
            executor = TransformExecutor(node, ctx)
            result = executor.run()

        assert attempted == ["postgresql", "iceberg"]
        assert result.status == ExecutionStatus.FAILED
        assert result.is_failed() is True
        assert result.error_message is not None
        assert "Required postgresql materialization failed" in result.error_message
        assert "transform.curated_orders[0]:postgresql" in result.error_message
        assert "failure_category=write_failed" in result.error_message
        assert "Iceberg" not in result.error_message

        mat_meta = result.metadata["materialization"]
        assert mat_meta["success"] is False
        assert mat_meta["total"] == 2
        assert mat_meta["succeeded"] == 1
        assert mat_meta["failed"] == 1
        assert mat_meta["required_failed"] is True
        assert mat_meta["results"][0]["type"] == "postgresql"
        assert mat_meta["results"][0]["success"] is False
        assert mat_meta["results"][0]["failure_category"] == "write_failed"
        assert "PostgreSQL connection refused" in mat_meta["results"][0]["error"]
        assert mat_meta["results"][1]["type"] == "iceberg"
        assert mat_meta["results"][1]["success"] is True

    def test_failed_postgresql_target_with_required_false_keeps_node_success(
        self, tmp_path: Path
    ):
        """Setting `required: false` on a postgresql target keeps best-effort behaviour:
        node status is SUCCESS while the failure is recorded in result metadata."""
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
                        "required": False,
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
        assert mat_meta["results"][1]["required"] is False
        assert "PostgreSQL connection refused" in mat_meta["results"][1]["error"]

    def test_postgresql_success_and_iceberg_append_failure_behaves_as_before(
        self, tmp_path: Path
    ):
        """Iceberg append/overwrite failures remain best-effort when PostgreSQL succeeds."""
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
                        "mode": "full",
                    },
                ],
            },
        )

        with (
            patch.object(
                MaterializationDispatcher,
                "_materialize_iceberg",
                side_effect=RuntimeError("Iceberg catalog unreachable"),
            ),
            patch.object(
                MaterializationDispatcher,
                "_materialize_postgresql",
                return_value=WriteResult(success=True, row_count=1, duration_seconds=0.05),
            ),
        ):
            executor = TransformExecutor(node, ctx)
            result = executor.run()

        assert result.status == ExecutionStatus.SUCCESS
        assert result.is_success() is True
        assert result.error_message is None
        mat_meta = result.metadata["materialization"]
        assert mat_meta["required_failed"] is False
        assert mat_meta["succeeded"] == 1
        assert mat_meta["failed"] == 1
        assert mat_meta["results"][0]["type"] == "iceberg"
        assert mat_meta["results"][0]["success"] is False
        assert mat_meta["results"][1]["type"] == "postgresql"
        assert mat_meta["results"][1]["success"] is True

    def test_failed_iceberg_advanced_mode_fails_node_via_required_failed(self, tmp_path: Path):
        """An Iceberg `upsert` failure sets required_failed=True and BaseExecutor.run()
        flips the node status to ExecutionStatus.FAILED."""
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

        # PostgreSQL still ran, and Iceberg upsert failure sets required_failed=True
        # which causes BaseExecutor.run() to mark the node FAILED.
        assert result.status == ExecutionStatus.FAILED
        assert result.is_failed() is True
        assert "Required Iceberg materialization failed" in (result.error_message or "")
        assert "transform.curated_orders[0]:iceberg" in (result.error_message or "")
        assert result.metadata["materialization"]["required_failed"] is True
        assert result.metadata["materialization"]["succeeded"] == 1
        assert result.metadata["materialization"]["failed"] == 1

    def test_python_feature_group_and_source_executors_honour_postgresql_required_semantics(
        self, tmp_path: Path
    ):
        """PythonExecutor, FeatureGroupExecutor, and SourceExecutor mark the node FAILED when
        a required postgresql target fails, and keep SUCCESS when `required: false`."""
        ctx = _make_exec_context(tmp_path)
        default_targets = [
            {"type": "iceberg", "table": "warehouse.curated.features", "mode": "overwrite"},
            {
                "type": "postgresql",
                "connection": "pg_local",
                "table": "public.features",
                "mode": "full",
            },
        ]
        opt_out_targets = [
            {"type": "iceberg", "table": "warehouse.curated.features", "mode": "overwrite"},
            {
                "type": "postgresql",
                "connection": "pg_local",
                "table": "public.features",
                "mode": "full",
                "required": False,
            },
        ]

        # 1. PythonExecutor post_execute reads intermediate parquet into `transform.<name>` view
        intermediate_dir = ctx.target_path / "intermediate"
        intermediate_dir.mkdir(parents=True, exist_ok=True)
        py_parquet = intermediate_dir / "python_py_feat.parquet"
        pd.DataFrame({"user_id": [1, 2], "score": [0.8, 0.9]}).to_parquet(py_parquet, index=False)

        dispatched_views: list[tuple[str, str]] = []

        def record_iceberg(self_disp, con, view_name, target_cfg):
            dispatched_views.append(("iceberg", view_name))
            return WriteResult(success=True, row_count=2, duration_seconds=0.01)

        def record_pg_fail(self_disp, con, view_name, target_cfg):
            dispatched_views.append(("postgresql", view_name))
            raise RuntimeError("pg write error")

        py_node = Node(
            id="python.py_feat",
            name="py_feat",
            node_type=NodeType.PYTHON,
            config={"materializations": default_targets},
        )
        py_exec = PythonExecutor(py_node, ctx)

        with (
            patch.object(MaterializationDispatcher, "_materialize_iceberg", record_iceberg),
            patch.object(MaterializationDispatcher, "_materialize_postgresql", record_pg_fail),
        ):
            py_res = py_exec.post_execute(
                ExecutorResult(node_id=py_node.id, status=ExecutionStatus.SUCCESS, row_count=2)
            )

        assert py_res.status == ExecutionStatus.FAILED
        assert py_res.metadata["materialization"]["required_failed"] is True
        assert "python.py_feat[1]:postgresql" in (py_res.error_message or "")
        assert "failure_category=write_failed" in (py_res.error_message or "")
        assert dispatched_views == [
            ("iceberg", "transform.py_feat"),
            ("postgresql", "transform.py_feat"),
        ]

        # PythonExecutor with required: false keeps SUCCESS
        py_node_opt = Node(
            id="python.py_feat",
            name="py_feat",
            node_type=NodeType.PYTHON,
            config={"materializations": opt_out_targets},
        )
        with (
            patch.object(MaterializationDispatcher, "_materialize_iceberg", record_iceberg),
            patch.object(MaterializationDispatcher, "_materialize_postgresql", record_pg_fail),
        ):
            py_opt_res = PythonExecutor(py_node_opt, ctx).post_execute(
                ExecutorResult(node_id=py_node_opt.id, status=ExecutionStatus.SUCCESS, row_count=2)
            )
        assert py_opt_res.status == ExecutionStatus.SUCCESS
        assert py_opt_res.metadata["materialization"]["required_failed"] is False

        # 2. FeatureGroupExecutor post_execute dispatches from `feature_group.<name>` view
        con = ctx.get_duckdb_connection()
        con.execute("CREATE SCHEMA IF NOT EXISTS feature_group")
        con.execute("CREATE OR REPLACE VIEW feature_group.fg_feat AS SELECT 1 AS user_id")

        fg_node = Node(
            id="feature_group.fg_feat",
            name="fg_feat",
            node_type=NodeType.FEATURE_GROUP,
            config={"materializations": default_targets},
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

        assert fg_res.status == ExecutionStatus.FAILED
        assert fg_res.metadata["materialization"]["required_failed"] is True
        assert "feature_group.fg_feat[1]:postgresql" in (fg_res.error_message or "")
        assert dispatched_views == [
            ("iceberg", "feature_group.fg_feat"),
            ("postgresql", "feature_group.fg_feat"),
        ]

        # 3. SourceExecutor post_execute dispatches from `source.<name>` view
        con.execute("CREATE SCHEMA IF NOT EXISTS source")
        con.execute("CREATE OR REPLACE VIEW source.src_feat AS SELECT 1 AS user_id")
        src_node = Node(
            id="source.src_feat",
            name="src_feat",
            node_type=NodeType.SOURCE,
            config={"source": "csv", "materializations": default_targets},
        )
        src_exec = SourceExecutor(src_node, ctx)
        dispatched_views.clear()

        with (
            patch.object(MaterializationDispatcher, "_materialize_iceberg", record_iceberg),
            patch.object(MaterializationDispatcher, "_materialize_postgresql", record_pg_fail),
        ):
            src_res = src_exec.post_execute(
                ExecutorResult(node_id=src_node.id, status=ExecutionStatus.SUCCESS, row_count=1)
            )

        assert src_res.status == ExecutionStatus.FAILED
        assert src_res.metadata["materialization"]["required_failed"] is True
        assert "source.src_feat[1]:postgresql" in (src_res.error_message or "")
        assert dispatched_views == [
            ("iceberg", "source.src_feat"),
            ("postgresql", "source.src_feat"),
        ]

    def test_secret_scrubbing_in_dispatcher_logs_metadata_and_node_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ):
        """When a postgresql target fails with a DSN or password=... in the exception,
        the secret must never appear in caplog records, result metadata, or node error_message."""
        ctx = _make_exec_context(tmp_path)
        secret_pw = "Sup3rS3cretP@ssw0rd99!"
        raw_exc_text = (
            f"FATAL: password authentication failed for "
            f"postgresql://svc:{secret_pw}@pg.internal:5432/pii_db "
            f"host=pg.internal user=svc password={secret_pw} dbname=pii_db"
        )
        node = Node(
            id="transform.pii_report",
            name="pii_report",
            node_type=NodeType.TRANSFORM,
            config={
                "kind": "transform",
                "name": "pii_report",
                "transform": "SELECT 1 AS report_id",
                "materializations": [
                    {
                        "type": "postgresql",
                        "connection": "pg_pii",
                        "table": "public.pii_report",
                        "mode": "full",
                    }
                ],
            },
        )

        with (
            caplog.at_level(logging.DEBUG),
            patch.object(
                MaterializationDispatcher,
                "_materialize_postgresql",
                side_effect=RuntimeError(raw_exc_text),
            ),
        ):
            result = TransformExecutor(node, ctx).run()

        assert result.status == ExecutionStatus.FAILED
        assert result.error_message is not None
        assert secret_pw not in result.error_message
        assert "svc:" not in result.error_message
        assert "transform.pii_report[0]:postgresql" in result.error_message
        assert "failure_category=write_failed" in result.error_message

        mat_meta = result.metadata["materialization"]
        assert secret_pw not in str(mat_meta)
        assert secret_pw not in mat_meta["results"][0]["error"]
        assert "svc:" not in mat_meta["results"][0]["error"]

        for record in caplog.records:
            assert secret_pw not in record.getMessage()
            assert "svc:" not in record.getMessage()

    @pytest.mark.parametrize("extra_flags", [[], ["--parallel"]])
    def test_cli_seeknal_run_exits_nonzero_when_postgresql_target_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extra_flags: list[str]
    ):
        """CLI `seeknal run` (sequential and `--parallel`) exits non-zero when a PostgreSQL
        materialization target fails."""
        monkeypatch.delenv("ATLAS_API_URL", raising=False)
        monkeypatch.delenv("ATLAS_PORTAL_URL", raising=False)
        monkeypatch.delenv("KEYCLOAK_ISSUER", raising=False)

        transforms_dir = tmp_path / "seeknal" / "transforms"
        transforms_dir.mkdir(parents=True)
        (transforms_dir / "pii_report.yml").write_text(
            "kind: transform\n"
            "name: pii_report\n"
            "transform: SELECT 1 AS customer_id, 'alice' AS name\n"
            "materializations:\n"
            "  - type: iceberg\n"
            "    table: warehouse.curated.pii_report\n"
            "    mode: append\n"
            "  - type: postgresql\n"
            "    connection: pg_local\n"
            "    table: public.pii_report\n"
            "    mode: full\n",
            encoding="utf-8",
        )

        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        with (
            patch.object(
                MaterializationDispatcher,
                "_materialize_iceberg",
                return_value=WriteResult(success=True, row_count=1, duration_seconds=0.01),
            ),
            patch.object(
                MaterializationDispatcher,
                "_materialize_postgresql",
                side_effect=RuntimeError("PostgreSQL target write failed"),
            ),
        ):
            res = runner.invoke(app, ["run", *extra_flags])

        assert res.exit_code != 0
        norm_out = " ".join(res.output.split())
        assert "transform.pii_report[1]:postgresql" in norm_out
        assert "FAILED" in res.output or "failed" in res.output


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
