"""Tests verifying that `kind: exposure, type: database` fails loudly at validation and run time."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
import typer
from typer.testing import CliRunner

from seeknal.cli.main import app
from seeknal.dag.manifest import Node, NodeType
from seeknal.dag.parser import ProjectParser
from seeknal.heartbeat.config import HeartbeatConfig
from seeknal.heartbeat.runner import HeartbeatRunner
from seeknal.workflow.dry_run import dry_run_command, dry_run_yaml
from seeknal.workflow.executor import execute_exposure, execute_preview
from seeknal.workflow.executors.base import (
    ExecutionStatus,
    ExecutorValidationError,
)
from seeknal.workflow.executors.exposure_executor import (
    DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE,
    ExposureExecutor,
    ExposureType,
)
from seeknal.workflow.validators import validate_schema


def _norm(text: str) -> str:
    """Normalize Rich console line-wrapped whitespace for substring checks."""
    return " ".join(text.split())


class DummyExecutionContext:
    """Minimal execution context for testing ExposureExecutor."""

    def __init__(self, tmp_path: Path, *, dry_run: bool = False):
        self.project_name = "test_project"
        self.workspace_path = tmp_path
        self.target_path = tmp_path / "target"
        self.target_path.mkdir(parents=True, exist_ok=True)
        self.dry_run = dry_run
        self.verbose = False
        self.materialize_enabled = False
        self._duckdb_conn = duckdb.connect(":memory:")
        self.duckdb_connection = self._duckdb_conn

    def get_duckdb_connection(self):
        return self._duckdb_conn

    def get_output_path(self, node: Node) -> Path:
        p = self.target_path / "output" / node.name
        p.mkdir(parents=True, exist_ok=True)
        return p

    def get_cache_path(self, node: Node) -> Path:
        p = self.target_path / "cache" / node.node_type.value / f"{node.name}.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        return p


@pytest.fixture
def context(tmp_path: Path) -> DummyExecutionContext:
    return DummyExecutionContext(tmp_path)


class TestExposureDatabaseValidation:
    """Config and schema validation must reject `type: database` (any case)."""

    def test_database_kept_in_exposure_type_enum(self):
        assert ExposureType.DATABASE.value == "database"

    @pytest.mark.parametrize("type_value", ["database", "Database", "DATABASE", "  DaTaBaSe  "])
    def test_executor_validate_rejects_database_any_case(self, context, type_value):
        node = Node(
            id="exposure.export_db",
            name="export_db",
            node_type=NodeType.EXPOSURE,
            config={
                "kind": "exposure",
                "name": "export_db",
                "type": type_value,
                "params": {"table": "production.results"},
            },
        )
        executor = ExposureExecutor(node, context)
        with pytest.raises(ExecutorValidationError) as exc_info:
            executor.validate()

        msg = str(exc_info.value)
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in msg
        assert "not implemented" in msg
        assert "materializations:" in msg
        assert "Invalid exposure type" not in msg

    def test_executor_validate_database_method_rejects_directly(self, context):
        node = Node(
            id="exposure.export_db",
            name="export_db",
            node_type=NodeType.EXPOSURE,
            config={"type": "database"},
        )
        executor = ExposureExecutor(node, context)
        with pytest.raises(ExecutorValidationError) as exc_info:
            executor._validate_database_exposure({"params": {"table": "public.out"}})

        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in str(exc_info.value)

    @pytest.mark.parametrize("type_value", ["database", "Database", "DATABASE"])
    def test_validate_schema_rejects_database_exposure(self, capsys, type_value):
        yaml_data = {
            "kind": "exposure",
            "name": "export_db",
            "type": type_value,
            "params": {"table": "public.results"},
        }
        with pytest.raises(typer.Exit) as exc_info:
            validate_schema(yaml_data)

        assert exc_info.value.exit_code == 1
        captured = capsys.readouterr()
        combined = _norm(captured.out + captured.err)
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in combined

    @pytest.mark.parametrize("schema_only", [False, True])
    @pytest.mark.parametrize("type_value", ["database", "Database"])
    def test_dry_run_yaml_rejects_database_exposure(
        self, tmp_path: Path, capsys, schema_only: bool, type_value: str
    ):
        yml_file = tmp_path / "draft_exposure_db.yml"
        yml_file.write_text(
            f"kind: exposure\n"
            f"name: export_db\n"
            f"type: {type_value}\n"
            f"params:\n"
            f"  table: public.results\n",
            encoding="utf-8",
        )
        with pytest.raises(typer.Exit) as exc_info:
            dry_run_yaml(yml_file, str(yml_file), limit=10, timeout=5, schema_only=schema_only)

        assert exc_info.value.exit_code == 1
        captured = capsys.readouterr()
        combined = _norm(captured.out + captured.err)
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in combined

        with pytest.raises(typer.Exit) as cmd_exc:
            dry_run_command(str(yml_file), limit=10, timeout=5, schema_only=schema_only)
        assert cmd_exc.value.exit_code == 1

    @pytest.mark.parametrize("type_value", ["database", "Database"])
    def test_execute_preview_and_execute_exposure_reject_database(self, tmp_path: Path, type_value: str):
        yaml_data = {
            "kind": "exposure",
            "name": "export_db",
            "type": type_value,
            "params": {"table": "public.results"},
        }
        with pytest.raises(typer.Exit) as exc_info:
            execute_exposure(yaml_data, limit=10, timeout=5)
        assert exc_info.value.exit_code == 1

        with pytest.raises(typer.Exit) as preview_exc:
            execute_preview(yaml_data, tmp_path / "draft.yml", limit=10, timeout=5)
        assert preview_exc.value.exit_code == 1

    def test_project_parser_rejects_database_exposure(self, tmp_path: Path):
        exp_dir = tmp_path / "seeknal" / "exposures"
        exp_dir.mkdir(parents=True)
        (exp_dir / "db_export.yml").write_text(
            "kind: exposure\n"
            "name: db_export\n"
            "type: Database\n"
            "params:\n"
            "  table: public.results\n",
            encoding="utf-8",
        )
        parser = ProjectParser("test_proj", str(tmp_path))
        parser.parse()

        parse_errors = parser.get_errors()
        assert any(DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in e for e in parse_errors)

        validation_errors = parser.validate()
        assert any(DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in e for e in validation_errors)


class TestExposureDatabaseCLIPaths:
    """CLI validation/plan/dry-run commands must fail with non-zero exit and no traceback."""

    def _create_project_with_db_exposure(self, tmp_path: Path, type_value: str = "database") -> Path:
        exposures_dir = tmp_path / "seeknal" / "exposures"
        exposures_dir.mkdir(parents=True)

        exp_file = exposures_dir / "db_out.yml"
        exp_file.write_text(
            f"kind: exposure\n"
            f"name: db_out\n"
            f"type: {type_value}\n"
            f"params:\n"
            f"  table: public.orders_out\n",
            encoding="utf-8",
        )
        return exp_file

    def test_cli_dry_run_rejects_database_exposure(self, tmp_path: Path):
        exp_file = self._create_project_with_db_exposure(tmp_path, type_value="Database")
        runner = CliRunner()
        result = runner.invoke(app, ["dry-run", str(exp_file)])

        assert result.exit_code != 0
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in _norm(result.output)
        assert "Traceback" not in result.output

    def test_cli_plan_rejects_database_exposure(self, tmp_path: Path):
        self._create_project_with_db_exposure(tmp_path, type_value="database")
        runner = CliRunner()
        result = runner.invoke(app, ["plan", "--project-path", str(tmp_path)])

        assert result.exit_code != 0
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in _norm(result.output)
        assert "Traceback" not in result.output

    def test_cli_parse_rejects_database_exposure(self, tmp_path: Path):
        self._create_project_with_db_exposure(tmp_path, type_value="DATABASE")
        runner = CliRunner()
        result = runner.invoke(app, ["parse", "--path", str(tmp_path)])

        assert result.exit_code != 0
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in _norm(result.output)
        assert "Traceback" not in result.output

    def test_cli_validate_rejects_database_exposure(self, tmp_path: Path):
        exp_file = self._create_project_with_db_exposure(tmp_path, type_value="database")
        runner = CliRunner()

        res_file = runner.invoke(app, ["validate", "--config", str(exp_file)])
        assert res_file.exit_code != 0
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in _norm(res_file.output)
        assert "Traceback" not in res_file.output

        res_dir = runner.invoke(app, ["validate", "--config", str(tmp_path)])
        assert res_dir.exit_code != 0
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in _norm(res_dir.output)
        assert "Traceback" not in res_dir.output

    def test_cli_run_and_run_dry_run_reject_database_exposure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        self._create_project_with_db_exposure(tmp_path, type_value="database")
        monkeypatch.delenv("ATLAS_API_URL", raising=False)
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()

        res_dry = runner.invoke(app, ["run", "--dry-run"])
        assert res_dry.exit_code != 0
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in _norm(res_dry.output)
        assert "Traceback" not in res_dry.output

        res_plan = runner.invoke(app, ["run", "--show-plan"])
        assert res_plan.exit_code != 0
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in _norm(res_plan.output)
        assert "Traceback" not in res_plan.output

        res_run = runner.invoke(app, ["run"])
        assert res_run.exit_code != 0
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in _norm(res_run.output)
        assert "Traceback" not in res_run.output

    def test_cli_run_dry_run_preserves_base_behavior_for_source_and_valid_exposure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Regression test: `seeknal run --dry-run` must not call SourceExecutor.validate()
        (which rejects `/tmp/...` paths) when no database exposure is present."""
        sources_dir = tmp_path / "seeknal" / "sources"
        exposures_dir = tmp_path / "seeknal" / "exposures"
        sources_dir.mkdir(parents=True)
        exposures_dir.mkdir(parents=True)

        (sources_dir / "orders.yml").write_text(
            f"kind: source\nname: orders\nsource: csv\ntable: {tmp_path / 'orders.csv'}\n",
            encoding="utf-8",
        )
        (exposures_dir / "file_out.yml").write_text(
            f"kind: exposure\n"
            f"name: file_out\n"
            f"type: file\n"
            f"params:\n"
            f"  format: csv\n"
            f"  path: {tmp_path / 'out.csv'}\n"
            f"inputs:\n"
            f"  - ref: source.orders\n",
            encoding="utf-8",
        )

        monkeypatch.delenv("ATLAS_API_URL", raising=False)
        monkeypatch.chdir(tmp_path)
        runner = CliRunner()
        res_dry = runner.invoke(app, ["run", "--dry-run"])
        assert res_dry.exit_code == 0
        assert "Would execute: source.orders" in res_dry.output
        assert "Would execute: exposure.file_out" in res_dry.output

    def test_cli_validate_ignores_dag_build_errors_when_no_database_exposure(
        self, tmp_path: Path
    ):
        """Regression test: `seeknal validate` must not add new hard failures for DAG
        build errors on projects without database exposures."""
        transforms_dir = tmp_path / "seeknal" / "transforms"
        transforms_dir.mkdir(parents=True)
        # Cyclic transform dependency without database exposure
        (transforms_dir / "a.yml").write_text(
            "kind: transform\nname: a\ntransform: SELECT 1\ninputs:\n  - ref: transform.b\n",
            encoding="utf-8",
        )
        (transforms_dir / "b.yml").write_text(
            "kind: transform\nname: b\ntransform: SELECT 1\ninputs:\n  - ref: transform.a\n",
            encoding="utf-8",
        )
        runner = CliRunner()
        res = runner.invoke(app, ["validate", "--config", str(tmp_path)])
        assert res.exit_code == 0


class TestExposureDatabaseRuntimeFailure:
    """Execution reaching database exposure anyway must fail with the exact message."""

    @pytest.mark.parametrize("type_value", ["database", "Database", "DATABASE"])
    def test_execute_returns_failed_status_when_validation_bypassed(
        self, tmp_path: Path, type_value: str
    ):
        ctx = DummyExecutionContext(tmp_path, dry_run=False)
        node = Node(
            id="exposure.export_db",
            name="export_db",
            node_type=NodeType.EXPOSURE,
            config={
                "kind": "exposure",
                "name": "export_db",
                "type": type_value,
                "params": {"table": "public.results"},
            },
        )
        executor = ExposureExecutor(node, ctx)

        # Direct execute() call (validation bypassed)
        result = executor.execute()
        assert result.status == ExecutionStatus.FAILED
        assert result.is_failed() is True
        assert result.is_success() is False
        assert result.error_message == DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE

        # run() with validate() patched out
        with patch.object(executor, "validate"):
            run_result = executor.run()
        assert run_result.status == ExecutionStatus.FAILED
        assert run_result.is_failed() is True
        assert run_result.is_success() is False
        assert run_result.error_message == DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE

    def test_execute_dry_run_does_not_report_success(self, tmp_path: Path):
        ctx = DummyExecutionContext(tmp_path, dry_run=True)
        node = Node(
            id="exposure.export_db",
            name="export_db",
            node_type=NodeType.EXPOSURE,
            config={
                "kind": "exposure",
                "name": "export_db",
                "type": "Database",
                "params": {"table": "public.results"},
            },
        )
        executor = ExposureExecutor(node, ctx)

        result = executor.execute()
        assert result.status == ExecutionStatus.FAILED
        assert result.is_failed() is True
        assert result.is_success() is False
        assert result.is_dry_run is True
        assert result.error_message == DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE

    def test_execute_database_exposure_helper_returns_failed_delivery_result(self, context):
        node = Node(
            id="exposure.export_db",
            name="export_db",
            node_type=NodeType.EXPOSURE,
            config={
                "type": "database",
                "params": {"table": "public.results"},
            },
        )
        executor = ExposureExecutor(node, context)
        delivery = executor._execute_database_exposure(data=[{"id": 1}])

        assert delivery.success is False
        assert delivery.records_sent == 0
        assert delivery.error_message == DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE

    def test_heartbeat_runner_marks_database_exposure_failed(self, tmp_path: Path):
        exposures_dir = tmp_path / "seeknal" / "exposures"
        exposures_dir.mkdir(parents=True)
        (exposures_dir / "db_out.yml").write_text(
            "kind: exposure\n"
            "name: db_out\n"
            "type: database\n"
            "params:\n"
            "  table: public.out\n",
            encoding="utf-8",
        )

        hb_config = HeartbeatConfig(project_name="test_hb")
        hb_runner = HeartbeatRunner.__new__(HeartbeatRunner)
        hb_runner._config = hb_config
        hb_runner._project_root = tmp_path

        # Normal heartbeat exposure run (triggers validate())
        errors: list[str] = []
        results = hb_runner._run_exposures(errors)
        assert len(results) == 1
        assert results[0].status == "failed"
        assert results[0].error is not None
        assert DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in results[0].error
        assert any(DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in err for err in errors)

        # Heartbeat exposure run with validate() bypassed (triggers execute())
        errors_bypassed: list[str] = []
        with patch.object(ExposureExecutor, "validate", return_value=None):
            results_bypassed = hb_runner._run_exposures(errors_bypassed)
        assert len(results_bypassed) == 1
        assert results_bypassed[0].status == "failed"
        assert results_bypassed[0].error == DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE
        assert any(DATABASE_EXPOSURE_NOT_IMPLEMENTED_MESSAGE in err for err in errors_bypassed)


class TestOtherExposureTypesUnchanged:
    """Other exposure types (file, api, notification, starrocks_mv, report) still validate and run."""

    def test_file_exposure_validates_and_executes(self, tmp_path: Path):
        out_csv = tmp_path / "exports" / "users.csv"
        yaml_data = {
            "kind": "exposure",
            "name": "export_users",
            "type": "file",
            "params": {"format": "csv", "path": str(out_csv)},
        }
        # Schema validation passes
        validate_schema(yaml_data)

        ctx = DummyExecutionContext(tmp_path)
        node = Node(
            id="exposure.export_users",
            name="export_users",
            node_type=NodeType.EXPOSURE,
            config=yaml_data,
        )
        executor = ExposureExecutor(node, ctx)
        executor.validate()
        result = executor.run()
        assert result.is_success()
        assert out_csv.exists()

    def test_api_exposure_validates(self, context):
        yaml_data = {
            "kind": "exposure",
            "name": "post_users",
            "type": "api",
            "url": "https://api.example.com/v1/users",
            "params": {"timeout": 10},
        }
        validate_schema(yaml_data)

        node = Node(
            id="exposure.post_users",
            name="post_users",
            node_type=NodeType.EXPOSURE,
            config=yaml_data,
        )
        executor = ExposureExecutor(node, context)
        executor.validate()

    def test_notification_starrocks_mv_and_report_exposures_validate(self, context):
        notif_node = Node(
            id="exposure.notify_slack",
            name="notify_slack",
            node_type=NodeType.EXPOSURE,
            config={
                "kind": "exposure",
                "name": "notify_slack",
                "type": "notification",
                "params": {"channel": "slack", "url": "https://hooks.slack.com/services/T/B/X"},
            },
        )
        ExposureExecutor(notif_node, context).validate()
        validate_schema(notif_node.config)

        sr_node = Node(
            id="exposure.sr_mv",
            name="sr_mv",
            node_type=NodeType.EXPOSURE,
            config={
                "kind": "exposure",
                "name": "sr_mv",
                "type": "starrocks_materialized_view",
                "params": {"mv_name": "mv_orders", "query": "SELECT 1"},
            },
        )
        ExposureExecutor(sr_node, context).validate()
        validate_schema(sr_node.config)

        report_node = Node(
            id="exposure.weekly_report",
            name="weekly_report",
            node_type=NodeType.EXPOSURE,
            config={
                "kind": "exposure",
                "name": "weekly_report",
                "type": "report",
                "params": {"prompt": "Weekly revenue summary"},
            },
        )
        ExposureExecutor(report_node, context).validate()
        validate_schema(report_node.config)
        res = ExposureExecutor(report_node, context).run()
        assert res.is_success()
