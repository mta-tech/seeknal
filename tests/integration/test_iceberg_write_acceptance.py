"""Acceptance coverage for advanced Iceberg writes through public entry points.

All catalog traffic is constrained to the authenticated loopback REST fixture.
No environment-provided catalog or object-store configuration is used.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform, MonthTransform
from pyiceberg.types import DateType, IntegerType, NestedField, StringType
from typer.testing import CliRunner

from seeknal.cli.main import app
from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.workflow.executors.base import (
    ExecutionContext,
    ExecutionStatus,
    ExecutorResult,
)
from seeknal.workflow.materialization.config import CatalogConfig
from seeknal.workflow.materialization.operations import WriteError, write_to_iceberg
from seeknal.workflow.runner import DAGRunner
from tests.integration.iceberg_rest_fixture import RunningRestFixture, run_rest_fixture

ICEBERG_SCHEMA = Schema(
    NestedField(1, "id", IntegerType()),
    NestedField(2, "value", StringType()),
    NestedField(3, "business_date", DateType()),
)


@pytest.fixture(autouse=True)
def scrub_external_catalog_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in tuple(os.environ):
        if name.startswith(("AWS_", "S3_", "LAKEKEEPER_", "ICEBERG_")):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")


@pytest.fixture
def rest_fixture(tmp_path: Path) -> Any:
    with run_rest_fixture(tmp_path) as fixture:
        yield fixture


@pytest.fixture
def con() -> Any:
    connection = duckdb.connect(":memory:")
    try:
        yield connection
    finally:
        connection.close()


def catalog_config(fixture: RunningRestFixture) -> CatalogConfig:
    return CatalogConfig(
        uri=fixture.uri,
        warehouse=fixture.warehouse,
        bearer_token=fixture.token,
    )


def replace_view(con: Any, projection: str) -> None:
    con.execute("DROP VIEW IF EXISTS incoming")
    con.execute(f"CREATE VIEW incoming AS {projection}")


def table_rows(fixture: RunningRestFixture, name: str) -> list[dict[str, Any]]:
    table = fixture.backend.load_table(tuple(name.split(".")))
    return sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["id"])


def snapshot_id(fixture: RunningRestFixture, name: str) -> int | None:
    table = fixture.backend.load_table(tuple(name.split(".")))
    current = table.current_snapshot()
    return current.snapshot_id if current else None


def create_target(
    fixture: RunningRestFixture,
    name: str,
    *,
    partition_spec: PartitionSpec | None = None,
) -> None:
    namespace, table_name = name.split(".")
    if (namespace,) not in fixture.backend.list_namespaces():
        fixture.backend.create_namespace(namespace)
    fixture.backend.create_table(
        (namespace, table_name),
        ICEBERG_SCHEMA,
        partition_spec=partition_spec or PartitionSpec(),
        properties={"format-version": "2"},
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("projection", "unique_keys", "message"),
    [
        (
            "SELECT 1 AS id, 'one' AS value, DATE '2026-09-14' AS business_date",
            ["missing_id"],
            "unique_keys columns are missing",
        ),
        (
            (
                "SELECT CAST(NULL AS INTEGER) AS id, 'one' AS value, "
                "DATE '2026-09-14' AS business_date"
            ),
            ["id"],
            "unique_keys columns must not contain null",
        ),
        (
            (
                "SELECT * FROM (VALUES (1, 'one', DATE '2026-09-14'), "
                "(1, 'duplicate', DATE '2026-09-15')) "
                "AS t(id, value, business_date)"
            ),
            ["id"],
            "duplicate rows for unique_keys",
        ),
    ],
    ids=["missing-key", "null-key", "duplicate-key"],
)
def test_invalid_upsert_keys_fail_before_any_catalog_request(
    con: Any,
    rest_fixture: RunningRestFixture,
    projection: str,
    unique_keys: list[str],
    message: str,
) -> None:
    replace_view(con, projection)
    requests_before = len(rest_fixture.state.received_authorization)

    with pytest.raises(WriteError, match=message):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.invalid_keys",
            "incoming",
            "upsert",
            unique_keys=unique_keys,
            catalog_config=catalog_config(rest_fixture),
        )

    assert len(rest_fixture.state.received_authorization) == requests_before
    assert rest_fixture.state.create_table_attempts == 0
    assert rest_fixture.state.commit_attempts == 0


@pytest.mark.integration
@pytest.mark.parametrize("non_finite", ["NaN", "Infinity"], ids=["nan", "infinity"])
def test_non_finite_float_upsert_keys_fail_before_catalog_mutation(
    con: Any,
    rest_fixture: RunningRestFixture,
    non_finite: str,
) -> None:
    replace_view(
        con,
        "SELECT * FROM (VALUES "
        f"('{non_finite}'::DOUBLE, 'first', DATE '2026-09-14'), "
        f"('{non_finite}'::DOUBLE, 'second', DATE '2026-09-15')) "
        "AS t(id, value, business_date)",
    )

    with pytest.raises(WriteError, match="(?i)(finite|unique_keys)"):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.non_finite_keys",
            "incoming",
            "upsert",
            unique_keys=["id"],
            catalog_config=catalog_config(rest_fixture),
        )

    assert rest_fixture.state.create_table_attempts == 0
    assert rest_fixture.state.commit_attempts == 0


@pytest.mark.integration
def test_batch_budget_accepts_exact_boundary_and_rejects_one_byte_over_preflight(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(
        con,
        "SELECT 1 AS id, 'one' AS value, DATE '2026-09-14' AS business_date",
    )
    frozen = con.execute("SELECT * FROM incoming").fetch_arrow_table()

    accepted = write_to_iceberg(
        con,
        "local_rest",
        "analytics.exact_budget",
        "incoming",
        "upsert",
        unique_keys=["id"],
        max_batch_bytes=frozen.nbytes,
        catalog_config=catalog_config(rest_fixture),
    )
    requests_before = len(rest_fixture.state.received_authorization)
    creates_before = rest_fixture.state.create_table_attempts
    commits_before = rest_fixture.state.commit_attempts

    with pytest.raises(WriteError, match="exceeds max_batch_bytes"):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.over_budget",
            "incoming",
            "upsert",
            unique_keys=["id"],
            max_batch_bytes=frozen.nbytes - 1,
            catalog_config=catalog_config(rest_fixture),
        )

    assert accepted.input_row_count == 1
    assert len(rest_fixture.state.received_authorization) == requests_before
    assert rest_fixture.state.create_table_attempts == creates_before
    assert rest_fixture.state.commit_attempts == commits_before


@pytest.mark.integration
def test_reordered_source_columns_are_matched_by_name(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(
        con,
        "SELECT 1 AS id, 'old' AS value, DATE '2026-09-14' AS business_date",
    )
    write_to_iceberg(
        con,
        "local_rest",
        "analytics.reordered",
        "incoming",
        "upsert",
        unique_keys=["id"],
        catalog_config=catalog_config(rest_fixture),
    )

    replace_view(
        con,
        "SELECT 'new' AS value, DATE '2026-09-15' AS business_date, 1 AS id",
    )
    result = write_to_iceberg(
        con,
        "local_rest",
        "analytics.reordered",
        "incoming",
        "upsert",
        unique_keys=["id"],
        catalog_config=catalog_config(rest_fixture),
    )

    assert result.updated_row_count == 1
    assert table_rows(rest_fixture, "analytics.reordered") == [
        {"id": 1, "value": "new", "business_date": date(2026, 9, 15)}
    ]


@pytest.mark.integration
def test_incompatible_source_schema_fails_without_commit(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(
        con,
        "SELECT 1 AS id, 'old' AS value, DATE '2026-09-14' AS business_date",
    )
    write_to_iceberg(
        con,
        "local_rest",
        "analytics.strict_schema",
        "incoming",
        "upsert",
        unique_keys=["id"],
        catalog_config=catalog_config(rest_fixture),
    )
    before = table_rows(rest_fixture, "analytics.strict_schema")
    before_snapshot = snapshot_id(rest_fixture, "analytics.strict_schema")
    commits_before = rest_fixture.state.commit_attempts
    replace_view(
        con,
        "SELECT 'not-an-integer' AS id, 'new' AS value, "
        "DATE '2026-09-15' AS business_date",
    )

    with pytest.raises(WriteError, match="Source schema is incompatible"):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.strict_schema",
            "incoming",
            "upsert",
            unique_keys=["id"],
            catalog_config=catalog_config(rest_fixture),
        )

    assert rest_fixture.state.commit_attempts == commits_before
    assert snapshot_id(rest_fixture, "analytics.strict_schema") == before_snapshot
    assert table_rows(rest_fixture, "analytics.strict_schema") == before


@pytest.mark.integration
@pytest.mark.parametrize(
    ("status", "expected_category"),
    [(409, "conflict"), (500, "commit_unknown")],
)
def test_commit_failures_have_machine_readable_category(
    con: Any,
    rest_fixture: RunningRestFixture,
    status: int,
    expected_category: str,
) -> None:
    replace_view(
        con,
        "SELECT 1 AS id, 'old' AS value, DATE '2026-09-14' AS business_date",
    )
    write_to_iceberg(
        con,
        "local_rest",
        "analytics.categorized_failure",
        "incoming",
        "upsert",
        unique_keys=["id"],
        catalog_config=catalog_config(rest_fixture),
    )
    rest_fixture.state.forced_commit_status = status
    replace_view(
        con,
        "SELECT 1 AS id, 'new' AS value, DATE '2026-09-14' AS business_date",
    )

    with pytest.raises(WriteError) as failure:
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.categorized_failure",
            "incoming",
            "upsert",
            unique_keys=["id"],
            catalog_config=catalog_config(rest_fixture),
        )

    assert failure.value.failure_category == expected_category


@pytest.mark.integration
def test_null_insert_overwrite_partition_fails_before_any_catalog_request(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    replace_view(
        con,
        "SELECT 1 AS id, 'one' AS value, CAST(NULL AS DATE) AS business_date",
    )
    requests_before = len(rest_fixture.state.received_authorization)

    with pytest.raises(WriteError, match="partition_by columns must not contain null"):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.null_partition",
            "incoming",
            "insert_overwrite",
            partition_by=["business_date"],
            catalog_config=catalog_config(rest_fixture),
        )

    assert len(rest_fixture.state.received_authorization) == requests_before
    assert rest_fixture.state.create_table_attempts == 0
    assert rest_fixture.state.commit_attempts == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    "target_shape",
    ["unpartitioned", "mismatched", "transformed", "evolved"],
)
def test_insert_overwrite_rejects_unsupported_target_spec_for_empty_batch(
    con: Any,
    rest_fixture: RunningRestFixture,
    target_shape: str,
) -> None:
    transformed = PartitionSpec(
        PartitionField(3, 1000, MonthTransform(), "business_date_month")
    )
    identity = PartitionSpec(
        PartitionField(3, 1000, IdentityTransform(), "business_date")
    )
    target_name = f"analytics.{target_shape}_target"
    if target_shape == "unpartitioned":
        create_target(rest_fixture, target_name)
    elif target_shape == "mismatched":
        create_target(rest_fixture, target_name, partition_spec=identity)
    elif target_shape == "transformed":
        create_target(rest_fixture, target_name, partition_spec=transformed)
    else:
        create_target(rest_fixture, target_name)
        table = rest_fixture.backend.load_table(tuple(target_name.split(".")))
        table.update_spec().add_identity("business_date").commit()
        assert len(table.refresh().metadata.specs()) > 1

    replace_view(
        con,
        "SELECT CAST(NULL AS INTEGER) AS id, CAST(NULL AS VARCHAR) AS value, "
        "CAST(NULL AS DATE) AS business_date WHERE FALSE",
    )
    commits_before = rest_fixture.state.commit_attempts

    expected = {
        "unpartitioned": "requires a partitioned Iceberg table",
        "mismatched": "partition_by does not match",
        "transformed": "supports identity partition transforms only",
        "evolved": "Partition spec evolution is not supported",
    }[target_shape]
    with pytest.raises(WriteError, match=expected):
        write_to_iceberg(
            con,
            "local_rest",
            target_name,
            "incoming",
            "insert_overwrite",
            partition_by=["id"] if target_shape == "mismatched" else ["business_date"],
            catalog_config=catalog_config(rest_fixture),
        )

    assert rest_fixture.state.commit_attempts == commits_before


@pytest.mark.integration
def test_source_relation_name_is_quoted_as_data_not_executed_as_sql(
    con: Any,
    rest_fixture: RunningRestFixture,
) -> None:
    con.execute("CREATE TABLE injection_guard(value INTEGER)")
    con.execute("INSERT INTO injection_guard VALUES (7)")
    malicious_name = 'incoming"; DROP TABLE injection_guard; --'
    requests_before = len(rest_fixture.state.received_authorization)

    with pytest.raises(WriteError, match="Failed to freeze source relation"):
        write_to_iceberg(
            con,
            "local_rest",
            "analytics.never_written",
            malicious_name,
            "upsert",
            unique_keys=["id"],
            catalog_config=catalog_config(rest_fixture),
        )

    assert con.execute("SELECT value FROM injection_guard").fetchone() == (7,)
    assert len(rest_fixture.state.received_authorization) == requests_before


@pytest.mark.integration
def test_seeknal_run_executes_yaml_upsert_against_explicit_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "rest"
    fixture_path.mkdir()
    with run_rest_fixture(fixture_path) as fixture:
        project = tmp_path / "project"
        transforms = project / "seeknal" / "transforms"
        transforms.mkdir(parents=True)
        profile = project / "profiles.yml"
        profile.write_text(
            "materialization:\n"
            "  enabled: true\n"
            "  catalog:\n"
            "    type: rest\n"
            f"    uri: {fixture.uri}\n"
            f"    warehouse: {fixture.warehouse}\n"
            f"    bearer_token: {fixture.token}\n"
            "    verify_tls: true\n"
        )
        transform = transforms / "dim_value.yml"
        transform.write_text(
            "kind: transform\n"
            "name: dim_value\n"
            "transform: |\n"
            "  SELECT 1 AS id, 'one' AS value, DATE '2026-09-14' AS business_date\n"
            "materializations:\n"
            "  - type: iceberg\n"
            "    table: local_rest.analytics.cli_dim_value\n"
            "    mode: upsert\n"
            "    unique_keys: [id]\n"
            "    create_table: true\n"
        )
        monkeypatch.chdir(project)

        result = CliRunner().invoke(
            app,
            ["run", "--full", "--profile", str(profile)],
        )

        assert result.exit_code == 0, result.output
        assert "Failed:         0" not in result.output
        assert table_rows(fixture, "analytics.cli_dim_value") == [
            {"id": 1, "value": "one", "business_date": date(2026, 9, 14)}
        ]
        state = json.loads((project / "target" / "run_state.json").read_text())
        materialization = state["nodes"]["transform.dim_value"]["metadata"][
            "materialization"
        ]
        assert materialization["success"] is True
        assert materialization["results"][0]["write_result"]["mode"] == "upsert"


@pytest.mark.integration
def test_seeknal_run_singular_materialization_uses_external_explicit_profile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "rest"
    fixture_path.mkdir()
    with run_rest_fixture(fixture_path) as fixture:
        project = tmp_path / "project"
        transforms = project / "seeknal" / "transforms"
        transforms.mkdir(parents=True)

        selected_dir = tmp_path / "selected-profile"
        selected_dir.mkdir()
        selected_profile = selected_dir / "profiles.yml"
        selected_profile.write_text(
            "materialization:\n"
            "  enabled: true\n"
            "  default_mode: upsert\n"
            "  unique_keys: [id]\n"
            "  catalog:\n"
            "    type: rest\n"
            f"    uri: {fixture.uri}\n"
            f"    warehouse: {fixture.warehouse}\n"
            f"    bearer_token: {fixture.token}\n"
            "    verify_tls: true\n"
        )
        hostile_dir = tmp_path / "hostile-default"
        hostile_dir.mkdir()
        hostile_profile = hostile_dir / "profiles.yml"
        hostile_profile.write_text(
            "materialization:\n"
            "  enabled: true\n"
            "  default_mode: insert_overwrite\n"
            "  catalog:\n"
            "    type: rest\n"
            "    uri: http://127.0.0.1:1/invalid-default\n"
            "    warehouse: file:///invalid-default\n"
            "    bearer_token: invalid-default-token\n"
        )
        from seeknal.workflow.materialization.profile_loader import ProfileLoader

        monkeypatch.setattr(ProfileLoader, "DEFAULT_PROFILE_PATH", hostile_profile)
        (transforms / "singular.yml").write_text(
            "kind: transform\n"
            "name: singular_profile\n"
            "transform: SELECT 7 AS id, 'selected' AS value\n"
            "materialization:\n"
            "  enabled: true\n"
            "  table: local_rest.analytics.singular_profile\n"
        )
        monkeypatch.chdir(project)

        result = CliRunner().invoke(
            app,
            ["run", "--full", "--profile", str(selected_profile)],
        )

        assert result.exit_code == 0, result.output
        assert table_rows(fixture, "analytics.singular_profile") == [
            {"id": 7, "value": "selected"}
        ]
        state = json.loads((project / "target" / "run_state.json").read_text())
        write_result = state["nodes"]["transform.singular_profile"]["metadata"][
            "materialization"
        ]["results"][0]["write_result"]
        assert write_result["mode"] == "upsert"
        assert write_result["inserted_row_count"] == 1


@pytest.mark.integration
def test_seeknal_run_marks_failed_yaml_materialization_as_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture_path = tmp_path / "rest"
    fixture_path.mkdir()
    with run_rest_fixture(fixture_path) as fixture:
        project = tmp_path / "project"
        transforms = project / "seeknal" / "transforms"
        transforms.mkdir(parents=True)
        profile = project / "profiles.yml"
        profile.write_text(
            "materialization:\n"
            "  enabled: true\n"
            "  catalog:\n"
            "    type: rest\n"
            f"    uri: {fixture.uri}\n"
            f"    warehouse: {fixture.warehouse}\n"
            f"    bearer_token: {fixture.token}\n"
            "    verify_tls: true\n"
        )
        (transforms / "invalid.yml").write_text(
            "kind: transform\n"
            "name: invalid_materialization\n"
            "transform: SELECT 1 AS id, 'one' AS value\n"
            "materializations:\n"
            "  - type: iceberg\n"
            "    table: local_rest.analytics.invalid_materialization\n"
            "    mode: upsert\n"
            "    unique_keys: [missing_id]\n"
        )
        monkeypatch.chdir(project)

        result = CliRunner().invoke(
            app,
            ["run", "--full", "--profile", str(profile)],
        )

        assert result.exit_code != 0
        state = json.loads((project / "target" / "run_state.json").read_text())
        assert state["nodes"]["transform.invalid_materialization"]["status"] == "failed"
        assert fixture.state.create_table_attempts == 0
        assert fixture.state.commit_attempts == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    ("failure_case", "expected_category"),
    [("conflict", "conflict"), ("acknowledgement-loss", "commit_unknown")],
)
def test_seeknal_run_does_not_retry_commit_requiring_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_case: str,
    expected_category: str,
) -> None:
    fixture_path = tmp_path / "rest"
    fixture_path.mkdir()
    with run_rest_fixture(fixture_path) as fixture:
        setup_con = duckdb.connect(":memory:")
        try:
            replace_view(
                setup_con,
                "SELECT 1 AS id, 'old' AS value, DATE '2026-09-14' AS business_date",
            )
            write_to_iceberg(
                setup_con,
                "local_rest",
                "analytics.retry_guard",
                "incoming",
                "upsert",
                unique_keys=["id"],
                catalog_config=catalog_config(fixture),
            )
        finally:
            setup_con.close()

        if failure_case == "conflict":
            fixture.state.forced_commit_status = 409
        else:
            original_commit = fixture.backend.commit_table

            def commit_then_lose_acknowledgement(*args: Any, **kwargs: Any) -> Any:
                original_commit(*args, **kwargs)
                raise RuntimeError("simulated acknowledgement loss after publication")

            monkeypatch.setattr(
                fixture.backend,
                "commit_table",
                commit_then_lose_acknowledgement,
            )

        project = tmp_path / "project"
        transforms = project / "seeknal" / "transforms"
        transforms.mkdir(parents=True)
        profile = tmp_path / "retry-profile.yml"
        profile.write_text(
            "materialization:\n"
            "  enabled: true\n"
            "  catalog:\n"
            "    type: rest\n"
            f"    uri: {fixture.uri}\n"
            f"    warehouse: {fixture.warehouse}\n"
            f"    bearer_token: {fixture.token}\n"
            "    verify_tls: true\n"
        )
        (transforms / "retry_guard.yml").write_text(
            "kind: transform\n"
            "name: retry_guard\n"
            "transform: |\n"
            "  SELECT 1 AS id, 'new' AS value, DATE '2026-09-14' AS business_date\n"
            "materializations:\n"
            "  - type: iceberg\n"
            "    table: local_rest.analytics.retry_guard\n"
            "    mode: upsert\n"
            "    unique_keys: [id]\n"
        )
        attempts_before = fixture.state.commit_attempts
        monkeypatch.chdir(project)

        result = CliRunner().invoke(
            app,
            [
                "run",
                "--full",
                "--continue-on-error",
                "--retry",
                "2",
                "--profile",
                str(profile),
            ],
        )

        assert result.exit_code != 0
        assert fixture.state.commit_attempts - attempts_before == 1
        assert "automatic retries suppressed" in result.output
        state = json.loads((project / "target" / "run_state.json").read_text())
        node_state = state["nodes"]["transform.retry_guard"]
        assert node_state["status"] == "failed"
        failure = node_state["metadata"]["materialization"]["results"][0]
        assert failure["failure_category"] == expected_category

        expected_value = "old" if failure_case == "conflict" else "new"
        assert table_rows(fixture, "analytics.retry_guard") == [
            {"id": 1, "value": expected_value, "business_date": date(2026, 9, 14)}
        ]


def test_dag_runner_with_execution_context_does_not_retry_unknown_commit(
    tmp_path: Path,
) -> None:
    node = Node(
        id="transform.retry_guard",
        name="retry_guard",
        node_type=NodeType.TRANSFORM,
        config={"transform": "SELECT 1"},
    )
    manifest = Manifest(project="retry-guard")
    manifest.add_node(node)
    target_path = tmp_path / "target"
    context = ExecutionContext(
        project_name="retry-guard",
        workspace_path=tmp_path,
        target_path=target_path,
    )
    runner = DAGRunner(manifest, target_path=target_path, exec_context=context)
    executor = MagicMock()
    executor.run.return_value = ExecutorResult(
        node_id=node.id,
        status=ExecutionStatus.FAILED,
        error_message="Required Iceberg materialization failed",
        metadata={
            "materialization": {
                "required_failed": True,
                "results": [{"failure_category": "commit_unknown"}],
            }
        },
    )

    with patch("seeknal.workflow.executors.get_executor", return_value=executor):
        summary = runner.run(full=True, continue_on_error=True, retry=2)

    assert executor.run.call_count == 1
    assert summary.failed_nodes == 1
    node_state = runner.run_state.nodes[node.id]
    assert node_state.status == "failed"
    assert (
        node_state.metadata["materialization"]["results"][0]["failure_category"]
        == "commit_unknown"
    )
