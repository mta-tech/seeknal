"""Feature Service discovery, DAG, manifest, and execution semantics."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from seeknal.cli.main import app
from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.workflow.dag import DAGBuilder
from seeknal.workflow.manifest_builder import build_manifest_from_dag
from seeknal.workflow.runner import DAGRunner
from seeknal.workflow.state import load_state

FEATURE_GROUP_YAML = """\
kind: feature_group
name: customer_profile
owner: ml-platform
entity:
  name: customer
  join_keys: [customer_id]
features:
  customer_id:
    dtype: string
  age:
    dtype: integer
materializations:
  - type: atlas_online
    connection: atlas_feature_store
    table: customer_profile
"""

FEATURE_SERVICE_YAML = """\
kind: feature_service
name: customer_analytics
version: "1"
variant: default
owner: ml-platform
views:
  - ref: feature_group.customer_profile
    features: [age]
"""


def _write_yaml_project(project: Path) -> None:
    feature_groups = project / "seeknal" / "feature_groups"
    feature_services = project / "seeknal" / "feature_services"
    feature_groups.mkdir(parents=True)
    feature_services.mkdir(parents=True)
    (feature_groups / "customer_profile.yml").write_text(
        FEATURE_GROUP_YAML, encoding="utf-8"
    )
    (feature_services / "customer_analytics.yml").write_text(
        FEATURE_SERVICE_YAML, encoding="utf-8"
    )


def _write_python_project(project: Path) -> None:
    feature_groups = project / "seeknal" / "feature_groups"
    feature_services = project / "seeknal" / "feature_services"
    feature_groups.mkdir(parents=True)
    feature_services.mkdir(parents=True)
    (feature_groups / "customer_profile.py").write_text(
        """\
from seeknal.pipeline import feature_group

@feature_group(
    name="customer_profile",
    entity={"name": "customer", "join_keys": ["customer_id"]},
    features={"customer_id": {"dtype": "string"}, "age": {"dtype": "integer"}},
    materializations=[{
        "type": "atlas_online",
        "connection": "atlas_feature_store",
        "table": "customer_profile",
    }],
)
def customer_profile(ctx):
    return ctx
""",
        encoding="utf-8",
    )
    (feature_services / "customer_analytics.py").write_text(
        """\
from seeknal.pipeline import FeatureView, feature_service

@feature_service(
    name="customer_analytics",
    version="1",
    variant="default",
    owner="ml-platform",
    views=[FeatureView(ref="feature_group.customer_profile", features=["age"])],
)
def customer_analytics():
    pass
""",
        encoding="utf-8",
    )


def test_yaml_and_python_feature_services_normalize_identically(tmp_path):
    yaml_project = tmp_path / "yaml"
    python_project = tmp_path / "python"
    _write_yaml_project(yaml_project)
    _write_python_project(python_project)

    yaml_builder = DAGBuilder(project_path=yaml_project)
    python_builder = DAGBuilder(project_path=python_project)
    yaml_builder.build()
    python_builder.build()

    yaml_node = yaml_builder.get_node("feature_service.customer_analytics")
    python_node = python_builder.get_node("feature_service.customer_analytics")
    assert yaml_node is not None
    assert python_node is not None
    assert yaml_node.yaml_data == python_node.yaml_data
    assert yaml_builder.get_upstream("feature_service.customer_analytics") == {
        "feature_group.customer_profile"
    }


def test_manifest_round_trips_feature_service(tmp_path):
    _write_yaml_project(tmp_path)
    builder = DAGBuilder(project_path=tmp_path)
    builder.build()
    manifest = build_manifest_from_dag(builder, "test")

    node = manifest.get_node("feature_service.customer_analytics")
    assert node is not None
    assert node.node_type is NodeType.FEATURE_SERVICE
    assert manifest.get_upstream_nodes(node.id) == {"feature_group.customer_profile"}

    restored = Manifest.from_dict(manifest.to_dict())
    assert restored.get_node(node.id).node_type is NodeType.FEATURE_SERVICE


def test_feature_service_is_excluded_from_execution(tmp_path):
    manifest = Manifest(project="test")
    manifest.add_node(
        Node(
            id="feature_service.customer_analytics",
            name="customer_analytics",
            node_type=NodeType.FEATURE_SERVICE,
        )
    )

    runner = DAGRunner(manifest, target_path=tmp_path / "target")
    to_run, reasons = runner._get_nodes_to_run(
        full=True,
        nodes=["feature_service.customer_analytics"],
    )

    assert to_run == set()
    assert reasons["feature_service.customer_analytics"] == "contract-only resource"


def test_yaml_run_applies_fingerprint_and_skips_feature_service(tmp_path, monkeypatch):
    _write_yaml_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    executor = MagicMock()
    execution_result = MagicMock()
    execution_result.is_success.return_value = True
    execution_result.row_count = 1
    execution_result.metadata = {}
    executor.run.return_value = execution_result

    with patch(
        "seeknal.workflow.executors.get_executor",
        return_value=executor,
    ) as get_executor:
        result = CliRunner().invoke(app, ["run", "--full"])

    assert result.exit_code == 0, result.output
    assert "contract-only" in result.output
    assert [call.args[0].kind for call in get_executor.call_args_list] == [
        NodeType.FEATURE_GROUP
    ]

    state = load_state(tmp_path / "target" / "run_state.json")
    assert state is not None
    feature_group = state.nodes["feature_group.customer_profile"]
    assert feature_group.is_success()
    assert feature_group.fingerprint is not None
    feature_service = state.nodes["feature_service.customer_analytics"]
    assert feature_service.is_success()
    assert feature_service.fingerprint is not None
    assert feature_service.metadata["contract_only"] is True

    execution_context = get_executor.call_args.args[1]
    fingerprints = execution_context.config["_seeknal_node_fingerprints"]
    assert fingerprints["feature_group.customer_profile"]["combined"] == (
        feature_group.fingerprint.combined
    )
    assert execution_context.config["_seeknal_run_id"] == state.run_id


def test_parallel_yaml_run_prepares_atlas_online_context_and_contract_state(
    tmp_path,
    monkeypatch,
):
    _write_yaml_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    executor = MagicMock()
    execution_result = MagicMock()
    execution_result.row_count = 1
    execution_result.metadata = {}
    executor.run.return_value = execution_result

    with patch(
        "seeknal.workflow.executors.get_executor",
        return_value=executor,
    ) as get_executor:
        result = CliRunner().invoke(app, ["run", "--full", "--parallel"])

    assert result.exit_code == 0, result.output
    state = load_state(tmp_path / "target" / "run_state.json")
    assert state is not None
    assert state.run_id

    feature_group = state.nodes["feature_group.customer_profile"]
    feature_service = state.nodes["feature_service.customer_analytics"]
    assert feature_group.fingerprint is not None
    assert feature_service.fingerprint is not None
    assert feature_service.metadata["contract_only"] is True

    execution_context = get_executor.call_args.args[1]
    fingerprints = execution_context.config["_seeknal_node_fingerprints"]
    assert fingerprints["feature_group.customer_profile"]["combined"] == (
        feature_group.fingerprint.combined
    )
    assert execution_context.config["_seeknal_run_id"] == state.run_id


def test_yaml_run_applies_changed_service_fingerprint_without_execution(
    tmp_path,
    monkeypatch,
):
    _write_yaml_project(tmp_path)
    monkeypatch.chdir(tmp_path)

    executor = MagicMock()
    execution_result = MagicMock()
    execution_result.is_success.return_value = True
    execution_result.row_count = 1
    execution_result.metadata = {}
    executor.run.return_value = execution_result

    with patch(
        "seeknal.workflow.executors.get_executor",
        return_value=executor,
    ):
        first = CliRunner().invoke(app, ["run", "--full"])
    assert first.exit_code == 0, first.output

    first_state = load_state(tmp_path / "target" / "run_state.json")
    assert first_state is not None
    first_fingerprint = first_state.nodes[
        "feature_service.customer_analytics"
    ].fingerprint
    assert first_fingerprint is not None

    service_path = (
        tmp_path / "seeknal" / "feature_services" / "customer_analytics.yml"
    )
    service_path.write_text(
        FEATURE_SERVICE_YAML.replace("owner: ml-platform", "owner: serving-team"),
        encoding="utf-8",
    )

    with patch(
        "seeknal.workflow.executors.get_executor",
    ) as get_executor:
        second = CliRunner().invoke(app, ["run"])

    assert second.exit_code == 0, second.output
    get_executor.assert_not_called()
    second_state = load_state(tmp_path / "target" / "run_state.json")
    assert second_state is not None
    second_fingerprint = second_state.nodes[
        "feature_service.customer_analytics"
    ].fingerprint
    assert second_fingerprint is not None
    assert second_fingerprint.combined != first_fingerprint.combined
    assert second_state.run_id == first_state.run_id
