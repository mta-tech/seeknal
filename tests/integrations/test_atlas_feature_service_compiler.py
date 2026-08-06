"""Applied-state compilation tests for Atlas Feature Service publication."""

from pathlib import Path

import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.integrations.atlas_feature_service import (
    FeatureServiceCompilationError,
    FeatureServiceCompiler,
)
from seeknal.workflow.state import (
    NodeFingerprint,
    NodeState,
    RunState,
    compute_dag_fingerprints,
    load_state,
    save_state,
)


def _fingerprints(manifest: Manifest) -> dict[str, NodeFingerprint]:
    return compute_dag_fingerprints(
        {
            node_id: {
                "kind": node.node_type.value,
                "config": node.config,
                "file_path": node.file_path or "unknown.yml",
                "columns": node.columns,
            }
            for node_id, node in manifest.nodes.items()
        },
        {
            node_id: manifest.get_upstream_nodes(node_id)
            for node_id in manifest.nodes
        },
    )


def _mark_service_applied(manifest: Manifest, state: RunState) -> None:
    service_id = "feature_service.customer_analytics"
    fingerprint = _fingerprints(manifest)[service_id]
    state.nodes[service_id] = NodeState(
        hash=fingerprint.content_hash,
        last_run="2026-08-05T00:00:00+00:00",
        status="success",
        fingerprint=fingerprint,
        metadata={"contract_only": True},
    )


def _applied_project(project: Path) -> tuple[NodeFingerprint, Path]:
    target = project / "target"
    target.mkdir(parents=True)
    feature_group = Node(
        id="feature_group.customer_profile",
        name="customer_profile",
        node_type=NodeType.FEATURE_GROUP,
        config={
            "kind": "feature_group",
            "name": "customer_profile",
            "entity": {"name": "customer", "join_keys": ["customer_id"]},
            "features": {
                "customer_id": {"dtype": "string"},
                "age": {"dtype": "integer", "description": "Age in years"},
                "observed_at": {"dtype": "timestamp"},
            },
            "materializations": [
                {
                    "type": "atlas_online",
                    "connection": "atlas_feature_store",
                    "table": "customer_profile",
                    "event_time_column": "observed_at",
                    "ttl_seconds": 3600,
                }
            ],
        },
    )
    service = Node(
        id="feature_service.customer_analytics",
        name="customer_analytics",
        node_type=NodeType.FEATURE_SERVICE,
        config={
            "kind": "feature_service",
            "name": "customer_analytics",
            "version": "1",
            "variant": "default",
            "owner": "ml-platform",
            "description": "Customer analytics features",
            "consumer": "churn-model",
            "tags": ["production"],
            "views": [
                {
                    "ref": "feature_group.customer_profile",
                    "features": ["age"],
                }
            ],
        },
    )
    manifest = Manifest(project="test")
    manifest.add_node(feature_group)
    manifest.add_node(service)
    manifest.add_edge(feature_group.id, service.id)
    manifest.save(str(target / "manifest.json"))

    fingerprint = _fingerprints(manifest)[feature_group.id]
    state = RunState(run_id="run-1")
    state.nodes[feature_group.id] = NodeState(
        hash=fingerprint.content_hash,
        last_run="2026-08-05T00:00:00+00:00",
        status="success",
        row_count=1,
        fingerprint=fingerprint,
        metadata={
            "materialization": {
                "success": True,
                "results": [
                    {
                        "type": "atlas_online",
                        "success": True,
                        "write_result": {
                            "success": True,
                            "table": "customer_profile",
                            "revision": fingerprint.combined,
                            "definition_sha": fingerprint.content_hash,
                            "schema_sha": fingerprint.schema_hash,
                            "publish_run_id": "run-1",
                            "row_count": 1,
                            "duration_seconds": 0.1,
                        },
                    }
                ],
            }
        },
    )
    _mark_service_applied(manifest, state)
    save_state(state, target / "run_state.json")
    return fingerprint, target


def test_compiles_canonical_payload_from_applied_materialization(tmp_path):
    fingerprint, target = _applied_project(tmp_path)

    compiled = FeatureServiceCompiler(tmp_path).compile(
        "feature_service.customer_analytics"
    )

    assert compiled.manifest_path == target / "manifest.json"
    assert compiled.payload["serviceId"] == "customer_analytics"
    assert compiled.payload["entityKeys"][0]["physicalName"] == "customer_id"
    selection = compiled.payload["selections"][0]
    assert selection["features"] == ["age"]
    assert selection["view"]["revision"] == fingerprint.combined
    assert selection["view"]["schemaRevision"] == fingerprint.schema_hash
    assert selection["view"]["sourceLocator"] == (
        f"seeknal:feature-group:customer_profile:{fingerprint.combined}"
    )
    assert selection["view"]["executionMode"] == "batch"
    assert len(selection["view"]["schemaHash"]) == 64
    assert compiled.to_json() == compiled.to_json()


def test_rejects_stale_materialization_evidence(tmp_path):
    _, target = _applied_project(tmp_path)
    state_path = target / "run_state.json"
    state = load_state(state_path)
    assert state is not None
    evidence = state.nodes["feature_group.customer_profile"].metadata[
        "materialization"
    ]["results"][0]["write_result"]
    evidence["revision"] = "0" * 64
    save_state(state, state_path)

    with pytest.raises(
        FeatureServiceCompilationError,
        match="materialized revision does not match applied state",
    ):
        FeatureServiceCompiler(tmp_path).compile("feature_service.customer_analytics")


def test_rejects_materialization_from_another_run(tmp_path):
    _, target = _applied_project(tmp_path)
    state_path = target / "run_state.json"
    state = load_state(state_path)
    assert state is not None
    evidence = state.nodes["feature_group.customer_profile"].metadata[
        "materialization"
    ]["results"][0]["write_result"]
    evidence["publish_run_id"] = "different-run"
    save_state(state, state_path)

    with pytest.raises(
        FeatureServiceCompilationError,
        match="materialized run does not match applied state",
    ):
        FeatureServiceCompiler(tmp_path).compile("feature_service.customer_analytics")


@pytest.mark.parametrize(
    "run_id",
    ["", "   ", "\t", "\n", " run-1 ", "run\t1", "run\n1"],
)
def test_rejects_noncanonical_materialization_run_identity(tmp_path, run_id):
    _, target = _applied_project(tmp_path)
    state_path = target / "run_state.json"
    state = load_state(state_path)
    assert state is not None
    state.run_id = run_id
    evidence = state.nodes["feature_group.customer_profile"].metadata[
        "materialization"
    ]["results"][0]["write_result"]
    evidence["publish_run_id"] = run_id
    save_state(state, state_path)

    with pytest.raises(
        FeatureServiceCompilationError,
        match="run identity is invalid",
    ):
        FeatureServiceCompiler(tmp_path).compile("feature_service.customer_analytics")


def test_rejects_manifest_schema_mutated_after_apply(tmp_path):
    _, target = _applied_project(tmp_path)
    manifest = Manifest.load(str(target / "manifest.json"))
    feature_group = manifest.get_node("feature_group.customer_profile")
    assert feature_group is not None
    feature_group.config["features"]["age"]["dtype"] = "string"
    manifest.save(str(target / "manifest.json"))

    with pytest.raises(
        FeatureServiceCompilationError,
        match="manifest fingerprint does not match applied state",
    ):
        FeatureServiceCompiler(tmp_path).compile("feature_service.customer_analytics")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("owner", "different-owner"),
        ("version", "2"),
        ("tags", ["staging"]),
    ],
)
def test_rejects_service_metadata_mutated_after_apply(tmp_path, field, value):
    _, target = _applied_project(tmp_path)
    manifest = Manifest.load(str(target / "manifest.json"))
    service = manifest.get_node("feature_service.customer_analytics")
    assert service is not None
    service.config[field] = value
    manifest.save(str(target / "manifest.json"))

    with pytest.raises(
        FeatureServiceCompilationError,
        match="feature_service.customer_analytics manifest fingerprint "
        "does not match applied state",
    ):
        FeatureServiceCompiler(tmp_path).compile("feature_service.customer_analytics")


def test_rejects_service_feature_selection_mutated_after_apply(tmp_path):
    _, target = _applied_project(tmp_path)
    manifest = Manifest.load(str(target / "manifest.json"))
    service = manifest.get_node("feature_service.customer_analytics")
    assert service is not None
    service.config["views"][0]["features"] = ["observed_at"]
    manifest.save(str(target / "manifest.json"))

    with pytest.raises(
        FeatureServiceCompilationError,
        match="feature_service.customer_analytics manifest fingerprint "
        "does not match applied state",
    ):
        FeatureServiceCompiler(tmp_path).compile("feature_service.customer_analytics")


def test_environment_uses_environment_applied_state(tmp_path):
    _, default_target = _applied_project(tmp_path)
    environment_target = tmp_path / "target" / "environments" / "staging"
    environment_target.mkdir(parents=True)
    (environment_target / "manifest.json").write_bytes(
        (default_target / "manifest.json").read_bytes()
    )
    (environment_target / "run_state.json").write_bytes(
        (default_target / "run_state.json").read_bytes()
    )

    compiled = FeatureServiceCompiler(tmp_path, environment="staging").compile(
        "feature_service.customer_analytics"
    )

    assert compiled.environment == "staging"
    assert compiled.state_path == environment_target / "run_state.json"


def test_compiles_multiple_groups_with_identical_typed_keys(tmp_path):
    _, target = _applied_project(tmp_path)
    manifest = Manifest.load(str(target / "manifest.json"))
    second = Node(
        id="feature_group.customer_activity",
        name="customer_activity",
        node_type=NodeType.FEATURE_GROUP,
        config={
            "kind": "feature_group",
            "name": "customer_activity",
            "entity": {"name": "customer", "join_keys": ["customer_id"]},
            "features": {
                "customer_id": {"dtype": "string"},
                "spend_30d": {"dtype": "float"},
            },
            "materializations": [
                {
                    "type": "atlas_online",
                    "connection": "atlas_feature_store",
                    "table": "customer_activity",
                }
            ],
        },
    )
    manifest.add_node(second)
    service = manifest.get_node("feature_service.customer_analytics")
    assert service is not None
    service.config["views"].append(
        {
            "ref": second.id,
            "features": ["spend_30d"],
        }
    )
    manifest.add_edge(second.id, service.id)
    manifest.save(str(target / "manifest.json"))

    state = load_state(target / "run_state.json")
    assert state is not None
    fingerprint = _fingerprints(manifest)[second.id]
    state.nodes[second.id] = NodeState(
        hash=fingerprint.content_hash,
        last_run="2026-08-05T00:00:00+00:00",
        status="success",
        fingerprint=fingerprint,
        metadata={
            "materialization": {
                "success": True,
                "results": [
                    {
                        "type": "atlas_online",
                        "success": True,
                        "write_result": {
                            "table": "customer_activity",
                            "revision": fingerprint.combined,
                            "definition_sha": fingerprint.content_hash,
                            "schema_sha": fingerprint.schema_hash,
                            "publish_run_id": state.run_id,
                        },
                    }
                ],
            }
        },
    )
    _mark_service_applied(manifest, state)
    save_state(state, target / "run_state.json")

    payload = (
        FeatureServiceCompiler(tmp_path)
        .compile("feature_service.customer_analytics")
        .payload
    )

    assert [item["view"]["viewId"] for item in payload["selections"]] == [
        "customer_profile",
        "customer_activity",
    ]


def test_rejects_incompatible_entity_key_types_between_groups(tmp_path):
    _, target = _applied_project(tmp_path)
    manifest = Manifest.load(str(target / "manifest.json"))
    second = Node(
        id="feature_group.customer_activity",
        name="customer_activity",
        node_type=NodeType.FEATURE_GROUP,
        config={
            "entity": {"name": "customer", "join_keys": ["customer_id"]},
            "features": {
                "customer_id": {"dtype": "integer"},
                "spend_30d": {"dtype": "float"},
            },
            "materializations": [
                {
                    "type": "atlas_online",
                    "connection": "atlas_feature_store",
                    "table": "customer_activity",
                }
            ],
        },
    )
    manifest.add_node(second)
    service = manifest.get_node("feature_service.customer_analytics")
    assert service is not None
    service.config["views"].append({"ref": second.id, "features": ["spend_30d"]})
    manifest.add_edge(second.id, service.id)
    manifest.save(str(target / "manifest.json"))
    fingerprint = _fingerprints(manifest)[second.id]
    state = load_state(target / "run_state.json")
    assert state is not None
    state.nodes[second.id] = NodeState(
        hash=fingerprint.content_hash,
        last_run="2026-08-05T00:00:00+00:00",
        status="success",
        fingerprint=fingerprint,
        metadata={
            "materialization": {
                "success": True,
                "results": [
                    {
                        "type": "atlas_online",
                        "success": True,
                        "write_result": {
                            "table": "customer_activity",
                            "revision": fingerprint.combined,
                            "definition_sha": fingerprint.content_hash,
                            "schema_sha": fingerprint.schema_hash,
                            "publish_run_id": state.run_id,
                        },
                    }
                ],
            }
        },
    )
    _mark_service_applied(manifest, state)
    save_state(state, target / "run_state.json")

    with pytest.raises(
        FeatureServiceCompilationError,
        match="identical typed entity keys",
    ):
        FeatureServiceCompiler(tmp_path).compile("feature_service.customer_analytics")
