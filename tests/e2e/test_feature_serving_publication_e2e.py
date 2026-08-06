"""E2E contract fixture: Feature Group creation through Atlas compilation."""

from __future__ import annotations

import importlib.util
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

from seeknal.integrations.atlas_feature_service import FeatureServiceCompiler
from seeknal.workflow.dag import DAGBuilder
from seeknal.workflow.manifest_builder import build_manifest_from_dag
from seeknal.workflow.state import (
    NodeState,
    RunState,
    compute_dag_fingerprints,
    save_state,
)

REPO_ROOT = Path(__file__).parents[2]
EXAMPLE_ROOT = REPO_ROOT / "examples" / "feature-serving-e2e"


def _build(project: Path):
    builder = DAGBuilder(project_path=project)
    builder.build()
    return builder, build_manifest_from_dag(builder, "feature-serving-e2e")


def _record_applied_state(project: Path, manifest):
    target = project / "target"
    target.mkdir(parents=True, exist_ok=True)
    manifest.save(str(target / "manifest.json"))
    upstream = {
        node_id: manifest.get_upstream_nodes(node_id) for node_id in manifest.nodes
    }
    fingerprints = compute_dag_fingerprints(
        {
            node_id: {
                "kind": node.node_type.value,
                "config": node.config,
                "file_path": node.file_path or "unknown.yml",
            }
            for node_id, node in manifest.nodes.items()
        },
        upstream,
    )
    feature_group_id = "feature_group.customer_profile"
    fingerprint = fingerprints[feature_group_id]
    state = RunState(run_id="example-run")
    state.nodes[feature_group_id] = NodeState(
        hash=fingerprint.content_hash,
        last_run="2026-08-05T00:00:00+00:00",
        status="success",
        row_count=3,
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
                            "publish_run_id": "example-run",
                            "row_count": 3,
                            "duration_seconds": 0.1,
                        },
                    }
                ],
            }
        },
    )
    feature_service_id = "feature_service.customer_analytics"
    service_fingerprint = fingerprints[feature_service_id]
    state.nodes[feature_service_id] = NodeState(
        hash=service_fingerprint.content_hash,
        last_run="2026-08-05T00:00:00+00:00",
        status="success",
        fingerprint=service_fingerprint,
        metadata={"contract_only": True},
    )
    save_state(state, target / "run_state.json")
    return fingerprint


def test_yaml_and_python_examples_create_equivalent_serving_resources(tmp_path):
    yaml_project = tmp_path / "yaml"
    python_project = tmp_path / "python"
    shutil.copytree(EXAMPLE_ROOT / "yaml", yaml_project)
    shutil.copytree(EXAMPLE_ROOT / "python", python_project)

    yaml_builder, yaml_manifest = _build(yaml_project)
    python_builder, python_manifest = _build(python_project)

    expected = {
        "source.customers",
        "feature_group.customer_profile",
        "feature_service.customer_analytics",
    }
    assert set(yaml_builder.nodes) == expected
    assert set(python_builder.nodes) == expected
    for manifest in (yaml_manifest, python_manifest):
        assert manifest.get_upstream_nodes("feature_group.customer_profile") == {
            "source.customers"
        }
        assert manifest.get_upstream_nodes("feature_service.customer_analytics") == {
            "feature_group.customer_profile"
        }
        feature_group = manifest.get_node("feature_group.customer_profile")
        assert feature_group.config["entity"]["join_keys"] == ["customer_id"]
        assert feature_group.config["materializations"][0]["type"] == "atlas_online"

    _record_applied_state(yaml_project, yaml_manifest)
    _record_applied_state(python_project, python_manifest)
    yaml_payload = (
        FeatureServiceCompiler(yaml_project)
        .compile("feature_service.customer_analytics")
        .payload
    )
    python_payload = (
        FeatureServiceCompiler(python_project)
        .compile("feature_service.customer_analytics")
        .payload
    )
    for payload in (yaml_payload, python_payload):
        view = payload["selections"][0]["view"]
        view.pop("revision")
        view.pop("sourceLocator")
        view.pop("schemaHash")
    assert python_payload == yaml_payload


def test_example_compiles_to_the_data_catalog_contract(tmp_path):
    project = tmp_path / "yaml"
    shutil.copytree(EXAMPLE_ROOT / "yaml", project)
    _, manifest = _build(project)
    _record_applied_state(project, manifest)

    payload = (
        FeatureServiceCompiler(project)
        .compile("feature_service.customer_analytics")
        .payload
    )

    assert payload["serviceId"] == "customer_analytics"
    assert payload["selections"][0]["features"] == [
        "age",
        "lifetime_value",
    ]

    model_path = (
        REPO_ROOT.parent
        / "la-data-catalog"
        / "services"
        / "atlas-data-platform"
        / "src"
        / "atlas"
        / "seeknal"
        / "feature_service_models.py"
    )
    if not model_path.is_file():
        pytest.skip("Data Catalog sibling checkout is unavailable")
    spec = importlib.util.spec_from_file_location(
        "_atlas_feature_service_contract", model_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    draft = module.FeatureServiceVersion(
        **payload,
        createdBy="e2e-test",
        createdAt=datetime.now(UTC),
    )
    published = draft.publish()

    assert published.lifecycle.value == "published"
    assert published.selections[0].view.online_table() == "customer_profile"
