"""Tests for seeknal.ask.modules.artifact_discovery.service."""

import json
import pytest
from pathlib import Path

from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery


@pytest.fixture
def project_dir(tmp_path):
    """Create a minimal seeknal project structure with test artifacts."""
    target = tmp_path / "target"
    target.mkdir()

    # Create entity catalog
    fs = target / "feature_store" / "customer"
    fs.mkdir(parents=True)
    catalog = {
        "entity_name": "customer",
        "join_keys": ["customer_id"],
        "feature_groups": {
            "demographics": {
                "features": {
                    "age": "INTEGER",
                    "city": "VARCHAR",
                }
            },
            "purchase_stats": {
                "features": {
                    "total_orders": "BIGINT",
                    "avg_order_value": "DOUBLE",
                }
            },
        },
    }
    (fs / "_entity_catalog.json").write_text(json.dumps(catalog))
    (fs / "features.parquet").touch()

    # Create intermediate
    intermediate = target / "intermediate"
    intermediate.mkdir()
    (intermediate / "transform_clean_orders.parquet").touch()
    (intermediate / "source_raw_orders.parquet").touch()

    # Create manifest
    manifest = {
        "nodes": [
            {"name": "clean_orders", "type": "transform"},
            {"name": "customer_demographics", "type": "feature_group"},
        ],
        "edges": [],
    }
    (target / "manifest.json").write_text(json.dumps(manifest))

    return tmp_path


class TestArtifactDiscovery:

    def test_discover_entities(self, project_dir):
        discovery = ArtifactDiscovery(project_dir)
        entities = discovery._discover_entities()
        assert len(entities) == 1
        assert entities[0]["entity_name"] == "customer"

    def test_get_entities_summary(self, project_dir):
        discovery = ArtifactDiscovery(project_dir)
        summary = discovery.get_entities_summary()
        assert len(summary) == 1
        assert summary[0]["name"] == "customer"
        assert summary[0]["join_keys"] == ["customer_id"]
        assert summary[0]["feature_group_count"] == 2

    def test_get_entity_catalog(self, project_dir):
        discovery = ArtifactDiscovery(project_dir)
        catalog = discovery.get_entity_catalog("customer")
        assert catalog is not None
        assert "demographics" in catalog["feature_groups"]

    def test_get_entity_catalog_not_found(self, project_dir):
        discovery = ArtifactDiscovery(project_dir)
        assert discovery.get_entity_catalog("nonexistent") is None

    def test_discover_intermediates(self, project_dir):
        discovery = ArtifactDiscovery(project_dir)
        intermediates = discovery._discover_intermediates()
        assert "source_raw_orders" in intermediates
        assert "transform_clean_orders" in intermediates

    def test_discover_dag(self, project_dir):
        discovery = ArtifactDiscovery(project_dir)
        dag = discovery._discover_dag()
        assert dag is not None
        assert len(dag["nodes"]) == 2

    def test_get_context_for_prompt(self, project_dir):
        discovery = ArtifactDiscovery(project_dir)
        context = discovery.get_context_for_prompt()
        assert "customer" in context
        assert "demographics" in context
        assert "clean_orders" in context

    def test_no_artifacts(self, tmp_path):
        discovery = ArtifactDiscovery(tmp_path)
        context = discovery.get_context_for_prompt()
        assert "No seeknal artifacts found" in context

    def test_malformed_catalog_skipped(self, tmp_path):
        fs = tmp_path / "target" / "feature_store" / "broken"
        fs.mkdir(parents=True)
        (fs / "_entity_catalog.json").write_text("not json")

        discovery = ArtifactDiscovery(tmp_path)
        entities = discovery._discover_entities()
        assert len(entities) == 0
