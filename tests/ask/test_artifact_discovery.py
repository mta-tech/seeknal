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


@pytest.fixture
def project_with_pipelines(tmp_path):
    """Project with both target/ and seeknal/ pipeline definitions."""
    # target artifacts
    target = tmp_path / "target"
    (target / "intermediate").mkdir(parents=True)

    # seeknal pipeline definitions
    seeknal = tmp_path / "seeknal"
    (seeknal / "sources").mkdir(parents=True)
    (seeknal / "transforms").mkdir(parents=True)
    (seeknal / "feature_groups").mkdir(parents=True)
    (seeknal / "common").mkdir(parents=True)

    # Source YAML
    (seeknal / "sources" / "raw_orders.yml").write_text(
        "kind: source\nname: raw_orders\ndescription: Raw order data\n"
        "source: csv\ntable: data/orders.csv\n"
    )

    # Transform YAML with SQL
    (seeknal / "transforms" / "clean_orders.yml").write_text(
        "kind: transform\nname: clean_orders\n"
        "description: Clean and enrich orders\n"
        "sql: |\n  SELECT *, amount * quantity AS revenue\n  FROM raw_orders\n"
        "  WHERE status != 'cancelled'\n"
    )

    # Feature group YAML
    (seeknal / "feature_groups" / "customer_features.yml").write_text(
        "kind: feature_group\nname: customer_features\n"
        "description: Customer feature group\n"
        "entity:\n  name: customer\n  join_keys: [customer_id]\n"
    )

    # Python pipeline
    (seeknal / "transforms" / "enriched_orders.py").write_text(
        "@transform(name='enriched_orders')\n"
        "def enriched_orders(ctx):\n"
        "    return ctx.sql('SELECT *, revenue / quantity AS unit_price FROM clean_orders')\n"
    )

    # common/ should be excluded
    (seeknal / "common" / "shared_config.yml").write_text(
        "kind: source\nname: should_be_excluded\n"
    )

    # Private file should be excluded
    (seeknal / "transforms" / "_helpers.py").write_text("# private helper\n")

    # .env and profiles.yml (should be blocked)
    (tmp_path / ".env").write_text("SECRET_KEY=abc123\n")
    (tmp_path / "profiles.yml").write_text(
        "connections:\n  pg:\n    type: postgresql\n    password: secret\n"
    )

    return tmp_path


class TestSourcePipelineDiscovery:

    def test_discover_yaml_pipelines(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        pipelines = discovery._discover_source_pipelines()
        names = [p["name"] for p in pipelines]
        assert "raw_orders" in names
        assert "clean_orders" in names
        assert "customer_features" in names

    def test_discover_python_pipelines(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        pipelines = discovery._discover_source_pipelines()
        names = [p["name"] for p in pipelines]
        assert "enriched_orders" in names

    def test_excludes_common_dir(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        pipelines = discovery._discover_source_pipelines()
        names = [p["name"] for p in pipelines]
        assert "should_be_excluded" not in names

    def test_excludes_private_files(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        pipelines = discovery._discover_source_pipelines()
        names = [p["name"] for p in pipelines]
        assert "_helpers" not in names

    def test_pipeline_has_correct_kind(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        pipelines = discovery._discover_source_pipelines()
        by_name = {p["name"]: p for p in pipelines}
        assert by_name["raw_orders"]["kind"] == "source"
        assert by_name["clean_orders"]["kind"] == "transform"
        assert by_name["enriched_orders"]["kind"] == "python_pipeline"

    def test_context_includes_pipeline_summary(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        context = discovery.get_context_for_prompt()
        assert "Pipeline Definitions" in context
        assert "raw_orders" in context
        assert "clean_orders" in context

    def test_cache_is_used(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        p1 = discovery._discover_source_pipelines()
        p2 = discovery._discover_source_pipelines()
        assert p1 is p2  # Same object = cache hit

    def test_no_seeknal_dir(self, tmp_path):
        discovery = ArtifactDiscovery(tmp_path)
        assert discovery._discover_source_pipelines() == []


class TestGetPipelineContent:

    def test_read_yaml_pipeline(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        content = discovery.get_pipeline_content("seeknal/sources/raw_orders.yml")
        assert content is not None
        assert "kind: source" in content

    def test_read_python_pipeline(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        content = discovery.get_pipeline_content(
            "seeknal/transforms/enriched_orders.py"
        )
        assert content is not None
        assert "@transform" in content

    def test_blocks_path_traversal(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        assert discovery.get_pipeline_content("../../../etc/passwd") is None
        assert discovery.get_pipeline_content("seeknal/../../etc/passwd") is None

    def test_blocks_env_file(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        assert discovery.get_pipeline_content(".env") is None

    def test_blocks_profiles_yml(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        assert discovery.get_pipeline_content("profiles.yml") is None

    def test_nonexistent_file(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        assert discovery.get_pipeline_content("seeknal/nothing.yml") is None


class TestSearchPipelineContent:

    def test_search_finds_sql_content(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        results = discovery.search_pipeline_content("revenue")
        assert len(results) >= 1
        names = [r["name"] for r in results]
        assert "clean_orders" in names or "enriched_orders" in names

    def test_search_case_insensitive(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        results = discovery.search_pipeline_content("REVENUE")
        assert len(results) >= 1

    def test_search_no_match(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        results = discovery.search_pipeline_content("nonexistent_xyz_123")
        assert len(results) == 0

    def test_search_returns_line_number(self, project_with_pipelines):
        discovery = ArtifactDiscovery(project_with_pipelines)
        results = discovery.search_pipeline_content("revenue")
        assert all("matched_line" in r for r in results)
        assert all(r["matched_line"] >= 1 for r in results)
