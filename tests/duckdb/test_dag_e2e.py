"""
End-to-end test demonstrating Seeknal 2.0 DAG infrastructure.

This test demonstrates:
1. Parsing common.yml for sources, transforms, rules, aggregations
2. Building the manifest with nodes and edges
3. Detecting changes between manifests
4. Identifying nodes that need rebuilding
"""
import json
import tempfile
from pathlib import Path

import pytest
import yaml

from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.dag.parser import ProjectParser
from seeknal.dag.diff import ManifestDiff, DiffType
from seeknal.dag.functions import source, ref, use_transform, use_rule, use_aggregation
from seeknal.dag.registry import DependencyRegistry


class TestDAGE2E:
    """End-to-end tests for the DAG infrastructure."""

    @pytest.fixture
    def sample_common_yml(self) -> dict:
        """Sample common.yml configuration."""
        return {
            "sources": [
                {
                    "id": "traffic_day",
                    "description": "Daily traffic events",
                    "source": "hive",
                    "table": "analytics.traffic_day",
                    "params": {"partition_column": "event_date"},
                },
                {
                    "id": "user_profiles",
                    "description": "User profile data",
                    "source": "hive",
                    "table": "core.user_profiles",
                },
            ],
            "transformations": [
                {
                    "id": "clean_nulls",
                    "description": "Replace nulls with defaults",
                    "className": "com.seeknal.transforms.CleanNulls",
                    "params": {"strategy": "default"},
                },
            ],
            "rules": [
                {
                    "id": "active_users",
                    "description": "Filter active users",
                    "rule": {"condition": "status = 'active'"},
                },
                {
                    "id": "valid_transactions",
                    "description": "Valid transactions only",
                    "rule": {"condition": "status = 'completed'"},
                },
            ],
            "aggregations": [
                {
                    "id": "daily_sum",
                    "description": "Sum by day",
                    "group_by": ["entity_id", "event_date"],
                    "agg_functions": [
                        {"column": "amount", "function": "sum", "alias": "daily_total"}
                    ],
                },
            ],
        }

    @pytest.fixture
    def project_dir(self, sample_common_yml) -> Path:
        """Create a temporary project directory with common.yml."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = Path(tmpdir)
            common_path = project_path / "common.yml"
            with open(common_path, "w") as f:
                yaml.dump(sample_common_yml, f)
            yield project_path

    def test_parse_common_yml(self, project_dir):
        """Test parsing common.yml creates correct nodes."""
        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        manifest = parser.parse()

        # Check sources were parsed
        assert "source.traffic_day" in manifest.nodes
        assert "source.user_profiles" in manifest.nodes

        traffic_node = manifest.nodes["source.traffic_day"]
        assert traffic_node.node_type == NodeType.SOURCE
        assert traffic_node.description == "Daily traffic events"
        assert traffic_node.config["table"] == "analytics.traffic_day"

        # Check transformations
        assert "transform.clean_nulls" in manifest.nodes
        transform_node = manifest.nodes["transform.clean_nulls"]
        assert transform_node.node_type == NodeType.TRANSFORM
        assert transform_node.config["className"] == "com.seeknal.transforms.CleanNulls"

        # Check rules
        assert "rule.active_users" in manifest.nodes
        assert "rule.valid_transactions" in manifest.nodes

        # Check aggregations
        assert "aggregation.daily_sum" in manifest.nodes

    def test_manifest_serialization(self, project_dir):
        """Test manifest can be serialized to JSON and back."""
        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        manifest = parser.parse()

        # Serialize to JSON
        json_str = manifest.to_json()
        assert json_str is not None

        # Parse back
        loaded = Manifest.from_json(json_str)
        assert loaded.metadata.project == manifest.metadata.project
        assert len(loaded.nodes) == len(manifest.nodes)
        assert set(loaded.nodes.keys()) == set(manifest.nodes.keys())

    def test_manifest_save_and_load(self, project_dir):
        """Test saving and loading manifest from file."""
        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        manifest = parser.parse()

        # Save to file
        manifest_path = project_dir / "manifest.json"
        manifest.save(str(manifest_path))

        assert manifest_path.exists()

        # Load from file
        loaded = Manifest.load(str(manifest_path))
        assert loaded.metadata.project == "test_project"
        assert "source.traffic_day" in loaded.nodes

    def test_diff_detect_added_nodes(self, project_dir, sample_common_yml):
        """Test diff detects added nodes."""
        # Create initial manifest
        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        old_manifest = parser.parse()

        # Add a new source to common.yml
        sample_common_yml["sources"].append({
            "id": "new_source",
            "description": "A new data source",
            "source": "hive",
            "table": "new.table",
        })

        # Write updated config
        common_path = project_dir / "common.yml"
        with open(common_path, "w") as f:
            yaml.dump(sample_common_yml, f)

        # Parse again
        new_parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        new_manifest = new_parser.parse()

        # Compare
        diff = ManifestDiff.compare(old_manifest, new_manifest)

        assert diff.has_changes()
        assert "source.new_source" in diff.added_nodes
        assert len(diff.removed_nodes) == 0
        assert len(diff.modified_nodes) == 0

    def test_diff_detect_removed_nodes(self, project_dir, sample_common_yml):
        """Test diff detects removed nodes."""
        # Create initial manifest
        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        old_manifest = parser.parse()

        # Remove a source
        sample_common_yml["sources"] = [
            s for s in sample_common_yml["sources"]
            if s["id"] != "user_profiles"
        ]

        # Write updated config
        common_path = project_dir / "common.yml"
        with open(common_path, "w") as f:
            yaml.dump(sample_common_yml, f)

        # Parse again
        new_parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        new_manifest = new_parser.parse()

        # Compare
        diff = ManifestDiff.compare(old_manifest, new_manifest)

        assert diff.has_changes()
        assert "source.user_profiles" in diff.removed_nodes
        assert len(diff.added_nodes) == 0

    def test_diff_detect_modified_nodes(self, project_dir, sample_common_yml):
        """Test diff detects modified nodes."""
        # Create initial manifest
        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        old_manifest = parser.parse()

        # Modify a source
        for source_config in sample_common_yml["sources"]:
            if source_config["id"] == "traffic_day":
                source_config["description"] = "Updated description"
                source_config["table"] = "analytics.traffic_day_v2"

        # Write updated config
        common_path = project_dir / "common.yml"
        with open(common_path, "w") as f:
            yaml.dump(sample_common_yml, f)

        # Parse again
        new_parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        new_manifest = new_parser.parse()

        # Compare
        diff = ManifestDiff.compare(old_manifest, new_manifest)

        assert diff.has_changes()
        assert "source.traffic_day" in diff.modified_nodes

        change = diff.modified_nodes["source.traffic_day"]
        assert change.change_type == DiffType.MODIFIED
        assert "description" in change.changed_fields
        assert "config" in change.changed_fields

    def test_diff_no_changes(self, project_dir):
        """Test diff reports no changes when manifests are identical."""
        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        manifest1 = parser.parse()
        manifest2 = parser.parse()

        diff = ManifestDiff.compare(manifest1, manifest2)

        assert not diff.has_changes()
        assert diff.summary() == "No changes detected"

    def test_diff_summary(self, project_dir, sample_common_yml):
        """Test diff summary generation."""
        # Create initial manifest
        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        old_manifest = parser.parse()

        # Add a node and modify another
        sample_common_yml["sources"].append({
            "id": "new_source",
            "description": "New source",
            "source": "hive",
            "table": "new.table",
        })
        sample_common_yml["sources"][0]["description"] = "Modified"

        common_path = project_dir / "common.yml"
        with open(common_path, "w") as f:
            yaml.dump(sample_common_yml, f)

        new_parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        new_manifest = new_parser.parse()

        diff = ManifestDiff.compare(old_manifest, new_manifest)
        summary = diff.summary()

        assert "added" in summary
        assert "modified" in summary

    def test_nodes_to_rebuild_includes_downstream(self):
        """Test that nodes to rebuild includes downstream dependents."""
        # Create a manifest with edges
        manifest = Manifest(project="test")

        # Add nodes
        manifest.add_node(Node(
            id="source.data",
            name="data",
            node_type=NodeType.SOURCE,
        ))
        manifest.add_node(Node(
            id="feature_group.features",
            name="features",
            node_type=NodeType.FEATURE_GROUP,
        ))
        manifest.add_node(Node(
            id="model.prediction",
            name="prediction",
            node_type=NodeType.MODEL,
        ))

        # Add edges: source -> features -> model
        manifest.add_edge("source.data", "feature_group.features")
        manifest.add_edge("feature_group.features", "model.prediction")

        # Create diff with source modified (different config triggers NON_BREAKING)
        from seeknal.dag.diff import NodeChange, DiffType
        old_source = Node(
            id="source.data", name="data", node_type=NodeType.SOURCE,
            config={"sql": "SELECT 1"},
        )
        new_source = Node(
            id="source.data", name="data", node_type=NodeType.SOURCE,
            config={"sql": "SELECT 2"},
        )
        diff = ManifestDiff()
        diff.modified_nodes["source.data"] = NodeChange(
            node_id="source.data",
            change_type=DiffType.MODIFIED,
            old_value=old_source,
            new_value=new_source,
            changed_fields=["config"],
        )

        # Get nodes to rebuild
        to_rebuild = diff.get_nodes_to_rebuild(manifest)

        # Should include the modified node and all downstream
        assert "source.data" in to_rebuild
        assert "feature_group.features" in to_rebuild
        assert "model.prediction" in to_rebuild

    def test_dependency_functions_register_correctly(self):
        """Test that dependency functions register in the registry."""
        registry = DependencyRegistry.get_instance()
        registry.reset()

        # Set current node context
        registry.set_current_node("feature_group.my_features")

        # Call dependency functions
        source("traffic_day")
        source("hive", "db.table")
        ref("other_features")
        use_transform("clean_nulls", None)
        use_rule("active_users")
        use_aggregation("daily_sum", None)

        # Check dependencies were registered
        deps = registry.get_dependencies_for_node("feature_group.my_features")
        assert len(deps) == 6

        dep_ids = {d.dependency_id for d in deps}
        assert "source.traffic_day" in dep_ids
        assert "source.db.table" in dep_ids
        assert "other_features" in dep_ids
        assert "transform.clean_nulls" in dep_ids
        assert "rule.active_users" in dep_ids
        assert "aggregation.daily_sum" in dep_ids

        # Cleanup
        registry.reset()

    def test_manifest_cycle_detection(self):
        """Test that manifest detects cycles in the DAG."""
        manifest = Manifest(project="test")

        # Add nodes
        manifest.add_node(Node(id="a", name="a", node_type=NodeType.TRANSFORM))
        manifest.add_node(Node(id="b", name="b", node_type=NodeType.TRANSFORM))
        manifest.add_node(Node(id="c", name="c", node_type=NodeType.TRANSFORM))

        # Add edges that create a cycle: a -> b -> c -> a
        manifest.add_edge("a", "b")
        manifest.add_edge("b", "c")
        manifest.add_edge("c", "a")

        has_cycle, cycle_path = manifest.detect_cycles()

        assert has_cycle
        assert len(cycle_path) > 0
        # The cycle should contain a, b, c
        assert set(cycle_path[:-1]) == {"a", "b", "c"}

    def test_manifest_no_cycle(self):
        """Test that valid DAG has no cycles."""
        manifest = Manifest(project="test")

        # Add nodes
        manifest.add_node(Node(id="a", name="a", node_type=NodeType.SOURCE))
        manifest.add_node(Node(id="b", name="b", node_type=NodeType.TRANSFORM))
        manifest.add_node(Node(id="c", name="c", node_type=NodeType.FEATURE_GROUP))

        # Add edges: a -> b -> c (no cycle)
        manifest.add_edge("a", "b")
        manifest.add_edge("b", "c")

        has_cycle, cycle_path = manifest.detect_cycles()

        assert not has_cycle
        assert cycle_path == []

    def test_parser_validation_errors(self, project_dir):
        """Test parser reports validation errors."""
        # Write invalid common.yml (missing required id)
        invalid_config = {
            "sources": [
                {
                    "description": "Missing id field",
                    "source": "hive",
                    "table": "some.table",
                },
            ],
        }

        common_path = project_dir / "common.yml"
        with open(common_path, "w") as f:
            yaml.dump(invalid_config, f)

        parser = ProjectParser(
            project_name="test_project",
            project_path=str(project_dir),
        )
        parser.parse()

        errors = parser.get_errors()
        assert len(errors) > 0
        assert "missing required 'id' field" in errors[0]

    def test_manifest_json_validation(self):
        """Test manifest validates JSON structure on load."""
        # Missing metadata
        with pytest.raises(ValueError, match="missing required 'metadata' field"):
            Manifest.from_json('{"nodes": {}}')

        # Metadata missing project
        with pytest.raises(ValueError, match="missing required 'project' field"):
            Manifest.from_json('{"metadata": {}}')

        # Invalid nodes type
        with pytest.raises(ValueError, match="'nodes' must be an object"):
            Manifest.from_json('{"metadata": {"project": "test"}, "nodes": []}')

        # Invalid edges type
        with pytest.raises(ValueError, match="'edges' must be an array"):
            Manifest.from_json('{"metadata": {"project": "test"}, "edges": {}}')

    def test_manifest_path_traversal_prevention(self):
        """Test manifest prevents path traversal attacks."""
        manifest = Manifest(project="test")

        with pytest.raises(ValueError, match="path traversal not allowed"):
            manifest.save("../../../etc/passwd")

        with pytest.raises(ValueError, match="path traversal not allowed"):
            Manifest.load("../../../etc/passwd")

    def test_parser_path_traversal_prevention(self):
        """Test parser prevents path traversal in project path."""
        with pytest.raises(ValueError, match="path traversal not allowed"):
            ProjectParser(
                project_name="test",
                project_path="../../../etc",
            )
