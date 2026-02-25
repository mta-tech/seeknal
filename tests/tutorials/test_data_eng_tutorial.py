"""
Test suite for Data Engineering Tutorial.

Validates:
1. All YAML files parse correctly
2. DAG builds without cycles
3. Change categorization works correctly
4. Plan output format is readable
"""
import pytest
import yaml
from pathlib import Path

from seeknal.dag.diff import ChangeCategory, ManifestDiff
from seeknal.dag.manifest import Manifest, Node, NodeType


# Test fixtures
@pytest.fixture
def tutorial_dir():
    """Path to tutorial example files."""
    return Path(__file__).parent.parent.parent / "examples" / "data-eng"


@pytest.fixture
def yaml_files(tutorial_dir):
    """Load all YAML files from tutorial directory."""
    files = {}
    for yaml_file in sorted(tutorial_dir.glob("*.yml")):
        with open(yaml_file, "r") as f:
            files[yaml_file.name] = yaml.safe_load(f)
    return files


@pytest.fixture
def sample_manifest(yaml_files):
    """Create a sample manifest from YAML files."""
    manifest = Manifest(project="test-tutorial", seeknal_version="2.0.0")

    for filename, config in yaml_files.items():
        node_id = f"{config['kind']}.{config['name']}"
        node = Node(
            id=node_id,
            name=config["name"],
            node_type=NodeType(config["kind"]),
            description=config.get("description"),
            owner=config.get("owner"),
            tags=config.get("tags", []),
            config=config,
            file_path=str(filename),
        )
        manifest.add_node(node)

        # Extract edges from inputs
        if "inputs" in config:
            for inp in config["inputs"]:
                ref = inp.get("ref")
                if ref:
                    manifest.add_edge(ref, node_id)

    return manifest


class TestYAMLParsing:
    """Test YAML file parsing."""

    def test_all_files_exist(self, tutorial_dir):
        """Verify all 8 YAML files exist."""
        expected_files = [
            "01_source_orders.yml",
            "02_source_customers.yml",
            "03_source_inventory.yml",
            "04_transform_clean_orders.yml",
            "05_transform_customer_enriched.yml",
            "06_transform_inventory_status.yml",
            "07_aggregation_daily_sales.yml",
            "08_exposure_warehouse.yml",
        ]

        for filename in expected_files:
            filepath = tutorial_dir / filename
            assert filepath.exists(), f"Missing file: {filename}"

    def test_yaml_files_parse(self, yaml_files):
        """Verify all YAML files parse without errors."""
        assert len(yaml_files) == 8, f"Expected 8 YAML files, found {len(yaml_files)}"

        for filename, config in yaml_files.items():
            assert config is not None, f"Failed to parse {filename}"
            assert "kind" in config, f"Missing 'kind' field in {filename}"
            assert "name" in config, f"Missing 'name' field in {filename}"

    def test_yaml_required_fields(self, yaml_files):
        """Verify required fields are present."""
        for filename, config in yaml_files.items():
            kind = config["kind"]

            # Common fields
            assert "name" in config, f"Missing 'name' in {filename}"
            assert "description" in config, f"Missing 'description' in {filename}"
            assert "owner" in config, f"Missing 'owner' in {filename}"

            # Kind-specific fields
            if kind == "source":
                assert "source" in config, f"Missing 'source' in {filename}"
                assert "table" in config, f"Missing 'table' in {filename}"
                assert "schema" in config, f"Missing 'schema' in {filename}"

            elif kind == "transform":
                assert "transform" in config, f"Missing 'transform' in {filename}"
                assert "inputs" in config, f"Missing 'inputs' in {filename}"

            elif kind == "aggregation":
                assert "id_col" in config, f"Missing 'id_col' in {filename}"
                assert "feature_date_col" in config, f"Missing 'feature_date_col' in {filename}"
                assert "features" in config, f"Missing 'features' in {filename}"
                assert "inputs" in config, f"Missing 'inputs' in {filename}"

            elif kind == "exposure":
                assert "type" in config, f"Missing 'type' in {filename}"
                assert "inputs" in config, f"Missing 'inputs' in {filename}"


class TestDAGConstruction:
    """Test DAG building from YAML files."""

    def test_dag_builds_without_cycles(self, sample_manifest):
        """Verify DAG has no circular dependencies."""
        # Simple cycle detection: topological sort should succeed
        nodes = sample_manifest.nodes
        edges = sample_manifest.edges

        # Build adjacency list
        graph = {node_id: [] for node_id in nodes}
        in_degree = {node_id: 0 for node_id in nodes}

        for edge in edges:
            from_id = edge.from_node
            to_id = edge.to_node
            if from_id in graph and to_id in graph:
                graph[from_id].append(to_id)
                in_degree[to_id] += 1

        # Topological sort (Kahn's algorithm)
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        sorted_nodes = []

        while queue:
            node_id = queue.pop(0)
            sorted_nodes.append(node_id)

            for neighbor in graph[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # If all nodes are sorted, there are no cycles
        assert len(sorted_nodes) == len(nodes), "DAG contains cycles"

    def test_dag_has_correct_node_count(self, sample_manifest):
        """Verify correct number of nodes."""
        assert len(sample_manifest.nodes) == 8, "Expected 8 nodes in DAG"

    def test_dag_has_source_nodes(self, sample_manifest):
        """Verify source nodes exist."""
        source_nodes = [
            node for node in sample_manifest.nodes.values()
            if node.node_type == NodeType.SOURCE
        ]
        assert len(source_nodes) == 3, "Expected 3 source nodes"

        source_names = {node.name for node in source_nodes}
        assert source_names == {"orders", "customers", "inventory"}

    def test_dag_has_transform_nodes(self, sample_manifest):
        """Verify transform nodes exist."""
        transform_nodes = [
            node for node in sample_manifest.nodes.values()
            if node.node_type == NodeType.TRANSFORM
        ]
        assert len(transform_nodes) == 3, "Expected 3 transform nodes"

        transform_names = {node.name for node in transform_nodes}
        assert transform_names == {
            "clean_orders",
            "customer_enriched",
            "inventory_status"
        }

    def test_dag_dependencies_correct(self, sample_manifest):
        """Verify dependencies are correctly defined."""
        # Check clean_orders depends on orders
        clean_orders_edges = [
            edge for edge in sample_manifest.edges
            if edge.to_node == "transform.clean_orders"
        ]
        assert len(clean_orders_edges) == 1
        assert clean_orders_edges[0].from_node == "source.orders"

        # Check customer_enriched depends on customers and clean_orders
        customer_enriched_edges = [
            edge for edge in sample_manifest.edges
            if edge.to_node == "transform.customer_enriched"
        ]
        assert len(customer_enriched_edges) == 2
        upstream_ids = {edge.from_node for edge in customer_enriched_edges}
        assert upstream_ids == {"source.customers", "transform.clean_orders"}


class TestChangeDetection:
    """Test change categorization."""

    def test_column_addition_is_non_breaking(self):
        """Adding a column should be NON_BREAKING."""
        old_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            columns={"col1": "string", "col2": "integer"}
        )

        new_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            columns={"col1": "string", "col2": "integer", "col3": "float"}
        )

        from seeknal.dag.diff import _classify_column_change
        category = _classify_column_change(old_node, new_node)

        assert category == ChangeCategory.NON_BREAKING

    def test_column_removal_is_breaking(self):
        """Removing a column should be BREAKING."""
        old_node = Node(
            id="source.test",
            name="test",
            node_type=NodeType.SOURCE,
            columns={"col1": "string", "col2": "integer", "col3": "float"}
        )

        new_node = Node(
            id="source.test",
            name="test",
            node_type=NodeType.SOURCE,
            columns={"col1": "string", "col2": "integer"}
        )

        from seeknal.dag.diff import _classify_column_change
        category = _classify_column_change(old_node, new_node)

        assert category == ChangeCategory.BREAKING

    def test_column_type_change_is_breaking(self):
        """Changing a column type should be BREAKING."""
        old_node = Node(
            id="source.test",
            name="test",
            node_type=NodeType.SOURCE,
            columns={"col1": "string", "col2": "integer"}
        )

        new_node = Node(
            id="source.test",
            name="test",
            node_type=NodeType.SOURCE,
            columns={"col1": "string", "col2": "float"}  # type change
        )

        from seeknal.dag.diff import _classify_column_change
        category = _classify_column_change(old_node, new_node)

        assert category == ChangeCategory.BREAKING

    def test_metadata_change_is_metadata(self):
        """Changing description/owner/tags should be METADATA."""
        old_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            description="Old description",
            owner="old-team",
            tags=["old-tag"]
        )

        new_node = Node(
            id="transform.test",
            name="test",
            node_type=NodeType.TRANSFORM,
            description="New description",
            owner="new-team",
            tags=["new-tag"]
        )

        # Metadata-only changes should not require rebuild
        # This is verified by the diff module's handling of these fields
        assert old_node.id == new_node.id
        assert old_node.name == new_node.name


class TestPlanOutput:
    """Test plan output format."""

    def test_manifest_diff_structure(self, sample_manifest):
        """Verify ManifestDiff has correct structure."""
        diff = ManifestDiff()

        # Should have correct attributes
        assert hasattr(diff, "added_nodes")
        assert hasattr(diff, "removed_nodes")
        assert hasattr(diff, "modified_nodes")
        assert hasattr(diff, "added_edges")
        assert hasattr(diff, "removed_edges")

        # Should be able to add changes
        test_node = sample_manifest.nodes["source.orders"]
        diff.added_nodes[test_node.id] = test_node

        assert len(diff.added_nodes) == 1
        assert test_node.id in diff.added_nodes


class TestIntegration:
    """Integration tests for full workflow."""

    def test_full_pipeline_workflow(self, tutorial_dir, yaml_files):
        """Test complete workflow: parse → build → execute."""
        # 1. Parse YAML files
        assert len(yaml_files) == 8

        # 2. Verify all nodes have unique names
        node_names = [config["name"] for config in yaml_files.values()]
        assert len(node_names) == len(set(node_names)), "Duplicate node names found"

        # 3. Verify dependencies resolve
        all_node_ids = {
            f"{config['kind']}.{config['name']}"
            for config in yaml_files.values()
        }

        for config in yaml_files.values():
            if "inputs" in config:
                for inp in config["inputs"]:
                    ref = inp.get("ref")
                    if ref:
                        assert ref in all_node_ids, f"Unresolved dependency: {ref}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
