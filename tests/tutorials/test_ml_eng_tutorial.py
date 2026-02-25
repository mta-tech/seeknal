"""
Tests for ML Engineering tutorial files.

Validates that the fraud detection pipeline YAML files:
- Parse correctly
- Build a valid DAG with correct layer structure
- Have 8 sources in layer 0 (parallel execution)
- Correctly categorize changes (BREAKING vs NON_BREAKING)
"""

import yaml
from pathlib import Path
from typing import Dict, Set

import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType, Edge
from seeknal.dag.diff import ManifestDiff, ChangeCategory


# Path to tutorial example files
EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples" / "ml-eng"


class TestMLEngTutorial:
    """Test suite for Phase 2 ML Engineering tutorial."""

    def test_all_yaml_files_exist(self):
        """Verify all 15 YAML files exist."""
        expected_files = [
            "01_source_transactions.yml",
            "02_source_user_profiles.yml",
            "03_source_device_fingerprints.yml",
            "04_source_ip_geolocation.yml",
            "05_source_merchant_data.yml",
            "06_source_card_data.yml",
            "07_source_fraud_labels.yml",
            "08_source_realtime_alerts.yml",
            "09_transform_txn_enriched.yml",
            "10_transform_merchant_risk.yml",
            "11_transform_card_velocity.yml",
            "12_transform_alert_features.yml",
            "13_feature_group_txn_features.yml",
            "14_feature_group_user_risk.yml",
            "15_exposure_training_data.yml",
        ]

        for filename in expected_files:
            filepath = EXAMPLES_DIR / filename
            assert filepath.exists(), f"Missing file: {filename}"

    def test_all_yaml_files_parse(self):
        """Verify all YAML files are valid YAML."""
        yaml_files = sorted(EXAMPLES_DIR.glob("*.yml"))
        assert len(yaml_files) == 15, f"Expected 15 YAML files, found {len(yaml_files)}"

        for filepath in yaml_files:
            with open(filepath) as f:
                data = yaml.safe_load(f)
                assert data is not None, f"Failed to parse {filepath.name}"
                assert "kind" in data, f"Missing 'kind' field in {filepath.name}"
                assert "name" in data, f"Missing 'name' field in {filepath.name}"

    def test_node_types_correct(self):
        """Verify correct distribution of node types."""
        yaml_files = sorted(EXAMPLES_DIR.glob("*.yml"))
        node_types = {}

        for filepath in yaml_files:
            with open(filepath) as f:
                data = yaml.safe_load(f)
                kind = data["kind"]
                name = data["name"]
                node_types[name] = kind

        # Count by type
        sources = sum(1 for k in node_types.values() if k == "source")
        transforms = sum(1 for k in node_types.values() if k == "transform")
        feature_groups = sum(1 for k in node_types.values() if k == "feature_group")
        exposures = sum(1 for k in node_types.values() if k == "exposure")

        assert sources == 8, f"Expected 8 sources, found {sources}"
        assert transforms == 4, f"Expected 4 transforms, found {transforms}"
        assert feature_groups == 2, f"Expected 2 feature groups, found {feature_groups}"
        assert exposures == 1, f"Expected 1 exposure, found {exposures}"

    def test_dag_builds_correctly(self):
        """Test that YAML files build a valid DAG with correct structure."""
        # Build manifest from YAML files
        nodes: Dict[str, Node] = {}
        edges: list[Edge] = []

        yaml_files = sorted(EXAMPLES_DIR.glob("*.yml"))

        for filepath in yaml_files:
            with open(filepath) as f:
                data = yaml.safe_load(f)

            # Create node
            kind = data["kind"]
            name = data["name"]

            # Map kind to NodeType
            node_type_map = {
                "source": NodeType.SOURCE,
                "transform": NodeType.TRANSFORM,
                "feature_group": NodeType.FEATURE_GROUP,
                "exposure": NodeType.EXPOSURE,
            }

            node_type = node_type_map.get(kind)
            assert node_type is not None, f"Unknown kind: {kind}"

            node_id = f"{kind}.{name}"
            node = Node(
                id=node_id,
                name=name,
                node_type=node_type,
                config=data,
            )
            nodes[node_id] = node

            # Create edges from inputs
            if "inputs" in data:
                for input_ref in data["inputs"]:
                    ref = input_ref["ref"]
                    edges.append(Edge(from_node=ref, to_node=node_id))

        manifest = Manifest(project="ml-eng")
        manifest.nodes = nodes
        manifest.edges = edges

        # Validate DAG structure
        assert len(manifest.nodes) == 15, f"Expected 15 nodes, got {len(manifest.nodes)}"
        assert len(manifest.edges) > 0, "Expected edges in DAG"

        # Check no cycles
        # Simple DFS-based cycle detection
        def has_cycle(manifest: Manifest) -> bool:
            visited = set()
            rec_stack = set()

            def dfs(node_id: str) -> bool:
                visited.add(node_id)
                rec_stack.add(node_id)

                for edge in manifest.edges:
                    if edge.from_node == node_id:
                        neighbor = edge.to_node
                        if neighbor not in visited:
                            if dfs(neighbor):
                                return True
                        elif neighbor in rec_stack:
                            return True

                rec_stack.remove(node_id)
                return False

            for node_id in manifest.nodes:
                if node_id not in visited:
                    if dfs(node_id):
                        return True
            return False

        assert not has_cycle(manifest), "DAG contains cycles"

    def test_topological_layers(self, tmp_path):
        """Test that DAG has correct topological layer structure.

        Layer 0: 8 sources (all independent)
        Layer 1: 4 transforms (depend on sources)
        Layer 2: 2 feature groups (depend on transforms)
        Layer 3: 1 exposure (depends on feature groups)
        """
        # Build manifest
        nodes: Dict[str, Node] = {}
        edges: list[Edge] = []

        yaml_files = sorted(EXAMPLES_DIR.glob("*.yml"))

        for filepath in yaml_files:
            with open(filepath) as f:
                data = yaml.safe_load(f)

            kind = data["kind"]
            name = data["name"]

            node_type_map = {
                "source": NodeType.SOURCE,
                "transform": NodeType.TRANSFORM,
                "feature_group": NodeType.FEATURE_GROUP,
                "exposure": NodeType.EXPOSURE,
            }

            node_type = node_type_map[kind]
            node_id = f"{kind}.{name}"
            node = Node(
                id=node_id,
                name=name,
                node_type=node_type,
                config=data,
            )
            nodes[node_id] = node

            if "inputs" in data:
                for input_ref in data["inputs"]:
                    ref = input_ref["ref"]
                    edges.append(Edge(from_node=ref, to_node=node_id))

        manifest = Manifest(project="ml-eng")
        manifest.nodes = nodes
        manifest.edges = edges

        # Compute topological layers using Kahn's algorithm
        def get_topological_layers(manifest: Manifest) -> list[list[str]]:
            """Compute topological layers (nodes with same depth)."""
            # Build adjacency list and in-degree count
            in_degree: Dict[str, int] = {node_id: 0 for node_id in manifest.nodes}
            adjacency: Dict[str, Set[str]] = {node_id: set() for node_id in manifest.nodes}

            for edge in manifest.edges:
                adjacency[edge.from_node].add(edge.to_node)
                in_degree[edge.to_node] = in_degree.get(edge.to_node, 0) + 1

            # Initialize queue with nodes having in-degree 0
            queue = [node_id for node_id, deg in in_degree.items() if deg == 0]
            layers: list[list[str]] = []

            while queue:
                # Current layer = all nodes in queue
                layer = list(queue)
                layers.append(layer)
                next_queue = []

                for node_id in layer:
                    # Reduce in-degree of neighbors
                    for neighbor in adjacency[node_id]:
                        in_degree[neighbor] -= 1
                        if in_degree[neighbor] == 0:
                            next_queue.append(neighbor)

                queue = next_queue

            return layers

        layers = get_topological_layers(manifest)

        # Validate layer structure
        assert len(layers) == 4, f"Expected 4 layers, got {len(layers)}"

        # Layer 0: 8 sources
        assert len(layers[0]) == 8, f"Expected 8 sources in layer 0, got {len(layers[0])}"
        for node_id in layers[0]:
            assert node_id.startswith("source."), f"Layer 0 should only contain sources, got {node_id}"

        # Layer 1: 4 transforms
        assert len(layers[1]) == 4, f"Expected 4 transforms in layer 1, got {len(layers[1])}"
        for node_id in layers[1]:
            assert node_id.startswith("transform."), f"Layer 1 should only contain transforms, got {node_id}"

        # Layer 2: 2 feature groups
        assert len(layers[2]) == 2, f"Expected 2 feature groups in layer 2, got {len(layers[2])}"
        for node_id in layers[2]:
            assert node_id.startswith("feature_group."), f"Layer 2 should only contain feature groups, got {node_id}"

        # Layer 3: 1 exposure
        assert len(layers[3]) == 1, f"Expected 1 exposure in layer 3, got {len(layers[3])}"
        assert layers[3][0].startswith("exposure."), f"Layer 3 should contain exposure, got {layers[3][0]}"

    def test_change_categorization_breaking(self):
        """Test that removing a feature is categorized as BREAKING."""
        # Create original manifest with feature
        original_nodes = {
            "feature_group.test": Node(
                id="feature_group.test",
                name="test",
                node_type=NodeType.FEATURE_GROUP,
                config={
                    "kind": "feature_group",
                    "name": "test",
                    "features": {
                        "feature_a": {"dtype": "integer"},
                        "feature_b": {"dtype": "float"},
                        "feature_c": {"dtype": "string"},
                    },
                },
            )
        }
        original_manifest = Manifest(project="test")
        original_manifest.nodes = original_nodes
        original_manifest.edges = []

        # Create new manifest with removed feature
        new_nodes = {
            "feature_group.test": Node(
                id="feature_group.test",
                name="test",
                node_type=NodeType.FEATURE_GROUP,
                config={
                    "kind": "feature_group",
                    "name": "test",
                    "features": {
                        "feature_a": {"dtype": "integer"},
                        "feature_b": {"dtype": "float"},
                        # feature_c removed
                    },
                },
            )
        }
        new_manifest = Manifest(project="test")
        new_manifest.nodes = new_nodes
        new_manifest.edges = []

        # Compute diff
        diff = ManifestDiff.compare(original_manifest, new_manifest)
        rebuild_map = diff.get_nodes_to_rebuild(new_manifest)

        # Verify BREAKING categorization
        assert "feature_group.test" in rebuild_map
        assert rebuild_map["feature_group.test"] == ChangeCategory.BREAKING

    def test_change_categorization_non_breaking(self):
        """Test that adding a feature is categorized as NON_BREAKING."""
        # Create original manifest
        original_nodes = {
            "feature_group.test": Node(
                id="feature_group.test",
                name="test",
                node_type=NodeType.FEATURE_GROUP,
                config={
                    "kind": "feature_group",
                    "name": "test",
                    "features": {
                        "feature_a": {"dtype": "integer"},
                        "feature_b": {"dtype": "float"},
                    },
                },
            )
        }
        original_manifest = Manifest(project="test")
        original_manifest.nodes = original_nodes
        original_manifest.edges = []

        # Create new manifest with added feature
        new_nodes = {
            "feature_group.test": Node(
                id="feature_group.test",
                name="test",
                node_type=NodeType.FEATURE_GROUP,
                config={
                    "kind": "feature_group",
                    "name": "test",
                    "features": {
                        "feature_a": {"dtype": "integer"},
                        "feature_b": {"dtype": "float"},
                        "feature_c": {"dtype": "string"},  # Added
                    },
                },
            )
        }
        new_manifest = Manifest(project="test")
        new_manifest.nodes = new_nodes
        new_manifest.edges = []

        # Compute diff
        diff = ManifestDiff.compare(original_manifest, new_manifest)
        rebuild_map = diff.get_nodes_to_rebuild(new_manifest)

        # Verify NON_BREAKING categorization
        assert "feature_group.test" in rebuild_map
        assert rebuild_map["feature_group.test"] == ChangeCategory.NON_BREAKING

    def test_parallel_execution_layer_structure(self):
        """Test that parallel execution would benefit from wide DAG structure."""
        # Build manifest
        nodes: Dict[str, Node] = {}
        edges: list[Edge] = []

        yaml_files = sorted(EXAMPLES_DIR.glob("*.yml"))

        for filepath in yaml_files:
            with open(filepath) as f:
                data = yaml.safe_load(f)

            kind = data["kind"]
            name = data["name"]

            node_type_map = {
                "source": NodeType.SOURCE,
                "transform": NodeType.TRANSFORM,
                "feature_group": NodeType.FEATURE_GROUP,
                "exposure": NodeType.EXPOSURE,
            }

            node_type = node_type_map[kind]
            node_id = f"{kind}.{name}"
            node = Node(
                id=node_id,
                name=name,
                node_type=node_type,
                config=data,
            )
            nodes[node_id] = node

            if "inputs" in data:
                for input_ref in data["inputs"]:
                    ref = input_ref["ref"]
                    edges.append(Edge(from_node=ref, to_node=node_id))

        manifest = Manifest(project="ml-eng")
        manifest.nodes = nodes
        manifest.edges = edges

        # Count nodes that could run in parallel (in same layer)
        def get_topological_layers(manifest: Manifest) -> list[list[str]]:
            in_degree: Dict[str, int] = {node_id: 0 for node_id in manifest.nodes}
            adjacency: Dict[str, Set[str]] = {node_id: set() for node_id in manifest.nodes}

            for edge in manifest.edges:
                adjacency[edge.from_node].add(edge.to_node)
                in_degree[edge.to_node] = in_degree.get(edge.to_node, 0) + 1

            queue = [node_id for node_id, deg in in_degree.items() if deg == 0]
            layers: list[list[str]] = []

            while queue:
                layer = list(queue)
                layers.append(layer)
                next_queue = []

                for node_id in layer:
                    for neighbor in adjacency[node_id]:
                        in_degree[neighbor] -= 1
                        if in_degree[neighbor] == 0:
                            next_queue.append(neighbor)

                queue = next_queue

            return layers

        layers = get_topological_layers(manifest)

        # Calculate parallel benefit
        total_nodes = sum(len(layer) for layer in layers)
        max_parallel = max(len(layer) for layer in layers)

        # In this DAG, layer 0 has 8 nodes that can run in parallel
        assert max_parallel == 8, f"Expected max parallelism of 8, got {max_parallel}"

        # Calculate theoretical speedup if each node takes 1 time unit
        sequential_time = total_nodes  # 15 units
        parallel_time = len(layers)     # 4 units (one per layer)
        theoretical_speedup = sequential_time / parallel_time

        # With 4 layers and 15 total nodes, theoretical max speedup is 15/4 = 3.75x
        assert theoretical_speedup >= 3.5, f"Expected speedup >= 3.5x, got {theoretical_speedup:.2f}x"
