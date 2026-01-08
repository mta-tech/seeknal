"""Tests for manifest data structures."""
import pytest
import json
from datetime import datetime
from seeknal.dag.manifest import (
    Manifest,
    Node,
    NodeType,
    Edge,
    ManifestMetadata,
)


class TestManifestMetadata:
    """Test ManifestMetadata class."""

    def test_metadata_creation(self):
        """Can create manifest metadata."""
        meta = ManifestMetadata(
            project="test_project",
            seeknal_version="2.0.0"
        )
        assert meta.project == "test_project"
        assert meta.seeknal_version == "2.0.0"
        assert meta.generated_at is not None

    def test_metadata_to_dict(self):
        """Metadata can be serialized to dict."""
        meta = ManifestMetadata(
            project="test_project",
            seeknal_version="2.0.0"
        )
        data = meta.to_dict()
        assert data["project"] == "test_project"
        assert "generated_at" in data


class TestNode:
    """Test Node class."""

    def test_node_creation(self):
        """Can create a node."""
        node = Node(
            id="feature_group.user_features",
            name="user_features",
            node_type=NodeType.FEATURE_GROUP,
            description="User features",
            owner="team@example.com",
            tags=["user", "features"],
        )
        assert node.id == "feature_group.user_features"
        assert node.node_type == NodeType.FEATURE_GROUP

    def test_node_with_columns(self):
        """Node can have column definitions."""
        node = Node(
            id="feature_group.user_features",
            name="user_features",
            node_type=NodeType.FEATURE_GROUP,
            columns={
                "user_id": "User identifier",
                "total_purchases": "Total purchase count"
            }
        )
        assert len(node.columns) == 2
        assert node.columns["user_id"] == "User identifier"

    def test_node_to_dict(self):
        """Node can be serialized to dict."""
        node = Node(
            id="source.traffic_day",
            name="traffic_day",
            node_type=NodeType.SOURCE,
        )
        data = node.to_dict()
        assert data["id"] == "source.traffic_day"
        assert data["type"] == "source"


class TestEdge:
    """Test Edge class."""

    def test_edge_creation(self):
        """Can create an edge."""
        edge = Edge(
            from_node="source.traffic_day",
            to_node="feature_group.user_features"
        )
        assert edge.from_node == "source.traffic_day"
        assert edge.to_node == "feature_group.user_features"


class TestManifest:
    """Test Manifest class."""

    def test_manifest_creation(self):
        """Can create an empty manifest."""
        manifest = Manifest(project="test_project")
        assert manifest.metadata.project == "test_project"
        assert len(manifest.nodes) == 0
        assert len(manifest.edges) == 0

    def test_add_node(self):
        """Can add nodes to manifest."""
        manifest = Manifest(project="test_project")

        node = Node(
            id="source.traffic_day",
            name="traffic_day",
            node_type=NodeType.SOURCE,
        )
        manifest.add_node(node)

        assert len(manifest.nodes) == 1
        assert "source.traffic_day" in manifest.nodes

    def test_add_edge(self):
        """Can add edges to manifest."""
        manifest = Manifest(project="test_project")

        manifest.add_edge("source.traffic_day", "feature_group.user_features")

        assert len(manifest.edges) == 1

    def test_get_node(self):
        """Can get a node by ID."""
        manifest = Manifest(project="test_project")
        node = Node(
            id="source.traffic_day",
            name="traffic_day",
            node_type=NodeType.SOURCE,
        )
        manifest.add_node(node)

        retrieved = manifest.get_node("source.traffic_day")
        assert retrieved.name == "traffic_day"

    def test_to_json(self):
        """Manifest can be serialized to JSON."""
        manifest = Manifest(project="test_project")
        manifest.add_node(Node(
            id="source.traffic_day",
            name="traffic_day",
            node_type=NodeType.SOURCE,
        ))
        manifest.add_edge("source.traffic_day", "feature_group.user_features")

        json_str = manifest.to_json()
        data = json.loads(json_str)

        assert data["metadata"]["project"] == "test_project"
        assert "source.traffic_day" in data["nodes"]
        assert len(data["edges"]) == 1

    def test_from_json(self):
        """Manifest can be deserialized from JSON."""
        json_data = {
            "metadata": {
                "project": "test_project",
                "seeknal_version": "2.0.0",
                "generated_at": "2026-01-08T10:00:00"
            },
            "nodes": {
                "source.traffic_day": {
                    "id": "source.traffic_day",
                    "name": "traffic_day",
                    "type": "source"
                }
            },
            "edges": [
                {"from": "source.traffic_day", "to": "feature_group.user"}
            ]
        }

        manifest = Manifest.from_dict(json_data)

        assert manifest.metadata.project == "test_project"
        assert len(manifest.nodes) == 1

    def test_get_upstream_nodes(self):
        """Can get upstream dependencies for a node."""
        manifest = Manifest(project="test_project")
        manifest.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        manifest.add_node(Node(id="source.b", name="b", node_type=NodeType.SOURCE))
        manifest.add_node(Node(id="fg.c", name="c", node_type=NodeType.FEATURE_GROUP))
        manifest.add_edge("source.a", "fg.c")
        manifest.add_edge("source.b", "fg.c")

        upstream = manifest.get_upstream_nodes("fg.c")
        assert len(upstream) == 2
        assert "source.a" in upstream
        assert "source.b" in upstream

    def test_get_downstream_nodes(self):
        """Can get downstream dependents for a node."""
        manifest = Manifest(project="test_project")
        manifest.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        manifest.add_node(Node(id="fg.b", name="b", node_type=NodeType.FEATURE_GROUP))
        manifest.add_node(Node(id="model.c", name="c", node_type=NodeType.MODEL))
        manifest.add_edge("source.a", "fg.b")
        manifest.add_edge("fg.b", "model.c")

        downstream = manifest.get_downstream_nodes("source.a")
        assert "fg.b" in downstream

    def test_detect_cycles(self):
        """Can detect cycles in the graph."""
        manifest = Manifest(project="test_project")
        manifest.add_node(Node(id="a", name="a", node_type=NodeType.SOURCE))
        manifest.add_node(Node(id="b", name="b", node_type=NodeType.FEATURE_GROUP))
        manifest.add_node(Node(id="c", name="c", node_type=NodeType.MODEL))
        manifest.add_edge("a", "b")
        manifest.add_edge("b", "c")
        manifest.add_edge("c", "a")  # Creates cycle

        has_cycle, cycle_path = manifest.detect_cycles()
        assert has_cycle is True
        assert len(cycle_path) > 0
