"""Tests for manifest diff detection."""
import pytest
from seeknal.dag.diff import ManifestDiff, DiffType
from seeknal.dag.manifest import Manifest, Node, NodeType


class TestManifestDiff:
    """Test the ManifestDiff class."""

    def test_diff_identical_manifests(self):
        """Identical manifests have no changes."""
        old = Manifest(project="test")
        old.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        diff = ManifestDiff.compare(old, new)

        assert len(diff.added_nodes) == 0
        assert len(diff.removed_nodes) == 0
        assert len(diff.modified_nodes) == 0

    def test_diff_detects_added_nodes(self):
        """Diff detects newly added nodes."""
        old = Manifest(project="test")
        old.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        new.add_node(Node(id="source.b", name="b", node_type=NodeType.SOURCE))

        diff = ManifestDiff.compare(old, new)

        assert len(diff.added_nodes) == 1
        assert "source.b" in diff.added_nodes

    def test_diff_detects_removed_nodes(self):
        """Diff detects removed nodes."""
        old = Manifest(project="test")
        old.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        old.add_node(Node(id="source.b", name="b", node_type=NodeType.SOURCE))

        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        diff = ManifestDiff.compare(old, new)

        assert len(diff.removed_nodes) == 1
        assert "source.b" in diff.removed_nodes

    def test_diff_detects_modified_nodes(self):
        """Diff detects modified nodes (config changes)."""
        old = Manifest(project="test")
        old.add_node(Node(
            id="source.a", name="a", node_type=NodeType.SOURCE,
            description="Old description"
        ))

        new = Manifest(project="test")
        new.add_node(Node(
            id="source.a", name="a", node_type=NodeType.SOURCE,
            description="New description"
        ))

        diff = ManifestDiff.compare(old, new)

        assert len(diff.modified_nodes) == 1
        assert "source.a" in diff.modified_nodes

    def test_diff_detects_added_edges(self):
        """Diff detects newly added edges."""
        old = Manifest(project="test")
        old.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        old.add_node(Node(id="fg.b", name="b", node_type=NodeType.FEATURE_GROUP))

        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        new.add_node(Node(id="fg.b", name="b", node_type=NodeType.FEATURE_GROUP))
        new.add_edge("source.a", "fg.b")

        diff = ManifestDiff.compare(old, new)

        assert len(diff.added_edges) == 1

    def test_diff_detects_removed_edges(self):
        """Diff detects removed edges."""
        old = Manifest(project="test")
        old.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        old.add_node(Node(id="fg.b", name="b", node_type=NodeType.FEATURE_GROUP))
        old.add_edge("source.a", "fg.b")

        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        new.add_node(Node(id="fg.b", name="b", node_type=NodeType.FEATURE_GROUP))

        diff = ManifestDiff.compare(old, new)

        assert len(diff.removed_edges) == 1

    def test_diff_has_changes(self):
        """has_changes returns True when there are changes."""
        old = Manifest(project="test")
        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        diff = ManifestDiff.compare(old, new)

        assert diff.has_changes() is True

    def test_diff_no_changes(self):
        """has_changes returns False when manifests are identical."""
        old = Manifest(project="test")
        old.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        diff = ManifestDiff.compare(old, new)

        assert diff.has_changes() is False

    def test_diff_to_dict(self):
        """Diff can be serialized to dict."""
        old = Manifest(project="test")
        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))

        diff = ManifestDiff.compare(old, new)
        data = diff.to_dict()

        assert "added_nodes" in data
        assert "removed_nodes" in data
        assert "modified_nodes" in data
        assert "added_edges" in data
        assert "removed_edges" in data

    def test_diff_summary(self):
        """Diff provides a summary."""
        old = Manifest(project="test")
        new = Manifest(project="test")
        new.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        new.add_node(Node(id="source.b", name="b", node_type=NodeType.SOURCE))

        diff = ManifestDiff.compare(old, new)
        summary = diff.summary()

        assert "2" in summary or "added" in summary.lower()
