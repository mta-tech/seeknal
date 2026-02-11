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


class TestSQLAwareChangeCategorization:
    """Test SQL-aware change categorization."""

    def test_identical_sql_no_change(self):
        """Identical SQL is classified as METADATA (no change)."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change(
            "SELECT a, b FROM foo",
            "SELECT a, b FROM foo"
        )

        assert category == ChangeCategory.METADATA

    def test_whitespace_only_sql_metadata(self):
        """Whitespace-only SQL changes are METADATA."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change(
            "SELECT a, b FROM foo",
            "SELECT  a,b  FROM  foo"
        )

        assert category == ChangeCategory.METADATA

    def test_column_added_non_breaking(self):
        """Adding a column is NON_BREAKING."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change(
            "SELECT a FROM foo",
            "SELECT a, b FROM foo"
        )

        assert category == ChangeCategory.NON_BREAKING

    def test_column_removed_breaking(self):
        """Removing a column is BREAKING."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change(
            "SELECT a, b FROM foo",
            "SELECT a FROM foo"
        )

        assert category == ChangeCategory.BREAKING

    def test_sql_added_non_breaking(self):
        """Adding SQL where there was none is NON_BREAKING."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change(None, "SELECT a FROM foo")

        assert category == ChangeCategory.NON_BREAKING

    def test_sql_removed_breaking(self):
        """Removing SQL is BREAKING."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change("SELECT a FROM foo", None)

        assert category == ChangeCategory.BREAKING

    def test_table_added_non_breaking(self):
        """Adding a table (new JOIN) is NON_BREAKING."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change(
            "SELECT * FROM users",
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        )

        assert category == ChangeCategory.NON_BREAKING

    def test_table_removed_breaking(self):
        """Removing a table is BREAKING."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            "SELECT * FROM users"
        )

        assert category == ChangeCategory.BREAKING

    def test_case_insensitive_no_change(self):
        """Case differences are normalized (METADATA)."""
        from seeknal.dag.diff import _classify_sql_change, ChangeCategory

        category = _classify_sql_change(
            "SELECT a FROM foo",
            "select a from foo"
        )

        assert category == ChangeCategory.METADATA


class TestConfigSQLAwareCategorization:
    """Test config change categorization with SQL-aware logic."""

    def test_sql_config_change_detected(self):
        """SQL changes in config are properly categorized."""
        from seeknal.dag.diff import _classify_config_change, ChangeCategory

        old_node = Node(
            id="source.a",
            name="a",
            node_type=NodeType.SOURCE,
            config={"sql": "SELECT a FROM foo"}
        )

        # Non-breaking: column added
        new_node = Node(
            id="source.a",
            name="a",
            node_type=NodeType.SOURCE,
            config={"sql": "SELECT a, b FROM foo"}
        )

        category = _classify_config_change(old_node, new_node)

        # Should be NON_BREAKING (column added)
        assert category == ChangeCategory.NON_BREAKING

    def test_sql_config_breaking_change(self):
        """Breaking SQL changes are detected."""
        from seeknal.dag.diff import _classify_config_change, ChangeCategory

        old_node = Node(
            id="source.a",
            name="a",
            node_type=NodeType.SOURCE,
            config={"sql": "SELECT a, b FROM foo"}
        )

        # Breaking: column removed
        new_node = Node(
            id="source.a",
            name="a",
            node_type=NodeType.SOURCE,
            config={"sql": "SELECT a FROM foo"}
        )

        category = _classify_config_change(old_node, new_node)

        # Should be BREAKING (column removed)
        assert category == ChangeCategory.BREAKING

    def test_metadata_only_sql_change(self):
        """Whitespace-only SQL changes are METADATA."""
        from seeknal.dag.diff import _classify_config_change, ChangeCategory

        old_node = Node(
            id="source.a",
            name="a",
            node_type=NodeType.SOURCE,
            config={"sql": "SELECT a FROM foo"}
        )

        # Only whitespace changed
        new_node = Node(
            id="source.a",
            name="a",
            node_type=NodeType.SOURCE,
            config={"sql": "SELECT  a  FROM  foo"}
        )

        category = _classify_config_change(old_node, new_node)

        # Should be METADATA (semantically identical)
        assert category == ChangeCategory.METADATA

    def test_non_sql_config_change_unchanged(self):
        """Non-SQL config changes work as before."""
        from seeknal.dag.diff import _classify_config_change, ChangeCategory

        old_node = Node(
            id="source.a",
            name="a",
            node_type=NodeType.SOURCE,
            config={"audits": ["audit1"]}
        )

        new_node = Node(
            id="source.a",
            name="a",
            node_type=NodeType.SOURCE,
            config={"audits": ["audit2"]}
        )

        category = _classify_config_change(old_node, new_node)

        # Audits are metadata-only
        assert category == ChangeCategory.METADATA
