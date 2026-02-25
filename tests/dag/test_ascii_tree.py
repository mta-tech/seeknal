"""Tests for ASCII tree lineage output."""

import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.dag.visualize import (
    LineageVisualizationError,
    render_ascii_tree,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def linear_manifest():
    """source.orders -> transform.clean -> feature_group.features"""
    m = Manifest(project="test")
    m.add_node(Node(id="source.orders", name="orders", node_type=NodeType.SOURCE))
    m.add_node(Node(id="transform.clean", name="clean", node_type=NodeType.TRANSFORM))
    m.add_node(Node(id="feature_group.features", name="features", node_type=NodeType.FEATURE_GROUP))
    m.add_edge("source.orders", "transform.clean")
    m.add_edge("transform.clean", "feature_group.features")
    return m


@pytest.fixture
def diamond_manifest():
    """Diamond: source.a -> transform.b, source.a -> transform.c, both -> feature_group.d"""
    m = Manifest(project="test")
    m.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
    m.add_node(Node(id="transform.b", name="b", node_type=NodeType.TRANSFORM))
    m.add_node(Node(id="transform.c", name="c", node_type=NodeType.TRANSFORM))
    m.add_node(Node(id="feature_group.d", name="d", node_type=NodeType.FEATURE_GROUP))
    m.add_edge("source.a", "transform.b")
    m.add_edge("source.a", "transform.c")
    m.add_edge("transform.b", "feature_group.d")
    m.add_edge("transform.c", "feature_group.d")
    return m


@pytest.fixture
def multi_root_manifest():
    """Two roots: source.orders -> transform.enriched, source.products -> transform.enriched"""
    m = Manifest(project="test")
    m.add_node(Node(id="source.orders", name="orders", node_type=NodeType.SOURCE))
    m.add_node(Node(id="source.products", name="products", node_type=NodeType.SOURCE))
    m.add_node(Node(id="transform.enriched", name="enriched", node_type=NodeType.TRANSFORM))
    m.add_node(Node(id="rule.check", name="check", node_type=NodeType.RULE))
    m.add_edge("source.orders", "transform.enriched")
    m.add_edge("source.products", "transform.enriched")
    m.add_edge("transform.enriched", "rule.check")
    return m


# ---------------------------------------------------------------------------
# Full DAG Tests
# ---------------------------------------------------------------------------


class TestFullDagTree:
    """Tests for full DAG ASCII tree rendering."""

    def test_linear_dag(self, linear_manifest):
        result = render_ascii_tree(linear_manifest)
        lines = result.split("\n")
        assert lines[0] == "source.orders"
        assert "transform.clean" in lines[1]
        assert "feature_group.features" in lines[2]

    def test_diamond_multi_parent(self, diamond_manifest):
        """Node with 2 parents appears under each parent."""
        result = render_ascii_tree(diamond_manifest)
        # feature_group.d should appear under both transform.b and transform.c
        assert result.count("feature_group.d") == 2

    def test_multi_root(self, multi_root_manifest):
        """Multiple source nodes each start a new tree section."""
        result = render_ascii_tree(multi_root_manifest)
        lines = result.split("\n")
        # Both sources should appear as root-level (no prefix)
        root_lines = [l for l in lines if not l.startswith((" ", "\u2502", "\u251c", "\u2514"))]
        assert "source.orders" in root_lines
        assert "source.products" in root_lines

    def test_empty_manifest(self):
        """Empty manifest returns empty string."""
        m = Manifest(project="test")
        result = render_ascii_tree(m)
        assert result == ""

    def test_single_node_no_edges(self):
        """Single node with no edges renders just the node."""
        m = Manifest(project="test")
        m.add_node(Node(id="source.lonely", name="lonely", node_type=NodeType.SOURCE))
        result = render_ascii_tree(m)
        assert result == "source.lonely"

    def test_box_drawing_characters(self, linear_manifest):
        """Output uses Unicode box-drawing characters."""
        result = render_ascii_tree(linear_manifest)
        assert "\u2514\u2500\u2500" in result or "\u251c\u2500\u2500" in result

    def test_children_sorted_alphabetically(self):
        """Children under a parent are sorted alphabetically."""
        m = Manifest(project="test")
        m.add_node(Node(id="source.root", name="root", node_type=NodeType.SOURCE))
        m.add_node(Node(id="transform.zebra", name="zebra", node_type=NodeType.TRANSFORM))
        m.add_node(Node(id="transform.alpha", name="alpha", node_type=NodeType.TRANSFORM))
        m.add_node(Node(id="transform.middle", name="middle", node_type=NodeType.TRANSFORM))
        m.add_edge("source.root", "transform.zebra")
        m.add_edge("source.root", "transform.alpha")
        m.add_edge("source.root", "transform.middle")
        result = render_ascii_tree(m)
        lines = result.split("\n")
        child_names = [l.split(" ")[-1] for l in lines[1:]]
        assert child_names == ["transform.alpha", "transform.middle", "transform.zebra"]

    def test_last_child_uses_corner(self, linear_manifest):
        """Last child uses corner connector, not tee."""
        result = render_ascii_tree(linear_manifest)
        assert "\u2514\u2500\u2500 " in result

    def test_non_last_child_uses_tee(self, diamond_manifest):
        """Non-last children use tee connector."""
        result = render_ascii_tree(diamond_manifest)
        assert "\u251c\u2500\u2500 " in result


# ---------------------------------------------------------------------------
# Focus Mode Tests
# ---------------------------------------------------------------------------


class TestFocusMode:
    """Tests for focused subgraph rendering."""

    def test_focus_shows_upstream_and_downstream(self, linear_manifest):
        """Focus on middle node shows upstream + downstream."""
        result = render_ascii_tree(linear_manifest, focus_node="transform.clean")
        assert "source.orders" in result
        assert "transform.clean" in result
        assert "feature_group.features" in result

    def test_focus_on_root_shows_downstream_only(self, linear_manifest):
        """Focus on source shows only downstream chain."""
        result = render_ascii_tree(linear_manifest, focus_node="source.orders")
        assert "source.orders" in result
        assert "transform.clean" in result
        assert "feature_group.features" in result

    def test_focus_on_leaf_shows_upstream_only(self, linear_manifest):
        """Focus on leaf shows only upstream chain."""
        result = render_ascii_tree(linear_manifest, focus_node="feature_group.features")
        assert "source.orders" in result
        assert "transform.clean" in result
        assert "feature_group.features" in result

    def test_focus_excludes_unrelated_nodes(self):
        """Focus filters out nodes not in upstream/downstream."""
        m = Manifest(project="test")
        m.add_node(Node(id="source.a", name="a", node_type=NodeType.SOURCE))
        m.add_node(Node(id="transform.b", name="b", node_type=NodeType.TRANSFORM))
        m.add_node(Node(id="source.unrelated", name="unrelated", node_type=NodeType.SOURCE))
        m.add_node(Node(id="transform.also_unrelated", name="also_unrelated", node_type=NodeType.TRANSFORM))
        m.add_edge("source.a", "transform.b")
        m.add_edge("source.unrelated", "transform.also_unrelated")

        result = render_ascii_tree(m, focus_node="transform.b")
        assert "source.a" in result
        assert "transform.b" in result
        assert "unrelated" not in result

    def test_focus_invalid_node_raises(self, linear_manifest):
        """Focusing on non-existent node raises error."""
        with pytest.raises(LineageVisualizationError, match="not found"):
            render_ascii_tree(linear_manifest, focus_node="transform.nope")

    def test_focus_diamond_subgraph(self, diamond_manifest):
        """Focus on diamond join node includes all parents."""
        result = render_ascii_tree(diamond_manifest, focus_node="feature_group.d")
        assert "source.a" in result
        assert "transform.b" in result
        assert "transform.c" in result
        assert "feature_group.d" in result


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge case tests."""

    def test_deep_tree_indentation(self):
        """Deep chains produce correct indentation."""
        m = Manifest(project="test")
        m.add_node(Node(id="source.l0", name="l0", node_type=NodeType.SOURCE))
        m.add_node(Node(id="transform.l1", name="l1", node_type=NodeType.TRANSFORM))
        m.add_node(Node(id="transform.l2", name="l2", node_type=NodeType.TRANSFORM))
        m.add_node(Node(id="transform.l3", name="l3", node_type=NodeType.TRANSFORM))
        m.add_edge("source.l0", "transform.l1")
        m.add_edge("transform.l1", "transform.l2")
        m.add_edge("transform.l2", "transform.l3")
        result = render_ascii_tree(m)
        lines = result.split("\n")
        assert len(lines) == 4
        assert lines[0] == "source.l0"
        assert lines[1].startswith("\u2514\u2500\u2500 ")
        assert lines[2].startswith("    \u2514\u2500\u2500 ")
        assert lines[3].startswith("        \u2514\u2500\u2500 ")

    def test_all_node_types_render(self):
        """Various node types render their full qualified name."""
        m = Manifest(project="test")
        m.add_node(Node(id="source.s", name="s", node_type=NodeType.SOURCE))
        m.add_node(Node(id="transform.t", name="t", node_type=NodeType.TRANSFORM))
        m.add_node(Node(id="rule.r", name="r", node_type=NodeType.RULE))
        m.add_node(Node(id="aggregation.a", name="a", node_type=NodeType.AGGREGATION))
        m.add_edge("source.s", "transform.t")
        m.add_edge("transform.t", "rule.r")
        m.add_edge("transform.t", "aggregation.a")
        result = render_ascii_tree(m)
        assert "source.s" in result
        assert "transform.t" in result
        assert "rule.r" in result
        assert "aggregation.a" in result

    def test_output_has_no_trailing_newline(self, linear_manifest):
        """Output does not end with a trailing newline."""
        result = render_ascii_tree(linear_manifest)
        assert not result.endswith("\n")

    def test_output_is_pipeable_no_ansi(self, linear_manifest):
        """Output contains no ANSI escape sequences."""
        result = render_ascii_tree(linear_manifest)
        assert "\033[" not in result
        assert "\x1b[" not in result
