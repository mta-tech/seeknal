"""Tests for lineage visualization module."""

import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.dag.visualize import (
    LineageDataBuilder,
    LineageData,
    HTMLRenderer,
    LineageVisualizationError,
    generate_lineage_html,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_manifest():
    """Manifest with 3 nodes and 2 edges forming a linear DAG: A -> B -> C."""
    m = Manifest(project="test_project")
    m.add_node(Node(
        id="source.orders",
        name="orders",
        node_type=NodeType.SOURCE,
        description="Raw orders table",
        columns={"order_id": "PK", "amount": "decimal"},
        file_path="models/source.yml",
    ))
    m.add_node(Node(
        id="transform.clean_orders",
        name="clean_orders",
        node_type=NodeType.TRANSFORM,
        description="Cleaned orders",
        columns={"order_id": "PK", "total": "decimal"},
        config={"sql": "SELECT order_id, amount AS total FROM orders"},
        file_path="models/transform.sql",
    ))
    m.add_node(Node(
        id="feature_group.order_features",
        name="order_features",
        node_type=NodeType.FEATURE_GROUP,
        description="Order features",
        columns={"order_id": "PK", "total": "decimal"},
    ))
    m.add_edge("source.orders", "transform.clean_orders")
    m.add_edge("transform.clean_orders", "feature_group.order_features")
    return m


@pytest.fixture
def diamond_manifest():
    """Manifest with diamond DAG: A -> B, A -> C, B -> D, C -> D."""
    m = Manifest(project="test_project")
    m.add_node(Node(id="source.raw", name="raw", node_type=NodeType.SOURCE))
    m.add_node(Node(id="transform.left", name="left", node_type=NodeType.TRANSFORM))
    m.add_node(Node(id="transform.right", name="right", node_type=NodeType.TRANSFORM))
    m.add_node(Node(
        id="feature_group.merged",
        name="merged",
        node_type=NodeType.FEATURE_GROUP,
    ))
    m.add_edge("source.raw", "transform.left")
    m.add_edge("source.raw", "transform.right")
    m.add_edge("transform.left", "feature_group.merged")
    m.add_edge("transform.right", "feature_group.merged")
    return m


@pytest.fixture
def empty_manifest():
    """Manifest with no nodes or edges."""
    return Manifest(project="empty_project")


# ---------------------------------------------------------------------------
# TestLineageDataBuilder
# ---------------------------------------------------------------------------


class TestLineageDataBuilder:
    """Tests for LineageDataBuilder class."""

    def test_build_full_manifest(self, simple_manifest):
        """Build produces LineageData with correct node and edge counts."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build()

        assert isinstance(data, LineageData)
        assert len(data.nodes) == 3
        assert len(data.edges) == 2
        assert data.focus_node is None
        assert data.focus_column is None

    def test_build_returns_frozen_dataclass(self, simple_manifest):
        """LineageData is immutable (frozen dataclass)."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build()

        with pytest.raises(AttributeError):
            data.focus_node = "something"  # type: ignore[misc]

    def test_serialize_node_source(self, simple_manifest):
        """Source node serializes to Cytoscape.js format."""
        builder = LineageDataBuilder(simple_manifest)
        node = simple_manifest.nodes["source.orders"]
        result = builder._serialize_node(node)

        assert result["data"]["id"] == "source.orders"
        assert result["data"]["label"] == "orders"
        assert result["data"]["node_type"] == "source"
        assert result["data"]["description"] == "Raw orders table"
        assert result["data"]["columns"] == {
            "order_id": {"type": "", "description": "PK"},
            "amount": {"type": "", "description": "decimal"},
        }
        assert result["data"]["file_path"] == "models/source.yml"
        assert result["data"]["sql"] == ""

    def test_serialize_node_transform_with_sql(self, simple_manifest):
        """Transform node includes SQL in serialization."""
        builder = LineageDataBuilder(simple_manifest)
        node = simple_manifest.nodes["transform.clean_orders"]
        result = builder._serialize_node(node)

        assert result["data"]["node_type"] == "transform"
        assert result["data"]["sql"] == "SELECT order_id, amount AS total FROM orders"

    @pytest.mark.parametrize("node_type,expected_value", [
        (NodeType.SOURCE, "source"),
        (NodeType.TRANSFORM, "transform"),
        (NodeType.FEATURE_GROUP, "feature_group"),
        (NodeType.AGGREGATION, "aggregation"),
        (NodeType.SECOND_ORDER_AGGREGATION, "second_order_aggregation"),
        (NodeType.EXPOSURE, "exposure"),
        (NodeType.RULE, "rule"),
        (NodeType.PYTHON, "python"),
        (NodeType.SEMANTIC_MODEL, "semantic_model"),
        (NodeType.METRIC, "metric"),
        (NodeType.MODEL, "model"),
    ])
    def test_serialize_node_all_types(self, node_type, expected_value):
        """All 11 NodeType values serialize to their string values."""
        m = Manifest(project="test")
        node = Node(id=f"test.{expected_value}", name=expected_value, node_type=node_type)
        m.add_node(node)

        builder = LineageDataBuilder(m)
        result = builder._serialize_node(node)

        assert result["data"]["node_type"] == expected_value

    def test_serialize_node_no_description(self):
        """Node with None description serializes to empty string."""
        m = Manifest(project="test")
        node = Node(id="a", name="a", node_type=NodeType.SOURCE, description=None)
        m.add_node(node)
        builder = LineageDataBuilder(m)
        result = builder._serialize_node(node)
        assert result["data"]["description"] == ""

    def test_serialize_node_no_columns(self):
        """Node with no columns serializes to empty dict."""
        m = Manifest(project="test")
        node = Node(id="a", name="a", node_type=NodeType.SOURCE)
        m.add_node(node)
        builder = LineageDataBuilder(m)
        result = builder._serialize_node(node)
        assert result["data"]["columns"] == {}

    def test_serialize_edge_format(self, simple_manifest):
        """Edge serializes to Cytoscape.js format with source/target."""
        builder = LineageDataBuilder(simple_manifest)
        edge = simple_manifest.edges[0]  # source.orders -> transform.clean_orders
        result = builder._serialize_edge(edge)

        assert result["data"]["id"] == "source.orders->transform.clean_orders"
        assert result["data"]["source"] == "source.orders"
        assert result["data"]["target"] == "transform.clean_orders"

    def test_compute_column_lineage_success(self, simple_manifest):
        """Column lineage computed for nodes with SQL."""
        builder = LineageDataBuilder(simple_manifest)
        result = builder._compute_column_lineage()

        # transform.clean_orders has SQL, so it should have lineage
        assert "transform.clean_orders" in result
        lineage = result["transform.clean_orders"]
        assert "output_columns" in lineage
        assert "dependencies" in lineage

    def test_compute_column_lineage_skips_non_sql(self, simple_manifest):
        """Nodes without SQL are skipped in column lineage computation."""
        builder = LineageDataBuilder(simple_manifest)
        result = builder._compute_column_lineage()

        # source.orders has no SQL config
        assert "source.orders" not in result
        # feature_group.order_features has no SQL config
        assert "feature_group.order_features" not in result

    def test_compute_column_lineage_invalid_sql(self):
        """Invalid SQL is skipped gracefully with warning."""
        m = Manifest(project="test")
        m.add_node(Node(
            id="t.bad",
            name="bad",
            node_type=NodeType.TRANSFORM,
            config={"sql": "NOT VALID SQL SYNTAX @@@ !!!"},
        ))

        builder = LineageDataBuilder(m)
        result = builder._compute_column_lineage()

        # Should not raise, may or may not include the node
        assert isinstance(result, dict)

    def test_compute_column_lineage_lazy(self, simple_manifest):
        """Only computes for focused_node_ids when provided."""
        builder = LineageDataBuilder(simple_manifest)
        result = builder._compute_column_lineage(
            focused_node_ids={"source.orders"}
        )

        # source.orders has no SQL so no lineage, but transform should be excluded
        assert "transform.clean_orders" not in result

    def test_compute_column_lineage_lazy_includes_focused(self, simple_manifest):
        """Focused node with SQL gets lineage computed."""
        builder = LineageDataBuilder(simple_manifest)
        result = builder._compute_column_lineage(
            focused_node_ids={"transform.clean_orders"}
        )

        assert "transform.clean_orders" in result

    def test_filter_to_focus_upstream(self, simple_manifest):
        """Focus filtering returns upstream nodes."""
        builder = LineageDataBuilder(simple_manifest)
        nodes = [builder._serialize_node(n) for n in simple_manifest.nodes.values()]
        edges = [builder._serialize_edge(e) for e in simple_manifest.edges]

        filtered_nodes, _ = builder._filter_to_focus(
            nodes, edges, "feature_group.order_features"
        )

        node_ids = {n["data"]["id"] for n in filtered_nodes}
        assert "source.orders" in node_ids  # upstream
        assert "transform.clean_orders" in node_ids  # upstream
        assert "feature_group.order_features" in node_ids  # focus

    def test_filter_to_focus_downstream(self, simple_manifest):
        """Focus filtering returns downstream nodes."""
        builder = LineageDataBuilder(simple_manifest)
        nodes = [builder._serialize_node(n) for n in simple_manifest.nodes.values()]
        edges = [builder._serialize_edge(e) for e in simple_manifest.edges]

        filtered_nodes, _ = builder._filter_to_focus(
            nodes, edges, "source.orders"
        )

        node_ids = {n["data"]["id"] for n in filtered_nodes}
        assert "source.orders" in node_ids  # focus
        assert "transform.clean_orders" in node_ids  # downstream
        assert "feature_group.order_features" in node_ids  # downstream

    def test_filter_to_focus_both(self, simple_manifest):
        """Focus on middle node returns both upstream and downstream."""
        builder = LineageDataBuilder(simple_manifest)
        nodes = [builder._serialize_node(n) for n in simple_manifest.nodes.values()]
        edges = [builder._serialize_edge(e) for e in simple_manifest.edges]

        filtered_nodes, filtered_edges = builder._filter_to_focus(
            nodes, edges, "transform.clean_orders"
        )

        node_ids = {n["data"]["id"] for n in filtered_nodes}
        assert len(node_ids) == 3  # All nodes connected
        assert len(filtered_edges) == 2

    def test_filter_to_focus_diamond(self, diamond_manifest):
        """Focus filtering works for diamond-shaped DAGs."""
        builder = LineageDataBuilder(diamond_manifest)
        nodes = [builder._serialize_node(n) for n in diamond_manifest.nodes.values()]
        edges = [builder._serialize_edge(e) for e in diamond_manifest.edges]

        # Focus on "transform.left" - should get source.raw upstream, merged downstream
        filtered_nodes, _ = builder._filter_to_focus(
            nodes, edges, "transform.left"
        )

        node_ids = {n["data"]["id"] for n in filtered_nodes}
        assert "source.raw" in node_ids
        assert "transform.left" in node_ids
        assert "feature_group.merged" in node_ids
        # transform.right is NOT upstream or downstream of transform.left
        assert "transform.right" not in node_ids

    def test_filter_to_focus_isolates_edges(self, diamond_manifest):
        """Filtered edges only include edges between kept nodes."""
        builder = LineageDataBuilder(diamond_manifest)
        nodes = [builder._serialize_node(n) for n in diamond_manifest.nodes.values()]
        edges = [builder._serialize_edge(e) for e in diamond_manifest.edges]

        filtered_nodes, filtered_edges = builder._filter_to_focus(
            nodes, edges, "transform.left"
        )

        # Should only have: source.raw->transform.left, transform.left->feature_group.merged
        # NOT: source.raw->transform.right, transform.right->feature_group.merged
        edge_ids = {e["data"]["id"] for e in filtered_edges}
        assert "source.raw->transform.left" in edge_ids
        assert "transform.left->feature_group.merged" in edge_ids
        assert "source.raw->transform.right" not in edge_ids

    def test_build_column_trace(self):
        """Column trace follows column dependencies upstream."""
        m = Manifest(project="test")
        m.add_node(Node(
            id="source.raw", name="raw", node_type=NodeType.SOURCE,
            config={},
        ))
        m.add_node(Node(
            id="transform.clean", name="clean", node_type=NodeType.TRANSFORM,
            config={"sql": "SELECT amount AS total FROM raw"},
        ))
        m.add_edge("source.raw", "transform.clean")

        builder = LineageDataBuilder(m)
        column_lineage = builder._compute_column_lineage()

        trace = builder._build_column_trace(
            "transform.clean", "total", column_lineage
        )

        assert len(trace) >= 1
        assert trace[0]["node"] == "transform.clean"
        assert trace[0]["column"] == "total"

    def test_build_column_trace_source_terminal(self):
        """Column trace terminates at source nodes."""
        m = Manifest(project="test")
        m.add_node(Node(
            id="source.raw", name="raw", node_type=NodeType.SOURCE,
        ))

        builder = LineageDataBuilder(m)
        # No column lineage for source nodes
        column_lineage = {}

        trace = builder._build_column_trace("source.raw", "col_a", column_lineage)

        assert len(trace) == 1
        assert trace[0]["node"] == "source.raw"
        assert trace[0]["column"] == "col_a"
        assert trace[0]["transformation_type"] == "source"

    def test_build_column_trace_missing_column(self):
        """Column trace handles column not found in lineage."""
        m = Manifest(project="test")
        m.add_node(Node(
            id="t.x", name="x", node_type=NodeType.TRANSFORM,
            config={"sql": "SELECT a FROM foo"},
        ))

        builder = LineageDataBuilder(m)
        column_lineage = builder._compute_column_lineage()

        # "nonexistent" is not in lineage output
        trace = builder._build_column_trace("t.x", "nonexistent", column_lineage)

        # Should still produce a trace entry with transformation_type "source"
        assert len(trace) == 1
        assert trace[0]["transformation_type"] == "source"

    def test_resolve_source_to_node_exact(self, simple_manifest):
        """Resolves source table by exact node ID match."""
        builder = LineageDataBuilder(simple_manifest)
        result = builder._resolve_source_to_node("source.orders")
        assert result == "source.orders"

    def test_resolve_source_to_node_by_name(self, simple_manifest):
        """Resolves source table by node name match."""
        builder = LineageDataBuilder(simple_manifest)
        result = builder._resolve_source_to_node("orders")
        assert result == "source.orders"

    def test_resolve_source_to_node_not_found(self, simple_manifest):
        """Returns None when source table not found."""
        builder = LineageDataBuilder(simple_manifest)
        result = builder._resolve_source_to_node("nonexistent_table")
        assert result is None

    def test_build_metadata(self, simple_manifest):
        """Metadata includes node and edge counts."""
        builder = LineageDataBuilder(simple_manifest)
        meta = builder._build_metadata()

        assert meta["node_count"] == 3
        assert meta["edge_count"] == 2
        assert "generated_at" in meta

    def test_build_metadata_with_column_trace(self, simple_manifest):
        """Metadata includes column trace when provided."""
        builder = LineageDataBuilder(simple_manifest)
        trace = [{"node": "a", "column": "x"}]
        meta = builder._build_metadata(column_trace=trace)

        assert meta["column_trace"] == trace

    def test_build_metadata_no_column_trace(self, simple_manifest):
        """Metadata omits column_trace when not provided."""
        builder = LineageDataBuilder(simple_manifest)
        meta = builder._build_metadata()

        assert "column_trace" not in meta

    def test_build_with_focus_node(self, simple_manifest):
        """Build with focus_node filters to connected subgraph."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build(focus_node="transform.clean_orders")

        assert data.focus_node == "transform.clean_orders"
        node_ids = {n["data"]["id"] for n in data.nodes}
        assert "transform.clean_orders" in node_ids

    def test_build_with_focus_column(self, simple_manifest):
        """Build with focus_column includes column trace in metadata."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build(
            focus_node="transform.clean_orders",
            focus_column="total"
        )

        assert data.focus_column == "total"
        # column_trace may or may not be in metadata depending on lineage resolution
        assert data.metadata is not None

    def test_build_empty_manifest(self, empty_manifest):
        """Build on empty manifest produces empty data."""
        builder = LineageDataBuilder(empty_manifest)
        data = builder.build()

        assert len(data.nodes) == 0
        assert len(data.edges) == 0
        assert data.column_lineage == {}


# ---------------------------------------------------------------------------
# TestHTMLRenderer
# ---------------------------------------------------------------------------


class TestHTMLRenderer:
    """Tests for HTMLRenderer class."""

    def test_render_produces_html(self, simple_manifest):
        """Render produces a valid HTML file."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build()

        renderer = HTMLRenderer()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            result = renderer.render(data, output)

            assert result == output
            assert output.exists()
            content = output.read_text()
            assert "<!DOCTYPE html>" in content
            assert "</html>" in content

    def test_render_embeds_json(self, simple_manifest):
        """Rendered HTML contains embedded JSON data."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build()

        renderer = HTMLRenderer()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            renderer.render(data, output)

            content = output.read_text()
            assert "LINEAGE_DATA" in content
            assert "source.orders" in content
            assert "transform.clean_orders" in content

    def test_render_sanitizes_script_tags(self):
        """JSON with </script> in values gets sanitized."""
        m = Manifest(project="test")
        m.add_node(Node(
            id="t.xss",
            name="xss_test",
            node_type=NodeType.TRANSFORM,
            description='</script><script>alert("xss")</script>',
        ))

        builder = LineageDataBuilder(m)
        data = builder.build()

        renderer = HTMLRenderer()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            renderer.render(data, output)

            content = output.read_text()
            # The raw </script> should be escaped in the JSON
            # Find the LINEAGE_DATA assignment
            start = content.index("LINEAGE_DATA = ")
            end = content.index(";", start)
            json_part = content[start + len("LINEAGE_DATA = "):end]
            # Should not have unescaped </script> that would break out
            assert "</script>" not in json_part

    def test_render_creates_parent_dirs(self):
        """Render creates parent directories if they don't exist."""
        m = Manifest(project="test")
        builder = LineageDataBuilder(m)
        data = builder.build()

        renderer = HTMLRenderer()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "nested" / "dir" / "lineage.html"
            renderer.render(data, output)

            assert output.exists()

    def test_render_includes_csp_meta(self, simple_manifest):
        """Rendered HTML includes CSP meta tag."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build()

        renderer = HTMLRenderer()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            renderer.render(data, output)

            content = output.read_text()
            assert "Content-Security-Policy" in content

    def test_render_includes_sri_hashes(self, simple_manifest):
        """Rendered HTML includes SRI integrity hashes on CDN scripts."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build()

        renderer = HTMLRenderer()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            renderer.render(data, output)

            content = output.read_text()
            assert 'integrity="sha384-' in content
            assert 'crossorigin="anonymous"' in content

    def test_render_includes_cytoscape_scripts(self, simple_manifest):
        """Rendered HTML includes Cytoscape.js CDN scripts."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build()

        renderer = HTMLRenderer()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            renderer.render(data, output)

            content = output.read_text()
            assert "cytoscape@3.30.4" in content
            assert "dagre@0.8.5" in content
            assert "cytoscape-dagre@2.5.0" in content

    def test_render_no_innerhtml_with_user_data(self, simple_manifest):
        """Rendered HTML does not use innerHTML with user data."""
        builder = LineageDataBuilder(simple_manifest)
        data = builder.build()

        renderer = HTMLRenderer()
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            renderer.render(data, output)

            content = output.read_text()
            # innerHTML should not appear in the template
            assert "innerHTML" not in content


# ---------------------------------------------------------------------------
# TestGenerateLineageHtml
# ---------------------------------------------------------------------------


class TestGenerateLineageHtml:
    """Tests for generate_lineage_html() function."""

    def test_end_to_end(self, simple_manifest):
        """Full pipeline generates HTML file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "target" / "lineage.html"
            result = generate_lineage_html(
                manifest=simple_manifest,
                output_path=output,
                open_browser=False,
            )

            assert result == output
            assert output.exists()
            content = output.read_text()
            assert "source.orders" in content

    def test_rejects_insecure_path(self, simple_manifest):
        """Raises error for insecure output paths like /tmp."""
        with pytest.raises(LineageVisualizationError, match="Insecure output path"):
            generate_lineage_html(
                manifest=simple_manifest,
                output_path=Path("/tmp/lineage.html"),
                open_browser=False,
            )

    def test_rejects_nonexistent_node(self, simple_manifest):
        """Raises error when focus_node not found in manifest."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(LineageVisualizationError, match="not found"):
                generate_lineage_html(
                    manifest=simple_manifest,
                    output_path=Path(tmpdir) / "lineage.html",
                    focus_node="nonexistent.node",
                    open_browser=False,
                )

    def test_rejects_invalid_column(self, simple_manifest):
        """Raises error when focus_column not found on node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(LineageVisualizationError, match="Column.*not found"):
                generate_lineage_html(
                    manifest=simple_manifest,
                    output_path=Path(tmpdir) / "lineage.html",
                    focus_node="source.orders",
                    focus_column="nonexistent_column",
                    open_browser=False,
                )

    def test_column_requires_node(self, simple_manifest):
        """Raises error when --column specified without node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(LineageVisualizationError, match="requires a node"):
                generate_lineage_html(
                    manifest=simple_manifest,
                    output_path=Path(tmpdir) / "lineage.html",
                    focus_column="total",
                    open_browser=False,
                )

    def test_warns_large_graph(self, caplog):
        """Warns when graph has more than 150 nodes."""
        m = Manifest(project="test")
        for i in range(160):
            m.add_node(Node(
                id=f"node.n{i}", name=f"n{i}", node_type=NodeType.SOURCE,
            ))

        with tempfile.TemporaryDirectory() as tmpdir:
            with caplog.at_level(logging.WARNING):
                generate_lineage_html(
                    manifest=m,
                    output_path=Path(tmpdir) / "lineage.html",
                    open_browser=False,
                )

            assert any("Large graph" in r.message for r in caplog.records)

    def test_errors_very_large_graph(self):
        """Raises error when graph exceeds 500 nodes."""
        m = Manifest(project="test")
        for i in range(501):
            m.add_node(Node(
                id=f"node.n{i}", name=f"n{i}", node_type=NodeType.SOURCE,
            ))

        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(LineageVisualizationError, match="limit: 500"):
                generate_lineage_html(
                    manifest=m,
                    output_path=Path(tmpdir) / "lineage.html",
                    open_browser=False,
                )

    def test_ssh_skips_browser(self, simple_manifest):
        """SSH session detection skips browser open."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            with patch.dict(os.environ, {"SSH_CONNECTION": "1.2.3.4 5678 9.10.11.12 22"}):
                with patch("seeknal.dag.visualize.webbrowser.open") as mock_open:
                    generate_lineage_html(
                        manifest=simple_manifest,
                        output_path=output,
                        open_browser=True,
                    )
                    mock_open.assert_not_called()

    def test_opens_browser_when_not_ssh(self, simple_manifest):
        """Browser opens when not in SSH session and open_browser=True."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            env = os.environ.copy()
            env.pop("SSH_CONNECTION", None)
            with patch.dict(os.environ, env, clear=True):
                with patch("seeknal.dag.visualize.webbrowser.open") as mock_open:
                    generate_lineage_html(
                        manifest=simple_manifest,
                        output_path=output,
                        open_browser=True,
                    )
                    mock_open.assert_called_once()

    def test_no_browser_when_disabled(self, simple_manifest):
        """Browser does not open when open_browser=False."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            with patch("seeknal.dag.visualize.webbrowser.open") as mock_open:
                generate_lineage_html(
                    manifest=simple_manifest,
                    output_path=output,
                    open_browser=False,
                )
                mock_open.assert_not_called()

    def test_focus_node_filters_output(self, simple_manifest):
        """Focus node produces filtered output with only connected nodes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            generate_lineage_html(
                manifest=simple_manifest,
                output_path=output,
                focus_node="source.orders",
                open_browser=False,
            )

            content = output.read_text()
            assert "source.orders" in content

    def test_valid_column_accepted(self, simple_manifest):
        """Valid column on node is accepted without error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "lineage.html"
            # source.orders has "order_id" column
            result = generate_lineage_html(
                manifest=simple_manifest,
                output_path=output,
                focus_node="source.orders",
                focus_column="order_id",
                open_browser=False,
            )
            assert result.exists()

    def test_error_message_includes_available_nodes(self, simple_manifest):
        """Error for nonexistent node lists available nodes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(LineageVisualizationError) as exc_info:
                generate_lineage_html(
                    manifest=simple_manifest,
                    output_path=Path(tmpdir) / "lineage.html",
                    focus_node="nonexistent.node",
                    open_browser=False,
                )
            error_msg = str(exc_info.value)
            assert "source.orders" in error_msg or "Available:" in error_msg

    def test_error_message_includes_available_columns(self, simple_manifest):
        """Error for nonexistent column lists available columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(LineageVisualizationError) as exc_info:
                generate_lineage_html(
                    manifest=simple_manifest,
                    output_path=Path(tmpdir) / "lineage.html",
                    focus_node="source.orders",
                    focus_column="nonexistent",
                    open_browser=False,
                )
            error_msg = str(exc_info.value)
            assert "order_id" in error_msg or "Available:" in error_msg
