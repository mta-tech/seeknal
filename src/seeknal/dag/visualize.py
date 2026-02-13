"""
Lineage visualization module for Seeknal.

Converts a Manifest + column lineage into a self-contained HTML file
with embedded Cytoscape.js for interactive DAG visualization.
"""

import dataclasses
import json
import logging
import webbrowser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import jinja2

from seeknal.dag.lineage import LineageBuilder
from seeknal.dag.manifest import Manifest, Node, Edge
from seeknal.utils.path_security import is_insecure_path

logger = logging.getLogger(__name__)


class LineageVisualizationError(Exception):
    """Raised when lineage visualization fails."""


@dataclass(frozen=True)
class LineageData:
    """Immutable lineage data for the HTML template."""
    nodes: list[dict]
    edges: list[dict]
    column_lineage: dict
    metadata: dict
    focus_node: Optional[str] = None
    focus_column: Optional[str] = None


class LineageDataBuilder:
    """Converts Manifest to serializable LineageData."""

    def __init__(self, manifest: Manifest):
        self.manifest = manifest
        self._lineage_builder = LineageBuilder()

    def build(
        self,
        focus_node: Optional[str] = None,
        focus_column: Optional[str] = None,
    ) -> LineageData:
        """Build complete lineage data from manifest."""
        nodes = [self._serialize_node(n) for n in self.manifest.nodes.values()]
        edges = [self._serialize_edge(e) for e in self.manifest.edges]

        if focus_node:
            nodes, edges = self._filter_to_focus(nodes, edges, focus_node)

        column_lineage = self._compute_column_lineage(
            focused_node_ids={n["data"]["id"] for n in nodes} if focus_node else None
        )

        # Build column trace path if --column specified
        column_trace = None
        if focus_node and focus_column:
            column_trace = self._build_column_trace(
                focus_node, focus_column, column_lineage
            )

        return LineageData(
            nodes=nodes,
            edges=edges,
            column_lineage=column_lineage,
            metadata=self._build_metadata(column_trace=column_trace),
            focus_node=focus_node,
            focus_column=focus_column,
        )

    def _serialize_node(self, node: Node) -> dict:
        """Convert Node to Cytoscape.js element format."""
        return {
            "data": {
                "id": node.id,
                "label": node.name,
                "node_type": node.node_type.value,
                "description": node.description or "",
                "columns": node.columns or {},
                "file_path": node.file_path or "",
                "sql": node.config.get("sql", ""),
            }
        }

    def _serialize_edge(self, edge: Edge) -> dict:
        """Convert Edge to Cytoscape.js element format."""
        return {
            "data": {
                "id": f"{edge.from_node}->{edge.to_node}",
                "source": edge.from_node,
                "target": edge.to_node,
            }
        }

    def _compute_column_lineage(
        self, focused_node_ids: Optional[set[str]] = None
    ) -> dict:
        """Compute column lineage for SQL nodes.

        If focused_node_ids provided, only computes for those nodes
        (lazy computation for performance).
        """
        result = {}
        for node_id, node in self.manifest.nodes.items():
            if focused_node_ids and node_id not in focused_node_ids:
                continue
            sql = node.config.get("sql")
            if not sql:
                continue
            try:
                lineage = self._lineage_builder.build_lineage(sql)
                if lineage:
                    result[node_id] = {
                        "output_columns": lineage.output_columns,
                        "dependencies": [
                            {
                                "output": dep.output_column,
                                "inputs": sorted(dep.input_columns),
                                "transformation_type": dep.transformation_type,
                                "expression": dep.expression,
                                "source_table": dep.source_table,
                            }
                            for dep in lineage.dependencies
                        ],
                    }
            except Exception:
                logger.warning("Column lineage failed for %s", node_id)
        return result

    def _filter_to_focus(
        self, nodes: list[dict], edges: list[dict], focus_node: str
    ) -> tuple[list[dict], list[dict]]:
        """Filter to upstream + downstream of focus node via BFS."""
        forward = {}  # source -> [targets]
        reverse = {}  # target -> [sources]
        for edge in edges:
            src = edge["data"]["source"]
            tgt = edge["data"]["target"]
            forward.setdefault(src, []).append(tgt)
            reverse.setdefault(tgt, []).append(src)

        # BFS upstream
        upstream = set()
        queue = [focus_node]
        while queue:
            current = queue.pop(0)
            for parent in reverse.get(current, []):
                if parent not in upstream:
                    upstream.add(parent)
                    queue.append(parent)

        # BFS downstream
        downstream = set()
        queue = [focus_node]
        while queue:
            current = queue.pop(0)
            for child in forward.get(current, []):
                if child not in downstream:
                    downstream.add(child)
                    queue.append(child)

        keep_ids = upstream | downstream | {focus_node}
        filtered_nodes = [n for n in nodes if n["data"]["id"] in keep_ids]
        filtered_edges = [
            e for e in edges
            if e["data"]["source"] in keep_ids and e["data"]["target"] in keep_ids
        ]
        return filtered_nodes, filtered_edges

    def _build_column_trace(
        self, focus_node: str, focus_column: str, column_lineage: dict,
    ) -> list[dict]:
        """Build column trace path from focus through upstream."""
        trace = []
        current_node = focus_node
        current_column = focus_column
        visited = set()

        while current_node and current_node not in visited:
            visited.add(current_node)
            node_lineage = column_lineage.get(current_node, {})
            deps = node_lineage.get("dependencies", [])

            matching_dep = next(
                (d for d in deps if d["output"] == current_column), None
            )
            if matching_dep:
                trace.append({
                    "node": current_node,
                    "column": current_column,
                    "expression": matching_dep.get("expression", ""),
                    "transformation_type": matching_dep.get("transformation_type", ""),
                    "inputs": matching_dep.get("inputs", []),
                    "source_table": matching_dep.get("source_table", ""),
                })
                source_table = matching_dep.get("source_table")
                if source_table and matching_dep["inputs"]:
                    upstream_node = self._resolve_source_to_node(source_table)
                    if upstream_node:
                        current_node = upstream_node
                        current_column = matching_dep["inputs"][0]
                        continue
            else:
                trace.append({
                    "node": current_node,
                    "column": current_column,
                    "expression": "",
                    "transformation_type": "source",
                })
            break
        return trace

    def _resolve_source_to_node(self, source_table: str) -> Optional[str]:
        """Resolve SQL source table name to manifest node ID."""
        if source_table in self.manifest.nodes:
            return source_table
        for node_id, node in self.manifest.nodes.items():
            if node.name == source_table:
                return node_id
        return None

    def _build_metadata(self, column_trace: Optional[list] = None) -> dict:
        meta = {
            "node_count": len(self.manifest.nodes),
            "edge_count": len(self.manifest.edges),
            "generated_at": datetime.now().isoformat(),
        }
        if column_trace:
            meta["column_trace"] = column_trace
        return meta


class HTMLRenderer:
    """Renders LineageData to HTML via Jinja2 template."""

    TEMPLATE_DIR = Path(__file__).parent / "templates"

    def render(self, lineage_data: LineageData, output_path: Path) -> Path:
        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(self.TEMPLATE_DIR)),
            autoescape=True,
        )
        template = env.get_template("lineage.html.j2")

        # Sanitize JSON to prevent script injection
        json_str = json.dumps(dataclasses.asdict(lineage_data))
        json_str = json_str.replace("</", "<\\/")

        html_content = template.render(lineage_data_json=json_str)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(html_content)
        return output_path


def generate_lineage_html(
    manifest: Manifest,
    output_path: Path,
    focus_node: Optional[str] = None,
    focus_column: Optional[str] = None,
    open_browser: bool = True,
) -> Path:
    """Top-level function: manifest -> HTML file -> browser."""
    if is_insecure_path(str(output_path)):
        raise LineageVisualizationError(
            f"Insecure output path: '{output_path}'. "
            "Use a path within your project directory."
        )

    if focus_node and focus_node not in manifest.nodes:
        available = ", ".join(sorted(manifest.nodes.keys())[:10])
        raise LineageVisualizationError(
            f"Node '{focus_node}' not found. Available: {available}"
        )

    if focus_column:
        if not focus_node:
            raise LineageVisualizationError(
                "--column requires a node argument. "
                "Usage: seeknal lineage <node_id> --column <column>"
            )
        node = manifest.nodes[focus_node]
        if node.columns and focus_column not in node.columns:
            available = ", ".join(sorted(node.columns.keys())[:10])
            raise LineageVisualizationError(
                f"Column '{focus_column}' not found in '{focus_node}'. "
                f"Available: {available}"
            )

    if len(manifest.nodes) > 500:
        raise LineageVisualizationError(
            f"Graph has {len(manifest.nodes)} nodes (limit: 500). "
            "Use focused view: seeknal lineage <node_id>"
        )
    if len(manifest.nodes) > 150:
        logger.warning(
            "Large graph (%d nodes). Consider focused view.",
            len(manifest.nodes),
        )

    builder = LineageDataBuilder(manifest)
    data = builder.build(focus_node=focus_node, focus_column=focus_column)

    renderer = HTMLRenderer()
    result_path = renderer.render(data, output_path)

    if open_browser:
        import os
        if not os.environ.get("SSH_CONNECTION"):
            webbrowser.open(f"file://{result_path.resolve()}")
        else:
            logger.info("SSH session detected. Open %s in your browser.", result_path)

    return result_path
