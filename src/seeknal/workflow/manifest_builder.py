"""Public manifest builder — converts a DAGBuilder result into a Manifest.

Previously lived as the private ``_build_manifest_from_dag`` helper in
``cli/main.py``. Promoted so the heartbeat daemon (and any future caller)
can reuse it without importing CLI internals.
"""

from __future__ import annotations

from typing import Any


def build_manifest_from_dag(dag_builder: Any, project_name: str):
    """Build a Manifest from a built ``DAGBuilder`` instance.

    Args:
        dag_builder: A built DAGBuilder (with ``.nodes`` dict and
            ``.get_downstream()`` method).
        project_name: Project name for the manifest metadata.

    Returns:
        A ``seeknal.dag.manifest.Manifest`` populated from the DAGBuilder.
    """
    from seeknal.dag.manifest import Manifest, Node, NodeType as ManifestNodeType

    node_type_map = {
        "source": ManifestNodeType.SOURCE,
        "transform": ManifestNodeType.TRANSFORM,
        "feature_group": ManifestNodeType.FEATURE_GROUP,
        "model": ManifestNodeType.MODEL,
        "rule": ManifestNodeType.RULE,
        "aggregation": ManifestNodeType.AGGREGATION,
        "second_order_aggregation": ManifestNodeType.SECOND_ORDER_AGGREGATION,
        "exposure": ManifestNodeType.EXPOSURE,
        "python": ManifestNodeType.PYTHON,
        "semantic_model": ManifestNodeType.SEMANTIC_MODEL,
        "metric": ManifestNodeType.METRIC,
        "profile": ManifestNodeType.PROFILE,
    }

    manifest = Manifest(project=project_name)
    for node_id, node in dag_builder.nodes.items():
        kind_str = node.kind.value if hasattr(node.kind, "value") else str(node.kind)
        manifest_node_type = node_type_map.get(kind_str, ManifestNodeType.SOURCE)
        raw_columns: dict[str, Any] = {}
        if hasattr(node, "yaml_data"):
            raw_columns = node.yaml_data.get("columns", {}) or {}

        manifest.add_node(
            Node(
                id=node_id,
                name=node.name,
                node_type=manifest_node_type,
                description=node.yaml_data.get("description")
                if hasattr(node, "yaml_data")
                else None,
                tags=list(node.tags) if hasattr(node, "tags") and node.tags else [],
                columns=raw_columns,
                config=node.yaml_data if hasattr(node, "yaml_data") else {},
                file_path=node.file_path if hasattr(node, "file_path") else None,
            )
        )

    # Dedup edges: a node may depend on the same upstream twice (once via the
    # YAML `inputs:` list, once via a `ref()` in the SQL body), so
    # `get_downstream` can yield the same pair more than once. `Manifest.edges`
    # is a plain list with no dedup, and `DAGRunner._get_topological_order`
    # counts in-degree from raw edges but decrements from the deduped
    # adjacency set — duplicate edges would leave a node's in-degree above
    # zero forever and be misreported as a cycle.
    seen_edges: set[tuple[str, str]] = set()
    for node_id in dag_builder.nodes:
        for downstream_id in dag_builder.get_downstream(node_id):
            edge_key = (node_id, downstream_id)
            if edge_key in seen_edges:
                continue
            seen_edges.add(edge_key)
            manifest.add_edge(node_id, downstream_id)

    return manifest


__all__ = ["build_manifest_from_dag"]
