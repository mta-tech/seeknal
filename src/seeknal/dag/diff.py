"""
Manifest diff detection for Seeknal 2.0.

Compares two manifests to detect what has changed:
- Added nodes
- Removed nodes
- Modified nodes (config changes)
- Added edges
- Removed edges

This enables incremental rebuilds - only re-process what changed.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Set, Tuple

from seeknal.dag.manifest import Manifest, Node, Edge


class DiffType(Enum):
    """Type of change detected."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


@dataclass
class NodeChange:
    """Represents a change to a node."""
    node_id: str
    change_type: DiffType
    old_value: Any = None
    new_value: Any = None
    changed_fields: List[str] = field(default_factory=list)


@dataclass
class EdgeChange:
    """Represents a change to an edge."""
    from_node: str
    to_node: str
    change_type: DiffType


@dataclass
class ManifestDiff:
    """
    Comparison result between two manifests.

    Contains lists of all changes detected between the old and new manifests.
    """
    added_nodes: Dict[str, Node] = field(default_factory=dict)
    removed_nodes: Dict[str, Node] = field(default_factory=dict)
    modified_nodes: Dict[str, NodeChange] = field(default_factory=dict)
    added_edges: List[EdgeChange] = field(default_factory=list)
    removed_edges: List[EdgeChange] = field(default_factory=list)

    @classmethod
    def compare(cls, old: Manifest, new: Manifest) -> "ManifestDiff":
        """
        Compare two manifests and return the diff.

        Args:
            old: The previous manifest (baseline)
            new: The new manifest to compare against

        Returns:
            ManifestDiff containing all detected changes
        """
        diff = cls()

        old_node_ids = set(old.nodes.keys())
        new_node_ids = set(new.nodes.keys())

        # Find added nodes
        for node_id in new_node_ids - old_node_ids:
            diff.added_nodes[node_id] = new.nodes[node_id]

        # Find removed nodes
        for node_id in old_node_ids - new_node_ids:
            diff.removed_nodes[node_id] = old.nodes[node_id]

        # Find modified nodes (exist in both, but changed)
        for node_id in old_node_ids & new_node_ids:
            old_node = old.nodes[node_id]
            new_node = new.nodes[node_id]

            changed_fields = cls._compare_nodes(old_node, new_node)
            if changed_fields:
                diff.modified_nodes[node_id] = NodeChange(
                    node_id=node_id,
                    change_type=DiffType.MODIFIED,
                    old_value=old_node,
                    new_value=new_node,
                    changed_fields=changed_fields
                )

        # Compare edges
        old_edges = {(e.from_node, e.to_node) for e in old.edges}
        new_edges = {(e.from_node, e.to_node) for e in new.edges}

        # Find added edges
        for from_node, to_node in new_edges - old_edges:
            diff.added_edges.append(EdgeChange(
                from_node=from_node,
                to_node=to_node,
                change_type=DiffType.ADDED
            ))

        # Find removed edges
        for from_node, to_node in old_edges - new_edges:
            diff.removed_edges.append(EdgeChange(
                from_node=from_node,
                to_node=to_node,
                change_type=DiffType.REMOVED
            ))

        return diff

    @staticmethod
    def _compare_nodes(old: Node, new: Node) -> List[str]:
        """
        Compare two nodes and return list of changed fields.

        Args:
            old: Old node
            new: New node

        Returns:
            List of field names that changed
        """
        changed = []

        # Compare basic fields
        fields_to_compare = [
            "name", "node_type", "description", "owner", "tags", "config"
        ]

        for field in fields_to_compare:
            old_val = getattr(old, field, None)
            new_val = getattr(new, field, None)
            if old_val != new_val:
                changed.append(field)

        # Compare columns separately (dict comparison)
        if old.columns != new.columns:
            changed.append("columns")

        return changed

    def has_changes(self) -> bool:
        """Check if there are any changes."""
        return bool(
            self.added_nodes or
            self.removed_nodes or
            self.modified_nodes or
            self.added_edges or
            self.removed_edges
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "added_nodes": list(self.added_nodes.keys()),
            "removed_nodes": list(self.removed_nodes.keys()),
            "modified_nodes": list(self.modified_nodes.keys()),
            "added_edges": [
                {"from": e.from_node, "to": e.to_node}
                for e in self.added_edges
            ],
            "removed_edges": [
                {"from": e.from_node, "to": e.to_node}
                for e in self.removed_edges
            ],
        }

    def summary(self) -> str:
        """Generate a human-readable summary of changes."""
        parts = []

        if self.added_nodes:
            parts.append(f"{len(self.added_nodes)} node(s) added")
        if self.removed_nodes:
            parts.append(f"{len(self.removed_nodes)} node(s) removed")
        if self.modified_nodes:
            parts.append(f"{len(self.modified_nodes)} node(s) modified")
        if self.added_edges:
            parts.append(f"{len(self.added_edges)} edge(s) added")
        if self.removed_edges:
            parts.append(f"{len(self.removed_edges)} edge(s) removed")

        if not parts:
            return "No changes detected"

        return ", ".join(parts)

    def get_affected_nodes(self) -> Set[str]:
        """Get all node IDs affected by changes (added, removed, or modified)."""
        affected = set()
        affected.update(self.added_nodes.keys())
        affected.update(self.removed_nodes.keys())
        affected.update(self.modified_nodes.keys())
        return affected

    def get_nodes_to_rebuild(self, manifest: Manifest) -> Set[str]:
        """
        Get nodes that need to be rebuilt based on changes.

        This includes directly changed nodes and their downstream dependents.

        Args:
            manifest: The new manifest to use for dependency analysis

        Returns:
            Set of node IDs that need rebuilding
        """
        to_rebuild = set()

        # Start with directly affected nodes
        affected = self.get_affected_nodes()
        to_rebuild.update(affected)

        # Add downstream dependents
        for node_id in affected:
            self._add_downstream_recursive(node_id, manifest, to_rebuild)

        return to_rebuild

    def _add_downstream_recursive(
        self,
        node_id: str,
        manifest: Manifest,
        to_rebuild: Set[str]
    ) -> None:
        """Recursively add downstream nodes to the rebuild set."""
        downstream = manifest.get_downstream_nodes(node_id)
        for downstream_id in downstream:
            if downstream_id not in to_rebuild:
                to_rebuild.add(downstream_id)
                self._add_downstream_recursive(downstream_id, manifest, to_rebuild)
