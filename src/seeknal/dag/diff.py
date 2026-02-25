"""
Manifest diff detection for Seeknal 2.0.

Compares two manifests to detect what has changed:
- Added nodes
- Removed nodes
- Modified nodes (config changes)
- Added edges
- Removed edges

This enables incremental rebuilds - only re-process what changed.

SQL-aware change detection uses semantic SQL comparison via sqlglot
to distinguish between breaking and non-breaking SQL changes.
"""
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from seeknal.dag.manifest import Manifest, Node

# Try to import SQL diff module for SQL-aware change detection
try:
    from seeknal.dag.sql_diff import SQLDiffer, SQLDialect, SQLDiffResult
    SQL_DIFF_AVAILABLE = True
except ImportError:
    SQL_DIFF_AVAILABLE = False


class DiffType(Enum):
    """Type of change detected."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"


class ChangeCategory(Enum):
    """Impact classification of a node change."""
    BREAKING = "breaking"           # Downstream rebuild required
    NON_BREAKING = "non_breaking"   # Only this node needs rebuild
    METADATA = "metadata"           # No execution needed


METADATA_FIELDS = {"description", "owner", "tags"}
BREAKING_CONFIG_KEYS = {"entity", "source_type"}
METADATA_CONFIG_KEYS = {"audits"}
SQL_CONFIG_KEYS = {"sql"}  # Config keys that contain SQL to diff semantically


def _severity(cat: ChangeCategory) -> int:
    """Get severity ordering for max() comparison."""
    return {
        ChangeCategory.METADATA: 0,
        ChangeCategory.NON_BREAKING: 1,
        ChangeCategory.BREAKING: 2,
    }[cat]


def _classify_column_change(old_node: Node, new_node: Node) -> ChangeCategory:
    """Classify column-level changes."""
    old_cols = old_node.columns or {}
    new_cols = new_node.columns or {}

    old_names = set(old_cols.keys())
    new_names = set(new_cols.keys())

    # Column removed = BREAKING
    if old_names - new_names:
        return ChangeCategory.BREAKING

    # Column type changes
    for col_name in old_names & new_names:
        old_type = str(old_cols[col_name]).upper() if old_cols[col_name] else ""
        new_type = str(new_cols[col_name]).upper() if new_cols[col_name] else ""
        if old_type != new_type:
            # Conservative: treat all type changes as BREAKING
            return ChangeCategory.BREAKING

    # Column added only = NON_BREAKING
    if new_names - old_names:
        return ChangeCategory.NON_BREAKING

    return ChangeCategory.METADATA


def _classify_sql_change(old_sql: Optional[str], new_sql: Optional[str]) -> ChangeCategory:
    """Classify SQL changes using semantic comparison.

    Uses SQL diff module to detect breaking vs non-breaking SQL changes:
    - Column/table removed: BREAKING
    - Column/table added: NON_BREAKING
    - Whitespace/formatting only: METADATA

    Args:
        old_sql: The old SQL string
        new_sql: The new SQL string

    Returns:
        ChangeCategory indicating the impact level
    """
    # Fast path: if SQL is identical, no change
    if old_sql == new_sql:
        return ChangeCategory.METADATA

    # Fast path: if one is None/empty and other isn't, treat as breaking
    if not old_sql and new_sql:
        return ChangeCategory.NON_BREAKING  # New SQL added
    if old_sql and not new_sql:
        return ChangeCategory.BREAKING  # SQL removed

    # If SQL diff module is available, use semantic comparison
    if SQL_DIFF_AVAILABLE:
        try:
            differ = SQLDiffer()
            diff_result = differ.diff(old_sql, new_sql)

            if not diff_result.has_changes:
                # Semantically identical (whitespace/comments only)
                return ChangeCategory.METADATA

            # Check if there are breaking changes
            if diff_result.is_breaking:
                return ChangeCategory.BREAKING

            # Has changes but none are breaking
            if diff_result.edits:
                return ChangeCategory.NON_BREAKING

            # No semantic changes detected
            return ChangeCategory.METADATA

        except Exception:
            # Fallback to string comparison if SQL diff fails
            pass

    # Fallback: string comparison (conservative)
    if old_sql != new_sql:
        # Any SQL change is treated as non-breaking by default
        # (column removal would be detected in column changes)
        return ChangeCategory.NON_BREAKING

    return ChangeCategory.METADATA


def _classify_config_change(old_node: Node, new_node: Node) -> ChangeCategory:
    """Classify config-level changes."""
    old_config = old_node.config or {}
    new_config = new_node.config or {}

    category = ChangeCategory.METADATA

    all_keys = set(old_config.keys()) | set(new_config.keys())
    changed_keys = {k for k in all_keys if old_config.get(k) != new_config.get(k)}

    for key in changed_keys:
        if key in METADATA_CONFIG_KEYS:
            pass  # stays METADATA
        elif key in SQL_CONFIG_KEYS:
            # SQL-aware change detection
            old_sql = old_config.get(key)
            new_sql = new_config.get(key)
            sql_cat = _classify_sql_change(old_sql, new_sql)
            category = max(category, sql_cat, key=_severity)
        elif key in BREAKING_CONFIG_KEYS:
            category = max(category, ChangeCategory.BREAKING, key=_severity)
        elif key == "features":
            old_features = (
                set(old_config.get("features", {}).keys())
                if isinstance(old_config.get("features"), dict)
                else set()
            )
            new_features = (
                set(new_config.get("features", {}).keys())
                if isinstance(new_config.get("features"), dict)
                else set()
            )
            if old_features - new_features:
                category = max(category, ChangeCategory.BREAKING, key=_severity)
            else:
                category = max(category, ChangeCategory.NON_BREAKING, key=_severity)
        elif key == "inputs":
            # inputs are lists of dicts (e.g. [{"ref": "source.X"}]) — convert to
            # frozensets of sorted items so they are hashable for set operations
            def _hashable(items: list) -> set:
                return {
                    frozenset(sorted(d.items())) if isinstance(d, dict) else d
                    for d in items
                }
            old_inputs = _hashable(old_config.get("inputs", []))
            new_inputs = _hashable(new_config.get("inputs", []))
            if old_inputs - new_inputs:  # input removed
                category = max(category, ChangeCategory.BREAKING, key=_severity)
            else:
                category = max(category, ChangeCategory.NON_BREAKING, key=_severity)
        else:
            # Default: NON_BREAKING for unknown keys
            category = max(category, ChangeCategory.NON_BREAKING, key=_severity)

    return category


@dataclass(slots=True)
class NodeChange:
    """Represents a change to a node."""
    node_id: str
    change_type: DiffType
    category: ChangeCategory = ChangeCategory.NON_BREAKING
    old_value: Any = None
    new_value: Any = None
    changed_fields: list[str] = field(default_factory=list)
    changed_details: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
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
    added_nodes: dict[str, Node] = field(default_factory=dict)
    removed_nodes: dict[str, Node] = field(default_factory=dict)
    modified_nodes: dict[str, NodeChange] = field(default_factory=dict)
    added_edges: list[EdgeChange] = field(default_factory=list)
    removed_edges: list[EdgeChange] = field(default_factory=list)

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
    def _compare_nodes(old: Node, new: Node) -> list[str]:
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

        for field_name in fields_to_compare:
            old_val = getattr(old, field_name, None)
            new_val = getattr(new, field_name, None)
            if old_val != new_val:
                changed.append(field_name)

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

    def to_dict(self) -> dict[str, Any]:
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

    def get_affected_nodes(self) -> set[str]:
        """Get all node IDs affected by changes (added, removed, or modified)."""
        affected: set[str] = set()
        affected.update(self.added_nodes.keys())
        affected.update(self.removed_nodes.keys())
        affected.update(self.modified_nodes.keys())
        return affected

    @staticmethod
    def classify_change(change: NodeChange) -> ChangeCategory:
        """Classify a node change by impact severity.

        Args:
            change: The node change to classify

        Returns:
            ChangeCategory indicating the impact level
        """
        if change.change_type != DiffType.MODIFIED:
            return ChangeCategory.NON_BREAKING  # ADDED/REMOVED are always executed

        category = ChangeCategory.METADATA  # start low, escalate

        for field_name in change.changed_fields:
            if field_name in METADATA_FIELDS:
                pass  # stays METADATA
            elif field_name == "columns":
                col_cat = _classify_column_change(change.old_value, change.new_value)
                category = max(category, col_cat, key=_severity)
            elif field_name == "config":
                cfg_cat = _classify_config_change(change.old_value, change.new_value)
                category = max(category, cfg_cat, key=_severity)
            elif field_name == "node_type":
                category = ChangeCategory.BREAKING
            elif field_name == "name":
                category = ChangeCategory.BREAKING
            else:
                category = max(category, ChangeCategory.NON_BREAKING, key=_severity)

        return category

    def _bfs_downstream(self, node_id: str, manifest: Manifest) -> set[str]:
        """Get all downstream nodes via BFS.

        Args:
            node_id: Starting node
            manifest: Manifest for dependency lookup

        Returns:
            Set of all downstream node IDs
        """
        downstream: set[str] = set()
        queue: deque[str] = deque([node_id])
        while queue:
            current = queue.popleft()
            for child in manifest.get_downstream_nodes(current):
                if child not in downstream:
                    downstream.add(child)
                    queue.append(child)
        return downstream

    def _build_change_details(self, change: NodeChange) -> dict[str, str]:
        """Build human-readable change details.

        Args:
            change: The node change to describe

        Returns:
            Dict mapping field names to human-readable descriptions
        """
        details: dict[str, str] = {}
        old_node = change.old_value
        new_node = change.new_value

        for field_name in change.changed_fields:
            if field_name == "columns":
                old_cols = set((old_node.columns or {}).keys())
                new_cols = set((new_node.columns or {}).keys())
                added = new_cols - old_cols
                removed = old_cols - new_cols
                parts = []
                if added:
                    parts.append(f"added: {', '.join(sorted(added))}")
                if removed:
                    parts.append(f"removed: {', '.join(sorted(removed))}")
                if not parts:
                    parts.append("type changed")
                details["columns"] = "; ".join(parts)
            elif field_name == "config":
                old_cfg = old_node.config or {}
                new_cfg = new_node.config or {}
                changed_keys = {
                    k for k in set(old_cfg.keys()) | set(new_cfg.keys())
                    if old_cfg.get(k) != new_cfg.get(k)
                }
                details["config"] = f"keys changed: {', '.join(sorted(changed_keys))}"
            elif field_name in ("description", "owner", "tags"):
                details[field_name] = "updated"
            else:
                details[field_name] = (
                    f"{getattr(old_node, field_name, '?')} -> "
                    f"{getattr(new_node, field_name, '?')}"
                )

        return details

    def get_nodes_to_rebuild(self, manifest: Manifest) -> dict[str, ChangeCategory]:
        """
        Get nodes that need rebuilding, with their change categories.

        BREAKING: this node + all downstream
        NON_BREAKING: this node + downstream
        METADATA: update manifest only, no execution

        Args:
            manifest: The new manifest to use for dependency analysis

        Returns:
            Dict mapping node IDs to their ChangeCategory
        """
        to_rebuild: dict[str, ChangeCategory] = {}

        for node_id, change in self.modified_nodes.items():
            change.category = self.classify_change(change)
            change.changed_details = self._build_change_details(change)

            if change.category == ChangeCategory.METADATA:
                to_rebuild[node_id] = ChangeCategory.METADATA
                continue

            to_rebuild[node_id] = change.category
            downstream = self._bfs_downstream(node_id, manifest)
            for downstream_id in downstream:
                if downstream_id not in to_rebuild:
                    to_rebuild[downstream_id] = ChangeCategory.NON_BREAKING

        # Added nodes always execute
        for node_id in self.added_nodes:
            to_rebuild[node_id] = ChangeCategory.NON_BREAKING

        return to_rebuild

    def format_plan_output(self, manifest: Manifest) -> str:
        """Format categorized changes for CLI display.

        Args:
            manifest: Manifest for downstream analysis

        Returns:
            Formatted string for CLI output
        """
        self.get_nodes_to_rebuild(manifest)
        lines: list[str] = []

        for node_id, change in self.modified_nodes.items():
            cat = change.category
            label = {
                "breaking": "[BREAKING - rebuilds downstream]",
                "non_breaking": "[CHANGED - rebuild this node]",
                "metadata": "[METADATA - no rebuild needed]",
            }[cat.value]
            lines.append(f"  {label} {node_id}")
            for detail_field, detail_msg in change.changed_details.items():
                lines.append(f"      {detail_field}: {detail_msg}")

            if cat == ChangeCategory.BREAKING:
                downstream = self._bfs_downstream(node_id, manifest)
                if downstream:
                    lines.append(f"      Downstream impact ({len(downstream)} nodes):")
                    for ds_id in list(downstream)[:5]:
                        lines.append(f"        -> REBUILD {ds_id}")
                    if len(downstream) > 5:
                        lines.append(f"        ... and {len(downstream) - 5} more")

        for node_id in self.added_nodes:
            lines.append(f"  [NEW] + {node_id}")
        for node_id in self.removed_nodes:
            lines.append(f"  [REMOVED] - {node_id}")

        # Summary counts
        cats = [c.category for c in self.modified_nodes.values()]
        breaking_count = sum(1 for c in cats if c == ChangeCategory.BREAKING)
        non_breaking_count = sum(1 for c in cats if c == ChangeCategory.NON_BREAKING)
        metadata_count = sum(1 for c in cats if c == ChangeCategory.METADATA)

        lines.append("")
        lines.append("Summary:")
        if breaking_count:
            lines.append(f"  {breaking_count} breaking change(s) — downstream nodes will rebuild")
        if non_breaking_count:
            lines.append(f"  {non_breaking_count} non-breaking change(s) — only these nodes rebuild")
        if metadata_count:
            lines.append(f"  {metadata_count} metadata-only change(s) — no rebuild needed")
        if self.added_nodes:
            lines.append(f"  {len(self.added_nodes)} new node(s)")
        if self.removed_nodes:
            lines.append(f"  {len(self.removed_nodes)} removed node(s)")
        return "\n".join(lines)
