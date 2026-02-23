"""
Diff engine for Seeknal workflow.

Resolves baselines and generates unified diffs for pipeline YAML files.
Supports git-native diffing with applied_state.json and manifest fallbacks.
"""

import difflib
import hashlib
import json
import re
import subprocess
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional

import yaml

from seeknal.dag.diff import ChangeCategory
from seeknal.dag.manifest import Manifest


# Valid node identifier pattern: type/name (e.g., sources/orders)
NODE_ID_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*/[a-zA-Z_][a-zA-Z0-9_]*$")

# Node type directory mapping
NODE_TYPE_DIRS = {
    "sources": "source",
    "transforms": "transform",
    "feature_groups": "feature_group",
    "models": "model",
    "rules": "rule",
    "aggregations": "aggregation",
    "exposures": "exposure",
    "semantic_models": "semantic_model",
    "metrics": "metric",
}

APPLIED_STATE_SCHEMA_VERSION = "1.0"


@dataclass
class NodeDiff:
    """Diff result for a single pipeline node."""
    node_id: str
    file_path: str
    status: Literal["modified", "new", "deleted", "unchanged"]
    baseline_source: Literal["applied_state", "git", "manifest", "none"]
    unified_diff: Optional[str] = None
    category: Optional[ChangeCategory] = None
    downstream_count: int = 0
    insertions: int = 0
    deletions: int = 0


def _sanitize_node_identifier(identifier: str) -> str:
    """Sanitize node identifier to prevent path traversal.

    Args:
        identifier: Raw user input (e.g., 'sources/orders')

    Returns:
        Sanitized identifier

    Raises:
        ValueError: If identifier contains path traversal or invalid characters
    """
    # Reject path traversal sequences
    if ".." in identifier or "\\" in identifier:
        raise ValueError(
            f"Invalid node identifier '{identifier}': "
            "path traversal sequences are not allowed"
        )

    # Normalize and validate format
    identifier = identifier.strip("/")
    if not NODE_ID_PATTERN.match(identifier):
        raise ValueError(
            f"Invalid node identifier '{identifier}': "
            "expected format 'type/name' (e.g., 'sources/orders')"
        )

    return identifier


def load_applied_state(project_path: Path) -> dict:
    """Load applied_state.json from target directory.

    Args:
        project_path: Root project directory

    Returns:
        Parsed state dict, or empty dict if file doesn't exist
    """
    state_path = project_path / "target" / "applied_state.json"
    if not state_path.exists():
        return {}

    try:
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)

        # Validate schema version
        version = state.get("schema_version", "")
        if version != APPLIED_STATE_SCHEMA_VERSION:
            return {}

        return state
    except (json.JSONDecodeError, OSError):
        return {}


def save_applied_state(project_path: Path, state: dict) -> None:
    """Atomically save applied_state.json.

    Args:
        project_path: Root project directory
        state: State dict to write
    """
    target_dir = project_path / "target"
    target_dir.mkdir(parents=True, exist_ok=True)

    state_path = target_dir / "applied_state.json"
    tmp_path = state_path.with_suffix(".tmp")

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    tmp_path.rename(state_path)


def write_applied_state_entry(
    project_path: Path, node_id: str, file_path: str, yaml_content: str
) -> None:
    """Write or update a single node entry in applied_state.json.

    Args:
        project_path: Root project directory
        node_id: Node identifier (e.g., 'source.orders')
        file_path: Relative path to the YAML file
        yaml_content: Raw YAML content string
    """
    state = load_applied_state(project_path)
    if not state:
        state = {"schema_version": APPLIED_STATE_SCHEMA_VERSION, "nodes": {}}

    state["nodes"][node_id] = {
        "file_path": file_path,
        "content_hash": hashlib.sha256(yaml_content.encode()).hexdigest(),
        "yaml_content": yaml_content,
        "applied_at": datetime.now().isoformat(),
    }

    save_applied_state(project_path, state)


class DiffEngine:
    """Resolve baselines and generate diffs for pipeline YAML files."""

    def __init__(self, project_path: Path):
        self.project_path = project_path.resolve()
        self._applied_state: Optional[dict] = None
        self._manifest: Optional[Manifest] = None

    @property
    def applied_state(self) -> dict:
        if self._applied_state is None:
            self._applied_state = load_applied_state(self.project_path)
        return self._applied_state

    @property
    def manifest(self) -> Optional[Manifest]:
        if self._manifest is None:
            manifest_path = self.project_path / "target" / "manifest.json"
            if manifest_path.exists():
                try:
                    self._manifest = Manifest.load(str(manifest_path))
                except Exception:
                    self._manifest = None
        return self._manifest

    def diff_all(self, node_type: Optional[str] = None) -> list[NodeDiff]:
        """Diff all pipeline YAML files, optionally filtered by type.

        Args:
            node_type: Filter by node type directory (e.g., 'sources', 'transforms')

        Returns:
            List of NodeDiff results for each changed file
        """
        seeknal_dir = self.project_path / "seeknal"
        if not seeknal_dir.exists():
            return []

        results: list[NodeDiff] = []

        # Determine which type directories to scan
        if node_type:
            type_dirs = [node_type]
        else:
            type_dirs = list(NODE_TYPE_DIRS.keys())

        # Track which node IDs we found on disk
        found_on_disk: set[str] = set()

        for type_dir in type_dirs:
            dir_path = seeknal_dir / type_dir
            if not dir_path.is_dir():
                continue

            manifest_type = NODE_TYPE_DIRS.get(type_dir, type_dir)

            for yml_file in sorted(dir_path.glob("*.yml")):
                name = yml_file.stem
                node_id = f"{manifest_type}.{name}"
                found_on_disk.add(node_id)
                rel_path = str(yml_file.relative_to(self.project_path))

                diff = self._diff_file(node_id, rel_path, yml_file)
                if diff.status != "unchanged":
                    results.append(diff)

        # Check for deleted nodes (in applied_state but not on disk)
        nodes = self.applied_state.get("nodes", {})
        for node_id, entry in nodes.items():
            if node_id not in found_on_disk:
                # Check type filter
                if node_type:
                    manifest_type = NODE_TYPE_DIRS.get(node_type, node_type)
                    if not node_id.startswith(f"{manifest_type}."):
                        continue

                results.append(NodeDiff(
                    node_id=node_id,
                    file_path=entry.get("file_path", ""),
                    status="deleted",
                    baseline_source="applied_state",
                ))

        return results

    def diff_node(self, node_identifier: str) -> NodeDiff:
        """Diff a specific node by identifier.

        Args:
            node_identifier: Node path like 'sources/orders'

        Returns:
            NodeDiff result

        Raises:
            ValueError: If identifier is invalid or node not found
        """
        identifier = _sanitize_node_identifier(node_identifier)
        type_dir, name = identifier.split("/")

        manifest_type = NODE_TYPE_DIRS.get(type_dir)
        if manifest_type is None:
            raise ValueError(
                f"Unknown node type '{type_dir}'. "
                f"Valid types: {', '.join(sorted(NODE_TYPE_DIRS.keys()))}"
            )

        file_path = self.project_path / "seeknal" / type_dir / f"{name}.yml"
        node_id = f"{manifest_type}.{name}"
        rel_path = f"seeknal/{type_dir}/{name}.yml"

        if not file_path.exists():
            # Check if it was deleted
            nodes = self.applied_state.get("nodes", {})
            if node_id in nodes:
                return NodeDiff(
                    node_id=node_id,
                    file_path=rel_path,
                    status="deleted",
                    baseline_source="applied_state",
                )
            raise ValueError(f"Node file not found: {rel_path}")

        return self._diff_file(node_id, rel_path, file_path)

    def _diff_file(self, node_id: str, rel_path: str, file_path: Path) -> NodeDiff:
        """Generate diff for a single file against its baseline.

        Args:
            node_id: Manifest node ID (e.g., 'source.orders')
            rel_path: Relative path from project root
            file_path: Absolute path to the file

        Returns:
            NodeDiff with diff details
        """
        try:
            current_content = file_path.read_text(encoding="utf-8")
        except OSError:
            return NodeDiff(
                node_id=node_id,
                file_path=rel_path,
                status="new",
                baseline_source="none",
            )

        # Resolve baseline
        baseline_content, baseline_source = self._resolve_baseline(node_id, rel_path)

        if baseline_content is None:
            # No baseline - this is a new file
            return NodeDiff(
                node_id=node_id,
                file_path=rel_path,
                status="new",
                baseline_source="none",
            )

        # Check if content is identical
        if baseline_content.rstrip() == current_content.rstrip():
            return NodeDiff(
                node_id=node_id,
                file_path=rel_path,
                status="unchanged",
                baseline_source=baseline_source,
            )

        # Generate unified diff
        unified = self._generate_unified_diff(
            baseline_content,
            current_content,
            f"applied ({baseline_source})",
            f"current ({rel_path})",
        )

        # Count insertions/deletions
        insertions = 0
        deletions = 0
        for line in unified.splitlines():
            if line.startswith("+") and not line.startswith("+++"):
                insertions += 1
            elif line.startswith("-") and not line.startswith("---"):
                deletions += 1

        # Classify change
        category = self._classify_change(baseline_content, current_content)

        # Count downstream
        downstream_count = self._count_downstream(node_id)

        return NodeDiff(
            node_id=node_id,
            file_path=rel_path,
            status="modified",
            baseline_source=baseline_source,
            unified_diff=unified,
            category=category,
            downstream_count=downstream_count,
            insertions=insertions,
            deletions=deletions,
        )

    BaselineSource = Literal["applied_state", "git", "manifest", "none"]

    def _resolve_baseline(
        self, node_id: str, rel_path: str
    ) -> tuple[Optional[str], "DiffEngine.BaselineSource"]:
        """Resolve baseline content using priority chain.

        Priority: applied_state → git → manifest

        Args:
            node_id: Manifest node ID
            rel_path: Relative path from project root

        Returns:
            Tuple of (baseline_content, source_name)
        """
        # Priority 1: applied_state.json
        content = self._get_applied_state_baseline(node_id)
        if content is not None:
            return content, "applied_state"

        # Priority 2: git committed version
        content = self._get_git_baseline(rel_path)
        if content is not None:
            return content, "git"

        # Priority 3: manifest reconstruction (lossy)
        content = self._get_manifest_baseline(node_id)
        if content is not None:
            return content, "manifest"

        return None, "none"

    def _get_applied_state_baseline(self, node_id: str) -> Optional[str]:
        """Get baseline from applied_state.json."""
        nodes = self.applied_state.get("nodes", {})
        entry = nodes.get(node_id)
        if entry and "yaml_content" in entry:
            return entry["yaml_content"]
        return None

    def _get_git_baseline(self, rel_path: str) -> Optional[str]:
        """Get committed version via git show HEAD:<path>."""
        # Prefix with ./ to prevent flag injection
        safe_path = f"./{rel_path}" if not rel_path.startswith("./") else rel_path

        try:
            result = subprocess.run(
                ["git", "show", f"HEAD:{safe_path}"],
                capture_output=True,
                text=True,
                timeout=5,
                cwd=str(self.project_path),
            )
            if result.returncode == 0:
                return result.stdout
            return None
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
            return None

    def _get_manifest_baseline(self, node_id: str) -> Optional[str]:
        """Reconstruct YAML from manifest.json node config (lossy).

        This is a best-effort reconstruction. The manifest stores
        parsed node data, not the original YAML, so comments and
        formatting are lost.
        """
        manifest = self.manifest
        if manifest is None:
            return None

        node = manifest.get_node(node_id)
        if node is None:
            return None

        # Reconstruct a basic YAML structure from manifest node
        reconstructed: dict = {}
        reconstructed["kind"] = node.node_type.value
        reconstructed["name"] = node.name

        if node.description:
            reconstructed["description"] = node.description
        if node.owner:
            reconstructed["owner"] = node.owner
        if node.tags:
            reconstructed["tags"] = node.tags
        if node.columns:
            reconstructed["columns"] = dict(node.columns)

        # Merge config fields
        if node.config:
            for key, value in node.config.items():
                if key not in reconstructed:
                    reconstructed[key] = value

        try:
            return yaml.dump(reconstructed, default_flow_style=False, sort_keys=False)
        except yaml.YAMLError:
            return None

    def _generate_unified_diff(
        self, old: str, new: str, old_label: str, new_label: str
    ) -> str:
        """Generate unified diff using difflib."""
        old_lines = old.splitlines(keepends=True)
        new_lines = new.splitlines(keepends=True)

        diff_lines = difflib.unified_diff(
            old_lines,
            new_lines,
            fromfile=old_label,
            tofile=new_label,
            lineterm="",
        )

        return "\n".join(line.rstrip() for line in diff_lines)

    def _classify_change(
        self, old_content: str, new_content: str
    ) -> Optional[ChangeCategory]:
        """Classify change category by parsing both YAML versions."""
        try:
            old_data = yaml.safe_load(old_content)
            new_data = yaml.safe_load(new_content)
        except yaml.YAMLError:
            return ChangeCategory.NON_BREAKING

        if not isinstance(old_data, dict) or not isinstance(new_data, dict):
            return ChangeCategory.NON_BREAKING

        # Check columns
        old_cols = old_data.get("columns", {})
        new_cols = new_data.get("columns", {})
        if isinstance(old_cols, dict) and isinstance(new_cols, dict):
            old_names = set(old_cols.keys())
            new_names = set(new_cols.keys())

            # Column removed = BREAKING
            if old_names - new_names:
                return ChangeCategory.BREAKING

            # Column type changed = BREAKING
            for col in old_names & new_names:
                if str(old_cols.get(col, "")).upper() != str(new_cols.get(col, "")).upper():
                    return ChangeCategory.BREAKING

            # Column added = NON_BREAKING
            if new_names - old_names:
                return ChangeCategory.NON_BREAKING

        # Check entity changes (BREAKING)
        if old_data.get("entity") != new_data.get("entity"):
            return ChangeCategory.BREAKING

        # Check SQL/transform changes
        old_sql = old_data.get("transform", old_data.get("sql", ""))
        new_sql = new_data.get("transform", new_data.get("sql", ""))
        if old_sql != new_sql:
            return ChangeCategory.NON_BREAKING

        # Check inputs/dependencies
        if old_data.get("inputs") != new_data.get("inputs"):
            return ChangeCategory.NON_BREAKING

        # Check features
        if old_data.get("features") != new_data.get("features"):
            return ChangeCategory.NON_BREAKING

        # Metadata-only fields
        metadata_fields = {"description", "owner", "tags"}
        changed_fields = {
            k for k in set(old_data.keys()) | set(new_data.keys())
            if old_data.get(k) != new_data.get(k)
        }

        if changed_fields and changed_fields <= metadata_fields:
            return ChangeCategory.METADATA

        if changed_fields:
            return ChangeCategory.NON_BREAKING

        return ChangeCategory.METADATA

    def _count_downstream(self, node_id: str) -> int:
        """Count downstream nodes using manifest edges."""
        manifest = self.manifest
        if manifest is None:
            return 0

        if node_id not in manifest.nodes:
            return 0

        # BFS to count all downstream
        downstream: set[str] = set()
        queue: deque[str] = deque([node_id])
        while queue:
            current = queue.popleft()
            for child in manifest.get_downstream_nodes(current):
                if child not in downstream:
                    downstream.add(child)
                    queue.append(child)

        return len(downstream)

    def get_downstream_nodes(self, node_id: str) -> list[str]:
        """Get list of downstream node IDs."""
        manifest = self.manifest
        if manifest is None:
            return []

        if node_id not in manifest.nodes:
            return []

        downstream: list[str] = []
        visited: set[str] = set()
        queue: deque[str] = deque([node_id])
        while queue:
            current = queue.popleft()
            for child in manifest.get_downstream_nodes(current):
                if child not in visited:
                    visited.add(child)
                    downstream.append(child)
                    queue.append(child)

        return downstream
