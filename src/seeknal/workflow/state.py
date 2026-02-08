"""
State tracking for Seeknal workflow execution.

This module provides functionality for tracking the execution state of workflow nodes,
detecting changes, and determining which nodes need to be re-run based on dependencies.

Key features:
- Persistent state storage with atomic writes
- SHA256 hash-based change detection
- Metadata-aware hashing (excludes non-functional fields)
- Downstream propagation using BFS traversal
- Automatic backup before state overwrites
"""

import hashlib
import json
import shutil
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Set, Dict, List
import yaml


class NodeStatus(Enum):
    """Status of a node execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CACHED = "cached"


@dataclass
class NodeState:
    """
    Execution state for a single node.

    Tracks the hash, execution status, timing, and metadata for each node
    in the workflow DAG.

    Attributes:
        hash: SHA256 hash of the node's functional content (transform SQL, features, dependencies)
        last_run: ISO timestamp of last execution attempt
        status: Current execution status
        duration_ms: Execution duration in milliseconds
        row_count: Number of rows processed (if applicable)
        version: Node version (for schema changes)
        metadata: Additional node metadata (e.g., error messages, warnings)

        # Iceberg materialization fields
        iceberg_snapshot_id: Optional Iceberg snapshot ID from last materialization
        iceberg_snapshot_timestamp: Optional ISO timestamp of snapshot creation
        iceberg_table_ref: Optional fully qualified Iceberg table name
        iceberg_schema_version: Optional Iceberg schema version
    """
    hash: str
    last_run: str
    status: str
    duration_ms: int = 0
    row_count: int = 0
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Iceberg materialization fields
    iceberg_snapshot_id: Optional[str] = None
    iceberg_snapshot_timestamp: Optional[str] = None
    iceberg_table_ref: Optional[str] = None
    iceberg_schema_version: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeState":
        """Create from dictionary for JSON deserialization."""
        return cls(
            hash=data["hash"],
            last_run=data["last_run"],
            status=data["status"],
            duration_ms=data.get("duration_ms", 0),
            row_count=data.get("row_count", 0),
            version=data.get("version", 1),
            metadata=data.get("metadata", {}),
            # Iceberg fields (with backward compatibility)
            iceberg_snapshot_id=data.get("iceberg_snapshot_id"),
            iceberg_snapshot_timestamp=data.get("iceberg_snapshot_timestamp"),
            iceberg_table_ref=data.get("iceberg_table_ref"),
            iceberg_schema_version=data.get("iceberg_schema_version"),
        )

    def is_success(self) -> bool:
        """Check if node execution was successful."""
        return self.status == NodeStatus.SUCCESS.value

    def is_failed(self) -> bool:
        """Check if node execution failed."""
        return self.status == NodeStatus.FAILED.value

    def is_pending(self) -> bool:
        """Check if node is pending execution."""
        return self.status == NodeStatus.PENDING.value


@dataclass
class RunState:
    """
    Complete execution state for a workflow run.

    Contains metadata about the run and state for all nodes in the DAG.
    Persists to target/run_state.json.

    Attributes:
        version: State schema version
        seeknal_version: Seeknal CLI version
        last_run: ISO timestamp of last workflow run
        run_id: Unique identifier for this run
        config: Workflow configuration snapshot
        nodes: Mapping of node_id -> NodeState
    """
    version: str = "1.0.0"
    seeknal_version: str = "2.0.0"
    last_run: str = field(default_factory=lambda: datetime.now().isoformat())
    run_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))
    config: Dict[str, Any] = field(default_factory=dict)
    nodes: Dict[str, NodeState] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "version": self.version,
            "seeknal_version": self.seeknal_version,
            "last_run": self.last_run,
            "run_id": self.run_id,
            "config": self.config,
            "nodes": {
                node_id: node_state.to_dict()
                for node_id, node_state in self.nodes.items()
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RunState":
        """Create from dictionary for JSON deserialization."""
        nodes = {
            node_id: NodeState.from_dict(node_data)
            for node_id, node_data in data.get("nodes", {}).items()
        }
        return cls(
            version=data.get("version", "1.0.0"),
            seeknal_version=data.get("seeknal_version", "2.0.0"),
            last_run=data.get("last_run", datetime.now().isoformat()),
            run_id=data.get("run_id", datetime.now().strftime("%Y%m%d_%H%M%S")),
            config=data.get("config", {}),
            nodes=nodes,
        )

    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    @classmethod
    def from_json(cls, json_str: str) -> "RunState":
        """Create from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)


class StateError(Exception):
    """Base exception for state-related errors."""
    pass


class StateHashError(StateError):
    """Exception raised when hash calculation fails."""
    pass


class StatePersistenceError(StateError):
    """Exception raised when state persistence fails."""
    pass


def calculate_node_hash(yaml_data: Dict[str, Any], yaml_path: Path) -> str:
    """
    Calculate SHA256 hash of a node's functional content.

    Hash includes only functional fields that affect execution:
    - transform SQL
    - features (for feature_groups)
    - dependencies/inputs
    - table/source
    - aggregation config
    - rule definition

    Hash excludes metadata fields that don't affect execution:
    - description
    - owner
    - tags

    Also excludes comments and whitespace-only changes by parsing
    the YAML structure rather than hashing raw text.

    Args:
        yaml_data: Parsed YAML dictionary
        yaml_path: Path to YAML file (for context)

    Returns:
        SHA256 hash as hexadecimal string

    Raises:
        StateHashError: If hash calculation fails

    Examples:
        >>> yaml_data = {"kind": "transform", "name": "my_transform",
        ...              "transform": "SELECT * FROM source", "description": "My transform"}
        >>> hash_val = calculate_node_hash(yaml_data, Path("transform.yml"))
        >>> len(hash_val) == 64
        True
    """
    try:
        # Extract functional content based on node type
        kind = yaml_data.get("kind", "")

        # Build normalized content dict for hashing
        functional_content = {}

        # Common fields that affect execution
        if "inputs" in yaml_data:
            # Normalize inputs for consistent hashing
            inputs = yaml_data["inputs"]
            if isinstance(inputs, list):
                # Sort by ref for consistent ordering
                functional_content["inputs"] = sorted(
                    [inp.get("ref", "") for inp in inputs if isinstance(inp, dict)]
                )

        if "table" in yaml_data:
            functional_content["table"] = yaml_data["table"]

        if "source" in yaml_data:
            functional_content["source"] = yaml_data["source"]

        # Type-specific fields
        if kind == "transform":
            if "transform" in yaml_data:
                functional_content["transform"] = yaml_data["transform"]
            if "params" in yaml_data:
                functional_content["params"] = yaml_data["params"]

        elif kind == "feature_group":
            if "transform" in yaml_data:
                functional_content["transform"] = yaml_data["transform"]

            if "features" in yaml_data:
                # Normalize features dict (sort keys)
                features = yaml_data["features"]
                if isinstance(features, dict):
                    functional_content["features"] = dict(sorted(features.items()))

            if "entity" in yaml_data:
                functional_content["entity"] = yaml_data["entity"]

            if "materialization" in yaml_data:
                functional_content["materialization"] = yaml_data["materialization"]

        elif kind == "model":
            if "aggregation" in yaml_data:
                functional_content["aggregation"] = yaml_data["aggregation"]
            if "training" in yaml_data:
                functional_content["training"] = yaml_data["training"]
            if "output_columns" in yaml_data:
                functional_content["output_columns"] = yaml_data["output_columns"]

        elif kind == "aggregation":
            if "id_col" in yaml_data:
                functional_content["id_col"] = yaml_data["id_col"]
            if "feature_date_col" in yaml_data:
                functional_content["feature_date_col"] = yaml_data["feature_date_col"]
            if "application_date_col" in yaml_data:
                functional_content["application_date_col"] = yaml_data["application_date_col"]
            if "features" in yaml_data:
                functional_content["features"] = yaml_data["features"]

        elif kind == "rule":
            if "rule" in yaml_data:
                functional_content["rule"] = yaml_data["rule"]
            if "params" in yaml_data:
                functional_content["params"] = yaml_data["params"]

        elif kind == "source":
            if "params" in yaml_data:
                functional_content["params"] = yaml_data["params"]
            if "freshness" in yaml_data:
                functional_content["freshness"] = yaml_data["freshness"]

        # Convert to normalized JSON string with sorted keys
        # This ensures consistent hashing regardless of dict ordering
        content_str = json.dumps(functional_content, sort_keys=True)

        # Calculate SHA256 hash
        hash_obj = hashlib.sha256()
        hash_obj.update(content_str.encode("utf-8"))
        hash_hex = hash_obj.hexdigest()

        return hash_hex

    except Exception as e:
        raise StateHashError(f"Failed to calculate hash for {yaml_path}: {e}") from e


def save_state(state: RunState, target_path: Path) -> None:
    """
    Save state to file with atomic write and backup.

    Uses atomic write pattern (write to temp file, then rename) to prevent
    corruption if the process is interrupted. Creates backup of existing
    state before overwriting.

    Args:
        state: RunState to save
        target_path: Path to save state file (typically target/run_state.json)

    Raises:
        StatePersistenceError: If save operation fails

    Examples:
        >>> state = RunState()
        >>> save_state(state, Path("target/run_state.json"))
    """
    try:
        # Ensure parent directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Create backup if existing state exists
        if target_path.exists():
            backup_path = target_path.parent / f"{target_path.name}.bak.{state.run_id}"
            shutil.copy2(target_path, backup_path)

        # Write to temporary file first (atomic write)
        temp_path = target_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(state.to_json())

        # Atomic rename to final path
        temp_path.replace(target_path)

    except Exception as e:
        # Clean up temp file if it exists
        if temp_path.exists():
            try:
                temp_path.unlink()
            except Exception:
                pass

        raise StatePersistenceError(f"Failed to save state to {target_path}: {e}") from e


def load_state(state_path: Path) -> Optional[RunState]:
    """
    Load state from file.

    Args:
        state_path: Path to state file (typically target/run_state.json)

    Returns:
        RunState if file exists and is valid, None otherwise

    Raises:
        StatePersistenceError: If file exists but is invalid

    Examples:
        >>> state = load_state(Path("target/run_state.json"))
        >>> if state:
        ...     print(f"Last run: {state.last_run}")
    """
    try:
        if not state_path.exists():
            return None

        with open(state_path, "r", encoding="utf-8") as f:
            json_str = f.read()

        return RunState.from_json(json_str)

    except json.JSONDecodeError as e:
        raise StatePersistenceError(f"Invalid state file {state_path}: {e}") from e
    except Exception as e:
        raise StatePersistenceError(f"Failed to load state from {state_path}: {e}") from e


def detect_changes(
    current_hashes: Dict[str, str],
    stored_state: Optional[RunState],
) -> tuple[Set[str], Set[str]]:
    """
    Detect changed and new nodes by comparing hashes.

    Args:
        current_hashes: Mapping of node_id -> current hash
        stored_state: Previously stored RunState (can be None for first run)

    Returns:
        Tuple of (changed_node_ids, new_node_ids)
        - changed_node_ids: Nodes with different hashes than stored
        - new_node_ids: Nodes not present in stored state

    Examples:
        >>> current = {"transform.my_transform": "abc123", "source.users": "def456"}
        >>> stored = RunState()
        >>> stored.nodes["transform.my_transform"] = NodeState(
        ...     hash="oldhash", last_run="2024-01-01T00:00:00", status="success"
        ... )
        >>> changed, new = detect_changes(current, stored)
        >>> "transform.my_transform" in changed
        True
        >>> "source.users" in new
        True
    """
    changed_nodes: Set[str] = set()
    new_nodes: Set[str] = set()

    if not stored_state:
        # First run - all nodes are new
        new_nodes = set(current_hashes.keys())
        return changed_nodes, new_nodes

    stored_nodes = stored_state.nodes

    # Check for changed and new nodes
    for node_id, current_hash in current_hashes.items():
        if node_id not in stored_nodes:
            # Node is new
            new_nodes.add(node_id)
        else:
            stored_node = stored_nodes[node_id]
            if stored_node.hash != current_hash:
                # Node hash changed
                changed_nodes.add(node_id)

    return changed_nodes, new_nodes


def find_downstream_nodes(
    dag: Dict[str, Set[str]],
    changed_nodes: Set[str],
) -> Set[str]:
    """
    Find all downstream nodes that need re-running using BFS traversal.

    Given a DAG adjacency mapping and set of changed nodes, finds all
    downstream dependents that need to be re-run due to the changes.

    Args:
        dag: Adjacency mapping {node_id -> set of downstream node IDs}
        changed_nodes: Set of node IDs that have changed

    Returns:
        Complete set of node IDs to re-run (changed + downstream)

    Examples:
        >>> dag = {
        ...     "source.users": {"transform.clean_users"},
        ...     "transform.clean_users": {"feature_group.user_features"},
        ...     "feature_group.user_features": set(),
        ... }
        >>> changed = {"source.users"}
        >>> downstream = find_downstream_nodes(dag, changed)
        >>> downstream == {"source.users", "transform.clean_users", "feature_group.user_features"}
        True
    """
    to_rerun: Set[str] = set(changed_nodes)

    # BFS traversal from each changed node
    from collections import deque

    for start_node in changed_nodes:
        queue = deque([start_node])

        while queue:
            current = queue.popleft()

            # Get downstream nodes
            downstream = dag.get(current, set())

            for downstream_node in downstream:
                if downstream_node not in to_rerun:
                    to_rerun.add(downstream_node)
                    queue.append(downstream_node)

    return to_rerun


def include_upstream_sources(
    nodes_to_run: Set[str],
    upstream_map: Dict[str, Set[str]],
) -> Set[str]:
    """
    Include upstream source dependencies for transforms in the nodes to run.

    When a transform needs to run, its upstream source dependencies must also
    run to ensure DuckDB views/tables are available for the transform's SQL.
    This function adds all source dependencies for any transform in nodes_to_run.

    Args:
        nodes_to_run: Set of node IDs that need to run
        upstream_map: Upstream adjacency mapping {node_id -> set of upstream dependencies}

    Returns:
        Updated set of node IDs to run (including source dependencies)

    Examples:
        >>> nodes = {"transform.clean_users"}
        >>> upstream = {
        ...     "transform.clean_users": {"source.raw_users"},
        ...     "source.raw_users": set(),
        ... }
        >>> result = include_upstream_sources(nodes, upstream)
        >>> result == {"transform.clean_users", "source.raw_users"}
        True
    """
    additional_nodes: Set[str] = set()

    for node_id in nodes_to_run:
        # Check if this is a transform node
        if node_id.startswith("transform."):
            # Get all upstream dependencies
            upstream_deps = upstream_map.get(node_id, set())
            for dep in upstream_deps:
                # Recursively add source dependencies
                if dep.startswith("source."):
                    additional_nodes.add(dep)
                elif dep.startswith("transform."):
                    # For transform-to-transform dependencies, add their sources too
                    if dep in upstream_map:
                        for trans_dep in upstream_map[dep]:
                            if trans_dep.startswith("source."):
                                additional_nodes.add(trans_dep)

    return nodes_to_run | additional_nodes


def build_dag_adjacency(
    nodes: List[Any],
    get_node_id: callable,
    get_dependencies: callable,
) -> Dict[str, Set[str]]:
    """
    Build DAG adjacency mapping from node list.

    Args:
        nodes: List of node objects
        get_node_id: Function to extract node ID from node object
        get_dependencies: Function to extract list of dependency IDs from node object

    Returns:
        Adjacency mapping {node_id -> set of downstream node IDs}

    Examples:
        >>> from seeknal.dag.manifest import Node, NodeType
        >>> nodes = [
        ...     Node(id="source.users", name="users", node_type=NodeType.SOURCE),
        ...     Node(id="transform.clean", name="clean", node_type=NodeType.TRANSFORM),
        ... ]
        >>> # Assuming get_dependencies returns upstream refs
        >>> dag = build_dag_adjacency(nodes, lambda n: n.id, lambda n: [])
        >>> "source.users" in dag
        True
    """
    # Build dependency mapping: node -> upstream dependencies
    upstream_map: Dict[str, Set[str]] = {}

    for node in nodes:
        node_id = get_node_id(node)
        dependencies = get_dependencies(node)
        upstream_map[node_id] = set(dependencies)

    # Reverse to build downstream adjacency: node -> downstream dependents
    downstream_map: Dict[str, Set[str]] = {node_id: set() for node_id in upstream_map}

    for node_id, upstreams in upstream_map.items():
        for upstream in upstreams:
            if upstream in downstream_map:
                downstream_map[upstream].add(node_id)
            else:
                # Handle external dependencies
                downstream_map[upstream] = {node_id}

    return downstream_map


def create_node_state(
    yaml_data: Dict[str, Any],
    yaml_path: Path,
    status: str = NodeStatus.PENDING.value,
) -> NodeState:
    """
    Create a new NodeState from YAML data.

    Calculates the hash and initializes state for a node.

    Args:
        yaml_data: Parsed YAML dictionary
        yaml_path: Path to YAML file
        status: Initial status (default: pending)

    Returns:
        NodeState instance with calculated hash

    Examples:
        >>> yaml_data = {"kind": "transform", "name": "my_transform",
        ...              "transform": "SELECT * FROM source"}
        >>> state = create_node_state(yaml_data, Path("transform.yml"))
        >>> state.status == "pending"
        True
        >>> len(state.hash) == 64
        True
    """
    hash_val = calculate_node_hash(yaml_data, yaml_path)

    return NodeState(
        hash=hash_val,
        last_run=datetime.now().isoformat(),
        status=status,
        duration_ms=0,
        row_count=0,
        version=1,
        metadata={},
    )


def update_node_state(
    state: RunState,
    node_id: str,
    status: str,
    duration_ms: int = 0,
    row_count: int = 0,
    metadata: Optional[Dict[str, Any]] = None,
    hash: Optional[str] = None,
) -> None:
    """
    Update state for a node after execution.

    Args:
        state: RunState to update
        node_id: Node identifier
        status: New status
        duration_ms: Execution duration in milliseconds
        row_count: Number of rows processed
        metadata: Additional metadata to merge
        hash: Content hash for change detection (optional, preserves existing if not provided)

    Examples:
        >>> state = RunState()
        >>> update_node_state(state, "transform.my_transform",
        ...                  status="success", duration_ms=1500, row_count=1000)
        >>> state.nodes["transform.my_transform"].status
        'success'
    """
    if node_id not in state.nodes:
        # Create new node state
        state.nodes[node_id] = NodeState(
            hash=hash or "",
            last_run=datetime.now().isoformat(),
            status=status,
            duration_ms=duration_ms,
            row_count=row_count,
            metadata=metadata or {},
        )
    else:
        # Update existing node state
        node_state = state.nodes[node_id]
        node_state.status = status
        node_state.last_run = datetime.now().isoformat()
        node_state.duration_ms = duration_ms
        node_state.row_count = row_count
        # Only update hash if provided (preserve existing hash otherwise)
        if hash is not None:
            node_state.hash = hash

        if metadata:
            node_state.metadata.update(metadata)


def get_nodes_to_run(
    current_hashes: Dict[str, str],
    stored_state: Optional[RunState],
    dag: Dict[str, Set[str]],
) -> Set[str]:
    """
    Determine complete set of nodes that need to be executed.

    Combines change detection with downstream propagation to determine
    which nodes should be run in the current execution.

    Args:
        current_hashes: Mapping of node_id -> current hash
        stored_state: Previously stored RunState (can be None)
        dag: DAG adjacency mapping {node_id -> downstream nodes}

    Returns:
        Set of node IDs that should be executed

    Examples:
        >>> hashes = {"t1": "abc", "t2": "def", "fg1": "ghi"}
        >>> dag = {"t1": {"t2"}, "t2": {"fg1"}, "fg1": set()}
        >>> to_run = get_nodes_to_run(hashes, None, dag)
        >>> to_run == {"t1", "t2", "fg1"}
        True
    """
    # Detect changed and new nodes
    changed_nodes, new_nodes = detect_changes(current_hashes, stored_state)

    # Combine changed and new nodes
    affected_nodes = changed_nodes | new_nodes

    if not affected_nodes:
        # No changes - nothing to run
        return set()

    # Find all downstream nodes
    nodes_to_run = find_downstream_nodes(dag, affected_nodes)

    return nodes_to_run
