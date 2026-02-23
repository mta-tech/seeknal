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


class NodeStatus(Enum):
    """Status of a node execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CACHED = "cached"


@dataclass
class NodeFingerprint:
    """
    Content-based fingerprint for a node.

    Used for smart change detection: if the fingerprint matches stored state,
    the node can be skipped. Changes propagate downstream via upstream_hash.

    Attributes:
        content_hash: SHA256 of normalized functional YAML content
        schema_hash: SHA256 of output column names + types (if available)
        upstream_hash: SHA256 of sorted upstream fingerprints
        config_hash: SHA256 of non-functional config (e.g. materialization settings)
    """
    content_hash: str = ""
    schema_hash: str = ""
    upstream_hash: str = ""
    config_hash: str = ""

    @property
    def combined(self) -> str:
        """Combined fingerprint hash for comparison."""
        combined_str = f"{self.content_hash}:{self.schema_hash}:{self.upstream_hash}:{self.config_hash}"
        return hashlib.sha256(combined_str.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "content_hash": self.content_hash,
            "schema_hash": self.schema_hash,
            "upstream_hash": self.upstream_hash,
            "config_hash": self.config_hash,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeFingerprint":
        if data is None:
            return cls()
        return cls(
            content_hash=data.get("content_hash", ""),
            schema_hash=data.get("schema_hash", ""),
            upstream_hash=data.get("upstream_hash", ""),
            config_hash=data.get("config_hash", ""),
        )


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

        # Interval tracking fields
        completed_intervals: List of (start, end) datetime tuples for completed intervals
        completed_partitions: List of partition identifiers that have been completed
        restatement_intervals: List of (start, end) datetime tuples for intervals that require restatement
    """
    hash: str
    last_run: str
    status: str
    duration_ms: int = 0
    row_count: int = 0
    version: int = 1
    metadata: Dict[str, Any] = field(default_factory=dict)
    fingerprint: Optional[NodeFingerprint] = None

    # Iceberg materialization fields
    iceberg_snapshot_id: Optional[str] = None
    iceberg_snapshot_timestamp: Optional[str] = None
    iceberg_table_ref: Optional[str] = None
    iceberg_schema_version: Optional[int] = None

    # Interval tracking fields
    completed_intervals: List[tuple[str, str]] = field(default_factory=list)
    completed_partitions: List[str] = field(default_factory=list)
    restatement_intervals: List[tuple[str, str]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        # Replace fingerprint with its dict form (asdict already handles this,
        # but we want None -> omitted for cleaner JSON)
        if self.fingerprint is None:
            d.pop("fingerprint", None)
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeState":
        """Create from dictionary for JSON deserialization."""
        fp_data = data.get("fingerprint")
        fingerprint = NodeFingerprint.from_dict(fp_data) if fp_data else None

        # Handle interval fields with backward compatibility
        completed_intervals = data.get("completed_intervals", [])
        # Convert list of lists to list of tuples for consistency
        if isinstance(completed_intervals, list):
            completed_intervals = [
                tuple(interval) if isinstance(interval, list) else interval
                for interval in completed_intervals
            ]

        completed_partitions = data.get("completed_partitions", [])

        restatement_intervals = data.get("restatement_intervals", [])
        if isinstance(restatement_intervals, list):
            restatement_intervals = [
                tuple(interval) if isinstance(interval, list) else interval
                for interval in restatement_intervals
            ]

        return cls(
            hash=data.get("hash", ""),
            last_run=data.get("last_run", ""),
            status=data.get("status", "pending"),
            duration_ms=data.get("duration_ms", 0),
            row_count=data.get("row_count", 0),
            version=data.get("version", 1),
            metadata=data.get("metadata", {}),
            fingerprint=fingerprint,
            # Iceberg fields (with backward compatibility)
            iceberg_snapshot_id=data.get("iceberg_snapshot_id"),
            iceberg_snapshot_timestamp=data.get("iceberg_snapshot_timestamp"),
            iceberg_table_ref=data.get("iceberg_table_ref"),
            iceberg_schema_version=data.get("iceberg_schema_version"),
            # Interval tracking fields (with backward compatibility)
            completed_intervals=completed_intervals,
            completed_partitions=completed_partitions,
            restatement_intervals=restatement_intervals,
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
        schema_version: State schema version for migration support
        seeknal_version: Seeknal CLI version
        last_run: ISO timestamp of last workflow run
        run_id: Unique identifier for this run
        config: Workflow configuration snapshot
        nodes: Mapping of node_id -> NodeState
    """
    schema_version: str = "2.0"
    seeknal_version: str = "2.0.0"
    last_run: str = field(default_factory=lambda: datetime.now().isoformat())
    run_id: str = field(default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"))
    config: Dict[str, Any] = field(default_factory=dict)
    nodes: Dict[str, NodeState] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "schema_version": self.schema_version,
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
        """Create from dictionary for JSON deserialization.

        Handles backward compatibility:
        - Old v1.0 format (runner.py style): plain dict {node_id: {status, duration, ...}}
        - Old v1.0 format (state.py style): {version: "1.0.0", nodes: {node_id: NodeState}}
        - New v2.0 format: {schema_version: "2.0", nodes: {node_id: NodeState}}
        """
        schema_version = data.get("schema_version", data.get("version", "1.0.0"))

        # Detect old runner.py format: no "nodes" key, values are dicts with "status"
        if "nodes" not in data and "schema_version" not in data and "version" not in data:
            # Old runner.py execution_state.json format: {node_id: {status, duration, ...}}
            nodes = {}
            for node_id, node_data in data.items():
                if isinstance(node_data, dict) and "status" in node_data:
                    nodes[node_id] = NodeState(
                        hash="",
                        last_run=str(node_data.get("last_run", "")),
                        status=node_data.get("status", "pending"),
                        duration_ms=int(node_data.get("duration", 0) * 1000),
                        row_count=node_data.get("row_count", 0),
                    )
            return cls(
                schema_version="2.0",
                nodes=nodes,
            )

        nodes = {
            node_id: NodeState.from_dict(node_data)
            for node_id, node_data in data.get("nodes", {}).items()
        }
        return cls(
            schema_version="2.0",
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
    Load state from file with automatic migration from old formats.

    Checks both the new unified path (target/run_state.json) and the
    old runner.py path (target/state/execution_state.json). If only the
    old path exists, migrates it to the new format.

    Args:
        state_path: Path to state file (typically target/run_state.json)

    Returns:
        RunState if file exists and is valid, None otherwise

    Raises:
        StatePersistenceError: If file exists but is invalid
    """
    try:
        if state_path.exists():
            with open(state_path, "r", encoding="utf-8") as f:
                json_str = f.read()
            return RunState.from_json(json_str)

        # Check for old runner.py state file and migrate
        old_state_path = state_path.parent / "state" / "execution_state.json"
        if old_state_path.exists():
            with open(old_state_path, "r", encoding="utf-8") as f:
                json_str = f.read()
            state = RunState.from_json(json_str)
            # Save in new format for next time
            save_state(state, state_path)
            return state

        return None

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


def compute_node_fingerprint(
    yaml_data: Dict[str, Any],
    yaml_path: Path,
    upstream_fingerprints: Optional[Dict[str, NodeFingerprint]] = None,
) -> NodeFingerprint:
    """
    Compute a NodeFingerprint for a node.

    Args:
        yaml_data: Parsed YAML data for the node
        yaml_path: Path to the YAML file
        upstream_fingerprints: Map of upstream node_id -> NodeFingerprint

    Returns:
        NodeFingerprint with all hash fields computed
    """
    # Content hash: reuse existing calculate_node_hash
    content_hash = calculate_node_hash(yaml_data, yaml_path)

    # Schema hash: from output columns if defined
    columns = yaml_data.get("columns", {})
    if columns:
        schema_str = json.dumps(dict(sorted(columns.items())), sort_keys=True)
    else:
        schema_str = ""
    schema_hash = hashlib.sha256(schema_str.encode("utf-8")).hexdigest()

    # Upstream hash: combined hash of sorted upstream fingerprints
    if upstream_fingerprints:
        upstream_parts = sorted(
            f"{nid}:{fp.combined}" for nid, fp in upstream_fingerprints.items()
        )
        upstream_str = "|".join(upstream_parts)
    else:
        upstream_str = ""
    upstream_hash = hashlib.sha256(upstream_str.encode("utf-8")).hexdigest()

    # Config hash: non-functional config like materialization, tags, owner
    config_fields = {}
    for key in ("materialization", "freshness", "schedule"):
        if key in yaml_data:
            config_fields[key] = yaml_data[key]
    config_str = json.dumps(config_fields, sort_keys=True) if config_fields else ""
    config_hash = hashlib.sha256(config_str.encode("utf-8")).hexdigest()

    return NodeFingerprint(
        content_hash=content_hash,
        schema_hash=schema_hash,
        upstream_hash=upstream_hash,
        config_hash=config_hash,
    )


def compute_dag_fingerprints(
    manifest_nodes: Dict[str, Any],
    dag_upstream: Dict[str, Set[str]],
) -> Dict[str, NodeFingerprint]:
    """
    Compute fingerprints for all nodes in topological order.

    Traverses the DAG bottom-up (sources first) so that upstream hashes
    are available when computing downstream fingerprints.

    Args:
        manifest_nodes: Map of node_id -> node data (with config, file_path, etc.)
        dag_upstream: Map of node_id -> set of upstream node IDs

    Returns:
        Map of node_id -> NodeFingerprint
    """
    from collections import deque

    # Build in-degree map for topological sort
    all_nodes = set(manifest_nodes.keys())
    in_degree: Dict[str, int] = {nid: 0 for nid in all_nodes}
    downstream: Dict[str, Set[str]] = {nid: set() for nid in all_nodes}

    for nid, upstreams in dag_upstream.items():
        for up in upstreams:
            if up in all_nodes:
                in_degree[nid] = in_degree.get(nid, 0) + 1
                downstream.setdefault(up, set()).add(nid)

    # Kahn's algorithm for topological order
    queue = deque([nid for nid, deg in in_degree.items() if deg == 0])
    topo_order: List[str] = []
    while queue:
        nid = queue.popleft()
        topo_order.append(nid)
        for child in downstream.get(nid, set()):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # Compute fingerprints in topological order
    fingerprints: Dict[str, NodeFingerprint] = {}
    for nid in topo_order:
        node_data = manifest_nodes[nid]
        yaml_data = node_data.get("config", {})
        yaml_data["kind"] = node_data.get("kind", "")
        yaml_path = Path(node_data.get("file_path", "unknown.yml"))

        # Gather upstream fingerprints
        upstream_fps = {}
        for up_id in dag_upstream.get(nid, set()):
            if up_id in fingerprints:
                upstream_fps[up_id] = fingerprints[up_id]

        fingerprints[nid] = compute_node_fingerprint(yaml_data, yaml_path, upstream_fps)

    return fingerprints
