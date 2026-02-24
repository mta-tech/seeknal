"""
Main orchestrator for DAG execution.

This module provides the core workflow runner that:
- Initializes DAG, state, and executors
- Determines nodes to run (changed + downstream or --full)
- Executes in topological order
- Handles errors (--continue-on-error, --retry)
- Reports progress and collects results
"""

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from seeknal.cli.main import _echo_success, _echo_error, _echo_info, _echo_warning
from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.dag.diff import ManifestDiff
from seeknal.workflow.state import (
    RunState, NodeStatus, NodeFingerprint,
    load_state, save_state, update_node_state,
    compute_node_fingerprint, compute_dag_fingerprints,
    calculate_node_hash, find_downstream_nodes,
)

# TYPE_CHECKING import to avoid circular deps at runtime
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from seeknal.workflow.executors.base import ExecutionContext


class ExecutionStatus(Enum):
    """Status of a node execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CACHED = "cached"
    SKIPPED = "skipped"


@dataclass(slots=True)
class NodeResult:
    """Result of executing a single node."""
    node_id: str
    status: ExecutionStatus
    duration: float = 0.0
    row_count: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "node_id": self.node_id,
            "status": self.status.value,
            "duration": self.duration,
            "row_count": self.row_count,
            "error_message": self.error_message,
            "metadata": self.metadata,
        }


@dataclass
class ExecutionSummary:
    """Summary of DAG execution."""
    total_nodes: int = 0
    changed_nodes: int = 0
    cached_nodes: int = 0
    successful_nodes: int = 0
    failed_nodes: int = 0
    skipped_nodes: int = 0
    total_duration: float = 0.0
    results: List[NodeResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_nodes": self.total_nodes,
            "changed_nodes": self.changed_nodes,
            "cached_nodes": self.cached_nodes,
            "successful_nodes": self.successful_nodes,
            "failed_nodes": self.failed_nodes,
            "skipped_nodes": self.skipped_nodes,
            "total_duration": self.total_duration,
            "results": [r.to_dict() for r in self.results],
        }


class DAGRunner:
    """
    Main orchestrator for DAG execution.

    Features:
    - Topological order execution
    - Change detection via manifest diff
    - State management for caching
    - Error handling with continue/retry
    - Progress reporting
    """

    def __init__(
        self,
        manifest: Manifest,
        old_manifest: Optional[Manifest] = None,
        target_path: Optional[Path] = None,
        exec_context: Optional["ExecutionContext"] = None,
    ):
        """Initialize the runner.

        Args:
            manifest: The current manifest to execute
            old_manifest: Previous manifest for change detection
            target_path: Path to target directory (default: target/)
            exec_context: Optional execution context for the new executor system.
                When provided, _execute_by_type uses get_executor() instead of
                the legacy execute_* functions.
        """
        self.manifest = manifest
        self.old_manifest = old_manifest
        self.target_path = target_path or Path("target")
        self.target_path.mkdir(parents=True, exist_ok=True)
        self.state_path = self.target_path / "run_state.json"
        self.exec_context = exec_context

        # Load or initialize unified state
        self.run_state: RunState = load_state(self.state_path) or RunState()

        # Determine what changed
        if old_manifest:
            self.diff = ManifestDiff.compare(old_manifest, manifest)
        else:
            self.diff = None

    def _get_topological_order(self) -> List[str]:
        """
        Get nodes in topological order using Kahn's algorithm.

        Returns:
            List of node IDs in topological order

        Raises:
            ValueError: If the DAG contains cycles
        """
        has_cycle, cycle_path = self.manifest.detect_cycles()
        if has_cycle:
            cycle_str = " -> ".join(cycle_path)
            raise ValueError(f"DAG contains cycle: {cycle_str}")

        # Kahn's algorithm
        in_degree: Dict[str, int] = {node_id: 0 for node_id in self.manifest.nodes}
        for edge in self.manifest.edges:
            if edge.to_node in in_degree and edge.from_node in in_degree:
                in_degree[edge.to_node] += 1

        # Start with nodes that have no dependencies
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        result = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)

            # Reduce in-degree for downstream nodes
            for downstream in self.manifest.get_downstream_nodes(node_id):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(result) != len(self.manifest.nodes):
            raise ValueError("DAG contains cycles")

        return result

    def _get_topological_layers(self) -> List[List[str]]:
        """Get nodes grouped into topological layers for parallel execution.

        Each layer contains nodes whose dependencies are all in previous layers.
        Nodes within a layer can safely execute in parallel.

        Returns:
            List of layers, each layer is a list of node IDs

        Raises:
            ValueError: If the DAG contains cycles
        """
        # Build adjacency directly from edges to avoid stale cache issues
        in_degree: Dict[str, int] = {node_id: 0 for node_id in self.manifest.nodes}
        downstream_adj: Dict[str, Set[str]] = {node_id: set() for node_id in self.manifest.nodes}
        for edge in self.manifest.edges:
            if edge.to_node in in_degree and edge.from_node in in_degree:
                in_degree[edge.to_node] += 1
            if edge.from_node in downstream_adj and edge.to_node in downstream_adj:
                downstream_adj[edge.from_node].add(edge.to_node)

        layers: List[List[str]] = []
        remaining = set(self.manifest.nodes.keys())

        while remaining:
            # Current layer = nodes with in_degree 0 among remaining
            layer = [nid for nid in remaining if in_degree[nid] == 0]
            if not layer:
                raise ValueError("DAG contains cycles")
            layers.append(sorted(layer))  # Sort for deterministic ordering
            remaining -= set(layer)
            # Reduce in_degree for downstream
            for nid in layer:
                for ds in downstream_adj.get(nid, set()):
                    if ds in in_degree:
                        in_degree[ds] -= 1

        return layers

    def _get_nodes_to_run(
        self,
        full: bool = False,
        node_types: Optional[List[str]] = None,
        nodes: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
    ) -> tuple[Set[str], Dict[str, str]]:
        """
        Determine which nodes need to run.

        Args:
            full: Run all nodes regardless of state
            node_types: Filter by node types
            nodes: Run specific nodes only
            exclude_tags: Skip nodes with these tags

        Returns:
            Tuple of (set of node IDs to execute, dict of node_id -> skip reason)
        """
        to_run: Set[str] = set()
        skip_reasons: Dict[str, str] = {}

        if full:
            # Run all nodes
            to_run.update(self.manifest.nodes.keys())
            for nid in to_run:
                skip_reasons[nid] = "full refresh"
        elif nodes:
            # Run specific nodes
            for node_name in nodes:
                for node_id, node in self.manifest.nodes.items():
                    if node.name == node_name or node_id == node_name:
                        to_run.add(node_id)
                        skip_reasons[node_id] = "explicitly selected"
                        downstream = self._get_all_downstream(node_id)
                        to_run.update(downstream)
                        for d in downstream:
                            skip_reasons[d] = f"downstream of {node_name}"
                        break
        elif self.diff:
            # Run changed nodes and downstream
            rebuild_map = self.diff.get_nodes_to_rebuild(self.manifest)
            to_run.update(rebuild_map.keys())
        else:
            # Fingerprint-based change detection
            to_run, skip_reasons = self._fingerprint_based_detection()

        # Apply type filter
        if node_types:
            to_run = {
                node_id for node_id in to_run
                if self.manifest.nodes[node_id].node_type.value in node_types
            }

        # Apply tag exclusions
        if exclude_tags:
            to_run = {
                node_id for node_id in to_run
                if not any(
                    tag in exclude_tags
                    for tag in self.manifest.nodes[node_id].tags
                )
            }

        return to_run, skip_reasons

    def _fingerprint_based_detection(self) -> tuple[Set[str], Dict[str, str]]:
        """Use fingerprint comparison to detect changed nodes."""
        # Build upstream map from manifest
        upstream_map: Dict[str, Set[str]] = {}
        for nid in self.manifest.nodes:
            upstream_map[nid] = self.manifest.get_upstream_nodes(nid)

        # Build node data for fingerprint computation
        manifest_nodes = {}
        for nid, node in self.manifest.nodes.items():
            manifest_nodes[nid] = {
                "kind": node.node_type.value,
                "config": node.config,
                "file_path": node.file_path or "unknown.yml",
                "columns": node.columns,
            }

        current_fps = compute_dag_fingerprints(manifest_nodes, upstream_map)

        # Compare with stored fingerprints
        changed: Set[str] = set()
        skip_reasons: Dict[str, str] = {}
        for nid, fp in current_fps.items():
            stored_node = self.run_state.nodes.get(nid)
            if stored_node is None or stored_node.fingerprint is None:
                changed.add(nid)
                skip_reasons[nid] = "new node" if stored_node is None else "no stored fingerprint"
            elif stored_node.fingerprint.combined != fp.combined:
                changed.add(nid)
                skip_reasons[nid] = "content changed"
            # Also check if cache file exists for nodes that should be cached
            else:
                node = self.manifest.nodes[nid]
                cache_path = self.target_path / "cache" / node.node_type.value / f"{node.name}.parquet"
                if not cache_path.exists() and stored_node.status == NodeStatus.SUCCESS.value:
                    changed.add(nid)
                    skip_reasons[nid] = "cache missing"

        # Store current fingerprints for later saving
        self._current_fingerprints = current_fps

        if not changed:
            return set(), skip_reasons

        # Build downstream adjacency for propagation
        downstream_adj: Dict[str, Set[str]] = {}
        for nid in self.manifest.nodes:
            downstream_adj[nid] = self.manifest.get_downstream_nodes(nid)

        to_run = find_downstream_nodes(downstream_adj, changed)
        for nid in to_run:
            if nid not in skip_reasons:
                skip_reasons[nid] = "downstream of changed node"

        return to_run, skip_reasons

    def _get_all_downstream(self, node_id: str) -> Set[str]:
        """Get all downstream nodes recursively."""
        downstream: Set[str] = set()
        queue = deque([node_id])

        while queue:
            current = queue.popleft()
            for child in self.manifest.get_downstream_nodes(current):
                if child not in downstream:
                    downstream.add(child)
                    queue.append(child)

        return downstream

    def _should_skip_node(self, node_id: str, to_run: Set[str]) -> bool:
        """Check if a node should be skipped."""
        return node_id not in to_run

    def _is_cached(self, node_id: str) -> bool:
        """Check if a node has a valid cached result."""
        if node_id not in self.run_state.nodes:
            return False
        return self.run_state.nodes[node_id].status == NodeStatus.SUCCESS.value

    def _execute_node(
        self,
        node_id: str,
        dry_run: bool = False,
    ) -> NodeResult:
        """
        Execute a single node.

        Args:
            node_id: Node to execute
            dry_run: If True, don't actually execute

        Returns:
            NodeResult with execution outcome
        """
        node = self.manifest.get_node(node_id)
        if not node:
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.FAILED,
                error_message=f"Node not found: {node_id}",
            )

        start_time = time.time()

        if dry_run:
            # Dry run - just show what would happen
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.SUCCESS,
                duration=0.0,
                metadata={"dry_run": True},
            )

        try:
            # Route to appropriate executor based on node type
            result = self._execute_by_type(node)

            duration = time.time() - start_time
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.SUCCESS,
                duration=duration,
                row_count=result.get("row_count", 0),
                metadata=result,
            )

        except Exception as e:
            duration = time.time() - start_time
            return NodeResult(
                node_id=node_id,
                status=ExecutionStatus.FAILED,
                duration=duration,
                error_message=str(e),
            )

    def _execute_by_type(self, node: Node) -> Dict[str, Any]:
        """
        Execute a node based on its type.

        When exec_context is provided, delegates to get_executor() from the
        new executor system. Otherwise, falls back to the legacy execute_*
        functions for backward compatibility.

        Args:
            node: Node to execute

        Returns:
            Result dictionary with execution info

        Raises:
            NotImplementedError: If node type is not supported
        """
        # New executor path: use get_executor when ExecutionContext is available
        if self.exec_context is not None:
            from seeknal.workflow.executors import get_executor

            executor = get_executor(node, self.exec_context)
            result = executor.run()

            return {
                "row_count": result.row_count,
                "status": result.status.value,
                "error_message": result.error_message,
                **(result.metadata or {}),
            }

        # Legacy path: use execute_* functions from executor.py
        from seeknal.workflow.executor import (
            execute_source,
            execute_transform,
            execute_feature_group,
            execute_model,
            execute_aggregation,
            execute_rule,
            execute_exposure,
        )

        yaml_data = {
            "kind": node.node_type.value,
            "name": node.name,
            **node.config,
        }

        match node.node_type:
            case NodeType.SOURCE:
                return execute_source(yaml_data, limit=0, timeout=30)
            case NodeType.TRANSFORM:
                return execute_transform(yaml_data, limit=0, timeout=30)
            case NodeType.FEATURE_GROUP:
                return execute_feature_group(yaml_data, limit=0, timeout=30)
            case NodeType.MODEL:
                return execute_model(yaml_data, limit=0, timeout=30)
            case NodeType.AGGREGATION:
                return execute_aggregation(yaml_data, limit=0, timeout=30)
            case NodeType.RULE:
                return execute_rule(yaml_data, limit=0, timeout=30)
            case NodeType.EXPOSURE:
                return execute_exposure(yaml_data, limit=0, timeout=30)
            case _:
                raise NotImplementedError(f"Unsupported node type: {node.node_type}")

    def run(
        self,
        full: bool = False,
        dry_run: bool = False,
        node_types: Optional[List[str]] = None,
        nodes: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
        continue_on_error: bool = False,
        retry: int = 0,
    ) -> ExecutionSummary:
        """
        Execute the DAG.

        Args:
            full: Run all nodes regardless of state
            dry_run: Show plan without executing
            node_types: Filter by node types
            nodes: Run specific nodes only
            exclude_tags: Skip nodes with these tags
            continue_on_error: Continue execution after failures
            retry: Number of retries for failed nodes

        Returns:
            ExecutionSummary with results
        """
        start_time = time.time()

        # Determine nodes to run
        to_run, skip_reasons = self._get_nodes_to_run(
            full=full,
            node_types=node_types,
            nodes=nodes,
            exclude_tags=exclude_tags,
        )

        if not to_run and not full:
            _echo_info("No changes detected. Nothing to run.")
            return ExecutionSummary(
                total_nodes=len(self.manifest.nodes),
                changed_nodes=0,
            )

        # Get topological order
        execution_order = self._get_topological_order()

        # Track results
        summary = ExecutionSummary(
            total_nodes=len(self.manifest.nodes),
            changed_nodes=len(to_run),
        )

        # Execute nodes in order
        for idx, node_id in enumerate(execution_order, 1):
            node = self.manifest.get_node(node_id)
            if not node:
                continue

            # Check if we should skip this node
            if self._should_skip_node(node_id, to_run):
                if self._is_cached(node_id):
                    _echo_info(f"{idx}/{len(execution_order)}: Skipping {node.name} (cached)")
                    summary.cached_nodes += 1
                    result = NodeResult(
                        node_id=node_id,
                        status=ExecutionStatus.CACHED,
                    )
                else:
                    _echo_info(f"{idx}/{len(execution_order)}: Skipping {node.name} (no changes)")
                    summary.skipped_nodes += 1
                    result = NodeResult(
                        node_id=node_id,
                        status=ExecutionStatus.SKIPPED,
                    )
                summary.results.append(result)
                continue

            # Execute the node
            reason = skip_reasons.get(node_id, "")
            reason_str = f" ({reason})" if reason else ""
            _echo_info(f"{idx}/{len(execution_order)}: Executing {node.name}{reason_str}")
            result = self._execute_node(node_id, dry_run=dry_run)

            # Update summary
            if result.status == ExecutionStatus.SUCCESS:
                _echo_success(f"{node.name} completed in {result.duration:.2f}s")
                summary.successful_nodes += 1

                # Update state on success
                if not dry_run:
                    update_node_state(
                        self.run_state,
                        node_id,
                        status=NodeStatus.SUCCESS.value,
                        duration_ms=int(result.duration * 1000),
                        row_count=result.row_count,
                    )
                    # Store fingerprint if computed
                    if hasattr(self, '_current_fingerprints') and node_id in self._current_fingerprints:
                        self.run_state.nodes[node_id].fingerprint = self._current_fingerprints[node_id]

            elif result.status == ExecutionStatus.FAILED:
                _echo_error(f"{node.name} failed: {result.error_message}")
                summary.failed_nodes += 1

                if not continue_on_error:
                    _echo_error("Stopping execution due to failure")
                    summary.results.append(result)
                    break

                # Retry logic
                if retry > 0:
                    for attempt in range(1, retry + 1):
                        _echo_warning(f"Retry {attempt}/{retry} for {node.name}")
                        result = self._execute_node(node_id, dry_run=dry_run)
                        if result.status == ExecutionStatus.SUCCESS:
                            _echo_success(f"{node.name} succeeded on retry {attempt}")
                            summary.failed_nodes -= 1
                            summary.successful_nodes += 1
                            break
                    else:
                        _echo_error(f"{node.name} failed after {retry} retries")

            summary.results.append(result)

        # Save state if not dry run
        if not dry_run:
            save_state(self.run_state, self.state_path)

        summary.total_duration = time.time() - start_time
        return summary

    def print_plan(
        self,
        full: bool = False,
        node_types: Optional[List[str]] = None,
        nodes: Optional[List[str]] = None,
        exclude_tags: Optional[List[str]] = None,
    ) -> None:
        """Print execution plan without running.

        Args:
            full: Show all nodes
            node_types: Filter by node types
            nodes: Show specific nodes only
            exclude_tags: Exclude nodes with these tags
        """
        to_run, skip_reasons = self._get_nodes_to_run(
            full=full,
            node_types=node_types,
            nodes=nodes,
            exclude_tags=exclude_tags,
        )

        execution_order = self._get_topological_order()

        _echo_info("Execution Plan:")
        _echo_info("=" * 60)

        for idx, node_id in enumerate(execution_order, 1):
            node = self.manifest.get_node(node_id)
            if not node:
                continue

            reason = skip_reasons.get(node_id, "")
            reason_str = f" ({reason})" if reason else ""

            if node_id in to_run:
                status = "RUN"
                status_msg = typer.style(status, fg=typer.colors.GREEN)
            elif self._is_cached(node_id):
                status = "CACHED"
                status_msg = typer.style(status, fg=typer.colors.BLUE)
            else:
                status = "SKIP"
                status_msg = typer.style(status, fg=typer.colors.YELLOW)

            tags_str = f" [{', '.join(node.tags)}]" if node.tags else ""
            print(f"  {idx:2d}. {status_msg} {node.name}{tags_str}{reason_str}")

        _echo_info("=" * 60)
        _echo_info(f"Total: {len(execution_order)} nodes, {len(to_run)} to run")


def print_summary(summary: ExecutionSummary) -> None:
    """Print execution summary.

    Args:
        summary: Execution summary to print
    """
    _echo_info("")
    _echo_info("=" * 60)
    _echo_info("Execution Summary")
    _echo_info("=" * 60)

    print(f"  Total nodes:     {summary.total_nodes}")
    print(f"  Changed nodes:   {summary.changed_nodes}")

    if summary.cached_nodes > 0:
        print(f"  Cached nodes:    {summary.cached_nodes}")

    if summary.successful_nodes > 0:
        success_msg = typer.style(
            f"{summary.successful_nodes}",
            fg=typer.colors.GREEN,
            bold=True,
        )
        print(f"  Successful:      {success_msg}")

    if summary.failed_nodes > 0:
        failed_msg = typer.style(
            f"{summary.failed_nodes}",
            fg=typer.colors.RED,
            bold=True,
        )
        print(f"  Failed:          {failed_msg}")

    if summary.skipped_nodes > 0:
        print(f"  Skipped:         {summary.skipped_nodes}")

    print(f"  Duration:        {summary.total_duration:.2f}s")
    _echo_info("=" * 60)

    # Show failed nodes
    if summary.failed_nodes > 0:
        _echo_error("")
        _echo_error("Failed nodes:")
        for result in summary.results:
            if result.status == ExecutionStatus.FAILED:
                _echo_error(f"  - {result.node_id}: {result.error_message}")


# Import typer for styling
import typer
