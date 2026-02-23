"""
Parallel DAG execution engine.

Executes independent DAG nodes concurrently using ThreadPoolExecutor.
DuckDB releases the GIL during query execution, so threads get real
parallelism for DuckDB operations.

Design:
- Topological layer grouping: nodes in the same layer have no dependencies on each other
- Per-thread DuckDB connections: each worker gets its own in-memory DuckDB
- Batch state updates: state file written once per layer (no concurrent writes)
- Error handling: failed nodes cause downstream to be skipped
"""

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from seeknal.cli.main import _echo_info, _echo_success, _echo_error, _echo_warning
from seeknal.workflow.runner import DAGRunner, ExecutionStatus, NodeResult
from seeknal.workflow.state import (
    NodeStatus, update_node_state, save_state, find_downstream_nodes,
)


@dataclass
class ParallelExecutionSummary:
    """Summary of parallel DAG execution."""
    total_nodes: int = 0
    executed_nodes: int = 0
    successful_nodes: int = 0
    failed_nodes: int = 0
    skipped_nodes: int = 0
    cached_nodes: int = 0
    total_layers: int = 0
    total_duration: float = 0.0
    results: List[NodeResult] = field(default_factory=list)


class ParallelDAGRunner:
    """Executes DAG nodes in parallel by topological layer.

    Each layer contains independent nodes that can safely run concurrently.
    A barrier sync happens between layers to ensure dependencies are met.
    """

    def __init__(
        self,
        runner: DAGRunner,
        max_workers: Optional[int] = None,
        continue_on_error: bool = False,
    ):
        self.runner = runner
        self.max_workers = max_workers or min(os.cpu_count() or 4, 8)
        self.continue_on_error = continue_on_error
        self._failed_nodes: Set[str] = set()
        self._lock = threading.Lock()

    def run(self, nodes_to_run: Set[str], dry_run: bool = False) -> ParallelExecutionSummary:
        """Execute nodes in parallel by topological layer.

        Args:
            nodes_to_run: Set of node IDs to execute
            dry_run: If True, don't actually execute

        Returns:
            ParallelExecutionSummary with results
        """
        start_time = time.time()
        layers = self.runner._get_topological_layers()

        summary = ParallelExecutionSummary(
            total_nodes=len(self.runner.manifest.nodes),
            total_layers=len(layers),
        )

        for layer_idx, layer in enumerate(layers):
            # Filter to nodes we need to run
            layer_nodes = [nid for nid in layer if nid in nodes_to_run]

            # Handle skipped/cached nodes not in to_run
            for nid in layer:
                if nid not in nodes_to_run:
                    result = NodeResult(
                        node_id=nid,
                        status=ExecutionStatus.CACHED if self.runner._is_cached(nid) else ExecutionStatus.SKIPPED,
                    )
                    summary.results.append(result)
                    if result.status == ExecutionStatus.CACHED:
                        summary.cached_nodes += 1
                    else:
                        summary.skipped_nodes += 1

            if not layer_nodes:
                continue

            # Skip nodes whose upstream has failed
            executable = []
            for nid in layer_nodes:
                upstream = self.runner.manifest.get_upstream_nodes(nid)
                if any(uid in self._failed_nodes for uid in upstream):
                    result = NodeResult(
                        nid, ExecutionStatus.SKIPPED,
                        error_message="Upstream node failed"
                    )
                    summary.results.append(result)
                    summary.skipped_nodes += 1
                    _echo_warning(f"Skipping {nid} (upstream failed)")
                    continue
                executable.append(nid)

            if not executable:
                continue

            # Execute layer
            _echo_info(
                f"[Layer {layer_idx + 1}/{len(layers)}] Running {len(executable)} node(s) in parallel..."
            )

            if dry_run:
                for nid in executable:
                    result = NodeResult(nid, ExecutionStatus.SUCCESS, metadata={"dry_run": True})
                    summary.results.append(result)
                    summary.successful_nodes += 1
                    summary.executed_nodes += 1
                continue

            layer_results = self._execute_layer(executable, layer_idx, len(layers))
            summary.results.extend(layer_results)

            # Batch state update after layer completes
            self._batch_update_state(layer_results)

            # Check for failures
            layer_failures = [r for r in layer_results if r.status == ExecutionStatus.FAILED]
            layer_successes = [r for r in layer_results if r.status == ExecutionStatus.SUCCESS]

            summary.executed_nodes += len(executable)
            summary.successful_nodes += len(layer_successes)
            summary.failed_nodes += len(layer_failures)

            if layer_failures:
                for r in layer_failures:
                    self._failed_nodes.add(r.node_id)
                    # Mark all downstream as to-skip
                    downstream_adj: Dict[str, Set[str]] = {}
                    for nid in self.runner.manifest.nodes:
                        downstream_adj[nid] = self.runner.manifest.get_downstream_nodes(nid)
                    downstream = find_downstream_nodes(downstream_adj, {r.node_id})
                    self._failed_nodes.update(downstream)

                if not self.continue_on_error:
                    _echo_error(
                        f"Stopping: {len(layer_failures)} node(s) failed in layer {layer_idx + 1}"
                    )
                    break

        # Save final state
        save_state(self.runner.run_state, self.runner.state_path)

        summary.total_duration = time.time() - start_time
        return summary

    def _execute_layer(
        self, node_ids: List[str], layer_idx: int, total_layers: int
    ) -> List[NodeResult]:
        """Execute all nodes in a layer concurrently."""
        results = []
        effective_workers = min(self.max_workers, len(node_ids))

        with ThreadPoolExecutor(max_workers=effective_workers) as pool:
            future_to_node = {
                pool.submit(self._execute_single, nid): nid
                for nid in node_ids
            }
            for future in as_completed(future_to_node):
                node_id = future_to_node[future]
                try:
                    result = future.result()
                except Exception as e:
                    result = NodeResult(
                        node_id, ExecutionStatus.FAILED,
                        error_message=str(e)
                    )

                node = self.runner.manifest.get_node(node_id)
                node_name = node.name if node else node_id

                if result.status == ExecutionStatus.SUCCESS:
                    _echo_success(f"  {node_name} completed in {result.duration:.2f}s")
                elif result.status == ExecutionStatus.FAILED:
                    _echo_error(f"  {node_name} failed: {result.error_message}")

                results.append(result)

        return results

    def _execute_single(self, node_id: str) -> NodeResult:
        """Execute a single node in its own thread."""
        return self.runner._execute_node(node_id)

    def _batch_update_state(self, results: List[NodeResult]) -> None:
        """Update state file once per layer (no concurrent writes)."""
        with self._lock:
            for result in results:
                status = (
                    NodeStatus.SUCCESS.value
                    if result.status == ExecutionStatus.SUCCESS
                    else NodeStatus.FAILED.value
                )
                update_node_state(
                    self.runner.run_state,
                    result.node_id,
                    status=status,
                    duration_ms=int(result.duration * 1000),
                    row_count=result.row_count,
                )
                # Store fingerprint if computed
                if (
                    hasattr(self.runner, '_current_fingerprints')
                    and result.node_id in self.runner._current_fingerprints
                ):
                    self.runner.run_state.nodes[result.node_id].fingerprint = (
                        self.runner._current_fingerprints[result.node_id]
                    )


def print_parallel_summary(summary: ParallelExecutionSummary) -> None:
    """Print parallel execution summary."""
    import typer

    _echo_info("")
    _echo_info("=" * 60)
    _echo_info("Parallel Execution Summary")
    _echo_info("=" * 60)

    print(f"  Total nodes:     {summary.total_nodes}")
    print(f"  Layers:          {summary.total_layers}")
    print(f"  Executed:        {summary.executed_nodes}")

    if summary.cached_nodes > 0:
        print(f"  Cached:          {summary.cached_nodes}")

    if summary.successful_nodes > 0:
        success_msg = typer.style(f"{summary.successful_nodes}", fg=typer.colors.GREEN, bold=True)
        print(f"  Successful:      {success_msg}")

    if summary.failed_nodes > 0:
        failed_msg = typer.style(f"{summary.failed_nodes}", fg=typer.colors.RED, bold=True)
        print(f"  Failed:          {failed_msg}")

    if summary.skipped_nodes > 0:
        print(f"  Skipped:         {summary.skipped_nodes}")

    print(f"  Duration:        {summary.total_duration:.2f}s")
    _echo_info("=" * 60)

    # Show failed nodes
    failed_results = [r for r in summary.results if r.status == ExecutionStatus.FAILED]
    if failed_results:
        _echo_error("")
        _echo_error("Failed nodes:")
        for result in failed_results:
            _echo_error(f"  - {result.node_id}: {result.error_message}")
