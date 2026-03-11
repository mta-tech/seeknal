"""
Deep Prefect integration for Seeknal pipeline orchestration.

Provides production-grade Prefect orchestration using Seeknal's internal
Python API instead of subprocess calls. Features:
- DAG-aware layer-based parallel execution
- Bidirectional state sync with run_state.json
- Prefect markdown artifacts with per-node results
- Entity consolidation as best-effort final task
- Optional dependency: install with `pip install seeknal[prefect]`
"""

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("seeknal.prefect")


# ---------------------------------------------------------------------------
# Optional Prefect dependency handling
# ---------------------------------------------------------------------------

PREFECT_AVAILABLE = False

try:
    from prefect import flow, task
    from prefect.cache_policies import NONE as NO_CACHE
    from prefect.futures import wait
    from prefect.logging import get_run_logger

    PREFECT_AVAILABLE = True
except ImportError:
    # No-op decorators when Prefect is not installed
    def flow(*args, **kwargs):  # type: ignore[misc]
        def decorator(func):
            return func
        if len(args) == 1 and callable(args[0]):
            return args[0]
        return decorator

    def task(*args, **kwargs):  # type: ignore[misc]
        def decorator(func):
            return func
        if len(args) == 1 and callable(args[0]):
            return args[0]
        return decorator

    NO_CACHE = None  # type: ignore[assignment]

    def wait(futures):  # type: ignore[misc]
        pass

    def get_run_logger():  # type: ignore[misc]
        return logger


def _require_prefect() -> None:
    """Raise ImportError with install instructions if Prefect is missing."""
    if not PREFECT_AVAILABLE:
        raise ImportError(
            "Prefect not installed. Install with: pip install seeknal[prefect]"
        )


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class PrefectNodeResult:
    """Result of a single node execution within a Prefect flow."""
    node_id: str
    status: str  # "success", "failed", "cached", "skipped"
    duration: float = 0.0
    row_count: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Core Prefect task: execute a single DAG node via internal API
# ---------------------------------------------------------------------------

@task(name="seeknal-node", retries=2, retry_delay_seconds=30, cache_policy=NO_CACHE)
def run_node_task(
    runner: Any,
    node_id: str,
) -> PrefectNodeResult:
    """Execute a single Seeknal DAG node as a Prefect task.

    Calls DAGRunner._execute_node() directly (internal API, no subprocess).

    Args:
        runner: Initialized DAGRunner instance.
        node_id: Node identifier to execute.

    Returns:
        PrefectNodeResult with execution outcome.
    """
    try:
        result = runner._execute_node(node_id)
        return PrefectNodeResult(
            node_id=result.node_id,
            status=result.status.value,
            duration=result.duration,
            row_count=result.row_count,
            error_message=result.error_message,
            metadata=result.metadata,
        )
    except Exception as e:
        return PrefectNodeResult(
            node_id=node_id,
            status="failed",
            duration=0.0,
            error_message=str(e),
        )


# ---------------------------------------------------------------------------
# SeeknalPrefectFlow: the main integration class
# ---------------------------------------------------------------------------

class SeeknalPrefectFlow:
    """Builds and runs Seeknal pipelines as Prefect flows.

    Uses DAGBuilder to discover the DAG, groups nodes into topological layers,
    and executes each layer in parallel using Prefect tasks. State is synced
    bidirectionally with run_state.json.

    Args:
        project_path: Path to the seeknal project root.
        max_workers: Maximum parallel tasks per layer.
        continue_on_error: Continue execution after node failures.
        env: Environment name for profile discovery.
        profile_path: Explicit profile path override.
        full_refresh: Force full refresh (ignore cache).
        params: Additional parameters for executors.
    """

    def __init__(
        self,
        project_path: Path,
        max_workers: Optional[int] = None,
        continue_on_error: bool = False,
        env: Optional[str] = None,
        profile_path: Optional[str] = None,
        full_refresh: bool = False,
        params: Optional[Dict[str, Any]] = None,
    ):
        _require_prefect()
        self.project_path = Path(project_path).resolve()
        self.max_workers = max_workers or min(os.cpu_count() or 4, 8)
        self.continue_on_error = continue_on_error
        self.env = env
        self.profile_path = profile_path
        self.full_refresh = full_refresh
        self.params = params or {}

    def _build_runner(self):
        """Build a DAGRunner from the project's YAML definitions.

        Returns:
            Tuple of (DAGRunner, target_path)
        """
        from seeknal.workflow.dag import DAGBuilder  # ty: ignore[unresolved-import]
        from seeknal.workflow.runner import DAGRunner  # ty: ignore[unresolved-import]
        from seeknal.workflow.executors.base import ExecutionContext  # ty: ignore[unresolved-import]
        from seeknal.dag.manifest import Manifest, Node, NodeType  # ty: ignore[unresolved-import]

        dag_builder = DAGBuilder(self.project_path)
        dag_builder.build()

        target_path = self.project_path / "target"

        # Build Manifest from DAGBuilder (same pattern as cli/main.py)
        manifest = Manifest(project=self.project_path.name)
        for node_id, node in dag_builder.nodes.items():
            manifest.add_node(Node(
                id=node_id,
                name=node.name,
                node_type=NodeType(node.kind.value),
                tags=list(node.tags) if node.tags else [],
                config=node.yaml_data,
                file_path=node.file_path,
            ))
        for node_id in dag_builder.nodes:
            for downstream_id in dag_builder.get_downstream(node_id):
                manifest.add_edge(node_id, downstream_id)

        # Build ExecutionContext
        _profile = Path(self.profile_path) if self.profile_path else None
        exec_context = ExecutionContext(
            project_name=self.project_path.name,
            workspace_path=self.project_path,
            target_path=target_path,
            dry_run=False,
            verbose=False,
            materialize_enabled=True,
            params=self.params,
            common_config=dag_builder._common_config,
            profile_path=_profile,
            env_name=self.env,
        )

        runner = DAGRunner(
            manifest, target_path=target_path, exec_context=exec_context
        )
        runner._full_refresh = self.full_refresh

        return runner, target_path

    def _build_flow(self):
        """Create and return the Prefect flow function."""
        _require_prefect()

        flow_name = f"seeknal-{self.project_path.name}"
        max_workers = self.max_workers
        continue_on_error = self.continue_on_error
        full_refresh = self.full_refresh
        builder = self  # capture for closure

        @flow(name=flow_name, log_prints=True)
        def seeknal_pipeline_flow() -> List[PrefectNodeResult]:
            return _execute_pipeline(builder, max_workers, continue_on_error, full_refresh)

        return seeknal_pipeline_flow

    def serve(
        self,
        name: Optional[str] = None,
        cron: Optional[str] = None,
        interval: Optional[int] = None,
    ) -> None:
        """Start serving the flow as a long-running process.

        Args:
            name: Deployment name (default: project directory name).
            cron: Cron schedule string (e.g., "0 2 * * *").
            interval: Interval in seconds between runs.
        """
        _require_prefect()
        flow_fn = self._build_flow()
        deploy_name = name or self.project_path.name

        serve_kwargs: Dict[str, Any] = {"name": deploy_name}
        if cron:
            serve_kwargs["cron"] = cron
        if interval:
            serve_kwargs["interval"] = interval

        # Use module_path entrypoint so Prefect doesn't try to resolve
        # the source file relative to CWD (fails when flow is in an
        # installed package and CWD is a project directory).
        try:
            from prefect.deployments.runner import EntrypointType
            serve_kwargs["entrypoint_type"] = EntrypointType.MODULE_PATH
        except ImportError:
            pass

        logger.info(f"Serving flow '{deploy_name}' (cron={cron}, interval={interval})")
        flow_fn.serve(**serve_kwargs)

    def deploy(
        self,
        name: Optional[str] = None,
        work_pool: str = "default",
        cron: Optional[str] = None,
        interval: Optional[int] = None,
    ) -> None:
        """Deploy the flow to Prefect Server/Cloud.

        Args:
            name: Deployment name (default: project directory name).
            work_pool: Prefect work pool name.
            cron: Cron schedule string.
            interval: Interval in seconds.
        """
        _require_prefect()
        flow_fn = self._build_flow()
        deploy_name = name or self.project_path.name

        deploy_kwargs: Dict[str, Any] = {
            "name": deploy_name,
            "work_pool_name": work_pool,
        }
        if cron:
            deploy_kwargs["cron"] = cron
        if interval:
            deploy_kwargs["interval"] = interval

        try:
            from prefect.deployments.runner import EntrypointType
            deploy_kwargs["entrypoint_type"] = EntrypointType.MODULE_PATH
        except ImportError:
            pass

        logger.info(f"Deploying flow '{deploy_name}' to work pool '{work_pool}'")
        flow_fn.deploy(**deploy_kwargs)

    def deploy_exposure(
        self,
        exposure_name: str,
        work_pool: str = "default",
    ) -> None:
        """Deploy a report exposure as a scheduled Prefect flow.

        Loads the exposure YAML, extracts the schedule, and creates a
        Prefect deployment that runs the report on the specified cron.

        Args:
            exposure_name: Name of the report exposure.
            work_pool: Prefect work pool name.
        """
        _require_prefect()
        from seeknal.ask.report.exposure import load_report_exposure

        config = load_report_exposure(self.project_path, exposure_name)
        schedule = config.get("schedule", {})
        cron_str = schedule.get("cron")
        if not cron_str:
            raise ValueError(
                f"Exposure '{exposure_name}' has no schedule.cron defined. "
                "Add a schedule: { cron: '...' } to the exposure YAML."
            )

        project_path = str(self.project_path)
        deploy_name = f"seeknal-report-{exposure_name}"

        @flow(name=deploy_name, log_prints=True)
        def report_flow():
            return _execute_report(project_path, exposure_name, config)

        deploy_kwargs: Dict[str, Any] = {
            "name": deploy_name,
            "work_pool_name": work_pool,
            "cron": cron_str,
        }
        timezone = schedule.get("timezone")
        if timezone:
            deploy_kwargs["timezone"] = timezone

        try:
            from prefect.deployments.runner import EntrypointType
            deploy_kwargs["entrypoint_type"] = EntrypointType.MODULE_PATH
        except ImportError:
            pass

        logger.info(
            f"Deploying report '{exposure_name}' to work pool '{work_pool}' "
            f"with cron='{cron_str}'"
        )
        report_flow.deploy(**deploy_kwargs)

    def generate_prefect_yaml(self) -> str:
        """Generate a prefect.yaml configuration file content.

        Returns:
            YAML string for prefect.yaml
        """
        import yaml

        config = {
            "deployments": [
                {
                    "name": f"seeknal-{self.project_path.name}",
                    "entrypoint": "seeknal.workflow.prefect_integration:create_pipeline_flow",
                    "parameters": {
                        "project_path": str(self.project_path),
                        "max_workers": self.max_workers,
                        "continue_on_error": self.continue_on_error,
                    },
                    "work_pool": {
                        "name": "default",
                    },
                }
            ]
        }
        return yaml.dump(config, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Pipeline execution logic (called from within the @flow)
# ---------------------------------------------------------------------------

def _execute_pipeline(
    builder: SeeknalPrefectFlow,
    max_workers: int,
    continue_on_error: bool,
    full_refresh: bool,
) -> List[PrefectNodeResult]:
    """Core pipeline execution logic.

    Builds the runner, groups nodes into topological layers, and executes
    each layer in parallel using Prefect tasks with barrier sync between layers.
    """
    from seeknal.workflow.runner import ExecutionStatus
    from seeknal.workflow.state import (
        NodeStatus, update_node_state, save_state, find_downstream_nodes,
    )

    try:
        run_logger = get_run_logger()
    except Exception:
        run_logger = logger

    # 1. Build runner
    runner, target_path = builder._build_runner()
    run_logger.info(f"Pipeline: {builder.project_path.name} ({len(runner.manifest.nodes)} nodes)")

    # 2. Recover stuck RUNNING nodes (crash recovery)
    _recover_running_nodes(runner.run_state, run_logger)

    # 3. Determine which nodes to run
    nodes_to_run = _get_nodes_to_run(runner, full_refresh)
    layers = runner._get_topological_layers()

    run_logger.info(
        f"Execution plan: {len(nodes_to_run)} nodes to run across {len(layers)} layers"
    )

    # 4. Layer-based execution
    all_results: List[PrefectNodeResult] = []
    failed_nodes: Set[str] = set()

    for layer_idx, layer in enumerate(layers):
        # Filter to nodes that need execution and aren't downstream of failures
        executable = [
            nid for nid in layer
            if nid in nodes_to_run and nid not in failed_nodes
        ]

        # Record skipped nodes
        for nid in layer:
            if nid not in nodes_to_run:
                status = "cached" if runner._is_cached(nid) else "skipped"
                all_results.append(PrefectNodeResult(node_id=nid, status=status))

        if not executable:
            continue

        run_logger.info(
            f"Layer {layer_idx + 1}/{len(layers)}: running {len(executable)} node(s)"
        )

        # Execute layer nodes as Prefect tasks.
        # Uses sequential task calls (not .submit()) because DAGRunner
        # shares a DuckDB connection that isn't thread-safe across
        # concurrent submit() calls. Each node still appears as a
        # separate task run in the Prefect UI.
        layer_results: Dict[str, PrefectNodeResult] = {}
        for nid in executable:
            try:
                layer_results[nid] = run_node_task(runner, nid)
            except Exception as e:
                layer_results[nid] = PrefectNodeResult(
                    node_id=nid, status="failed", error_message=str(e)
                )

        # Batch state update (single write per layer)
        _batch_update_state(runner, layer_results)
        all_results.extend(layer_results.values())

        # Track failures and skip downstream
        for nid, result in layer_results.items():
            if result.status == "failed":
                failed_nodes.add(nid)
                # Find all downstream nodes to skip
                downstream_adj: Dict[str, Set[str]] = {}
                for mn in runner.manifest.nodes:
                    downstream_adj[mn] = runner.manifest.get_downstream_nodes(mn)
                downstream = find_downstream_nodes(downstream_adj, {nid})
                failed_nodes.update(downstream)

                run_logger.warning(f"Node '{nid}' failed: {result.error_message}")

                if not continue_on_error:
                    run_logger.error(f"Stopping: node '{nid}' failed")
                    break

    # 5. Entity consolidation (best-effort)
    _run_consolidation(runner, all_results, run_logger)

    # 6. Save final state
    save_state(runner.run_state, runner.state_path)

    # 7. Create markdown artifact
    _create_results_artifact(all_results, runner)

    # Summary
    success_count = sum(1 for r in all_results if r.status == "success")
    failed_count = sum(1 for r in all_results if r.status == "failed")
    cached_count = sum(1 for r in all_results if r.status == "cached")
    run_logger.info(
        f"Pipeline complete: {success_count} succeeded, {failed_count} failed, "
        f"{cached_count} cached"
    )

    return all_results


# ---------------------------------------------------------------------------
# Report execution (for exposure scheduling)
# ---------------------------------------------------------------------------

def _execute_report(
    project_path_str: str,
    exposure_name: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Execute a report exposure within a Prefect flow.

    Routes to deterministic renderer for sections-based reports,
    or to agent invocation for prompt-based reports.
    """
    from pathlib import Path as _Path

    project_path = _Path(project_path_str)
    has_sections = bool(config.get("sections"))

    if has_sections:
        from seeknal.ask.report.deterministic import render_deterministic_report
        result = render_deterministic_report(config, project_path)
        return {"status": "success", "report": str(result)}

    # Prompt-based: use agent
    params = config.get("params", {})
    prompt = params.get("prompt", "")
    if not prompt:
        return {"status": "failed", "error": "No prompt or sections in exposure"}

    from seeknal.ask.agents.agent import create_agent, ask as ask_agent
    agent, agent_config = create_agent(project_path)
    response = ask_agent(agent, agent_config, prompt)
    return {"status": "success", "response": response}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _recover_running_nodes(run_state, log) -> None:
    """Reset nodes stuck in RUNNING state to FAILED (crash recovery).

    If the previous flow crashed mid-execution, some nodes may be stuck
    in RUNNING state. Reset them to FAILED so they get re-executed.
    """
    from seeknal.workflow.state import NodeStatus

    for node_id, node_state in run_state.nodes.items():
        if node_state.status == NodeStatus.RUNNING.value:
            log.warning(
                f"Recovering node '{node_id}' from RUNNING state (previous crash?). "
                "Resetting to FAILED for re-execution."
            )
            node_state.status = NodeStatus.FAILED.value


def _get_nodes_to_run(runner, full_refresh: bool) -> Set[str]:
    """Determine which nodes need execution.

    If full_refresh, run everything. Otherwise, skip cached/succeeded nodes
    and include their downstream (change detection).
    """
    all_nodes = set(runner.manifest.nodes.keys())

    if full_refresh:
        return all_nodes

    # Skip nodes that already succeeded
    to_skip = set()
    for node_id in all_nodes:
        if runner._is_cached(node_id):
            to_skip.add(node_id)

    return all_nodes - to_skip


def _batch_update_state(
    runner,
    layer_results: Dict[str, PrefectNodeResult],
) -> None:
    """Update run_state.json once per layer (no concurrent writes)."""
    from seeknal.workflow.state import NodeStatus, update_node_state, save_state

    for nid, result in layer_results.items():
        status = (
            NodeStatus.SUCCESS.value
            if result.status == "success"
            else NodeStatus.FAILED.value
        )
        update_node_state(
            runner.run_state,
            nid,
            status=status,
            duration_ms=int(result.duration * 1000),
            row_count=result.row_count,
        )

    # Write state after entire layer
    save_state(runner.run_state, runner.state_path)


def _run_consolidation(runner, all_results, log) -> None:
    """Run entity consolidation as best-effort final step.

    Never fails the flow — all errors are caught and logged.
    """
    from seeknal.workflow.runner import ExecutionStatus

    try:
        fg_nodes = {
            nid: node for nid, node in runner.manifest.nodes.items()
            if getattr(node.node_type, 'value', str(node.node_type)) == "feature_group"
        }
        if not fg_nodes:
            return

        # Determine which FGs succeeded
        changed_fgs = set()
        for result in all_results:
            if (
                result.status == "success"
                and result.node_id.startswith("feature_group.")
            ):
                fg_name = result.node_id.split(".", 1)[1]
                changed_fgs.add(fg_name)

        if not changed_fgs:
            log.info("No FG nodes succeeded — skipping consolidation")
            return

        from seeknal.workflow.consolidation.consolidator import EntityConsolidator  # ty: ignore[unresolved-import]

        log.info(f"Running entity consolidation for {len(changed_fgs)} FG(s)")
        consolidator = EntityConsolidator(runner.target_path)
        results = consolidator.consolidate_all(
            runner.manifest.nodes,
            changed_fgs=changed_fgs,
        )

        for cr in results:
            if cr.success:
                log.info(
                    f"  Consolidated entity '{cr.entity_name}': "
                    f"{cr.fg_count} FGs, {cr.row_count} rows"
                )
            else:
                log.warning(f"  Consolidation failed for '{cr.entity_name}': {cr.error}")

    except Exception as e:
        log.warning(f"Entity consolidation failed (non-fatal): {e}")


def _create_results_artifact(
    all_results: List[PrefectNodeResult],
    runner,
) -> None:
    """Create a Prefect markdown artifact summarizing the run."""
    if not PREFECT_AVAILABLE:
        return

    try:
        from prefect.artifacts import create_markdown_artifact
    except ImportError:
        return

    # Build markdown table
    lines = [
        "# Seeknal Pipeline Results\n",
        f"**Project:** {runner.manifest.metadata.project}",
        f"**Run ID:** {runner.run_state.run_id}",
        f"**Time:** {datetime.now().isoformat()}\n",
        "| Node | Status | Duration | Rows |",
        "|------|--------|----------|------|",
    ]

    success_count = 0
    failed_count = 0

    for result in sorted(all_results, key=lambda r: r.node_id):
        status_icon = {
            "success": "✅",
            "failed": "❌",
            "cached": "⏭️",
            "skipped": "⏭️",
        }.get(result.status, "❓")

        duration_str = f"{result.duration:.2f}s" if result.duration > 0 else "—"
        row_str = str(result.row_count) if result.row_count > 0 else "—"

        lines.append(
            f"| {result.node_id} | {status_icon} {result.status} | "
            f"{duration_str} | {row_str} |"
        )

        if result.status == "success":
            success_count += 1
        elif result.status == "failed":
            failed_count += 1

    # Summary line
    lines.append(f"\n**Summary:** {success_count} succeeded, {failed_count} failed")

    # Show errors
    errors = [r for r in all_results if r.status == "failed" and r.error_message]
    if errors:
        lines.append("\n## Errors\n")
        for r in errors:
            lines.append(f"- **{r.node_id}:** {r.error_message}")

    markdown = "\n".join(lines)

    try:
        create_markdown_artifact(
            key=f"seeknal-run-{runner.run_state.run_id}",
            markdown=markdown,
            description=f"Seeknal pipeline run: {success_count} ok, {failed_count} failed",
        )
    except Exception:
        logger.debug("Failed to create Prefect artifact (non-fatal)")


# ---------------------------------------------------------------------------
# Backfill flow
# ---------------------------------------------------------------------------

@flow(name="seeknal-backfill", log_prints=True)
def seeknal_backfill_flow(
    project_path: str,
    node_id: str,
    start_date: str,
    end_date: str,
    schedule: str = "@daily",
    env: Optional[str] = None,
    profile_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Execute backfill as a Prefect flow using internal API.

    Runs a node for multiple historical time intervals using the
    interval calculator. Each interval executes via the internal API.

    Args:
        project_path: Path to Seeknal project.
        node_id: Node to backfill.
        start_date: Start date (YYYY-MM-DD).
        end_date: End date (YYYY-MM-DD).
        schedule: Cron schedule for intervals.
        env: Environment name.
        profile_path: Profile path.

    Returns:
        List of interval results.
    """
    _require_prefect()

    from seeknal.workflow.intervals import create_interval_calculator

    try:
        run_logger = get_run_logger()
    except Exception:
        run_logger = logger

    project = Path(project_path).resolve()

    calc = create_interval_calculator(schedule)
    start_dt = datetime.fromisoformat(f"{start_date}T00:00:00")
    end_dt = datetime.fromisoformat(f"{end_date}T23:59:59")

    intervals = calc.get_pending_intervals([], start_dt, end_dt)
    run_logger.info(f"Backfill: {len(intervals)} intervals for node '{node_id}'")

    results = []
    for i, interval in enumerate(intervals, 1):
        interval_key = f"{interval.start.isoformat()}_{interval.end.isoformat()}"
        run_logger.info(f"  [{i}/{len(intervals)}] {interval_key}")

        try:
            # Build fresh runner per interval with date params
            builder = SeeknalPrefectFlow(
                project_path=project,
                env=env,
                profile_path=profile_path,
                params={
                    "start_date": interval.start.date().isoformat(),
                    "end_date": interval.end.date().isoformat(),
                },
            )
            runner, _ = builder._build_runner()
            result = runner._execute_node(node_id)

            results.append({
                "interval": interval_key,
                "status": result.status.value,
                "duration": result.duration,
                "row_count": result.row_count,
                "error": result.error_message,
            })
        except Exception as e:
            results.append({
                "interval": interval_key,
                "status": "failed",
                "duration": 0.0,
                "row_count": 0,
                "error": str(e),
            })

    return results


# ---------------------------------------------------------------------------
# Factory function for prefect.yaml entrypoint
# ---------------------------------------------------------------------------

def create_pipeline_flow(
    project_path: str,
    max_workers: int = 8,
    continue_on_error: bool = False,
    full_refresh: bool = False,
    env: Optional[str] = None,
    profile_path: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
) -> List[PrefectNodeResult]:
    """Entry point for prefect.yaml deployments.

    Creates a SeeknalPrefectFlow and executes the pipeline.
    Can be referenced as: seeknal.workflow.prefect_integration:create_pipeline_flow
    """
    builder = SeeknalPrefectFlow(
        project_path=Path(project_path),
        max_workers=max_workers,
        continue_on_error=continue_on_error,
        full_refresh=full_refresh,
        env=env,
        profile_path=profile_path,
        params=params or {},
    )
    flow_fn = builder._build_flow()
    return flow_fn()


# ---------------------------------------------------------------------------
# Exports
# ---------------------------------------------------------------------------

__all__ = [
    "PREFECT_AVAILABLE",
    "SeeknalPrefectFlow",
    "PrefectNodeResult",
    "run_node_task",
    "seeknal_backfill_flow",
    "create_pipeline_flow",
]
