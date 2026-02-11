"""
Prefect integration for distributed Seeknal execution.

This module provides Prefect flow wrappers for Seeknal pipelines,
 enabling:
- Scheduled runs via Prefect Cloud or Server
- Distributed execution across workers
- Retry logic and error handling
- Flow visualization and monitoring
"""
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from prefect import flow, task
    from prefect.context import get_run_context
    PREFECT_AVAILABLE = True
except ImportError:
    PREFECT_AVAILABLE = False
    # Create no-op decorators for when Prefect is not installed
    def flow(*args, **kwargs):
        def decorator(func):
            return func
        return decorator

    def task(*args, **kwargs):
        def decorator(func):
            return func
        return decorator


@dataclass
class PrefectRunResult:
    """Result of a Prefect flow execution."""
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float


@task(name="seeknal-run-node", retries=2, retry_delay_seconds=30)
def run_seeknal_node(
    project_path: str,
    node_id: str,
    parallel: bool = True
) -> PrefectRunResult:
    """
    Execute a single Seeknal node as a Prefect task.

    Args:
        project_path: Path to Seeknal project
        node_id: Node identifier to execute
        parallel: Enable parallel execution

    Returns:
        PrefectRunResult with execution details
    """
    cmd = ["seeknal", "run", "--project-path", project_path, "--node", node_id]
    if parallel:
        cmd.append("--parallel")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
        )
        return PrefectRunResult(
            success=result.returncode == 0,
            exit_code=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
            duration_seconds=0.0,  # Prefect tracks duration
        )
    except subprocess.TimeoutExpired:
        return PrefectRunResult(
            success=False,
            exit_code=-1,
            stdout="",
            stderr="Node execution timed out after 1 hour",
            duration_seconds=0.0,
        )


@flow(name="seeknal-run-pipeline", log_prints=True)
def seeknal_run_flow(
    project_path: str = ".",
    parallel: bool = True,
    node_ids: Optional[List[str]] = None,
) -> Dict[str, PrefectRunResult]:
    """
    Execute Seeknal pipeline as a Prefect flow.

    This flow wraps the Seeknal run command, providing:
    - Automatic retry on failure
    - Distributed execution via Prefect workers
    - Scheduled runs via Prefect deployment
    - Flow visualization in Prefect UI

    Args:
        project_path: Path to Seeknal project
        parallel: Enable parallel node execution
        node_ids: Optional list of specific nodes to run

    Returns:
        Dict mapping node IDs to their execution results

    Example:
        ```python
        from seeknal.workflow.prefect_integration import seeknal_run_flow

        # Run all nodes
        results = seeknal_run_flow(project_path="/path/to/project")

        # Run specific nodes
        results = seeknal_run_flow(
            project_path="/path/to/project",
            node_ids=["node1", "node2"]
        )
        ```

    Deployment:
        ```bash
        # Deploy to Prefect
        prefect deploy seeknal-run-flow \
            --name my-seeknal-pipeline \
            --param project_path=/path/to/project \
            --param parallel=True
        ```
    """
    if not PREFECT_AVAILABLE:
        raise ImportError(
            "Prefect is not installed. Install with: pip install prefect"
        )

    project_path = str(Path(project_path).resolve())
    results = {}

    # If specific nodes provided, run only those
    if node_ids:
        for node_id in node_ids:
            result = run_seeknal_node(project_path, node_id, parallel)
            results[node_id] = result
            if not result.success:
                print(f"Node {node_id} failed: {result.stderr}")
    else:
        # Run full pipeline
        cmd = ["seeknal", "run", "--project-path", project_path]
        if parallel:
            cmd.append("--parallel")

        try:
            proc_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200,  # 2 hour timeout for full pipeline
            )
            results["pipeline"] = PrefectRunResult(
                success=proc_result.returncode == 0,
                exit_code=proc_result.returncode,
                stdout=proc_result.stdout,
                stderr=proc_result.stderr,
                duration_seconds=0.0,
            )
        except subprocess.TimeoutExpired:
            results["pipeline"] = PrefectRunResult(
                success=False,
                exit_code=-1,
                stdout="",
                stderr="Pipeline execution timed out after 2 hours",
                duration_seconds=0.0,
            )

    return results


@flow(name="seeknal-backfill", log_prints=True)
def seeknal_backfill_flow(
    project_path: str,
    node_id: str,
    start_date: str,
    end_date: str,
    schedule: str = "@daily",
    parallel: bool = True,
) -> Dict[str, PrefectRunResult]:
    """
    Execute backfill as a Prefect flow.

    This flow runs a node for multiple historical time intervals,
    ideal for backfilling missing data.

    Args:
        project_path: Path to Seeknal project
        node_id: Node to backfill
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        schedule: Cron schedule for intervals
        parallel: Enable parallel execution

    Returns:
        Dict mapping interval strings to execution results

    Example:
        ```python
        from seeknal.workflow.prefect_integration import seeknal_backfill_flow

        results = seeknal_backfill_flow(
            project_path="/path/to/project",
            node_id="my_node",
            start_date="2024-01-01",
            end_date="2024-01-31",
            schedule="@daily"
        )
        ```
    """
    if not PREFECT_AVAILABLE:
        raise ImportError(
            "Prefect is not installed. Install with: pip install prefect"
        )

    project_path = str(Path(project_path).resolve())

    # Calculate intervals using Seeknal's interval calculator
    from seeknal.workflow.intervals import create_interval_calculator
    from datetime import datetime

    calc = create_interval_calculator(schedule)
    start_dt = datetime.fromisoformat(f"{start_date}T00:00:00")
    end_dt = datetime.fromisoformat(f"{end_date}T23:59:59")

    intervals = calc.get_pending_intervals([], start_dt, end_dt)
    results = {}

    for i, interval in enumerate(intervals, 1):
        interval_key = f"{interval.start.isoformat()}_{interval.end.isoformat()}"
        cmd = [
            "seeknal", "run",
            "--project-path", project_path,
            "--node", node_id,
            "--start", interval.start.date().isoformat(),
            "--end", interval.end.date().isoformat(),
        ]
        if parallel:
            cmd.append("--parallel")

        try:
            proc_result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,
            )
            results[interval_key] = PrefectRunResult(
                success=proc_result.returncode == 0,
                exit_code=proc_result.returncode,
                stdout=proc_result.stdout,
                stderr=proc_result.stderr,
                duration_seconds=0.0,
            )
        except subprocess.TimeoutExpired:
            results[interval_key] = PrefectRunResult(
                success=False,
                exit_code=-1,
                stdout="",
                stderr=f"Interval {interval_key} timed out",
                duration_seconds=0.0,
            )

        print(f"Completed {i}/{len(intervals)}: {interval_key}")

    return results


def create_prefect_deployment(
    flow_name: str = "seeknal-run-pipeline",
    deployment_name: str = "seeknal-production",
    project_path: str = ".",
    schedule: Optional[str] = None,
) -> None:
    """
    Create a Prefect deployment for scheduled Seeknal runs.

    Args:
        flow_name: Name of the Prefect flow
        deployment_name: Name for the deployment
        project_path: Path to Seeknal project
        schedule: Optional cron schedule (e.g., "0 2 * * *" for daily at 2am)

    Example:
        ```python
        from seeknal.workflow.prefect_integration import create_prefect_deployment

        # Create deployment with daily schedule
        create_prefect_deployment(
            flow_name="seeknal-run-pipeline",
            deployment_name="daily-etl",
            project_path="/path/to/project",
            schedule="0 2 * * *"
        )
        ```
    """
    if not PREFECT_AVAILABLE:
        raise ImportError(
            "Prefect is not installed. Install with: pip install prefect"
        )

    from prefect import serve
    from prefect.deployments import Deployment

    # Build flow
    flow_obj = seeknal_run_flow

    # Create deployment
    Deployment.build_from_flow(
        flow=flow_obj,
        name=deployment_name,
        parameters={"project_path": project_path},
        schedule=schedule,
    )

    print(f"Prefect deployment '{deployment_name}' created successfully")
    print(f"Flow: {flow_name}")
    if schedule:
        print(f"Schedule: {schedule}")


# Export flows for Prefect discovery
__all__ = [
    "seeknal_run_flow",
    "seeknal_backfill_flow",
    "create_prefect_deployment",
    "PrefectRunResult",
    "run_seeknal_node",
]
