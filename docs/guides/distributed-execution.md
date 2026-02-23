# Distributed Execution Guide

## Overview

Distributed execution via Prefect enables:
- **Scheduled runs** - Cron-based automation
- **Horizontal scaling** - Execute across multiple workers
- **Retry logic** - Automatic retry on failure
- **Flow monitoring** - Prefect UI visualization

## Prerequisites

Install Prefect:

```bash
pip install prefect
```

## Quick Start

### 1. Create a Flow

```python
from seeknal.workflow.prefect_integration import seeknal_run_flow

# Run pipeline as Prefect flow
results = seeknal_run_flow(
    project_path="/path/to/project",
    parallel=True
)
```

### 2. Deploy to Prefect

```python
from seeknal.workflow.prefect_integration import create_prefect_deployment

create_prefect_deployment(
    flow_name="seeknal-run-pipeline",
    deployment_name="daily-etl",
    project_path="/path/to/project",
    schedule="0 2 * * *"  # Daily at 2am
)
```

### 3. Run via Prefect CLI

```bash
# Run flow
prefect run seeknal-run-pipeline

# Start worker
prefect worker work-queue --pool threads
```

## Flows

### seeknal_run_flow

Execute full pipeline with optional node filtering:

```python
from seeknal.workflow.prefect_integration import seeknal_run_flow

# Run all nodes
results = seeknal_run_flow(project_path="/path/to/project")

# Run specific nodes
results = seeknal_run_flow(
    project_path="/path/to/project",
    node_ids=["source_node", "transform_node"]
)
```

### seeknal_backfill_flow

Backfill historical data across intervals:

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

## Configuration

### Schedule Definition

Use cron syntax:

```python
# Daily at 2am
schedule="0 2 * * *"

# Hourly
schedule="0 * * * *"

# Weekly (Sunday at midnight)
schedule="0 0 * * 0"

# Custom cron
schedule="*/30 * * * *"  # Every 30 minutes
```

### Deployment Options

```python
create_prefect_deployment(
    flow_name="seeknal-run-pipeline",
    deployment_name="production",
    project_path="/path/to/project",
    schedule="0 2 * * *",  # Daily at 2am
    tags=["etl", "production"],
)
```

## CLI Usage

### Direct Python

```bash
# Run flow directly
python -c "
from seeknal.workflow.prefect_integration import seeknal_run_flow
seeknal_run_flow(project_path='.').wait()
"
```

### Prefect Deploy

```bash
# Deploy flow
prefect deploy seeknal-run-pipeline \
    --name my-deployment \
    --param project_path=/path/to/project \
    --param parallel=True
```

### Prefect Worker

```bash
# Start worker
prefect worker seeknal-work-queue

# With custom worker pool
prefect worker seeknal-work-queue --pool processes 4
```

## Monitoring

### Prefect UI

```bash
# Start Prefect UI
prefect server start

# Access at http://localhost:4200
```

View:
- Flow runs
- Task states
- Retry history
- Execution logs

### Flow State

```python
# Check flow state
from prefect import get_run_logger

logger = get_run_logger()
flow_run = logger.flow_run

print(f"State: {flow_run.state}")
print(f"Start time: {flow_run.start_time}")
print(f"End time: {flow_run.end_time}")
```

## Best Practices

### 1. Use Tags for Organization

```python
@flow(name="my-flow", tags=["etl", "production"])
def my_flow():
    pass
```

### 2. Set Timeouts

```python
@flow(name="my-flow", timeout_seconds=3600)
def my_flow():
    pass
```

### 3. Configure Retries

```python
@task(retries=3, retry_delay_seconds=30)
def my_task():
    pass
```

### 4. Use Resource Limits

```bash
# Limit worker concurrency
prefect worker work-queue --limit-concurrency 2
```

## Examples

### Scheduled Daily Pipeline

```python
from seeknal.workflow.prefect_integration import seeknal_run_flow, create_prefect_deployment

# Create deployment
create_prefect_deployment(
    flow_name="daily-pipeline",
    deployment_name="production",
    project_path="/data/etl",
    schedule="0 2 * * *",  # Daily at 2am
)
```

### Backfill with Retry

```python
from seeknal.workflow.prefect_integration import seeknal_backfill_flow

# Backfill with automatic retry on failure
results = seeknal_backfill_flow(
    project_path="/data/etl",
    node_id="backfill_node",
    start_date="2024-01-01",
    end_date="2024-12-31",
    schedule="@daily"
)
```

### Custom Task

```python
from prefect import task
from seeknal.workflow.prefect_integration import PrefectRunResult

@task(name="custom-node-executor", retries=2)
def execute_node(node_id: str, project_path: str) -> PrefectRunResult:
    """Execute a single Seeknal node."""
    import subprocess

    cmd = ["seeknal", "run", "--project-path", project_path, "--node", node_id]
    result = subprocess.run(cmd, capture_output=True)

    return PrefectRunResult(
        success=result.returncode == 0,
        exit_code=result.returncode,
        stdout=result.stdout,
        stderr=result.stderr,
        duration_seconds=0.0,
    )
```

## Troubleshooting

### Prefect Not Installed

```bash
Error: Prefect is not installed
Solution: pip install prefect
```

### Flow Not Found

```bash
Error: Flow 'seeknal-run-pipeline' not found
Solution: Ensure the flow module is imported in your deployment script
```

### Worker Not Processing

```bash
# Check work queue
prefect work-queue inspect

# Verify worker is connected
prefect worker work-queue
```

## Advanced

### Custom Flow Definition

```python
from prefect import flow
from pathlib import Path

@flow(name="custom-etl")
def custom_etl(
    project_path: str = ".",
    nodes: list[str] = None,
):
    """Custom ETL flow with specific nodes."""
    from seeknal.workflow.prefect_integration import run_seeknal_node

    results = {}
    for node_id in (nodes or []):
        result = run_seeknal_node.submit(node_id, project_path)
        results[node_id] = result

    return results
```

### Conditional Execution

```python
from prefect import task
from datetime import datetime

@task
def check_dependencies():
    """Check if dependencies are met."""
    # Custom logic
    return True

@flow
def conditional_flow():
    """Execute only if dependencies met."""
    if check_dependencies():
        return seeknal_run_flow(project_path=".")
    else:
        return {"skipped": "dependencies not met"}
```

## API Reference

### seeknal_run_flow

```python
def seeknal_run_flow(
    project_path: str = ".",
    parallel: bool = True,
    node_ids: Optional[List[str]] = None,
) -> Dict[str, PrefectRunResult]
```

Execute Seeknal pipeline as Prefect flow.

### seeknal_backfill_flow

```python
def seeknal_backfill_flow(
    project_path: str,
    node_id: str,
    start_date: str,
    end_date: str,
    schedule: str = "@daily",
    parallel: bool = True,
) -> Dict[str, PrefectRunResult]
```

Execute backfill as Prefect flow.

### PrefectRunResult

```python
@dataclass
class PrefectRunResult:
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    duration_seconds: float
```

Result of a Prefect task execution.
