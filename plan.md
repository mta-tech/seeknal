# Seeknal + Prefect Integration Plan

## Summary
Enhance the existing `prefect_integration.py` with a **hybrid approach**: use DAGBuilder's Python API to discover the DAG structure and map nodes to Prefect tasks, but execute each node via subprocess for isolation. Add CLI commands (`seeknal prefect deploy/serve`). Target both Prefect Server and Cloud.

## Architecture

```
seeknal prefect serve/deploy
        │
        ▼
┌─────────────────────────┐
│  DAGBuilder.build()     │  ← Python API: discover nodes & edges
│  topological_sort()     │
│  get_topological_layers │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Build Prefect flow     │  ← Dynamically create @task per node
│  with task.submit()     │     with wait_for dependencies
│  + wait_for deps        │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Each task runs:        │  ← subprocess: seeknal run --nodes <node>
│  subprocess.run(        │     Process isolation per node
│    seeknal run ...)     │
└─────────────────────────┘
```

## Files to Change

### 1. `src/seeknal/workflow/prefect_integration.py` — **Rewrite**
- Keep `PrefectRunResult` dataclass
- New `SeeknalPrefectFlow` class:
  - `__init__(project_path, parallel, max_workers, env, profile, task_mapping)`
  - `task_mapping`: `"per_node"` (default) or `"pipeline"` — configurable
  - `build()` → uses `DAGBuilder` to discover DAG, creates Prefect flow dynamically
  - `_create_node_task(node)` → wraps `run_seeknal_node()` with node-specific config
  - `_build_per_node_flow()` → creates a flow where each node is a task with `wait_for` deps
  - `_build_pipeline_flow()` → creates a flow with single task (whole pipeline)
  - `serve(name, cron, rrule, interval_seconds)` → calls `flow.serve()`
  - `deploy(name, work_pool, cron, ...)` → calls `flow.deploy()`
- Keep `run_seeknal_node` as the subprocess task
- Keep `seeknal_backfill_flow` (update to Prefect 3.x patterns)
- Add Prefect artifact reporting (markdown with node results table)
- Remove deprecated `Deployment.build_from_flow` usage

### 2. `src/seeknal/cli/main.py` — **Add Prefect subcommands**
- New Typer group: `prefect_app = typer.Typer(name="prefect")`
- Commands:
  - `seeknal prefect serve` — Start a long-running process serving the flow
    - `--name`: Deployment name (default: project name)
    - `--cron`: Cron schedule (e.g., `"0 2 * * *"`)
    - `--interval`: Interval in seconds
    - `--task-mapping`: `per_node` or `pipeline` (default: `per_node`)
    - `--parallel/--no-parallel`
    - `--env`: Environment name
    - `--profile`: Profile path
  - `seeknal prefect deploy` — Create a deployment on Prefect Cloud/Server
    - Same args as serve + `--work-pool` (required)
    - `--image`: Docker image for remote execution
  - `seeknal prefect generate` — Generate a `prefect.yaml` config file
    - Outputs a ready-to-use `prefect.yaml` for `prefect deploy` CLI

### 3. `tests/test_prefect_integration.py` — **New test file**
- Test `SeeknalPrefectFlow.build()` with mock DAGBuilder
- Test per-node and pipeline task mapping modes
- Test CLI commands with CliRunner
- Test artifact generation
- Test graceful behavior when Prefect not installed

## Implementation Details

### Per-node task mapping (default)
```python
@flow(name="seeknal-{project_name}", log_prints=True)
def seeknal_pipeline(project_path, ...):
    dag = DAGBuilder(project_path).build()
    order = dag.topological_sort()

    futures = {}
    for node_id in order:
        upstream = dag.get_upstream(node_id)
        wait_for = [futures[dep] for dep in upstream if dep in futures]
        futures[node_id] = run_seeknal_node.submit(
            project_path, node_id, wait_for=wait_for
        )

    # Collect results, create artifact
    results = {nid: f.result() for nid, f in futures.items()}
    _create_results_artifact(results)
    return results
```

### Pipeline task mapping (simple)
```python
@flow(name="seeknal-{project_name}", log_prints=True)
def seeknal_pipeline(project_path, ...):
    result = run_seeknal_pipeline.submit(project_path, parallel, ...)
    return {"pipeline": result.result()}
```

### Artifact reporting
After execution, create a Prefect markdown artifact summarizing:
- Node name, status, duration, row count
- Errors (if any)
- Link to Seeknal run state

### CLI generate command
Outputs `prefect.yaml`:
```yaml
deployments:
  - name: seeknal-my-project
    entrypoint: seeknal.workflow.prefect_integration:seeknal_run_flow
    parameters:
      project_path: /path/to/project
      parallel: true
      task_mapping: per_node
    schedules:
      - cron: "0 2 * * *"
    work_pool:
      name: default
```

## Step-by-step execution plan

1. Rewrite `prefect_integration.py` with `SeeknalPrefectFlow` class
2. Add per-node and pipeline flow builders
3. Add artifact reporting
4. Add CLI commands in `cli/main.py`
5. Add `prefect.yaml` generation
6. Write tests
7. Update existing docstrings/examples

## What we WON'T do (keep it simple)
- No custom Prefect blocks or infrastructure blocks
- No Prefect events/automations integration
- No custom Prefect UI extensions
- No Prefect variables/secrets integration (use Seeknal's own profiles)
