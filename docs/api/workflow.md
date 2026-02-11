# Workflow API Reference

This section documents the workflow modules that provide advanced data pipeline capabilities including interval tracking, change detection, state management, and distributed execution.

## Module: `seeknal.workflow.intervals`

Interval tracking for time-series incremental processing with cron-based scheduling.

### Classes

#### `IntervalCalculator`

Calculate time intervals for incremental data processing.

```python
from seeknal.workflow.intervals import IntervalCalculator

calculator = IntervalCalculator()
intervals = calculator.calculate_intervals(
    start_date="2024-01-01",
    end_date="2024-01-31",
    schedule="@daily"
)
```

**Methods:**

- `calculate_intervals(start_date, end_date, schedule)` - Calculate intervals based on cron schedule
- `get_next_interval(current_interval, schedule)` - Get next interval in sequence
- `get_completed_intervals(run_state)` - Get intervals that have been completed

### Functions

#### `parse_cron_schedule(schedule: str) -> CronSchedule`

Parse a cron expression or interval shorthand into a schedule object.

**Parameters:**
- `schedule` - Cron expression (e.g., `0 2 * * *`) or shorthand (e.g., `@daily`)

**Returns:** `CronSchedule` object

**Example:**
```python
from seeknal.workflow.intervals import parse_cron_schedule

schedule = parse_cron_schedule("@daily")
# or
schedule = parse_cron_schedule("0 2 * * *")  # Daily at 2am
```

---

## Module: `seeknal.dag.diff`

SQL-aware change detection for efficient incremental rebuilds.

### Classes

#### `ManifestDiff`

Compare two manifests and detect changes with categorization.

```python
from seeknal.dag.diff import ManifestDiff
from seeknal.dag.manifest import Manifest

old_manifest = Manifest.load("manifest.old.json")
new_manifest = Manifest.load("manifest.new.json")

diff = ManifestDiff.compare(old_manifest, new_manifest)
if diff.has_changes():
    to_rebuild = diff.get_nodes_to_rebuild(new_manifest)
```

**Methods:**

- `compare(old, new)` - Compare two manifests
- `has_changes()` - Return True if any changes detected
- `get_nodes_to_rebuild(manifest)` - Get dict of node_id -> change category
- `get_categorized_changes()` - Get changes grouped by category

#### `ChangeCategory`

Enum representing change impact categories.

```python
from seeknal.dag.diff import ChangeCategory

# Categories
ChangeCategory.BREAKING      # Requires downstream rebuild
ChangeCategory.NON_BREAKING  # Only this node rebuilds
ChangeCategory.METADATA      # No rebuild needed
```

### Functions

#### `classify_sql_change(old_sql: str, new_sql: str) -> ChangeCategory`

Classify SQL changes as breaking or non-breaking.

**Parameters:**
- `old_sql` - Original SQL query
- `new_sql` - Modified SQL query

**Returns:** `ChangeCategory` enum value

---

## Module: `seeknal.state.backend`

Pluggable state backend protocol for distributed execution.

### Classes

#### `StateBackend` (Protocol)

Protocol for state backend implementations.

**Methods:**

- `initialize()` - Setup backend storage
- `create_run(run_id, **kwargs)` - Create new run record
- `update_run(run_id, **kwargs)` - Update run status
- `get_run(run_id)` - Retrieve run info
- `set_node_state(run_id, node_id, **kwargs)` - Update node execution state
- `get_node_state(run_id, node_id)` - Retrieve node state
- `add_completed_interval(run_id, node_id, start, end)` - Track processed interval
- `transaction()` - Begin transaction context
- `acquire_lock(run_id, timeout_ms)` - Acquire optimistic lock

#### `FileBackend`

File-based state backend using JSON storage.

```python
from seeknal.state.backend import create_state_backend

backend = create_state_backend("file", base_path="target")
backend.initialize()
```

#### `DatabaseBackend`

Database-based state backend using SQLite/Turso.

```python
from seeknal.state.backend import create_state_backend

backend = create_state_backend("database", db_path="target/state.db")
backend.initialize()
```

### Functions

#### `create_state_backend(backend_type: str, **kwargs) -> StateBackend`

Factory function to create state backend instances.

**Parameters:**
- `backend_type` - Either `"file"` or `"database"`
- `**kwargs` - Backend-specific configuration

**Returns:** `StateBackend` instance

**Example:**
```python
from seeknal.state.backend import create_state_backend

# File backend
file_backend = create_state_backend("file", base_path="target")

# Database backend
db_backend = create_state_backend("database", db_path="target/state.db")
```

---

## Module: `seeknal.workflow.environment`

Environment management for plan/apply workflow with isolated deployments.

### Classes

#### `EnvironmentManager`

Manage isolated environments for testing changes.

```python
from seeknal.workflow.environment import EnvironmentManager

manager = EnvironmentManager(target_path="target")

# Create plan
manifest = Manifest.load("manifest.json")
plan = manager.plan("dev", manifest)

# Apply plan
result = manager.apply("dev")

# Promote to production
manager.promote("dev", "prod")
```

**Methods:**

- `plan(env_name, manifest)` - Create plan for environment
- `apply(env_name, parallel=True)` - Execute plan in environment
- `promote(source_env, target_env)` - Promote environment to target
- `list_environments()` - List all environments
- `delete(env_name)` - Delete environment
- `cleanup_expired()` - Remove expired environments

#### `Environment`

Represents an isolated environment.

**Attributes:**
- `name` - Environment name
- `status` - One of: `planned`, `applied`, `promoted`
- `created_at` - Timestamp of creation
- `ttl_hours` - Time-to-live in hours
- `plan` - Associated execution plan

---

## Module: `seeknal.workflow.prefect_integration`

Prefect integration for distributed execution with scheduled runs.

### Functions

#### `seeknal_run_flow(project_path, parallel=True, node_ids=None) -> Dict[str, PrefectRunResult]`

Execute Seeknal pipeline as Prefect flow.

**Parameters:**
- `project_path` - Path to Seeknal project
- `parallel` - Execute nodes in parallel
- `node_ids` - Optional list of specific nodes to run

**Returns:** Dictionary of node_id -> `PrefectRunResult`

**Example:**
```python
from seeknal.workflow.prefect_integration import seeknal_run_flow

results = seeknal_run_flow(
    project_path="/path/to/project",
    parallel=True
)
```

#### `seeknal_backfill_flow(project_path, node_id, start_date, end_date, schedule="@daily", parallel=True) -> Dict[str, PrefectRunResult]`

Backfill historical data across intervals.

**Parameters:**
- `project_path` - Path to Seeknal project
- `node_id` - Node to backfill
- `start_date` - Start date (ISO format)
- `end_date` - End date (ISO format)
- `schedule` - Interval schedule (default: `@daily`)
- `parallel` - Execute intervals in parallel

**Returns:** Dictionary of interval -> `PrefectRunResult`

**Example:**
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

#### `create_prefect_deployment(flow_name, deployment_name, project_path, schedule, tags=None)`

Deploy flow to Prefect for scheduled execution.

**Parameters:**
- `flow_name` - Name of the flow
- `deployment_name` - Name of the deployment
- `project_path` - Path to Seeknal project
- `schedule` - Cron schedule for deployment
- `tags` - Optional list of tags for organization

**Example:**
```python
from seeknal.workflow.prefect_integration import create_prefect_deployment

create_prefect_deployment(
    flow_name="seeknal-run-pipeline",
    deployment_name="daily-etl",
    project_path="/path/to/project",
    schedule="0 2 * * *"  # Daily at 2am
)
```

### Classes

#### `PrefectRunResult`

Result of a Prefect task execution.

**Attributes:**
- `success` - Boolean indicating success
- `exit_code` - Process exit code
- `stdout` - Standard output
- `stderr` - Standard error
- `duration_seconds` - Execution duration

---

## CLI Commands

### Interval Commands

```bash
# Show completed intervals
seeknal intervals show

# List pending intervals
seeknal intervals pending --schedule @daily

# Mark interval as complete
seeknal intervals complete --interval "2024-01-01"
```

### Plan Commands

```bash
# Create plan for environment
seeknal plan dev

# Show plan summary
seeknal plan dev --summary

# Export plan to file
seeknal plan dev --output plan.json
```

### Environment Commands

```bash
# Apply plan to environment
seeknal env apply dev

# Apply with parallel execution
seeknal env apply dev --parallel

# Promote environment
seeknal env promote dev prod

# List environments
seeknal env list

# Delete environment
seeknal env delete dev

# Cleanup expired environments
seeknal env cleanup
```

### State Migration Commands

```bash
# Migrate state to new backend (dry run)
seeknal migrate-state --backend database

# Execute migration
seeknal migrate-state --backend database --no-dry-run

# Show migration summary
seeknal migrate-state --backend database --summary
```

---

## Best Practices

### 1. Use Intervals for Incremental Processing

```python
# Calculate intervals for incremental processing
calculator = IntervalCalculator()
intervals = calculator.get_pending_intervals(
    last_completed="2024-01-01",
    schedule="@daily",
    end_date="2024-01-31"
)

# Process each interval
for interval in intervals:
    process_interval(interval)
```

### 2. Review Changes Before Execution

```python
# Always check changes first
diff = ManifestDiff.compare(old_manifest, new_manifest)
if diff.has_changes():
    # Review impact
    to_rebuild = diff.get_nodes_to_rebuild(new_manifest)
    print(f"Nodes to rebuild: {len(to_rebuild)}")
```

### 3. Use Environments for Safe Deployments

```python
# Create plan in dev environment
plan = manager.plan("dev", manifest)

# Review plan before applying
print(plan.summary)

# Apply only after review
result = manager.apply("dev")

# Test thoroughly before promotion
pytest tests/integration/ --env dev

# Promote only after validation
manager.promote("dev", "prod")
```

### 4. Use Database Backend for Production

```python
# Production: Use database backend for concurrent access
backend = create_state_backend("database", db_path="target/state.db")

# Development: File backend is sufficient
backend = create_state_backend("file", base_path="target")
```

### 5. Deploy Prefect Flows for Scheduled Execution

```python
# Create deployment for scheduled runs
create_prefect_deployment(
    flow_name="daily-pipeline",
    deployment_name="production",
    project_path="/path/to/project",
    schedule="0 2 * * *",  # Daily at 2am
    tags=["etl", "production"]
)
```

---

## See Also

- [Interval Tracking Guide](../guides/interval-tracking.md) - Complete interval tracking documentation
- [Change Detection Guide](../guides/change-detection.md) - Change detection features
- [Plan/Apply Workflow Guide](../guides/plan-apply-workflow.md) - Environment management
- [State Backends Guide](../guides/state-backends.md) - State backend configuration
- [Distributed Execution Guide](../guides/distributed-execution.md) - Prefect integration
