# DAG Runner - Workflow Execution Orchestrator

## Overview

The `DAGRunner` is the main orchestrator for executing Seeknal 2.0 DAGs. It provides:

- **Change Detection**: Only runs nodes that changed (and downstream dependencies)
- **Topological Execution**: Executes nodes in dependency order
- **State Management**: Tracks execution results for caching
- **Error Handling**: Continue-on-error and retry support
- **Progress Reporting**: Real-time execution status
- **Flexible Filtering**: Run by type, tags, or specific nodes

## Quick Start

```python
from seeknal.dag.manifest import Manifest
from seeknal.workflow.runner import DAGRunner, print_summary

# Load manifest
manifest = Manifest.load("target/manifest.json")

# Create runner
runner = DAGRunner(manifest)

# Run only changed nodes
summary = runner.run()

# Print summary
print_summary(summary)
```

## Core Concepts

### Execution Modes

1. **Incremental (default)**: Only run changed nodes + downstream
2. **Full run**: Run all nodes regardless of state (`--full`)
3. **Dry run**: Show plan without executing (`--dry-run`)

### Node Status

- `PENDING`: Node is waiting to run
- `RUNNING`: Node is currently executing
- `SUCCESS`: Node completed successfully
- `FAILED`: Node execution failed
- `CACHED`: Node was already built (state cached)
- `SKIPPED`: Node was excluded by filters

## API Reference

### DAGRunner

```python
class DAGRunner:
    def __init__(
        self,
        manifest: Manifest,
        old_manifest: Optional[Manifest] = None,
        state_dir: Optional[Path] = None,
    ):
        """Initialize runner.

        Args:
            manifest: Current manifest to execute
            old_manifest: Previous manifest for change detection
            state_dir: Directory for state storage (default: target/state)
        """
```

### run()

Execute the DAG with various options:

```python
def run(
    self,
    full: bool = False,              # Run all nodes
    dry_run: bool = False,            # Show plan only
    node_types: Optional[List[str]] = None,    # Filter by type
    nodes: Optional[List[str]] = None,         # Run specific nodes
    exclude_tags: Optional[List[str]] = None,  # Exclude tags
    continue_on_error: bool = False,  # Continue on failure
    retry: int = 0,                   # Retry failed nodes N times
) -> ExecutionSummary:
```

### print_plan()

Show execution plan without running:

```python
runner.print_plan(
    full=False,
    node_types=None,
    nodes=None,
    exclude_tags=None,
)
```

Output:
```
ℹ Execution Plan:
ℹ ============================================================
   1. RUN source.users [source]
   2. RUN transform.clean_users [transform]
   3. SKIP feature_group.old_features [deprecated]
ℹ ============================================================
```

## Usage Examples

### Example 1: Run Changed Nodes Only

```python
# Load old and new manifests
old_manifest = Manifest.load("target/manifest.json.old")
new_manifest = Manifest.load("target/manifest.json")

# Create runner with change detection
runner = DAGRunner(new_manifest, old_manifest=old_manifest)

# Run only what changed
summary = runner.run()
print_summary(summary)
```

### Example 2: Full Run

```python
# Run all nodes regardless of state
runner = DAGRunner(manifest)
summary = runner.run(full=True)
print_summary(summary)
```

### Example 3: Filter by Node Type

```python
# Run only feature_groups
runner = DAGRunner(manifest)
summary = runner.run(
    full=True,
    node_types=["feature_group"]
)
```

Supported types:
- `source`
- `transform`
- `feature_group`
- `model`
- `aggregation`
- `rule`
- `exposure`

### Example 4: Run Specific Nodes

```python
# Run specific nodes (will also run downstream dependencies)
runner = DAGRunner(manifest)
summary = runner.run(
    nodes=["user_features", "order_features"]
)
```

### Example 5: Exclude Tags

```python
# Skip nodes tagged as 'expensive' or 'experimental'
runner = DAGRunner(manifest)
summary = runner.run(
    full=True,
    exclude_tags=["expensive", "experimental"]
)
```

### Example 6: Continue on Error

```python
# Keep going even if some nodes fail
runner = DAGRunner(manifest)
summary = runner.run(
    full=True,
    continue_on_error=True,
    retry=2,  # Retry failed nodes twice
)
print_summary(summary)
```

### Example 7: Dry Run

```python
# Show what would be executed
runner = DAGRunner(manifest)

# Just show the plan
runner.print_plan(full=True)

# Or dry run the execution
summary = runner.run(full=True, dry_run=True)
print_summary(summary)
```

## Execution Summary

The `ExecutionSummary` contains:

```python
@dataclass
class ExecutionSummary:
    total_nodes: int        # Total nodes in DAG
    changed_nodes: int      # Nodes that changed
    cached_nodes: int       # Nodes from cache
    successful_nodes: int   # Nodes that succeeded
    failed_nodes: int       # Nodes that failed
    skipped_nodes: int      # Nodes excluded by filters
    total_duration: float   # Execution time in seconds
    results: List[NodeResult]  # Per-node results
```

### Example Summary Output

```
ℹ ============================================================
ℹ Execution Summary
ℹ ============================================================
  Total nodes:     15
  Changed nodes:   3
  Cached nodes:    10
  Successful:      3
  Failed:          0
  Duration:        5.23s
ℹ ============================================================
```

## State Management

The runner maintains execution state in `target/state/execution_state.json`:

```json
{
  "source.users": {
    "status": "success",
    "duration": 0.52,
    "row_count": 1000,
    "last_run": 1706500000.0
  },
  "transform.clean_users": {
    "status": "success",
    "duration": 1.23,
    "row_count": 950,
    "last_run": 1706500001.0
  }
}
```

This state enables:
- **Caching**: Skip nodes that haven't changed
- **Incremental builds**: Only run what's needed
- **Resume capability**: Recover from failures

## Change Detection

The runner uses `ManifestDiff` to detect changes:

1. **Added nodes**: New nodes in the DAG
2. **Removed nodes**: Nodes that no longer exist
3. **Modified nodes**: Nodes with changed config
4. **Added edges**: New dependencies
5. **Removed edges**: Removed dependencies

Changed nodes and all their downstream dependencies are rebuilt.

## Topological Ordering

The runner uses **Kahn's algorithm** to determine execution order:

1. Find all nodes with no dependencies (in-degree = 0)
2. Execute those nodes
3. Remove their outgoing edges
4. Repeat until all nodes are executed

This ensures that dependencies are always built before their dependents.

## Error Handling

### Stop on First Error (default)

```python
summary = runner.run(full=True)
# Execution stops at first failure
```

### Continue on Error

```python
summary = runner.run(
    full=True,
    continue_on_error=True
)
# Keeps running after failures
```

### Retry Failed Nodes

```python
summary = runner.run(
    full=True,
    retry=3  # Retry up to 3 times
)
```

## Progress Reporting

The runner provides real-time progress updates:

```
ℹ 1/15: source.users [RUNNING]
✓ users completed in 0.52s
ℹ 2/15: transform.clean_users [RUNNING]
✓ clean_users completed in 1.23s
ℹ 3/15: feature_group.user_features [RUNNING]
✓ user_features completed in 2.15s
...
```

## CLI Integration

The runner integrates with the Seeknal CLI:

```bash
# Run changed nodes only
seeknal run

# Run all nodes
seeknal run --full

# Dry run
seeknal run --dry-run

# Filter by type
seeknal run --types feature_group

# Run specific nodes
seeknal run --nodes user_features,order_features

# Exclude tags
seeknal run --exclude-tags expensive,experimental

# Continue on error
seeknal run --continue-on-error

# Retry failed nodes
seeknal run --retry 3
```

## Performance Considerations

### Caching

Nodes are cached based on:
- Node ID (name + type)
- Configuration hash
- Dependency state

Cached nodes are skipped in incremental runs.

### Parallel Execution

Currently, nodes execute **sequentially** in topological order.

Future versions may support:
- Parallel execution of independent nodes
- Configurable parallelism level
- Resource-aware scheduling

### State Size

For large DAGs (1000+ nodes), consider:
- Partitioning into sub-DAGs
- Using incremental builds
- Cleaning old state periodically

## Best Practices

### 1. Use Incremental Builds

```python
# Good: Only run what changed
runner = DAGRunner(new_manifest, old_manifest=old_manifest)
summary = runner.run()

# Avoid: Full run when not needed
summary = runner.run(full=True)
```

### 2. Tag Expensive Nodes

```yaml
kind: feature_group
name: large_aggregation
tags:
  - expensive
  - requires_spark
```

Then skip them during development:

```python
summary = runner.run(
    full=True,
    exclude_tags=["expensive"]
)
```

### 3. Use Dry Run for Validation

```python
# Check what will run before executing
runner.print_plan(full=True)

# Or dry run to validate
summary = runner.run(full=True, dry_run=True)
```

### 4. Handle Errors Gracefully

```python
# In production: Continue on error with retries
summary = runner.run(
    full=True,
    continue_on_error=True,
    retry=3
)

# Check results
if summary.failed_nodes > 0:
    for result in summary.results:
        if result.status == ExecutionStatus.FAILED:
            print(f"Failed: {result.node_id} - {result.error_message}")
```

### 5. Monitor State Size

```python
import os
state_file = "target/state/execution_state.json"
size_mb = os.path.getsize(state_file) / (1024 * 1024)

if size_mb > 10:
    print(f"Warning: State file is {size_mb:.1f}MB")
    # Consider cleanup
```

## Troubleshooting

### Issue: Nodes Not Running

**Symptom**: `No changes detected. Nothing to run.`

**Solutions**:
- Use `--full` flag to run all nodes
- Check that `old_manifest` is provided for change detection
- Verify nodes have actual changes

### Issue: Cycle Detected

**Symptom**: `ValueError: DAG contains cycle: source.users -> transform.clean_users -> source.users`

**Solutions**:
- Check for circular dependencies in YAML
- Use `seeknal parse` to validate DAG
- Review edges in manifest.json

### Issue: State Corruption

**Symptom**: Nodes marked as cached but actually failed

**Solutions**:
- Delete `target/state/execution_state.json`
- Run with `--full` to rebuild
- Check file permissions

### Issue: Slow Execution

**Symptom**: Execution takes too long

**Solutions**:
- Use incremental builds (default)
- Exclude expensive nodes with tags
- Filter by node type
- Check for I/O bottlenecks

## Advanced Topics

### Custom Executors

Extend the runner with custom node executors:

```python
def execute_custom_node(node: Node) -> Dict[str, Any]:
    """Execute a custom node type."""
    # Your custom logic here
    return {
        "row_count": 100,
        "metadata": {"custom": True}
    }

# Register in runner._execute_by_type()
```

### State Backends

Replace file-based state with custom backend:

```python
class CustomStateBackend:
    def load(self) -> Dict:
        # Load from database, S3, etc.
        pass

    def save(self, state: Dict) -> None:
        # Save to database, S3, etc.
        pass
```

### Event Hooks

Add hooks for execution events:

```python
class DAGRunnerWithHooks(DAGRunner):
    def _execute_node(self, node_id: str, dry_run: bool = False):
        # Before execution
        self.on_node_start(node_id)

        result = super()._execute_node(node_id, dry_run)

        # After execution
        self.on_node_complete(node_id, result)

        return result
```

## See Also

- [DAG Parser](./parser.md) - Building manifests from YAML
- [Manifest Diff](./diff.md) - Change detection
- [CLI Commands](./cli.md) - Command-line interface
- [Examples](../examples/) - Complete examples
