# State Backends Guide

## Overview

State backends provide pluggable storage for execution state, supporting both file-based and database storage with atomic transactions and locking.

## Available Backends

### File Backend (Default)

- **Path**: `target/run_state.json`
- **Use case**: Single-node deployments, development
- **Pros**: Simple, no dependencies
- **Cons**: Not suitable for distributed execution

### Database Backend

- **Path**: `target/state.db` (SQLite/Turso)
- **Use case**: Production, distributed execution
- **Pros**: Concurrent access, transactional integrity
- **Cons**: Requires database setup

## Configuration

Set backend in `seeknal_project.yml`:

```yaml
name: my_project
version: 1.0.0
state_backend: file  # Options: file, database
```

Or via environment variable:

```bash
export SEEKNAL_STATE_BACKEND=database
```

## File Backend

Default storage using JSON files:

```json
{
  "run_id": "run_20240110_120000",
  "started_at": "2024-01-10T12:00:00Z",
  "nodes": {
    "my_node": {
      "run_id": "run_20240110_120000",
      "node_id": "my_node",
      "status": "success",
      "duration_ms": 1500,
      "row_count": 1000,
      "fingerprint": "abc123..."
    }
  }
}
```

## Database Backend

SQLite or Turso-based storage with tables:

- `runs`: Run metadata
- `node_states`: Node execution status
- `completed_intervals`: Interval tracking
- `partitions`: Partition tracking

### Using SQLite

```yaml
state_backend: database
# Automatically uses target/state.db
```

### Using Turso (Remote)

```yaml
state_backend: database
database_url: "libsql://token@database-name.turso.io"
```

## Migration

### Migrate File to Database

```bash
# Preview migration
seeknal migrate-state --backend database

# Execute migration
seeknal migrate-state --backend database --no-dry-run
```

This preserves:
- ✅ All run history
- ✅ Node execution states
- ✅ Completed intervals
- ✅ Fingerprints
- ✅ Row counts

### Rollback

Backup created automatically at `run_state.json.bak`.

To rollback:

```bash
# Restore from backup
cp target/run_state.json.bak target/run_state.json
```

## API Usage

### Using the Backend Protocol

```python
from seeknal.state.backend import create_state_backend

# Create file backend
file_backend = create_state_backend("file", base_path="target")
file_backend.initialize()

# Create database backend
db_backend = create_state_backend("database", db_path="target/state.db")
db_backend.initialize()

# Use unified interface
backend.create_run("run_1", status="running", started_at=datetime.now())
backend.set_node_state("run_1", "node1", status="success")
backend.add_completed_interval("run_1", "node1", start, end)
```

### Transactions

```python
# Atomic state updates
with backend.transaction() as tx:
    backend.set_node_state("run_1", "node1", status="success")
    backend.set_node_state("run_1", "node2", status="success")
    # Both succeed or both fail
```

### Locking

```python
# Acquire lock for concurrent execution
acquired = backend.acquire_lock("run_1", timeout_ms=5000)
if acquired:
    try:
        # Execute with lock held
        backend.set_node_state("run_1", "node1", status="running")
    finally:
        # Lock released automatically
        pass
```

## Best Practices

### 1. Use Database for Production

```yaml
# Production
state_backend: database
```

### 2. Use File for Development

```yaml
# Development
state_backend: file
```

### 3. Backup Before Migration

```bash
# Always backup first
cp target/run_state.json target/run_state.json.pre-migration.bak

# Then migrate
seeknal migrate-state --backend database
```

### 4. Test Migration in Dry Run

```bash
# Preview first
seeknal migrate-state --backend database

# Check output, then execute
seeknal migrate-state --backend database --no-dry-run
```

## Troubleshooting

### Migration Failed

```bash
# Check backup
ls -la target/*.bak

# Restore backup
cp target/run_state.json.bak target/run_state.json

# Investigate error
seeknal migrate-state --backend database --dry-run
```

### Database Locked

```bash
# SQLite: Check for locks
sqlite3 target/state.db "PRAGMA lock_status"

# Close other processes using the database
```

### Performance Issues

For large state files:

1. **Migrate to database** - Better concurrent access
2. **Clean old runs** - `seeknal state cleanup --days 30`
3. **Use partitions** - Enable partition tracking

## Advanced

### Custom Backend

Implement the `StateBackend` protocol:

```python
from seeknal.state.backend import StateBackend

class CustomBackend(StateBackend):
    def initialize(self):
        # Setup
        pass

    def create_run(self, run_id, **kwargs):
        # Create run
        pass

    # Implement all abstract methods...
```

Register and use:

```python
from seeknal.state.backend import StateBackendFactory

StateBackendFactory.register("custom", CustomBackend)
backend = create_state_backend("custom", option="value")
```

## Reference

### Backend Protocol Methods

- `initialize()` - Setup backend storage
- `create_run()` - Create new run record
- `update_run()` - Update run status
- `get_run()` - Retrieve run info
- `set_node_state()` - Update node execution state
- `get_node_state()` - Retrieve node state
- `add_completed_interval()` - Track processed interval
- `transaction()` - Begin transaction context
- `acquire_lock()` - Acquire optimistic lock
