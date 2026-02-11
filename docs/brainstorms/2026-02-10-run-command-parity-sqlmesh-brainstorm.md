# RUN Command Parity with SQLMesh

**Date**: 2026-02-10
**Status**: Brainstorm
**Inspired By**: SQLMesh execution/scheduling architecture analysis
**Related**: `docs/brainstorms/2026-02-08-sqlmesh-inspired-features-brainstorm.md`

---

## What We're Building

Full production-grade `seeknal run` command with feature parity to SQLMesh's execution engine. Focus on interval-based incremental execution, plan/apply workflow, and robust state management while maintaining Seeknal's simplicity.

## Quick Summary: What's Built vs What's New

| Feature | Status | Location |
|---------|--------|----------|
| **Single-machine parallel** | ✅ Built | `src/seeknal/workflow/parallel.py` |
| **Content hashing** | ✅ Built | Phase 2 spec |
| **Change detection** | ✅ Built | Phase 2 spec |
| **Interval tracking** | ❌ New | This doc (Phase 1) |
| **Backfill support** | ❌ New | This doc (Phase 1) |
| **Plan/Apply workflow** | ❌ New | This doc (Phase 3) |
| **Change categorization** | ❌ New | This doc (Phase 2) |
| **Database state backend** | ❌ New | This doc (Phase 4) |
| **Distributed workers** | ❌ New | This doc (Phase 5) |

**Key Point**: Single-machine parallel execution (`seeknal run --parallel`) is **already implemented**. This brainstorm adds **NEW capabilities**: interval tracking, change categorization, plan/apply, database state, and distributed multi-machine workers.

## Why RUN Parity Matters

Seeknal's current `seeknal run` has solid foundations (content hashing, caching, parallel execution) but lacks critical production features:

1. **No time-series incremental processing** - Sources run fully every time, no interval tracking
2. **No backfill support** - Can't reprocess missing or failed time ranges
3. **No plan/apply workflow** - No preview before execution, no approval prompt
4. **No change categorization** - Can't distinguish breaking vs non-breaking changes
5. **File-based state only** - No support for distributed runners (Phase 5)

These gaps prevent Seeknal from being a production data pipeline orchestrator.

---

## Current State: Seeknal vs SQLMesh

### What Seeknal Already Has

| Feature | Implementation |
|---------|----------------|
| **Content hashing** | SHA256 of functional YAML content (excludes metadata) |
| **Incremental execution** | Skip nodes with unchanged content hash |
| **Parallel execution** | `--parallel` flag with topological DAG ordering |
| **State persistence** | `target/run_state.json` with atomic writes |
| **Output caching** | `target/cache/{kind}/{node_name}.parquet` |
| **Status tracking** | PENDING, RUNNING, SUCCESS, FAILED, SKIPPED, CACHED |

### What SQLMesh Has That Seeknal Doesn't

| Feature | SQLMesh | Seeknal Gap |
|---------|---------|-------------|
| **Interval tracking** | `(start_ts, end_ts)` tuples per snapshot | ❌ None |
| **Backfill calculator** | `missing_intervals()` finds gaps | ❌ None |
| **Plan/Apply workflow** | Interactive preview before execution | ❌ Only dry-run |
| **Change categorization** | BREAKING, NON_BREAKING, FORWARD_ONLY, METADATA | ❌ None |
| **StateSync backend** | Database-backed state | ❌ JSON file only |
| **Cron scheduling** | Native cron support | ❌ None |
| **Restatement** | Mark intervals for reprocessing | ❌ None |
| **Environment promotion** | Virtual environments | ❌ None |

---

## Proposed Architecture

### 1. Hybrid State Storage

**Decision**: Support both file-based and database-backed state with unified API.

```yaml
# seeknal_project.yml
state:
  backend: auto  # auto | file | database
  file:
    path: target/run_state.json
  database:
    url: ${SEEKNAL_STATE_DB}  # SQLite or Turso
    table: run_state
```

**Implementation**:
- Abstract state interface: `StateBackend` protocol
- `FileStateBackend`: Current JSON implementation (default for local dev)
- `DatabaseStateBackend`: New SQL-based backend (for production)
- `AutoBackend`: Uses file if no DB configured, otherwise DB
- Backward compatible: existing `run_state.json` still works

**Benefits**:
- Zero migration needed for existing users
- Production users get distributed state
- Local dev stays simple (no database needed)

### 2. Interval Tracking (Hybrid Model)

**Decision**: Support both time intervals and partition-based tracking.

```yaml
kind: source
name: events
# Time-series interval tracking
schedule: "0 * * * *"  # hourly
time_column: event_time
lookback: 3 days       # reprocess last 3 days each run

# OR partition-based tracking
kind: source
name: daily_snapshots
partition_column: snapshot_date
partition_format: "%Y-%m-%d"
```

**State model extension**:
```python
@dataclass
class NodeState:
    # Existing fields
    hash: str
    last_run: datetime
    status: str

    # NEW: Interval tracking
    completed_intervals: list[tuple[datetime, datetime]]  # Time ranges
    completed_partitions: list[str]  # Partition values (e.g., dates)
    restatement_intervals: list[tuple[datetime, datetime]]  # Marked for reprocessing
```

**Backfill command**:
```bash
# Backfill specific time range
seeknal run --start 2026-01-01 --end 2026-01-31

# Backfill missing intervals only
seeknal run --backfill

# Restate specific intervals
seeknal run --restate source.events --start 2026-01-15 --end 2026-01-16
```

### 3. Plan/Apply Workflow

**Decision**: Add `seeknal plan` command with interactive approval.

```bash
$ seeknal plan --env dev

Analyzing pipeline...
  5 nodes to create
  2 nodes modified (1 breaking, 1 non-breaking)
  12 nodes unchanged (cached)
  Estimated backfill: 3 days (72 intervals)

Changes:

  transform.clean_users [NON_BREAKING]
    ~ SQL changed: added column 'signup_month'
    Downstream impact: none (new column, not used)
    Intervals to backfill: none

  transform.user_metrics [BREAKING]
    ~ SQL changed: removed column 'legacy_score'
    Downstream impact:
      - feature_group.user_features (uses legacy_score) -> REBUILD
      - aggregation.daily_scores (uses legacy_score) -> REBUILD
    Intervals to backfill: 72 (2026-01-01 to 2026-01-31)

  source.events [METADATA]
    ~ description changed
    Downstream impact: none

Apply these changes? [y/N/q]:
```

**Implementation**:
- `seeknal plan` analyzes changes without executing
- Shows categorized changes with downstream impact
- Calculates backfill needs from intervals
- `seeknal apply` executes the approved plan
- `seeknal run --yes` skips prompt (CI/CD friendly)

### 4. Change Categorization

**Decision**: Auto-categorize changes based on SQL AST and lineage.

```python
@dataclass
class ChangeCategory(Enum):
    BREAKING = "breaking"          # Downstream rebuild needed
    NON_BREAKING = "non_breaking"  # Only this node
    FORWARD_ONLY = "forward_only"  # Append-only, no backfill
    METADATA = "metadata"          # No data impact
```

**Classification rules**:
- Column added + not used downstream → NON_BREAKING
- Column removed + used downstream → BREAKING
- Column removed + NOT used downstream → NON_BREAKING
- SQL logic changed → BREAKING (conservative)
- Config-only (description, tags) → METADATA
- Sort order changed → NON_BREAKING
- Filter/where changed → BREAKING

**Integration with plan**:
```python
def categorize_change(old_state: NodeState, new_state: NodeState) -> ChangeCategory:
    # Compare schema hashes
    if old_state.schema_hash != new_state.schema_hash:
        # Schema changed - check lineage for column usage
        if column_removed_and_used_downstream():
            return ChangeCategory.BREAKING
        return ChangeCategory.NON_BREAKING

    # Compare content hashes
    if old_state.content_hash != new_state.content_hash:
        return ChangeCategory.BREAKING

    # Only metadata changed
    return ChangeCategory.METADATA
```

### 5. Enhanced Run Command

**New CLI interface**:
```bash
# Standard run (incremental with intervals)
seeknal run

# Backfill specific range
seeknal run --start 2026-01-01 --end 2026-01-31

# Run specific nodes
seeknal run --nodes source.events,transform.clean_users

# Dry run (show what would execute)
seeknal run --dry-run

# Force re-run (ignore cache)
seeknal run --force

# Continue on error
seeknal run --continue-on-error

# Check intervals status
seeknal intervals
seeknal intervals source.events

# Promote environment
seeknal promote dev -> prod
```

---

## Implementation Phases

### Phase 1: Interval Tracking (2-3 weeks)

**Goals**: Add interval state tracking and backfill support.

1. Extend `NodeState` with interval fields
2. Add interval calculation from cron schedules
3. Implement backfill logic (find missing intervals)
4. Add `--start/--end` flags to `seeknal run`
5. Add `seeknal intervals` command

**Success criteria**:
```bash
$ seeknal run
Processing source.events:
  - Completed: 2026-01-01 00:00 to 2026-01-10 23:00 (240 intervals)
  - Missing: 2026-01-11 00:00 to 2026-01-11 23:00 (24 intervals)
  - Processing 24 intervals...
```

### Phase 2: Change Categorization (1-2 weeks)

**Goals**: Auto-classify changes and show impact.

1. Build column-level lineage (SQLGlot or sqlparse)
2. Implement change categorization logic
3. Add downstream impact analysis
4. Integrate with `seeknal plan`

**Success criteria**:
```bash
$ seeknal plan
transform.user_features [BREAKING]
  - Removed column: legacy_score
  - Used by: aggregation.daily_metrics, exposure.api
```

### Phase 3: Plan/Apply Workflow (1-2 weeks)

**Goals**: Interactive preview before execution.

1. Create `seeknal plan` command
2. Add approval prompt
3. Implement `seeknal apply`
4. Add environment promotion workflow

**Success criteria**:
```bash
$ seeknal plan
[Shows changes and asks approval]

$ seeknal apply
[Executes approved plan]
```

### Phase 4: Database State Backend (1-2 weeks)

**Goals**: Optional database-backed state for production.

1. Create `StateBackend` protocol
2. Implement `DatabaseStateBackend`
3. Add configuration option
4. Migration tool for JSON → DB

**Success criteria**:
```bash
# Production with database
$ export SEEKNAL_STATE_DB="sqlite:///var/lib/seeknal/state.db"
$ seeknal run  # Uses database state
```

### Phase 5: Distributed Worker Execution (2-3 weeks) 🆕

**Goals**: Enable multi-machine distributed execution for large-scale pipelines.

> **IMPORTANT**: This phase is **NOT** part of the existing Phase 2 spec. Phase 2 already includes single-machine parallel execution (`seeknal run --parallel`) which is **already built** in `src/seeknal/workflow/parallel.py`. Phase 5 adds **new capability**: distributed workers across multiple machines.

**What's Already Built** (Single-Machine):
- ✅ `ParallelDAGRunner` with `ThreadPoolExecutor`
- ✅ Per-thread DuckDB connections
- ✅ Topological layer execution
- ✅ `--parallel` and `--max-workers` flags
- Location: `src/seeknal/workflow/parallel.py`

**What's New** (Multi-Machine):
- ❌ Worker coordination across machines
- ❌ Shared state backend (database)
- ❌ Work claiming protocol
- ❌ Distributed interval processing

**Implementation**:

1. **Work Queue Table**:
```sql
CREATE TABLE work_queue (
    interval_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
    status TEXT DEFAULT 'PENDING',  -- PENDING, RUNNING, COMPLETED, FAILED
    worker_id TEXT,
    claimed_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);
```

2. **Worker CLI**:
```bash
# Start a worker that polls for work
seeknal worker --name worker-1 --state-db $DATABASE_URL

# Worker loop:
# 1. Claim pending intervals (UPDATE ... RETURNING)
# 2. Process interval
# 3. Mark as COMPLETED
# 4. Repeat
```

3. **Work Claiming** (SQL):
```sql
-- Claim up to N intervals atomically
UPDATE work_queue
SET status = 'RUNNING',
    worker_id = :worker_id,
    claimed_at = NOW()
WHERE interval_id IN (
    SELECT interval_id FROM work_queue
    WHERE status = 'PENDING'
    LIMIT 10
    FOR UPDATE SKIP LOCKED  -- Don't wait for locked rows
)
RETURNING *;
```

4. **Backfill Orchestration**:
```bash
# Populate work queue for backfill
seeknal queue-backfill --start 2025-01-01 --end 2025-12-31

# Start multiple workers
seeknal worker --name worker-1 &
seeknal worker --name worker-2 &
seeknal worker --name worker-3 &
```

5. **Monitoring**:
```bash
# Check worker status
seeknal worker-status

# Show work queue
seeknal queue-status --node source.events
```

**Success criteria**:
```bash
# Queue backfill work
$ seeknal queue-backfill --start 2025-01-01 --end 2025-01-31
Queued: 744 intervals (31 days × 24 hours)

# Start 3 workers
$ seeknal worker --name worker-1 &
$ seeknal worker --name worker-2 &
$ seeknal worker --name worker-3 &

# Check progress
$ seeknal queue-status
node_id       | pending | running | completed | failed
--------------|---------|---------|-----------|-------
source.events | 120     | 3       | 621       | 0

# Workers self-organize:
# - Worker 1: 207 intervals completed
# - Worker 2: 208 intervals completed
# - Worker 3: 206 intervals completed
```

**Architecture**:
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Worker 1       │    │  Worker 2       │    │  Worker 3       │
│  (Machine A)    │    │  (Machine B)    │    │  (Machine C)    │
│                 │    │                 │    │                 │
│  seeknal worker │    │  seeknal worker │    │  seeknal worker │
└────────┬────────┘    └────────┬────────┘    └────────┬────────┘
         │                     │                     │
         └──────────┬──────────┴──────────┬──────────┘
                    │                     │
            ┌───────▼─────────────────────▼───────┐
            │     Shared State Database           │
            │     (Turso / PostgreSQL / SQLite)   │
            │                                     │
            │  ┌─────────────────────────────┐   │
            │  │ work_queue table            │   │
            │  │ - interval_id (PK)          │   │
            │  │ - status (PENDING/RUNNING/  │   │
            │  │         COMPLETED/FAILED)   │   │
            │  │ - worker_id                 │   │
            │  │ - claimed_at                │   │
            │  └─────────────────────────────┘   │
            └─────────────────────────────────────┘
```

**Use Cases**:
- Large backfills (months/years of data)
- Kubernetes deployments (multiple pods)
- CI/CD parallel matrix jobs
- Multi-region deployments

**Complexity**: HIGH (distributed systems challenges: race conditions, worker failures, duplicate work prevention)

---

## Clarification: Parallel vs Distributed

| Aspect | Single-Machine Parallel (Built) | Distributed Workers (New) |
|--------|--------------------------------|--------------------------|
| **Status** | ✅ Already in `parallel.py` | ❌ New feature (Phase 5) |
| **Coordination** | Single process, threads | Multiple processes/machines |
| **State storage** | Local file `run_state.json` | Shared database |
| **Locking** | `threading.Lock()` | Database row locks |
| **Scalability** | Limited to one machine | Scales to N machines |
| **Use case** | Local dev, small pipelines | Production backfills |
| **CLI command** | `seeknal run --parallel` | `seeknal worker` |
| **Spec status** | Phase 2 (already done) | Phase 5 (new) |

---

## Key Decisions Made

1. **Hybrid state storage** - File for local dev, database for production, unified API
2. **Hybrid interval tracking** - Time intervals for time-series, partitions for batch
3. **Conservative change categorization** - Default to BREAKING when uncertain
4. **Interactive opt-in** - Plan/apply is optional, `seeknal run` works as before
5. **Backward compatibility** - Existing `run_state.json` continues to work
6. **SQLGlot for parsing** - Use SQLGlot for lineage (same as SQLMesh)

---

## Open Questions

1. **Interval granularity**: Should intervals be per-node or global? (Decision: per-node for flexibility)
2. **State backend migration**: Automatic or manual migration from JSON to DB? (Decision: manual `seeknal migrate-state` command)
3. **Partition format**: How to specify partition patterns? (Decision: strftime format strings)
4. **Backfill parallelism**: Should backfill intervals run in parallel? (Decision: yes, with `--workers` flag)
5. **Restatement syntax**: `seeknal run --restate` or separate command? (Decision: flag on run command)
6. **Environment isolation**: File-based or database-backed environments? (Decision: defer to separate environments feature)
7. **Cron parsing**: Use python-cron or custom parser? (Decision: python-cron for compatibility)

---

## Complexity Assessment

| Phase | Complexity | Duration | Risk | Status |
|-------|-----------|----------|------|--------|
| Phase 1: Intervals | HIGH | 2-3 weeks | State migration, interval algebra | New |
| Phase 2: Change categorization | MEDIUM | 1-2 weeks | SQL parsing, lineage graph | New |
| Phase 3: Plan/Apply | MEDIUM | 1-2 weeks | CLI UX, approval workflow | New |
| Phase 4: Database state | MEDIUM | 1-2 weeks | Protocol design, migration | New |
| Phase 5: Distributed workers | HIGH | 2-3 weeks | Distributed systems, race conditions | New |
| **Total (Phases 1-4)** | **HIGH** | **5-9 weeks** | Incremental delivery possible | - |
| **Total (all 5 phases)** | **VERY HIGH** | **7-12 weeks** | Phase 5 optional, depends on Phase 4 | - |

**Note**: Phase 5 (Distributed Workers) is **optional** and can be delivered separately from Phases 1-4. It depends on Phase 4 (Database State Backend) being complete.

---

## Related Features

### Already Built (in Phase 2 spec)

This RUN parity work builds on existing features:

1. **Feature 9 from Phase 2 spec**: Parallel Node Execution
   - ✅ Already implemented in `src/seeknal/workflow/parallel.py`
   - ✅ Single-machine parallel via `ThreadPoolExecutor`
   - ✅ `seeknal run --parallel --max-workers N`
   - Location: Phase 2 Safety & Speed spec

### New in This Brainstorm

These are **new features** beyond the existing Phase 2 spec:

1. **Feature 2 from SQLMesh doc**: Content-based fingerprinting (extended)
   - Add interval tracking to existing fingerprinting
   - NEW: `completed_intervals`, `completed_partitions` fields

2. **Feature 7 from SQLMesh doc**: Scheduled runs with interval tracking
   - NEW: Cron-based interval calculation
   - NEW: Backfill command with `--start/--end`
   - NEW: Interval state persistence

3. **Feature 8 from SQLMesh doc**: Change categorization
   - NEW: Breaking vs Non-Breaking vs Metadata
   - NEW: Downstream impact analysis

4. **Feature 1 from SQLMesh doc**: Virtual environments (enhanced)
   - Extend existing Phase 2 environments with interval state
   - NEW: Environment-aware interval tracking

5. **Phase 5: Distributed Workers** (completely new)
   - NEW: Multi-machine worker coordination
   - NEW: Work queue table and claiming protocol
   - NEW: `seeknal worker` command

---

## Success Metrics

1. **Backfill efficiency**: Only process missing intervals (< 10% overhead)
2. **Change impact accuracy**: > 95% correct categorization
3. **State backend performance**: < 100ms state read/write
4. **Plan command accuracy**: Show exact changes before execution
5. **Backward compatibility**: Existing projects work without changes

---

## Technical Deep Dive: State Backend Protocol

### Overview

The State Backend Protocol provides a unified abstraction over state persistence, allowing Seeknal to work with both file-based storage (local dev) and database-backed storage (production) without code changes.

### Protocol Interface

```python
# src/seeknal/state/backend.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol
from enum import Enum
import threading

class NodeStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    CACHED = "cached"

@dataclass
class NodeFingerprint:
    content_hash: str      # SHA256 of functional content
    schema_hash: str       # SHA256 of output schema
    upstream_hash: str     # Combined upstream hashes
    config_hash: str       # SHA256 of config

@dataclass
class NodeState:
    node_id: str
    hash: str
    last_run: datetime
    status: NodeStatus
    duration_ms: int
    row_count: int
    fingerprint: Optional[NodeFingerprint]

    # NEW: Interval tracking fields
    completed_intervals: list[tuple[datetime, datetime]] = None
    completed_partitions: list[str] = None
    restatement_intervals: list[tuple[datetime, datetime]] = None

@dataclass
class RunMetadata:
    run_id: str
    started_at: datetime
    completed_at: Optional[datetime]
    config: dict

class StateBackend(ABC):
    """Abstract protocol for state persistence backends."""

    @abstractmethod
    def load_run_state(self) -> dict[str, NodeState]:
        """Load all node states."""
        pass

    @abstractmethod
    def save_run_state(self, nodes: dict[str, NodeState], metadata: RunMetadata) -> None:
        """Save all node states atomically."""
        pass

    @abstractmethod
    def get_node_state(self, node_id: str) -> Optional[NodeState]:
        """Get state for a single node."""
        pass

    @abstractmethod
    def update_node_state(self, node_id: str, state: NodeState) -> None:
        """Update state for a single node."""
        pass

    @abstractmethod
    def get_nodes_by_status(self, status: NodeStatus) -> list[str]:
        """Get all node IDs with a given status."""
        pass

    @abstractmethod
    def begin_transaction(self) -> "StateTransaction":
        """Begin a transaction for atomic updates."""
        pass

    @abstractmethod
    def acquire_lock(self, timeout: int = 30) -> bool:
        """Acquire exclusive lock for state updates."""
        pass

    @abstractmethod
    def release_lock(self) -> None:
        """Release exclusive lock."""
        pass

class StateTransaction(ABC):
    """Context manager for transactional state updates."""

    @abstractmethod
    def update_node(self, node_id: str, state: NodeState) -> None:
        """Stage a node state update."""
        pass

    @abstractmethod
    def commit(self) -> None:
        """Commit all staged updates."""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Rollback all staged updates."""
        pass
```

### Implementation: FileStateBackend

```python
# src/seeknal/state/file_backend.py

import json
import tempfile
from pathlib import Path
from typing import Optional
import threading
import fcntl  # Unix file locking

class FileStateBackend(StateBackend):
    """File-based state backend using JSON storage."""

    def __init__(self, state_path: Path):
        self.state_path = state_path
        self._lock = threading.Lock()
        self._file_lock = None

    def load_run_state(self) -> dict[str, NodeState]:
        if not self.state_path.exists():
            return {}

        with open(self.state_path, 'r') as f:
            data = json.load(f)

        return {
            node_id: NodeState(
                node_id=node_id,
                **node_data
            )
            for node_id, node_data in data.get('nodes', {}).items()
        }

    def save_run_state(self, nodes: dict[str, NodeState], metadata: RunMetadata) -> None:
        # Create backup first
        self._create_backup()

        # Write to temp file, then atomic rename
        with tempfile.NamedTemporaryFile(
            mode='w',
            dir=self.state_path.parent,
            prefix='.tmp_',
            suffix='.json',
            delete=False
        ) as tmp:
            json.dump({
                'schema_version': '2.0',
                'seeknal_version': '2.0.0',
                'run_id': metadata.run_id,
                'last_run': metadata.started_at.isoformat(),
                'config': metadata.config,
                'nodes': {
                    node_id: self._serialize_node(state)
                    for node_id, state in nodes.items()
                }
            }, tmp, indent=2)
            tmp_path = Path(tmp.name)

        # Atomic rename
        tmp_path.replace(self.state_path)

    def get_node_state(self, node_id: str) -> Optional[NodeState]:
        states = self.load_run_state()
        return states.get(node_id)

    def update_node_state(self, node_id: str, state: NodeState) -> None:
        # Load-modify-write pattern (not optimal, but safe)
        with self._lock:
            states = self.load_run_state()
            states[node_id] = state
            self.save_run_state(states, RunMetadata(...))

    def get_nodes_by_status(self, status: NodeStatus) -> list[str]:
        states = self.load_run_state()
        return [
            node_id for node_id, state in states.items()
            if state.status == status
        ]

    def begin_transaction(self) -> "FileStateTransaction":
        return FileStateTransaction(self)

    def acquire_lock(self, timeout: int = 30) -> bool:
        """Acquire exclusive file lock using fcntl."""
        self._file_lock = open(self.state_path, 'w')
        try:
            fcntl.flock(self._file_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False

    def release_lock(self) -> None:
        if self._file_lock:
            fcntl.flock(self._file_lock.fileno(), fcntl.LOCK_UN)
            self._file_lock.close()
            self._file_lock = None

    def _create_backup(self) -> None:
        """Create backup before overwriting state."""
        if self.state_path.exists():
            backup_path = self.state_path.parent / f"{self.state_path.name}.bak.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(self.state_path, backup_path)

class FileStateTransaction(StateTransaction):
    """Transaction implementation for file backend."""

    def __init__(self, backend: FileStateBackend):
        self.backend = backend
        self.staged_updates: dict[str, NodeState] = {}
        self.original_states: dict[str, NodeState] = backend.load_run_state()

    def update_node(self, node_id: str, state: NodeState) -> None:
        self.staged_updates[node_id] = state

    def commit(self) -> None:
        new_states = self.original_states | self.staged_updates
        self.backend.save_run_state(new_states, RunMetadata(...))

    def rollback(self) -> None:
        self.staged_updates.clear()
```

### Implementation: DatabaseStateBackend

```python
# src/seeknal/state/database_backend.py

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from contextlib import contextmanager

class DatabaseStateBackend(StateBackend):
    """Database-backed state backend using SQLAlchemy."""

    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema."""
        with Session(self.engine) as session:
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS run_metadata (
                    run_id TEXT PRIMARY KEY,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    config JSON
                )
            """))
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS node_state (
                    node_id TEXT PRIMARY KEY,
                    hash TEXT NOT NULL,
                    last_run TIMESTAMP NOT NULL,
                    status TEXT NOT NULL,
                    duration_ms INTEGER,
                    row_count INTEGER,
                    fingerprint_content_hash TEXT,
                    fingerprint_schema_hash TEXT,
                    fingerprint_upstream_hash TEXT,
                    fingerprint_config_hash TEXT,
                    completed_intervals JSON,  -- Array of [start, end] tuples
                    completed_partitions JSON, -- Array of partition strings
                    restatement_intervals JSON -- Array of [start, end] tuples
                )
            """))
            session.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_node_status ON node_state(status)
            """))
            session.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_node_last_run ON node_state(last_run)
            """))
            session.commit()

    def load_run_state(self) -> dict[str, NodeState]:
        with Session(self.engine) as session:
            result = session.execute(text("""
                SELECT * FROM node_state
            """))
            return {
                row.node_id: self._deserialize_node(row)
                for row in result
            }

    def save_run_state(self, nodes: dict[str, NodeState], metadata: RunMetadata) -> None:
        with Session(self.engine) as session:
            # Insert run metadata
            session.execute(text("""
                INSERT INTO run_metadata (run_id, started_at, config)
                VALUES (:run_id, :started_at, :config)
            """), {
                'run_id': metadata.run_id,
                'started_at': metadata.started_at,
                'config': json.dumps(metadata.config)
            })

            # Upsert all node states
            for node_id, state in nodes.items():
                session.execute(text("""
                    INSERT INTO node_state (
                        node_id, hash, last_run, status, duration_ms, row_count,
                        fingerprint_content_hash, fingerprint_schema_hash,
                        fingerprint_upstream_hash, fingerprint_config_hash,
                        completed_intervals, completed_partitions, restatement_intervals
                    ) VALUES (
                        :node_id, :hash, :last_run, :status, :duration_ms, :row_count,
                        :content_hash, :schema_hash, :upstream_hash, :config_hash,
                        :intervals, :partitions, :restate
                    )
                    ON CONFLICT (node_id) DO UPDATE SET
                        hash = excluded.hash,
                        last_run = excluded.last_run,
                        status = excluded.status,
                        duration_ms = excluded.duration_ms,
                        row_count = excluded.row_count,
                        completed_intervals = excluded.completed_intervals,
                        completed_partitions = excluded.completed_partitions,
                        restatement_intervals = excluded.restatement_intervals
                """), self._serialize_node(state))
            session.commit()

    def get_node_state(self, node_id: str) -> Optional[NodeState]:
        with Session(self.engine) as session:
            result = session.execute(text("""
                SELECT * FROM node_state WHERE node_id = :node_id
            """), {'node_id': node_id}).fetchone()
            return self._deserialize_node(result) if result else None

    def update_node_state(self, node_id: str, state: NodeState) -> None:
        # Optimistic concurrency: check hash before update
        with Session(self.engine) as session:
            result = session.execute(text("""
                UPDATE node_state
                SET hash = :hash, status = :status, last_run = :last_run,
                    duration_ms = :duration_ms, row_count = :row_count,
                    completed_intervals = :intervals, completed_partitions = :partitions
                WHERE node_id = :node_id
            """), self._serialize_node(state) | {'node_id': node_id})
            session.commit()

    def get_nodes_by_status(self, status: NodeStatus) -> list[str]:
        with Session(self.engine) as session:
            result = session.execute(text("""
                SELECT node_id FROM node_state WHERE status = :status
            """), {'status': status.value})
            return [row.node_id for row in result]

    def begin_transaction(self) -> "DatabaseStateTransaction":
        return DatabaseStateTransaction(self.engine)

    def acquire_lock(self, timeout: int = 30) -> bool:
        """Acquire advisory lock using database-specific mechanism."""
        # SQLite: no built-in locking, use file lock
        # PostgreSQL: SELECT pg_advisory_lock(42)
        # Turso: no advisory locks, rely on transaction isolation
        return True  # Simplified for now

    def release_lock(self) -> None:
        pass

class DatabaseStateTransaction(StateTransaction):
    """Transaction implementation using database transactions."""

    def __init__(self, engine):
        self.engine = engine
        self.session = Session(engine)
        self.staged_updates: list[dict] = []

    def update_node(self, node_id: str, state: NodeState) -> None:
        self.staged_updates.append((node_id, state))

    def commit(self) -> None:
        try:
            for node_id, state in self.staged_updates:
                # Execute updates
                pass
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def rollback(self) -> None:
        self.session.rollback()
```

### Concurrency Strategy

| Aspect | File Backend | Database Backend |
|--------|-------------|------------------|
| **Locking** | fcntl file locks | Database transactions |
| **Isolation** | Single writer at a time | READ_COMMITTED isolation |
| **Conflict Resolution** | Last write wins | Optimistic concurrency |
| **Distributed Support** | ❌ No | ✅ Yes |

### Migration Strategy

```python
# src/seeknal/state/migrate.py

class StateMigrator:
    """Migrate state from file backend to database backend."""

    def migrate(self, source: FileStateBackend, target: DatabaseStateBackend) -> None:
        """Migrate all state from file to database."""
        # Load from file
        states = source.load_run_state()

        # Write to database
        with target.begin_transaction() as tx:
            for node_id, state in states.items():
                tx.update_node(node_id, state)
            tx.commit()

        # Verify migration
        assert len(target.load_run_state()) == len(states)

        # Create backup of original file
        source._create_backup()
```

### Performance Considerations

| Operation | File Backend | Database Backend |
|-----------|-------------|------------------|
| **Load all states** | 10-50ms | 20-100ms |
| **Single node read** | 10-50ms (full file load) | 5-20ms (indexed query) |
| **Single node write** | 10-50ms (full file write) | 5-20ms (indexed update) |
| **Query by status** | 10-50ms (full scan) | 5-15ms (indexed) |
| **Concurrent readers** | ❌ Locked | ✅ Yes |

### Configuration

```yaml
# seeknal_project.yml
state:
  backend: auto  # auto | file | database

  file:
    path: target/run_state.json
    backup_count: 5  # Keep last 5 backups

  database:
    url: ${SEEKNAL_STATE_DB}  # sqlite:///path or turso://url
    pool_size: 5
    max_overflow: 10
```

### Key Design Decisions

1. **Protocol-based abstraction** - Uses Python ABC/Protocol for clear interface
2. **Optimistic concurrency** - Database backend assumes low contention, uses transactions
3. **Atomic operations** - Both backends guarantee atomic state updates
4. **Backward compatibility** - File backend remains default for existing users
5. **Graceful degradation** - Falls back to file if database unavailable

---

## Technical Deep Dive: Distributed Worker Coordination

### Overview

Phase 5 enables multi-machine distributed execution by implementing a worker coordination system inspired by SQLMesh's concurrent execution patterns, extended for true distributed deployment across machines.

### Architecture Components

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Distributed Seeknal Architecture                  │
└─────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐      ┌──────────────┐      ┌──────────────┐
    │   Worker 1   │      │   Worker 2   │      │   Worker 3   │
    │  (Pod/Node)  │      │  (Pod/Node)  │      │  (Pod/Node)  │
    └──────┬───────┘      └──────┬───────┘      └──────┬───────┘
           │                     │                     │
           │    Worker Loop      │                     │
           │   ┌──────────────┐  │                     │
           │   │ 1. Claim work│  │                     │
           │   │ 2. Execute  │  │                     │
           │   │ 3. Complete │  │                     │
           │   │ 4. Repeat   │  │                     │
           │   └──────────────┘  │                     │
           │                     │                     │
           └──────────┬──────────┴──────────┬──────────┘
                      │                     │
        ┌─────────────▼─────────────────────▼─────────────┐
        │           Shared State Database                    │
        │         (Turso / PostgreSQL / SQLite)             │
        │                                                    │
        │  ┌────────────────────────────────────────────┐  │
        │  │ work_queue (work claiming)                │  │
        │  │ - interval_id (PK)                        │  │
        │  │ - status (PENDING/RUNNING/COMPLETED/FAILED)│  │
        │  │ - worker_id                               │  │
        │  │ - claimed_at                              │  │
        │  │ - heartbeat_at (liveness tracking)        │  │
        │  │ - retry_count                             │  │
        │  └────────────────────────────────────────────┘  │
        │  ┌────────────────────────────────────────────┐  │
        │  │ worker_registry (worker discovery)         │  │
        │  │ - worker_id (PK)                          │  │
        │  │ - status (ACTIVE/IDLE/SHUTTING_DOWN)      │  │
        │  │ - last_heartbeat                          │  │
        │  │ - capabilities (max intervals, etc.)      │  │
        │  └────────────────────────────────────────────┘  │
        │  ┌────────────────────────────────────────────┐  │
        │  │ interval_state (progress tracking)         │  │
        │  │ - node_id + interval_start (PK)           │  │
        │  │ - processed_count                          │  │
        │  │ - failed_count                             │  │
        │  │ - last_attempt                             │  │
        │  └────────────────────────────────────────────┘  │
        └────────────────────────────────────────────────────┘
```

### Core Design Patterns (from SQLMesh + Seeknal Research)

#### 1. Work Claiming with SKIP LOCKED

```python
# src/seeknal/workflow/distributed/worker.py

import asyncio
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy import text

class DistributedWorker:
    """Worker that claims and processes intervals from shared queue."""

    def __init__(
        self,
        worker_id: str,
        db_url: str,
        max_concurrent_intervals: int = 5,
        heartbeat_interval: int = 30,
        claim_timeout: int = 300,
    ):
        self.worker_id = worker_id
        self.db_url = db_url
        self.max_concurrent = max_concurrent_intervals
        self.heartbeat_interval = heartbeat_interval
        self.claim_timeout = claim_timeout
        self.engine = create_async_engine(db_url)
        self._running = False
        self._current_intervals: set[str] = set()

    async def claim_work(self, limit: int = 10) -> list[dict]:
        """Claim pending intervals using SELECT FOR UPDATE SKIP LOCKED.

        This SQL pattern prevents race conditions:
        - Multiple workers run this query simultaneously
        - SKIP LOCKED means workers don't wait for locked rows
        - Each worker gets different intervals
        - No two workers process the same interval
        """
        async with self.engine.begin() as conn:
            # Claim pending intervals atomically
            result = await conn.execute(text("""
                UPDATE work_queue
                SET status = 'RUNNING',
                    worker_id = :worker_id,
                    claimed_at = NOW(),
                    heartbeat_at = NOW()
                WHERE interval_id IN (
                    SELECT interval_id FROM work_queue
                    WHERE status = 'PENDING'
                    ORDER BY priority DESC, created_at ASC
                    LIMIT :limit
                    FOR UPDATE SKIP LOCKED  -- Key: skip intervals being claimed by others
                )
                RETURNING interval_id, node_id, start_ts, end_ts, config
            """), {
                "worker_id": self.worker_id,
                "limit": limit,
            })

            claimed = [dict(row) for row in result]
            self._current_intervals.update(row["interval_id"] for row in claimed)
            return claimed

    async def send_heartbeat(self) -> None:
        """Send heartbeat for all running intervals.

        If a worker crashes, its intervals will be reclaimed after
        heartbeat_at + claim_timeout expires.
        """
        if not self._current_intervals:
            return

        async with self.engine.begin() as conn:
            await conn.execute(text("""
                UPDATE work_queue
                SET heartbeat_at = NOW()
                WHERE interval_id = ANY(:interval_ids)
                    AND worker_id = :worker_id
                    AND status = 'RUNNING'
            """), {
                "interval_ids": list(self._current_intervals),
                "worker_id": self.worker_id,
            })

    async def complete_interval(
        self,
        interval_id: str,
        status: str,
        row_count: int = 0,
        error_message: Optional[str] = None,
    ) -> None:
        """Mark an interval as complete or failed."""
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                UPDATE work_queue
                SET status = :status,
                    completed_at = NOW(),
                    row_count = :row_count,
                    error_message = :error_message,
                    worker_id = NULL
                WHERE interval_id = :interval_id
                    AND worker_id = :worker_id
            """), {
                "status": status,
                "row_count": row_count,
                "error_message": error_message,
                "interval_id": interval_id,
                "worker_id": self.worker_id,
            })

        self._current_intervals.discard(interval_id)
```

#### 2. Stale Work Reclamation (Circuit Breaker Pattern)

```python
# src/seeknal/workflow/distributed/reclaimer.py

class StaleWorkReclaimer:
    """Reclaims work from crashed or dead workers.

    Inspired by SQLMesh's circuit breaker pattern - detect when workers
    have failed and reclaim their work for other workers to process.
    """

    async def reclaim_stale_work(
        self,
        db_url: str,
        claim_timeout: int = 300,  # 5 minutes
        max_retries: int = 3,
    ) -> int:
        """Reclaim intervals from dead workers.

        An interval is stale if:
        1. Status is RUNNING
        2. No heartbeat for claim_timeout seconds
        3. Not already at max_retries

        Returns count of reclaimed intervals.
        """
        async with AsyncEngine(create_async_engine(db_url)).begin() as conn:
            # Find stale intervals
            result = await conn.execute(text("""
                UPDATE work_queue
                SET status = 'PENDING',
                    worker_id = NULL,
                    claimed_at = NULL,
                    retry_count = retry_count + 1
                WHERE status = 'RUNNING'
                    AND heartbeat_at < NOW() - INTERVAL ':timeout seconds'
                    AND retry_count < :max_retries
                RETURNING interval_id
            """), {
                "timeout": claim_timeout,
                "max_retries": max_retries,
            })

            reclaimed_count = len(result.fetchall())

            # Intervals at max_retries are marked PERMANENTLY_FAILED
            await conn.execute(text("""
                UPDATE work_queue
                SET status = 'PERMANENTLY_FAILED',
                    error_message = 'Failed after ' || retry_count || ' attempts'
                WHERE status = 'RUNNING'
                    AND heartbeat_at < NOW() - INTERVAL ':timeout seconds'
                    AND retry_count >= :max_retries
            """), {
                "timeout": claim_timeout,
                "max_retries": max_retries,
            })

            return reclaimed_count
```

#### 3. Worker Registry (Discovery & Health)

```python
# src/seeknal/workflow/distributed/registry.py

class WorkerRegistry:
    """Track active workers for monitoring and coordination."""

    async def register_worker(
        self,
        worker_id: str,
        capabilities: dict,
    ) -> None:
        """Register a new worker."""
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                INSERT INTO worker_registry (worker_id, status, last_heartbeat, capabilities)
                VALUES (:worker_id, 'IDLE', NOW(), :capabilities)
                ON CONFLICT (worker_id) DO UPDATE SET
                    status = 'IDLE',
                    last_heartbeat = NOW(),
                    capabilities = :capabilities
            """), {
                "worker_id": worker_id,
                "capabilities": json.dumps(capabilities),
            })

    async def update_heartbeat(self, worker_id: str, active_intervals: int) -> None:
        """Update worker heartbeat and activity."""
        status = "ACTIVE" if active_intervals > 0 else "IDLE"
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                UPDATE worker_registry
                SET last_heartbeat = NOW(),
                    status = :status,
                    active_intervals = :active_intervals
                WHERE worker_id = :worker_id
            """), {
                "worker_id": worker_id,
                "status": status,
                "active_intervals": active_intervals,
            })

    async def detect_dead_workers(self, timeout: int = 120) -> list[str]:
        """Find workers that haven't sent heartbeat recently."""
        async with self.engine.begin() as conn:
            result = await conn.execute(text("""
                SELECT worker_id FROM worker_registry
                WHERE last_heartbeat < NOW() - INTERVAL ':timeout seconds'
                    AND status != 'SHUTTING_DOWN'
            """), {"timeout": timeout})
            return [row.worker_id for row in result]
```

#### 4. Exponential Backoff Retry (from SQLMesh)

```python
# src/seeknal/workflow/distributed/retry.py

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

class TransientError(Exception):
    """Base class for transient errors that should be retried."""

class DatabaseConnectionError(TransientError):
    """Transient database connection error."""

class IntervalProcessingError(Exception):
    """Non-transient error - don't retry."""

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type(TransientError),
)
async def process_interval_with_retry(
    node_id: str,
    start_ts: datetime,
    end_ts: datetime,
    config: dict,
) -> dict:
    """Process an interval with automatic retry on transient errors.

    Retry strategy (from SQLMesh):
    - Attempt 1: immediate
    - Attempt 2: wait ~4 seconds (2^2 * 1)
    - Attempt 3: wait ~5 seconds (2^2.something * 1)
    - Attempt 4: wait ~8 seconds
    - Attempt 5: wait ~10 seconds (max)
    - Give up after 5 attempts
    """
    try:
        return await _execute_interval(node_id, start_ts, end_ts, config)
    except DatabaseConnectionError as e:
        # Transient error - retry automatically
        raise
    except IntervalProcessingError as e:
        # Non-transient - don't retry
        raise  # Will be marked as PERMANENTLY_FAILED
```

#### 5. Worker Main Loop

```python
# src/seeknal/workflow/distributed/worker.py

class DistributedWorker:
    """Main worker loop."""

    async def run(self) -> None:
        """Main worker loop."""
        await self.register()
        self._running = True

        # Start background tasks
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        reclaimer_task = asyncio.create_task(self._reclaimer_loop())

        try:
            while self._running:
                # Check capacity
                if len(self._current_intervals) >= self.max_concurrent:
                    await asyncio.sleep(1)
                    continue

                # Claim work
                claimed = await self.claim_work(
                    limit=self.max_concurrent - len(self._current_intervals)
                )

                if not claimed:
                    # No work available, wait a bit
                    await asyncio.sleep(5)
                    continue

                # Process intervals in parallel
                tasks = [
                    self._process_interval(interval)
                    for interval in claimed
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

        finally:
            # Graceful shutdown
            await self._shutdown(heartbeat_task, reclaimer_task)

    async def _heartbeat_loop(self) -> None:
        """Send heartbeat every heartbeat_interval seconds."""
        while self._running:
            try:
                await self.send_heartbeat()
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                _echo_error(f"Heartbeat failed: {e}")

    async def _process_interval(self, interval: dict) -> None:
        """Process a single interval."""
        interval_id = interval["interval_id"]
        try:
            result = await process_interval_with_retry(
                interval["node_id"],
                interval["start_ts"],
                interval["end_ts"],
                interval.get("config", {}),
            )
            await self.complete_interval(
                interval_id,
                status="COMPLETED",
                row_count=result.get("row_count", 0),
            )
        except Exception as e:
            await self.complete_interval(
                interval_id,
                status="FAILED",
                error_message=str(e),
            )
```

### Database Schema

```sql
-- Work queue for interval claiming
CREATE TABLE work_queue (
    interval_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    start_ts TIMESTAMP NOT NULL,
    end_ts TIMESTAMP NOT NULL,
    config JSONB,

    -- Work claiming fields
    status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING, RUNNING, COMPLETED, FAILED, PERMANENTLY_FAILED
    worker_id TEXT,
    claimed_at TIMESTAMP,
    heartbeat_at TIMESTAMP,
    completed_at TIMESTAMP,

    -- Retry tracking
    retry_count INTEGER DEFAULT 0,
    error_message TEXT,

    -- Priority (for ordered processing)
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),

    -- Indexes for worker queries
    INDEX idx_work_queue_status (status),
    INDEX idx_work_queue_worker (worker_id, status),
    INDEX idx_work_queue_heartbeat (status, heartbeat_at) WHERE status = 'RUNNING'
);

-- Worker registry for discovery
CREATE TABLE worker_registry (
    worker_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,  -- IDLE, ACTIVE, SHUTTING_DOWN, DEAD
    last_heartbeat TIMESTAMP NOT NULL,
    active_intervals INTEGER DEFAULT 0,
    capabilities JSONB,
    started_at TIMESTAMP DEFAULT NOW(),

    INDEX idx_worker_registry_heartbeat (last_heartbeat)
);

-- Interval progress tracking
CREATE TABLE interval_state (
    node_id TEXT NOT NULL,
    interval_start TIMESTAMP NOT NULL,
    interval_end TIMESTAMP NOT NULL,
    processed_count INTEGER DEFAULT 0,
    failed_count INTEGER DEFAULT 0,
    last_attempt TIMESTAMP,
    last_worker_id TEXT,

    PRIMARY KEY (node_id, interval_start, interval_end)
);
```

### CLI Interface

```bash
# Start a worker
seeknal worker \
    --name worker-1 \
    --state-database $DATABASE_URL \
    --max-concurrent 5 \
    --heartbeat-interval 30

# Queue backfill work
seeknal queue-backfill \
    --node source.events \
    --start 2025-01-01 \
    --end 2025-12-31 \
    --priority 10

# Monitor queue status
seeknal queue-status \
    --node source.events \
    --watch

# Show worker status
seeknal worker-status

# Graceful shutdown
seeknal worker --name worker-1 --stop
```

### Failure Handling Scenarios

| Scenario | Detection | Recovery |
|----------|-----------|----------|
| **Worker crashes** | No heartbeat for timeout | Reclaimer marks intervals PENDING |
| **Network partition** | Worker can't reach DB | Worker exits, intervals reclaimed |
| **Database outage** | Connection errors | Retry with exponential backoff |
| **Poison pill** (bad interval) | Repeated failures | Marked PERMANENTLY_FAILED after max_retries |
| **Duplicate claim** | SELECT FOR UPDATE SKIP LOCKED | Database prevents, rows skipped |

### Key Design Decisions

1. **SKIP LOCKED pattern** - Prevents workers from waiting on locked rows, enables true parallel claiming
2. **Heartbeat-based liveness** - Detects crashed workers without complex distributed coordination
3. **Optimistic retry** - Transient errors retry automatically, permanent errors fail fast
4. **At-least-once semantics** - Idempotent interval processing prevents duplicate work
5. **Graceful degradation** - Workers continue operating even if some components fail
6. **No central coordinator** - Workers coordinate via database, no single point of failure

### Performance Considerations

| Aspect | Target | Strategy |
|--------|--------|----------|
| **Claim latency** | < 100ms | Indexed queries, LIMIT clause |
| **Worker throughput** | 10+ intervals/sec/min | Parallel processing, async I/O |
| **Recovery time** | < 2 * heartbeat_interval | Frequent heartbeats (30s default) |
| **Scalability** | 100+ workers | Stateless workers, shared database |
| **Database load** | < 1000 QPS | Connection pooling, batch updates |

---

## Alternative: Prefect Integration Instead of Custom Distributed Workers

### Overview

Instead of building a custom distributed worker system (Phase 5), we can integrate with **Prefect** - the industry-standard workflow orchestrator. This leverages Prefect's battle-tested infrastructure for:
- Scheduling and orchestration
- Distributed execution
- Worker management
- State tracking
- UI/monitoring
- Alerting and notifications

### Architecture Comparison

```
┌─────────────────────────────────────────────────────────────────────────┐
│                   Custom Distributed Workers (Phase 5)                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │
│  │ Worker 1    │  │ Worker 2    │  │ Worker 3    │                   │
│  │ (custom)    │  │ (custom)    │  │ (custom)    │                   │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                   │
│         │                │                │                           │
│         └────────────────┴────────────────┴────┐                      │
│                                              │                       │
│         ┌──────────────────────────────────────▼──────┐              │
│         │   Custom Database (work_queue, registry)     │              │
│         │   - Custom worker coordination              │              │
│         │   - Custom state tracking                   │              │
│         │   - Custom failure handling                 │              │
│         │   - No UI (unless built separately)          │              │
│         └──────────────────────────────────────────────┘              │
│                                                                       │
│  Build effort: HIGH (2-3 weeks)                                        │
│  Maintenance burden: Seeknal team                                      │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│                   Prefect Integration (Alternative)                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                   │
│  │ Prefect     │  │ Prefect     │  │ Prefect     │                   │
│  │ Worker 1    │  │ Worker 2    │  │ Worker 3    │                   │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                   │
│         │                │                │                           │
│         └────────────────┴────────────────┴────┐                      │
│                                              │                       │
│         ┌──────────────────────────────────────▼──────┐              │
│         │       Prefect Server / Cloud                 │              │
│         │   - Built-in scheduling (cron, interval)     │              │
│         │   - State tracking & history                 │              │
│         │   - Worker coordination                     │              │
│         │   - UI & dashboards                          │              │
│         │   - Alerting & notifications                 │              │
│         │   - Retry logic & failure handling           │              │
│         └──────────────────────────────────────────────┘              │
│                                                                       │
│  Build effort: LOW (1 week wrapper)                                    │
│  Maintenance burden: Prefect team                                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Integration Strategy

#### Option 1: Flow-Level Integration (Simplest)

Wrap the entire `seeknal run` as a Prefect flow:

```python
# src/seeknal/prefect_integration.py

from prefect import flow, get_run_logger
from pathlib import Path
from typing import Optional
import subprocess

@flow(name="seeknal-run-pipeline")
def seeknal_run_flow(
    project_path: str = ".",
    nodes: Optional[list[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    parallel: bool = True,
) -> dict:
    """Run Seeknal pipeline as a Prefect flow.

    This provides:
    - Automatic retry on failure
    - State tracking in Prefect UI
    - Scheduling via Prefect
    - Logging and observability
    """
    logger = get_run_logger()

    # Build command
    cmd = ["seeknal", "run", "--project-path", project_path]
    if parallel:
        cmd.append("--parallel")
    if nodes:
        cmd.extend(["--nodes", ",".join(nodes)])
    if start:
        cmd.extend(["--start", start])
    if end:
        cmd.extend(["--end", end])

    logger.info(f"Executing: {' '.join(cmd)}")

    # Execute seeknal run
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=Path(project_path),
    )

    if result.returncode != 0:
        logger.error(f"Seeknal run failed: {result.stderr}")
        raise RuntimeError(f"Pipeline failed: {result.stderr}")

    logger.info("Pipeline completed successfully")
    return {"stdout": result.stdout, "returncode": result.returncode}


@flow(name="seeknal-backfill")
def seeknal_backfill_flow(
    project_path: str = ".",
    node: str = None,
    start: str = None,
    end: str = None,
) -> dict:
    """Backfill Seeknal pipeline for a date range.

    Uses Prefect's parallel task execution for distributed backfill.
    """
    from prefect import task

    # Calculate intervals from start to end
    intervals = _calculate_intervals(start, end)

    # Create backfill tasks for each interval
    @task(retries=3, retry_delay_seconds=60)
    def process_interval(interval_start: str, interval_end: str):
        return subprocess.run([
            "seeknal", "run",
            "--project-path", project_path,
            "--nodes", node,
            "--start", interval_start,
            "--end", interval_end,
        ], check=True, capture_output=True)

    # Process intervals in parallel (Prefect handles this)
    futures = [
        process_interval.submit(interval[0], interval[1])
        for interval in intervals
    ]

    results = [future.result() for future in futures]
    return results
```

**Deployment**:
```python
# Deploy to Prefect
if __name__ == "__main__":
    from prefect import serve

    # Option A: Serve locally
    seeknal_run_flow.serve(
        name="seeknal-daily-pipeline",
        interval=86400,  # Daily
        parameters={"project_path": "/data/pipelines"},
    )

    # Option B: Deploy to Prefect Cloud/Server
    from prefect import deploy
    seeknal_run_flow.deploy(
        name="seeknal-daily",
        work_pool_name="seeknal-pool",
        schedule={"cron": "0 2 * * *"},  # Daily at 2 AM
    )
```

#### Option 2: Node-Level Integration (Fine-Grained)

Map each Seeknal node to a Prefect task for fine-grained tracking:

```python
# src/seeknal/prefect_integration.py

from prefect import flow, task
from seeknal.workflow.manifest import Manifest
from seeknal.workflow.runner import DAGRunner

@task(retries=3)
def execute_seeknal_node(
    project_path: str,
    node_id: str,
    config: dict,
) -> dict:
    """Execute a single Seeknal node as a Prefect task.

    Benefits:
    - Individual node retry
    - Fine-grained progress tracking
    - Parallel execution of independent nodes
    """
    runner = DAGRunner(
        project_path=Path(project_path),
        manifest=Manifest.load(project_path),
    )
    result = runner._execute_node(node_id)
    return {
        "node_id": node_id,
        "status": result.status.value,
        "duration": result.duration,
        "row_count": result.row_count,
    }

@flow(name="seeknal-dag-execution")
def seeknal_dag_flow(project_path: str = ".") -> dict:
    """Execute Seeknal DAG using Prefect for orchestration.

    Prefect handles:
    - Topological ordering
    - Parallel execution of independent nodes
    - State tracking per node
    - Retry logic
    - Visualization in Prefect UI
    """
    manifest = Manifest.load(project_path)

    # Get topological layers (Seeknal already has this!)
    layers = _get_topological_layers(manifest)

    results = {}
    for layer_idx, layer in enumerate(layers):
        # Execute all nodes in this layer in parallel
        futures = [
            execute_seeknal_node.submit(project_path, node_id, {})
            for node_id in layer
        ]

        # Wait for layer to complete
        for future in futures:
            result = future.result()
            results[result["node_id"]] = result

    return results
```

### Configuration

```yaml
# seeknal_project.yml
prefect:
  enabled: true
  integration_mode: flow  # flow | node | worker
  server_url: ${PREFECT_API_URL}  # Prefect Cloud or self-hosted
  deployment:
    name: seeknal-pipeline
    work_pool: seeknal-pool
    schedules:
      - name: daily
        cron: "0 2 * * *"  # Daily at 2 AM
      - name: hourly
        interval: 3600  # Every hour
    notifications:
      - type: slack
        webhook_url: ${SLACK_WEBHOOK}
```

### CLI Integration

```bash
# Run pipeline through Prefect
seeknal run --prefect

# Deploy to Prefect
seeknal deploy --prefect

# View Prefect UI
seeknal prefect ui

# Check Prefect status
seeknal prefect status
```

### Key Decision: Custom Workers vs Prefect

| Aspect | Custom Workers (Phase 5) | Prefect Integration |
|--------|-------------------------|---------------------|
| **Build effort** | HIGH (2-3 weeks) | LOW (1 week) |
| **Maintenance** | Seeknal team | Prefect team |
| **UI/Monitoring** | Must build separately | Built-in |
| **Scheduling** | Must implement | Built-in |
| **Worker coordination** | Custom SKIP LOCKED | Built-in |
| **State tracking** | Custom database | Built-in |
| **Failure handling** | Custom retry logic | Built-in |
| **Industry adoption** | Seeknal-only | Industry standard |
| **Learning curve** | Custom patterns | Well-documented |

### Recommendation

**Use Prefect Integration** instead of custom Phase 5 workers:

1. **Leverage existing infrastructure** - Prefect has solved distributed orchestration
2. **Faster time to production** - 1 week vs 2-3 weeks
3. **Better UX** - Prefect UI is battle-tested
4. **Industry standard** - Users already know Prefect
5. **Less maintenance** - Prefect team handles the complexity

**Phases 1-4 remain valuable** - State backend protocol, interval tracking, change categorization, and plan/apply workflow are still needed. Only Phase 5 (distributed workers) is replaced by Prefect.

### Migration Path

1. **Phase 1-4**: Build interval tracking, change categorization, plan/apply, database state
2. **Phase 5A**: Build Prefect integration wrapper (1 week)
3. **Phase 5B**: (Optional) Custom workers only if Prefect doesn't meet specific needs

### Summary: Two Paths Forward

```
                    ┌─────────────────────────────────────┐
                    │         SEEKNAL RUN PARITY          │
                    └─────────────────────────────────────┘
                                      │
                    ┌───────────────┴───────────────┐
                    │                               │
           ┌────────▼─────────┐           ┌────────▼─────────┐
           │  Path A: Custom  │           │  Path B: Prefect │
           │  Distributed     │           │  Integration     │
           │  (Phase 5)       │           │  (Alternative)   │
           └──────────────────┘           └──────────────────┘

Phase 1: Intervals          Required                 Required
Phase 2: Change Categorization Required                 Required
Phase 3: Plan/Apply            Required                 Required
Phase 4: Database State        Required                 Required
Phase 5: Distributed Workers   Custom (2-3 weeks)      Prefect (1 week)
```

---

## Technical Deep Dive: Change Detection Logic

### Overview

Change detection is **partially implemented** in Seeknal. The existing system uses hash-based comparison and manifest diffing, but lacks SQL-level parsing and column-level lineage. This deep dive extends the existing foundation with SQLGlot-based SQL analysis for more precise change categorization.

### What Seeknal Already Has

```python
# src/seeknal/workflow/state.py (existing)

@dataclass
class NodeFingerprint:
    content_hash: str   # SHA256 of functional YAML content
    schema_hash: str    # SHA256 of output column names + types
    upstream_hash: str  # SHA256 of sorted upstream fingerprints
    config_hash: str    # SHA256 of non-functional config

def calculate_node_hash(yaml_data: Dict, yaml_path: Path) -> str:
    """Hash functional content (SQL, features, dependencies)."""
    # Excludes: description, owner, tags (metadata)
```

```python
# src/seeknal/dag/diff.py (existing)

class ChangeCategory(Enum):
    BREAKING = "breaking"          # Downstream rebuild required
    NON_BREAKING = "non_breaking"  # Only this node needs rebuild
    METADATA = "metadata"          # No execution needed

class NodeChange:
    node_id: str
    category: ChangeCategory
    description: str
```

### What's Missing: SQL-Level Change Detection

```
┌─────────────────────────────────────────────────────────────────┐
│              Current Change Detection (Seeknal)                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Transform Node                                                 │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ SELECT * FROM source.users                           │     │
│  │ WHERE age > 0                                        │     │
│  │ HASH → "abc123"                                      │     │
│  └──────────────────────────────────────────────────────┘     │
│                                                                 │
│  Change detected if:                                         │
│  - content_hash differs (SQL string comparison)              │
│  - schema_hash differs (column names/types)                  │
│  - upstream_hash differs                                    │
│                                                                 │
│  Problem: "SELECT user_id, name FROM users" ≠                │
│           "SELECT user_id, name, age FROM users"              │
│  → Different hash, but NON_BREAKING (only added column)      │
│  → Currently classified as BREAKING (conservative)            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│              Enhanced Change Detection (with SQLGlot)          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Transform Node                                                 │
│  ┌──────────────────────────────────────────────────────┐     │
│  │ OLD: SELECT user_id, name FROM users                  │     │
│  │ NEW: SELECT user_id, name, age FROM users              │     │
│  │                                                        │     │
│  │ SQLGlot AST Diff:                                      │     │
│  │   ├─ user_id: UNCHANGED                                │     │
│  │   ├─ name: UNCHANGED                                   │     │
│  │   └─ age: INSERT (new projection)                      │     │
│  │                                                        │     │
│  │ Category: NON_BREAKING (only insertions)               │     │
│  └──────────────────────────────────────────────────────┘     │
│                                                                 │
│  Change detected if:                                         │
│  - AST diff shows INSERT only → NON_BREAKING                │
│  - AST diff shows UPDATE/DELETE → BREAKING                 │
│  - Column removed + used downstream → BREAKING              │
│  - Column removed + NOT used → NON_BREAKING                │
└─────────────────────────────────────────────────────────────────┘
```

### Implementation: SQLGlot-Based Change Detection

```python
# src/seeknal/dag/sql_diff.py (new file)

from sqlglot import parse_one, exp
from sqlglot.diff import diff
from typing import Optional, List, Set, Tuple
from dataclasses import dataclass

@dataclass
class SQLChange:
    """Represents a change detected between two SQL queries."""
    change_type: str  # INSERT, UPDATE, DELETE, UNCHANGED
    object_type: str  # COLUMN, TABLE, EXPRESSION
    object_name: str  # Name of changed object
    old_value: Optional[str] = None
    new_value: Optional[str] = None

class SQLChangeAnalyzer:
    """Analyze SQL changes using SQLGlot AST diffing."""

    def __init__(self, dialect: str = "duckdb"):
        self.dialect = dialect

    def categorize_sql_change(
        self,
        old_sql: str,
        new_sql: str,
        column_usage: Optional[Set[str]] = None,
    ) -> ChangeCategory:
        """Categorize a SQL change as BREAKING or NON_BREAKING.

        Logic (from SQLMesh):
        - If all AST edits are INSERT operations → NON_BREAKING
        - If any AST edit is UPDATE/DELETE → BREAKING
        - If column removed and used downstream → BREAKING
        - If column removed and NOT used → NON_BREAKING
        """
        # Parse SQL into ASTs
        old_ast = parse_one(old_sql, dialect=self.dialect)
        new_ast = parse_one(new_sql, dialect=self.dialect)

        # Get AST diff
        edits = list(diff(
            old_ast,
            new_ast,
            matchings=[(old_ast, new_ast)],
            delta_only=True,
            dialect=self.dialect,
        ))

        # Categorize based on edit types
        has_destructive = False
        has_only_inserts = len(edits) > 0

        for edit in edits:
            if isinstance(edit, (exp.Delete, exp.Update)):
                # UPDATE or DELETE in AST = breaking
                has_destructive = True
                has_only_inserts = False
                break
            elif isinstance(edit, exp.Insert):
                # INSERT = potentially non-breaking
                # But check if removing used column
                if self._removes_used_column(edit, column_usage):
                    has_destructive = True
                    break
            else:
                # Other edit type = conservative (breaking)
                has_destructive = True
                has_only_inserts = False
                break

        if has_destructive:
            return ChangeCategory.BREAKING
        elif has_only_inserts:
            return ChangeCategory.NON_BREAKING
        else:
            # No structural changes (likely reformatting)
            return ChangeCategory.METADATA

    def _removes_used_column(
        self,
        edit: exp.Insert,
        used_columns: Set[str],
    ) -> bool:
        """Check if an INSERT edit removes a column used downstream."""
        if not used_columns:
            return False

        # Check if any used column is missing from new projection
        new_columns = self._extract_column_names(edit.expression)
        return bool(used_columns - new_columns)

    def extract_column_dependencies(
        self,
        sql: str,
        upstream_schemas: dict[str, list[str]],
    ) -> Set[str]:
        """Extract which upstream columns this SQL query depends on.

        Uses SQLGlot lineage to trace column dependencies.

        Args:
            sql: The SQL query to analyze
            upstream_schemas: Map of table_name -> list of column names

        Returns:
            Set of (table_name, column_name) tuples this query depends on
        """
        from sqlglot.lineage import lineage

        query_ast = parse_one(sql, dialect=self.dialect)

        # Find all column references
        dependencies = set()

        for column in query_ast.find_all(exp.Column):
            # Get the table this column comes from
            table = column.find_ancestor(exp.Table)
            if table:
                table_name = table.name
                column_name = column.name
                dependencies.add((table_name, column_name))

        return dependencies

    def extract_output_schema(self, sql: str) -> dict[str, str]:
        """Extract output schema (column names and types) from SQL.

        Returns: dict of {column_name: column_type}
        """
        query_ast = parse_one(sql, dialect=self.dialect)

        # Find SELECT projections
        select = query_ast.find(exp.Select)
        if not select:
            return {}

        schema = {}
        for projection in select.expressions:
            if isinstance(projection, exp.Column):
                col_name = projection.name or projection.alias
                # Try to infer type (basic inference)
                col_type = self._infer_column_type(projection)
                schema[col_name] = col_type

        return schema

    def _infer_column_type(self, column: exp.Column) -> str:
        """Infer column type from expression."""
        # Basic type inference
        # In production, this would need schema information
        if isinstance(column.type, exp.DataType):
            return column.type.sql()
        return "UNKNOWN"
```

### Implementation: Column-Level Lineage

```python
# src/seeknal/dag/lineage.py (new file)

from sqlglot import parse_one
from sqlglot.lineage import lineage as sqlglot_lineage
from typing import Dict, Set, Tuple
from dataclasses import dataclass

@dataclass
class ColumnLineage:
    """Traces column usage across the DAG."""
    node_id: str
    output_columns: Set[str]  # Columns this node produces
    input_dependencies: Dict[str, Set[str]]  # {upstream_node: {columns}}

class LineageBuilder:
    """Build column-level lineage for the entire DAG."""

    def __init__(self, dialect: str = "duckdb"):
        self.dialect = dialect

    def build_lineage(
        self,
        manifest: "Manifest",
    ) -> Dict[str, ColumnLineage]:
        """Build column-level lineage for all nodes in manifest.

        Returns:
            Map of node_id -> ColumnLineage
        """
        lineage = {}

        for node_id, node in manifest.nodes.items():
            if node.kind in ("transform", "aggregation", "feature_group"):
                lineage[node_id] = self._analyze_node(node, manifest)

        return lineage

    def _analyze_node(
        self,
        node: "Node",
        manifest: "Manifest",
    ) -> ColumnLineage:
        """Analyze column dependencies for a single node."""
        if not node.sql:
            # No SQL = no lineage (source nodes, etc.)
            return ColumnLineage(
                node_id=node.node_id,
                output_columns=set(),
                input_dependencies={},
            )

        # Parse SQL and extract dependencies
        dependencies = self._extract_dependencies(node.sql, node.inputs, manifest)

        # Extract output schema
        output_columns = self._extract_output_columns(node.sql)

        return ColumnLineage(
            node_id=node.node_id,
            output_columns=output_columns,
            input_dependencies=dependencies,
        )

    def _extract_dependencies(
        self,
        sql: str,
        inputs: list[str],
        manifest: "Manifest",
    ) -> Dict[str, Set[str]]:
        """Extract which upstream columns this SQL depends on.

        Returns:
            Map of {input_node_id: {column_names}}
        """
        from sqlglot.lineage import lineage

        query_ast = parse_one(sql, dialect=self.dialect)
        dependencies = {}

        # Build table name mapping
        table_to_node = {}
        for input_ref in inputs:
            input_node = manifest.get_node(input_ref)
            if input_node:
                table_to_node[input_node.name] = input_node.node_id

        # Find all column references
        for column in query_ast.find_all(exp.Column):
            table = column.find_ancestor(exp.Table)
            if table and table.name in table_to_node:
                node_id = table_to_node[table.name]
                if node_id not in dependencies:
                    dependencies[node_id] = set()
                dependencies[node_id].add(column.name)

        return dependencies

    def _extract_output_columns(self, sql: str) -> Set[str]:
        """Extract output column names from SQL."""
        query_ast = parse_one(sql, dialect=self.dialect)
        select = query_ast.find(exp.Select)

        columns = set()
        if select:
            for projection in select.expressions:
                if isinstance(projection, exp.Column):
                    name = projection.alias or projection.name
                    if name:
                        columns.add(name)

        return columns

def is_column_used_downstream(
    column_name: str,
    node_id: str,
    lineage: Dict[str, ColumnLineage],
    dag: Dict[str, Set[str]],
) -> bool:
    """Check if a column is used by any downstream nodes.

    This determines if removing a column is breaking.
    """
    # BFS traversal to find column usage
    visited = set()
    queue = list(dag.get(node_id, set()))

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)

        if current in lineage:
            current_lineage = lineage[current]
            # Check each upstream dependency
            for upstream_id, columns in current_lineage.input_dependencies.items():
                if upstream_id == node_id and column_name in columns:
                    return True

        # Add downstream nodes
        queue.extend(dag.get(current, set()))

    return False
```

### Integration with Existing Change Categorization

```python
# src/seeknal/dag/diff.py (extend existing)

from seeknal.dag.sql_diff import SQLChangeAnalyzer
from seeknal.dag.lineage import LineageBuilder, is_column_used_downstream

class ManifestDiff:
    """Extended manifest diff with SQL-level change detection."""

    def __init__(self, old_manifest: Manifest, new_manifest: Manifest):
        self.old = old_manifest
        self.new = new_manifest
        self.sql_analyzer = SQLChangeAnalyzer()
        self.lineage_builder = LineageBuilder()

    def categorize_change(self, node_id: str) -> ChangeCategory:
        """Categorize change with SQL-level analysis."""
        old_node = self.old.get_node(node_id)
        new_node = self.new.get_node(node_id)

        if not old_node:
            return ChangeCategory.NON_BREAKING  # New node

        # Fast path: metadata-only changes
        if self._is_metadata_only_change(old_node, new_node):
            return ChangeCategory.METADATA

        # SQL-level analysis for transform nodes
        if old_node.sql and new_node.sql:
            # Build column lineage if not already built
            if not hasattr(self, '_lineage'):
                self._lineage = self.lineage_builder.build_lineage(self.new)

            # Get columns used downstream
            node_lineage = self._lineage.get(node_id)
            if node_lineage:
                used_columns = node_lineage.output_columns
            else:
                used_columns = None

            # Categorize SQL change
            return self.sql_analyzer.categorize_sql_change(
                old_node.sql,
                new_node.sql,
                column_usage=used_columns,
            )

        # Fallback to hash-based comparison (existing logic)
        return self._categorize_by_hash(old_node, new_node)

    def _is_metadata_only_change(
        self,
        old_node: Node,
        new_node: Node,
    ) -> bool:
        """Check if only metadata fields changed."""
        metadata_fields = {"description", "owner", "tags", "doc"}

        for field in metadata_fields:
            old_val = getattr(old_node, field, None)
            new_val = getattr(new_node, field, None)
            if old_val != new_val:
                # Something changed, check if anything else changed
                return self._is_only_metadata_changed(old_node, new_node)

        return False

    def _is_only_metadata_changed(
        self,
        old_node: Node,
        new_node: Node,
    ) -> bool:
        """Check if ONLY metadata changed (no functional changes)."""
        # Compare hashes
        if (old_node.fingerprint.content_hash !=
            new_node.fingerprint.content_hash):
            return False  # Functional content changed
        if (old_node.fingerprint.schema_hash !=
            new_node.fingerprint.schema_hash):
            return False  # Schema changed

        return True  # Only metadata changed
```

### Enhanced Change Categories

```python
# src/seeknal/dag/change.py (extended)

from enum import Enum

class ChangeCategory(Enum):
    """Enhanced change categories with SQL-level detection."""

    # Existing categories
    BREAKING = "breaking"               # Downstream rebuild required
    NON_BREAKING = "non_breaking"       # Only this node needs rebuild
    METADATA = "metadata"               # No execution needed

    # New fine-grained categories (optional, for better UX)
    COLUMN_ADDED = "column_added"       # NON_BREAKING: New column added
    COLUMN_REMOVED_UNUSED = "column_removed_unused"  # NON_BREAKING: Removed column not used downstream
    COLUMN_REMOVED_USED = "column_removed_used"      # BREAKING: Removed column used downstream
    SQL_LOGIC_CHANGED = "sql_logic_changed"         # BREAKING: SQL WHERE/JOIN changed
    TYPE_WIDENED = "type_widened"         # NON_BREAKING: int -> bigint
    TYPE_NARROWED = "type_narrowed"       # BREAKING: bigint -> int (data loss)
    DEPENDENCY_CHANGED = "dependency_changed"         # BREAKING: Input changed

    @property
    def is_breaking(self) -> bool:
        """True if this change requires downstream rebuild."""
        return self in (
            ChangeCategory.BREAKING,
            ChangeCategory.COLUMN_REMOVED_USED,
            ChangeCategory.SQL_LOGIC_CHANGED,
            ChangeCategory.TYPE_NARROWED,
            ChangeCategory.DEPENDENCY_CHANGED,
        )

    @property
    def is_non_breaking(self) -> bool:
        """True if only this node needs rebuild."""
        return self in (
            ChangeCategory.NON_BREAKING,
            ChangeCategory.COLUMN_ADDED,
            ChangeCategory.COLUMN_REMOVED_UNUSED,
            ChangeCategory.TYPE_WIDENED,
        )
```

### CLI Output Example

```bash
$ seeknal plan

Analyzing pipeline changes...

Changes detected:

  transform.users_with_signup_month [NON_BREAKING: COLUMN_ADDED]
    ~ SQL changed: added projection 'signup_month'
    Downstream impact: none (new column, not used yet)
    + Added column: signup_month (DATE)

  transform.user_metrics [BREAKING: SQL_LOGIC_CHANGED]
    ~ SQL changed: WHERE clause modified
      OLD: WHERE created_at >= '2025-01-01'
      NEW: WHERE created_at >= '2025-02-01'
    Downstream impact:
      - feature_group.user_features → REBUILD
      - aggregation.daily_metrics → REBUILD
    ⚠ Date filter change affects output rows

  source.users [METADATA]
    ~ description changed: "User source" → "User source table"
    Downstream impact: none (data unchanged)

  transform.drop_legacy [NON_BREAKING: COLUMN_REMOVED_UNUSED]
    ~ SQL changed: dropped column 'legacy_flag'
    Downstream impact: none (column not used downstream)
    - Removed column: legacy_flag (BOOLEAN)

  transform.narrow_type [BREAKING: TYPE_NARROWED]
    ~ Schema changed: score_column BIGINT → INTEGER
    Downstream impact:
      - aggregation.high_scores → REBUILD
    ⚠ Type narrowing may cause data loss

Plan summary:
  2 breaking changes (2 downstream nodes to rebuild)
  2 non-breaking changes (no downstream impact)
  1 metadata change (no action needed)

Estimated backfill intervals:
  - transform.user_metrics: 72 intervals (3 days)
  - aggregation.daily_metrics: 72 intervals (3 days)

Apply these changes? [y/N/q]
```

### Implementation Phases

| Phase | What | Duration |
|-------|------|----------|
| **2A: SQLGlot Integration** | Add SQLGlot dependency, basic parsing | 2-3 days |
| **2B: SQL Diff Analysis** | AST-based change detection | 3-4 days |
| **2C: Column Lineage** | Build column dependency graph | 3-4 days |
| **2D: Enhanced Categorization** | Fine-grained change types | 2-3 days |
| **2E: CLI Integration** | Update plan output with detailed info | 2-3 days |
| **Total Phase 2** | | ~2 weeks |

### Key Design Decisions

1. **Extend, don't replace** - Build on existing hash-based detection
2. **Conservative default** - When unsure, classify as BREAKING
3. **Column lineage first** - Build lineage before enhanced categorization
4. **SQLGlot dependency** - Same library SQLMesh uses, well-maintained
5. **Incremental rollout** - Can use SQL diff without full lineage initially

---

## Next Steps

1. **Review and approve** this brainstorm with stakeholders
2. **Create detailed spec** for Phase 1 (Interval Tracking)
3. **Set up SQLGlot integration** for lineage parsing
4. **Design interval algebra** (merge, gap detection, overlap handling)
5. **Prototype backfill logic** with sample time-series data
