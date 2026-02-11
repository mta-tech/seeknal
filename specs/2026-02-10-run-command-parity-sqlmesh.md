---
title: "RUN Command Parity with SQLMesh - Full Implementation Plan"
type: feat
date: 2026-02-10
status: ready
source_brainstorm: docs/brainstorms/2026-02-10-run-command-parity-sqlmesh-brainstorm.md
original_plan: docs/plans/2026-02-10-run-command-parity-full-plan.md
---

# Plan: RUN Command Parity with SQLMesh

## Overview

Implement full production-grade `seeknal run` command with feature parity to SQLMesh's execution engine. This 5-phase plan transforms Seeknal from a capable YAML pipeline tool into a production data platform with safety guarantees, performance, and multi-engine portability.

**Total Duration**: 7-12 weeks (5-9 weeks without Phase 5, or 7-12 weeks with Prefect integration)

**Phases Overview**:

| Phase | Name | Duration | Priority | Status |
|-------|------|----------|----------|--------|
| **Phase 1** | Interval Tracking & Backfill | 2-3 weeks | P0 - Foundation | NEW |
| **Phase 2** | Change Detection & Column Lineage | 2-3 weeks | P0 - Must have | Ready to implement |
| **Phase 3** | Plan/Apply Workflow | 1-2 weeks | P1 - Safety | NEW |
| **Phase 4** | Database State Backend | 1-2 weeks | P1 - Production | NEW |
| **Phase 5** | Distributed Execution | 2-3 weeks | P2 - Scale | NEW (Prefect option) |

## Task Description

Implement production-grade `seeknal run` command with feature parity to SQLMesh, including:

1. **Time-series incremental processing** - Only process new data, not full table scans
2. **Backfill support** - Reprocess missing or failed time ranges
3. **Plan/apply workflow** - Interactive preview before execution
4. **SQL-aware change detection** - Distinguish breaking vs non-breaking changes
5. **Database state backend** - Support for distributed runners
6. **Distributed execution** - Multi-machine parallel processing (Prefect integration)

## Objective

Transform Seeknal's `run` command from a basic YAML pipeline executor into a production-grade data platform with:

- **Performance**: <10% overhead for incremental runs vs full table scans
- **Safety**: Plan/apply workflow with change categorization >95% accuracy
- **Scalability**: Support for 100+ distributed workers
- **Reliability**: Database-backed state with high availability
- **Documentation**: All new features fully documented with tested tutorials

## Problem Statement

Seeknal's current `seeknal run` has solid foundations (content hashing, caching, parallel execution) but lacks critical production features:

1. **No time-series incremental processing** - Sources run fully every time, no interval tracking
2. **No backfill support** - Can't reprocess missing or failed time ranges
3. **No plan/apply workflow** - No preview before execution, no approval prompt
4. **No change categorization** - Can't distinguish breaking vs non-breaking changes
5. **File-based state only** - No support for distributed runners

These gaps prevent Seeknal from being a production data pipeline orchestrator.

## Relevant Files

### Existing Files to Modify
- `src/seeknal/workflow/state.py` - Add interval fields, refactor for backend protocol
- `src/seeknal/dag/diff.py` - Extend with SQL-aware change detection
- `src/seeknal/cli/main.py` - Add interval, environment, and state backend commands
- `src/seeknal/context.py` - Add state backend selection
- `tests/dag/test_change_categorization.py` - Extend with new tests

### New Files to Create

**Phase 1: Interval Tracking**
- `src/seeknal/workflow/intervals.py` - Interval calculation logic
- `tests/workflow/test_intervals.py` - Interval calculation tests

**Phase 2: Change Detection**
- `src/seeknal/dag/sql_parser.py` - SQL parsing module
- `src/seeknal/dag/sql_diff.py` - AST-based SQL diffing
- `src/seeknal/dag/lineage.py` - Column-level lineage
- `tests/dag/test_sql_parser.py` - SQL parser tests
- `tests/dag/test_sql_diff.py` - SQL diff tests
- `tests/dag/test_lineage.py` - Lineage tests

**Phase 3: Plan/Apply Workflow**
- `src/seeknal/workflow/environment.py` - Environment management
- `tests/workflow/test_environment.py` - Environment tests

**Phase 4: Database State Backend**
- `src/seeknal/state/backend.py` - StateBackend protocol
- `src/seeknal/state/database_backend.py` - Database implementation
- `tests/state/test_backend.py` - Backend protocol tests
- `tests/state/test_database_backend.py` - Database backend tests

**Phase 5: Distributed Execution**
- `src/seeknal/workflow/prefect_integration.py` - Prefect integration (Option B)
- `tests/workflow/test_prefect_integration.py` - Prefect tests

**Documentation (All Phases)**
- `docs/guides/interval-tracking.md` - Interval tracking guide
- `docs/guides/change-detection.md` - Change detection guide
- `docs/guides/plan-apply-workflow.md` - Plan/apply guide
- `docs/guides/state-backends.md` - State backend guide
- `docs/guides/distributed-execution.md` - Distributed execution guide
- `docs/tutorials/interval-backfill-tutorial.md` - Backfill tutorial
- `docs/tutorials/plan-apply-tutorial.md` - Plan/apply tutorial
- `docs/api/interval-tracking.md` - Interval API reference
- `docs/api/change-detection.md` - Change detection API reference

## Proposed Solution

### Architecture Overview

```
+-----------------------------------------------------------------------+
|                    Seeknal Pipeline Architecture                      |
+-----------------------------------------------------------------------+
|                                                                       |
|  Phase 1: Interval Tracking                                         |
|  +--------------------------------------------------------------+     |
|  | source.events -> [completed_intervals, backfill logic]       |     |
|  +--------------------------------------------------------------+     |
|                                                                       |
|  Phase 2: Change Detection & Lineage                                 |
|  +--------------------------------------------------------------+     |
|  | SQLGlot integration -> column tracking, SQL diff                |     |
|  +--------------------------------------------------------------+     |
|                                                                       |
|  Phase 3: Plan/Apply Workflow                                        |
|  +--------------------------------------------------------------+     |
|  | Virtual environments -> plan/apply/promote workflow              |     |
|  +--------------------------------------------------------------+     |
|                                                                       |
|  Phase 4: Database State Backend                                     |
|  +--------------------------------------------------------------+     |
|  | StateBackend protocol -> File for dev, Database for prod          |     |
|  +--------------------------------------------------------------+     |
|                                                                       |
|  Phase 5: Distributed Execution (Optional)                           |
|  +--------------------------------------------------------------+     |
|  | Option A: Custom workers OR Option B: Prefect integration          |     |
|  +--------------------------------------------------------------+     |
|                                                                       |
+-----------------------------------------------------------------------+
```

### Implementation Phases

```
Phase 1: Intervals (2-3 weeks) -> FOUNDATION
    |
    v
Phase 2: Change Detection (2-3 weeks) -> can run in parallel with Phase 3
    |
    v
Phase 3: Plan/Apply (1-2 weeks) -> depends on Phase 2
    |
    v
Phase 4: Database State (1-2 weeks) -> can run in parallel with Phase 3
    |
    v
Phase 5: Distributed (2-3 weeks OR Prefect 1 week) -> depends on Phase 4
```

**Recommended Sequence**:
1. **Month 1**: Phases 1-2 (Foundation + Change Detection)
2. **Month 2**: Phases 3-4 (Safety + Production)
3. **Month 3**: Phase 5 (Scaling)

## Team Orchestration

As the team lead, you have access to powerful tools for coordinating work across multiple agents. You NEVER write code directly - you orchestrate team members using these tools.

### Task Management Tools

**TaskCreate** - Create tasks in the shared task list:

```python
TaskCreate({
  subject: "Implement interval tracking",
  description: "Create interval calculation logic with cron support...",
  activeForm: "Implementing interval tracking"  # Shows in UI spinner
})
# Returns: taskId (e.g., "1")
```

**TaskUpdate** - Update task status, assignment, or dependencies:

```python
TaskUpdate({
  taskId: "1",
  status: "in_progress",  # pending -> in_progress -> completed
  owner: "builder-backend"
})
```

**TaskList** - View all tasks and their status:

```python
TaskList({})
# Returns: Array of tasks with id, subject, status, owner, blockedBy
```

**TaskGet** - Get full details of a specific task:

```python
TaskGet({ taskId: "1" })
# Returns: Full task including description
```

### Task Dependencies

Use `addBlockedBy` to create sequential dependencies - blocked tasks cannot start until dependencies complete:

```python
# Task 2 depends on Task 1
TaskUpdate({
  taskId: "2",
  addBlockedBy: ["1"]  # Task 2 blocked until Task 1 completes
})
```

### Agent Deployment with Task Tool

**Task** - Deploy an agent to do work:

```python
Task({
  description: "Implement interval tracking",
  prompt: "Implement the interval calculation logic as specified...",
  subagent_type: "general-purpose",
  model: "opus"  # or "haiku" for simple tasks
})
# Returns: agentId (e.g., "a1b2c3")
```

### Resume Pattern

```python
# First deployment
Task({
  description: "Build interval module",
  prompt: "Create interval calculation logic...",
  subagent_type: "general-purpose"
})
# Returns: agentId: "abc123"

# Later - resume SAME agent with full context preserved
Task({
  description: "Continue interval module",
  prompt: "Now add cron schedule support...",
  subagent_type: "general-purpose",
  resume: "abc123"  # Continues with previous context
})
```

### Parallel Execution

Run multiple agents simultaneously with `run_in_background: true`:

```python
# Launch multiple agents in parallel
Task({
  description: "Build SQL parser",
  prompt: "...",
  subagent_type: "general-purpose",
  run_in_background: true
})

Task({
  description: "Build interval calculator",
  prompt: "...",
  subagent_type: "general-purpose",
  run_in_background: true
})

# Both agents now working simultaneously
```

### Team Members

#### Builder - Core Backend
- **Name:** `builder-backend`
- **Role:** Backend developer
- **Agent Type:** `general-purpose`
- **Responsibilities:** Core feature implementation (intervals, state backend, SQL parsing)
- **Resume:** true

#### Builder - CLI & Tools
- **Name:** `builder-cli`
- **Role:** CLI and tooling developer
- **Agent Type:** `general-purpose`
- **Responsibilities:** CLI commands, configuration, migration tools
- **Resume:** true

#### Builder - Tests
- **Name:** `builder-tests`
- **Role:** Test developer
- **Agent Type:** `general-purpose`
- **Responsibilities:** Unit tests, integration tests, test coverage
- **Resume:** true

#### Documentation Updater
- **Name:** `docs-updater`
- **Role:** Technical writer and documentation specialist
- **Agent Type:** `general-purpose`
- **Responsibilities:**
  - Write user guides for all new features
  - Create API reference documentation
  - Write and test tutorial examples
  - Update getting-started guides
  - Document configuration options
- **Resume:** true
- **Critical Deliverables:**
  - All new features must have user guides
  - All tutorials must be tested and verified working
  - API reference must be complete
  - Examples must be copy-paste runnable

#### QA - Tutorial Validator
- **Name:** `qa-tutorials`
- **Role:** Quality assurance for tutorials
- **Agent Type:** `general-purpose`
- **Responsibilities:**
  - Test all tutorials end-to-end
  - Verify examples are copy-paste runnable
  - Report documentation bugs
  - Validate quick start guides
- **Resume:** true

#### Orchestrator
- **Name:** `orchestrator`
- **Role:** Team lead and coordinator
- **Agent Type:** `general-purpose`
- **Responsibilities:** Task assignment, dependency management, progress tracking

## Step by Step Tasks

### Phase 1: Interval Tracking & Smart Incremental Execution (2-3 weeks)

#### 1.1 Extend State Schema (2 days)
- **Task ID:** phase1-task1
- **Depends On:** none
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to modify:**
- `src/seeknal/workflow/state.py` - Add interval fields to NodeState

**Implementation:**
```python
@dataclass
class NodeState:
    # Existing fields
    hash: str
    last_run: datetime
    status: str
    duration_ms: int
    row_count: int

    # NEW: Interval tracking fields
    completed_intervals: list[tuple[datetime, datetime]] = field(default_factory=list)
    completed_partitions: list[str] = field(default_factory=list)
    restatement_intervals: list[tuple[datetime, datetime]] = field(default_factory=list)
```

**Acceptance Criteria:**
- [ ] NodeState extended with interval fields
- [ ] State load/save handles interval data correctly
- [ ] Backward compatible with existing state files

#### 1.2 Interval Calculation (2-3 days)
- **Task ID:** phase1-task2
- **Depends On:** phase1-task1
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to create:**
- `src/seeknal/workflow/intervals.py` - Interval calculation logic
- `tests/workflow/test_intervals.py` - Tests

**Acceptance Criteria:**
- [ ] `IntervalCalculator` class with interval calculation
- [ ] Cron-based interval generation using python-croniter
- [ ] Interval merge logic for handling overlaps
- [ ] Gap detection for finding missing intervals
- [ ] Unit tests for interval calculation

#### 1.3 CLI Integration - Intervals (2 days)
- **Task ID:** phase1-task3
- **Depends On:** phase1-task2
- **Assigned To:** `builder-cli`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to modify:**
- `src/seeknal/cli/main.py` - Add interval commands

**Acceptance Criteria:**
- [ ] `seeknal intervals` command shows interval status
- [ ] `seeknal run --start/--end` for backfill
- [ ] `seeknal run --backfill` processes missing intervals
- [ ] `seeknal run --restate` marks intervals for reprocessing

#### 1.4 Tests for Intervals (1-2 days)
- **Task ID:** phase1-task4
- **Depends On:** phase1-task2, phase1-task3
- **Assigned To:** `builder-tests`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to create:**
- `tests/workflow/test_intervals.py` - Comprehensive tests

**Acceptance Criteria:**
- [ ] Tests for interval calculation
- [ ] Tests for cron schedule expansion
- [ ] Tests for interval merge logic
- [ ] Tests for gap detection
- [ ] All tests passing

#### 1.5 Documentation - Interval Tracking (2 days)
- **Task ID:** phase1-docs
- **Depends On:** phase1-task4
- **Assigned To:** `docs-updater`
- **Agent Type:** `general-purpose`
- **Parallel:** true

**Files to create:**
- `docs/guides/interval-tracking.md` - User guide
- `docs/tutorials/interval-backfill-tutorial.md` - Tutorial
- `docs/api/interval-tracking.md` - API reference

**Acceptance Criteria:**
- [ ] User guide explaining interval tracking concepts
- [ ] Tutorial with copy-paste runnable examples
- [ ] API reference for all interval functions
- [ ] Examples tested and verified working

#### 1.6 Tutorial Validation - Intervals (1 day)
- **Task ID:** phase1-qa
- **Depends On:** phase1-docs
- **Assigned To:** `qa-tutorials`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Acceptance Criteria:**
- [ ] Tutorial tested end-to-end
- [ ] Examples verified copy-paste runnable
- [ ] Documentation bugs reported and fixed

---

### Phase 2: Change Detection & Column Lineage (2-3 weeks)

#### 2.1 SQLGlot Integration (2-3 days)
- **Task ID:** phase2-task1
- **Depends On:** none
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** true (with Phase 1 after task 1.1)

**Files to create:**
- `src/seeknal/dag/sql_parser.py` - SQL parsing module

**Dependencies:**
```toml
dependencies = [
    "sqlglot~=25.0.0",
]
```

**Acceptance Criteria:**
- [ ] `sqlglot~=25.0.0` added to dependencies
- [ ] `SQLParser` class with normalize, extract_columns, extract_dependencies
- [ ] Unit tests for SQL normalization and column extraction

#### 2.2 SQL Diff Analysis (3-4 days)
- **Task ID:** phase2-task2
- **Depends On:** phase2-task1
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to create:**
- `src/seeknal/dag/sql_diff.py` - AST-based SQL diffing
- `tests/dag/test_sql_diff.py` - Tests

**Acceptance Criteria:**
- [ ] `SQLDiffer` class with AST-based SQL comparison
- [ ] Edit type classification (INSERT/DELETE/UPDATE)
- [ ] Change categorization based on edit types
- [ ] Unit tests for various SQL patterns

#### 2.3 Column Lineage Builder (3-4 days)
- **Task ID:** phase2-task3
- **Depends On:** phase2-task1
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to create:**
- `src/seeknal/dag/lineage.py` - Column-level lineage
- `tests/dag/test_lineage.py` - Tests

**Acceptance Criteria:**
- [ ] `ColumnLineage` dataclass with output_columns, input_dependencies
- [ ] `LineageBuilder` class for building column dependency graph
- [ ] Extract output columns from SQL
- [ ] Extract input dependencies from SQL
- [ ] Unit tests for lineage building

#### 2.4 Enhanced Change Categorization (2-3 days)
- **Task ID:** phase2-task4
- **Depends On:** phase2-task2
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to modify:**
- `src/seeknal/dag/diff.py` - Extend with SQL-aware logic

**Acceptance Criteria:**
- [ ] Extended `categorize_change()` with SQL-aware logic
- [ ] Metadata-only change detection (fast path)
- [ ] SQL normalization before comparison
- [ ] Backward compatibility with existing hash-based fallback

#### 2.5 CLI Integration - Change Detection (2-3 days)
- **Task ID:** phase2-task5
- **Depends On:** phase2-task4
- **Assigned To:** `builder-cli`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to modify:**
- `src/seeknal/dag/diff.py` - Enhanced plan output

**Acceptance Criteria:**
- [ ] Enhanced plan output with SQL diffs
- [ ] Fine-grained change types (COLUMN_ADDED, SQL_LOGIC_CHANGED)
- [ ] Column-level downstream impact display
- [ ] Backward compatible with existing CLI

#### 2.6 Tests for Change Detection (2-3 days)
- **Task ID:** phase2-tests
- **Depends On:** phase2-task5
- **Assigned To:** `builder-tests`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Acceptance Criteria:**
- [ ] ~40 new tests passing
- [ ] All 27 existing tests still passing
- [ ] Change categorization accuracy >95% (validated by tests)

#### 2.7 Documentation - Change Detection (2 days)
- **Task ID:** phase2-docs
- **Depends On:** phase2-tests
- **Assigned To:** `docs-updater`
- **Agent Type:** `general-purpose`
- **Parallel:** true

**Files to create:**
- `docs/guides/change-detection.md` - User guide
- `docs/api/change-detection.md` - API reference

**Acceptance Criteria:**
- [ ] User guide explaining change detection
- [ ] Examples of breaking vs non-breaking changes
- [ ] API reference complete
- [ ] All examples tested

#### 2.8 Tutorial Validation - Change Detection (1 day)
- **Task ID:** phase2-qa
- **Depends On:** phase2-docs
- **Assigned To:** `qa-tutorials`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Acceptance Criteria:**
- [ ] Tutorial tested end-to-end
- [ ] Examples verified runnable

---

### Phase 3: Plan/Apply Workflow (1-2 weeks)

#### 3.1 Environment Manager (1 week)
- **Task ID:** phase3-task1
- **Depends On:** phase2-tests
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to create:**
- `src/seeknal/workflow/environment.py` - Environment management
- `tests/workflow/test_environment.py` - Tests

**Implementation:**
```python
@dataclass
class EnvironmentConfig:
    name: str
    state_path: Path
    base: str | None

class EnvironmentPlan:
    environment: str
    changes: list[NodeChange]
    created_at: datetime
    expires_at: datetime | None

class EnvironmentManager:
    def plan(self, environment: str) -> EnvironmentPlan:
        """Create a plan showing changes for the environment."""

    def apply(self, plan: EnvironmentPlan) -> None:
        """Apply the approved plan."""

    def promote(self, source: str, target: str) -> None:
        """Promote source environment to target."""
```

**Acceptance Criteria:**
- [ ] `EnvironmentManager` class with plan/apply/promote methods
- [ ] Virtual environments reference production for unchanged nodes
- [ ] Atomic promotion via temp directory + rename
- [ ] TTL-based cleanup for expired environments

#### 3.2 CLI Commands - Environments (3-4 days)
- **Task ID:** phase3-task2
- **Depends On:** phase3-task1
- **Assigned To:** `builder-cli`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to modify:**
- `src/seeknal/cli/main.py` - Add environment subcommands

**Acceptance Criteria:**
- [ ] `seeknal plan <env>` shows categorized changes
- [ ] `seeknal apply <env>` executes in isolated environment
- [ ] `seeknal promote <source> <target>` atomically promotes
- [ ] `seeknal env list` shows all environments
- [ ] `seeknal env delete <name>` removes environment

#### 3.3 Documentation - Plan/Apply (2 days)
- **Task ID:** phase3-docs
- **Depends On:** phase3-task2
- **Assigned To:** `docs-updater`
- **Agent Type:** `general-purpose`
- **Parallel:** true

**Files to create:**
- `docs/guides/plan-apply-workflow.md` - User guide
- `docs/tutorials/plan-apply-tutorial.md` - Tutorial

**Acceptance Criteria:**
- [ ] User guide explaining plan/apply concepts
- [ ] Tutorial with copy-paste runnable examples
- [ ] All examples tested

#### 3.4 Tutorial Validation - Plan/Apply (1 day)
- **Task ID:** phase3-qa
- **Depends On:** phase3-docs
- **Assigned To:** `qa-tutorials`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Acceptance Criteria:**
- [ ] Tutorial tested end-to-end
- [ ] Examples verified runnable

---

### Phase 4: Database State Backend (1-2 weeks)

#### 4.1 State Backend Protocol (2-3 days)
- **Task ID:** phase4-task1
- **Depends On:** none (parallel with Phase 3)
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** true

**Files to create:**
- `src/seeknal/state/backend.py` - StateBackend protocol

**Acceptance Criteria:**
- [ ] `StateBackend` ABC with core methods
- [ ] Protocol designed for both file and database backends
- [ ] Transaction support for atomic updates

#### 4.2 File Backend Refactor (2 days)
- **Task ID:** phase4-task2
- **Depends On:** phase4-task1
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to modify:**
- `src/seeknal/workflow/state.py` - Refactor to use protocol

**Acceptance Criteria:**
- [ ] File operations extracted to `FileStateBackend`
- [ ] Existing `seeknal run` unchanged behavior
- [ ] Backward compatible with existing state files

#### 4.3 Database Backend (3-4 days)
- **Task ID:** phase4-task3
- **Depends On:** phase4-task1
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to create:**
- `src/seeknal/state/database_backend.py` - Database implementation
- `tests/state/test_database_backend.py` - Tests

**Database schema:**
```sql
CREATE TABLE run_state (
    run_id TEXT PRIMARY KEY,
    started_at TIMESTAMP,
    config JSONB
);

CREATE TABLE node_state (
    node_id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES run_state(run_id),
    hash TEXT,
    status TEXT,
    duration_ms INTEGER,
    row_count INTEGER,
    fingerprint JSONB,
    completed_intervals JSONB,
    completed_partitions JSONB
);
```

**Acceptance Criteria:**
- [ ] `DatabaseStateBackend` implements full protocol
- [ ] Database schema created automatically
- [ ] Transaction support for atomic operations
- [ ] Unit tests for database backend

#### 4.4 Configuration & Migration (1-2 days)
- **Task ID:** phase4-task4
- **Depends On:** phase4-task3
- **Assigned To:** `builder-cli`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to modify:**
- `src/seeknal/context.py` - Add state backend selection

**Acceptance Criteria:**
- [ ] Configuration option in `seeknal_project.yml`
- [ ] `seeknal migrate-state` command for migration
- [ ] Auto-detection (file vs database) working
- [ ] Migration preserves all existing state

#### 4.5 Documentation - State Backends (2 days)
- **Task ID:** phase4-docs
- **Depends On:** phase4-task4
- **Assigned To:** `docs-updater`
- **Agent Type:** `general-purpose`
- **Parallel:** true

**Files to create:**
- `docs/guides/state-backends.md` - User guide

**Acceptance Criteria:**
- [ ] User guide explaining state backend options
- [ ] Configuration examples for file and database
- [ ] Migration guide from file to database

#### 4.6 Tutorial Validation - State Backends (1 day)
- **Task ID:** phase4-qa
- **Depends On:** phase4-docs
- **Assigned To:** `qa-tutorials`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Acceptance Criteria:**
- [ ] Examples tested and verified

---

### Phase 5: Distributed Execution (2-3 weeks OR Prefect 1 week)

#### 5.1 Prefect Integration (1 week) - RECOMMENDED
- **Task ID:** phase5-task1
- **Depends On:** phase4-task4
- **Assigned To:** `builder-backend`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to create:**
- `src/seeknal/workflow/prefect_integration.py` - Prefect integration
- `tests/workflow/test_prefect_integration.py` - Tests

**Implementation:**
```python
from prefect import flow

@flow(name="seeknal-run-pipeline")
def seeknal_run_flow(project_path: str = "."):
    cmd = ["seeknal", "run", "--project-path", project_path, "--parallel"]
    result = subprocess.run(cmd, capture_output=True)
    return result
```

**Acceptance Criteria:**
- [ ] Prefect flow wrapping `seeknal run`
- [ ] Configuration for scheduled runs
- [ ] Integration with Prefect UI
- [ ] Tests for Prefect integration

#### 5.2 Documentation - Distributed Execution (2 days)
- **Task ID:** phase5-docs
- **Depends On:** phase5-task1
- **Assigned To:** `docs-updater`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Files to create:**
- `docs/guides/distributed-execution.md` - User guide

**Acceptance Criteria:**
- [ ] User guide for Prefect integration
- [ ] Setup instructions for Prefect server
- [ ] Examples of scheduled pipelines

#### 5.3 Tutorial Validation - Distributed (1 day)
- **Task ID:** phase5-qa
- **Depends On:** phase5-docs
- **Assigned To:** `qa-tutorials`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Acceptance Criteria:**
- [ ] Tutorial tested end-to-end
- [ ] Examples verified runnable

---

### Final Documentation & Integration (1 week)

#### 6.1 Update Getting Started Guide (2 days)
- **Task ID:** final-docs1
- **Depends On:** phase2-qa, phase3-qa, phase4-qa
- **Assigned To:** `docs-updater`
- **Agent Type:** `general-purpose`
- **Parallel:** true

**Files to modify:**
- `docs/getting-started-comprehensive.md` - Add new features

**Acceptance Criteria:**
- [ ] Interval tracking added to quick start
- [ ] Plan/apply workflow documented
- [ ] Change detection explained
- [ ] All examples tested

#### 6.2 Complete API Reference (2 days)
- **Task ID:** final-docs2
- **Depends On:** phase2-qa, phase3-qa, phase4-qa
- **Assigned To:** `docs-updater`
- **Agent Type:** `general-purpose`
- **Parallel:** true

**Files to create/update:**
- `docs/api/` - Complete API reference

**Acceptance Criteria:**
- [ ] All new APIs documented
- [ ] All functions have docstrings
- [ ] Examples for all major functions

#### 6.3 Tutorial Suite Verification (2 days)
- **Task ID:** final-qa
- **Depends On:** final-docs1, final-docs2
- **Assigned To:** `qa-tutorials`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Acceptance Criteria:**
- [ ] All tutorials tested end-to-end
- [ ] All examples verified copy-paste runnable
- [ ] Quick start guide tested
- [ ] Documentation bugs fixed

#### 6.4 Release Notes & Changelog (1 day)
- **Task ID:** final-release
- **Depends On:** final-qa
- **Assigned To:** `docs-updater`
- **Agent Type:** `general-purpose`
- **Parallel:** false

**Acceptance Criteria:**
- [ ] Release notes drafted
- [ ] Changelog updated
- [ ] Migration guide included

## Acceptance Criteria

### Functional Requirements

**Phase 1: Interval Tracking**
- [ ] Backfill processes only missing intervals (<10% overhead)
- [ ] Interval state persists across runs
- [ ] `seeknal intervals` shows completed/missing intervals
- [ ] Existing `seeknal run` works unchanged

**Phase 2: Change Detection**
- [ ] SQLGlot~=25.0.0 added to dependencies
- [ ] SQL parsing adds <100ms overhead
- [ ] Change categorization accuracy >95%
- [ ] ~40 new tests passing
- [ ] All 27 existing tests passing

**Phase 3: Plan/Apply**
- [ ] Interactive plan/apply workflow working
- [ ] Plan staleness detection with `--force`
- [ ] Environment isolation
- [ ] Atomic promotion

**Phase 4: Database State**
- [ ] State backend protocol implemented
- [ ] File backend refactored and working
- [ ] Database backend with SQLite/Turso support
- [ ] State migration tool working

**Phase 5: Distributed**
- [ ] Workers can process intervals in parallel
- [ ] Or: Prefect integration with scheduling

### Non-Functional Requirements

**Performance**
- [ ] Incremental runs <10% overhead vs full table scan
- [ ] SQL parsing <100ms overhead
- [ ] State load/save <100ms (database)

**Reliability**
- [ ] Backward compatible with existing state files
- [ ] All existing tests passing
- [ ] Transaction support for state updates

**Scalability**
- [ ] Support for 100+ distributed workers (Phase 5)
- [ ] Database state backend for production

### Quality Gates

**Testing**
- [ ] Unit tests for all new modules
- [ ] Integration tests for end-to-end workflows
- [ ] Test coverage >80% for new code

**Documentation**
- [ ] All new features have user guides
- [ ] All tutorials tested and verified working
- [ ] API reference complete
- [ ] Examples are copy-paste runnable

**Code Quality**
- [ ] Black formatting (line length 100)
- [ ] Type hints on all public APIs
- [ ] Docstrings (Google style)

## Success Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| Incremental execution overhead | N/A | <10% | Benchmark timing |
| Change categorization accuracy | ~70% | >95% | Test validation |
| State backend performance | N/A | <100ms | Benchmark timing |
| Distributed execution | ❌ No | 100+ workers | Production deployment |
| Plan preview accuracy | N/A | 100% | User feedback |
| Documentation coverage | Partial | 100% | All features documented |
| Tutorial test pass rate | N/A | 100% | All tutorials tested |

## Dependencies & Prerequisites

### External Dependencies

| Dependency | Version | Purpose | Risk |
|------------|---------|---------|------|
| sqlglot | ~=25.0.0 | SQL parsing and AST diffing | Low - stable library |
| python-croniter | ~=3.0.0 | Cron schedule expansion | Low - mature library |
| sqlalchemy | >=2.0 | Database state backend | Low - standard library |
| prefect | >=3.0 | Distributed execution (optional) | Low - optional dependency |

### Internal Dependencies

| Dependency | Status | Notes |
|------------|--------|-------|
| Phase 1 | NEW | Foundation for intervals |
| Phase 2 | Ready | Can run in parallel with Phase 3 |
| Phase 3 | NEW | Depends on Phase 2 |
| Phase 4 | NEW | Can run in parallel with Phase 3 |
| Phase 5 | NEW | Depends on Phase 4 |

## Risk Analysis & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| SQLGlot API changes | Low | High | Use ~= for version pinning |
| Database migration issues | Medium | High | Comprehensive testing, backup tool |
| Tutorial example failures | Medium | Medium | QA validation before release |
| Prefect integration complexity | Low | Low | Optional feature, can defer |
| Documentation lag | High | Medium | Dedicated docs role, continuous updates |

## Resource Requirements

### Development Team

| Role | Count | Allocation |
|------|-------|------------|
| Backend Developer | 1 | 100% across all phases |
| CLI Developer | 1 | 50% across all phases |
| Test Developer | 1 | 50% across all phases |
| Documentation Specialist | 1 | 30% continuous |
| QA (Tutorial Validator) | 1 | 20% continuous |
| Team Lead/Orchestrator | 1 | 20% coordination |

### Development Time Estimate

| Phase | Complexity | Estimate | Buffer | Total |
|-------|------------|----------|-------|-------|
| Phase 1: Intervals | Medium | 10 days | 20% | 12 days |
| Phase 2: Change Detection | Medium | 12 days | 20% | 14 days |
| Phase 3: Plan/Apply | Low | 7 days | 20% | 8 days |
| Phase 4: Database State | Medium | 8 days | 20% | 10 days |
| Phase 5: Distributed | Low (Prefect) | 5 days | 20% | 6 days |
| Final: Docs & QA | Medium | 7 days | 20% | 8 days |
| **TOTAL** | | | | **58 days (~12 weeks)** |

## Documentation Plan

### Guides

| Document | Location | When | Owner |
|----------|----------|------|-------|
| Interval Tracking Guide | `docs/guides/interval-tracking.md` | Phase 1 | docs-updater |
| Change Detection Guide | `docs/guides/change-detection.md` | Phase 2 | docs-updater |
| Plan/Apply Guide | `docs/guides/plan-apply-workflow.md` | Phase 3 | docs-updater |
| State Backends Guide | `docs/guides/state-backends.md` | Phase 4 | docs-updater |
| Distributed Execution Guide | `docs/guides/distributed-execution.md` | Phase 5 | docs-updater |

### Tutorials

| Document | Location | When | Owner |
|----------|----------|------|-------|
| Interval Backfill Tutorial | `docs/tutorials/interval-backfill-tutorial.md` | Phase 1 | docs-updater |
| Plan/Apply Tutorial | `docs/tutorials/plan-apply-tutorial.md` | Phase 3 | docs-updater |

### API Reference

| Document | Location | When | Owner |
|----------|----------|------|-------|
| Interval Tracking API | `docs/api/interval-tracking.md` | Phase 1 | docs-updater |
| Change Detection API | `docs/api/change-detection.md` | Phase 2 | docs-updater |

### Updates

| Document | Updates | When | Owner |
|----------|---------|------|-------|
| Getting Started | Add new features | Final | docs-updater |
| README | Update features list | Final | docs-updater |
| CLAUDE.md | Add patterns | Final | orchestrator |

## Validation Commands

### Phase 1: Interval Tracking

```bash
# Run interval tests
pytest tests/workflow/test_intervals.py -v

# Test interval CLI
seeknal intervals
seeknal run --backfill
seeknal run --start 2026-01-01 --end 2026-01-31
```

### Phase 2: Change Detection

```bash
# Run change detection tests
pytest tests/dag/test_sql_parser.py tests/dag/test_sql_diff.py tests/dag/test_lineage.py -v

# Verify existing tests still pass
pytest tests/dag/test_change_categorization.py -v
```

### Phase 3: Plan/Apply

```bash
# Run environment tests
pytest tests/workflow/test_environment.py -v

# Test plan/apply CLI
seeknal plan dev
seeknal apply dev
seeknal promote dev staging
```

### Phase 4: Database State

```bash
# Run backend tests
pytest tests/state/test_backend.py tests/state/test_database_backend.py -v

# Test migration
seeknal migrate-state --from file --to database
```

### Phase 5: Distributed

```bash
# Run Prefect tests
pytest tests/workflow/test_prefect_integration.py -v

# Test Prefect flow
python -c "from seeknal.workflow.prefect_integration import seeknal_run_flow; seeknal_run_flow()"
```

### Final: Tutorial Validation

```bash
# Test all tutorials
# (Manual verification by qa-tutorials)

# Run full test suite
pytest -v

# Check documentation coverage
# (Manual review)
```

## Notes

### Key Design Decisions

1. **Interval granularity**: Per-node for flexibility
2. **State backend migration**: Manual `seeknal migrate-state`
3. **SQLGlot version**: ~=25.0.0 for stability
4. **Distributed execution**: Prefect recommended over custom workers
5. **Partition format**: strftime format strings

### Documentation Strategy

All new features MUST have:
1. User guide explaining concepts
2. Tutorial with copy-paste runnable examples
3. API reference for developers
4. Integration with getting-started guide

Documentation is developed in parallel with implementation and validated by QA before release.

### Testing Strategy

- Unit tests for all new modules
- Integration tests for end-to-end workflows
- Existing tests must continue passing
- Tutorial examples validated as runnable code

---

## Checklist Summary

### Phase 1: Interval Tracking 🟡
- [ ] 1.1 Extend State Schema (2 days)
- [ ] 1.2 Interval Calculation (2-3 days)
- [ ] 1.3 CLI Integration - Intervals (2 days)
- [ ] 1.4 Tests for Intervals (1-2 days)
- [ ] 1.5 Documentation - Interval Tracking (2 days)
- [ ] 1.6 Tutorial Validation - Intervals (1 day)

### Phase 2: Change Detection 🟡
- [ ] 2.1 SQLGlot Integration (2-3 days)
- [ ] 2.2 SQL Diff Analysis (3-4 days)
- [ ] 2.3 Column Lineage Builder (3-4 days)
- [ ] 2.4 Enhanced Change Categorization (2-3 days)
- [ ] 2.5 CLI Integration - Change Detection (2-3 days)
- [ ] 2.6 Tests for Change Detection (2-3 days)
- [ ] 2.7 Documentation - Change Detection (2 days)
- [ ] 2.8 Tutorial Validation - Change Detection (1 day)

### Phase 3: Plan/Apply Workflow ⬜
- [ ] 3.1 Environment Manager (1 week)
- [ ] 3.2 CLI Commands - Environments (3-4 days)
- [ ] 3.3 Documentation - Plan/Apply (2 days)
- [ ] 3.4 Tutorial Validation - Plan/Apply (1 day)

### Phase 4: Database State Backend ⬜
- [ ] 4.1 State Backend Protocol (2-3 days)
- [ ] 4.2 File Backend Refactor (2 days)
- [ ] 4.3 Database Backend (3-4 days)
- [ ] 4.4 Configuration & Migration (1-2 days)
- [ ] 4.5 Documentation - State Backends (2 days)
- [ ] 4.6 Tutorial Validation - State Backends (1 day)

### Phase 5: Distributed Execution ⬜
- [ ] 5.1 Prefect Integration (1 week) - RECOMMENDED
- [ ] 5.2 Documentation - Distributed Execution (2 days)
- [ ] 5.3 Tutorial Validation - Distributed (1 day)

### Final: Documentation & Integration ⬜
- [ ] 6.1 Update Getting Started Guide (2 days)
- [ ] 6.2 Complete API Reference (2 days)
- [ ] 6.3 Tutorial Suite Verification (2 days)
- [ ] 6.4 Release Notes & Changelog (1 day)
