---
title: "SQLMesh-Inspired Features (Phase 1 + StarRocks/Semantic Layer)"
type: feat
date: 2026-02-08
status: ready
source: docs/plans/2026-02-08-feat-sqlmesh-inspired-features-plan.md
brainstorm: docs/brainstorms/2026-02-08-sqlmesh-inspired-features-brainstorm.md
---

# Plan: SQLMesh-Inspired Features

## Overview

Three features that transform Seeknal from a capable YAML pipeline tool into a production-grade data platform:

1. **Feature 2: Content-Based Fingerprinting & Smart Incremental Execution** — Fix broken incremental runs, persist source outputs, hash-based change detection
2. **Feature 4: Built-in Data Quality Audits** — Inline audit rules on any node, automatic execution during pipeline runs
3. **Feature 11: StarRocks Integration & Semantic Layer** — OLAP serving layer + metric definitions for self-service analytics

**Key Deliverables:**
- Unified state system with content-based fingerprinting
- Source output caching to Parquet (fixes "Table does not exist" DuckDB bug)
- Smart execution decisions (skip unchanged nodes, propagate changes downstream)
- Inline data quality audits with 5 built-in types
- StarRocks connection, REPL, source/exposure executors
- Semantic model + metric YAML parser, SQL compilation engine, `seeknal query` CLI

**Architecture Note:**
- Python 3.11+, DuckDB as primary engine, Typer CLI
- Executor registry pattern with `@register_executor()` decorator
- State persisted to `target/run_state.json` (atomic writes)
- DAG via `Manifest` with `Node`/`Edge` dataclasses in `src/seeknal/dag/manifest.py`

## Task Description

Implement three SQLMesh-inspired features for the Seeknal workflow system:

**Feature 2** solves the critical bug where DuckDB in-memory views disappear when source nodes are cached/skipped, causing downstream transforms to fail with "Table does not exist". The fix: persist source outputs to Parquet in `target/cache/`, reload as DuckDB views when needed. Also adds content-based fingerprinting (SHA256 of normalized functional content + upstream hashes) for intelligent change detection.

**Feature 4** adds inline `audits:` YAML section to any node, with 5 built-in audit types (not_null, unique, accepted_values, row_count, custom_sql) that execute in `BaseExecutor.post_execute()`.

**Feature 11** integrates StarRocks via pymysql (MySQL protocol, port 9030) for OLAP serving, and adds a semantic layer with YAML-defined semantic models (entities, dimensions, measures) and metrics (simple, derived, ratio, cumulative) compiled to SQL.

## Objective

- `seeknal run` on unchanged pipeline: all nodes skipped, completes in <5s
- Edit one transform SQL, `seeknal run`: only that node + downstream re-execute
- All 5 audit types pass correctness tests with severity-based control flow
- `seeknal query --metrics X --dimensions Y` returns correct results via DuckDB or StarRocks
- Generated SQL matches hand-written SQL for all 4 metric types

## Problem Statement

- **Incremental execution is broken**: Source views disappear from in-memory DuckDB when cached, causing transforms to fail with "Table does not exist"
- **No data quality gates**: Bad data propagates silently through the pipeline
- **No serving layer**: Features computed in DuckDB with no path to low-latency analytics
- **No metric consistency**: Each BI tool defines metrics differently

## Proposed Solution

See full technical approach in `docs/plans/2026-02-08-feat-sqlmesh-inspired-features-plan.md`.

Key architectural decisions:
- **State unification**: Consolidate `state.py` (target/run_state.json) and `runner.py` (target/state/execution_state.json) into single state system
- **NodeFingerprint dataclass**: content_hash, schema_hash, upstream_hash, config_hash
- **Cache storage**: `target/cache/{kind}/{node_id}.parquet`
- **Audit engine**: DuckDB-native SQL queries, no PySpark dependency
- **StarRocks driver**: pymysql (pure Python, MySQL protocol compatible)
- **Semantic SQL compiler**: Entity graph for join planning, MetricCompiler class

## Relevant Files

### Existing Files (to modify)
- `src/seeknal/workflow/state.py` — State management, hash calculation, change detection
- `src/seeknal/workflow/runner.py` — DAG execution orchestration (uses separate state file - BLOCKER)
- `src/seeknal/workflow/executors/base.py` — BaseExecutor, ExecutorResult, ExecutorRegistry, ExecutionContext
- `src/seeknal/workflow/executors/source_executor.py` — Source loading (already has cache write logic)
- `src/seeknal/workflow/executors/transform_executor.py` — SQL transforms (needs cache read in pre_execute)
- `src/seeknal/workflow/executors/exposure_executor.py` — Exposure output (extend for StarRocks MV)
- `src/seeknal/dag/manifest.py` — NodeType enum, Node/Edge/Manifest dataclasses
- `src/seeknal/dag/parser.py` — YAML parsing, manifest building
- `src/seeknal/cli/main.py` — CLI commands
- `src/seeknal/cli/repl.py` — SQL REPL (extend for StarRocks)

### New Files to Create
- `src/seeknal/workflow/audits.py` — Audit engine with 5 built-in types
- `src/seeknal/connections/starrocks.py` — StarRocks connection factory
- `src/seeknal/workflow/semantic/` — New directory
  - `__init__.py`
  - `models.py` — SemanticModel, Metric, Entity, Dimension, Measure dataclasses
  - `compiler.py` — MetricCompiler SQL generation
  - `deploy.py` — StarRocks MV deployment
- `tests/workflow/test_state_unified.py` — State unification tests
- `tests/workflow/test_fingerprint.py` — Fingerprint computation tests
- `tests/workflow/test_cache.py` — Cache read/write tests
- `tests/workflow/test_audits.py` — Audit engine tests
- `tests/workflow/test_semantic.py` — Semantic layer tests

## Implementation Phases

### Phase 1: Unify State System (Foundation - BLOCKER)

**Tasks:**
1.1. **Audit state read/write points**
- Map all state I/O in `state.py` (target/run_state.json) and `runner.py` (target/state/execution_state.json)
- Document which fields are used where
- Files: `src/seeknal/workflow/state.py`, `src/seeknal/workflow/runner.py`

1.2. **Consolidate to single state file**
- Migrate `runner.py` to use `state.py`'s `RunState`/`NodeState` with `save_state()`/`load_state()`
- Add schema version field `"schema_version": "2.0"` for future migrations
- Ensure backward compatibility: `from_dict()` handles old schema gracefully
- Remove `runner.py`'s `_load_state()`/`_save_state()` methods, replace with `state.py` functions
- Files: `src/seeknal/workflow/state.py`, `src/seeknal/workflow/runner.py`

1.3. **Tests: state persistence roundtrip, schema migration**
- Files: `tests/workflow/test_state_unified.py`

**Success Criteria:**
- [ ] Single state file at `target/run_state.json`
- [ ] `runner.py` uses `state.py` functions exclusively
- [ ] Old state files auto-migrated on load
- [ ] Atomic write with temp file + rename (already in state.py)

---

### Phase 2: Fingerprint Computation

**Tasks:**
2.1. **Add NodeFingerprint dataclass to state.py**
- `content_hash`: SHA256 of normalized functional YAML (extend existing `calculate_node_hash`)
- `schema_hash`: SHA256 of output column names + types
- `upstream_hash`: SHA256 of sorted upstream fingerprints
- `config_hash`: SHA256 of non-functional config
- Files: `src/seeknal/workflow/state.py`

2.2. **Add fingerprint to NodeState, update save/load**
- Extend `NodeState.to_dict()` / `from_dict()` with fingerprint field
- Backward compat: fingerprint=None if loading old state
- Files: `src/seeknal/workflow/state.py`

2.3. **Compute upstream hash from DAG traversal**
- Topological order traversal, compute fingerprints bottom-up
- Files: `src/seeknal/workflow/state.py`, `src/seeknal/dag/manifest.py`

2.4. **Tests: hash stability, sensitivity, upstream propagation**
- Files: `tests/workflow/test_fingerprint.py`

**Success Criteria:**
- [ ] Same content = same hash (stability)
- [ ] Different content = different hash (sensitivity)
- [ ] Upstream change propagates to downstream fingerprints

---

### Phase 3: Source Output Caching (Critical Bug Fix)

**Tasks:**
3.1. **Create cache directory structure in executor context**
- Add `get_cache_path()` to `ExecutionContext` (already exists but returns state_dir based path)
- Change to `target/cache/{kind}/{node_id}.parquet` pattern
- Files: `src/seeknal/workflow/executors/base.py`

3.2. **SourceExecutor: ensure cache write works correctly**
- Existing source_executor.py already writes to cache_path (line 251-252)
- Verify it works with new cache path structure
- Files: `src/seeknal/workflow/executors/source_executor.py`

3.3. **TransformExecutor: load cached views in pre_execute**
- In `pre_execute()`: check all input refs, if view doesn't exist in DuckDB, load from cache
- `CREATE OR REPLACE VIEW {ref} AS SELECT * FROM read_parquet('{cache_path}')`
- Files: `src/seeknal/workflow/executors/transform_executor.py`

3.4. **Cache invalidation: delete when fingerprint changes**
- In executor flow: if `should_execute()` returns True, delete old cache before re-executing
- Files: `src/seeknal/workflow/executors/base.py`

3.5. **Handle corrupted cache: detect read failures, force re-execution**
- Files: `src/seeknal/workflow/executors/transform_executor.py`

3.6. **Tests: cache write, read, invalidation, corruption recovery**
- Files: `tests/workflow/test_cache.py`

**Success Criteria:**
- [ ] Source outputs persisted to `target/cache/sources/*.parquet`
- [ ] Transform pre_execute loads cached views when DuckDB view missing
- [ ] Cache deleted when fingerprint changes
- [ ] Corrupted cache triggers re-execution

---

### Phase 4: Smart Execution Decisions

**Tasks:**
4.1. **Implement `should_execute()` using fingerprint comparison**
- Replace simple hash comparison in `detect_changes()` with fingerprint-aware logic
- Files: `src/seeknal/workflow/state.py`

4.2. **Update `get_nodes_to_run()` to use fingerprints**
- Files: `src/seeknal/workflow/state.py`

4.3. **Add downstream propagation: changed node marks all downstream**
- Already exists via `find_downstream_nodes()` BFS - verify it integrates with fingerprints
- Files: `src/seeknal/workflow/state.py`

4.4. **Support `--full` flag to bypass caching**
- Files: `src/seeknal/workflow/runner.py`, `src/seeknal/cli/main.py`

4.5. **Support `--nodes X` for selective execution**
- Already in runner.py `_get_nodes_to_run()` - verify integration
- Files: `src/seeknal/workflow/runner.py`

4.6. **CLI output: show skip/execute reasons per node**
- Files: `src/seeknal/workflow/runner.py`

4.7. **Tests: no-op run, single change propagation, full refresh, selective**
- Files: `tests/workflow/test_state_unified.py`

**Success Criteria:**
- [ ] No-op run (all cached) completes in <5s
- [ ] Single content change re-executes only that node + downstream
- [ ] `--full` bypasses all caching
- [ ] CLI shows "Skipping X (cached)" / "Executing Y (content changed)"

---

### Phase 5: Audit Parser & Models (parallelizable with Phase 3+)

**Tasks:**
5.1. **Add AuditResult dataclass to state.py**
- `audit_type`, `passed`, `failing_rows`, `severity`, `message`, `columns`, `duration_ms`
- Files: `src/seeknal/workflow/state.py`

5.2. **Parse `audits:` section from node YAML config**
- In DAG parser, extract audits list and attach to Node.config
- Validate audit schema at parse time (type, required params per type)
- Files: `src/seeknal/dag/parser.py`

5.3. **Tests: YAML parsing, schema validation, invalid audit detection**
- Files: `tests/workflow/test_audits.py`

**Success Criteria:**
- [ ] `audits:` section parsed from YAML into node config
- [ ] Schema validation catches invalid audit types/params
- [ ] AuditResult serializes/deserializes correctly

---

### Phase 6: Audit Engine

**Tasks:**
6.1. **Create `src/seeknal/workflow/audits.py` with audit runner**
- `AuditRunner.run_audits(node, conn, output_table)` -> list[AuditResult]
- Files: `src/seeknal/workflow/audits.py` (new)

6.2. **Implement 5 built-in audit types**
- `NotNullAudit`: `SELECT COUNT(*) FROM __THIS__ WHERE {col} IS NULL`
- `UniqueAudit`: `SELECT {cols}, COUNT(*) FROM __THIS__ GROUP BY {cols} HAVING COUNT(*) > 1`
- `AcceptedValuesAudit`: `SELECT * FROM __THIS__ WHERE {col} NOT IN (...)`
- `RowCountAudit`: `SELECT COUNT(*) FROM __THIS__` + min/max check
- `CustomSqlAudit`: User SQL with `__THIS__` replacement, returns failing rows
- Files: `src/seeknal/workflow/audits.py`

6.3. **SQL injection prevention for custom SQL and column names**
- Use `validation.py` functions
- Files: `src/seeknal/workflow/audits.py`

6.4. **Tests: each audit type (pass + fail), custom SQL, injection blocked**
- Files: `tests/workflow/test_audits.py`

**Success Criteria:**
- [ ] All 5 audit types generate correct DuckDB SQL
- [ ] Zero rows returned = PASS, >0 = FAIL
- [ ] SQL injection attempts blocked

---

### Phase 7: Executor Audit Integration

**Tasks:**
7.1. **Add audit execution to BaseExecutor.post_execute()**
- After `execute()`, run audits if `audits:` present in node config
- On error-severity failure: set `ExecutorResult.status = FAILED`
- On warn-severity failure: add warning to `ExecutorResult.metadata`
- Files: `src/seeknal/workflow/executors/base.py`

7.2. **Store AuditResult in NodeState.metadata["audits"]**
- Files: `src/seeknal/workflow/executors/base.py`, `src/seeknal/workflow/state.py`

7.3. **CLI output: show audit pass/fail per node**
- Files: `src/seeknal/workflow/runner.py`

7.4. **Tests: audit during run (pass/fail/warn), --continue-on-error with audit failure**
- Files: `tests/workflow/test_audits.py`

**Success Criteria:**
- [ ] `severity: error` stops downstream execution
- [ ] `severity: warn` logs warning, continues pipeline
- [ ] Audit results stored in state

---

### Phase 8: Standalone Audit Command

**Tasks:**
8.1. **Add `seeknal audit` CLI command**
- Runs all audits on last execution outputs (from cache)
- `seeknal audit <node>` for single-node audit
- Files: `src/seeknal/cli/main.py`

8.2. **Output format: table with node, audit type, status, details**
- Files: `src/seeknal/cli/main.py`

8.3. **Tests: standalone audit on cached data**
- Files: `tests/cli/test_audit_command.py`

**Success Criteria:**
- [ ] `seeknal audit` works on cached outputs without re-execution
- [ ] Table output shows node, audit type, pass/fail, details

---

### Phase 9: StarRocks Connection (parallelizable with Phase 1+)

**Tasks:**
9.1. **Add pymysql to dependencies**
- Files: `pyproject.toml`

9.2. **Create StarRocks connection factory**
- `create_starrocks_connection(config)` returns pymysql.Connection
- Env var interpolation (`${VAR}`)
- Files: `src/seeknal/connections/starrocks.py` (new)

9.3. **Extend profiles.yml parser for `type: starrocks`**
- Files: `src/seeknal/workflow/materialization/profile_loader.py`

9.4. **Add `seeknal connection test <name>` CLI command**
- Files: `src/seeknal/cli/main.py`

9.5. **Tests: connection creation, env var interpolation, connection test**
- Files: `tests/connections/test_starrocks.py`

**Success Criteria:**
- [ ] `seeknal connection test starrocks_prod` validates connectivity
- [ ] Credentials from env vars work
- [ ] Connection factory returns working pymysql connection

---

### Phase 10: StarRocks REPL

**Tasks:**
10.1. **Extend ReplManager with StarRocks connection type**
- `.connect starrocks://user:pass@host:9030/db`
- Query via pymysql, results as tabulate table
- Files: `src/seeknal/cli/repl.py`

10.2. **`.tables`, `.schema` commands for StarRocks**
- Files: `src/seeknal/cli/repl.py`

10.3. **Error handling: connection failures, query errors**
- Files: `src/seeknal/cli/repl.py`

10.4. **Tests: REPL StarRocks commands (mock pymysql)**
- Files: `tests/cli/test_repl_starrocks.py`

**Success Criteria:**
- [ ] REPL connects to StarRocks and executes queries
- [ ] `.tables` and `.schema` work
- [ ] Errors don't crash REPL

---

### Phase 11: StarRocks Source & Exposure Executors

**Tasks:**
11.1. **Add `"starrocks"` to supported source types in SourceExecutor.validate()**
- Files: `src/seeknal/workflow/executors/source_executor.py`

11.2. **StarRocks source: query via pymysql, load into DuckDB view**
- Add `_load_starrocks()` method
- Files: `src/seeknal/workflow/executors/source_executor.py`

11.3. **StarRocks exposure: generate CREATE MATERIALIZED VIEW DDL**
- Extend exposure_executor.py for `type: starrocks_materialized_view`
- Files: `src/seeknal/workflow/executors/exposure_executor.py`

11.4. **Data type mapping: StarRocks types <-> DuckDB types**
- Files: `src/seeknal/connections/starrocks.py`

11.5. **`seeknal starrocks setup-catalog` generates Iceberg catalog SQL**
- Files: `src/seeknal/cli/main.py`

11.6. **Tests: source read, exposure write, MV creation, type mapping**
- Files: `tests/workflow/test_starrocks_executors.py`

**Success Criteria:**
- [ ] Source node with `type: starrocks` reads data into DuckDB
- [ ] Exposure with `type: starrocks_materialized_view` creates MV on StarRocks
- [ ] Type mapping handles common types correctly

---

### Phase 12: Semantic Model Parser (parallelizable with Phase 9+)

**Tasks:**
12.1. **Add SEMANTIC_MODEL and METRIC to NodeType enum**
- Files: `src/seeknal/dag/manifest.py`

12.2. **Parse `seeknal/semantic_models/*.yml` and `seeknal/metrics/*.yml`**
- Files: `src/seeknal/dag/parser.py`

12.3. **Create semantic model dataclasses**
- SemanticModel, Entity, Dimension, Measure
- Files: `src/seeknal/workflow/semantic/models.py` (new)

12.4. **Build entity graph from entity references**
- Primary/foreign key relationships for join planning
- Files: `src/seeknal/workflow/semantic/models.py`

12.5. **Parse and validate all 4 metric types**
- simple, derived, ratio, cumulative
- Detect circular metric dependencies
- Files: `src/seeknal/workflow/semantic/models.py`

12.6. **Tests: YAML parsing, validation, entity graph, cycle detection**
- Files: `tests/workflow/test_semantic.py`

**Success Criteria:**
- [ ] Semantic model YAML parsed and validated
- [ ] Entity graph built from entity references
- [ ] Circular metric dependencies detected
- [ ] All 4 metric types parse correctly

---

### Phase 13: SQL Compilation Engine

**Tasks:**
13.1. **Create MetricCompiler class**
- `compile(query: MetricQuery) -> str`
- Files: `src/seeknal/workflow/semantic/compiler.py` (new)

13.2. **Implement compilation for each metric type**
- Simple: direct aggregation
- Ratio: numerator/denominator with NULLIF(denominator, 0)
- Cumulative: window functions or CTE with time spine
- Derived: resolve metric dependencies, compose expressions
- Files: `src/seeknal/workflow/semantic/compiler.py`

13.3. **Join planning via entity relationships**
- Multi-model queries resolve joins automatically
- Files: `src/seeknal/workflow/semantic/compiler.py`

13.4. **Filter compilation and dimension expression resolution**
- Time grain: `ordered_at__month` -> `date_trunc('month', ordered_at)`
- Files: `src/seeknal/workflow/semantic/compiler.py`

13.5. **SQL injection prevention for user-provided filters**
- Files: `src/seeknal/workflow/semantic/compiler.py`

13.6. **Tests: SQL for each metric type, multi-model joins, filters, edge cases**
- Files: `tests/workflow/test_semantic.py`

**Success Criteria:**
- [ ] Simple metric generates correct aggregate SQL
- [ ] Ratio metric uses NULLIF for division-by-zero
- [ ] Multi-model queries produce correct JOINs
- [ ] SQL injection blocked in filters

---

### Phase 14: Query CLI Command

**Tasks:**
14.1. **Add `seeknal query` command**
- `--metrics`, `--dimensions`, `--filter`, `--order-by`, `--limit`
- `--compile`: show SQL without execution
- `--connection`: DuckDB (default) or StarRocks
- `--format`: table (default), json, csv
- Files: `src/seeknal/cli/main.py`

14.2. **Output formatting with tabulate**
- Files: `src/seeknal/cli/main.py`

14.3. **Tests: argument parsing, query execution, output formats**
- Files: `tests/cli/test_query_command.py`

**Success Criteria:**
- [ ] `seeknal query --metrics X --dimensions Y` returns correct results
- [ ] `--compile` shows generated SQL
- [ ] `--format json` outputs valid JSON

---

### Phase 15: BI Integration - StarRocks MV Deployment

**Tasks:**
15.1. **`seeknal deploy-metrics --connection starrocks_prod`**
- Generate StarRocks MVs from metric definitions
- MV naming: `mv_{metric_name}`
- Files: `src/seeknal/workflow/semantic/deploy.py` (new), `src/seeknal/cli/main.py`

15.2. **MV refresh strategy configuration**
- `ASYNC EVERY(INTERVAL ...)` configurable per metric
- `force_external_table_query_rewrite` for Iceberg external tables
- Files: `src/seeknal/workflow/semantic/deploy.py`

15.3. **`--dry-run` shows DDL without executing**
- Files: `src/seeknal/workflow/semantic/deploy.py`

15.4. **Handle MV updates: DROP+CREATE for schema changes**
- Files: `src/seeknal/workflow/semantic/deploy.py`

15.5. **Tests: DDL generation, deployment (mock StarRocks), idempotent**
- Files: `tests/workflow/test_semantic_deploy.py`

**Success Criteria:**
- [ ] `seeknal deploy-metrics` creates StarRocks MVs
- [ ] `--dry-run` shows DDL without executing
- [ ] Schema changes handled with DROP+CREATE

## Alternative Approaches Considered

1. **SQLAlchemy for StarRocks** — Rejected: starrocks-sqlalchemy has SQLAlchemy 2.0 compatibility issues. pymysql is lighter and sufficient.
2. **PySpark-based audits** — Rejected: existing validator framework assumes PySpark. DuckDB-native SQL audits avoid dependency.
3. **Redis-based state** — Rejected: file-based state with atomic writes is simpler and has no external dependencies.
4. **dbt MetricFlow integration** — Rejected: too heavyweight. Custom SQL compiler aligned to Seeknal's architecture is simpler.

## Team Orchestration

### Team Members

#### Builder: State & Fingerprinting
- **Name:** builder-state
- **Role:** backend
- **Agent Type:** general-purpose
- **Resume:** true
- **Scope:** Phases 1-4 (state unification, fingerprints, caching, smart execution)

#### Builder: Audit Engine
- **Name:** builder-audits
- **Role:** backend
- **Agent Type:** general-purpose
- **Resume:** true
- **Scope:** Phases 5-8 (audit parser, engine, executor integration, CLI command)

#### Builder: StarRocks Integration
- **Name:** builder-starrocks
- **Role:** backend
- **Agent Type:** general-purpose
- **Resume:** true
- **Scope:** Phases 9-11 (connection, REPL, executors)

#### Builder: Semantic Layer
- **Name:** builder-semantic
- **Role:** backend
- **Agent Type:** general-purpose
- **Resume:** true
- **Scope:** Phases 12-15 (parser, compiler, query CLI, MV deployment)

## Step by Step Tasks

### 1. Audit state read/write points (Phase 1.1)
- **Task ID:** phase-1.1
- **Depends On:** none
- **Assigned To:** builder-state
- **Agent Type:** general-purpose
- **Parallel:** false
- Map all state I/O in state.py and runner.py
- Document which fields each module reads/writes
- Identify backward compatibility requirements

### 2. Consolidate to single state file (Phase 1.2)
- **Task ID:** phase-1.2
- **Depends On:** phase-1.1
- **Assigned To:** builder-state
- **Agent Type:** general-purpose
- **Parallel:** false
- Migrate runner.py to use state.py's RunState/NodeState
- Add schema version field
- Remove runner.py's private state methods

### 3. State unification tests (Phase 1.3)
- **Task ID:** phase-1.3
- **Depends On:** phase-1.2
- **Assigned To:** builder-state
- **Agent Type:** general-purpose
- **Parallel:** false
- State persistence roundtrip tests
- Schema migration tests (old -> new format)

### 4. Add NodeFingerprint to state.py (Phase 2.1-2.3)
- **Task ID:** phase-2
- **Depends On:** phase-1.3
- **Assigned To:** builder-state
- **Agent Type:** general-purpose
- **Parallel:** false
- NodeFingerprint dataclass with 4 hash fields
- Extend NodeState serialization
- DAG traversal for upstream hash computation

### 5. Fingerprint tests (Phase 2.4)
- **Task ID:** phase-2-tests
- **Depends On:** phase-2
- **Assigned To:** builder-state
- **Agent Type:** general-purpose
- **Parallel:** false

### 6. Source output caching (Phase 3)
- **Task ID:** phase-3
- **Depends On:** phase-2
- **Assigned To:** builder-state
- **Agent Type:** general-purpose
- **Parallel:** false
- Update cache path structure
- TransformExecutor pre_execute cache loading
- Cache invalidation and corruption recovery

### 7. Smart execution decisions (Phase 4)
- **Task ID:** phase-4
- **Depends On:** phase-3
- **Assigned To:** builder-state
- **Agent Type:** general-purpose
- **Parallel:** false
- should_execute() with fingerprint comparison
- CLI skip/execute messaging
- --full flag support

### 8. Audit parser & models (Phase 5)
- **Task ID:** phase-5
- **Depends On:** none
- **Assigned To:** builder-audits
- **Agent Type:** general-purpose
- **Parallel:** true (with Phase 3+)
- AuditResult dataclass
- YAML audits: section parsing
- Schema validation

### 9. Audit engine implementation (Phase 6)
- **Task ID:** phase-6
- **Depends On:** phase-5
- **Assigned To:** builder-audits
- **Agent Type:** general-purpose
- **Parallel:** false
- 5 built-in audit types as DuckDB SQL
- SQL injection prevention
- AuditRunner class

### 10. Executor audit integration (Phase 7)
- **Task ID:** phase-7
- **Depends On:** phase-6
- **Assigned To:** builder-audits
- **Agent Type:** general-purpose
- **Parallel:** false
- BaseExecutor.post_execute() audit hook
- Severity-based control flow

### 11. Standalone audit command (Phase 8)
- **Task ID:** phase-8
- **Depends On:** phase-7, phase-3
- **Assigned To:** builder-audits
- **Agent Type:** general-purpose
- **Parallel:** false
- seeknal audit CLI command
- Table output format

### 12. StarRocks connection (Phase 9)
- **Task ID:** phase-9
- **Depends On:** none
- **Assigned To:** builder-starrocks
- **Agent Type:** general-purpose
- **Parallel:** true (independent)
- pymysql dependency, connection factory
- Profile parser extension
- seeknal connection test CLI

### 13. StarRocks REPL (Phase 10)
- **Task ID:** phase-10
- **Depends On:** phase-9
- **Assigned To:** builder-starrocks
- **Agent Type:** general-purpose
- **Parallel:** false
- ReplManager StarRocks support
- .tables, .schema commands

### 14. StarRocks executors (Phase 11)
- **Task ID:** phase-11
- **Depends On:** phase-9
- **Assigned To:** builder-starrocks
- **Agent Type:** general-purpose
- **Parallel:** false (but parallel with phase-10)
- Source executor: starrocks type
- Exposure executor: StarRocks MV DDL
- Type mapping

### 15. Semantic model parser (Phase 12)
- **Task ID:** phase-12
- **Depends On:** none
- **Assigned To:** builder-semantic
- **Agent Type:** general-purpose
- **Parallel:** true (with Phase 9+)
- SEMANTIC_MODEL and METRIC NodeTypes
- YAML parsing for semantic_models/ and metrics/
- Entity graph, cycle detection

### 16. SQL compilation engine (Phase 13)
- **Task ID:** phase-13
- **Depends On:** phase-12
- **Assigned To:** builder-semantic
- **Agent Type:** general-purpose
- **Parallel:** false
- MetricCompiler for 4 metric types
- Join planning, filter compilation
- SQL injection prevention

### 17. Query CLI command (Phase 14)
- **Task ID:** phase-14
- **Depends On:** phase-13
- **Assigned To:** builder-semantic
- **Agent Type:** general-purpose
- **Parallel:** false
- seeknal query command
- Output formats: table, json, csv

### 18. MV deployment (Phase 15)
- **Task ID:** phase-15
- **Depends On:** phase-13, phase-9
- **Assigned To:** builder-semantic
- **Agent Type:** general-purpose
- **Parallel:** false
- seeknal deploy-metrics command
- StarRocks MV generation from metrics
- --dry-run support

## Acceptance Criteria

### Functional Requirements
- [ ] `seeknal run` on unchanged pipeline: all nodes skipped, <5s
- [ ] Edit one transform, `seeknal run`: only changed + downstream re-execute
- [ ] Delete cache file, `seeknal run`: node re-executes automatically
- [ ] `seeknal run --full`: all nodes execute regardless
- [ ] State file survives partial failures (atomic writes)
- [ ] CLI shows skip/execute reasons per node
- [ ] `not_null` audit fails with count of NULL rows
- [ ] `unique` audit fails with count of duplicate rows
- [ ] `accepted_values` fails with invalid values
- [ ] `row_count` with min=1000 on 500-row table fails
- [ ] `custom_sql` with `__THIS__` resolves correctly
- [ ] `severity: error` stops downstream, `severity: warn` continues
- [ ] `seeknal audit` works on cached outputs
- [ ] StarRocks REPL: interactive SQL queries
- [ ] StarRocks source node reads into DuckDB
- [ ] StarRocks exposure creates materialized view
- [ ] `seeknal connection test` validates connectivity
- [ ] Semantic model YAML parsed and validated
- [ ] All 4 metric types generate correct SQL
- [ ] `seeknal query --metrics X --dimensions Y` returns correct results
- [ ] `seeknal query --compile` shows SQL
- [ ] Ratio metrics handle division by zero (NULL)
- [ ] `seeknal deploy-metrics` creates StarRocks MVs
- [ ] SQL injection blocked in filters and custom SQL

### Non-Functional Requirements
- [ ] DAG building <1s for 100 nodes
- [ ] Hash calculation <100ms per file
- [ ] State I/O <500ms
- [ ] No-op run <5s
- [ ] StarRocks queries sub-second for typical metrics

### Quality Gates
- [ ] All existing tests still pass after state unification
- [ ] >80% test coverage on new code
- [ ] No PySpark dependency in new audit code

## Success Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| Incremental run (no changes) | Full re-execution | <5s skip | Time `seeknal run` on unchanged pipeline |
| Change detection accuracy | Simple hash | 0 false negatives | Test all change types |
| Audit types | 0 | 5 built-in | Count passing test suites |
| Metric SQL correctness | N/A | 4/4 types | Compare generated vs hand-written SQL |
| StarRocks query latency | N/A | <1s | Typical metric query |

## Dependencies & Prerequisites

### External Dependencies
| Dependency | Version | Purpose | Risk |
|------------|---------|---------|------|
| pymysql | >=1.0 | StarRocks connection | LOW - mature, pure Python |
| duckdb | existing | Primary engine | Already in use |
| tabulate | existing | Output formatting | Already in use |
| pyyaml | existing | YAML parsing | Already in use |

### Internal Dependencies
| Dependency | Status | Notes |
|------------|--------|-------|
| State unification (Phase 1) | BLOCKER | Must complete before Phases 2-4 |
| Phase 3 (caching) | Required by Phase 8 | Standalone audit needs cached data |
| Phase 9 (SR connection) | Required by Phase 11, 15 | Executors need connection |
| Phase 12 (semantic parser) | Required by Phase 13 | Compiler needs parsed models |

## Risk Analysis & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| State unification breaks existing runs | HIGH | HIGH | Backward-compatible schema, migration on load |
| DuckDB MySQL scanner doesn't support StarRocks | MEDIUM | MEDIUM | Use pymysql directly (bypass DuckDB scanner) |
| SQL compiler complexity for derived/cumulative metrics | MEDIUM | MEDIUM | Start simple+ratio, add derived+cumulative iteratively |
| Large Parquet caches consume disk | LOW | LOW | Add cache size limit later |
| PySpark dependency in validators.py | LOW | LOW | DuckDB-native audit implementations |

## Resource Requirements

### Development Time Estimate
| Phase | Complexity | Estimate |
|-------|------------|----------|
| Phase 1: State Unification | medium | 2-3 days |
| Phase 2: Fingerprints | medium | 2-3 days |
| Phase 3: Caching | medium | 3-4 days |
| Phase 4: Smart Execution | medium | 2-3 days |
| Phase 5-8: Audits | medium | 8-12 days |
| Phase 9-11: StarRocks | complex | 10-14 days |
| Phase 12-15: Semantic | complex | 13-21 days |

## Validation Commands

```bash
# Run all tests
pytest tests/ -v

# Run specific test suites
pytest tests/workflow/test_state_unified.py -v
pytest tests/workflow/test_fingerprint.py -v
pytest tests/workflow/test_cache.py -v
pytest tests/workflow/test_audits.py -v
pytest tests/workflow/test_semantic.py -v
pytest tests/cli/test_audit_command.py -v
pytest tests/cli/test_query_command.py -v

# Integration tests
pytest tests/connections/test_starrocks.py -v
pytest tests/workflow/test_starrocks_executors.py -v
```

## Notes

- Phase 2.5 (Interval Tracking) from original plan deferred to post-MVP
- StarRocks integration requires actual StarRocks instance for integration testing (unit tests use mocks)
- Semantic layer SQL compiler is the highest complexity item - consider iterative delivery (simple+ratio first)

---

## Checklist Summary

### Phase 1: State Unification 🟡
- [ ] Audit state I/O points
- [ ] Consolidate to single state file
- [ ] Tests pass

### Phase 2: Fingerprints ⬜
- [ ] NodeFingerprint dataclass
- [ ] Upstream hash computation
- [ ] Tests pass

### Phase 3: Caching ⬜
- [ ] Cache directory structure
- [ ] TransformExecutor cache loading
- [ ] Invalidation + corruption recovery

### Phase 4: Smart Execution ⬜
- [ ] should_execute() with fingerprints
- [ ] CLI messaging
- [ ] --full flag

### Phase 5-8: Audits ⬜
- [ ] AuditResult + YAML parsing
- [ ] 5 built-in audit types
- [ ] Executor integration
- [ ] seeknal audit CLI

### Phase 9-11: StarRocks ⬜
- [ ] pymysql connection factory
- [ ] REPL integration
- [ ] Source + Exposure executors

### Phase 12-15: Semantic Layer ⬜
- [ ] Semantic model + metric parser
- [ ] SQL compilation engine
- [ ] seeknal query CLI
- [ ] seeknal deploy-metrics
