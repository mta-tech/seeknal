---
title: "feat: Project-Aware REPL + Environment-Aware Iceberg & PostgreSQL - Merged Implementation Plan"
type: feat
date: 2026-02-21
status: ready
merge_sources:
  - docs/plans/2026-02-21-feat-project-aware-repl-plan.md
  - docs/plans/2026-02-21-feat-env-aware-iceberg-postgresql-plan.md
---

# Plan: Project-Aware REPL + Environment-Aware Iceberg & PostgreSQL

## Task Description

Two complementary features that improve seeknal's developer experience for Iceberg and PostgreSQL workflows:

**Feature A — Project-Aware REPL:** Extend `seeknal repl` to auto-detect project context and register all queryable data (intermediate parquets, PostgreSQL tables, Iceberg catalogs) so developers can inspect pipeline outputs with plain SQL.

**Feature B — Environment-Aware Execution:** Make `seeknal env` fully environment-aware so each virtual environment (dev, staging) reads from env-specific sources, writes to env-specific targets, and promotes with re-materialization to production.

Both features share a common dependency: profile_path propagation through all code paths. Feature B's execution path unification (Phase 0) also benefits the REPL since it uses the same ProfileLoader and DAGBuilder infrastructure.

## Objective

1. Eliminate manual `.connect` steps in REPL — pipeline outputs are instantly queryable
2. Enable environment-isolated development — dev pipeline runs never touch production targets
3. Support promotion with re-materialization — promote from dev to production writes to production external targets
4. Fix profile_path propagation gaps that affect parallel mode, env mode, and REPL

## Relevant Files

### Existing Files (Modify)
- `src/seeknal/cli/main.py` — REPL command, env commands, run command, `_run_in_environment()`
- `src/seeknal/cli/repl.py` — REPL class, `run_repl()`, `_connect()`, `_show_tables()`
- `src/seeknal/workflow/runner.py` — `DAGRunner.__init__`, `_execute_by_type()`
- `src/seeknal/workflow/executors/base.py` — `ExecutionContext` dataclass
- `src/seeknal/workflow/environment.py` — `EnvironmentManager`, `plan()`, `apply()`, `promote()`
- `src/seeknal/workflow/materialization/dispatcher.py` — `MaterializationDispatcher.dispatch()`
- `src/seeknal/workflow/materialization/postgresql.py` — PostgreSQL write helpers
- `src/seeknal/workflow/materialization/profile_loader.py` — `ProfileLoader`, `load_source_defaults()`
- `src/seeknal/workflow/dag.py` — `DAGBuilder`, `_merge_source_defaults()`
- `tests/workflow/test_environments.py` — Environment tests

### Key Integration Points
| Component | File | Purpose |
|-----------|------|---------|
| REPL class | `src/seeknal/cli/repl.py:84-93` | Inject project context |
| ProfileLoader | `profile_loader.py:391-443` | Resolve connection creds |
| DAGBuilder | `dag.py:151-250` | Discover nodes + materializations |
| PostgreSQL attach | `connections/postgresql.py:247-275` | `ATTACH AS (TYPE POSTGRES, READ_ONLY)` |
| Iceberg attach | `materialization/operations.py:362-421` | `DuckDBIcebergExtension.attach_rest_catalog()` |
| RunState | `state.py:194-271` | Node status, row counts |
| DAGRunner | `runner.py:100-450` | Pipeline execution |
| ExecutionContext | `executors/base.py:142-213` | Execution parameters |
| EnvironmentManager | `environment.py:63-381` | Plan/apply/promote |
| MaterializationDispatcher | `dispatcher.py:56-241` | Multi-target write routing |

## Step by Step Tasks

### Section A: Project-Aware REPL (from Feature A)

#### 1. Add --profile flag to seeknal repl command
- **Depends On:** none
- **Assigned To:** builder-repl
- **Agent Type:** general-purpose
- **Parallel:** true (independent of Section B tasks 4-7)
- Modify `repl` command in `src/seeknal/cli/main.py:2774` to add `--profile` option
- Detect project context: check `Path.cwd() / "seeknal_project.yml"` exists
- Pass `project_path` and `profile_path` to `run_repl()`

#### 2. Implement _auto_register_project() in REPL class
- **Depends On:** 1
- **Assigned To:** builder-repl
- **Agent Type:** general-purpose
- **Parallel:** false
- Modify `REPL.__init__()` in `repl.py:84` to accept optional `project_path` and `profile_path`
- Modify `run_repl()` at `repl.py:585` to accept and forward params
- Add `_auto_register_project()` method with three phases:
  - Phase 1: Register intermediate parquets as views (`target/intermediate/*.parquet` → DuckDB views named by node ID)
  - Phase 2: Attach PostgreSQL connections read-only via `ProfileLoader.load_connection_profile()` + DuckDB `ATTACH`
  - Phase 3: Attach Iceberg catalogs via `DuckDBIcebergExtension.attach_rest_catalog()`
- Each phase wrapped in try/except with warning on failure (best-effort)
- Track registration counts for startup banner

#### 3. Update REPL startup banner and .tables command
- **Depends On:** 2
- **Assigned To:** builder-repl
- **Agent Type:** general-purpose
- **Parallel:** false
- Modify banner text at `repl.py:126` to show project context when detected:
  ```
  Project: my_project (8 nodes, last run: 2026-02-21 10:38)
    Intermediate outputs: 6 tables registered
    PostgreSQL (local_pg): attached (read-only)
    Iceberg (atlas): attached
  ```
- Modify `_show_tables()` at `repl.py:425` to include auto-registered views
- Add `httpfs` and `iceberg` to extension list when Iceberg targets detected

### Section B: Environment-Aware Execution (from Feature B)

#### 4. Unify execution paths — DAGRunner gains ExecutionContext
- **Depends On:** none
- **Assigned To:** builder-env
- **Agent Type:** general-purpose
- **Parallel:** true (independent of Section A tasks 1-3)
- Refactor `DAGRunner.__init__()` in `runner.py` to accept optional `exec_context: ExecutionContext`
- Refactor `_execute_by_type()` at `runner.py:409` to delegate to `get_executor(node, context)` when context is provided (backward compat: fall back to direct calls when no context)
- Update `_run_in_environment()` at `main.py:3544` to create `ExecutionContext` and pass to `DAGRunner`
- Update parallel path at `main.py:1002-1026` to create `ExecutionContext` and pass to `DAGRunner`
- **Critical:** Maintain backward compatibility — existing DAGRunner callers without context must still work

#### 5. Wire profile_path through env plan, env apply, and run --env
- **Depends On:** 4
- **Assigned To:** builder-env
- **Agent Type:** general-purpose
- **Parallel:** false
- Add `profile_path` parameter to `_run_in_environment()` signature at `main.py:3544`
- Forward `profile` from `run()` at `main.py:740-752` to `_run_in_environment()`
- Add `--profile` option to `env_plan()` at `main.py:3491`
- Pass `profile_path` to `DAGBuilder` at `main.py:3505`
- Save `profile_path` in `plan.json` via `EnvironmentManager` at `environment.py`
- Restore `profile_path` from `plan.json` during `env apply`

#### 6. Implement per-env profile auto-discovery
- **Depends On:** 5
- **Assigned To:** builder-env
- **Agent Type:** general-purpose
- **Parallel:** false
- Add `_resolve_env_profile()` helper in `main.py`:
  - Priority: `--profile` flag > `profiles-{env}.yml` in project root > `~/.seeknal/profiles-{env}.yml` > default `profiles.yml`
  - Display INFO message: "Using profiles-dev.yml for environment 'dev'"
- Call from `run()` when `--env` is specified and `--profile` is not
- Call from `env_plan()` when `--profile` is not specified

#### 7. Implement convention-based namespace prefixing
- **Depends On:** 5
- **Assigned To:** builder-env
- **Agent Type:** general-purpose
- **Parallel:** false
- Add `env_name: Optional[str]` field to `ExecutionContext` at `base.py:142`
- Modify `MaterializationDispatcher.dispatch()` at `dispatcher.py` to accept env_name from context
- When env_name is set and no per-env profile was found:
  - PostgreSQL: `analytics.table` → `{env}_analytics.table` (prefix schema)
  - Iceberg: `atlas.namespace.table` → `atlas.{env}_namespace.table` (prefix namespace)
- Add `CREATE SCHEMA IF NOT EXISTS` in `postgresql.py` before write
- Add Iceberg namespace creation via Lakekeeper API before write

#### 8. Implement promotion with re-materialization
- **Depends On:** 5
- **Assigned To:** builder-env
- **Agent Type:** general-purpose
- **Parallel:** false
- Modify `promote()` in `environment.py:268` to gain `rematerialize`, `profile_path`, `dry_run` params
- After cache/state copy (existing behavior), if `rematerialize=True`:
  - Load production profile (`profiles.yml` or `profiles-prod.yml`)
  - For each node with materialization config, read intermediate parquet from env cache
  - Dispatch materialization to production targets (no namespace prefix)
  - Best-effort: continue on individual materialization failure
- Add `--dry-run` and `--profile` flags to `env_promote()` CLI command
- `--dry-run` shows which tables would be written without executing

### Section C: Testing & Validation

#### 9. Write tests for REPL auto-registration
- **Depends On:** 3
- **Assigned To:** builder-repl
- **Agent Type:** general-purpose
- **Parallel:** true (independent of task 10)
- Test project detection (seeknal_project.yml present vs absent)
- Test intermediate parquet registration (mock filesystem)
- Test PostgreSQL attach failure handling (connection refused → warning, not crash)
- Test Iceberg attach when env vars not set (skip gracefully)
- Test .tables includes auto-registered views
- Test --profile flag override
- Test banner output with and without project context

#### 10. Write tests for env-aware execution
- **Depends On:** 8
- **Assigned To:** builder-env
- **Agent Type:** general-purpose
- **Parallel:** true (independent of task 9)
- Test DAGRunner with ExecutionContext (backward compat: works without context too)
- Test profile_path propagation through env plan → env apply chain
- Test profile auto-discovery (`profiles-dev.yml` found vs not found)
- Test namespace prefixing (PostgreSQL schema prefix, Iceberg namespace prefix)
- Test promotion re-materialization with mock dispatcher
- Test `--dry-run` on promote (no writes, only summary)
- Test plan.json stores and restores profile_path

## Team Orchestration

As the team lead, coordinate two parallel builders — one for the REPL feature, one for the environment feature. Both can work simultaneously since they touch different files (repl.py vs runner.py/environment.py), converging only at main.py CLI commands.

### Team Members

#### Builder REPL
- **Name:** builder-repl
- **Role:** Implement project-aware REPL (Tasks 1-3, 9)
- **Agent Type:** general-purpose
- **Resume:** true

#### Builder Env
- **Name:** builder-env
- **Role:** Implement env-aware execution (Tasks 4-8, 10)
- **Agent Type:** general-purpose
- **Resume:** true

## Acceptance Criteria

### Functional Requirements (REPL)
- [ ] `seeknal repl` inside a project auto-detects `seeknal_project.yml`
- [ ] Intermediate parquet files from `target/intermediate/` are queryable by node name (e.g., `SELECT * FROM "source.raw_orders" LIMIT 5`)
- [ ] PostgreSQL materialization targets are attached read-only via profile credentials
- [ ] Iceberg materialization targets are attached via REST catalog with OAuth2
- [ ] Each registration phase fails independently with a warning (not an error)
- [ ] `seeknal repl` outside a project works exactly as before (no regression)
- [ ] Startup banner shows project context and registration summary
- [ ] `--profile` flag on `seeknal repl` overrides default `~/.seeknal/profiles.yml`
- [ ] `.tables` command lists all auto-registered views and attached databases

### Functional Requirements (Environment)
- [ ] `seeknal run --env dev --profile profiles-dev.yml` reads from dev sources and writes to dev targets
- [ ] `seeknal run --env dev` auto-discovers `profiles-dev.yml` if it exists
- [ ] Without per-env profile, materialization targets get `{env}_` prefix on schema/namespace
- [ ] `seeknal env plan dev --profile profiles-dev.yml` saves profile_path in plan.json
- [ ] `seeknal env apply dev` restores profile_path from plan.json
- [ ] `seeknal run --parallel --profile profiles-dev.yml` correctly uses dev profile for materialization
- [ ] `seeknal env promote dev` re-materializes to production targets
- [ ] `seeknal env promote dev --dry-run` shows what would be written without executing
- [ ] Dev schema/namespace auto-created if not exists (PostgreSQL + Iceberg)

### Quality Gates
- [ ] All existing tests pass (no regressions)
- [ ] DAGRunner backward compatibility maintained (callers without ExecutionContext still work)
- [ ] REPL outside project unchanged (no regression)
- [ ] Tests cover: project detection, parquet registration, PG/Iceberg attach failure, namespace prefix, profile auto-discovery, promotion re-materialization

## Dependencies & Prerequisites

### External Dependencies
| Dependency | Version | Purpose | Risk |
|------------|---------|---------|------|
| DuckDB postgres extension | latest | ATTACH PostgreSQL databases | Low — already used |
| DuckDB iceberg extension | latest | ATTACH Iceberg catalogs | Low — already used |
| DuckDB httpfs extension | latest | S3 access for Iceberg | Low — already used |
| Lakekeeper REST API | v1 | Namespace creation | Low — already used |

### Internal Dependencies
| Dependency | Status | Notes |
|------------|--------|-------|
| ProfileLoader | Implemented | Used by both features |
| DAGBuilder | Implemented | Needs profile_path plumbing |
| MaterializationDispatcher | Implemented | Needs env_name support |
| DuckDBIcebergExtension | Implemented | REPL reuses existing methods |
| ExecutionContext | Implemented | Needs env_name field |

## Success Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| REPL startup friction | 3+ manual .connect steps | 0 steps (auto) | Developer workflow observation |
| Env isolation | Local cache only | Full source+target isolation | Feature test coverage |
| Profile propagation gaps | 3 code paths broken | 0 gaps | Unit test coverage |

## Notes

- **ADR-005 applies:** DuckDB postgres extension is the sole bridge for PostgreSQL I/O — REPL attaches via DuckDB, not direct psycopg2
- **Known bug:** Source defaults alias normalization mismatch in `profile_loader.py:478-481` — should be fixed as part of Task 5
- **Fingerprint caveat:** Profile changes don't invalidate fingerprints — document that `--full` is needed when switching profiles
- **REPL timeout:** Use 5s connect_timeout for PostgreSQL attach to avoid slow REPL startup on connection failures

---

## Checklist Summary

### Section A: Project-Aware REPL
- [x] Task 1: Add --profile to seeknal repl, detect project context
- [x] Task 2: Implement _auto_register_project() (intermediates, PG, Iceberg)
- [x] Task 3: Update REPL banner and .tables
- [x] Task 9: Write REPL tests

### Section B: Environment-Aware Execution
- [x] Task 4: Unify execution paths (DAGRunner + ExecutionContext)
- [x] Task 5: Wire profile_path through env plan/apply/run
- [x] Task 6: Implement per-env profile auto-discovery
- [x] Task 7: Implement convention-based namespace prefixing
- [x] Task 8: Implement promotion with re-materialization
- [x] Task 10: Write env tests

## Compounded

- [x] Last compounded: 2026-02-21
- [x] ADRs created: 4
- [x] Solutions documented: 4
- [x] Deployment changes: 1
- [x] Patterns added: 4
- [x] Planning patterns: 6

**Generated Documents:**

Architecture Decisions (ADRs):
- docs/adr/adr-008-repl-three-phase-best-effort-auto-registration.md
- docs/adr/adr-009-executioncontext-optional-parameter-with-legacy-fallback-in-dagrunner.md
- docs/adr/adr-010-profile-path-round-trip-through-plan-json.md
- docs/adr/adr-011-convention-based-namespace-prefixing-with-env-prefix.md

Mistakes & Solutions:
- docs/solutions/duckdb-sql/hugeint-to-bigint-cast-for-iceberg.md
- docs/solutions/duckdb-sql/group-by-completeness-for-aggregations.md
- docs/solutions/profile-loading/source-defaults-alias-normalization-mismatch.md
- docs/solutions/source-config/source-defaults-table-query-conflict.md

Deployment: docs/deployment.md (updated with changelog)
Agent Guidelines: CLAUDE.md (updated with 4 new patterns)
Planning Patterns: docs/planning-patterns.md (6 new patterns)
