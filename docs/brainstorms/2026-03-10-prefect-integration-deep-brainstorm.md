---
title: Deep Prefect Integration for Seeknal
topic: prefect-integration
date: 2026-03-10
status: complete
---

# Deep Prefect Integration for Seeknal

## What We're Building

A production-grade Prefect orchestration layer that replaces the current thin subprocess wrapper with a deep integration using Seeknal's internal Python API. The integration covers three pillars:

1. **Production Scheduling** — Reliable cron-based pipeline and report scheduling with monitoring, retries, and alerting via Prefect Server/Cloud.
2. **DAG-Level Parallelism** — Prefect understands the DAG topology from `DAGRunner._get_topological_layers()`, so independent nodes run in parallel with explicit `wait_for` dependencies.
3. **Observability** — Per-node status, duration, row counts, and errors visible in the Prefect UI dashboard. Markdown artifacts summarize each run.

Additionally, report exposures with a `schedule:` field will be deployable as Prefect flows, unifying pipeline and report scheduling under one system.

## Why This Approach

### Internal API over Subprocess

The current integration shells out to `seeknal run --nodes <id>` via `subprocess.run()` for every task. This is slow (process startup overhead), loses structured results (only exit code + stdout), and prevents DuckDB connection sharing.

By calling `DAGRunner._execute_node()` or the executor layer directly, we get:
- ~10x faster task startup (no process spawn)
- Rich `NodeResult` data (row counts, duration, schema changes)
- Shared DuckDB connections across nodes in the same flow
- Direct access to `RunState` for bidirectional state sync

### Bidirectional State Sync

Prefect tasks write back to `run_state.json` after each node completes. This means:
- `seeknal run` sees what Prefect already completed (skip-if-cached)
- `seeknal prefect serve` sees what manual `seeknal run` completed
- Mixed manual + scheduled execution works seamlessly
- Interval tracking (`completed_intervals`) stays accurate

### Optional Dependency

Prefect moves from a hard dependency to `pip install seeknal[prefect]`. Users who only use `seeknal run` locally skip the ~100MB Prefect install. CLI commands (`seeknal prefect ...`) show a clear error with install instructions when Prefect is missing.

## Key Decisions

1. **Execution mode: Internal API** — Call Seeknal's executor layer directly from Prefect tasks. No subprocess isolation. Requires Prefect workers to have seeknal installed in the same environment.

2. **State: Bidirectional sync** — Prefect tasks update `run_state.json` after each node. Both `seeknal run` and `seeknal prefect serve` share the same state file.

3. **Dependency: Optional** — `prefect>=3.1.10` moves to `[project.optional-dependencies]` under `prefect = [...]`. Import guards with helpful error messages.

4. **Consolidation: Final DAG task** — Entity consolidation runs as a Prefect task that depends on all feature_group-type tasks. Best-effort (never fails the flow), visible in Prefect UI.

5. **Report scheduling: Unified** — Report exposures with `schedule:` field are deployable to Prefect alongside pipelines. `seeknal prefect deploy` handles both types.

6. **Parallelism: Layer-based** — Mirror the existing `ParallelDAGRunner` pattern. Group nodes into topological layers using `_get_topological_layers()`. All nodes in a layer run as parallel Prefect tasks; barrier sync between layers. Batch state writes per layer — no race conditions, no file locking needed.

7. **Recovery: Skip-cached nodes** — On re-run, check `run_state.json` and skip nodes that already succeeded. Only run the failed node + its downstream. Same behavior as `seeknal run` today.

## Scope

### In Scope

- Rewrite `prefect_integration.py` with `SeeknalPrefectFlow` class using internal API
- DAG-aware Prefect flow builder using `_get_topological_layers()`
- Bidirectional `RunState` sync (Prefect writes back to `run_state.json`)
- CLI commands: `seeknal prefect serve`, `seeknal prefect deploy`, `seeknal prefect generate`
- Prefect markdown artifacts with per-node results table
- Entity consolidation as final task in the Prefect DAG
- Report exposure `schedule:` field parsing and Prefect deployment
- Move Prefect to optional dependency (`seeknal[prefect]`)
- Backfill flow update for Prefect 3.x patterns
- Tests with mocked DAGRunner and executor

### Out of Scope

- Custom Prefect blocks or infrastructure blocks
- Prefect events/automations integration
- Custom Prefect UI extensions
- Prefect variables/secrets integration (use Seeknal's own profiles)
- Docker image building for remote workers
- Multi-tenant / multi-project orchestration

## Parallelism Strategy: Layer-Based

### Why Layer-Based (Not Per-Node wait_for)

Three approaches were evaluated:

| Approach | Parallelism | State Safety | Complexity |
|----------|------------|--------------|------------|
| **Layer-based (chosen)** | Good — all independent nodes in a layer run in parallel | Safe — batch write after each layer, no concurrent writes | Low — mirrors existing `ParallelDAGRunner` |
| Per-node wait_for | Maximum — each node starts as soon as its deps finish | Unsafe — concurrent writes to `run_state.json` need file locking or SQLite | High — race conditions, DuckDB connection management |
| Hybrid | Varies | Varies | High — two code paths |

Layer-based is the clear winner: it's already proven in `parallel.py`, the layer barrier cost is typically milliseconds, and it avoids all concurrency issues.

### How It Works

```
DAG:   source_a ──> transform_x ──> feature_group_1
       source_b ──> transform_y ──> feature_group_2
                    transform_z ──> feature_group_2

Prefect Flow Execution:
  Layer 1: task.submit(source_a), task.submit(source_b)     ← parallel
  barrier: wait for all Layer 1 results
  batch_update_state(layer_1_results)                       ← single write

  Layer 2: task.submit(transform_x), task.submit(transform_y), task.submit(transform_z)
  barrier: wait for all Layer 2 results
  batch_update_state(layer_2_results)

  Layer 3: task.submit(fg_1), task.submit(fg_2)
  barrier: wait for all Layer 3 results
  batch_update_state(layer_3_results)

  Layer 4: task.submit(consolidation)                       ← best-effort final task
  create_artifact(all_results)                              ← markdown summary
```

### DuckDB Connection Model

Each Prefect task gets its own DuckDB connection (same as current `ParallelDAGRunner`). Write operations output to separate parquet files per node (`target/intermediate/{node_name}.parquet`), so there's no DuckDB write contention.

## Architecture Overview

```
seeknal prefect serve/deploy
        |
        v
+---------------------------+
|  DAGBuilder.build()       |  <-- Discover nodes & edges
|  _get_topological_layers()|  <-- Group into parallel layers
+----------+----------------+
           |
           v
+---------------------------+
|  Build Prefect flow       |  <-- @task per node per layer
|  barrier sync per layer   |  <-- Wait for layer N before N+1
+----------+----------------+
           |
           v
+---------------------------+
|  Each task calls:         |  <-- Internal API (no subprocess)
|  get_executor(node).run() |  <-- Direct Python execution
|  batch_update_state()     |  <-- One write per layer
+----------+----------------+
           |
           v
+---------------------------+
|  Consolidation task       |  <-- Depends on all FG tasks
|  -> Artifact reporting    |  <-- Markdown summary
+---------------------------+
```

### Report Exposure Scheduling

```
seeknal prefect deploy --exposure weekly_kpis
        |
        v
+---------------------------+
|  load_report_exposure()   |  <-- Read YAML with schedule: field
|  parse schedule cron      |
+----------+----------------+
           |
           v
+---------------------------+
|  Prefect flow:            |  <-- @flow wrapping report runner
|  render_deterministic_    |
|  report() or agent.invoke |
+----------+----------------+
           |
           v
+---------------------------+
|  Prefect deployment       |  <-- Cron schedule from YAML
|  with schedule from YAML  |
+---------------------------+
```

## Key Files to Change

| File | Change |
|------|--------|
| `src/seeknal/workflow/prefect_integration.py` | Major rewrite — `SeeknalPrefectFlow` class with internal API |
| `src/seeknal/cli/main.py` | Add `prefect` Typer subgroup with `serve`, `deploy`, `generate` |
| `src/seeknal/ask/report/exposure.py` | Add `schedule` field to validation schema |
| `src/seeknal/ask/agents/tools/save_report_exposure.py` | Accept optional `schedule` parameter |
| `pyproject.toml` | Move prefect to optional dep, add `[prefect]` extra |
| `tests/workflow/test_prefect_integration.py` | Rewrite tests for new API |

## Resolved Questions

1. **Q: Internal API vs subprocess?** A: Internal API. Faster, richer results, shared connections.
2. **Q: State sync strategy?** A: Bidirectional. Prefect and `seeknal run` share `run_state.json`.
3. **Q: Hard vs optional dependency?** A: Optional — `seeknal[prefect]`.
4. **Q: Consolidation handling?** A: Final task in Prefect DAG, best-effort.
5. **Q: Report scheduling?** A: Unified — both pipelines and report exposures deploy to Prefect.
6. **Q: Concurrent state writes?** A: Not an issue — layer-based approach batches state writes per layer (single write after all tasks in a layer complete). Same pattern as existing `ParallelDAGRunner._batch_update_state()`.
7. **Q: DuckDB concurrency?** A: Each task gets its own connection. Write targets are separate parquet files per node. No contention.
8. **Q: Error recovery?** A: Skip-cached nodes on re-run. Check `run_state.json`, skip succeeded nodes, run failed + downstream only.
