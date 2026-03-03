---
title: "Iceberg Incremental Run with Partition Pruning"
date: 2026-03-03
status: brainstorm
topic: iceberg-incremental-partition-pruning
---

# Iceberg Incremental Run with Partition Pruning

**Date:** 2026-03-03
**Status:** Brainstorm
**Related:** Iceberg Materialization (2026-01-26), State Management (`state.py`), Source Executor

---

## What We're Building

Make Seeknal's incremental run mechanism work with Iceberg sources at scale (100GB+). Today, an Iceberg source node always executes `SELECT * FROM catalog.namespace.table` — a full table scan regardless of whether the data changed. At 100GB on S3, this is impractical. The goal is to detect data changes via Iceberg metadata (zero data scanning) and apply partition pruning to read only new/changed data.

---

## Problem Statement

When a pipeline includes an Iceberg source, the current behavior has four critical gaps:

1. **No data change detection**: `calculate_node_hash()` hashes only YAML config (`params`, `freshness`). Iceberg is not in `_FILE_BASED_SOURCES`, so data changes in S3 are invisible — the hash stays the same and the node is cached even when upstream tables have new data.

2. **Full table scan on every execution**: `_load_iceberg()` creates `SELECT * FROM catalog.namespace.table` with no WHERE clause. At 100GB, DuckDB will attempt to read everything into memory.

3. **Unused state infrastructure**: `NodeState` has `iceberg_snapshot_id`, `iceberg_snapshot_timestamp`, `iceberg_table_ref`, `iceberg_schema_version`, `completed_partitions`, and `completed_intervals` — but none are populated or checked during source execution.

4. **No partition-aware reads**: Iceberg stores partition statistics in metadata. Even when we know which partitions have new data, the source executor doesn't generate partition-filtered queries.

---

## Key Decisions

### 1. Scope: Source-Side Only (Read Path)

**Decision:** This feature is about *reading* Iceberg sources incrementally, not about writing/materializing to Iceberg.

**Rationale:** The materialization side (write path) already has a separate design (see `2026-01-26-iceberg-materialization-brainstorm.md`). The incremental read problem is independent and has the highest user impact — 100GB full scans are the bottleneck.

### 2. Change Detection: Snapshot ID Comparison

**Decision:** Use Iceberg snapshot ID to detect whether an Iceberg source has new data.

**Approach:**
- After loading an Iceberg source, query `iceberg_snapshots('catalog.namespace.table')` to get the current snapshot ID
- Store it in `NodeState.iceberg_snapshot_id`
- On next run, compare stored vs current snapshot ID
- If different → data changed → re-execute the node
- If same → data unchanged → skip (cached)

**Why snapshot ID and not file list diffing:**
- Snapshot ID comparison is a single metadata lookup — no scanning of data files
- Iceberg guarantees snapshot ID changes when any data mutation occurs
- Snapshot IDs are immutable and globally unique within a table

### 3. Partition Pruning Strategy: Time-Column WHERE Clause

**Decision:** When incremental mode is enabled and a `freshness.time_column` is configured, inject a WHERE clause that filters by partition key based on the last completed timestamp.

**YAML configuration:**
```yaml
kind: source
name: orders
source: iceberg
table: atlas.ecommerce.orders
freshness:
  time_column: order_date
  warn_after: {count: 1, period: day}
```

**Behavior:**
- First run: Full scan (no watermark yet). Store `iceberg_snapshot_id` and `last_watermark = MAX(time_column)`.
- Subsequent runs:
  1. Check if `iceberg_snapshot_id` differs from stored
  2. If yes → generate `SELECT * FROM ... WHERE order_date > '{last_watermark}'`
  3. DuckDB + Iceberg's metadata-driven predicate pushdown skips unchanged partitions entirely
  4. Update watermark and snapshot ID in state

**Why time-column based (not partition-column based):**
- `freshness.time_column` already exists in YAML schema — no new config needed
- Works for both partitioned and non-partitioned tables
- Iceberg's predicate pushdown handles partition mapping internally
- Users already configure `freshness` for data quality — this reuses that intent

### 4. Fallback: Full Scan When No `freshness.time_column`

**Decision:** If an Iceberg source has no `freshness.time_column`, the system falls back to current behavior (full scan) but still uses snapshot ID for cache invalidation.

**Effect:** Even without partition pruning, the pipeline won't re-run an unchanged Iceberg source. This alone is a major improvement — a 100GB table with no new data runs in milliseconds instead of minutes.

### 5. Incremental vs Full Refresh Modes

**Decision:** The source node supports two modes controlled by `--full` flag:

| Mode | Trigger | Behavior |
|------|---------|----------|
| Incremental (default) | `seeknal run` | Snapshot check → partition-pruned read of new data only |
| Full refresh | `seeknal run --full` | Ignore stored state, full table scan |

**Existing `--full` flag already exists** and forces all nodes to re-run. No new CLI flags needed.

### 6. Memory Safety: Streaming via DuckDB Views

**Decision:** Keep the DuckDB view-based approach. DuckDB's lazy evaluation means the view is not materialized into memory until downstream transforms query it — and those queries can further filter/aggregate.

**For extreme cases (100GB+ with no partition pruning):** Document that users should either:
1. Add `freshness.time_column` for partition pruning
2. Use `params.query` with custom SQL for manual filtering
3. Increase DuckDB memory limit via `PRAGMA memory_limit`

---

## Proposed Architecture

### Component Changes

```
┌───────────────────────────────┐
│  calculate_node_hash()        │  ← Add Iceberg snapshot ID to hash
│  (state.py)                   │     so changed data invalidates cache
└──────────────┬────────────────┘
               │
┌──────────────▼────────────────┐
│  _load_iceberg()              │  ← Add WHERE clause when freshness.time_column
│  (source_executor.py)         │     and stored watermark exist
│                               │  ← Query iceberg_snapshots() after load
│                               │  ← Store snapshot_id + watermark in result
└──────────────┬────────────────┘
               │
┌──────────────▼────────────────┐
│  DAGRunner                    │  ← After source execution, persist
│  (runner.py)                  │     iceberg_snapshot_id and watermark
│                               │     to NodeState
└───────────────────────────────┘
```

### Change Detection Flow (New)

```
seeknal run
  │
  ├─ For each Iceberg source node:
  │   ├─ Query current snapshot_id from catalog metadata
  │   ├─ Compare with NodeState.iceberg_snapshot_id
  │   ├─ If SAME → CACHED (skip execution)
  │   └─ If DIFFERENT (or first run):
  │       ├─ Has freshness.time_column?
  │       │   ├─ YES → SELECT * WHERE time_col > last_watermark
  │       │   └─ NO  → SELECT * (full scan)
  │       ├─ Store new snapshot_id in NodeState
  │       ├─ Store MAX(time_column) as new watermark
  │       └─ Invalidate downstream nodes
  │
  └─ Continue with normal DAG execution
```

---

## Technical Details

### Querying Iceberg Snapshot ID

DuckDB's Iceberg extension provides:
```sql
-- Get current snapshot
SELECT snapshot_id, timestamp_ms
FROM iceberg_snapshots('catalog.namespace.table')
ORDER BY timestamp_ms DESC
LIMIT 1;
```

This queries Iceberg catalog metadata only — no data files are read. Cost: single HTTP request to Lakekeeper.

### Watermark Storage

Reuse existing `NodeState` fields:
- `iceberg_snapshot_id` → current snapshot ID (already exists, unused)
- `iceberg_snapshot_timestamp` → snapshot timestamp (already exists, unused)
- `metadata["last_watermark"]` → `MAX(time_column)` from last successful run
- `iceberg_table_ref` → fully qualified table name (already exists, unused)

No new state fields needed. All infrastructure is already in place.

### Partition-Pruned Query Generation

```python
# In _load_iceberg():
if freshness_time_column and last_watermark:
    view_sql = (
        f"SELECT * FROM {catalog_alias}.{namespace}.{table_name} "
        f"WHERE {freshness_time_column} > '{last_watermark}'"
    )
else:
    view_sql = f"SELECT * FROM {catalog_alias}.{namespace}.{table_name}"

con.execute(
    f"CREATE OR REPLACE VIEW {schema}.{view_name} AS {view_sql}"
)
```

Iceberg's metadata layer tells DuckDB which data files contain rows matching the WHERE predicate. Partitioned tables skip entire partition directories. Non-partitioned tables still benefit from min/max column stats in manifest files.

### Hash Integration

In `calculate_node_hash()`, for Iceberg sources:
```python
elif kind == "source" and source_type == "iceberg":
    # Include current snapshot ID in hash so data changes invalidate cache
    # This is a cheap metadata lookup, not a data scan
    functional_content["_iceberg_snapshot_id"] = current_snapshot_id
```

This requires the hash function to accept an optional `iceberg_metadata` parameter, or the snapshot check happens in the runner before hashing.

**Alternative (simpler):** Don't modify the hash at all. Instead, add a separate Iceberg-specific check in `_fingerprint_based_detection()` that queries snapshot IDs for all Iceberg source nodes and marks them as changed if the snapshot differs. This avoids complicating the generic hash function.

**Chosen approach: Separate check in runner** — cleaner separation of concerns.

---

## Out of Scope (v1)

| Item | Reason |
|------|--------|
| Snapshot diffing (diff between two snapshots) | Complex; watermark-based pruning covers 90% of use cases |
| Iceberg time-travel reads (`AS OF snapshot_id`) | Useful but separate feature |
| Automatic partition key detection | Too magic; `freshness.time_column` is explicit and auditable |
| Write-side incremental (materialization) | Covered by separate brainstorm |
| Spark engine support | DuckDB-only for now; Spark has its own Iceberg integration |
| Restatement intervals for Iceberg | Can be added later; initial focus is forward-only incremental |

---

## Edge Cases

| Edge Case | Behavior |
|-----------|----------|
| First run (no stored snapshot) | Full scan, store snapshot ID + watermark |
| Iceberg table deleted and recreated | Snapshot ID will differ → full re-scan (correct) |
| Schema evolution (new columns) | Schema version check via `iceberg_schema_version`; trigger full re-scan |
| `--full` flag | Ignore all stored state, full scan |
| `--env dev` | State stored per-env; each env tracks its own snapshot/watermark |
| Catalog unreachable | Fail the node with clear error (no silent fallback to full scan) |
| `params.query` (custom SQL) | Skip partition pruning — user controls the query |
| Multiple Iceberg sources in pipeline | Each source tracks its own snapshot ID independently |
| Concurrent writes to Iceberg table | Snapshot ID is atomic — we always see a consistent snapshot |
| Watermark column has NULLs | Use `WHERE time_col > '{watermark}' OR time_col IS NULL` |
| `freshness.time_column` not in table | Fail at execution with clear error message |

---

## Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| DuckDB Iceberg extension doesn't support `iceberg_snapshots()` for REST catalogs | Medium | High | Test with Lakekeeper first; fallback to REST API query if needed |
| Watermark drift (data arrives out of order) | Medium | Medium | Document that watermark is "last seen max" — late-arriving data before watermark is missed. Users can use `--full` or restatement intervals |
| Large partition count slows metadata queries | Low | Low | Iceberg metadata is designed for this; DuckDB handles it efficiently |
| Breaking change if snapshot ID format changes | Very Low | Low | Store as opaque string, compare as string equality |

---

## Success Metrics

- Iceberg source with no new data: execution time < 1 second (metadata check only)
- Iceberg source with partition pruning: reads only new partitions, not full table
- `FAILED: None` never appears for Iceberg errors — clear error messages
- Downstream nodes correctly invalidated when Iceberg source has new data
- State correctly persists `iceberg_snapshot_id` and watermark across runs

---

## Implementation Priority

### Phase 1: Snapshot-Based Cache Invalidation (Highest Impact)
**Files:** `runner.py`, `source_executor.py`, `state.py`

Wire up `iceberg_snapshot_id` in NodeState. After executing an Iceberg source, query and store the snapshot ID. In the runner, compare stored vs current snapshot ID to decide if the source needs re-execution.

**Impact:** Even without partition pruning, this prevents unnecessary full scans of unchanged tables.

### Phase 2: Partition-Pruned Reads
**Files:** `source_executor.py`

When `freshness.time_column` exists and a previous watermark is stored, inject `WHERE time_col > '{last_watermark}'` into the source view SQL. Store new watermark after execution.

**Impact:** Reduces data read from 100GB to only new partitions (e.g., 1-2GB per daily partition).

### Phase 3: Metadata-Only Snapshot Check (Pre-Execution)
**Files:** `runner.py`, potentially new `iceberg_metadata.py` utility

Before executing any node, check Iceberg snapshot IDs for all Iceberg source nodes via catalog metadata API. Mark unchanged sources as cached without executing them at all.

**Impact:** Eliminates even the DuckDB connection setup for unchanged sources.

---

## References

- **Iceberg materialization brainstorm:** `docs/brainstorms/2026-01-26-iceberg-materialization-brainstorm.md`
- **NodeState fields:** `src/seeknal/workflow/state.py:80-127`
- **Source executor Iceberg loader:** `src/seeknal/workflow/executors/source_executor.py:914-1079`
- **Hash calculation:** `src/seeknal/workflow/state.py:299-437`
- **Runner fingerprint detection:** `src/seeknal/workflow/runner.py:289-343`
- **Interval calculator:** `src/seeknal/workflow/intervals.py`
- **Database backend (state persistence):** `src/seeknal/state/database_backend.py`
