# PostgreSQL Incremental Detection - Brainstorm

**Date:** 2026-03-03
**Status:** Exploring
**Related:** Iceberg incremental detection pattern

## What We're Building

Add PostgreSQL data source change detection and incremental processing, mirroring the existing Iceberg pattern. When a pipeline runs:

1. **Detect changes**: Query `MAX(time_column)` and compare to stored watermark
2. **Skip unchanged sources**: If no new data, skip reprocessing (fingerprint + watermark match)
3. **Incremental reads**: When processing, inject `WHERE time_col > last_watermark` clause
4. **Propagate downstream**: Invalidate dependent nodes when source changes

## Why This Approach

### Alignment with Existing Patterns

The Iceberg incremental detection (implemented in #XXX) provides a proven template:

- **Detection**: Iceberg uses snapshot ID comparison; PostgreSQL will use `MAX(time_column)` comparison
- **State**: Both use `run_state.json` with per-node tracking
- **WHERE injection**: Both use `source_executor.py` for incremental clause generation
- **Propagation**: Both use BFS traversal to invalidate downstream nodes

### Simplicity

- Reuses existing `freshness.time_column` configuration (no new YAML keys)
- Minimal code changes (extend existing detection logic)
- No infrastructure dependencies (no CDC, no triggers, no replication slots)

### Performance

- Single `SELECT MAX(time_col)` query for detection (O(1) with index)
- WHERE clause leverages PostgreSQL index scans
- Skip entire source nodes when unchanged

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Detection method | Timestamp-based | Reuses `freshness.time_column`, simple, no schema changes |
| State storage | `run_state.json` | Consistent with Iceberg, easy to inspect/debug |
| Trigger model | Manual with skip | No daemon complexity, user controls when to check |
| Incremental mode | WHERE clause injection | Efficient for large tables, consistent with Iceberg |

## Architecture

### Detection Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    DAGRunner._should_run_node()             │
├─────────────────────────────────────────────────────────────┤
│  1. Fingerprint check (YAML hash) - if same, continue check │
│  2. Source type check: is this a postgresql source?         │
│  3. Query: SELECT MAX(time_col) FROM source_table           │
│  4. Compare: current_max == stored_watermark?               │
│     - Same → SKIP (no changes)                              │
│     - Different → RUN + update watermark                    │
│  5. If RUN: propagate to downstream nodes (BFS)             │
└─────────────────────────────────────────────────────────────┘
```

### WHERE Clause Injection

```
┌─────────────────────────────────────────────────────────────┐
│              SourceExecutor._build_incremental_query()       │
├─────────────────────────────────────────────────────────────┤
│  Original: SELECT * FROM users                              │
│  With watermark: SELECT * FROM users                        │
│                  WHERE updated_at > '2026-03-02 10:00:00'   │
│                           OR updated_at IS NULL             │
└─────────────────────────────────────────────────────────────┘
```

### State Schema (Extended)

```json
{
  "nodes": {
    "source_users": {
      "fingerprint": "abc123",
      "postgresql_watermark": "2026-03-02T10:00:00Z",
      "status": "completed"
    },
    "source_orders": {
      "fingerprint": "def456",
      "iceberg_snapshot_id": "1234567890",
      "status": "completed"
    }
  }
}
```

## Files to Modify

| File | Changes |
|------|---------|
| `workflow/runner.py` | Add PostgreSQL watermark detection in `_should_run_node()` |
| `workflow/state.py` | Add `postgresql_watermark` field to `NodeState` |
| `workflow/executors/source_executor.py` | Add PostgreSQL WHERE clause generation |
| `workflow/postgresql_metadata.py` | NEW - Query `MAX(time_column)` via existing connection |

## Evaluation: Is This a Good Approach?

### Strengths

1. **Proven Pattern**: Mirrors working Iceberg implementation
2. **Minimal Complexity**: No new infrastructure, no daemons
3. **Reuse**: Leverages existing `freshness.time_column` config
4. **Consistency**: Same state management, same propagation logic
5. **Performance**: O(1) detection with indexed timestamp column

### Weaknesses / Limitations

1. **Requires Timestamp Column**: Source tables must have a reliable time column
2. **Not True CDC**: Only detects new rows via timestamp, not updates/deletes
3. **Connection Overhead**: Detection requires PostgreSQL connection (vs Iceberg's REST API)
4. **Clock Dependency**: Relies on source system timestamps being accurate

### When This Works Well

- Append-only tables (logs, events, transactions)
- Tables with reliable `updated_at` / `created_at` columns
- Batch-oriented processing (daily/hourly runs)
- Tables with timestamp column indexes

### When This May Not Work

- Tables with frequent updates/deletes (consider logical replication)
- Tables without timestamp columns (would need schema change)
- Real-time / sub-minute latency requirements (consider CDC)
- Multi-source transactions requiring exact consistency

## Resolved Questions

1. **NULL handling**: Should we include `OR time_col IS NULL` like Iceberg does?
   - **Decision**: Yes - include NULLs to prevent silent drops for new tables

2. **Watermark precision**: Store with timezone or without?
   - **Decision**: Store as-is from database, no timezone conversion

3. **Multi-column watermarks**: Support composite time columns?
   - **Decision**: Single column only (start simple, extend if needed)

## Next Steps

1. Implement `postgresql_metadata.py` with `get_max_watermark()` function
2. Extend `_should_run_node()` in `runner.py` for PostgreSQL detection
3. Add WHERE clause generation in `source_executor.py`
4. Update `state.py` with `postgresql_watermark` field
5. Add tests mirroring Iceberg incremental tests

## References

- Iceberg incremental detection: `src/seeknal/workflow/iceberg_metadata.py`
- Source executor: `src/seeknal/workflow/executors/source_executor.py`
- State management: `src/seeknal/workflow/state.py`
- Existing freshness config: `freshness.time_column` in YAML sources
