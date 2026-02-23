---
category: python-pipeline
component: PostgresMaterializationHelper
tags: [duckdb, sql, timestamp, cast, postgresql]
date_resolved: 2026-02-20
related_build: lineage-inspect-postgresql-materialization
related_tasks: [C-5]
---

# DuckDB CAST Required for Timestamp Arithmetic with String Literals

## Problem Symptom

`materialize_incremental()` failed with a DuckDB binder error: `-(STRING_LITERAL, INTERVAL)` when computing the time range for a DELETE operation.

## Investigation Steps

1. Error occurred in `WHERE {time_column} >= '{min_time}' - INTERVAL '{lookback} days'`
2. `min_time` was retrieved from `MIN(order_date)` — returns a string when interpolated into SQL
3. DuckDB cannot perform arithmetic (subtract INTERVAL) on a string literal

## Root Cause

When a `MIN(time_column)` result is interpolated into an f-string SQL query, it becomes a string literal. DuckDB does not auto-cast string literals to TIMESTAMP for arithmetic operations, so the expression `'2024-01-01' - INTERVAL '7 days'` raises a binder error.

## Working Solution

Wrap the interpolated value with explicit `CAST`:

```python
# Before (broken):
f"WHERE {time_column} >= '{min_time}' - INTERVAL '{lookback} days'"

# After (fixed):
f"WHERE {time_column} >= CAST('{min_time}' AS TIMESTAMP) - INTERVAL '{lookback} days'"
```

**Result:** Incremental materialization now correctly computes the time range for the DELETE + INSERT cycle.

## Prevention Strategies

1. Always use explicit `CAST` when interpolating Python values into DuckDB SQL for arithmetic operations
2. Consider using parameterized queries where possible to avoid string interpolation entirely
3. Test incremental materialization with real date values, not just mocks — the binder error only surfaces at runtime with actual data

## Cross-References

- `src/seeknal/workflow/materialization/postgresql.py:232` — Fix location
