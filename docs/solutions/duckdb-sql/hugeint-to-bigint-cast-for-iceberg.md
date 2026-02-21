---
category: duckdb-sql
component: materialization-dispatcher
tags: [duckdb, iceberg, type-compat, aggregate, cast, bigint, hugeint]
date_resolved: 2026-02-21
related_build: project-aware-repl-env-aware-execution
related_tasks: [qa-iceberg-medallion, qa-python-iceberg-medallion]
---

# DuckDB HUGEINT to BIGINT Cast for Iceberg Compatibility

## Problem Symptom

Iceberg materialization failed with a type error when writing transforms that used `COUNT(*)` or `SUM()`:

**Error:** `HUGEINT is not a valid Iceberg type`

The error was raised by the Iceberg REST catalog when DuckDB attempted to register the output schema.

## Root Cause

DuckDB's aggregate functions `COUNT(*)` and `SUM()` return `HUGEINT` (128-bit integer) by default to avoid overflow on large tables. The Iceberg type system has no `HUGEINT` equivalent — only `INT` (32-bit) and `LONG`/`BIGINT` (64-bit) are supported for integers. When DuckDB infers the output schema and submits it to the Iceberg REST catalog, the `HUGEINT` column type is rejected.

## Working Solution

Wrap aggregate expressions with explicit `CAST` at the `SELECT` level in the transform SQL:

```sql
-- Before (broken): COUNT(*) returns HUGEINT -- not supported by Iceberg
SELECT
    category,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM orders
GROUP BY category

-- After (fixed): explicit CAST to Iceberg-compatible types
SELECT
    category,
    CAST(COUNT(*) AS BIGINT) AS order_count,
    CAST(SUM(amount) AS DOUBLE) AS total_amount
FROM orders
GROUP BY category
```

Apply the cast in the final view or the `SELECT` that feeds into `write_to_iceberg()` / `MaterializationDispatcher.dispatch()`.

**Result:** Iceberg materialization succeeded. `BIGINT` and `DOUBLE` are valid Iceberg types that round-trip correctly through the REST catalog.

## Prevention Strategies

1. Always use `CAST(COUNT(*) AS BIGINT)` and `CAST(SUM(...) AS BIGINT or DOUBLE)` in any transform that targets Iceberg
2. Add a pre-write schema validation step that detects `HUGEINT` columns and raises a clear error message pointing to this fix
3. Document the required casts in the Iceberg materialization guide so new pipeline authors see it at authoring time
4. Cover aggregation nodes in QA medallion tests that target Iceberg to catch regressions automatically

## Test Cases Added

- `test_iceberg_count_star_cast_bigint`
- `test_python_iceberg_medallion_gold_aggregations`

## Cross-References

- Spec: specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md
- Related: docs/solutions/duckdb-sql/group-by-completeness-for-aggregations.md
- `src/seeknal/workflow/materialization/dispatcher.py` — write path that submits schema to Iceberg catalog
