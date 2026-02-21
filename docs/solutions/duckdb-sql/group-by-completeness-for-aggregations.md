---
category: duckdb-sql
component: transform-executor
tags: [duckdb, sql, group-by, aggregate, iceberg, strict-sql]
date_resolved: 2026-02-21
related_build: project-aware-repl-env-aware-execution
related_tasks: [qa-iceberg-medallion, qa-python-iceberg-medallion]
---

# GROUP BY Completeness Required for DuckDB Aggregations

## Problem Symptom

DuckDB raised an error during aggregation transforms that targeted Iceberg:

**Error:** `column "product_category" must appear in the GROUP BY clause or be used in an aggregate function`

The transform ran without errors in local testing with SQLite or MySQL but failed under DuckDB.

## Root Cause

DuckDB enforces the SQL standard strictly: in any `SELECT` that contains at least one aggregate function, every non-aggregate column in the `SELECT` list must also appear in the `GROUP BY` clause. Transforms ported from permissive SQL dialects (SQLite, or MySQL with `ONLY_FULL_GROUP_BY` disabled) may omit columns from `GROUP BY` that are functionally determined by the grouped key. DuckDB does not infer functional dependencies and rejects the query.

## Working Solution

Audit every `SELECT` that contains an aggregate function and ensure all non-aggregate columns appear in `GROUP BY`:

```sql
-- Before (broken): product_category in SELECT but missing from GROUP BY
SELECT
    region,
    product_category,
    CAST(COUNT(*) AS BIGINT) AS order_count
FROM orders
GROUP BY region

-- After (fixed): all non-aggregate columns listed in GROUP BY
SELECT
    region,
    product_category,
    CAST(COUNT(*) AS BIGINT) AS order_count
FROM orders
GROUP BY region, product_category
```

The rule is mechanical: for every column expression in `SELECT`, it must either be wrapped in an aggregate function (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`) or appear verbatim in `GROUP BY`.

**Result:** Aggregation transforms ran successfully once `GROUP BY` was complete.

## Prevention Strategies

1. Use DuckDB for local development and testing — catch `GROUP BY` errors before CI rather than at Iceberg write time
2. Add a code review checklist item: for every `SELECT` with aggregate functions, verify all non-aggregate columns are in `GROUP BY`
3. Consider a static SQL linter step in the pipeline validation phase that flags incomplete `GROUP BY` clauses
4. When porting transforms from SQLite or MySQL, treat all `GROUP BY` clauses as suspect and re-verify against DuckDB

## Test Cases Added

- `test_aggregation_group_by_completeness`

## Cross-References

- Spec: specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md
- Related: docs/solutions/duckdb-sql/hugeint-to-bigint-cast-for-iceberg.md
