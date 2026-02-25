# Data Profiling & Profile-Based Validation

**Date:** 2026-02-24
**Status:** Brainstorm Complete
**Author:** Claude + Fitra

---

## What We're Building

A new `kind: profile` node type that computes statistical profiles (row count, avg, stddev, nulls, distinct counts, etc.) from any upstream node, outputs results as a queryable parquet table, and feeds into enhanced rule checks for data validation.

### The Flow

```
source.products
  │
  ├──> profile.products_stats       (computes statistics)
  │         │
  │         └──> rule.products_quality  (checks thresholds against stats)
  │
  └──> transform.clean_products     (normal pipeline continues)
```

### User Experience

```bash
# 1. Draft a profile
seeknal draft profile products_stats

# 2. Edit the YAML (or accept auto-detect defaults)
# 3. Validate
seeknal dry-run draft_profile_products_stats.yml

# 4. Apply
seeknal apply draft_profile_products_stats.yml

# 5. Run pipeline — profile computes, rules check
seeknal run

# 6. Query profile stats in REPL
seeknal repl
> SELECT * FROM profile_products_stats WHERE column_name = 'price';
```

---

## Why This Approach

**Separate profile node** over inline rule computation because:

1. **Reusability** — Multiple rules can reference the same profile without recomputing
2. **REPL access** — Profile stats are queryable as a regular table
3. **DAG clarity** — Profiling and validation are distinct concerns with clear edges
4. **Soda alignment** — Mirrors soda-core's separation of metrics vs checks
5. **Composability** — Profiles can feed into transforms, exposures, or reports — not just rules

---

## Key Decisions

### 1. Architecture: Separate `kind: profile` Node

A new node type (`NodeType.PROFILE`) with its own executor (`ProfileExecutor`), following the same registration pattern as existing nodes.

**YAML structure:**

```yaml
# Minimal — auto-detect all columns
kind: profile
name: products_stats
inputs:
  - ref: source.products

# With column filter and params
kind: profile
name: products_stats
inputs:
  - ref: source.products
profile:
  columns: [price, quantity, category]
  params:
    max_top_values: 10   # default: 5
```

### 2. Metrics: Comprehensive, Type-Aware

Profile auto-detects column types and computes appropriate metrics:

| Column Type | Metrics |
|-------------|---------|
| **All** | row_count, null_count, null_percent, distinct_count |
| **Numeric** | avg, sum, stddev, variance, min, max, p25, p50 (median), p75 |
| **String** | top_values (top N frequent values + percentages, default 5, configurable via `params.max_top_values`) |
| **Timestamp** | min_timestamp, max_timestamp, freshness_hours |

When `columns:` is omitted, ALL columns are profiled. When specified, only those columns are included.

### 3. Output Format: Long-Format Stats Parquet

One row per (column, metric) pair. Stored at `target/intermediate/profile_<name>.parquet`.

```
| column_name | metric         | value   | detail           |
|-------------|----------------|---------|------------------|
| _table_     | row_count      | 1000    |                  |
| price       | avg            | 52.30   |                  |
| price       | stddev         | 18.75   |                  |
| price       | null_count     | 3       |                  |
| price       | null_percent   | 0.3     |                  |
| price       | min            | 5.99    |                  |
| price       | max            | 199.99  |                  |
| price       | distinct_count | 847     |                  |
| price       | p25            | 29.99   |                  |
| price       | p50            | 49.99   |                  |
| price       | p75            | 69.99   |                  |
| category    | distinct_count | 5       |                  |
| category    | top_values     |         | Electronics:40%  |
| created_at  | freshness_hours| 2.5     |                  |
```

**Why long-format:** Extensible (new metrics don't change schema), filterable (`WHERE column_name = 'price'`), and uniform across column types.

### 4. Rule Enhancement: Soda-Style Checks + SQL Escape Hatch

Rules gain a new `type: profile_check` that reads profile output and evaluates thresholds:

```yaml
kind: rule
name: products_quality
inputs:
  - ref: profile.products_stats
rule:
  type: profile_check
  checks:
    # Table-level
    - metric: row_count
      fail: "= 0"
      warn: "< 100"

    # Column-level
    - column: price
      metric: avg
      warn: "between 10 and 500"

    - column: price
      metric: null_percent
      warn: "> 5"
      fail: "> 20"

    - column: price
      metric: stddev
      warn: "> 100"

    - column: category
      metric: distinct_count
      fail: "< 2"
```

**SQL escape hatch** for complex checks:

```yaml
rule:
  type: custom
  sql: |
    SELECT column_name, metric, value
    FROM input_0
    WHERE (metric = 'null_percent' AND CAST(value AS DOUBLE) > 20)
       OR (metric = 'row_count' AND CAST(value AS DOUBLE) = 0)
```

### 5. Failure Behavior: Halt on Fail, Continue on Warn

Consistent with existing rule behavior:
- `fail:` threshold breached → pipeline halts, downstream nodes skip
- `warn:` threshold breached → log warning, pipeline continues
- No threshold breached → pass silently

### 6. Column Auto-Detection

Profile inspects the input schema via DuckDB `DESCRIBE` to determine column types, then selects appropriate metrics per type. No manual type annotation needed.

---

## Scope Boundaries

**In scope:**
- `ProfileExecutor` with DuckDB-based stat computation
- Long-format parquet output registered in intermediate/
- `profile_check` rule type with soda-style threshold syntax
- Auto-detect column types, optional column filter
- `seeknal draft profile <name>` CLI support
- dry-run preview showing sample stats
- REPL access to profile tables

**Out of scope (future):**
- Historical profile comparison (profile drift over time)
- Anomaly detection (automatic threshold learning)
- Profile visualization / HTML reports
- Sampling for large tables (compute on full data for now)
- Soda-core library integration (we compute our own stats via DuckDB)

---

## Implementation Guidance

### Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `src/seeknal/workflow/executors/profile_executor.py` | Create | ProfileExecutor with stat computation |
| `src/seeknal/dag/manifest.py` | Modify | Add `NodeType.PROFILE` to enum |
| `src/seeknal/workflow/executors/rule_executor.py` | Modify | Add `profile_check` rule type |
| `src/seeknal/workflow/dag.py` | Modify | Handle `kind: profile` in DAG builder |
| `src/seeknal/workflow/templates/profile.yml.j2` | Create | Template for `seeknal draft profile` |
| `tests/workflow/test_profile_executor.py` | Create | Unit tests |
| `tests/workflow/test_rule_profile_check.py` | Create | Rule + profile integration tests |

### DuckDB Stat Computation Strategy

Generate per-column SQL dynamically based on detected type:

```sql
-- Numeric column
SELECT 'price' AS column_name, 'avg' AS metric,
       CAST(AVG(price) AS VARCHAR) AS value, '' AS detail
FROM input_data
UNION ALL
SELECT 'price', 'stddev', CAST(STDDEV(price) AS VARCHAR), ''
FROM input_data
-- ... etc
```

Use `UNION ALL` to build a single query producing the long-format output.
