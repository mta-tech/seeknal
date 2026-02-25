# Chapter 7: Data Profiling & Validation

> **Duration:** 20 minutes | **Difficulty:** Intermediate | **Format:** YAML & CLI

Learn to compute statistical profiles from your data and validate them with threshold-based quality checks.

---

## What You'll Build

A profiling and validation pipeline that computes statistics and checks thresholds:

```
source.products ──→ profile.products_stats ──→ rule.products_quality
                         │
                         └── row_count, avg, stddev, null_percent,
                             distinct_count, top_values, freshness
```

**After this chapter, you'll have:**
- A profile node that computes 14+ metrics per column automatically
- A profile_check rule that validates thresholds (soda-style)
- Understanding of the `source → profile → rule` validation pipeline

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: File Sources](1-file-sources.md) — Sources loaded (especially `source.products`)
- [ ] [Chapter 3: Data Rules](3-data-rules.md) — Understanding of rule nodes

---

## Part 1: Creating a Profile (8 minutes)

### Understanding Profiles

A **profile** is a node type that reads data from an upstream source or transform and computes aggregate statistics. Unlike rules (which produce pass/fail), profiles produce a **stats table** — a long-format parquet with one row per (column, metric) pair.

Key concepts:
- **`kind: profile`** — Declares a profile node
- **Auto-detection** — Column types are detected via DuckDB `DESCRIBE`; metrics are computed per type
- **`profile.columns:`** — Optional column filter (default: all columns)
- **`profile.params.max_top_values:`** — How many frequent values to track (default: 5)

### Draft a Profile

```bash
seeknal draft profile products_stats
```

This creates `draft_profile_products_stats.yml`. Edit it:

```yaml
kind: profile
name: products_stats
description: "Statistical profile of the products catalog"
inputs:
  - ref: source.products
```

That's it! With no `profile:` section, all columns are profiled with default settings.

!!! info "Auto-Detection"
    The profile executor uses DuckDB `DESCRIBE` to classify each column:

    | Column Type | Metrics Computed |
    |------------|-----------------|
    | Numeric (INTEGER, DOUBLE, DECIMAL...) | min, max, avg, sum, stddev, p25, p50, p75 |
    | Timestamp (TIMESTAMP, DATE...) | min, max, freshness_hours |
    | String (VARCHAR, TEXT...) | top_values (with counts and percentages) |
    | Boolean | top_values |
    | All columns | null_count, null_percent, distinct_count |
    | Table-level | row_count |

### Validate and Apply

```bash
seeknal dry-run draft_profile_products_stats.yml
seeknal apply draft_profile_products_stats.yml
```

**Checkpoint:** The dry-run shows a preview of the profile configuration. The file moves to `seeknal/profiles/products_stats.yml`.

### Run and Explore

```bash
seeknal run
```

Watch the output — you'll see the profile execute:

```
profile.products_stats: SUCCESS in 0.05s (6 input rows → 42 metrics)
```

Now explore the stats in the REPL:

```bash
seeknal repl
```

```sql
-- See all computed metrics
SELECT * FROM profile_products_stats;

-- Check table-level row count
SELECT value FROM profile_products_stats
WHERE column_name = '_table_' AND metric = 'row_count';

-- Numeric stats for the price column
SELECT metric, value
FROM profile_products_stats
WHERE column_name = 'price'
ORDER BY metric;

-- Top categories
SELECT detail
FROM profile_products_stats
WHERE column_name = 'category' AND metric = 'top_values';
```

!!! tip "Output Schema"
    The profile parquet always has exactly 4 columns:

    | Column | Type | Description |
    |--------|------|-------------|
    | `column_name` | VARCHAR | Source column, or `_table_` for table-level |
    | `metric` | VARCHAR | Metric name (e.g., `avg`, `null_percent`) |
    | `value` | VARCHAR | Stringified metric value |
    | `detail` | VARCHAR | JSON detail (for `top_values`), otherwise NULL |

**Checkpoint:** You should see metrics like `row_count = 6`, `price.avg ≈ 85.58`, `price.stddev ≈ 62.88`, `category.distinct_count = 5`, etc.

---

## Part 2: Filtering Columns & Parameters (5 minutes)

### Profile with Column Filter

For large tables, you may only want to profile specific columns. Create a focused profile:

```bash
seeknal draft profile products_price_stats
```

Edit `draft_profile_products_price_stats.yml`:

```yaml
kind: profile
name: products_price_stats
description: "Focused profile on price and category"
inputs:
  - ref: source.products
profile:
  columns: [price, category]
  params:
    max_top_values: 3
```

```bash
seeknal dry-run draft_profile_products_price_stats.yml
seeknal apply draft_profile_products_price_stats.yml
seeknal run
```

!!! info "Column Filter Behavior"
    - Only the listed columns are profiled (plus the table-level `row_count`)
    - If a column in the filter doesn't exist in the data, it's skipped with a warning
    - `max_top_values: 3` limits the top frequent values to 3 (useful for high-cardinality columns)

### Explore the Focused Profile

```bash
seeknal repl
```

```sql
-- Focused profile has fewer metrics
SELECT column_name, metric, value
FROM profile_products_price_stats
ORDER BY column_name, metric;
```

**Checkpoint:** You should see metrics only for `_table_`, `price`, and `category` — not for `product_id`, `name`, or `launch_date`.

---

## Part 3: Profile Check Rules (7 minutes)

### Understanding Profile Checks

A **profile_check** is a rule type that evaluates threshold expressions against profile stats. Instead of checking individual rows (like `null` or `range` rules), it checks aggregate metrics: "Is the average price too high?", "Did the row count drop to zero?", "Are there enough distinct categories?"

Key concepts:
- **`rule.type: profile_check`** — Declares this rule reads profile stats
- **`rule.checks:`** — List of threshold checks
- **`metric:`** — Which metric to check (e.g., `avg`, `null_percent`, `row_count`)
- **`column:`** — Which column (optional; defaults to `_table_` for table-level metrics)
- **`fail:`** — Threshold that halts the pipeline
- **`warn:`** — Threshold that logs a warning but continues

### Threshold Expression Syntax

Expressions compare the metric value against a threshold:

| Expression | Meaning |
|------------|---------|
| `"> 5"` | Value greater than 5 |
| `">= 10"` | Value greater than or equal to 10 |
| `"< 100"` | Value less than 100 |
| `"= 0"` | Value equals 0 |
| `"!= 3"` | Value not equal to 3 |
| `"between 10 and 500"` | Value between 10 and 500 (inclusive) |

### Create a Profile Check Rule

```bash
seeknal draft rule products_quality
```

Edit `draft_rule_products_quality.yml`:

```yaml
kind: rule
name: products_quality
description: "Validate product data quality via profile stats"
inputs:
  - ref: profile.products_stats
rule:
  type: profile_check
  checks:
    # Table-level: ensure we have data
    - metric: row_count
      fail: "= 0"

    # Column-level: price sanity
    - column: price
      metric: avg
      warn: "> 500"

    - column: price
      metric: null_percent
      fail: "> 20"

    # Category diversity
    - column: category
      metric: distinct_count
      fail: "< 2"
```

!!! info "Check Structure"
    Each check needs:

    - **`metric:`** (required) — The metric name from the profile
    - **`column:`** (optional) — Column name; omit for table-level metrics like `row_count`
    - **`fail:`** and/or **`warn:`** (at least one required) — Threshold expression

    If both `fail:` and `warn:` are specified, `fail:` is evaluated first.

### Validate and Apply

```bash
seeknal dry-run draft_rule_products_quality.yml
seeknal apply draft_rule_products_quality.yml
```

### Run the Full Pipeline

```bash
seeknal plan
```

You should see the dependency chain:

```
source.products
  └── profile.products_stats
        └── rule.products_quality
```

```bash
seeknal run
```

**Expected output:**
```
source.products: SUCCESS in 0.01s
profile.products_stats: SUCCESS in 0.05s (6 input rows)
rule.products_quality: SUCCESS in 0.01s
  ✓ row_count != 0
  ✓ price.avg <= 500
  ✓ price.null_percent <= 20
  ✓ category.distinct_count >= 2
```

All checks pass because:
- Row count is 6 (not 0)
- Average price is ~85.58 (not > 500)
- No null prices (null_percent = 0)
- 5 distinct categories (not < 2)

**Checkpoint:** The full `source → profile → rule` pipeline runs successfully with all checks passing.

---

## Part 4: Testing Failures (5 minutes)

### Trigger a Warning

Update the rule to set a stricter warn threshold:

Edit `seeknal/rules/products_quality.yml` directly:

```yaml
    # Tighten the warning threshold
    - column: price
      metric: avg
      warn: "> 50"
```

```bash
seeknal run
```

Now you'll see a warning for `price.avg = 85.58 > 50`, but the pipeline continues.

### Trigger a Failure

Change the category threshold to see a failure:

```yaml
    # This will fail — we have 5, not 10+
    - column: category
      metric: distinct_count
      fail: "< 10"
```

```bash
seeknal run
```

The pipeline stops at `rule.products_quality` because `distinct_count = 5 < 10` triggers the fail threshold. Downstream nodes (if any) won't execute.

### Restore the Rule

Revert to the original thresholds:

```yaml
    - column: price
      metric: avg
      warn: "> 500"

    - column: category
      metric: distinct_count
      fail: "< 2"
```

```bash
seeknal run
```

All checks pass again.

### SQL Escape Hatch

For complex checks that threshold expressions can't cover, use a `type: custom` rule with raw SQL against the profile parquet:

```yaml
kind: rule
name: products_advanced_check
inputs:
  - ref: profile.products_stats
rule:
  type: custom
  sql: |
    SELECT column_name, metric, value
    FROM input_0
    WHERE (metric = 'null_percent' AND CAST(value AS DOUBLE) > 20)
       OR (metric = 'row_count' AND CAST(value AS DOUBLE) = 0)
```

!!! tip "When to Use SQL vs profile_check"
    | Approach | Best For |
    |----------|----------|
    | `type: profile_check` | Simple threshold checks on individual metrics |
    | `type: custom` SQL | Complex logic, cross-metric comparisons, conditional checks |

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Profile reads from wrong input**

    - Symptom: Profile has 0 rows or unexpected columns
    - Fix: Check that `inputs: [ref: source.my_source]` points to an existing, already-executed node. Use `seeknal repl` to verify the upstream data exists.

    **2. Metric not found in profile check**

    - Symptom: Rule fails with "Metric 'X' not found for column 'Y'"
    - Fix: Check spelling of `metric:` and `column:` in your checks. Use `SELECT DISTINCT metric FROM profile_<name>` in REPL to see available metrics.

    **3. NULL metric value triggers unexpected failure**

    - Symptom: Check fails even though the threshold looks correct
    - Fix: If a metric value is NULL (e.g., `stddev` on a single-row table), a `fail:` threshold will treat it as a failure. Use `warn:` instead if NULL is acceptable.

    **4. Quoting threshold expressions**

    - Symptom: YAML parse error on `fail: > 5`
    - Fix: Always quote expressions: `fail: "> 5"`. Without quotes, YAML interprets `> 5` as a block scalar.

    **5. Profile check sees stale data**

    - Symptom: Check results don't match current source data
    - Fix: The profile reads from intermediate parquets. Run `seeknal run` to refresh both the source and profile before checking.

---

## Summary

In this chapter, you learned:

- [x] **Profile Nodes** — Compute statistics automatically with `kind: profile`
- [x] **Auto-Detection** — Column types detected via DuckDB; metrics computed per type
- [x] **Column Filtering** — Focus profiling on specific columns with `profile.columns`
- [x] **Profile Check Rules** — Validate metrics with threshold expressions (`> 5`, `between 10 and 500`)
- [x] **Fail vs Warn** — `fail:` halts the pipeline, `warn:` logs and continues
- [x] **End-to-End Pipeline** — `source → profile → rule` for automated data quality

**Metric Types by Column:**

| Column Type | Available Metrics |
|------------|-------------------|
| All columns | `null_count`, `null_percent`, `distinct_count` |
| Table-level | `row_count` |
| Numeric | `min`, `max`, `avg`, `sum`, `stddev`, `p25`, `p50`, `p75` |
| Timestamp | `min`, `max`, `freshness_hours` |
| String/Boolean | `top_values` (JSON in `detail` column) |

**Profile Check Operators:**

| Operator | Example | Description |
|----------|---------|-------------|
| `>` | `"> 5"` | Greater than |
| `>=` | `">= 10"` | Greater than or equal |
| `<` | `"< 100"` | Less than |
| `<=` | `"<= 0"` | Less than or equal |
| `=` | `"= 0"` | Equals |
| `!=` | `"!= 3"` | Not equals |
| `between` | `"between 10 and 500"` | Inclusive range |

**YAML Structure:**

```yaml
# Profile node
kind: profile
name: my_stats
inputs:
  - ref: source.my_source
# Optional: filter columns and params
profile:
  columns: [col1, col2]
  params:
    max_top_values: 10    # default: 5

# Profile check rule
kind: rule
name: my_quality_check
inputs:
  - ref: profile.my_stats
rule:
  type: profile_check
  checks:
    - metric: row_count           # table-level (no column:)
      fail: "= 0"
    - column: col1                # column-level
      metric: avg
      warn: "> 500"
      fail: "> 1000"
```

**Key Commands:**
```bash
seeknal draft profile <name>        # Generate profile template
seeknal draft rule <name>           # Generate rule template
seeknal dry-run <draft_file>.yml    # Preview configuration
seeknal apply <draft_file>.yml      # Apply to project
seeknal plan                        # View DAG (profile → rule chain)
seeknal run                         # Execute pipeline with profiling
seeknal repl                        # Query profile stats interactively
```

---

## What's Next?

Explore other advanced capabilities or dive into the reference documentation:

- **[Chapter 4: Lineage & Inspection](4-lineage.md)** — Visualize your profile → rule data flow
- **[Chapter 5: Named ref() References](5-named-refs.md)** — Self-documenting SQL references
- **[Chapter 6: Common Configuration](6-common-config.md)** — Shared configuration across nodes

---

## See Also

- **[Chapter 3: Data Rules](3-data-rules.md)** — Row-level validation rules (null, range, uniqueness, freshness)
- **[CLI Reference](../../reference/cli.md)** — All commands and flags
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Source, transform, profile, and rule schemas
