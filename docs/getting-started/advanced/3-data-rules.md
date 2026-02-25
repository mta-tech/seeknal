# Chapter 3: Data Rules

> **Duration:** 25 minutes | **Difficulty:** Beginner | **Format:** YAML & CLI

Learn to validate data quality with automated rule checks in your Seeknal pipeline.

---

## What You'll Build

Three rule nodes that validate your pipeline data:

```
transform.events_cleaned ──→ rule.not_null_quantity    (null check)
                         ──→ rule.positive_quantity    (range check)
source.products          ──→ rule.valid_prices         (range on source)
```

**After this chapter, you'll have:**
- Rules that check for null values, valid ranges, and data integrity
- Understanding of `severity: error` vs `severity: warn`
- A complete pipeline with both transforms and validation rules

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: File Sources](1-file-sources.md) — Three sources loaded
- [ ] [Chapter 2: Transformations](2-transformations.md) — Three transforms created and pipeline running

---

## Part 1: Null & Range Rules (8 minutes)

### Understanding Rules

Rules are separate YAML nodes that validate data from upstream sources or transforms. They use the same `inputs:` and `ref:` syntax as transforms, but instead of producing new tables, they produce pass/fail results.

Key concepts:
- **`rule.type:`** — The kind of validation: `null`, `range`, `uniqueness`, or `freshness`
- **`rule.columns:` or `rule.column:`** — Which column(s) to validate
- **`rule.params:`** — Type-specific thresholds (e.g., `min_val`, `max_null_percentage`)
- **`severity: error`** — Fails the pipeline run if the rule is violated
- **`severity: warn`** — Logs a warning but continues the pipeline run

### Check for Null Quantities

Even though `events_cleaned` filters nulls, let's add a rule as a safety net:

```bash
seeknal draft rule not_null_quantity
```

Edit `draft_rule_not_null_quantity.yml`:

```yaml
kind: rule
name: not_null_quantity
description: "Ensure quantity is never null in cleaned events"
inputs:
  - ref: transform.events_cleaned
rule:
  type: "null"
  columns:
    - quantity
  params:
    max_null_percentage: 0.0
params:
  severity: error
  error_message: "Null quantity found in cleaned events"
```

!!! info "Rule Type: `null`"
    The `null` type checks for null values across the listed columns. `max_null_percentage: 0.0` means zero nulls are allowed. Setting it to `0.05` would allow up to 5% null rows.

!!! info "Rules Need `inputs:`"
    Just like transforms, rules must declare their upstream dependency in `inputs:`. The rule evaluates against the data from that node.

### Check for Positive Quantities

```bash
seeknal draft rule positive_quantity
```

Edit `draft_rule_positive_quantity.yml`:

```yaml
kind: rule
name: positive_quantity
description: "Ensure all quantities are positive"
inputs:
  - ref: transform.events_cleaned
rule:
  type: range
  column: quantity
  params:
    min_val: 1
params:
  severity: error
  error_message: "Non-positive quantity detected"
```

!!! info "Rule Type: `range`"
    The `range` type checks that values in `column:` fall within `min_val` and/or `max_val`. Here, `min_val: 1` ensures all quantities are at least 1. You can use `min_val`, `max_val`, or both.

### Validate and Apply Both Rules

```bash
seeknal dry-run draft_rule_not_null_quantity.yml
seeknal apply draft_rule_not_null_quantity.yml

seeknal dry-run draft_rule_positive_quantity.yml
seeknal apply draft_rule_positive_quantity.yml
```

**Checkpoint:** Both rules should apply successfully. They're now part of your project at `seeknal/rules/`.

---

## Part 2: Source Validation & Severity Levels (7 minutes)

### Validate Product Prices

Rules can check source data too, not just transforms. Let's validate that product prices are reasonable:

```bash
seeknal draft rule valid_prices
```

Edit `draft_rule_valid_prices.yml`:

```yaml
kind: rule
name: valid_prices
description: "Ensure product prices are within valid range"
inputs:
  - ref: source.products
rule:
  type: range
  column: price
  params:
    min_val: 0
    max_val: 10000
params:
  severity: warn
  error_message: "Product price outside valid range (0-10000)"
```

!!! tip "Error vs Warn"
    This rule uses `severity: warn` instead of `error`. Here's the difference:

    | Severity | Behavior | Use When |
    |----------|----------|----------|
    | `error` | **Fails** the pipeline run | Data must be valid (e.g., no nulls in required fields) |
    | `warn` | **Logs warning**, continues | Data should be valid but pipeline can proceed (e.g., price sanity checks) |

    Use `error` for hard constraints and `warn` for soft guidelines.

```bash
seeknal dry-run draft_rule_valid_prices.yml
seeknal apply draft_rule_valid_prices.yml
```

### Multiple Checks in One Rule

You can combine multiple validation checks in a single rule file by using a list under `rule:`. This keeps related checks together:

```yaml
kind: rule
name: product_quality
description: "Validate product data quality"
inputs:
  - ref: source.products
rule:
  - type: "null"
    columns: [name, price]
    params:
      max_null_percentage: 0.0
  - type: range
    column: price
    params:
      min_val: 0
      max_val: 10000
params:
  severity: warn
  error_message: "Product data quality check failed"
```

All checks run against the same input data. If **any** check fails, the rule fails (respecting the `severity` setting). The results metadata includes details for each individual check.

!!! info "Single vs Multi-Check"
    Both formats work — use whichever fits your needs:

    | Format | When to Use |
    |--------|-------------|
    | `rule: {type: ...}` | Single focused validation |
    | `rule: [{type: ...}, ...]` | Multiple related validations on the same input |

---

## Part 2b: SQL Assertion Rules (5 minutes)

### Custom SQL Validation

For checks that don't fit the built-in rule types (null, range, uniqueness, freshness), use `sql_assertion`. This is a dbt-style test — you write a SQL query that returns "bad rows". If the query returns any rows, the rule fails.

```bash
seeknal draft rule no_duplicate_events
```

Edit `draft_rule_no_duplicate_events.yml`:

```yaml
kind: rule
name: no_duplicate_events
description: "Ensure no duplicate event_id values exist"
inputs:
  - ref: transform.events_cleaned
rule:
  type: sql_assertion
  sql: |
    SELECT event_id, COUNT(*) as cnt
    FROM input_data
    GROUP BY event_id
    HAVING cnt > 1
params:
  severity: error
  error_message: "Duplicate event_id found in cleaned events"
```

!!! info "Rule Type: `sql_assertion`"
    The `sql_assertion` type runs your SQL query against the input data. If the query returns **zero rows**, the rule passes. If it returns **any rows**, those are violations and the rule fails.

    The first input is automatically available as `input_data`. You can write any SQL — aggregations, subqueries, window functions — as long as it returns the "bad rows" you want to detect.

### Cross-Table Assertions

`sql_assertion` supports multiple inputs. Each input ref becomes a named view (dots replaced with underscores):

```yaml
kind: rule
name: valid_order_references
description: "Ensure all orders reference valid products"
inputs:
  - ref: source.sales_events
  - ref: source.products
rule:
  type: sql_assertion
  sql: |
    SELECT e.event_id, e.product_id
    FROM source_sales_events e
    LEFT JOIN source_products p ON e.product_id = p.product_id
    WHERE p.product_id IS NULL
params:
  severity: warn
  error_message: "Orders reference non-existent products"
```

!!! tip "Named Views from Inputs"
    Each `ref:` in `inputs:` creates a DuckDB view named by replacing dots with underscores:

    | Input ref | View name |
    |-----------|-----------|
    | `source.sales_events` | `source_sales_events` |
    | `source.products` | `source_products` |
    | `transform.events_cleaned` | `transform_events_cleaned` |

    The first input is also available as `input_data` for backward compatibility.

### Aggregate Checks

Use `sql_assertion` for aggregate validations that the built-in types don't cover:

```yaml
kind: rule
name: reasonable_avg_price
description: "Ensure average product price is reasonable"
inputs:
  - ref: source.products
rule:
  type: sql_assertion
  sql: |
    SELECT 1
    WHERE (SELECT AVG(price) FROM input_data) > 5000
params:
  severity: warn
```

Apply and test:

```bash
seeknal dry-run draft_rule_no_duplicate_events.yml
seeknal apply draft_rule_no_duplicate_events.yml
```

**Checkpoint:** The sql_assertion rule should apply successfully and pass when running `seeknal run`.

---

## Part 3: Running Rules in Your Pipeline (5 minutes)

### View the Full DAG

Now let's see the complete pipeline with all transforms and rules:

```bash
seeknal plan
```

You should see a DAG with 9 nodes:
```
source.products
source.sales_events
source.sales_snapshot
transform.events_cleaned        (depends on: source.sales_events)
transform.sales_enriched        (depends on: transform.events_cleaned, source.products)
transform.sales_summary         (depends on: transform.sales_enriched)
rule.not_null_quantity           (depends on: transform.events_cleaned)
rule.positive_quantity           (depends on: transform.events_cleaned)
rule.valid_prices                (depends on: source.products)
```

Rules appear as leaf nodes — they depend on their input but nothing depends on them.

### Run the Full Pipeline

```bash
seeknal run
```

Watch the output — you'll see each node execute in dependency order. The rules run after their inputs and report validation results:

**Expected output:**
```
...
rule.valid_prices: SUCCESS in 0.02s (6 rows)
rule.not_null_quantity: SUCCESS in 0.01s (5 rows)
rule.positive_quantity: SUCCESS in 0.01s (5 rows)
...
```

All rules pass because:
- `events_cleaned` already filters null and deduplicates, so `not_null_quantity` and `positive_quantity` pass
- All product prices are between 0 and 10,000, so `valid_prices` passes

### Test with Failing Data

To see what happens when a rule fails, temporarily add a bad row to your products:

```bash
echo 'PRD-007,Broken Item,Test,-5.00,2026-01-01' >> data/products.csv
seeknal run
```

The `valid_prices` rule will now log a **warning** (since it uses `severity: warn`). The pipeline continues running.

If `not_null_quantity` or `positive_quantity` fail (they use `severity: error`), the pipeline would **stop** at that point.

After testing, remove the bad row:

```bash
# Restore original products.csv (keep only first 7 lines: header + 6 data rows)
head -7 data/products.csv > data/products_clean.csv
mv data/products_clean.csv data/products.csv
```

**Checkpoint:** Your full pipeline runs successfully with all 9 nodes — 3 sources, 3 transforms, and 3 rules.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Rule references a column that doesn't exist**

    - Symptom: Rule fails with "column not found" error
    - Fix: Check that the column in `rule.column:` or `rule.columns:` exists in the output of the node referenced in `inputs:`. Use `seeknal repl` to query the upstream node and verify column names.

    **2. Missing `inputs:` in rule YAML**

    - Symptom: Rule doesn't know what data to validate
    - Fix: Every rule needs `inputs:` to declare which node it validates. Add `inputs: [ref: source.my_source]` or `inputs: [ref: transform.my_transform]`.

    **3. `severity: error` stops the entire pipeline**

    - Symptom: Downstream nodes don't execute
    - Fix: This is by design. Use `severity: warn` if you want the pipeline to continue despite violations. Reserve `error` for hard constraints.

    **4. Quoting the `null` type**

    - Symptom: YAML parse error on `type: null`
    - Fix: In YAML, unquoted `null` is interpreted as a null value. Always quote it: `type: "null"`.

---

## Summary

In this chapter, you learned:

- [x] **Rule Nodes** — Validate data quality with YAML rule definitions
- [x] **Rule Types** — `null` checks for missing values, `range` checks numeric bounds
- [x] **SQL Assertions** — Custom SQL queries for arbitrary validation logic
- [x] **Severity Levels** — `error` stops the pipeline, `warn` continues with a warning
- [x] **Source Validation** — Rules can check source data, not just transforms
- [x] **Pipeline Integration** — Rules execute as part of `seeknal run` in DAG order

**Supported Rule Types:**

| Type | Purpose | Key Params |
|------|---------|------------|
| `null` | Check for null/missing values | `columns:`, `max_null_percentage:` |
| `range` | Check numeric bounds | `column:`, `min_val:`, `max_val:` |
| `uniqueness` | Check for duplicates | `columns:`, `max_duplicate_percentage:` |
| `freshness` | Check timestamp recency | `column:`, `max_age:` (e.g., "24h", "7d") |
| `sql_assertion` | Custom SQL (returns bad rows) | `sql:` (query), multiple `inputs:` supported |

**Rule YAML Structure:**

```yaml
# Single check
kind: rule
name: my_rule
description: "What this rule checks"
inputs:
  - ref: source.my_source     # or transform.my_transform
rule:
  type: range                  # null, range, uniqueness, freshness
  column: my_column            # single column (range, freshness)
  # columns: [col1, col2]     # multiple columns (null, uniqueness)
  params:
    min_val: 0                 # type-specific thresholds
    max_val: 100
params:
  severity: error              # error or warn
  error_message: "Description of violation"
```

```yaml
# Multiple checks in one rule
kind: rule
name: my_combined_rule
inputs:
  - ref: source.my_source
rule:
  - type: "null"
    columns: [col1, col2]
    params:
      max_null_percentage: 0.0
  - type: range
    column: col1
    params:
      min_val: 0
      max_val: 100
params:
  severity: error
```

```yaml
# SQL assertion (dbt-style: returns bad rows)
kind: rule
name: my_sql_check
inputs:
  - ref: source.my_source
  - ref: source.my_other_source    # optional: cross-table checks
rule:
  type: sql_assertion
  sql: |
    SELECT id FROM input_data
    WHERE some_condition
params:
  severity: error
```

**Key Commands:**
```bash
seeknal draft rule <name>            # Generate rule template
seeknal dry-run <draft_file>.yml     # Preview validation
seeknal apply <draft_file>.yml       # Apply to project
seeknal plan                         # View DAG (rules appear as leaf nodes)
seeknal run                          # Execute pipeline with rule checks
```

---

## What's Next?

In **[Chapter 4: Lineage & Inspection](4-lineage.md)**, you'll visualize your pipeline's data flow and learn to inspect intermediate outputs for debugging.

---

## See Also

- **[CLI Reference](../../reference/cli.md)** — All commands and flags
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Source, transform, and rule schemas
