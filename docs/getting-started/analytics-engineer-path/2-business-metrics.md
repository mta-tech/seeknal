# Chapter 2: Define Business Metrics

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** YAML

Learn to define advanced business metrics — ratio, cumulative, and derived KPIs — all inside your semantic model YAML. Export results to files and document downstream consumers with exposures.

---

## What You'll Build

Advanced metrics layered on your semantic model:

```
Semantic Model (orders)
  ├── measures
  │     ├── total_revenue          ← direct query via seeknal query
  │     ├── completed_revenue      ← filtered measure
  │     └── order_count
  │
  └── metrics (inline)
        ├── avg_order_value        ← ratio (revenue / orders)
        ├── cumulative_revenue     ← running total
        └── revenue_per_customer   ← derived (composes metrics)
```

**After this chapter, you'll have:**
- Filtered measures for scoped aggregations
- Ratio metrics computing numerator/denominator
- Cumulative metrics for running totals
- Derived metrics composing from other metrics
- File export of query results (CSV, JSON, Parquet)
- Exposures documenting downstream consumers
- All defined in a single semantic model file — no separate metric YAMLs needed

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Semantic Models](1-semantic-models.md) — Semantic model with measures defined
- [ ] Comfortable with SQL aggregations and window functions

---

## Part 1: Filtered Measures (5 minutes)

### What Changed from Chapter 1

In Chapter 1, you defined basic measures like `total_revenue`. But what about "revenue from completed orders only"? In traditional SQL, you'd write:

```sql
SELECT SUM(CASE WHEN status = 'COMPLETED' THEN revenue END) AS completed_revenue
```

With seeknal's semantic model, you define this as a **filtered measure** — no separate metric file needed:

### Add Filtered Measures to Your Semantic Model

Open `seeknal/semantic_models/orders.yml` and verify your measures include the filtered one from Chapter 1:

```yaml
measures:
  - name: total_revenue
    expr: revenue
    agg: sum
    description: "Total revenue from all orders"

  - name: completed_revenue
    expr: revenue
    agg: sum
    description: "Revenue from completed orders only"
    filters:
      - sql: "status = 'COMPLETED'"

  - name: order_count
    expr: "1"
    agg: count
    description: "Number of orders"

  - name: completed_orders
    expr: "1"
    agg: count
    description: "Number of completed orders"
    filters:
      - sql: "status = 'COMPLETED'"

  - name: avg_order_value
    expr: revenue
    agg: average
    description: "Average order value"
```

### Query Filtered vs Unfiltered

```bash
seeknal query --metrics total_revenue,completed_revenue --dimensions status
```

**Expected output:**
```
+------------+-----------------+---------------------+
| status     |   total_revenue |   completed_revenue |
|------------+-----------------+---------------------|
| COMPLETED  |          880.68 |              880.68 |
| PENDING    |            0.00 |                     |
+------------+-----------------+---------------------+
```

Notice how `completed_revenue` only counts COMPLETED rows — the filter is applied at the measure level using `CASE WHEN`, so it works correctly even when grouped by dimensions.

### How It Works Under the Hood

```bash
seeknal query --metrics total_revenue,completed_revenue --compile
```

**Generated SQL:**
```sql
SELECT
  SUM(revenue) AS total_revenue,
  SUM(CASE WHEN status = 'COMPLETED' THEN revenue END) AS completed_revenue
FROM transform_orders_cleaned
```

!!! info "Cube.js-Style Measure Filters"
    This follows the Cube.js pattern where filters belong to the measure definition, not to the query. This means the same query can mix filtered and unfiltered measures — each measure applies its own filters independently.

**Checkpoint:** You should see filtered measures working alongside unfiltered ones.

---

## Part 2: Ratio Metrics (7 minutes)

### When You Need Advanced Metrics

Most metrics are just measures (defined in your semantic model's `measures:` section). But some metric **types** require combining measures in ways that go beyond simple aggregation:

| Pattern | Where to Define | Example |
|---------|----------------|---------|
| Simple aggregation | `measures:` | `total_revenue`, `order_count` |
| Filtered aggregation | `measures:` + `filters:` | `completed_revenue` |
| Ratio (A / B) | `metrics:` | `avg_order_value = revenue / orders` |
| Running total | `metrics:` | `cumulative_revenue` |
| Compose metrics | `metrics:` | `revenue_per_customer = revenue / customers` |

All of these live in the **same semantic model YAML** — no separate metric files needed.

### Add Ratio Metric to Your Semantic Model

Open `seeknal/semantic_models/orders.yml` and add a `metrics:` section after your measures:

```yaml
metrics:
  - name: avg_order_value_ratio
    type: ratio
    description: "Average order value (revenue / orders)"
    numerator: total_revenue
    denominator: order_count
```

!!! info "How Ratio Metrics Work"
    The `numerator` and `denominator` reference **measure names** from the same semantic model. Seeknal compiles this into `SUM(revenue) / NULLIF(COUNT(1), 0)` — the `NULLIF` prevents division by zero.

### Query the Ratio Metric

```bash
seeknal query --metrics avg_order_value_ratio --dimensions order_date
```

**Expected output:**
```
order_date      avg_order_value_ratio
------------  -----------------------
2026-01-15                     89.5
2026-01-16                    125
2026-01-17                     37.625
2026-01-18                    182.995
2026-01-19                    174.97
2026-01-20                    132.745
2026-01-21                      0
ℹ 
7 rows returned
```

### Verify in REPL

```bash
seeknal repl
```

```sql
SELECT
  order_date,
  SUM(revenue) / NULLIF(COUNT(*), 0) AS avg_order_value_ratio
FROM transform_orders_cleaned
GROUP BY order_date
ORDER BY order_date;
```

**Checkpoint:** REPL results should match the `seeknal query` output.

---

## Part 3: Cumulative and Derived Metrics (8 minutes)

### Cumulative Metric — Running Revenue

A cumulative metric computes running totals over time. Add it to your `metrics:` section:

```yaml
metrics:
  - name: avg_order_value_ratio
    type: ratio
    description: "Average order value (revenue / orders)"
    numerator: total_revenue
    denominator: order_count

  - name: cumulative_revenue
    type: cumulative
    description: "Running total of revenue"
    measure: total_revenue
    grain_to_date: month
```

!!! info "grain_to_date"
    `grain_to_date: month` resets the running total at the start of each month. Options: `month`, `quarter`, `year`. Omit for an all-time running total.

### Query Cumulative Metric

```bash
seeknal query --metrics cumulative_revenue --dimensions order_date
```

**Expected output:**
```
+--------------+----------------------+
| order_date   |   cumulative_revenue |
|--------------+----------------------|
| 2026-01-15   |               239.49 |
| 2026-01-16   |               489.49 |
| 2026-01-17   |               564.74 |
| 2026-01-18   |               930.73 |
| 2026-01-19   |              1280.67 |
+--------------+----------------------+
```

### Derived Metric — Revenue per Customer

A derived metric composes from other metrics using an expression. Add it to your `metrics:` section:

```yaml
  - name: revenue_per_customer
    type: derived
    description: "Average revenue per unique customer"
    expr: "total_revenue / unique_customers"
    inputs:
      - metric: total_revenue
      - metric: unique_customers
```

!!! tip "Derived Metric Expressions"
    The `expr` field uses metric names as variables. Each `inputs` entry references a metric or measure name. The expression supports basic arithmetic: `+`, `-`, `*`, `/`.

### Complete Semantic Model with Inline Metrics

Here is the full `seeknal/semantic_models/orders.yml` after this chapter:

```yaml
kind: semantic_model
name: orders
description: "E-commerce order analytics model"
model: "ref('transform.orders_cleaned')"
default_time_dimension: order_date

entities:
  - name: order_id
    type: primary
  - name: customer_id
    type: foreign

dimensions:
  - name: order_date
    type: time
    expr: order_date
    time_granularity: day
    description: "Date the order was placed"

  - name: status
    type: string
    expr: status
    description: "Order status (COMPLETED, PENDING, etc.)"

  - name: revenue
    type: number
    expr: revenue
    description: "Order revenue amount"

measures:
  - name: total_revenue
    expr: revenue
    agg: sum
    description: "Total revenue from orders"

  - name: completed_revenue
    expr: revenue
    agg: sum
    description: "Revenue from completed orders only"
    filters:
      - sql: "status = 'COMPLETED'"

  - name: order_count
    expr: "1"
    agg: count
    description: "Number of orders"

  - name: avg_order_value
    expr: revenue
    agg: average
    description: "Average order value"

metrics:
  - name: avg_order_value_ratio
    type: ratio
    description: "Average order value (revenue / orders)"
    numerator: total_revenue
    denominator: order_count

  - name: cumulative_revenue
    type: cumulative
    description: "Running total of revenue"
    measure: total_revenue
    grain_to_date: month

  - name: revenue_per_customer
    type: derived
    description: "Average revenue per unique customer"
    expr: "total_revenue / unique_customers"
    inputs:
      - metric: total_revenue
      - metric: unique_customers
```

### Query All Metrics Together

You can mix measures and inline metrics in one query:

```bash
seeknal query --metrics total_revenue,completed_revenue,order_count,avg_order_value_ratio --dimensions status
```

**Expected output:**
```
+------------+-----------------+---------------------+--------------+------------------------+
| status     |   total_revenue |   completed_revenue |   order_count |   avg_order_value_ratio |
|------------+-----------------+---------------------+--------------+------------------------|
| COMPLETED  |          880.68 |              880.68 |            6 |                 146.78 |
| PENDING    |            0.00 |                     |            1 |                   0.00 |
+------------+-----------------+---------------------+--------------+------------------------+
```

### Show Generated SQL

```bash
seeknal query --metrics total_revenue,avg_order_value_ratio --compile
```

This shows the SQL that Seeknal generates from your metric definitions — useful for debugging and understanding the compilation.

!!! success "Congratulations!"
    You've built a complete metrics layer — all in a single semantic model file. Simple measures, filtered measures, and advanced metrics (ratio, cumulative, derived) coexist in one YAML.

---

## Part 4: Export Results & Exposures (5 minutes)

### Export Query Results to File

Use `--output` / `-o` to export query results directly to a file. The format is inferred from the file extension:

```bash
# Export to CSV
seeknal query --metrics total_revenue,order_count --dimensions status -o results.csv

# Export to JSON
seeknal query --metrics total_revenue --dimensions order_date -o metrics.json

# Export to Parquet (for downstream pipelines)
seeknal query --metrics total_revenue,completed_revenue --dimensions order_date -o revenue.parquet
```

**Expected output:**
```
✓ Exported 6 rows to revenue.parquet
```

!!! tip "When to Use File Export"
    - **CSV**: Sharing with stakeholders, loading into spreadsheets
    - **JSON**: Feeding into APIs or web dashboards
    - **Parquet**: Downstream data pipelines, efficient columnar storage

### Define Exposures — Where Your Metrics Go

An **exposure** defines a downstream consumer of your data — a file export, API endpoint, notification, or StarRocks materialized view. Seeknal can execute exposures to deliver data to these destinations.

```
Semantic Model (orders)
  └── Exposures
        ├── file         → CSV/Parquet/JSON export
        ├── api          → POST data to REST endpoint
        ├── notification → Slack or webhook alert
        └── starrocks_mv → StarRocks materialized view
```

### Create a File Export Exposure

```bash
seeknal draft exposure revenue_export
```

Open `draft_exposure_revenue_export.yml` and edit:

```yaml
kind: exposure
name: revenue_export
description: "Daily revenue CSV export"
owner: "analytics-team@example.com"
type: file
depends_on:
  - ref: transform.orders_cleaned
tags:
  - finance
  - daily
params:
  format: csv
  path: "target/exports/revenue_export.csv"
```

Apply it:

```bash
seeknal dry-run draft_exposure_revenue_export.yml
seeknal apply draft_exposure_revenue_export.yml
```

### Exposure Types

| Type | Purpose | Status |
|------|---------|--------|
| `file` | Export to CSV, Parquet, JSON, or JSONL | Fully supported |
| `api` | POST data to a REST endpoint | Fully supported |
| `notification` | Send alerts via webhook or Slack | Supported (webhook, Slack) |
| `starrocks_materialized_view` | Deploy as StarRocks MV | Fully supported |

### Notification Exposure Example

Send a Slack alert when data is ready:

```yaml
kind: exposure
name: revenue_alert
description: "Slack alert when revenue data refreshes"
owner: "analytics-team@example.com"
type: notification
depends_on:
  - ref: transform.orders_cleaned
params:
  channel: slack
  url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
  message: "Revenue data has been refreshed"
```

!!! info "Exposures vs `--output`"
    The `--output` flag is for **ad-hoc** file exports from the CLI. Exposures are **declared pipeline nodes** — they run as part of `seeknal apply` and are tracked in lineage. When you change a semantic model, `seeknal` can tell you which exports, APIs, and alerts are affected.

**Checkpoint:** You should have at least one exposure applied in `seeknal/exposures/`.

---

## Metrics vs Measures — When to Use Which

| I want to... | Section | Example |
|-------------|---------|---------|
| Sum/count/avg a column | `measures:` | `total_revenue`, `order_count` |
| Filter an aggregation | `measures:` + `filters:` | `completed_revenue` |
| Divide measure A by measure B | `metrics:` + `type: ratio` | `avg_order_value_ratio` |
| Running total over time | `metrics:` + `type: cumulative` | `cumulative_revenue` |
| Combine multiple metrics | `metrics:` + `type: derived` | `revenue_per_customer` |

**Rule of thumb:** If it's a single aggregation (with or without a filter), define it as a measure. If it combines multiple measures or needs window functions, define it as an inline metric.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. "Metric not found"**

    - Symptom: `seeknal query` can't find your metric
    - Fix: Ensure the metric is defined in the `metrics:` section of your semantic model, or as a measure in `measures:`

    **2. Ratio metric returns NULL**

    - Symptom: `avg_order_value_ratio` shows NULL
    - Fix: Check that `numerator` and `denominator` reference valid measure names from the same semantic model

    **3. Derived metric circular reference**

    - Symptom: Error about circular dependency
    - Fix: Derived metrics can only reference other metrics, not themselves

    **4. Filter syntax error**

    - Symptom: SQL error in metric query
    - Fix: The `filter` field must be valid SQL WHERE clause syntax (e.g., `"status = 'COMPLETED'"`)

    **5. Measure filter vs query filter**

    - Symptom: Unexpected results mixing `--filter` with measure filters
    - Clarification: Measure `filters:` apply per-measure (via CASE WHEN). Query `--filter` applies to the entire WHERE clause. They work independently.

---

## Summary

In this chapter, you learned:

- [x] **Filtered Measures** — Scoped aggregations using `filters:` on measures (no extra files)
- [x] **Ratio Metrics** — Divide one measure by another with safe division
- [x] **Cumulative Metrics** — Running totals with `grain_to_date` resets
- [x] **Derived Metrics** — Compose metrics from other metrics with expressions
- [x] **Inline Metrics** — All metric types defined in the semantic model YAML (no separate files)
- [x] **File Export** — Export query results to CSV, JSON, or Parquet with `--output`
- [x] **Exposures** — Document downstream consumers (dashboards, APIs, file exports, notifications)
- [x] **Measures vs Metrics** — When to use each pattern

**Key Commands:**
```bash
seeknal query --metrics <names>                         # Query metrics
seeknal query --metrics <names> --dimensions <names>    # Add dimensions
seeknal query --metrics <names> --filter "<expr>"       # Filter results
seeknal query --metrics <names> --compile               # Show SQL
seeknal query --metrics <names> --format csv            # CSV to stdout
seeknal query --metrics <names> -o results.csv          # Export to file
seeknal query --metrics <names> -o data.parquet         # Export to Parquet
seeknal draft exposure <name>                           # Create exposure
```

---

## What's Next?

[Chapter 3: Deploy to StarRocks →](3-self-serve-analytics.md)

Deploy your metrics as materialized views on StarRocks, enabling fast BI queries and self-serve analytics for stakeholders.

---

## See Also

- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Semantic model and metric YAML fields
- **[CLI Reference](../../reference/cli.md)** — All `seeknal query` flags
