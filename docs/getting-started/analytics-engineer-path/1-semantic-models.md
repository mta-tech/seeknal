# Chapter 1: Build a Semantic Model

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** YAML

Learn to create a semantic model that provides business-friendly views of your data, enabling self-service analytics.

---

## What You'll Build

A semantic model for e-commerce analytics:

```
orders_cleaned (transform) → Semantic Model YAML → seeknal query
                                  ├── Entities (order_id, customer_id)
                                  ├── Dimensions (order_date, status)
                                  └── Measures (total_revenue, order_count, avg_order_value)
```

**After this chapter, you'll have:**
- A semantic model YAML defining entities, dimensions, and measures
- Queryable metrics via `seeknal query`
- Foundation for business metrics in Chapter 2

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [DE Path Chapter 1](../data-engineer-path/1-elt-pipeline.md) — You need the `orders_cleaned` transform
- [ ] [AE Path Overview](index.md) — Introduction to this path
- [ ] Comfortable with SQL aggregations

---

## Part 1: Verify Your Data Foundation (5 minutes)

### Check Your Pipeline

You should already have a working e-commerce pipeline from the DE path. Verify it:

```bash
seeknal repl
```

```sql
SELECT * FROM orders_cleaned LIMIT 5;
```

**Checkpoint:** You should see cleaned order data with columns: `order_id`, `customer_id`, `order_date`, `status`, `revenue`, `items`, `quality_flag`, `processed_at`.

!!! tip "Don't have the pipeline yet?"
    Complete [DE Path Chapter 1](../data-engineer-path/1-elt-pipeline.md) first. It takes 25 minutes and sets up the e-commerce data you'll use throughout this path.

### Understand the Data

Run a quick summary in the REPL:

```sql
SELECT
  COUNT(*) as total_orders,
  COUNT(DISTINCT customer_id) as unique_customers,
  SUM(revenue) as total_revenue,
  AVG(revenue) as avg_revenue
FROM orders_cleaned
WHERE quality_flag = 0;
```

**Expected output:**
```
+----------------+--------------------+-----------------+--------------+
|   total_orders |   unique_customers |   total_revenue |   avg_revenue |
|----------------+--------------------+-----------------+--------------|
|              9 |                  7 |          826.17 |       91.7967|
+----------------+--------------------+-----------------+--------------+
```

This is the data your semantic model will describe.

---

## Part 2: Define the Semantic Model (12 minutes)

### What Is a Semantic Model?

A semantic model maps your data to business concepts:

| Traditional SQL | Semantic Model |
|-----------------|----------------|
| `SELECT SUM(revenue) FROM orders_cleaned` | `total_revenue` measure |
| `WHERE status = 'COMPLETED'` | `status` dimension |
| `GROUP BY DATE(order_date)` | `order_date` time dimension |

Benefits:
- **Business language** — stakeholders query `total_revenue`, not raw SQL
- **Governed** — one definition, consistent everywhere
- **Reusable** — same model feeds metrics, queries, and BI tools

### Generate the Semantic Model Template

```bash
seeknal draft semantic-model orders
```

**Expected output:**
```
ℹ Using package templates
✓ Draft file created: draft_semantic_model_orders.yml
```

### Edit the Draft YAML

Open `draft_semantic_model_orders.yml` and replace its contents with:

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
```

### Validate and Apply

```bash
seeknal dry-run draft_semantic_model_orders.yml
seeknal apply draft_semantic_model_orders.yml
```

**Expected output:**
```
ℹ Validating YAML...
✓ YAML syntax valid
✓ Schema validation passed
✓ Dependency check passed
ℹ Moving file...
ℹ   TO: ./seeknal/semantic_models/orders.yml
✓ Applied successfully
```

!!! info "YAML Format Reference"
    - **kind**: Must be `semantic_model`
    - **model**: References a Seeknal node using `ref('kind.name')` format
    - **entities**: Columns that identify business objects (`primary`, `foreign`, `unique`)
    - **dimensions**: Attributes for filtering/grouping (`time`, `string`, `number`, `boolean`)
    - **measures**: Aggregatable expressions (`sum`, `count`, `average`, `min`, `max`, `count_distinct`)
    - **metrics**: Advanced metrics (`ratio`, `cumulative`, `derived`) — see [Chapter 2](2-business-metrics.md)
    - **joins**: Relationships to other semantic models (`one_to_one`, `one_to_many`, `many_to_one`)

### Understand Each Component

**Entities** identify business objects in your data:

```yaml
entities:
  - name: order_id      # Primary key
    type: primary
  - name: customer_id   # Foreign key (links to customers)
    type: foreign
```

**Dimensions** are what you filter and group by:

```yaml
dimensions:
  - name: order_date
    type: time           # Enables time-based queries (day, week, month)
    time_granularity: day
  - name: status
    type: string         # Text dimension for GROUP BY
  - name: revenue
    type: number         # Numeric dimension
```

**Measures** are what you aggregate:

```yaml
measures:
  - name: total_revenue
    expr: revenue        # Column to aggregate
    agg: sum             # Aggregation function

  - name: completed_revenue
    expr: revenue
    agg: sum
    filters:             # Measure-level filters (Cube.js-style)
      - sql: "status = 'COMPLETED'"
```

**Joins** connect semantic models (for multi-model queries):

```yaml
joins:
  - name: customers              # Target model name
    relationship: many_to_one    # one_to_one, one_to_many, many_to_one
    sql: "{CUBE}.customer_id = {customers}.customer_id"
```

---

## Part 3: Query the Semantic Layer (8 minutes)

### Query Metrics by Dimension

`seeknal query` can query measures directly from your semantic model — no separate metric files needed:

```bash
seeknal query --metrics total_revenue,order_count --dimensions status
```

**Expected output:**
```
status       total_revenue    order_count
---------  ---------------  -------------
PENDING               0                 2
COMPLETED          1396.17             10
ℹ 
2 rows returned
```

### Show Generated SQL

Use the `--compile` flag to see what SQL is generated:

```bash
seeknal query --metrics total_revenue --dimensions order_date --compile
```

**Expected output:**
```sql
SELECT
  order_date,
  SUM(revenue) AS total_revenue
FROM orders_cleaned
GROUP BY order_date
ORDER BY order_date
```

!!! info "How It Works"
    `seeknal query` reads your metric YAMLs and semantic model, resolves the `model` reference to the underlying transform, compiles measures into SQL aggregations, applies dimension grouping, and executes against DuckDB.

### Verify in REPL

Open the REPL and run the equivalent SQL manually:

```bash
seeknal repl
```

```sql
-- This is what seeknal query generates under the hood
SELECT
  status,
  SUM(revenue) AS total_revenue,
  COUNT(*) AS order_count
FROM orders_cleaned
GROUP BY status;
```

**Checkpoint:** The REPL results should match the `seeknal query` output exactly.

### Add a Filter

```bash
seeknal query --metrics total_revenue --dimensions order_date --filter "status = 'COMPLETED'"
```

**Expected output:**
```
order_date      total_revenue
------------  ---------------
2026-01-15              89.5
2026-01-16             250
2026-01-17              75.25
2026-01-18             365.99
2026-01-19             349.94
2026-01-20             265.49
ℹ 
6 rows returned
```

!!! success "Congratulations!"
    You've built a semantic model that turns raw SQL into business-friendly queries. Stakeholders can now query `total_revenue` by `status` or `order_date` without writing SQL.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. "No semantic models found"**

    - Symptom: `seeknal query` says no semantic models found
    - Fix: Ensure your YAML is in `seeknal/semantic_models/` directory and has `kind: semantic_model`

    **2. Missing `model` reference**

    - Symptom: Validation error about missing model
    - Fix: The `model` field must reference an existing node: `ref('transform.orders_cleaned')`

    **3. Invalid aggregation type**

    - Symptom: Error parsing measure
    - Fix: Use one of: `sum`, `count`, `count_distinct`, `average`, `min`, `max`

    **4. Dimension not found in query**

    - Symptom: Column not found error
    - Fix: Ensure the dimension `expr` matches an actual column in your transform output

---

## Summary

In this chapter, you learned:

- [x] **Semantic Models** — Define business-friendly data views in YAML
- [x] **Entities** — Identify primary and foreign keys
- [x] **Dimensions** — Create time, string, number, and boolean groupings
- [x] **Measures** — Define aggregatable metrics with optional filters
- [x] **Joins** — Connect semantic models with relationship types
- [x] **seeknal query** — Query the semantic layer from the CLI (no separate metric files needed)

**Key Commands:**
```bash
seeknal query --metrics <names> --dimensions <names>   # Query metrics
seeknal query --metrics <names> --compile              # Show generated SQL
seeknal query --metrics <names> --filter "<expr>"      # Filter results
seeknal repl                                           # Interactive verification
```

---

## What's Next?

[Chapter 2: Define Business Metrics →](2-business-metrics.md)

Create reusable business metrics — simple KPIs, ratio metrics, cumulative totals, and derived metrics that compose from simpler ones.

---

## See Also

- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Semantic model field reference
- **[CLI Reference](../../reference/cli.md)** — All `seeknal query` flags
