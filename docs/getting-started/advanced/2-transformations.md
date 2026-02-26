# Chapter 2: Transformations

> **Duration:** 20 minutes | **Difficulty:** Beginner | **Format:** YAML & CLI

Learn to clean, join, and aggregate data using SQL transforms with Seeknal's `ref()` syntax.

---

## What You'll Build

Three transforms that process your source data into analytics-ready tables:

```
source.sales_events ──→ transform.events_cleaned  (filter & deduplicate)
                                    │
source.products ────────────────────┤
                                    ├──→ transform.sales_enriched  (JOIN)
                                    │
                                    └──→ transform.sales_summary   (aggregation)
```

**After this chapter, you'll have:**
- A cleaned events table with nulls and duplicates removed
- An enriched table joining events with product details
- A summary table with revenue by category
- A full pipeline DAG you can run end-to-end

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: File Sources](1-file-sources.md) — Three sources loaded (products, sales_events, sales_snapshot)
- [ ] Basic SQL knowledge (JOIN, GROUP BY, window functions)

---

## Part 1: Single-Input Transform (7 minutes)

### Understanding Transforms

Transforms take data from upstream nodes (sources or other transforms) and produce new tables using SQL. Two key concepts:

- **`inputs:`** — Declares which nodes this transform depends on (DAG edges)
- **`ref()`** — References an input node's table in your SQL

### Clean the Events Data

The sales events have nulls and duplicates. Let's fix that:

```bash
seeknal draft transform events_cleaned
```

Edit `draft_transform_events_cleaned.yml`:

```yaml
kind: transform
name: events_cleaned
description: "Clean sales events: remove nulls and deduplicate"
inputs:
  - ref: source.sales_events
transform: |
  SELECT
    event_id,
    product_id,
    quantity,
    sale_date,
    region
  FROM ref('source.sales_events')
  WHERE quantity IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY sale_date DESC) = 1
```

!!! info "What This SQL Does"
    1. `WHERE quantity IS NOT NULL` — Removes the row with null quantity (EVT-003)
    2. `QUALIFY ROW_NUMBER()` — Keeps only the latest occurrence of each `event_id`, removing the duplicate EVT-001

### Validate and Apply

```bash
seeknal dry-run draft_transform_events_cleaned.yml
seeknal apply draft_transform_events_cleaned.yml
```

**Checkpoint:** The dry-run preview should show 5 rows (7 original - 1 null - 1 duplicate). EVT-003 (null quantity) and the duplicate EVT-001 from Jan 10 are removed.

!!! tip "REPL requires pipeline execution"
    Tables become queryable in `seeknal repl` only after `seeknal run` executes the pipeline. We'll run the full pipeline in Part 3 and explore all results there.

---

## Part 2: Multi-Input Transform — JOIN (8 minutes)

### Enrich Events with Product Details

Now let's join the cleaned events with the product catalog to add product names, categories, and prices:

```bash
seeknal draft transform sales_enriched
```

Edit `draft_transform_sales_enriched.yml`:

```yaml
kind: transform
name: sales_enriched
description: "Enrich sales events with product details"
inputs:
  - ref: transform.events_cleaned
  - ref: source.products
transform: |
  SELECT
    e.event_id,
    e.product_id,
    p.name AS product_name,
    p.category,
    e.quantity,
    p.price,
    e.quantity * p.price AS total_amount,
    e.sale_date,
    e.region
  FROM ref('transform.events_cleaned') e
  LEFT JOIN ref('source.products') p ON e.product_id = p.product_id
```

!!! info "Why LEFT JOIN?"
    We use LEFT JOIN because some events reference `PRD-999` which doesn't exist in the products table. A LEFT JOIN keeps the event row with NULL product details, rather than silently dropping it. This makes orphan references visible for investigation.

### Apply

```bash
seeknal dry-run draft_transform_sales_enriched.yml
seeknal apply draft_transform_sales_enriched.yml
```

**Checkpoint:** The dry-run preview should show 5 rows. The EVT-004 row (PRD-999) has NULL for product_name, category, price, and total_amount due to the LEFT JOIN.

---

## Part 3: Aggregation Transform (5 minutes)

### Summarize Revenue by Category

Create a summary table aggregating sales by product category:

```bash
seeknal draft transform sales_summary
```

Edit `draft_transform_sales_summary.yml`:

```yaml
kind: transform
name: sales_summary
description: "Revenue summary by product category"
inputs:
  - ref: transform.sales_enriched
transform: |
  SELECT
    category,
    COUNT(*) AS order_count,
    SUM(quantity) AS total_units,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
  FROM ref('transform.sales_enriched')
  WHERE category IS NOT NULL
  GROUP BY category
  ORDER BY total_revenue DESC
```

```bash
seeknal dry-run draft_transform_sales_summary.yml
seeknal apply draft_transform_sales_summary.yml
```

### Run the Full Pipeline

Now let's see the full DAG and run everything:

```bash
# View the execution plan
seeknal plan
```

You should see a DAG with 6 nodes:
```
source.products
source.sales_events
source.sales_snapshot
transform.events_cleaned      (depends on: source.sales_events)
transform.sales_enriched      (depends on: transform.events_cleaned, source.products)
transform.sales_summary       (depends on: transform.sales_enriched)
```

```bash
# Execute the full pipeline
seeknal run
```

### Verify Results

```bash
seeknal repl
```

```sql
-- Revenue by category
SELECT * FROM transform_sales_summary;
```

**Checkpoint:** You should see categories like Electronics, Kitchen, Sports, Apparel with their aggregated metrics. The orphan PRD-999 row is excluded by `WHERE category IS NOT NULL`.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. "Referenced node not found"**

    - Symptom: `dry-run` fails with unresolved reference
    - Fix: Every `ref()` in your SQL must have a matching entry in `inputs:`. If your SQL uses `ref('source.products')`, then `inputs:` must include `- ref: source.products`.

    **2. Mixing `ref()` and `input_0` accidentally**

    - Symptom: Confusing which table is which
    - Fix: Both `ref('source.products')` and `input_0` are valid ways to reference inputs. We recommend `ref()` with named references for clarity. If you use positional references (`input_0`, `input_1`), the order matches the `inputs:` list.

    **3. JOIN produces more rows than expected**

    - Symptom: Row count multiplied after JOIN
    - Fix: Ensure your join keys are unique on at least one side. Deduplicate upstream (as we did in `events_cleaned`) before joining.

    **4. Transform SQL syntax errors**

    - Symptom: `seeknal run` fails on a transform node
    - Fix: Test your SQL in `seeknal repl` first. Replace `ref()` calls with the actual table names to debug.

---

## Summary

In this chapter, you learned:

- [x] **Single-Input Transform** — Filter and deduplicate with `WHERE` and `QUALIFY`
- [x] **Multi-Input Transform** — JOIN data from multiple sources using `ref()`
- [x] **Aggregation Transform** — GROUP BY for summary tables
- [x] **`ref()` Syntax** — `ref('source.name')` or `ref('transform.name')` to reference inputs
- [x] **Pipeline Execution** — `seeknal plan` to see the DAG, `seeknal run` to execute

**Transform Patterns:**

| Pattern | Use Case | Example |
|---------|----------|---------|
| Filter | Remove invalid rows | `WHERE quantity IS NOT NULL` |
| Deduplicate | Keep latest per key | `QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) = 1` |
| JOIN | Combine datasets | `FROM ref('a') JOIN ref('b') ON ...` |
| Aggregate | Summarize | `GROUP BY category` |

**Key Commands:**
```bash
seeknal draft transform <name>       # Generate transform template
seeknal dry-run <draft_file>.yml     # Preview validation
seeknal apply <draft_file>.yml       # Apply to project
seeknal plan                         # View execution DAG
seeknal run                          # Execute full pipeline
```

---

## What's Next?

In **[Chapter 3: Data Rules](3-data-rules.md)**, you'll add automated data quality checks that validate your pipeline outputs.
