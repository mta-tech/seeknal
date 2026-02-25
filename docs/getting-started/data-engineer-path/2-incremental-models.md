# Chapter 2: Add Incremental Models

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** YAML

Learn to extend your pipeline with multiple transforms, understand Seeknal's caching system, and handle growing datasets with merge logic and aggregation.

---

## What You'll Build

Extend the Chapter 1 pipeline with aggregation and merge transforms:

```
raw_orders (CSV) ──→ orders_cleaned ──┐
                                      ├──→ orders_merged ──→ daily_revenue
orders_updates (CSV) ─────────────────┘
```

**After this chapter, you'll have:**
- A multi-step DAG with source, transform, and aggregation layers
- Understanding of Seeknal's incremental caching system
- Merge logic that handles data corrections and new records
- An aggregation layer for daily revenue metrics

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Build ELT Pipeline](1-elt-pipeline.md) — Working pipeline with `raw_orders` and `orders_cleaned`
- [ ] Comfortable with SQL window functions (`ROW_NUMBER`, `PARTITION BY`)

---

## Part 1: Understand Incremental Caching (5 minutes)

### How Seeknal's Caching Works

When you run `seeknal run`, Seeknal fingerprints each node (SQL hash, input files, dependencies). On the next run, unchanged nodes are skipped automatically:

```bash
seeknal run
```

If nothing changed since Chapter 1, you'll see all nodes cached:

```
Seeknal Pipeline Execution
============================================================
  Project: ecommerce-pipeline
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 2 nodes, 1 edges
ℹ Loaded previous state from run: 20260223_155912
ℹ Detecting changes...
✓ No changes detected. Nothing to run.
```

This is incremental execution — only nodes with changes re-run.

### Trigger a Selective Re-run

Add new orders to `data/orders.csv` to see selective re-execution:

```bash
cat >> data/orders.csv << 'EOF'
ORD-010,CUST-106,2026-01-20 10:00:00,completed,175.50,3
ORD-011,CUST-100,2026-01-20 14:30:00,completed,89.99,2
ORD-012,CUST-107,2026-01-21 09:15:00,pending,0.00,1
EOF
```

```bash
seeknal run
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
  Project: ecommerce-pipeline
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 2 nodes, 1 edges
ℹ Loaded previous state from run: 20260223_160204
ℹ Detecting changes...
ℹ Nodes to run: 2

Execution
============================================================
1/2: raw_orders [RUNNING]
  SUCCESS in 0.04s
  Rows: 13
2/2: orders_cleaned [RUNNING]
ℹ Loaded Python node output as input_0
ℹ Resolved SQL for orders_cleaned
ℹ   Executing statement 1/1
ℹ Created view 'transform.orders_cleaned' for materialization
ℹ Wrote transform output to ./target/intermediate/transform_orders_cleaned.parquet
  SUCCESS in 0.13s
  Rows: 12
✓ State saved

Execution Summary
============================================================
  Total nodes:    2
  Executed:       2
  Duration:       0.17s
============================================================
```

Notice: `raw_orders` re-runs because the CSV file changed, and `orders_cleaned` re-runs because its upstream input changed. Other nodes stay cached.

### Verify in REPL

```bash
seeknal repl
```

```sql
-- Confirm new orders are included
SELECT order_id, customer_id, status, revenue
FROM orders_cleaned
ORDER BY order_id;
```

**Checkpoint:** You should see 12 unique orders (ORD-001 through ORD-012, with duplicate ORD-001 deduplicated).

---

## Part 2: Add a Daily Revenue Aggregation (8 minutes)

Build a summary layer on top of the cleaned orders for business metrics.

### Draft and Edit the Aggregation

```bash
seeknal draft transform daily_revenue
```

Edit `draft_transform_daily_revenue.yml`:

```yaml
kind: transform
name: daily_revenue
description: "Daily revenue summary from cleaned orders"

inputs:
  - ref: transform.orders_cleaned

transform: |
  SELECT
    order_date,
    COUNT(*) as total_orders,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_orders,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending_orders,
    COALESCE(SUM(revenue), 0) as total_revenue,
    ROUND(COALESCE(AVG(revenue), 0), 2) as avg_order_value,
    SUM(items) as total_items,
    COUNT(CASE WHEN quality_flag = 1 THEN 1 END) as flagged_orders

  FROM ref('transform.orders_cleaned')
  WHERE status != 'CANCELLED'
  GROUP BY order_date
  ORDER BY order_date
```

### Apply and Run

```bash
seeknal dry-run draft_transform_daily_revenue.yml
seeknal apply draft_transform_daily_revenue.yml
```

**Expected output:**
```
ℹ Validating YAML...
✓ YAML syntax valid
✓ Schema validation passed
✓ Dependency check passed
ℹ Executing preview (limit 10 rows)...
ℹ Loaded input_0 <- transform.orders_cleaned (from previous run)
+--------------+----------------+--------------------+------------------+-----------------+-------------------+---------------+------------------+
| order_date   |   total_orders |   completed_orders |   pending_orders |   total_revenue |   avg_order_value |   total_items |   flagged_orders |
|--------------+----------------+--------------------+------------------+-----------------+-------------------+---------------+------------------|
| 2026-01-15   |              1 |                  1 |                0 |           89.5  |             89.5  |             2 |                0 |
| 2026-01-16   |              2 |                  1 |                1 |          250    |            125    |             6 |                1 |
| 2026-01-17   |              2 |                  2 |                0 |           75.25 |             75.25 |             3 |                1 |
| 2026-01-18   |              2 |                  2 |                0 |          365.99 |            183    |             2 |                1 |
| 2026-01-19   |              2 |                  2 |                0 |          349.94 |            174.97 |             7 |                0 |
| 2026-01-20   |              2 |                  2 |                0 |          265.49 |            132.75 |             5 |                0 |
| 2026-01-21   |              1 |                  0 |                1 |            0    |              0    |             1 |                0 |
+--------------+----------------+--------------------+------------------+-----------------+-------------------+---------------+------------------+
✓ Preview completed in 0.0s
ℹ Run 'seeknal apply draft_transform_daily_revenue.yml' to apply
ℹ Validating...
✓ All checks passed
ℹ Moving file...
ℹ   FROM: draft_transform_daily_revenue.yml
ℹ   TO:  ./seeknal/transforms/daily_revenue.yml
ℹ Updating manifest...
✓ Manifest regenerated
ℹ 
ℹ Added:
ℹ   + transform.daily_revenue
ℹ     - depends_on: transform.orders_cleaned
✓ Applied successfully
```

Now generate the DAG and run:

```bash
seeknal plan
seeknal run
```

### Verify in REPL

```bash
seeknal repl
```

```sql
-- Daily revenue summary
SELECT * FROM daily_revenue;

-- Best performing day
SELECT order_date, total_revenue, total_orders
FROM daily_revenue
ORDER BY total_revenue DESC
LIMIT 3;

-- Days with quality issues
SELECT order_date, flagged_orders, total_orders
FROM daily_revenue
WHERE flagged_orders > 0;
```

**Checkpoint:** You should see 7 rows (one per day), with January 18th having the highest revenue from the ORD-007 ($320) order.

---

## Part 3: Handle Data Updates with Merge Logic (10 minutes)

In real-world pipelines, data arrives with corrections and late updates. Let's handle this with a merge transform.

### Create Updated Order Data

Create a second CSV with corrections and new orders:

```bash
cat > data/orders_updates.csv << 'EOF'
order_id,customer_id,order_date,status,revenue,items
ORD-004,CUST-100,2026-01-16 14:20:00,completed,55.00,1
ORD-005,CUST-102,2026-01-17 08:15:00,completed,25.00,2
ORD-013,CUST-108,2026-01-22 11:00:00,completed,299.99,5
ORD-014,CUST-109,2026-01-22 15:30:00,completed,42.50,1
EOF
```

!!! info "What's in the Updates?"
    - **ORD-004**: Status changed from `PENDING` → `completed`, revenue corrected to `$55.00`
    - **ORD-005**: Revenue corrected from `-$10.00` → `$25.00` (was flagged as quality issue)
    - **ORD-013, ORD-014**: Brand new orders

### Create a Source for Updates

```bash
seeknal draft source orders_updates
```

Edit `draft_source_orders_updates.yml`:

```yaml
kind: source
name: orders_updates
description: "Order corrections and new data"
source: csv
table: "data/orders_updates.csv"
columns:
  order_id: "Order identifier"
  customer_id: "Customer identifier"
  order_date: "Order timestamp"
  status: "Order status"
  revenue: "Order revenue in USD"
  items: "Number of items"
```

```bash
seeknal dry-run draft_source_orders_updates.yml
seeknal apply draft_source_orders_updates.yml
```

### Create a Merge Transform

```bash
seeknal draft transform orders_merged
```

Edit `draft_transform_orders_merged.yml`:

```yaml
kind: transform
name: orders_merged
description: "Merge original orders with updates, keeping latest version"

inputs:
  - ref: transform.orders_cleaned
  - ref: source.orders_updates

transform: |
  WITH combined AS (
    -- Original cleaned orders
    SELECT
      order_id, customer_id, order_date, status,
      revenue, items, quality_flag,
      'original' as source_batch
    FROM ref('transform.orders_cleaned')

    UNION ALL

    -- Updates and corrections (apply same cleaning)
    SELECT
      order_id,
      customer_id,
      DATE(order_date) as order_date,
      UPPER(TRIM(status)) as status,
      CASE WHEN revenue >= 0 THEN revenue ELSE NULL END as revenue,
      CASE WHEN items >= 0 THEN items ELSE 0 END as items,
      CASE
        WHEN revenue < 0 OR items < 0 THEN 1
        ELSE 0
      END as quality_flag,
      'update' as source_batch
    FROM ref('source.orders_updates')
  )

  SELECT
    order_id, customer_id, order_date, status,
    revenue, items, quality_flag, source_batch,
    CURRENT_TIMESTAMP as merged_at
  FROM combined
  -- Keep updates over originals, then latest date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY
      CASE WHEN source_batch = 'update' THEN 0 ELSE 1 END,
      order_date DESC
  ) = 1
```

### Apply and Run

```bash
seeknal dry-run draft_transform_orders_merged.yml
seeknal apply draft_transform_orders_merged.yml
seeknal plan
seeknal run
```

### Verify the Merge

```bash
seeknal repl
```

```sql
-- All merged orders
SELECT order_id, status, revenue, source_batch
FROM orders_merged
ORDER BY order_id;

-- Verify ORD-004 was updated (was PENDING $0, now COMPLETED $55)
SELECT order_id, status, revenue, source_batch
FROM orders_merged
WHERE order_id = 'ORD-004';

-- Verify ORD-005 revenue was corrected (was negative, now $25)
SELECT order_id, revenue, quality_flag, source_batch
FROM orders_merged
WHERE order_id = 'ORD-005';

-- Count by source batch
SELECT source_batch, COUNT(*) as cnt
FROM orders_merged
GROUP BY source_batch;
```

**Checkpoint:** You should see:

- **14 total orders** (12 original + 2 new, with 2 updated in place)
- **ORD-004**: `status = COMPLETED`, `revenue = 55.00`, `source_batch = update`
- **ORD-005**: `revenue = 25.00`, `quality_flag = 0`, `source_batch = update`
- **ORD-013, ORD-014**: New orders with `source_batch = update`

!!! success "Merge Complete!"
    Updates override originals via `QUALIFY ROW_NUMBER()` with batch priority ordering. The same pattern works for any correction workflow — just add new data and re-run.

### Update Daily Revenue to Use Merged Data

Now that `orders_merged` has the corrected data, `daily_revenue` should aggregate from it instead of `orders_cleaned`.

Open `seeknal/transforms/daily_revenue.yml` and update the input ref and the `FROM` clause:

```yaml
inputs:
  - ref: transform.orders_merged    # was: transform.orders_cleaned

transform: |
  SELECT
    order_date,
    COUNT(*) as total_orders,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_orders,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) as pending_orders,
    COALESCE(SUM(revenue), 0) as total_revenue,
    ROUND(COALESCE(AVG(revenue), 0), 2) as avg_order_value,
    SUM(items) as total_items,
    COUNT(CASE WHEN quality_flag = 1 THEN 1 END) as flagged_orders

  FROM ref('transform.orders_merged')
  WHERE status != 'CANCELLED'
  GROUP BY order_date
  ORDER BY order_date
```

Run to pick up the change:

```bash
seeknal run
```

Seeknal detects the SQL change in `daily_revenue` and re-runs only that node:

```
ℹ Detecting changes...
ℹ Nodes to run: 1

Execution
============================================================
1/1: daily_revenue [RUNNING]
  SUCCESS in 0.05s
```

Verify in the REPL that daily revenue now includes the corrected orders and new dates:

```bash
seeknal repl
```

```sql
SELECT * FROM daily_revenue ORDER BY order_date;
```

**Checkpoint:** You should now see **8 rows** (Jan 15–22), including Jan 22nd from the new orders (ORD-013, ORD-014).

---

## Part 4: Observe Incremental Behavior (2 minutes)

Now see caching in action with a larger DAG. Run the pipeline again without any changes:

```bash
seeknal run
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
  Project: ecommerce-pipeline
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 5 nodes, 5 edges
ℹ Loaded previous state from run: 20260223_150123
ℹ Detecting changes...
✓ No changes detected. Nothing to run.
```

All 5 nodes are cached. Now add a column to the aggregation SQL to trigger a selective re-run.

Open `seeknal/transforms/daily_revenue.yml` and add `max_order_value` after the `avg_order_value` line:

```yaml
    ROUND(COALESCE(AVG(revenue), 0), 2) as avg_order_value,
    MAX(revenue) as max_order_value,                         -- add this line
    SUM(items) as total_items,
```

Run the pipeline:

```bash
seeknal run
```

Only `daily_revenue` re-runs — sources and other transforms stay cached:

```
ℹ Detecting changes...
ℹ Nodes to run: 1

Execution
============================================================
1/1: daily_revenue [RUNNING]
  SUCCESS in 0.05s
```

This is how Seeknal keeps large pipelines fast — only the node whose SQL changed re-executes.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Duplicates After Merge**

    - Symptom: Same `order_id` appears multiple times
    - Fix: Ensure `QUALIFY ROW_NUMBER()` with proper `PARTITION BY` and `ORDER BY`

    **2. Column Mismatch in UNION**

    - Symptom: `UNION column count mismatch` error
    - Fix: Both sides of `UNION ALL` must have the same number and types of columns

    **3. Cache Not Invalidated**

    - Symptom: Old data still appears after updating a CSV
    - Fix: Seeknal detects file changes automatically — ensure you saved the file

    **4. Missing Dependencies**

    - Symptom: `Referenced node not found` error
    - Fix: Check that `inputs:` list includes every `ref()` used in the SQL

---

## Summary

In this chapter, you learned:

- [x] **Incremental Caching** — Seeknal fingerprints nodes and skips unchanged ones
- [x] **Aggregation** — Build daily metrics on top of cleaned data
- [x] **Merge Logic** — Combine datasets with update priority using `QUALIFY`
- [x] **Multi-step DAG** — Chain source → transform → aggregation layers
- [x] **Selective Re-execution** — Only modified nodes and their dependents re-run

**Your DAG now looks like:**
```
raw_orders (CSV) ──→ orders_cleaned ──┐
                                      ├──→ orders_merged ──→ daily_revenue
orders_updates (CSV) ─────────────────┘
```

**Key Commands:**
```bash
seeknal draft transform <name>     # Generate transform template
seeknal dry-run <file>             # Validate before applying
seeknal apply <file>               # Save node definition
seeknal plan                       # Regenerate DAG manifest
seeknal run                        # Execute (with caching)
seeknal repl                       # Inspect results interactively
```

---

## What's Next?

[Chapter 3: Deploy to Production Environments →](3-production-environments.md)

Learn to use environments for safe testing and production deployment, with isolated namespaces and environment-specific profiles.

---

## See Also

- **[Virtual Environments](../../concepts/virtual-environments.md)** — Isolate development and production
- **[Change Categorization](../../concepts/change-categorization.md)** — Understanding breaking vs non-breaking changes
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — All supported source types and fields
