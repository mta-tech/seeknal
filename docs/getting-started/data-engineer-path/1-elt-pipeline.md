# Chapter 1: Build ELT Pipeline

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** YAML

Learn to build a production-grade ELT pipeline with Seeknal, transforming raw CSV data into clean, validated tables.

---

## What You'll Build

A complete e-commerce order processing pipeline:

```
CSV Files → Sources (Raw) → Transform → Orders (Clean) → Parquet
```

**After this chapter, you'll have:**
- CSV sources that load raw data files
- DuckDB transformation with data quality checks
- Clean output with deduplication and validation
- A pipeline you can run end-to-end on your laptop

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Quick Start](../../quick-start/) — Basic pipeline builder workflow
- [ ] [DE Path Overview](index.md) — Introduction to this path
- [ ] Comfortable with SQL JOINs and data types

---

## Part 1: Set Up Project and Load Data (8 minutes)

### Initialize the Project

```bash
seeknal init --name ecommerce-pipeline --description "E-commerce ELT pipeline"
```

### Create Sample Data

Create a CSV file with sample order data:

```bash
mkdir data
cat > data/orders.csv << 'EOF'
order_id,customer_id,order_date,status,revenue,items
ORD-001,CUST-100,2026-01-15 10:30:00,completed,149.99,3
ORD-002,CUST-101,2026-01-15 11:45:00,completed,89.50,2
ORD-003,,2026-01-16 09:00:00,completed,250.00,5
ORD-004,CUST-100,2026-01-16 14:20:00,pending,0.00,1
ORD-005,CUST-102,2026-01-17 08:15:00,completed,-10.00,2
ORD-006,CUST-103,2026-01-17 16:30:00,  Completed  ,75.25,1
ORD-007,CUST-101,2026-01-18 12:00:00,completed,320.00,-1
ORD-008,CUST-104,2026-01-18 15:45:00,completed,45.99,2
ORD-001,CUST-100,2026-01-19 10:30:00,completed,149.99,3
ORD-009,CUST-105,2026-01-19 09:00:00,completed,199.95,4
EOF
```

!!! info "Data Quality Issues (On Purpose)"
    This sample data includes common real-world problems that your pipeline will handle:

    - **Row 3**: Missing `customer_id`
    - **Row 5**: Negative `revenue`
    - **Row 6**: Inconsistent status formatting (`  Completed  `)
    - **Row 7**: Negative `items`
    - **Row 9**: Duplicate `ORD-001`

### Understanding CSV Sources

Seeknal's source nodes load data from files or databases into your pipeline DAG:

| Source Type | Use Case |
|-------------|----------|
| `csv` | CSV files |
| `parquet` | Parquet files |
| `json` / `jsonl` | JSON files |
| `postgresql` | PostgreSQL databases |
| `iceberg` | Apache Iceberg tables |

### Draft and Edit the Source

```bash
seeknal draft source raw_orders
```

Edit `draft_source_raw_orders.yml`:

```yaml
kind: source
name: raw_orders
description: "Raw e-commerce order data"
source: csv
table: "data/orders.csv"
columns:
  order_id: "Order identifier"
  customer_id: "Customer identifier"
  order_date: "Order timestamp"
  status: "Order status"
  revenue: "Order revenue in USD"
  items: "Number of items"
```

Validate and apply the source:

```bash
seeknal dry-run draft_source_raw_orders.yml
seeknal apply draft_source_raw_orders.yml
```

**Expected output:**
```
ℹ Validating YAML...
✓ YAML syntax valid
✓ Schema validation passed
✓ Dependency check passed
ℹ Executing preview (limit 10 rows)...
+------------+---------------+---------------------+-----------+-----------+---------+
| order_id   | customer_id   | order_date          | status    |   revenue |   items |
|------------+---------------+---------------------+-----------+-----------+---------|
| ORD-001    | CUST-100      | 2026-01-15 10:30:00 | completed |    149.99 |       3 |
| ORD-002    | CUST-101      | 2026-01-15 11:45:00 | completed |     89.5  |       2 |
| ORD-003    |               | 2026-01-16 09:00:00 | completed |    250    |       5 |
| ORD-004    | CUST-100      | 2026-01-16 14:20:00 | pending   |      0    |       1 |
| ORD-005    | CUST-102      | 2026-01-17 08:15:00 | completed |    -10    |       2 |
| ORD-006    | CUST-103      | 2026-01-17 16:30:00 | Completed |     75.25 |       1 |
| ORD-007    | CUST-101      | 2026-01-18 12:00:00 | completed |    320    |      -1 |
| ORD-008    | CUST-104      | 2026-01-18 15:45:00 | completed |     45.99 |       2 |
| ORD-001    | CUST-100      | 2026-01-19 10:30:00 | completed |    149.99 |       3 |
| ORD-009    | CUST-105      | 2026-01-19 09:00:00 | completed |    199.95 |       4 |
+------------+---------------+---------------------+-----------+-----------+---------+
✓ Preview completed in 0.0s
ℹ Run 'seeknal apply draft_source_raw_orders.yml' to apply
ℹ Validating...
✓ All checks passed
ℹ Moving file...
ℹ   FROM: draft_source_raw_orders.yml
ℹ   TO:   ./seeknal/sources/raw_orders.yml
ℹ Updating manifest...
✓ Manifest regenerated
ℹ 
ℹ Added:
ℹ   + source.raw_orders
ℹ     - order_id (Order identifier)
ℹ     - customer_id (Customer identifier)
ℹ     - order_date (Order timestamp)
ℹ     - status (Order status)
ℹ     - revenue (Order revenue in USD)
ℹ     - items (Number of items)
✓ Applied successfully
```

---

## Part 2: Clean and Transform with DuckDB (10 minutes)

### Understanding DuckDB Transformations

DuckDB is an in-process SQL OLAP database — perfect for data transformations:

- **Fast**: Columnar execution engine
- **SQL Compliant**: Standard SQL with advanced analytics
- **Zero Setup**: No database server required
- **Scalable**: Handles millions of rows on a laptop

### Draft and Edit the Transform

```bash
seeknal draft transform orders_cleaned
```

Edit `draft_transform_orders_cleaned.yml`:

```yaml
kind: transform
name: orders_cleaned
description: "Clean and validate order data"

transform: |
  SELECT
    -- Primary key
    order_id,

    -- Foreign key with validation
    CASE
      WHEN customer_id IS NOT NULL
      AND LENGTH(customer_id) > 0
      THEN customer_id
      ELSE 'UNKNOWN'
    END as customer_id,

    -- Date processing
    DATE(order_date) as order_date,
    CAST(order_date AS TIME) as order_time,

    -- Status normalization
    UPPER(TRIM(status)) as status,

    -- Revenue validation
    CASE
      WHEN revenue >= 0 THEN revenue
      ELSE NULL  -- Flag negative values
    END as revenue,

    -- Items validation
    CASE
      WHEN items >= 0 THEN items
      ELSE 0
    END as items,

    -- Data quality flag
    CASE
      WHEN revenue < 0 THEN 1
      WHEN customer_id IS NULL OR LENGTH(customer_id) = 0 THEN 1
      WHEN items < 0 THEN 1
      ELSE 0
    END as quality_flag,

    -- Audit column
    CURRENT_TIMESTAMP as processed_at

  FROM ref('source.raw_orders')

  -- Remove duplicates (keep latest)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY order_date DESC
  ) = 1

inputs:
  - ref: source.raw_orders
```

### Apply the Transform

```bash
seeknal dry-run draft_transform_orders_cleaned.yml
seeknal apply draft_transform_orders_cleaned.yml
```

**Expected output:**
```
seeknal apply draft_transform_orders_cleaned.yml
ℹ Validating YAML...
✓ YAML syntax valid
✓ Schema validation passed
✓ Dependency check passed
ℹ Executing preview (limit 10 rows)...
ℹ Loaded input_0 <- source.raw_orders (from orders.csv)
+------------+---------------+--------------+--------------+-----------+-----------+---------+----------------+----------------------------------+
| order_id   | customer_id   | order_date   | order_time   | status    |   revenue |   items |   quality_flag | processed_at                     |
|------------+---------------+--------------+--------------+-----------+-----------+---------+----------------+----------------------------------|
| ORD-009    | CUST-105      | 2026-01-19   | 09:00:00     | COMPLETED |    199.95 |       4 |              0 | 2026-02-23 14:49:14.347492+07:00 |
| ORD-005    | CUST-102      | 2026-01-17   | 08:15:00     | COMPLETED |           |       2 |              1 | 2026-02-23 14:49:14.347492+07:00 |
| ORD-007    | CUST-101      | 2026-01-18   | 12:00:00     | COMPLETED |    320    |       0 |              1 | 2026-02-23 14:49:14.347492+07:00 |
| ORD-001    | CUST-100      | 2026-01-19   | 10:30:00     | COMPLETED |    149.99 |       3 |              0 | 2026-02-23 14:49:14.347492+07:00 |
| ORD-002    | CUST-101      | 2026-01-15   | 11:45:00     | COMPLETED |     89.5  |       2 |              0 | 2026-02-23 14:49:14.347492+07:00 |
| ORD-003    | UNKNOWN       | 2026-01-16   | 09:00:00     | COMPLETED |    250    |       5 |              1 | 2026-02-23 14:49:14.347492+07:00 |
| ORD-006    | CUST-103      | 2026-01-17   | 16:30:00     | COMPLETED |     75.25 |       1 |              0 | 2026-02-23 14:49:14.347492+07:00 |
| ORD-008    | CUST-104      | 2026-01-18   | 15:45:00     | COMPLETED |     45.99 |       2 |              0 | 2026-02-23 14:49:14.347492+07:00 |
| ORD-004    | CUST-100      | 2026-01-16   | 14:20:00     | PENDING   |      0    |       1 |              0 | 2026-02-23 14:49:14.347492+07:00 |
+------------+---------------+--------------+--------------+-----------+-----------+---------+----------------+----------------------------------+
✓ Preview completed in 0.2s
ℹ Run 'seeknal apply draft_transform_orders_cleaned.yml' to apply
ℹ Validating...
✓ All checks passed
ℹ Moving file...
ℹ   FROM: draft_transform_orders_cleaned.yml
ℹ   TO:   ./seeknal/transforms/orders_cleaned.yml
ℹ Updating manifest...
✓ Manifest regenerated
ℹ 
ℹ Added:
ℹ   + transform.orders_cleaned
ℹ     - depends_on: source.raw_orders
✓ Applied successfully
```

!!! info "What's QUALIFY?"
    `QUALIFY` is a DuckDB extension that filters window function results. It's like `HAVING` for window functions — cleaner than subqueries!

---

## Part 3: Run the Pipeline (5 minutes)

### Generate Manifest and Execute

```bash
# Generate the DAG manifest
seeknal plan

# Run the full pipeline
seeknal run
```

**Expected output:**
```
> seeknal plan
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 2 nodes, 1 edges

Node Summary:
  - source: 1
  - transform: 1
ℹ No previous manifest found (first run)
✓ Manifest saved to ./target/manifest.json

Execution Plan:
------------------------------------------------------------
   1. RUN raw_orders
   2. RUN orders_cleaned

ℹ Total: 2 nodes, 2 to run
> seeknal run

Seeknal Pipeline Execution
============================================================
  Project: ecommerce-pipeline
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 2 nodes, 1 edges
ℹ No previous state found (first run)
ℹ Detecting changes...
ℹ Nodes to run: 2

Execution
============================================================
1/2: raw_orders [RUNNING]
  SUCCESS in 0.04s
  Rows: 10
2/2: orders_cleaned [RUNNING]
ℹ Loaded Python node output as input_0
ℹ Resolved SQL for orders_cleaned
ℹ   Executing statement 1/1
ℹ Created view 'transform.orders_cleaned' for materialization
ℹ Wrote transform output to ./target/intermediate/transform_orders_cleaned.parquet
  SUCCESS in 0.11s
  Rows: 9
✓ State saved

Execution Summary
============================================================
  Total nodes:    2
  Executed:       2
  Duration:       0.14s
============================================================
```

### Verify Output

Use the REPL to inspect the results:

```bash
seeknal repl
```

```sql
-- Check cleaned orders
SELECT * FROM orders_cleaned;

-- Count quality issues
SELECT quality_flag, COUNT(*) as cnt
FROM orders_cleaned
GROUP BY quality_flag;

-- Verify deduplication (ORD-001 should appear only once)
SELECT order_id, COUNT(*) as cnt
FROM orders_cleaned
GROUP BY order_id
HAVING cnt > 1;
```

**Checkpoint:** You should see:
- 9 unique orders (duplicate ORD-001 removed)
- 3 rows with `quality_flag = 1` (missing customer, negative revenue, negative items)
- All statuses normalized to uppercase

!!! success "Congratulations!"
    You've built a complete ELT pipeline with CSV ingestion, DuckDB transformation, and data quality validation.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. File Not Found**

    - Symptom: `FileNotFoundError: data/orders.csv`
    - Fix: Ensure the CSV file exists relative to your project root

    **2. Schema Drift**

    - Symptom: `Column not found` errors
    - Fix: Use `COALESCE` for new optional columns

    **3. Duplicate Records**

    - Symptom: More output rows than expected
    - Fix: Add `QUALIFY ROW_NUMBER()` to deduplicate

    **4. Type Mismatches**

    - Symptom: `Conversion Error` in DuckDB
    - Fix: Use explicit `CAST()` for type conversions

---

## Summary

In this chapter, you learned:

- [x] **CSV Sources** — Load data files into your pipeline
- [x] **DuckDB Transforms** — Clean and validate data with SQL
- [x] **Data Quality Flags** — Identify and handle bad records
- [x] **Deduplication** — Remove duplicate rows with `QUALIFY`
- [x] **REPL Verification** — Inspect results interactively

**Key Commands:**
```bash
seeknal init --name <name>         # Create project
seeknal draft source <name>        # Generate source template
seeknal draft transform <name>     # Generate transform template
seeknal dry-run <file>             # Validate YAML
seeknal apply <file>               # Save node definition
seeknal plan                       # Generate DAG manifest
seeknal run                        # Execute pipeline
seeknal repl                       # Interactive SQL queries
```

---

## What's Next?

[Chapter 2: Add Incremental Models →](2-incremental-models.md)

Make your pipeline more efficient with incremental processing, change data capture, and scheduled runs.

---

## See Also

- **[Virtual Environments](../../concepts/virtual-environments.md)** — Isolate development and production
- **[Change Categorization](../../concepts/change-categorization.md)** — Understanding breaking vs non-breaking changes
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — All supported source types and fields
