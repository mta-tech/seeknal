# Phase 2 Tutorial: Virtual Environments and Parallel Execution

> **Estimated Time:** 45 minutes | **Difficulty:** Intermediate | **Last Updated:** 2026-02-09

Learn Seeknal's Phase 2 features: virtual environments for safe testing and parallel execution for faster pipelines. This tutorial demonstrates the plan/apply/promote workflow for an e-commerce ETL pipeline.

---

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Part 1: Setup Sample Pipeline](#part-1-setup-sample-pipeline)
- [Part 2: First Run (Sequential Execution)](#part-2-first-run-sequential-execution)
- [Part 3: Make a Change](#part-3-make-a-change)
- [Part 4: Plan the Change](#part-4-plan-the-change)
- [Part 5: Apply in Dev Environment](#part-5-apply-in-dev-environment)
- [Part 6: Verify Results](#part-6-verify-results)
- [Part 7: Promote to Production](#part-7-promote-to-production)
- [Part 8: Breaking Change Example](#part-8-breaking-change-example)
- [Part 9: Parallel Execution](#part-9-parallel-execution)
- [Summary](#summary)

---

## Overview

### What You'll Build

An e-commerce ETL pipeline with:
- **3 sources**: orders, customers, inventory
- **3 transforms**: clean orders, enrich customers, inventory status
- **1 aggregation**: daily sales metrics
- **1 exposure**: data warehouse export

### Pipeline Architecture

```
┌─────────┐  ┌───────────┐  ┌───────────┐
│ orders  │  │ customers │  │ inventory │
└────┬────┘  └─────┬─────┘  └─────┬─────┘
     │            │              │
     v            │              v
┌─────────────┐   │        ┌──────────────────┐
│clean_orders │   │        │inventory_status  │
└──────┬──────┘   │        └──────────────────┘
       │          │
       v          v
  ┌─────────────────────┐
  │customer_enriched    │
  └──────────┬──────────┘
             v
       ┌──────────┐
       │daily_sales│
       └──────────┘
             v
    ┌──────────────────┐
    │warehouse_export  │
    └──────────────────┘
```

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Virtual Environment** | Isolated workspace for testing changes |
| **Plan** | Preview what will change before execution |
| **Apply** | Execute changes in an environment |
| **Promote** | Move changes from dev to production |
| **Parallel Execution** | Run independent nodes simultaneously |
| **Change Category** | BREAKING, NON_BREAKING, or METADATA |

---

## Prerequisites

Before starting, ensure you have:

- Seeknal installed: `pip install seeknal`
- Python 3.11 or higher
- Basic understanding of YAML pipelines (see [YAML Pipeline Tutorial](yaml-pipeline-tutorial.md))

Verify installation:

```bash
seeknal --version
seeknal --help | grep "env "
```

Expected output should include `env plan`, `env apply`, `env promote` commands.

---

## Part 1: Setup Sample Pipeline

### 1.1 Create Project Directory

```bash
mkdir -p ~/seeknal-phase2-tutorial
cd ~/seeknal-phase2-tutorial
```

### 1.2 Copy Example YAML Files

```bash
# Create seeknal directory structure
mkdir -p seeknal/sources seeknal/transforms seeknal/aggregations seeknal/exposures

# Copy the 8 example YAML files from the examples directory
cp -r /path/to/seeknal/examples/phase2-data-eng/* seeknal/

# Verify structure
tree seeknal/
```

Expected output:
```
seeknal/
├── 01_source_orders.yml
├── 02_source_customers.yml
├── 03_source_inventory.yml
├── 04_transform_clean_orders.yml
├── 05_transform_customer_enriched.yml
├── 06_transform_inventory_status.yml
├── 07_aggregation_daily_sales.yml
└── 08_exposure_warehouse.yml
```

### 1.3 Create Sample Data

Create minimal CSV files for testing:

```bash
mkdir -p data

# Create orders.csv
cat > data/orders.csv << 'EOF'
order_id,customer_id,product_id,order_date,quantity,unit_price,total_amount,status
1,101,1001,2024-01-10,2,25.00,50.00,completed
2,102,1002,2024-01-11,1,100.00,100.00,completed
3,101,1003,2024-01-12,3,15.00,45.00,pending
EOF

# Create customers.csv
cat > data/customers.csv << 'EOF'
customer_id,email,first_name,last_name,country,signup_date,customer_segment
101,john@example.com,John,Doe,US,2024-01-01,premium
102,jane@example.com,Jane,Smith,UK,2024-01-02,standard
EOF

# Create inventory.csv
cat > data/inventory.csv << 'EOF'
product_id,product_name,category,stock_quantity,reorder_level,last_updated
1001,Widget A,Electronics,50,10,2024-01-15
1002,Widget B,Electronics,5,10,2024-01-15
1003,Widget C,Home,100,20,2024-01-15
EOF
```

Verify files:
```bash
ls -lh data/
```

Expected output:
```
-rw-r--r--  1 you  staff   245B Feb 09 10:00 customers.csv
-rw-r--r--  1 you  staff   198B Feb 09 10:00 inventory.csv
-rw-r--r--  1 you  staff   287B Feb 09 10:00 orders.csv
```

---

## Part 2: First Run (Sequential Execution)

### 2.1 Parse the Pipeline

Validate the pipeline structure using the unified `plan` command:

```bash
seeknal plan
```

Expected output:
```
Parsing project: seeknal-phase2-tutorial
  Path: /Users/you/seeknal-phase2-tutorial
✓ Manifest generated: target/manifest.json
  Nodes: 8
  Edges: 7
```

> **Note:** `seeknal parse` also works as a backward-compatible alias.

### 2.2 Run the Full Pipeline

```bash
seeknal run
```

Expected output:
```
Seeknal Pipeline Execution
============================================================
  Project: seeknal-phase2-tutorial
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 8 nodes, 7 edges
ℹ No previous state found (first run)
ℹ Detecting changes...
ℹ Nodes to run: 8

Execution
============================================================
1/8: orders [RUNNING]
  SUCCESS in 0.02s
  Rows: 3

2/8: customers [RUNNING]
  SUCCESS in 0.01s
  Rows: 2

3/8: inventory [RUNNING]
  SUCCESS in 0.01s
  Rows: 3

4/8: clean_orders [RUNNING]
  SUCCESS in 0.03s
  Rows: 3

5/8: inventory_status [RUNNING]
  SUCCESS in 0.02s
  Rows: 3

6/8: customer_enriched [RUNNING]
  SUCCESS in 0.04s
  Rows: 2

7/8: daily_sales [RUNNING]
  SUCCESS in 0.05s

8/8: warehouse_export [RUNNING]
  SUCCESS in 0.06s

✓ State saved

Execution Summary
============================================================
  Total nodes:    8
  Executed:       8
  Duration:       0.24s
============================================================
```

---

## Part 3: Make a Change

### 3.1 Modify the Transform SQL

Edit the clean orders transform to add a discount calculation:

```bash
cat > seeknal/04_transform_clean_orders.yml << 'EOF'
kind: transform
name: clean_orders
description: Clean and validate order data with discount calculation
owner: analytics-team
transform: |
  SELECT
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    total_amount,
    UPPER(TRIM(status)) as status,
    CASE
      WHEN total_amount != (quantity * unit_price) THEN 'invalid'
      WHEN total_amount <= 0 THEN 'invalid'
      ELSE 'valid'
    END as validation_status,
    -- NEW: Calculate discount percentage
    CASE
      WHEN quantity >= 3 THEN 0.10
      WHEN quantity >= 2 THEN 0.05
      ELSE 0.00
    END as discount_pct
  FROM source.orders
  WHERE order_date IS NOT NULL
    AND customer_id IS NOT NULL
    AND total_amount > 0
inputs:
  - ref: source.orders
tags:
  - transformation
  - data-quality
EOF
```

**What changed**: Added a `discount_pct` column (NON_BREAKING change).

---

## Part 4: Plan the Change

### 4.1 Create a Plan

Use the unified `plan` command with an environment:

```bash
seeknal plan dev
```

> **Note:** `seeknal env plan dev` also works as a backward-compatible alias.

Expected output:
```
Environment Plan: dev
============================================================
  Base: production (latest state)
  Target: seeknal/ directory (current state)

Change Detection
============================================================
ℹ Analyzing 8 nodes...

Modified Nodes:
  1. clean_orders
     Category: NON_BREAKING
     Changed fields: config
     Details:
       - transform: Added 'discount_pct' column

Downstream Impact:
  2. customer_enriched
     Reason: Depends on clean_orders
     Category: NON_BREAKING

  3. daily_sales
     Reason: Depends on clean_orders
     Category: NON_BREAKING

  4. warehouse_export
     Reason: Depends on customer_enriched
     Category: NON_BREAKING

Execution Plan:
============================================================
  Total nodes to rebuild: 4
  Breaking changes: 0
  Non-breaking changes: 4
  Metadata-only changes: 0

Nodes to Execute:
  1. orders (source for clean_orders)
  2. clean_orders [NON_BREAKING]
  3. customer_enriched [downstream]
  4. daily_sales [downstream]
  5. warehouse_export [downstream]

ℹ Plan saved to: target/plans/dev.json
```

### 4.2 Understand the Plan

The plan shows:
- **Modified nodes**: What you changed
- **Change category**: Impact level (BREAKING, NON_BREAKING, METADATA)
- **Downstream impact**: What else needs to rebuild
- **Execution order**: Topologically sorted nodes

---

## Part 5: Apply in Dev Environment

### 5.1 Execute the Plan

Use the unified `run` command with an environment:

```bash
seeknal run --env dev
```

> **Note:** `seeknal env apply dev` also works as a backward-compatible alias.

Expected output:
```
Applying Environment: dev
============================================================
ℹ Loading plan from: target/plans/dev.json
ℹ Plan created: 2026-02-09 10:15:00
ℹ Base state: production
ℹ Nodes to execute: 5

Execution
============================================================
1/5: orders [RUNNING]
  SUCCESS in 0.02s
  Rows: 3

2/5: clean_orders [RUNNING]
  SUCCESS in 0.03s
  Rows: 3

3/5: customer_enriched [RUNNING]
  SUCCESS in 0.04s
  Rows: 2

4/5: daily_sales [RUNNING]
  SUCCESS in 0.05s

5/5: warehouse_export [RUNNING]
  SUCCESS in 0.06s

✓ Environment state saved: target/environments/dev/state.json

Execution Summary
============================================================
  Total nodes:    5
  Executed:       5
  Duration:       0.20s
============================================================
```

### 5.2 Apply with Parallel Execution

For faster execution, use the `--parallel` flag:

```bash
seeknal run --env dev --parallel
```

> **Note:** `seeknal env apply dev --parallel` also works as a backward-compatible alias.

Expected output:
```
Applying Environment: dev (parallel mode)
============================================================
ℹ Loading plan from: target/plans/dev.json
ℹ Nodes to execute: 5
ℹ Parallel workers: 4

Execution
============================================================
[Parallel] Executing batch 1 (1 nodes)
  1. orders [RUNNING]
     SUCCESS in 0.02s
     Rows: 3

[Parallel] Executing batch 2 (1 nodes)
  2. clean_orders [RUNNING]
     SUCCESS in 0.03s
     Rows: 3

[Parallel] Executing batch 3 (2 nodes)
  3. customer_enriched [RUNNING]
     SUCCESS in 0.04s
  4. daily_sales [RUNNING]
     SUCCESS in 0.05s

[Parallel] Executing batch 4 (1 nodes)
  5. warehouse_export [RUNNING]
     SUCCESS in 0.06s

✓ Environment state saved

Execution Summary
============================================================
  Total nodes:    5
  Executed:       5
  Duration:       0.15s (33% faster)
  Parallelism:    2 nodes executed concurrently
============================================================
```

---

## Part 6: Verify Results

### 6.1 List Environments

```bash
seeknal env list
```

Expected output:
```
Virtual Environments
============================================================
  Environment      | Nodes | Last Updated        | Status
  -----------------|-------|---------------------|--------
  dev              | 5     | 2026-02-09 10:20:00 | active
  production       | 8     | 2026-02-09 10:05:00 | active
```

### 6.2 Query Dev Environment Data

The dev environment has isolated state. You can query it separately:

```bash
# Show state file
cat target/environments/dev/state.json | python -m json.tool | head -20
```

This shows execution history and node hashes for the dev environment.

---

## Part 7: Promote to Production

### 7.1 Promote Changes

Once you've verified the dev environment, promote to production using the unified command:

```bash
seeknal promote dev
```

> **Note:** `seeknal env promote dev prod` also works as a backward-compatible alias.

Expected output:
```
Promoting Environment: dev → prod
============================================================
ℹ Source environment: dev
ℹ Target environment: prod
ℹ Comparing states...

Changes to Promote:
  Modified nodes: 1
    - clean_orders (NON_BREAKING)

  Downstream rebuilds: 4
    - customer_enriched
    - daily_sales
    - warehouse_export

ℹ No breaking changes detected
✓ Safe to promote

Execution
============================================================
ℹ Copying dev state to prod...
✓ Production environment updated

Summary
============================================================
  Environment: prod
  Nodes updated: 5
  Breaking changes: 0
  Promoted at: 2026-02-09 10:25:00
============================================================
```

### 7.2 Verify Production

```bash
seeknal env list
```

Expected output:
```
Virtual Environments
============================================================
  Environment      | Nodes | Last Updated        | Status
  -----------------|-------|---------------------|--------
  dev              | 5     | 2026-02-09 10:20:00 | active
  prod             | 5     | 2026-02-09 10:25:00 | active
```

---

## Part 8: Breaking Change Example

### 8.1 Make a Breaking Change

Rename a column in the source (BREAKING change):

```bash
cat > seeknal/01_source_orders.yml << 'EOF'
kind: source
name: orders
description: E-commerce order transactions
owner: data-team
source: csv
table: data/orders.csv
params:
  delimiter: ","
  header: true
schema:
  - name: order_id
    data_type: integer
  - name: cust_id  # RENAMED: customer_id → cust_id
    data_type: integer
  - name: product_id
    data_type: integer
  - name: order_date
    data_type: date
  - name: quantity
    data_type: integer
  - name: unit_price
    data_type: float
  - name: total_amount
    data_type: float
  - name: status
    data_type: string
tags:
  - raw-data
  - sales
EOF
```

### 8.2 Plan the Breaking Change

```bash
seeknal plan staging
```

> **Note:** `seeknal env plan staging` also works as a backward-compatible alias.

Expected output:
```
Environment Plan: staging
============================================================

Change Detection
============================================================
Modified Nodes:
  1. orders
     Category: BREAKING
     Changed fields: schema
     Details:
       - schema.customer_id: REMOVED (column deleted)
       - schema.cust_id: ADDED (new column)

⚠ WARNING: Breaking changes detected!

Downstream Impact:
  2. clean_orders
     Reason: Depends on orders (BREAKING change)
     Category: BREAKING
     Impact: References missing column 'customer_id'

  3. customer_enriched
     Reason: Depends on clean_orders
     Category: BREAKING

  4. daily_sales
     Reason: Depends on clean_orders
     Category: BREAKING

  5. warehouse_export
     Reason: Depends on customer_enriched
     Category: BREAKING

Execution Plan:
============================================================
  Total nodes to rebuild: 5
  Breaking changes: 5
  Non-breaking changes: 0

⚠ Action Required:
  - Update downstream transforms to use 'cust_id' instead of 'customer_id'
  - Review all SQL queries referencing this column
  - Test thoroughly before promoting to production
```

This demonstrates how Seeknal detects breaking changes and shows downstream impact.

---

## Part 9: Parallel Execution

### 9.1 Run Full Pipeline with Parallel Execution

Restore the original schema, then run with parallel execution:

```bash
# Restore original schema (revert breaking change)
git restore seeknal/01_source_orders.yml

# Run with parallel execution
seeknal run --parallel
```

Expected output:
```
Seeknal Pipeline Execution (Parallel Mode)
============================================================
  Project: seeknal-phase2-tutorial
  Mode: Incremental
  Parallelism: enabled
  Max workers: 4

ℹ Building DAG from seeknal/ directory...
✓ DAG built: 8 nodes, 7 edges
ℹ Loaded previous state
ℹ Detecting changes...
ℹ Nodes to run: 8

Execution
============================================================
[Batch 1] Executing 3 nodes in parallel
  1. orders [RUNNING]
     SUCCESS in 0.02s
  2. customers [RUNNING]
     SUCCESS in 0.01s
  3. inventory [RUNNING]
     SUCCESS in 0.01s

[Batch 2] Executing 2 nodes in parallel
  4. clean_orders [RUNNING]
     SUCCESS in 0.03s
  5. inventory_status [RUNNING]
     SUCCESS in 0.02s

[Batch 3] Executing 2 nodes in parallel
  6. customer_enriched [RUNNING]
     SUCCESS in 0.04s
  7. daily_sales [RUNNING]
     SUCCESS in 0.05s

[Batch 4] Executing 1 node
  8. warehouse_export [RUNNING]
     SUCCESS in 0.06s

✓ State saved

Execution Summary
============================================================
  Total nodes:    8
  Executed:       8
  Duration:       0.14s (42% faster than sequential)
  Batches:        4
  Max concurrent: 3 nodes
============================================================
```

### 9.2 Configure Max Workers

Control the level of parallelism:

```bash
# Use 8 parallel workers
seeknal run --parallel --max-workers 8

# Use 2 parallel workers (conservative)
seeknal run --parallel --max-workers 2
```

### 9.3 When to Use Parallel Execution

**Use parallel execution when:**
- Pipeline has many independent nodes
- Each node is relatively fast (<5 minutes)
- You have sufficient CPU/memory resources

**Avoid parallel execution when:**
- Pipeline is mostly sequential (few parallel opportunities)
- Nodes are resource-intensive (may overwhelm the system)
- Debugging issues (sequential is easier to trace)

---

## Summary

### What You Learned

You now know how to:

- Create virtual environments for isolated testing
- Preview changes with `seeknal env plan`
- Apply changes safely with `seeknal env apply`
- Promote changes from dev to production
- Detect breaking vs non-breaking changes
- Run pipelines in parallel for faster execution

### Key Commands Reference

| Command | Purpose |
|---------|---------|
| `seeknal plan <name>` | Preview changes in a virtual environment |
| `seeknal run --env <name>` | Execute plan in environment |
| `seeknal run --env <name> --parallel` | Execute with parallel processing |
| `seeknal promote <from>` | Promote environment changes |
| `seeknal env list` | List all environments |
| `seeknal env delete <name>` | Delete an environment |
| `seeknal run --parallel` | Run pipeline with parallel execution |
| `seeknal run --parallel --max-workers N` | Set max parallel workers |

**Backward-compatible aliases:**
- `seeknal env plan <name>` → `seeknal plan <name>`
- `seeknal env apply <name>` → `seeknal run --env <name>`
- `seeknal env promote <from> <to>` → `seeknal promote <from>`

### Change Categories

| Category | Description | Example |
|----------|-------------|---------|
| **BREAKING** | Downstream nodes must rebuild | Column removed, type changed |
| **NON_BREAKING** | Only this node rebuilds | Column added, SQL logic changed |
| **METADATA** | No rebuild needed | Description, tags, owner changed |

### Best Practices

**Virtual Environments:**
- Always `plan` before `apply`
- Test in `dev` before promoting to `prod`
- Review breaking changes carefully
- Use descriptive environment names

**Parallel Execution:**
- Start with default workers (4)
- Monitor resource usage
- Use sequential mode for debugging
- Combine with `--dry-run` for planning

### Next Steps

- Explore [YAML Pipeline Tutorial](yaml-pipeline-tutorial.md) for basics
- Learn about [Iceberg Materialization](../iceberg-materialization.md)
- Set up CI/CD with environment promotion
- Read [YAML Schema Reference](../reference/yaml-schema.md)

---

**Tutorial Complete!**

You now understand Seeknal's Phase 2 features: virtual environments and parallel execution. These features enable safe testing and faster pipelines for production data engineering.
