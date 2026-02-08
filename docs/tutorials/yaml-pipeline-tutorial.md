# YAML Pipeline Tutorial: Build and Execute Data Pipelines

> **Estimated Time:** 75 minutes | **Difficulty:** Beginner-Intermediate | **Last Updated:** 2026-01-26

Learn Seeknal's dbt-inspired YAML workflow to define, validate, and execute data pipelines. This hands-on tutorial covers the complete workflow from creating YAML definitions to running production-ready pipelines with incremental execution, state tracking, and optional Iceberg materialization.

---

## Table of Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Part 1: Setup and Sample Data](#part-1-setup-and-sample-data)
- [Part 2: Define Data Sources](#part-2-define-data-sources)
- [Part 3: Create Transformations](#part-3-create-transformations)
- [Part 4: Build Feature Groups](#part-4-build-feature-groups)
- [Part 5: Validate and Preview](#part-5-validate-and-preview)
- [Part 6: Apply and Run](#part-6-apply-and-run)
- [Part 7: Incremental Runs](#part-7-incremental-runs)
- [Part 8: Advanced Features](#part-8-advanced-features)
  - [8.8 Second-Order Aggregations](#88-second-order-aggregations)
- [Part 9: Production Tips](#part-9-production-tips)
- [Part 10: Iceberg Materialization](#part-10-iceberg-materialization)
- [Troubleshooting](#troubleshooting)

---

## Introduction

Seeknal's YAML pipeline workflow provides a **dbt-like experience** for defining and executing data transformations. Instead of writing Python code, you define your data pipeline as YAML files, and Seeknal handles:

- ✅ **Dependency resolution** - Automatic topological ordering
- ✅ **Change detection** - Only runs what changed
- ✅ **State tracking** - Remembers previous runs
- ✅ **Validation** - Dry-run before applying
- ✅ **Incremental execution** - Skip unchanged nodes

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Source** | Raw data input (CSV, Parquet, database, etc.) |
| **Transform** | SQL transformation logic |
| **Feature Group** | ML feature definitions with entity keys |
| **Aggregation** | First-level aggregations (e.g., user-level metrics) |
| **Second-Order Aggregation** | Aggregations of aggregations (e.g., region-level from user-level) |
| **Node** | A single unit in the pipeline (source/transform/etc.) |
| **DAG** | Directed Acyclic Graph of node dependencies |
| **State** | Execution history with hashes for change detection |

### Workflow Overview

```
┌─────────┐    ┌──────────┐    ┌────────┐    ┌──────────┐    ┌──────┐
│  Draft  │ -> │ Dry-Run  │ -> │ Apply  │ -> │  Parse   │ -> │ Run  │
│ (create)│    │(validate)│    │(save)  │    │(manifest)│    │(exec)│
└─────────┘    └──────────┘    └────────┘    └──────────┘    └──────┘
```

---

## Prerequisites

Before starting, ensure you have:

- ✅ Python 3.11 or higher
- ✅ Seeknal installed: `pip install seeknal`
- ✅ A terminal/shell with basic commands
- ✅ Text editor (VS Code, vim, nano, etc.)

### Verify Installation

```bash
# Check Seeknal is installed
seeknal --version

# List available commands
seeknal --help
```

Expected output includes commands:
- `draft` - Create YAML templates
- `dry-run` - Validate and preview
- `apply` - Apply changes to production
- `parse` - Generate manifest
- `run` - Execute the pipeline

---

## Part 1: Setup and Sample Data

### 1.1 Create Project Directory

Create a clean workspace for the tutorial:

```bash
# Create and enter project directory
mkdir -p ~/seeknal-tutorial
cd ~/seeknal-tutorial

# Verify current directory
pwd
# Expected output: /Users/your-username/seeknal-tutorial
```

### 1.2 Create Sample Data

We'll build an analytics pipeline for a fictional e-commerce company. Let's create sample CSV files.

**Create customers data:**

```bash
cat > customers.csv << 'EOF'
customer_id,email,country,signup_date
1,user1@example.com,US,2024-01-01
2,user2@example.com,UK,2024-01-02
3,user3@example.com,US,2024-01-03
4,user4@example.com,CA,2024-01-04
5,user5@example.com,US,2024-01-05
EOF
```

**Create orders data:**

```bash
cat > orders.csv << 'EOF'
order_id,customer_id,order_date,amount
101,1,2024-01-10,100.00
102,2,2024-01-11,250.50
103,1,2024-01-12,75.00
104,3,2024-01-13,500.00
105,4,2024-01-14,125.00
EOF
```

**Verify the files:**

```bash
ls -lh *.csv
cat customers.csv
cat orders.csv
```

Expected output:
```
-rw-r--r--  1 you  staff   189B Jan 26 15:00 customers.csv
-rw-r--r--  1 you  staff   165B Jan 26 15:00 orders.csv
```

✅ **Checkpoint**: You should have two CSV files in your directory.

---

## Part 2: Define Data Sources

Sources define where your raw data comes from. We'll create two sources: customers and orders.

### 2.1 Create the Source Directory

```bash
mkdir -p seeknal/sources
```

### 2.2 Create Customers Source

Create `seeknal/sources/customers.yml`:

```bash
cat > seeknal/sources/customers.yml << 'EOF'
kind: source
name: customers
description: "Customer master data"
owner: "data-team"
source: csv
table: "customers.csv"
params:
  delimiter: ","
  header: true
schema:
  - name: customer_id
    data_type: integer
  - name: email
    data_type: string
  - name: country
    data_type: string
  - name: signup_date
    data_type: date
tags: []
EOF
```

**What each field means:**

| Field | Description | Example |
|-------|-------------|---------|
| `kind` | Node type | `source`, `transform`, `feature_group` |
| `name` | Simple name (used as table/view name) | `customers` |
| `description` | Human-readable description | "Customer master data" |
| `owner` | Team or person responsible | "data-team" |
| `source` | Data source type | `csv`, `parquet`, `json`, `postgresql` |
| `table` | File path or table name | "customers.csv" |
| `params` | Source-specific parameters | delimiter, header, etc. |
| `schema` | Column definitions | name + data_type |
| `tags` | Organizational tags | For filtering/grouping |

### 2.3 Create Orders Source

Create `seeknal/sources/orders.yml`:

```bash
cat > seeknal/sources/orders.yml << 'EOF'
kind: source
name: orders
description: "Order transactions"
owner: "data-team"
source: csv
table: "orders.csv"
params:
  delimiter: ","
  header: true
schema:
  - name: order_id
    data_type: integer
  - name: customer_id
    data_type: integer
  - name: order_date
    data_type: date
  - name: amount
    data_type: float
tags: []
EOF
```

### 2.4 Verify Directory Structure

```bash
tree seeknal/
```

Expected output:
```
seeknal/
└── sources/
    ├── customers.yml
    └── orders.yml
```

✅ **Checkpoint**: You have two source YAML files in `seeknal/sources/`.

---

## Part 3: Create Transformations

Transforms define SQL logic to process and join data. We'll create two transforms.

### 3.1 Create Transform Directory

```bash
mkdir -p seeknal/transforms
```

### 3.2 Create Active Customers Transform

This transform filters for US customers only.

Create `seeknal/transforms/active_customers.yml`:

```bash
cat > seeknal/transforms/active_customers.yml << 'EOF'
kind: transform
name: active_customers
description: "Filter active US customers"
owner: "data-team"
transform: |
  SELECT
    customer_id,
    email,
    signup_date
  FROM source.customers
  WHERE country = 'US'
inputs:
  - ref: source.customers
tags:
  - transformation
EOF
```

**Key points:**

- The `transform` field contains SQL (can be multi-statement)
- `inputs` defines dependencies using `ref:` syntax
- `source.customers` refers to the source we created earlier
- The `ref` format is: `kind.name` (e.g., `source.customers`)

### 3.3 Create Customer Orders Transform

This transform joins customers with their orders.

Create `seeknal/transforms/customer_orders.yml`:

```bash
cat > seeknal/transforms/customer_orders.yml << 'EOF'
kind: transform
name: customer_orders
description: "Join customers with their orders"
owner: "data-team"
transform: |
  SELECT
    c.customer_id,
    c.email,
    c.country,
    o.order_id,
    o.order_date,
    o.amount,
    o.order_date AS prediction_date
  FROM source.customers c
  INNER JOIN source.orders o
    ON c.customer_id = o.customer_id
inputs:
  - ref: source.customers
  - ref: source.orders
tags:
  - transformation
  - join
EOF
```

**SQL Tips:**

- Use table aliases (`c`, `o`) for readability
- Reference sources as `kind.name` (e.g., `source.customers`)
- Supports all DuckDB SQL syntax
- Can use CTEs, subqueries, etc.
- For second-order aggregations, include `prediction_date` (or other application date column) in the transform output. This allows the aggregation to use it for time-based calculations.

### 3.4 Verify Directory Structure

```bash
tree seeknal/
```

Expected output:
```
seeknal/
├── sources/
│   ├── customers.yml
│   └── orders.yml
└── transforms/
    ├── active_customers.yml
    └── customer_orders.yml
```

✅ **Checkpoint**: You have 4 YAML files (2 sources, 2 transforms).

---

## Part 4: Build Feature Groups

Feature groups define ML features with entity keys for serving.

### 4.1 Create Feature Group Directory

```bash
mkdir -p seeknal/feature_groups
```

### 4.2 Create Customer Features

Create `seeknal/feature_groups/customer_features.yml`:

```bash
cat > seeknal/feature_groups/customer_features.yml << 'EOF'
kind: feature_group
name: customer_features
description: "ML features for customer segmentation"
owner: "ml-team"
entity:
  name: customer
  join_keys: ["customer_id"]
materialization:
  event_time_col: latest_order_date
  offline:
    enabled: true
    format: parquet
  online:
    enabled: false
    ttl: 7d
features:
  customer_id:
    dtype: integer
  email:
    dtype: string
  country:
    dtype: string
  total_orders:
    dtype: integer
  total_spent:
    dtype: float
  avg_order_value:
    dtype: float
  latest_order_date:
    dtype: date
inputs:
  - ref: transform.customer_orders
tags:
  - ml
  - features
EOF
```

**What makes feature groups special:**

- `entity` defines the join key entity with:
  - `name`: Entity identifier (e.g., `customer`, `user`, `product`)
  - `join_keys`: List of columns used to join features (e.g., `["customer_id"]`)
- `materialization` configures how features are stored:
  - `event_time_col`: Column for point-in-time joins (required)
  - `offline`: Batch feature store configuration
  - `online`: Real-time serving configuration
- `features` define the output schema with data types
- Supports both offline (batch) and online (real-time) serving
- Automatically handles point-in-time joins to prevent data leakage

### 4.3 Verify Complete Structure

```bash
tree seeknal/
```

Expected output:
```
seeknal/
├── sources/
│   ├── customers.yml
│   └── orders.yml
├── transforms/
│   ├── active_customers.yml
│   └── customer_orders.yml
└── feature_groups/
    └── customer_features.yml
```

✅ **Checkpoint**: You have 5 YAML files defining a complete pipeline.

---

## Part 5: Validate and Preview

Before running, validate all your YAML files.

### 5.1 Parse and Generate Manifest

```bash
seeknal parse
```

Expected output:
```
Parsing project: seeknal-tutorial
  Path: /Users/your-username/seeknal-tutorial
✓ Manifest generated: target/manifest.json
  Nodes: 5
  Edges: 4
```

**What is a manifest?**

The manifest (`target/manifest.json`) contains:
- All nodes in your pipeline
- Dependencies between nodes
- Topological sort order
- Metadata for each node

### 5.2 Show Execution Plan

See what will execute (without actually running):

```bash
seeknal run --show-plan
```

Expected output:
```
Seeknal Pipeline Execution
============================================================
  Project: seeknal-tutorial
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 5 nodes, 4 edges
ℹ No previous state found (first run)
ℹ Detecting changes...
ℹ
ℹ Execution Plan:
ℹ ------------------------------------------------------------
   1. RUN customers
   2. RUN orders
   3. RUN active_customers [transformation]
   4. RUN customer_orders [transformation, join]
   5. RUN customer_features [ml, features]

ℹ Total: 5 nodes, 5 to run
```

**Understanding the output:**

| Column | Meaning |
|--------|---------|
| `RUN` | Node will execute |
| `CACHED` | Node skipped (unchanged) |
| `[tags]` | Organizational tags |
| `5 to run` | Number of nodes that will execute |

### 5.3 Dry Run Execution

Preview what would happen during execution:

```bash
seeknal run --dry-run
```

This validates everything without actually executing.

---

## Part 6: Apply and Run

Now let's execute the pipeline for real.

### 6.1 Run the Pipeline

```bash
seeknal run
```

Expected output:
```
Seeknal Pipeline Execution
============================================================
  Project: seeknal-tutorial
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 5 nodes, 4 edges
ℹ No previous state found (first run)
ℹ Detecting changes...
ℹ Nodes to run: 5

Execution
============================================================
1/5: customers [RUNNING]
  SUCCESS in 0.02s
  Rows: 5

2/5: orders [RUNNING]
  SUCCESS in 0.00s
  Rows: 5

3/5: active_customers [RUNNING]
ℹ Resolved SQL for active_customers
ℹ   Executing statement 1/1
  SUCCESS in 0.00s
  Rows: 3

4/5: customer_orders [RUNNING]
ℹ Resolved SQL for customer_orders
ℹ   Executing statement 1/1
  SUCCESS in 0.00s
  Rows: 5

5/5: customer_features [RUNNING]
2026-01-26 15:00:00 - INFO - Feature group 'customer_features' executed
  SUCCESS in 0.01s

✓ State saved

Execution Summary
============================================================
  Total nodes:    5
  Executed:       5
  Duration:       0.05s
============================================================
```

### 6.2 Inspect the State

```bash
cat target/run_state.json | python -m json.tool | head -40
```

This shows:
- Execution history
- Node hashes for change detection
- Row counts and timing
- Status of each node

### 6.3 Verify DuckDB Views

Seeknal creates DuckDB views you can query:

```bash
python -c "
import duckdb
con = duckdb.connect(':memory:')
# Re-create a view to test
con.execute(\"CREATE VIEW test_customers AS SELECT * FROM read_csv('customers.csv')\")
print(con.execute('SELECT * FROM test_customers WHERE country=\\\"US\\\"').df())
"
```

✅ **Checkpoint**: First pipeline execution complete!

---

## Part 7: Incremental Runs

Seeknal tracks state and only runs changed nodes.

### 7.1 Verify No Changes Detected

Run again without changing anything:

```bash
seeknal run
```

Expected output:
```
Seeknal Pipeline Execution
============================================================
  Project: seeknal-tutorial
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 5 nodes, 4 edges
ℹ Loaded previous state from run: 20260126_150000
ℹ Detecting changes...
✓ No changes detected. Nothing to run.
```

### 7.2 Modify a Transform

Edit `seeknal/transforms/active_customers.yml`:

```bash
cat > seeknal/transforms/active_customers.yml << 'EOF'
kind: transform
name: active_customers
description: "Filter active US and CA customers"
owner: "data-team"
transform: |
  SELECT
    customer_id,
    email,
    signup_date,
    country
  FROM source.customers
  WHERE country IN ('US', 'CA')
inputs:
  - ref: source.customers
tags:
  - transformation
EOF
```

**What changed:** We added `country` to the SELECT and changed `WHERE` to include 'CA'.

### 7.3 Show Incremental Plan

```bash
seeknal run --show-plan
```

Expected output:
```
ℹ Execution Plan:
ℹ ------------------------------------------------------------
   1. RUN customers
   2. CACHED orders
   3. RUN active_customers [transformation]
   4. CACHED customer_orders [transformation, join]
   5. CACHED customer_features [ml, features]

ℹ Total: 5 nodes, 2 to run
```

**Notice:**
- `customers` runs because `active_customers` depends on it
- `active_customers` runs because we changed it
- `orders` is cached (not needed)
- `customer_orders` and `customer_features` are cached (not affected)

### 7.4 Run Incremental Execution

```bash
seeknal run
```

Expected output:
```
Seeknal Pipeline Execution
============================================================
  Project: seeknal-tutorial
  Mode: Incremental
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 5 nodes, 4 edges
ℹ Loaded previous state from run: 20260126_150000
ℹ Detecting changes...
ℹ Nodes to run: 2

Execution
============================================================
1/5: customers [RUNNING]
  SUCCESS in 0.03s
  Rows: 5

2/5: orders [CACHED]

3/5: active_customers [RUNNING]
ℹ Resolved SQL for active_customers
ℹ   Executing statement 1/1
  SUCCESS in 0.00s
  Rows: 4

4/5: customer_orders [CACHED]

5/5: customer_features [CACHED]

✓ State saved

Execution Summary
============================================================
  Total nodes:    5
  Executed:       2
  Cached:         3
  Duration:       0.03s
============================================================
```

### 7.5 How Incremental Execution Works

```
┌─────────────────────────────────────────────────────────────┐
│                    Change Detection                          │
├─────────────────────────────────────────────────────────────┤
│  1. Calculate hash of each node's YAML content              │
│  2. Compare with stored hash from previous run              │
│  3. Mark changed nodes as "to run"                          │
│  4. Add upstream SOURCE dependencies for transforms         │
│  5. Add all downstream dependencies (BFS traversal)          │
└─────────────────────────────────────────────────────────────┘
```

**Why sources execute with transforms:**

Transforms execute SQL like `SELECT * FROM source.customers`. This requires the `source.customers` view to exist in DuckDB. When a transform changes, we execute its upstream sources to ensure these views are available.

> **Note:** This is a smart dependency approach - sources are relatively cheap to execute, and this ensures correctness without requiring persistent materialization.

✅ **Checkpoint:** Incremental execution works!

---

## Part 8: Advanced Features

### 8.1 Run Specific Nodes

Run only specific nodes and their downstream dependents:

```bash
seeknal run --show-plan --nodes customers
```

Output:
```
ℹ Execution Plan:
   1. RUN customers
   2. RUN active_customers
   3. RUN customer_orders
   4. RUN customer_features
```

### 8.2 Filter by Node Type

Run only sources and transforms (skip feature groups):

```bash
seeknal run --show-plan --types source,transform
```

Output:
```
ℹ Execution Plan:
   1. CACHED customers
   2. CACHED orders
   3. CACHED active_customers
   4. CACHED customer_orders
```

### 8.3 Full Refresh

Ignore state and run everything:

```bash
seeknal run --show-plan --full
```

Output:
```
Mode: Full
ℹ Execution Plan:
   1. RUN customers
   2. RUN orders
   3. RUN active_customers
   4. RUN customer_orders
   5. RUN customer_features
```

Use this when:
- You want to ensure fresh data from all sources
- Source data has changed externally
- Debugging pipeline issues

### 8.4 Dry Run

Preview what would execute without running:

```bash
seeknal run --dry-run
```

### 8.5 Continue on Error

Continue execution even if some nodes fail:

```bash
seeknal run --continue-on-error
```

### 8.6 Retry Failed Nodes

Automatically retry failed nodes:

```bash
seeknal run --retry 3
```

### 8.7 Combine Flags

You can combine multiple flags:

```bash
# Show plan for only sources, with full refresh
seeknal run --show-plan --types source --full

# Dry run with specific nodes
seeknal run --dry-run --nodes active_customers
```

### 8.8 Second-Order Aggregations

Second-order aggregations enable **aggregations of aggregations** - a powerful pattern for multi-level feature engineering. For example, you can aggregate user-level features to region-level, or product-level features to category-level metrics.

#### What are Second-Order Aggregations?

```
Raw Data → First-Level Aggregation → Second-Order Aggregation
           (user metrics)             (region metrics)
```

**Example use cases:**
- User-level metrics → Region-level averages
- Store-level sales → Country-level totals
- Product-level features → Category-level aggregations
- Daily metrics → Weekly/monthly patterns

#### Creating a Second-Order Aggregation

First, let's create an aggregation node (first level), then aggregate it again (second order).

**Step 1: Create an aggregation directory**
```bash
mkdir -p seeknal/aggregations
```

**Step 2: Create first-level aggregation (user daily features)**

Create `seeknal/aggregations/user_daily_features.yml`:

```bash
cat > seeknal/aggregations/user_daily_features.yml << 'EOF'
kind: aggregation
name: user_daily_features
description: "Daily features per user"
owner: "ml-team"
id_col: customer_id
feature_date_col: order_date
application_date_col: order_date
group_by:
  - country
features:
  - name: spend_metrics
    basic:
      - sum
      - count
    column: amount
  - name: volume_metrics
    basic:
      - sum
    column: amount
inputs:
  - ref: transform.customer_orders
tags:
  - aggregation
  - daily-features
EOF
```

> **Important:** For second-order aggregations to work:
> - The first-level aggregation must include the `group_by` field with columns that will be used for second-level grouping (e.g., `country`)
> - Include `prediction_date` in the transform output if the second-order aggregation uses it as `application_date_col`
> - All `group_by` columns must exist in the upstream transform output

> **Note:** First-level aggregations use a **list** format for features, where each item has a name, basic aggregations, and column.

**Step 3: Create second-order aggregation (region user metrics)**

Create `seeknal/aggregations/region_user_metrics.yml`:

```bash
cat > seeknal/aggregations/region_user_metrics.yml << 'EOF'
kind: second_order_aggregation
name: region_user_metrics
description: "Aggregate user-level features to region level"
owner: "analytics"
id_col: country
feature_date_col: order_date
application_date_col: prediction_date
source: aggregation.user_daily_features
features:
  # Count users per region (use customer_id to count rows)
  total_users:
    basic: [count]
    source_feature: customer_id

  # Average spending across users in region
  avg_user_spend:
    basic: [mean]
    source_feature: spend_metrics_sum

  # Maximum spending by users in region
  max_user_spend:
    basic: [max]
    source_feature: spend_metrics_sum

  # Total volume across users in region
  total_volume:
    basic: [sum]
    source_feature: volume_metrics_sum

inputs:
  - ref: aggregation.user_daily_features
tags:
  - second-order
  - feature-engineering
  - analytics
EOF
```

> **Important:**
> - Always specify `source_feature` for each feature - it tells the executor which upstream column to aggregate
> - For counting rows, use a unique ID column (e.g., `customer_id`) as the source_feature
> - The `source_feature` must exist in the upstream aggregation's output (e.g., `spend_metrics_sum`)
> - If `application_date_col` is used, ensure the upstream aggregation includes this column in its `group_by`

> **Note:** Second-order aggregations use a **dictionary** format for features, where the key is the output feature name and the value contains the aggregation specification.

**Key fields for second-order aggregations:**

| Field | Description | Example |
|-------|-------------|---------|
| `kind` | Node type | `second_order_aggregation` |
| `id_col` | Entity ID for second-level grouping | `country`, `region` |
| `feature_date_col` | Date column for features | `date` |
| `source` | Upstream aggregation reference | `aggregation.user_daily_features` |
| `features` | Feature specifications (dict format) | See below |

**Feature aggregation types:**

1. **Basic aggregations** - Simple statistical functions:
   ```yaml
   total_users:
     basic: [count]  # count, sum, mean, stddev, min, max
   ```

2. **Aggregating specific source features**:
   ```yaml
   avg_user_spend:
     basic: [mean]
     source_feature: spend_metrics_sum  # Aggregate this upstream feature
   ```

3. **Window aggregations** - Time-based windows:
   ```yaml
   weekly_total:
     window: [7, 7]  # [lower_bound, upper_bound] in days
     basic: [sum]
     source_feature: daily_volume
   ```

4. **Ratio aggregations** - Numerator/denominator comparisons:
   ```yaml
   recent_vs_historical:
     ratio:
       numerator: [1, 7]    # Days 1-7
       denominator: [8, 30]  # Days 8-30
       aggs: [sum]
     source_feature: total_spend
   ```

> **Important:** When using `source_feature`, reference the upstream feature name. First-level aggregations produce features with names like `spend_metrics_sum`, `spend_metrics_count`, etc. (feature name + aggregation function).
       aggs: [sum]
     source_feature: amount
   ```

#### Verify and Run

```bash
# Show execution plan with new nodes
seeknal run --show-plan
```

Expected output:
```
ℹ Execution Plan:
   1. RUN customers
   2. RUN orders
   3. RUN active_customers
   4. RUN customer_orders
   5. RUN user_daily_features [aggregation, daily-features]
   6. RUN region_user_metrics [second-order, feature-engineering, analytics]
```

```bash
# Run the pipeline
seeknal run
```

#### Using the Draft Command

You can also generate second-order aggregation templates using the CLI:

```bash
# Generate YAML template
seeknal draft second-order-aggregation region_metrics

# Generate Python template
seeknal draft second-order-aggregation region_metrics --python
```

This creates a draft file that you can customize:

```yaml
# draft_second_order_aggregation_region_metrics.yml
kind: second_order_aggregation
name: region_metrics
description: "second order aggregation node"
id_col: region_id
feature_date_col: date
application_date_col: application_date
source: aggregation.upstream_aggregation
features:
  total_entities:
    basic: [count]
  avg_feature_value:
    basic: [mean, stddev]
    source_feature: feature_value
  weekly_total:
    window: [7, 7]
    basic: [sum]
    source_feature: daily_amount
```

---

## Part 9: Production Tips

### 9.1 Best Practices

**DO:**
- ✅ Use `dry-run` before `apply` (if using draft workflow)
- ✅ Use `--show-plan` before `run`
- ✅ Run `parse` after applying changes
- ✅ Commit `target/manifest.json` to version control
- ✅ Use descriptive names and tags
- ✅ Document complex transforms with comments in SQL
- ✅ Organize nodes with tags (`staging`, `production`, `experimental`)

**DON'T:**
- ❌ Apply files without validation
- ❌ Skip `dry-run` in production
- ❌ Ignore state in production (use incremental)
- ❌ Create circular dependencies
- ❌ Use `--full` unless necessary
- ❌ Commit `target/run_state.json` (this is runtime state, not source)

### 9.2 Project Structure Recommendations

```
my-project/
├── seeknal/
│   ├── sources/
│   │   ├── raw_users.yml
│   │   └── raw_orders.yml
│   ├── transforms/
│   │   ├── clean_users.yml
│   │   └── calculate_metrics.yml
│   ├── feature_groups/
│   │   └── user_features.yml
│   └── models/
│       └── churn_model.yml
├── data/
│   ├── users.csv
│   └── orders.csv
├── target/
│   ├── manifest.json          # Commit this
│   ├── run_state.json         # Don't commit (runtime state)
│   └── state/                 # Cache files (optional)
└── README.md
```

### 9.3 YAML File Templates

**Source Template:**

```yaml
kind: source
name: my_source
description: "Description of what this source provides"
owner: "your-team"
source: csv  # or parquet, json, postgresql, etc.
table: "path/to/file.csv"
params:
  delimiter: ","
  header: true
schema:
  - name: column_name
    data_type: string  # integer, float, date, boolean, etc.
tags:
  - raw
  - staging
```

**Transform Template:**

```yaml
kind: transform
name: my_transform
description: "What this transform does"
owner: "your-team"
transform: |
  -- Your SQL here
  SELECT
    column1,
    column2
  FROM source.my_source
  WHERE condition = 'value'
inputs:
  - ref: source.my_source
tags:
  - transformation
  - business_logic
```

**Feature Group Template:**

```yaml
kind: feature_group
name: my_features
description: "ML features for model"
owner: "ml-team"
entity:
  name: user  # Entity identifier (customer, user, product, etc.)
  join_keys: ["user_id"]  # Columns used to join features
materialization:
  event_time_col: event_timestamp  # Required: column for point-in-time joins
  offline:
    enabled: true
    format: parquet
  online:
    enabled: false
    ttl: 7d
features:
  feature_name:
    dtype: integer
  another_feature:
    dtype: float
inputs:
  - ref: transform.my_transform
tags:
  - ml
  - production
```

### 9.4 CI/CD Integration

**GitLab CI Example:**

```yaml
# .gitlab-ci.yml
stages:
  - validate
  - run

validate-pipeline:
  stage: validate
  script:
    - seeknal parse
    - seeknal run --show-plan
  only:
    - merge_requests

run-pipeline:
  stage: run
  script:
    - seeknal parse
    - seeknal run
  only:
    - main
```

**GitHub Actions Example:**

```yaml
# .github/workflows/pipeline.yml
name: Run Pipeline
on: [push]

jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'

      - name: Install Seeknal
        run: pip install seeknal

      - name: Validate Pipeline
        run: |
          seeknal parse
          seeknal run --show-plan

      - name: Run Pipeline
        run: |
          seeknal parse
          seeknal run
```

### 9.5 Working with Large Datasets

For datasets larger than ~100M rows:

1. **Use Parquet instead of CSV:**
   ```yaml
   kind: source
   name: large_data
   source: parquet
   table: "data/large_data.parquet"
   ```

2. **Filter at source level:**
   ```yaml
   params:
     # DuckDB read_parquet filters
     filters:
       - column: "date"
         operator: ">"
         value: "2024-01-01"
   ```

3. **Use incremental materialization:**
   - Process data in batches
   - Use date partitions
   - Consider Spark engine for very large datasets

### 9.6 Debugging Tips

**Check DAG structure:**

```bash
# View the manifest
cat target/manifest.json | python -m json.tool

# Look for cycles or missing dependencies
seeknal parse
```

**Inspect execution state:**

```bash
# View last run state
cat target/run_state.json | python -m json.tool

# Check specific node
cat target/run_state.json | python -m json.tool | grep -A 10 "transform.my_transform"
```

**Enable verbose output:**

```bash
# Add verbose flag (if implemented)
seeknal run --verbose
```

**Test individual transforms:**

```bash
# Use DuckDB directly to test SQL
duckdb :memory:
# Then paste your SQL to test
```

---

## Part 10: Iceberg Materialization

> **Optional Feature** - Requires Lakekeeper catalog and MinIO/S3 storage

Iceberg materialization allows you to persist pipeline results as **Apache Iceberg tables** in an object storage backend (like MinIO or S3). This enables:
- ✅ **Persistent storage** - Data survives pipeline restarts
- ✅ **ACID transactions** - Reliable, atomic writes
- ✅ **Schema evolution** - Modify schemas without breaking queries
- ✅ **Time travel** - Query historical data versions
- ✅ **Scalability** - Handle datasets larger than memory

### 10.1 What is Apache Iceberg?

Apache Iceberg is a table format for analytic datasets that brings:
- **ACID transactions** - Reliable writes with snapshot isolation
- **Schema evolution** - Add, remove, or rename columns
- **Hidden partitioning** - Automatic partition pruning
- **Partition evolution** - Change partitioning without rewriting data
- **Time travel** - Query data as it was at any point in time

**When to use Iceberg:**
- Production workloads requiring persistent storage
- Datasets that need to be shared across teams
- Requirements for schema evolution over time
- Need for time travel/audit capabilities
- Datasets larger than available memory

### 10.2 Setup Requirements

#### Lakekeeper REST Catalog

Seeknal uses Lakekeeper as the Iceberg catalog REST API server.

**Install Lakekeeper:**
```bash
# Clone Lakekeeper
git clone https://github.com/anyproto/lakekeeper.git
cd lakekeeper

# Run with Docker (quick start)
docker run -d \
  --name lakekeeper \
  -p 8181:8181 \
  -e LAKEKEEPER__LOG__RUST_LOG=debug \
  ghcr.io/anyproto/lakekeeper:latest

# Verify it's running
curl http://localhost:8181/metrics
```

#### MinIO Object Storage

MinIO provides S3-compatible object storage for Iceberg data files.

**Install MinIO:**
```bash
# Run MinIO with Docker
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Access MinIO Console
# URL: http://localhost:9001
# Username: minioadmin
# Password: minioadmin

# Create a bucket named "warehouse" in the console
```

#### Configure Environment Variables

Set these environment variables for Seeknal to connect to Lakekeeper and MinIO:

```bash
# Lakekeeper REST Catalog
export SEEKNAL_ICEBERG_CATALOG_URI="http://localhost:8181/catalog/v1"

# Lakekeeper authentication (if using OAuth2/STS)
export SEEKNAL_ICEBERG_CATALOG_CLIENT_ID="your-client-id"
export SEEKNAL_ICEBERG_CATALOG_CLIENT_SECRET="your-client-secret"
export SEEKNAL_ICEBERG_CATALOG_TOKEN_URI="https://your-keycloak.com/realms/your-realm/protocol/openid-connect/token"

# MinIO/S3 credentials (for STS temporary credentials)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_ENDPOINT="http://localhost:9000"
export AWS_REGION="us-east-1"
```

Or set them in a `.env` file:
```bash
cat > .env << 'EOF'
# Lakekeeper configuration
SEEKNAL_ICEBERG_CATALOG_URI=http://localhost:8181/catalog/v1
SEEKNAL_ICEBERG_CATALOG_CLIENT_ID=seeknal-client
SEEKNAL_ICEBERG_CATALOG_CLIENT_SECRET=your-secret
SEEKNAL_ICEBERG_CATALOG_TOKEN_URI=https://keycloak.example.com/realms/master/protocol/openid-connect/token

# MinIO credentials
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT=http://localhost:9000
AWS_REGION=us-east-1
EOF
```

### 10.3 Enable Materialization in YAML

Add a `materialization` section to any source, transform, or feature group YAML file:

```yaml
kind: source
name: customers
description: "Customer master data"
source: csv
table: "customers.csv"
# New: Iceberg materialization configuration
materialization:
  enabled: true           # Enable/disable materialization
  table: "warehouse.curated.customers"  # Iceberg table name
  mode: overwrite         # overwrite | append
```

**Materialization field breakdown:**

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `enabled` | boolean | Enable materialization for this node | `true` or `false` |
| `table` | string | Fully qualified Iceberg table name | `"warehouse.curated.customers"` |
| `mode` | string | Write mode | `overwrite` or `append` |

### 10.4 Materialization Modes

#### Overwrite Mode

**Behavior**: Drop and recreate the table on each run.

**Use when:**
- Source data is a complete snapshot
- You need to replace all existing data
- Data freshness is critical
- No need for historical versions

**Example:**
```yaml
kind: source
name: customers
description: "Daily customer snapshot"
source: csv
table: "customers.csv"
materialization:
  enabled: true
  table: "warehouse.curated.customers"
  mode: overwrite  # Replace all data
```

**Result:**
- Run 1: Table created with 100 rows
- Run 2: Table replaced with 150 rows (old 100 rows gone)
- Run 3: Table replaced with 120 rows (old 150 rows gone)

#### Append Mode

**Behavior**: Insert new data into existing table.

**Use when:**
- Processing incremental data batches
- Need to accumulate data over time
- Historical records must be preserved
- Building time-series or event logs

**Example:**
```yaml
kind: transform
name: customer_orders
description: "Customer order history"
transform: |
  SELECT * FROM source.orders
inputs:
  - ref: source.orders
materialization:
  enabled: true
  table: "warehouse.curated.customer_orders"
  mode: append  # Accumulate data
```

**Result:**
- Run 1: Table created with 5 rows
- Run 2: 5 new rows inserted (total: 10 rows)
- Run 3: 5 new rows inserted (total: 15 rows)

### 10.5 Complete Example: Materialized Pipeline

Update your tutorial YAML files to enable Iceberg materialization:

**customers.yml** (source):
```yaml
kind: source
name: customers
description: "Customer master data"
source: csv
table: "customers.csv"
materialization:
  enabled: true
  table: "warehouse.curated.customers"
  mode: append
schema:
  - name: customer_id
    data_type: integer
  - name: email
    data_type: string
  - name: country
    data_type: string
  - name: signup_date
    data_type: date
tags: []
```

**orders.yml** (source):
```yaml
kind: source
name: orders
description: "Order transactions"
source: csv
table: "orders.csv"
materialization:
  enabled: true
  table: "warehouse.curated.orders"
  mode: append
schema:
  - name: order_id
    data_type: integer
  - name: customer_id
    data_type: integer
  - name: order_date
    data_type: date
  - name: amount
    data_type: float
tags: []
```

**customer_orders.yml** (transform):
```yaml
kind: transform
name: customer_orders
description: "Join customers with their orders"
transform: |
  SELECT
    c.customer_id,
    c.email,
    c.country,
    o.order_id,
    o.order_date,
    o.amount
  FROM source.customers c
  INNER JOIN source.orders o
    ON c.customer_id = o.customer_id
inputs:
  - ref: source.customers
  - ref: source.orders
materialization:
  enabled: true
  table: "warehouse.curated.customer_orders"
  mode: overwrite
tags:
  - transformation
  - join
```

**Run the materialized pipeline:**
```bash
# Ensure Lakekeeper and MinIO are running
docker ps | grep -E "lakekeeper|minio"

# Run the pipeline
seeknal run
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
  Project: seeknal-tutorial
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 5 nodes, 4 edges
ℹ Detecting changes...
ℹ Nodes to run: 5

Execution
============================================================
1/5: customers [RUNNING]
  SUCCESS in 0.02s
  Rows: 5
  ℹ Materialized to Iceberg table: warehouse.curated.customers (5 rows)

2/5: orders [RUNNING]
  SUCCESS in 0.00s
  Rows: 5
  ℹ Materialized to Iceberg table: warehouse.curated.orders (5 rows)

3/5: active_customers [RUNNING]
  SUCCESS in 0.00s
  Rows: 3

4/5: customer_orders [RUNNING]
  SUCCESS in 0.00s
  Rows: 5
  ℹ Materialized to Iceberg table: warehouse.curated.customer_orders (5 rows)

5/5: customer_features [RUNNING]
  SUCCESS in 0.01s

✓ State saved
```

### 10.6 CLI Flags for Materialization Control

Override materialization settings from the command line:

#### Force Enable Materialization

```bash
# Enable materialization even if disabled in YAML
seeknal run --materialize
```

**Use case:** Test materialization without modifying YAML files.

#### Force Disable Materialization

```bash
# Disable materialization even if enabled in YAML
seeknal run --no-materialize
```

**Use case:** Quick testing without writing to Iceberg tables.

#### Use Node Config (Default)

```bash
# No flag - use YAML configuration
seeknal run
```

**Use case:** Production runs using configured settings.

### 10.7 Materialization Strategies

#### Strategy 1: Staging to Production

**Staging environment** (no materialization):
```bash
# Fast iteration, no storage cost
seeknal run --no-materialize
```

**Production environment** (with materialization):
```bash
# Persistent storage for production data
seeknal run --materialize
```

#### Strategy 2: Layered Data Architecture

```yaml
# Layer 1: Raw data (append mode)
kind: source
name: raw_events
materialization:
  enabled: true
  table: "warehouse.raw.events"
  mode: append  # Never delete raw data

# Layer 2: Cleaned data (overwrite mode)
kind: transform
name: clean_events
transform: |
  SELECT * FROM raw_events WHERE is_valid = true
materialization:
  enabled: true
  table: "warehouse.curated.events"
  mode: overwrite  # Replace with latest clean version

# Layer 3: Aggregations (append mode)
kind: transform
name: daily_metrics
materialization:
  enabled: true
  table: "warehouse.analytics.daily_metrics"
  mode: append  # Accumulate daily snapshots
```

#### Strategy 3: Incremental Processing

Process only new data and append to existing tables:

```yaml
kind: transform
name: new_orders_today
description: "Process today's orders"
transform: |
  SELECT * FROM source.orders
  WHERE order_date = CURRENT_DATE
materialization:
  enabled: true
  table: "warehouse.curated.orders"
  mode: append  # Add to existing data
```

### 10.8 Verifying Materialization

#### Check Iceberg Table Exists

```bash
# Using DuckDB with Iceberg extension
python << 'EOF'
import duckdb

con = duckdb.connect(':memory:')
# Load Iceberg extension
con.execute("INSTALL iceberg; LOAD iceberg;")

# Set Iceberg catalog
con.execute("SET s3_endpoint = 'http://localhost:9000';")
con.execute("SET s3_access_key_id = 'minioadmin';")
con.execute("SET s3_secret_access_key = 'minioadmin';")
con.execute("SET s3_use_ssl = false;")

# Attach Lakekeeper catalog
con.execute("""
  ATTACH 'http://localhost:8181/catalog/v1' AS iceberg (TYPE iceberg);
""")

# Query materialized table
result = con.execute("SELECT COUNT(*) FROM iceberg.warehouse.curated.customers").fetchone()
print(f"Customers table has {result[0]} rows")
EOF
```

#### Query Materialized Data

```bash
# Direct query with DuckDB
python << 'EOF'
import duckdb

con = duckdb.connect(':memory:')
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("SET s3_endpoint = 'http://localhost:9000';")
con.execute("SET s3_access_key_id = 'minioadmin';")
con.execute("SET s3_secret_access_key = 'minioadmin';")
con.execute("SET s3_use_ssl = false;")
con.execute("ATTACH 'http://localhost:8181/catalog/v1' AS iceberg (TYPE iceberg);")

# Show sample data
df = con.execute("SELECT * FROM iceberg.warehouse.curated.customers LIMIT 5").df()
print(df)
EOF
```

### 10.9 Troubleshooting Materialization

#### Problem: "Failed to connect to Lakekeeper"

**Symptoms:**
```
ERROR: Connection refused to http://localhost:8181/catalog/v1
```

**Solutions:**
```bash
# Check Lakekeeper is running
docker ps | grep lakekeeper

# Check Lakekeeper logs
docker logs lakekeeper

# Restart Lakekeeper
docker restart lakekeeper

# Verify catalog URI
echo $SEEKNAL_ICEBERG_CATALOG_URI
# Should output: http://localhost:8181/catalog/v1
```

#### Problem: "S3 credentials not available"

**Symptoms:**
```
ERROR: AWS credentials not found for materialization
```

**Solutions:**
```bash
# Check environment variables
env | grep AWS

# Set MinIO credentials
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT=http://localhost:9000
export AWS_REGION=us-east-1

# Or use .env file
cat > .env << 'EOF'
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT=http://localhost:9000
AWS_REGION=us-east-1
EOF
```

#### Problem: "Table already exists" (append mode)

**Symptoms:**
```
ERROR: Table warehouse.curated.customers already exists
```

**Solutions:**
```bash
# This shouldn't happen - append mode handles existing tables
# If it does, check your mode configuration:

# Verify YAML has mode: append
grep -A 5 "materialization:" seeknal/sources/customers.yml

# Should show:
# materialization:
#   enabled: true
#   table: "warehouse.curated.customers"
#   mode: append  # Must be append, not overwrite
```

### 10.10 Best Practices

**DO:**
- ✅ Use `append` mode for accumulating historical data
- ✅ Use `overwrite` mode for full snapshots
- ✅ Use `--no-materialize` for development/testing
- ✅ Use `--materialize` for production runs
- ✅ Organize tables in namespaces (e.g., `warehouse.raw`, `warehouse.curated`)
- ✅ Monitor storage usage in MinIO
- ✅ Set up IAM policies for secure access

**DON'T:**
- ❌ Use `overwrite` mode for data that must be preserved
- ❌ Mix append and overwrite for the same table
- ❌ Forget to set environment variables for catalog and storage
- ❌ Run large materializations without testing with `--dry-run` first
- ❌ Store sensitive credentials in YAML files (use environment variables)

### 10.11 Summary

Iceberg materialization provides:
- **Persistent storage** in object storage (MinIO/S3)
- **ACID transactions** via Lakekeeper catalog
- **Flexible modes** (append vs overwrite)
- **CLI control** for testing and production
- **Scalability** for large datasets

**Quick reference:**

| Feature | YAML Config | CLI Flag |
|---------|-------------|----------|
| Enable | `materialization.enabled: true` | `--materialize` |
| Disable | `materialization.enabled: false` | `--no-materialize` |
| Append mode | `mode: append` | (YAML only) |
| Overwrite mode | `mode: overwrite` | (YAML only) |

✅ **Checkpoint:** You now know how to persist pipeline results as Iceberg tables!

---

## Troubleshooting

### Problem: "Missing required fields for feature_group: materialization"

**Solution:**
Feature groups require a `materialization` section and proper `entity` structure:

```yaml
# Wrong - missing materialization and entity is just a string
entity: customer
features:
  ...

# Correct - includes materialization and entity object
entity:
  name: customer
  join_keys: ["customer_id"]
materialization:
  event_time_col: latest_order_date  # Required: column for point-in-time joins
  offline:
    enabled: true
    format: parquet
  online:
    enabled: false
    ttl: 7d
features:
  ...
```

The `event_time_col` should reference a date/timestamp column in your features that can be used for point-in-time joins.

### Problem: "No YAML files found"

**Solution:**
```bash
# Check you're in the right directory
pwd
# Should show your project directory

# Check seeknal/ directory exists
ls seeknal/
# Should show sources/, transforms/, feature_groups/
```

### Problem: "Cycle detected in DAG"

**Solution:**
```bash
# Check for circular dependencies
# Example: A depends on B, B depends on A

# Review inputs in your YAML files
grep -r "ref:" seeknal/
```

### Problem: "Missing dependency"

**Solution:**
```bash
# Ensure upstream nodes exist
# Example: transform refers to source.customers
# Check that seeknal/sources/customers.yml exists

ls seeknal/sources/
```

### Problem: "Table with name X does not exist"

**Solution:**
This usually means:
1. Source file path is wrong
2. Source hasn't been executed yet
3. Ref in inputs doesn't match the source name

```bash
# Verify file exists
ls customers.csv

# Check source definition
cat seeknal/sources/customers.yml

# Check ref matches
grep "ref:" seeknal/transforms/*.yml
```

### Problem: Incremental runs not working

**Solution:**
```bash
# Clear state for full refresh
rm target/run_state.json
seeknal run --full

# Or check state file
cat target/run_state.json
```

### Problem: Permission denied

**Solution:**
```bash
# Check file permissions
ls -la *.csv

# Fix permissions
chmod 644 *.csv
```

---

## Summary

Congratulations! You've learned:

✅ How to define data sources (CSV, Parquet, etc.)
✅ How to create SQL transforms
✅ How to build feature groups
✅ How to validate and preview execution
✅ How to run pipelines incrementally
✅ How to use advanced CLI flags
✅ Production best practices
✅ How to enable Iceberg materialization (optional)

### Key Commands Reference

| Command | Purpose |
|---------|---------|
| `seeknal parse` | Generate manifest from YAML files |
| `seeknal run --show-plan` | Show execution plan without running |
| `seeknal run` | Execute pipeline (incremental) |
| `seeknal run --full` | Run all nodes (ignore state) |
| `seeknal run --dry-run` | Preview without executing |
| `seeknal run --nodes <name>` | Run specific node + downstream |
| `seeknal run --types <type>` | Filter by node type |
| `seeknal run --materialize` | Force enable Iceberg materialization |
| `seeknal run --no-materialize` | Force disable Iceberg materialization |

### Next Steps

- Explore advanced node types (models, aggregations, rules)
- Learn about feature serving and online stores
- Set up scheduled runs with cron/Airflow
- Integrate with your data warehouse (Snowflake, BigQuery)
- Set up Lakekeeper and MinIO for Iceberg materialization
- Read [API Reference](../api/yaml-schema.md) for full YAML schema

### Getting Help

- Check [Documentation](../README.md)
- Review [Examples](../examples/)
- Report issues at [GitHub](https://github.com/mta-tech/seeknal/issues)

---

**Tutorial Complete!** 🎉

You now have a working YAML pipeline with incremental execution. Happy data engineering!
