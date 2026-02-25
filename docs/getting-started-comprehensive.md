# Getting Started with Seeknal

> **Estimated Time:** 30 minutes | **Difficulty:** Beginner

This comprehensive guide takes you from installation to your first materialized feature group. By the end, you'll understand how Seeknal simplifies feature engineering for ML applications.

---

## Table of Contents

- [Why Seeknal?](#why-seeknal)
- [What You'll Learn](#what-youll-learn)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Development Approaches](#development-approaches)
- [Quick Start Tutorial](#quick-start-tutorial)
  - [Part 1: Load and Explore Data](#part-1-load-and-explore-data-5-minutes)
  - [Part 2: Feature Engineering](#part-2-feature-engineering-10-minutes)
  - [Part 3: Save and Analyze Features](#part-3-save-and-analyze-features-5-minutes)
- [Core Concepts](#core-concepts)
- [DuckDB vs Spark: When to Use Each](#duckdb-vs-spark-when-to-use-each)
- [Next Steps](#next-steps)
- [Troubleshooting](#troubleshooting)

---

## Why Seeknal?

Seeknal is an all-in-one platform for data and AI/ML engineering that abstracts away the complexity of data transformation and feature engineering.

### Key Benefits

| Feature | Benefit |
|---------|---------|
| **Simple Setup** | Uses SQLite for metadata - no external database required |
| **Dual Engine Support** | DuckDB for development, Spark for production |
| **Pythonic API** | Define transformations with familiar Python and SQL |
| **Feature Reuse** | Register and share features across teams |
| **Point-in-Time Correctness** | Automatic handling to prevent data leakage |

### How Seeknal Compares

Unlike other feature stores that require complex infrastructure setup, Seeknal:

- **Starts simple**: SQLite metadata + DuckDB processing = zero infrastructure
- **Scales when needed**: Same code works with Spark for production
- **Stays flexible**: Mix and match processing engines as needed

---

## What You'll Learn

By completing this guide, you will:

1. **Install Seeknal** and initialize a project
2. **Define data sources** with YAML pipeline definitions
3. **Create transforms** using SQL-based data transformations
4. **Build feature groups** for ML-ready feature engineering
5. **Run the pipeline** with the `draft → dry-run → apply → run` workflow
6. **Explore results** using the interactive REPL

> **Note:** This tutorial uses Seeknal's **YAML pipeline workflow** — a dbt-inspired approach with CLI commands. For Python decorator-based pipelines, see the [Python Pipelines Tutorial](./tutorials/python-pipelines-tutorial.md).

---

## Prerequisites

Before starting, ensure you have:

| Requirement | Version | Check Command |
|-------------|---------|---------------|
| Python | 3.11 or higher | `python --version` |
| pip | Latest | `pip --version` |
| uv (recommended) | Latest | `uv --version` |

### Optional but Recommended

- **Jupyter Notebook** - For interactive exploration
- **VS Code** or **PyCharm** - For running Python scripts

---

## Installation

> **Estimated Time:** 5 minutes

Choose your preferred installation method based on your operating system.

### Install from PyPI

```bash
pip install seeknal
```

This installs Seeknal and all required dependencies (DuckDB, pandas, pyarrow, etc.).

### Verify Installation

Run this command to verify Seeknal is installed correctly:

```bash
python -c "from seeknal.tasks.duckdb import DuckDBTask; print('Seeknal installed successfully!')"
```

**Expected Output:**
```
Seeknal installed successfully!
```

### Configure Environment (Optional)

Seeknal works out of the box with sensible defaults. Configuration is **not required** for this tutorial.

For advanced usage (custom config location, database backends, or team environments), you can override the default paths:

```bash
# Optional: Override default config directory (default: ~/.seeknal/)
export SEEKNAL_BASE_CONFIG_PATH="$HOME/.seeknal"

# Optional: Override user config file (default: ~/.seeknal/config.toml)
export SEEKNAL_USER_CONFIG_PATH="$HOME/.seeknal/config.toml"
```

**When would you need this?**

- **Custom install paths** — if your home directory is not writable or you want config in a shared location
- **Multiple environments** — separate configs for dev/staging/prod via different `SEEKNAL_BASE_CONFIG_PATH` values
- **CI/CD pipelines** — point to ephemeral config directories

Seeknal auto-creates `~/.seeknal/` and its SQLite database on first use, so most users never need to set these.

---

## Quick Start Tutorial

> **Total Estimated Time:** 15 minutes

In this tutorial, you'll build a data pipeline that loads CSV data, cleans it, and creates user features — all using YAML definitions and the CLI.

### Step 1: Initialize a Project (2 minutes)

```bash
seeknal init --name ecommerce_analytics --description "E-commerce user features"
cd ecommerce_analytics
```

This creates the project structure:

```
ecommerce_analytics/
├── seeknal_project.yml    # Project configuration
├── profiles.yml           # Connection credentials (gitignored)
├── seeknal/
│   ├── sources/           # Data source definitions
│   ├── transforms/        # SQL transformations
│   └── feature_groups/    # Feature engineering
└── target/                # Pipeline outputs
```

### Step 2: Create a Data Source (3 minutes)

Generate a source template:

```bash
seeknal draft source raw_orders --description "Raw e-commerce orders"
```

Edit the generated `draft_source_raw_orders.yml`:

```yaml
kind: source
name: raw_orders
description: "Raw e-commerce order data"
tags: ["orders", "ecommerce"]
source: csv
table: "data/orders.csv"
columns:
  order_id: "Unique order identifier"
  customer_id: "Customer who placed the order"
  order_date: "Date the order was placed"
  product: "Product name"
  quantity: "Number of items"
  price: "Unit price"
  total: "Order total amount"
```

Validate and preview:

```bash
seeknal dry-run draft_source_raw_orders.yml
```

Apply to register the source:

```bash
seeknal apply draft_source_raw_orders.yml
```

### Step 3: Create a Transform (3 minutes)

Generate a transform template:

```bash
seeknal draft transform clean_orders --description "Clean and validate orders"
```

Edit `draft_transform_clean_orders.yml`:

```yaml
kind: transform
name: clean_orders
description: "Remove invalid orders and add computed columns"
input:
  - ref('source.raw_orders')
query: |
  SELECT
      order_id,
      customer_id,
      CAST(order_date AS DATE) AS order_date,
      product,
      quantity,
      price,
      total,
      (quantity * price) AS computed_total
  FROM input_0
  WHERE
      quantity > 0
      AND price > 0
      AND customer_id IS NOT NULL
```

Apply it:

```bash
seeknal dry-run draft_transform_clean_orders.yml
seeknal apply draft_transform_clean_orders.yml
```

### Step 4: Create a Feature Group (3 minutes)

Generate a feature group template:

```bash
seeknal draft feature-group customer_features --description "Customer purchase features"
```

Edit `draft_feature_group_customer_features.yml`:

```yaml
kind: feature_group
name: customer_features
description: "Customer-level purchase behavior features"
entity: customer
entity_keys:
  - customer_id
input:
  - ref('transform.clean_orders')
query: |
  SELECT
      customer_id,
      COUNT(DISTINCT order_id) AS order_count,
      SUM(total) AS total_spend,
      AVG(total) AS avg_order_value,
      MAX(total) AS max_order_value,
      MIN(order_date) AS first_order_date,
      MAX(order_date) AS last_order_date,
      COUNT(DISTINCT product) AS unique_products
  FROM input_0
  GROUP BY customer_id
materialization:
  format: parquet
```

Apply it:

```bash
seeknal dry-run draft_feature_group_customer_features.yml
seeknal apply draft_feature_group_customer_features.yml
```

### Step 5: Run the Pipeline (2 minutes)

Preview the execution plan, then run:

```bash
# See what will be executed
seeknal run --dry-run

# Execute the full pipeline
seeknal run
```

**Expected output:**

```
Seeknal Pipeline Execution
============================================================
ℹ Building DAG from seeknal/ directory...
✓ DAG built: 3 nodes, 2 edges

Execution Plan:
   1. RUN raw_orders [orders, ecommerce]
   2. RUN clean_orders
   3. RUN customer_features

Execution
============================================================
1/3: raw_orders [RUNNING]
  SUCCESS in 0.5s — Rows: 1,000

2/3: clean_orders [RUNNING]
  SUCCESS in 0.3s — Rows: 985

3/3: customer_features [RUNNING]
  SUCCESS in 0.4s — Rows: 200

✓ All nodes executed successfully
```

### Step 6: Explore Results (2 minutes)

Use the interactive REPL to query your pipeline outputs:

```bash
seeknal repl
```

```sql
-- The REPL auto-registers all pipeline outputs
-- Query your feature group directly:
SELECT * FROM customer_features ORDER BY total_spend DESC LIMIT 5;

-- Explore the intermediate transform:
SELECT * FROM clean_orders LIMIT 10;

-- Run ad-hoc analysis:
SELECT
    CASE
        WHEN order_count = 1 THEN 'One-time'
        WHEN order_count <= 3 THEN 'Occasional'
        ELSE 'Frequent'
    END AS segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spend), 2) AS avg_spend
FROM customer_features
GROUP BY 1
ORDER BY customer_count DESC;
```

Output files are in `target/intermediate/`:

```bash
ls target/intermediate/
# feature_group_customer_features.parquet
# transform_clean_orders.parquet
# source_raw_orders.parquet
```

---

## Core Concepts

### Pipeline Nodes

Seeknal pipelines are built from **nodes** — YAML or Python files that define each step of your data workflow:

| Node Type | Purpose | Example |
|-----------|---------|---------|
| **Source** | Load raw data (CSV, PostgreSQL, etc.) | `kind: source` |
| **Transform** | SQL-based data transformations | `kind: transform` |
| **Feature Group** | ML feature engineering with entity keys | `kind: feature_group` |
| **Model** | ML model training/inference | `kind: model` |
| **Exposure** | Export results to external systems | `kind: exposure` |

### DAG (Directed Acyclic Graph)

Nodes are connected via `ref()` references, forming a DAG that Seeknal executes in dependency order:

```
source.raw_orders → transform.clean_orders → feature_group.customer_features
```

### CLI Workflow

The core workflow follows a dbt-inspired pattern:

```bash
seeknal draft <type> <name>   # Generate a YAML template
seeknal dry-run <file>        # Validate and preview
seeknal apply <file>          # Register the node
seeknal run                   # Execute the full DAG
seeknal repl                  # Query results interactively
```

### Materialization

Pipeline outputs are saved as Parquet files by default. You can also materialize to:

- **PostgreSQL** — `type: postgresql` with modes: `full`, `incremental_by_time`, `upsert_by_key`
- **Apache Iceberg** — `type: iceberg` for lakehouse storage with time travel

### Environments

Isolate dev/staging/prod with environment-aware execution:

```bash
seeknal env plan dev          # Create a dev environment plan
seeknal env apply dev         # Execute with environment isolation
```

---

## Using Iceberg Storage

**Apache Iceberg** provides ACID transactions, time travel, and cloud-native storage for feature groups. With Iceberg, you can store features in S3, GCS, or Azure Blob storage while maintaining full version history and rollback capabilities.

### Prerequisites

For Iceberg storage, you need:
- **DuckDB** with Iceberg extension (included with Seeknal)
- **REST Catalog** (e.g., Lakekeeper, Hive Metastore)
- **Cloud storage** (S3, GCS, Azure) or local filesystem

### Configure Catalog

Create or update `~/.seeknal/profiles.yml`:

```yaml
materialization:
  catalog:
    uri: http://localhost:8181  # Lakekeeper REST catalog
    warehouse: s3://my-bucket/warehouse
    bearer_token: optional_token  # If auth required
```

Or use environment variables:
```bash
export LAKEKEEPER_URI=http://localhost:8181
export LAKEKEEPER_WAREHOUSE=s3://my-bucket/warehouse
```

### Create Feature Group with Iceberg

```python
from seeknal.featurestore import (
    FeatureGroup,
    Materialization,
    OfflineMaterialization,
    OfflineStore,
    OfflineStoreEnum,
    IcebergStoreOutput,
)
from seeknal.entity import Entity
from datetime import datetime
import pandas as pd

# Create sample data
df = pd.DataFrame({
    "customer_id": ["A001", "A002", "A003"],
    "event_date": [datetime(2024, 1, 1)] * 3,
    "total_orders": [5, 10, 3],
    "total_spend": [100.0, 250.0, 75.0],
})

# Create entity
customer_entity = Entity(name="customer", join_keys=["customer_id"])

# Create feature group with Iceberg storage
fg = FeatureGroup(
    name="customer_features",
    entity=customer_entity,
    materialization=Materialization(
        event_time_col="event_date",
        offline=True,
        offline_materialization=OfflineMaterialization(
            store=OfflineStore(
                kind=OfflineStoreEnum.ICEBERG,  # Use Iceberg!
                value=IcebergStoreOutput(
                    catalog="lakekeeper",        # Catalog name from profiles.yml
                    namespace="prod",            # Iceberg namespace (database)
                    table="customer_features",   # Table name
                    mode="append"               # "append" or "overwrite"
                )
            ),
            mode="append"
        )
    )
)

# Write features to Iceberg
fg.set_dataframe(df).set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))

# Features are now in Iceberg table: lakekeeper.prod.customer_features
```

### Query Iceberg Tables

After writing, query features with DuckDB or Spark:

```python
import duckdb

# Query Iceberg table directly
con = duckdb.connect(":memory:")

# Load Iceberg extension
con.install_extension("iceberg")
con.load_extension("iceberg")

# Attach REST catalog
con.execute(f"""
    ATTACH 'http://localhost:8181' AS iceberg_catalog (
        TYPE iceberg,
        WAREHOUSE 's3://my-bucket/warehouse'
    )
""")

# Query features
result = con.execute("""
    SELECT customer_id, total_orders, total_spend
    FROM iceberg_catalog.prod.customer_features
""").fetchall()

print(result)  # [('A001', 5, 100.0), ('A002', 10, 250.0), ...]
```

### Time Travel with Iceberg

Iceberg supports time travel - query features as of any snapshot:

```python
# Get snapshot ID from write result
result = fg.write(feature_start_time=datetime(2024, 1, 1))
snapshot_id = result["snapshot_id"]

# Query as of specific snapshot
con.execute(f"USE SNAPSHOT '{snapshot_id}'")
historical_features = con.execute("""
    SELECT * FROM iceberg_catalog.prod.customer_features
""").fetchall()
```

### Write Modes

**Append Mode** (default):
```python
IcebergStoreOutput(table="features", mode="append")
# Adds new rows to existing table
```

**Overwrite Mode**:
```python
IcebergStoreOutput(table="features", mode="overwrite")
# Replaces table data (keeps schema history)
```

### Benefits of Iceberg Storage

| Feature | Benefit |
|---------|---------|
| **ACID Transactions** | Atomic writes, no partial data |
| **Time Travel** | Query features as of any point in time |
| **Schema Evolution** | Add/modify features without rewrites |
| **Cloud Storage** | S3, GCS, Azure Blob support |
| **Compatibility** | Works with DuckDB, Spark, Trino, and more |
| **Partitioning** | Efficient data pruning for large datasets |

---

## Next Steps

### Tutorials

- **[YAML Pipeline Tutorial](./tutorials/yaml-pipeline-tutorial.md)** — Deep dive into the YAML workflow with a complete e-commerce dataset
- **[Python Pipelines Tutorial](./tutorials/python-pipelines-tutorial.md)** — Build pipelines using Python decorators (`@source`, `@transform`, `@feature_group`)
- **[E-commerce Workflow Tutorial](./tutorials/workflow-tutorial-ecommerce.md)** — Full draft/dry-run/apply workflow with 5 tables

### Guides

- **[Python Pipelines Guide](./guides/python-pipelines.md)** — Decorator API reference and patterns
- **[Training to Serving](./guides/training-to-serving.md)** — End-to-end ML feature lifecycle
- **[Testing and Audits](./guides/testing-and-audits.md)** — Data quality validation
- **[Semantic Layer](./guides/semantic-layer.md)** — Metrics and materialized views

### Reference

- **[CLI Reference](./reference/cli.md)** — All CLI commands and options
- **[YAML Schema Reference](./reference/yaml-schema.md)** — Complete YAML node specification
- **[Configuration](./reference/configuration.md)** — Profiles, connections, and settings

### Community

- **GitHub**: [mta-tech/seeknal](https://github.com/mta-tech/seeknal)
- **Issues**: Report bugs or request features
- **PyPI**: `pip install seeknal`

---

## Troubleshooting

### Installation Issues

#### "ModuleNotFoundError: No module named 'seeknal'"

**Cause:** Seeknal is not installed in your active Python environment.

**Solution:**
```bash
# Check which Python you're using
which python

# Ensure virtual environment is activated
source .venv/bin/activate  # Linux/macOS
.\.venv\Scripts\activate   # Windows

# Reinstall Seeknal
pip install seeknal
```

#### "ImportError: cannot import name 'DuckDBTask'"

**Cause:** Incorrect import or outdated Seeknal version.

**Solution:**
```bash
# Verify Seeknal version
pip show seeknal

# Update to latest version if needed
pip install --upgrade seeknal
```

### Runtime Issues

#### "FileNotFoundError" for CSV source

**Cause:** The `table:` path in your source YAML doesn't point to an existing file.

**Solution:**
```bash
# Check the path in your source YAML
# Use absolute paths or paths relative to the project root
ls -la data/orders.csv
```

#### "ref not found" in dry-run

**Cause:** The referenced upstream node hasn't been applied yet.

**Solution:**
```bash
# Apply nodes in dependency order
seeknal apply draft_source_raw_orders.yml       # Apply source first
seeknal apply draft_transform_clean_orders.yml   # Then transform
```

### Getting Help

If you're still stuck:

1. **Check the logs** — Look for error messages in the console
2. **Use `seeknal docs`** — Search built-in documentation from the CLI
3. **Search GitHub Issues** — Someone may have solved your problem
4. **Open a new issue** — Include your Python version, OS, and error message

---

## Summary

Congratulations! You've completed the Seeknal Getting Started guide. You now know how to:

- Install Seeknal from PyPI
- Initialize a project with `seeknal init`
- Define sources, transforms, and feature groups in YAML
- Use the `draft → dry-run → apply → run` workflow
- Query results with `seeknal repl`

**Ready for more?** Try the [YAML Pipeline Tutorial](./tutorials/yaml-pipeline-tutorial.md) for a deeper walkthrough, or the [Python Pipelines Tutorial](./tutorials/python-pipelines-tutorial.md) for decorator-based pipelines.

---

*Last updated: February 2026 | Seeknal Documentation*
