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

## Development Approaches

Seeknal supports two development workflows. Choose based on your preference:

### Approach 1: CLI-Based Workflow (Recommended for Teams)

The **draft workflow** provides a structured, team-friendly approach:

```bash
# 1. Create a draft with scaffolding
seeknal draft source raw_data --description "Raw customer data"

# 2. Edit the generated template
# Edit draft_source_raw_data.yml

# 3. Preview what will happen
seeknal dry-run draft_source_raw_data.yml

# 4. Apply to create the actual source
seeknal apply draft_source_raw_data.yml
```

**Benefits:**
- Validated templates prevent errors
- Preview changes before applying
- Git-friendly YAML files
- Works with CI/CD pipelines
- **If you're coming from dbt, this will feel familiar!**

**When to use:** Team collaboration, production pipelines, GitOps workflows

**Learn more:** [Workflow Tutorial](./tutorials/workflow-tutorial-ecommerce.md)

### Approach 2: Python API (This Tutorial)

The **Python API** provides programmatic control:

```python
from seeknal.tasks.duckdb import DuckDBTask

# Create task and define transformation
task = DuckDBTask()
task.add_input(dataframe=arrow_table)
task.add_sql("SELECT user_id, SUM(amount) FROM __THIS__ GROUP BY user_id")
result = task.transform()
```

**Benefits:**
- Full Python flexibility
- Jupyter Notebook compatible
- Interactive development
- Direct debugging

**When to use:** Exploratory analysis, notebooks, data science workflows

**Learn more:** Continue with this tutorial

### Choosing Your Approach

| Factor | CLI Workflow | Python API |
|--------|--------------|------------|
| **Team size** | Multiple developers | Individual or small team |
| **Deployment** | Production pipelines | Notebooks & scripts |
| **Version control** | Git-friendly YAML | Standard Python |
| **Learning curve** | Moderate | Low (if you know Python) |
| **Use case** | Production features | Exploratory analysis |

---

## What You'll Learn

By completing this guide, you will:

1. **Install Seeknal** on your local machine
2. **Load data** using DuckDBTask for local processing
3. **Engineer features** with SQL transformations
4. **Aggregate user behavior** into ML-ready features
5. **Save features** in efficient formats (Parquet, CSV)
6. **Understand** when to use DuckDB vs Spark

> **Note:** This tutorial teaches Seeknal's **Python API** for programmatic feature engineering. If you prefer a **CLI-based workflow** with YAML definitions and the `draft → dry-run → apply` pattern, see the [Workflow Tutorial](./tutorials/workflow-tutorial-ecommerce.md). Both approaches are valid - choose based on your workflow preference.

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

### Method 1: Using uv (Recommended)

[uv](https://docs.astral.sh/uv/) is a fast Python package manager. This is the recommended approach.

#### macOS / Linux

```bash
# Install uv if you don't have it
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create and activate a virtual environment
uv venv --python 3.11
source .venv/bin/activate

# Download Seeknal from releases
# Visit: https://github.com/mta-tech/seeknal/releases
# Download the latest .whl file

# Install Seeknal
uv pip install seeknal-<version>-py3-none-any.whl

# Install additional dependencies for this tutorial
uv pip install pandas pyarrow
```

#### Windows (PowerShell)

```powershell
# Install uv if you don't have it
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Create and activate a virtual environment
uv venv --python 3.11
.\.venv\Scripts\activate

# Download Seeknal from releases
# Visit: https://github.com/mta-tech/seeknal/releases
# Download the latest .whl file

# Install Seeknal
uv pip install seeknal-<version>-py3-none-any.whl

# Install additional dependencies for this tutorial
uv pip install pandas pyarrow
```

### Method 2: Using pip

If you prefer standard pip:

#### macOS / Linux

```bash
# Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Download and install Seeknal
# Visit: https://github.com/mta-tech/seeknal/releases
pip install seeknal-<version>-py3-none-any.whl

# Install additional dependencies
pip install pandas pyarrow
```

#### Windows (PowerShell)

```powershell
# Create a virtual environment
python -m venv .venv
.\.venv\Scripts\activate

# Upgrade pip
pip install --upgrade pip

# Download and install Seeknal
# Visit: https://github.com/mta-tech/seeknal/releases
pip install seeknal-<version>-py3-none-any.whl

# Install additional dependencies
pip install pandas pyarrow
```

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

For advanced usage, create a configuration file:

```bash
# Create a config directory
mkdir -p ~/.seeknal

# Set environment variables (add to your .bashrc or .zshrc)
export SEEKNAL_BASE_CONFIG_PATH="$HOME/.seeknal"
export SEEKNAL_USER_CONFIG_PATH="$HOME/.seeknal/config.toml"
```

For this quickstart tutorial, configuration is **not required** - Seeknal works out of the box with sensible defaults.

---

## Quick Start Tutorial

> **Total Estimated Time:** 20 minutes

In this tutorial, you'll build user behavior features from e-commerce data. These features could be used for:

- **Recommendation systems** - Suggest products based on user preferences
- **Churn prediction** - Identify users likely to leave
- **Customer segmentation** - Group users by behavior patterns

### Sample Dataset Overview

We'll use a sample dataset (`sample_data.csv`) containing e-commerce user behavior:

| Column | Type | Description |
|--------|------|-------------|
| `user_id` | string | Unique user identifier |
| `event_time` | datetime | When the event occurred |
| `product_category` | string | Category of product (Electronics, Clothing, etc.) |
| `action_type` | string | User action (view, add_to_cart, purchase, etc.) |
| `device_type` | string | Device used (mobile, desktop, tablet) |
| `purchase_amount` | float | Purchase value (0.0 if no purchase) |
| `session_duration` | int | Session length in seconds |
| `items_viewed` | int | Number of items viewed |
| `cart_value` | float | Current cart value |

---

### Part 1: Load and Explore Data (5 minutes)

> **Goal:** Load sample data and understand its structure

#### Step 1.1: Get the Sample Data

First, download the quickstart examples:

```bash
# Clone the examples (or download from GitHub)
git clone https://github.com/mta-tech/seeknal.git
cd seeknal/examples/quickstart
```

Or create the sample data manually by copying from the examples directory.

#### Step 1.2: Create Your First Script

Create a new file called `my_first_features.py`:

```python
#!/usr/bin/env python3
"""
My First Seeknal Features
=========================
This script demonstrates loading data and creating features with Seeknal.
"""

import pandas as pd
import pyarrow as pa
from seeknal.tasks.duckdb import DuckDBTask

# Step 1: Load the sample data
print("Loading sample data...")
df = pd.read_csv("sample_data.csv")

print(f"Loaded {len(df):,} rows")
print(f"Columns: {list(df.columns)}")
print("\nFirst 5 rows:")
print(df.head())
```

#### Step 1.3: Run and Verify

```bash
python my_first_features.py
```

**Expected Output:**
```
Loading sample data...
Loaded 750 rows
Columns: ['user_id', 'event_time', 'product_category', 'action_type', 'device_type', 'purchase_amount', 'session_duration', 'items_viewed', 'cart_value']

First 5 rows:
   user_id           event_time product_category action_type device_type  purchase_amount  session_duration  items_viewed  cart_value
  user_089  2024-01-01 03:49:27           Sports        view      tablet              0.0              1090             8       10.04
  user_008  2024-01-01 06:19:13    Home & Garden    purchase      mobile            68.78              1048            12      355.90
  ...
```

**Checkpoint:** You should see 750 rows with 9 columns. If not, check that `sample_data.csv` is in the same directory.

---

### Part 2: Feature Engineering (10 minutes)

> **Goal:** Transform raw events into user-level features

#### Step 2.1: Create a DuckDB Task

Add the following to your script:

```python
# Step 2: Create a DuckDB Task
print("\nCreating DuckDB task...")

# Convert pandas DataFrame to PyArrow Table (required for DuckDBTask)
arrow_table = pa.Table.from_pandas(df)

# Create the task and add input data
duckdb_task = DuckDBTask()
duckdb_task.add_input(dataframe=arrow_table)

print(f"Task created: {duckdb_task.kind}")
print(f"Uses Spark: {duckdb_task.is_spark_job}")  # False - we're using DuckDB
```

**Why PyArrow?** DuckDB works with Apache Arrow tables for efficient columnar processing. Converting from pandas is straightforward with `pa.Table.from_pandas()`.

#### Step 2.2: Define Feature Engineering SQL

Now, define the SQL to aggregate user behavior into features:

```python
# Step 3: Define feature engineering SQL
user_features_sql = """
SELECT
    user_id,

    -- Transaction counts
    COUNT(*) as total_events,
    COUNT(CASE WHEN action_type = 'purchase' THEN 1 END) as total_purchases,
    COUNT(CASE WHEN action_type = 'view' THEN 1 END) as total_views,
    COUNT(CASE WHEN action_type = 'add_to_cart' THEN 1 END) as total_cart_adds,

    -- Revenue metrics
    SUM(purchase_amount) as total_revenue,
    AVG(purchase_amount) as avg_purchase_amount,
    MAX(purchase_amount) as max_purchase_amount,

    -- Engagement metrics
    AVG(session_duration) as avg_session_duration,
    SUM(items_viewed) as total_items_viewed,
    AVG(items_viewed) as avg_items_per_session,

    -- Cart behavior
    AVG(cart_value) as avg_cart_value,
    MAX(cart_value) as max_cart_value,

    -- Device preferences
    COUNT(CASE WHEN device_type = 'mobile' THEN 1 END) as mobile_sessions,
    COUNT(CASE WHEN device_type = 'desktop' THEN 1 END) as desktop_sessions,
    COUNT(CASE WHEN device_type = 'tablet' THEN 1 END) as tablet_sessions,

    -- Category preferences
    MODE(product_category) as favorite_category,
    COUNT(DISTINCT product_category) as categories_explored,

    -- Time range
    MIN(event_time) as first_event_time,
    MAX(event_time) as last_event_time

FROM __THIS__
GROUP BY user_id
ORDER BY total_revenue DESC
"""

# Add SQL to the task
duckdb_task.add_sql(user_features_sql)
print("\nFeature SQL added to pipeline")
```

**Key Points:**
- `__THIS__` is a placeholder that refers to the input data
- SQL runs on DuckDB, supporting advanced functions like `MODE()`
- Features are aggregated at the user level (`GROUP BY user_id`)

#### Step 2.3: Execute the Transformation

```python
# Step 4: Execute the transformation
print("\nExecuting feature transformation...")
result_arrow = duckdb_task.transform()

# Convert back to pandas for inspection
result_df = result_arrow.to_pandas()

print(f"Created features for {len(result_df):,} unique users")
print(f"Number of features: {len(result_df.columns)}")
print("\nFeature columns:")
for col in result_df.columns:
    print(f"  - {col}")
```

**Expected Output:**
```
Executing feature transformation...
Created features for 100 unique users
Number of features: 19

Feature columns:
  - user_id
  - total_events
  - total_purchases
  - total_views
  - total_cart_adds
  - total_revenue
  ...
```

---

### Part 3: Save and Analyze Features (5 minutes)

> **Goal:** Persist features and run analysis

#### Step 3.1: Save Features to Files

```python
# Step 5: Save features
from pathlib import Path

output_dir = Path("output")
output_dir.mkdir(exist_ok=True)

# Save as Parquet (efficient for ML pipelines)
parquet_path = output_dir / "user_features.parquet"
result_df.to_parquet(parquet_path, index=False)
print(f"\nSaved Parquet: {parquet_path}")

# Save as CSV (human-readable)
csv_path = output_dir / "user_features.csv"
result_df.to_csv(csv_path, index=False)
print(f"Saved CSV: {csv_path}")
```

#### Step 3.2: Analyze Your Features

```python
# Step 6: Analyze the features
print("\n" + "="*50)
print(" Feature Analysis")
print("="*50)

# Top users by revenue
print("\nTop 5 Users by Revenue:")
print(result_df[['user_id', 'total_purchases', 'total_revenue', 'favorite_category']].head())

# Feature statistics
print("\nFeature Statistics:")
stats_cols = ['total_events', 'total_purchases', 'total_revenue', 'avg_session_duration']
print(result_df[stats_cols].describe().round(2))
```

#### Step 3.3: Create User Segments (Bonus)

Run additional analysis using DuckDB:

```python
# Step 7: Create user segments
print("\n" + "="*50)
print(" User Segmentation")
print("="*50)

# Create a new task for segmentation
segment_task = DuckDBTask()
segment_task.add_input(dataframe=pa.Table.from_pandas(result_df))

segment_sql = """
SELECT
    CASE
        WHEN total_purchases = 0 THEN 'Browser'
        WHEN total_purchases = 1 THEN 'One-time Buyer'
        WHEN total_purchases <= 3 THEN 'Occasional Buyer'
        ELSE 'Frequent Buyer'
    END as segment,
    COUNT(*) as user_count,
    ROUND(AVG(total_revenue), 2) as avg_revenue,
    ROUND(AVG(avg_session_duration), 0) as avg_session_secs
FROM __THIS__
GROUP BY 1
ORDER BY user_count DESC
"""

segment_task.add_sql(segment_sql)
segments = segment_task.transform().to_pandas()

print("\nUser Segments:")
print(segments.to_string(index=False))
```

**Expected Output:**
```
User Segments:
         segment  user_count  avg_revenue  avg_session_secs
         Browser          45         0.00               850
  One-time Buyer          28       125.50               920
Occasional Buyer          20       380.25              1050
  Frequent Buyer           7       890.00              1180
```

---

### Complete Script

Here's the complete script for reference:

```python
#!/usr/bin/env python3
"""
Complete Seeknal Quickstart Script
==================================
Creates user behavior features from e-commerce data.
"""

import pandas as pd
import pyarrow as pa
from pathlib import Path
from seeknal.tasks.duckdb import DuckDBTask

def main():
    # Load data
    print("Loading sample data...")
    df = pd.read_csv("sample_data.csv")
    print(f"Loaded {len(df):,} rows")

    # Create DuckDB task
    print("\nCreating DuckDB task...")
    arrow_table = pa.Table.from_pandas(df)
    duckdb_task = DuckDBTask()
    duckdb_task.add_input(dataframe=arrow_table)

    # Define features
    user_features_sql = """
    SELECT
        user_id,
        COUNT(*) as total_events,
        COUNT(CASE WHEN action_type = 'purchase' THEN 1 END) as total_purchases,
        SUM(purchase_amount) as total_revenue,
        AVG(session_duration) as avg_session_duration,
        MODE(product_category) as favorite_category,
        COUNT(DISTINCT product_category) as categories_explored
    FROM __THIS__
    GROUP BY user_id
    ORDER BY total_revenue DESC
    """

    duckdb_task.add_sql(user_features_sql)

    # Execute
    print("\nExecuting transformation...")
    result = duckdb_task.transform().to_pandas()
    print(f"Created features for {len(result):,} users")

    # Save
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    result.to_parquet(output_dir / "user_features.parquet", index=False)
    result.to_csv(output_dir / "user_features.csv", index=False)
    print(f"\nFeatures saved to {output_dir}/")

    # Display results
    print("\nTop 5 users by revenue:")
    print(result.head())

if __name__ == "__main__":
    main()
```

---

## Core Concepts

### Tasks

**Tasks** are the building blocks of Seeknal pipelines. They define transformations on data.

```python
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.sparkengine import SparkEngineTask

# DuckDB for local development
duckdb_task = DuckDBTask()
duckdb_task.add_input(dataframe=arrow_table)
duckdb_task.add_sql("SELECT * FROM __THIS__ WHERE value > 0")

# Spark for production (same SQL works!)
spark_task = SparkEngineTask()
spark_task.add_sql("SELECT * FROM __THIS__ WHERE value > 0")
```

### Flows

**Flows** chain multiple tasks together into a pipeline:

```python
from seeknal.flow import Flow, FlowInput, FlowOutput

flow = Flow(
    name="my_feature_pipeline",
    input=FlowInput(kind="hive_table", value="raw_events"),
    tasks=[task1, task2, task3],
    output=FlowOutput()
)

# Save and reuse the flow
flow.get_or_create()
result = flow.run()
```

### Feature Groups

**Feature Groups** are collections of related features with metadata:

```python
from seeknal.entity import Entity
from seeknal.featurestore.feature_group import FeatureGroup

# Define the entity (primary key)
entity = Entity(name="user", join_keys=["user_id"])

# Create feature group
fg = FeatureGroup(
    name="user_behavior_features",
    entity=entity,
    description="User behavior features for ML models"
)

# Attach transformation and register features
fg.set_flow(my_flow)
fg.set_features()
fg.get_or_create()
```

### Projects and Workspaces

Organize your work with **Projects** and **Workspaces**:

```python
from seeknal.project import Project
from seeknal.workspace import Workspace

# Create a project
project = Project(name="ecommerce_ml", description="E-commerce ML features")
project.get_or_create()

# Create a workspace within the project
workspace = Workspace(name="user_features")
workspace.get_or_create()
```

---

## DuckDB vs Spark: When to Use Each

| Aspect | DuckDB | Spark |
|--------|--------|-------|
| **Best For** | Development, prototyping, small data | Production, large scale |
| **Data Size** | < 10GB | Unlimited |
| **Setup** | None (embedded) | Cluster required |
| **Speed** | Fast for small data | Fast for large data |
| **Use Case** | Local feature development | Production materialization |

### Development Workflow

1. **Develop with DuckDB** - Fast iteration, no infrastructure
2. **Test locally** - Verify logic and outputs
3. **Switch to Spark** - Same SQL, production scale

```python
# Development (DuckDB)
from seeknal.tasks.duckdb import DuckDBTask
task = DuckDBTask()
task.add_sql("SELECT user_id, SUM(amount) FROM __THIS__ GROUP BY user_id")

# Production (Spark) - Same SQL!
from seeknal.tasks.sparkengine import SparkEngineTask
task = SparkEngineTask()
task.add_sql("SELECT user_id, SUM(amount) FROM __THIS__ GROUP BY user_id")
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

### Choose Your Path

**For production-ready, team-based development:**
- **[Workflow Tutorial: E-commerce Analytics Pipeline](./tutorials/workflow-tutorial-ecommerce.md)** - Learn the `draft → dry-run → apply` CLI workflow for creating production-grade pipelines with YAML definitions.

**For continued Python API learning:**
- **[Spark Transformers Reference](./spark-transformers-reference.md)** - Production-scale feature engineering
- **[Feature Store Examples](./examples/featurestore.md)** - Complete feature store workflow
- **[Python Pipelines Tutorial](./tutorials/python-pipelines-tutorial.md)** - Advanced Python pipeline patterns

### Learn More

-- **[API Reference](https://github.com/mta-tech/seeknal)** - Full documentation
-- **[Spark Transformers Reference](./spark-transformers-reference.md)** - Production-scale feature engineering
-- **[Feature Store Examples](./examples/featurestore.md)** - Complete feature store workflow
-- **[API Reference](./api/)** - Full API documentation
-- **[Workflow Guides](./guides/)** - Advanced workflow features


### Try These Exercises

1. **Add more features** - Extend the SQL to include day-of-week patterns
2. **Create time windows** - Calculate 7-day and 30-day rolling metrics
3. **Use Spark** - Run the same features with SparkEngineTask
4. **Build a model** - Use the features in a scikit-learn classifier

### Join the Community

- **GitHub**: [mta-tech/seeknal](https://github.com/mta-tech/seeknal)
- **Issues**: Report bugs or request features
- **Discussions**: Ask questions and share ideas

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
pip install seeknal-<version>-py3-none-any.whl
```

#### "ImportError: cannot import name 'DuckDBTask'"

**Cause:** Incorrect import or outdated Seeknal version.

**Solution:**
```bash
# Verify Seeknal version
pip show seeknal

# Update to latest version if needed
pip install --upgrade seeknal-<version>-py3-none-any.whl
```

### Runtime Issues

#### "FileNotFoundError: sample_data.csv"

**Cause:** Script can't find the sample data file.

**Solution:**
```bash
# Ensure you're in the correct directory
cd examples/quickstart

# Verify the file exists
ls -la sample_data.csv

# Or use absolute path in your script
df = pd.read_csv("/full/path/to/sample_data.csv")
```

#### "pyarrow.lib.ArrowInvalid: Could not convert..."

**Cause:** Data type mismatch when converting to Arrow.

**Solution:**
```python
# Ensure clean data types before conversion
df = df.fillna(0)  # Handle null values
df['column'] = df['column'].astype(str)  # Explicit type conversion

# Then convert to Arrow
arrow_table = pa.Table.from_pandas(df)
```

### Performance Issues

#### "Transformation is slow"

**Cause:** Large dataset or complex SQL.

**Solutions:**
1. **Sample your data** during development:
   ```python
   df_sample = df.sample(n=10000)
   ```

2. **Optimize SQL** - Add filters early:
   ```sql
   SELECT * FROM __THIS__
   WHERE event_time >= '2024-01-01'  -- Filter first
   ```

3. **Use Spark** for large datasets:
   ```python
   from seeknal.tasks.sparkengine import SparkEngineTask
   task = SparkEngineTask()  # Handles large data efficiently
   ```

### Windows-Specific Issues

#### "Path issues with backslashes"

**Cause:** Windows uses backslashes in paths.

**Solution:**
```python
from pathlib import Path

# Use Path for cross-platform compatibility
data_path = Path("examples") / "quickstart" / "sample_data.csv"
df = pd.read_csv(data_path)
```

#### "Permission denied when saving files"

**Cause:** File is open in another program or restricted directory.

**Solution:**
```bash
# Close any programs using the output files
# Save to a directory where you have write permissions
output_dir = Path.home() / "seeknal_output"
output_dir.mkdir(exist_ok=True)
```

### Getting Help

If you're still stuck:

1. **Check the logs** - Look for error messages in the console
2. **Search GitHub Issues** - Someone may have solved your problem
3. **Open a new issue** - Include your Python version, OS, and error message
4. **Join discussions** - Ask the community for help
5. **See the [Troubleshooting Guide](reference/troubleshooting.md)** - Comprehensive issue diagnosis

---

## Summary

Congratulations! You've completed the Seeknal Getting Started guide. You now know how to:

- Install Seeknal on any platform
- Load data with DuckDBTask
- Engineer features with SQL transformations
- Save features in efficient formats
- Choose between DuckDB and Spark

**Time to build something amazing with your features!**

---

*Last updated: January 2024 | Seeknal Documentation*
