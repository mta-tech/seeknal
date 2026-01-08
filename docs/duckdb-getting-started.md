# Getting Started with DuckDB in Seeknal

**Last Updated:** 2026-01-07  
**Engine:** DuckDB (Pure Python, No JVM Required)

## Table of Contents

1. [Introduction](#introduction)
2. [Why DuckDB?](#why-duckdb)
3. [Installation](#installation)
4. [Quick Start](#quick-start)
5. [Core Concepts](#core-concepts)
6. [DuckDBTask API](#duckdbtask-api)
7. [Transformers](#transformers)
8. [Aggregators](#aggregators)
9. [Feature Store with DuckDB](#feature-store-with-duckdb)
10. [Flow Pipelines](#flow-pipelines)
11. [Migration from Spark](#migration-from-spark)

---

## Introduction

Seeknal's DuckDB engine provides a lightweight, pure-Python alternative to Spark for data processing and feature engineering. It uses **PyArrow Tables** as its primary data structure and executes transformations using **DuckDB SQL**.

### Key Benefits

- **No JVM Required:** Pure Python implementation, faster startup
- **Better Performance:** 10-20x faster for datasets <100M rows
- **Lower Memory Footprint:** Efficient columnar processing
- **Easy Debugging:** Pure Python stack traces
- **API Parity:** Same API as SparkEngineTask

### When to Use DuckDB

✅ **Use DuckDB when:**
- Dataset < 100M rows
- Single-machine deployment
- Development/testing environment
- Rapid prototyping
- Cost-sensitive deployment
- Team prefers pure Python

❌ **Use Spark when:**
- Dataset > 100M rows
- Distributed processing required
- Existing Spark infrastructure
- Need Delta Lake features
- Production-scale workloads

---

## Why DuckDB?

### Performance Comparison

| Dataset Size | Spark Time | DuckDB Time | Speedup |
|--------------|------------|-------------|---------|
| 1K rows      | ~2s        | ~0.1s       | **20x**  |
| 100K rows    | ~5s        | ~0.5s       | **10x**  |
| 10M rows     | ~30s       | ~10s        | **3x**   |
| 100M rows    | ~2min      | ~3min       | 0.67x    |

### Architecture Comparison

| Aspect | Spark | DuckDB |
|--------|-------|--------|
| **Language** | Scala/Java backend | Pure Python |
| **Startup** | JVM startup (slow) | Instant |
| **Memory** | High overhead | Low overhead |
| **Data Format** | PySpark DataFrame | PyArrow Table |
| **SQL Engine** | Spark SQL | DuckDB SQL |
| **Distribution** | Cluster | Single-node |

---

## Installation

### Prerequisites

- Python 3.11+
- No JVM or Spark installation required!

### Install Dependencies

```bash
# Install Seeknal with DuckDB support
pip install seeknal[duckdb]

# Or install manually
pip install seeknal duckdb pyarrow pandas pydantic
```

### Verify Installation

```python
import duckdb
import pyarrow as pa
from seeknal.tasks.duckdb import DuckDBTask

print(f"DuckDB version: {duckdb.__version__}")
print("✓ DuckDB is ready!")
```

---

## Quick Start

### Basic Data Processing

```python
import pandas as pd
import pyarrow as pa
from seeknal.tasks.duckdb import DuckDBTask

# 1. Create sample data
df = pd.DataFrame({
    "user_id": ["A", "B", "C", "D", "E"],
    "amount": [100, 200, 150, 250, 180],
    "status": ["active", "inactive", "active", "active", "inactive"]
})

# 2. Convert to PyArrow Table
arrow_table = pa.Table.from_pandas(df)

# 3. Create DuckDBTask and process data
result = (
    DuckDBTask(name="process_data")
    .add_input(dataframe=arrow_table)
    .add_filter_by_expr("status = 'active' AND amount > 100")
    .add_new_column("amount * 1.1", "adjusted_amount")
    .select_columns(["user_id", "amount", "adjusted_amount"])
    .transform()
)

# 4. Result is a PyArrow Table
print(f"Processed {len(result)} rows")
print(result.to_pandas())
```

**Output:**
```
Processed 2 rows
  user_id  amount  adjusted_amount
0        C     150            165.0
1        D     250            275.0
```

---

## Core Concepts

### PyArrow Tables

Seeknal's DuckDB engine uses **PyArrow Tables** as the primary data structure:

```python
import pyarrow as pa
import pandas as pd

# Convert pandas to PyArrow
df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
arrow_table = pa.Table.from_pandas(df)

# Convert back to pandas
df_back = arrow_table.to_pandas()

# PyArrow is zero-copy compatible with DuckDB
# No serialization overhead!
```

### DuckDBTask

The `DuckDBTask` class represents a data transformation pipeline:

```python
from seeknal.tasks.duckdb import DuckDBTask

task = DuckDBTask(name="my_task")

# Add input data
task.add_input(dataframe=arrow_table)
# OR
task.add_input(path="/path/to/data.parquet")
# OR
task.add_input(sql="SELECT * FROM table")

# Add transformations
task.add_sql("SELECT * FROM __THIS__ WHERE amount > 100")
task.add_new_column("amount * 1.1", "adjusted")
task.add_filter_by_expr("status = 'active'")

# Execute pipeline
result = task.transform()
```

### The `__THIS__` Placeholder

DuckDB uses `__THIS__` as a placeholder for the current dataset in SQL statements:

```python
# These are equivalent:
task.add_sql("SELECT * FROM __THIS__ WHERE amount > 100")

# Gets replaced with:
# SELECT * FROM _input_table WHERE amount > 100
```

---

## DuckDBTask API

### Input Methods

```python
from seeknal.tasks.duckdb import DuckDBTask
import pyarrow as pa

task = DuckDBTask(name="example")

# Method 1: PyArrow Table (recommended)
arrow_table = pa.Table.from_pandas(df)
task.add_input(dataframe=arrow_table)

# Method 2: File path (Parquet, CSV, etc.)
task.add_input(path="/data/my_data.parquet")

# Method 3: Raw SQL query
task.add_input(sql="SELECT * FROM source_table WHERE date > '2024-01-01'")
```

### Transformation Methods

```python
from seeknal.tasks.duckdb import DuckDBTask

task = DuckDBTask(name="transform")

# 1. SQL transformations
task.add_sql("SELECT * FROM __THIS__ WHERE amount > 100")

# 2. Add computed columns
task.add_new_column("amount * 1.1", "adjusted_amount")
task.add_new_column("CASE WHEN status = 'active' THEN 1 ELSE 0 END", "is_active")

# 3. Filter rows
task.add_filter_by_expr("status = 'active'")
task.add_filter_by_expr("amount > 100 AND category = 'food'")

# 4. Select specific columns
task.select_columns(["user_id", "amount", "date"])

# 5. Drop columns
task.drop_columns(["temp_column", "debug_info"])

# Execute
result = task.transform()
```

### Using with Context Manager

```python
with DuckDBTask(name="example") as task:
    result = (task
        .add_input(dataframe=arrow_table)
        .add_filter_by_expr("amount > 100")
        .transform()
    )
```

### Return Types

```python
# Return as PyArrow Table (default)
result = task.transform()  # Returns PyArrow Table

# Return as pandas DataFrame
result = task.transform(params={"return_as_pandas": True})  # Returns pd.DataFrame

# Return self for chaining
result = task.transform(materialize=True)  # Returns DuckDBTask
```

---

## Transformers

Transformers modify data row-by-row or column-by-column.

### Simple Transformers

#### 1. SQL Transformer

Execute arbitrary SQL:

```python
from seeknal.tasks.duckdb.transformers import SQL

transformer = SQL(
    statement="SELECT user_id, amount * 1.1 AS adjusted FROM __THIS__ WHERE amount > 100"
)
```

#### 2. ColumnRenamed

Rename a column:

```python
from seeknal.tasks.duckdb.transformers import ColumnRenamed

transformer = ColumnRenamed(
    inputCol="old_name",
    outputCol="new_name"
)
```

#### 3. AddColumnByExpr

Add computed column:

```python
from seeknal.tasks.duckdb.transformers import AddColumnByExpr

transformer = AddColumnByExpr(
    expression="amount * 1.1",
    outputCol="adjusted_amount"
)
```

#### 4. FilterByExpr

Filter rows:

```python
from seeknal.tasks.duckdb.transformers import FilterByExpr

transformer = FilterByExpr(
    expression="status = 'active' AND amount > 100"
)
```

#### 5. SelectColumns / DropCols

```python
from seeknal.tasks.duckdb.transformers import SelectColumns, DropCols

# Keep only these columns
select = SelectColumns(inputCols=["user_id", "amount", "date"])

# Drop these columns
drop = DropCols(inputCols=["temp_col", "debug_info"])
```

### Medium Transformers

#### 6. JoinTablesByExpr

Join multiple tables:

```python
from seeknal.tasks.duckdb.transformers import JoinTablesByExpr, TableJoinDef, JoinType

transformer = JoinTablesByExpr(
    select_stm="a.*, b.value2",
    alias="a",
    tables=[
        TableJoinDef(
            table="other_table",
            alias="b",
            joinType=JoinType.LEFT,
            joinExpression="a.id = b.id"
        )
    ]
)
```

#### 7. CastColumn

Change data type:

```python
from seeknal.tasks.duckdb.transformers import CastColumn

transformer = CastColumn(
    inputCol="amount",
    outputCol="amount_int",
    dataType="INTEGER"
)
```

### Complex Transformers

#### 8. AddWindowFunction

Add window function column:

```python
from seeknal.tasks.duckdb.transformers import AddWindowFunction, WindowFunction

# LAG function
lag = AddWindowFunction(
    inputCol="amount",
    offset=1,
    windowFunction=WindowFunction.LAG,
    partitionCols=["user_id"],
    orderCols=["date"],
    outputCol="prev_amount"
)

# ROW_NUMBER
row_num = AddWindowFunction(
    inputCol="amount",
    windowFunction=WindowFunction.ROW_NUMBER,
    partitionCols=["user_id"],
    orderCols=["date"],
    outputCol="row_num"
)

# Running total
running_sum = AddWindowFunction(
    inputCol="amount",
    windowFunction=WindowFunction.SUM,
    partitionCols=["user_id"],
    orderCols=["date"],
    outputCol="running_total"
)
```

### Using Transformers with DuckDBTask

```python
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb.transformers import AddWindowFunction, WindowFunction

task = DuckDBTask(name="window_example")

# Using convenience method
task.add_new_column("amount * 1.1", "adjusted")

# Using add_stage with transformer
lag = AddWindowFunction(
    inputCol="amount",
    offset=1,
    windowFunction=WindowFunction.LAG,
    partitionCols=["user_id"],
    orderCols=["date"],
    outputCol="prev_amount"
)
task.add_stage(transformer=lag)

result = task.transform()
```

---

## Aggregators

Aggregators group data and compute summary statistics.

### Simple Aggregators

#### 1. FunctionAggregator

Standard SQL functions:

```python
from seeknal.tasks.duckdb.aggregators import DuckDBAggregator, FunctionAggregator

aggregator = DuckDBAggregator(
    group_by_cols=["user_id", "category"],
    aggregators=[
        FunctionAggregator(
            inputCol="amount",
            outputCol="total_amount",
            accumulatorFunction="sum"
        ),
        FunctionAggregator(
            inputCol="amount",
            outputCol="avg_amount",
            accumulatorFunction="avg"
        ),
        FunctionAggregator(
            inputCol="amount",
            outputCol="count_rows",
            accumulatorFunction="count"
        ),
        FunctionAggregator(
            inputCol="amount",
            outputCol="min_amount",
            accumulatorFunction="min"
        ),
        FunctionAggregator(
            inputCol="amount",
            outputCol="max_amount",
            accumulatorFunction="max"
        ),
    ]
)
```

#### 2. ExpressionAggregator

Custom SQL expressions:

```python
from seeknal.tasks.duckdb.aggregators import ExpressionAggregator

aggregator = DuckDBAggregator(
    group_by_cols=["user_id"],
    aggregators=[
        ExpressionAggregator(
            expression="SUM(CASE WHEN status = 'active' THEN amount ELSE 0 END)",
            outputCol="active_sum"
        ),
        ExpressionAggregator(
            expression="COUNT(DISTINCT category)",
            outputCol="category_count"
        )
    ]
)
```

#### 3. DayTypeAggregator

Weekday/weekend filtering:

```python
from seeknal.tasks.duckdb.aggregators import DayTypeAggregator

aggregator = DuckDBAggregator(
    group_by_cols=["user_id"],
    aggregators=[
        DayTypeAggregator(
            inputCol="transactions",
            outputCol="weekend_sum",
            accumulatorFunction="sum",
            weekdayCol="is_weekend"  # Boolean column
        )
    ]
)
```

### Complex Aggregators

#### 4. LastNDaysAggregator

Time-based windowing:

```python
from seeknal.tasks.duckdb.aggregators import LastNDaysAggregator, FunctionAggregator

aggregator = LastNDaysAggregator(
    group_by_cols=["user_id"],
    window=7,  # 7-day window
    date_col="date_id",
    date_pattern="yyyyMMdd",  # or "yyyy-MM-dd"
    aggregators=[
        FunctionAggregator(
            inputCol="amount",
            outputCol="amount_sum_7d",
            accumulatorFunction="sum"
        ),
        FunctionAggregator(
            inputCol="transactions",
            outputCol="transaction_count_7d",
            accumulatorFunction="count"
        )
    ]
)
```

### Using Aggregators with DuckDBTask

```python
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb.aggregators import DuckDBAggregator, FunctionAggregator

# Create aggregator
agg = DuckDBAggregator(
    group_by_cols=["user_id"],
    aggregators=[
        FunctionAggregator(
            inputCol="amount",
            outputCol="total",
            accumulatorFunction="sum"
        )
    ]
)

# Use with DuckDBTask
result = (
    DuckDBTask(name="aggregate")
    .add_input(dataframe=arrow_table)
    .add_filter_by_expr("amount > 0")
    .add_stage(aggregator=agg)  # Add aggregator as a stage
    .transform()
)
```

---

## Feature Store with DuckDB

Seeknal's Feature Store provides feature versioning, materialization, and serving. The DuckDB implementation uses Parquet files for storage.

### Basic Feature Group

```python
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    Materialization,
    Entity
)
from seeknal.project import Project
import pandas as pd

# Create project
project = Project(name="my_project")
project.get_or_create()

# Define entity
entity = Entity(
    name="user",
    join_keys=["user_id"],
    description="User entity"
)

# Define feature group
fg = FeatureGroupDuckDB(
    name="user_features",
    entity=entity,
    materialization=Materialization(
        event_time_col="timestamp",
        offline_store_path="/features/user_features",
        online_store_path="/features/user_features_online"
    ),
    project="my_project"
)

# Set features
df = pd.DataFrame({
    "user_id": ["A", "B", "C"],
    "timestamp": pd.date_range("2024-01-01", periods=3),
    "total_amount": [1000, 2000, 1500],
    "transaction_count": [10, 20, 15],
    "avg_amount": [100.0, 100.0, 100.0]
})

fg.set_dataframe(df).set_features()

# Write feature group
fg.write(feature_start_time=pd.Timestamp("2024-01-01"))
```

### Historical Features

```python
from seeknal.featurestore.duckdbengine.feature_group import (
    HistoricalFeaturesDuckDB,
    FeatureLookup
)

# Create feature lookup
lookup = FeatureLookup(
    source=fg,
    features=["total_amount", "transaction_count", "avg_amount"]
)

# Create historical features with point-in-time joins
hist = HistoricalFeaturesDuckDB(
    lookups=[lookup],
    event_time_col="application_date",
    event_timestamp_format="yyyy-MM-dd"
)

# Get historical features
spine_df = pd.DataFrame({
    "user_id": ["A", "B", "C"],
    "application_date": ["2024-01-05", "2024-01-10", "2024-01-15"]
})

# Convert spine to PyArrow and get features
spine_arrow = pa.Table.from_pandas(spine_df)
features_df = hist.to_dataframe(
    spine=spine_arrow,
    feature_start_time=pd.Timestamp("2024-01-01")
).to_pandas()
```

### Online Features

```python
from seeknal.featurestore.duckdbengine.feature_group import OnlineFeaturesDuckDB

# Create online serving table
online_table = hist.serve(
    name="user_features_online",
    feature_start_time=pd.Timestamp("2024-01-01")
)

# Query online features
features = online_table.get_features(
    keys=[{"user_id": "A"}, {"user_id": "B"}]
)

print(features)
# Output:
# [{'user_id': 'A', 'total_amount': 1000, 'transaction_count': 10, ...},
#  {'user_id': 'B', 'total_amount': 2000, 'transaction_count': 20, ...}]
```

### Feature Group Versioning

```python
# List versions
versions = fg.list_versions()
print(f"Found {len(versions)} versions")

# Get specific version
fg_v1 = fg.get_version(version=1)

# Compare versions
diff = fg.compare_versions(from_version=1, to_version=2)

# Materialize specific version
fg.write(
    feature_start_time=pd.Timestamp("2024-01-01"),
    version=1  # Materialize version 1
)
```

---

## Flow Pipelines

The `Flow` class orchestrates multi-stage data pipelines with support for mixed Spark and DuckDB tasks.

### Pure DuckDB Pipeline

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.duckdb import DuckDBTask
import pyarrow as pa

# Create input data
df = pd.DataFrame({
    "user_id": ["A", "B", "C", "D", "E"],
    "amount": [100, 200, 150, 250, 180],
    "category": ["food", "travel", "food", "entertainment", "travel"]
})
arrow_table = pa.Table.from_pandas(df)

# Create Flow
flow = Flow(
    name="duckdb_pipeline",
    input=FlowInput(
        kind=FlowInputEnum.SOURCE,  # Use SOURCE for PyArrow Tables
        value=arrow_table
    ),
    tasks=[
        DuckDBTask(name="filter")
            .add_filter_by_expr("amount > 100"),
        
        DuckDBTask(name="transform")
            .add_new_column("amount * 0.9", "discounted_amount"),
        
        DuckDBTask(name="select")
            .select_columns(["user_id", "amount", "discounted_amount"])
    ],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)
)

# Save flow
flow.get_or_create()

# Run pipeline
result = flow.run()
print(f"Result: {len(result)} rows")
```

### Pipeline with Aggregations

```python
from seeknal.tasks.duckdb.aggregators import DuckDBAggregator, FunctionAggregator

# Create aggregator
agg = DuckDBAggregator(
    group_by_cols=["category"],
    aggregators=[
        FunctionAggregator(
            inputCol="amount",
            outputCol="total_amount",
            accumulatorFunction="sum"
        ),
        FunctionAggregator(
            inputCol="amount",
            outputCol="avg_amount",
            accumulatorFunction="avg"
        )
    ]
)

# Create flow with aggregation
flow = Flow(
    name="aggregation_pipeline",
    input=FlowInput(kind=FlowInputEnum.SOURCE, value=arrow_table),
    tasks=[
        DuckDBTask(name="aggregate").add_stage(aggregator=agg)
    ],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)
)

result = flow.run()
```

### Mixed Spark/DuckDB Pipeline

```python
from seeknal.tasks.sparkengine import SparkEngineTask

# Flow automatically detects if Spark is needed
flow = Flow(
    name="mixed_pipeline",
    input=FlowInput(
        kind=FlowInputEnum.HIVE_TABLE,
        value="source_table"
    ),
    tasks=[
        # Spark task for initial processing
        SparkEngineTask()
            .add_sql("SELECT * FROM __THIS__ WHERE date = '2024-01-07'"),
        
        # DuckDB task for feature engineering
        DuckDBTask(name="feature_eng")
            .add_sql("SELECT user_id, amount * 1.1 AS adjusted FROM __THIS__")
    ],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)
)

# Flow will create SparkSession only because SparkEngineTask is present
result = flow.run()
```

### Flow with Feature Groups

```python
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    Materialization
)

# Create feature group
fg = FeatureGroupDuckDB(
    name="user_features",
    entity=Entity(name="user", join_keys=["user_id"]),
    materialization=Materialization(
        event_time_col="timestamp",
        offline_store_path="/features/user"
    ),
    project="my_project"
)

# Use feature group in flow
flow = Flow(
    name="feature_pipeline",
    input=FlowInput(
        kind=FlowInputEnum.FEATURE_GROUP,
        value=fg
    ),
    tasks=[
        DuckDBTask(name="add_features")
            .add_sql("SELECT user_id, feature1, feature2 FROM __THIS__")
    ],
    output=FlowOutput()
)

result = flow.run()
```

---

## Migration from Spark

### Step-by-Step Migration

#### Step 1: Change Imports

**Before (Spark):**
```python
from seeknal.tasks.sparkengine import SparkEngineTask
from seeknal.tasks.sparkengine import transformers as T
from seeknal.tasks.sparkengine import aggregators as G
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
```

**After (DuckDB):**
```python
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb import transformers as T
from seeknal.tasks.duckdb import aggregators as G
import pyarrow as pa

# No SparkSession needed!
```

#### Step 2: Update Data Types

**Before:**
```python
# PySpark DataFrame
df = spark.read.parquet("/data/my_data.parquet")
```

**After:**
```python
# PyArrow Table
import pandas as pd
df = pd.read_parquet("/data/my_data.parquet")
arrow_table = pa.Table.from_pandas(df)
```

#### Step 3: Update Task Initialization

**Before:**
```python
task = SparkEngineTask()
result = task.add_input(table="my_table").transform()
```

**After:**
```python
task = DuckDBTask(name="my_task")
result = task.add_input(dataframe=arrow_table).transform()
```

#### Step 4: Update Output Handling

**Before:**
```python
# PySpark DataFrame
result.show()
pandas_df = result.toPandas()
```

**After:**
```python
# PyArrow Table
print(result)
pandas_df = result.to_pandas()
```

### Complete Migration Example

**Before (Spark):**
```python
from pyspark.sql import SparkSession
from seeknal.tasks.sparkengine import SparkEngineTask
from seeknal.tasks.sparkengine.aggregators import Aggregator, FunctionAggregator

spark = SparkSession.builder.getOrCreate()

# Read data
df = spark.read.parquet("/data/transactions.parquet")

# Create task
task = SparkEngineTask()
result = (
    task
    .add_input(dataframe=df)
    .add_sql("SELECT * FROM __THIS__ WHERE amount > 100")
    .add_stage(
        aggregator=Aggregator(
            group_by_cols=["user_id"],
            aggregators=[
                FunctionAggregator(
                    inputCol="amount",
                    outputCol="total",
                    accumulatorFunction="sum"
                )
            ]
        )
    )
    .transform()
)

# Show result
result.show()
```

**After (DuckDB):**
```python
import pandas as pd
import pyarrow as pa
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb.aggregators import DuckDBAggregator, FunctionAggregator

# Read data
df = pd.read_parquet("/data/transactions.parquet")
arrow_table = pa.Table.from_pandas(df)

# Create task
task = DuckDBTask(name="aggregate")
result = (
    task
    .add_input(dataframe=arrow_table)
    .add_sql("SELECT * FROM __THIS__ WHERE amount > 100")
    .add_stage(
        aggregator=DuckDBAggregator(
            group_by_cols=["user_id"],
            aggregators=[
                FunctionAggregator(
                    inputCol="amount",
                    outputCol="total",
                    accumulatorFunction="sum"
                )
            ]
        )
    )
    .transform()
)

# Show result
print(result.to_pandas())
```

**Changes Summary:**
1. ✓ Changed imports (SparkEngineTask → DuckDBTask)
2. ✓ Changed data format (PySpark DataFrame → PyArrow Table)
3. ✓ Changed aggregator class (Aggregator → DuckDBAggregator)
4. ✓ Changed output handling (.show() → .to_pandas())

### API Compatibility Matrix

| Feature | Spark | DuckDB | Notes |
|---------|-------|--------|-------|
| SQL transformations | ✓ | ✓ | Same SQL syntax |
| Column operations | ✓ | ✓ | Same API |
| Filtering | ✓ | ✓ | Same API |
| Aggregations | ✓ | ✓ | Same API |
| Joins | ✓ | ✓ | Same API |
| Window functions | ✓ | ✓ | Same API |
| Feature Store | ✓ | ✓ | Same API |

---

## Advanced Examples

### Example 1: Customer Churn Features

```python
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb.aggregators import DuckDBAggregator, FunctionAggregator, ExpressionAggregator
from seeknal.tasks.duckdb.transformers import AddWindowFunction, WindowFunction
import pandas as pd
import pyarrow as pa

# Sample transaction data
df = pd.DataFrame({
    "customer_id": ["A", "A", "A", "B", "B", "B"],
    "transaction_date": ["2024-01-01", "2024-01-05", "2024-01-10", 
                        "2024-01-02", "2024-01-06", "2024-01-11"],
    "amount": [100, 150, 200, 80, 120, 90],
    "category": ["food", "travel", "food", "food", "travel", "food"]
})

arrow_table = pa.Table.from_pandas(df)

# Pipeline: Calculate churn features
task = DuckDBTask(name="churn_features")

# Step 1: Add LAG features (previous transaction amount)
lag = AddWindowFunction(
    inputCol="amount",
    offset=1,
    windowFunction=WindowFunction.LAG,
    partitionCols=["customer_id"],
    orderCols=["transaction_date"],
    outputCol="prev_amount"
)

# Step 2: Add time since last transaction
task = (
    DuckDBTask(name="churn_pipeline")
    .add_input(dataframe=arrow_table)
    .add_stage(transformer=lag)
    .add_new_column(
        "DATEDIFF('day', prev_transaction_date, transaction_date)",
        "days_since_last"
    )
)

# Step 3: Aggregate per customer
agg = DuckDBAggregator(
    group_by_cols=["customer_id"],
    aggregators=[
        FunctionAggregator(inputCol="amount", outputCol="total_amount", accumulatorFunction="sum"),
        FunctionAggregator(inputCol="amount", outputCol="avg_amount", accumulatorFunction="avg"),
        FunctionAggregator(inputCol="amount", outputCol="transaction_count", accumulatorFunction="count"),
        ExpressionAggregator(
            expression="MAX(days_since_last)",
            outputCol="recency_days"
        )
    ]
)

result = task.add_stage(aggregator=agg).transform()
print(result.to_pandas())
```

### Example 2: Time-Series Features

```python
from seeknal.tasks.duckdb.aggregators import LastNDaysAggregator, FunctionAggregator

# Sensor data with timestamps
df = pd.DataFrame({
    "sensor_id": ["S1", "S1", "S1", "S1", "S2", "S2", "S2"],
    "timestamp": pd.date_range("2024-01-01", periods=7),
    "temperature": [20.5, 21.0, 20.8, 21.2, 19.5, 19.8, 20.1],
    "humidity": [65, 68, 67, 70, 62, 64, 63]
})

# Format date as string for LastNDaysAggregator
df["date_id"] = df["timestamp"].dt.strftime("%Y%m%d")
arrow_table = pa.Table.from_pandas(df)

# 7-day rolling aggregation
agg = LastNDaysAggregator(
    group_by_cols=["sensor_id"],
    window=7,
    date_col="date_id",
    date_pattern="yyyyMMdd",
    aggregators=[
        FunctionAggregator(
            inputCol="temperature",
            outputCol="avg_temp_7d",
            accumulatorFunction="avg"
        ),
        FunctionAggregator(
            inputCol="humidity",
            outputCol="avg_humidity_7d",
            accumulatorFunction="avg"
        )
    ]
)

result = (
    DuckDBTask(name="sensor_features")
    .add_input(dataframe=arrow_table)
    .add_stage(aggregator=agg)
    .transform()
)

print(result.to_pandas())
```

### Example 3: Feature Engineering Pipeline

```python
# Complete pipeline: Clean → Transform → Aggregate → Select
df = pd.DataFrame({
    "user_id": ["A", "A", "B", "B", "C", "C"] * 10,
    "transaction_amount": np.random.randint(10, 500, 60),
    "category": np.random.choice(["food", "travel", "entertainment"], 60),
    "date": pd.date_range("2024-01-01", periods=60)
})

arrow_table = pa.Table.from_pandas(df)

# Multi-stage pipeline
result = (
    DuckDBTask(name="feature_engineering")
    .add_input(dataframe=arrow_table)
    
    # Stage 1: Clean data
    .add_filter_by_expr("transaction_amount > 0")
    
    # Stage 2: Feature engineering
    .add_new_column("transaction_amount * 0.9", "after_discount")
    .add_new_column(
        "CASE WHEN category = 'food' THEN 1 WHEN category = 'travel' THEN 2 ELSE 3 END",
        "category_code"
    )
    
    # Stage 3: Aggregation
    .add_stage(
        aggregator=DuckDBAggregator(
            group_by_cols=["user_id", "category_code"],
            aggregators=[
                FunctionAggregator(
                    inputCol="transaction_amount",
                    outputCol="total_spent",
                    accumulatorFunction="sum"
                ),
                FunctionAggregator(
                    inputCol="after_discount",
                    outputCol="total_discounted",
                    accumulatorFunction="sum"
                ),
                FunctionAggregator(
                    inputCol="transaction_amount",
                    outputCol="transaction_count",
                    accumulatorFunction="count"
                )
            ]
        )
    )
    
    # Stage 4: Final selection
    .select_columns(["user_id", "category_code", "total_spent", "transaction_count"])
    
    .transform()
)

print(f"Final result: {len(result)} aggregated rows")
```

---

## Performance Tips

### 1. Use Parquet Files

```python
# Parquet is faster than CSV for columnar operations
task.add_input(path="/data/my_data.parquet")  # ✓ Fast
task.add_input(path="/data/my_data.csv")      # ✗ Slower
```

### 2. Filter Early

```python
# Good: Filter first, then transform
task.add_filter_by_expr("amount > 100")
task.add_new_column("amount * 1.1", "adjusted")

# Bad: Transform all data, then filter
task.add_new_column("amount * 1.1", "adjusted")
task.add_filter_by_expr("amount > 100")
```

### 3. Use Aggregations Instead of Window Functions When Possible

```python
# Good: Simple aggregation
agg = DuckDBAggregator(
    group_by_cols=["user_id"],
    aggregators=[
        FunctionAggregator(inputCol="amount", outputCol="total", accumulatorFunction="sum")
    ]
)

# Less efficient: Window function when you just need aggregation
win = AddWindowFunction(
    inputCol="amount",
    windowFunction=WindowFunction.SUM,
    partitionCols=["user_id"],
    outputCol="total"
)
```

### 4. Batch Operations

```python
# Good: Single SQL statement with multiple operations
task.add_sql("""
    SELECT 
        user_id,
        amount * 1.1 AS adjusted,
        CASE WHEN amount > 100 THEN 'high' ELSE 'low' END AS tier
    FROM __THIS__
    WHERE amount > 50
""")

# Less efficient: Multiple stages
task.add_filter_by_expr("amount > 50")
task.add_new_column("amount * 1.1", "adjusted")
task.add_new_column("CASE WHEN amount > 100 THEN 'high' ELSE 'low' END", "tier")
```

---

## Troubleshooting

### Issue 1: "Table does not exist"

**Problem:**
```
Catalog Error: Table with name my_table does not exist!
```

**Solution:**
```python
# Register table in DuckDB connection first
task = DuckDBTask(name="example")
task.conn.register("my_table", arrow_table)

# Now use it in joins
from seeknal.tasks.duckdb.transformers import JoinTablesByExpr, TableJoinDef

join = JoinTablesByExpr(
    select_stm="a.*, b.value2",
    alias="a",
    tables=[
        TableJoinDef(
            table="my_table",  # Now this works
            alias="b",
            joinType=JoinType.LEFT,
            joinExpression="a.id = b.id"
        )
    ]
)
```

### Issue 2: "Invalid date format"

**Problem:**
```
Conversion Error: invalid date field format: "20240107"
```

**Solution:**
```python
# Use correct date pattern
agg = LastNDaysAggregator(
    date_col="date_id",
    date_pattern="yyyyMMdd",  # For "20240107" format
    # OR
    date_pattern="yyyy-MM-dd",  # For "2024-01-07" format
    ...
)
```

### Issue 3: PyArrow vs Pandas

**Problem:**
```
TypeError: expected pyarrow.Table, got pandas.DataFrame
```

**Solution:**
```python
# Always convert pandas to PyArrow first
import pyarrow as pa

# Wrong
task.add_input(dataframe=df)  # pandas DataFrame

# Correct
arrow_table = pa.Table.from_pandas(df)
task.add_input(dataframe=arrow_table)  # PyArrow Table
```

---

## Best Practices

### 1. Always Name Your Tasks

```python
# Good
task = DuckDBTask(name="aggregate_user_data")

# Less helpful for debugging
task = DuckDBTask()
```

### 2. Use Type Hints

```python
from typing import Union
import pyarrow as pa
import pandas as pd

def process_data(
    data: Union[pa.Table, pd.DataFrame],
    filters: str
) -> pa.Table:
    """Process data with DuckDB."""
    if isinstance(data, pd.DataFrame):
        data = pa.Table.from_pandas(data)
    
    return (
        DuckDBTask(name="process")
        .add_input(dataframe=data)
        .add_filter_by_expr(filters)
        .transform()
    )
```

### 3. Chain Methods

```python
# Good: Method chaining
result = (task
    .add_input(dataframe=arrow_table)
    .add_filter_by_expr("amount > 100")
    .add_new_column("amount * 1.1", "adjusted")
    .transform()
)

# Less readable: Separate statements
task.add_input(dataframe=arrow_table)
task.add_filter_by_expr("amount > 100")
task.add_new_column("amount * 1.1", "adjusted")
result = task.transform()
```

### 4. Validate Inputs

```python
from seeknal.validation import validate_column_name

# Validate column names before using them
validate_column_name("user_id")  # OK
validate_column_name("user id")  # Raises error (space not allowed)
```

---

## Next Steps

- [Explore Flow Examples](./examples/duckdb-flow-examples.md)
- [Feature Store Deep Dive](./api/duckdb-feature-store.md)
- [API Reference](./api/duckdb-api.md)
- [Migration Guide](./migration-spark-to-duckdb.md)

---

**Last Updated:** 2026-01-07  
**Version:** 1.0.0  
**Status:** Production Ready ✅
