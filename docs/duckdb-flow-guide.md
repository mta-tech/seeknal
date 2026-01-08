# DuckDB Flow Guide

**Last Updated:** 2026-01-07

## Table of Contents

1. [Introduction to Flow](#introduction-to-flow)
2. [Why Use Flow?](#why-use-flow)
3. [Flow Architecture](#flow-architecture)
4. [Creating Flows](#creating-flows)
5. [Flow Inputs](#flow-inputs)
6. [Flow Outputs](#flow-outputs)
7. [DuckDB Tasks in Flow](#duckdb-tasks-in-flow)
8. [Mixed Spark/DuckDB Pipelines](#mixed-sparkduckdb-pipelines)
9. [Flow with Feature Groups](#flow-with-feature-groups)
10. [Advanced Flow Patterns](#advanced-flow-patterns)
11. [Best Practices](#best-practices)

---

## Introduction to Flow

The `Flow` class is Seeknal's orchestration layer for data pipelines. It manages multi-stage transformations, supports multiple engines (Spark and DuckDB), and integrates with the Feature Store.

### Key Features

- **Multi-Engine Support:** Mix Spark and DuckDB tasks in the same pipeline
- **Automatic Engine Detection:** Flow knows when to create SparkSession
- **Type Conversions:** Handles conversions between PySpark DataFrames and PyArrow Tables
- **Feature Store Integration:** Read from and write to feature groups
- **Declarative Pipelines:** Define pipeline structure once, run multiple times

---

## Why Use Flow?

### Without Flow (Manual Orchestration)

```python
# Manual approach - error-prone and hard to maintain
import pandas as pd
import pyarrow as pa

# Step 1: Load data
df = pd.read_parquet("/data/transactions.parquet")
table = pa.Table.from_pandas(df)

# Step 2: Filter
task1 = DuckDBTask(name="filter")
result1 = task1.add_input(dataframe=table).add_filter_by_expr("amount > 100").transform()

# Step 3: Transform
task2 = DuckDBTask(name="transform")
result2 = task2.add_input(dataframe=result1).add_new_column("amount * 1.1", "adjusted").transform()

# Step 4: Aggregate
task3 = DuckDBTask(name="aggregate")
result3 = task3.add_input(dataframe=result2).add_stage(aggregator=agg).transform()

# Step 5: Save
result3.to_pandas().to_parquet("/output/result.parquet")

# Problems:
# - Manual data passing between tasks
# - No pipeline versioning
# - Hard to reuse
# - No metadata tracking
```

### With Flow (Declarative)

```python
# Declarative approach - clean and maintainable
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.duckdb import DuckDBTask

flow = Flow(
    name="transaction_pipeline",
    input=FlowInput(kind=FlowInputEnum.SOURCE, value=table),
    tasks=[
        DuckDBTask(name="filter").add_filter_by_expr("amount > 100"),
        DuckDBTask(name="transform").add_new_column("amount * 1.1", "adjusted"),
        DuckDBTask(name="aggregate").add_stage(aggregator=agg)
    ],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)
)

# Save pipeline definition
flow.get_or_create()

# Run pipeline
result = flow.run()

# Benefits:
# - Automatic data passing between tasks
# - Pipeline versioning and metadata
# - Easy to reuse
# - Reproducible runs
```

---

## Flow Architecture

### Flow Components

```
┌─────────────────────────────────────────────────────────────┐
│                         Flow                                │
├─────────────────────────────────────────────────────────────┤
│  Input → Task1 → Task2 → Task3 → ... → Output              │
│           ↓        ↓        ↓                                  │
│        Engine Detection                                       │
│        Type Conversions                                       │
│        Metadata Tracking                                      │
└─────────────────────────────────────────────────────────────┘
```

### Engine Detection

Flow automatically detects if Spark is needed:

```python
# Pure DuckDB pipeline - no Spark needed
flow = Flow(
    name="duckdb_only",
    tasks=[
        DuckDBTask(name="task1").add_sql("SELECT * FROM __THIS__"),
        DuckDBTask(name="task2").add_filter_by_expr("amount > 100")
    ]
)

# Flow detects: has_spark_job = False
# No SparkSession created
```

```python
# Mixed pipeline - Spark needed
from seeknal.tasks.sparkengine import SparkEngineTask

flow = Flow(
    name="mixed_pipeline",
    tasks=[
        SparkEngineTask().add_sql("SELECT * FROM __THIS__"),  # Spark!
        DuckDBTask(name="task2").add_sql("SELECT * FROM __THIS__")  # DuckDB
    ]
)

# Flow detects: has_spark_job = True
# SparkSession created automatically
```

### Type Conversions

Flow handles automatic conversions between engines:

```
PySpark DataFrame → PyArrow Table → Pandas DataFrame
      ↓                    ↓                   ↓
   SparkEngine         DuckDBTask          Output
```

---

## Creating Flows

### Basic Flow Structure

```python
from seeknal.flow import Flow, FlowInput, FlowOutput

flow = Flow(
    name="my_pipeline",              # Unique pipeline name
    input=FlowInput(...),             # Data source
    tasks=[task1, task2, task3],      # List of tasks
    output=FlowOutput(...)            # Data destination
)
```

### Simple DuckDB Flow

```python
import pyarrow as pa
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.duckdb import DuckDBTask

# Create input data
df = pd.DataFrame({
    "user_id": ["A", "B", "C"],
    "amount": [100, 200, 150]
})
arrow_table = pa.Table.from_pandas(df)

# Define flow
flow = Flow(
    name="simple_pipeline",
    input=FlowInput(
        kind=FlowInputEnum.SOURCE,
        value=arrow_table
    ),
    tasks=[
        DuckDBTask(name="filter").add_filter_by_expr("amount > 100")
    ],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)
)

# Save and run
flow.get_or_create()  # Save to database
result = flow.run()    # Execute pipeline
```

### Multi-Stage Flow

```python
flow = Flow(
    name="multi_stage_pipeline",
    input=FlowInput(kind=FlowInputEnum.SOURCE, value=arrow_table),
    tasks=[
        # Stage 1: Filter
        DuckDBTask(name="filter")
            .add_filter_by_expr("amount > 100"),
        
        # Stage 2: Feature engineering
        DuckDBTask(name="features")
            .add_new_column("amount * 1.1", "adjusted")
            .add_new_column("amount * 0.9", "discounted"),
        
        # Stage 3: Aggregation
        DuckDBTask(name="aggregate")
            .add_stage(aggregator=agg),
        
        # Stage 4: Final selection
        DuckDBTask(name="select")
            .select_columns(["user_id", "total_amount"])
    ],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)
)
```

---

## Flow Inputs

### Input Types

Flow supports various input types through `FlowInputEnum`:

```python
from seeknal.flow import FlowInput, FlowInputEnum
```

#### 1. SOURCE (PyArrow Table)

```python
# Direct PyArrow Table input
import pyarrow as pa

arrow_table = pa.Table.from_pandas(df)

flow_input = FlowInput(
    kind=FlowInputEnum.SOURCE,
    value=arrow_table
)
```

#### 2. PARQUET (File Path)

```python
# Read from Parquet file
flow_input = FlowInput(
    kind=FlowInputEnum.PARQUET,
    value="/data/my_data.parquet"
)
```

#### 3. HIVE_TABLE (Spark Table)

```python
# Read from Hive table
flow_input = FlowInput(
    kind=FlowInputEnum.HIVE_TABLE,
    value="database.table_name"
)
```

#### 4. FEATURE_GROUP (Feature Store)

```python
# Read from feature group
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB

fg = FeatureGroupDuckDB(name="user_features", ...)

flow_input = FlowInput(
    kind=FlowInputEnum.FEATURE_GROUP,
    value=fg
)
```

#### 5. EXTRACTOR (Custom Data Source)

```python
# Use custom extractor
from seeknal.extractors import BaseExtractor

class MyExtractor(BaseExtractor):
    def extract(self):
        # Custom extraction logic
        return arrow_table

flow_input = FlowInput(
    kind=FlowInputEnum.EXTRACTOR,
    value=MyExtractor()
)
```

---

## Flow Outputs

### Output Types

Flow supports various output types through `FlowOutputEnum`:

```python
from seeknal.flow import FlowOutput, FlowOutputEnum
```

#### 1. ARROW_DATAFRAME (PyArrow Table)

```python
# Return as PyArrow Table
flow_output = FlowOutput(
    kind=FlowOutputEnum.ARROW_DATAFRAME
)

result = flow.run()  # Returns PyArrow Table
```

#### 2. PANDAS_DATAFRAME (Pandas DataFrame)

```python
# Return as Pandas DataFrame
flow_output = FlowOutput(
    kind=FlowOutputEnum.PANDAS_DATAFRAME
)

result = flow.run()  # Returns pd.DataFrame
```

#### 3. SPARK_DATAFRAME (PySpark DataFrame)

```python
# Return as PySpark DataFrame
flow_output = FlowOutput(
    kind=FlowOutputEnum.SPARK_DATAFRAME
)

result = flow.run()  # Returns PySpark DataFrame
```

#### 4. PARQUET (Write to File)

```python
# Write to Parquet file
flow_output = FlowOutput(
    kind=FlowOutputEnum.PARQUET,
    value="/output/result.parquet"
)

flow.run()  # Writes to file, returns None
```

#### 5. FEATURE_GROUP (Write to Feature Store)

```python
# Write to feature group
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB

fg = FeatureGroupDuckDB(name="output_features", ...)

flow_output = FlowOutput(
    kind=FlowOutputEnum.FEATURE_GROUP,
    value=fg
)

flow.run()  # Writes to feature store
```

---

## DuckDB Tasks in Flow

### Creating DuckDB Tasks for Flow

```python
from seeknal.tasks.duckdb import DuckDBTask

# Method 1: Create task with name (recommended for Flow)
task = DuckDBTask(name="my_transformation")
task.add_sql("SELECT * FROM __THIS__ WHERE amount > 100")

# Method 2: Chain methods
task = (
    DuckDBTask(name="transform")
    .add_filter_by_expr("amount > 100")
    .add_new_column("amount * 1.1", "adjusted")
)

# Method 3: Add transformers directly
from seeknal.tasks.duckdb.transformers import AddWindowFunction, WindowFunction

task = DuckDBTask(name="window_features")
window = AddWindowFunction(
    inputCol="amount",
    windowFunction=WindowFunction.LAG,
    partitionCols=["user_id"],
    orderCols=["date"],
    outputCol="prev_amount"
)
task.add_stage(transformer=window)
```

### Using Aggregators in Flow

```python
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

# Add to DuckDBTask
task = DuckDBTask(name="aggregate")
task.add_stage(aggregator=agg)

# Use in Flow
flow = Flow(
    name="aggregation_flow",
    input=FlowInput(kind=FlowInputEnum.SOURCE, value=arrow_table),
    tasks=[task],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)
)
```

### Complex Pipelines with Pre/Post Stages

```python
from seeknal.tasks.duckdb.aggregators import LastNDaysAggregator, FunctionAggregator

# LastNDaysAggregator has built-in pre/post stages
agg = LastNDaysAggregator(
    group_by_cols=["user_id"],
    window=7,
    date_col="date_id",
    date_pattern="yyyyMMdd",
    aggregators=[
        FunctionAggregator(
            inputCol="amount",
            outputCol="amount_sum_7d",
            accumulatorFunction="sum"
        )
    ]
)
# Automatically adds:
# - Pre-stages: Calculate window bounds
# - Aggregation stage
# - Post-stages: Clean up temp columns

task = DuckDBTask(name="time_window")
task.add_stage(aggregator=agg)
```

---

## Mixed Spark/DuckDB Pipelines

### Why Mix Engines?

```
┌────────────────────────────────────────────────────┐
│              Data Processing Pipeline              │
├────────────────────────────────────────────────────┤
│                                                      │
│  [Spark] → Large-scale data processing             │
│     ↓                                                │
│  [DuckDB] → Feature engineering (faster!)          │
│     ↓                                                │
│  [DuckDB] → Aggregations (lighter)                 │
│     ↓                                                │
│  [Spark] → Write to Delta Lake (if needed)         │
│                                                      │
└────────────────────────────────────────────────────┘
```

### Example: ETL Pipeline

```python
from seeknal.tasks.sparkengine import SparkEngineTask
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum

# Mixed pipeline: Spark for ETL, DuckDB for features
flow = Flow(
    name="etl_and_features",
    input=FlowInput(
        kind=FlowInputEnum.HIVE_TABLE,
        value="raw_transactions"
    ),
    tasks=[
        # Stage 1: Spark - Extract and clean large dataset
        SparkEngineTask()
            .add_sql("SELECT * FROM __THIS__ WHERE date >= '2024-01-01'")
            .add_sql("SELECT user_id, amount, category FROM __THIS__"),
        
        # Stage 2: DuckDB - Feature engineering (faster!)
        DuckDBTask(name="feature_eng")
            .add_new_column("amount * 1.1", "adjusted")
            .add_new_column(
                "CASE WHEN category = 'food' THEN 1 ELSE 0 END",
                "is_food"
            ),
        
        # Stage 3: DuckDB - Aggregations
        DuckDBTask(name="aggregate")
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
    ],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)
)

# Flow will:
# 1. Detect SparkEngineTask.is_spark_job = True
# 2. Create SparkSession
# 3. Run Spark task
# 4. Convert PySpark DataFrame → PyArrow Table
# 5. Run DuckDB tasks
# 6. Convert result back to PySpark DataFrame
result = flow.run()
```

### Benefits of Mixed Pipelines

1. **Performance:** Use DuckDB for feature engineering (10x faster)
2. **Scalability:** Use Spark for large-scale data processing
3. **Flexibility:** Choose the right tool for each stage
4. **Cost:** Reduce compute costs by using lighter engine where possible

---

## Flow with Feature Groups

### Reading from Feature Groups

```python
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB
from seeknal.flow import Flow, FlowInput, FlowInputEnum

# Get feature group
fg = FeatureGroupDuckDB(name="user_features", ...)
fg.get_or_create()

# Read from feature group in flow
flow = Flow(
    name="feature_pipeline",
    input=FlowInput(
        kind=FlowInputEnum.FEATURE_GROUP,
        value=fg
    ),
    tasks=[
        DuckDBTask(name="add_more_features")
            .add_new_column("total_amount * 0.95", "after_tax")
    ],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)
)

result = flow.run()
```

### Writing to Feature Groups

```python
# Create output feature group
output_fg = FeatureGroupDuckDB(
    name="enhanced_features",
    entity=Entity(name="user", join_keys=["user_id"]),
    materialization=Materialization(
        event_time_col="timestamp",
        offline_store_path="/features/enhanced"
    ),
    project="my_project"
)

# Write flow output to feature group
flow = Flow(
    name="feature_creation",
    input=FlowInput(kind=FlowInputEnum.SOURCE, value=arrow_table),
    tasks=[
        DuckDBTask(name="create_features")
            .add_new_column("amount * 1.1", "adjusted")
            .add_stage(aggregator=agg)
    ],
    output=FlowOutput(
        kind=FlowOutputEnum.FEATURE_GROUP,
        value=output_fg
    )
)

# Run flow to create features
flow.run()
```

### Feature Group Versioning in Flow

```python
# Get specific version of feature group
fg_v1 = fg.get_version(version=1)

flow = Flow(
    name="use_v1_features",
    input=FlowInput(
        kind=FlowInputEnum.FEATURE_GROUP,
        value=fg_v1  # Use version 1
    ),
    tasks=[...],
    output=FlowOutput()
)
```

---

## Advanced Flow Patterns

### Pattern 1: Conditional Pipelines

```python
from datetime import datetime

# Choose pipeline based on data size
def get_flow(data_size: int) -> Flow:
    if data_size > 1_000_000:
        # Large data - use Spark
        return Flow(
            name="large_pipeline",
            input=FlowInput(kind=FlowInputEnum.PARQUET, value="large_data.parquet"),
            tasks=[
                SparkEngineTask().add_sql("SELECT * FROM __THIS__")
            ],
            output=FlowOutput()
        )
    else:
        # Small data - use DuckDB (faster!)
        return Flow(
            name="small_pipeline",
            input=FlowInput(kind=FlowInputEnum.PARQUET, value="small_data.parquet"),
            tasks=[
                DuckDBTask(name="process").add_sql("SELECT * FROM __THIS__")
            ],
            output=FlowOutput()
        )

# Use
flow = get_flow(data_size=100_000)
result = flow.run()
```

### Pattern 2: Parameterized Flows

```python
def create_feature_pipeline(
    feature_start_time: datetime,
    window_days: int = 7
) -> Flow:
    """Create a feature pipeline with parameters."""
    
    agg = LastNDaysAggregator(
        group_by_cols=["user_id"],
        window=window_days,
        date_col="date_id",
        date_pattern="yyyyMMdd",
        aggregators=[
            FunctionAggregator(
                inputCol="amount",
                outputCol=f"amount_sum_{window_days}d",
                accumulatorFunction="sum"
            )
        ]
    )
    
    return Flow(
        name=f"features_{window_days}d",
        input=FlowInput(kind=FlowInputEnum.SOURCE, value=arrow_table),
        tasks=[
            DuckDBTask(name="aggregate").add_stage(aggregator=agg)
        ],
        output=FlowOutput(kind=FlowOutputEnum.FEATURE_GROUP, value=fg)
    )

# Use with different parameters
flow_7d = create_feature_pipeline(
    feature_start_time=datetime(2024, 1, 1),
    window_days=7
)

flow_30d = create_feature_pipeline(
    feature_start_time=datetime(2024, 1, 1),
    window_days=30
)
```

### Pattern 3: Chained Flows

```python
# Flow 1: Raw → Features
flow1 = Flow(
    name="create_features",
    input=FlowInput(kind=FlowInputEnum.PARQUET, value="raw.parquet"),
    tasks=[...],
    output=FlowOutput(
        kind=FlowOutputEnum.FEATURE_GROUP,
        value=feature_group
    )
)

# Flow 2: Features → Model Training
flow2 = Flow(
    name="train_model",
    input=FlowInput(
        kind=FlowInputEnum.FEATURE_GROUP,
        value=feature_group
    ),
    tasks=[...],
    output=FlowOutput(kind=FlowOutputEnum.PARQUET, value="training_data.parquet")
)

# Run sequentially
flow1.run()
flow2.run()
```

---

## Best Practices

### 1. Always Name Your Tasks

```python
# Good - clear task names
tasks=[
    DuckDBTask(name="filter_invalid").add_filter_by_expr("amount > 0"),
    DuckDBTask(name="add_discount").add_new_column("amount * 0.9", "discounted"),
    DuckDBTask(name="aggregate_by_user").add_stage(aggregator=agg)
]

# Bad - no task names
tasks=[
    DuckDBTask().add_filter_by_expr("amount > 0"),
    DuckDBTask().add_new_column("amount * 0.9", "discounted"),
    DuckDBTask().add_stage(aggregator=agg)
]
```

### 2. Use Appropriate Input/Output Types

```python
# For PyArrow Tables (DuckDB)
FlowInput(kind=FlowInputEnum.SOURCE, value=arrow_table)
FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)

# For PySpark DataFrames (Spark)
FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="table_name")
FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)

# For files
FlowInput(kind=FlowInputEnum.PARQUET, value="/data/input.parquet")
FlowOutput(kind=FlowOutputEnum.PARQUET, value="/data/output.parquet")
```

### 3. Optimize Task Order

```python
# Good: Filter early, transform later
tasks=[
    DuckDBTask(name="filter").add_filter_by_expr("amount > 100"),
    DuckDBTask(name="transform").add_new_column("amount * 1.1", "adjusted")
]

# Bad: Transform all data, then filter
tasks=[
    DuckDBTask(name="transform").add_new_column("amount * 1.1", "adjusted"),
    DuckDBTask(name="filter").add_filter_by_expr("adjusted > 100")
]
```

### 4. Save Flow Definitions

```python
# Always save flow to database
flow.get_or_create()  # Saves pipeline definition

# Now you can:
# - Version control pipelines
# - Reproduce runs
# - Share with team
# - Track metadata
```

### 5. Use Pure DuckDB When Possible

```python
# If you don't need Spark, use DuckDB only (faster!)
flow = Flow(
    name="duckdb_pipeline",
    tasks=[
        DuckDBTask(name="task1"),
        DuckDBTask(name="task2")
    ]
    # No SparkSession created!
    # 10x faster startup
    # Lower memory usage
)
```

---

## Flow API Reference

### Flow Class

```python
class Flow:
    def __init__(
        self,
        name: str,                    # Unique pipeline name
        input: FlowInput,              # Data source
        tasks: List[Task],             # List of tasks (SparkEngineTask or DuckDBTask)
        output: FlowOutput,            # Data destination
        description: str = "",         # Optional description
        tags: List[str] = [],          # Optional tags
    )
```

### Methods

```python
# Save flow to database
flow.get_or_create()

# Run pipeline
result = flow.run()

# Get flow metadata
flow_info = flow.get()

# List all flows
flows = Flow.list()
```

---

## Troubleshooting

### Issue 1: "SparkSession not available"

**Problem:**
```
TypeError: Cannot convert PySpark DataFrame without SparkSession
```

**Solution:**
```python
# Flow will create SparkSession automatically if needed
# But you can also provide one explicitly

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

result = flow.run(spark=spark)
```

### Issue 2: "Type conversion failed"

**Problem:**
```
Error: Cannot convert PyArrow Table to PySpark DataFrame
```

**Solution:**
```python
# Ensure output type matches task types
# Pure DuckDB tasks → ARROW_DATAFRAME or PANDAS_DATAFRAME
# Tasks with Spark → SPARK_DATAFRAME

flow = Flow(
    tasks=[DuckDBTask(...), DuckDBTask(...)],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME)  # ✓ Correct
)
```

---

## Examples

See [Examples](./examples/duckdb-flow-examples.md) for complete, runnable examples.

---

**Last Updated:** 2026-01-07  
**Version:** 1.0.0
