# Flows

This guide demonstrates how to create and manage data processing pipelines using Seeknal Flows. Flows are the core abstraction for defining ETL/ELT pipelines that chain inputs, transformation tasks, and outputs.

## Prerequisites

Before running these examples, ensure you have:

1. Seeknal installed (see [Installation Guide](../index.md#installation))
2. Project configured (see [Initialization](initialization.md))
3. A SparkSession active (Seeknal will create one if needed for Spark-based tasks)
4. Environment variables configured:
   ```bash
   export SEEKNAL_BASE_CONFIG_PATH="/path/to/config/directory"
   export SEEKNAL_USER_CONFIG_PATH="/path/to/config.toml"
   ```

## Core Concepts

### Flow Architecture

A **Flow** connects three components:

1. **Input** (`FlowInput`) - Data source configuration (Hive tables, Parquet files, extractors, sources)
2. **Tasks** - List of transformation tasks executed in sequence (SparkEngineTask, DuckDBTask)
3. **Output** (`FlowOutput`) - Output destination or format (DataFrames, Hive tables, Parquet files)

### Input Types

| Type | Description |
|------|-------------|
| `HIVE_TABLE` | Read from a Hive table |
| `PARQUET` | Read from Parquet files |
| `EXTRACTOR` | Use a custom Extractor class |
| `SOURCE` | Read from a defined Source configuration |
| `FEATURE_GROUP` | Read from a Feature Group (not yet implemented) |

### Output Types

| Type | Description |
|------|-------------|
| `SPARK_DATAFRAME` | Return as PySpark DataFrame |
| `ARROW_DATAFRAME` | Return as PyArrow Table |
| `PANDAS_DATAFRAME` | Return as Pandas DataFrame |
| `HIVE_TABLE` | Write to a Hive table |
| `PARQUET` | Write to Parquet files |
| `LOADER` | Use a custom loader |
| `FEATURE_GROUP` | Output to a Feature Group |
| `FEATURE_SERVING` | Output for feature serving |

## Creating a Basic Flow

### Flow with Hive Table Input

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum

# Define input from Hive table
flow_input = FlowInput(
    value="my_database.source_table",
    kind=FlowInputEnum.HIVE_TABLE
)

# Define output as Spark DataFrame
flow_output = FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)

# Create the flow
simple_flow = Flow(
    name="simple_etl_flow",
    input=flow_input,
    output=flow_output,
    description="Simple flow reading from Hive table"
)

# Run the flow
result = simple_flow.run()
result.show()
```

!!! note "Name Normalization"
    Flow names are automatically converted to snake_case. For example,
    `"My ETL Flow"` becomes `"my_etl_flow"`.

### Flow with Parquet Input

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum

# Read from Parquet files
parquet_flow = Flow(
    name="parquet_reader_flow",
    input=FlowInput(
        value="/data/warehouse/events.parquet",
        kind=FlowInputEnum.PARQUET
    ),
    output=FlowOutput(kind=FlowOutputEnum.PANDAS_DATAFRAME),
    description="Flow reading Parquet files and returning Pandas DataFrame"
)

# Run and get Pandas DataFrame
df = parquet_flow.run()
print(df.head())
```

### Flow with Source Input

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum

# Read from a configured Source
source_flow = Flow(
    name="source_reader_flow",
    input=FlowInput(
        value="customer_transactions",  # Source ID defined in common.yaml
        kind=FlowInputEnum.SOURCE
    ),
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
    description="Flow reading from predefined Source"
)

result = source_flow.run()
```

## Adding Transformation Tasks

Tasks define the data transformations applied to input data. Seeknal supports two task engines:

- **SparkEngineTask**: Spark-based transformations (distributed processing)
- **DuckDBTask**: DuckDB-based transformations (single-node, SQL-centric)

### Flow with SparkEngineTask

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Create transformation task with SQL
transform_task = (
    SparkEngineTask(name="filter_and_transform")
    .add_sql("SELECT user_id, event_type, amount FROM __THIS__ WHERE amount > 0")
    .add_new_column("amount * 1.1", "adjusted_amount")
    .add_filter_by_expr("event_type = 'purchase'")
)

# Create flow with task
spark_flow = Flow(
    name="spark_transformation_flow",
    input=FlowInput(value="events_db.raw_events", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[transform_task],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
    description="Flow with Spark transformations"
)

result = spark_flow.run()
result.show()
```

### Flow with DuckDBTask

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.duckdb import DuckDBTask

# Create DuckDB transformation task
duckdb_task = (
    DuckDBTask()
    .add_sql("""
        SELECT
            user_id,
            COUNT(*) as event_count,
            SUM(amount) as total_amount
        FROM __THIS__
        GROUP BY user_id
    """)
)

# Create flow with DuckDB task (returns PyArrow Table by default)
duckdb_flow = Flow(
    name="duckdb_aggregation_flow",
    input=FlowInput(value="/data/events.parquet", kind=FlowInputEnum.PARQUET),
    tasks=[duckdb_task],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME),
    description="Flow with DuckDB aggregation"
)

result = duckdb_flow.run()
print(result.to_pandas())
```

### Chaining Multiple Tasks

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# First task: Clean and filter data
cleaning_task = (
    SparkEngineTask(name="data_cleaning")
    .add_filter_by_expr("user_id IS NOT NULL")
    .add_sql("SELECT DISTINCT * FROM __THIS__")
)

# Second task: Feature engineering
feature_task = (
    SparkEngineTask(name="feature_engineering")
    .add_new_column("DATEDIFF(current_date(), signup_date)", "days_since_signup")
    .add_new_column("total_purchases / order_count", "avg_order_value")
)

# Third task: Final aggregation
aggregation_task = (
    SparkEngineTask(name="user_aggregation")
    .add_sql("""
        SELECT
            segment,
            COUNT(*) as user_count,
            AVG(avg_order_value) as segment_avg_order
        FROM __THIS__
        GROUP BY segment
    """)
)

# Chain tasks in the flow
multi_task_flow = Flow(
    name="multi_stage_pipeline",
    input=FlowInput(value="analytics.user_data", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[cleaning_task, feature_task, aggregation_task],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
    description="Multi-stage ETL pipeline"
)

result = multi_task_flow.run()
result.show()
```

!!! tip "Task Execution Order"
    Tasks are executed sequentially in the order they appear in the list.
    Each task receives the output of the previous task as input.

## Date Filtering

### Setting Up Date Column Configuration

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Create flow with date column configuration
date_flow = Flow(
    name="daily_aggregation_flow",
    input=FlowInput(value="events_db.daily_events", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[SparkEngineTask().add_sql("SELECT * FROM __THIS__")],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
    description="Flow with date-based filtering"
)

# Configure the date column for filtering
date_flow.set_input_date_col(
    date_col="event_date",
    date_pattern="yyyyMMdd"  # Date format pattern
)

# Run with date range filter
result = date_flow.run(
    start_date="2024-01-01",
    end_date="2024-01-31"
)
result.show()
```

### Running with Single Date Filter

```python
# Run for a specific date
result = date_flow.run(date="2024-01-15")
result.show()
```

## Output Types

### Writing to Hive Table

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Create flow that writes to Hive table
hive_output_flow = Flow(
    name="write_to_hive_flow",
    input=FlowInput(value="source_db.raw_data", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[SparkEngineTask().add_sql("SELECT * FROM __THIS__ WHERE is_valid = true")],
    output=FlowOutput(
        value="target_db.processed_data",
        kind=FlowOutputEnum.HIVE_TABLE
    ),
    description="Flow that writes results to Hive table"
)

# Run - output is written to Hive table, returns None
hive_output_flow.run()
```

### Writing to Parquet Files

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Create flow that writes to Parquet
parquet_output_flow = Flow(
    name="write_to_parquet_flow",
    input=FlowInput(value="source_db.raw_data", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[SparkEngineTask().add_sql("SELECT * FROM __THIS__")],
    output=FlowOutput(
        value="/data/output/processed_data.parquet",
        kind=FlowOutputEnum.PARQUET
    ),
    description="Flow that writes results to Parquet files"
)

# Run - output is written to Parquet files, returns None
parquet_output_flow.run()
```

### Using Custom Loader

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask
from seeknal.tasks.sparkengine.loaders import Loader

# Configure a custom loader
custom_loader = Loader(
    class_name="tech.mta.seeknal.loaders.JDBCLoader",
    params={
        "url": "jdbc:postgresql://localhost:5432/mydb",
        "table": "output_table",
        "mode": "overwrite"
    }
)

# Create flow with loader output
loader_flow = Flow(
    name="jdbc_loader_flow",
    input=FlowInput(value="source_db.data", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[SparkEngineTask().add_sql("SELECT * FROM __THIS__")],
    output=FlowOutput(
        value=custom_loader,
        kind=FlowOutputEnum.LOADER
    ),
    description="Flow that loads data via custom loader"
)

loader_flow.run()
```

## Running Flows with Parameters

### Passing Parameters to Tasks

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Create flow
parameterized_flow = Flow(
    name="parameterized_flow",
    input=FlowInput(value="analytics.events", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[SparkEngineTask().add_sql("SELECT * FROM __THIS__")],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
    description="Flow with runtime parameters"
)

# Run with custom parameters
result = parameterized_flow.run(
    params={"threshold": 100, "category": "premium"},
    start_date="2024-01-01",
    end_date="2024-01-31"
)
```

### Applying Runtime Filters

```python
# Run with additional filters
result = parameterized_flow.run(
    filters={"region": "APAC", "status": "active"}
)
```

## Saving and Loading Flows

### Saving Flow to Backend

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Create flow
my_flow = Flow(
    name="production_etl_flow",
    input=FlowInput(value="prod_db.source", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[SparkEngineTask().add_sql("SELECT * FROM __THIS__")],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
    description="Production ETL pipeline"
)

# Save to backend (or load if already exists)
my_flow = my_flow.get_or_create()

# Flow now has an ID
print(f"Flow ID: {my_flow.flow_id}")
```

### Listing All Flows

```python
from seeknal.flow import Flow

# Display all flows in the current project
Flow.list()
```

Output:
```
| name                 | description              | flow_spec        | created_at          | updated_at          |
|----------------------|--------------------------|------------------|---------------------|---------------------|
| production_etl_flow  | Production ETL pipeline  | ...              | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
```

### Updating a Flow

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Load existing flow
flow = Flow(name="production_etl_flow").get_or_create()

# Create new task
new_task = SparkEngineTask().add_sql("SELECT * FROM __THIS__ WHERE is_active = true")

# Update the flow
flow.update(
    tasks=[new_task],
    description="Updated production ETL pipeline with active filter"
)
```

### Deleting a Flow

```python
from seeknal.flow import Flow

# Load and delete flow
flow = Flow(name="production_etl_flow").get_or_create()
flow.delete()
```

!!! warning "Irreversible Operation"
    Deleting a flow permanently removes it from the backend. This operation
    cannot be undone.

## Serialization

### Converting Flow to Dictionary

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Create flow
flow = Flow(
    name="serializable_flow",
    input=FlowInput(value="db.table", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[SparkEngineTask().add_sql("SELECT * FROM __THIS__")],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
    description="Flow for serialization example"
)

# Convert to dictionary
flow_dict = flow.as_dict()
print(flow_dict)
```

Output:
```python
{
    'name': 'serializable_flow',
    'input': {'value': 'db.table', 'kind': 'hive_table'},
    'output': {'value': None, 'kind': 'spark_dataframe'},
    'tasks': [...],
    'description': 'Flow for serialization example',
    'input_date_col': None
}
```

### Converting Flow to YAML

```python
# Convert to YAML string
flow_yaml = flow.as_yaml()
print(flow_yaml)
```

### Creating Flow from Dictionary

```python
from seeknal.flow import Flow

# Flow configuration dictionary
flow_config = {
    "name": "restored_flow",
    "input": {"value": "db.source_table", "kind": "hive_table"},
    "output": {"value": None, "kind": "spark_dataframe"},
    "tasks": [
        {
            "class_name": "seeknal.tasks.sparkengine.SparkEngineTask",
            "stages": []
        }
    ]
}

# Create flow from dictionary
restored_flow = Flow.from_dict(flow_config)
print(f"Restored flow name: {restored_flow.name}")
```

## Using run_flow Helper

The `run_flow` function provides a convenient way to execute flows:

```python
from seeknal.flow import run_flow, Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Option 1: Run by flow name (loads from backend)
result = run_flow(
    flow_name="production_etl_flow",
    start_date="2024-01-01",
    end_date="2024-01-31"
)

# Option 2: Run by flow instance
my_flow = Flow(
    name="adhoc_flow",
    input=FlowInput(value="db.table", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[SparkEngineTask()],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)
)

result = run_flow(
    flow=my_flow,
    params={"key": "value"}
)
```

## Complete Example: End-to-End ETL Pipeline

```python
from seeknal.project import Project
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# ============================================
# Step 1: Initialize Project
# ============================================
project = Project(
    name="etl_project",
    description="ETL pipeline project"
).get_or_create()

print(f"Project initialized: {project.name}")

# ============================================
# Step 2: Define Input Source
# ============================================
flow_input = FlowInput(
    value="raw_db.customer_events",
    kind=FlowInputEnum.HIVE_TABLE
)

# ============================================
# Step 3: Create Transformation Tasks
# ============================================

# Task 1: Data cleaning
cleaning_task = (
    SparkEngineTask(name="data_cleaning")
    .add_filter_by_expr("customer_id IS NOT NULL")
    .add_filter_by_expr("event_timestamp IS NOT NULL")
)

# Task 2: Feature computation
feature_task = (
    SparkEngineTask(name="feature_computation")
    .add_sql("""
        SELECT
            customer_id,
            event_date,
            COUNT(*) as event_count,
            SUM(purchase_amount) as total_purchases,
            AVG(purchase_amount) as avg_purchase
        FROM __THIS__
        GROUP BY customer_id, event_date
    """)
)

# Task 3: Final enrichment
enrichment_task = (
    SparkEngineTask(name="enrichment")
    .add_new_column("total_purchases / event_count", "purchase_frequency")
    .add_new_column("CASE WHEN total_purchases > 1000 THEN 'high' ELSE 'normal' END", "customer_tier")
)

# ============================================
# Step 4: Define Output
# ============================================
flow_output = FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)

# ============================================
# Step 5: Create and Configure Flow
# ============================================
customer_etl_flow = Flow(
    name="customer_feature_pipeline",
    input=flow_input,
    tasks=[cleaning_task, feature_task, enrichment_task],
    output=flow_output,
    description="ETL pipeline for customer feature computation"
)

# Configure date column for filtering
customer_etl_flow.set_input_date_col(
    date_col="event_date",
    date_pattern="yyyy-MM-dd"
)

# Save to backend
customer_etl_flow = customer_etl_flow.get_or_create()
print(f"Flow saved with ID: {customer_etl_flow.flow_id}")

# ============================================
# Step 6: Execute the Flow
# ============================================

# Run for a specific date range
result_df = customer_etl_flow.run(
    start_date="2024-01-01",
    end_date="2024-01-31"
)

print("Flow execution completed!")
print(f"Result row count: {result_df.count()}")
result_df.show(10)

# ============================================
# Step 7: Export Flow Configuration
# ============================================

# Export as YAML for version control
flow_yaml = customer_etl_flow.as_yaml()
print("\nFlow configuration (YAML):")
print(flow_yaml)
```

## Best Practices

!!! tip "Flow Design"
    - **Single Responsibility**: Each task should perform one logical transformation
    - **Meaningful Names**: Use descriptive names for flows and tasks
    - **Documentation**: Always provide descriptions for flows
    - **Date Configuration**: Set up date columns for time-series data to enable efficient filtering

!!! tip "Performance"
    - Use SparkEngineTask for distributed processing of large datasets
    - Use DuckDBTask for smaller datasets or when Spark overhead is unnecessary
    - Chain filters early in the pipeline to reduce data volume
    - Use date filtering to limit data scope when possible

!!! warning "Production Considerations"
    - Always test flows with sample data before production runs
    - Use `get_or_create()` for idempotent flow creation
    - Monitor task execution times to identify bottlenecks
    - Store flow configurations in version control (export as YAML)

## Error Handling

Handle common flow errors gracefully:

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum

# Handle flow not saved
try:
    flow = Flow(name="test_flow")
    # Attempting operations without calling get_or_create() first
    flow.update(description="New description")
except ValueError as e:
    print(f"Flow must be saved first: {e}")

# Handle invalid input type
try:
    flow_input = FlowInput(value=123, kind=FlowInputEnum.HIVE_TABLE)
    flow = Flow(name="invalid_flow", input=flow_input, output=FlowOutput())
    flow.run()
except ValueError as e:
    print(f"Invalid input: {e}")
```

See [Error Handling](error_handling.md) for more comprehensive exception handling patterns.

## Next Steps

After mastering Flows, explore:

1. **Feature Store** - Store computed features ([FeatureStore Example](featurestore.md))
2. **Configuration** - Advanced settings ([Configuration Example](configuration.md))
3. **Error Handling** - Handle edge cases ([Error Handling](error_handling.md))
