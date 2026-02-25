# Flows & Pipelines

This guide demonstrates how to create and manage data processing pipelines in Seeknal. Seeknal supports three pipeline approaches:

1. **YAML Pipelines** (recommended) - Declarative YAML definitions in the `seeknal/` directory
2. **Python Decorator Pipelines** - `@source`, `@transform`, `@materialize` decorators in `.py` files
3. **Legacy Spark Flows** - Programmatic `Flow` objects with SparkEngineTask/DuckDBTask

## YAML Pipelines

YAML pipelines are the primary way to define data transformations. They live in your project's `seeknal/` directory and are executed via the CLI.

### Project Structure

```
my_project/
├── seeknal.yml              # Project configuration
├── profiles.yml             # Connection profiles
└── seeknal/
    ├── sources/
    │   └── raw_orders.yml   # Data source definitions
    └── transforms/
        └── order_summary.yml # Transformation definitions
```

### Defining a Source

```yaml
# seeknal/sources/raw_orders.yml
name: raw_orders
source: csv
table: data/orders.csv
columns:
  order_id: VARCHAR
  customer_id: VARCHAR
  amount: DOUBLE
  order_date: DATE
```

### Defining a Source from PostgreSQL

```yaml
# seeknal/sources/pg_customers.yml
name: pg_customers
source: postgresql
connection: local_pg
table: public.customers
```

### Defining a Transform

```yaml
# seeknal/transforms/order_summary.yml
name: order_summary
inputs:
  - ref: source.raw_orders
sql: |
  SELECT
    customer_id,
    CAST(COUNT(*) AS BIGINT) AS order_count,
    CAST(SUM(amount) AS DOUBLE) AS total_spend,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order
  FROM input_0
  GROUP BY customer_id
```

### Multi-Input Transform

```yaml
# seeknal/transforms/customer_orders.yml
name: customer_orders
inputs:
  - ref: source.raw_orders
  - ref: source.pg_customers
sql: |
  SELECT
    c.customer_id,
    c.name,
    CAST(COUNT(o.order_id) AS BIGINT) AS order_count,
    CAST(SUM(o.amount) AS DOUBLE) AS total_spend
  FROM input_1 c
  LEFT JOIN input_0 o ON c.customer_id = o.customer_id
  GROUP BY c.customer_id, c.name
```

### Transform with Named Refs

Instead of positional `input_0`, `input_1`, use named refs:

```yaml
# seeknal/transforms/enriched_orders.yml
name: enriched_orders
inputs:
  - ref: source.raw_orders
  - ref: source.pg_customers
sql: |
  SELECT
    o.*,
    c.name AS customer_name
  FROM ref('source.raw_orders') o
  JOIN ref('source.pg_customers') c ON o.customer_id = c.customer_id
```

### Adding Materialization

Write pipeline outputs to external storage targets:

```yaml
# seeknal/transforms/order_summary.yml
name: order_summary
inputs:
  - ref: source.raw_orders
sql: |
  SELECT
    customer_id,
    CAST(COUNT(*) AS BIGINT) AS order_count,
    CAST(SUM(amount) AS DOUBLE) AS total_spend
  FROM input_0
  GROUP BY customer_id

# Single target materialization
materialization:
  type: postgresql
  connection: local_pg
  table: analytics.order_summary
  mode: full
```

### Multi-Target Materialization

Write to multiple targets from a single transform:

```yaml
# seeknal/transforms/order_summary.yml
name: order_summary
inputs:
  - ref: source.raw_orders
sql: |
  SELECT
    customer_id,
    CAST(COUNT(*) AS BIGINT) AS order_count,
    CAST(SUM(amount) AS DOUBLE) AS total_spend
  FROM input_0
  GROUP BY customer_id

# Multi-target: write to both PostgreSQL and Iceberg
materializations:
  - type: postgresql
    connection: local_pg
    table: analytics.order_summary
    mode: upsert_by_key
    unique_keys: [customer_id]
  - type: iceberg
    table: atlas.analytics.order_summary
```

### Running YAML Pipelines

```bash
# Preview what will be executed
seeknal dry-run

# Execute the pipeline
seeknal apply

# Execute in parallel
seeknal apply --parallel --max-workers 8

# Query results in REPL
seeknal repl
```

!!! tip "YAML Pipeline Tips"
    - Use `CAST(COUNT(*) AS BIGINT)` and `CAST(SUM(...) AS DOUBLE)` for Iceberg targets (DuckDB HUGEINT is not supported by Iceberg)
    - Use `ref('source.X')` named refs for readability; positional `input_0` also works
    - Use `seeknal dry-run` to preview the execution plan before applying

---

## Python Decorator Pipelines

For pipelines that need Python logic beyond SQL, use decorators in `.py` files under `seeknal/pipelines/`.

### Basic Pipeline

```python
# seeknal/pipelines/customer_pipeline.py
# /// script
# dependencies = ["pandas", "duckdb"]
# ///

from seeknal.pipeline.decorators import source, transform

@source(name="raw_users", source="csv", table="data/users.csv")
def raw_users():
    pass

@transform(name="active_users", inputs=["source.raw_users"])
def active_users(ctx):
    df = ctx.ref("source.raw_users")
    return ctx.duckdb.sql("""
        SELECT * FROM df WHERE status = 'active'
    """).df()
```

### Pipeline with Materialization

```python
# seeknal/pipelines/analytics_pipeline.py
# /// script
# dependencies = ["pandas", "duckdb"]
# ///

from seeknal.pipeline.decorators import source, transform, materialize

@source(name="orders", source="csv", table="data/orders.csv")
def orders():
    pass

# Multi-target materialization with stacked decorators
@transform(name="order_metrics", inputs=["source.orders"])
@materialize(
    type="postgresql",
    connection="local_pg",
    table="analytics.order_metrics",
    mode="upsert_by_key",
    unique_keys=["customer_id"],
)
@materialize(
    type="iceberg",
    table="atlas.analytics.order_metrics",
)
def order_metrics(ctx):
    df = ctx.ref("source.orders")
    return ctx.duckdb.sql("""
        SELECT
            customer_id,
            CAST(COUNT(*) AS BIGINT) AS order_count,
            CAST(SUM(amount) AS DOUBLE) AS total_spend
        FROM df
        GROUP BY customer_id
    """).df()
```

### Pipeline with PostgreSQL Source

```python
# seeknal/pipelines/pg_pipeline.py
# /// script
# dependencies = ["pandas", "duckdb"]
# ///

from seeknal.pipeline.decorators import source, transform

@source(
    name="pg_orders",
    source="postgres",
    connection="local_pg",
    query="SELECT * FROM orders WHERE status = 'active'",
)
def pg_orders():
    pass

@transform(name="order_analysis", inputs=["source.pg_orders"])
def order_analysis(ctx):
    df = ctx.ref("source.pg_orders")
    return ctx.duckdb.sql("""
        SELECT
            region,
            CAST(COUNT(*) AS BIGINT) AS order_count,
            CAST(AVG(amount) AS DOUBLE) AS avg_amount
        FROM df
        GROUP BY region
    """).df()
```

### Feature Group Decorator

```python
from seeknal.pipeline.decorators import source, feature_group
from seeknal.pipeline.materialization import Materialization, OfflineConfig

@source(name="user_activity", source="parquet", table="data/activity.parquet")
def user_activity():
    pass

@feature_group(
    name="user_engagement",
    entity="user",
    inputs=["source.user_activity"],
    materialization=Materialization(offline=OfflineConfig(format="parquet")),
)
def user_engagement(ctx):
    df = ctx.ref("source.user_activity")
    return ctx.duckdb.sql("""
        SELECT
            user_id,
            CAST(COUNT(*) AS BIGINT) AS session_count,
            CAST(AVG(duration) AS DOUBLE) AS avg_duration
        FROM df
        GROUP BY user_id
    """).df()
```

!!! tip "Python Pipeline Tips"
    - Add PEP 723 `# /// script` metadata at the top with dependencies
    - Use `ctx.ref("source.X")` to reference upstream nodes
    - Use `ctx.duckdb.sql(...)` for DuckDB SQL queries
    - Stack multiple `@materialize` decorators for multi-target output
    - Assign `ctx.ref()` results to a local variable before using in DuckDB SQL

---

## Legacy: Spark Flows

The `Flow` API provides programmatic pipeline creation using SparkEngineTask or DuckDBTask. This approach is suited for Spark-based distributed processing.

> **Note:** For new projects, prefer YAML pipelines or Python decorator pipelines. The Flow API is maintained for backward compatibility with existing Spark-based projects.

### Prerequisites

- Apache Spark or PySpark installed
- SparkSession available

### Basic Flow with SparkEngineTask

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Define input from Hive table
flow_input = FlowInput(
    value="my_database.source_table",
    kind=FlowInputEnum.HIVE_TABLE
)

# Create transformation task with SQL
transform_task = (
    SparkEngineTask(name="filter_and_transform")
    .add_sql("SELECT user_id, event_type, amount FROM __THIS__ WHERE amount > 0")
    .add_new_column("amount * 1.1", "adjusted_amount")
    .add_filter_by_expr("event_type = 'purchase'")
)

# Create flow
spark_flow = Flow(
    name="spark_transformation_flow",
    input=flow_input,
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

# DuckDB transformation
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

duckdb_flow = Flow(
    name="duckdb_aggregation_flow",
    input=FlowInput(value="/data/events.parquet", kind=FlowInputEnum.PARQUET),
    tasks=[duckdb_task],
    output=FlowOutput(kind=FlowOutputEnum.ARROW_DATAFRAME),
)

result = duckdb_flow.run()
print(result.to_pandas())
```

### Chaining Multiple Tasks

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Chain multiple tasks - each receives the output of the previous
cleaning_task = (
    SparkEngineTask(name="data_cleaning")
    .add_filter_by_expr("user_id IS NOT NULL")
    .add_sql("SELECT DISTINCT * FROM __THIS__")
)

feature_task = (
    SparkEngineTask(name="feature_engineering")
    .add_new_column("DATEDIFF(current_date(), signup_date)", "days_since_signup")
    .add_new_column("total_purchases / order_count", "avg_order_value")
)

aggregation_task = (
    SparkEngineTask(name="aggregation")
    .add_sql("""
        SELECT segment, COUNT(*) as user_count, AVG(avg_order_value) as segment_avg
        FROM __THIS__
        GROUP BY segment
    """)
)

multi_task_flow = Flow(
    name="multi_stage_pipeline",
    input=FlowInput(value="analytics.user_data", kind=FlowInputEnum.HIVE_TABLE),
    tasks=[cleaning_task, feature_task, aggregation_task],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
)

result = multi_task_flow.run()
result.show()
```

### Saving and Loading Flows

```python
from seeknal.flow import Flow

# Save to backend (idempotent)
my_flow = my_flow.get_or_create()
print(f"Flow ID: {my_flow.flow_id}")

# List all flows
Flow.list()

# Export as YAML for version control
flow_yaml = my_flow.as_yaml()
```

!!! tip "Task Execution Order"
    Tasks are executed sequentially in the order they appear in the list.
    Each task receives the output of the previous task as input.

---

## Best Practices

!!! tip "Choosing a Pipeline Approach"
    - **YAML pipelines**: Best for SQL-centric transforms, team collaboration, and reproducibility
    - **Python decorator pipelines**: Best when transforms need Python logic beyond SQL
    - **Spark Flows**: Best for distributed processing of large datasets (>100M rows)

!!! tip "Performance"
    - Use `seeknal apply --parallel` for parallel execution of independent nodes
    - Use DuckDB-based approaches for datasets under 100M rows
    - Use virtual environments (`seeknal plan dev`) to test changes safely
    - Chain filters early in the pipeline to reduce data volume

!!! warning "Iceberg Compatibility"
    When writing to Iceberg tables, always cast aggregation results:
    - `CAST(COUNT(*) AS BIGINT)` instead of bare `COUNT(*)`
    - `CAST(SUM(...) AS DOUBLE)` instead of bare `SUM(...)`
    DuckDB returns HUGEINT for these, which Iceberg does not support.

## Next Steps

1. **Feature Store** - Store computed features ([FeatureStore Example](featurestore.md))
2. **DAG Tutorial** - Dependency tracking and incremental builds ([DAG Tutorial](seeknal-2.0-dag-tutorial.md))
3. **Virtual Environments** - Test changes safely ([Virtual Environments](../concepts/virtual-environments.md))
4. **CLI Reference** - Full command reference ([CLI Reference](../reference/cli.md))
