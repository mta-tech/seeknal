# Python Pipelines Quick Reference

Quick reference for Seeknal's Python pipeline API.

## Decorators

### `@source` - Define Data Source

```python
from seeknal.pipeline import source

@source(
    name="my_source",              # Node name
    source="csv",                   # Source type: csv, parquet, postgres, etc.
    table="path/to/data.csv",       # File path or table name
    description="My data source",    # Optional description
    tags=["raw", "production"],      # Optional tags
    # columns={"col1": "int"},        # Optional schema
)
def my_source(ctx=None):
    """Load data from source."""
    # Return a DataFrame
    return pd.read_csv("path/to/data.csv")
```

### `@transform` - Define Transformation

```python
from seeknal.pipeline import transform

@transform(
    name="my_transform",
    description="Transform data",
    # inputs=["source.upstream"],    # Optional explicit dependencies
)
def my_transform(ctx):
    """Transform upstream data."""
    # Reference upstream node
    df = ctx.ref("source.my_source")

    # Use DuckDB for SQL
    result = ctx.duckdb.sql("SELECT * FROM df WHERE active = true").df()

    # Return transformed DataFrame
    return result
```

### `@feature_group` - Define Feature Group

```python
from seeknal.pipeline import feature_group
from seeknal.pipeline.materialization import Materialization, OfflineConfig

@feature_group(
    name="my_features",
    entity="customer",              # Entity name for joins
    description="Customer features",
    materialization=Materialization(
        offline=OfflineConfig(
            format="parquet",
            partition_by=["year", "month"]
        )
    ),
)
def my_features(ctx):
    """Build feature group."""
    df = ctx.ref("transform.my_transform")

    return ctx.duckdb.sql("""
        SELECT customer_id, COUNT(*) as purchase_count
        FROM df
        GROUP BY customer_id
    """).df()
```

## PipelineContext (ctx)

### `ctx.ref(node_id)` - Reference Upstream Data

```python
# Reference by node_id
df = ctx.ref("source.raw_data")
df = ctx.ref("transform.clean_data")
df = ctx.ref("feature_group.user_features")
```

### `ctx.duckdb` - DuckDB Connection

```python
# SQL queries
result = ctx.duckdb.sql("SELECT * FROM df").df()

# Register multiple DataFrames
ctx.duckdb.register("orders", orders_df)
ctx.duckdb.register("customers", customers_df)

# Complex joins
result = ctx.duckdb.sql("""
    SELECT c.*, COUNT(o.order_id) as order_count
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id
""").df()
```

### `ctx.config` - Profile Configuration

```python
# Access profile config
db_path = ctx.config.get("path", ":memory:")
warehouse_path = ctx.config.get("warehouse", "./warehouse")
```

## PEP 723 Inline Dependencies

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas>=2.0",
#     "pyarrow>=14.0",
#     "duckdb>=0.9.0",
#     "scikit-learn",
# ]
# ///

# Your code here...
```

**Common dependencies:**
- `pandas` - DataFrame operations
- `pyarrow` - Parquet support (required for pandas.to_parquet())
- `duckdb` - SQL queries
- `scikit-learn` - Machine learning
- `numpy` - Numerical operations
- `requests` - HTTP calls

## CLI Commands

### Initialize Project

```bash
seeknal init --name my_project --description "My data project"
cd my_project
```

### Run Pipeline

```bash
# Show execution plan
seeknal run --show-plan

# Run all nodes
seeknal run

# Run specific nodes
seeknal run --nodes my_transform

# Run by type
seeknal run --type transform

# Dry-run (validate without executing)
seeknal dry-run seeknal/transforms/my_transform.yml
```

### Draft Python Templates

```bash
# Create Python source
seeknal draft source my_data --python --deps pandas,pyarrow

# Create Python transform
seeknal draft transform clean_data --python -d "Clean raw data"

# Create Python feature group
seeknal draft feature-group user_features --python
```

## File Locations

```
my_project/
├── seeknal/
│   ├── pipelines/              # Python pipeline files (*.py)
│   │   ├── sources/
│   │   ├── transforms/
│   │   └── feature_groups/
│   └── templates/             # Custom Jinja templates (optional)
├── target/
│   ├── manifest.json           # Unified DAG manifest
│   ├── intermediate/           # Cross-node outputs (*.parquet)
│   └── state/                 # Execution state
├── seeknal_project.yml         # Project config
└── profiles.yml               # Credentials (gitignored)
```

## Best Practices

### 1. Use Descriptive Node Names

```python
# Good
@source(name="raw_web_events")

# Avoid
@source(name="data")
```

### 2. Add Dependencies to PEP 723 Header

```python
# /// script
# dependencies = [
#     "pandas",
#     "pyarrow",  # Required for parquet support
# ]
# ///
```

### 3. Use Type Hints

```python
from typing import Any
import pandas as pd

def my_transform(ctx) -> pd.DataFrame:
    """Transform data with type hints."""
    df: pd.DataFrame = ctx.ref("source.raw")
    return df
```

### 4. Handle Errors Gracefully

```python
@transform(name="robust_transform")
def robust_transform(ctx):
    """Transform with error handling."""
    try:
        df = ctx.ref("source.raw")
        return ctx.duckdb.sql("SELECT * FROM df").df()
    except Exception as e:
        # Log error and return empty DataFrame
        print(f"Warning: {e}")
        return pd.DataFrame()
```

### 5. Document Your Pipelines

```python
"""
Customer churn prediction pipeline.

This pipeline:
1. Loads raw customer data (source)
2. Cleans and validates (transform)
3. Computes churn features (feature_group)
4. Trains prediction model (model)
5. Exports predictions (exposure)

Author: data@company.com
Last updated: 2025-01-28
"""
```

## Common Patterns

### Filter Data

```python
@transform(name="active_users")
def active_users(ctx):
    df = ctx.ref("source.users")
    return ctx.duckdb.sql("""
        SELECT * FROM df WHERE is_active = true
    """).df()
```

### Aggregations

```python
@transform(name="daily_metrics")
def daily_metrics(ctx):
    df = ctx.ref("source.events")
    return ctx.duckdb.sql("""
        SELECT
            DATE(event_time) as date,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_id) as unique_users
        FROM df
        GROUP BY DATE(event_time)
        ORDER BY date
    """).df()
```

### Joins

```python
@transform(name="enriched_orders")
def enriched_orders(ctx):
    orders = ctx.ref("source.orders")
    customers = ctx.ref("source.customers")

    # Register for SQL access
    ctx.duckdb.register("orders", orders)
    ctx.duckdb.register("customers", customers)

    return ctx.duckdb.sql("""
        SELECT o.*, c.name, c.segment
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
    """).df()
```

### Window Functions

```python
@transform(name="user_rankings")
def user_rankings(ctx):
    df = ctx.ref("transform.user_metrics")
    return ctx.duckdb.sql("""
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY country ORDER BY score DESC) as country_rank,
            RANK() OVER (ORDER BY score DESC) as global_rank
        FROM df
    """).df()
```

## Troubleshooting

### ModuleNotFoundError

```
Solution: Add missing package to PEP 723 dependencies
# dependencies = [
#     "missing-package",
# ]
```

### Circular Dependencies

```
Solution: Break cycle by creating intermediate nodes
# Instead of: A -> B -> C -> A
# Create: A -> B -> C, A -> D
```

### Large Intermediate Files

```
Solution: Partition outputs
@feature_group(
    materialization=Materialization(
        offline=OfflineConfig(
            format="parquet",
            partition_by=["year", "month"]
        )
    )
)
```

## Further Reading

- [Full Tutorial](python-pipelines-tutorial.md)
- [API Reference](../api/)
- [Materialization Options](../api/materialization.md)
- [Executor Internals](../architecture/executors.md)
