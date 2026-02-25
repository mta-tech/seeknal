# Examples

This section provides practical code examples demonstrating common Seeknal usage patterns. Each example is designed to be self-contained and illustrates best practices for working with the Seeknal platform.

## Available Examples

| Example | Description |
|---------|-------------|
| [Initialization](initialization.md) | Project and entity setup, basic configuration |
| [FeatureStore](featurestore.md) | Feature group creation, materialization, and retrieval |
| [Flows](flows.md) | Data pipelines: YAML, Python decorators, and Spark flows |
| [DAG Tutorial](seeknal-2.0-dag-tutorial.md) | DAG dependency tracking, manifest, and incremental builds |
| [Error Handling](error_handling.md) | Exception handling and debugging patterns |
| [Configuration](configuration.md) | Config file usage and environment variables |

## Quick Start

Seeknal supports three primary workflows. Choose the one that fits your use case.

### Workflow 1: CLI Pipeline (Recommended for Data Engineering)

The `draft → dry-run → apply` CLI workflow is the primary way to build data pipelines:

```bash
# 1. Initialize a project
seeknal init --name my_project

# 2. Define sources and transforms in seeknal/ directory (YAML or Python)
# See the tutorials for detailed examples

# 3. Preview what will be executed
seeknal dry-run

# 4. Execute the pipeline
seeknal apply

# 5. Query results interactively
seeknal repl
```

### Workflow 2: Python Decorator Pipeline

Define pipelines in Python using `@source`, `@transform`, and `@materialize` decorators:

```python
# seeknal/pipelines/my_pipeline.py
# /// script
# dependencies = ["pandas", "duckdb"]
# ///

from seeknal.pipeline.decorators import source, transform, materialize

@source(name="raw_orders", source="csv", table="data/orders.csv")
def raw_orders():
    pass

@transform(name="order_summary", inputs=["source.raw_orders"])
@materialize(type="iceberg", table="atlas.analytics.order_summary")
def order_summary(ctx):
    df = ctx.ref("source.raw_orders")
    return ctx.duckdb.sql("""
        SELECT customer_id, COUNT(*) AS order_count, SUM(amount) AS total
        FROM df GROUP BY customer_id
    """).df()
```

```bash
# Execute the Python pipeline
seeknal apply
```

### Workflow 3: Python API (Feature Store)

Programmatic feature group creation for ML feature engineering:

```python
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB, Materialization,
)
from seeknal.entity import Entity
import pandas as pd

# Create feature group with DuckDB engine
entity = Entity(name="user", join_keys=["user_id"])
fg = FeatureGroupDuckDB(
    name="user_features",
    entity=entity,
    materialization=Materialization(event_time_col="event_date"),
)

# Load data and write features
df = pd.read_parquet("data/user_activity.parquet")
fg.set_dataframe(df).set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))
```

## Prerequisites

Before running the examples, ensure you have:

1. **Seeknal installed**: `pip install seeknal` or from source (see [Installation Guide](../index.md#installation))
2. **Project initialized**: `seeknal init --name my_project`
3. **Data engine**: DuckDB (included) or Apache Spark (optional, for distributed processing)

## Example Categories

### Getting Started

If you're new to Seeknal, start with these:

1. [Initialization](initialization.md) - Set up projects and entities
2. [Configuration](configuration.md) - Understand configuration options

### Data Engineering

For data pipeline and transformation workflows:

1. [Flows](flows.md) - YAML pipelines, Python decorators, and Spark flows
2. [DAG Tutorial](seeknal-2.0-dag-tutorial.md) - Dependency tracking and incremental builds
3. [Error Handling](error_handling.md) - Handle errors gracefully

### Feature Engineering

For ML feature management:

1. [FeatureStore](featurestore.md) - Manage features for ML models (DuckDB and Spark)

## Running Examples

Most examples can be run in a Python environment with Seeknal installed:

```bash
# Activate your virtual environment
source .venv/bin/activate

# CLI workflow
seeknal init --name example_project
seeknal apply

# Interactive REPL
seeknal repl

# Run Python scripts
python examples/my_example.py
```

Or in a Jupyter notebook for interactive exploration.

## Best Practices

!!! tip "Code Patterns"
    - Use `seeknal init` to bootstrap new projects
    - Use the `draft → dry-run → apply` CLI workflow for data pipelines
    - Use `@source`, `@transform`, `@materialize` decorators for Python pipelines
    - Use `FeatureGroupDuckDB` for single-node ML feature engineering
    - Always use `get_or_create()` for idempotent Python API operations

!!! warning "Production Considerations"
    - Never hardcode credentials in your code
    - Use environment variables or `profiles.yml` for connection configuration
    - Use virtual environments (`seeknal plan dev` / `seeknal run --env dev`) for safe testing
    - Test your pipelines with `seeknal dry-run` before production runs
