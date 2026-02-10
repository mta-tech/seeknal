# Tasks

Tasks provide advanced Python processing capabilities in Seeknal pipelines.

---

## Overview

Tasks are Python functions that process data using either DuckDB or Spark engines. They're used when built-in YAML constructs aren't sufficient.

---

## Task Types

### DuckDB Tasks

Use DuckDB for fast, single-node processing:

```python
from seeknal.tasks.duckdb import DuckDBTask

task = DuckDBTask()
task.set_config({
    "query": "SELECT * FROM sales WHERE amount > 100"
})

result = task.execute()
```

### Spark Tasks

Use Spark for distributed processing of large datasets:

```python
from seeknal.tasks.sparkengine import SparkEngineTask

task = SparkEngineTask()
task.set_config({
    "operation": "filter",
    "condition": "amount > 100"
})

result = task.execute(spark_df)
```

---

## Common Task Patterns

### Data Validation

```python
from seeknal.tasks.duckdb import DuckDBTask

def validate_data(df):
    task = DuckDBTask()
    task.set_dataframe(df)

    # Check for nulls
    nulls = task.execute(
        "SELECT COUNT(*) FROM df WHERE column IS NULL"
    )

    # Check data quality
    quality = task.execute("""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT id) as unique_ids
        FROM df
    """)

    return nulls, quality
```

### Custom Transformations

```python
from seeknal.tasks.duckdb.aggregators import SimpleAggregator

aggregator = SimpleAggregator(
    group_by_columns=["customer_id"],
    aggregations={
        "total": "sum(amount)",
        "count": "count(*)"
    }
)

result = aggregator.aggregate(df)
```

### Data Enrichment

```python
from seeknal.tasks.duckdb.transformers import SimpleTransformer

transformer = SimpleTransformer(
    sql="""
        SELECT
            s.*,
            c.segment,
            c.tier
        FROM sales s
        LEFT JOIN customers c
            ON s.customer_id = c.customer_id
    """
)

result = transformer.transform(df)
```

---

## Task Configuration

### Common Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `engine` | string | duckdb or spark | Required |
| `query` | string | SQL query | Optional |
| `dataframe` | DataFrame | Input data | Optional |
| `output` | string | Output table | Optional |

---

## When to Use Tasks

### Use Tasks When:
- You need custom Python logic
- Built-in YAML constructs aren't sufficient
- You need complex data validation
- You require fine-grained control over processing

### Use YAML When:
- You're doing standard transformations
- You want declarative pipelines
- You prefer configuration over code

---

## Best Practices

1. **Use DuckDB tasks** for small-to-medium datasets
2. **Use Spark tasks** for large-scale processing
3. **Test tasks** with sample data first
4. **Document task logic** clearly
5. **Handle errors** gracefully

---

## Related Topics

- [Transforms](transforms.md) - YAML-based transformations
- [Python Pipelines Guide](../../guides/python-pipelines.md) - Complete Python pipeline documentation
- [API Reference](../../api/tasks.md) - Complete task API

---

**Return to**: [Building Blocks](index.md)
