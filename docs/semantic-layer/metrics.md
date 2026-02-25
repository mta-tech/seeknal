# Metrics

Metrics are business calculations built on top of semantic models, providing consistent KPIs across your organization. Each metric is defined as an individual YAML document with `kind: metric`.

---

## Metric Types

Seeknal supports four metric types. Each type uses different fields from the `Metric` dataclass:

| Type | Required Fields | Description |
|------|----------------|-------------|
| `simple` | `measure` | Direct aggregation of a single measure |
| `ratio` | `numerator`, `denominator` | Division of two measures (with NULLIF protection) |
| `cumulative` | `measure`, `grain_to_date` | Running totals within time boundaries |
| `derived` | `expr`, `inputs` | Combine other metrics with custom expressions |

### Simple Metrics

Reference a measure defined in a semantic model:

```yaml
kind: metric
name: total_revenue
type: simple
measure: total_revenue
description: "Sum of all order amounts"
filter: "order_status = 'completed'"
```

The `measure` field references a **measure name** from a semantic model (not a raw SQL expression).

### Ratio Metrics

Divide one measure by another:

```yaml
kind: metric
name: conversion_rate
type: ratio
numerator: order_count
denominator: visitor_count
description: "Orders divided by visitors"
```

The compiler generates `numerator / NULLIF(denominator, 0)` to prevent division by zero. Both `numerator` and `denominator` are **measure names** referencing measures from semantic models.

### Cumulative Metrics

Running totals or grain-to-date aggregations:

```yaml
kind: metric
name: month_to_date_revenue
type: cumulative
measure: total_revenue
grain_to_date: month
description: "Revenue accumulated within each month"
```

Valid `grain_to_date` values: `month`, `quarter`, `year`.

Generates SQL with window functions:
```sql
SUM(amount) OVER (
  PARTITION BY date_trunc('month', order_date)
  ORDER BY order_date
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

### Derived Metrics

Combine other metrics with custom expressions:

```yaml
kind: metric
name: revenue_per_customer
type: derived
expr: "total_revenue / customer_count"
inputs:
  - metric: total_revenue
    alias: total_revenue
  - metric: customer_count
    alias: customer_count
description: "Average revenue per customer"
```

The `inputs` list references other metrics by name. Each input has an `alias` that is used in the `expr` field. The compiler substitutes aliases with the compiled SQL of the referenced metrics.

---

## Defining Metrics

### YAML Definition

Each metric is a separate YAML document with `kind: metric`:

```yaml
# seeknal/semantic/metrics/revenue.yml
kind: metric
name: total_revenue
type: simple
measure: total_revenue
description: "Sum of all order amounts"
filter: "order_status = 'completed'"
```

```yaml
# seeknal/semantic/metrics/conversion.yml
kind: metric
name: conversion_rate
type: ratio
numerator: order_count
denominator: visitor_count
description: "Orders divided by visitors"
```

```yaml
# seeknal/semantic/metrics/mtd_revenue.yml
kind: metric
name: mtd_revenue
type: cumulative
measure: total_revenue
grain_to_date: month
description: "Month-to-date revenue"
```

```yaml
# seeknal/semantic/metrics/revenue_per_customer.yml
kind: metric
name: revenue_per_customer
type: derived
expr: "total_revenue / customer_count"
inputs:
  - metric: total_revenue
    alias: total_revenue
  - metric: customer_count
    alias: customer_count
description: "Average revenue per customer"
```

### Python Definition

```python
from seeknal.workflow.semantic.models import (
    Metric, MetricType, MetricInput,
)

# Simple metric
total_revenue = Metric(
    name="total_revenue",
    type=MetricType.SIMPLE,
    measure="total_revenue",
    description="Sum of all order amounts",
    filter="order_status = 'completed'",
)

# Ratio metric
conversion_rate = Metric(
    name="conversion_rate",
    type=MetricType.RATIO,
    numerator="order_count",
    denominator="visitor_count",
    description="Orders divided by visitors",
)

# Cumulative metric
mtd_revenue = Metric(
    name="mtd_revenue",
    type=MetricType.CUMULATIVE,
    measure="total_revenue",
    grain_to_date="month",
    description="Month-to-date revenue",
)

# Derived metric
revenue_per_customer = Metric(
    name="revenue_per_customer",
    type=MetricType.DERIVED,
    expr="total_revenue / customer_count",
    inputs=[
        MetricInput(metric="total_revenue", alias="total_revenue"),
        MetricInput(metric="customer_count", alias="customer_count"),
    ],
    description="Average revenue per customer",
)
```

---

## Metric Validation

Metrics are validated automatically. Each type requires specific fields:

```python
errors = total_revenue.validate()
if errors:
    for error in errors:
        print(f"Validation error: {error}")
```

Common validation errors:
- Simple metric missing `measure`
- Ratio metric missing `numerator` or `denominator`
- Cumulative metric missing `measure`
- Derived metric missing `expr` or `inputs`

---

## Querying Metrics

### CLI Query

Use `seeknal query` to query metrics from the semantic layer:

```bash
# Basic query
seeknal query \
  --metrics total_revenue,order_count \
  --dimensions region \
  --limit 10

# With time grain (double underscore syntax)
seeknal query \
  --metrics total_revenue,order_count \
  --dimensions region,order_date__month \
  --order-by order_date__month \
  --limit 20

# With filter
seeknal query \
  --metrics total_revenue \
  --dimensions region \
  --filter "order_date > '2024-01-01'" \
  --order-by -total_revenue

# Compile-only mode (view SQL without executing)
seeknal query \
  --metrics total_revenue \
  --dimensions order_date__month \
  --compile

# Output formats
seeknal query --metrics total_revenue --dimensions region --format table
seeknal query --metrics total_revenue --dimensions region --format json
seeknal query --metrics total_revenue --dimensions region --format csv > output.csv
```

**CLI Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--metrics` | Yes | Comma-separated metric names |
| `--dimensions` | No | Comma-separated dimensions (use `dim__grain` for time grains) |
| `--filter` | No | SQL filter expression |
| `--order-by` | No | Order by columns (prefix `-` for DESC) |
| `--limit` | No | Maximum rows to return (default: 100) |
| `--compile` | No | Show generated SQL without executing |
| `--format` | No | Output format: `table`, `json`, `csv` (default: `table`) |
| `--project-path` | No | Path to project directory (default: `.`) |

---

## Metric Dependencies

Derived metrics create dependency chains. Seeknal automatically:

1. **Detects cycles** - Circular dependencies are caught during validation
2. **Resolves order** - Metrics are compiled in topological order (dependencies first)

```python
from seeknal.workflow.semantic.models import MetricRegistry

registry = MetricRegistry()
registry.add(total_revenue)
registry.add(customer_count)
registry.add(revenue_per_customer)

# Check for circular dependencies
has_cycle, cycle_path = registry.detect_cycles()

# Get compilation order
order = registry.resolve_order()
# Returns: ['total_revenue', 'customer_count', 'revenue_per_customer']
```

---

## Best Practices

1. **Use measure references** - Simple and ratio metrics reference measure names, not raw SQL
2. **Name metrics** with business terminology that stakeholders understand
3. **Use aliases** in derived metrics that match the metric names for clarity
4. **Document descriptions** - Every metric should have a clear `description` field
5. **Test calculations** - Use `--compile` to verify generated SQL before executing
6. **Handle zero division** - Ratio metrics automatically use `NULLIF` protection

---

## Related Topics

- [Semantic Models](semantic-models.md) - Foundation for metrics (defines measures and dimensions)
- [Deployment](deployment.md) - Deploying metrics as materialized views
- [Querying](querying.md) - Advanced query patterns

---

**Next**: Learn about [Deployment](deployment.md) or return to [Semantic Layer Overview](index.md)
