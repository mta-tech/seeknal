# Semantic Layer & Metrics

Seeknal's semantic layer lets you define reusable metrics on top of your pipeline outputs and query them with a simple CLI command. It provides a dbt MetricFlow-inspired approach to defining business logic once and querying it anywhere.

## Overview

The semantic layer consists of two components:

- **Semantic Models** — Define the data structure (tables, columns, entities, dimensions, measures)
- **Metrics** — Define calculations on top of semantic models (simple aggregations, ratios, cumulative metrics, derived metrics)

### Why Use the Semantic Layer?

| Benefit | Description |
|---------|-------------|
| **Single Source of Truth** | Define business logic once, use everywhere |
| **SQL Generation** | Automatically generates optimized SQL from high-level definitions |
| **Type Safety** | Catch errors at compile time, not runtime |
| **BI Integration** | Deploy metrics as StarRocks materialized views for dashboards |
| **Version Control** | Track metric definitions alongside code |

## Directory Structure

Organize your semantic layer in your Seeknal project:

```
seeknal/
├── semantic_models/
│   ├── orders_model.yml
│   ├── customers_model.yml
│   └── products_model.yml
└── metrics/
    ├── revenue_metrics.yml
    ├── customer_metrics.yml
    └── product_metrics.yml
```

## Defining a Semantic Model

A semantic model maps a Seeknal pipeline output (node reference) to business concepts.

### Basic Structure

```yaml
kind: semantic_model
name: orders
model: ref('transform.orders')  # Reference to your pipeline node
description: "Order transactions with customer and product details"

entities:
  - name: order
    type: primary
  - name: customer
    type: foreign
  - name: product
    type: foreign

dimensions:
  - name: order_date
    type: time
    expr: order_date
    time_granularity: day
  - name: region
    type: categorical
    expr: customer_region
  - name: status
    type: categorical
    expr: order_status

measures:
  - name: total_revenue
    agg: sum
    expr: amount
    description: "Total order amount"
  - name: order_count
    agg: count
    expr: order_id
  - name: avg_order_value
    agg: average
    expr: amount
```

### Entity Types

Entities define join keys for connecting semantic models:

| Type | Description | Example |
|------|-------------|---------|
| `primary` | Primary key of this model | `order_id` in orders model |
| `foreign` | Foreign key to another model | `customer_id` in orders model |
| `unique` | Unique but not primary | `email` in customers model |

### Dimension Types

| Type | Description | Example |
|------|-------------|---------|
| `time` | Time-based dimension for aggregation | `order_date`, `created_at` |
| `categorical` | Categorical attribute | `region`, `status`, `category` |

**Time Granularity** (for time dimensions):
- `day` - Daily aggregation
- `week` - Weekly aggregation
- `month` - Monthly aggregation
- `quarter` - Quarterly aggregation
- `year` - Yearly aggregation

### Measure Aggregation Types

| Aggregation | SQL Function | Use Case |
|-------------|--------------|----------|
| `sum` | `SUM()` | Total revenue, quantity |
| `count` | `COUNT()` | Number of orders |
| `count_distinct` | `COUNT(DISTINCT)` | Unique customers |
| `average` / `avg` | `AVG()` | Average order value |
| `min` | `MIN()` | Minimum price |
| `max` | `MAX()` | Maximum quantity |

## Defining Metrics

Metrics build on semantic models to define business calculations. Seeknal supports four metric types.

### 1. Simple Metrics

Direct aggregation of a measure:

```yaml
kind: metric
name: total_revenue
type: simple
measure: total_revenue
description: "Sum of all order amounts"
filter: "order_status = 'completed'"  # Optional filter
```

### 2. Ratio Metrics

Divide one measure by another:

```yaml
kind: metric
name: conversion_rate
type: ratio
numerator: order_count
denominator: visitor_count
description: "Orders divided by visitors"
```

The compiler generates `numerator / NULLIF(denominator, 0)` to prevent division by zero.

### 3. Cumulative Metrics

Running totals or grain-to-date aggregations:

```yaml
kind: metric
name: month_to_date_revenue
type: cumulative
measure: total_revenue
grain_to_date: month  # Reset at month boundaries
description: "Revenue accumulated within each month"
```

Generates SQL with window functions:
```sql
SUM(amount) OVER (
  PARTITION BY date_trunc('month', order_date)
  ORDER BY order_date
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
```

### 4. Derived Metrics

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

The compiler substitutes input metrics with their compiled SQL expressions.

## Querying Metrics

Use the `seeknal query` command to query metrics with SQL-like syntax.

### Basic Query

```bash
seeknal query \
  --metrics revenue,order_count \
  --dimensions order_date \
  --limit 10
```

### With Dimensions and Time Grains

Use double underscore syntax for time grain: `dimension__grain`

```bash
seeknal query \
  --metrics total_revenue,order_count \
  --dimensions region,order_date__month \
  --order-by order_date__month \
  --limit 20
```

**Generates SQL:**
```sql
SELECT
  region AS region,
  date_trunc('month', order_date) AS order_date_month,
  SUM(amount) AS total_revenue,
  COUNT(order_id) AS order_count
FROM transform.orders
GROUP BY region, date_trunc('month', order_date)
ORDER BY order_date_month ASC
LIMIT 20
```

### With Filters

```bash
seeknal query \
  --metrics total_revenue \
  --dimensions region \
  --filter "order_date > '2024-01-01' AND region IN ('US', 'EU')" \
  --order-by -total_revenue  # '-' prefix for DESC
```

### Output Formats

```bash
# Table format (default, requires tabulate)
seeknal query --metrics revenue --dimensions region --format table

# JSON format
seeknal query --metrics revenue --dimensions region --format json

# CSV format
seeknal query --metrics revenue --dimensions region --format csv > output.csv
```

### Compile-Only Mode

View generated SQL without executing:

```bash
seeknal query \
  --metrics total_revenue \
  --dimensions order_date__month \
  --compile
```

## Deploying to StarRocks

Deploy metrics as materialized views for BI layer pre-aggregation.

### Configure StarRocks Connection

```bash
# Set environment variables
export STARROCKS_HOST=localhost
export STARROCKS_PORT=9030
export STARROCKS_USER=root
export STARROCKS_PASSWORD=your_password
export STARROCKS_DATABASE=analytics
```

### Deploy a Metric

```bash
seeknal deploy-metrics \
  --connection "starrocks://root@localhost:9030/analytics" \
  --dimensions region,order_date \
  --refresh-interval "1 HOUR" \
  --dry-run
```

**Generated DDL:**
```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_total_revenue
REFRESH ASYNC EVERY(INTERVAL 1 HOUR)
AS
SELECT
  region AS region,
  order_date AS order_date,
  SUM(amount) AS total_revenue
FROM transform.orders
GROUP BY region, order_date
```

### Deploy with Options

```bash
seeknal deploy-metrics \
  --connection "starrocks://root@localhost:9030/analytics" \
  --dimensions region,order_date__month \
  --refresh-interval "1 DAY" \
  --drop-existing  # Drop and recreate existing MVs
```

### Python API for Deployment

```python
from seeknal.workflow.semantic.models import SemanticModel, Metric
from seeknal.workflow.semantic.compiler import MetricCompiler
from seeknal.workflow.semantic.deploy import MetricDeployer, DeployConfig

# Load semantic models and metrics
semantic_models = [...]  # Load from YAML
metrics = [...]  # Load from YAML

# Create compiler
compiler = MetricCompiler(semantic_models, metrics)

# Configure deployment
config = DeployConfig(
    refresh_interval="1 HOUR",
    drop_existing=True,
    properties={
        "replication_num": "3",
        "storage_medium": "SSD"
    }
)

# Deploy metrics
deployer = MetricDeployer(
    compiler=compiler,
    connection_config={
        "host": "localhost",
        "port": 9030,
        "user": "root",
        "database": "analytics"
    }
)

results = deployer.deploy(
    metrics=[metric1, metric2],
    config=config,
    dimensions=["region", "order_date"],
    dry_run=False
)

for result in results:
    if result.success:
        print(f"✓ Deployed {result.metric_name} as {result.mv_name}")
    else:
        print(f"✗ Failed {result.metric_name}: {result.error}")
```

## Advanced Features

### Multi-Model Joins

The semantic layer automatically joins models via shared entities:

```yaml
# customers_model.yml
kind: semantic_model
name: customers
model: ref('source.customers')
entities:
  - name: customer
    type: primary

dimensions:
  - name: signup_date
    type: time
    expr: created_at
  - name: tier
    type: categorical
    expr: customer_tier

# orders_model.yml
kind: semantic_model
name: orders
model: ref('transform.orders')
entities:
  - name: order
    type: primary
  - name: customer
    type: foreign  # Links to customers model

measures:
  - name: order_count
    agg: count
    expr: order_id
```

Query across models:

```bash
seeknal query \
  --metrics order_count \
  --dimensions tier  # From customers model
```

**Generated SQL:**
```sql
SELECT
  customers.customer_tier AS tier,
  COUNT(orders.order_id) AS order_count
FROM transform.orders
JOIN source.customers ON orders.customer_id = customers.customer_id
GROUP BY customers.customer_tier
```

### Metric Dependencies

Derived metrics can reference other derived metrics:

```yaml
kind: metric
name: revenue_growth
type: derived
expr: "(current_revenue - previous_revenue) / previous_revenue"
inputs:
  - metric: current_revenue
  - metric: previous_revenue
```

The compiler performs topological sort to resolve dependencies.

### Validation

Semantic layer definitions are validated at query time:

- **Missing measures:** Error if metric references undefined measure
- **Circular dependencies:** Error if derived metrics form a cycle
- **Invalid filters:** Error if filter contains SQL injection patterns
- **Type mismatches:** Error if time grain used on non-time dimension

## Best Practices

### 1. Organize by Domain

Group related semantic models and metrics:

```
seeknal/
├── semantic_models/
│   ├── sales/
│   │   ├── orders.yml
│   │   └── invoices.yml
│   └── marketing/
│       ├── campaigns.yml
│       └── conversions.yml
└── metrics/
    ├── sales/
    │   └── revenue_metrics.yml
    └── marketing/
        └── conversion_metrics.yml
```

### 2. Use Descriptive Names

```yaml
# Good
name: monthly_recurring_revenue
description: "MRR from subscription customers"

# Bad
name: mrr
description: "MRR"
```

### 3. Document Business Logic

```yaml
kind: metric
name: churn_rate
type: ratio
numerator: churned_customers
denominator: active_customers
description: |
  Percentage of customers who canceled in the current period.
  Calculated as churned customers / total active customers.
  Used for executive dashboards and forecasting.
```

### 4. Version Control Everything

```bash
git add seeknal/semantic_models/ seeknal/metrics/
git commit -m "Add revenue metrics for Q1 reporting"
```

### 5. Test Metric Definitions

```bash
# Compile-only check
seeknal query --metrics all_metrics --compile

# Test with sample data
seeknal query --metrics revenue --dimensions region --limit 1
```

## Examples

### E-commerce Metrics

```yaml
# semantic_models/orders.yml
kind: semantic_model
name: orders
model: ref('transform.fact_orders')
entities:
  - name: order
    type: primary
  - name: customer
    type: foreign
dimensions:
  - name: order_date
    type: time
    expr: ordered_at
  - name: status
    type: categorical
measures:
  - name: revenue
    agg: sum
    expr: total_amount
  - name: order_count
    agg: count
    expr: order_id

---
# metrics/revenue_metrics.yml
kind: metric
name: total_revenue
type: simple
measure: revenue

---
kind: metric
name: aov
type: ratio
numerator: revenue
denominator: order_count
description: "Average Order Value"

---
kind: metric
name: daily_revenue
type: simple
measure: revenue
filter: "order_date = CURRENT_DATE()"
```

Query:

```bash
seeknal query \
  --metrics total_revenue,aov \
  --dimensions order_date__month,status \
  --filter "order_date >= '2024-01-01'" \
  --order-by order_date__month
```

## Troubleshooting

### "Unknown metric: X"

**Cause:** Metric not defined or file not loaded.

**Solution:**
1. Check metric exists in `seeknal/metrics/*.yml`
2. Verify `kind: metric` is set
3. Run from project root directory

### "Measure 'X' not found in any semantic model"

**Cause:** Metric references undefined measure.

**Solution:**
1. Check measure is defined in semantic model
2. Verify measure name spelling matches
3. Ensure semantic model file is loaded

### "Circular metric dependency detected"

**Cause:** Derived metrics reference each other in a cycle.

**Solution:**
1. Review metric dependencies
2. Break the cycle by redefining metrics
3. Use `--compile` to see dependency graph

### "Filter contains forbidden SQL pattern"

**Cause:** Filter contains potential SQL injection.

**Solution:**
1. Use parameterized filters
2. Avoid SQL keywords: `DROP`, `UNION`, `--`, etc.
3. Use simple comparison operators: `=`, `>`, `IN`

## See Also

- **Concepts**: [Semantic Model](../concepts/glossary.md#semantic-model), [Metric](../concepts/glossary.md#metric), [Dimension](../concepts/glossary.md#dimension)
- **Reference**: [CLI Query Command](../reference/cli.md#seeknal-query), [YAML Schema Reference](../reference/yaml-schema.md)
- **Guides**: [Comparison to dbt](./comparison.md), [Testing & Audits](./testing-and-audits.md)
- **Tutorials**: [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md), [Mixed YAML + Python](../tutorials/mixed-yaml-python-pipelines.md)
