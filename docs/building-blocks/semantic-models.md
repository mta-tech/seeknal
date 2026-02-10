# Semantic Models

Semantic models define metrics for analytics and business intelligence.

---

## Overview

Semantic models provide a unified, business-friendly view of your data. They define entities, dimensions, and metrics that can be queried consistently across tools.

---

## Semantic Model Concepts

### Entities

Entities represent the core objects in your data (customers, products, orders):

```yaml
name: sales_semantic_model
entities:
  - name: customer
    type: primary
    join_keys: [customer_id]
  - name: product
    type: foreign
    join_keys: [product_id]
```

### Metrics

Metrics define aggregations and calculations:

```yaml
metrics:
  - name: total_revenue
    expression: SUM(amount)
    description: Total revenue from all transactions

  - name: average_order_value
    expression: AVG(amount)
    description: Average transaction amount

  - name: transaction_count
    expression: COUNT(*)
    description: Number of transactions
```

### Dimensions

Dimensions provide context for filtering and grouping:

```yaml
dimensions:
  - name: region
    type: string
    description: Geographic region

  - name: product_category
    type: string
    description: Product category

  - name: transaction_date
    type: time
    description: Date of transaction
```

---

## Defining Semantic Models

### YAML Definition

```yaml
name: ecommerce_metrics
model: |
  SELECT
    customer_id,
    product_id,
    region,
    category,
    transaction_date,
    amount
  FROM {{ source('sales') }}

entities:
  - name: customer
    type: primary
    join_keys: [customer_id]

metrics:
  - name: total_revenue
    expression: SUM(amount)

dimensions:
  - name: region
    type: string
```

### Python Definition

```python
from seeknal.workflow.semantic.models import SemanticModel

model = SemanticModel(
    name="ecommerce_metrics",
    query="SELECT * FROM sales"
)

model.add_entity("customer", type="primary", keys=["customer_id"])
model.add_metric("total_revenue", "SUM(amount)")
```

---

## Querying Semantic Models

### CLI Query

```bash
seeknal query ecommerce_metrics \
  --metrics total_revenue \
  --dimensions region \
  --filters "region='North America'"
```

### Python Query

```python
from seeknal.cli.query import query_metrics

df = query_metrics(
    model="ecommerce_metrics",
    metrics=["total_revenue", "transaction_count"],
    dimensions=["region"],
    filters={"region": "North America"}
)
```

---

## Deploying Metrics

Deploy semantic models as materialized views in StarRocks:

```bash
seeknal deploy-metrics ecommerce_metrics
```

This creates optimized views that BI tools can query directly.

---

## Semantic Model Configuration

### Common Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `name` | string | Unique identifier | Required |
| `model` | string | SQL query | Required |
| `entities` | list | Entity definitions | Required |
| `metrics` | list | Metric definitions | Required |
| `dimensions` | list | Dimension definitions | Optional |

---

## Best Practices

1. **Use semantic models** for all business metrics
2. **Define clear entities** and relationships
3. **Document metrics** with descriptions
4. **Deploy to StarRocks** for BI tool integration
5. **Version models** for production management

---

## Related Topics

- [Semantic Layer Guide](../../guides/semantic-layer.md) - Complete semantic layer documentation
- [Metrics](semantic-layer/metrics.md) - Advanced metric types
- [Analytics Engineer Path](../../getting-started/analytics-engineer-path/) - Hands-on tutorial

---

**Next**: Learn about [Tasks](tasks.md) or return to [Building Blocks](index.md)
