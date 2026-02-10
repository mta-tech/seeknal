# Metrics

Metrics are business calculations built on top of semantic models, providing consistent KPIs across your organization.

---

## Metric Types

### Simple Metrics

Basic aggregations of a single measure:

```yaml
name: total_revenue
type: simple
expression: SUM(order_amount)
description: Total revenue from all orders
```

**Examples**:
- Total revenue: `SUM(amount)`
- Transaction count: `COUNT(*)`
- Average order value: `AVG(amount)`

### Ratio Metrics

Calculations between two metrics:

```yaml
name: conversion_rate
type: ratio
numerator: COUNT(DISTINCT customer_id)
denominator: COUNT(DISTINCT session_id)
description: Percentage of sessions that convert to customers
```

**Examples**:
- Conversion rate: `customers / sessions`
- Profit margin: `profit / revenue`
- Customer retention: `returning_customers / total_customers`

### Cumulative Metrics

Running totals over time:

```yaml
name: ytd_revenue
type: cumulative
expression: SUM(revenue)
window: year_to_date
description: Year-to-date revenue
```

**Examples**:
- YTD revenue: Year-to-date sum
- Rolling 7-day: 7-day rolling total
- Month-over-month: Monthly cumulative

### Derived Metrics

Complex calculations from other metrics:

```yaml
name: customer_ltv
type: derived
expression: avg_annual_revenue * avg_customer_lifespan
description: Predicted customer lifetime value
```

**Examples**:
- Customer LTV: Revenue × Lifespan
- NPS score: Complex survey calculation
- Churn risk: Multiple factor score

---

## Defining Metrics

### YAML Definition

```yaml
name: ecommerce_metrics
semantic_model: sales

metrics:
  - name: total_revenue
    type: simple
    expression: SUM(amount)
    format: currency

  - name: conversion_rate
    type: ratio
    numerator: COUNT(DISTINCT customer_id)
    denominator: COUNT(DISTINCT session_id)
    format: percentage

  - name: ytd_revenue
    type: cumulative
    expression: SUM(amount)
    window: year_to_date
```

### Python Definition

```python
from seeknal.workflow.semantic.models import Metric

# Simple metric
total_revenue = Metric(
    name="total_revenue",
    expression="SUM(amount)",
    type="simple"
)

# Ratio metric
conversion_rate = Metric(
    name="conversion_rate",
    numerator="COUNT(DISTINCT customer_id)",
    denominator="COUNT(DISTINCT session_id)",
    type="ratio"
)
```

---

## Metric Formatting

### Format Types

```yaml
metrics:
  - name: total_revenue
    format: currency  # $1,234.56

  - name: conversion_rate
    format: percentage  # 45.2%

  - name: order_count
    format: number  # 1,234
```

### Custom Formats

```yaml
metrics:
  - name: response_time
    format: "{:.2f} seconds"  # 1.23 seconds
```

---

## Querying Metrics

### CLI Query

```bash
seeknal query ecommerce_metrics \
  --metrics total_revenue,conversion_rate \
  --dimensions region \
  --time_range "2024-01-01 to 2024-12-31"
```

### Python Query

```python
from seeknal.cli.query import query_metrics

df = query_metrics(
    model="ecommerce_metrics",
    metrics=["total_revenue", "conversion_rate"],
    dimensions=["region"],
    time_range=("2024-01-01", "2024-12-31")
)
```

---

## Best Practices

1. **Use simple metrics** as building blocks
2. **Name metrics** with business terminology
3. **Document formulas** clearly
4. **Test calculations** with known data
5. **Version metrics** for production changes

---

## Related Topics

- [Semantic Models](semantic-models.md) - Foundation for metrics
- [Deployment](deployment.md) - Deploying metrics to production
- [Querying](querying.md) - Query patterns

---

**Next**: Learn about [Deployment](deployment.md) or return to [Semantic Layer Overview](index.md)
