# Aggregations

Aggregations compute metrics and rollups across entities and time periods.

---

## Overview

Seeknal supports first-order aggregations (standard GROUP BY) and second-order aggregations (hierarchical rollups).

---

## First-Order Aggregations

### Basic Aggregations

```yaml
name: customer_metrics
kind: aggregation
entity: customer
time_grain: day
metrics:
  - name: total_revenue
    expression: SUM(amount)
  - name: transaction_count
    expression: COUNT(*)
  - name: avg_transaction_value
    expression: AVG(amount)
```

### Window Functions

```yaml
name: rolling_metrics
kind: aggregation
entity: customer
window: 7d
metrics:
  - name: rolling_7day_revenue
    expression: SUM(amount)
  - name: rolling_7day_transactions
    expression: COUNT(*)
```

---

## Second-Order Aggregations

### Multi-Level Rollups

```yaml
name: regional_customer_metrics
kind: second_order_aggregation
base_entity: customer
aggregate_entity: region
metrics:
  - name: regional_total_revenue
    expression: SUM(customer.total_revenue)
  - name: regional_avg_customer_revenue
    expression: AVG(customer.total_revenue)
```

### Conditional Aggregations

```yaml
name: customer_segment_metrics
kind: second_order_aggregation
base_entity: customer
aggregate_entity: segment
condition: "signup_date >= '2024-01-01'"
metrics:
  - name: new_customer_revenue
    expression: SUM(total_revenue)
```

---

## Python Aggregations

Define aggregations using Python:

```python
from seeknal.workflow.decorators import aggregation

@aggregation(
    name="python_aggregation",
    entity="customer",
    output="customer_features"
)
def compute_features(df):
    return df.groupby("customer_id").agg({
        "amount": ["sum", "count", "mean"]
    })
```

---

## Aggregation Configuration

### Common Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `name` | string | Unique identifier | Required |
| `entity` | string | Entity to aggregate over | Required |
| `time_grain` | string | Time granularity (day, week, month) | Optional |
| `metrics` | list | Metric definitions | Required |
| `window` | string | Rolling window size | Optional |

---

## Best Practices

1. **Define clear entities** for aggregation
2. **Use time grains** for temporal aggregations
3. **Document metric logic** in descriptions
4. **Test with sample data** first
5. **Consider performance** for large datasets

---

## Related Topics

- [Second-Order Aggregations](../../concepts/second-order-aggregations.md) - Conceptual overview
- [Feature Groups](feature-groups.md) - Aggregations for ML features
- [Transforms](transforms.md) - Data preparation

---

**Next**: Learn about [Feature Groups](feature-groups.md) or return to [Building Blocks](index.md)
