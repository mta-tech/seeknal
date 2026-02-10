# Transforms

Transforms specify how to modify, clean, or enrich your data in Seeknal.

---

## Overview

Transforms take data from sources (or other transforms) and apply operations to create new datasets.

---

## Transform Types

### SQL Transforms

Use SQL for data transformation:

```yaml
name: clean_orders
kind: transform
sql: |
  SELECT
    order_id,
    customer_id,
    order_date,
    total_amount
  FROM {{ source('raw_orders') }}
  WHERE status = 'completed'
    AND total_amount > 0
```

### Python Transforms

Use Python for complex logic:

```python
from seeknal.workflow.decorators import transform

@transform(
    name="enrich_customers",
    inputs={"raw": "raw_customers"},
    output="enriched_customers"
)
def enrich(df):
    df["segment"] = df["total_purchases"].apply(categorize)
    return df
```

---

## Common Transform Patterns

### Data Cleaning

```yaml
name: clean_data
kind: transform
sql: |
  SELECT
    id,
    COALESCE(name, 'Unknown') as name,
    LOWER(TRIM(email)) as email,
    CAST(amount AS FLOAT) as amount
  FROM {{ source('raw_data') }}
```

### Data Enrichment

```yaml
name: enriched_sales
kind: transform
sql: |
  SELECT
    s.*,
    c.segment,
    c.tier,
    p.category
  FROM {{ source('sales') }} s
  LEFT JOIN {{ source('customers') }} c
    ON s.customer_id = c.customer_id
  LEFT JOIN {{ source('products') }} p
    ON s.product_id = p.product_id
```

### Aggregation

```yaml
name: daily_metrics
kind: transform
sql: |
  SELECT
    DATE(transaction_time) as date,
    COUNT(*) as transactions,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
  FROM {{ source('sales') }}
  GROUP BY DATE(transaction_time)
```

---

## Transform Configuration

### Common Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `name` | string | Unique identifier | Required |
| `kind` | string | Transform type | Required |
| `sql` | string | SQL query | For SQL transforms |
| `depends_on` | list | Upstream dependencies | Auto-detected |
| `description` | string | Human-readable description | Optional |

---

## Best Practices

1. **Use SQL** for simple transformations
2. **Use Python** for complex business logic
3. **Chain transforms** for multi-step processing
4. **Use descriptive names** for clarity
5. **Document transform logic** in descriptions

---

## Related Topics

- [Sources](sources.md) - Input data for transforms
- [Aggregations](aggregations.md) - Advanced aggregations
- [YAML Pipeline Tutorial](../../tutorials/yaml-pipeline-tutorial.md) - Hands-on examples

---

**Next**: Learn about [Aggregations](aggregations.md) or return to [Building Blocks](index.md)
