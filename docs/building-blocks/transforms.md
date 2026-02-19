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
inputs:
  - ref: source.raw_orders
transform: |
  SELECT
    order_id,
    customer_id,
    order_date,
    total_amount
  FROM input_0
  WHERE status = 'completed'
    AND total_amount > 0
```

> **Input references:** You can reference inputs by name using `ref('source.raw_orders')` or by position using `input_0`, `input_1`, etc. Both styles work and can be mixed.

### Named Input References

Use `ref()` to reference inputs by name instead of positional index:

```yaml
name: enriched_sales
kind: transform
inputs:
  - ref: source.sales
  - ref: source.products
transform: |
  SELECT
    s.*,
    p.category,
    p.brand
  FROM ref('source.sales') s
  JOIN ref('source.products') p
    ON s.product_id = p.product_id
```

Named refs are resolved to positional `input_N` identifiers at execution time, based on the order of inputs. You can also mix named and positional syntax:

```yaml
transform: |
  SELECT s.*, p.category
  FROM ref('source.sales') s
  JOIN input_1 p ON s.product_id = p.product_id
```

Both single quotes (`ref('...')`) and double quotes (`ref("...")`) are supported.

### Common Config Expressions

Use `{{ }}` expressions to reference reusable definitions from `seeknal/common/`:

```yaml
name: voice_revenue
kind: transform
inputs:
  - ref: source.traffic
transform: |
  SELECT
    date_id,
    msisdn,
    SUM(CASE WHEN {{ rules.callExpression }} THEN revenue ELSE 0 END) AS voice_revenue,
    SUM(CASE WHEN {{ rules.smsExpression }} THEN revenue ELSE 0 END) AS sms_revenue
  FROM ref('source.traffic')
  WHERE {{ rules.activeSubscriber }}
  GROUP BY date_id, msisdn
```

The expressions are resolved at build time from `seeknal/common/rules.yml`:

```yaml
# seeknal/common/rules.yml
rules:
  - id: callExpression
    value: "service_type = 'Voice'"
  - id: smsExpression
    value: "service_type = 'SMS'"
  - id: activeSubscriber
    value: "status = 'Active'"
```

See [Common Config](common-config.md) for the full reference on sources, rules, and transformations config files.

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
inputs:
  - ref: source.raw_data
transform: |
  SELECT
    id,
    COALESCE(name, 'Unknown') as name,
    LOWER(TRIM(email)) as email,
    CAST(amount AS FLOAT) as amount
  FROM input_0
```

### Data Enrichment (Multi-Input Join)

```yaml
name: enriched_sales
kind: transform
inputs:
  - ref: source.sales          # input_0
  - ref: source.customers      # input_1
  - ref: source.products       # input_2
transform: |
  SELECT
    s.*,
    c.segment,
    c.tier,
    p.category
  FROM input_0 s
  LEFT JOIN input_1 c
    ON s.customer_id = c.customer_id
  LEFT JOIN input_2 p
    ON s.product_id = p.product_id
```

### Aggregation

```yaml
name: daily_metrics
kind: transform
inputs:
  - ref: source.sales
transform: |
  SELECT
    DATE(transaction_time) as date,
    COUNT(*) as transactions,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
  FROM input_0
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

## Iceberg Materialization

Persist transform results as Iceberg tables:

```yaml
name: order_enriched
kind: transform
inputs:
  - ref: source.orders
  - ref: source.customers
transform: |
  SELECT o.order_id, o.amount, c.name, c.region
  FROM input_0 o
  JOIN input_1 c ON o.customer_id = c.customer_id
materialization:
  enabled: true
  mode: overwrite
  table: atlas.production.order_enriched
```

See [Iceberg Materialization](../iceberg-materialization.md) for full setup guide.

---

## Best Practices

1. **Use SQL** for simple transformations
2. **Use Python** for complex business logic
3. **Chain transforms** for multi-step processing
4. **Use descriptive names** for clarity
5. **Document transform logic** in descriptions
6. **Use `ref('source.name')`** for readable multi-input transforms (or `input_0`, `input_1` for brevity)

---

## Related Topics

- [Sources](sources.md) - Input data for transforms
- [Aggregations](aggregations.md) - Advanced aggregations
- [YAML Pipeline Tutorial](../../tutorials/yaml-pipeline-tutorial.md) - Hands-on examples
- [Iceberg Materialization](../iceberg-materialization.md) - Persist data to Iceberg tables

---

**Next**: Learn about [Aggregations](aggregations.md) or return to [Building Blocks](index.md)
