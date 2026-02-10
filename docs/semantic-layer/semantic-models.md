# Semantic Models

Semantic models provide a business-friendly view of your data for analytics and BI tools.

---

## What are Semantic Models?

Semantic models define your data in business terms rather than database tables. They abstract away complex joins and calculations, providing:

- **Entities**: Core business objects (customers, products, orders)
- **Dimensions**: Attributes for filtering and grouping (region, category, date)
- **Measures**: Quantitative values that can be aggregated (revenue, count, amount)

---

## Defining Semantic Models

### YAML Definition

```yaml
name: ecommerce_semantic_model
description: E-commerce sales and customer data

# Define entities
entities:
  - name: customer
    type: primary
    join_keys: [customer_id]
    description: Unique customer identifier

  - name: product
    type: foreign
    join_keys: [product_id]
    description: Product catalog

# Define dimensions (filtering/grouping attributes)
dimensions:
  - name: region
    type: string
    description: Geographic region

  - name: category
    type: string
    description: Product category

  - name: order_date
    type: time
    description: Date of order

# Define measures (aggregatable values)
measures:
  - name: total_revenue
    expression: SUM(amount)
    description: Total revenue from orders

  - name: order_count
    expression: COUNT(*)
    description: Number of orders

  - name: avg_order_value
    expression: AVG(amount)
    description: Average order value
```

### Python Definition

```python
from seeknal.workflow.semantic.models import SemanticModel

model = SemanticModel(
    name="ecommerce_semantic_model",
    query="SELECT * FROM orders"
)

# Add entities
model.add_entity("customer", type="primary", keys=["customer_id"])
model.add_entity("product", type="foreign", keys=["product_id"])

# Add measures
model.add_measure("total_revenue", "SUM(amount)")
model.add_measure("order_count", "COUNT(*)")

# Add dimensions
model.add_dimension("region", type="string")
model.add_dimension("order_date", type="time")
```

---

## Entity Types

### Primary Entities

The main entity being analyzed (e.g., customer, order).

```yaml
entities:
  - name: customer
    type: primary
    join_keys: [customer_id]
```

### Foreign Entities

Related entities that provide additional context (e.g., product, region).

```yaml
entities:
  - name: product
    type: foreign
    join_keys: [product_id]
```

---

## Dimension Types

### Categorical Dimensions

String values for filtering and grouping:

```yaml
dimensions:
  - name: region
    type: string
    values: [North America, Europe, Asia]
```

### Time Dimensions

Dates and times for temporal analysis:

```yaml
dimensions:
  - name: order_date
    type: time
    granularity: [day, week, month]
```

### Geographic Dimensions

Location-based filtering:

```yaml
dimensions:
  - name: country
    type: geographic
    hierarchy: [country, region, city]
```

---

## Querying Semantic Models

### CLI Query

```bash
seeknal query ecommerce_semantic_model \
  --measures total_revenue,order_count \
  --dimensions region \
  --filters "region='North America'"
```

### Python Query

```python
from seeknal.cli.query import query_metrics

df = query_metrics(
    model="ecommerce_semantic_model",
    measures=["total_revenue", "order_count"],
    dimensions=["region"],
    filters={"region": "North America"}
)
```

---

## Best Practices

1. **Use business names** for entities and measures
2. **Document measures** with clear descriptions
3. **Define dimension types** for proper filtering
4. **Test queries** before deploying
5. **Version models** for production changes

---

## Related Topics

- [Metrics](metrics.md) - Creating business metrics
- [Deployment](deployment.md) - Deploying to production
- [Querying](querying.md) - Query patterns and examples

---

**Next**: Learn about [Metrics](metrics.md) or return to [Semantic Layer Overview](index.md)
