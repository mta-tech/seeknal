# YAML vs Python

Seeknal supports two approaches for defining data pipelines: YAML configuration and Python code. Both have equal prominence and can be mixed in the same project.

---

## Overview

| Aspect | YAML | Python |
|--------|------|--------|
| **Style** | Declarative | Programmatic |
| **Learning Curve** | Gentle | Steeper |
| **Flexibility** | Structured | Unlimited |
| **Best For** | Standard pipelines | Complex logic |
| **Version Control** | Clear diffs | Standard diffs |

---

## YAML Approach

### When to Use YAML

- Standard data transformations
- Declarative pipeline definitions
- Team collaboration with reviews
- Configuration-driven workflows

### YAML Example

```yaml
name: clean_sales
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

### Benefits

- **Readable**: Easy to understand at a glance
- **Validatable**: Schema validation catches errors early
- **Reviewable**: Clear diffs in pull requests
- **Documented**: Self-documenting structure

---

## Python Approach

### When to Use Python

- Complex business logic
- Dynamic pipeline generation
- Custom transformations
- Integration with Python ecosystems

### Python Example

```python
from seeknal.workflow.decorators import transform

@transform(
    name="clean_sales",
    inputs={"raw": "raw_orders"},
    output="clean_sales"
)
def clean_orders(raw):
    df = raw[raw["status"] == "completed"]
    df = df[df["total_amount"] > 0]
    return df[["order_id", "customer_id", "order_date", "total_amount"]]
```

### Benefits

- **Flexible**: Any Python logic
- **Testable**: Standard Python testing
- **IDE Support**: Autocomplete, debugging
- **Ecosystem**: Use any Python library

---

## Mixing YAML and Python

You can use both approaches in the same project:

```yaml
# YAML for simple transformations
name: raw_data
kind: source
source: csv
table: data/sales.csv
```

```python
# Python for complex logic
@transform(name="complex_calculation", ...)
def complex_transform(df):
    # Complex Python logic here
    return result
```

---

## Choosing Your Approach

### Start with YAML if:
- You're new to Seeknal
- You have standard ETL patterns
- Your team prefers configuration over code

### Use Python if:
- You need complex business logic
- You're comfortable with Python
- You want to integrate with Python libraries

---

## Migration Path

You can always switch between approaches:

- **YAML → Python**: Rewrite YAML as Python decorators
- **Python → YAML**: Extract patterns to YAML templates

Both approaches generate the same underlying pipeline representation.

---

## Related Topics

- [Pipeline Builder Workflow](pipeline-builder.md) - Core workflow
- [Building Blocks: Transforms](../building-blocks/transforms.md) - Transform options
- [Python Pipelines Guide](../guides/python-pipelines.md) - Deep dive on Python

---

**Next**: Learn about [Virtual Environments](virtual-environments.md) or return to [Concepts Overview](index.md)
