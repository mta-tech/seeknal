# Sources

Sources define where your data comes from and how to ingest it into Seeknal.

---

## Overview

Sources are the starting point for any Seeknal pipeline. They connect to your data systems and bring data into the Seeknal ecosystem.

---

## Source Types

### File Sources

Read from files in various formats:

**CSV Files**:
```yaml
name: raw_sales
kind: source
source: csv
table: data/sales.csv
```

**Parquet Files**:
```yaml
name: customer_data
kind: source
source: parquet
table: data/customers.parquet
```

**JSON Files**:
```yaml
name: api_data
kind: source
source: json
table: data/api_response.json
```

### Database Sources

Connect to databases for batch or incremental ingestion:

**PostgreSQL**:
```yaml
name: users_db
kind: source
source: postgresql
connection_ref: my_postgres
table: public.users
```

**MySQL**:
```yaml
name: orders_db
kind: source
source: mysql
connection_ref: my_mysql
table: app.orders
```

### API Sources

Ingest data from REST APIs:

```yaml
name: api_ingestion
kind: source
source: http
url: https://api.example.com/data
method: GET
headers:
  Authorization: "Bearer ${API_TOKEN}"
```

---

## Source Configuration

### Common Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `name` | string | Unique identifier for the source | Required |
| `kind` | string | Source type (csv, parquet, postgresql, etc.) | Required |
| `table` | string | Table or file path | Required |
| `description` | string | Human-readable description | Optional |
| `tags` | list | Organizational tags | Optional |

### Column Definition

Explicitly define columns for type safety:

```yaml
name: typed_sales
kind: source
source: csv
table: data/sales.csv
columns:
  transaction_id: int
  date: date
  customer_id: string
  amount: float
```

---

## Python Sources

Define sources using Python decorators:

```python
from seeknal.workflow.decorators import source

@source(
    name="python_source",
    output="raw_data"
)
def get_data():
    import pandas as pd
    return pd.read_csv("data/sales.csv")
```

---

## Incremental Sources

Configure sources for incremental updates:

```yaml
name: incremental_orders
kind: source
source: postgresql
connection_ref: my_db
table: app.orders
incremental: true
incremental_key: updated_at
```

---

## Best Practices

1. **Use explicit column definitions** for type safety
2. **Tag sources** for organization (e.g., `raw`, `staging`, `production`)
3. **Use connection references** instead of hardcoded credentials
4. **Configure incremental sources** for large datasets
5. **Add descriptions** for documentation

---

## Related Topics

- [Transforms](transforms.md) - Process source data
- [Incremental Processing](../getting-started/data-engineer-path/2-incremental-models.md) - Advanced source patterns
- [Connections](../reference/configuration.md) - Configure database connections

---

**Next**: Learn about [Transforms](transforms.md) or return to [Building Blocks](index.md)
