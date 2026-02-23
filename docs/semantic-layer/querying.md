# Querying the Semantic Layer

Query semantic models and metrics using SQL, CLI, or Python APIs.

---

## Query Methods

### CLI Query

```bash
seeknal query ecommerce_semantic_model \
  --measures total_revenue,order_count \
  --dimensions region,category
```

### Python Query

```python
from seeknal.cli.query import query_metrics

df = query_metrics(
    model="ecommerce_semantic_model",
    measures=["total_revenue", "order_count"],
    dimensions=["region", "category"]
)
```

### Direct SQL Query

```sql
SELECT
    region,
    SUM(total_revenue) as total_revenue,
    COUNT(*) as order_count
FROM ecommerce_semantic_model_mv
GROUP BY region;
```

---

## Query Patterns

### Time-Series Analysis

```bash
seeknal query ecommerce_metrics \
  --measures total_revenue \
  --dimensions order_date \
  --time_range "2024-01-01 to 2024-12-31"
```

### Filtering

```bash
seeknal query ecommerce_metrics \
  --measures total_revenue \
  --dimensions region \
  --filters "region='North America'" \
  --filters "category='Electronics'"
```

### Grouping

```bash
seeknal query ecommerce_metrics \
  --measures total_revenue,order_count \
  --dimensions region,category \
  --group-by region,category
```

### Ordering

```bash
seeknal query ecommerce_metrics \
  --measures total_revenue \
  --dimensions region \
  --order-by total_revenue DESC
```

---

## Advanced Queries

### Subqueries

```sql
SELECT
    region,
    total_revenue,
    percentile
FROM (
    SELECT
        region,
        total_revenue,
        NTILE(4) OVER (ORDER BY total_revenue ASC) as percentile
    FROM regional_metrics
) subquery
WHERE percentile = 4;  -- Top quartile
```

### Window Functions

```sql
SELECT
    order_date,
    total_revenue,
    SUM(total_revenue) OVER (
        ORDER BY order_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_revenue
FROM daily_metrics;
```

### Joins

```sql
SELECT
    r.region,
    r.total_revenue,
    c.customer_count
FROM regional_metrics r
JOIN customer_counts c
  ON r.region = c.region;
```

---

## BI Tool Integration

### Tableau

```
Connection: StarRocks ODBC
Server: your-starrocks-host
Port: 9030
Database: analytics_prod
Tables: ecommerce_semantic_model_mv, ecommerce_metrics_mv
```

### Power BI

```
Connection: StarRocks
Server: tcp:your-starrocks-host,9030
Database: analytics_prod
Tables: ecommerce_semantic_model_mv, ecommerce_metrics_mv
```

### Metabase

```
Database: StarRocks
Host: your-starrocks-host
Port: 9030
Database: analytics_prod
Username: seeknal_user
Password: your-password
```

---

## Query Optimization

### Use Materialized Views

```sql
-- This uses the pre-computed materialized view (fast)
SELECT region, SUM(total_revenue)
FROM ecommerce_metrics_mv
GROUP BY region;
```

### Avoid Complex Subqueries

```sql
-- Instead of this
SELECT * FROM (
    SELECT * FROM (
        SELECT * FROM base_table
    ) WHERE condition1
) WHERE condition2;

-- Use this
SELECT * FROM base_table
WHERE condition1 AND condition2;
```

### Limit Result Sets

```bash
seeknal query ecommerce_metrics \
  --measures total_revenue \
  --dimensions region \
  --limit 10
```

---

## Troubleshooting

### Query Timeout

**Issue**: Query takes too long to execute.

**Solutions**:
- Check materialization strategy
- Add appropriate filters
- Review query execution plan

### Incorrect Results

**Issue**: Query returns unexpected values.

**Solutions**:
- Verify semantic model definition
- Check metric formulas
- Validate data sources

---

## Best Practices

1. **Use filters** to reduce data volume
2. **Leverage materialized views** for performance
3. **Test queries** before BI integration
4. **Document common queries** for team
5. **Monitor query performance** regularly

---

## Related Topics

- [Semantic Models](semantic-models.md) - Model structure
- [Metrics](metrics.md) - Metric definitions
- [Deployment](deployment.md) - Production deployment

---

**Return to**: [Semantic Layer Overview](index.md)
