# Semantic Layer Deployment

Deploy semantic models and metrics to production data warehouses for BI tool consumption.

---

## Deployment Overview

Seeknal deploys semantic models and metrics to StarRocks, creating optimized materialized views that BI tools can query directly.

---

## Pre-Deployment Checklist

Before deploying, ensure:

- [ ] Semantic models are tested and validated
- [ ] Metrics are calculated correctly
- [ ] Performance is acceptable
- [ ] Access controls are configured
- [ ] Documentation is complete

---

## Deploying to StarRocks

### Initial Deployment

```bash
# Deploy all semantic models and metrics
seeknal deploy-semantic-layer

# Deploy specific model
seeknal deploy-semantic-layer --model ecommerce_semantic_model

# Deploy with environment
seeknal deploy-semantic-layer --env production
```

### What Gets Deployed

1. **Base Tables**: Raw data from sources
2. **Semantic Models**: Joined and transformed data
3. **Metric Tables**: Pre-calculated aggregations
4. **Materialized Views**: Optimized query performance

---

## Deployment Configuration

### StarRocks Connection

```yaml
# config/prod.toml
[starrocks]
host = "prod-starrocks.example.com"
port = 9030
username = "seeknal_user"
password = "${STARROCKS_PASSWORD}"
database = "analytics_prod"
```

### Materialization Strategy

```yaml
semantic_model:
  name: ecommerce_semantic_model

  materialization:
    type: materialized_view
    refresh_strategy: manual  # or scheduled
    schedule: "0 2 * * *"  # 2 AM daily
```

---

## Refresh Strategies

### Manual Refresh

```bash
# Trigger manual refresh
seeknal refresh-semantic-layer --model ecommerce_semantic_model
```

### Scheduled Refresh

```yaml
materialization:
  type: materialized_view
  refresh_strategy: scheduled
  schedule: "0 2 * * *"  # Cron schedule
```

### Incremental Refresh

```yaml
materialization:
  type: incremental
  incremental_key: updated_at
  lookback_days: 7
```

---

## Validation

### Post-Deployment Validation

```bash
# Validate deployment
seeknal validate-semantic-layer --model ecommerce_semantic_model
```

**Checks**:
- View creation success
- Row counts match expectations
- Query performance acceptable
- Access controls working

### Manual Validation

```sql
-- Test deployed semantic model
SELECT region, total_revenue
FROM ecommerce_semantic_model_mv
GROUP BY region;
```

---

## Access Control

### Granting Permissions

```sql
-- Grant read access to BI users
GRANT SELECT ON ecommerce_semantic_model_mv TO bi_user;
GRANT SELECT ON ecommerce_metrics_mv TO bi_user;
```

### Row-Level Security

```yaml
semantic_model:
  name: customer_metrics

  security:
    row_filter: "region = CURRENT_USER_REGION()"
```

---

## Troubleshooting

### Deployment Failures

**Issue**: Deployment fails with connection error.

**Solution**: Check StarRocks connection configuration.

**Issue**: View creation fails due to invalid SQL.

**Solution**: Test semantic model locally first.

### Performance Issues

**Issue**: Queries are slow after deployment.

**Solutions**:
- Check materialization strategy
- Add appropriate indexes
- Review query plans

---

## Best Practices

1. **Test locally** before deploying
2. **Use environments** (dev → staging → prod)
3. **Monitor performance** after deployment
4. **Version changes** with Git
5. **Document refresh** schedules

---

## Related Topics

- [Semantic Models](semantic-models.md) - Defining models
- [Metrics](metrics.md) - Creating metrics
- [Querying](querying.md) - Query patterns

---

**Next**: Learn about [Querying](querying.md) or return to [Semantic Layer Overview](index.md)
