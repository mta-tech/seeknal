# Chapter 3: Deploy for Self-Serve Analytics

> **Duration:** 25 minutes | **Difficulty:** Advanced | **Format:** YAML & CLI

Learn to deploy your semantic layer metrics to StarRocks as materialized views, enabling fast BI queries for your organization.

---

## What You'll Build

A production analytics deployment:

```
Metrics (YAML) → seeknal deploy-metrics → StarRocks MVs → BI Tools
                                               ↓
                                         MySQL Protocol
                                    (Tableau, Metabase, etc.)
```

**After this chapter, you'll have:**
- StarRocks connection configured
- Metrics deployed as materialized views
- DDL preview with `--dry-run`
- BI tool connections via MySQL protocol

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Semantic Models](1-semantic-models.md) — Semantic model defined
- [ ] [Chapter 2: Business Metrics](2-business-metrics.md) — Metrics defined
- [ ] StarRocks instance accessible (local or remote)

!!! info "StarRocks Optional"
    This chapter requires access to a StarRocks instance. If you don't have one, you can still follow along using the `--dry-run` flag to preview the DDL that would be generated. The semantic layer and metrics from Chapters 1-2 work without StarRocks.

---

## Part 1: Configure StarRocks Connection (8 minutes)

### Understanding StarRocks

StarRocks is a fast, real-time analytics database:

- **Sub-second queries** — Interactive dashboards
- **MySQL protocol** — Standard BI tool compatibility
- **Materialized views** — Pre-computed aggregations
- **High concurrency** — Many users simultaneously

### Set Up Connection

Add StarRocks connection to your `profiles.yml`:

```yaml
# profiles.yml (or ~/.seeknal/profiles.yml)
connections:
  starrocks:
    type: starrocks
    host: localhost
    port: 9030
    user: root
    password: ""
    database: analytics
```

Or use a connection URL directly:

```bash
export STARROCKS_URL="starrocks://root@localhost:9030/analytics"
```

### Test Connection

Verify the connection works:

```bash
seeknal repl
```

```sql
-- If using StarRocks connection in profiles
.connect starrocks
SELECT 1 AS test;
```

**Checkpoint:** You should see a successful query result. If StarRocks isn't available, proceed with `--dry-run` in the next parts.

---

## Part 2: Deploy Metrics as Materialized Views (10 minutes)

### Preview DDL

Always preview before deploying:

```bash
seeknal deploy-metrics \
  --connection "starrocks://root@localhost:9030/analytics" \
  --dry-run
```

**Expected output:**
```
ℹ Auto-including dimension 'order_date' (required for StarRocks MV keys)

ℹ -- Metric: avg_order_value_ratio -> mv_avg_order_value_ratio
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_avg_order_value_ratio
REFRESH ASYNC EVERY(INTERVAL 1 DAY)
AS
SELECT
  order_date,
  SUM(revenue) / NULLIF(COUNT(1), 0) AS avg_order_value_ratio
FROM orders_cleaned
GROUP BY order_date

ℹ -- Metric: total_revenue -> mv_total_revenue
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_total_revenue
REFRESH ASYNC EVERY(INTERVAL 1 DAY)
AS
SELECT
  order_date,
  SUM(revenue) AS total_revenue
FROM orders_cleaned
GROUP BY order_date

ℹ -- Metric: order_count -> mv_order_count
...

ℹ Dry run: 4 MVs would be created
```

!!! tip "Auto-Include Dimensions"
    When you don't specify `--dimensions`, seeknal automatically includes the `default_time_dimension` from your semantic model (in this case, `order_date`). StarRocks MVs require at least one non-aggregate key column, and the time dimension fulfills that. You can override this by passing `--dimensions` explicitly.

!!! tip "Review Before Deploy"
    The `--dry-run` flag shows exactly what DDL will execute. Review column types, refresh intervals, and filter logic before deploying.

!!! info "Cumulative Metrics Are Skipped"
    Cumulative metrics use window functions which StarRocks materialized views don't support. These metrics are automatically skipped during deployment — query them via `seeknal query` instead.

### Deploy for Real

```bash
seeknal deploy-metrics --connection "starrocks://root@localhost:9030/analytics"
```

**Expected output:**
```
ℹ Auto-including dimension 'order_date' (required for StarRocks MV keys)
✓ Deployed avg_order_value_ratio -> mv_avg_order_value_ratio
✓ Deployed total_revenue -> mv_total_revenue
✓ Deployed order_count -> mv_order_count
✓ Deployed avg_order_value -> mv_avg_order_value
ℹ Deployed: 4 succeeded, 0 failed
```

### Add Extra Dimensions to MVs

To include additional dimensions beyond the auto-included time dimension, use `--dimensions`:

```bash
seeknal deploy-metrics \
  --connection "starrocks://root@localhost:9030/analytics" \
  --dimensions order_date,status \
  --dry-run
```

This generates MVs grouped by both `order_date` and `status`, enabling fast GROUP BY queries on those dimensions.

### Customize Refresh Interval

```bash
seeknal deploy-metrics \
  --connection "starrocks://root@localhost:9030/analytics" \
  --refresh-interval "1 HOUR"
```

!!! info "Refresh Intervals"
    - `1 HOUR` — Near real-time dashboards
    - `1 DAY` — Daily reporting (default)
    - StarRocks handles refresh automatically in the background

### Drop and Recreate

If you change metric definitions, use `--drop-existing`:

```bash
seeknal deploy-metrics \
  --connection "starrocks://root@localhost:9030/analytics" \
  --drop-existing
```

---

## Part 3: Query Deployed Metrics (7 minutes)

### Query StarRocks Directly

Connect to StarRocks using any MySQL client:

```bash
mysql -h localhost -P 9030 -u root analytics
```

```sql
-- Query materialized view directly
SELECT * FROM mv_total_revenue ORDER BY order_date;

-- Aggregated query across MVs
SELECT
  t.order_date,
  t.total_revenue,
  c.order_count,
  a.avg_order_value_ratio
FROM mv_total_revenue t
JOIN mv_order_count c ON t.order_date = c.order_date
JOIN mv_avg_order_value_ratio a ON t.order_date = a.order_date
ORDER BY t.order_date;
```

### Connect BI Tools

StarRocks uses MySQL protocol, so any MySQL-compatible BI tool works:

| BI Tool | Connection Type | Port |
|---------|----------------|------|
| **Metabase** | MySQL | 9030 |
| **Tableau** | MySQL JDBC | 9030 |
| **Power BI** | MySQL ODBC | 9030 |
| **Grafana** | MySQL data source | 9030 |
| **Apache Superset** | MySQL SQLAlchemy | 9030 |

**Connection string format:**
```
mysql://root@localhost:9030/analytics
```

### Verify Metrics Match

Compare deployed MV results with your local `seeknal query`:

```bash
# Local query
seeknal query --metrics total_revenue --dimensions order_date --format csv

# StarRocks query (via mysql CLI)
mysql -h localhost -P 9030 -u root analytics -e \
  "SELECT order_date, total_revenue FROM mv_total_revenue ORDER BY order_date"
```

**Checkpoint:** Both should return identical results.

!!! success "Congratulations!"
    You've deployed a governed semantic layer. Stakeholders can now query consistent metrics through any BI tool, without writing SQL or worrying about metric definitions.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Connection refused**

    - Symptom: `Can't connect to StarRocks`
    - Fix: Verify StarRocks is running and port 9030 is accessible

    **2. "Table not found" in MV**

    - Symptom: MV creation fails with table reference error
    - Fix: The base table (`orders_cleaned`) must exist in StarRocks. Load your pipeline data first.

    **3. MV refresh failures**

    - Symptom: Stale data in materialized views
    - Fix: Check `SHOW MATERIALIZED VIEWS` for refresh status. Adjust interval if needed.

    **4. Permission denied**

    - Symptom: Cannot create materialized views
    - Fix: Ensure your StarRocks user has CREATE MATERIALIZED VIEW privilege

    **5. Replication error on single-node StarRocks**

    - Symptom: `Table replication num should be less than or equal to the number of available backends`
    - Fix: Set `PROPERTIES ("replication_num" = "1")` when creating the base table

    **6. Cumulative metric skipped**

    - Symptom: `SKIPPED: Cumulative metric uses window functions`
    - Explanation: Window functions are not supported in StarRocks MVs. Use `seeknal query --metrics cumulative_revenue` locally instead

---

## Summary

In this chapter, you learned:

- [x] **StarRocks Connection** — Configure connection in profiles.yml or URL
- [x] **DDL Preview** — Use `--dry-run` to review before deploying
- [x] **Metric Deployment** — Deploy metrics as materialized views
- [x] **Refresh Intervals** — Control how often MVs refresh
- [x] **BI Integration** — Connect any MySQL-compatible BI tool

**Key Commands:**
```bash
seeknal deploy-metrics --connection <url> --dry-run    # Preview DDL
seeknal deploy-metrics --connection <url>              # Deploy MVs
seeknal deploy-metrics --connection <url> --drop-existing  # Recreate
seeknal deploy-metrics --dimensions <names>            # Include dimensions
seeknal deploy-metrics --refresh-interval "1 HOUR"     # Custom refresh
```

---

## Path Complete!

You've completed the Analytics Engineer path. Here's what you built:

```
orders_cleaned → Semantic Model → Metrics → StarRocks MVs → BI Tools
                      ↓               ↓
                 entities         simple, ratio,
                 dimensions       cumulative, derived
                 measures
```

### What's Next?

- **[Data Engineer Path](../data-engineer-path/)** — Build production ELT pipelines
- **[ML Engineer Path](../ml-engineer-path/)** — Feature stores and ML workflows
- **[CLI Reference](../../reference/cli.md)** — Full command reference

---

## See Also

- **[CLI Reference](../../reference/cli.md)** — All `seeknal deploy-metrics` flags
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Semantic model and metric schemas
