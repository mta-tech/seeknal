# Chapter 3: Deploy for Self-Serve Analytics

> **Duration:** 25 minutes | **Difficulty:** Advanced | **Format:** YAML & Python

Learn to deploy semantic models and metrics to StarRocks, and enable BI tool integration for self-serve analytics across your organization.

---

## What You'll Build

A complete self-serve analytics deployment:

```
Semantic Models + Metrics → StarRocks → BI Tools → Business Users
                                         ↓
                                   Governed Access
```

**After this chapter, you'll have:**
- StarRocks deployment with semantic models
- Materialized views for performance
- BI tool connections (Tableau, Power BI, Metabase)
- User access controls and governance

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Semantic Models](1-semantic-models.md) — Semantic layer foundation
- [ ] [Chapter 2: Business Metrics](2-business-metrics.md) — Business metrics defined
- [ ] StarRocks installed or access to StarRocks instance
- [ ] Basic understanding of BI tools

---

## Part 1: Deploy to StarRocks (10 minutes)

### Understanding StarRocks

StarRocks is a fast, real-time analytics database that powers:

- **High-concurrency queries** — Many users simultaneously
- **Sub-second response** — Interactive dashboards
- **SQL compatibility** — Standard MySQL protocol
- **Materialized views** — Pre-computed aggregations

!!! info "Why StarRocks?"
    StarRocks combines the best of:
    - **Data warehouses** (Snowflake, BigQuery)
    - **OLAP databases** (ClickHouse, Apache Doris)
    - **Feature stores** (Real-time serving)

=== "Configure StarRocks Connection"

    Edit `seeknal.yml`:

    ```yaml
    project:
      name: ecommerce-analytics

    # StarRocks configuration
    warehouses:
      starrocks:
        type: starrocks
        host: ${STARROCKS_HOST}
        port: 9030
        user: ${STARROCKS_USER}
        password: ${STARROCKS_PASSWORD}
        database: analytics

        # Connection pool settings
        pool:
          min_size: 2
          max_size: 10
          timeout: 30
    ```

    Set environment variables:

    ```bash
    export STARROCKS_HOST="starrocks.example.com"
    export STARROCKS_USER="admin"
    export STARROCKS_PASSWORD="your-password"
    ```

=== "Deploy Semantic Layer"

    ```bash
    # Deploy semantic models and metrics to StarRocks
    seeknal deploy-semantic-layer --target starrocks
    ```

    **Expected output:**
    ```
    Deploying semantic layer to StarRocks...
      ✓ Validating semantic models
      ✓ Validating metrics
      ✓ Creating database schema
      ✓ Creating tables: orders, customers, products
      ✓ Creating materialized views: mv_daily_revenue
      ✓ Creating views: v_orders_status
      ✓ Deploying 12 metrics
      ✓ Granting permissions
    Semantic layer deployed successfully!

    StarRocks endpoint: jdbc:starrocks://starrocks.example.com:9030/analytics
    ```

=== "What Gets Deployed"

    **Tables**: Base semantic model tables
    ```sql
    CREATE TABLE orders (
      order_id STRING,
      customer_id STRING,
      order_date DATE,
      status STRING,
      revenue DECIMAL(10,2),
      ...
    )
    ```

    **Materialized Views**: Pre-aggregated data
    ```sql
    CREATE MATERIALIZED VIEW mv_daily_revenue AS
    SELECT
      order_date,
      status,
      SUM(revenue) as total_revenue,
      COUNT(*) as order_count
    FROM orders
    GROUP BY order_date, status
    ```

    **Views**: Virtual layer for metrics
    ```sql
    CREATE VIEW v_orders_status AS
    SELECT
      status,
      COUNT(*) as order_count
    FROM orders
    GROUP BY status
    ```

---

## Part 2: Create Materialized Views (8 minutes)

### Understanding Materialization Strategies

Materialized views pre-compute aggregations for performance:

| Strategy | Use Case | Refresh |
|----------|----------|---------|
| **Incremental** | Append-only data | Every 5 minutes |
| **Full** | Frequently changing data | Every hour |
| **On-demand** | Rarely accessed | Manual refresh |

=== "Define Materialization Strategy"

    Edit `semantic-models/orders.yaml`:

    ```yaml
    kind: semantic_model
    name: orders
    # ... existing configuration ...

    # Materialization strategy
    materialization:
      target: starrocks

      # Materialized views
      views:
        - name: mv_daily_revenue_by_status
          sql: |
            SELECT
              order_date,
              status,
              SUM(revenue) as total_revenue,
              COUNT(*) as order_count,
              COUNT(DISTINCT customer_id) as unique_customers
            FROM orders
            WHERE status = 'completed'
            GROUP BY order_date, status
          schedule: "0 */2 * * *"  # Every 2 hours

        - name: mv_monthly_metrics
          sql: |
            SELECT
              DATE_TRUNC('month', order_date) as month,
              SUM(revenue) as total_revenue,
              AVG(revenue) as avg_revenue,
              COUNT(*) as order_count
            FROM orders
            WHERE status = 'completed'
            GROUP BY DATE_TRUNC('month', order_date)
          schedule: "0 2 * * *"  # Daily at 2 AM

      # Indexes for performance
      indexes:
        - name: idx_orders_date
          columns: [order_date]
          type: btree

        - name: idx_orders_customer
          columns: [customer_id]
          type: btree
    ```

=== "Refresh Materialized Views"

    ```bash
    # Refresh all materialized views
    seeknal refresh-materialized-views

    # Refresh specific view
    seeknal refresh-materialized-views --view mv_daily_revenue_by_status

    # Schedule automatic refresh
    seeknal schedule-materialized-views --schedule "0 */2 * * *"
    ```

---

## Part 3: Connect BI Tools (7 minutes)

### Understanding BI Integration

Seeknal's semantic layer works with any BI tool that supports:

| BI Tool | Connection Method | Best For |
|---------|------------------|----------|
| **Tableau** | ODBC/JDBC | Interactive dashboards |
| **Power BI** | ODBC | Enterprise reporting |
| **Metabase** | MySQL protocol | Self-serve analytics |
| **Looker** | LookML | Embedded analytics |
| **Superset** | SQLAlchemy | Open-source dashboards |

=== "Tableau Integration"

    **Step 1: Install ODBC Driver**

    Download StarRocks ODBC driver:
    ```bash
    # macOS
    brew install starrocks-odbc

    # Windows
    # Download from StarRocks releases page
    ```

    **Step 2: Configure DSN**

    Create `/etc/odbcinst.ini`:
    ```ini
    [StarRocks ODBC Driver]
    Description = StarRocks ODBC Driver
    Driver = /usr/local/lib/libstarrocks_odbc.so
    UsageCount = 1
    ```

    Create `~/.odbc.ini`:
    ```ini
    [StarRocks Analytics]
    Driver = StarRocks ODBC Driver
    SERVER = starrocks.example.com
    PORT = 9030
    DATABASE = analytics
    UID = admin
    PWD = your-password
    ```

    **Step 3: Connect Tableau**

    1. Open Tableau
    2. Select "Connect to Server" → "Other Databases (ODBC)"
    3. Select "StarRocks Analytics" DSN
    4. Drag tables to canvas
    5. Build your dashboard

=== "Power BI Integration"

    **Step 1: Get Connection String**

    ```bash
    seeknal get-connection-string --target starrocks
    ```

    **Output:**
    ```
    jdbc:starrocks://starrocks.example.com:9030/analytics
    ODBC;DRIVER={StarRocks ODBC Driver};SERVER=starrocks.example.com;PORT=9030;DATABASE=analytics
    ```

    **Step 2: Connect Power BI**

    1. Open Power BI Desktop
    2. Select "Get Data" → "Database" → "ODBC"
    3. Enter connection string
    4. Enter credentials
    5. Select tables/views
    6. Load and build reports

=== "Metabase Integration"

    **Step 1: Add Database Connection**

    In Metabase:
    1. Click "Add database" → "MySQL"
    2. Enter connection details:
       - Host: `starrocks.example.com`
       - Port: `9030`
       - Database: `analytics`
       - Username: `admin`
       - Password: `your-password`
    3. Click "Connect"

    **Step 2: Enable Semantic Layer**

    Metabase can query views directly:

    ```sql
    -- Business-friendly query
    SELECT
      status,
      SUM(total_revenue) as revenue,
      SUM(order_count) as orders
    FROM v_orders_status
    GROUP BY status
    ```

---

## User Access & Governance

!!! tip "Role-Based Access Control"
    ```yaml
    # Define user roles
    roles:
      - name: executive
        permissions:
          - read:metrics:executive-dashboard
          - read:models:orders,customers

      - name: analyst
        permissions:
          - read:metrics:*
          - query:models:*
          - write:dashboards:*

      - name: viewer
        permissions:
          - read:dashboards:*
    ```

!!! warning "Governance Best Practices"
    - **Approve all metrics** before deployment
    - **Document metric definitions** in one place
    - **Version control** all semantic model changes
    - **Audit access** to sensitive data
    - **Test queries** in development first

---

## Summary

In this chapter, you learned:

- [x] **StarRocks Deployment** — Deploy semantic models to StarRocks
- [x] **Materialized Views** — Create pre-computed aggregations
- [x] **BI Tool Integration** — Connect Tableau, Power BI, Metabase
- [x] **User Access Control** — Implement role-based permissions
- [x] **Governance** — Best practices for managed analytics

**Key Commands:**
```bash
seeknal deploy-semantic-layer --target starrocks
seeknal refresh-materialized-views
seeknal get-connection-string --target starrocks
```

---

## Path Completion

Congratulations! You've completed the Analytics Engineer path.

### What You've Learned

1. **Semantic Models** — Define reusable entities, dimensions, measures
2. **Business Metrics** — Create governed KPIs and business metrics
3. **Self-Serve Analytics** — Deploy to StarRocks and enable BI tools

### Next Steps

**Continue Learning:**
- [ ] [Data Engineer Path](../data-engineer-path/) — Data pipelines and infrastructure
- [ ] [ML Engineer Path](../ml-engineer-path/) — Feature stores and ML workflows

**Deep Dives:**
- [ ] [Semantic Layer Guide](../../guides/semantic-layer.md) — Advanced semantic layer concepts
- [ ] [StarRocks Documentation](../../reference/starrocks.md) — Database specifics

**Build Your Semantic Layer:**
- [ ] Deploy semantic models for your data
- [ ] Create business metrics and KPIs
- [ ] Enable self-serve analytics for your team

---

## See Also

- **[StarRocks Guide](../../guides/starrocks.md)** — Deep dive on StarRocks
- **[BI Tool Integration](../../guides/bi-integration.md)** — BI-specific guides
- **[Governance](../../guides/governance.md)** — Access control and governance

**Analytics Engineer Path complete!** 🎉