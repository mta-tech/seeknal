# Mixed YAML and Python Pipelines Tutorial

Learn how to combine YAML and Python pipeline nodes in a single Seeknal project for maximum flexibility.

## Overview

Seeknal supports **mixed pipelines** where you can:
- Reference YAML nodes from Python nodes via `ctx.ref()`
- Reference Python nodes from YAML nodes via `inputs`
- Use YAML for simple operations and Python for complex transformations
- Get the best of both worlds

## When to Use Mixed Pipelines

| Use YAML For | Use Python For |
|---------------|----------------|
| Simple data sources | Complex business logic |
| Basic SQL transforms | Machine learning models |
| Static configurations | Custom algorithms |
| DBT-style workflows | Data science pipelines |
| Version-controlled schemas | External API integrations |

---

## Example Project: Sales Analytics

We'll build a sales analytics pipeline that:
1. **YAML Source**: Load sales data (simple)
2. **Python Transform**: Enrich with external data (complex)
3. **YAML Transform**: Aggregate by region (simple SQL)
4. **Python Model**: Forecast sales (ML)
5. **YAML Exposure**: Export results (static)

---

## Step 1: Initialize Project

```bash
seeknal init --name sales_analytics
cd sales_analytics
```

---

## Step 2: Create YAML Source

Create `seeknal/sources/sales_data.yml`:

```yaml
name: raw_sales
kind: source
source: csv
table: data/sales.csv
description: Raw sales transactions
tags:
  - sales
  - raw
columns:
  transaction_id: int
  date: date
  product_id: str
  quantity: int
  unit_price: float
  region: str
```

---

## Step 3: Create Python Transform (References YAML)

Create `seeknal/pipelines/enrich_sales.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
#     "requests",
# ]
# ///

"""
Transform: Enrich sales data with product information from external API.

This demonstrates Python nodes referencing YAML nodes.
"""

from seeknal.pipeline import transform


@transform(
    name="enriched_sales",
    description="Add product category and margin to sales data",
)
def enriched_sales(ctx):
    """Enrich sales data with product information."""
    # Reference YAML source
    sales = ctx.ref("source.raw_sales")

    return ctx.duckdb.sql("""
        SELECT
            s.*,
            -- Add product categories (would come from API or lookup)
            CASE
                WHEN s.product_id LIKE 'ELE%' THEN 'Electronics'
                WHEN s.product_id LIKE 'CLO%' THEN 'Clothing'
                WHEN s.product_id LIKE 'FOO%' THEN 'Food'
                ELSE 'Other'
            END as product_category,
            -- Calculate margin (would come from cost data)
            (s.unit_price * 0.7) as unit_cost,
            (s.unit_price - (s.unit_price * 0.7)) as margin
        FROM sales s
    """).df()
```

**Key Point:** `ctx.ref("source.raw_sales")` references the YAML source node!

---

## Step 4: Create YAML Transform (References Python)

Create `seeknal/transforms/region_totals.yml`:

```yaml
name: regional_totals
kind: transform
description: Aggregate sales by region
inputs:
  - ref: transform.enriched_sales
sql: |
  SELECT
    region,
    product_category,
    COUNT(*) as transaction_count,
    SUM(quantity) as total_quantity,
    SUM(margin) as total_margin
  FROM __THIS__
  GROUP BY region, product_category
  ORDER BY total_margin DESC
```

**Key Point:** `ref: transform.enriched_sales` references the Python node!

---

## Step 5: Create Python Model Node

Create `seeknal/pipelines/sales_forecast.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
#     "scikit-learn",
# ]
# ///

"""
Model: Simple sales forecast using moving average.

This demonstrates Python nodes referencing mixed upstream sources.
"""

from seeknal.pipeline import transform


@transform(
    name="sales_forecast",
    description="Forecast next month sales by region",
)
def sales_forecast(ctx):
    """Generate sales forecast using moving average."""
    # Reference YAML transform output
    aggregated = ctx.ref("transform.regional_totals")

    return ctx.duckdb.sql("""
        WITH time_series AS (
            SELECT
                region,
                product_category,
                SUM(total_margin) as monthly_margin,
                ROW_NUMBER() OVER (PARTITION BY region ORDER BY region) as rn
            FROM aggregated
        ),
        moving_avg AS (
            SELECT
                region,
                product_category,
                monthly_margin,
                AVG(monthly_margin) OVER (
                    PARTITION BY region
                    ORDER BY region
                    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
                ) as forecast_margin
            FROM time_series
            WHERE rn <= 12  -- Use first 12 periods for training
        )
        SELECT
            region,
            product_category,
            forecast_margin,
            CASE
                WHEN forecast_margin > monthly_margin * 1.1 THEN 'UP'
                WHEN forecast_margin < monthly_margin * 0.9 THEN 'DOWN'
                ELSE 'STABLE'
            END as trend
        FROM moving_avg
        ORDER BY forecast_margin DESC
    """).df()
```

---

## Step 6: Create YAML Exposure

Create `seeknal/exposures/manager_dashboard.yml`:

```yaml
name: manager_dashboard
kind: exposure
description: Export for regional management dashboard
inputs:
  - ref: transform.sales_forecast
sql: |
  SELECT
    region,
    product_category,
    forecast_margin,
    trend,
    RANK() OVER (ORDER BY forecast_margin DESC) as margin_rank
  FROM __THIS__
  WHERE trend IN ('UP', 'STABLE')
materialization:
  offline:
    format: csv
    path: exports/manager_dashboard.csv
```

---

## Step 7: Build and Run Pipeline

```bash
# Show execution plan
seeknal run --show-plan

# Execute pipeline
seeknal run
```

**Expected output:**
```
Execution Plan:
============================================================
   1. RUN raw_sales [sales, raw]
   2. RUN enriched_sales
   3. RUN regional_totals
   4. RUN sales_forecast
   5. RUN manager_dashboard

Note: Mix of YAML (1, 2, 5) and Python (3, 4) nodes
```

---

## Visualization: Mixed Pipeline DAG

```
┌─────────────────────────────────────────────────────────────┐
│                    Mixed Pipeline DAG                          │
├─────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐          ctx.ref()          ┌──────────┐  │
│  │ sales_data    │  ───────────────────────▶  │enriched  │  │
│  │   (YAML)      │                            │  sales    │  │
│  └──────────────┘                            └──────────┘  │
│         │                                            │         │
│         │                                            ▼         │
│  ┌──────────────┐                            ┌──────────┐  │
│  │  regional     │ ◀─────────────────────────│  sales   │  │
│  │   totals      │     inputs:            │ forecast │  │
│  │   (YAML)      │     transform.          │(Python)  │  │
│  └──────────────┘     enriched_sales      └──────────┘  │
│         │                                            │         │
│         ▼                                            ▼         │
│  ┌──────────────┐                            ┌──────────┐  │
│  │   manager     │ ◀─────────────────────────│ dashboard│  │
│  │   dashboard   │     inputs:            │ export   │  │
│  │   (YAML)      │     transform.sales_    │ (YAML)   │  │
│  └──────────────┘     forecast                └──────────┘  │
│                                                                   │
└─────────────────────────────────────────────────────────────┘
```

---

## Complete Example: E-Commerce Pipeline

Let's build a more realistic e-commerce analytics pipeline with mixed nodes.

### Project Structure

```
sales_analytics/
├── seeknal/
│   ├── sources/
│   │   ├── orders.yml              # YAML: Load orders
│   │   └── products.yml            # YAML: Product catalog
│   ├── transforms/
│   │   ├── daily_revenue.yml       # YAML: Simple aggregation
│   │   └── cohort_analysis.yml     # YAML: Cohort metrics
│   ├── pipelines/
│   │   ├── customer_segments.py    # Python: ML clustering
│   │   ├── recommendation_engine.py # Python: Collaborative filtering
│   │   └── inventory_optimization.py # Python: Optimization algorithm
│   ├── feature_groups/
│   │   ├── customer_features.yml   # YAML: Simple features
│   │   └── product_affinity.yml    # YAML: Product affinity
│   └── exposures/
│       └── marketing_campaign.yml  # YAML: Export for marketing
└── data/
    ├── orders.csv
    └── products.csv
```

### 1. YAML Sources

**seeknal/sources/orders.yml:**
```yaml
name: raw_orders
kind: source
source: csv
table: data/orders.csv
description: Raw order data
tags: [orders, raw]
```

**seeknal/sources/products.yml:**
```yaml
name: product_catalog
kind: source
source: csv
table: data/products.csv
description: Product catalog
tags: [products, catalog]
```

### 2. Python Transform: Complex Join (References YAML Sources)

**seeknal/pipelines/customer_ltv.py:**
```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""
Transform: Calculate customer lifetime value.

Complex business logic using data from multiple YAML sources.
"""

from seeknal.pipeline import transform


@transform(
    name="customer_ltv",
    description="Calculate customer lifetime value (LTV)",
)
def customer_ltv(ctx):
    """Calculate LTV from orders and products."""
    # Reference multiple YAML sources
    orders = ctx.ref("source.raw_orders")
    products = ctx.ref("source.product_catalog")

    # Register for SQL access
    ctx.duckdb.register("orders", orders)
    ctx.duckdb.register("products", products)

    return ctx.duckdb.sql("""
        WITH order_totals AS (
            SELECT
                o.customer_id,
                SUM(o.quantity * p.unit_price) as total_spend,
                COUNT(DISTINCT o.order_id) as order_count,
                MIN(o.order_date) as first_order,
                DATEDIFF('day', CURRENT_DATE, MIN(o.order_date)) as days_since_first_order
            FROM orders o
            LEFT JOIN products p ON o.product_id = p.product_id
            GROUP BY o.customer_id
        ),
        customer_metrics AS (
            SELECT
                customer_id,
                total_spend,
                order_count,
                first_order,
                -- Calculate average order value
                total_spend / NULLIF(order_count, 0) as avg_order_value,
                days_since_first_order,
                -- Simple LTV prediction (3x AOV * order_count)
                (total_spend / NULLIF(order_count, 0)) * 3 * order_count as predicted_ltv
            FROM order_totals
        )
        SELECT
            c.*,
            NTILE(10) OVER (ORDER BY predicted_ltv DESC) as ltv_decile,
            CASE
                WHEN predicted_ltv > 1000 THEN 'High Value'
                WHEN predicted_ltv > 500 THEN 'Medium Value'
                ELSE 'Low Value'
            END as value_segment
        FROM customer_metrics c
        ORDER BY predicted_ltv DESC
    """).df()
```

### 3. YAML Transform: Simple Aggregation (References Python Node)

**seeknal/transforms/segment_summary.yml:**
```yaml
name: segment_summary
kind: transform
description: Summarize customer segments
inputs:
  - ref: transform.customer_ltv
sql: |
  SELECT
    value_segment,
    ltv_decile,
    COUNT(*) as customer_count,
    AVG(avg_order_value) as segment_aov,
    SUM(predicted_ltv) as_total_ltv,
    AVG(days_since_first_order) as avg_recency
  FROM __THIS__
  GROUP BY value_segment, ltv_decile
  ORDER BY total_ltv DESC
```

### 4. Run Mixed Pipeline

```bash
seeknal run
```

**Execution flow:**
```
1. YAML: Load orders (source)
2. YAML: Load products (source)
3. Python: Calculate LTV (uses both YAML sources)
4. YAML: Summarize segments (uses Python output)
```

---

## Advanced: Dynamic Imports

Python nodes can also import YAML-defined schemas and metadata:

**seeknal/pipelines/dynamic_validation.py:**
```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "duckdb",
# ]
# ///

"""
Transform: Dynamic validation using YAML schema.

Demonstrates reading YAML metadata from Python nodes.
"""

from seeknal.pipeline import transform
import yaml
from pathlib import Path


@transform(
    name="validated_sales",
    description="Validate sales data against YAML schema",
)
def validated_sales(ctx):
    """Validate data using schema defined in YAML."""
    # Get data
    sales = ctx.ref("source.raw_sales")

    # Load YAML schema (if defined)
    schema_path = Path("seeknal/sources/sales_data.yml")
    if schema_path.exists():
        with open(schema_path) as f:
            schema = yaml.safe_load(f)
            # Access schema metadata
            expected_columns = schema.get("columns", {})
            print(f"Validating against schema: {schema.get('name')}")

    # Apply validation logic
    return ctx.duckdb.sql("""
        SELECT
            *,
            -- Add data quality flags
            CASE
                WHEN quantity <= 0 THEN 'Invalid Quantity'
                WHEN unit_price <= 0 THEN 'Invalid Price'
                WHEN product_id IS NULL THEN 'Missing Product'
                ELSE 'Valid'
            END as validation_status
        FROM sales
    """).df()
```

---

## Best Practices for Mixed Pipelines

### 1. Clear Naming Conventions

```python
# Good: Clear indication of what type of node it is
@source(name="user_events")        # Could be YAML or Python
@transform(name="enrich_users")    # Complex = Python
@feature_group(name="user_features")  # Could be either

# Use prefixes for clarity in Python files
def load_user_data(ctx):    # Clear it's a Python function
```

### 2. Documentation in Mixed Projects

```yaml
# Always document if YAML nodes reference Python
name: regional_sales
kind: transform
description: Aggregate by region (uses Python enrichment)
inputs:
  - ref: transform.enrich_sales  # Note: This is a Python node!
```

### 3. Dependency Management

```python
# Python nodes need explicit dependencies
# dependencies = [
#     "pandas",      # For ctx.ref() results
#     "duckdb",      # For SQL queries
#     "scikit-learn", # For ML models
# ]
```

### 4. Testing Strategies

```bash
# Test Python nodes in isolation
seeknal run --nodes customer_ltv

# Test specific types
seeknal run --type source
seeknal run --type transform

# Dry-run validation
seeknal dry-run seeknal/transforms/segment_summary.yml
```

---

## Common Patterns

### Pattern 1: YAML Sources → Python Enrichment

```yaml
# sources/web_analytics.yml
name: raw_events
kind: source
source: postgres
table: web_events
```

```python
# pipelines/enrich_events.py
@transform(name="enriched_events")
def enriched_events(ctx):
    events = ctx.ref("source.raw_events")
    # Complex Python logic...
    return enriched_df
```

### Pattern 2: Python Compute → YAML Aggregation

```python
# pipelines/compute_metrics.py
@transform(name="computed_metrics")
def computed_metrics(ctx):
    df = ctx.ref("source.raw_data")
    # Complex calculations...
    return metrics_df
```

```yaml
# transforms/aggregate_metrics.yml
name: daily_metrics
kind: transform
inputs:
  - ref: transform.computed_metrics
sql: |
  SELECT DATE(event_time) as date, SUM(value) as total
  FROM __THIS__
  GROUP BY DATE(event_time)
```

### Pattern 3: Bidirectional References

```python
# pipelines/customer_360.py
@transform(name="customer_360")
def customer_360(ctx):
    # Reference both YAML and Python nodes
    orders = ctx.ref("source.raw_orders")      # YAML source
    ltv = ctx.ref("transform.customer_ltv")       # Python transform
    segments = ctx.ref("transform.segments")     # YAML transform

    # Complex join logic...
    return customer_360_df
```

---

## Troubleshooting Mixed Pipelines

### Issue: Node Not Found

```
Error: Node 'source.raw_data' not found
```

**Solution:** Ensure the YAML file is in the correct directory and valid:
```bash
# Check if YAML is discovered
seeknal run --show-plan

# Validate YAML syntax
python -c "import yaml; yaml.safe_load(open('seeknal/sources/my_source.yml'))"
```

### Issue: Type Mismatch

```
Error: Cannot join DataFrame with dict
```

**Solution:** Ensure consistent return types. Python nodes must return pandas DataFrames:
```python
# Always return DataFrame
@transform(name="my_transform")
def my_transform(ctx):
    df = ctx.ref("source.raw_data")
    result = some_complex_logic(df)
    return pd.DataFrame(result)  # Ensure DataFrame type
```

### Issue: Circular Dependencies

```
Error: Circular dependency detected
```

**Solution:** Break the cycle with intermediate nodes:
```python
# Instead of: A → B → C → A
# Create: A → B, A → C, B → D, C → D
```

---

## Summary

Mixed pipelines give you flexibility:

| Aspect | YAML | Python |
|--------|------|--------|
| **Learning curve** | Simple | Moderate |
| **Data sources** | Excellent | Good |
| **SQL transforms** | Built-in | Via DuckDB |
| **Complex logic** | Limited | Unlimited |
| **ML/AI** | No | Yes (scikit-learn, etc.) |
| **External APIs** | No | Yes (requests, etc.) |
| **Version control** | Git-friendly | Git-friendly |

**Use both for maximum productivity!**
- Use YAML for what it's good at (sources, simple SQL)
- Use Python for everything else (complex transforms, ML, APIs)
