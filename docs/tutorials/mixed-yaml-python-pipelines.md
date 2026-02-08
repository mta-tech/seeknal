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
4. **Python ML Forecast**: Predict sales using scikit-learn RandomForest
5. **YAML Exposure**: Export results (static)

---

## Step 1: Initialize Project

```bash
seeknal init --name sales_analytics
cd sales_analytics
```

---

## Step 2: Generate Sample Data

Create a sample sales dataset using Python:

```bash
# Create data directory
mkdir -p data

# Generate sample sales data
python3 << 'EOF'
import pandas as pd
import random
from datetime import datetime, timedelta

# Set random seed for reproducibility
random.seed(42)

# Generate 1000 sample transactions
regions = ['USA', 'UK', 'Germany', 'France', 'Japan']
product_ids = [f'ELE-{i:04d}' for i in range(100)] + \
               [f'CLO-{i:04d}' for i in range(100)] + \
               [f'FOO-{i:04d}' for i in range(100)]

data = []
for i in range(1000):
    data.append({
        'transaction_id': f'TXN-{i:06d}',
        'date': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d'),
        'product_id': random.choice(product_ids),
        'quantity': random.randint(1, 10),
        'unit_price': round(random.uniform(10, 500), 2),
        'region': random.choice(regions)
    })

df = pd.DataFrame(data)
df.to_csv('data/sales.csv', index=False)
print(f"✓ Generated {len(df)} sample sales transactions")
print(f"  Columns: {list(df.columns)}")
print(f"  File: data/sales.csv")
EOF
```

**Expected output:**
```
✓ Generated 1000 sample sales transactions
  Columns: ['transaction_id', 'date', 'product_id', 'quantity', 'unit_price', 'region']
  File: data/sales.csv
```

---

## Step 3: Create YAML Source

Create `seeknal/sources/raw_sales.yml`:

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

## Step 4: Create Python Transform (References YAML)

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

## Step 5: Create YAML Transform (References Python)

Create `seeknal/transforms/regional_totals.yml`:

```yaml
name: regional_totals
kind: transform
description: Aggregate sales by region
inputs:
  - ref: transform.enriched_sales
transform: |
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

## Step 6: Create ML Forecast Node

Create `seeknal/pipelines/sales_forecast.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
#     "scikit-learn",
#     "numpy",
# ]
# ///

"""
Transform: ML-based sales forecast using scikit-learn.

This demonstrates Python nodes with ML models.
Uses RandomForestRegressor to predict future margins based on
historical performance metrics.
"""

from seeknal.pipeline import transform
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor


@transform(
    name="sales_forecast",
    description="Forecast next period sales using ML regression",
)
def sales_forecast(ctx):
    """Generate ML-based sales forecast."""
    # Reference YAML transform output
    aggregated = ctx.ref("transform.regional_totals")

    # Convert to DataFrame
    if not isinstance(aggregated, pd.DataFrame):
        aggregated = aggregated.df()

    # Prepare features for ML
    df = aggregated.copy()

    # Feature engineering
    df['margin_per_unit'] = df.apply(
        lambda row: row['total_margin'] / row['total_quantity'] if row['total_quantity'] > 0 else 0,
        axis=1
    )
    df['avg_transaction_size'] = df.apply(
        lambda row: row['total_quantity'] / row['transaction_count'] if row['transaction_count'] > 0 else 0,
        axis=1
    )

    # Encode categorical variables
    df['region_encoded'] = df['region'].astype('category').cat.codes
    df['category_encoded'] = df['product_category'].astype('category').cat.codes

    # Prepare feature matrix
    feature_cols = ['total_quantity', 'transaction_count', 'margin_per_unit',
                    'avg_transaction_size', 'region_encoded', 'category_encoded']

    X = df[feature_cols].values
    y = df['total_margin'].values

    # Train RandomForest model
    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X, y)

    # Make predictions with growth factor (simulating future period)
    X_forecast = X.copy()
    # Use numpy slicing for column assignment
    X_forecast[:, 0:1] = X[:, 0:1] * 1.10  # Increase total_quantity by 10%
    X_forecast[:, 1:2] = X[:, 1:2] * 1.08  # Increase transaction_count by 8%

    forecast_margin = model.predict(X_forecast)

    # Add predictions to dataframe
    df['forecast_margin'] = forecast_margin
    df['projected_growth'] = df['forecast_margin'] - df['total_margin']
    df['growth_percentage'] = (df['projected_growth'] / df['total_margin'] * 100).round(2)

    # Determine trend based on ML prediction
    df['trend'] = pd.cut(
        df['growth_percentage'],
        bins=[-np.inf, -2, 2, np.inf],
        labels=['DOWN', 'STABLE', 'UP']
    )

    # Calculate confidence score (R²)
    df['forecast_confidence'] = model.score(X, y)

    # Select output columns
    result = df[[
        'region', 'product_category', 'total_margin', 'forecast_margin',
        'projected_growth', 'growth_percentage', 'trend', 'forecast_confidence'
    ]].rename(columns={'total_margin': 'current_margin'})

    return result.sort_values('forecast_margin', ascending=False)
```

**Key ML Features:**
- **RandomForestRegressor** with 100 trees for robust predictions
- **Feature engineering**: margin_per_unit, avg_transaction_size
- **Categorical encoding**: region and product_category
- **Forecast simulation**: 10% quantity growth, 8% transaction growth
- **Confidence score**: R² score indicates model reliability

---

## Step 7: Enable Iceberg Materialization (Optional)

**Both YAML and Python transforms support Iceberg materialization!**

When enabled, transform outputs are automatically written to Iceberg tables via:
- **Lakekeeper**: Iceberg REST Catalog (catalog management)
- **MinIO**: S3-compatible object storage (data storage)

This gives you:
- Long-term storage in your data lakehouse
- Cross-query with other tools (Spark, Trino, etc.)
- Data versioning and time travel
- Production data serving

### Environment Configuration

**Seeknal automatically loads `.env` files from your project directory!**

Create a `.env` file in your project root with your Lakekeeper/MinIO credentials:

```bash
# .env file in project root
# Lakekeeper (Iceberg REST Catalog)
LAKEKEEPER_URI=http://172.19.0.9:8181
LAKEKEEPER_WAREHOUSE_ID=c008ea5c-fb89-11f0-aa64-c32ca2f52144

# MinIO (S3-compatible Object Storage)
AWS_ENDPOINT_URL=http://172.19.0.9:9000
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin_change_in_production
AWS_REGION=us-east-1

# Keycloak (OAuth2 Authentication)
KEYCLOAK_TOKEN_URL=http://172.19.0.9:8080/realms/atlas/protocol/openid-connect/token
KEYCLOAK_CLIENT_ID=duckdb
KEYCLOAK_CLIENT_SECRET=duckdb-secret-change-in-production
```

**How it works:**

- When you run any `seeknal` command, it automatically searches for `.env` files:
  1. Current directory
  2. Parent directories (up to 3 levels up)
  3. Falls back to default `.env` loading behavior

- **No need to manually source the file** - just create it and run commands!

**Verify environment variables are loaded:**

```bash
# Check if credentials are loaded
seeknal run --help  # This will trigger .env loading

# Or check a specific variable
echo $LAKEKEEPER_URI
```

### Materialization for Python Transforms

**seeknal/pipelines/sales_forecast.py** (already created in Step 6):

The Python transform code remains the same. Materialization is configured separately via YAML.

**Create the materialization config:**

```bash
# Create the transforms directory
mkdir -p seeknal/transforms

# Create materialization config
cat > seeknal/transforms/sales_forecast.yml << 'EOF'
name: sales_forecast
kind: transform
file: seeknal/pipelines/sales_forecast.py
description: ML-based sales forecast with Iceberg materialization
materialization:
  enabled: true
  table: "warehouse.prod.sales_forecast"
  mode: overwrite
  create_table: true
EOF
```

**Configuration options:**

| Option | Type | Description |
|--------|------|-------------|
| `enabled` | boolean | Enable/disable materialization |
| `table` | string | Fully qualified table name (database.schema.table) |
| `mode` | string | `append` or `overwrite` |
| `create_table` | boolean | Auto-create table if it doesn't exist |

### How Python Materialization Works

When `materialization.enabled: true`:

```
┌─────────────────────────────────────────────────────────────┐
│  Python Transform Execution                                  │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  1. Python Function Executes                                 │
│     sales_forecast(ctx) → DataFrame                          │
│                          │                                    │
│                          ▼                                    │
│  2. Intermediate Storage                                      │
│     target/intermediate/transform_sales_forecast.parquet     │
│                          │                                    │
│                          ▼                                    │
│  3. DuckDB View Created                                      │
│     CREATE VIEW transform.sales_forecast AS                  │
│       SELECT * FROM read_parquet('...parquet')               │
│                          │                                    │
│                          ▼                                    │
│  4. Iceberg Materialization                                  │
│     ├─ Get OAuth2 token from Keycloak                        │
│     ├─ Attach to Lakekeeper catalog                          │
│     ├─ CREATE TABLE atlas.prod.sales_forecast AS             │
│     │   SELECT * FROM transform.sales_forecast                │
│     └─ Data written to MinIO S3 as Iceberg                   │
│                          │                                    │
│                          ▼                                    │
│  5. Verification                                             │
│     ✓ Table exists in Lakekeeper catalog                     │
│     ✓ Data stored in MinIO S3                                │
│     ✓ Queryable from any tool                                │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

### Materialization Modes

| Mode | Behavior | SQL | Use Case |
|------|----------|-----|----------|
| `overwrite` | `DROP TABLE + CREATE TABLE` | Replaces all data | Full refresh, daily snapshots |
| `append` | `INSERT INTO` | Adds to existing table | Incremental updates, time-series |

**Example - Incremental forecasts:**
```yaml
materialization:
  enabled: true
  table: "warehouse.prod.sales_forecast_history"
  mode: append  # Keep historical forecasts
```

**Example - Latest snapshot:**
```yaml
materialization:
  enabled: true
  table: "warehouse.prod.sales_forecast_latest"
  mode: overwrite  # Only keep latest forecast
```

### Querying Materialized Data

After materialization, query from any tool:

**From DuckDB (same connection):**
```python
import duckdb

# Query the materialized Iceberg table
df = duckdb.sql("""
    SELECT region, product_category, forecast_margin, trend
    FROM atlas.prod.sales_forecast
    WHERE trend = 'UP'
    ORDER BY forecast_margin DESC
""").df()
```

**From a fresh DuckDB connection:**
```python
from seeknal.workflow.materialization.yaml_integration import IcebergMaterializationHelper

with IcebergMaterializationHelper.get_duckdb_connection() as con:
    df = con.execute("""
        SELECT * FROM atlas.prod.sales_forecast
        ORDER BY forecast_margin DESC
    """).df()
```

**From Trino/Presto:**
```sql
-- Cross-query from Trino
SELECT region, AVG(forecast_confidence) as avg_confidence
FROM warehouse.prod.sales_forecast
GROUP BY region;
```

**From Spark:**
```scala
// Read Iceberg table in Spark
val df = spark.read.format("iceberg").load("warehouse.prod.sales_forecast")
df.show()
```

---

## Step 8: Create YAML Exposure

First create the exposures directory:
```bash
mkdir -p seeknal/exposures
```

Then create `seeknal/exposures/manager_dashboard.yml`:

```yaml
name: manager_dashboard
kind: exposure
type: file
description: Export for regional management dashboard
depends_on:
  - ref: transform.sales_forecast
params:
  path: exports/manager_dashboard.csv
  format: csv
```

**Note:** Exposures export data as-is from the input ref. They don't support SQL filtering. If you need to filter or transform data before export, create a separate transform node first.

---

## Step 9: Build and Run Pipeline

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
   4. RUN sales_forecast (ML: RandomForestRegressor)
   5. RUN manager_dashboard

Note: Mix of YAML (1, 3, 5) and Python (2, 4) nodes
Step 4 uses scikit-learn's RandomForestRegressor for ML-based forecasting.
```

---

## Visualization: Mixed Pipeline DAG

```
┌─────────────────────────────────────────────────────────────┐
│                    Mixed Pipeline DAG                          │
├─────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌──────────────┐          ctx.ref()          ┌──────────┐  │
│  │  raw_sales    │  ───────────────────────▶  │enriched  │  │
│  │   (YAML)      │                            │  sales    │  │
│  └──────────────┘                            └──────────┘  │
│         │                                            │         │
│         │                                            ▼         │
│  ┌──────────────┐                            ┌──────────┐  │
│  │  regional     │ ◀─────────────────────────│  sales   │  │
│  │   totals      │     inputs:            │ forecast │  │
│  │   (YAML)      │     transform.          │(Python)  │  │
│  └──────────────┘     enriched_sales      └──────────┘  │
│         │              [ML Forecast:            │         │
│         │               RandomForest]           ▼         │
│         ▼                                      ┌──────────┐  │
│  ┌──────────────┐                            │ dashboard│  │
│  │   manager     │ ◀─────────────────────────│ export   │  │
│  │   dashboard   │     inputs:            │ (YAML)   │  │
│  │   (YAML)      │     transform.sales_    └──────────┘  │
│  └──────────────┘     forecast                
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
transform: |
  SELECT
    value_segment,
    ltv_decile,
    COUNT(*) as customer_count,
    AVG(avg_order_value) as segment_aov,
    SUM(predicted_ltv) as total_ltv,
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

### 1. Exposure Node Format

Exposures don't support SQL queries - they export data as-is. Use the correct format:

```yaml
# File export
kind: exposure
type: file
depends_on:
  - ref: transform.my_data
params:
  path: exports/data.csv
  format: csv  # csv, parquet, json, jsonl

# API POST
kind: exposure
type: api
depends_on:
  - ref: transform.results
params:
  url: https://api.example.com/data
  method: POST

# Database write
kind: exposure
type: database
depends_on:
  - ref: transform.processed
params:
  table: production.results
```

**Note:** Use `depends_on` (not `inputs`) for exposure nodes.

**If you need SQL filtering**, create a transform node before the exposure:
```yaml
# Transform: Filter and format data
kind: transform
name: manager_dashboard_filtered
inputs:
  - ref: transform.sales_forecast
transform: |
  SELECT
    region,
    product_category,
    forecast_margin,
    trend,
    RANK() OVER (ORDER BY forecast_margin DESC) as margin_rank
  FROM __THIS__
  WHERE trend IN ('UP', 'STABLE')

# Exposure: Export the filtered data
kind: exposure
type: file
depends_on:
  - ref: transform.manager_dashboard_filtered
params:
  path: exports/manager_dashboard.csv
  format: csv
```

### 2. Clear Naming Conventions

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
transform: |
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
