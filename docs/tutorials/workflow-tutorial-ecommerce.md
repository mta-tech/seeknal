# Seeknal Workflow Tutorial: E-commerce Analytics Pipeline

**Learn Seeknal's dbt-inspired draft/dry-run/apply workflow using real e-commerce data**

---

## Prerequisites

- Seeknal installed with the new workflow commands
- Python 3.11+
- Dataset location: `~/project/self/dbt_get_started/data_seeds/`

---

## Dataset Overview

This tutorial uses an **e-commerce dataset** with 5 tables representing a typical online store:

| Table | Description | Key Fields |
|-------|-------------|------------|
| `raw_customers` | Customer information | customer_id, email, city, state |
| `raw_orders` | Order headers | order_id, customer_id, order_date, order_total |
| `raw_products` | Product catalog | product_id, product_name, category_id, price |
| `raw_categories` | Product categories | category_id, category_name, parent_category |
| `raw_order_items` | Order line items | order_item_id, order_id, product_id, quantity |

**Relationships:**
```
customers → orders → order_items → products → categories
    (1:N)      (1:N)        (N:1)         (N:1)
```

---

## Quick Start (5 Minutes)

### Step 1: Create Your First Source Node

Generate a YAML template for the customers source:

```bash
cd ~/project/self/dbt_get_started/data_seeds/
seeknal draft source raw_customers --description "Customer master data"
```

**Expected output:**
```
✓ Created: draft_source_raw_customers.yml
ℹ Edit the file, then run: seeknal dry-run draft_source_raw_customers.yml
```

### Step 2: Edit the Source Definition

Open `draft_source_raw_customers.yml` and update it:

```yaml
kind: source
name: raw_customers
description: "Customer master data from e-commerce platform"
owner: "data-team@example.com"
tags: ["customers", "ecommerce"]
source: csv
table: "/Users/YOUR_USERNAME/project/self/dbt_get_started/data_seeds/raw_customers.csv"
params:
  delimiter: ","
  header: true
columns:
  customer_id: "Unique customer identifier"
  email: "Customer email address"
  first_name: "Customer first name"
  last_name: "Customer last name"
  city: "Customer city"
  state: "Customer state"
  country: "Customer country"
  zipcode: "Customer postal code"
  status: "Customer status (active/inactive)"
  created_at: "Account creation timestamp"
freshness:
  warn_after: 24h
  error_after: 48h
```

> **Tip:** Replace `YOUR_USERNAME` with your actual username (or use `~` if supported)

### Step 3: Validate and Preview

```bash
seeknal dry-run draft_source_raw_customers.yml
```

**Expected output:**
```
ℹ Validating YAML...
✓ YAML syntax valid
✓ Schema validation passed
✓ Dependency check passed
ℹ Executing preview (limit 10 rows)...
+--------------+-----------------------------------+
| Column       | Description                      |
|--------------+-----------------------------------|
| customer_id  | Unique customer identifier       |
| email        | Customer email address           |
| first_name   | Customer first name              |
| last_name    | Customer last name               |
| city         | Customer city                    |
| state        | Customer state                   |
| country      | Customer country                 |
| zipcode      | Customer postal code             |
| status       | Customer status (active/inactive)|
| created_at   | Account creation timestamp       |
+--------------+-----------------------------------+
✓ Preview completed in 0.1s
ℹ Run 'seeknal apply draft_source_raw_customers.yml' to apply
```

### Step 4: Apply the Source

```bash
seeknal apply draft_source_raw_customers.yml
```

**Expected output:**
```
ℹ Validating...
✓ All checks passed
ℹ Moving file...
ℹ   FROM: ./draft_source_raw_customers.yml
ℹ   TO:   ./seeknal/sources/raw_customers.yml
ℹ Updating manifest...
✓ Manifest regenerated
ℹ Diff:
ℹ   + source.raw_customers
✓ Applied successfully
```

🎉 **Congratulations!** You've created your first source node.

---

## Complete E-commerce Pipeline

Now let's build a complete analytics pipeline with all tables and feature engineering.

### Part 1: Create All Source Nodes

#### 1. Orders Source

```bash
seeknal draft source raw_orders --description "Order transaction data"
```

Edit `draft_source_raw_orders.yml`:

```yaml
kind: source
name: raw_orders
description: "Order transaction data"
owner: "data-team@example.com"
tags: ["orders", "ecommerce"]
source: csv
table: "/Users/YOUR_USERNAME/project/self/dbt_get_started/data_seeds/raw_orders.csv"
params:
  delimiter: ","
  header: true
columns:
  order_id: "Unique order identifier"
  customer_id: "Foreign key to customers"
  order_date: "Order date"
  order_status: "Order status (delivered/pending/etc)"
  payment_method: "Payment method used"
  shipping_method: "Shipping method"
  order_total: "Total order amount"
  shipping_cost: "Shipping cost"
  tax_amount: "Tax amount"
freshness:
  warn_after: 6h
  error_after: 12h
```

Apply it:
```bash
seeknal dry-run draft_source_raw_orders.yml
seeknal apply draft_source_raw_orders.yml
```

#### 2. Products Source

```bash
seeknal draft source raw_products --description "Product catalog"
```

Edit `draft_source_raw_products.yml`:

```yaml
kind: source
name: raw_products
description: "Product catalog"
owner: "data-team@example.com"
tags: ["products", "ecommerce"]
source: csv
table: "/Users/YOUR_USERNAME/project/self/dbt_get_started/data_seeds/raw_products.csv"
params:
  delimiter: ","
  header: true
columns:
  product_id: "Unique product identifier"
  product_name: "Product name"
  category_id: "Foreign key to categories"
  brand: "Product brand"
  price: "Product price"
  cost: "Product cost"
  sku: "Stock keeping unit"
  is_active: "Product availability status"
freshness:
  warn_after: 24h
  error_after: 72h
```

Apply it:
```bash
seeknal dry-run draft_source_raw_products.yml
seeknal apply draft_source_raw_products.yml
```

#### 3. Remaining Sources

Repeat for the remaining tables:

**Categories:**
```bash
seeknal draft source raw_categories --description "Product categories"
# Edit table path, then:
seeknal dry-run draft_source_raw_categories.yml && seeknal apply draft_source_raw_categories.yml
```

**Order Items:**
```bash
seeknal draft source raw_order_items --description "Order line items"
# Edit table path, then:
seeknal dry-run draft_source_raw_order_items.yml && seeknal apply draft_source_raw_order_items.yml
```

### Part 2: Create Customer Features

Now let's create feature groups for analytics.

#### Customer Lifetime Features

```bash
seeknal draft feature-group customer_lifetime_features --description "Customer lifetime value and behavior"
```

Edit `draft_feature_group_customer_lifetime_features.yml`:

```yaml
kind: feature_group
name: customer_lifetime_features
description: "Customer lifetime value and purchase behavior"
owner: "ml-team@example.com"
tags: ["customers", "analytics", "ml"]
entity:
  name: customer
  join_keys: ["customer_id"]
materialization:
  event_time_col: created_at
  offline:
    enabled: true
    format: parquet
  online:
    enabled: true
    ttl: 7d
inputs:
  - ref: source.raw_customers
  - ref: source.raw_orders
transform: |
  SELECT
    c.customer_id,
    c.email,
    c.city,
    c.state,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.order_total) as lifetime_value,
    AVG(o.order_total) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    DATEDIFF('day', MIN(o.order_date), MAX(o.order_date)) as customer_age_days
  FROM source.raw_customers c
  LEFT JOIN source.raw_orders o ON c.customer_id = o.customer_id
  GROUP BY c.customer_id, c.email, c.city, c.state
features:
  total_orders:
    description: "Total number of orders placed"
    dtype: int
  lifetime_value:
    description: "Total lifetime value of customer"
    dtype: float
  avg_order_value:
    description: "Average order value"
    dtype: float
  customer_age_days:
    description: "Days between first and last order"
    dtype: int
tests:
  - not_null: ["customer_id"]
  - unique: ["customer_id"]
```

Apply it:
```bash
seeknal dry-run draft_feature_group_customer_lifetime_features.yml
seeknal apply draft_feature_group_customer_lifetime_features.yml
```

#### Order Analytics Features

```bash
seeknal draft feature-group order_analytics --description "Order-level analytics features"
```

Edit `draft_feature_group_order_analytics.yml`:

```yaml
kind: feature_group
name: order_analytics
description: "Order-level analytics with product and customer info"
owner: "analytics-team@example.com"
tags: ["orders", "analytics"]
entity:
  name: order
  join_keys: ["order_id"]
materialization:
  event_time_col: order_date
  offline:
    enabled: true
    format: parquet
inputs:
  - ref: source.raw_orders
  - ref: source.raw_order_items
  - ref: source.raw_products
transform: |
  SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.order_total,
    SUM(oi.quantity) as total_items,
    COUNT(DISTINCT oi.product_id) as unique_products,
    AVG(p.price) as avg_product_price
  FROM source.raw_orders o
  LEFT JOIN source.raw_order_items oi ON o.order_id = oi.order_id
  LEFT JOIN source.raw_products p ON oi.product_id = p.product_id
  GROUP BY o.order_id, o.customer_id, o.order_date, o.order_status, o.order_total
features:
  total_items:
    description: "Total items in order"
    dtype: int
  unique_products:
    description: "Number of unique products"
    dtype: int
  avg_product_price:
    description: "Average product price in order"
    dtype: float
tests:
  - not_null: ["order_id"]
  - unique: ["order_id"]
```

Apply it:
```bash
seeknal dry-run draft_feature_group_order_analytics.yml
seeknal apply draft_feature_group_order_analytics.yml
```

### Part 3: Create Data Transformations

#### Monthly Sales Summary

```bash
seeknal draft transform monthly_sales_summary --description "Monthly sales aggregation"
```

Edit `draft_transform_monthly_sales_summary.yml`:

```yaml
kind: transform
name: monthly_sales_summary
description: "Monthly sales aggregated by state"
owner: "analytics-team@example.com"
tags: ["sales", "reporting"]
inputs:
  - ref: source.raw_orders
transform: |
  SELECT
    DATE_TRUNC('month', order_date)::DATE as report_month,
    state,
    COUNT(*) as total_orders,
    SUM(order_total) as total_revenue,
    AVG(order_total) as avg_order_value,
    SUM(shipping_cost) as total_shipping_cost
  FROM source.raw_orders
  WHERE order_status = 'delivered'
  GROUP BY DATE_TRUNC('month', order_date), state
  ORDER BY report_month DESC, total_revenue DESC
output:
  columns:
    report_month: "Reporting month"
    state: "Customer state"
    total_orders: "Number of orders"
    total_revenue: "Total revenue"
    avg_order_value: "Average order value"
    total_shipping_cost: "Total shipping cost"
```

Apply it:
```bash
seeknal dry-run draft_transform_monthly_sales_summary.yml
seeknal apply draft_transform_monthly_sales_summary.yml
```

### Part 4: Create ML Model Definition

```bash
seeknal draft model customer_churn_predictor --description "Predict customer churn"
```

Edit `draft_model_customer_churn_predictor.yml`:

```yaml
kind: model
name: customer_churn_predictor
description: "Predict customer churn based on purchase behavior"
owner: "ml-team@example.com"
tags: ["ml", "churn", "customers"]
output_columns:
  - churn_probability
  - churn_risk_tier
inputs:
  - ref: feature_group.customer_lifetime_features
aggregation:
  id_col: customer_id
  feature_date_col: last_order_date
  application_date_col: prediction_date
  features:
    - name: lifetime_value
      basic: [sum, mean, max]
      rolling:
        - window: [30, 90]
          aggs: [sum, count]
    - name: total_orders
      basic: [sum, count, max]
training:
  label_source: source.raw_customers
  label_column: status
  algorithm: xgboost
  params:
    n_estimators: 100
    max_depth: 6
    learning_rate: 0.1
tests:
  - min_accuracy: 0.8
  - min_auc: 0.85
```

Apply it:
```bash
seeknal dry-run draft_model_customer_churn_predictor.yml
seeknal apply draft_model_customer_churn_predictor.yml
```

---

## Workflow Summary

### Complete Pipeline Structure

After following this tutorial, your `seeknal/` directory will look like:

```
seeknal/
├── sources/
│   ├── raw_customers.yml
│   ├── raw_orders.yml
│   ├── raw_products.yml
│   ├── raw_categories.yml
│   └── raw_order_items.yml
├── transforms/
│   └── monthly_sales_summary.yml
├── feature_groups/
│   ├── customer_lifetime_features.yml
│   └── order_analytics.yml
└── models/
    └── customer_churn_predictor.yml
```

### DAG Visualization

```
raw_customers ──────┐
                     ├──> customer_lifetime_features ──> churn_predictor
raw_orders ─────────┤        └──> order_analytics
                     │
raw_products ────────┴───────> monthly_sales_summary
raw_order_items ─────┘
raw_categories ──────┘
```

---

## Reference: All Node Types

### Source
External data sources (databases, files, APIs)

### Transform
Reusable SQL transformations and aggregations

### Feature Group
Feature engineering for ML and analytics

### Model
ML model definitions with training configuration

### Aggregation
Reusable aggregation patterns for feature engineering

### Rule
Business rules and filters

### Exposure
Downstream consumers (dashboards, reports, APIs)

---

## Common Commands

```bash
# Create new node
seeknal draft <type> <name> --description "My description"

# Validate before applying
seeknal dry-run draft_<type>_<name>.yml

# Apply to production
seeknal apply draft_<type>_<name>.yml

# Apply with force (overwrite existing)
seeknal apply draft_<type>_<name>.yml --force

# Dry-run with options
seeknal dry-run draft_*.yml --limit 5 --timeout 60

# Schema validation only
seeknal dry-run draft_*.yml --schema-only

# List all nodes
seeknal list feature-groups

# View manifest
seeknal parse

# Get help
seeknal draft --help
seeknal dry-run --help
seeknal apply --help
```

---

## Troubleshooting

### "File not found" error
- **Cause:** Draft file doesn't exist
- **Solution:** Check filename and path, or create with `seeknal draft`

### "Invalid node type" error
- **Cause:** Wrong node type specified
- **Solution:** Use valid types: source, transform, feature-group, model, aggregation, rule, exposure

### "Node already exists" error
- **Solution 1:** Use `--force` flag: `seeknal apply draft_*.yml --force`
- **Solution 2:** Delete existing node first: `rm seeknal/<type>s/<name>.yml`

### YAML syntax error
- **Cause:** Malformed YAML (indentation, special characters)
- **Solution:** Check line number in error, fix indentation (use spaces, not tabs)

### Template rendering error
- **Cause:** Missing required field
- **Solution:** Edit template and add required fields

---

## Next Steps

1. **Explore Your Data:**
   ```bash
   seeknal repl
   seeknal> .connect /path/to/raw_customers.csv
   seeknal> SELECT * FROM db0 LIMIT 10
   ```

2. **Build More Features:**
   - Product performance features
   - Category analytics
   - Customer segmentation

3. **Materialize Features:**
   ```bash
   seeknal materialize customer_lifetime_features --start-date 2023-01-01
   ```

4. **Version Your Features:**
   ```bash
   seeknal version list customer_lifetime_features
   seeknal version diff customer_lifetime_features --from 1 --to 2
   ```

---

**Tutorial created:** 2026-01-25
**Dataset:** dbt_get_started (e-commerce)
**Seeknal version:** 2.0.0+
