# YAML Schema Reference

Complete reference for all YAML node types used in Seeknal pipelines.

## Overview

Seeknal uses YAML files to define data pipelines. Each YAML file represents a **node** in your pipeline's directed acyclic graph (DAG). Nodes are organized by **kind** (type), which determines what fields are valid.

### Node Kinds

| Kind | Purpose | Example Use Case |
|------|---------|------------------|
| `source` | Data ingestion from files or databases | Load CSV, Parquet, or database tables |
| `transform` | SQL-based data transformation | Clean, join, filter, aggregate data |
| `feature_group` | ML feature definitions with entity keys | User features for ML models |
| `aggregation` | Time-series aggregations | Rolling windows, historical metrics |
| `second_order_aggregation` | Aggregations of aggregations | Region-level from user-level metrics |
| `model` | ML model training and inference | Churn prediction, classification |
| `rule` | Data quality and validation rules | Non-null checks, value ranges |
| `exposure` | Data export to external systems | API endpoints, file exports, databases |
| `semantic_model` | Semantic layer definitions | Business metrics, dimensions |
| `metric` | Business metric calculations | Revenue, conversion rates |

---

## Common Fields

All node types share these base fields:

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `kind` | string | **Yes** | Node type (see table above) | - |
| `name` | string | **Yes** | Unique node identifier (used as table/view name) | - |
| `description` | string | No | Human-readable description | `""` |
| `owner` | string | No | Team or person responsible | `""` |
| `tags` | list[string] | No | Organizational tags for filtering/grouping | `[]` |
| `inputs` | list[object] | Depends | Dependencies (see [Inputs](#inputs)) | `[]` |

### Inputs

The `inputs` field specifies dependencies on other nodes. Each input is an object with a `ref` field:

```yaml
inputs:
  - ref: source.raw_users
  - ref: transform.clean_orders
```

**Reference format:** `<kind>.<name>`
- `source.customers` - References the source named "customers"
- `transform.clean_users` - References the transform named "clean_users"
- `feature_group.user_features` - References the feature group named "user_features"

---

## Source

Sources define data ingestion from files or databases.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `source` | string | **Yes** | Source type: `csv`, `parquet`, `json`, `postgresql`, `hive`, etc. | - |
| `table` | string | Depends | File path or table name (depends on `source` type) | - |
| `params` | object | No | Source-specific parameters (see [Source Params](#source-params)) | `{}` |
| `schema` | list[object] | No | Column definitions (see [Schema](#schema)) | `[]` |
| `columns` | object | No | Column descriptions (key=name, value=description or object) | `{}` |
| `freshness` | object | No | Data freshness checks (see [Freshness](#freshness)) | - |

### Source Params

Parameters vary by source type:

**CSV/Parquet:**
```yaml
params:
  delimiter: ","        # CSV delimiter
  header: true          # Has header row
  path: data/file.csv   # Alternative to 'table'
```

**Database:**
```yaml
params:
  dateCol: "date_id"    # Date column for partitioning
  idCol: "id"           # ID column
```

### Schema

Define columns as a list of objects:

```yaml
schema:
  - name: user_id
    data_type: integer
  - name: email
    data_type: string
  - name: created_at
    data_type: datetime
```

**Supported data types:** `integer`, `float`, `string`, `boolean`, `date`, `datetime`, `timestamp`

### Columns (Alternative Format)

Define columns as key-value pairs:

```yaml
columns:
  user_id:
    description: "Unique user identifier"
    dtype: string
  email:
    description: "User's email address"
    dtype: string
```

Or as simple descriptions:

```yaml
columns:
  user_id: "Unique user identifier"
  email: "User's email address"
```

### Freshness

Define data freshness checks:

```yaml
freshness:
  warn_after: 24h    # Warn if data older than 24 hours
  error_after: 48h   # Error if data older than 48 hours
```

### Example

```yaml
kind: source
name: users
description: "Customer master data"
owner: "data-team"
source: csv
table: "data/users.csv"
params:
  delimiter: ","
  header: true
schema:
  - name: user_id
    data_type: integer
  - name: email
    data_type: string
  - name: country
    data_type: string
  - name: signup_date
    data_type: date
freshness:
  warn_after: 24h
  error_after: 48h
tags:
  - raw-data
  - pii
```

---

## Transform

Transforms define SQL-based data transformations.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `transform` | string | **Yes** | SQL query or transformation logic (multi-statement supported) | - |
| `inputs` | list[object] | **Yes** | Dependencies (upstream nodes) | - |
| `materialization` | object | No | Iceberg materialization config (see [Materialization](#materialization)) | - |

### Transform SQL

The `transform` field contains SQL (DuckDB dialect):

```yaml
transform: |
  SELECT
    user_id,
    TRIM(name) as name,
    LOWER(email) as email,
    created_at
  FROM source.users
  WHERE email IS NOT NULL
```

**SQL Features:**
- Full DuckDB SQL syntax
- Multi-statement support (separated by `;`)
- CTEs (Common Table Expressions)
- Subqueries
- Window functions
- Reference upstream nodes: `source.name`, `transform.name`
- Use `__THIS__` placeholder to reference the first input

### Example

```yaml
kind: transform
name: clean_users
description: "Clean and standardize user data"
owner: "data-team"
transform: |
  SELECT
    user_id,
    TRIM(name) as name,
    LOWER(email) as email,
    UPPER(country) as country,
    created_at
  FROM source.users
  WHERE email IS NOT NULL
    AND email LIKE '%@%'
inputs:
  - ref: source.users
materialization:
  enabled: true
  table: "warehouse.curated.clean_users"
  mode: overwrite
tags:
  - data-cleaning
  - transformation
```

---

## Feature Group

Feature groups define ML feature sets with entity keys for serving.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `entity` | string or object | **Yes** | Entity identifier (see [Entity](#entity)) | - |
| `features` | object | **Yes** | Feature definitions (see [Features](#features)) | - |
| `materialization` | object | **Yes** | Feature store config (see [Feature Materialization](#feature-materialization)) | - |
| `transform` | string | No | SQL to compute features (alternative to upstream transform) | - |
| `inputs` | list[object] | **Yes** | Dependencies (upstream transforms) | - |
| `tests` | list[object] | No | Data quality tests (see [Tests](#tests)) | `[]` |

### Entity

Define the entity (join keys) for features:

**Simple format:**
```yaml
entity: user
```

**Object format (recommended):**
```yaml
entity:
  name: user              # Entity identifier
  join_keys: [user_id]    # Columns for joining features
```

### Features

Define features as key-value pairs:

```yaml
features:
  transaction_count:
    description: "Total number of transactions"
    dtype: int
  total_amount:
    description: "Sum of all transaction amounts"
    dtype: float
  avg_transaction_value:
    description: "Average transaction amount"
    dtype: float
```

**Supported dtypes:** `int`, `integer`, `float`, `double`, `string`, `str`, `bool`, `boolean`, `date`, `datetime`, `timestamp`

### Feature Materialization

Configure offline and online feature stores:

```yaml
materialization:
  event_time_col: latest_order_date    # Required: column for point-in-time joins
  offline:
    enabled: true                       # Enable offline (batch) store
    format: parquet                     # Storage format
    partition_by: [country]             # Partition columns (optional)
  online:
    enabled: false                      # Enable online (real-time) store
    ttl: 7d                            # Time-to-live for online features
```

### Tests

Define data quality tests:

```yaml
tests:
  - not_null: [user_id, transaction_count]
  - unique: [user_id]
  - accepted_values:
      column: country
      values: [US, UK, CA, DE]
```

### Example

```yaml
kind: feature_group
name: user_features
description: "User-level features for ML"
owner: "ml-team"
entity:
  name: user
  join_keys: [user_id]
materialization:
  event_time_col: latest_order_date
  offline:
    enabled: true
    format: parquet
    partition_by: [country]
  online:
    enabled: true
    ttl: 7d
features:
  transaction_count:
    description: "Total number of transactions"
    dtype: int
  total_amount:
    description: "Sum of all transaction amounts"
    dtype: float
  avg_transaction_value:
    description: "Average transaction amount"
    dtype: float
  favorite_merchant:
    description: "Most frequently visited merchant"
    dtype: string
  days_since_last_transaction:
    description: "Days since last transaction"
    dtype: int
transform: |
  SELECT
    user_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_transaction_value,
    MODE() WITHIN GROUP (ORDER BY merchant) as favorite_merchant,
    DATEDIFF('day', CURRENT_DATE, MAX(timestamp)) as days_since_last_transaction
  FROM transform.user_transactions
  GROUP BY user_id
inputs:
  - ref: transform.user_transactions
tests:
  - not_null: [user_id]
  - unique: [user_id]
tags:
  - features
  - ml
```

---

## Aggregation

Aggregations define time-series aggregations with rolling windows.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `id_col` | string | **Yes** | Entity ID column for grouping | - |
| `feature_date_col` | string | **Yes** | Date column for features | - |
| `application_date_col` | string | **Yes** | Application/prediction date column | - |
| `features` | list[object] | **Yes** | Feature specifications (see [Aggregation Features](#aggregation-features)) | - |
| `group_by` | list[string] | No | Additional grouping columns | `[]` |
| `inputs` | list[object] | **Yes** | Dependencies (upstream nodes) | - |

### Aggregation Features

Features are defined as a **list** of objects:

```yaml
features:
  - name: transaction_metrics
    basic: [count, sum, avg]
    column: amount
    rolling:
      - window: 7d
        functions: [sum, avg, count]
      - window: 30d
        functions: [sum, avg, count]
      - window: 90d
        functions: [sum, avg, count]
```

**Fields:**
- `name`: Feature name prefix
- `basic`: Basic aggregations to compute
- `column`: Column to aggregate
- `rolling`: Time windows (optional)

**Basic aggregations:** `count`, `sum`, `avg`, `mean`, `min`, `max`, `stddev`

**Rolling window format:**
```yaml
rolling:
  - window: 7d      # 7 days
    functions: [sum, avg]
  - window: 30d     # 30 days
    functions: [sum, avg, count]
```

### Example

```yaml
kind: aggregation
name: user_transaction_history
description: "Time-series aggregation of user transactions"
owner: "ml-team"
id_col: user_id
feature_date_col: timestamp
application_date_col: application_date
group_by:
  - country
features:
  - name: transaction_metrics
    basic: [count, sum, avg]
    column: amount
    rolling:
      - window: 7d
        functions: [sum, avg, count]
      - window: 30d
        functions: [sum, avg, count]
      - window: 90d
        functions: [sum, avg, count]
  - name: merchant_frequency
    basic: [count]
    column: merchant
    rolling:
      - window: 7d
        functions: [count]
      - window: 30d
        functions: [count]
inputs:
  - ref: source.transactions
tags:
  - aggregation
  - time-series
```

---

## Second-Order Aggregation

Second-order aggregations enable aggregations of aggregations (e.g., region-level from user-level metrics).

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `id_col` | string | **Yes** | Entity ID for second-level grouping | - |
| `feature_date_col` | string | **Yes** | Date column for features | - |
| `application_date_col` | string | No | Application/prediction date column | - |
| `source` | string | **Yes** | Upstream aggregation reference (e.g., `aggregation.user_daily_features`) | - |
| `features` | object | **Yes** | Feature specifications (see [Second-Order Features](#second-order-features)) | - |
| `inputs` | list[object] | **Yes** | Dependencies (must match `source`) | - |

### Second-Order Features

Features are defined as a **dictionary** (unlike first-order aggregations):

```yaml
features:
  # Count users per region
  total_users:
    basic: [count]
    source_feature: customer_id

  # Average spending across users in region
  avg_user_spend:
    basic: [mean, stddev]
    source_feature: spend_metrics_sum

  # Weekly total volume (7-day windows)
  weekly_total_volume:
    window: [7, 7]
    basic: [sum]
    source_feature: daily_volume

  # Ratio of recent to historical spending
  recent_vs_historical_ratio:
    ratio:
      numerator: [1, 7]       # Days 1-7
      denominator: [8, 30]    # Days 8-30
      aggs: [sum]
    source_feature: transaction_amount
```

**Aggregation types:**

1. **Basic aggregations:**
```yaml
total_users:
  basic: [count, sum, mean, stddev, min, max]
  source_feature: upstream_feature_name
```

2. **Window aggregations:**
```yaml
weekly_total:
  window: [7, 7]    # [lower_bound, upper_bound] in days
  basic: [sum]
  source_feature: daily_amount
```

3. **Ratio aggregations:**
```yaml
recent_vs_historical:
  ratio:
    numerator: [1, 7]     # Recent window
    denominator: [8, 30]  # Historical window
    aggs: [sum]
  source_feature: metric_value
```

**Important:** Always specify `source_feature` - it references the upstream aggregation's output column.

### Example

```yaml
kind: second_order_aggregation
name: region_user_metrics
description: "Aggregate user-level features to region level"
owner: "data-science"
id_col: region_id
feature_date_col: date
application_date_col: application_date
source: aggregation.user_daily_features
features:
  # Count users per region (use customer_id to count rows)
  total_users:
    basic: [count]
    source_feature: customer_id

  # Average spending across users in region
  avg_user_spend:
    basic: [mean, stddev]
    source_feature: spend_metrics_sum

  # Maximum spending by users in region
  max_user_spend:
    basic: [max]
    source_feature: spend_metrics_sum

  # Total volume across users in region
  total_volume:
    basic: [sum]
    source_feature: volume_metrics_sum

  # Weekly total volume (aggregate 7-day windows into weekly blocks)
  weekly_total_volume:
    window: [7, 7]
    basic: [sum]
    source_feature: daily_volume

  # Ratio of recent to historical spending
  recent_vs_historical_ratio:
    ratio:
      numerator: [1, 7]
      denominator: [8, 30]
      aggs: [sum]
    source_feature: transaction_amount
inputs:
  - ref: aggregation.user_daily_features
tags:
  - second-order
  - feature-engineering
  - analytics
```

---

## Model

Models define ML model training and inference configurations.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `training` | object | **Yes** | Training configuration (see [Training](#training)) | - |
| `output_columns` | list[string] | **Yes** | Output column names from model | - |
| `transform` | string | No | SQL to prepare training data | - |
| `inputs` | list[object] | **Yes** | Dependencies (feature groups, transforms) | - |

### Training

Configure model training:

```yaml
training:
  algorithm: RandomForestClassifier
  params:
    n_estimators: 100
    max_depth: 10
    random_state: 42
  test_size: 0.2
```

**Common algorithms:**
- `RandomForestClassifier`
- `LogisticRegression`
- `XGBoostClassifier`
- `LightGBM`
- `LinearRegression`

### Example

```yaml
kind: model
name: churn_predictor
description: "Predict customer churn probability"
owner: "ml-team"
training:
  algorithm: RandomForestClassifier
  params:
    n_estimators: 100
    max_depth: 10
    random_state: 42
  test_size: 0.2
output_columns:
  - user_id
  - churn_probability
  - churn_risk_level
transform: |
  SELECT
    user_id,
    transaction_count,
    total_amount,
    avg_transaction_value,
    days_since_last_transaction,
    CASE
      WHEN days_since_last_transaction > 90 THEN 1
      ELSE 0
    END as is_churned
  FROM feature_group.user_features
inputs:
  - ref: feature_group.user_features
tags:
  - ml
  - prediction
  - churn
```

---

## Rule

Rules define data quality and validation checks.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `rule` | object or string | **Yes** | Validation rule (see [Rule Definition](#rule-definition)) | - |
| `params` | object | No | Rule parameters (see [Rule Params](#rule-params)) | `{}` |
| `columns` | list[string] | No | Columns to validate | `[]` |

### Rule Definition

Define rules as an object:

```yaml
rule:
  value: "transaction_count >= 0 AND total_amount >= 0"
```

Or as a simple string:

```yaml
rule: "amount > 0"
```

### Rule Params

Configure rule behavior:

```yaml
params:
  severity: error          # error, warn, info
  error_message: "Negative values detected in transaction metrics"
```

### Example

```yaml
kind: rule
name: data_quality_rules
description: "Validate data quality in user features"
owner: "data-quality"
rule:
  value: "transaction_count >= 0 AND total_amount >= 0"
params:
  severity: error
  error_message: "Negative values detected in transaction metrics"
columns:
  - transaction_count
  - total_amount
tags:
  - validation
  - data-quality
```

---

## Exposure

Exposures define data exports to external systems (files, APIs, databases).

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `type` | string | **Yes** | Exposure type: `file`, `api`, `database` | - |
| `url` | string | Depends | API endpoint URL (for `type: api`) | - |
| `params` | object | Depends | Type-specific parameters (see [Exposure Params](#exposure-params)) | `{}` |
| `inputs` | list[object] | **Yes** | Dependencies (data to export) | - |

**Note:** Exposures use `inputs` (not `depends_on`) for consistency with other node types.

### Exposure Params

Parameters vary by exposure type:

**File export:**
```yaml
type: file
params:
  path: outputs/data.parquet
  format: parquet              # csv, parquet, json, jsonl
  compression: snappy          # snappy, gzip, zstd
  partition_by: [date, region] # Partition columns (optional)
```

**API export:**
```yaml
type: api
url: https://api.example.com/v1/features
params:
  method: POST
  headers:
    Authorization: "Bearer {API_TOKEN}"
    Content-Type: "application/json"
  refresh_interval: 3600       # Seconds between refreshes
```

**Database export:**
```yaml
type: database
params:
  connection_string: "postgresql://host/db"
  table: production.results
  mode: overwrite              # overwrite, append
```

### Example (File)

```yaml
kind: exposure
name: churn_predictions_export
description: "Export churn predictions to file"
owner: "ml-team"
type: file
params:
  path: outputs/churn_predictions.parquet
  format: parquet
  compression: snappy
  partition_by: [churn_risk_level]
inputs:
  - ref: model.churn_predictor
tags:
  - export
  - output
```

### Example (API)

```yaml
kind: exposure
name: user_features_api
description: "Expose user features via REST API"
owner: "ml-team"
type: api
url: https://api.example.com/v1/user-features
params:
  method: POST
  headers:
    Authorization: "Bearer {API_TOKEN}"
    Content-Type: "application/json"
  refresh_interval: 3600
inputs:
  - ref: feature_group.user_features
tags:
  - api
  - production
```

---

## Semantic Model

Semantic models define business entities, dimensions, and measures for analytics.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `model` | string | **Yes** | Reference to underlying data model (e.g., `ref('transform.orders')`) | - |
| `default_time_dimension` | string | No | Default time dimension for queries | - |
| `entities` | list[object] | **Yes** | Entity definitions (see [Entities](#entities)) | - |
| `dimensions` | list[object] | No | Dimension definitions (see [Dimensions](#dimensions)) | `[]` |
| `measures` | list[object] | **Yes** | Measure definitions (see [Measures](#measures)) | - |

### Entities

Define entities for semantic modeling:

```yaml
entities:
  - name: order_id
    type: primary
  - name: customer_id
    type: foreign
```

**Entity types:** `primary`, `foreign`

### Dimensions

Define dimensions for slicing and filtering:

```yaml
dimensions:
  - name: ordered_at
    type: time
    expr: ordered_at
    time_granularity: day
  - name: region
    type: categorical
    expr: region
```

**Dimension types:**
- `time`: Time-based dimension
- `categorical`: Categorical dimension (default)

**Time granularities:** `day`, `week`, `month`, `quarter`, `year`

### Measures

Define measures for aggregation:

```yaml
measures:
  - name: revenue
    expr: amount
    agg: sum
  - name: order_count
    expr: 1
    agg: sum
  - name: unique_customers
    expr: customer_id
    agg: count_distinct
```

**Aggregation types:** `sum`, `avg`, `average`, `count`, `count_distinct`, `min`, `max`

### Example

```yaml
kind: semantic_model
name: orders
description: "Order fact table semantic model"
owner: "analytics"
model: "ref('transform.order_summary')"
default_time_dimension: ordered_at
entities:
  - name: order_id
    type: primary
  - name: customer_id
    type: foreign
dimensions:
  - name: ordered_at
    type: time
    expr: ordered_at
    time_granularity: day
  - name: region
    type: categorical
    expr: region
  - name: product_category
    type: categorical
    expr: product_category
measures:
  - name: revenue
    expr: amount
    agg: sum
  - name: order_count
    expr: 1
    agg: sum
  - name: unique_customers
    expr: customer_id
    agg: count_distinct
  - name: avg_order_value
    expr: amount
    agg: avg
tags:
  - semantic
  - analytics
```

---

## Metric

Metrics define business metric calculations.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `type` | string | **Yes** | Metric type: `simple`, `ratio`, `derived` | - |
| `type_params` | object | **Yes** | Type-specific parameters (see [Metric Types](#metric-types)) | - |
| `filter` | string | No | SQL filter expression | - |

### Metric Types

**Simple metric:**
```yaml
type: simple
type_params:
  measure: revenue
```

**Ratio metric:**
```yaml
type: ratio
type_params:
  numerator: active_users
  denominator: total_users
```

**Derived metric:**
```yaml
type: derived
type_params:
  expr: "metric('revenue') / metric('order_count')"
```

### Example

```yaml
kind: metric
name: total_revenue
description: "Total revenue across all orders"
owner: "finance"
type: simple
type_params:
  measure: revenue
filter: "status = 'completed'"
tags:
  - metric
  - revenue
```

---

## Materialization

Materialization enables persisting transform outputs as Apache Iceberg tables.

### Fields

| Field | Type | Required | Description | Default |
|-------|------|----------|-------------|---------|
| `enabled` | boolean | **Yes** | Enable/disable materialization | - |
| `table` | string | **Yes** | Fully qualified Iceberg table name (e.g., `warehouse.prod.table`) | - |
| `mode` | string | **Yes** | Write mode: `append` or `overwrite` | - |
| `create_table` | boolean | No | Auto-create table if it doesn't exist | `true` |

### Modes

| Mode | Behavior | SQL Equivalent | Use Case |
|------|----------|----------------|----------|
| `overwrite` | Drop and recreate table | `DROP TABLE` + `CREATE TABLE` | Full refresh, daily snapshots |
| `append` | Insert into existing table | `INSERT INTO` | Incremental updates, time-series |

### Example

**In transform:**
```yaml
kind: transform
name: customer_orders
transform: |
  SELECT * FROM source.orders
inputs:
  - ref: source.orders
materialization:
  enabled: true
  table: "warehouse.curated.customer_orders"
  mode: overwrite
  create_table: true
```

**Python transform with materialization config:**

Create a separate YAML config file:

```yaml
# transforms/sales_forecast.yml
name: sales_forecast
kind: transform
file: pipelines/sales_forecast.py
description: "ML-based sales forecast with Iceberg materialization"
materialization:
  enabled: true
  table: "warehouse.prod.sales_forecast"
  mode: overwrite
  create_table: true
```

---

## CLI Flags

Override YAML configurations from the command line:

### Execution Flags

```bash
# Show execution plan without running
seeknal run --show-plan

# Run all nodes (ignore state)
seeknal run --full

# Run specific nodes and downstream dependents
seeknal run --nodes customers,orders

# Filter by node type
seeknal run --types source,transform

# Dry run (validate without executing)
seeknal run --dry-run

# Continue on error
seeknal run --continue-on-error

# Retry failed nodes
seeknal run --retry 3
```

### Materialization Flags

```bash
# Force enable materialization (override YAML)
seeknal run --materialize

# Force disable materialization (override YAML)
seeknal run --no-materialize

# Use YAML configuration (default)
seeknal run
```

---

## Best Practices

### File Organization

```
my-project/
├── seeknal/
│   ├── sources/           # Data sources
│   │   ├── customers.yml
│   │   └── orders.yml
│   ├── transforms/        # SQL transformations
│   │   ├── clean_users.yml
│   │   └── enriched_orders.yml
│   ├── feature_groups/    # ML features
│   │   └── user_features.yml
│   ├── aggregations/      # Time-series aggregations
│   │   └── daily_metrics.yml
│   ├── models/            # ML models
│   │   └── churn_model.yml
│   ├── rules/             # Data quality rules
│   │   └── validation.yml
│   └── exposures/         # Exports
│       └── api_export.yml
└── data/                  # Source data files
    ├── customers.csv
    └── orders.csv
```

### Naming Conventions

- **Names:** Use lowercase with underscores (`user_features`, `clean_orders`)
- **Descriptions:** Start with capital letter, no period at end
- **Tags:** Use lowercase, hyphenated (`ml-features`, `data-quality`)

### Security

- Never hardcode credentials in YAML files
- Use environment variables: `{API_TOKEN}`, `{DB_PASSWORD}`
- Store sensitive credentials in `.env` files (auto-loaded by Seeknal)

### Version Control

**DO commit:**
- All YAML files in `seeknal/`
- `target/manifest.json` (execution plan)

**DON'T commit:**
- `target/run_state.json` (runtime state)
- `.env` (sensitive credentials)
- Data files (use `.gitignore`)

---

## Further Reading

- [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md) - Step-by-step guide
- [Mixed YAML and Python Pipelines](../tutorials/mixed-yaml-python-pipelines.md) - Combine YAML and Python
- [Iceberg Materialization Guide](../iceberg-materialization.md) - Persistent storage with Iceberg
- [Getting Started](../getting-started-comprehensive.md) - Comprehensive introduction

---

**Last Updated:** February 2026
