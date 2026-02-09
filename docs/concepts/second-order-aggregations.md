# Second-Order Aggregations

## What Are Second-Order Aggregations?

Second-order aggregations are **aggregations of already-aggregated data**, enabling multi-level feature engineering patterns in your data pipelines. While first-order aggregations compute statistics per entity (like user-level metrics from raw transactions), second-order aggregations aggregate those statistics to a higher level (like region-level averages from user-level metrics).

This powerful pattern enables hierarchical rollups, cross-time window analytics, comparative metrics, and distribution features that would be complex or inefficient to compute directly from raw data. For example, you might first aggregate transactions to compute each user's 30-day spending, then aggregate those user-level metrics to compute the average and standard deviation of spending across all users in a region.

## Visual Concept

```
Raw Transaction Data
┌─────────────────────────────────────────┐
│ user_id │ date       │ amount │ region │
├─────────────────────────────────────────┤
│ 1       │ 2024-01-10 │ 100.00 │ US     │
│ 1       │ 2024-01-12 │  75.00 │ US     │
│ 2       │ 2024-01-11 │ 250.50 │ UK     │
│ 3       │ 2024-01-13 │ 500.00 │ US     │
└─────────────────────────────────────────┘
                    ↓
        First-Order Aggregation
        (User-level metrics)
┌──────────────────────────────────────────┐
│ user_id │ region │ total_spend_30d │ ... │
├──────────────────────────────────────────┤
│ 1       │ US     │ 175.00          │     │
│ 2       │ UK     │ 250.50          │     │
│ 3       │ US     │ 500.00          │     │
└──────────────────────────────────────────┘
                    ↓
        Second-Order Aggregation
        (Region-level metrics)
┌───────────────────────────────────────────────┐
│ region │ avg_user_spend │ stddev │ user_count │
├───────────────────────────────────────────────┤
│ US     │ 337.50         │ 162.63 │ 2          │
│ UK     │ 250.50         │ 0.00   │ 1          │
└───────────────────────────────────────────────┘
```

## Use Cases

### 1. Hierarchical Rollups
Aggregate metrics across organizational or geographic hierarchies:
- **User → Store → Region → Country**: Compute regional performance metrics from user-level features
- **Product → Category → Department**: Roll up product features to higher-level groupings
- **Transaction → Customer → Merchant → Industry**: Multi-level business intelligence

### 2. Cross-Time Windows
Aggregate daily metrics into longer time periods:
- **Daily → Weekly → Monthly**: Aggregate daily transaction volumes to weekly patterns
- **Hourly → Daily → Weekly**: Roll up time-series data to different granularities
- **Event-level → Session-level → User-level**: Progressive aggregation of behavioral data

### 3. Comparative Analytics
Compare metrics across different time windows:
- **Recent vs Historical**: Ratio of last 7 days to previous 30 days
- **Month-over-Month**: Current month performance vs previous month
- **Seasonal Patterns**: Compare current period to same period last year

### 4. Distribution Features
Calculate statistical distributions across entities:
- **Standard Deviation**: Variability of user spending within regions
- **Percentiles**: P95 response time across all services
- **Min/Max/Range**: Distribution bounds for anomaly detection

## YAML Definition

Here's a complete YAML example showing all major aggregation types:

```yaml
kind: second_order_aggregation
name: region_user_metrics
description: "Aggregate user-level features to region level"
owner: "analytics-team"

# Entity ID for second-level grouping
id_col: region_id

# Date columns for time-based operations
feature_date_col: order_date
application_date_col: prediction_date  # Optional: for window calculations

# Reference to upstream first-order aggregation
source: aggregation.user_daily_features

# Feature specifications (dictionary format)
features:
  # Basic aggregation - count users per region
  total_users:
    basic: [count]
    source_feature: customer_id

  # Basic aggregation - mean and stddev
  avg_user_spend:
    basic: [mean, stddev]
    source_feature: spend_metrics_sum

  # Window aggregation - aggregate 7-day windows
  weekly_total:
    window: [7, 7]  # [lower_bound, upper_bound] in days
    basic: [sum]
    source_feature: daily_volume

  # Ratio aggregation - compare two time windows
  recent_vs_historical:
    ratio:
      numerator: [1, 7]    # Days 1-7
      denominator: [8, 30]  # Days 8-30
      aggs: [sum]
    source_feature: transaction_amount

  # Since (conditional) aggregation - count if condition is met
  high_value_users:
    since:
      condition: "total_spend > 1000"
      aggs: [count]
    source_feature: customer_id

inputs:
  - ref: aggregation.user_daily_features

tags:
  - second-order
  - feature-engineering
```

### Important YAML Notes

- **`source_feature`**: Always specify which upstream column to aggregate. First-order aggregations produce features with names like `spend_metrics_sum`, `spend_metrics_count` (feature name + aggregation function).
- **`source`**: Reference format is `kind.name` (e.g., `aggregation.user_daily_features`).
- **`id_col`**: Must exist in the upstream aggregation's output (usually included via `group_by` in first-order aggregation).
- **`application_date_col`**: If specified, this column must exist in the source aggregation output.
- **Dictionary format**: Second-order aggregations use a dictionary format for features (unlike first-order aggregations which use a list format).

## Python Definition

The `@second_order_aggregation` decorator enables Python-based definitions:

```python
from seeknal.pipeline.decorators import second_order_aggregation
from seeknal.pipeline.materialization_config import MaterializationConfig
import pandas as pd

@second_order_aggregation(
    name="region_metrics",
    source="aggregation.user_metrics",
    id_col="region_id",
    feature_date_col="date",
    application_date_col="prediction_date",  # Optional
    description="Aggregate user features to region level",
    owner="analytics-team",
    tags=["second-order", "regional-analytics"],
    materialization=MaterializationConfig(
        enabled=True,
        table="warehouse.analytics.region_metrics",
        mode="append"
    )
)
def region_metrics(ctx, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate user-level metrics to region level.

    Args:
        ctx: Pipeline context with access to other nodes
        df: Pre-aggregated user metrics DataFrame

    Returns:
        Region-level aggregated DataFrame
    """
    return df.groupby("region_id").agg({
        "total_spend_30d": ["mean", "std", "median"],
        "transaction_count": ["sum", "count"],
        "last_active_date": "max"
    }).reset_index()
```

### Python Decorator Parameters

| Parameter | Type | Description | Required |
|-----------|------|-------------|----------|
| `name` | str | Node name | Yes |
| `source` | str | Upstream aggregation reference (e.g., "aggregation.user_metrics") | Yes |
| `id_col` | str | Entity ID column for grouping | Yes |
| `feature_date_col` | str | Date column for features | Yes |
| `application_date_col` | str | Reference date for window calculations | No |
| `description` | str | Human-readable description | No |
| `owner` | str | Team/person responsible | No |
| `tags` | list[str] | Organizational tags | No |
| `materialization` | MaterializationConfig | Iceberg table config | No |

## Aggregation Types

### Basic

Simple aggregation across entities. Supported aggregation functions:
- `sum` - Sum of values
- `avg`, `mean` - Average value
- `min` - Minimum value
- `max` - Maximum value
- `count` - Count of non-null values
- `stddev`, `std` - Standard deviation

**Example:**
```yaml
avg_user_spend:
  basic: [mean, stddev]
  source_feature: total_spend_30d
```

**Generated columns:**
- `total_spend_30d_MEAN`
- `total_spend_30d_STDDEV`

### Window (basic_days)

Time-window cross-aggregation with `[lower, upper]` day range relative to the application date.

**Example:**
```yaml
weekly_total:
  window: [7, 7]  # Aggregate 7-day windows
  basic: [sum]
  source_feature: daily_volume
```

**Generated columns:**
- `daily_volume_SUM_7_7`

**SQL logic:**
```sql
SUM(daily_volume) FILTER (WHERE days_between BETWEEN 7 AND 7)
```

Where `days_between = date_diff('day', feature_date_col, application_date_col)`

### Ratio

Compare two time windows by computing numerator/denominator ratios.

**Example:**
```yaml
recent_vs_historical:
  ratio:
    numerator: [1, 7]    # Recent: days 1-7
    denominator: [8, 30]  # Historical: days 8-30
    aggs: [sum]
  source_feature: transaction_amount
```

**Generated columns:**
- `transaction_amount_SUM1_7_SUM8_30`

**SQL logic:**
```sql
CAST(SUM(transaction_amount) FILTER (WHERE days_between BETWEEN 1 AND 7) AS DOUBLE)
/ NULLIF(CAST(SUM(transaction_amount) FILTER (WHERE days_between BETWEEN 8 AND 30) AS DOUBLE), 0)
```

### Since (Conditional)

Aggregation filtered by a custom condition, enabling count_if and sum_if patterns.

**Example:**
```yaml
high_value_users:
  since:
    condition: "total_spend > 1000"
    aggs: [count]
  source_feature: customer_id
```

**Generated columns:**
- `SINCE_COUNT_customer_id_GEO_D`

**SQL logic:**
```sql
COUNT(customer_id) FILTER (WHERE total_spend > 1000)
```

**Note:** The condition syntax uses `==` which is automatically converted to `=` for SQL compatibility.

## Feature Naming Convention

Second-order aggregations auto-generate feature names following these patterns:

### Basic Aggregations
**Pattern:** `{source_feature}_{AGG}`

**Examples:**
- `amount_SUM` - Sum of amount
- `amount_AVG` - Average of amount
- `amount_STDDEV` - Standard deviation of amount

### Window Aggregations (basic_days)
**Pattern:** `{source_feature}_{AGG}_{lower}_{upper}`

**Examples:**
- `amount_SUM_1_30` - Sum of amount for days 1-30
- `volume_MEAN_7_7` - Mean of volume for 7-day window

### Ratio Aggregations
**Pattern:** `{source_feature}_{AGG}{lower1}_{upper1}_{AGG}{lower2}_{upper2}`

**Examples:**
- `amount_SUM1_7_SUM30_90` - Ratio of sum(1-7 days) / sum(30-90 days)
- `volume_AVG1_7_AVG8_30` - Ratio of avg(1-7 days) / avg(8-30 days)

### Since (Conditional) Aggregations
**Pattern:** `SINCE_{AGG}_{source_feature}_GEO_D`

**Examples:**
- `SINCE_COUNT_flag_GEO_D` - Count where flag condition is true
- `SINCE_SUM_amount_GEO_D` - Sum where condition is met

## How It Works

The execution flow for second-order aggregations:

```
1. Parse YAML
   └─> Validate schema and required fields
   └─> Check source reference format (kind.name)

2. Resolve Source
   └─> Look up upstream aggregation node
   └─> Load aggregation output (from view or parquet)

3. Validate Source Schema
   └─> Verify id_col exists in source
   └─> Verify feature_date_col exists
   └─> Verify application_date_col exists (if specified)
   └─> Check that source_features are available

4. Build AggregationSpec List
   └─> Convert YAML features to AggregationSpec objects
   └─> Handle basic, window, ratio, and since types
   └─> Map aggregation functions (mean→avg, std→stddev)

5. Execute with SecondOrderAggregator
   └─> DuckDB: Use SecondOrderAggregator from tasks/duckdb/
   └─> Spark: Use SparkSecondOrderAggregator from tasks/sparkengine/
   └─> Generate SQL with CTEs for days_between calculation
   └─> Execute aggregation query with GROUP BY id_col

6. Save Results
   └─> Save to parquet: target/{kind}/{name}/result.parquet
   └─> Register view: {kind}.{name}
   └─> Return ExecutorResult with row counts and timing
```

### Key Implementation Details

1. **Days Between Calculation**: A CTE calculates `days_between = date_diff('day', feature_date_col, application_date_col)` once, then all window/ratio filters reference this column.

2. **FILTER Clauses**: DuckDB's `FILTER (WHERE condition)` syntax enables efficient conditional aggregations without CTEs or subqueries.

3. **Null Handling**: Ratio aggregations use `NULLIF` to prevent division by zero, returning `NULL` for undefined ratios.

4. **Source Resolution**: The executor tries multiple paths to find source data:
   - View: `aggregation.{name}`
   - Parquet: `target/aggregation/{name}/result.parquet`
   - Transform intermediate storage

## Relationship to First-Order Aggregations

| Aspect | First-Order (aggregation) | Second-Order |
|--------|--------------------------|--------------|
| **Feature format** | List of items with name, basic, column | Dictionary with feature keys |
| **Source** | Source or Transform node | Aggregation node only |
| **Entity** | Same entity as raw data | Higher-level entity |
| **id_col** | Groups by entity (e.g., user_id) | Groups by parent entity (e.g., region_id) |
| **Output columns** | `{name}_{agg}` (e.g., spend_SUM) | Same naming pattern |
| **Use case** | Raw data → entity metrics | Entity metrics → rollup metrics |
| **Example** | Transactions → user features | User features → region features |

### First-Order Example

```yaml
kind: aggregation
name: user_daily_features
id_col: customer_id
feature_date_col: order_date
group_by: [country]  # Important: needed for second-order
features:
  - name: spend_metrics
    basic: [sum, count]
    column: amount
```

**Output columns:** `customer_id`, `country`, `order_date`, `spend_metrics_sum`, `spend_metrics_count`

### Second-Order Example

```yaml
kind: second_order_aggregation
name: region_metrics
id_col: country  # Must exist in first-order output
source: aggregation.user_daily_features
features:
  avg_user_spend:
    basic: [mean]
    source_feature: spend_metrics_sum  # References first-order output
```

**Output columns:** `country`, `spend_metrics_sum_MEAN`

## CLI Commands

### Generate Templates

```bash
# Generate YAML template
seeknal draft second-order-aggregation region_metrics

# Generate Python template
seeknal draft second-order-aggregation region_metrics --python
```

### Execute Pipelines

```bash
# Run entire pipeline (includes second-order aggregations)
seeknal run

# Run specific second-order aggregation node
seeknal run --nodes second_order_aggregation.region_metrics

# Show execution plan
seeknal run --show-plan

# Dry-run (validate without executing)
seeknal run --dry-run
```

### Inspect Results

```bash
# View manifest (shows DAG structure)
seeknal plan
cat target/manifest.json | python -m json.tool

# View execution state
cat target/run_state.json | python -m json.tool

# Check output files
ls -lh target/second_order_aggregation/region_metrics/
```

## Common Pitfalls

### 1. Missing Grouping Column in Source

**Problem:** Second-order aggregation references a column that doesn't exist in the source aggregation.

**Example:**
```yaml
# First-order aggregation (WRONG - missing group_by)
kind: aggregation
name: user_metrics
id_col: user_id
features:
  - name: spend
    basic: [sum]
    column: amount

# Second-order aggregation (FAILS - country not in output)
kind: second_order_aggregation
name: region_metrics
id_col: country  # ERROR: country doesn't exist
source: aggregation.user_metrics
```

**Solution:** Add `group_by` to first-order aggregation:
```yaml
kind: aggregation
name: user_metrics
id_col: user_id
group_by: [country]  # Now country is included in output
features:
  - name: spend
    basic: [sum]
    column: amount
```

### 2. Using List Format Instead of Dictionary

**Problem:** Second-order aggregations require dictionary format for features.

**Example (WRONG):**
```yaml
features:
  - name: total_users  # List format - WRONG
    basic: [count]
```

**Example (CORRECT):**
```yaml
features:
  total_users:  # Dictionary format - CORRECT
    basic: [count]
    source_feature: customer_id
```

### 3. Missing source_feature

**Problem:** Not specifying which upstream column to aggregate.

**Example (WRONG):**
```yaml
features:
  avg_spend:
    basic: [mean]  # Missing source_feature - WRONG
```

**Example (CORRECT):**
```yaml
features:
  avg_spend:
    basic: [mean]
    source_feature: spend_metrics_sum  # Explicit source - CORRECT
```

### 4. Missing application_date_col in Source

**Problem:** Using time-based windows without the reference date column.

**Example (WRONG):**
```yaml
# First-order aggregation (missing application_date_col)
kind: aggregation
name: user_metrics
id_col: user_id
feature_date_col: order_date
# application_date_col: NOT SET

# Second-order aggregation (FAILS - needs application_date_col)
kind: second_order_aggregation
name: region_metrics
id_col: country
feature_date_col: order_date
application_date_col: prediction_date  # ERROR: not in source
```

**Solution:** Include application_date_col in first-order aggregation:
```yaml
kind: aggregation
name: user_metrics
id_col: user_id
feature_date_col: order_date
application_date_col: prediction_date  # Now included
group_by: [country, prediction_date]  # Add to group_by
```

### 5. Wrong Source Reference Format

**Problem:** Using incorrect format for source reference.

**Example (WRONG):**
```yaml
source: user_daily_features  # Missing kind prefix - WRONG
```

**Example (CORRECT):**
```yaml
source: aggregation.user_daily_features  # kind.name format - CORRECT
```

## See Also

- [Glossary: Second-Order Aggregation](glossary.md#second-order-aggregation)
- [YAML Schema Reference](../reference/yaml-schema.md#second-order-aggregation)
- [YAML Pipeline Tutorial: Section 8.8](../tutorials/yaml-pipeline-tutorial.md#88-second-order-aggregations)
- [Python Pipelines Guide](../guides/python-pipelines.md)
- [DAG Runner](../workflows/runner.md)

---

**Last Updated:** 2026-02-09
