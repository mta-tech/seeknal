# Chapter 2: Second-Order Aggregations

> **Duration:** 30 minutes | **Difficulty:** Advanced | **Format:** Python Transform + SOA Engine

Learn to generate hierarchical features from transaction data using a Python `@feature_group` for first-order aggregation and Seeknal's SOA engine for second-order meta-features — with built-in support for basic, window, and ratio aggregations. Choose between **YAML** (declarative) or **Python** (`@second_order_aggregation` decorator) to define your SOA nodes.

---

## What You'll Build

Building on Chapter 1's feature store, you'll add a hierarchical feature pipeline:

```
(from Chapter 1)
source.transactions ──→ feature_group.customer_features
         ↓
feature_group.customer_daily_agg ──→ second_order_aggregation.region_metrics
         (Python @feature_group)              (YAML or Python SOA)
                                              ├── basic: sum, mean, max, stddev
                                              ├── window: recent 7-day totals
                                              └── ratio: recent vs past spending
```

**After this chapter, you'll have:**
- A Python feature group computing first-order daily aggregations via `ctx.ref()`
- An SOA node using the built-in aggregation engine (basic, window, ratio) — via YAML or Python
- Pipeline execution with REPL verification
- Understanding of hierarchical feature engineering

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Feature Store](1-feature-store.md) — `source.transactions` and `feature_group.customer_features` created
- [ ] Comfortable with aggregation concepts (SUM, COUNT, window functions)

---

## Part 1: Understand Second-Order Aggregations (5 minutes)

### First-Order vs Second-Order

**First-order aggregation**: Raw data → Features
```
Transactions → GROUP BY customer_id → total_spent, avg_amount, order_count
```

**Second-order aggregation**: Features → Meta-features
```
Customer features → GROUP BY region → avg_customer_spend, max_order_count
                  → Time windows    → recent 7-day totals, spending trends
                  → Ratios          → recent_vs_past spending direction
```

### When to Use Second-Order Aggregations

| Use Case | First-Order | Second-Order |
|----------|-------------|--------------|
| Customer total spend | `SUM(amount)` per customer | Average of customer spends per region |
| Purchase frequency | `COUNT(*)` per customer | Stddev of frequencies across regions |
| Spending trends | Not possible | Ratio of recent vs past spending per region |

!!! info "Why Second-Order?"
    Second-order aggregations capture patterns **across** aggregated entities — e.g., how customers in a region behave collectively, or how spending trends shift over time. These features are powerful for ML models because they capture context and distribution patterns.

### Seeknal's SOA Engine

Seeknal provides an **SOA engine** that generates optimized DuckDB SQL from a simple `features:` spec. You can define SOA nodes in **YAML** (declarative) or **Python** (`@second_order_aggregation` decorator). Both approaches use the same engine — you declare **what** to compute:

| Aggregation Type | What It Does | Example |
|------------------|-------------|---------|
| **basic** | Simple aggregation over all history | `basic: [sum, mean, max]` |
| **window** | Aggregation over a time window (days) | `window: [1, 7]` → last 7 days |
| **ratio** | Ratio of two time windows | `numerator: [1, 7]`, `denominator: [8, 14]` |
| **since** | Conditional aggregation | `condition: "flag = 1"` |

The engine computes `_days_between` (the difference between the feature date and a reference application date), then uses it to filter time windows automatically.

### The `application_date` Column

The SOA engine's time-window and ratio features require a **reference date** to answer "how many days ago did this event happen?" This reference date is called `application_date_col`.

**How it works:**

```
_days_between = date_diff('day', feature_date_col, application_date_col)
```

For example, if `order_date` is `2026-01-15` and `application_date` is `2026-01-21`:

```
_days_between = 6  (the order was 6 days before the reference date)
```

The engine then uses `_days_between` to filter time windows:

- `window: [1, 7]` → keeps rows where `_days_between` is between 1 and 7
- `ratio: {numerator: [1, 7], denominator: [8, 14]}` → compares days 1–7 vs days 8–14

**What value should `application_date` be?**

| Scenario | Value | Why |
|----------|-------|-----|
| **Tutorial / testing** | Fixed date like `'2026-01-21'` | Reproducible results; pick 1 day after your latest data |
| **Daily batch pipeline** | `CURRENT_DATE` | Features always relative to "today" |
| **ML prediction** | The date you're predicting for | Features describe history *as of* that date |
| **Backfill** | The batch run date parameter | Use `ctx.params.get('run_date')` for parameterized runs |

!!! warning "Critical: `application_date` must exist in the upstream data"
    The SOA engine expects `application_date_col` as an actual column in the upstream DataFrame — it is **not** generated automatically. Your first-order transform must include it in its output. If it's missing, you'll get:
    ```
    Binder Error: Referenced column "application_date" not found in FROM clause!
    ```
    The fix is to add `application_date` to your transform's SELECT clause (see Step 1 below).

**For basic-only aggregations** (no window or ratio features), the `application_date_col` is still required by the engine but `_days_between` values won't affect your results since basic aggregations operate over all rows regardless of time.

---

## Part 2: Build the Pipeline (15 minutes)

### Reuse Chapter 1's Source

Chapter 1 already created `source.transactions` from `data/transactions.csv` with columns: `customer_id`, `order_id`, `order_date`, `revenue`, `product_category`, `region`. We'll reference it directly — no need to create a new source.

### Step 1: Create the First-Order Feature Group (Python)

This feature group aggregates raw transactions to daily per-customer metrics — the input for the SOA engine. We use `@feature_group` (instead of `@transform`) because daily customer metrics **are features** — they capture temporal purchase behavior that downstream nodes (SOA, training pipelines) will consume.

The output **must** include both `order_date` (as `feature_date_col`) and `application_date` (as `application_date_col`).

```bash
seeknal draft feature-group customer_daily_agg --python --deps pandas,duckdb
```

Edit `draft_feature_group_customer_daily_agg.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Feature Group: Daily aggregation per customer for SOA input."""

from seeknal.pipeline import feature_group


@feature_group(
    name="customer_daily_agg",
    description="Daily aggregation per customer with region context",
)
def customer_daily_agg(ctx):
    """Aggregate transactions to daily customer level."""
    txn = ctx.ref("source.transactions")

    return ctx.duckdb.sql("""
        SELECT
            customer_id,
            region,
            CAST(order_date AS DATE) AS order_date,
            CAST('2026-01-21' AS DATE) AS application_date,
            CAST(SUM(revenue) AS DOUBLE) AS daily_amount,
            CAST(COUNT(*) AS BIGINT) AS daily_count,
            CURRENT_TIMESTAMP AS event_time
        FROM txn
        GROUP BY customer_id, region, CAST(order_date AS DATE)
    """).df()
```

!!! info "Why `@feature_group` without `entity`?"
    We omit the `entity=` parameter here because this feature group has a **daily granularity** (one row per customer per day), not an entity-level granularity (one row per customer). Without `entity`, it won't participate in entity consolidation (Chapter 4), but it's still a feature group — it computes reusable features that the SOA engine and training pipelines consume.

!!! info "The `application_date` column in practice"
    We use a fixed date (`2026-01-21`) that is one day after the latest transaction in our sample data. This ensures all transactions have a positive `_days_between` value and fall within calculable window ranges. In production, replace with `CURRENT_DATE` or a parameterized run date. See the `application_date` section above for details.

    **Both `order_date` and `application_date` must appear in the SELECT** — the SOA engine reads them from the upstream output to compute `_days_between`. If either is missing, you'll get a "column not found" error.

```bash
seeknal dry-run draft_feature_group_customer_daily_agg.py
seeknal apply draft_feature_group_customer_daily_agg.py
```

**Checkpoint:** The file moves to `seeknal/feature_groups/customer_daily_agg.py`.

### Step 2: Create the Second-Order Aggregation (YAML)

Now create the meta-features using Seeknal's declarative SOA engine:

```bash
seeknal draft second-order-aggregation region_metrics
```

This creates `draft_second_order_aggregation_region_metrics.yml`. Edit it:

```yaml
kind: second_order_aggregation
name: region_metrics
description: "Region-level meta-features from customer daily aggregations"
id_col: region
feature_date_col: order_date
application_date_col: application_date
source: feature_group.customer_daily_agg
features:
  daily_amount:
    basic: [sum, mean, max, stddev]
  daily_count:
    basic: [sum, mean]
```

**Understanding the YAML spec:**

| Field | Value | Purpose |
|-------|-------|---------|
| `id_col` | `region` | Group-by column for second-order aggregation |
| `feature_date_col` | `order_date` | Date column for time-window calculations |
| `application_date_col` | `application_date` | Reference date column for `_days_between` (must exist in upstream output) |
| `source` | `feature_group.customer_daily_agg` | Upstream feature group to aggregate from |
| `features` | `daily_amount`, `daily_count` | Feature columns and their aggregation types |

The `basic: [sum, mean, max, stddev]` declaration tells the SOA engine to generate four aggregations per feature — no manual SQL or pandas code needed.

Apply the YAML definition:

```bash
seeknal apply draft_second_order_aggregation_region_metrics.yml
```

**Checkpoint:** The file moves to `seeknal/second_order_aggregations/region_metrics.yml`.

### Alternative: Python SOA Decorator

Instead of YAML, you can define the same SOA node in Python using `@second_order_aggregation`. The decorator uses the **same built-in engine** — the difference is that your function body loads the data via `ctx.ref()`, giving you full Python flexibility for joins, filters, or multi-source inputs.

```bash
seeknal draft second-order-aggregation region_metrics --python --deps pandas,duckdb
```

Edit `draft_second_order_aggregation_region_metrics.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Second-order aggregation: Region-level meta-features."""

from seeknal.pipeline import second_order_aggregation


@second_order_aggregation(
    name="region_metrics",
    id_col="region",
    feature_date_col="order_date",
    application_date_col="application_date",
    features={
        "daily_amount": {"basic": ["sum", "mean", "max", "stddev"]},
        "daily_count": {"basic": ["sum", "mean"]},
    },
)
def region_metrics(ctx):
    """Load data for SOA engine — the engine handles aggregation."""
    return ctx.ref("feature_group.customer_daily_agg")
```

```bash
seeknal dry-run draft_second_order_aggregation_region_metrics.py
seeknal apply draft_second_order_aggregation_region_metrics.py
```

!!! info "YAML vs Python SOA — When to Use Which"
    | Aspect | YAML | Python (`@second_order_aggregation`) |
    |--------|------|--------------------------------------|
    | **Data loading** | `source:` points to one upstream node | `ctx.ref()` — join multiple sources, filter, transform |
    | **Aggregation** | Built-in engine | Same built-in engine |
    | **Best for** | Simple single-source SOA | Complex data prep before aggregation |
    | **Syntax** | Declarative YAML | Python function + decorator |

    Both produce identical output — the engine generates the same DuckDB SQL. Choose Python when you need custom data preparation (joins, filters, computed columns) before aggregation.

### Step 3: Plan and Run

Generate the execution plan and run the full pipeline:

```bash
seeknal plan
seeknal run
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
1/4: transactions [RUNNING]
  SUCCESS in 0.01s
  Rows: 10
2/4: customer_features [RUNNING]
  SUCCESS in 1.2s
  Rows: 6
3/4: customer_daily_agg [RUNNING]
  SUCCESS in 1.2s
  Rows: 10
4/4: region_metrics [RUNNING]
  SUCCESS in 0.8s
  Rows: 4
✓ State saved
✓ Consolidated entity 'customer': 1 FGs, 6 rows in 0.01s
```

!!! note "Chapter 1 Nodes Run Too"
    `seeknal run` executes the full pipeline. You'll see `transactions` and `customer_features` from Chapter 1 run alongside the new nodes. The consolidation output shows `customer_features` (the only FG with `entity="customer"`) being consolidated — `customer_daily_agg` has no entity so it's excluded from consolidation.

---

## Part 3: Verify and Iterate (10 minutes)

### Inspect Generated Features

```bash
seeknal repl
```

```sql
-- View region-level meta-features
SELECT * FROM second_order_aggregation_region_metrics;
```

**Expected output:**
```
+--------+------------------+-------------------+-------------------+---------------------+------------------+-------------------+
| region | daily_amount_SUM | daily_amount_MEAN | daily_amount_MAX  | daily_amount_STDDEV | daily_count_SUM  | daily_count_MEAN  |
+--------+------------------+-------------------+-------------------+---------------------+------------------+-------------------+
| east   |            75.25 |             75.25 |             75.25 |                NULL |                1 |               1.0 |
| north  |           384.93 |            96.233 |            199.95 |              73.xxx |                4 |               1.0 |
| south  |           749.49 |           187.373 |            250.00 |              68.xxx |                4 |               1.0 |
| west   |            45.99 |             45.99 |             45.99 |                NULL |                1 |               1.0 |
+--------+------------------+-------------------+-------------------+---------------------+------------------+-------------------+
```

!!! tip "Auto-Generated Column Names"
    The SOA engine names output columns as `{feature}_{AGG}` — e.g., `daily_amount_SUM`, `daily_amount_MEAN`. This convention keeps features traceable to their source and aggregation type.

### Check Feature Columns

```sql
-- See all generated feature columns
DESCRIBE second_order_aggregation_region_metrics;
```

You'll see columns like:
- `daily_amount_SUM`, `daily_amount_MEAN`, `daily_amount_MAX`, `daily_amount_STDDEV` — Basic aggregations of spending
- `daily_count_SUM`, `daily_count_MEAN` — Basic aggregations of transaction counts

### Compare Regions

```sql
SELECT
    region,
    ROUND(daily_amount_SUM, 2) AS total_spend,
    ROUND(daily_amount_MEAN, 2) AS avg_spend,
    ROUND(daily_amount_STDDEV, 2) AS spend_stddev
FROM second_order_aggregation_region_metrics
ORDER BY daily_amount_SUM DESC;
```

**Checkpoint:** You should see 4 regions. `south` has the highest total spend (749.49), while `north` shows the highest spending variability. Regions with a single transaction (`east`, `west`) have NULL stddev.

### Add Window and Ratio Features

The real power of the SOA engine is its **time-window** and **ratio** features. Add them to your SOA definition:

=== "YAML"

    Edit `seeknal/second_order_aggregations/region_metrics.yml`:

    ```yaml
    kind: second_order_aggregation
    name: region_metrics
    description: "Region-level meta-features from customer daily aggregations"
    id_col: region
    feature_date_col: order_date
    application_date_col: application_date
    source: feature_group.customer_daily_agg
    features:
      daily_amount:
        basic: [sum, mean, max, stddev]
      daily_count:
        basic: [sum, mean]
      # NEW: Recent 7-day spending (days 1-7 before application_date)
      recent_spending:
        window: [1, 7]
        basic: [sum]
        source_feature: daily_amount
      # NEW: Ratio of recent vs past week spending
      spending_trend:
        ratio:
          numerator: [1, 7]
          denominator: [8, 14]
          aggs: [sum]
        source_feature: daily_amount
    ```

=== "Python"

    Edit `seeknal/second_order_aggregations/region_metrics.py`:

    ```python
    @second_order_aggregation(
        name="region_metrics",
        id_col="region",
        feature_date_col="order_date",
        application_date_col="application_date",
        features={
            "daily_amount": {"basic": ["sum", "mean", "max", "stddev"]},
            "daily_count": {"basic": ["sum", "mean"]},
            # NEW: Recent 7-day spending
            "recent_spending": {
                "window": [1, 7],
                "basic": ["sum"],
                "source_feature": "daily_amount",
            },
            # NEW: Ratio of recent vs past week spending
            "spending_trend": {
                "ratio": {"numerator": [1, 7], "denominator": [8, 14], "aggs": ["sum"]},
                "source_feature": "daily_amount",
            },
        },
    )
    def region_metrics(ctx):
        return ctx.ref("feature_group.customer_daily_agg")
    ```

**What the new features do:**

| Feature | Type | Meaning |
|---------|------|---------|
| `recent_spending` | window [1, 7] | Total spending in the last 7 days (Jan 14–20) |
| `spending_trend` | ratio [1,7]/[8,14] | Recent week spend / past week spend — values >1 mean increasing |

Re-run:

```bash
seeknal run
```

Verify the new time-window features:

```bash
seeknal repl
```

```sql
SELECT
    region,
    ROUND("daily_amount_SUM_1_7", 2) AS recent_7d,
    ROUND("daily_amount_SUM1_7_SUM8_14", 2) AS trend_ratio
FROM second_order_aggregation_region_metrics
ORDER BY "daily_amount_SUM1_7_SUM8_14" DESC NULLS LAST;
```

**Expected output:**
```
+--------+-----------+-------------+
| region | recent_7d | trend_ratio |
+--------+-----------+-------------+
| north  |    334.94 |        6.70 |
| south  |    299.99 |        0.67 |
| west   |     45.99 |        NULL |
| east   |      NULL |        NULL |
+--------+-----------+-------------+
```

**Interpreting results:**
- **north** (trend 6.70): Spending surged — 6.7x more in the recent week vs the prior week
- **south** (trend 0.67): Spending declined — recent week is only 67% of the prior week
- **east/west**: Only 1 transaction each, so window features are NULL (no data in the window range)

!!! success "Congratulations!"
    You've built a hierarchical feature pipeline combining a Python transform with Seeknal's declarative SOA engine. The YAML `features:` spec generates optimized SQL automatically — no manual pandas or DuckDB code needed for the aggregation logic.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. Missing `application_date_col` or `feature_date_col` in upstream data**

    - Symptom: `Binder Error: Referenced column "application_date" not found in FROM clause!`
    - Cause: The upstream transform's SELECT clause doesn't include `application_date` (or `order_date`).
    - Fix: Add both columns to your feature group's SQL output. See the `application_date` section in Part 1 for what value to use. If you've fixed the SQL but still get the error, run `seeknal run --full` to rebuild the cached output.

    **2. Wrong source reference format**

    - Symptom: `Invalid source reference: 'customer_daily_agg'. Expected format: 'kind.name'`
    - Fix: Source must be `kind.name` format, e.g., `source: feature_group.customer_daily_agg`

    **3. Window features all NULL**

    - Symptom: Window and ratio columns are NULL for all regions
    - Fix: Check that `_days_between` values fall within your window range. If `application_date` is too far from `order_date`, all transactions fall outside the window. Adjust `application_date` in your transform.

    **4. Feature column not found**

    - Symptom: `Missing feature columns: {'daily_amount'}`
    - Fix: Ensure the `features:` key names (or `source_feature:` values) match actual column names in the upstream feature group output.

    **5. Empty output (0 rows)**

    - Symptom: 0 rows in result
    - Fix: Check that `id_col` matches an actual column in the source feature group output.

---

## Summary

In this chapter, you learned:

- [x] **Feature Groups for Aggregation** — Compute first-order daily aggregations with `@feature_group` and `ctx.ref()`
- [x] **SOA Engine** — Declare meta-features with `features:` spec (basic, window, ratio)
- [x] **YAML vs Python SOA** — YAML for simple single-source SOA; Python `@second_order_aggregation` for custom data prep
- [x] **Application Date** — Reference date column for `_days_between` computation; must be present in upstream data
- [x] **Auto-Generated Columns** — `{feature}_{AGG}` naming convention from the SOA engine
- [x] **Pipeline Execution** — Apply, plan, run, and verify with REPL

**SOA Features Reference:**

| Type | YAML Syntax | Output Column |
|------|-------------|---------------|
| Basic | `basic: [sum, mean]` | `{feature}_SUM`, `{feature}_MEAN` |
| Window | `window: [1, 7]` + `basic: [sum]` | `{feature}_SUM_1_7` |
| Ratio | `ratio: {numerator: [1,7], denominator: [8,14], aggs: [sum]}` | `{feature}_SUM1_7_SUM8_14` |
| Since | `since: {condition: "flag = 1", aggs: [count]}` | `SINCE_COUNT_{feature}_GEO_D` |

**Key Commands:**
```bash
seeknal draft feature-group <name> --python             # Python feature group template
seeknal draft second-order-aggregation <name>           # YAML SOA template
seeknal draft second-order-aggregation <name> --python  # Python SOA template
seeknal dry-run <file>.py                               # Preview Python node
seeknal apply <file>.py                                 # Save Python node
seeknal apply <file>.yml                                # Save YAML node
seeknal plan                                            # Generate DAG manifest
seeknal run                                             # Execute pipeline
seeknal repl                                            # Interactive verification
```

---

## What's Next?

[Chapter 3: Build an ML Model →](3-training-serving-parity.md)

Train a machine learning model using your feature pipeline, with feature validation and serving for inference.

---

## See Also

- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — Full decorator reference and patterns
- **[CLI Reference](../../reference/cli.md)** — Pipeline execution commands
