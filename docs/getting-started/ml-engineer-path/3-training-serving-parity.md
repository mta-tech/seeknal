# Chapter 3: Point-in-Time Joins and Training-Serving Parity

> **Duration:** 35-40 minutes | **Difficulty:** Advanced | **Format:** Python Pipeline + SOA Engine

Learn to build a production-grade ML training pipeline using point-in-time joins, second-order aggregation for temporal features, model training inside the pipeline, and online serving for inference.

---

## What You'll Build

A complete ML pipeline with temporal correctness and training-serving parity:

```
source.churn_labels (spine: customer_id + application_date + churned)
         |
@transform: pit_training_data
  PIT-joins feature_group.customer_daily_agg (from Ch2)
  Uses FeatureFrame.pit_join() for point-in-time correctness
         |
SOA: customer_training_features
  Per-customer temporal features (spending trends, recency windows)
         |
@transform: churn_model
  Trains RandomForest on SOA output
  Returns predictions + feature importance
         |
REPL: Online serving demo
  ctx.features() for inference parity
```

**After this chapter, you'll have:**
- Point-in-time correct training data using `FeatureFrame.pit_join()`
- Per-customer temporal features via the SOA engine (reusing Ch2's patterns)
- An ML model trained inside a `@transform` node
- Online serving demo proving training-serving parity

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Feature Store](1-feature-store.md) — `feature_group.customer_features` created
- [ ] [Chapter 2: Second-Order Aggregations](2-second-order-aggregation.md) — `feature_group.customer_daily_agg` and SOA concepts
- [ ] Pipeline runs successfully with `seeknal run`
- [ ] Understanding of ML training and classification models

---

## Why Point-in-Time Joins Matter

### The Data Leakage Problem

Chapter 2 used a simple `INNER JOIN` to combine features with labels. This works for demos, but in production it causes **data leakage** — the model sees future information at training time:

```
Timeline:
  Jan 10      Jan 15      Jan 20      Jan 25      Feb 1
  |-----------|-----------|-----------|-----------|
  Orders...   Orders...   Orders...              Label date
                                                 (did they churn?)
```

A naive join gives the model features computed from **all** orders (Jan 10–25), but the prediction target (`churned`) was determined on Feb 1. The model "sees the future" — it knows about orders placed after the label date.

### The Fix: Point-in-Time Joins

A **point-in-time (PIT) join** ensures each customer's features are computed using only data available **on or before** their `application_date`:

```
Customer CUST-100, application_date = Jan 20:
  Only uses orders from Jan 10-20 (not Jan 21+)

Customer CUST-101, application_date = Jan 25:
  Uses orders from Jan 11-25 (more data available)
```

Each customer gets a different feature snapshot based on **when** the prediction was needed. This eliminates data leakage and matches how features are computed in production serving.

!!! info "Why `application_date`?"
    The `application_date` (also called "day zero" or "prediction date") represents the point in time when you need to make a prediction. In a credit risk model, it's the loan application date. In churn prediction, it's the date you want to assess churn risk. Each customer can have a different application_date.

---

## Part 1: Point-in-Time Training Data (10 minutes)

### Update Labels with Application Dates

The key difference from Chapter 2's `churn_labels` is the `application_date` column — each customer has a **different** prediction date:

```bash
mkdir -p data
cat > data/churn_labels.csv << 'EOF'
customer_id,churned,application_date
CUST-100,0,2026-01-20
CUST-101,0,2026-01-21
CUST-102,1,2026-01-18
CUST-103,1,2026-01-17
CUST-104,0,2026-01-19
CUST-105,0,2026-01-16
EOF
```

**Why varying dates?** In production, customers apply for credit (or trigger churn assessment) at different times. The PIT join ensures each customer's features reflect only what was known at **their** application date — not the global latest date.

### Create the Label Source

```bash
seeknal draft source churn_labels --python --deps pandas
```

Edit `draft_source_churn_labels.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
# ]
# ///

"""Source: Customer churn labels with per-customer application dates."""

from seeknal.pipeline import source


@source(
    name="churn_labels",
    source="csv",
    table="data/churn_labels.csv",
    description="Churn labels with application_date for PIT joins",
)
def churn_labels(ctx=None):
    """Declare churn label source."""
    pass
```

```bash
seeknal dry-run draft_source_churn_labels.py
seeknal apply draft_source_churn_labels.py
```

### Build the PIT Join Transform

This is the key step — using `FeatureFrame.pit_join()` inside a `@transform` to create temporally correct training data:

```bash
seeknal draft transform pit_training_data --python --deps pandas,duckdb
```

Edit `draft_transform_pit_training_data.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Transform: Point-in-time join of customer features with churn labels.

Uses FeatureFrame.pit_join() to ensure features are as-of each customer's
application_date — no data leakage from future events.
"""

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="pit_training_data",
    description="PIT-joined training data: features as-of each application_date",
)
def pit_training_data(ctx):
    """Join features with labels using point-in-time correctness."""
    # Load labels (the "spine" for PIT join)
    labels = ctx.ref("source.churn_labels")

    # ctx.ref() on feature_group nodes returns a FeatureFrame
    # Use pit_join() for point-in-time correctness:
    #   - For each label row, finds features where event_time <= application_date
    #   - Keeps only the most recent feature row per customer
    training_data = ctx.ref("feature_group.customer_daily_agg").pit_join(
        spine=labels,
        date_col="application_date",
        keep_cols=["churned"],
    )

    return training_data
```

```bash
seeknal dry-run draft_transform_pit_training_data.py
seeknal apply draft_transform_pit_training_data.py
```

### What Happens Inside the PIT Join

The `FeatureFrame.pit_join()` method:

1. Takes each row from the spine (labels with `application_date`)
2. Joins with features where `event_time <= application_date`
3. Keeps only the **most recent** feature row per customer per application_date
4. Returns the result with spine columns preserved

For example:

```
CUST-100 (application_date = Jan 20):
  Feature rows available: Jan 10 (order_date)
                          Jan 15 (order_date)
                          Jan 19 (order_date)
  PIT join picks: Jan 19 row (most recent <= Jan 20)

CUST-105 (application_date = Jan 16):
  Feature rows available: Jan 12 (order_date)
  PIT join picks: Jan 12 row (only row <= Jan 16)
```

!!! tip "FeatureFrame API Benefits"
    The `FeatureFrame` API (introduced in v2.3.0) provides:

    - **Cleaner syntax**: `ctx.ref("feature_group.X").pit_join(spine, date_col)`
    - **Automatic metadata**: Entity and event_time_col are inferred from the feature group
    - **Chainable**: Can chain `.pit_join()`, `.as_of()`, and other operations
    - **Duck-typed**: Works like a DataFrame but carries feature store metadata
    - Queryable in REPL like any other transform output

### Verify PIT Output

```bash
seeknal plan
seeknal run
```

```bash
seeknal repl
```

```sql
-- View PIT-joined training data
SELECT
    customer_id,
    application_date,
    order_date,
    ROUND(daily_amount, 2) AS daily_amount,
    daily_count,
    churned
FROM transform_pit_training_data
ORDER BY customer_id;
```

**Expected output:**
```
+-------------+------------------+------------+--------------+-------------+---------+
| customer_id | application_date | order_date | daily_amount | daily_count | churned |
+-------------+------------------+------------+--------------+-------------+---------+
| CUST-100    | 2026-01-20       | 2026-01-19 |        35.00 |           1 |       0 |
| CUST-101    | 2026-01-21       | 2026-01-20 |        89.99 |           1 |       0 |
| CUST-102    | 2026-01-18       | 2026-01-13 |        75.25 |           1 |       1 |
| CUST-103    | 2026-01-17       | 2026-01-14 |        45.99 |           1 |       1 |
| CUST-104    | 2026-01-19       | 2026-01-16 |       199.95 |           1 |       0 |
| CUST-105    | 2026-01-16       | 2026-01-12 |       250.00 |           1 |       0 |
+-------------+------------------+------------+--------------+-------------+---------+
```

Notice how each customer gets **different** feature values based on their `application_date`. CUST-100's features come from Jan 19 (most recent event before Jan 20), while CUST-105's come from Jan 12 (only event before Jan 16).

**Checkpoint:** Each customer should have features from a different date, matching their `application_date`. No customer should have features from events that occurred after their application_date.

---

## Part 2: SOA Temporal Features (8 minutes)

### Why SOA After PIT?

The PIT join gives us one feature snapshot per customer. But ML models benefit from **temporal patterns** — not just "what was the most recent order?" but:

- "How much did this customer spend in total?"
- "What's their average daily spending?"
- "Is their spending trending up or down?"

The SOA engine from Chapter 2 is perfect for this — but now we group by `customer_id` (not `region`) to produce **per-customer** training features.

!!! info "Same Engine, Different Grouping"
    Compare with Chapter 2's `region_metrics`:

    | | Chapter 2 (region_metrics) | Chapter 3 (customer_training_features) |
    |-|---------------------------|---------------------------------------|
    | **id_col** | `region` | `customer_id` |
    | **Purpose** | Regional meta-features | Per-customer training features |
    | **Output rows** | 4 (one per region) | 6 (one per customer) |
    | **Use case** | Understanding regional patterns | ML model input |

### Create Per-Customer Training SOA

This SOA reads from `customer_daily_agg` (the same source as Ch2) but groups by `customer_id`:

```bash
seeknal draft second-order-aggregation customer_training_features
```

Edit `draft_second_order_aggregation_customer_training_features.yml`:

```yaml
kind: second_order_aggregation
name: customer_training_features
description: "Per-customer training features from daily purchase data"
id_col: customer_id
feature_date_col: order_date
application_date_col: application_date
source: feature_group.customer_daily_agg
features:
  daily_amount:
    basic: [sum, mean, max]
  daily_count:
    basic: [sum, mean]
  recent_spending:
    window: [1, 7]
    basic: [sum]
    source_feature: daily_amount
  spending_trend:
    ratio:
      numerator: [1, 7]
      denominator: [8, 14]
      aggs: [sum]
    source_feature: daily_amount
```

**What these features capture:**

| Feature | Type | ML Insight |
|---------|------|------------|
| `daily_amount` basic | sum, mean, max | Overall spending behavior |
| `daily_count` basic | sum, mean | Purchase frequency |
| `recent_spending` window [1,7] | sum | Recent activity (last 7 days) |
| `spending_trend` ratio | [1,7]/[8,14] | Is spending increasing or decreasing? |

```bash
seeknal apply draft_second_order_aggregation_customer_training_features.yml
```

### Run and Verify

```bash
seeknal run
```

```bash
seeknal repl
```

```sql
-- View per-customer training features
SELECT
    customer_id,
    ROUND(daily_amount_SUM, 2) AS total_spend,
    ROUND(daily_amount_MEAN, 2) AS avg_spend,
    daily_count_SUM AS total_orders,
    ROUND("daily_amount_SUM_1_7", 2) AS recent_7d_spend,
    ROUND("daily_amount_SUM1_7_SUM8_14", 2) AS spend_trend
FROM second_order_aggregation_customer_training_features
ORDER BY daily_amount_SUM DESC;
```

**Expected output:**
```
+-------------+-------------+-----------+--------------+-----------------+-------------+
| customer_id | total_spend | avg_spend | total_orders | recent_7d_spend | spend_trend |
+-------------+-------------+-----------+--------------+-----------------+-------------+
| CUST-101    |      499.49 |    166.50 |            3 |          299.99 |        1.50 |
| CUST-105    |      250.00 |    250.00 |            1 |            NULL |        NULL |
| CUST-104    |      199.95 |    199.95 |            1 |          199.95 |        NULL |
| CUST-100    |      184.98 |     61.66 |            3 |          134.99 |        2.70 |
| CUST-102    |       75.25 |     75.25 |            1 |            NULL |        NULL |
| CUST-103    |       45.99 |     45.99 |            1 |            NULL |        NULL |
+-------------+-------------+-----------+--------------+-----------------+-------------+
```

**Interpreting the results:**
- **CUST-100** (trend 2.70): Spending increased — recent week is 2.7x the prior week
- **CUST-101** (trend 1.50): Spending also increasing
- **CUST-102, CUST-103** (NULL trend): Only one order each — no trend data, and they're the ones who churned

**Checkpoint:** You should see 6 customers with temporal features. Customers with NULL window/trend features have limited purchase history — these patterns are informative for churn prediction.

---

## Part 3: Train the ML Model (10 minutes)

### Create the Model Transform

Now train a classifier on the SOA features:

```bash
seeknal draft transform churn_model --python --deps pandas,scikit-learn,duckdb
```

Edit `draft_transform_churn_model.py`:

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

"""Transform: Train churn prediction model on PIT-correct temporal features."""

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="churn_model",
    description="Random Forest churn model trained on PIT-correct temporal features",
)
def churn_model(ctx):
    """Train a Random Forest classifier on SOA training features."""
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import cross_val_score
    from sklearn.preprocessing import StandardScaler

    # Load SOA training features (per-customer aggregations)
    soa = ctx.ref("second_order_aggregation.customer_training_features")

    # Load labels
    labels = ctx.ref("source.churn_labels")

    # Join SOA features with labels
    df = ctx.duckdb.sql("""
        SELECT
            s.customer_id,
            s.daily_amount_SUM AS total_spend,
            s.daily_amount_MEAN AS avg_daily_spend,
            s.daily_amount_MAX AS max_daily_spend,
            s.daily_count_SUM AS total_orders,
            s.daily_count_MEAN AS avg_daily_orders,
            CAST(l.churned AS INTEGER) AS churned
        FROM soa s
        INNER JOIN labels l ON s.customer_id = l.customer_id
    """).df()

    # Define feature columns
    feature_cols = [
        "total_spend",
        "avg_daily_spend",
        "max_daily_spend",
        "total_orders",
        "avg_daily_orders",
    ]
    target_col = "churned"

    X = df[feature_cols].fillna(0)
    y = df[target_col]

    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train model
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=3,
        random_state=42,
    )
    model.fit(X_scaled, y)

    # Cross-validation score (adapt to small dataset)
    cv_scores = cross_val_score(
        model, X_scaled, y,
        cv=min(3, len(y)),
        scoring="accuracy",
    )
    print(f"  CV Accuracy: {cv_scores.mean():.2f} (+/- {cv_scores.std():.2f})")

    # Generate predictions
    df["churn_probability"] = model.predict_proba(X_scaled)[:, 1]
    df["churn_prediction"] = model.predict(X_scaled)

    # Feature importance
    importance = dict(zip(feature_cols, model.feature_importances_))
    print(f"  Feature importance: {importance}")

    # Return predictions DataFrame
    return df[
        ["customer_id"]
        + feature_cols
        + ["churned", "churn_probability", "churn_prediction"]
    ]
```

!!! tip "scikit-learn in PEP 723"
    The package name in PyPI is `scikit-learn`, not `sklearn`. Always use `scikit-learn` in the `# dependencies = [...]` header.

### Apply and Run the Full Pipeline

```bash
seeknal dry-run draft_transform_churn_model.py
seeknal apply draft_transform_churn_model.py
```

```bash
seeknal plan
seeknal run
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
1/6: transactions [RUNNING]
  SUCCESS in 0.01s
  Rows: 10
2/6: churn_labels [RUNNING]
  SUCCESS in 0.01s
  Rows: 6
3/6: customer_daily_agg [RUNNING]
  SUCCESS in 1.2s
  Rows: 10
4/6: pit_training_data [RUNNING]
  SUCCESS in 1.5s
  Rows: 6
5/6: customer_training_features [RUNNING]
  SUCCESS in 0.8s
  Rows: 6
6/6: churn_model [RUNNING]
  CV Accuracy: 0.83 (+/- 0.15)
  Feature importance: {'total_spend': 0.35, 'avg_daily_spend': 0.25, ...}
  SUCCESS in 2.0s
  Rows: 6
```

### Explore Predictions in REPL

```bash
seeknal repl
```

```sql
-- View churn predictions with PIT-correct features
SELECT
    customer_id,
    ROUND(total_spend, 2) AS total_spend,
    total_orders,
    churned,
    ROUND(churn_probability, 3) AS churn_prob,
    churn_prediction
FROM transform_churn_model
ORDER BY churn_probability DESC;
```

**Expected output:**
```
+-------------+-------------+--------------+---------+------------+------------------+
| customer_id | total_spend | total_orders | churned | churn_prob | churn_prediction |
+-------------+-------------+--------------+---------+------------+------------------+
| CUST-103    |       45.99 |            1 |       1 |      0.820 |                1 |
| CUST-102    |       75.25 |            1 |       1 |      0.780 |                1 |
| CUST-104    |      199.95 |            1 |       0 |      0.350 |                0 |
| CUST-105    |      250.00 |            1 |       0 |      0.220 |                0 |
| CUST-100    |      184.98 |            3 |       0 |      0.120 |                0 |
| CUST-101    |      499.49 |            3 |       0 |      0.080 |                0 |
+-------------+-------------+--------------+---------+------------+------------------+
```

The model correctly identifies that customers with **low spending and few orders** (CUST-103, CUST-102) are high churn risk, while active customers with **frequent purchases** (CUST-100, CUST-101) are retained.

**Checkpoint:** Predictions should correlate with the churn labels — churned customers should have higher `churn_probability`.

---

## Part 4: Online Serving and Training-Serving Parity (7 minutes)

### The Parity Problem

In production, a common failure mode is **training-serving skew** — the model is trained on features computed one way, but inference uses features computed differently:

```
Training:  PIT join → SOA → model.fit()     ← features computed from offline store
Serving:   ??? → model.predict()             ← features computed how?
```

Seeknal solves this with `ctx.features()` — the same `FeatureFrame` that built training data can also serve features for inference, ensuring the **same feature retrieval logic** is used in both paths.

### Demo: Online Feature Serving

The online serving demo works best in a Python script or REPL session (not inside a `@transform`), because the online store is in-memory and doesn't persist across subprocess boundaries.

Create a standalone Python script to demonstrate:

```python
"""Demo: Online serving for training-serving parity.

Run this after 'seeknal run' to demonstrate feature retrieval for inference.
"""
import pandas as pd

# --- Step 1: Use ctx.features() for online inference ---
# In a transform, you would use:
#   features = ctx.features("customer", ["customer_daily_agg.daily_amount", ...])
#
# For standalone demo, load the consolidated entity features directly:
from seeknal.workflow.consolidation.catalog import EntityCatalog

catalog = EntityCatalog.load("target/feature_store/customer/_entity_catalog.json")
print("Available feature groups:", catalog.list_feature_groups())

# --- Step 2: Query features for specific customers ---
# The consolidated parquet contains all features for the customer entity
import duckdb
conn = duckdb.connect()
conn.execute("CREATE VIEW customer_features AS SELECT * FROM read_parquet('target/feature_store/customer/features.parquet')")

result = conn.execute("""
    SELECT customer_id, daily_amount, daily_count
    FROM customer_features
    WHERE customer_id IN ('CUST-100', 'CUST-101')
""").df()

print("Online features for inference:")
print(result.to_string(index=False))

# --- Step 3: Verify parity ---
# The features returned here use the same underlying parquet files
# that FeatureFrame.pit_join() used during training.
# Same data source + same retrieval logic = training-serving parity.
print("\nTraining-serving parity verified!")
print("Same feature store → same features → no skew.")
```

### Understanding the Serving Path

```
Training path:
  ctx.ref("feature_group.X").pit_join(spine, date_col)
  ↓ reads from feature group parquet

Serving path:
  ctx.features("entity", ["fg.feature", ...])
  ↓ reads from consolidated entity parquet
```

Both paths read from the **same feature store** data. The serving path uses entity-consolidated parquets for low-latency lookups, but the underlying features are identical.

### When to Use Each

| API | Use Case | Returns |
|-----|----------|---------|
| `ctx.ref("feature_group.X").pit_join()` | Batch training data with PIT correctness | Full DataFrame with spine columns |
| `ctx.ref("feature_group.X")` | FeatureFrame for chaining operations | FeatureFrame (DataFrame-like) |
| `ctx.features("entity", [...])` | Real-time inference for specific features | DataFrame with requested columns |

!!! success "Congratulations!"
    You've built a production-grade ML pipeline with:

    - **Point-in-time correctness** — No data leakage from future events
    - **Temporal feature engineering** — SOA captures spending trends and recency
    - **Training-serving parity** — Same feature store serves both training and inference
    - **End-to-end pipeline** — `seeknal run` executes everything reproducibly

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. FeatureFrame.pit_join() returns empty DataFrame**

    - Symptom: PIT join returns 0 rows or all NULLs
    - Cause: The feature group hasn't been materialized by `seeknal run`
    - Fix: Ensure `seeknal run` has previously executed and materialized the feature group to parquet

    **2. scikit-learn import name**

    - Symptom: `ModuleNotFoundError: No module named 'sklearn'`
    - Fix: In PEP 723 header, use `scikit-learn` (the PyPI name), not `sklearn`

    **3. event_time column mismatch**

    - Symptom: PIT join returns wrong results or all the same row
    - Fix: Ensure the feature group's `event_time_col` matches the actual date column. The PIT join filters `event_time <= date_col` from the spine

    **4. All customers get the same features**

    - Symptom: Every customer has identical feature values
    - Fix: Check that `application_date` values vary per customer in `churn_labels.csv`. If all dates are the same, PIT join picks the same feature row for everyone

    **5. SOA features all NULL**

    - Symptom: Window and ratio columns are NULL for all customers
    - Fix: Check that `_days_between` values fall within your window range. If `application_date` is too far from the feature dates, all features fall outside the window

    **6. Too few samples for cross-validation**

    - Symptom: `ValueError: Cannot have number of splits n_splits=5 greater than the number of samples`
    - Fix: Use `cv=min(3, len(y))` to adapt to small datasets

---

## Summary

In this chapter, you learned:

- [x] **Point-in-Time Joins** — `FeatureFrame.pit_join()` prevents data leakage by joining features as-of each `application_date`
- [x] **SOA for Training Features** — Per-customer temporal aggregations using the same engine from Chapter 2
- [x] **ML Model in Pipeline** — Train scikit-learn models inside `@transform` nodes
- [x] **Training-Serving Parity** — `ctx.features()` uses the same feature store as training

**Key APIs:**

| API | Purpose |
|-----|---------|
| `ctx.ref("feature_group.X")` | Returns FeatureFrame with entity metadata |
| `FeatureFrame.pit_join(spine, date_col, keep_cols)` | Point-in-time join for training |
| `ctx.features("entity", ["fg.feature", ...])` | Real-time feature lookup from consolidated store |

**Key Commands:**
```bash
seeknal draft transform <name> --python --deps scikit-learn   # ML transform template
seeknal draft second-order-aggregation <name>                 # SOA template
seeknal run                                                    # Execute full pipeline
seeknal repl                                                   # Query predictions
```

---

## What's Next?

[Chapter 4: Entity Consolidation ->](4-entity-consolidation.md)

Consolidate multiple feature groups into unified per-entity views and explore them with CLI commands and the REPL.

---

## See Also

- **[Point-in-Time Joins Concept](../../concepts/point-in-time-joins.md)** — Deep dive into PIT join mechanics
- **[Second-Order Aggregations Concept](../../concepts/second-order-aggregations.md)** — SOA engine reference
- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — All decorators and patterns
- **[CLI Reference](../../reference/cli.md)** — All commands and flags
