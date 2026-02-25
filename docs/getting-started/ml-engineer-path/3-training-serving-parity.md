# Chapter 3: Build and Serve an ML Model

> **Duration:** 30 minutes | **Difficulty:** Advanced | **Format:** Python Pipeline

Learn to train an ML model inside a Seeknal pipeline using `@transform`, validate features with CLI commands, and serve predictions for inference.

---

## What You'll Build

A complete ML pipeline — from features to predictions — using Python decorators:

```
feature_group.customer_features (Ch1) ──→ transform.training_data (Python)
                                                     ↓
                                          transform.churn_model (Python + scikit-learn)
                                                     ↓
                                          REPL: Query predictions
                                                     ↓
                                          seeknal validate-features → Parity Check
```

**After this chapter, you'll have:**
- A transform that prepares training data from feature groups
- An ML model trained inside a `@transform` node using scikit-learn
- Prediction results queryable in the REPL
- Feature validation via CLI

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Feature Store](1-feature-store.md) — `feature_group.customer_features` created
- [ ] [Chapter 2: Second-Order Aggregations](2-second-order-aggregation.md) — Pipeline concepts
- [ ] Understanding of ML training and classification models

---

## Part 1: Prepare Training Data (8 minutes)

### Create Labels

First, create a labels dataset with churn indicators for each customer:

```bash
mkdir -p data
cat > data/churn_labels.csv << 'EOF'
customer_id,churned,label_date
CUST-100,0,2026-02-01
CUST-101,0,2026-02-01
CUST-102,1,2026-02-01
CUST-103,1,2026-02-01
CUST-104,0,2026-02-01
CUST-105,0,2026-02-01
EOF
```

### Create a Label Source

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

"""Source: Customer churn labels for model training."""

from seeknal.pipeline import source


@source(
    name="churn_labels",
    source="csv",
    table="data/churn_labels.csv",
    description="Binary churn labels per customer",
)
def churn_labels(ctx=None):
    """Declare churn label source."""
    pass
```

```bash
seeknal dry-run draft_source_churn_labels.py
seeknal apply draft_source_churn_labels.py
```

### Build Training Data Transform

Join features with labels to create a training dataset:

```bash
seeknal draft transform training_data --python --deps pandas,duckdb
```

Edit `draft_transform_training_data.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Transform: Join customer features with churn labels for model training."""

from seeknal.pipeline import transform


@transform(
    name="training_data",
    description="Training dataset: customer features + churn labels",
)
def training_data(ctx):
    """Join feature group output with churn labels."""
    features = ctx.ref("feature_group.customer_features")
    labels = ctx.ref("source.churn_labels")

    return ctx.duckdb.sql("""
        SELECT
            f.customer_id,
            f.total_orders,
            f.total_revenue,
            f.avg_order_value,
            f.days_since_first_order,
            CAST(l.churned AS INTEGER) AS churned
        FROM features f
        INNER JOIN labels l ON f.customer_id = l.customer_id
    """).df()
```

```bash
seeknal dry-run draft_transform_training_data.py
seeknal apply draft_transform_training_data.py
```

**Checkpoint:** Run `seeknal plan` to see the DAG — `training_data` should depend on `customer_features` and `churn_labels`.

---

## Part 2: Train an ML Model (12 minutes)

### Why Train Inside the Pipeline?

Training inside a `@transform` node means:

- **Reproducibility** — Model inputs are tracked in the DAG
- **Versioning** — Feature schema changes are detected automatically
- **End-to-end** — `seeknal run` executes the full pipeline including training
- **Queryable** — Predictions are available in the REPL

### Create the Model Transform

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

"""Transform: Train churn prediction model and generate predictions."""

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="churn_model",
    description="Random Forest churn model with predictions and feature importance",
)
def churn_model(ctx):
    """Train a Random Forest classifier and output predictions."""
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import cross_val_score
    from sklearn.preprocessing import StandardScaler

    # Load training data from upstream transform
    df = ctx.ref("transform.training_data")

    # Define feature columns and target
    feature_cols = [
        "total_orders",
        "total_revenue",
        "avg_order_value",
        "days_since_first_order",
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

    # Cross-validation score
    cv_scores = cross_val_score(model, X_scaled, y, cv=min(3, len(y)), scoring="accuracy")
    print(f"  CV Accuracy: {cv_scores.mean():.2f} (+/- {cv_scores.std():.2f})")

    # Generate predictions
    df["churn_probability"] = model.predict_proba(X_scaled)[:, 1]
    df["churn_prediction"] = model.predict(X_scaled)

    # Add feature importance
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
1/5: transactions [RUNNING]
  SUCCESS in 0.01s
  Rows: 10
2/5: churn_labels [RUNNING]
  SUCCESS in 0.01s
  Rows: 6
3/5: customer_features [RUNNING]
  SUCCESS in 1.2s
  Rows: 6
4/5: training_data [RUNNING]
  SUCCESS in 1.1s
  Rows: 6
5/5: churn_model [RUNNING]
  CV Accuracy: 0.83 (+/- 0.15)
  Feature importance: {'total_orders': 0.25, 'total_revenue': 0.35, ...}
  SUCCESS in 2.0s
  Rows: 6
✓ State saved
```

### Explore Predictions in REPL

```bash
seeknal repl
```

```sql
-- View churn predictions
SELECT
    customer_id,
    total_orders,
    total_revenue,
    churned,
    ROUND(churn_probability, 3) AS churn_prob,
    churn_prediction
FROM churn_model
ORDER BY churn_probability DESC;
```

**Expected output:**
```
+-------------+--------------+---------------+---------+------------+------------------+
| customer_id | total_orders | total_revenue | churned | churn_prob | churn_prediction |
+-------------+--------------+---------------+---------+------------+------------------+
| CUST-102    |            1 |         75.25 |       1 |      0.820 |                1 |
| CUST-103    |            1 |         45.99 |       1 |      0.780 |                1 |
| CUST-104    |            1 |        199.95 |       0 |      0.350 |                0 |
| CUST-105    |            1 |        250.00 |       0 |      0.220 |                0 |
| CUST-100    |            3 |        184.98 |       0 |      0.120 |                0 |
| CUST-101    |            3 |        499.49 |       0 |      0.080 |                0 |
+-------------+--------------+---------------+---------+------------+------------------+
```

```sql
-- High-risk customers (churn probability > 50%)
SELECT customer_id, ROUND(churn_probability, 3) AS churn_prob
FROM churn_model
WHERE churn_probability > 0.5
ORDER BY churn_probability DESC;
```

**Checkpoint:** You should see predictions for all 6 customers, with churned customers having higher probabilities.

---

## Part 3: Validate Features (10 minutes)

### Why Validation Matters

Training-serving skew happens when:
- Feature code differs between training and serving
- Data distributions shift over time
- Schema changes aren't propagated

### Run Feature Validation

Use the CLI to validate feature group data quality:

```bash
seeknal validate-features customer_features --mode fail --verbose
```

**Expected output:**
```
ℹ Validating feature group: customer_features

Validators:
  ✓ null_check: No null values in entity keys
  ✓ type_check: All feature types consistent
  ✓ range_check: Feature values within expected ranges
  ✓ freshness_check: Features updated within 24h

Summary:
  Total validators: 4
  Passed: 4
  Failed: 0

✓ All validations passed
```

### Validation Modes

| Mode | Behavior |
|------|----------|
| `--mode fail` | Stops on first failure (for CI/CD pipelines) |
| `--mode warn` | Logs all failures, continues (for monitoring) |

```bash
# Warn mode — log but don't fail
seeknal validate-features customer_features --mode warn

# Verbose — show detailed results
seeknal validate-features customer_features --mode fail --verbose
```

### Version-Specific Validation

Ensure you're validating the same version used during training:

```bash
# Check which versions exist
seeknal version list customer_features

# View specific version schema
seeknal version show customer_features --version 1

# Compare versions to detect breaking changes
seeknal version diff customer_features --from 1 --to 2
```

!!! tip "Reproducibility Tip"
    Pin your feature group version when generating training data. If the schema changes (new features added, features removed), Seeknal tracks it automatically so you can always recreate past training datasets.

### Verify Pipeline Integrity

Run the entire pipeline end-to-end and check all outputs:

```bash
# Full pipeline run
seeknal run

# Verify all nodes produced output
seeknal repl
```

```sql
-- Check row counts across the pipeline
SELECT 'transactions' AS node, COUNT(*) AS rows FROM transactions
UNION ALL
SELECT 'customer_features', COUNT(*) FROM customer_features
UNION ALL
SELECT 'training_data', COUNT(*) FROM training_data
UNION ALL
SELECT 'churn_model', COUNT(*) FROM churn_model;
```

**Expected output:**
```
+-------------------+------+
| node              | rows |
+-------------------+------+
| transactions      |   10 |
| customer_features |    6 |
| training_data     |    6 |
| churn_model       |    6 |
+-------------------+------+
```

!!! success "Congratulations!"
    You've built a complete ML pipeline — from raw data through feature engineering to model training and predictions — entirely using Python pipeline decorators. Your pipeline is reproducible, versioned, and queryable.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. scikit-learn import name**

    - Symptom: `ModuleNotFoundError: No module named 'sklearn'`
    - Fix: In PEP 723 header, use `scikit-learn` (the PyPI name), not `sklearn`: `"scikit-learn",`

    **2. Missing features in training data**

    - Symptom: `KeyError: 'days_since_first_order'`
    - Fix: Ensure the upstream `feature_group.customer_features` includes the feature. Run Chapter 1 Part 3 to add `days_since_first_order`.

    **3. NaN values break the model**

    - Symptom: `ValueError: Input contains NaN`
    - Fix: Use `.fillna(0)` or handle missing values before passing to scikit-learn. DuckDB SQL `COALESCE()` also works.

    **4. Too few samples for cross-validation**

    - Symptom: `ValueError: Cannot have number of splits n_splits=5 greater than the number of samples`
    - Fix: Use `cv=min(3, len(y))` to adapt to small datasets.

    **5. Model output not a DataFrame**

    - Symptom: `TypeError: Cannot convert dict to DataFrame`
    - Fix: Always return a pandas DataFrame from `@transform` functions. Wrap results with `pd.DataFrame(data)` if needed.

---

## Summary

In this chapter, you learned:

- [x] **Training Data Transform** — Join features with labels using `ctx.ref()`
- [x] **ML Model in Pipeline** — Train scikit-learn models inside `@transform` nodes
- [x] **Predictions in REPL** — Query model outputs with SQL
- [x] **Feature Validation** — CLI-based quality checks with `seeknal validate-features`
- [x] **Version Tracking** — Reproducible features with `seeknal version` commands

**Key Commands:**
```bash
seeknal draft transform <name> --python --deps scikit-learn   # ML transform template
seeknal run                                                    # Execute full pipeline
seeknal repl                                                   # Query predictions
seeknal validate-features <fg_name> --mode fail --verbose     # Validate features
seeknal version list <fg_name>                                 # List versions
seeknal version diff <fg_name> --from 1 --to 2                # Compare versions
```

---

## Path Complete!

You've completed the ML Engineer path. Here's what you built:

```
source.transactions ──→ feature_group.customer_features ──→ transform.training_data
                                                                      ↓
source.churn_labels ─────────────────────────────────────→ transform.churn_model
                                                                      ↓
second_order_aggregation.region_metrics                    REPL: Query predictions
          ↓
  Regional meta-features                           seeknal validate-features
```

### What's Next?

- **[Advanced Guide: Python Pipelines](../advanced/8-python-pipelines.md)** — Mixed YAML + Python, advanced decorators
- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — Full decorator reference
- **[Data Engineer Path](../data-engineer-path/)** — Build production ELT pipelines
- **[Analytics Engineer Path](../analytics-engineer-path/)** — Semantic layers and metrics

---

## See Also

- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — All decorators and patterns
- **[CLI Reference](../../reference/cli.md)** — All feature validation commands
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Feature group YAML schema
