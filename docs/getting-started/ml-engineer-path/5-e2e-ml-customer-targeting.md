# Chapter 5: End-to-End ML — Customer Targeting with MLflow

> **Duration:** 20 minutes | **Difficulty:** Intermediate | **Format:** Python Pipeline + MLflow

Combine everything from Chapters 1–4 into a complete ML workflow: train a propensity model using the feature store, track experiments with MLflow, and run batch predictions to score customers for a marketing campaign.

---

## What You'll Build

A two-pipeline ML system — one for training, one for scoring:

```
transform.training_dataset (Ch4) ──→ transform.train_propensity (scikit-learn + MLflow)
                                              ↓
                                        mlruns/ (model artifact)
                                              ↓
                                        target/mlflow_run_id.txt
                                              ↓
second_order_aggregation.customer_training_features ─┐
                                                     ├──→ transform.score_customers
feature_group.product_preferences ───────────────────┘          ↓
                                                     REPL: Propensity ranking
```

**After this chapter, you'll have:**
- A training pipeline that logs experiments to MLflow (params, metrics, model artifact)
- A prediction pipeline that loads the trained model and scores all customers
- Propensity scores with ranks and segments queryable in the REPL
- Separation of training and inference — the production pattern

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Feature Store](1-feature-store.md) — `feature_group.customer_features` created
- [ ] [Chapter 2: Second-Order Aggregation](2-second-order-aggregation.md) — `feature_group.customer_daily_agg` created
- [ ] [Chapter 3: Build and Serve an ML Model](3-training-serving-parity.md) — `source.churn_labels` and training concepts
- [ ] [Chapter 4: Entity Consolidation](4-entity-consolidation.md) — `transform.training_dataset` with SOA + entity features
- [ ] All pipeline nodes run successfully with `seeknal run`

---

## Part 1: Verify Your Setup (2 minutes)

Before creating new pipelines, confirm that the required upstream nodes exist and have been run:

```bash
seeknal repl
```

```sql
-- Check that training_dataset exists (from Chapter 4)
SELECT COUNT(*) AS rows FROM transform_training_dataset;

-- Check SOA features exist
SELECT COUNT(*) AS rows FROM second_order_aggregation_customer_training_features;

-- Check product preferences exist
SELECT COUNT(*) AS rows FROM feature_group_product_preferences;
```

**Expected:** All three queries return 6 rows.

!!! warning "If queries fail"
    If any table is missing, re-run the full pipeline: `seeknal run`. This executes all nodes from Chapters 1–4 and produces the required intermediate parquets.

---

## Part 2: Training Pipeline (8 minutes)

### Why Separate Training and Prediction?

In production ML systems, training and inference are separate concerns:

| Aspect | Training pipeline | Prediction pipeline |
|--------|-------------------|---------------------|
| **Runs** | Periodically (weekly/monthly) | On-demand or daily |
| **Input** | Historical features + labels | Current features only |
| **Output** | Model artifact + metrics | Scored customer table |
| **Goal** | Find best model | Apply model to new data |

### Create the Training Transform

```bash
seeknal draft transform train_propensity --python --deps pandas,scikit-learn,mlflow
```

Edit `draft_transform_train_propensity.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "scikit-learn",
#     "mlflow",
# ]
# ///

"""Transform: Train a propensity model and log to MLflow."""

from pathlib import Path

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="train_propensity",
    tags=["churn_model"],
    description="Train churn propensity model, log experiment to MLflow",
)
def train_propensity(ctx):
    """Train a Random Forest classifier and log to MLflow."""
    import mlflow
    import mlflow.sklearn
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import cross_val_score, train_test_split
    from sklearn.metrics import roc_auc_score

    # ── 1. Load training dataset from Chapter 4 ────────────────────────
    df = ctx.ref("transform.training_dataset")

    feature_cols = [
        "total_spend",
        "avg_daily_spend",
        "max_daily_spend",
        "total_orders",
        "category_count",
        "electronics_ratio",
    ]
    target_col = "churned"

    X = df[feature_cols].fillna(0)
    y = df[target_col]

    # ── 2. Train / evaluate ─────────────────────────────────────────────
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42
    )

    model = RandomForestClassifier(
        n_estimators=100, max_depth=3, random_state=42
    )
    model.fit(X_train, y_train)

    # Metrics
    accuracy = model.score(X_test, y_test)
    cv_scores = cross_val_score(
        model, X, y, cv=min(3, len(y)), scoring="accuracy"
    )
    y_proba = model.predict_proba(X_test)[:, 1]
    try:
        auc = roc_auc_score(y_test, y_proba)
    except ValueError:
        auc = 0.0  # not enough classes in small test set

    print(f"  Accuracy:     {accuracy:.3f}")
    print(f"  CV Accuracy:  {cv_scores.mean():.3f} (+/- {cv_scores.std():.3f})")
    print(f"  AUC-ROC:      {auc:.3f}")

    # ── 3. Log to MLflow ────────────────────────────────────────────────
    tracking_uri = f"file:{Path.cwd() / 'mlruns'}"
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment("churn_propensity")

    with mlflow.start_run(run_name="propensity_v1") as run:
        mlflow.log_params({
            "model": "RandomForestClassifier",
            "n_estimators": 100,
            "max_depth": 3,
            "features": ", ".join(feature_cols),
            "train_size": len(X_train),
            "test_size": len(X_test),
        })
        mlflow.log_metrics({
            "accuracy": accuracy,
            "cv_accuracy_mean": cv_scores.mean(),
            "auc_roc": auc,
        })
        mlflow.sklearn.log_model(model, "propensity_model")

        run_id = run.info.run_id
        print(f"  MLflow run:   {run_id}")

    # ── 4. Persist run_id for downstream prediction pipeline ────────────
    run_id_path = Path.cwd() / "target" / "mlflow_run_id.txt"
    run_id_path.parent.mkdir(parents=True, exist_ok=True)
    run_id_path.write_text(run_id)
    print(f"  Run ID saved: target/mlflow_run_id.txt")

    # ── 5. Return metrics as DataFrame ──────────────────────────────────
    importance = dict(zip(feature_cols, model.feature_importances_))
    return pd.DataFrame([{
        "run_id": run_id,
        "accuracy": round(accuracy, 4),
        "cv_accuracy": round(cv_scores.mean(), 4),
        "auc_roc": round(auc, 4),
        **{f"imp_{k}": round(v, 4) for k, v in importance.items()},
    }])
```

!!! info "Why import MLflow inside the function?"
    MLflow and scikit-learn are imported **inside** the function body rather than at module level. This is a best practice for PEP 723 pipelines — the imports only execute at runtime when `uv` has installed the dependencies in an isolated environment.

### Apply and Run

```bash
seeknal dry-run draft_transform_train_propensity.py
seeknal apply draft_transform_train_propensity.py
```

```bash
seeknal run --nodes transform.train_propensity
```

!!! warning "First Run Takes Longer"
    The first time you run a node with `mlflow` in its PEP 723 dependencies, `uv` downloads and installs MLflow and its transitive dependencies (~50 packages). This can take 2–5 minutes. Subsequent runs use the cached environment and are fast.

    To pre-warm the cache, run: `uv pip install mlflow scikit-learn`

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
...
transform.train_propensity [RUNNING]
  Accuracy:     0.500
  CV Accuracy:  0.583 (+/- 0.118)
  AUC-ROC:      0.000
  MLflow run:   a1b2c3d4e5f6...
  Run ID saved: target/mlflow_run_id.txt
  SUCCESS in 8.5s
  Rows: 1
✓ State saved
```

!!! note "Small Dataset Disclaimer"
    The tutorial dataset has only 6 customers. Metrics like accuracy and AUC are not meaningful at this scale — they serve to demonstrate the workflow. In production, you'd train on thousands to millions of rows. The `cv=min(3, len(y))` pattern prevents cross-validation errors with small datasets.

**Checkpoint:** Verify two things:
1. `target/intermediate/transform_train_propensity.parquet` exists (model metrics)
2. `target/mlflow_run_id.txt` exists (run ID for prediction pipeline)

---

## Part 3: Inspect in MLflow UI (3 minutes)

### Launch the UI

From your project root directory:

```bash
mlflow ui --backend-store-uri file:./mlruns
```

Open [http://localhost:5000](http://localhost:5000) in your browser.

!!! tip "Port Conflicts"
    If port 5000 is in use, specify a different port: `mlflow ui --port 5001 --backend-store-uri file:./mlruns`

### What You'll See

1. **Experiments sidebar** — Click `churn_propensity`
2. **Run list** — You'll see `propensity_v1` with logged metrics
3. **Run details** — Click the run to see:
   - **Parameters:** model type, hyperparameters, feature list, train/test sizes
   - **Metrics:** accuracy, cv_accuracy_mean, auc_roc
   - **Artifacts:** `propensity_model/` folder containing the serialized scikit-learn model

### Compare Runs

Try changing the model hyperparameters (e.g., `n_estimators=200`, `max_depth=5`), re-run, and compare runs side-by-side in the MLflow UI. Each `seeknal run --nodes transform.train_propensity` creates a new MLflow run.

Stop the UI with `Ctrl+C` when done.

---

## Part 4: Batch Prediction Pipeline (5 minutes)

### Create the Scoring Transform

```bash
seeknal draft transform score_customers --python --deps pandas,duckdb,scikit-learn,mlflow
```

Edit `draft_transform_score_customers.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
#     "scikit-learn",
#     "mlflow",
# ]
# ///

"""Transform: Batch-score customers using trained propensity model."""

from pathlib import Path

from seeknal.pipeline import transform


@transform(
    name="score_customers",
    tags=["churn_predict"],
    description="Load trained model from MLflow, score all customers",
)
def score_customers(ctx):
    """Load model from MLflow and produce propensity scores."""
    import mlflow
    import mlflow.sklearn

    # ── 1. Load the trained model ───────────────────────────────────────
    run_id_path = Path.cwd() / "target" / "mlflow_run_id.txt"
    if not run_id_path.exists():
        raise FileNotFoundError(
            "No trained model found. Run the training pipeline first:\n"
            "  seeknal run --nodes transform.train_propensity"
        )
    run_id = run_id_path.read_text().strip()

    tracking_uri = f"file:{Path.cwd() / 'mlruns'}"
    mlflow.set_tracking_uri(tracking_uri)
    model = mlflow.sklearn.load_model(f"runs:/{run_id}/propensity_model")
    print(f"  Loaded model from MLflow run: {run_id}")

    # ── 2. Assemble feature vector for all customers ────────────────────
    soa = ctx.ref("second_order_aggregation.customer_training_features")
    prefs = ctx.ref("feature_group.product_preferences")

    features_df = ctx.duckdb.sql("""
        SELECT
            s.customer_id,
            s.daily_amount_SUM  AS total_spend,
            s.daily_amount_MEAN AS avg_daily_spend,
            s.daily_amount_MAX  AS max_daily_spend,
            s.daily_count_SUM   AS total_orders,
            p.category_count,
            p.electronics_ratio
        FROM soa s
        LEFT JOIN prefs p ON s.customer_id = p.customer_id
    """).df()

    feature_cols = [
        "total_spend",
        "avg_daily_spend",
        "max_daily_spend",
        "total_orders",
        "category_count",
        "electronics_ratio",
    ]

    # ── 3. Score ────────────────────────────────────────────────────────
    X = features_df[feature_cols].fillna(0)
    features_df["propensity_score"] = model.predict_proba(X)[:, 1]

    # ── 4. Rank and segment ─────────────────────────────────────────────
    features_df["propensity_rank"] = (
        features_df["propensity_score"]
        .rank(ascending=False, method="min")
        .astype(int)
    )
    features_df["segment"] = features_df["propensity_score"].apply(
        lambda s: "high" if s > 0.7 else ("medium" if s > 0.3 else "low")
    )

    print(f"  Scored {len(features_df)} customers")
    print(f"  Segments: {features_df['segment'].value_counts().to_dict()}")

    return features_df[
        ["customer_id", "propensity_score", "propensity_rank", "segment"]
        + feature_cols
    ]
```

### Apply and Run

```bash
seeknal dry-run draft_transform_score_customers.py
seeknal apply draft_transform_score_customers.py
```

```bash
seeknal run --nodes transform.score_customers
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
...
transform.score_customers [RUNNING]
  Loaded model from MLflow run: a1b2c3d4e5f6...
  Scored 6 customers
  Segments: {'low': 3, 'medium': 2, 'high': 1}
  SUCCESS in 5.2s
  Rows: 6
✓ State saved
```

**Checkpoint:** `target/intermediate/transform_score_customers.parquet` exists with 6 rows.

---

## Part 5: Analyze Results in REPL (2 minutes)

```bash
seeknal repl
```

### Top Propensity Customers

```sql
SELECT
    customer_id,
    ROUND(propensity_score, 3) AS score,
    propensity_rank AS rank,
    segment,
    ROUND(total_spend, 2) AS total_spend,
    total_orders
FROM transform_score_customers
ORDER BY propensity_score DESC;
```

**Expected output:**
```
+-------------+-------+------+---------+-------------+--------------+
| customer_id | score | rank | segment | total_spend | total_orders |
+-------------+-------+------+---------+-------------+--------------+
| CUST-102    | 0.820 |    1 | high    |       75.25 |            1 |
| CUST-103    | 0.680 |    2 | medium  |       45.99 |            1 |
| CUST-104    | 0.350 |    3 | medium  |      199.95 |            1 |
| CUST-105    | 0.220 |    4 | low     |      250.00 |            1 |
| CUST-100    | 0.120 |    5 | low     |      184.98 |            3 |
| CUST-101    | 0.080 |    6 | low     |      499.49 |            3 |
+-------------+-------+------+---------+-------------+--------------+
```

### Segment Distribution

```sql
SELECT
    segment,
    CAST(COUNT(*) AS BIGINT) AS customers,
    ROUND(AVG(propensity_score), 3) AS avg_score
FROM transform_score_customers
GROUP BY segment
ORDER BY avg_score DESC;
```

### Campaign Target List

```sql
-- Export high + medium propensity customers for marketing
SELECT customer_id, ROUND(propensity_score, 3) AS score, segment
FROM transform_score_customers
WHERE segment IN ('high', 'medium')
ORDER BY propensity_score DESC;
```

!!! success "Congratulations!"
    You've built a complete ML targeting system — from feature engineering through model training with experiment tracking to batch predictions with customer segmentation. The training and prediction pipelines are separated, reproducible, and queryable.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. MLflow tracking URI path**

    - Symptom: `MlflowException: Could not find experiment` or model not found
    - Fix: Use an absolute path: `f"file:{Path.cwd() / 'mlruns'}"`. Relative paths like `file:./mlruns` resolve differently across subprocess invocations.

    **2. FeatureFrame in SQL — call .to_df() first**

    - Symptom: `Catalog Error: Table with name "df" does not exist`
    - Fix: Assign `ctx.ref()` to a **local variable** and use that name in SQL. DuckDB finds variables via Python's local scope.

    **3. run_id mismatch**

    - Symptom: `RESOURCE_DOES_NOT_EXIST: Run '<id>' not found`
    - Fix: Ensure `target/mlflow_run_id.txt` was written by the training pipeline. Re-run training if needed: `seeknal run --nodes transform.train_propensity`

    **4. First-run install time**

    - Symptom: Pipeline hangs for 2–5 minutes on first run
    - Fix: This is normal — `uv` is installing MLflow dependencies. Pre-warm with: `uv pip install mlflow scikit-learn`

    **5. scikit-learn package name**

    - Symptom: `ModuleNotFoundError: No module named 'sklearn'`
    - Fix: In PEP 723 header, use `scikit-learn` (PyPI name), not `sklearn`.

---

## Summary

In this chapter, you learned:

- [x] **MLflow Experiment Tracking** — Log parameters, metrics, and model artifacts with `mlflow.start_run()`
- [x] **Separate Train/Predict Pipelines** — Production pattern for decoupled training and inference
- [x] **Model Artifact Persistence** — Save models with `mlflow.sklearn.log_model()`, load with `mlflow.sklearn.load_model()`
- [x] **Run ID Hand-off** — Transfer model reference between pipelines via `target/mlflow_run_id.txt`
- [x] **Batch Scoring** — Score all customers and segment by propensity rank
- [x] **Pipeline Composition** — Reuse Chapter 4's training dataset as input to the training pipeline

**Key Commands:**
```bash
seeknal draft transform <name> --python --deps mlflow,scikit-learn   # ML transform template
seeknal run --nodes transform.train_propensity                       # Run training only
seeknal run --nodes transform.score_customers                        # Run predictions only
mlflow ui --backend-store-uri file:./mlruns                          # Launch MLflow UI
seeknal repl                                                         # Query predictions
```

---

## Production Tips

!!! tip "Going to Production"
    **Database output:** Stack `@materialize` on the scoring transform to write propensity scores to PostgreSQL:

    ```python
    from seeknal.pipeline import transform
    from seeknal.pipeline.materialization import materialize

    @materialize(type="postgresql", connection="analytics_db",
                 table="marketing.customer_scores", mode="full")
    @transform(name="score_customers", tags=["churn_predict"])
    def score_customers(ctx):
        ...
    ```

    **Model Registry:** Use MLflow Model Registry instead of raw run IDs:

    ```python
    # In training pipeline
    mlflow.register_model(f"runs:/{run_id}/propensity_model", "churn_propensity")

    # In prediction pipeline
    model = mlflow.sklearn.load_model("models:/churn_propensity/Production")
    ```

    **Scheduled retraining:** Use tags to run only the training nodes: `seeknal run --tags churn_model`

    **Feature monitoring:** Track feature drift with `seeknal dq dashboard` (see Advanced Guide).

---

## What's Next?

You've completed the ML Engineer Path! Explore these advanced topics:

- **[Advanced: Pipeline Tags](../advanced/11-pipeline-tags.md)** — Organize and selectively run pipeline subsets with tags
- **[Advanced: Python Pipelines](../advanced/8-python-pipelines.md)** — Mixed YAML + Python patterns
- **[Data Engineer Path](../data-engineer-path/)** — ELT pipelines, data profiling, and quality rules

---

## See Also

- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — Full decorator reference and patterns
- **[Entity Consolidation Guide](../../guides/entity-consolidation.md)** — Cross-FG retrieval and materialization
- **[CLI Reference](../../reference/cli.md)** — All commands and flags

---

*Last updated: February 2026 | Seeknal Documentation*
