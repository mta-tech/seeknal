# Brainstorm: Chapter 5 — End-to-End ML: Customer Targeting with MLflow

**Date**: 2026-02-28
**Status**: Draft
**Author**: Claude + User

## What We're Building

A new **Chapter 5** in the ML Engineer Path (`docs/getting-started/ml-engineer-path/5-e2e-ml-customer-targeting.md`) that teaches an end-to-end ML workflow for customer propensity targeting using Seeknal's feature store.

The tutorial walks through:
1. **Feature engineering** — reuse feature groups + PIT joins + SOA features from chapters 1-4
2. **Model training** — train a scikit-learn classifier on historical labeled data
3. **Experiment tracking** — log parameters, metrics, and model artifacts to MLflow
4. **Batch prediction** — load the trained model, score all customers, output a propensity table

**Use case**: A marketing team wants to identify high-propensity customers for a targeting campaign. The ML pipeline produces a scored customer table with propensity ranks.

## Why This Approach

### Manual MLflow integration (not native Seeknal feature)
- MLflow is used inside `@transform` nodes via PEP 723 dependencies
- No new Seeknal code needed — just `mlflow` in the `# /// script` header
- Users call `mlflow.start_run()`, `mlflow.log_model()` etc. directly
- This teaches the pattern without coupling Seeknal to MLflow

### Reuse chapters 1-4 data
- Builds naturally on `customer_features`, `customer_daily_agg`, SOA output
- Adds only one new data file: `data/churn_labels.csv` (customer_id, churned, label_date)
- Shows the power of the feature store — features built once, reused for ML

### Two pipeline files (train + predict)
- Mirrors real production pattern where training and inference are separate concerns
- `seeknal/pipelines/train_propensity.py` — feature retrieval + model training + MLflow logging
- `seeknal/pipelines/predict_propensity.py` — model loading + batch scoring + output table
- Clear separation makes it easy to re-run predictions without retraining

### Parquet-only output
- Prediction table stored as `target/intermediate/transform_customer_scores.parquet`
- Queryable in REPL: `SELECT * FROM transform_customer_scores ORDER BY propensity_score DESC`
- No external database dependency needed to follow the tutorial
- Tutorial mentions `@materialize(type='postgresql')` as a production option

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| MLflow integration | Manual in `@transform` | No new code needed, teaches transferable pattern |
| Tutorial location | Chapter 5 in ML path | Natural continuation after chapters 1-4 |
| Data source | Reuse existing + add labels CSV | Builds on prior work, minimal new data |
| ML framework | scikit-learn | Already used in chapter 3, mention alternatives |
| MLflow depth | Track + log + load for prediction | Shows full train→predict loop |
| Pipeline architecture | Two files (train + predict) | Mirrors production separation |
| Output target | Parquet only | Simplest, no external deps |

## Pipeline Architecture

### File 1: `seeknal/pipelines/train_propensity.py`

```
Nodes:
  source.churn_labels          ← CSV: customer_id, churned (0/1), label_date
  transform.train_propensity   ← inputs: feature_group.customer_features (PIT join)
                                         + second_order_aggregation.customer_soa
                                         + source.churn_labels
                                  actions: join features → train sklearn → log MLflow
                                  output: model_metrics DataFrame (accuracy, AUC, feature importance)
```

**PEP 723 dependencies**: `scikit-learn`, `mlflow`, `pandas`

**Key patterns shown**:
- `ctx.ref("feature_group.customer_features")` → `FeatureFrame`
- `.pit_join(spine=labels, date_col="label_date", keep_cols=["churned"])`
- `ctx.ref("second_order_aggregation.customer_soa").to_df()` → plain DataFrame
- Joining PIT features + SOA features on `customer_id`
- `mlflow.set_tracking_uri("file:./mlruns")`
- `mlflow.start_run(run_name="propensity_v1")`
- `mlflow.log_params({"model": "RandomForestClassifier", "n_estimators": 100})`
- `mlflow.log_metrics({"accuracy": acc, "auc_roc": auc})`
- `mlflow.sklearn.log_model(model, "propensity_model")`

### File 2: `seeknal/pipelines/predict_propensity.py`

```
Nodes:
  transform.score_customers   ← inputs: feature_group.customer_features
                                        + second_order_aggregation.customer_soa
                                actions: load MLflow model → .as_of(today) → score → rank
                                output: customer_id, propensity_score, propensity_rank, segment
```

**PEP 723 dependencies**: `scikit-learn`, `mlflow`, `pandas`

**Key patterns shown**:
- `mlflow.sklearn.load_model("runs:/<run_id>/propensity_model")` OR `mlflow.pyfunc.load_model(...)`
- `.as_of("2026-02-28")` for latest feature snapshot
- Scoring all customers → `predict_proba()[:, 1]`
- Adding `propensity_rank` (1 = highest) and `segment` (high/medium/low)
- Output table queryable in REPL

### Data File: `data/churn_labels.csv`

```csv
customer_id,churned,label_date
CUST-001,0,2026-01-15
CUST-002,1,2026-01-15
CUST-003,0,2026-01-20
...
```

- ~50-100 rows matching existing customer IDs from tutorial data
- `churned`: binary (0=retained, 1=churned)
- `label_date`: when the label was observed (used for PIT join)

## Tutorial Flow (Chapter 5 Outline)

### Section 1: Introduction
- What we're building: propensity model for customer targeting
- Prerequisites: chapters 1-4 completed (feature store + SOA + PIT + entity)
- New concept: MLflow experiment tracking

### Section 2: Setup
- Add `data/churn_labels.csv` to the project
- Start MLflow tracking server (or use local file store)
- Explain PEP 723 deps for `mlflow` + `scikit-learn`

### Section 3: Training Pipeline (`train_propensity.py`)
- Define label source
- Retrieve features via PIT join (`FeatureFrame.pit_join()`)
- Retrieve SOA features
- Join features into training matrix
- Train/evaluate model (train-test split, accuracy, AUC)
- Log everything to MLflow
- Run: `seeknal run --select train_propensity`

### Section 4: Inspect in MLflow UI
- `mlflow ui` to view experiments
- Show logged params, metrics, artifacts
- Compare runs if parameters change

### Section 5: Batch Prediction Pipeline (`predict_propensity.py`)
- Load model from MLflow
- Get latest features via `.as_of()`
- Score all customers
- Create propensity ranking and segments
- Run: `seeknal run --select predict_propensity`

### Section 6: Analyze Results in REPL
- `seeknal repl`
- Query top propensity customers
- Segment distribution analysis
- Export for marketing campaign

### Section 7: Production Tips
- Mention `@materialize(type='postgresql')` for writing to a database
- Scheduled retraining pattern
- Model versioning with MLflow registry

## Open Questions

_None — all key decisions resolved during brainstorming._

## Dependencies

- Chapters 1-4 of ML Engineer Path must be completed first
- `customer_features` feature group must exist
- `customer_daily_agg` and SOA node must exist
- MLflow must be available (pip-installed or via PEP 723)

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| MLflow subprocess isolation | Each `@transform` runs in `uv run` subprocess — MLflow writes to `./mlruns` relative to project root. Verify path consistency. |
| Model artifact portability | Use `mlflow.sklearn.log_model()` which serializes to a standard format. `runs:/<run_id>/...` URI works across transforms if `mlruns/` path is consistent. |
| SOA node naming varies by chapter | Reference exact node names from chapters 1-4. May need to specify expected names. |
| PIT join requires event_time column | Feature group must have `event_time` column. Verify `customer_features` schema from chapter 1. |
