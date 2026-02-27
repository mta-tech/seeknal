---
topic: ML Engineer Path - PIT Joins, SOA Training Pipeline, and Online Serving
date: 2026-02-27
status: complete
---

# ML Path: Point-in-Time Joins + SOA Training Pipeline

## What We're Building

A rewrite of **Chapter 3 (Build & Serve ML Model)** in the ML Engineer path to teach a real-world ML pipeline using:

1. **Point-in-time joins** via `HistoricalFeaturesDuckDB` with spine (labels + `application_date`)
2. **Second-order aggregation** on PIT-joined daily features for temporal feature engineering
3. **Model training** inside `@transform` using scikit-learn
4. **Online serving** via `OnlineFeaturesDuckDB.get_features()` for training-serving parity

### Pipeline Flow

```
source.churn_labels (spine: customer_id + application_date + churned)
         ↓
@transform: pit_training_data
  Uses HistoricalFeaturesDuckDB(spine=labels, date_col="application_date")
  PIT-joins feature_group.customer_features (from Ch1)
         ↓
SOA: customer_training_features
  Groups by customer_id, aggregates PIT-joined daily features
  Produces temporal features (spending trends, recency windows)
         ↓
@transform: churn_model
  Trains RandomForest on SOA output
  Returns predictions + feature importance
         ↓
@transform (or standalone): online serving
  Uses OnlineFeaturesDuckDB.get_features() to demonstrate inference
  Proves training-serving parity (same features, same schema)
```

### Chapter Structure (~35-40 minutes)

- **Part 1: Point-in-Time Training Data** (~10 min) — Create labels source with `application_date`, use `HistoricalFeaturesDuckDB` inside `@transform` to PIT-join customer features
- **Part 2: SOA Temporal Features** (~8 min) — New per-customer SOA on PIT-joined output for spending trends, recency windows, ratios
- **Part 3: Train ML Model** (~10 min) — Train scikit-learn model inside `@transform`, output predictions
- **Part 4: Online Serving** (~7 min) — `OnlineFeaturesDuckDB.get_features()` for inference, verify parity

## Why This Approach

### Problem with Current Chapter 3
The current Chapter 3 does a simple `INNER JOIN` between features and labels — no temporal awareness. In production ML, this causes **data leakage**: the model sees future feature values at training time. `HistoricalFeaturesDuckDB` with a spine ensures features are as-of the `application_date` for each customer.

### Why HistoricalFeatures inside @transform
Keeps everything in the pipeline DAG. The `@transform` function reads the feature store, creates `HistoricalFeaturesDuckDB` with the spine, and returns a PIT-correct DataFrame. This means:
- `seeknal run` executes the full pipeline end-to-end
- Reproducible: same spine + same features = same training data
- Queryable in REPL like any other transform

### Why SOA after PIT (not before)
PIT join first ensures temporal correctness. SOA then computes window aggregations (e.g., "spending in last 7 days before application_date") on already-correct data. This matches real production pipelines where:
1. Features are materialized to the offline store
2. Training queries join features as-of label timestamps
3. Temporal aggregations compute windowed patterns

## Key Decisions

1. **Rewrite Chapter 3 only** — Keep Ch1 (feature store), Ch2 (SOA intro with region_metrics), Ch4 (entity consolidation) unchanged
2. **HistoricalFeatures inside @transform** — Pipeline-integrated, not standalone script
3. **New per-customer SOA** — Groups by `customer_id` on PIT output, not reusing Ch2's region_metrics
4. **Full training-serving parity** — Include `OnlineFeaturesDuckDB.get_features()` to show inference
5. **Spine = labels source** — `churn_labels` with `customer_id`, `churned`, `application_date` serves as the spine DataFrame

## Impact on Other Chapters

- **Chapter 2** stays as-is (SOA intro with region_metrics). Chapter 3 shows SOA in an ML context.
- **Chapter 4** currently includes a training dataset with SOA (customer_training_features + transform.training_dataset). After Ch3 rewrite, Ch4 should be refocused on pure entity consolidation + struct column exploration, removing the training dataset parts that now live in Ch3.
- **index.md** needs updated descriptions for Ch3 and possibly Ch4.

## Key API Surface to Document

### HistoricalFeaturesDuckDB
```python
from seeknal.featurestore.duckdbengine.feature_group import (
    HistoricalFeaturesDuckDB, FeatureLookup, FeatureGroupDuckDB
)

# Create feature group reference
fg = FeatureGroupDuckDB(name="customer_features", ...)

# Create historical features with spine
lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
hist = hist.using_spine(spine=labels_df, date_col="application_date")
features_df = hist.to_dataframe_with_spine()
```

### OnlineFeaturesDuckDB
```python
online = hist.serve(name="customer_features_online")
result = online.get_features(keys=[{"customer_id": "CUST-100"}])
```

## Scope Boundaries

### In scope
- Rewrite Ch3 with 4 parts (PIT, SOA, model, serving)
- Update Ch4 to remove training dataset overlap
- Update index.md for Ch3 description

### Out of scope
- Changing Ch1 or Ch2
- Teaching Spark HistoricalFeatures (DuckDB only in getting-started)
- Multi-entity PIT joins (keep it single entity: customer)
- `ctx.features(as_of=...)` — this is for consolidated entities, not the focus here

## Resolved Questions

1. **Which chapters to modify?** — Ch3 only (rewrite), Ch4 (refocus), index.md (update)
2. **Pipeline flow?** — Labels source → HistoricalFeatures PIT → SOA → Model → Serve
3. **Integration style?** — Inside @transform (pipeline-integrated)
4. **SOA config?** — New per-customer SOA on PIT output
5. **Online serving included?** — Yes, full training-serving parity
