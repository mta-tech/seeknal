# Feature Store Guide

> **Status:** 📝 Documentation in progress

A comprehensive guide to building and managing feature stores with Seeknal.

---

## What is a Feature Store?

A **feature store** is a centralized repository for storing, serving, and managing ML features. It solves the core problem of ensuring consistency between features used during model training and features available during model serving.

---

## Key Concepts

### Feature Group

A feature group is a collection of related features for a specific entity (e.g., `user`, `product`, `transaction`).

```python
from seeknal.entity import Entity
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    Materialization,
)

# Define entity
user_entity = Entity(name="user", join_keys=["user_id"])

# Create feature group
fg = FeatureGroupDuckDB(
    name="user_features",
    entity=user_entity,
    materialization=Materialization(event_time_col="created_at"),
    project="my_project"
)
```

### Offline vs Online Store

| Store | Purpose | Use Case |
|--------|---------|----------|
| **Offline** | Batch training data | Model training, batch inference |
| **Online** | Real-time feature serving | Online predictions, real-time scoring |

---

## Quick Start

### 1. Create a Feature Group

```python
import pandas as pd
from datetime import datetime
from seeknal.entity import Entity
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    Materialization,
)

# Define entity
user_entity = Entity(name="user", join_keys=["user_id"])

# Create feature group
fg = FeatureGroupDuckDB(
    name="user_features",
    entity=user_entity,
    materialization=Materialization(event_time_col="timestamp"),
    project="my_project"
)

# Prepare feature data
features_df = pd.DataFrame({
    'user_id': ['user_001', 'user_002', 'user_003'],
    'timestamp': [
        datetime(2024, 1, 15, 10, 30),
        datetime(2024, 1, 15, 11, 45),
        datetime(2024, 1, 15, 12, 15)
    ],
    'total_purchases': [15, 8, 23],
    'avg_spend': [125.50, 89.99, 200.00],
    'last_category': ['electronics', 'clothing', 'home']
})

# Set features from DataFrame
fg.set_dataframe(features_df).set_features()

# Write to offline store
fg.write(feature_start_time=datetime(2024, 1, 1))
```

### 2. Read Historical Features (PIT Join)

Use `FeatureFrame.pit_join()` within a transform:

```python
from seeknal.pipeline import transform

@transform(name="training_data")
def training_data(ctx):
    # Spine with entity keys and prediction dates
    labels = ctx.ref("source.churn_labels")  # user_id, application_date, label

    # PIT join: get features as of each application_date
    features_df = ctx.ref("feature_group.user_features").pit_join(
        spine=labels,
        date_col="application_date",
        keep_cols=["label"],
    )
    return features_df
```

### 3. Serve Online Features

Use `ctx.features()` for real-time feature lookup:

```python
from seeknal.pipeline import transform

@transform(name="online_predictions")
def online_predictions(ctx):
    # Get latest features from consolidated entity store
    features = ctx.features("user", [
        "user_features.total_purchases",
        "user_features.avg_spend",
    ])
    return features
```

---

## Feature Versioning

Seeknal automatically versions feature groups when schemas change:

```bash
# List all versions
seeknal version list user_features

# Show specific version
seeknal version show user_features --version 2

# Compare versions
seeknal version diff user_features --from 1 --to 2

# Materialize specific version
seeknal materialize user_features --start-date 2024-01-01 --version 1
```

---

## Best Practices

### 1. Always Specify `event_time_col`

```python
# ✅ Correct
materialization = Materialization(event_time_col="timestamp")

# ❌ Wrong - no temporal column
materialization = Materialization()
```

### 2. Use Meaningful Entity Names

```python
# ✅ Good - descriptive
Entity(name="customer", join_keys=["customer_id"])

# ❌ Bad - generic
Entity(name="entity1", join_keys=["id"])
```

### 3. Set Appropriate Time Windows

```python
# ✅ Good - 90-day lookback for behavioral features
fg.write(feature_start_time=datetime(2024, 1, 1))

# ❌ Bad - includes years of stale data
fg.write(feature_start_time=datetime(2020, 1, 1))
```

---

## Entity Consolidation

When you have multiple feature groups for the same entity (e.g., `customer_features` and `product_features` both keyed on `customer_id`), Seeknal automatically consolidates them into a single per-entity view with struct-namespaced columns.

```bash
# List consolidated entities
seeknal entity list

# Show entity details
seeknal entity show customer
```

Retrieve features across feature groups in your transforms:

```python
@transform(name="training_data")
def build_training_data(ctx):
    df = ctx.features("customer", [
        "customer_features.revenue",
        "product_features.avg_price",
    ])
    return df
```

See the **[Entity Consolidation Guide](entity-consolidation.md)** for full details.

---

## Related Topics

- **[Entity Consolidation](entity-consolidation.md)** — Cross-FG retrieval and materialization
- **[Point-in-Time Joins](../concepts/point-in-time-joins.md)** — Preventing data leakage
- **[Training-to-Serving Parity](../concepts/training-to-serving.md)** — Ensuring consistency
- **[ML Engineer Path](../getting-started/ml-engineer-path/)** — Complete feature store tutorial

---

## API Reference

- **[Feature Store API](../api/featurestore.md)** — Complete API documentation
- **[CLI Commands](../reference/cli.md)** — Feature store CLI commands

---

*Coming soon: Advanced patterns, performance optimization, and production deployment strategies.*