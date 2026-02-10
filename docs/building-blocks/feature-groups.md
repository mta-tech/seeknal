# Feature Groups

Feature groups organize features for machine learning with point-in-time correctness.

---

## Overview

Feature groups are collections of features for a specific entity (e.g., customer, product). They enable feature reuse and prevent data leakage through point-in-time joins.

---

## Feature Group Concepts

### Entities

Entities define the join key for features:

```python
from seeknal.entity import Entity

customer_entity = Entity(
    name="customer",
    join_keys=["customer_id"]
)
```

### Materialization

Control when and how features are computed:

```python
from seeknal.featurestore.duckdbengine.feature_group import Materialization

materialization = Materialization(
    event_time_col="transaction_time",
    lookback_days=30
)
```

---

## Creating Feature Groups

### DuckDB Feature Groups

```python
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB

fg = FeatureGroupDuckDB(
    name="customer_features",
    entity=customer_entity,
    materialization=materialization,
    project="my_project"
)

# Auto-detect features from DataFrame
fg.set_dataframe(df).set_features()

# Write to offline store
fg.write(feature_start_time=datetime(2024, 1, 1))
```

### Spark Feature Groups

```python
from seeknal.featurestore.feature_group import FeatureGroup

fg = FeatureGroup(
    name="customer_features",
    entity=customer_entity,
    project="my_project"
)

# Set features explicitly
fg.set_features({
    "total_purchases": "sum(purchase_amount)",
    "transaction_count": "count(*)"
})
```

---

## Point-in-Time Joins

Prevent data leakage by joining features as they appeared at prediction time:

```python
from seeknal.featurestore.duckdbengine.feature_group import HistoricalFeaturesDuckDB

lookup = FeatureLookup(source=fg)

hist = HistoricalFeaturesDuckDB(
    lookups=[lookup],
    feature_start_time=datetime(2024, 1, 1)
)

# Get features with point-in-time correctness
features_df = hist.to_dataframe(
    spine_df=prediction_times,
    feature_start_time=datetime(2024, 1, 1)
)
```

---

## Online Serving

Deploy features for low-latency serving:

```python
from seeknal.featurestore.duckdbengine.feature_group import OnlineFeaturesDuckDB

online = hist.serve(name="customer_features_online")

# Get features for inference
features = online.get_features(
    keys=[{"customer_id": "CUST001"}]
)
```

---

## Feature Group Configuration

### Common Options

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `name` | string | Unique identifier | Required |
| `entity` | Entity | Entity definition | Required |
| `materialization` | Materialization | Materialization config | Optional |
| `project` | string | Project name | Required |

---

## Best Practices

1. **Always use point-in-time joins** for ML features
2. **Define clear entities** with join keys
3. **Version feature groups** for production
4. **Document feature logic** clearly
5. **Test feature serving** before deployment

---

## Related Topics

- [Point-in-Time Joins](../../concepts/point-in-time-joins.md) - Preventing data leakage
- [Training to Serving](../../guides/training-to-serving.md) - End-to-end ML workflow
- [ML Engineer Path](../../getting-started/ml-engineer-path/) - Complete ML tutorial

---

**Next**: Learn about [Semantic Models](semantic-models.md) or return to [Building Blocks](index.md)
