# Training-to-Serving Parity

Ensure consistency between features used for model training and features served in production.

---

## The Problem

When features change between training and serving, models make incorrect predictions. This is the "training-serving skew" problem.

**Example**:
- **Training**: Feature uses data available at prediction time
- **Serving**: Feature uses different data or calculation
- **Result**: Model performance degrades

---

## Point-in-Time Consistency

Seeknal ensures point-in-time joins prevent data leakage:

```python
from seeknal.featurestore.duckdbengine.feature_group import HistoricalFeaturesDuckDB

# Features as they appeared at prediction time
hist = HistoricalFeaturesDuckDB(
    lookups=[customer_lookup, product_lookup],
    feature_start_time=datetime(2024, 1, 1)
)

# Get training data with point-in-time correctness
training_df = hist.to_dataframe(
    spine_df=prediction_times,
    feature_start_time=datetime(2024, 1, 1)
)
```

---

## Training Workflow

### 1. Create Training Data

```python
# Get historical features
training_df = hist.to_dataframe(
    spine_df=label_df,  # Your labels
    feature_start_time=datetime(2024, 1, 1)
)

# Merge with labels
training_data = label_df.merge(
    training_df,
    on=["customer_id", "prediction_time"]
)

# Save for training
training_data.to_parquet("training_data.parquet")
```

### 2. Train Model

```python
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# Load training data
df = pd.read_parquet("training_data.parquet")

# Separate features and labels
X = df.drop(["churn", "customer_id"], axis=1)
y = df["churn"]

# Train model
model = RandomForestClassifier()
model.fit(X, y)
```

### 3. Store Feature Schema

```python
# Save feature names for serving
feature_names = list(X.columns)
import json
with open("feature_schema.json", "w") as f:
    json.dump(feature_names, f)
```

---

## Serving Workflow

### 1. Serve Features Online

```python
from seeknal.featurestore.duckdbengine.feature_group import OnlineFeaturesDuckDB

# Deploy online feature table
online_table = hist.serve(name="churn_features_online")
```

### 2. Get Features for Prediction

```python
# Load feature schema
with open("feature_schema.json", "r") as f:
    feature_names = json.load(f)

# Get features for specific customer
features = online_table.get_features(
    keys=[{"customer_id": "CUST001"}],
    feature_names=feature_names
)
```

### 3. Make Prediction

```python
# Prepare features in correct order
X = [[features[f] for f in feature_names]]

# Make prediction
prediction = model.predict(X)[0]
probability = model.predict_proba(X)[0][1]

print(f"Churn risk: {probability:.2%}")
```

---

## Ensuring Parity

### 1. Feature Versioning

```python
# Version your feature groups
fg.write(
    feature_start_time=datetime(2024, 1, 1),
    version="v1"
)
```

### 2. Consistency Validation

```python
# Compare training and serving features
def validate_parity(training_features, serving_features):
    missing = set(training_features) - set(serving_features)
    extra = set(serving_features) - set(training_features)

    if missing or extra:
        raise ValueError(f"Feature mismatch: missing={missing}, extra={extra}")
```

### 3. Model Monitoring

```python
# Monitor prediction drift
def monitor_predictions(model, features, expected_distribution):
    prediction = model.predict(features)
    # Compare to expected distribution
    # Alert if significant drift
```

---

## Best Practices

1. **Always use point-in-time joins** for training data
2. **Version feature groups** used in training
3. **Store feature schema** with model
4. **Validate parity** before deploying
5. **Monitor drift** in production

---

## Common Pitfalls

### Using Future Data

**Wrong**: Using data from after prediction time in training

```python
# DON'T: This leaks future information
df["total_spend_30d_after"] = ...
```

**Correct**: Only use data available at prediction time

```python
# DO: Use historical data
df["total_spend_30d_before"] = ...
```

### Inconsistent Feature Calculations

**Wrong**: Different calculation in training vs serving

```python
# Training
feature = spend / transactions

# Serving
feature = spend / (transactions + 1)  # Different!
```

**Correct**: Use identical calculations

```python
# Both training and serving
feature = spend / max(transactions, 1)
```

---

## Related Topics

- [Point-in-Time Joins](../../concepts/point-in-time-joins.md) - Preventing data leakage
- [Feature Groups](../building-blocks/feature-groups.md) - Feature organization
- [Training to Serving Guide](../../guides/training-to-serving.md) - Complete workflow

---

**Congratulations!** You've completed the ML Engineer path.

**Next Steps**:
- Explore [Data Engineer Path](../data-engineer-path/) - Learn infrastructure
- Explore [Analytics Engineer Path](../analytics-engineer-path/) - Learn metrics
- Dive into [Building Blocks](../../building-blocks/) - Advanced topics
