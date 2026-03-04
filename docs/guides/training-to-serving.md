# Training to Serving: End-to-End ML Feature Workflow

This guide walks you through the complete ML feature workflow in Seeknal, from feature engineering to online serving, ensuring training-serving parity at every step.

## Overview

Seeknal provides a unified workflow from feature engineering to online serving, ensuring that features used in training match exactly what's served in production.

### The Training-Serving Gap

Traditional ML workflows often suffer from the **training-serving gap**:

| Problem | Impact | Seeknal Solution |
|---------|--------|------------------|
| **Different code paths** | Features computed differently in training vs serving | Single feature definition for both |
| **Data leakage** | Future data leaks into training | Point-in-time correct joins |
| **Schema drift** | Production features don't match training | Version-tracked feature groups |
| **Slow serving** | Recomputing features at inference time | Materialized online store |

### Seeknal's Approach

Seeknal solves these problems with:

1. **Single Feature Definition** - Define features once, use everywhere
2. **Point-in-Time Joins** - Automatic PIT-correct historical features
3. **Materialization** - Write features to offline store for training
4. **Online Serving** - Serve features from online store for inference
5. **Version Tracking** - Schema changes are automatically versioned

## Complete Workflow

```
┌─────────────────┐
│ 1. Define       │  Define feature transformations
│    Features     │  (FeatureGroup + Materialization)
└────────┬────────┘
         │
         v
┌─────────────────┐
│ 2. Write to     │  Materialize features to offline store
│    Offline      │  (fg.write() with start/end time)
└────────┬────────┘
         │
         v
┌─────────────────┐
│ 3. Create       │  Point-in-time join for training
│    Training     │  (HistoricalFeatures + spine)
│    Dataset      │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ 4. Train Model  │  Use features with scikit-learn,
│                 │  PyTorch, TensorFlow, etc.
└────────┬────────┘
         │
         v
┌─────────────────┐
│ 5. Serve        │  Serve features from online store
│    Online       │  (OnlineFeatures.get_features())
└─────────────────┘
```

## Step 1: Define Features

Create a feature group with entity definition and materialization config.

### DuckDB Feature Group

For development and small-to-medium datasets (<100M rows):

```python
from datetime import datetime
from seeknal.entity import Entity
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    Materialization,
)
import pandas as pd

# Load your data
df = pd.read_parquet("data/user_activity.parquet")

# Define entity (join keys)
user_entity = Entity(name="user", join_keys=["user_id"])

# Define materialization config
materialization = Materialization(
    event_time_col="event_time",  # Time column for PIT joins
    offline=True,  # Enable offline store
    online=True,   # Enable online store
)

# Create feature group
user_features = FeatureGroupDuckDB(
    name="user_behavior_features",
    entity=user_entity,
    materialization=materialization,
    description="User behavior features for churn prediction",
    project="churn_model_v1"
)

# Attach dataframe and detect features
user_features.set_dataframe(df).set_features()

# Features are auto-detected from DataFrame columns
# (all columns except entity keys and event_time)
print(f"Detected features: {user_features.features}")
```

### Spark Feature Group

For production and large datasets (>100M rows):

```python
from seeknal.featurestore.feature_group import (
    FeatureGroup,
    Materialization,
    OfflineMaterialization,
    OfflineStore,
    OfflineStoreEnum,
    FeatureStoreFileOutput,
)
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("feature_store").getOrCreate()

# Load your data
df = spark.read.parquet("s3://warehouse/user_activity/")

# Define entity and materialization
user_entity = Entity(name="user", join_keys=["user_id"])

materialization = Materialization(
    event_time_col="event_time",
    offline_materialization=OfflineMaterialization(
        store=OfflineStore(
            kind=OfflineStoreEnum.FILE,
            name="s3_offline",
            value=FeatureStoreFileOutput(path="s3://warehouse/feature_store")
        ),
        mode="append",
    ),
    offline=True,
)

# Create feature group
user_features = FeatureGroup(
    name="user_behavior_features",
    entity=user_entity,
    materialization=materialization,
)

user_features.set_dataframe(df).set_features()
```

### Feature Definition Best Practices

```python
# Explicitly define features for production
user_features.set_features([
    "total_sessions_7d",
    "total_sessions_30d",
    "avg_session_duration",
    "pages_per_session",
    "bounce_rate",
    "days_since_last_visit",
    "preferred_category",
    "device_type",
    "conversion_count",
])
```

### Advanced Feature Engineering

For complex feature engineering scenarios like multi-level aggregations (e.g., computing regional totals from user-level metrics), see [Second-Order Aggregations](../concepts/second-order-aggregations.md). This technique enables hierarchical rollups and is particularly useful for building features at different granularities.

## Step 2: Write Feature Data

Materialize features to the offline store for training.

### DuckDB Write

```python
from datetime import datetime

# Write features for a specific time range
user_features.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 12, 31),
    mode="overwrite"  # or "append"
)

# Verify write
print(f"Offline watermarks: {user_features.offline_watermarks}")
```

### Spark Write

```python
# Write features
user_features.get_or_create()  # Save metadata
user_features.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 12, 31),
)

# Features are now in s3://warehouse/feature_store/
```

### Incremental Writes

```python
# Initial write
user_features.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 6, 30),
    mode="overwrite"
)

# Later: append new data
user_features.write(
    feature_start_time=datetime(2024, 7, 1),
    feature_end_time=datetime(2024, 12, 31),
    mode="append"
)
```

## Step 3: Create Training Dataset

Use point-in-time joins to create training datasets without data leakage.

### Point-in-Time Join with FeatureFrame (Recommended)

Within a `@transform` function, use `FeatureFrame.pit_join()` for the simplest API:

```python
from seeknal.pipeline import transform

@transform(
    name="training_data",
    description="PIT-joined training data for churn model"
)
def training_data(ctx):
    """Create training dataset with point-in-time correctness."""
    # Get labels spine (has user_id, application_date, label)
    labels = ctx.ref("source.churn_labels")

    # PIT join: get features as of each application_date
    training_df = ctx.ref("feature_group.user_behavior_features").pit_join(
        spine=labels,
        date_col="application_date",
        keep_cols=["label"],
    )

    return training_df
```

**What Point-in-Time Join Does:**

For each row in the spine:
1. Takes `application_date` from spine
2. Gets features with `event_time <= application_date`
3. Uses the most recent feature value before application_date
4. Ensures no future data leaks into training

### Multiple Feature Groups

Join features from multiple feature groups by chaining PIT joins:

```python
@transform(name="training_data")
def training_data(ctx):
    labels = ctx.ref("source.churn_labels")

    # PIT join from user features
    df = ctx.ref("feature_group.user_behavior_features").pit_join(
        spine=labels,
        date_col="application_date",
        keep_cols=["label"],
    )

    # PIT join from product features (use previous result as spine)
    df = ctx.ref("feature_group.product_features").pit_join(
        spine=df,
        date_col="application_date",
    )

    return df
```

### Handle Missing Features

```python
@transform(name="training_data")
def training_data(ctx):
    labels = ctx.ref("source.churn_labels")

    training_df = ctx.ref("feature_group.user_behavior_features").pit_join(
        spine=labels,
        date_col="application_date",
        keep_cols=["label"],
    )

    # Fill nulls using DuckDB COALESCE
    return ctx.duckdb.sql("""
        SELECT
            user_id,
            application_date,
            label,
            COALESCE(total_sessions_7d, 0) AS total_sessions_7d,
            COALESCE(avg_session_duration, 0.0) AS avg_session_duration,
            COALESCE(conversion_count, 0) AS conversion_count
        FROM training_df
    """).df()
```

## Step 4: Train Your Model

Use the training dataset with standard ML frameworks.

### Scikit-Learn Example

```python
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score

# Prepare data
feature_cols = [
    "total_sessions_7d",
    "avg_session_duration",
    "pages_per_session",
    "conversion_count"
]

X = training_df[feature_cols]
y = training_df['label']

# Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Train
model = RandomForestClassifier(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, y_pred)
print(f"AUC: {auc:.4f}")

# Save model
import joblib
joblib.dump(model, "churn_model.pkl")
```

### PyTorch Example

```python
import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader

class ChurnDataset(Dataset):
    def __init__(self, features, labels):
        self.features = torch.FloatTensor(features.values)
        self.labels = torch.FloatTensor(labels.values)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        return self.features[idx], self.labels[idx]

# Prepare data
X = training_df[feature_cols].fillna(0)
y = training_df['label']

dataset = ChurnDataset(X, y)
dataloader = DataLoader(dataset, batch_size=32, shuffle=True)

# Define model
class ChurnModel(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, 64)
        self.fc2 = nn.Linear(64, 32)
        self.fc3 = nn.Linear(32, 1)
        self.dropout = nn.Dropout(0.2)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = self.dropout(x)
        x = torch.relu(self.fc2(x))
        x = torch.sigmoid(self.fc3(x))
        return x

model = ChurnModel(input_dim=len(feature_cols))
criterion = nn.BCELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# Train
for epoch in range(10):
    for features, labels in dataloader:
        optimizer.zero_grad()
        outputs = model(features)
        loss = criterion(outputs.squeeze(), labels)
        loss.backward()
        optimizer.step()

# Save model
torch.save(model.state_dict(), "churn_model.pth")
```

## Step 5: Serve Features Online

Deploy features to the online store for low-latency inference.

### Retrieve Features with ctx.features()

Within a `@transform` function, use `ctx.features()` for online feature lookup:

```python
from seeknal.pipeline import transform

@transform(name="predictions")
def predictions(ctx):
    """Generate predictions using online features."""
    # Get latest features from consolidated entity store
    features = ctx.features("user", [
        "user_behavior.total_sessions_7d",
        "user_behavior.avg_session_duration",
        "user_behavior.pages_per_session",
        "user_behavior.conversion_count",
    ])

    return features
```

### Retrieve Features for Inference

```python
@transform(name="churn_predictions")
def churn_predictions(ctx):
    import joblib

    # Load trained model
    model = joblib.load("models/churn_model.pkl")

    # Get latest features for all users
    features_df = ctx.features("user", [
        "user_behavior.total_sessions_7d",
        "user_behavior.avg_session_duration",
        "user_behavior.pages_per_session",
        "user_behavior.conversion_count",
    ])

    # Make predictions
    feature_cols = [
        "total_sessions_7d",
        "avg_session_duration",
        "pages_per_session",
        "conversion_count",
    ]

    X = features_df[feature_cols].fillna(0)
    features_df["churn_probability"] = model.predict_proba(X)[:, 1]

    return features_df[["user_id", "churn_probability"]]
```

### Batch Inference

```python
@transform(name="batch_predictions")
def batch_predictions(ctx):
    import joblib

    model = joblib.load("models/churn_model.pkl")

    # Get features for all users in the entity store
    features_df = ctx.features("user", [
        "user_behavior.total_sessions_7d",
        "user_behavior.avg_session_duration",
        "user_behavior.conversion_count",
    ])

    # Batch prediction
    feature_cols = ["total_sessions_7d", "avg_session_duration", "conversion_count"]
    X = features_df[feature_cols].fillna(0)
    features_df["churn_probability"] = model.predict_proba(X)[:, 1]

    return features_df[["user_id", "churn_probability"]]
```

## Training-Serving Parity

### Why Parity Matters

| Without Parity | With Seeknal |
|----------------|--------------|
| Features computed differently in training vs serving | Single feature definition |
| Manual alignment required | Automatic consistency |
| Production bugs from mismatches | Guaranteed correctness |
| Separate codebases to maintain | One codebase |

### How Seeknal Ensures Parity

1. **Single Feature Definition**
   - Feature group defines features once
   - Same code path for training and serving

2. **Versioned Schemas**
   - Schema changes create new versions
   - Training and serving use the same version

3. **Materialization**
   - Offline store: Batch write for training
   - Online store: Low-latency read for serving
   - Same underlying data

4. **Type Safety**
   - Feature types enforced at write time
   - Consistent types in training and serving

### Verifying Parity

In REPL, verify that training and serving use the same features:

```sql
-- Check feature schema from consolidated entity
DESCRIBE entity_user;

-- Check offline training data schema
DESCRIBE transform_training_data;

-- Compare column names and types
SELECT column_name, data_type
FROM (DESCRIBE transform_training_data)
ORDER BY column_name;
```

Or in Python:

```python
# Check entity catalog
!seeknal entity show user

# Verify features are available
features = ctx.features("user", [
    "user_behavior.total_sessions_7d",
    "user_behavior.avg_session_duration",
])
print(f"Features available: {list(features.columns)}")
```

## Production Patterns

### Pattern 1: Feature Backfill

```python
# Initial backfill
for month in pd.date_range('2024-01-01', '2024-12-31', freq='MS'):
    start = month
    end = month + pd.DateOffset(months=1) - pd.DateOffset(days=1)

    print(f"Backfilling {start.date()} to {end.date()}")

    user_features.write(
        feature_start_time=start,
        feature_end_time=end,
        mode="append"
    )
```

### Pattern 2: Daily Feature Updates

```bash
# Daily batch job - run the pipeline to update features
seeknal run --nodes feature_group.user_behavior_features

# Entity consolidation happens automatically after run
# Features are now available via ctx.features()
```

### Pattern 3: Model Retraining

Use a transform node for retraining:

```python
from seeknal.pipeline import transform
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
import joblib

@transform(name="retrain_model")
def retrain_model(ctx):
    """Retrain model with latest data."""
    from datetime import datetime, timedelta

    # 1. Get training data (from PIT-joined transform)
    training_df = ctx.ref("transform.training_data")

    # 2. Train new model
    feature_cols = ["total_sessions_7d", "avg_session_duration", "conversion_count"]
    X = training_df[feature_cols].fillna(0)
    y = training_df["label"]

    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X, y)

    # 3. Validate
    # (In production, you'd use a separate test set)
    y_pred = model.predict_proba(X)[:, 1]
    auc = roc_auc_score(y, y_pred)

    # 4. Save model if good
    if auc > 0.75:
        joblib.dump(model, "models/churn_model_latest.pkl")
        print(f"Model saved with AUC: {auc:.4f}")

    return pd.DataFrame([{"auc": auc, "model_saved": auc > 0.75}])
```

Run retraining:

```bash
# Run the retraining transform
seeknal run --nodes transform.retrain_model
```

### Pattern 4: Feature Monitoring

```python
@transform(name="feature_drift_report")
def feature_drift_report(ctx):
    """Monitor feature distributions for drift."""
    from datetime import datetime, timedelta

    # Get current features
    current = ctx.features("user", [
        "user_behavior.total_sessions_7d",
        "user_behavior.avg_session_duration",
    ])

    # Compare to baseline (stored in a reference table)
    # In REPL, you can query historical snapshots:
    # SELECT * FROM feature_group_user_behavior_features
    # WHERE event_time >= '2024-01-01' AND event_time < '2024-01-08';

    # Check for drift
    feature_cols = ["total_sessions_7d", "avg_session_duration"]
    drift_report = []

    for col in feature_cols:
        current_mean = current[col].mean()
        # Compare to stored baseline mean
        # baseline_mean = ... (load from reference)
        # drift = abs(current_mean - baseline_mean) / baseline_mean

        drift_report.append({
            "feature": col,
            "current_mean": current_mean,
            # "baseline_mean": baseline_mean,
            # "drift_pct": drift,
        })

    return pd.DataFrame(drift_report)
```

In REPL, you can monitor feature distributions:

```sql
-- Check recent feature distributions
SELECT
    'total_sessions_7d' as feature,
    AVG(total_sessions_7d) as mean_value,
    STDDEV(total_sessions_7d) as stddev,
    MIN(total_sessions_7d) as min_value,
    MAX(total_sessions_7d) as max_value
FROM feature_group_user_behavior_features
WHERE event_time >= CURRENT_DATE - INTERVAL 7 DAY;
```

## Best Practices

### 1. Use Consistent Time Columns

```python
# Good: Consistent naming
materialization = Materialization(event_time_col="event_time")

# Bad: Inconsistent naming
materialization = Materialization(event_time_col="timestamp")  # Different from other groups
```

### 2. Version Your Features

```python
# Include version in project name
user_features = FeatureGroupDuckDB(
    name="user_behavior_features",
    entity=user_entity,
    project="churn_model_v2"  # Version in project
)
```

### 3. Document Features

```python
user_features = FeatureGroupDuckDB(
    name="user_behavior_features",
    entity=user_entity,
    description="""
    User behavior features for churn prediction model v2.

    Features:
    - total_sessions_7d: Count of sessions in last 7 days
    - avg_session_duration: Average session duration in seconds
    - conversion_count: Number of conversions in last 30 days

    Updated: Daily at 2am UTC
    Owner: ml-team@company.com
    """,
)
```

### 4. Test Features Before Production

```python
# Validate features on small subset
sample_df = df.sample(n=1000)
user_features.set_dataframe(sample_df).set_features()

# Test write
user_features.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 1, 7),
    mode="overwrite"
)

# Test read
hist = HistoricalFeaturesDuckDB(lookups=[FeatureLookup(source=user_features)])
test_df = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))

assert len(test_df) > 0, "No features retrieved"
assert not test_df.isnull().all().any(), "Features have all nulls"
```

### 5. Monitor Feature Freshness

```python
# Check watermarks
fg = FeatureGroupDuckDB(name="user_behavior_features")
fg.get_or_create()

latest_watermark = max(fg.offline_watermarks) if fg.offline_watermarks else None
if latest_watermark:
    age = datetime.now() - latest_watermark
    if age > timedelta(days=2):
        print(f"WARNING: Features are {age.days} days old")
```

## Troubleshooting

### "No features retrieved from offline store"

**Cause:** Date range doesn't overlap with written data.

**Solution:**
```python
# Check watermarks
print(f"Offline watermarks: {user_features.offline_watermarks}")

# Adjust date range
training_df = hist.to_dataframe(
    feature_start_time=min(user_features.offline_watermarks)
)
```

### "Point-in-time join returns empty DataFrame"

**Cause:** Spine dates are before feature data.

**Solution:**
```python
# Check spine dates vs feature dates
print(f"Spine min date: {spine['application_date'].min()}")
print(f"Features min date: {min(user_features.offline_watermarks)}")

# Ensure spine dates are within feature range
```

### "Features in serving don't match training"

**Cause:** Online store not refreshed after offline write.

**Solution:**
```python
# Always refresh online store after offline write
user_features.write(feature_start_time=datetime(2024, 1, 1))

# Then serve
hist = HistoricalFeaturesDuckDB(lookups=[FeatureLookup(source=user_features)])
hist.serve(name="user_features_online")
```

## See Also

- **Concepts**: [Point-in-Time Joins](../concepts/point-in-time-joins.md), [Feature Group](../concepts/glossary.md#feature-group), [Materialization](../concepts/glossary.md#materialization)
- **Reference**: [CLI Materialize Command](../reference/cli.md#seeknal-materialize), [Configuration Reference](../reference/configuration.md)
- **Guides**: [Testing & Audits](./testing-and-audits.md), [Comparison to Feast](./comparison.md), [Semantic Layer](./semantic-layer.md)
- **Tutorials**: [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md), [Python Pipelines Tutorial](../tutorials/python-pipelines-tutorial.md)
