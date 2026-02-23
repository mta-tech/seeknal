# Point-in-Time Joins

Point-in-time (PIT) joins are the most critical concept in feature engineering for machine learning. They ensure that features used for training reflect only the data that was available at prediction time, preventing **data leakage**.

## Why Point-in-Time Joins Matter

### The Data Leakage Problem

Without PIT joins, your model may train on "future" data that wouldn't be available during inference, leading to artificially inflated training performance but poor production results.

Consider predicting customer churn on January 15th. Without PIT joins, you might accidentally include:

- Transactions from January 20th (future data)
- Features computed after the prediction date
- Events that occurred after the customer churned

This creates **data leakage** where the model learns patterns from data it won't have access to in production.

### Concrete Example

```
Timeline:
────────────────────────────────────────────────────>
Jan 1    Jan 10     Jan 15     Jan 20     Jan 30

Events:    purchase   PREDICTION   purchase    purchase
           $50        (here)       $200        $100

Without PIT join: avg_spend = ($50 + $200 + $100) / 3 = $116.67  ❌ Uses future data!
With PIT join:    avg_spend = $50 / 1 = $50.00                   ✅ Only past data
```

### How Point-in-Time Joins Work

Point-in-time joins retrieve features as they existed at or before a specific timestamp. For each prediction point:

1. **Identify the application date** - When the prediction is being made
2. **Look backward only** - Include only events with event_time ≤ application_date
3. **Get the most recent features** - For each entity, retrieve the latest feature values before the application date

This ensures temporal consistency between training and serving.

## Key Parameters

### `event_time_col`

The timestamp column in your data that indicates when each row was created or observed. This is the temporal anchor for point-in-time joins.

```python
materialization = Materialization(event_time_col="transaction_date")
```

Common names:
- `event_time`, `timestamp`, `created_at`
- `transaction_date`, `purchase_date`
- `observation_time`, `recorded_at`

**Important**: This must be the time when the event actually occurred, not when it was processed or ingested.

### `feature_start_time`

The earliest timestamp to include when computing features. Controls the lookback window.

```python
fg.write(feature_start_time=datetime(2024, 1, 1))

# Only includes features from Jan 1, 2024 onwards
training_df = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))
```

This parameter allows you to:
- Filter historical data to a relevant time window
- Exclude stale or outdated features
- Control the training data date range

### `feature_end_time`

Optional end timestamp for the feature window. Useful for creating training/test splits or backfilling historical features.

```python
# Get features for Q1 2024 only
training_df = hist.to_dataframe(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 3, 31)
)
```

## Code Example

### Basic Point-in-Time Join

```python
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    FeatureLookup,
    Materialization,
)
from seeknal.entity import Entity
from datetime import datetime
import pandas as pd

# Define entity and feature group
user_entity = Entity(name="user", join_keys=["user_id"])

materialization = Materialization(
    event_time_col="transaction_date",  # Temporal anchor
    offline=True
)

fg = FeatureGroupDuckDB(
    name="user_transactions",
    entity=user_entity,
    materialization=materialization,
    project="my_project"
)

# Prepare transaction data
transactions_df = pd.DataFrame({
    'user_id': ['user_001', 'user_001', 'user_002'],
    'transaction_date': [
        datetime(2024, 1, 10),
        datetime(2024, 1, 20),  # Future data for Jan 15 prediction
        datetime(2024, 1, 12)
    ],
    'amount': [50.0, 200.0, 75.0]
})

# Write features
fg.set_dataframe(transactions_df).set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))

# Read with PIT join - only includes data up to the label timestamp
lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])

# Get features as of Jan 15 - transaction on Jan 20 won't be included
training_df = hist.to_dataframe(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 1, 15)
)
```

### Point-in-Time Join with Spine

A **spine** is a DataFrame containing entity keys and application dates (when predictions should be made). This is the most common pattern for training data preparation.

```python
# Create spine with prediction dates
spine_df = pd.DataFrame({
    'user_id': ['user_001', 'user_001', 'user_002'],
    'application_date': [
        datetime(2024, 1, 15),  # Predict churn on Jan 15
        datetime(2024, 1, 25),  # Predict again on Jan 25
        datetime(2024, 1, 18)
    ],
    'label': [0, 1, 0]  # Churn labels (ground truth)
})

# Perform point-in-time join
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
training_df = hist.using_spine(
    spine=spine_df,
    date_col='application_date',
    keep_cols=['label']  # Keep ground truth labels
).to_dataframe_with_spine()

# Result: features joined correctly at each application_date
# user_001 on Jan 15 will only see transaction from Jan 10 ($50)
# user_001 on Jan 25 will see transactions from Jan 10 and Jan 20 ($250 total)
```

### Multiple Feature Groups

```python
# Define multiple feature groups
purchase_fg = FeatureGroupDuckDB(
    name="purchase_features",
    entity=user_entity,
    materialization=Materialization(event_time_col="purchase_date")
)

clickstream_fg = FeatureGroupDuckDB(
    name="clickstream_features",
    entity=user_entity,
    materialization=Materialization(event_time_col="click_timestamp")
)

# Perform point-in-time join across all feature groups
hist = HistoricalFeaturesDuckDB(
    lookups=[
        FeatureLookup(source=purchase_fg),
        FeatureLookup(source=clickstream_fg)
    ]
)

training_df = hist.using_spine(
    spine=spine_df,
    date_col='application_date'
).to_dataframe_with_spine()

# Both feature groups are joined with temporal correctness
```

## How Seeknal Implements Point-in-Time Joins

Seeknal uses DuckDB's window functions to perform efficient point-in-time joins:

```sql
SELECT
    spine.*,
    features.* EXCLUDE (user_id),
    ROW_NUMBER() OVER (
        PARTITION BY spine.user_id, spine.application_date
        ORDER BY features.event_time DESC
    ) as rn
FROM spine
LEFT JOIN features
    ON spine.user_id = features.user_id
    AND features.event_time <= spine.application_date  -- Key temporal constraint
```

The query:
1. Joins spine with features where `event_time <= application_date`
2. Ranks features by event_time descending (most recent first)
3. Keeps only the most recent feature value (`rn = 1`)

This ensures each prediction point gets the latest feature values that were available at that time.

## Comparison to Other Tools

| Feature | Seeknal | Feast | Tecton | Feature Store on AWS |
|---------|---------|-------|--------|---------------------|
| PIT joins | Built-in | Built-in | Built-in | Requires manual implementation |
| Lookback window | `feature_start_time` | `ttl` parameter | `batch_schedule` | Custom SQL |
| Implementation | DuckDB (fast, single-node) | Redis/BigQuery | Managed service | S3 + Athena |
| Ease of use | High | Medium | High | Low |
| Cost | Free (open source) | Free (open source) | Paid | AWS costs |
| Spine support | Native | Native | Native | Manual |

### Seeknal Advantages

1. **No infrastructure required** - DuckDB runs in-process
2. **Fast iteration** - Test PIT joins locally before production
3. **Simple API** - Pythonic interface, no complex configs
4. **Transparent SQL** - Inspect generated queries for debugging
5. **Zero cost** - Open source, no usage fees

## Common Pitfalls

### 1. Forgetting `event_time_col`

```python
# ❌ Wrong - no temporal column specified
materialization = Materialization(offline=True)

# ✅ Correct - specify event_time_col
materialization = Materialization(
    event_time_col="transaction_date",
    offline=True
)
```

Without `event_time_col`, Seeknal cannot determine temporal ordering, and point-in-time joins will fail or produce incorrect results.

### 2. Too Narrow `feature_start_time`

```python
# ❌ Wrong - only 7 days of history
training_df = hist.to_dataframe(
    feature_start_time=datetime(2024, 3, 24),  # Too recent
    feature_end_time=datetime(2024, 3, 31)
)
```

**Problem**: Insufficient historical data means missing important patterns.

**Solution**: Use longer lookback windows (30-90 days for behavioral features).

```python
# ✅ Correct - 90 days of history
training_df = hist.to_dataframe(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 3, 31)
)
```

### 3. Too Wide `feature_start_time`

```python
# ❌ Wrong - includes very old, stale data
training_df = hist.to_dataframe(
    feature_start_time=datetime(2020, 1, 1),  # 4 years ago
    feature_end_time=datetime(2024, 3, 31)
)
```

**Problem**: Old data may not reflect current user behavior patterns.

**Solution**: Balance recency with sufficient history. Use domain knowledge to determine appropriate lookback windows.

### 4. Using Processing Time Instead of Event Time

```python
# ❌ Wrong - using ingestion timestamp
df['event_time'] = datetime.now()  # When data was processed

# ✅ Correct - using actual event timestamp
df['event_time'] = df['transaction_date']  # When event occurred
```

**Always use the timestamp when the event actually happened**, not when it was processed or ingested into your system.

### 5. Inconsistent Time Zones

```python
# ❌ Wrong - mixed time zones
spine_df['application_date'] = pd.to_datetime(['2024-01-15'])  # Naive datetime
features_df['event_time'] = pd.to_datetime(['2024-01-15 10:00:00'], utc=True)  # UTC

# ✅ Correct - consistent time zones
spine_df['application_date'] = pd.to_datetime(['2024-01-15'], utc=True)
features_df['event_time'] = pd.to_datetime(['2024-01-15 10:00:00'], utc=True)
```

**Always use timezone-aware datetimes** and ensure consistency across spine and feature data.

## Advanced Patterns

### Sliding Window Features

Compute features over fixed time windows before each prediction:

```python
# 7-day and 30-day sliding windows
spine_df = pd.DataFrame({
    'user_id': ['user_001', 'user_001'],
    'application_date': [
        datetime(2024, 1, 15),
        datetime(2024, 2, 1)
    ]
})

# For each application_date, aggregate features from the past 7 days
# This requires custom SQL in your feature engineering
conn = duckdb.connect()
conn.register("transactions", transactions_df)
conn.register("spine", spine_df)

windowed_features = conn.execute("""
    SELECT
        spine.user_id,
        spine.application_date,
        COUNT(*) as txn_count_7d,
        SUM(t.amount) as total_amount_7d
    FROM spine
    LEFT JOIN transactions t
        ON spine.user_id = t.user_id
        AND t.transaction_date BETWEEN
            spine.application_date - INTERVAL 7 DAY
            AND spine.application_date
    GROUP BY spine.user_id, spine.application_date
""").df()
```

### Online Serving with Point-in-Time Consistency

When serving features online, point-in-time logic ensures production predictions use only available data:

```python
# Materialize latest features to online store
online_table = hist.serve(name="user_features_online")

# At serving time, features are retrieved as of "now"
# Automatically respects temporal ordering
features = online_table.get_features(
    keys=[{"user_id": "user_001"}]
)

# Only includes transactions that have occurred before this moment
```

## Debugging Point-in-Time Joins

### Verify Temporal Alignment

```python
# Check event times in your features
print(transactions_df[['user_id', 'transaction_date', 'amount']].sort_values('transaction_date'))

# Check spine dates
print(spine_df[['user_id', 'application_date']].sort_values('application_date'))

# After PIT join, verify no future data leakage
result = hist.using_spine(spine_df, date_col='application_date').to_dataframe_with_spine()

# Each row should only contain features with event_time <= application_date
for idx, row in result.iterrows():
    app_date = row['application_date']
    event_time = row.get('event_time', None)
    if event_time and event_time > app_date:
        print(f"WARNING: Future data leakage detected at index {idx}")
```

### Inspect Generated SQL

Seeknal uses DuckDB's explain plan to show the actual join logic:

```python
import duckdb
conn = duckdb.connect()

# Register your data
conn.register("spine", spine_df)
conn.register("features", transactions_df)

# Explain the point-in-time join
explain_query = conn.execute("""
    EXPLAIN SELECT * FROM spine
    LEFT JOIN features
        ON spine.user_id = features.user_id
        AND features.event_time <= spine.application_date
""")

print(explain_query.fetchall())
```

## Best Practices

1. **Always specify `event_time_col`** - Required for temporal correctness
2. **Use timezone-aware datetimes** - Prevent timezone-related bugs
3. **Validate time alignment** - Ensure no future data leakage in training sets
4. **Test with spine** - Use explicit application dates for reproducibility
5. **Start with DuckDB** - Fast iteration locally, then scale to Spark if needed
6. **Monitor feature freshness** - Track how old features are at serving time
7. **Document lookback windows** - Make feature time dependencies explicit

## See Also

- **Concepts**: [Data Leakage](glossary.md#data-leakage), [Feature Start Time](glossary.md#feature-start-time), [Materialization](glossary.md#materialization)
- **Tutorials**: [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md), [Python Pipelines Tutorial](../tutorials/python-pipelines-tutorial.md)
- **Guides**: [Training to Serving Guide](../guides/training-to-serving.md)
- **Reference**: [CLI Commands](../reference/cli.md), [Configuration](../reference/configuration.md)

## Summary

Point-in-time joins are essential for preventing data leakage in ML pipelines. Seeknal provides:

- **Automatic temporal correctness** through `event_time_col`
- **Flexible time windows** with `feature_start_time` and `feature_end_time`
- **Spine-based joins** for explicit prediction dates
- **DuckDB-powered performance** for fast local iteration
- **Production-ready semantics** that mirror online serving behavior

By using point-in-time joins consistently, you ensure your training data accurately reflects what your model will see in production, leading to better generalization and more reliable predictions.
