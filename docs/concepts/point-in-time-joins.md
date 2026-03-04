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

### Using FeatureFrame in Pipeline Transforms (Recommended)

The simplest way to perform PIT joins is within a `@transform` function using `FeatureFrame.pit_join()`:

```python
from seeknal.pipeline import transform

@transform(
    name="training_data",
    description="PIT-joined training data"
)
def training_data(ctx):
    """Create training data with point-in-time correctness."""
    # Spine with entity keys and prediction dates
    labels = ctx.ref("source.churn_labels")  # has user_id, application_date, label

    # PIT join: get features as of each application_date
    training_df = ctx.ref("feature_group.user_transactions").pit_join(
        spine=labels,
        date_col="application_date",
        keep_cols=["label"],
    )

    return training_df
```

### Point-in-Time Join with Spine

A **spine** is a DataFrame containing entity keys and application dates (when predictions should be made). This is the most common pattern for training data preparation.

```python
@transform(name="training_data")
def training_data(ctx):
    # Create spine with prediction dates
    spine = ctx.ref("source.churn_labels")  # user_id, application_date, label

    # PIT join: features as they existed at application_date
    training_df = ctx.ref("feature_group.user_transactions").pit_join(
        spine=spine,
        date_col='application_date',
        keep_cols=['label']  # Keep ground truth labels
    )

    # Result: features joined correctly at each application_date
    # user_001 on Jan 15 will only see transaction from Jan 10 ($50)
    # user_001 on Jan 25 will see transactions from Jan 10 and Jan 20 ($250 total)
    return training_df
```

### Multiple Feature Groups

Join features from multiple feature groups with a single spine:

```python
@transform(name="training_data")
def training_data(ctx):
    labels = ctx.ref("source.churn_labels")

    # PIT join from first feature group
    df = ctx.ref("feature_group.purchase_features").pit_join(
        spine=labels,
        date_col="application_date",
        keep_cols=["label"],
    )

    # Merge with second feature group (already PIT-correct)
    df = ctx.ref("feature_group.clickstream_features").pit_join(
        spine=df,  # Use previous result as spine
        date_col="application_date",
    )

    return df
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

When serving features online, use `ctx.features()` for real-time feature lookup:

```python
from seeknal.pipeline import transform

@transform(name="prediction")
def prediction(ctx):
    """Online feature lookup for inference."""
    # Get latest features for entity keys
    features = ctx.features("user", [
        "transactions.total_spend",
        "transactions.order_count",
    ])

    # Features are retrieved as of "now" with temporal ordering
    # Only includes data that has been materialized
    return features
```

## Debugging Point-in-Time Joins

### Verify Temporal Alignment

```python
# In REPL, check your data before joining
SELECT user_id, transaction_date, amount
FROM source_transactions
ORDER BY transaction_date;

# Check your spine (labels) dates
SELECT user_id, application_date, label
FROM source_churn_labels
ORDER BY application_date;

# After PIT join in transform, verify in REPL
SELECT user_id, application_date, event_time, amount
FROM transform_training_data
WHERE event_time > application_date;  -- Should return 0 rows

# If any rows returned, there's data leakage!
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
