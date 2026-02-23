# Chapter 1: Build Feature Stores

> **Duration:** 30 minutes | **Difficulty:** Intermediate | **Format:** Python API

Learn to build production feature stores that prevent data leakage and ensure training-serving parity for ML models.

---

## What You'll Build

A feature store for customer churn prediction that:

```
Customer Events → Feature Group → Point-in-Time Join → Training Data
                       ↓
                 Online Serving → Inference Features
```

**After this chapter, you'll have:**
- Feature groups with entities and join keys
- Point-in-time joins to prevent data leakage
- Historical feature retrieval for training
- Online feature serving for inference
- Feature versioning for reproducibility

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Quick Start](../../quick-start/) — Basic Seeknal workflow
- [ ] [MLE Path Overview](index.md) — Introduction to this path
- [ ] Python 3.11+ with pandas and pyarrow installed
- [ ] Understanding of ML features and training data

---

## Part 1: Understand Feature Store Concepts (8 minutes)

### What Are Feature Stores?

Feature stores solve the **training-serving skew** problem:

| Problem | Traditional Approach | Feature Store Approach |
|---------|---------------------|----------------------|
| **Training Features** | SQL script computed at training time | Stored in offline feature store |
| **Serving Features** | Different code for inference | Same features from online store |
| **Consistency** | Manual verification required | Guaranteed by design |
| **Versioning** | Ad-hoc feature versions | Automatic schema versioning |

!!! tip "Why Feature Stores?"
    - **No Data Leakage**: Point-in-time joins prevent future data
    - **Reusability**: Define once, use for multiple models
    - **Consistency**: Same features in training and production
    - **Performance**: Pre-computed features for fast serving

### Key Components

=== "Entities"

    **Entities** are the objects you want to make predictions about:

    ```python
    from seeknal.entity import Entity

    customer = Entity(
        name="customer",
        join_keys=["customer_id"],
        description="A customer in our system"
    )

    product = Entity(
        name="product",
        join_keys=["product_id"],
        description="A product in our catalog"
    )
    ```

    **Join keys** uniquely identify entities and enable joins.

=== "Feature Groups"

    **Feature groups** are collections of related features:

    ```python
    from seeknal.featurestore.duckdbengine.feature_group import (
        FeatureGroupDuckDB,
        Materialization,
    )
    from datetime import datetime

    fg = FeatureGroupDuckDB(
        name="customer_features",
        entity=customer,
        description="Customer behavior and transaction features",
        materialization=Materialization(
            event_time_col="event_timestamp",
        ),
    )
    ```

    Feature groups have:
    - **Name**: Unique identifier
    - **Entity**: What the features describe
    - **Features**: The actual feature columns
    - **Event Time**: Temporal column for point-in-time joins

=== "Point-in-Time Joins"

    **Point-in-time (PIT) joins** ensure historical correctness:

    ```
    Timeline:
    ──────────────────────────────────────────────────────→
    Jan 1    Jan 10     Jan 15     Jan 20     Jan 30

    Events:    purchase   PREDICTION   purchase    purchase
               $50        (here)       $200        $100

    Without PIT: avg_spend = ($50 + $200 + $100) / 3 = $116.67  ❌
    With PIT:    avg_spend = $50 / 1 = $50.00                   ✅
    ```

    Only data **before** the prediction point is used!

---

## Part 2: Create Feature Group (10 minutes)

### Load Sample Data

First, create some sample customer transaction data:

```python
#!/usr/bin/env python3
"""
Feature Store Demo - Chapter 1
Building customer churn prediction features
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from seeknal.entity import Entity
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    OnlineFeaturesDuckDB,
    FeatureLookup,
    Materialization,
)
import pyarrow as pa

# Generate sample customer transaction data
np.random.seed(42)
dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')

transactions = []
for date in dates:
    for customer_id in range(1, 101):  # 100 customers
        # Random number of transactions per day (0-3)
        num_transactions = np.random.randint(0, 4)

        for _ in range(num_transactions):
            transactions.append({
                'customer_id': f'C{customer_id:03d}',
                'event_timestamp': date + pd.Timedelta(hours=np.random.randint(0, 24)),
                'transaction_amount': np.random.uniform(10, 500),
                'product_category': np.random.choice(['Electronics', 'Clothing', 'Home', 'Sports']),
                'is_return': np.random.choice([0, 1], p=[0.9, 0.1]),
            })

df = pd.DataFrame(transactions)
print(f"Generated {len(df)} transactions for {df['customer_id'].nunique()} customers")
print(f"Date range: {df['event_timestamp'].min()} to {df['event_timestamp'].max()}")
```

**Expected output:**
```
Generated 4237 transactions for 100 customers
Date range: 2024-01-01 00:00:00 to 2024-01-31 23:00:00
```

### Define Entity and Feature Group

```python
# Define the customer entity
customer_entity = Entity(
    name="customer",
    join_keys=["customer_id"],
    description="Customer for churn prediction",
)

# Create feature group
fg = FeatureGroupDuckDB(
    name="customer_transaction_features",
    entity=customer_entity,
    description="Customer transaction behavior features for churn prediction",
    materialization=Materialization(
        event_time_col="event_timestamp",
    ),
)

# Set the input DataFrame
fg.set_dataframe(df)

# Define features programmatically
fg.set_features()
```

**What happened?**
- Created an entity for customers
- Created a feature group with event timestamp
- Set features automatically from DataFrame schema

---

## Part 3: Write Feature Group (5 minutes)

### Write to Offline Store

```python
# Write features to offline store
print("Writing features to offline store...")
fg.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 1, 31),
)

print(f"Feature group '{fg.name}' written successfully!")
print(f"Features: {list(fg.features.keys())}")
```

**Expected output:**
```
Writing features to offline store...
Feature group 'customer_transaction_features' written successfully!
Features: ['customer_id', 'event_timestamp', 'transaction_amount', 'product_category', 'is_return']
```

!!! info "Feature Versions"
    Seeknal automatically versions feature groups when schemas change:
    - Version 1: Initial schema
    - Version 2: Added new feature
    - Version 3: Changed feature type

    Use `seeknal version list <fg_name>` to see all versions.

---

## Part 4: Create Historical Features (7 minutes)

### Understanding Historical Feature Retrieval

For training data, you need **historically accurate** features:

```python
# Create prediction points (spine DataFrame)
prediction_dates = pd.date_range(start='2024-01-15', end='2024-01-31', freq='D')

# Sample customers for prediction
spine_df = pd.DataFrame({
    'customer_id': [f'C{i:03d}' for i in range(1, 11)],  # 10 customers
    'prediction_timestamp': np.random.choice(prediction_dates, 10),
})

print(f"Created spine with {len(spine_df)} prediction points")
print(spine_df.head())
```

**Expected output:**
```
Created spine with 10 prediction points
  customer_id prediction_timestamp
0        C001 2024-01-20
1        C002 2024-01-28
2        C003 2024-01-17
...
```

### Point-in-Time Correct Features

```python
# Create historical feature lookup
hist = HistoricalFeaturesDuckDB(
    lookups=[
        FeatureLookup(
            source=fg,
            feature_names=[
                'transaction_amount',
                'product_category',
                'is_return',
            ],
        )
    ],
)

# Get historical features (point-in-time correct!)
print("Computing historical features...")
training_df = hist.to_dataframe(
    entity_df=spine_df,
    feature_start_time=datetime(2024, 1, 1),
    timestamp_col="prediction_timestamp",
)

print(f"Created training data: {training_df.shape}")
print("\nSample features:")
print(training_df[['customer_id', 'prediction_timestamp', 'transaction_amount']].head())
```

**Expected output:**
```
Computing historical features...
Created training data: (10, 5)

Sample features:
  customer_id prediction_timestamp  transaction_amount
0        C001 2024-01-20 00:00:00              145.32
1        C002 2024-01-28 00:00:00               89.45
2        C003 2024-01-17 00:00:00              234.12
...
```

!!! success "No Data Leakage!"
    Each row uses **only transactions before** the prediction timestamp. This ensures your model doesn't train on future information!

---

## Part 5: Deploy Online Serving (5 minutes)

### Understanding Online vs Offline

| Aspect | Offline Store | Online Store |
|--------|---------------|--------------|
| **Purpose** | Model training | Real-time inference |
| **Access** | Batch queries | Low-latency lookups |
| **Storage** | Parquet files | In-memory / fast database |
| **Freshness** | Historical snapshots | Latest feature values |

### Create Online Feature Table

```python
# Create online serving table
online_fg = OnlineFeaturesDuckDB(
    name="customer_features_online",
    lookup_key="customer_id",
)

# Materialize latest features for online serving
print("Creating online feature table...")
online_fg.materialize(
    source_fg=fg,
    feature_names=[
        'transaction_amount',
        'product_category',
        'is_return',
    ],
)

print(f"Online table created: {online_fg.name}")
```

**Expected output:**
```
Creating online feature table...
Online table created: customer_features_online
```

### Query Online Features

```python
# Get features for inference (low latency!)
print("Fetching online features...")
features = online_fg.get_features(
    keys=[{"customer_id": "C001"}, {"customer_id": "C005"}],
)

print("\nOnline features:")
for customer_features in features:
    print(f"  {customer_features}")
```

**Expected output:**
```
Fetching online features...

Online features:
  {'customer_id': 'C001', 'transaction_amount': 156.78, 'product_category': 'Electronics', 'is_return': 0}
  {'customer_id': 'C005', 'transaction_amount': 89.23, 'product_category': 'Clothing', 'is_return': 0}
```

!!! tip "Training-to-Serving Parity"
    The same feature definition (`fg`) works for both:
    - **Training**: `HistoricalFeaturesDuckDB.to_dataframe()`
    - **Serving**: `OnlineFeaturesDuckDB.get_features()`

    No code duplication, guaranteed consistency!

---

## Summary

In this chapter, you learned:

- [x] **Feature Store Concepts** — Entities, feature groups, point-in-time joins
- [x] **Creating Feature Groups** — Define entities and write features
- [x] **Historical Features** — Point-in-time correct training data
- [x] **Online Serving** — Low-latency feature lookup for inference
- [x] **Training-to-Serving Parity** — Same features for training and serving

**Key Commands/Classes:**
```python
from seeknal.entity import Entity
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    OnlineFeaturesDuckDB,
    FeatureLookup,
)

# Create feature group
fg = FeatureGroupDuckDB(name="...", entity=entity)
fg.set_dataframe(df)
fg.set_features()
fg.write(feature_start_time=...)

# Historical features
hist = HistoricalFeaturesDuckDB(lookups=[...])
training_df = hist.to_dataframe(entity_df=...)

# Online serving
online = OnlineFeaturesDuckDB(name="...", lookup_key="...")
features = online.get_features(keys=[...])
```

---

## What's Next?

[Chapter 2: Second-Order Aggregations →](2-second-order-aggregation.md)

Learn to create multi-level features from aggregations, building hierarchical features for more powerful models.

---

## See Also

- **[Point-in-Time Joins](../../concepts/point-in-time-joins.md)** — Deep dive on temporal correctness
- **[Python API Reference](../../api/featurestore.md)** — Complete API documentation
- **[Feature Store Best Practices](../../guides/feature-store.md)** — Production patterns
