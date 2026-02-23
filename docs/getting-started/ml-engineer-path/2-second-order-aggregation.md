# Chapter 2: Second-Order Aggregations

> **Duration:** 35 minutes | **Difficulty:** Advanced | **Format:** Python API

Learn to create multi-level features from aggregations, building hierarchical features that capture complex behavioral patterns.

---

## What You'll Build

A hierarchical feature system for customer behavior analysis:

```
Raw Transactions → Customer Features → Category Features → Global Features
                      ↓                    ↓                    ↓
                 User-Level          Group-Level         System-Level
```

**After this chapter, you'll have:**
- Second-order aggregation features from customer transactions
- Multi-level rollups (customer → category → global)
- Time-based window aggregations (7-day, 30-day, 90-day)
- Performance optimization for production

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Feature Store](1-feature-store.md) — Basic feature groups
- [ ] Understanding of SQL aggregations and window functions
- [ ] Comfortable with Python decorators and data manipulation

---

## Part 1: Understand Second-Order Aggregations (10 minutes)

### What Are Second-Order Aggregations?

**First-order aggregation**: Raw data → Features
```python
# Transaction → Customer features
customer_features = transactions.groupby('customer_id').agg({
    'amount': ['sum', 'mean', 'count']
})
```

**Second-order aggregation**: Features → Higher-level features
```python
# Customer features → Category features
category_features = customer_features.groupby('category').agg({
    'amount_sum': ['mean', 'max'],
    'amount_count': ['sum', 'count']
})
```

### Why Second-Order Aggregations?

| Use Case | First-Order | Second-Order |
|----------|-------------|---------------|
| **Churn Prediction** | Customer's total spend | Avg spend in customer's segment |
| **Recommendation** | User's purchase count | Popular items in user's category |
| **Fraud Detection** | Transaction amount | Transaction amount vs category average |
| **Pricing** | Product revenue | Product revenue vs category average |

!!! tip "When to Use Second-Order Aggregations"
    Use when you need:
    - **Contextual features**: Compare entity to its group
    - **Hierarchical patterns**: Multi-level behavioral patterns
    - **Normalization**: Entity features relative to peer group
    - **Anomaly detection**: Flag outliers relative to group

---

## Part 2: Create First-Order Features (8 minutes)

### Aggregate Customer Transactions

```python
#!/usr/bin/env python3
"""
Second-Order Aggregation - Chapter 2
Building hierarchical features for ML models
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from seeknal.entity import Entity
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    Materialization,
)

# Generate sample transaction data
np.random.seed(42)
dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')

transactions = []
categories = ['Electronics', 'Clothing', 'Home', 'Sports']

for date in dates:
    for customer_id in range(1, 101):
        for _ in range(np.random.randint(1, 5)):  # 1-4 transactions per day
            transactions.append({
                'customer_id': f'C{customer_id:03d}',
                'event_timestamp': date + pd.Timedelta(hours=np.random.randint(0, 24)),
                'transaction_amount': np.random.uniform(10, 500),
                'product_category': np.random.choice(categories),
                'is_return': np.random.choice([0, 1], p=[0.9, 0.1]),
            })

df = pd.DataFrame(transactions)
print(f"Generated {len(df)} transactions")
```

### Create Customer-Level Feature Group

```python
# Define customer entity
customer_entity = Entity(
    name="customer",
    join_keys=["customer_id"],
    description="Customer for behavioral analysis",
)

# Create first-order feature group
customer_fg = FeatureGroupDuckDB(
    name="customer_features_first_order",
    entity=customer_entity,
    description="Customer-level transaction features",
    materialization=Materialization(
        event_time_col="event_timestamp",
    ),
)

customer_fg.set_dataframe(df)

# Define first-order aggregations
customer_fg.set_features()
```

### Write First-Order Features

```python
# Write to offline store
customer_fg.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 1, 31),
)

print(f"First-order features: {list(customer_fg.features.keys())}")
```

**Expected output:**
```
First-order features: ['customer_id', 'event_timestamp', 'transaction_amount', 'product_category', 'is_return']
```

---

## Part 3: Create Second-Order Features (12 minutes)

### Understanding the Pattern

Second-order aggregations follow this pattern:

```python
# Step 1: Read first-order features
first_order_df = pd.read_parquet("output/customer_features_first_order/*.parquet")

# Step 2: Aggregate to create second-order features
second_order_features = first_order_df.groupby(['customer_id', 'product_category']).agg({
    'transaction_amount': [
        ('sum', 'sum'),
        ('mean', 'mean'),
        ('count', 'count'),
        ('std', 'std'),
    ]
}).reset_index()

# Step 3: Flatten column names
second_order_features.columns = ['_'.join(col).strip() for col in second_order_features.columns.values]
```

### Complete Second-Order Example

```python
from seeknal.tasks.duckdb import DuckDBTask
import pyarrow as pa

# Read first-order features
first_order_arrow = pa.Table.from_pandas(
    pd.read_parquet("output/customer_features_first_order/*.parquet")
)

# Create second-order aggregation task
second_order_task = DuckDBTask()
second_order_task.add_input(dataframe=first_order_arrow)

# Define second-order aggregation SQL
second_order_sql = """
SELECT
    customer_id,
    product_category,

    -- Aggregations of aggregations
    SUM(transaction_amount) as category_total_spend,
    AVG(transaction_amount) as category_avg_spend,
    COUNT(*) as category_transaction_count,
    STDDEV(transaction_amount) as category_spend_std,

    -- Time-based windows
    SUM(CASE WHEN event_timestamp >= CURRENT_DATE - INTERVAL '7 days'
        THEN transaction_amount END) as spend_last_7_days,
    SUM(CASE WHEN event_timestamp >= CURRENT_DATE - INTERVAL '30 days'
        THEN transaction_amount END) as spend_last_30_days,
    SUM(CASE WHEN event_timestamp >= CURRENT_DATE - INTERVAL '90 days'
        THEN transaction_amount END) as spend_last_90_days,

    -- Ratios
    CAST(COUNT(*) AS FLOAT) / NULLIF(COUNT(DISTINCT product_category), 0) as transactions_per_category

FROM __THIS__
GROUP BY customer_id, product_category
HAVING SUM(transaction_amount) > 0
"""

second_order_task.add_sql(second_order_sql)

# Execute transformation
second_order_result = second_order_task.transform()
second_order_df = second_order_result.to_pandas()

print(f"Created second-order features: {second_order_df.shape}")
print(f"Features: {list(second_order_df.columns)}")
```

**Expected output:**
```
Created second-order features: (287, 12)
Features: ['customer_id', 'product_category', 'category_total_spend', 'category_avg_spend', 'category_transaction_count', 'category_spend_std', 'spend_last_7_days', 'spend_last_30_days', 'spend_last_90_days', 'transactions_per_category', ...]
```

### Create Second-Order Feature Group

```python
# Create second-order feature group
second_order_fg = FeatureGroupDuckDB(
    name="customer_category_features",
    entity=customer_entity,
    description="Second-order customer-category level features",
    materialization=Materialization(
        event_time_col="event_timestamp",
    ),
)

second_order_fg.set_dataframe(second_order_df)
second_order_fg.set_features()

# Write second-order features
second_order_fg.write(
    feature_start_time=datetime(2024, 1, 1),
)
```

---

## Part 4: Create Hierarchical Rollups (10 minutes)

### Understanding Hierarchical Features

Multi-level features capture patterns at different scales:

```
Level 1: Global features
    └── All customers aggregated

Level 2: Category features
    └── Customers grouped by category

Level 3: Customer features
    └── Individual customer features
```

### Third-Order Aggregation (Global Level)

```python
# Create category-level entity
category_entity = Entity(
    name="product_category",
    join_keys=["product_category"],
    description="Product category for global aggregations",
)

# Aggregate to global level
global_task = DuckDBTask()
global_task.add_input(dataframe=pa.Table.from_pandas(second_order_df))

global_sql = """
SELECT
    product_category,

    -- Global category statistics
    AVG(category_total_spend) as avg_spend_per_customer_category,
    MAX(category_total_spend) as max_spend_per_customer_category,
    SUM(category_transaction_count) as total_category_transactions,
    AVG(category_spend_std) as avg_volatility_per_customer,

    -- Distribution metrics
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY category_avg_spend) as median_customer_spend,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY category_avg_spend) as p90_customer_spend,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY category_avg_spend) as p95_customer_spend

FROM __THIS__
GROUP BY product_category
"""

global_task.add_sql(global_sql)
global_result = global_task.transform()
global_df = global_result.to_pandas()

print(f"Global features: {global_df.shape}")
print(global_df)
```

**Expected output:**
```
Global features: (4, 8)
product_category  avg_spend_per_customer_category  max_spend_per_customer_category  ...
Electronics                      1250.50                          4500.00               ...
Clothing                           850.25                           2800.00               ...
Home                              620.75                           2100.00               ...
Sports                            945.00                           3200.00               ...
```

---

## Part 5: Optimize for Production (5 minutes)

### Performance Optimization Strategies

!!! tip "Incremental Materialization"
    ```python
    # Materialize incrementally to avoid recomputing everything
    second_order_fg.write(
        feature_start_time=datetime(2024, 1, 1),
        incremental=True,  # Only process new data
        incremental_key="event_timestamp",
    )
    ```

!!! tip "Partitioned Storage"
    ```python
    # Partition by date for efficient queries
    second_order_fg.write(
        feature_start_time=datetime(2024, 1, 1),
        partition_by=["product_category", "event_date"],
    )
    ```

!!! tip "Feature Selection"
    Not all second-order features are valuable:
    - **High cardinality**: Too many unique values
    - **Low variance**: Little predictive power
    - **High correlation**: Redundant with other features

    ```python
    # Analyze feature importance
    correlation_matrix = second_order_df.corr()
    low_variance_features = second_order_df.std()[second_order_df.std() < 0.01].index
    print(f"Low variance features to drop: {list(low_variance_features)}")
    ```

---

## Summary

In this chapter, you learned:

- [x] **Second-Order Aggregations** — Features from features
- [x] **Hierarchical Features** — Multi-level rollups
- [x] **Time-Based Windows** — 7-day, 30-day, 90-day aggregations
- [x] **Global Features** — Third-order aggregations
- [x] **Performance Optimization** — Incremental, partitioning, selection

**Key Patterns:**
```python
# First-order: Raw → Entity features
customer_fg.set_dataframe(transactions_df)
customer_fg.write(...)

# Second-order: Entity → Group features
second_order_df = first_order_df.groupby(['entity', 'group']).agg(...)

# Third-order: Group → Global features
global_df = second_order_df.groupby('group').agg(...)
```

---

## What's Next?

[Chapter 3: Training-to-Serving Parity →](3-training-to-serving-parity.md)

Learn to ensure consistent features between offline training and online serving, with materialization strategies and monitoring.

---

## See Also

- **[Second-Order Aggregations Guide](../../concepts/second-order-aggregations.md)** — Deep dive on patterns
- **[Feature Store Best Practices](../../guides/feature-store.md)** — Production patterns
- **[DuckDB SQL Functions](../../reference/cli.md)** — Complete command reference