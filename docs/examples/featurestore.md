# FeatureStore

This guide demonstrates how to create and manage feature groups in Seeknal's Feature Store. Feature groups are the primary way to organize, store, and serve features for machine learning models.

> **Note:** This guide demonstrates Seeknal's **Python API** for programmatic feature group creation and management. For a **CLI-based workflow** using the `draft → dry-run → apply` pattern with YAML definitions, see the [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md). Both approaches are valid - choose based on your workflow preference.

## Prerequisites

Before running these examples, ensure you have:

1. Seeknal installed (see [Installation Guide](../index.md#installation))
2. Project and Entity configured (see [Initialization](initialization.md))
3. **DuckDB engine** (default, no additional setup) or **Spark engine** (requires PySpark)

## Core Concepts

### Feature Groups

A **FeatureGroup** is a logical grouping of related features that:

- Share the same entity (e.g., user, product, transaction)
- Are typically computed from the same data source
- Support both offline (batch) and online (real-time) materialization

### Engines

Seeknal provides two feature store engines:

| Engine | Best For | DataFrame Type | Setup |
|--------|----------|---------------|-------|
| **DuckDB** (`FeatureGroupDuckDB`) | Datasets <100M rows, dev/test, rapid prototyping | Pandas | None (included) |
| **Spark** (`FeatureGroup`) | Datasets >100M rows, distributed processing | PySpark | Requires Spark |

### Materialization

**Materialization** is the process of computing and storing features. Seeknal supports:

- **Offline Store**: For batch processing and training (Parquet/Delta format)
- **Online Store**: For low-latency feature serving

---

## DuckDB Engine (Recommended for Most Use Cases)

### Creating a Feature Group

```python
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    OnlineFeaturesDuckDB,
    FeatureLookup,
    Materialization,
)
from seeknal.entity import Entity
from datetime import datetime

# Create entity
user_entity = Entity(name="user", join_keys=["user_id"])

# Create feature group with DuckDB engine
fg = FeatureGroupDuckDB(
    name="user_activity_features",
    entity=user_entity,
    materialization=Materialization(event_time_col="event_date"),
    description="User activity aggregation features",
)
```

### Creating from DataFrame

```python
import pandas as pd

# Create sample DataFrame
user_df = pd.DataFrame({
    "user_id": ["user_001", "user_002", "user_003"],
    "event_date": ["2024-01-15", "2024-01-15", "2024-01-15"],
    "total_spend": [150.0, 75.5, 200.0],
    "order_count": [3, 1, 5],
})

# Set DataFrame and auto-detect features
fg.set_dataframe(user_df).set_features()

# Write features
fg.write(feature_start_time=datetime(2024, 1, 1))
```

### Retrieving Historical Features

```python
# Create feature lookup
lookup = FeatureLookup(source=fg)

# Retrieve latest features
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
df = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))
print(df)
```

### Online Feature Serving

```python
# Serve features for real-time inference
online_table = hist.serve(name="user_features_online")

# Get features for specific entity keys
features = online_table.get_features(keys=[{"user_id": "user_001"}])
print(features)
# Output: [{"user_id": "user_001", "total_spend": 150.0, "order_count": 3}]

# Get features for multiple entities
features = online_table.get_features(
    keys=[
        {"user_id": "user_001"},
        {"user_id": "user_002"},
    ]
)
```

### DuckDB Performance

Based on real dataset (73,194 rows x 35 columns):
- **Write**: 0.08s (897K rows/sec)
- **Read**: 0.02s (3.6M rows/sec)
- **Point-in-time join**: <0.5s

---

## Spark Engine (For Large-Scale Processing)

> **Note:** The Spark engine requires PySpark and optionally a Spark cluster. For datasets under 100M rows, prefer the DuckDB engine above.

### Step 1: Set Up Project and Entity

```python
from seeknal.project import Project
from seeknal.entity import Entity

project = Project(
    name="recommendation_engine",
    description="Features for product recommendations",
).get_or_create()

user_entity = Entity(
    name="user",
    join_keys=["user_id"],
    description="User entity for recommendation features",
).get_or_create()
```

### Step 2: Create a Data Flow

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

transform_task = SparkEngineTask().add_stage(
    class_name="tech.mta.seeknal.transformers.SelectExpr",
    params={
        "expressions": [
            "user_id",
            "event_date",
            "COUNT(*) as activity_count",
            "SUM(purchase_amount) as total_spend",
        ]
    },
)

user_activity_flow = Flow(
    name="user_activity_features",
    input=FlowInput(path="/data/user_activity.parquet", format="parquet"),
    tasks=[transform_task],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
).get_or_create()
```

### Step 3: Create Feature Group with Materialization

```python
from seeknal.featurestore import FeatureGroup, Materialization

materialization = Materialization(
    event_time_col="event_date",
    date_pattern="yyyy-MM-dd",
    offline=True,
    online=False,
)

user_features = FeatureGroup(
    name="user_activity_features",
    entity=user_entity,
    materialization=materialization,
    description="User activity aggregation features",
)

user_features = (
    user_features
    .set_flow(user_activity_flow)
    .set_features()
    .get_or_create()
)
```

### Step 4: Write Features

```python
from datetime import datetime

# Write features for a specific time range
user_features.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 1, 31),
)

# Write features for today (default)
user_features.write()
```

### Step 5: Create from DataFrame Directly

```python
from pyspark.sql import SparkSession
from seeknal.featurestore import FeatureGroup, Materialization

spark = SparkSession.builder.getOrCreate()

user_df = spark.createDataFrame([
    ("user_001", "2024-01-15", 150.0, 3),
    ("user_002", "2024-01-15", 75.5, 1),
    ("user_003", "2024-01-15", 200.0, 5),
], ["user_id", "event_date", "total_spend", "order_count"])

df_features = FeatureGroup(
    name="user_spend_features",
    entity=user_entity,
    materialization=Materialization(event_time_col="event_date"),
)

df_features = (
    df_features
    .set_dataframe(user_df)
    .set_features()
    .get_or_create()
)

df_features.write()
```

---

## Retrieving Historical Features (Spark)

### Point-in-Time Joins with Spine

For training data, use a spine DataFrame to ensure point-in-time correctness:

```python
import pandas as pd
from seeknal.featurestore import HistoricalFeatures, FeatureLookup

# Training data spine with entity keys and timestamps
spine = pd.DataFrame({
    "user_id": ["user_001", "user_002", "user_003"],
    "label_date": ["2024-01-20", "2024-01-20", "2024-01-20"],
    "target": [1, 0, 1],
})

# Retrieve features with point-in-time correctness
training_df = (
    HistoricalFeatures(lookups=[FeatureLookup(source=user_features)])
    .using_spine(
        spine=spine,
        date_col="label_date",
        offset=0,
        keep_cols=["target"],
    )
    .to_dataframe()
)

training_df.show()
```

!!! warning "Point-in-Time Correctness"
    Point-in-time joins ensure you only use features that were available at the
    time of prediction, preventing data leakage in your ML models.

### Handling Null Values

```python
from seeknal.featurestore import HistoricalFeatures, FeatureLookup, FillNull

fill_nulls = [
    FillNull(value="0", dataType="double", columns=["total_spend"]),
    FillNull(value="-1", dataType="int", columns=["order_count"]),
]

historical = HistoricalFeatures(
    lookups=[FeatureLookup(source=user_features)],
    fill_nulls=fill_nulls,
)

df = historical.using_latest().to_dataframe()
```

---

## Migration from Spark to DuckDB

Only **2 line changes** needed:

1. Import path: `.duckdbengine.feature_group` instead of `.feature_group`
2. DataFrame type: Pandas instead of PySpark

**Before (Spark)**:
```python
from seeknal.featurestore.feature_group import FeatureGroup
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("my_data")
```

**After (DuckDB)**:
```python
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB
import pandas as pd

df = pd.read_parquet("my_data.parquet")
```

Everything else (API, features, materialization) is **identical**.

---

## Managing Feature Groups

### Listing Feature Groups

```bash
# CLI
seeknal list feature-groups
```

### Version Management

Feature groups are automatically versioned when the schema changes.

```bash
# List versions
seeknal version list user_activity_features

# Show specific version
seeknal version show user_activity_features --version 1

# Compare versions
seeknal version diff user_activity_features --from 1 --to 2
```

### Deleting a Feature Group

```bash
# CLI
seeknal delete feature-group user_activity_features
```

```python
# Python API
existing_fg = FeatureGroup(name="user_activity_features").get_or_create()
existing_fg.delete()
```

!!! danger "Irreversible Operation"
    Deleting a feature group removes all associated data from the offline store.
    This operation cannot be undone.

---

## Best Practices

!!! tip "Feature Group Design"
    - **Single Entity**: Each feature group should have exactly one entity
    - **Related Features**: Group features that are computed together
    - **Use DuckDB**: For most use cases, `FeatureGroupDuckDB` is faster and simpler
    - **Document**: Add descriptions to feature groups and individual features

!!! tip "Performance"
    - Use `merge` mode for incremental updates instead of full rewrites
    - Set appropriate TTL values to manage storage costs
    - Enable online storage only for features needed in real-time

!!! warning "Data Quality"
    - Always specify `event_time_col` for time-series features
    - Use point-in-time joins with spine DataFrames for training data
    - Handle null values explicitly with `FillNull` configurations

## Next Steps

1. **Data Pipelines** - Build transformations with [Flows](flows.md)
2. **DAG Tutorial** - Dependency tracking ([DAG Tutorial](seeknal-2.0-dag-tutorial.md))
3. **Virtual Environments** - Test changes safely ([Virtual Environments](../concepts/virtual-environments.md))
4. **Error Handling** - Handle edge cases ([Error Handling](error_handling.md))
