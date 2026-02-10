# FeatureStore

This guide demonstrates how to create and manage feature groups in Seeknal's Feature Store. Feature groups are the primary way to organize, store, and serve features for machine learning models.

> **Note:** This guide demonstrates Seeknal's **Python API** for programmatic feature group creation and management. For a **CLI-based workflow** using the `draft → dry-run → apply` pattern with YAML definitions, see the [Workflow Tutorial](../tutorials/workflow-tutorial-ecommerce.md). Both approaches are valid - choose based on your workflow preference.

## Prerequisites

Before running these examples, ensure you have:

1. Seeknal installed (see [Installation Guide](../index.md#installation))
2. Project and Entity configured (see [Initialization](initialization.md))
3. A SparkSession active (Seeknal will create one if needed)
4. Environment variables configured:
   ```bash
   export SEEKNAL_BASE_CONFIG_PATH="/path/to/config/directory"
   export SEEKNAL_USER_CONFIG_PATH="/path/to/config.toml"
   ```

## Core Concepts

### Feature Groups

A **FeatureGroup** is a logical grouping of related features that:

- Share the same entity (e.g., user, product, transaction)
- Are typically computed from the same data source
- Support both offline (batch) and online (real-time) materialization

### Materialization

**Materialization** is the process of computing and storing features. Seeknal supports:

- **Offline Store**: For batch processing and training (Delta Lake format)
- **Online Store**: For low-latency feature serving (Parquet files)

## Creating a Basic Feature Group

### Step 1: Set Up Project and Entity

```python
from seeknal.project import Project
from seeknal.entity import Entity

# Initialize project
project = Project(
    name="recommendation_engine",
    description="Features for product recommendations"
)
project = project.get_or_create()

# Create entity
user_entity = Entity(
    name="user",
    join_keys=["user_id"],
    description="User entity for recommendation features"
)
user_entity = user_entity.get_or_create()
```

### Step 2: Create a Data Flow

```python
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Define input source
flow_input = FlowInput(
    path="/data/user_activity.parquet",
    format="parquet"
)

# Create transformation task
transform_task = SparkEngineTask().add_stage(
    class_name="tech.mta.seeknal.transformers.SelectExpr",
    params={
        "expressions": [
            "user_id",
            "event_date",
            "COUNT(*) as activity_count",
            "SUM(purchase_amount) as total_spend"
        ]
    }
)

# Build the flow
user_activity_flow = Flow(
    name="user_activity_features",
    input=flow_input,
    tasks=[transform_task],
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)
)
user_activity_flow = user_activity_flow.get_or_create()
```

### Step 3: Create Feature Group with Materialization

```python
from seeknal.featurestore import FeatureGroup, Materialization

# Configure materialization settings
materialization = Materialization(
    event_time_col="event_date",      # Column containing event timestamps
    date_pattern="yyyy-MM-dd",         # Date format pattern
    offline=True,                      # Enable offline storage
    online=False                       # Disable online storage initially
)

# Create the feature group
user_features = FeatureGroup(
    name="user_activity_features",
    entity=user_entity,
    materialization=materialization,
    description="User activity aggregation features"
)

# Set the flow and auto-detect features from schema
user_features = (
    user_features
    .set_flow(user_activity_flow)
    .set_features()
    .get_or_create()
)

print(f"Feature Group ID: {user_features.feature_group_id}")
print(f"Features: {[f.name for f in user_features.features]}")
```

## Materialization to Offline Store

### Writing Features

```python
from datetime import datetime

# Write features for a specific time range
user_features.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 1, 31)
)

# Write features for today (default behavior)
user_features.write()
```

!!! info "Watermarks"
    Each write operation creates a watermark timestamp, allowing you to track
    data freshness and retrieve features as of a specific point in time.

### Configuring Offline Store Location

```python
from seeknal.featurestore import (
    FeatureGroup,
    Materialization,
    OfflineMaterialization,
    OfflineStore,
    OfflineStoreEnum,
    FeatureStoreFileOutput,
    FileKindEnum
)

# Configure file-based offline store
file_output = FeatureStoreFileOutput(
    path="/data/feature_store/offline",
    kind=FileKindEnum.DELTA
)

offline_store = OfflineStore(
    value=file_output,
    kind=OfflineStoreEnum.FILE,
    name="my_offline_store"
)

# Configure Hive table-based offline store (alternative)
from seeknal.featurestore import FeatureStoreHiveTableOutput

hive_output = FeatureStoreHiveTableOutput(database="feature_db")
hive_offline_store = OfflineStore(
    value=hive_output,
    kind=OfflineStoreEnum.HIVE_TABLE,
    name="hive_offline_store"
)

# Use in materialization configuration
materialization = Materialization(
    event_time_col="event_date",
    offline=True,
    offline_materialization=OfflineMaterialization(
        store=offline_store,
        mode="overwrite",  # Options: "overwrite", "append", "merge"
        ttl=30             # Data retention in days
    )
)
```

### Write Modes

| Mode | Description |
|------|-------------|
| `overwrite` | Replace existing data for the specified time range |
| `append` | Add new records without replacing existing data |
| `merge` | Upsert records based on primary key (`__pk__`) |

```python
# Example: Using merge mode for incremental updates
materialization = Materialization(
    event_time_col="event_date",
    offline_materialization=OfflineMaterialization(
        mode="merge",
        ttl=90  # Keep 90 days of data
    )
)
```

## Materialization to Online Store

### Enabling Online Storage

```python
from seeknal.featurestore import (
    Materialization,
    OnlineMaterialization,
    OnlineStore,
    OnlineStoreEnum,
    FeatureStoreFileOutput
)

# Configure online store
online_output = FeatureStoreFileOutput(path="/data/feature_store/online")
online_store = OnlineStore(
    value=online_output,
    kind=OnlineStoreEnum.FILE,
    name="my_online_store"
)

# Create materialization with both offline and online enabled
materialization = Materialization(
    event_time_col="event_date",
    offline=True,
    online=True,
    online_materialization=OnlineMaterialization(
        store=online_store,
        ttl=1440  # TTL in minutes (1 day)
    )
)

# Create feature group with online materialization
user_features = FeatureGroup(
    name="user_realtime_features",
    entity=user_entity,
    materialization=materialization
)
user_features = (
    user_features
    .set_flow(user_activity_flow)
    .set_features()
    .get_or_create()
)

# Materialize to both stores
user_features.write()
```

### Updating Materialization Settings

```python
# Enable online storage for an existing feature group
user_features.update_materialization(
    online=True,
    online_materialization=OnlineMaterialization(
        store=online_store,
        ttl=2880  # 2 days TTL
    )
)
```

## Creating Features from DataFrame

You can create feature groups directly from a Spark DataFrame without a Flow:

```python
from pyspark.sql import SparkSession
from seeknal.featurestore import FeatureGroup, Materialization

spark = SparkSession.builder.getOrCreate()

# Create sample DataFrame
user_df = spark.createDataFrame([
    ("user_001", "2024-01-15", 150.0, 3),
    ("user_002", "2024-01-15", 75.5, 1),
    ("user_003", "2024-01-15", 200.0, 5),
], ["user_id", "event_date", "total_spend", "order_count"])

# Create feature group from DataFrame
df_features = FeatureGroup(
    name="user_spend_features",
    entity=user_entity,
    materialization=Materialization(event_time_col="event_date")
)

df_features = (
    df_features
    .set_dataframe(user_df)
    .set_features()
    .get_or_create()
)

# Write features
df_features.write()
```

## Specifying Features Explicitly

Instead of auto-detecting features, you can explicitly define them:

```python
from seeknal.featurestore import FeatureGroup, Feature, Materialization

# Define features with descriptions
features = [
    Feature(
        name="total_spend",
        description="Total purchase amount by user",
        data_type="double"
    ),
    Feature(
        name="order_count",
        description="Number of orders placed",
        data_type="int"
    ),
    Feature(
        name="avg_order_value",
        description="Average value per order",
        data_type="double"
    )
]

# Create feature group with explicit features
explicit_fg = FeatureGroup(
    name="user_purchase_features",
    entity=user_entity,
    materialization=Materialization(event_time_col="event_date")
)

explicit_fg = (
    explicit_fg
    .set_flow(user_purchase_flow)
    .set_features(features=features)
    .get_or_create()
)
```

## Retrieving Historical Features

### Using HistoricalFeatures

The `HistoricalFeatures` class enables point-in-time correct feature retrieval for training data:

```python
from seeknal.featurestore import HistoricalFeatures, FeatureLookup, FeatureGroup

# Load existing feature groups
user_features = FeatureGroup(name="user_activity_features").get_or_create()
user_spend = FeatureGroup(name="user_spend_features").get_or_create()

# Create feature lookups
lookups = [
    FeatureLookup(source=user_features),
    FeatureLookup(
        source=user_spend,
        features=["total_spend", "order_count"],  # Select specific features
        exclude_features=None
    )
]

# Create historical features retriever
historical = HistoricalFeatures(lookups=lookups)

# Get latest features
df = historical.using_latest().to_dataframe()
df.show()
```

### Point-in-Time Joins with Spine

For training data, use a spine DataFrame to ensure point-in-time correctness:

```python
import pandas as pd
from seeknal.featurestore import HistoricalFeatures, FeatureLookup

# Training data spine with entity keys and timestamps
spine = pd.DataFrame({
    "user_id": ["user_001", "user_002", "user_003"],
    "label_date": ["2024-01-20", "2024-01-20", "2024-01-20"],
    "target": [1, 0, 1]
})

# Retrieve features with point-in-time correctness
historical = HistoricalFeatures(lookups=[
    FeatureLookup(source=user_features)
])

training_df = (
    historical
    .using_spine(
        spine=spine,
        date_col="label_date",     # Date column for point-in-time join
        offset=0,                   # Days offset (0 = same day features)
        keep_cols=["target"]        # Keep label column from spine
    )
    .to_dataframe()
)

training_df.show()
```

!!! warning "Point-in-Time Correctness"
    Point-in-time joins ensure you only use features that were available at the
    time of prediction, preventing data leakage in your ML models.

### Handling Null Values

Fill null values during feature retrieval:

```python
from seeknal.featurestore import HistoricalFeatures, FeatureLookup, FillNull

# Define null filling strategies
fill_nulls = [
    FillNull(
        value="0",
        dataType="double",
        columns=["total_spend", "avg_order_value"]
    ),
    FillNull(
        value="-1",
        dataType="int",
        columns=["order_count"]
    )
]

historical = HistoricalFeatures(
    lookups=[FeatureLookup(source=user_features)],
    fill_nulls=fill_nulls
)

df = historical.using_latest().to_dataframe()
```

## Serving Features Online

### Creating Online Features

```python
from seeknal.featurestore import OnlineFeatures, FeatureLookup, HistoricalFeatures
from datetime import timedelta

# Option 1: Serve from HistoricalFeatures
historical = HistoricalFeatures(lookups=[
    FeatureLookup(source=user_features)
])

online = historical.using_latest().serve(
    target=online_store,
    name="user_online_features",
    ttl=timedelta(days=1)
)

# Option 2: Serve directly from DataFrame
from seeknal.featurestore import OnlineFeatures

online = OnlineFeatures(
    name="direct_user_features",
    lookup_key=user_entity,
    lookups=[FeatureLookup(source=user_features)],
    dataframe=feature_df,  # Your feature DataFrame
    ttl=timedelta(hours=12),
    online_store=online_store
)
```

### Retrieving Features for Inference

```python
# Get features for specific entity keys
features = online.get_features(
    keys=[{"user_id": "user_001"}]
)
print(features)
# Output: [{"user_id": "user_001", "total_spend": 150.0, "order_count": 3}]

# Get features for multiple entities
features = online.get_features(
    keys=[
        {"user_id": "user_001"},
        {"user_id": "user_002"},
        {"user_id": "user_003"}
    ]
)

# Using Entity objects
user_entity = Entity(name="user", join_keys=["user_id"]).get_or_create()
user_entity = user_entity.set_key_values("user_001")

features = online.get_features(keys=[user_entity])
```

### Filtering Online Features

```python
# Get features with additional filter
features = online.get_features(
    keys=[{"user_id": "user_001"}],
    filter="total_spend > 100",
    drop_event_time=True  # Exclude event_time column
)
```

## Complete Example: End-to-End Feature Pipeline

```python
from seeknal.project import Project
from seeknal.entity import Entity
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowOutputEnum
from seeknal.featurestore import (
    FeatureGroup,
    Materialization,
    OfflineMaterialization,
    OnlineMaterialization,
    OfflineStore,
    OnlineStore,
    OfflineStoreEnum,
    OnlineStoreEnum,
    FeatureStoreFileOutput,
    HistoricalFeatures,
    FeatureLookup,
    Feature
)
from datetime import datetime, timedelta

# ============================================
# Step 1: Initialize Project and Entity
# ============================================
project = Project(name="ecommerce_ml").get_or_create()
customer_entity = Entity(
    name="customer",
    join_keys=["customer_id"],
    description="Customer entity for e-commerce features"
).get_or_create()

# ============================================
# Step 2: Create Data Transformation Flow
# ============================================
customer_flow = Flow(
    name="customer_purchase_flow",
    input=FlowInput(path="/data/purchases.parquet", format="parquet"),
    output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)
).get_or_create()

# ============================================
# Step 3: Configure Storage
# ============================================
offline_store = OfflineStore(
    value=FeatureStoreFileOutput(path="/data/features/offline"),
    kind=OfflineStoreEnum.FILE,
    name="ecommerce_offline"
)

online_store = OnlineStore(
    value=FeatureStoreFileOutput(path="/data/features/online"),
    kind=OnlineStoreEnum.FILE,
    name="ecommerce_online"
)

# ============================================
# Step 4: Create Feature Group
# ============================================
customer_features = FeatureGroup(
    name="customer_purchase_features",
    entity=customer_entity,
    description="Customer purchase behavior features",
    materialization=Materialization(
        event_time_col="purchase_date",
        date_pattern="yyyy-MM-dd",
        offline=True,
        online=True,
        offline_materialization=OfflineMaterialization(
            store=offline_store,
            mode="merge",
            ttl=365  # Keep 1 year of data
        ),
        online_materialization=OnlineMaterialization(
            store=online_store,
            ttl=1440  # 1 day TTL for online
        )
    )
)

# Configure and save
customer_features = (
    customer_features
    .set_flow(customer_flow)
    .set_features()
    .get_or_create()
)

# ============================================
# Step 5: Materialize Features
# ============================================
customer_features.write(
    feature_start_time=datetime(2024, 1, 1),
    feature_end_time=datetime(2024, 12, 31)
)

# ============================================
# Step 6: Retrieve for Training
# ============================================
import pandas as pd

training_spine = pd.DataFrame({
    "customer_id": ["C001", "C002", "C003"],
    "label_date": ["2024-06-01", "2024-06-01", "2024-06-01"],
    "churned": [0, 1, 0]
})

training_data = (
    HistoricalFeatures(lookups=[FeatureLookup(source=customer_features)])
    .using_spine(spine=training_spine, date_col="label_date", keep_cols=["churned"])
    .to_dataframe()
)

print("Training data ready!")
training_data.show()

# ============================================
# Step 7: Serve for Inference
# ============================================
online = (
    HistoricalFeatures(lookups=[FeatureLookup(source=customer_features)])
    .using_latest()
    .serve(target=online_store, ttl=timedelta(days=1))
)

# Real-time inference
customer_features_realtime = online.get_features(
    keys=[{"customer_id": "C001"}]
)
print(f"Features for inference: {customer_features_realtime}")
```

## Managing Feature Groups

### Listing Feature Groups

```python
from seeknal.featurestore import FeatureGroup

# List all feature groups (CLI output)
# Use: seeknal featuregroup list
```

### Loading Existing Feature Group

```python
# Load by name
existing_fg = FeatureGroup(name="user_activity_features").get_or_create()

# Access properties
print(f"Features: {[f.name for f in existing_fg.features]}")
print(f"Version: {existing_fg.version}")
print(f"Offline Watermarks: {existing_fg.offline_watermarks}")
```

### Deleting Feature Group

```python
# Delete feature group and its data
existing_fg.delete()
```

!!! danger "Irreversible Operation"
    Deleting a feature group removes all associated data from the offline store.
    This operation cannot be undone.

## Best Practices

!!! tip "Feature Group Design"
    - **Single Entity**: Each feature group should have exactly one entity
    - **Related Features**: Group features that are computed together
    - **Meaningful Names**: Use descriptive names for feature groups and features
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

After setting up your feature store, explore:

1. **Data Pipelines** - Build complex transformations with [Flows](flows.md)
2. **Error Handling** - Handle edge cases with [Error Handling](error_handling.md)
3. **Configuration** - Advanced settings in [Configuration](configuration.md)
