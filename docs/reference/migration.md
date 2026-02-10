# Migration Guides

Comprehensive guides for migrating to Seeknal from other platforms and between Seeknal engines.

> **Navigation:**
> - [Migrating from Spark to DuckDB](#migrating-from-spark-to-duckdb)
> - [Migrating from dbt to Seeknal](#migrating-from-dbt-to-seeknal)
> - [Migrating from Feast to Seeknal](#migrating-from-feast-to-seeknal)
> - [Migrating from Featuretools to Seeknal](#migrating-from-featuretools-to-seeknal)

---

## Migrating from Spark to DuckDB

Seeknal supports both Spark and DuckDB engines with identical APIs. This guide helps you migrate from Spark to DuckDB for faster development, lower costs, and simpler infrastructure.

### Why Migrate to DuckDB?

| Aspect | Spark | DuckDB |
|--------|-------|--------|
| **Setup** | Requires JVM, Spark installation | Pure Python, `pip install` |
| **Memory** | High memory footprint | Lightweight, in-process |
| **Storage** | Delta Lake format | Parquet + JSON metadata |
| **Performance** | Optimized for big data (>100M rows) | Fast for <100M rows |
| **Cost** | Higher infrastructure costs | Lower compute costs |
| **Development** | Slower iteration | Rapid prototyping |

### When to Use DuckDB vs Spark

**Use DuckDB when:**
- Dataset < 100M rows
- Single-node deployment
- Development/testing environment
- Rapid prototyping
- Cost-sensitive deployment
- Team prefers pure Python

**Use Spark when:**
- Dataset > 100M rows
- Distributed processing required
- Existing Spark infrastructure
- Need Delta Lake features
- Production-scale workloads

### Migration Steps

#### Step 1: Update Import Paths

**Before (Spark):**
```python
from seeknal.featurestore.feature_group import (
    FeatureGroup,
    HistoricalFeatures,
    OnlineFeatures,
)
from pyspark.sql import SparkSession
```

**After (DuckDB):**
```python
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    OnlineFeaturesDuckDB,
)
import pandas as pd
```

#### Step 2: Replace Spark DataFrame with Pandas

**Before (Spark):**
```python
spark = SparkSession.builder.getOrCreate()
df = spark.read.table("my_data")
```

**After (DuckDB):**
```python
df = pd.read_parquet("my_data.parquet")
# Or from CSV
df = pd.read_csv("my_data.csv")
```

#### Step 3: Update Feature Group Creation

**Before (Spark):**
```python
from seeknal.featurestore.feature_group import FeatureGroup
from seeknal.entity import Entity

entity = Entity(name="user", join_keys=["user_id"])
fg = FeatureGroup(
    name="user_features",
    entity=entity,
    project="my_project"
)
fg.set_dataframe(spark_df)
fg.set_features()
```

**After (DuckDB):**
```python
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB
from seeknal.entity import Entity

entity = Entity(name="user", join_keys=["user_id"])
fg = FeatureGroupDuckDB(
    name="user_features",
    entity=entity,
    project="my_project"
)
fg.set_dataframe(pandas_df)  # Same API!
fg.set_features()            # Same API!
```

#### Step 4: Update Historical Features

**Before (Spark):**
```python
from seeknal.featurestore.feature_group import HistoricalFeatures, FeatureLookup

lookup = FeatureLookup(source=fg)
hist = HistoricalFeatures(lookups=[lookup])
df = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))
```

**After (DuckDB):**
```python
from seeknal.featurestore.duckdbengine.feature_group import (
    HistoricalFeaturesDuckDB,
    FeatureLookup,
)

lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])  # Same API!
df = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))
```

### Complete Migration Example

**Before (Full Spark Example):**
```python
from pyspark.sql import SparkSession
from seeknal.featurestore.feature_group import (
    FeatureGroup,
    HistoricalFeatures,
    FeatureLookup,
)
from seeknal.entity import Entity
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Load data
df = spark.read.parquet("data.parquet")

# Create entity and feature group
entity = Entity(name="user", join_keys=["user_id"])
fg = FeatureGroup(name="user_features", entity=entity)
fg.set_dataframe(df)
fg.set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))

# Query historical features
lookup = FeatureLookup(source=fg)
hist = HistoricalFeatures(lookups=[lookup])
result = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))
```

**After (Full DuckDB Example):**
```python
import pandas as pd
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    FeatureLookup,
)
from seeknal.entity import Entity
from datetime import datetime

# Load data (no Spark needed!)
df = pd.read_parquet("data.parquet")

# Create entity and feature group (same API!)
entity = Entity(name="user", join_keys=["user_id"])
fg = FeatureGroupDuckDB(name="user_features", entity=entity)
fg.set_dataframe(df)
fg.set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))

# Query historical features (same API!)
lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
result = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))
```

### DuckDB Performance Benchmarks

Based on real dataset (73,194 rows × 35 columns):

| Operation | Time | Throughput |
|-----------|------|------------|
| **Write** | 0.08s | 897K rows/sec |
| **Read** | 0.02s | 3.6M rows/sec |
| **Point-in-time join** | <0.5s | N/A |

### Hybrid Approach: Use Both Engines

Seeknal allows you to mix engines in the same project:

```python
# Use DuckDB for development
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB

fg_dev = FeatureGroupDuckDB(name="features_dev", entity=entity)
fg_dev.set_dataframe(pandas_df)

# Switch to Spark for production
from seeknal.featurestore.feature_group import FeatureGroup

fg_prod = FeatureGroup(name="features_prod", entity=entity)
fg_prod.set_dataframe(spark_df)  # Same transformations!
```

---

## Migrating from dbt to Seeknal

Seeknal provides a dbt-like CLI experience with the added benefit of built-in feature store capabilities.

### Key Similarities

| Concept | dbt | Seeknal |
|---------|-----|---------|
| **Project structure** | `models/` directory | `seeknal/` directory |
| **CLI** | `dbt run`, `dbt test` | `seeknal run`, `seeknal validate` |
| **Configuration** | `dbt_project.yml` | `seeknal_project.yml` |
| **Profiles** | `profiles.yml` | `profiles.yml` |
| **Materializations** | Table, View, Incremental | Feature Groups, Models |

### Migration Steps

#### Step 1: Initialize Seeknal Project

**dbt structure:**
```
my_dbt_project/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   └── marts/
└── tests/
```

**Seeknal equivalent:**
```bash
seeknal init --name my_project
```

**Creates:**
```
my_project/
├── seeknal_project.yml
├── profiles.yml
├── seeknal/
│   ├── sources/
│   ├── transforms/
│   ├── feature_groups/
│   └── models/
└── target/
```

#### Step 2: Convert dbt Models to Seeknal Transforms

**dbt model (`models/staging/users.sql`):**
```sql
{{ config(
    materialized='table',
    schema='staging'
) }}

SELECT
    id as user_id,
    email,
    created_at,
    updated_at
FROM {{ source('raw', 'users') }}
WHERE deleted_at IS NULL
```

**Seeknal transform (`seeknal/transforms/stg_users.yml`):**
```yaml
name: stg_users
kind: transform
description: Staging users table
tags: [staging, users]
deps:
  - source.raw_users
sql: |
  SELECT
    id as user_id,
    email,
    created_at,
    updated_at
  FROM raw_users
  WHERE deleted_at IS NULL
```

#### Step 3: Convert Sources

**dbt source (`models/schema.yml`):**
```yaml
sources:
  - name: raw
    tables:
      - name: users
        description: Raw users table
```

**Seeknal source (`seeknal/sources/raw_users.yml`):**
```yaml
name: raw_users
kind: source
description: Raw users table
connection: postgres://user:pass@host/db
sql: |
  SELECT * FROM public.users
```

#### Step 4: Convert Tests to Validators

**dbt test (`tests/users_unique.yml`):**
```sql
SELECT
    user_id
FROM {{ ref('stg_users') }}
GROUP BY user_id
HAVING COUNT(*) > 1
```

**Seeknal validator (in feature group config):**
```yaml
name: user_features
kind: feature_group
entity: user
validation_config:
  validators:
    - validator_type: uniqueness
      columns: [user_id]
      max_duplicate_percentage: 0.0
```

#### Step 5: Update Commands

| dbt Command | Seeknal Equivalent |
|-------------|-------------------|
| `dbt run` | `seeknal run` |
| `dbt parse` | `seeknal parse` |
| `dbt test` | `seeknal audit` or `seeknal validate-features` |
| `dbt compile` | `seeknal dry-run` |
| `dbt docs generate` | `seeknal parse` (generates manifest) |

### Feature Store Advantages

Unlike dbt, Seeknal provides built-in feature store capabilities:

```yaml
# Define features with metadata
name: user_features
kind: feature_group
entity: user
features:
  - name: total_purchases
    data_type: int
    description: Total number of purchases
  - name: avg_order_value
    data_type: float
    description: Average order value
```

This enables:
- Automatic feature versioning
- Point-in-time correctness
- Online serving with low latency
- Training-to-serving parity

---

## Migrating from Feast to Seeknal

Feast and Seeknal both provide feature store capabilities, but Seeknal offers a more integrated approach with transformation pipelines.

### Key Concept Mapping

| Feast Concept | Seeknal Equivalent |
|---------------|-------------------|
| **Feature View** | Feature Group |
| **Entity** | Entity (same concept) |
| **Data Source** | Source |
| **Provider** | DuckDB or Spark engine |
| **Feature Service** | HistoricalFeatures + OnlineFeatures |

### Migration Steps

#### Step 1: Convert Feature Definitions

**Feast feature view:**
```python
from feast import FeatureView, Field
from feast.types import Float32, Int64

user_features_fv = FeatureView(
    name="user_features",
    entities=["user_id"],
    schema=[
        Field(name="total_purchases", dtype=Int64),
        Field(name="avg_order_value", dtype=Float32),
    ],
    ttl=timedelta(days=30),
)
```

**Seeknal feature group:**
```python
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
)
from seeknal.entity import Entity

entity = Entity(name="user", join_keys=["user_id"])
fg = FeatureGroupDuckDB(
    name="user_features",
    entity=entity,
    description="User behavior features",
)
fg.set_dataframe(df)
fg.set_features()
```

#### Step 2: Convert Feature Retrieval

**Feast:**
```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")
features = store.get_online_features(
    features=["user_features:total_purchases"],
    entity_rows=[{"user_id": "123"}],
)
```

**Seeknal:**
```python
from seeknal.featurestore.duckdbengine.feature_group import (
    OnlineFeaturesDuckDB,
)

online_table = OnlineFeaturesDuckDB(
    name="user_features_online",
    lookup_key=entity,
)
features = online_table.get_features(keys=[{"user_id": "123"}])
```

#### Step 3: Convert Historical Retrieval

**Feast:**
```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")
historical_features = store.get_historical_features(
    entity_df=pd.DataFrame({"user_id": ["123", "456"]}),
    features=["user_features:total_purchases"],
)
```

**Seeknal:**
```python
from seeknal.featurestore.duckdbengine.feature_group import (
    HistoricalFeaturesDuckDB,
    FeatureLookup,
)

lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
df = hist.to_dataframe(
    entity_df=pd.DataFrame({"user_id": ["123", "456"]}),
)
```

### Seeknal Advantages Over Feast

1. **Integrated Transformations**: Build features directly in Seeknal without external orchestration
2. **Point-in-Time Joins**: Automatic correctness handling
3. **Version Management**: Built-in feature versioning and rollback
4. **Multi-Engine**: DuckDB for development, Spark for production
5. **Validation Framework**: Data quality checks built-in

---

## Migrating from Featuretools to Seeknal

Featuretools is a library for automated feature engineering. Seeknal provides similar capabilities with production serving.

### Key Concept Mapping

| Featuretools Concept | Seeknal Equivalent |
|---------------------|-------------------|
| **EntitySet** | Sources + Entities |
| **dfs** (Deep Feature Synthesis) | Transform SQL + Feature Groups |
| **Feature Primitives** | SQL functions + aggregations |
| **Cutoff Time** | feature_start_time in HistoricalFeatures |

### Migration Steps

#### Step 1: Convert Entity Setup

**Featuretools:**
```python
import featuretools as ft

entities = {
    "users": (users_df, "user_id"),
    "sessions": (sessions_df, "session_id", "user_id"),
    "events": (events_df, "event_id", "session_id"),
}

es = ft.EntitySet(id="activity", entities=entities)
es = es.add_relationships([
    ("users", "user_id", "sessions", "user_id"),
    ("sessions", "session_id", "events", "session_id"),
])
```

**Seeknal:**
```yaml
# seeknal/sources/users.yml
name: users
kind: source
sql: SELECT * FROM users

# seeknal/sources/sessions.yml
name: sessions
kind: source
sql: SELECT * FROM sessions

# seeknal/entities/user.yml
name: user
join_keys: [user_id]
```

#### Step 2: Convert DFS to Transforms

**Featuretools:**
```python
feature_matrix, features = ft.dfs(
    entityset=es,
    target_entity="users",
    trans_primitives=["hour", "day"],
    agg_primitives=["count", "sum", "mean"],
    max_depth=2,
)
```

**Seeknal:**
```yaml
# seeknal/transforms/user_features.yml
name: user_features
kind: transform
deps:
  - source.users
  - source.sessions
sql: |
  SELECT
    u.user_id,
    COUNT(DISTINCT s.session_id) as session_count,
    SUM(s.duration) as total_session_time,
    AVG(s.duration) as avg_session_duration,
    EXTRACT(HOUR FROM MIN(s.start_time)) as first_session_hour
  FROM users u
  LEFT JOIN sessions s ON u.user_id = s.user_id
  GROUP BY u.user_id
```

#### Step 3: Convert Cutoff Time Handling

**Featuretools:**
```python
cutoff_times = pd.DataFrame({
    "user_id": [1, 2, 3],
    "time": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
})

feature_matrix = ft.calculate_feature_matrix(
    features=features,
    entityset=es,
    cutoff_time=cutoff_times,
)
```

**Seeknal:**
```python
from datetime import datetime
from seeknal.featurestore.duckdbengine.feature_group import (
    HistoricalFeaturesDuckDB,
)

# Point-in-time correctness built-in
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
df = hist.to_dataframe(
    feature_start_time=datetime(2024, 1, 1),
)
```

---

## Common Migration Patterns

### Batch Migration with Validation

```python
# Migrate feature group from Spark to DuckDB with validation
from seeknal.featurestore.feature_group import FeatureGroup
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB

# Load from Spark
fg_spark = FeatureGroup.load(name="user_features", version=1)
spark_df = fg_spark.get_dataframe()

# Convert to Pandas
pandas_df = spark_df.toPandas()

# Create DuckDB feature group
fg_duckdb = FeatureGroupDuckDB(
    name="user_features",
    entity=fg_spark.entity,
)
fg_duckdb.set_dataframe(pandas_df)

# Validate outputs match
assert len(pandas_df) == spark_df.count()
print(f"Migrated {len(pandas_df)} features successfully")
```

### Gradual Migration Strategy

1. **Week 1**: Set up DuckDB in parallel
2. **Week 2**: Migrate non-critical feature groups
3. **Week 3**: Validate outputs and performance
4. **Week 4**: Migrate critical features
5. **Week 5**: Deprecate Spark infrastructure

### A/B Testing with Versions

```python
# Keep both engines running with versions
fg_spark = FeatureGroup(name="features", version=1)  # Spark
fg_duckdb = FeatureGroupDuckDB(name="features", version=2)  # DuckDB

# Query both for comparison
result_v1 = fg_spark.get_dataframe()
result_v2 = fg_duckdb.get_dataframe()

# Compare outputs
print(f"Spark rows: {len(result_v1)}")
print(f"DuckDB rows: {len(result_v2)}")
print(f"Difference: {abs(len(result_v1) - len(result_v2))}")
```

---

## Troubleshooting Migrations

### Type Conversion Issues

**Problem**: Spark and DuckDB handle types differently

**Solution**:
```python
# Explicit type conversion before migration
pandas_df = spark_df.toPandas()
pandas_df['column'] = pandas_df['column'].astype('float64')
```

### Performance Regression

**Problem**: DuckDB slower than expected

**Solution**:
```python
# Use Parquet format for optimal performance
pandas_df.to_parquet("data.parquet", compression='snappy')
df = pd.read_parquet("data.parquet")
```

### Memory Issues

**Problem**: Large datasets don't fit in memory

**Solution**: Use chunked processing
```python
chunk_size = 100000
for i in range(0, len(spark_df), chunk_size):
    chunk = spark_df.limit(i).offset(i + chunk_size).toPandas()
    fg_duckdb.set_dataframe(chunk)
    fg_duckdb.write(feature_start_time=datetime(2024, 1, 1))
```

---

## Additional Resources

- [CLI Reference](cli.md) - Complete command documentation
- [YAML Schema Reference](yaml-schema.md) - Schema definitions
- [Configuration Reference](configuration.md) - Project and profile configuration
- [Troubleshooting Guide](troubleshooting.md) - Debug migration issues
- [Getting Started Guide](../getting-started-comprehensive.md) - Tutorial
- [API Reference](../api/index.md) - Python API docs

---

*Last updated: February 2026 | Seeknal Documentation*
