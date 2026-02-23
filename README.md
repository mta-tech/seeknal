<html>
    <h1 align="center">
        Seeknal
    </h1>
    <h3 align="center">
        An all-in-one platform for data and AI/ML engineering
    </h3>
    <p align="center">
        <img src="https://img.shields.io/badge/version-2.1.0-blue.svg" alt="Version 2.1.0">
        <a href="docs/"><img src="https://img.shields.io/badge/docs-comprehensive-green.svg" alt="Documentation"></a>
        <a href="CLAUDE.md"><img src="https://img.shields.io/badge/CLAUDE.md-AI--ready-purple.svg" alt="CLAUDE.md"></a>
    </p>
</html>

Seeknal is a platform that abstracts away the complexity of data transformation and AI/ML engineering. It is a collection of tools that help you transform data, store it, and use it for machine learning and data analytics.

Seeknal lets you:

- **Define** data and feature transformations from raw data sources using Pythonic APIs and YAML.
- **Register** transformations and feature groups by names and get transformed data and features for various use cases including AI/ML modeling, data engineering, business metrics calculation and more.
- **Share** transformations and feature groups across teams and company.

Seeknal is useful in multiple use cases including:

- AI/ML modeling: computes your feature transformations and incorporates them into your training data, using point-in-time joins to prevent data leakage while supporting the materialization and deployment of your features for online use in production.
- Data analytics: build data pipelines to extract features and metrics from raw data for Analytics and AI/ML modeling.

Seeknal is designed as a comprehensive data processing tool that enables you to create an end-to-end pipeline by allowing you to utilize one or more data processing engines (such as Apache Spark combined with DuckDB). To facilitate execution across various engines, Seeknal defines the pipeline in JSON format, which the respective engine processes. In this context, the engines need to support JSON input for the pipeline to function correctly. Since some data processors do not naturally handle YAML input, we enhance these data processors to incorporate this feature, which we refer to as engines. These engines are located in the`engines` folder.

## Documentation

- **[Documentation Homepage](docs/index.md)** — Start here
- **[CLI Reference](docs/reference/cli.md)** — All commands and flags
- **[YAML Schema](docs/reference/yaml-schema.md)** — Pipeline YAML reference
- **[Glossary](docs/concepts/glossary.md)** — Key term definitions
- **Tutorials**: [YAML Pipelines](docs/tutorials/yaml-pipeline-tutorial.md) · [Python Pipelines](docs/tutorials/python-pipelines-tutorial.md) · [Mixed YAML + Python](docs/tutorials/mixed-yaml-python-pipelines.md)
- **Guides**: [Testing & Audits](docs/guides/testing-and-audits.md) · [Semantic Layer](docs/guides/semantic-layer.md) · [Training to Serving](docs/guides/training-to-serving.md)
- **Concepts**: [Point-in-Time Joins](docs/concepts/point-in-time-joins.md) · [Virtual Environments](docs/concepts/virtual-environments.md) · [Change Categorization](docs/concepts/change-categorization.md)

## Getting started

### Option 1: Install from Source (Recommended)

```bash
# Clone the repository
git clone https://github.com/mta-tech/seeknal.git
cd seeknal

# Create virtual environment and install (using uv - faster)
uv venv --python 3.11
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -e ".[all]"

# Or using standard pip
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -e ".[all]"

# Verify installation
seeknal --help
```

### Option 2: Install from GitHub Releases

Visit the [releases page](https://github.com/mta-tech/seeknal/releases) and download the latest wheel file.

```bash
pip install seeknal-<version>-py3-none-any.whl
```

### Configuration

1. Copy example configuration files:
   ```bash
   cp .env.example .env
   mkdir -p ~/.seeknal
   cp config.toml.example ~/.seeknal/config.toml
   ```

2. Edit the files with your settings:
   ```bash
   # .env - Set your paths
   SEEKNAL_BASE_CONFIG_PATH="${HOME}/.seeknal"
   SEEKNAL_USER_CONFIG_PATH="${HOME}/.seeknal/config.toml"
   ```

3. (Optional) For production with Turso:
   ```toml
   # ~/.seeknal/config.toml
   [context.database]
   TURSO_DATABASE_URL = "your-turso-database-url"
   TURSO_AUTH_TOKEN = "your-turso-auth-token"
   ```

**Congratulations!** Seeknal is now installed and ready to use.

**Next Steps:**

- **For the CLI workflow (recommended for teams)**: Try the [Workflow Tutorial](docs/tutorials/workflow-tutorial-ecommerce.md) to learn the `draft → dry-run → apply` pattern for creating production-grade pipelines
- **For Python API users**: Check out the demo notebooks:
  - `feature-store-demo.ipynb` - Spark-based feature store demo
  - `duckdb_feature_store_demo.ipynb` - DuckDB-based feature store demo (73K real data rows)

## DuckDB Integration

Seeknal offers a lightweight DuckDB-based feature store engine that provides an alternative to Apache Spark for small-to-medium datasets (<100M rows). This engine is ideal for:

- Single-node deployments
- Development and testing environments
- Cost-effective data pipelines
- Rapid prototyping without JVM overhead

### Why DuckDB?

| Feature | Spark + Delta Lake | DuckDB |
|---------|-------------------|--------|
| **Setup** | Requires JVM, Spark installation | Pure Python, pip install |
| **Memory** | High memory footprint | Lightweight, in-process |
| **Storage** | Delta Lake format | Parquet + JSON metadata |
| **Performance** | Optimized for big data | Fast for <100M rows |
| **Cost** | Higher infrastructure costs | Lower compute costs |

### DuckDB Feature Store in Action

```python
from seeknal.entity import Entity
from seeknal.featurestore.duckdbengine.feature_group import (
    FeatureGroupDuckDB,
    HistoricalFeaturesDuckDB,
    OnlineFeaturesDuckDB,
    FeatureLookup,
    Materialization,
)
import pandas as pd

# Load your data (73K rows example)
df = pd.read_parquet("data/communications_features.parquet")

# Create entity and feature group
msisdn_entity = Entity(name="msisdn", join_keys=["msisdn"])
materialization = Materialization(event_time_col="day")

fg = FeatureGroupDuckDB(
    name="comm_features",
    entity=msisdn_entity,
    materialization=materialization,
    project="my_project"
)

# Auto-detect features from DataFrame
fg.set_dataframe(df).set_features()

# Write to offline store
fg.write(feature_start_time=datetime(2019, 2, 1))

# Read features back
lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
features_df = hist.to_dataframe(feature_start_time=datetime(2019, 2, 1))

# Serve to online store
online_table = hist.serve(name="comm_features_online")
```

### Migration from Spark to DuckDB

Migrating from Spark to DuckDB requires only minimal code changes:

**Before (Spark)**:
```python
from seeknal.featurestore.feature_group import FeatureGroup
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.table("my_data")

fg = FeatureGroup(
    name="my_features",
    entity=Entity(name="user", join_keys=["user_id"]),
    materialization=Materialization(event_time_col="timestamp")
)
fg.set_dataframe(df).set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))
```

**After (DuckDB)**:
```python
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB
import pandas as pd

df = pd.read_parquet("my_data.parquet")

fg = FeatureGroupDuckDB(
    name="my_features",
    entity=Entity(name="user", join_keys=["user_id"]),
    materialization=Materialization(event_time_col="timestamp")
)
fg.set_dataframe(df).set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))
```

**Only 2 changes needed**:
1. Import from `.duckdbengine.feature_group`
2. Use Pandas DataFrame instead of Spark DataFrame

Everything else (API, features, materialization) is identical!

### Performance Benchmarks

Based on a real-world dataset (73,194 rows × 35 columns):

| Operation | Time | Throughput |
|-----------|------|------------|
| **Write** | 0.08s | 897K rows/sec |
| **Read** | 0.02s | 3.6M rows/sec |
| **Point-in-time join** | <0.5s | - |

For a complete demo, see `duckdb_feature_store_demo.ipynb`.

## Apache Iceberg Materialization

Seeknal supports Apache Iceberg table format for materializing pipeline outputs with ACID transactions, time travel, and incremental updates.

### Why Iceberg?

- **ACID Transactions**: Atomic commits with automatic rollback
- **Time Travel**: Query data as it was at any point in time
- **Schema Evolution**: Add/modify columns without rewriting data
- **Hidden Partitioning**: Partition evolution without data migration
- **Compatibility**: Works with DuckDB, Spark, Trino, and more

### Quick Start

1. **Configure Iceberg** in `~/.seeknal/profiles.yml`:

```yaml
materialization:
  enabled: true
  catalog:
    type: rest
    uri: ${LAKEKEEPER_URI}
    warehouse: s3://my-bucket/warehouse
  default_mode: append
```

2. **Set credentials** as environment variables:

```bash
export LAKEKEEPER_URI=https://lakekeeper.example.com
export LAKEKEEPER_WAREHOUSE=s3://my-bucket/warehouse
```

3. **Enable materialization** in your YAML nodes:

```yaml
kind: source
name: orders
materialization:
  enabled: true
  mode: append
  partition_by:
    - order_date
```

4. **Run your pipeline**:

```bash
seeknal run
```

### CLI Commands

```bash
# Validate configuration
seeknal iceberg validate-materialization

# Show current profile
seeknal iceberg profile-show

# List snapshots
seeknal iceberg snapshot-list warehouse.prod.orders

# Interactive setup
seeknal iceberg setup
```

### Documentation

For comprehensive documentation, see [Iceberg Materialization Guide](docs/iceberg-materialization.md).

## Seeknal in action (Spark Engine)

1. Create a data pipeline

    ```python
    from seeknal.project import Project
    from seeknal.flow import (
        Flow,
        FlowInput,
        FlowOutput,
        FlowInputEnum,
        FlowOutputEnum,
    )
    from seeknal.tasks.sparkengine import SparkEngineTask
    from seeknal.tasks.duckdb import DuckDBTask

    project = Project(name="my_project", description="My project")
    project.get_or_create()

    flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="my_df")
    flow_output = FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)

    # Develop a pipeline that mixes Spark and DuckDB.
    task_on_spark = SparkEngineTask().add_sql("SELECT * FROM __THIS__ WHERE day = date_format(current_date(), 'yyyy-MM-dd')")
    task_on_duckdb = DuckDBTask().add_sql("SELECT id, lat, lon, movement_type, day FROM __THIS__")
    flow = Flow(
        name="my_flow",
        input=flow_input,
        tasks=[task_on_spark, task_on_duckdb],
        output=FlowOutput(),
    )
    # save the data pipeline
    flow.get_or_create()
    res = flow.run()
    ```

2. Load the saved data pipeline

    ```python
    project = Project(name="my_project", description="My project")
    project.get_or_create()

    flow = Flow(name="my_flow").get_or_create()
    res = flow.run()
    ```

3. Save the results to a feature group

    ```python
    from datetime import datetime
    from seeknal.entity import Entity
    from seeknal.featurestore.feature_group import (
        FeatureGroup,
        Materialization,
        OfflineMaterialization,
        OfflineStore,
        OfflineStoreEnum,
        FeatureStoreFileOutput,
        OnlineStore,
        OnlineStoreEnum,
        HistoricalFeatures,
        FeatureLookup,
        FillNull,
        GetLatestTimeStrategy,
        OnlineFeatures,
    )

    # Define a materialization for the offline feature store
    materialization = Materialization(event_time_col="day", 
    offline_materialization=OfflineMaterialization(
        store=OfflineStore(kind=OfflineStoreEnum.FILE, 
                           name="object_storage",
                           value=FeatureStoreFileOutput(path="s3a://warehouse/feature_store")), 
                           mode="overwrite", ttl=None),
        offline=True)
    # Define feature group
    loc_feature_group = FeatureGroup(
        name="location_feature_group",
        entity=Entity(name="user_movement", join_keys=["msisdn", "movement_type"]).get_or_create(),
        materialization=materialization,
    )
    # Attach transformation for create the feature group
    loc_feature_group.set_flow(flow)

    # Register all columns as features
    loc_feature_group.set_features()

    # Save feature group
    loc_feature_group.get_or_create()

    # materialize the feature group to offline feature store
    loc_feature_group.write(
        # store features from specific date to the latest
        feature_start_time=datetime(2019, 3, 5)
    )
    ```

4. Load feature group from offline feature store

    ```python
    loc_feature_group = FeatureGroup(name="location_feature_group").get_or_create()
    # lookup for all features of loc_feature_group
    fs = FeatureLookup(source=loc_feature_group)
    # impute null to 0.0
    fillnull = FillNull(value="0.0", dataType="double")
    # load the features from offline feature store
    hist = HistoricalFeatures(lookups=[fs], fill_nulls=[fillnull])
    df = hist.to_dataframe(feature_start_time=datetime(2019, 3, 5))
    ```

5. Serve features to online feature store

    ```python
    latest_features = hist.using_latest.serve()
    user_one = Entity(name="user_movement").get_or_create().set_key_values("05X5wBWKN3")
    user_one_features = latest_features.get_features(keys=[user_one])
    ```


## Use Turso as Database

Seeknal uses an SQLite database to store internal data. For production or collaborative use of Seeknal, we suggest using [Turso](https://turso.com/) as your database provider. This allows you to share your Seeknal projects seamlessly across teams and environments, given that it operates using the same database. To set up Turso as your database, edit the `config.toml` file and adjust the `context.database` setting accordingly:

```toml
[context.database]
TURSO_DATABASE_URL = "<your-turso-database-url>"
TURSO_AUTH_TOKEN = "<your-turso-auth-token>"
```

## Storage Security Best Practices

Seeknal handles potentially sensitive feature data, so it's important to use secure storage paths. By default, Seeknal stores configuration and data in `~/.seeknal/`, which is a secure, user-specific directory.

### Why Avoid `/tmp` and World-Writable Directories

Using `/tmp` or other world-writable directories for data storage creates several security risks:

| Risk | Description |
|------|-------------|
| **Data Exposure** | Other users on the system can read your feature data |
| **Symlink Attacks** | Malicious users can create symlinks to redirect writes |
| **Data Persistence** | Temporary directories may be cleared unexpectedly |
| **Container Sharing** | In containerized environments, `/tmp` might be shared across containers |

### Recommended Storage Paths

We recommend using one of these secure storage locations:

1. **Default Path** (`~/.seeknal/`): User-specific directory with restricted permissions
2. **XDG-Compliant Path** (`~/.local/share/seeknal/`): If `XDG_DATA_HOME` is set
3. **Custom Path**: Set via `SEEKNAL_BASE_CONFIG_PATH` environment variable

Example secure path configurations:

```python
# Using secure default path
from seeknal.featurestore.feature_group import OfflineStore, OfflineStoreEnum, FeatureStoreFileOutput

# Recommended: Use ~/.seeknal/ paths
offline_store = OfflineStore(
    kind=OfflineStoreEnum.FILE,
    name="secure_store",
    value=FeatureStoreFileOutput(path="~/.seeknal/feature_store")
)

# For production: Use cloud storage
offline_store = OfflineStore(
    kind=OfflineStoreEnum.FILE,
    name="cloud_store",
    value=FeatureStoreFileOutput(path="s3a://my-bucket/feature_store")
)
```

### Configuring Custom Storage Path

You can configure a custom base storage path using the `SEEKNAL_BASE_CONFIG_PATH` environment variable. This is useful for:

- Team environments with shared storage
- Production deployments with specific storage requirements
- Testing with isolated directories

Set the environment variable in your `.env` file or shell:

```bash
# In .env file
SEEKNAL_BASE_CONFIG_PATH="/path/to/secure/storage"

# Or export in shell
export SEEKNAL_BASE_CONFIG_PATH="/path/to/secure/storage"
```

When creating directories for Seeknal data, ensure they have secure permissions:

```python
import os

# Create directory with secure permissions (owner read/write/execute only)
os.makedirs(os.path.expanduser("~/.seeknal/feature_store"), mode=0o700, exist_ok=True)
```

### Automatic Security Warnings

Seeknal automatically validates storage paths and logs warnings when insecure locations are detected. If you see a warning like:

```
Security Warning: Using insecure path '/tmp/feature_store' for offline store.
Consider using a secure alternative like '/home/user/.seeknal/feature_store'.
```

This indicates you should update your configuration to use a secure path. The warning will include a recommended secure alternative based on your current path.

## Feature Group Version Management

Seeknal automatically versions feature groups whenever the schema changes. This enables ML teams to track schema evolution, compare changes between versions, and safely roll back to previous versions when needed.

### Listing Versions

View all versions of a feature group:

```bash
# List all versions
seeknal version list user_features

# List last 5 versions
seeknal version list user_features --limit 5

# Output as JSON
seeknal version list user_features --format json
```

Example output:
```
Versions for feature group: user_features
--------------------------------------------------
Version    Created At             Features
---------  -------------------    --------
3          2024-01-15 10:30:00    12
2          2024-01-10 08:15:00    10
1          2024-01-05 14:20:00    8
```

### Viewing Version Details

Inspect the schema and metadata for a specific version:

```bash
# Show latest version
seeknal version show user_features

# Show specific version
seeknal version show user_features --version 2
```

### Comparing Versions

Compare schemas between two versions to identify added, removed, or modified features:

```bash
seeknal version diff user_features --from 1 --to 2
```

Example output:
```
Feature Group: user_features
Comparing version 1 → 2
============================================================

Added (+):
  + age_bucket: string
  + signup_source: string

Removed (-):
  - legacy_flag: boolean

Modified (~):
  ~ score: int → double

------------------------------------------------------------
Summary: 2 added, 1 removed, 1 modified
```

### Version-Specific Materialization

Materialize a specific version instead of the latest (useful for rollbacks):

```bash
# Materialize version 1 instead of latest
seeknal materialize user_features --start-date 2024-01-01 --version 1
```

### Python API

You can also manage versions programmatically:

```python
from seeknal.featurestore.feature_group import FeatureGroup

# Load feature group
fg = FeatureGroup(name="user_features").get_or_create()

# List all versions
versions = fg.list_versions()
for v in versions:
    print(f"Version {v['version']}: {v['feature_count']} features, created {v['created_at']}")

# Get specific version details
v1 = fg.get_version(1)
if v1:
    print(f"Version 1 schema: {v1['avro_schema']}")

# Compare versions
diff = fg.compare_versions(from_version=1, to_version=2)
print(f"Added features: {diff['added']}")
print(f"Removed features: {diff['removed']}")
print(f"Modified features: {diff['modified']}")
```

### Rollback Workflow

When you need to roll back to a previous version:

1. **Identify the target version:**
   ```bash
   seeknal version list user_features
   ```

2. **Compare schemas to understand the differences:**
   ```bash
   seeknal version diff user_features --from 2 --to 1
   ```

3. **Materialize the previous version:**
   ```bash
   seeknal materialize user_features --start-date 2024-01-01 --version 1
   ```

## Contributing
Contributions are welcome! Please read our contributing guidelines before submitting pull requests.