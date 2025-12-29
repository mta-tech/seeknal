<html>
    <h1 align="center">
        Seeknal
    </h1>
    <h3 align="center">
        An all-in-one platform for data and AI/ML engineering
    </h3>
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

## Getting started
We recommend to use uv for installing Seeknal. The following steps are expecting you to have [UV](https://docs.astral.sh/uv/guides/install-python/) installed.


To install Seeknal, follow these steps:

1. Download the Seeknal package:
    
    - Visit the [releases](https://github.com/mta-tech/seeknal/releases) page and download the latest package.

2. Extract the Downloaded File:
    - Unzip the downloaded zip file to your working directory.

3. Initialize the environment using uv:
    - Open your terminal and navigate to the directory where you extracted the files. Then, run the following command to initialize the environment:

    ```
    $ cd seeknal_build
    $ uv venv --python 3.11
    ```

    - Activate the environment:

    ```
    source .venv/bin/activate  
    ```

4. Install Seeknal using `uv pip`:
    ```
    uv pip install seeknal-<version>-py3-none-any.whl
    ```
    Replace <version> with the actual version number of the wheel file you downloaded.

5. Verify the Installation:

    To ensure that Seeknal has been installed correctly, you can run:
    
    ```
    uv pip show seeknal
    ```
    This command will display information about the installed package, confirming that the installation was successful.

6. Edit `.env` variable `SEEKNAL_BASE_CONFIG_PATH` and `SEEKNAL_USER_CONFIG_PATH` to point to the directory where you have `config.toml` file. For getting started, we have an example config.toml which you can find inside the `seeknal_build` directory. This case necessary update to the .env to point to the directory.

    ```
    SEEKNAL_BASE_CONFIG_PATH="path/to/seeknal_build"
    SEEKNAL_USER_CONFIG_PATH="path/to/seeknal_build/config.toml"
    ```

Congratulation!
Your seeknal has been installed on your machine and ready to use in your projects. To see it in action, check out the `feature-store-demo.ipynb` notebook or see it below.

## Seeknal in action

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

## Contributing
Contributions are welcome! Please read our contributing guidelines before submitting pull requests.