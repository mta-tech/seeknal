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

Seeknal is designed as a comprehensive data processing software that enables you to create an end-to-end pipeline by allowing you to utilize one or more data processing engines (such as Apache Spark combined with DuckDB). To facilitate execution across various engines, Seeknal defines the pipeline in JSON format, which the respective engine processes. In this context, the engines need to support JSON input for the pipeline to function correctly. Since some data processors do not naturally handle YAML input, we enhance these data processors to incorporate this feature, which we refer to as engines. These engines are located in the`engines` folder.

See Seeknal in action:

```python
from seeknal.project import Project
from seeknal.flow import (
    Flow,
    FlowInput,
    FlowOutput,
    FlowInputEnum,
    FlowOutputEnum,
)
from spark_engine.task import SparkEngineTask, DuckDBTask

project = Project(name="my_project", description="My project")
project.get_or_create()

flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="my_df")
flow_output = FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)

# develop pipeline mix of Spark and DuckDB
task_on_spark = SparkEngineTask().add_sql("SELECT * FROM __THIS__ WHERE day = date_format(current_date(), 'yyyy-MM-dd')")
task_on_duckdb = DuckDBTask().add_sql("SELECT id, lat, lon, movement_type, day FROM __THIS__")
flow = Flow(
    name="my_flow",
    input=flow_input,
    tasks=[task_on_spark, task_on_duckdb],
    output=FlowOutput(),
)

flow.get_or_create()
res = flow.run()
```


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
Your seeknal has been installed on your machine and ready to use in your projects. To see it on action, check out the `feature-store-demo.ipynb` notebook.

## Use Turso as Database

Seeknal uses an SQLite database to store internal data. For production or collaborative use of Seeknal, we suggest using [Turso](https://turso.com/) as your database provider. This allows you to share your Seeknal projects seamlessly across teams and environments, given that it operates using the same database. To set up Turso as your database, edit the `config.toml` file and adjust the `context.database` setting accordingly:

```toml
[context.database]
TURSO_DATABASE_URL = "<your-turso-database-url>"
TURSO_AUTH_TOKEN = "<your-turso-auth-token>"
```

## Contributing
Contributions are welcome! Please read our contributing guidelines before submitting pull requests.