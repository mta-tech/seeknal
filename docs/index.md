# Seeknal API Reference

Welcome to the Seeknal API Reference documentation. This site provides comprehensive API documentation for Seeknal, an all-in-one platform for data and AI/ML engineering.

## What is Seeknal?

Seeknal is a platform that abstracts away the complexity of data transformation and AI/ML engineering. It is a collection of tools that help you transform data, store it, and use it for machine learning and data analytics.

Seeknal lets you:

- **Define** data and feature transformations from raw data sources using Pythonic APIs and YAML.
- **Register** transformations and feature groups by names and get transformed data and features for various use cases including AI/ML modeling, data engineering, business metrics calculation and more.
- **Share** transformations and feature groups across teams and company.

## Use Cases

Seeknal is useful in multiple scenarios:

- **AI/ML Modeling**: Computes your feature transformations and incorporates them into your training data, using point-in-time joins to prevent data leakage while supporting the materialization and deployment of your features for online use in production.
- **Data Analytics**: Build data pipelines to extract features and metrics from raw data for Analytics and AI/ML modeling.

## Quick Links

<div class="grid cards" markdown>

-   :material-code-braces: **API Reference**

    ---

    Explore the complete API documentation for all Seeknal modules.

    [:octicons-arrow-right-24: API Reference](api/index.md)

-   :material-book-open-variant: **Examples**

    ---

    Learn through practical code examples demonstrating common patterns.

    [:octicons-arrow-right-24: Examples](examples/index.md)

</div>

## Getting Started

### Installation

We recommend using uv for installing Seeknal:

```bash
# Create and activate virtual environment
uv venv --python 3.11
source .venv/bin/activate

# Install Seeknal
uv pip install seeknal-<version>-py3-none-any.whl
```

### Quick Example

```python
from seeknal.project import Project
from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
from seeknal.tasks.sparkengine import SparkEngineTask

# Create a project
project = Project(name="my_project", description="My project")
project.get_or_create()

# Define a data pipeline
flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="my_df")
task = SparkEngineTask().add_sql("SELECT * FROM __THIS__ WHERE active = true")
flow = Flow(
    name="my_flow",
    input=flow_input,
    tasks=[task],
    output=FlowOutput(),
)

# Save and run the pipeline
flow.get_or_create()
result = flow.run()
```

## Core Modules

| Module | Description |
|--------|-------------|
| [`seeknal.project`](api/core.md#project) | Project management and configuration |
| [`seeknal.entity`](api/core.md#entity) | Entity definitions for feature stores |
| [`seeknal.flow`](api/core.md#flow) | Data pipeline and transformation flows |
| [`seeknal.featurestore`](api/featurestore.md) | Feature store management and materialization |
| [`seeknal.tasks`](api/tasks.md) | Task definitions for Spark, DuckDB, and more |

## Environment Setup

Seeknal requires the following environment variables:

| Variable | Description |
|----------|-------------|
| `SEEKNAL_BASE_CONFIG_PATH` | Path to base configuration directory |
| `SEEKNAL_USER_CONFIG_PATH` | Path to user config file (e.g., `/path/to/config.toml`) |

## Additional Resources

- [GitHub Repository](https://github.com/mta-tech/seeknal)
- [README](https://github.com/mta-tech/seeknal/blob/main/README.md)
