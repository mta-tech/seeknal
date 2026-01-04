# API Reference

This section provides comprehensive API documentation for all Seeknal modules. The documentation is auto-generated from Python docstrings using [mkdocstrings](https://mkdocstrings.github.io/).

## Module Overview

Seeknal is organized into several key modules, each serving a specific purpose in the data and ML engineering workflow.

### Core Modules

| Module | Description |
|--------|-------------|
| [Core](core.md) | Core classes including Project, Entity, Flow, and Context |
| [FeatureStore](featurestore.md) | Feature store management, feature groups, and materialization |
| [Tasks](tasks.md) | Task definitions for different data processing engines |

## Module Hierarchy

```
seeknal/
├── project.py          # Project management
├── entity.py           # Entity definitions
├── flow.py             # Data pipeline flows
├── context.py          # Execution context
├── configuration.py    # Configuration management
├── featurestore/       # Feature store module
│   ├── featurestore.py # Feature store core
│   └── feature_group.py# Feature group definitions
├── tasks/              # Task definitions
│   ├── base.py         # Base task class
│   ├── sparkengine/    # Spark engine tasks
│   └── duckdb/         # DuckDB tasks
└── exceptions/         # Exception classes
```

## Quick Navigation

### Core Classes

- **[Project](core.md#seeknal.project.Project)** - Manage Seeknal projects and their lifecycle
- **[Entity](core.md#seeknal.entity.Entity)** - Define entities with join keys for feature stores
- **[Flow](core.md#seeknal.flow.Flow)** - Create and manage data transformation pipelines
- **[Context](core.md#seeknal.context.Context)** - Execution context and session management

### FeatureStore Classes

- **[FeatureStore](featurestore.md#seeknal.featurestore.featurestore.FeatureStore)** - Feature store management
- **[FeatureGroup](featurestore.md#seeknal.featurestore.feature_group.FeatureGroup)** - Define and materialize feature groups
- **[Materialization](featurestore.md#seeknal.featurestore.feature_group.Materialization)** - Configure feature materialization
- **[HistoricalFeatures](featurestore.md#seeknal.featurestore.feature_group.HistoricalFeatures)** - Retrieve historical feature values

### Task Classes

- **[Task](tasks.md#seeknal.tasks.base.Task)** - Base class for all tasks
- **[SparkEngineTask](tasks.md#seeknal.tasks.sparkengine.SparkEngineTask)** - Spark-based data processing
- **[DuckDBTask](tasks.md#seeknal.tasks.duckdb.DuckDBTask)** - DuckDB-based data processing

## Docstring Format

All API documentation follows the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings) for docstrings. Each documented item includes:

- **Description**: What the class/function does
- **Args/Parameters**: Input parameters with types and descriptions
- **Returns**: Return value type and description
- **Raises**: Exceptions that may be raised
- **Example**: Usage examples where applicable

## Filtering

!!! note "Private Members"
    Private members (those starting with `_`) are excluded from the documentation by default. Only public APIs are documented.
