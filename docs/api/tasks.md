# Tasks API Reference

This page documents the Tasks module, which provides the core infrastructure for building and executing data processing pipelines in Seeknal. Tasks are the fundamental units of work for data transformations.

## Overview

The Tasks module provides essential classes for building data processing pipelines:

| Class | Purpose |
|-------|---------|
| [`Task`](#seeknal.tasks.base.Task) | Abstract base class defining the task interface |
| [`SparkEngineTask`](#seeknal.tasks.sparkengine.sparkengine.SparkEngineTask) | Spark-based task for large-scale data processing |
| [`DuckDBTask`](#seeknal.tasks.duckdb.duckdb.DuckDBTask) | Lightweight task using DuckDB for local processing |
| [`Stage`](#seeknal.tasks.sparkengine.sparkengine.Stage) | Configuration for a single pipeline stage |
| [`Stages`](#seeknal.tasks.sparkengine.sparkengine.Stages) | Container for multiple pipeline stages |

## Task

The `Task` class is the abstract base class that defines the interface for all data processing tasks. It provides a common structure for inputs, transformation stages, and outputs.

:::seeknal.tasks.base.Task
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## SparkEngineTask

The `SparkEngineTask` class provides a fluent interface for building Spark-based data transformation pipelines with support for extractors, transformers, aggregators, and loaders.

:::seeknal.tasks.sparkengine.sparkengine.SparkEngineTask
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Stage

The `Stage` class represents a single step in a SparkEngine transformation pipeline.

:::seeknal.tasks.sparkengine.sparkengine.Stage
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Stages

The `Stages` class is a container for grouping multiple Stage instances together.

:::seeknal.tasks.sparkengine.sparkengine.Stages
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## DuckDBTask

The `DuckDBTask` class provides a lightweight task implementation using DuckDB for smaller datasets and local development.

:::seeknal.tasks.duckdb.duckdb.DuckDBTask
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Transformers

Transformers modify data within a pipeline stage. The transformers module provides various transformation types.

### Transformer

Base class for all transformers.

:::seeknal.tasks.sparkengine.transformers.base_transformer.Transformer
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### FeatureTransformer

Transformer specialized for feature engineering operations.

:::seeknal.tasks.sparkengine.transformers.base_transformer.FeatureTransformer
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### SQL

Execute SQL transformations on data.

:::seeknal.tasks.sparkengine.transformers.spark_engine_transformers.SQL
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### AddColumnByExpr

Add a column using a Spark SQL expression.

:::seeknal.tasks.sparkengine.transformers.spark_engine_transformers.AddColumnByExpr
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### FilterByExpr

Filter data using a Spark SQL expression.

:::seeknal.tasks.sparkengine.transformers.spark_engine_transformers.FilterByExpr
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### ColumnRenamed

Rename a column in the DataFrame.

:::seeknal.tasks.sparkengine.transformers.spark_engine_transformers.ColumnRenamed
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### PointInTime

Point-in-time join transformer for temporal feature engineering.

:::seeknal.tasks.sparkengine.transformers.spark_engine_transformers.PointInTime
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Extractors

Extractors load data from external sources into the pipeline.

### Extractor

Base class for all data extractors.

:::seeknal.tasks.sparkengine.extractors.base_extractor.Extractor
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Loaders

Loaders write processed data to external destinations.

### Loader

Base class for all data loaders.

:::seeknal.tasks.sparkengine.loaders.base_loader.Loader
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Aggregators

Aggregators perform grouping and aggregation operations on data.

### Aggregator

Base class for all aggregators.

:::seeknal.tasks.sparkengine.aggregators.base_aggregator.Aggregator
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### AggregatorFunction

Defines an aggregation function for use with aggregators.

:::seeknal.tasks.sparkengine.aggregators.base_aggregator.AggregatorFunction
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### LastNDaysAggregator

Aggregate data over the last N days.

:::seeknal.tasks.sparkengine.aggregators.base_aggregator.LastNDaysAggregator
    options:
      show_root_heading: false
      show_source: true
      members_order: source

### SecondOrderAggregator

Perform second-order aggregations (aggregations on aggregated data).

:::seeknal.tasks.sparkengine.aggregators.second_order_aggregator.SecondOrderAggregator
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Module Reference

Complete module reference for the tasks package.

:::seeknal.tasks
    options:
      show_root_heading: false
      show_source: true
      members_order: source
