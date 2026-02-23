# Tasks API Reference

This page documents the Tasks module, which provides the core infrastructure for building and executing data processing pipelines in Seeknal. Tasks are the fundamental units of work for data transformations.

## Overview

The Tasks module provides essential classes for building data processing pipelines:

| Class | Purpose |
|-------|---------|
| [`Task`](#task) | Abstract base class defining the task interface |
| [`SparkEngineTask`](#sparkenginetask) | Spark-based task for large-scale data processing |
| [`DuckDBTask`](#duckdbtask) | Lightweight task using DuckDB for local processing |
| [`Stage`](#stage) | Configuration for a single pipeline stage |

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

:::seeknal.tasks.sparkengine.py_impl.spark_engine_task.SparkEngineTask
    options:
      show_root_heading: false
      show_source: true
      members_order: source

---

## Stage

The `Stage` class represents a single step in a SparkEngine transformation pipeline.

:::seeknal.tasks.sparkengine.py_impl.spark_engine_task.Stage
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

## SecondOrderAggregator

Perform second-order aggregations (aggregations on aggregated data).

:::seeknal.tasks.sparkengine.py_impl.aggregators.second_order_aggregator.SecondOrderAggregator
    options:
      show_root_heading: false
      show_source: true
      members_order: source
