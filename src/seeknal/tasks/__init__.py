"""Task execution framework for data processing pipelines.

This module provides the core task infrastructure for building and executing
data processing pipelines in Seeknal. Tasks are the fundamental units of work
that define data transformations, from simple SQL queries to complex multi-stage
feature engineering pipelines.

Key Components:
    - Task: Abstract base class defining the task interface with methods for
      adding inputs, stages, and executing transformations.
    - SparkEngineTask: Spark-based task implementation for large-scale data
      processing with support for extractors, transformers, aggregators, and
      loaders.
    - DuckDBTask: Lightweight task implementation using DuckDB for smaller
      datasets and local development.

Submodules:
    - sparkengine: Spark-based task execution with ETL components
    - duckdb: DuckDB-based task execution for lightweight processing

Typical Usage:
    Using SparkEngineTask for Spark-based processing::

        from seeknal.tasks.sparkengine import SparkEngineTask

        task = (
            SparkEngineTask(name="feature_pipeline")
            .add_input(table="raw_data.events")
            .set_date_col(date_col="event_date")
            .add_sql("SELECT user_id, COUNT(*) as event_count FROM __THIS__ GROUP BY user_id")
            .add_output(table="features.user_events", partitions=["event_date"])
        )
        result = task.transform()

    Using DuckDBTask for lightweight processing::

        from seeknal.tasks.duckdb import DuckDBTask

        task = (
            DuckDBTask()
            .add_input(path="data/events.parquet")
            .add_sql("SELECT * FROM __THIS__ WHERE status = 'active'")
        )
        result = task.transform()

See Also:
    seeknal.tasks.sparkengine: Spark-based task components and transformers
    seeknal.tasks.duckdb: DuckDB-based task implementation
    seeknal.flow: Pipeline flow orchestration
"""

from .base import Task

__all__ = ["Task"]
