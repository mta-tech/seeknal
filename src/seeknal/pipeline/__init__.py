"""
Seeknal Python Pipeline API.

This module provides a decorator-based API for defining data pipelines in Python.
It enables users to define sources, transforms, and feature groups using Python
functions while maintaining full compatibility with YAML-based workflows.

Example Usage:
    # /// script
    # requires-python = ">=3.11"
    # dependencies = ["pandas", "requests"]
    # ///
    from seeknal.pipeline import source, transform, feature_group
    from seeknal.pipeline.materialization import Materialization, OfflineConfig
    from seeknal.tasks.duckdb import DuckDBTask
    from seeknal.tasks.duckdb import transformers as T

    @source(name="users", source="csv", table="data/users.csv")
    def users():
        pass

    @transform(name="clean_users")
    def clean_users(ctx):
        df = ctx.ref("source.users")
        return (
            DuckDBTask()
            .add_input(dataframe=df)
            .add_stage(transformer=T.SQL("SELECT * FROM __THIS__ WHERE active"))
            .transform(ctx.duckdb)
        )

    @feature_group(
        name="user_features",
        entity="user",
        materialization=Materialization(
            offline=OfflineConfig(format="parquet"),
        ),
    )
    def user_features(ctx):
        df = ctx.ref("transform.clean_users")
        return ctx.duckdb.sql("SELECT user_id, COUNT(*) as cnt FROM df GROUP BY 1").df()

Key Components:
    - @source: Define external data sources
    - @transform: Define transformation logic
    - @feature_group: Define feature groups with materialization
    - PipelineContext: Execution context with ref(), duckdb, config
    - Materialization: Configuration for offline/online stores

For more details, see:
    - src/seeknal/pipeline/decorators.py: Decorator implementations
    - src/seeknal/pipeline/context.py: PipelineContext documentation
    - src/seeknal/pipeline/materialization.py: Materialization dataclasses
"""

from seeknal.pipeline.decorators import (
    source,
    transform,
    feature_group,
    get_registered_nodes,
    clear_registry,
)
from seeknal.pipeline.context import PipelineContext
from seeknal.pipeline.materialization import (
    Materialization,
    OfflineConfig,
    OnlineConfig,
    IcebergTable,
)

__all__ = [
    # Decorators
    "source",
    "transform",
    "feature_group",
    # Context
    "PipelineContext",
    # Materialization
    "Materialization",
    "OfflineConfig",
    "OnlineConfig",
    "IcebergTable",
    # Registry
    "get_registered_nodes",
    "clear_registry",
]
