"""
DuckDB Aggregators for Seeknal.

This module provides aggregator classes that mirror the SparkEngine aggregators
but use DuckDB as the execution engine.
"""

from .base_aggregator import (
    DuckDBAggregator,
    DuckDBAggregatorFunction,
    ColByExpression,
    RenamedCols,
    AggregateValueType,
)

from .simple_aggregators import (
    FunctionAggregator,
    ExpressionAggregator,
    DayTypeAggregator,
)

from .complex_aggregators import (
    LastNDaysAggregator,
)

__all__ = [
    # Base classes
    "DuckDBAggregator",
    "DuckDBAggregatorFunction",
    "ColByExpression",
    "RenamedCols",
    "AggregateValueType",
    # Simple aggregators
    "FunctionAggregator",
    "ExpressionAggregator",
    "DayTypeAggregator",
    # Complex aggregators
    "LastNDaysAggregator",
]
