"""Spark engine tasks.

This module now provides pure PySpark implementation.
"""

# Import from new PySpark implementation
from .py_impl.spark_engine_task import SparkEngineTask, Stage
from .py_impl.transformers import (
    FilterByExpr,
    AddColumnByExpr,
    ColumnRenamed,
    JoinById,
    JoinByExpr,
    SQL,
    AddEntropy,
    AddLatLongDistance,
)
from .py_impl.aggregators import FunctionAggregator, AggregationFunction

__all__ = [
    "SparkEngineTask",
    "Stage",
    "FilterByExpr",
    "AddColumnByExpr",
    "ColumnRenamed",
    "JoinById",
    "JoinByExpr",
    "SQL",
    "AddEntropy",
    "AddLatLongDistance",
    "FunctionAggregator",
    "AggregationFunction",
]
