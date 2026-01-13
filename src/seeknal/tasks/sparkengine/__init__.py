"""Spark engine tasks.

This module now provides pure PySpark implementation.
"""

# Import from new PySpark implementation
from .pyspark.spark_engine_task import SparkEngineTask, Stage
from .pyspark.transformers import (
    FilterByExpr,
    AddColumnByExpr,
    ColumnRenamed,
    JoinById,
    JoinByExpr,
    SQL,
    AddEntropy,
    AddLatLongDistance,
)
from .pyspark.aggregators import FunctionAggregator, AggregationFunction

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
