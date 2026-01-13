"""Tests for aggregators."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.data_utils import create_sample_dataframe


def test_function_aggregator_sum(spark_session: SparkSession):
    """Test FunctionAggregator with sum."""
    from seeknal.tasks.sparkengine.pyspark.aggregators.function_aggregator import (
        FunctionAggregator,
        AggregationFunction,
    )

    df = create_sample_dataframe(spark_session, 10)

    aggregator = FunctionAggregator(
        group_by_columns=[],
        aggregations=[
            AggregationFunction(column="value", function="sum", alias="total_value")
        ],
    )
    result = aggregator.aggregate(df)

    assert result.count() == 1  # Single row
    assert "total_value" in result.columns


def test_function_aggregator_count(spark_session: SparkSession):
    """Test FunctionAggregator with count."""
    from seeknal.tasks.sparkengine.pyspark.aggregators.function_aggregator import (
        FunctionAggregator,
        AggregationFunction,
    )

    df = create_sample_dataframe(spark_session, 10)

    aggregator = FunctionAggregator(
        group_by_columns=[],
        aggregations=[
            AggregationFunction(column="*", function="count", alias="row_count")
        ],
    )
    result = aggregator.aggregate(df)

    assert result.count() == 1  # Single row
    assert result.first()["row_count"] == 10


def test_function_aggregator_group_by(spark_session: SparkSession):
    """Test FunctionAggregator with group by."""
    from seeknal.tasks.sparkengine.pyspark.aggregators.function_aggregator import (
        FunctionAggregator,
        AggregationFunction,
    )

    df = create_sample_dataframe(spark_session, 10)

    aggregator = FunctionAggregator(
        group_by_columns=["name"],
        aggregations=[
            AggregationFunction(column="value", function="avg", alias="avg_value")
        ],
    )
    result = aggregator.aggregate(df)

    # Each name is unique, so we should have 10 groups
    assert result.count() == 10
    assert "avg_value" in result.columns
