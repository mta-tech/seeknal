"""Tests for join transformers."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.assertions import assert_dataframes_equal
from tests.sparkengine.data_utils import (
    create_join_left_dataframe,
    create_join_right_dataframe
)


def test_join_by_id(spark_session: SparkSession):
    """Test JoinById transformer."""
    from seeknal.tasks.sparkengine.py_impl.transformers.joins import JoinById

    left = create_join_left_dataframe(spark_session)
    right = create_join_right_dataframe(spark_session)

    transformer = JoinById(
        right_df=right,
        left_columns=["id"],
        right_columns=["user_id"],
        join_type="inner"
    )
    result = transformer.transform(left)

    # Should have 3 matching rows
    assert result.count() == 3
    assert "id" in result.columns
    assert "name" in result.columns
    assert "score" in result.columns


def test_join_by_expr(spark_session: SparkSession):
    """Test JoinByExpr transformer."""
    from seeknal.tasks.sparkengine.py_impl.transformers.joins import JoinByExpr

    left = create_join_left_dataframe(spark_session)
    right = create_join_right_dataframe(spark_session)

    transformer = JoinByExpr(
        right_df=right,
        expression="id = user_id",
        join_type="inner"
    )
    result = transformer.transform(left)

    # Should have 3 matching rows
    assert result.count() == 3


def test_join_by_id_left(spark_session: SparkSession):
    """Test JoinById with left join."""
    from seeknal.tasks.sparkengine.py_impl.transformers.joins import JoinById

    left = create_join_left_dataframe(spark_session)
    right = create_join_right_dataframe(spark_session)

    transformer = JoinById(
        right_df=right,
        left_columns=["id"],
        right_columns=["user_id"],
        join_type="left"
    )
    result = transformer.transform(left)

    # Should have 4 rows (all from left, even unmatched)
    assert result.count() == 4
