"""Tests for SQL transformer."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.data_utils import create_sample_dataframe


def test_sql_transformer(spark_session: SparkSession):
    """Test SQL transformer."""
    from seeknal.tasks.sparkengine.py_impl.transformers.sql import SQL

    df = create_sample_dataframe(spark_session, 10)

    transformer = SQL(
        spark=spark_session,
        query="SELECT id, name, value * 2 AS doubled FROM input WHERE id > 5",
        view_name="input"
    )
    result = transformer.transform(df)

    # Should have 4 rows (id 6-9)
    assert result.count() == 4
    assert "doubled" in result.columns


def test_sql_transformer_custom_view(spark_session: SparkSession):
    """Test SQL transformer with custom view name."""
    from seeknal.tasks.sparkengine.py_impl.transformers.sql import SQL

    df = create_sample_dataframe(spark_session, 10)

    transformer = SQL(
        spark=spark_session,
        query="SELECT * FROM my_view WHERE value > 50",
        view_name="my_view"
    )
    result = transformer.transform(df)

    # value = id * 10.5, so value > 50 means id >= 5
    # Rows 5-9 = 5 rows
    assert result.count() == 5
