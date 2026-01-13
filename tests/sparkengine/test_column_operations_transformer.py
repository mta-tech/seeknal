"""Tests for column operation transformers."""

import pytest
from pyspark.sql import SparkSession
from seeknal.tasks.sparkengine.pyspark.transformers.column_operations import ColumnRenamed
from tests.sparkengine.assertions import assert_dataframes_equal
from tests.sparkengine.data_utils import create_sample_dataframe


def test_column_renamed(spark_session: SparkSession):
    """Test ColumnRenamed transformer."""
    df = create_sample_dataframe(spark_session, 10)

    transformer = ColumnRenamed(old_name="name", new_name="user_name")
    result = transformer.transform(df)

    # Verify column renamed
    assert "user_name" in result.columns
    assert "name" not in result.columns

    # Verify data unchanged
    assert result.count() == 10
    assert result.filter("user_name = 'user_5'").count() == 1


def test_column_renamed_multiple(spark_session: SparkSession):
    """Test renaming multiple columns by chaining transformers."""
    df = create_sample_dataframe(spark_session, 10)

    # Chain multiple renames
    transformer1 = ColumnRenamed(old_name="name", new_name="user_name")
    transformer2 = ColumnRenamed(old_name="value", new_name="amount")

    result = transformer2.transform(transformer1.transform(df))

    # Verify columns renamed
    assert "user_name" in result.columns
    assert "amount" in result.columns
    assert "name" not in result.columns
    assert "value" not in result.columns

    # Verify data unchanged
    assert result.count() == 10


def test_column_renamed_config(spark_session: SparkSession):
    """Test that transformer stores configuration correctly."""
    transformer = ColumnRenamed(old_name="old", new_name="new", extra_param="value")

    config = transformer.get_config()
    assert config["old_name"] == "old"
    assert config["new_name"] == "new"
    assert config["extra_param"] == "value"


def test_filter_by_expr(spark_session: SparkSession):
    """Test FilterByExpr transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.column_operations import FilterByExpr

    df = create_sample_dataframe(spark_session, 10)

    transformer = FilterByExpr(expression="id > 5")
    result = transformer.transform(df)

    # Should have 4 rows (id 6-9, since data has rows 0-9)
    assert result.count() == 4
    assert result.filter("id <= 5").count() == 0


def test_add_column_by_expr(spark_session: SparkSession):
    """Test AddColumnByExpr transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.column_operations import AddColumnByExpr

    df = create_sample_dataframe(spark_session, 10)

    transformer = AddColumnByExpr(
        column_name="doubled_value",
        expression="value * 2"
    )
    result = transformer.transform(df)

    # Verify new column exists
    assert "doubled_value" in result.columns

    # Verify values
    row = result.filter("id = 1").first()
    assert row["doubled_value"] == row["value"] * 2
