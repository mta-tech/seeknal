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
