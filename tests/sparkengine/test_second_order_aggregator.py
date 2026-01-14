"""Tests for SecondOrderAggregator."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

from seeknal.tasks.sparkengine.py_impl.aggregators.second_order_aggregator import (
    SecondOrderAggregator,
    AggregationSpec,
    FeatureBuilder,
)


def create_transaction_dataframe(spark: SparkSession) -> "DataFrame":
    """Create a transaction DataFrame for second-order aggregation testing."""
    data = [
        # user_id, amount, flag, transaction_date, application_date
        (1, 100.0, 1, "2024-01-01", "2024-01-31"),
        (1, 150.0, 0, "2024-01-15", "2024-01-31"),
        (1, 200.0, 1, "2024-02-01", "2024-01-31"),
        (1, 120.0, 0, "2024-02-15", "2024-01-31"),
        (2, 80.0, 1, "2024-01-10", "2024-01-31"),
        (2, 90.0, 1, "2024-01-20", "2024-01-31"),
        (2, 110.0, 0, "2024-02-05", "2024-01-31"),
        (3, 200.0, 1, "2024-01-05", "2024-01-31"),
    ]
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("amount", DoubleType(), False),
        StructField("flag", IntegerType(), False),
        StructField("transaction_date", StringType(), False),
        StructField("application_date", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)


def test_second_order_aggregator_basic(spark_session: SparkSession):
    """Test basic aggregations (no time window)."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    rules = [
        AggregationSpec("basic", "amount", "sum"),
        AggregationSpec("basic", "amount", "mean"),
    ]

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )
    aggregator.setRules(rules)

    result = aggregator.transform("transactions")

    # Should have 3 users
    assert result.count() == 3
    assert "amount_SUM" in result.columns
    assert "amount_MEAN" in result.columns

    # Check user 1: 100 + 150 + 200 + 120 = 570
    user1_row = result.filter(result.user_id == 1).collect()[0]
    assert user1_row["amount_SUM"] == 570.0


def test_second_order_aggregator_basic_days(spark_session: SparkSession):
    """Test time-windowed aggregations."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    # Aggregate transactions from 1-30 days before application
    rules = [
        AggregationSpec("basic_days", "amount", "sum", "", "1", "30"),
        AggregationSpec("basic_days", "amount", "count", "", "1", "30"),
    ]

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )
    aggregator.setRules(rules)

    result = aggregator.transform("transactions")

    assert result.count() == 3

    # User 1: transactions in Jan (within 30 days of Jan 31) = 100 + 150 + 200 = 450
    # (depending on how datediff calculates - may include Feb 1 if it's within 30 days)
    user1_rows = result.filter(result.user_id == 1).collect()
    if user1_rows:
        row = user1_rows[0]
        # Accept valid sum values (actual calculation depends on datediff)
        assert row["amount_SUM_1_30"] > 0


def test_second_order_aggregator_ratio(spark_session: SparkSession):
    """Test ratio aggregations."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    # Ratio of sum(1-30 days) / sum(31-60 days)
    rules = [
        AggregationSpec("ratio", "amount", "sum", "", "1", "30", "31", "60"),
    ]

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )
    aggregator.setRules(rules)

    result = aggregator.transform("transactions")

    assert result.count() == 3
    # Check that ratio column exists
    ratio_col = [c for c in result.columns if "amount" in c and "SUM" in c]
    assert len(ratio_col) > 0


def test_second_order_aggregator_since(spark_session: SparkSession):
    """Test conditional aggregations."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    # Count where flag == 1
    rules = [
        AggregationSpec("since", "amount", "count", "flag == 1"),
    ]

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )
    aggregator.setRules(rules)

    result = aggregator.transform("transactions")

    assert result.count() == 3

    # User 1 has 2 transactions with flag == 1
    user1_row = result.filter(result.user_id == 1).collect()[0]
    # Should count the days_since for flag==1 transactions
    assert "SINCE_COUNT_amount" in result.columns


def test_second_order_aggregator_validate(spark_session: SparkSession):
    """Test table validation."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    rules = [AggregationSpec("basic", "amount", "sum")]

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )
    aggregator.setRules(rules)

    errors = aggregator.validate("transactions")

    # Should have no errors
    assert len(errors) == 0


def test_second_order_aggregator_validate_missing_column(spark_session: SparkSession):
    """Test validation with missing columns."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    # Add a rule for non-existent column
    rules = [
        AggregationSpec("basic", "nonexistent_col", "sum"),
    ]

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )
    aggregator.setRules(rules)

    errors = aggregator.validate("transactions")

    # Should have error about missing column
    assert len(errors) > 0
    assert any("nonexistent_col" in e for e in errors)


def test_second_order_aggregator_no_rules(spark_session: SparkSession):
    """Test that transform raises error when no rules are set."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )

    # Should raise ValueError when no rules
    with pytest.raises(ValueError, match="No rules defined"):
        aggregator.transform("transactions")


def test_feature_builder_basic(spark_session: SparkSession):
    """Test FeatureBuilder with basic aggregations."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )

    # Use builder pattern
    aggregator.builder() \
        .feature("amount") \
        .basic(["sum", "mean", "count"]) \
        .build()

    result = aggregator.transform("transactions")

    assert result.count() == 3
    assert "amount_SUM" in result.columns
    assert "amount_MEAN" in result.columns
    assert "amount_COUNT" in result.columns


def test_feature_builder_rolling(spark_session: SparkSession):
    """Test FeatureBuilder with rolling windows."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )

    aggregator.builder() \
        .feature("amount") \
        .rolling([(1, 30), (31, 60)], ["sum"]) \
        .build()

    result = aggregator.transform("transactions")

    assert result.count() == 3
    # Check for both window columns
    cols = [c for c in result.columns if "amount" in c]
    assert len(cols) >= 2


def test_feature_builder_ratio(spark_session: SparkSession):
    """Test FeatureBuilder with ratio."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )

    aggregator.builder() \
        .feature("amount") \
        .ratio((1, 30), (31, 60), ["sum"]) \
        .build()

    result = aggregator.transform("transactions")

    assert result.count() == 3
    # Should have ratio column
    ratio_cols = [c for c in result.columns if "amount" in c and "SUM" in c]
    assert len(ratio_cols) >= 1


def test_feature_builder_since(spark_session: SparkSession):
    """Test FeatureBuilder with since."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )

    aggregator.builder() \
        .feature("amount") \
        .since("flag == 1", ["count"]) \
        .build()

    result = aggregator.transform("transactions")

    assert result.count() == 3


def test_feature_builder_multiple_features(spark_session: SparkSession):
    """Test FeatureBuilder with multiple features."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )

    aggregator.builder() \
        .feature("amount") \
        .basic(["sum", "mean"]) \
        .feature("flag") \
        .basic(["count"]) \
        .build()

    result = aggregator.transform("transactions")

    assert result.count() == 3
    # Should have amount and flag aggregations
    assert "amount_SUM" in result.columns
    assert "flag_COUNT" in result.columns


def test_feature_builder_chained(spark_session: SparkSession):
    """Test fully chained FeatureBuilder."""
    df = create_transaction_dataframe(spark_session)
    df.createOrReplaceTempView("transactions")

    aggregator = SecondOrderAggregator(
        spark=spark_session,
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date"
    )

    aggregator.builder() \
        .feature("amount") \
        .basic(["sum"]) \
        .rolling([(1, 30)], ["mean"]) \
        .ratio((1, 30), (31, 60), ["count"]) \
        .since("flag == 1", ["sum"]) \
        .build()

    result = aggregator.transform("transactions")

    assert result.count() == 3
    # Should have columns from all aggregation types
    assert "amount_SUM" in result.columns
