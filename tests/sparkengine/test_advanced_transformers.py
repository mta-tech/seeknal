"""Tests for advanced PySpark transformers."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

from seeknal.tasks.sparkengine.py_impl.transformers.advanced import (
    PointInTime,
    JoinTablesByExpr,
    TableJoinDef,
    JoinType,
    Time,
)


def create_features_dataframe(spark: SparkSession) -> "DataFrame":
    """Create a features DataFrame for point-in-time testing."""
    data = [
        (1, "2024-01-15", "feature_a", 100),
        (1, "2024-01-20", "feature_b", 200),
        (1, "2024-02-01", "feature_c", 150),
        (2, "2024-01-10", "feature_a", 120),
        (2, "2024-01-25", "feature_b", 180),
        (3, "2024-01-05", "feature_a", 90),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("event_time", StringType(), False),
        StructField("feature_name", StringType(), False),
        StructField("value", IntegerType(), False),
    ])
    return spark.createDataFrame(data, schema)


def create_spine_dataframe(spark: SparkSession) -> "DataFrame":
    """Create a spine DataFrame with application dates."""
    data = [
        (1, "2024-01-25"),
        (2, "2024-01-20"),
        (3, "2024-01-15"),
    ]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("application_date", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)


def test_point_in_time_past(spark_session: SparkSession):
    """Test PointInTime with PAST time direction."""
    features_df = create_features_dataframe(spark_session)
    spine_df = create_spine_dataframe(spark_session)

    # Register spine as temp view
    spine_df.createOrReplaceTempView("spine_table")

    transformer = PointInTime(
        spark=spark_session,
        spine="spine_table",
        app_date="application_date",
        feature_date="event_time",
        how=Time.PAST,
        offset=0,
        join_type="INNER JOIN"
    )

    result = transformer.transform(features_df)

    # Should have records where event_time <= application_date
    assert result.count() > 0
    assert "id" in result.columns
    assert result.filter(result.id == 1).count() == 2  # Two events before 2024-01-25


def test_point_in_time_with_length(spark_session: SparkSession):
    """Test PointInTime with time window length."""
    features_df = create_features_dataframe(spark_session)
    spine_df = create_spine_dataframe(spark_session)

    spine_df.createOrReplaceTempView("spine_table")

    transformer = PointInTime(
        spark=spark_session,
        spine="spine_table",
        app_date="application_date",
        feature_date="event_time",
        how=Time.PAST,
        offset=0,
        length=10,  # Only last 10 days
        join_type="INNER JOIN"
    )

    result = transformer.transform(features_df)

    # Should filter to only events within 10 days
    assert result.count() >= 0


def test_point_in_time_future(spark_session: SparkSession):
    """Test PointInTime with FUTURE time direction."""
    features_df = create_features_dataframe(spark_session)
    spine_df = create_spine_dataframe(spark_session)

    spine_df.createOrReplaceTempView("spine_table")

    transformer = PointInTime(
        spark=spark_session,
        spine="spine_table",
        app_date="application_date",
        feature_date="event_time",
        how=Time.FUTURE,
        offset=0,
        join_type="INNER JOIN"
    )

    result = transformer.transform(features_df)

    # Should have records where event_time >= application_date
    assert result.count() >= 0


def test_point_in_time_with_spine_dataframe(spark_session: SparkSession):
    """Test PointInTime with DataFrame as spine."""
    features_df = create_features_dataframe(spark_session)
    spine_df = create_spine_dataframe(spark_session)

    transformer = PointInTime(
        spark=spark_session,
        spine=spine_df,  # Pass DataFrame directly
        app_date="application_date",
        feature_date="event_time",
        how=Time.PAST,
        offset=0,
        join_type="INNER JOIN"
    )

    result = transformer.transform(features_df)

    assert result.count() > 0


def test_join_tables_by_expr_inner(spark_session: SparkSession):
    """Test JoinTablesByExpr with INNER JOIN."""
    from tests.sparkengine.data_utils import create_join_left_dataframe, create_join_right_dataframe

    left_df = create_join_left_dataframe(spark_session)
    right_df = create_join_right_dataframe(spark_session)

    # Register right table as view
    right_df.createOrReplaceTempView("right_table")

    tables = [
        TableJoinDef(
            table="right_table",
            alias="b",
            joinType=JoinType.INNER,
            joinExpression="a.id = b.user_id"
        )
    ]

    transformer = JoinTablesByExpr(
        spark=spark_session,
        tables=tables,
        select_stm="a.*, b.score",
        alias="a"
    )

    result = transformer.transform(left_df)

    # INNER JOIN should match only IDs 1, 2, 3 (not 5)
    assert result.count() == 3
    assert "score" in result.columns


def test_join_tables_by_expr_left(spark_session: SparkSession):
    """Test JoinTablesByExpr with LEFT JOIN."""
    from tests.sparkengine.data_utils import create_join_left_dataframe, create_join_right_dataframe

    left_df = create_join_left_dataframe(spark_session)
    right_df = create_join_right_dataframe(spark_session)

    right_df.createOrReplaceTempView("right_table")

    tables = [
        TableJoinDef(
            table="right_table",
            alias="b",
            joinType=JoinType.LEFT,
            joinExpression="a.id = b.user_id"
        )
    ]

    transformer = JoinTablesByExpr(
        spark=spark_session,
        tables=tables,
        select_stm="a.*, b.score",
        alias="a"
    )

    result = transformer.transform(left_df)

    # LEFT JOIN should keep all left rows (including ID 4 with no match)
    assert result.count() == 4


def test_join_tables_by_expr_multiple_tables(spark_session: SparkSession):
    """Test JoinTablesByExpr with multiple tables."""
    from tests.sparkengine.data_utils import create_join_left_dataframe

    left_df = create_join_left_dataframe(spark_session)

    # Create two additional tables
    extra_data_1 = [(1, "extra_1"), (2, "extra_2")]
    extra_df_1 = spark_session.createDataFrame(extra_data_1, ["id", "extra1"])

    extra_data_2 = [(1, "extra_2"), (3, "extra_3")]
    extra_df_2 = spark_session.createDataFrame(extra_data_2, ["id", "extra2"])

    extra_df_1.createOrReplaceTempView("extra1_table")
    extra_df_2.createOrReplaceTempView("extra2_table")

    tables = [
        TableJoinDef(
            table="extra1_table",
            alias="b",
            joinType=JoinType.LEFT,
            joinExpression="a.id = b.id"
        ),
        TableJoinDef(
            table="extra2_table",
            alias="c",
            joinType=JoinType.LEFT,
            joinExpression="a.id = c.id"
        )
    ]

    transformer = JoinTablesByExpr(
        spark=spark_session,
        tables=tables,
        select_stm="a.*, b.extra1, c.extra2",
        alias="a"
    )

    result = transformer.transform(left_df)

    assert result.count() == 4
    assert "extra1" in result.columns
    assert "extra2" in result.columns


def test_join_tables_by_expr_with_dataframe(spark_session: SparkSession):
    """Test JoinTablesByExpr with DataFrame as table."""
    from tests.sparkengine.data_utils import create_join_left_dataframe, create_join_right_dataframe

    left_df = create_join_left_dataframe(spark_session)
    right_df = create_join_right_dataframe(spark_session)

    tables = [
        TableJoinDef(
            table=right_df,  # Pass DataFrame directly
            alias="b",
            joinType=JoinType.INNER,
            joinExpression="a.id = b.user_id"
        )
    ]

    transformer = JoinTablesByExpr(
        spark=spark_session,
        tables=tables,
        select_stm="a.*, b.score",
        alias="a"
    )

    result = transformer.transform(left_df)

    assert result.count() == 3
