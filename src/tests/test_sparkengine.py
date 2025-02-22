import datetime

import pytest
from seeknal.tasks.sparkengine import SparkEngineTask
from seeknal.tasks.sparkengine import aggregators as G
from seeknal.tasks.sparkengine import transformers as T
from seeknal.tasks.sparkengine.transformers.spark_engine_transformers import (
    JoinTablesByExpr,
    JoinType,
    TableJoinDef,
)
from pyspark.sql import DataFrame

from .conftest import are_equal

input_table = "testfile"
test_tables = {
    "df1": "daily_features_1",
    "df2": "cdr_features_1",
}


def get_ids_from_df(df, id_col):
    result = df.orderBy(id_col).collect()
    ids = [i[id_col] for i in result]
    return ids


@pytest.fixture(scope="module")
def tmpdir_spark(tmpdir_factory):
    output_dir = tmpdir_factory.mktemp("input-data-spark")
    return output_dir


@pytest.fixture(scope="module")
def input_data_spark(spark):
    columns = "day:string, feature1:float, feature2:float, id:string"
    vals = [
        ("20190620", 1.0, 1.0, "1"),
        ("20190610", -1.0, -1.0, "1"),
        ("20190602", 50.0, 50.0, "1"),
        ("20190601", 0.0, 0.0, "1"),
        ("20190520", 22.2, 22.2, "1"),
        ("20190510", 2.0, 2.0, "1"),
        ("20190501", 2.1, 2.1, "1"),
        ("20190620", 1.0, 1.0, "2"),
        ("20190710", None, None, "2"),
        ("20190602", 50.0, 50.0, "2"),
        ("20190601", 0.0, 0.0, "2"),
        ("20190520", 22.2, 22.2, "2"),
        ("20190510", 2.0, 2.0, "2"),
        ("20190501", 2.1, 2.1, "2"),
    ]

    daily_features_1 = spark.createDataFrame(vals, columns)
    daily_features_1.write.saveAsTable(test_tables["df1"])

    columns = "msisdn:string, b_patry:string, date_id:string, service_type:string, device_type:string, direction_type:string, site_id:string, roaming_position:string, duration:int, hit:int, downlink_volume:float, uplink_volume:float, url_domain:string, total_hit:int"
    vals = [
        (
            "1234",
            "2345",
            "20180611",
            "Voice",
            "Outgoing",
            "9999",
            "OUTBOUND",
            2,
            2,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "1234",
            "2345",
            "20180617",
            "Voice",
            "Outgoing",
            "9998",
            "OUTBOUND",
            4,
            4,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "1234",
            "4567",
            "20180709",
            "Voice",
            "Outgoing",
            "9997",
            "LOCAL",
            8,
            8,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "1234",
            "5678",
            "20180715",
            "Voice",
            "Outgoing",
            "9999",
            "LOCAL",
            16,
            16,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "1234",
            "5678",
            "20180715",
            "Data",
            None,
            "9999",
            "OUTBOUND",
            None,
            16,
            10,
            15.0,
            25.0,
            "domain1",
            7,
        ),
        (
            "1234",
            "2345",
            "20180715",
            "Voice",
            "Outgoing",
            "9998",
            "LOCAL",
            16,
            16,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "1234",
            "2345",
            "20180715",
            "Data",
            None,
            "9998",
            "OUTBOUND",
            None,
            16,
            5,
            30.0,
            35.0,
            "domain1",
            2,
        ),
        (
            "1234",
            "2345",
            "20180715",
            "Sms",
            "Outgoing",
            "9999",
            "OUTBOUND",
            20,
            20,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "1234",
            "2345",
            "20180715",
            "Data",
            None,
            "9999",
            "LOCAL",
            None,
            20,
            10,
            10.0,
            20.0,
            "domain2",
            3,
        ),
    ]
    cdr_1 = spark.createDataFrame(vals, columns)
    cdr_1.write.saveAsTable(test_tables["df2"])


def test_transform_data_with_yaml(spark, input_data_spark):
    yaml_str = """
    pipeline:
      input:
        table: {}
      stages:
        - className: tech.mta.seeknal.transformers.SQL
          params:
            statement: >-
              SELECT day, CONCAT(id, "-append") as id, feature1, feature2 FROM __THIS__
    """.format(
        test_tables["df1"]
    )

    res = SparkEngineTask().add_yaml(yaml_str).transform(spark)
    assert ["1-append", "2-append"] == get_ids_from_df(
        res.select("id").distinct(), "id"
    )


def test_transform_data_with_yaml_and_common(spark, input_data_spark):
    common_str = """
    sources:
        - id: my_test_data
          source: hive
          table: {}
    """.format(
        test_tables["df1"]
    )

    yaml_str = """
    pipeline:
      input:
        id: my_test_data
      stages:
        - className: tech.mta.seeknal.transformers.SQL
          params:
            statement: >-
              SELECT day, CONCAT(id, "-append") as id, feature1, feature2 FROM __THIS__
    """

    res = (
        SparkEngineTask()
        .add_common_yaml(common_str)
        .add_yaml(yaml_str)
        .transform(spark)
    )

    assert ["1-append", "2-append"] == get_ids_from_df(
        res.select("id").distinct(), "id"
    )


def test_transform_with_dataframe(spark, input_data_spark):
    df = spark.table(test_tables["df1"])
    yaml_str = """
    pipeline:
      stages:
        - className: tech.mta.seeknal.transformers.SQL
          params:
            statement: >-
              SELECT day, CONCAT(id, "-append") as id, feature1, feature2 FROM __THIS__
    """
    res = (
        SparkEngineTask().add_yaml(yaml_str).add_input(dataframe=df).transform(spark)
    )
    assert ["1-append", "2-append"] == get_ids_from_df(
        res.select("id").distinct(), "id"
    )


def test_transform_with_no_yaml(spark, input_data_spark):
    df = spark.table(test_tables["df1"])
    add_month = T.Transformer(
        T.ClassName.ADD_DATE,
        inputCol="day",
        outputCol="month",
        inputDateFormat="yyyyMMdd",
        outputDateFormat="yyyy-MM-01",
    )
    res = (
        SparkEngineTask()
        .add_input(dataframe=df)
        .add_stage(
            transformer=T.SQL(
                statement="SELECT day, CONCAT(id, '-append') as id, feature1, feature2 FROM __THIS__"
            )
        )
        .add_stage(transformer=add_month)
        .transform(spark)
    )
    assert ["1-append", "2-append"] == get_ids_from_df(
        res.select("id").distinct(), "id"
    )

    aggr = G.Aggregator(
        group_by_cols=["id"],
        aggregators=[
            G.FunctionAggregator(
                inputCol="feature1", outputCol="feature1_sum", accumulatorFunction="sum"
            ),
            G.ExpressionAggregator(
                outputCol="feature2_sum", expression="sum(feature2)"
            ),
        ],
    )
    res = (
        SparkEngineTask(name="create aggregates")
        .add_input(dataframe=df)
        .add_stage(aggregator=aggr)
        .transform(spark)
    )

    assert ["id", "feature1_sum", "feature2_sum"] == res.columns

    last_n_aggr = G.LastNDaysAggregator(
        group_by_cols=["id"],
        window=50,
        date_col="day",
        date_pattern="yyyyMMdd",
        aggregators=[
            G.FunctionAggregator(
                inputCol="feature1", outputCol="feature1_sum", accumulatorFunction="sum"
            ),
            G.ExpressionAggregator(
                outputCol="feature2_sum", expression="sum(feature2)"
            ),
        ],
    )

    res = (
        SparkEngineTask(name="create aggregates with LastNDays")
        .add_input(dataframe=df)
        .add_stage(aggregator=last_n_aggr)
        .transform(spark)
    )

    expected_columns = "id:string, day:string, feature1_sum:float, feature2_sum:float"
    expected_vals = [
        ("1", "20190710", 50.0, 50.0),
        ("2", "20190710", 51.0, 51.0),
    ]

    expected_df = spark.createDataFrame(expected_vals, expected_columns)

    assert are_equal(res, expected_df)

def test_transform_with_point_in_time(spark, input_data_spark):
    spine_columns = "id:string, app_day:string"
    spine_vals = [("1", "20190510"), ("2", "20190715")]
    spine_df = spark.createDataFrame(spine_vals, spine_columns)

    point_in_time = T.PointInTime(
        how=T.Time.PAST,
        offset=0,
        length=30,
        feature_date="day",
        app_date="app_day",
        feature_date_format="yyyyMMdd",
        app_date_format="yyyyMMdd",
        spine=spine_df,
    )
    res = (
        SparkEngineTask()
        .add_input(table=test_tables["df1"])
        .add_stage(transformer=point_in_time)
        .transform(spark)
        .orderBy("day")
        .collect()
    )
    res_date = [i["day"] for i in res]
    assert ["20190501", "20190510", "20190620", "20190710"] == res_date


def test_transform_with_add_window_function(spark, input_data_spark):
    window_function = T.AddWindowFunction(
        inputCol="feature1",
        windowFunction=T.WindowFunction.AVG,
        partitionCols=["id"],
        outputCol="feature1_avg",
    )
    res = (
        SparkEngineTask()
        .add_input(table=test_tables["df1"])
        .add_stage(transformer=window_function)
        .transform(spark)
    )
    assert "feature1_avg" in res.columns
    window_function_last_distinct = T.AddWindowFunction(
        inputCol="day",
        windowFunction=T.WindowFunction.LAST_DISTINCT,
        partitionCols=["id"],
        orderCols=["day"],
        outputCol="last_date_feature1_equal_50",
        expression="feature1 = 50.0",
    )
    res = (
        SparkEngineTask()
        .add_input(table=test_tables["df1"])
        .add_stage(transformer=window_function_last_distinct)
        .transform(spark)
    )
    assert "last_date_feature1_equal_50" in res.columns


def test_transform_and_save(spark, input_data_spark, tmpdir_spark):
    yaml_str = """
    pipeline:
      input:
        table: {}
      output:
        source: file
        params:
            path: {}
            format: parquet
      stages:
        - className: tech.mta.seeknal.transformers.SQL
          params:
            statement: >-
              SELECT day, CONCAT(id, "-append") as id, feature1, feature2 FROM __THIS__
    """.format(
        test_tables["df1"], tmpdir_spark + "/test_output"
    )

    SparkEngineTask().add_yaml(yaml_str).transform(spark, materialize=True)


def test_transform_save_with_no_yaml(spark, input_data_spark, tmpdir_spark):
    df = spark.table(test_tables["df1"])
    res = (
        SparkEngineTask()
        .add_input(dataframe=df)
        .add_stage(
            transformer=T.SQL(
                statement="SELECT day, CONCAT(id, '-append') as id, feature1, feature2 FROM __THIS__"
            )
        )
        .transform(spark)
    )
    assert ["1-append", "2-append"] == get_ids_from_df(
        res.select("id").distinct(), "id"
    )

    aggr = G.Aggregator(
        group_by_cols=["id"],
        aggregators=[
            G.FunctionAggregator(
                inputCol="feature1", outputCol="feature1_sum", accumulatorFunction="sum"
            ),
            G.ExpressionAggregator(
                outputCol="feature2_sum", expression="sum(feature2)"
            ),
        ],
    )
    res = (
        SparkEngineTask(name="create aggregates")
        .add_input(dataframe=df)
        .add_stage(aggregator=aggr)
        .add_output(
            source="file",
            params={"path": "{}/test_output".format(tmpdir_spark), "format": "parquet"},
        )
        .transform(spark, materialize=True)
    )


def test_transform_with_join_by_expr(spark, input_data_spark):
    spine_columns = "msisdn:string, app_day:string, feature3:float"
    spine_vals = [("1", "20190510", 1.0), ("2", "20190715", 2.0)]
    spine_df = spark.createDataFrame(spine_vals, spine_columns)

    tables = [
        TableJoinDef(
            table=spine_df,
            joinType=JoinType.LEFT,
            alias="b",
            joinExpression="a.id = b.msisdn",
        )
    ]
    join = JoinTablesByExpr(tables=tables, select_stm="a.*, b.feature3, b.app_day")
    res = (
        SparkEngineTask()
        .add_input(dataframe=spark.table(test_tables["df1"]))
        .add_stage(transformer=join)
        .transform(spark)
    )
    res.show()
    assert ["day", "feature1", "feature2", "id", "feature3", "app_day"] == res.columns


def test_copy(spark, input_data_spark):
    df = spark.table(test_tables["df1"])
    add_month = T.Transformer(
        T.ClassName.ADD_DATE,
        inputCol="day",
        outputCol="month",
        inputDateFormat="yyyyMMdd",
        outputDateFormat="yyyy-MM-01",
    )
    preprocess = (
        SparkEngineTask()
        .add_input(dataframe=df)
        .add_stage(
            transformer=T.SQL(
                statement="SELECT day, CONCAT(id, '-append') as id, feature1, feature2 FROM __THIS__"
            )
        )
    )
    res_one = preprocess.copy().add_stage(transformer=add_month).transform(spark)
    res_two = (
        preprocess.copy()
        .add_sql("SELECT *, CONCAT(id, '-append-again') as id_b FROM __THIS__")
        .transform(spark)
    )

    assert ["1-append-append-again", "2-append-append-again"] == get_ids_from_df(
        res_two.select("id_b").distinct(), "id_b"
    )


def test_update_stage(spark, input_data_spark):
    df = spark.table(test_tables["df1"])
    preprocess = (
        SparkEngineTask()
        .add_input(dataframe=df)
        .add_stage(
            transformer=T.SQL(
                statement="SELECT day, CONCAT(id, '-append') as id, feature1, feature2 FROM __THIS__"
            )
        )
    )

    preprocess.update_stage(
        0,
        transformer=T.SQL(
            statement="SELECT *, CONCAT(id, '-append-again') as id_b FROM __THIS__"
        ),
    )

    res = preprocess.transform(spark)
    assert ["1-append-again", "2-append-again"] == get_ids_from_df(
        res.select("id_b").distinct(), "id_b"
    )


def test_insert_stage(spark, input_data_spark):
    df = spark.table(test_tables["df1"])
    preprocess = (
        SparkEngineTask()
        .add_input(dataframe=df)
        .add_stage(
            transformer=T.SQL(
                statement="SELECT day, CONCAT(id, '-append') as id, feature1, feature2 FROM __THIS__"
            )
        )
    )
    preprocess.insert_stage(
        0,
        transformer=T.SQL(
            statement="SELECT *, CONCAT(id, '-append-again') as id_b FROM __THIS__"
        ),
    )
    preprocess.print_yaml()

    res = preprocess.transform(spark)
    assert ["1-append", "2-append"] == get_ids_from_df(
        res.select("id").distinct(), "id"
    )

def test_save_yaml(spark, input_data_spark, tmpdir_spark):
    df = spark.table(test_tables["df1"])
    preprocess = (
        SparkEngineTask()
        .add_input(dataframe=df)
        .add_stage(
            transformer=T.SQL(
                statement="SELECT day, CONCAT(id, '-append') as id, feature1, feature2 FROM __THIS__"
            )
        )
    )
    preprocess.to_yaml_file(f"{tmpdir_spark}/test.yaml")

def test_run_with_date_range(spark, input_data_spark):
    df = spark.table(test_tables["df1"])

    res = (
        SparkEngineTask()
        .add_input(dataframe=df)
        .set_date_col("day")
        .add_stage(
            transformer=T.SQL(
                statement="SELECT day, CONCAT(id, '-append') as id, feature1, feature2 FROM __THIS__"
            )
        )
        .transform(
            start_date=datetime.datetime(2019, 5, 1),
            end_date=datetime.datetime(2019, 5, 10),
        )
    )
    res.show()


def test_get_date_available(spark, input_data_spark):
    df = spark.table(test_tables["df1"])

    res = (
        SparkEngineTask()
        .add_input(dataframe=df)
        .set_date_col("day")
        .get_date_available(after_date=datetime.datetime(2019, 5, 10))
    )
    print(res)
