from pyspark.sql import SparkSession, DataFrame, functions as F
import pytest
from .conftest import are_equal

from seeknal.tasks.sparkengine import SparkEngineTask
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.flow import (
    Flow,
    FlowInput,
    FlowOutput,
    FlowInputEnum,
    FlowOutputEnum,
    run_flow,
)
from seeknal.project import Project
from seeknal.common_artifact import Source, Dataset


@pytest.fixture(scope="module")
def input_data_spark(spark):
    Project(name="test_project").get_or_create()
    columns = "msisdn:string, hour:string, cell_id:string, azimuth:string, location_type:string, lat:double, lon:double, date_id:string, timestamp:string"
    vals = [
        (
            "id1",
            "07",
            "cell2",
            "310",
            "mega",
            40.8379525833,
            -73.71209875,
            "20190901",
            "2019-09-01 07:15:00",
        ),
        (
            "id1",
            "07",
            "cell1",
            "120",
            "mini",
            40.8379525833,
            -73.70209875,
            "20190901",
            "2019-09-01 07:20:00",
        ),
        (
            "id1",
            "07",
            "cell2",
            "310",
            "mega",
            40.8379525833,
            -73.71209875,
            "20190901",
            "2019-09-01 07:40:00",
        ),
        (
            "id1",
            "07",
            "cell2",
            "100",
            "mega",
            40.8379525833,
            -73.72209875,
            "20190901",
            "2019-09-01 07:51:00",
        ),
        (
            "id1",
            "07",
            "cell3",
            "100",
            "mini",
            40.8379525833,
            -73.72209875,
            "20190901",
            "2019-09-01 07:52:00",
        ),
        (
            "id1",
            "07",
            "cell3",
            "100",
            "mini",
            40.8379525833,
            -73.72209875,
            "20190901",
            "2019-09-01 07:53:00",
        ),
        (
            "id1",
            "07",
            "cell4",
            "200",
            "micro",
            40.8379525833,
            -73.72209875,
            "20190901",
            "2019-09-01 07:54:00",
        ),
        (
            "id1",
            "07",
            "cell4",
            "100",
            "micro",
            40.8379525833,
            -73.72209875,
            "20190901",
            "2019-09-01 07:55:00",
        ),
        (
            "id1",
            "07",
            "cell4",
            "100",
            "micro",
            40.8379525833,
            -73.72209875,
            "20190901",
            "2019-09-01 07:56:00",
        ),
        (
            "id1",
            "08",
            "cell4",
            "100",
            "micro",
            40.8379525833,
            -73.72209875,
            "20190901",
            "2019-09-01 08:01:00",
        ),
        (
            "id1",
            "09",
            "cell2",
            "100",
            "mega",
            40.848,
            -73.7231,
            "20190901",
            "2019-09-01 09:01:00",
        ),
        (
            "id1",
            "09",
            "cell3",
            "100",
            "mini",
            40.848,
            -73.7231,
            "20190901",
            "2019-09-01 09:05:00",
        ),
        (
            "id1",
            "09",
            "cell3",
            "100",
            "mini",
            40.848,
            -73.7223,
            "20190901",
            "2019-09-01 09:10:00",
        ),
        (
            "id1",
            "09",
            "cell4",
            "200",
            "micro",
            40.848,
            -73.723,
            "20190901",
            "2019-09-01 09:15:00",
        ),
    ]

    user_loc = spark.createDataFrame(vals, columns)
    user_loc.write.mode("overwrite").saveAsTable("user_loc")

    columns = "msisdn:string, lat:double, lon:double, start_time:string, end_time:string, count_hours:double, radius:double, movement_type:string, day:string"
    vals = [
        (
            "id1",
            3.1165,
            101.5663,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            1395.04,
            "stay",
            "20190101",
        ),
        (
            "id2",
            3.812033,
            103.324633,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            841.36,
            "stay",
            "20190101",
        ),
        (
            "id3",
            3.0637,
            101.47016,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            1387.35,
            "stay",
            "20190101",
        ),
        (
            "id1",
            3.1186,
            101.6639,
            "2019-01-01 07:00:00",
            "2019-01-01 08:00:00",
            1.0,
            1234.22,
            "stay",
            "20190101",
        ),
        (
            "id1",
            3.1165,
            101.5663,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            1395.04,
            "stay",
            "20190102",
        ),
        (
            "id2",
            3.812033,
            103.324633,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            841.36,
            "stay",
            "20190102",
        ),
        (
            "id3",
            3.0637,
            101.47016,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            1387.35,
            "stay",
            "20190102",
        ),
        (
            "id1",
            3.1186,
            101.6639,
            "2019-01-01 07:00:00",
            "2019-01-01 08:00:00",
            1.0,
            1234.22,
            "stay",
            "20190102",
        ),
        (
            "id1",
            3.1165,
            101.5663,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            1395.04,
            "stay",
            "20190105",
        ),
        (
            "id2",
            3.812033,
            103.324633,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            841.36,
            "stay",
            "20190105",
        ),
        (
            "id3",
            3.0637,
            101.47016,
            "2019-01-01 06:00:00",
            "2019-01-01 07:00:00",
            1.0,
            1387.35,
            "stay",
            "20190105",
        ),
        (
            "id1",
            3.1186,
            101.6639,
            "2019-01-01 07:00:00",
            "2019-01-01 08:00:00",
            1.0,
            1234.22,
            "stay",
            "20190105",
        ),
    ]

    user_stay = spark.createDataFrame(vals, columns)
    (
        SparkEngineTask()
        .add_input(dataframe=user_stay)
        .transform()
        .write.mode("overwrite")
        .saveAsTable("user_stay")
    )
    return spark


@pytest.fixture(scope="module")
def create_flow_one(spark):

    flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="user_stay")
    flow_output = FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)

    task_one = SparkEngineTask().add_sql("SELECT * FROM __THIS__")
    task_two = DuckDBTask().add_sql("SELECT msisdn, lat, lon, movement_type, day FROM __THIS__")
    flow = Flow(
        name="my_flow",
        input=flow_input,
        tasks=[task_one, task_two],
        output=FlowOutput(),
    )

    return flow


@pytest.fixture(scope="module")
def create_flow_two(spark):

    flow_input = FlowInput(kind=FlowInputEnum.HIVE_TABLE, value="user_stay")
    flow_output = FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)

    task_one = SparkEngineTask().add_sql("SELECT * FROM __THIS__")
    flow = Flow(
        name="my_flow_two",
        input=flow_input,
        tasks=[task_one],
        output=FlowOutput(),
    )

    return flow


def test_flow_run(input_data_spark, create_flow_one):
    flow = create_flow_one
    result = flow.run()
    print(result)


def test_serialize_flow(input_data_spark, create_flow_one):
    flow = create_flow_one
    serialized = flow.as_dict()
    my_flow = flow.from_dict(serialized)
    my_flow.run()

    print(serialized)


def test_save_flow(input_data_spark, create_flow_one):
    Project(name="test_project").get_or_create()

    flow = create_flow_one

    flow.get_or_create()

    abc = Flow("my_flow").get_or_create()
    abc.run()


def test_update_flow(input_data_spark, create_flow_one, create_flow_two):
    Project(name="test_project").get_or_create()

    flow = create_flow_one
    flow_two = create_flow_two

    abc = Flow("my_flow").get_or_create()
    abc.update(tasks=flow_two.tasks)

    res = abc.run()
    print(res)

    abc.update(tasks=flow.tasks)
    res = abc.run()
    print(res)


def test_delete_flow(create_flow_one, create_flow_two):
    Project(name="test_project").get_or_create()

    flow = create_flow_one
    flow.get_or_create()
    flow.delete()

    Flow.list()
    flow.get_or_create()
    Flow.list()
