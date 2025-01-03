import pyarrow.parquet as pq
import pytest
from seeknal.tasks.duckdb import DuckDBTask
import os


def test_duckdb_task():

    arrow_df = pq.read_table("tests/data/poi_sample.parquet")
    my_duckdb = (
        DuckDBTask()
        .add_input(dataframe=arrow_df)
        .add_sql("SELECT poi_name, lat, long FROM __THIS__")
        .add_sql("SELECT poi_name, lat FROM __THIS__")
    )
    print(my_duckdb.__dict__)
    res = my_duckdb.transform()

    print(res)


def test_duckdb_task_with_path():

    my_duckdb = (
        DuckDBTask()
        .add_input(path="tests/data/poi_sample.parquet/*.parquet")
        .add_sql("SELECT poi_name, lat, long FROM __THIS__")
        .add_sql("SELECT poi_name, lat FROM __THIS__")
    )
    print(my_duckdb.__dict__)
    res = my_duckdb.transform()

    print(res)
