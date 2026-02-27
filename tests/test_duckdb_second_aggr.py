import pytest
import duckdb
import pandas as pd
import numpy as np
from seeknal.tasks.duckdb.aggregators.second_order_aggregator import SecondOrderAggregator, AggregationSpec

@pytest.fixture
def input_data():
    # Schema: id, feature1 (numeric), f_date (feature date), a_date (application date), feature2 (categorical/string)
    data = {
        "id": [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3],
        "feature1": [None, 2.0, 2.0, 2.0, 3.0, 10.0, 4.0, 3.0, 3.0, None, 2.0, None, None],
        "f_date": [
            "2019-01-10", "2019-01-13", "2019-01-15", "2019-01-16", "2019-01-14", "2019-01-21",
            "2019-01-01", "2019-01-02", "2019-01-03", "2019-01-12", "2019-01-18", "2019-01-15",
            "2019-01-22",
        ],
        "a_date": [
            "2019-01-20", "2019-01-20", "2019-01-20", "2019-01-20", "2019-01-20", "2019-01-20",
            "2019-01-23", "2019-01-23", "2019-01-23", "2019-01-23", "2019-01-23", "2019-01-23",
            "2019-01-30",
        ],
        "feature2": ["b", None, "a", "a", "a", "b", "a", None, "a", "c", "a", "a", "c"],
    }
    df = pd.DataFrame(data)
    # Ensure string columns use 'object' dtype (DuckDB doesn't recognize pandas 'str' dtype)
    for col in ["f_date", "a_date", "feature2"]:
        df[col] = df[col].astype(object)
    return df

@pytest.fixture
def duckdb_con(input_data):
    con = duckdb.connect()
    con.register("input_table", input_data)
    return con

def test_basic_aggregation(duckdb_con):
    aggregator = SecondOrderAggregator(
        idCol="id",
        featureDateCol="f_date",
        applicationDateCol="a_date",
        conn=duckdb_con
    )

    rules = [
        AggregationSpec("basic", "feature1", "count, sum, mean, min, max, std"),
        AggregationSpec("basic", "feature2", "count"),
    ]

    res = aggregator.setRules(rules).transform("input_table").df()
    res = res.sort_values("id").reset_index(drop=True)

    # Basic aggregations filter _days_between >= 0 (only historical data).
    # ID 1: app_date=2019-01-20
    #   Dates: 10(diff=10), 13(7), 15(5), 16(4), 14(6), 21(-1 FUTURE excluded)
    #   feature1 values (historical only): [None, 2, 2, 2, 3] → non-null: [2, 2, 2, 3]
    #   Count=4, Sum=9, Mean=2.25, Min=2, Max=3
    # ID 2: app_date=2019-01-23 — all dates before, all historical
    #   feature1: [4, 3, 3, None, 2, None] → non-null: [4, 3, 3, 2]. Count=4, Sum=12, Mean=3
    # ID 3: app_date=2019-01-30, f_date=2019-01-22 (diff=8, historical)
    #   feature1: [None] → Count=0

    assert len(res) == 3

    # Check ID 1 (future row excluded by _days_between >= 0)
    row1 = res[res.id == 1].iloc[0]
    assert row1["feature1_COUNT"] == 4
    assert row1["feature1_SUM"] == 9.0
    assert np.isclose(row1["feature1_MEAN"], 2.25, atol=1e-4)
    assert row1["feature1_MIN"] == 2.0
    assert row1["feature1_MAX"] == 3.0
    assert np.isclose(row1["feature1_STD"], 0.5, atol=1e-4)

    # Check ID 3 (All nulls — still historical, but no non-null values)
    row3 = res[res.id == 3].iloc[0]
    assert row3["feature1_COUNT"] == 0
    assert pd.isna(row3["feature1_SUM"]) or row3["feature1_SUM"] == 0

def test_basic_days_aggregation(duckdb_con):
    aggregator = SecondOrderAggregator(
        idCol="id",
        featureDateCol="f_date",
        applicationDateCol="a_date",
        conn=duckdb_con
    )

    # ID 1: app_date=2019-01-20.
    # Dates: 10 (diff 10), 13 (7), 15 (5), 16 (4), 14 (6), 21 (-1).
    # Window 1-6 days: 15 (5), 16 (4), 14 (6).
    # feature1 values for these: 2.0, 2.0, 3.0.
    # Count: 3. Sum: 7. Mean: 2.333.

    rules = [
        AggregationSpec("basic_days", "feature1", "count, sum, mean", "", 1, 6)
    ]

    res = aggregator.setRules(rules).transform("input_table").df()
    row1 = res[res.id == 1].iloc[0]

    assert row1["feature1_COUNT_1_6"] == 3
    assert row1["feature1_SUM_1_6"] == 7.0
    assert np.isclose(row1["feature1_MEAN_1_6"], 2.3333, atol=1e-3)

def test_ratio_aggregation(duckdb_con):
    aggregator = SecondOrderAggregator(
        idCol="id",
        featureDateCol="f_date",
        applicationDateCol="a_date",
        conn=duckdb_con
    )

    # ID 1:
    # Num: Window 1-6 days (val: 2, 2, 3) -> Mean 2.333
    # Denom: Window 7-25 days.
    # Dates in 7-25: 13 (7), 10 (10). 21 is -1 (excluded by window).
    # feature1 values: 2.0, None.
    # So valid for denom: 2.0. Mean = 2.0.
    # Ratio: 2.333 / 2.0 = 1.1666

    rules = [
        AggregationSpec("ratio", "feature1", "mean", "", 1, 6, 7, 25)
    ]

    res = aggregator.setRules(rules).transform("input_table").df()
    row1 = res[res.id == 1].iloc[0]

    col_name = "feature1_MEAN1_6_MEAN7_25"
    assert np.isclose(row1[col_name], 1.1666, atol=1e-3)

def test_since_aggregation(duckdb_con):
    aggregator = SecondOrderAggregator(
        idCol="id",
        featureDateCol="f_date",
        applicationDateCol="a_date",
        conn=duckdb_con
    )

    # ID 1: feature2 has data.
    # Condition: feature2 == 'a'
    # Rows for ID 1 where feature2='a':
    # 2019-01-15 (val 2.0), 2019-01-16 (val 2.0), 2019-01-14 (val 3.0).
    # All are valid. Count = 3.

    # Rule with SQL syntax equivalent (DuckDB matches string literals)
    rules = [
        AggregationSpec("since", "feature1", "count", "feature2 = 'a'"),
        AggregationSpec("since", "feature1", "sum", "feature2 = 'a'")
    ]

    res = aggregator.setRules(rules).transform("input_table").df()
    row1 = res[res.id == 1].iloc[0]

    assert row1["SINCE_COUNT_feature1_GEO_D"] == 3
    assert row1["SINCE_SUM_feature1_GEO_D"] == 7.0
