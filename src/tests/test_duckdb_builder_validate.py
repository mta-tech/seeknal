import pytest
import duckdb
import pandas as pd
from seeknal.tasks.duckdb.aggregators.second_order_aggregator import SecondOrderAggregator, AggregationSpec

@pytest.fixture
def input_data():
    data = [
        (1, "2019-01-10", "2019-01-20", 100),
    ]
    df = pd.DataFrame(data, columns=["id", "f_date", "a_date", "amount"])
    return df

@pytest.fixture
def duckdb_con(input_data):
    con = duckdb.connect()
    con.register("transactions", input_data)
    return con

def test_validate_method(duckdb_con):
    aggregator = SecondOrderAggregator(
        idCol="id", 
        featureDateCol="f_date", 
        applicationDateCol="a_date",
        conn=duckdb_con
    )
    
    # All columns present
    aggregator.rules = [AggregationSpec("basic", "amount", "sum")]
    errors = aggregator.validate("transactions")
    assert len(errors) == 0
    
    # Missing optional feature columns
    aggregator.rules.append(AggregationSpec("basic", "missing_col", "sum"))
    errors = aggregator.validate("transactions")
    assert len(errors) == 1
    assert "Missing feature columns" in errors[0]
    assert "'missing_col'" in errors[0] or "missing_col" in errors[0]

    # Missing ID column config
    aggregator_bad = SecondOrderAggregator(
        idCol="bad_id", 
        featureDateCol="f_date", 
        applicationDateCol="a_date",
        conn=duckdb_con
    )
    errors = aggregator_bad.validate("transactions")
    assert any("Missing ID column" in e for e in errors)

def test_feature_builder_construction(duckdb_con):
    aggregator = SecondOrderAggregator(conn=duckdb_con)
    
    aggregator.builder() \
        .feature("amount") \
            .basic(["sum", "mean"]) \
            .rolling(days=[(1, 30)], aggs=["max"]) \
            .ratio(numerator=(1, 30), denominator=(31, 60), aggs=["mean"]) \
        .feature("flag") \
            .since(condition="flag=1", aggs=["count"]) \
        .build()
        
    assert len(aggregator.rules) == 5
    
    # Check rule content
    r1 = aggregator.rules[0] # basic sum
    assert r1.name == "basic" and r1.features == "amount" and r1.aggregations == "sum"
    
    r3 = aggregator.rules[2] # rolling max
    assert r3.name == "basic_days" and r3.dayLimitLower1 == 1 and r3.dayLimitUpper1 == 30
    
    r5 = aggregator.rules[4] # since count
    assert r5.name == "since" and r5.features == "flag" and r5.filterCondition == "flag=1"

def test_builder_integration(duckdb_con):
    aggregator = SecondOrderAggregator(
        idCol="id", 
        featureDateCol="f_date", 
        applicationDateCol="a_date",
        conn=duckdb_con
    )
    
    res = aggregator.builder() \
        .feature("amount") \
        .basic(["sum"]) \
        .build() \
        .transform("transactions") \
        .df()
        
    assert len(res) == 1
    assert res.iloc[0]["id"] == 1
    assert res.iloc[0]["amount_SUM"] == 100
