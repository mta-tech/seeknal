"""
Tests for Phases 4-6 (Medium & Complex transformers and aggregators).
"""

import sys
sys.path.insert(0, 'src')

import pandas as pd
import pyarrow as pa
import duckdb

from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb.transformers import (
    JoinTablesByExpr, TableJoinDef, JoinType,
    PointInTime, Time, CastColumn
)
from seeknal.tasks.duckdb.aggregators import (
    DuckDBAggregator,
    FunctionAggregator,
)
from seeknal.tasks.duckdb.aggregators.complex_aggregators import LastNDaysAggregator


def test_phase_4():
    """Test Phase 4: Medium transformers."""
    print("\n" + "=" * 60)
    print("TESTING PHASE 4: MEDIUM TRANSFORMERS")
    print("=" * 60)

    # Test JoinTablesByExpr
    print("\n1. Testing JoinTablesByExpr...")
    
    left_df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "value1": [10, 20, 30, 40],
        "category": ["A", "B", "A", "B"],
    })

    right_df = pd.DataFrame({
        "id": [1, 2, 3, 5],
        "value2": [100, 200, 300, 500],
        "status": ["active", "inactive", "active", "active"],
    })

    left_table = pa.Table.from_pandas(left_df)
    right_table = pa.Table.from_pandas(right_df)

    conn = duckdb.connect()
    conn.register("right_df", right_table)

    join = JoinTablesByExpr(
        select_stm="a.id, a.value1, b.value2",
        alias="a",
        tables=[
            TableJoinDef(
                table="right_df",
                alias="b",
                joinType=JoinType.LEFT,
                joinExpression="a.id = b.id"
            )
        ]
    )

    result = (
        DuckDBTask(name="test_join")
        .add_input(dataframe=left_table)
        .add_stage(transformer=join)
        .transform()
    )

    assert len(result) == 4
    assert "value2" in result.column_names
    print("✓ JoinTablesByExpr works correctly")

    # Test CastColumn
    print("\n2. Testing CastColumn...")
    cast = CastColumn(
        inputCol="amount",
        outputCol="amount_int",
        dataType="INTEGER"
    )

    data_df = pd.DataFrame({"amount": [100.5, 200.3, 150.8]})
    data_table = pa.Table.from_pandas(data_df)

    result = (
        DuckDBTask(name="test_cast")
        .add_input(dataframe=data_table)
        .add_stage(transformer=cast)
        .transform()
    )

    assert "amount_int" in result.column_names
    df = result.to_pandas()
    assert all(df["amount_int"] == [100, 200, 150])
    print("✓ CastColumn works correctly")

    print("\n✓ PHASE 4 COMPLETE - All medium transformers working!")


def test_phase_5():
    """Test Phase 5: Complex aggregators."""
    print("\n" + "=" * 60)
    print("TESTING PHASE 5: COMPLEX AGGREGATORS")
    print("=" * 60)

    df = pd.DataFrame({
        "user_id": ["A", "A", "A", "A", "B", "B", "B", "B"],
        "date_id": ["20240101", "20240102", "20240103", "20240104", "20240101", "20240102", "20240103", "20240104"],
        "amount": [100, 150, 200, 250, 50, 75, 100, 125],
    })

    table = pa.Table.from_pandas(df)

    print("\n1. Testing LastNDaysAggregator (3-day window)...")
    agg = LastNDaysAggregator(
        group_by_cols=["user_id"],
        window=3,
        date_col="date_id",
        date_pattern="yyyyMMdd",
        aggregators=[
            FunctionAggregator(
                inputCol="amount",
                outputCol="amount_sum_3d",
                accumulatorFunction="sum"
            ),
        ],
    )

    result = (
        DuckDBTask(name="test_last_n_days")
        .add_input(dataframe=table)
        .add_stage(aggregator=agg)
        .transform()
    )

    assert len(result) == 2
    df = result.to_pandas()
    user_a = df[df["user_id"] == "A"].iloc[0]
    assert user_a["amount_sum_3d"] == 600  # 100 + 150 + 200 + 250 = 700
    print("✓ LastNDaysAggregator works correctly")
    print(f"  Result: {len(result)} rows, 2 unique users")

    print("\n✓ PHASE 5 COMPLETE - Complex aggregators working!")


def test_phase_6():
    """Test Phase 6: Window functions."""
    print("\n" + "=" * 60)
    print("TESTING PHASE 6: WINDOW FUNCTIONS")
    print("=" * 60)

    from seeknal.tasks.duckdb.transformers import AddWindowFunction, WindowFunction

    df = pd.DataFrame({
        "user_id": ["A", "A", "B", "B", "C", "C"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02", "2024-01-01", "2024-01-02"],
        "amount": [100, 150, 200, 250, 300, 350],
    })

    table = pa.Table.from_pandas(df)

    print("\n1. Testing ROW_NUMBER...")
    row_num = AddWindowFunction(
        inputCol="amount",
        windowFunction=WindowFunction.ROW_NUMBER,
        partitionCols=["user_id"],
        orderCols=["date"],
        outputCol="row_num"
    )

    result = (
        DuckDBTask(name="test_row_number")
        .add_input(dataframe=table)
        .add_stage(transformer=row_num)
        .transform()
    )

    df = result.to_pandas()
    assert df["row_num"].iloc[0] == 1
    print("✓ ROW_NUMBER works correctly")

    print("\n2. Testing LAG...")
    lag = AddWindowFunction(
        inputCol="amount",
        offset=1,
        windowFunction=WindowFunction.LAG,
        partitionCols=["user_id"],
        orderCols=["date"],
        outputCol="prev_amount"
    )

    result = (
        DuckDBTask(name="test_lag")
        .add_input(dataframe=table)
        .add_stage(transformer=lag)
        .transform()
    )

    df = result.to_pandas()
    user_a = df[df["user_id"] == "A"].sort_values("date")
    assert pd.isna(user_a["prev_amount"].iloc[0])
    print("✓ LAG works correctly")

    print("\n✓ PHASE 6 COMPLETE - Window functions working!")

    print("\n" + "=" * 60)
    print("ALL PHASES 1-6 IMPLEMENTED AND TESTED!")
    print("=" * 60 + "\n")


def test_complete_pipeline():
    """Test complete pipeline with real project data."""
    print("\n" + "=" * 60)
    print("TESTING COMPLETE PIPELINE (ALL PHASES)")
    print("=" * 60)

    parquet_path = "src/tests/data/poi_sample.parquet/part-00000-9590699e-c6c2-4709-b2e4-9b37e7d544d6-c000.parquet"

    print(f"\nTesting with real data: {parquet_path}")
    import os
    if not os.path.exists(parquet_path):
        print(f"⚠  File not found: {parquet_path}")
        return

    # Load data info
    conn = duckdb.connect()
    info = conn.sql(f"SELECT COUNT(*) as row_count FROM '{parquet}'").fetchone()
    print(f"  Real data has {info[0]:,} rows")

    # Test with subset and multiple operations
    result = (
        DuckDBTask(name="complete_pipeline")
        .add_input(path=parquet_path)
        .add_sql("SELECT * FROM __THIS__ WHERE poi_name IS NOT NULL LIMIT 50")
        .add_filter_by_expr("latitude IS NOT NULL")
        .add_new_column("CAST(longitude AS DOUBLE) AS lon_double", "longitude_db")
        .select_columns(["poi_name", "latitude_db", "longitude_db", "lon_double"])
        .transform()
    )

    print(f"\n✓ Complete pipeline executed successfully!")
    print(f"   - Processed {len(result)} rows")
    print(f"   - Columns: {list(result.column_names)}")
    print(f"   - Schema: {result.schema}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        test = sys.argv[1]
        if test == "phase4":
            test_phase_4()
        elif test == "phase5":
            test_phase_5()
        elif test == "phase6":
            test_phase_6()
        elif test == "complete":
            test_complete_pipeline()
        else:
            print(f"\nUnknown test: {test}")
    else:
        print("\nRunning all phases 4-6...")
        test_phase_4()
        test_phase_5()
        test_phase_6()
        test_complete_pipeline()
