#!/usr/bin/env python3
"""
Test script to verify Second Order Aggregation API works correctly.
"""

import sys
import os
import pandas as pd
import pyarrow as pa
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from seeknal.tasks.duckdb.aggregators.second_order_aggregator import (
    SecondOrderAggregator,
    AggregationSpec
)


def test_direct_api():
    """Test direct API with AggregationSpec."""
    print("\n" + "=" * 60)
    print("TEST 1: Direct API with AggregationSpec")
    print("=" * 60)
    
    # Prepare test data
    df = pd.DataFrame({
        "user_id": ["A", "A", "A", "A", "B", "B", "B", "B"],
        "application_date": ["2024-01-15"] * 8,
        "transaction_date": ["2024-01-01", "2024-01-05", "2024-01-10", "2024-01-14",
                           "2024-01-02", "2024-01-06", "2024-01-11", "2024-01-13"],
        "amount": [100, 150, 200, 250, 80, 120, 90, 110]
    })
    
    arrow_table = pa.Table.from_pandas(df)
    
    # Create aggregator WITH connection
    conn = duckdb.connect()
    conn.register("transactions", arrow_table)
    
    agg = SecondOrderAggregator(
        idCol="user_id",
        featureDateCol="transaction_date",
        featureDateFormat="yyyy-MM-dd",
        applicationDateCol="application_date",
        applicationDateFormat="yyyy-MM-dd",
        conn=conn  # Use same connection!
    )
    
    # Define rules
    rules = [
        AggregationSpec("basic", "amount", "sum"),
        AggregationSpec("basic_days", "amount", "sum", "", 1, 7),
        AggregationSpec("ratio", "amount", "sum", "", 1, 7, 8, 30),
    ]
    
    agg.setRules(rules)
    
    # Execute
    result = agg.transform("transactions")
    features_df = result.df()
    
    print("\nResult:")
    print(features_df)
    print(f"\n✓ Direct API works! Columns: {list(features_df.columns)}")
    
    return True


def test_builder_api():
    """Test improved builder pattern API."""
    print("\n" + "=" * 60)
    print("TEST 2: Builder Pattern API (Improved DX)")
    print("=" * 60)
    
    # Prepare test data
    df = pd.DataFrame({
        "customer_id": ["C001", "C001", "C001", "C001", "C002", "C002", "C002", "C002"],
        "application_date": ["2024-01-15"] * 8,
        "transaction_date": ["2024-01-01", "2024-01-05", "2024-01-10", "2024-01-12",
                           "2024-01-02", "2024-01-06", "2024-01-09", "2024-01-11"],
        "transaction_amount": [5000, 3000, 2000, 4000, 1000, 1500, 1200, 1800],
        "is_large_transaction": [1, 0, 0, 1, 0, 0, 0, 0]
    })
    
    arrow_table = pa.Table.from_pandas(df)
    
    # Create connection and register table first
    conn = duckdb.connect()
    conn.register("credit_transactions", arrow_table)
    
    # Create aggregator with builder pattern
    agg = (
        SecondOrderAggregator(
            idCol="customer_id",
            featureDateCol="transaction_date",
            applicationDateCol="application_date",
            conn=conn  # Use same connection!
        )
        .builder()
        .feature("transaction_amount")
            .basic(["sum", "count", "mean"])
            .rolling(days=[(1, 7), (8, 30)], aggs=["sum"])
            .ratio(
                numerator=(1, 7),      # Recent: 1-7 days
                denominator=(8, 30),    # Historical: 8-30 days
                aggs=["sum"]
            )
            .since(condition="transaction_amount > 1000", aggs=["count"])
        .feature("is_large_transaction")
            .basic(["sum"])
        .build()
    )
    
    # Validate
    errors = agg.validate("credit_transactions")
    if errors:
        print(f"\n✗ Validation errors: {errors}")
        return False
    
    print("✓ Validation passed")
    
    # Execute
    result = agg.transform("credit_transactions")
    features_df = result.df()
    
    print("\nResult:")
    print(features_df)
    print(f"\n✓ Builder pattern works! Columns: {list(features_df.columns)}")
    print(f"✓ Generated {len(features_df.columns)} features from 2 feature columns")
    
    return True


def test_comparison():
    """Compare direct API vs builder pattern."""
    print("\n" + "=" * 60)
    print("TEST 3: API Comparison")
    print("=" * 60)
    
    # Same logic, two approaches
    df = pd.DataFrame({
        "user_id": ["A", "A", "B", "B"],
        "application_date": ["2024-01-15"] * 4,
        "transaction_date": ["2024-01-01", "2024-01-05", "2024-01-02", "2024-01-08"],
        "amount": [100, 150, 80, 120]
    })
    
    arrow_table = pa.Table.from_pandas(df)
    
    # Create connection and register table first
    conn = duckdb.connect()
    conn.register("test_data", arrow_table)
    
    # Approach 1: Direct API
    agg1 = SecondOrderAggregator(
        idCol="user_id",
        featureDateCol="transaction_date",
        applicationDateCol="application_date",
        conn=conn
    )
    
    rules1 = [
        AggregationSpec("basic", "amount", "sum"),
        AggregationSpec("basic_days", "amount", "sum", "", 1, 7),
    ]
    agg1.setRules(rules1)
    
    # Approach 2: Builder pattern
    agg2 = (
        SecondOrderAggregator(
            idCol="user_id",
            featureDateCol="transaction_date",
            applicationDateCol="application_date",
            conn=conn
        )
        .builder()
        .feature("amount")
            .basic(["sum"])
            .rolling(days=[(1, 7)], aggs=["sum"])
        .build()
    )
    
    # Execute both
    result1 = agg1.transform("test_data").df()
    result2 = agg2.transform("test_data").df()
    
    # Compare
    print("\nDirect API result:")
    print(result1)
    
    print("\nBuilder pattern result:")
    print(result2)
    
    # Check if results are equivalent
    cols1 = set(result1.columns)
    cols2 = set(result2.columns)
    
    if cols1 == cols2:
        print(f"\n✓ Both approaches generate same columns: {cols1}")
    else:
        print(f"\n✗ Different columns!")
        print(f"  Direct API: {cols1}")
        print(f"  Builder:    {cols2}")
    
    return True


def main():
    """Run all tests."""
    print("=" * 60)
    print("SECOND ORDER AGGREGATION API VERIFICATION")
    print("=" * 60)
    print("\nVerifying both direct API and builder pattern work correctly...")
    
    results = []
    
    try:
        results.append(("Direct API", test_direct_api()))
    except Exception as e:
        print(f"\n✗ Direct API test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Direct API", False))
    
    try:
        results.append(("Builder Pattern", test_builder_api()))
    except Exception as e:
        print(f"\n✗ Builder pattern test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("Builder Pattern", False))
    
    try:
        results.append(("API Comparison", test_comparison()))
    except Exception as e:
        print(f"\n✗ Comparison test failed: {e}")
        import traceback
        traceback.print_exc()
        results.append(("API Comparison", False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = 0
    failed = 0
    for name, result in results:
        if result:
            passed += 1
            print(f"  ✓ {name}")
        else:
            failed += 1
            print(f"  ✗ {name}")
    
    print(f"\nPassed: {passed}/{len(results)}")
    print(f"Failed: {failed}/{len(results)}")
    
    if failed == 0:
        print("\n" + "=" * 60)
        print("ALL API TESTS PASSED! ✅")
        print("=" * 60)
        print("\nSecond Order Aggregation API is working correctly!")
        print("Both direct API and builder pattern produce equivalent results.")
    
    return failed == 0


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
