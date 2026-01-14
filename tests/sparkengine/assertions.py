"""DataFrame comparison utilities for testing."""

from pyspark.sql import DataFrame
import math


def assert_dataframes_equal(
    df1: DataFrame,
    df2: DataFrame,
    tolerance: float = 1e-6,
    ignore_order: bool = True,
    check_schema: bool = True
) -> None:
    """Assert two DataFrames are equal.

    Args:
        df1: First DataFrame
        df2: Second DataFrame
        tolerance: Floating-point comparison tolerance
        ignore_order: If True, sort data before comparing
        check_schema: If True, verify schemas match

    Raises:
        AssertionError: If DataFrames differ
    """
    # Check row count
    count1 = df1.count()
    count2 = df2.count()
    assert count1 == count2, f"Row count mismatch: {count1} != {count2}"

    if check_schema:
        assert_schemas_equal(df1.schema, df2.schema)

    # Collect and compare data
    data1 = df1.collect()
    data2 = df2.collect()

    if ignore_order:
        data1 = sorted(data1, key=lambda r: str(r))
        data2 = sorted(data2, key=lambda r: str(r))

    for i, (row1, row2) in enumerate(zip(data1, data2)):
        assert len(row1) == len(row2), f"Row {i} length mismatch"
        for j, (val1, val2) in enumerate(zip(row1, row2)):
            if isinstance(val1, float) and isinstance(val2, float):
                if math.isnan(val1) and math.isnan(val2):
                    continue
                if math.isinf(val1) or math.isinf(val2):
                    assert val1 == val2, \
                        f"Row {i}, col {j}: {val1} != {val2}"
                assert abs(val1 - val2) <= tolerance, \
                    f"Row {i}, col {j}: {val1} != {val2} (tolerance: {tolerance})"
            else:
                assert val1 == val2, f"Row {i}, col {j}: {val1} != {val2}"


def assert_schemas_equal(schema1, schema2) -> None:
    """Assert two schemas are equal.

    Args:
        schema1: First schema
        schema2: Second schema

    Raises:
        AssertionError: If schemas differ
    """
    fields1 = schema1.fields
    fields2 = schema2.fields

    assert len(fields1) == len(fields2), \
        f"Schema field count mismatch: {len(fields1)} != {len(fields2)}"

    for i, (f1, f2) in enumerate(zip(fields1, fields2)):
        assert f1.name == f2.name, f"Field {i}: name mismatch {f1.name} != {f2.name}"
        assert f1.dataType == f2.dataType, \
            f"Field {i} ({f1.name}): type mismatch {f1.dataType} != {f2.dataType}"
        assert f1.nullable == f2.nullable, \
            f"Field {i} ({f1.name}): nullable mismatch {f1.nullable} != {f2.nullable}"


def save_baseline(df: DataFrame, path: str) -> None:
    """Save a DataFrame as a baseline for comparison.

    Args:
        df: DataFrame to save
        path: Path to save (without extension)
    """
    # Save as Parquet
    df.write.parquet(f"{path}.parquet")

    # Save schema as JSON
    with open(f"{path}.schema.json", "w") as f:
        f.write(df.schema.json())

    # Save row count
    with open(f"{path}.count.txt", "w") as f:
        f.write(str(df.count()))


def load_baseline(spark, path: str) -> DataFrame:
    """Load a baseline DataFrame.

    Args:
        spark: SparkSession
        path: Path to load (without extension)

    Returns:
        DataFrame
    """
    return spark.read.parquet(f"{path}.parquet")
