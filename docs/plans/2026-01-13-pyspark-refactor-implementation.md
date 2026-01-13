# PySpark Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace Scala-based Spark Engine with pure PySpark implementation while maintaining 100% feature parity.

**Architecture:** Direct replacement - same public API, internal implementation changed from JavaWrapper→Scala to pure PySpark DataFrame API.

**Tech Stack:** PySpark 3.0+, spark-avro, pytest

---

## Phase 1: Pre-Refactoring - Test Infrastructure

### Task 1: Create Test Directory Structure

**Files:**
- Create: `tests/sparkengine/__init__.py`
- Create: `tests/sparkengine/fixtures/__init__.py`
- Create: `tests/sparkengine/fixtures/baseline/.gitkeep`

**Step 1: Create directory structure**

Run: `mkdir -p tests/sparkengine/fixtures/baseline`

**Step 2: Create __init__.py files**

Run: `touch tests/sparkengine/__init__.py tests/sparkengine/fixtures/__init__.py`

**Step 3: Create .gitkeep for baseline directory**

Run: `touch tests/sparkengine/fixtures/baseline/.gitkeep`

**Step 4: Commit**

```bash
git add tests/sparkengine/
git commit -m "test(sparkengine): Add test directory structure for PySpark refactor"
```

---

### Task 2: Create Test Data Utilities

**Files:**
- Create: `tests/sparkengine/data_utils.py`

**Step 1: Write the data utilities module**

```python
"""Test data utilities for Spark engine tests."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType, FloatType
)
import pytest


@pytest.fixture(scope="module")
def spark_session():
    """Create a local Spark session for testing."""
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    yield spark
    spark.stop()


def create_sample_dataframe(spark: SparkSession, rows: int = 100) -> DataFrame:
    """Create a sample DataFrame with various data types.

    Args:
        spark: SparkSession
        rows: Number of rows to generate

    Returns:
        DataFrame with columns: id, name, value, timestamp, score
    """
    data = [
        (i, f"user_{i}", i * 10.5, f"2024-01-{i % 28 + 1:02d}", 0.1 + (i % 100) / 100)
        for i in range(rows)
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("timestamp", StringType(), False),
        StructField("score", FloatType(), False),
    ])

    return spark.createDataFrame(data, schema)


def create_null_dataframe(spark: SparkSession) -> DataFrame:
    """Create a DataFrame with null values for edge case testing.

    Args:
        spark: SparkSession

    Returns:
        DataFrame with various null combinations
    """
    data = [
        (1, "a", 10.0, None),
        (2, None, 20.0, 0.5),
        (3, "c", None, 0.7),
        (4, "d", 40.0, 0.9),
        (5, None, None, None),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("score", FloatType(), True),
    ])

    return spark.createDataFrame(data, schema)


def create_join_left_dataframe(spark: SparkSession) -> DataFrame:
    """Create left DataFrame for join testing."""
    data = [(1, "alice"), (2, "bob"), (3, "charlie"), (4, "david")]
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
    ])
    return spark.createDataFrame(data, schema)


def create_join_right_dataframe(spark: SparkSession) -> DataFrame:
    """Create right DataFrame for join testing."""
    data = [(1, 100), (2, 200), (3, 300), (5, 500)]
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("score", IntegerType(), False),
    ])
    return spark.createDataFrame(data, schema)
```

**Step 2: Create the file**

**Step 3: Commit**

```bash
git add tests/sparkengine/data_utils.py
git commit -m "test(sparkengine): Add test data utilities for Spark tests"
```

---

### Task 3: Create DataFrame Comparison Utilities

**Files:**
- Create: `tests/sparkengine/assertions.py`

**Step 1: Write the comparison utilities**

```python
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
```

**Step 2: Create the file**

**Step 3: Commit**

```bash
git add tests/sparkengine/assertions.py
git commit -m "test(sparkengine): Add DataFrame comparison utilities"
```

---

### Task 4: Document Current Scala Transformers

**Files:**
- Create: `docs/spark-transformers-reference.md`

**Step 1: Analyze existing Scala transformers**

Run: `find engines/spark-engine/src -name "*Transformer*.scala" | head -30`

**Step 2: Create documentation file**

```markdown
# Spark Transformers Reference

This document catalogs all Scala transformers for porting to PySpark.

## Transformer List

1. **FilterByExpr** - Filter rows by expression
   - Scala: `transformers/FilterByExpr.scala`
   - PySpark: `df.filter(expr)`

2. **ColumnRenamed** - Rename a column
   - Scala: `transformers/ColumnRenamed.scala`
   - PySpark: `df.withColumnRenamed(old, new)`

3. **AddColumnByExpr** - Add column from expression
   - Scala: `transformers/AddColumnByExpr.scala`
   - PySpark: `df.withColumn(name, expr(expr_str))`

4. **SQL** - Execute SQL query
   - Scala: `transformers/SQL.scala`
   - PySpark: `df.createOrReplaceTempView(); spark.sql()`

5. **AddWindowFunction** - Add window function column
   - Scala: `transformers/AddWindowFunction.scala`
   - PySpark: `Window.spec + F.over()`

6. **GroupByColumns** - Group by columns
   - Scala: `transformers/GroupByColumns.scala`
   - PySpark: `df.groupBy(cols).agg()`

7. **JoinByExpr** - Join by expression
   - Scala: `transformers/JoinByExpr.scala`
   - PySpark: `df.join(on=expr)`

8. **JoinById** - Join by ID columns
   - Scala: `transformers/JoinById.scala`
   - PySpark: `df.join(on=[col1, col2])`

9. **AddDateDifference** - Add date difference column
   - Scala: `transformers/AddDateDifference.scala`
   - PySpark: `F.datediff(end, start)`

10. **AddDayType** - Add day type column
    - Scala: `transformers/AddDayType.scala`
    - PySpark: Custom logic + F.when()

11. **AddWeek** - Add week column
    - Scala: `transformers/AddWeek.scala`
    - PySpark: `F.weekofyear()`

12. **AddEntropy** - Add entropy calculation (requires UDF)
    - Scala: `transformers/AddEntropy.scala`
    - PySpark: Python UDF

13. **AddLatLongDistance** - Add haversine distance (requires UDF)
    - Scala: `transformers/AddLatLongDistance.scala`
    - PySpark: Python UDF

... (continue for all 26 transformers)

Generated from: engines/spark-engine/src/main/scala/tech/mta/seeknal/transformers/
```

**Step 3: Commit**

```bash
git add docs/spark-transformers-reference.md
git commit -m "docs: Add Scala transformers reference for PySpark port"
```

---

## Phase 2: PySpark Implementation - Base Classes

### Task 5: Create PySpark Base Classes

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/__init__.py`
- Create: `src/seeknal/tasks/sparkengine/pyspark/base.py`

**Step 1: Write base classes**

```python
"""Base classes for PySpark transformers and aggregators."""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from typing import Any, Dict, Optional
import yaml


class BaseTransformerPySpark(ABC):
    """Base class for all PySpark transformers.

    All transformers must implement the transform() method.
    """

    def __init__(self, **kwargs):
        """Initialize transformer with configuration.

        Args:
            **kwargs: Transformer-specific configuration
        """
        self.config = kwargs

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform the input DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Transformed DataFrame
        """
        pass

    def get_config(self) -> Dict[str, Any]:
        """Get transformer configuration.

        Returns:
            Configuration dictionary
        """
        return self.config

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "BaseTransformerPySpark":
        """Create transformer from YAML configuration.

        Args:
            yaml_str: YAML configuration string

        Returns:
            Transformer instance
        """
        config = yaml.safe_load(yaml_str)
        return cls(**config)


class BaseAggregatorPySpark(ABC):
    """Base class for all PySpark aggregators."""

    def __init__(self, **kwargs):
        """Initialize aggregator with configuration.

        Args:
            **kwargs: Aggregator-specific configuration
        """
        self.config = kwargs

    @abstractmethod
    def aggregate(self, df: DataFrame) -> DataFrame:
        """Aggregate the input DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Aggregated DataFrame
        """
        pass

    def get_config(self) -> Dict[str, Any]:
        """Get aggregator configuration.

        Returns:
            Configuration dictionary
        """
        return self.config


class BaseExtractorPySpark(ABC):
    """Base class for data source extractors."""

    def __init__(self, spark: SparkSession, **kwargs):
        """Initialize extractor.

        Args:
            spark: SparkSession
            **kwargs: Extractor-specific configuration
        """
        self.spark = spark
        self.config = kwargs

    @abstractmethod
    def extract(self) -> DataFrame:
        """Extract data from source.

        Returns:
            DataFrame
        """
        pass


class BaseLoaderPySpark(ABC):
    """Base class for data loaders."""

    def __init__(self, spark: SparkSession, **kwargs):
        """Initialize loader.

        Args:
            spark: SparkSession
            **kwargs: Loader-specific configuration
        """
        self.spark = spark
        self.config = kwargs

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """Load DataFrame to destination.

        Args:
            df: DataFrame to load
        """
        pass
```

**Step 2: Create files**

**Step 3: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/
git commit -m "feat(sparkengine): Add PySpark base classes for transformers"
```

---

### Task 6: Implement ColumnRenamed Transformer

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/transformers/__init__.py`
- Create: `src/seeknal/tasks/sparkengine/pyspark/transformers/column_operations.py`
- Create: `tests/sparkengine/test_column_operations_transformer.py`

**Step 1: Write failing test**

```python
"""Tests for column operation transformers."""

import pytest
from pyspark.sql import SparkSession
from seeknal.tasks.sparkengine.pyspark.transformers.column_operations import ColumnRenamed
from tests.sparkengine.assertions import assert_dataframes_equal
from tests.sparkengine.data_utils import create_sample_dataframe


def test_column_renamed(spark_session: SparkSession):
    """Test ColumnRenamed transformer."""
    df = create_sample_dataframe(spark_session, 10)

    transformer = ColumnRenamed(old_name="name", new_name="user_name")
    result = transformer.transform(df)

    # Verify column renamed
    assert "user_name" in result.columns
    assert "name" not in result.columns

    # Verify data unchanged
    assert result.count() == 10
    assert result.filter("user_name = 'user_5'").count() == 1
```

**Step 2: Run test to verify it fails**

Run: `cd /Users/fitrakacamarga/project/mta/signal/.worktrees/pyspark-refactor && pytest tests/sparkengine/test_column_operations_transformer.py::test_column_renamed -v`

Expected: FAIL with "ModuleNotFoundError: No module named 'seeknal.tasks.sparkengine.pyspark.transformers'"

**Step 3: Write minimal implementation**

```python
"""Column operation transformers."""

from pyspark.sql import DataFrame
from .base import BaseTransformerPySpark


class ColumnRenamed(BaseTransformerPySpark):
    """Rename a column.

    Args:
        old_name: Existing column name
        new_name: New column name
    """

    def __init__(self, old_name: str, new_name: str, **kwargs):
        super().__init__(**kwargs)
        self.old_name = old_name
        self.new_name = new_name

    def transform(self, df: DataFrame) -> DataFrame:
        """Rename the specified column.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with renamed column
        """
        return df.withColumnRenamed(self.old_name, self.new_name)
```

**Step 4: Run test to verify it passes**

Run: `pytest tests/sparkengine/test_column_operations_transformer.py::test_column_renamed -v`

Expected: PASS

**Step 5: Add __init__.py exports**

```python
"""PySpark transformers."""

from .column_operations import ColumnRenamed

__all__ = ["ColumnRenamed"]
```

**Step 6: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/transformers/
git add tests/sparkengine/test_column_operations_transformer.py
git commit -m "feat(sparkengine): Add ColumnRenamed transformer with test"
```

---

### Task 7: Implement FilterByExpr Transformer

**Files:**
- Modify: `src/seeknal/tasks/sparkengine/pyspark/transformers/column_operations.py`
- Modify: `tests/sparkengine/test_column_operations_transformer.py`

**Step 1: Write failing test**

Add to test file:

```python
def test_filter_by_expr(spark_session: SparkSession):
    """Test FilterByExpr transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.column_operations import FilterByExpr

    df = create_sample_dataframe(spark_session, 10)

    transformer = FilterByExpr(expression="id > 5")
    result = transformer.transform(df)

    # Should have 5 rows (id 6-10)
    assert result.count() == 5
    assert result.filter("id <= 5").count() == 0
```

**Step 2: Run test to verify it fails**

**Step 3: Write implementation**

```python
class FilterByExpr(BaseTransformerPySpark):
    """Filter rows by expression.

    Args:
        expression: SQL expression for filtering
    """

    def __init__(self, expression: str, **kwargs):
        super().__init__(**kwargs)
        self.expression = expression

    def transform(self, df: DataFrame) -> DataFrame:
        """Filter DataFrame by expression.

        Args:
            df: Input DataFrame

        Returns:
            Filtered DataFrame
        """
        return df.filter(self.expression)
```

**Step 4: Run test to verify it passes**

**Step 5: Update __init__.py**

```python
from .column_operations import ColumnRenamed, FilterByExpr

__all__ = ["ColumnRenamed", "FilterByExpr"]
```

**Step 6: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/transformers/
git add tests/sparkengine/test_column_operations_transformer.py
git commit -m "feat(sparkengine): Add FilterByExpr transformer with test"
```

---

### Task 8: Implement AddColumnByExpr Transformer

**Files:**
- Modify: `src/seeknal/tasks/sparkengine/pyspark/transformers/column_operations.py`
- Modify: `tests/sparkengine/test_column_operations_transformer.py`

**Step 1: Write failing test**

```python
def test_add_column_by_expr(spark_session: SparkSession):
    """Test AddColumnByExpr transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.column_operations import AddColumnByExpr

    df = create_sample_dataframe(spark_session, 10)

    transformer = AddColumnByExpr(
        column_name="doubled_value",
        expression="value * 2"
    )
    result = transformer.transform(df)

    # Verify new column exists
    assert "doubled_value" in result.columns

    # Verify values
    row = result.filter("id = 1").first()
    assert row["doubled_value"] == row["value"] * 2
```

**Step 2: Run test to verify it fails**

**Step 3: Write implementation**

```python
from pyspark.sql import functions as F


class AddColumnByExpr(BaseTransformerPySpark):
    """Add column from expression.

    Args:
        column_name: Name of new column
        expression: SQL expression for column value
    """

    def __init__(self, column_name: str, expression: str, **kwargs):
        super().__init__(**kwargs)
        self.column_name = column_name
        self.expression = expression

    def transform(self, df: DataFrame) -> DataFrame:
        """Add column to DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with new column
        """
        return df.withColumn(self.column_name, F.expr(self.expression))
```

**Step 4: Run test to verify it passes**

**Step 5: Update exports and commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/transformers/
git add tests/sparkengine/test_column_operations_transformer.py
git commit -m "feat(sparkengine): Add AddColumnByExpr transformer with test"
```

---

### Task 9: Implement Join Transformers

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/transformers/joins.py`
- Create: `tests/sparkengine/test_join_transformers.py`

**Step 1: Write failing tests**

```python
"""Tests for join transformers."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.assertions import assert_dataframes_equal
from tests.sparkengine.data_utils import (
    create_join_left_dataframe,
    create_join_right_dataframe
)


def test_join_by_id(spark_session: SparkSession):
    """Test JoinById transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.joins import JoinById

    left = create_join_left_dataframe(spark_session)
    right = create_join_right_dataframe(spark_session)

    transformer = JoinById(
        right_df=right,
        left_columns=["id"],
        right_columns=["user_id"],
        join_type="inner"
    )
    result = transformer.transform(left)

    # Should have 3 matching rows
    assert result.count() == 3
    assert "id" in result.columns
    assert "name" in result.columns
    assert "score" in result.columns


def test_join_by_expr(spark_session: SparkSession):
    """Test JoinByExpr transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.joins import JoinByExpr

    left = create_join_left_dataframe(spark_session)
    right = create_join_right_dataframe(spark_session)

    transformer = JoinByExpr(
        right_df=right,
        expression="id = user_id",
        join_type="inner"
    )
    result = transformer.transform(left)

    # Should have 3 matching rows
    assert result.count() == 3
```

**Step 2: Run tests to verify they fail**

**Step 3: Write implementation**

```python
"""Join transformers."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .base import BaseTransformerPySpark


class JoinById(BaseTransformerPySpark):
    """Join two DataFrames by column names.

    Args:
        right_df: Right DataFrame to join
        left_columns: Left join columns
        right_columns: Right join columns
        join_type: Type of join (inner, left, right, full)
    """

    def __init__(
        self,
        right_df: DataFrame,
        left_columns: list,
        right_columns: list,
        join_type: str = "inner",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.right_df = right_df
        self.left_columns = left_columns
        self.right_columns = right_columns
        self.join_type = join_type

    def transform(self, df: DataFrame) -> DataFrame:
        """Join DataFrames by column IDs.

        Args:
            df: Left DataFrame

        Returns:
            Joined DataFrame
        """
        join_expr = [
            col(left) == col(right)
            for left, right in zip(self.left_columns, self.right_columns)
        ]

        return df.join(
            self.right_df,
            reduce(and_, join_expr) if len(join_expr) > 1 else join_expr[0],
            self.join_type
        )


class JoinByExpr(BaseTransformerPySpark):
    """Join two DataFrames by expression.

    Args:
        right_df: Right DataFrame to join
        expression: Join expression
        join_type: Type of join (inner, left, right, full)
    """

    def __init__(
        self,
        right_df: DataFrame,
        expression: str,
        join_type: str = "inner",
        **kwargs
    ):
        super().__init__(**kwargs)
        self.right_df = right_df
        self.expression = expression
        self.join_type = join_type

    def transform(self, df: DataFrame) -> DataFrame:
        """Join DataFrames by expression.

        Args:
            df: Left DataFrame

        Returns:
            Joined DataFrame
        """
        return df.join(self.right_df, F.expr(self.expression), self.join_type)
```

**Step 4: Run tests to verify they pass**

**Step 5: Update exports and commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/transformers/
git add tests/sparkengine/test_join_transformers.py
git commit -m "feat(sparkengine): Add join transformers with tests"
```

---

### Task 10: Implement SQL Transformer

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/transformers/sql.py`
- Create: `tests/sparkengine/test_sql_transformer.py`

**Step 1: Write failing test**

```python
"""Tests for SQL transformer."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.data_utils import create_sample_dataframe


def test_sql_transformer(spark_session: SparkSession):
    """Test SQL transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.sql import SQL

    df = create_sample_dataframe(spark_session, 10)

    transformer = SQL(
        spark=spark_session,
        query="SELECT id, name, value * 2 AS doubled FROM input WHERE id > 5",
        view_name="input"
    )
    result = transformer.transform(df)

    # Should have 5 rows
    assert result.count() == 5
    assert "doubled" in result.columns
```

**Step 2: Run test to verify it fails**

**Step 3: Write implementation**

```python
"""SQL transformer."""

from pyspark.sql import DataFrame, SparkSession
from .base import BaseTransformerPySpark


class SQL(BaseTransformerPySpark):
    """Execute SQL query on DataFrame.

    Args:
        spark: SparkSession
        query: SQL query to execute
        view_name: Temporary view name for input DataFrame
    """

    def __init__(self, spark: SparkSession, query: str, view_name: str = "input", **kwargs):
        super().__init__(**kwargs)
        self.spark = spark
        self.query = query
        self.view_name = view_name

    def transform(self, df: DataFrame) -> DataFrame:
        """Execute SQL query.

        Args:
            df: Input DataFrame

        Returns:
            Result DataFrame
        """
        df.createOrReplaceTempView(self.view_name)
        return self.spark.sql(self.query)
```

**Step 4: Run test to verify it passes**

**Step 5: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/transformers/sql.py
git add tests/sparkengine/test_sql_transformer.py
git commit -m "feat(sparkengine): Add SQL transformer with test"
```

---

### Task 11: Implement Special Transformers (Entropy, Distance)

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/transformers/special.py`
- Create: `tests/sparkengine/test_special_transformers.py`

**Step 1: Write failing tests**

```python
"""Tests for special transformers (UDFs)."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.data_utils import create_sample_dataframe


def test_add_entropy(spark_session: SparkSession):
    """Test AddEntropy transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.special import AddEntropy

    # Create data with values
    data = [(1, [0.1, 0.2, 0.7]), (2, [0.5, 0.5, 0.0])]
    df = spark_session.createDataFrame(data, ["id", "probs"])

    transformer = AddEntropy(
        input_col="probs",
        output_col="entropy"
    )
    result = transformer.transform(df)

    assert "entropy" in result.columns
    # Entropy should be positive
    assert result.filter("entropy <= 0").count() == 0


def test_add_latlong_distance(spark_session: SparkSession):
    """Test AddLatLongDistance transformer."""
    from seeknal.tasks.sparkengine.pyspark.transformers.special import AddLatLongDistance

    data = [
        (1, 40.7128, -74.0060, 34.0522, -118.2437),  # NYC to LA
        (2, 51.5074, -0.1278, 48.8566, 2.3522),      # London to Paris
    ]
    df = spark_session.createDataFrame(data, ["id", "lat1", "lon1", "lat2", "lon2"])

    transformer = AddLatLongDistance(
        lat1_col="lat1",
        lon1_col="lon1",
        lat2_col="lat2",
        lon2_col="lon2",
        output_col="distance"
    )
    result = transformer.transform(df)

    assert "distance" in result.columns
    # Distance should be positive
    assert result.filter("distance <= 0").count() == 0
    # NYC to LA is ~3944 km
    row = result.filter("id = 1").first()
    assert 3900 < row["distance"] < 4000
```

**Step 2: Run tests to verify they fail**

**Step 3: Write implementation**

```python
"""Special transformers requiring UDFs."""

import math
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import FloatType, ArrayType
from .base import BaseTransformerPySpark


class AddEntropy(BaseTransformerPySpark):
    """Add entropy calculation column.

    Args:
        input_col: Input column with array of probabilities
        output_col: Output column name
    """

    def __init__(self, input_col: str, output_col: str, **kwargs):
        super().__init__(**kwargs)
        self.input_col = input_col
        self.output_col = output_col

    @F.udf(FloatType())
    def _entropy(probs):
        """Calculate Shannon entropy."""
        if not probs:
            return 0.0
        return -sum(p * math.log2(p) for p in probs if p > 0)

    def transform(self, df: DataFrame) -> DataFrame:
        """Add entropy column.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with entropy column
        """
        return df.withColumn(self.output_col, self._entropy(F.col(self.input_col)))


class AddLatLongDistance(BaseTransformerPySpark):
    """Add haversine distance column between two lat/long points.

    Args:
        lat1_col: First latitude column
        lon1_col: First longitude column
        lat2_col: Second latitude column
        lon2_col: Second longitude column
        output_col: Output column name
    """

    def __init__(
        self,
        lat1_col: str,
        lon1_col: str,
        lat2_col: str,
        lon2_col: str,
        output_col: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.lat1_col = lat1_col
        self.lon1_col = lon1_col
        self.lat2_col = lat2_col
        self.lon2_col = lon2_col
        self.output_col = output_col

    @F.udf(FloatType())
    def _haversine_distance(lat1, lon1, lat2, lon2):
        """Calculate haversine distance in kilometers."""
        R = 6371  # Earth radius in km

        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (math.sin(dlat / 2) ** 2 +
             math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
        c = 2 * math.asin(math.sqrt(a))

        return R * c

    def transform(self, df: DataFrame) -> DataFrame:
        """Add distance column.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with distance column
        """
        return df.withColumn(
            self.output_col,
            self._haversine_distance(
                F.col(self.lat1_col),
                F.col(self.lon1_col),
                F.col(self.lat2_col),
                F.col(self.lon2_col)
            )
        )
```

**Step 4: Run tests to verify they pass**

**Step 5: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/transformers/special.py
git add tests/sparkengine/test_special_transformers.py
git commit -m "feat(sparkengine): Add special transformers (entropy, distance) with UDFs"
```

---

## Phase 3: Aggregators

### Task 12: Implement FunctionAggregator

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/aggregators/__init__.py`
- Create: `src/seeknal/tasks/sparkengine/pyspark/aggregators/function_aggregator.py`
- Create: `tests/sparkengine/test_aggregators.py`

**Step 1: Write failing test**

```python
"""Tests for aggregators."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.data_utils import create_sample_dataframe


def test_function_aggregator_sum(spark_session: SparkSession):
    """Test FunctionAggregator with sum."""
    from seeknal.tasks.sparkengine.pyspark.aggregators.function_aggregator import (
        FunctionAggregator, AggregationFunction
    )

    df = create_sample_dataframe(spark_session, 10)

    aggregator = FunctionAggregator(
        group_by_columns=["id"],
        aggregations=[
            AggregationFunction(column="value", function="sum", alias="total_value")
        ]
    )
    result = aggregator.aggregate(df)

    assert result.count() == 10  # One row per ID
    assert "total_value" in result.columns


def test_function_aggregator_count(spark_session: SparkSession):
    """Test FunctionAggregator with count."""
    from seeknal.tasks.sparkengine.pyspark.aggregators.function_aggregator import (
        FunctionAggregator, AggregationFunction
    )

    df = create_sample_dataframe(spark_session, 10)

    aggregator = FunctionAggregator(
        group_by_columns=[],
        aggregations=[
            AggregationFunction(column="*", function="count", alias="row_count")
        ]
    )
    result = aggregator.aggregate(df)

    assert result.count() == 1  # Single row
    assert result.first()["row_count"] == 10
```

**Step 2: Run tests to verify they fail**

**Step 3: Write implementation**

```python
"""Function-based aggregators."""

from dataclasses import dataclass
from pyspark.sql import DataFrame, functions as F
from .base import BaseAggregatorPySpark


@dataclass
class AggregationFunction:
    """Aggregation function specification.

    Args:
        column: Column to aggregate (use "*" for count)
        function: Function name (sum, count, avg, min, max)
        alias: Output column name
    """
    column: str
    function: str
    alias: str


class FunctionAggregator(BaseAggregatorPySpark):
    """Aggregate using built-in functions.

    Args:
        group_by_columns: Columns to group by
        aggregations: List of AggregationFunction specs
    """

    def __init__(self, group_by_columns: list, aggregations: list, **kwargs):
        super().__init__(**kwargs)
        self.group_by_columns = group_by_columns
        self.aggregations = aggregations

    def aggregate(self, df: DataFrame) -> DataFrame:
        """Perform aggregation.

        Args:
            df: Input DataFrame

        Returns:
            Aggregated DataFrame
        """
        # Map function names to PySpark functions
        func_map = {
            "sum": F.sum,
            "count": F.count,
            "avg": F.avg,
            "min": F.min,
            "max": F.max,
        }

        # Build aggregations
        agg_exprs = []
        for agg in self.aggregations:
            col = F.lit(1) if agg.column == "*" else F.col(agg.column)
            func = func_map[agg.function]
            agg_exprs.append(func(col).alias(agg.alias))

        # Group and aggregate
        if self.group_by_columns:
            return df.groupBy(*self.group_by_columns).agg(*agg_exprs)
        else:
            return df.agg(*agg_exprs)
```

**Step 4: Run tests to verify they pass**

**Step 5: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/aggregators/
git add tests/sparkengine/test_aggregators.py
git commit -m "feat(sparkengine): Add FunctionAggregator with tests"
```

---

## Phase 4: Connectors (Extractors & Loaders)

### Task 13: Implement Extractors

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/extractors/__init__.py`
- Create: `src/seeknal/tasks/sparkengine/pyspark/extractors/file_source.py`
- Create: `tests/sparkengine/test_extractors.py`

**Step 1: Write failing test**

```python
"""Tests for extractors."""

import pytest
from pyspark.sql import SparkSession
import tempfile
import os


def test_file_source_parquet(spark_session: SparkSession, tmp_path):
    """Test FileSource with Parquet."""
    from seeknal.tasks.sparkengine.pyspark.extractors.file_source import FileSource

    # Create test file
    from tests.sparkengine.data_utils import create_sample_dataframe
    test_df = create_sample_dataframe(spark_session, 10)
    test_path = os.path.join(tmp_path, "test.parquet")
    test_df.write.parquet(test_path)

    extractor = FileSource(spark=spark_session, path=test_path, format="parquet")
    result = extractor.extract()

    assert result.count() == 10
    assert len(result.columns) == 5


def test_generic_source(spark_session: SparkSession, tmp_path):
    """Test GenericSource."""
    from seeknal.tasks.sparkengine.pyspark.extractors.generic_source import GenericSource

    test_df = spark_session.createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    test_path = os.path.join(tmp_path, "test.json")
    test_df.write.json(test_path)

    extractor = GenericSource(
        spark=spark_session,
        path=test_path,
        format="json",
        options={"multiLine": "true"}
    )
    result = extractor.extract()

    assert result.count() == 2
```

**Step 2: Run tests to verify they fail**

**Step 3: Write implementation**

```python
"""File-based extractors."""

from pyspark.sql import DataFrame, SparkSession
from .base import BaseExtractorPySpark


class FileSource(BaseExtractorPySpark):
    """Extract data from files.

    Args:
        spark: SparkSession
        path: File path
        format: File format (parquet, csv, json, orc)
        options: Optional format options
    """

    def __init__(self, spark: SparkSession, path: str, format: str = "parquet", options: dict = None, **kwargs):
        super().__init__(spark, **kwargs)
        self.path = path
        self.format = format
        self.options = options or {}

    def extract(self) -> DataFrame:
        """Extract data from file.

        Returns:
            DataFrame
        """
        reader = getattr(self.spark.read, self.format)
        if self.options:
            return reader(**self.options)(self.path)
        return reader(self.path)


class GenericSource(BaseExtractorPySpark):
    """Generic data source extractor.

    Args:
        spark: SparkSession
        path: Data path
        format: Data format
        options: Optional format options
    """

    def __init__(self, spark: SparkSession, path: str, format: str, options: dict = None, **kwargs):
        super().__init__(spark, **kwargs)
        self.path = path
        self.format = format
        self.options = options or {}

    def extract(self) -> DataFrame:
        """Extract data using generic format reader.

        Returns:
            DataFrame
        """
        reader = self.spark.read.format(self.format)
        if self.options:
            reader = reader.options(**self.options)
        return reader.load(self.path)
```

**Step 4: Run tests to verify they pass**

**Step 5: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/extractors/
git add tests/sparkengine/test_extractors.py
git commit -m "feat(sparkengine): Add file extractors with tests"
```

---

### Task 14: Implement Loaders

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/loaders/__init__.py`
- Create: `src/seeknal/tasks/sparkengine/pyspark/loaders/parquet_writer.py`
- Create: `tests/sparkengine/test_loaders.py`

**Step 1: Write failing test**

```python
"""Tests for loaders."""

import pytest
from pyspark.sql import SparkSession
import tempfile
import os


def test_parquet_writer(spark_session: SparkSession, tmp_path):
    """Test ParquetWriter."""
    from seeknal.tasks.sparkengine.pyspark.loaders.parquet_writer import ParquetWriter
    from tests.sparkengine.data_utils import create_sample_dataframe

    df = create_sample_dataframe(spark_session, 10)
    output_path = os.path.join(tmp_path, "output.parquet")

    loader = ParquetWriter(spark=spark_session, path=output_path)
    loader.load(df)

    # Verify file was written
    assert os.path.exists(output_path)

    # Verify data
    result = spark_session.read.parquet(output_path)
    assert result.count() == 10
```

**Step 2: Run test to verify it fails**

**Step 3: Write implementation**

```python
"""Data loaders."""

from pyspark.sql import DataFrame, SparkSession
from .base import BaseLoaderPySpark


class ParquetWriter(BaseLoaderPySpark):
    """Write DataFrame to Parquet file.

    Args:
        spark: SparkSession
        path: Output path
        mode: Write mode (overwrite, append, ignore, error)
        partition_by: Optional partitioning columns
    """

    def __init__(
        self,
        spark: SparkSession,
        path: str,
        mode: str = "overwrite",
        partition_by: list = None,
        **kwargs
    ):
        super().__init__(spark, **kwargs)
        self.path = path
        self.mode = mode
        self.partition_by = partition_by

    def load(self, df: DataFrame) -> None:
        """Write DataFrame to Parquet.

        Args:
            df: DataFrame to write
        """
        writer = df.write.mode(self.mode)
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        writer.parquet(self.path)
```

**Step 4: Run test to verify it passes**

**Step 5: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/loaders/
git add tests/sparkengine/test_loaders.py
git commit -m "feat(sparkengine): Add ParquetWriter loader with test"
```

---

## Phase 5: SparkEngineTask Rewrite

### Task 15: Create New SparkEngineTask

**Files:**
- Create: `src/seeknal/tasks/sparkengine/pyspark/spark_engine_task.py`
- Create: `tests/sparkengine/test_spark_engine_task.py`

**Step 1: Write failing test**

```python
"""Tests for SparkEngineTask."""

import pytest
from pyspark.sql import SparkSession
from tests.sparkengine.data_utils import create_sample_dataframe


def test_spark_engine_task_simple_pipeline(spark_session: SparkSession, tmp_path):
    """Test SparkEngineTask with simple pipeline."""
    from seeknal.tasks.sparkengine.pyspark.spark_engine_task import SparkEngineTask
    from seeknal.tasks.sparkengine.pyspark.transformers.column_operations import (
        FilterByExpr, AddColumnByExpr
    )

    # Create test data
    df = create_sample_dataframe(spark_session, 100)
    test_path = f"{tmp_path}/input.parquet"
    df.write.parquet(test_path)

    # Build pipeline
    task = SparkEngineTask(spark=spark_session)
    task.add_input(path=test_path, format="parquet")
    task.add_stage(
        stage_id="filter",
        transformer_class="FilterByExpr",
        params={"expression": "id > 50"}
    )
    task.add_stage(
        stage_id="double",
        transformer_class="AddColumnByExpr",
        params={"column_name": "doubled", "expression": "value * 2"}
    )
    result = task.transform()

    # Should have 50 rows (id 51-100)
    assert result.count() == 50
    assert "doubled" in result.columns
```

**Step 2: Run test to verify it fails**

**Step 3: Write implementation**

```python
"""PySpark-based SparkEngineTask."""

from typing import Dict, Any, Optional, List
from pyspark.sql import DataFrame, SparkSession
from .extractors.file_source import FileSource
from .loaders.parquet_writer import ParquetWriter


class Stage:
    """Pipeline stage.

    Args:
        stage_id: Unique stage identifier
        transformer_class: Transformer class name
        params: Transformer parameters
    """

    def __init__(self, stage_id: str, transformer_class: str, params: Dict[str, Any]):
        self.stage_id = stage_id
        self.transformer_class = transformer_class
        self.params = params

    def get_transformer(self):
        """Get transformer instance.

        Returns:
            Transformer instance
        """
        # Import transformer classes
        from .transformers import (
            FilterByExpr, AddColumnByExpr, ColumnRenamed,
            JoinById, JoinByExpr, SQL
        )
        from .transformers.special import AddEntropy, AddLatLongDistance

        # Map class names to classes
        transformer_map = {
            "FilterByExpr": FilterByExpr,
            "AddColumnByExpr": AddColumnByExpr,
            "ColumnRenamed": ColumnRenamed,
            "JoinById": JoinById,
            "JoinByExpr": JoinByExpr,
            "SQL": SQL,
            "AddEntropy": AddEntropy,
            "AddLatLongDistance": AddLatLongDistance,
        }

        cls = transformer_map.get(self.transformer_class)
        if cls is None:
            raise ValueError(f"Unknown transformer: {self.transformer_class}")

        return cls(**self.params)


class SparkEngineTask:
    """PySpark-based data pipeline task.

    This replaces the Scala-based SparkEngine with pure PySpark.

    Args:
        spark: SparkSession
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.input_config: Optional[Dict[str, Any]] = None
        self.stages: List[Stage] = []
        self.output_config: Optional[Dict[str, Any]] = None

    def add_input(self, path: str, format: str = "parquet", **options) -> "SparkEngineTask":
        """Add input source.

        Args:
            path: Input path
            format: File format
            **options: Additional options

        Returns:
            Self for chaining
        """
        self.input_config = {"path": path, "format": format, "options": options}
        return self

    def add_stage(
        self,
        stage_id: str,
        transformer_class: str,
        params: Dict[str, Any]
    ) -> "SparkEngineTask":
        """Add transformation stage.

        Args:
            stage_id: Unique stage identifier
            transformer_class: Transformer class name
            params: Transformer parameters

        Returns:
            Self for chaining
        """
        stage = Stage(stage_id, transformer_class, params)
        self.stages.append(stage)
        return self

    def add_output(self, path: str, format: str = "parquet", **options) -> "SparkEngineTask":
        """Add output destination.

        Args:
            path: Output path
            format: File format
            **options: Additional options

        Returns:
            Self for chaining
        """
        self.output_config = {"path": path, "format": format, "options": options}
        return self

    def transform(self) -> DataFrame:
        """Execute transformation pipeline.

        Returns:
            Transformed DataFrame
        """
        # Load input
        if not self.input_config:
            raise ValueError("No input configured")
        extractor = FileSource(
            spark=self.spark,
            path=self.input_config["path"],
            format=self.input_config.get("format", "parquet"),
            options=self.input_config.get("options", {})
        )
        df = extractor.extract()

        # Apply stages
        for stage in self.stages:
            transformer = stage.get_transformer()
            df = transformer.transform(df)

        return df

    def evaluate(self) -> None:
        """Execute pipeline and write output."""
        result = self.transform()

        if self.output_config:
            if self.output_config.get("format") == "parquet":
                loader = ParquetWriter(
                    spark=self.spark,
                    path=self.output_config["path"],
                    mode=self.output_config.get("mode", "overwrite")
                )
                loader.load(result)
            else:
                raise ValueError(f"Unsupported output format: {self.output_config.get('format')}")
```

**Step 4: Run test to verify it passes**

**Step 5: Commit**

```bash
git add src/seeknal/tasks/sparkengine/pyspark/spark_engine_task.py
git add tests/sparkengine/test_spark_engine_task.py
git commit -m "feat(sparkengine): Add PySpark-based SparkEngineTask with test"
```

---

## Phase 6: Cutover - Replace Old Implementation

### Task 16: Update Main Module Exports

**Files:**
- Modify: `src/seeknal/tasks/sparkengine/__init__.py`

**Step 1: Update exports to use new implementation**

```python
"""Spark engine tasks.

This module now provides pure PySpark implementation.
"""

# Import from new PySpark implementation
from .pyspark.spark_engine_task import SparkEngineTask, Stage
from .pyspark.transformers import (
    FilterByExpr,
    AddColumnByExpr,
    ColumnRenamed,
)
from .pyspark.transformers.joins import JoinById, JoinByExpr
from .pyspark.transformers.sql import SQL
from .pyspark.aggregators.function_aggregator import (
    FunctionAggregator,
    AggregationFunction,
)

__all__ = [
    "SparkEngineTask",
    "Stage",
    "FilterByExpr",
    "AddColumnByExpr",
    "ColumnRenamed",
    "JoinById",
    "JoinByExpr",
    "SQL",
    "FunctionAggregator",
    "AggregationFunction",
]
```

**Step 2: Test imports still work**

Run: `python -c "from seeknal.tasks.sparkengine import SparkEngineTask; print('OK')"`

**Step 3: Commit**

```bash
git add src/seeknal/tasks/sparkengine/__init__.py
git commit -m "refactor(sparkengine): Update exports to use PySpark implementation"
```

---

### Task 17: Remove Scala Engine Directory

**Files:**
- Delete: `engines/spark-engine/`

**Step 1: Verify tests pass with PySpark implementation**

Run: `pytest tests/sparkengine/ -v`

**Step 2: Remove Scala engine directory**

Run: `rm -rf engines/spark-engine`

**Step 3: Update .gitignore if needed**

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor(sparkengine): Remove Scala-based spark-engine directory"
```

---

### Task 18: Update Dependencies

**Files:**
- Modify: `pyproject.toml`

**Step 1: Remove findspark, add spark-avro**

```toml
dependencies = [
    "delta-spark==3.2.0",
    "duckdb>=1.1.3",
    # "findspark>=2.0.1",  # REMOVE: No longer needed
    "spark-avro",  # ADD: For Avro support
    # ... rest of dependencies
]
```

**Step 2: Commit**

```bash
git add pyproject.toml
git commit -m "deps(sparkengine): Remove findspark, add spark-avro"
```

---

### Task 19: Update Documentation

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`

**Step 1: Update CLAUDE.md**

Find and update the Spark Engine section:

```markdown
### Spark Engine (`tasks/sparkengine/`)
- Pure PySpark implementation (v3.0+)
- Transformers, aggregators, loaders, extractors
- No JVM required
```

**Step 2: Update README.md**

Remove Scala/JVM setup instructions.

**Step 3: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "docs: Update documentation for pure PySpark implementation"
```

---

### Task 20: Final Integration Tests

**Files:**
- Create: `tests/e2e/test_pyspark_pipelines.py`

**Step 1: Create end-to-end integration tests**

```python
"""End-to-end integration tests for PySpark pipelines."""

import pytest
from pyspark.sql import SparkSession
import tempfile
import os
from seeknal.tasks.sparkengine import SparkEngineTask


def test_full_pipeline(spark_session: SparkSession, tmp_path):
    """Test complete pipeline with input, transforms, and output."""
    # Create test data
    data = [(i, f"user_{i}", i * 10) for i in range(100)]
    df = spark_session.createDataFrame(data, ["id", "name", "value"])

    input_path = f"{tmp_path}/input.parquet"
    output_path = f"{tmp_path}/output.parquet"

    df.write.parquet(input_path)

    # Build and run pipeline
    task = SparkEngineTask(spark=spark_session)
    task.add_input(path=input_path, format="parquet")
    task.add_stage(
        stage_id="filter",
        transformer_class="FilterByExpr",
        params={"expression": "id > 50"}
    )
    task.add_stage(
        stage_id="add_doubled",
        transformer_class="AddColumnByExpr",
        params={"column_name": "doubled", "expression": "value * 2"}
    )
    task.add_output(path=output_path, format="parquet")
    task.evaluate()

    # Verify output
    result = spark_session.read.parquet(output_path)
    assert result.count() == 50
    assert "doubled" in result.columns
    assert result.filter("id <= 50").count() == 0
```

**Step 2: Run integration tests**

Run: `pytest tests/e2e/test_pyspark_pipelines.py -v`

**Step 3: Commit**

```bash
git add tests/e2e/test_pyspark_pipelines.py
git commit -m "test(sparkengine): Add end-to-end integration tests"
```

---

### Task 21: Version Bump and Changelog

**Files:**
- Modify: `pyproject.toml`
- Create: `CHANGELOG.md`

**Step 1: Bump major version**

```toml
[project]
name = "seeknal"
version = "3.0.0"  # Major version bump for breaking change
```

**Step 2: Create changelog**

```markdown
# Changelog

## [3.0.0] - 2026-01-XX

### Breaking Changes
- **SparkEngineTask**: Scala-based implementation replaced with pure PySpark
  - No JVM or Gradle installation required
  - Same public API - user code unchanged
  - All 26 transformers ported to PySpark

### Removed
- Scala spark-engine (`engines/spark-engine/`)
- `findspark` dependency (no longer needed)
- JVM/Scala installation requirement

### Added
- Pure PySpark transformer implementations
- PySpark aggregator implementations
- PySpark extractor and loader implementations
- Comprehensive test suite for Spark engine

### Migration
- Users: Update to v3.0.0 via pip
- No code changes required - API unchanged
- See documentation for details
```

**Step 3: Commit**

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "chore: Bump version to 3.0.0 and add changelog"
```

---

### Task 22: Final Validation

**Step 1: Run full test suite**

Run: `pytest tests/ -v`

**Step 2: Verify imports work**

Run: `python -c "from seeknal.tasks.sparkengine import SparkEngineTask, FilterByExpr; print('OK')"`

**Step 3: Check no Scala references remain**

Run: `grep -r "JavaWrapper" src/seeknal/tasks/sparkengine/ || echo "No JavaWrapper found - OK"`

**Step 4: Verify engines directory removed**

Run: `ls engines/ 2>&1 || echo "engines directory removed - OK"`

**Step 5: Final commit**

```bash
git add -A
git commit -m "chore: Final validation for PySpark refactor"
```

---

## Summary

This implementation plan covers:

- **Phase 1**: Test infrastructure setup (Tasks 1-4)
- **Phase 2**: PySpark base classes and transformers (Tasks 5-11)
- **Phase 3**: Aggregators (Task 12)
- **Phase 4**: Extractors and loaders (Tasks 13-14)
- **Phase 5**: SparkEngineTask rewrite (Task 15)
- **Phase 6**: Cutover and validation (Tasks 16-22)

**Total**: 22 tasks, each with TDD workflow and atomic commits.

**Estimated effort**: 3-4 weeks with test-first approach.
