# DuckDB Transformers and Aggregators Porting Specification

**Date:** 2026-01-07  
**Status:** Approved  
**Author:** AI Assistant (Brainstorming Mode)  
**Type:** Architecture & Implementation Specification

## Executive Summary

This specification outlines the complete port of all SparkEngine transformers and aggregators to DuckDB, enabling Seeknal users to run the same data pipelines that previously required Spark using DuckDB's lightweight, pure-Python engine.

**Key Objectives:**
- Port ALL transformers from `sparkengine/transformers` to `duckdb/transformers`
- Port ALL aggregators from `sparkengine/aggregators` to `duckdb/aggregators`
- Create `DuckDBTask` class that mirrors `SparkEngineTask` API
- Maintain full API compatibility for easy migration
- Use PyArrow Tables as primary data structure

**Success Criteria:**
- Same pipeline code works with both engines (change imports only)
- DuckDB faster than Spark for datasets <100M rows
- 100% test coverage for new code
- Complete documentation and migration guide

---

## Table of Contents

1. [Current Architecture Analysis](#1-current-architecture-analysis)
2. [Design Overview](#2-design-overview)
3. [Transformer Specifications](#3-transformer-specifications)
4. [Aggregator Specifications](#4-aggregator-specifications)
5. [DuckDBTask Architecture](#5-duckdbtask-architecture)
6. [Flow Integration](#6-flow-integration)
7. [Implementation Plan](#7-implementation-plan)
8. [Testing Strategy](#8-testing-strategy)
9. [API Compatibility](#9-api-compatibility)
10. [Performance Considerations](#10-performance-considerations)
11. [Error Handling](#11-error-handling)
12. [Documentation](#12-documentation)

---

## 1. Current Architecture Analysis

### 1.1 Existing Components

**SparkEngine Transformers** (in `src/seeknal/tasks/sparkengine/transformers/`):
- **Simple:** `ColumnRenamed`, `SQL`, `AddColumnByExpr`, `FilterByExpr`
- **Medium:** `JoinTablesByExpr`, `PointInTime`
- **Complex:** `AddWindowFunction` (with ranking, offset, aggregate functions)

**SparkEngine Aggregators** (in `src/seeknal/tasks/sparkengine/aggregators/`):
- **Base Classes:** `Aggregator`, `AggregatorFunction`, `ColByExpression`, `RenamedCols`
- **Simple:** `FunctionAggregator`, `ExpressionAggregator`, `DayTypeAggregator`
- **Complex:** `SecondOrderAggregator` (with rules: basic, basic_days, ratio, since)
- **Time-based:** `LastNDaysAggregator`

**DuckDB Existing:**
- Basic `DuckDBTask` with simple SQL chaining ✓
- `SecondOrderAggregator` already ported ✓

### 1.2 Flow Architecture

The `Flow` class orchestrates data pipelines with:
- **Inputs:** Hive tables, Parquet files, feature groups, extractors
- **Tasks:** `SparkEngineTask` or `DuckDBTask` instances
- **Outputs:** DataFrames, tables, files, feature groups

**Current Behavior:**
- Detects if tasks need Spark (`task.is_spark_job`)
- Creates SparkSession only when needed
- Converts between PySpark DataFrames and PyArrow Tables automatically

---

## 2. Design Overview

### 2.1 Core Design Principles

**Data Structure:**
- **Spark:** PySpark DataFrames
- **DuckDB:** PyArrow Tables (native DuckDB format, zero-copy)

**Transformer Execution:**
- **Spark:** Java/Scala backend via `JavaWrapper`
- **DuckDB:** Pure Python using DuckDB SQL with string replacement

**SQL Placeholder:**
- Both engines use `__THIS__` as the dataset placeholder
- DuckDB replaces `__THIS__` with registered view names or CTEs

### 2.2 Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| **API Parity** | DuckDBTask mirrors SparkEngineTask API exactly |
| **PyArrow Tables** | Native DuckDB format, zero-copy optimization |
| **SQL Generation** | Each transformer generates SQL fragments |
| **Sequential Execution** | View-based pipeline execution |
| **Mixed Engine Support** | Flow supports both Spark and DuckDB tasks |

---

## 3. Transformer Specifications

### 3.1 Base Transformer Classes

**File:** `src/seeknal/tasks/duckdb/transformers/base_transformer.py`

```python
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel

class DuckDBClassName(str, Enum):
    """Enumeration of DuckDB transformer class names."""
    FILTER_BY_EXPR = "seeknal.tasks.duckdb.transformers.FilterByExpr"
    COLUMN_RENAMED = "seeknal.tasks.duckdb.transformers.ColumnRenamed"
    ADD_COLUMN_BY_EXPR = "seeknal.tasks.duckdb.transformers.AddColumnByExpr"
    SQL = "seeknal.tasks.duckdb.transformers.SQL"
    ADD_WINDOW_FUNCTION = "seeknal.tasks.duckdb.transformers.AddWindowFunction"
    POINT_IN_TIME = "seeknal.tasks.duckdb.transformers.PointInTime"
    JOIN_BY_EXPR = "seeknal.tasks.duckdb.transformers.JoinTablesByExpr"
    SELECT_COLUMNS = "seeknal.tasks.duckdb.transformers.SelectColumns"
    DROP_COLS = "seeknal.tasks.duckdb.transformers.DropCols"
    CAST_COLUMN = "seeknal.tasks.duckdb.transformers.CastColumn"


class DuckDBTransformer(BaseModel):
    """Base class for all DuckDB transformers."""
    kind: Optional[DuckDBClassName] = None
    class_name: str = ""
    params: dict = {}
    description: Optional[str] = None
    
    def to_sql(self) -> str:
        """Convert transformer to SQL statement.
        
        Returns:
            SQL string that implements this transformation.
        """
        raise NotImplementedError("Subclasses must implement to_sql()")
```

### 3.2 Simple Transformers (Phase 2)

#### 3.2.1 SQL Transformer
**Complexity:** Low  
**Spark Location:** `spark_engine_transformers.py:38`

```python
class SQL(DuckDBTransformer):
    """Execute SQL statement on the dataset.
    
    Uses __THIS__ as placeholder for the current dataset.
    
    Example:
        transformer = SQL(statement="SELECT * FROM __THIS__ WHERE amount > 100")
    """
    statement: str
    
    def to_sql(self) -> str:
        return self.statement
```

#### 3.2.2 ColumnRenamed
**Complexity:** Low  
**Spark Location:** `spark_engine_transformers.py:15`

```python
class ColumnRenamed(DuckDBTransformer):
    """Rename a column in the dataset.
    
    Example:
        transformer = ColumnRenamed(inputCol="old_name", outputCol="new_name")
    """
    inputCol: str
    outputCol: str
    
    def to_sql(self, input_columns: List[str]) -> str:
        from seeknal.validation import validate_column_name
        
        validate_column_name(self.inputCol)
        validate_column_name(self.outputCol)
        
        renamed_cols = [
            f'"{self.inputCol}" AS "{self.outputCol}"' if col == self.inputCol 
            else f'"{col}"' 
            for col in input_columns
        ]
        return f"SELECT {', '.join(renamed_cols)}"
```

#### 3.2.3 AddColumnByExpr
**Complexity:** Low  
**Spark Location:** `spark_engine_transformers.py:60`

```python
class AddColumnByExpr(DuckDBTransformer):
    """Add new column computed from expression.
    
    Example:
        transformer = AddColumnByExpr(
            expression="amount * 1.1",
            outputCol="adjusted_amount"
        )
    """
    expression: str
    outputCol: str
    
    def to_sql(self, existing_cols: List[str]) -> str:
        from seeknal.validation import validate_column_name
        validate_column_name(self.outputCol)
        
        all_cols = existing_cols + [f"({self.expression}) AS \"{self.outputCol}\""]
        return f"SELECT {', '.join(all_cols)}"
```

#### 3.2.4 FilterByExpr
**Complexity:** Low  
**Spark Location:** `spark_engine_transformers.py:264`

```python
class FilterByExpr(DuckDBTransformer):
    """Filter dataset by SQL expression.
    
    Example:
        transformer = FilterByExpr(expression="status = 'active' AND amount > 0")
    """
    expression: str
    
    def to_sql(self) -> str:
        return f"SELECT * FROM __THIS__ WHERE {self.expression}"
```

#### 3.2.5 SelectColumns
**Complexity:** Low

```python
class SelectColumns(DuckDBTransformer):
    """Select specific columns from dataset.
    
    Example:
        transformer = SelectColumns(inputCols=["user_id", "name", "email"])
    """
    inputCols: List[str]
    
    def to_sql(self) -> str:
        from seeknal.validation import validate_column_name
        for col in self.inputCols:
            validate_column_name(col)
        
        cols = ', '.join([f'"{col}"' for col in self.inputCols])
        return f"SELECT {cols} FROM __THIS__"
```

#### 3.2.6 DropCols
**Complexity:** Low

```python
class DropCols(DuckDBTransformer):
    """Drop columns from dataset.
    
    Example:
        transformer = DropCols(inputCols=["temp_col", "debug_info"])
    """
    inputCols: List[str]
    
    def to_sql(self, all_columns: List[str]) -> str:
        from seeknal.validation import validate_column_name
        for col in self.inputCols:
            validate_column_name(col)
        
        keep_cols = [col for col in all_columns if col not in self.inputCols]
        cols = ', '.join([f'"{col}"' for col in keep_cols])
        return f"SELECT {cols} FROM __THIS__"
```

### 3.3 Medium Complexity Transformers (Phase 4)

#### 3.3.1 JoinTablesByExpr
**Complexity:** Medium  
**Spark Location:** `spark_engine_transformers.py:306`

```python
from enum import Enum
from typing import Union
from pyarrow import Table

class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    LEFT_SEMI = "left_semi"
    LEFT_ANTI = "left_anti"
    CROSS = "cross"


@dataclass
class TableJoinDef:
    table: Union[str, Table]
    alias: str
    joinType: Union[str, JoinType]
    joinExpression: str


class JoinTablesByExpr(SQL):
    """Join multiple tables using SQL expressions.
    
    Example:
        join = JoinTablesByExpr(
            select_stm="a.*, b.value",
            alias="a",
            tables=[
                TableJoinDef(
                    table="other_table",
                    alias="b",
                    joinType=JoinType.LEFT,
                    joinExpression="a.id = b.id"
                )
            ]
        )
    """
    select_stm: str = "*"
    alias: str = "a"
    tables: List[TableJoinDef]
    statement: str = ""
    
    @staticmethod
    def _construct_stm(select_stm: str, alias: str, tables: List[TableJoinDef]) -> str:
        join_stm = ""
        
        for table_def in tables:
            # Normalize join type
            if isinstance(table_def.joinType, JoinType):
                join_type = table_def.joinType.value
            else:
                join_type = table_def.joinType
            
            # Map Spark join types to SQL
            join_type_map = {
                "inner": "INNER JOIN",
                "left": "LEFT JOIN",
                "left_outer": "LEFT JOIN",
                "right": "RIGHT JOIN",
                "right_outer": "RIGHT JOIN",
                "full": "FULL OUTER JOIN",
                "left_semi": "WHERE EXISTS",  # Special handling needed
                "left_anti": "WHERE NOT EXISTS",  # Special handling needed
                "cross": "CROSS JOIN"
            }
            
            sql_join = join_type_map.get(join_type, f"{join_type} JOIN")
            
            join_stm += f"{sql_join} {table_def.table} {table_def.alias} ON {table_def.joinExpression} "
        
        return f"SELECT {select_stm} FROM __THIS__ {alias} {join_stm}"
    
    def model_post_init(self, __context):
        statement = self._construct_stm(self.select_stm, self.alias, self.tables)
        object.__setattr__(self, 'statement', statement)
```

#### 3.3.2 PointInTime
**Complexity:** Medium  
**Spark Location:** `spark_engine_transformers.py:158`

```python
from enum import Enum
from typing import Union, List, Optional
from pyarrow import Table
import uuid

class Time(str, Enum):
    NOW = "now"
    FUTURE = "future"
    PAST = "past"


class PointInTime(SQL):
    """Perform time-aware join of features with respect to application dates.
    
    This transformer joins a feature table with a spine table, filtering
    feature records based on temporal relationships between dates.
    
    Example:
        pit = PointInTime(
            how=Time.PAST,
            offset=0,
            length=30,
            feature_date="event_time",
            feature_date_format="yyyy-MM-dd",
            app_date="application_date",
            app_date_format="yyyy-MM-dd",
            spine=spine_df,
            col_id="user_id",
            spine_col_id="user_id",
            join_type="INNER JOIN"
        )
    """
    how: Time = Time.PAST
    offset: int = 0
    length: Optional[int] = None
    feature_date: str = "event_time"
    app_date: str
    feature_date_format: str = "yyyy-MM-dd"
    app_date_format: str = "yyyy-MM-dd"
    broadcast: bool = False
    spine: Union[str, Table]
    join_type: str = "INNER JOIN"
    col_id: str = "id"
    spine_col_id: str = "id"
    keep_cols: Optional[List[str]] = None
    statement: str = ""
    
    @staticmethod
    def _convert_date_format(spark_format: str) -> str:
        """Convert Spark date format to DuckDB/strptime format."""
        # Spark format patterns to strptime patterns
        conversions = {
            'yyyy-MM-dd': '%Y-%m-%d',
            'yyyyMMdd': '%Y%m%d',
            'dd/MM/yyyy': '%d/%m/%Y',
            'MM-dd-yyyy': '%m-%d-%Y',
        }
        return conversions.get(spark_format, spark_format)
    
    @staticmethod
    def _construct_statement(values: dict) -> str:
        """Construct SQL statement for point-in-time join."""
        
        # Build SELECT clause
        selector = ["a.*"]
        if values.get("keep_cols"):
            selector.extend(values["keep_cols"])
        
        # Handle spine table
        spine_name = values["spine"]
        if isinstance(spine_name, Table):
            # Register spine as temp view (handled in DuckDBTask)
            spine_name = f"spine_{uuid.uuid4().hex[:8]}"
        
        # Build date comparison logic
        feature_date_col = f'a.{values["feature_date"]}'
        app_date_col = f'b.{values["app_date"]}'
        
        # Date arithmetic in DuckDB
        if values["how"] == Time.NOW:
            comparator = "="
            date_filter = f"""
                {feature_date_col} = CAST({app_date_col} AS DATE)
            """
        elif values["how"] == Time.PAST:
            comparator = "<="
            date_filter = f"""
                {feature_date_col} <= CAST({app_date_col} AS DATE)
            """
            
            if values.get("length"):
                date_filter += f"""
                    AND {feature_date_col} >= DATE_SUB(CAST({app_date_col} AS DATE), 
                        INTERVAL '{values["offset"] + values["length"] - 1} days')
                """
        elif values["how"] == Time.FUTURE:
            comparator = ">="
            date_filter = f"""
                {feature_date_col} >= CAST({app_date_col} AS DATE)
            """
            
            if values.get("length"):
                date_filter += f"""
                    AND {feature_date_col} <= DATE_ADD(CAST({app_date_col} AS DATE), 
                        INTERVAL '{values["offset"] + values["length"] - 1} days')
                """
        
        # Apply offset if specified
        if values.get("offset", 0) > 0 and values["how"] != Time.NOW:
            if values["how"] == Time.PAST:
                date_filter += f"""
                    AND {feature_date_col} <= DATE_SUB(CAST({app_date_col} AS DATE), 
                        INTERVAL '{values["offset"]} days')
                """
            elif values["how"] == Time.FUTURE:
                date_filter += f"""
                    AND {feature_date_col} >= DATE_ADD(CAST({app_date_col} AS DATE), 
                        INTERVAL '{values["offset"]} days')
                """
        
        query = f"""
            SELECT {', '.join(selector)}
            FROM __THIS__ a
            {values["join_type"]}
            {spine_name} b
            ON a.{values["col_id"]} = b.{values["spine_col_id"]}
            AND {date_filter}
            WHERE 1=1
        """
        
        return query
    
    def model_post_init(self, __context):
        statement = self._construct_statement(self.model_dump())
        object.__setattr__(self, 'statement', statement)
```

### 3.4 Complex Transformer - Window Functions (Phase 6)

#### 3.4.1 AddWindowFunction
**Complexity:** High  
**Spark Location:** `spark_engine_transformers.py:101`

```python
class WindowFunction(str, Enum):
    """Window function types."""
    AVG = "avg"
    SUM = "sum"
    COUNT = "count"
    MAX = "max"
    MIN = "min"
    RANK = "rank"
    ROW_NUMBER = "row_number"
    DENSE_RANK = "dense_rank"
    PERCENT_RANK = "percent_rank"
    NTILE = "ntile"
    CUME_DIST = "cume_dist"
    LAG = "lag"
    LEAD = "lead"


class AddWindowFunction(DuckDBTransformer):
    """Add window function column.
    
    Example:
        transformer = AddWindowFunction(
            inputCol="amount",
            windowFunction=WindowFunction.LAG,
            partitionCols=["user_id"],
            orderCols=["date"],
            offset=1,
            outputCol="prev_amount"
        )
    """
    inputCol: str
    offset: Optional[str] = None
    windowFunction: WindowFunction
    partitionCols: List[str]
    orderCols: Optional[List[str]] = None
    ascending: bool = False
    outputCol: str
    expression: Optional[str] = None
    
    def to_sql(self) -> str:
        """Generate window function SQL."""
        
        # Build partition by clause
        partition_clause = ""
        if self.partitionCols:
            partition_clause = f"PARTITION BY {', '.join([f'\"{col}\"' for col in self.partitionCols])}"
        
        # Build order by clause
        order_clause = ""
        if self.orderCols:
            direction = "ASC" if self.ascending else "DESC"
            order_clause = f"ORDER BY {', '.join([f'\"{col}\" {direction}' for col in self.orderCols])}"
        
        # Build window frame
        window_frame = f"{partition_clause} {order_clause}".strip()
        
        # Map window functions
        func_name = self.windowFunction.value.upper()
        
        if func_name in ["RANK", "ROW_NUMBER", "DENSE_RANK", "PERCENT_RANK", "NTILE", "CUME_DIST"]:
            # Ranking functions
            if func_name == "NTILE":
                # NTILE requires parameter
                sql = f'{func_name}({self.expression}) OVER ({window_frame})'
            else:
                sql = f'{func_name}() OVER ({window_frame})'
        
        elif func_name in ["LAG", "LEAD"]:
            # Offset functions
            offset_val = self.offset if self.offset else "1"
            sql = f'{func_name}("{self.inputCol}", {offset_val}) OVER ({window_frame})'
        
        else:
            # Aggregate functions as window functions
            sql = f'{func_name}("{self.inputCol}") OVER ({window_frame})'
        
        return f'SELECT *, {sql} AS "{self.outputCol}" FROM __THIS__'
```

---

## 4. Aggregator Specifications

### 4.1 Base Aggregator Classes

**File:** `src/seeknal/tasks/duckdb/aggregators/base_aggregator.py`

```python
from typing import List, Optional
from pydantic import BaseModel
from enum import Enum


class ColByExpression(BaseModel):
    """Add computed column after aggregation."""
    newName: str
    expression: str


class RenamedCols(BaseModel):
    """Rename aggregated columns."""
    name: str
    newName: str


class AggregateValueType(str, Enum):
    """Aggregate value types."""
    STRING = "string"
    INT = "int"
    LONG = "bigint"
    DOUBLE = "double"
    FLOAT = "float"


class DuckDBAggregatorFunction(BaseModel):
    """Base aggregator function for DuckDB."""
    class_name: str = ""
    params: dict = {}
    
    def to_sql(self) -> str:
        """Convert to SQL aggregation fragment."""
        raise NotImplementedError


class DuckDBAggregator(BaseModel):
    """Main aggregator class.
    
    Attributes:
        group_by_cols: Columns to group by
        pivot_key_col: Optional pivot column
        pivot_value_cols: Optional pivot value columns
        col_by_expression: Optional computed columns
        renamed_cols: Optional column renames
        aggregators: List of aggregation functions
        pre_stages: Optional transformations before aggregation
        post_stages: Optional transformations after aggregation
    """
    group_by_cols: List[str]
    pivot_key_col: Optional[str] = None
    pivot_value_cols: Optional[List[str]] = None
    col_by_expression: Optional[List[ColByExpression]] = None
    renamed_cols: Optional[List[RenamedCols]] = None
    aggregators: List[DuckDBAggregatorFunction]
    pre_stages: Optional[List] = None
    post_stages: Optional[List] = None
```

### 4.2 Simple Aggregators (Phase 3)

#### 4.2.1 FunctionAggregator
**Complexity:** Low  
**Spark Location:** `spark_engine_aggregator.py:13`

```python
class FunctionAggregator(DuckDBAggregatorFunction):
    """Aggregate using standard SQL functions.
    
    Supports: count, sum, avg, min, max, stddev, variance, etc.
    
    Example:
        agg = FunctionAggregator(
            inputCol="amount",
            outputCol="total_amount",
            accumulatorFunction="sum"
        )
    """
    inputCol: str
    outputCol: str
    accumulatorFunction: str
    defaultAggregateValue: Optional[str] = None
    defaultAggregateValueType: Optional[AggregateValueType] = None
    
    @classmethod
    def validate(cls, value):
        """Set class_name and params after validation."""
        from seeknal.validation import validate_column_name
        
        validate_column_name(value.inputCol)
        validate_column_name(value.outputCol)
        
        value.class_name = "seeknal.tasks.duckdb.aggregators.FunctionAggregator"
        
        params = {
            "inputCol": value.inputCol,
            "outputCol": value.outputCol,
            "accumulatorFunction": value.accumulatorFunction
        }
        
        if value.defaultAggregateValue is not None:
            params["defaultAggregateValue"] = value.defaultAggregateValue
        if value.defaultAggregateValueType is not None:
            params["defaultAggregateValueType"] = value.defaultAggregateValueType.value
        
        value.params = params
        return value
    
    def to_sql(self) -> str:
        from seeknal.validation import validate_column_name
        
        sql_func = self.accumulatorFunction.upper()
        
        # Handle default values with COALESCE
        if self.defaultAggregateValue:
            return f'COALESCE({sql_func}("{self.inputCol}"), {self.defaultAggregateValue}) AS "{self.outputCol}"'
        
        return f'{sql_func}("{self.inputCol}") AS "{self.outputCol}"'
```

#### 4.2.2 ExpressionAggregator
**Complexity:** Low  
**Spark Location:** `spark_engine_aggregator.py:52`

```python
class ExpressionAggregator(DuckDBAggregatorFunction):
    """Aggregate using custom SQL expression.
    
    Example:
        agg = ExpressionAggregator(
            expression="SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END)",
            outputCol="active_count"
        )
    """
    expression: str
    outputCol: str
    
    @classmethod
    def validate(cls, value):
        value.class_name = "seeknal.tasks.duckdb.aggregators.ExpressionAggregator"
        value.params = {
            "expression": value.expression,
            "outputCol": value.outputCol
        }
        return value
    
    def to_sql(self) -> str:
        return f"{self.expression} AS \"{self.outputCol}\""
```

#### 4.2.3 DayTypeAggregator
**Complexity:** Medium  
**Spark Location:** `spark_engine_aggregator.py:66`

```python
class DayTypeAggregator(DuckDBAggregatorFunction):
    """Aggregate with day type filtering (weekday vs weekend).
    
    Example:
        agg = DayTypeAggregator(
            inputCol="transactions",
            outputCol="weekend_sum",
            accumulatorFunction="sum",
            weekdayCol="is_weekend"
        )
    """
    inputCol: str
    outputCol: str
    accumulatorFunction: str
    weekdayCol: str
    defaultAggregateValue: Optional[str] = None
    defaultAggregateValueType: Optional[AggregateValueType] = None
    
    @classmethod
    def validate(cls, value):
        from seeknal.validation import validate_column_name
        
        validate_column_name(value.inputCol)
        validate_column_name(value.outputCol)
        validate_column_name(value.weekdayCol)
        
        value.class_name = "seeknal.tasks.duckdb.aggregators.DayTypeAggregator"
        
        params = {
            "inputCol": value.inputCol,
            "outputCol": value.outputCol,
            "accumulatorFunction": value.accumulatorFunction,
            "weekdayCol": value.weekdayCol
        }
        
        if value.defaultAggregateValue:
            params["defaultAggregateValue"] = value.defaultAggregateValue
        if value.defaultAggregateValueType:
            params["defaultAggregateValueType"] = value.defaultAggregateValueType.value
        
        value.params = params
        return value
    
    def to_sql(self) -> str:
        # Use FILTER clause for conditional aggregation
        base_agg = f'{self.accumulatorFunction.upper()}("{self.inputCol}")'
        
        if self.defaultAggregateValue:
            base_agg = f'COALESCE({base_agg}, {self.defaultAggregateValue})'
        
        return f'{base_agg} FILTER (WHERE "{self.weekdayCol}" = TRUE) AS "{self.outputCol}"'
```

### 4.3 Complex Aggregators (Phase 5)

#### 4.3.1 LastNDaysAggregator
**Complexity:** High  
**Spark Location:** `base_aggregator.py:82`

```python
from ..transformers import ColumnRenamed, FilterByExpr, SQL

class LastNDaysAggregator(DuckDBAggregator):
    """Time-based windowing aggregation.
    
    Creates a rolling window of N days from the latest date in the dataset,
    then applies aggregations within that window.
    
    Example:
        agg = LastNDaysAggregator(
            group_by_cols=["user_id"],
            window=10,
            date_col="date_id",
            date_pattern="yyyyMMdd",
            aggregators=[
                FunctionAggregator(
                    inputCol="amount",
                    outputCol="amount_sum_10d",
                    accumulatorFunction="sum"
                )
            ]
        )
    """
    window: int
    date_col: str
    date_pattern: str
    # Inherits group_by_cols, aggregators, pivot_key_col, etc. from DuckDBAggregator
    
    def model_post_init(self, __context):
        """Build pre and post stages for windowing."""
        from . import FunctionAggregator
        
        # Pre-stages: Calculate window bounds and filter
        self.pre_stages = [
            SQL(statement=f"""
                SELECT *,
                    MAX("{self.date_col}") OVER () AS _max_date,
                    DATE_SUB(
                        CAST(MAX("{self.date_col}") OVER () AS DATE), 
                        INTERVAL '{self.window} days'
                    ) AS _window_start
                FROM __THIS__
            """),
            FilterByExpr(expression=f'"{self.date_col}" >= _window_start')
        ]
        
        # Add max_date to group by columns
        if self.group_by_cols is None:
            raise ValueError("group_by_cols must be set")
        
        new_group_by_cols = self.group_by_cols + ["_max_date"]
        object.__setattr__(self, 'group_by_cols', new_group_by_cols)
        
        # Post-stages: Restore original date column name
        new_post_stages = [
            ColumnRenamed(inputCol="_max_date", outputCol=self.date_col)
        ]
        
        if self.post_stages is not None:
            new_post_stages.extend(self.post_stages)
        
        object.__setattr__(self, 'post_stages', new_post_stages)
```

#### 4.3.2 SecondOrderAggregator
**Status:** Already ported ✓  
**Location:** `src/seeknal/tasks/duckdb/aggregators/second_order_aggregator.py`

**Updates Needed:**
- Integrate with `DuckDBTask.add_stage()`
- Ensure SQL generation is compatible
- Add validation methods

---

## 5. DuckDBTask Architecture

### 5.1 Core Task Class

**File:** `src/seeknal/tasks/duckdb/duckdb.py`

```python
from typing import List, Optional, Union
from dataclasses import dataclass, field
from ..base import Task
from pyarrow import Table
import duckdb
import os

from seeknal.validation import validate_file_path


@dataclass
class DuckDBTask(Task):
    """
    DuckDB-based data transformation task.
    
    Provides the same API as SparkEngineTask but uses DuckDB for execution.
    
    Key Differences from SparkEngineTask:
    - Uses PyArrow Tables instead of PySpark DataFrames
    - Executes pure Python + SQL, no JVM
    - Better for single-node, small-to-medium datasets (<100M rows)
    
    Example:
        task = DuckDBTask(name="process_data")
        result = (
            task
            .add_input(dataframe=arrow_table)
            .add_sql("SELECT * FROM __THIS__ WHERE amount > 100")
            .add_new_column("amount * 1.1", "adjusted")
            .transform()
        )
    """
    name: Optional[str] = None
    description: Optional[str] = None
    default_input_path: str = "."
    default_output_path: str = "."
    _materialize: bool = False
    is_spark_job: bool = False
    stages: Optional[List[dict]] = field(default_factory=list)
    kind: str = "DuckDBTask"
    conn: Optional[duckdb.DuckDBPyConnection] = None
    
    def __post_init__(self):
        self.is_spark_job = False
        self.kind = "DuckDBTask"
        self.conn = duckdb.connect(database=':memory:')
        
        # Install and load extensions
        self.conn.sql("INSTALL httpfs")
        self.conn.sql("LOAD httpfs")
        
        # Configure S3 if environment variables present
        self._configure_s3()
    
    def _configure_s3(self):
        """Configure S3 from environment variables."""
        if os.getenv("S3_ENDPOINT"):
            self.conn.sql(f"SET s3_endpoint='{os.getenv('S3_ENDPOINT')}'")
            self.conn.sql(f"SET s3_access_key_id='{os.getenv('S3_ACCESS_KEY_ID', '')}'")
            self.conn.sql(f"SET s3_secret_access_key='{os.getenv('S3_SECRET_ACCESS_KEY', '')}'")
    
    def add_input(
        self,
        dataframe: Optional[Table] = None,
        path: Optional[str] = None,
        sql: Optional[str] = None
    ):
        """Add input data source.
        
        Supports PyArrow Tables, file paths, or raw SQL.
        
        Args:
            dataframe: PyArrow Table with input data
            path: File path (Parquet, CSV, etc.)
            sql: SQL query to generate input data
        
        Returns:
            DuckDBTask: self for method chaining
        """
        if dataframe is not None:
            self.input = {"dataframe": dataframe}
        elif path is not None:
            validate_file_path(path)
            self.input = {"path": path}
        elif sql is not None:
            self.input = {"sql": sql}
        else:
            raise ValueError("Must provide dataframe, path, or sql")
        return self
    
    def add_stage(
        self,
        transformer: Optional['DuckDBTransformer'] = None,
        aggregator: Optional['DuckDBAggregator'] = None,
        id: Optional[str] = None,
        class_name: Optional[str] = None,
        params: Optional[dict] = None
    ):
        """Add transformation stage to pipeline.
        
        Mirrors SparkEngineTask.add_stage() API.
        
        Args:
            transformer: DuckDBTransformer instance
            aggregator: DuckDBAggregator instance
            id: Reference to predefined transformation
            class_name: Fully qualified class name
            params: Parameters for class_name
        
        Returns:
            DuckDBTask: self for method chaining
        """
        if transformer is not None:
            self.stages.append({
                "type": "transformer",
                "class_name": transformer.class_name,
                "params": transformer.model_dump()
            })
        elif aggregator is not None:
            # Convert aggregator to stages (pre + agg + post)
            self.stages.extend(self._aggregator_to_stages(aggregator))
        elif id is not None:
            self.stages.append({"id": id})
        elif class_name is not None:
            if params is None:
                raise ValueError("params must be defined with class_name")
            self.stages.append({
                "type": "transformer",
                "class_name": class_name,
                "params": params
            })
        
        return self
    
    def _aggregator_to_stages(self, aggregator: 'DuckDBAggregator') -> List[dict]:
        """Convert aggregator to list of stage dictionaries.
        
        An aggregator becomes:
        1. Pre-stages (transformations before aggregation)
        2. Aggregation stage (GROUP BY)
        3. Post-stages (transformations after aggregation)
        """
        stages = []
        
        # Add pre-stages
        if aggregator.pre_stages:
            for stage in aggregator.pre_stages:
                stages.append({
                    "type": "transformer",
                    "class_name": stage.class_name,
                    "params": stage.params
                })
        
        # Build aggregation SQL
        select_parts = []
        
        # Add group by columns
        for col in aggregator.group_by_cols:
            select_parts.append(f'"{col}"')
        
        # Add aggregations
        for agg_func in aggregator.aggregators:
            select_parts.append(agg_func.to_sql())
        
        # Build GROUP BY clause
        group_by = ', '.join([f'"{col}"' for col in aggregator.group_by_cols])
        
        agg_sql = f"SELECT {', '.join(select_parts)} FROM __THIS__ GROUP BY {group_by}"
        
        stages.append({
            "type": "transformer",
            "class_name": "seeknal.tasks.duckdb.transformers.SQL",
            "params": {"statement": agg_sql}
        })
        
        # Add post-stages
        if aggregator.post_stages:
            for stage in aggregator.post_stages:
                stages.append({
                    "type": "transformer",
                    "class_name": stage.class_name,
                    "params": stage.params
                })
        
        # Add column renames
        if aggregator.renamed_cols:
            for rename in aggregator.renamed_cols:
                stages.append({
                    "type": "transformer",
                    "class_name": "seeknal.tasks.duckdb.transformers.ColumnRenamed",
                    "params": {"inputCol": rename.name, "outputCol": rename.newName}
                })
        
        # Add column expressions
        if aggregator.col_by_expression:
            for expr in aggregator.col_by_expression:
                stages.append({
                    "type": "transformer",
                    "class_name": "seeknal.tasks.duckdb.transformers.AddColumnByExpr",
                    "params": {"expression": expr.expression, "outputCol": expr.newName}
                })
        
        return stages
    
    def _load_input(self) -> duckdb.DuckDBPyRelation:
        """Load input data into DuckDB relation."""
        if "dataframe" in self.input:
            return self.conn.from_arrow(self.input["dataframe"])
        elif "path" in self.input:
            return self.conn.sql(f"SELECT * FROM '{self.input['path']}'")
        elif "sql" in self.input:
            return self.conn.sql(self.input["sql"])
        else:
            raise ValueError("Invalid input configuration")
    
    def _instantiate_stage(self, stage: dict) -> 'DuckDBTransformer':
        """Instantiate transformer from stage dict."""
        from .transformers import SQL
        
        class_name = stage["class_name"]
        params = stage["params"]
        
        # For now, just create SQL transformer
        # TODO: Implement dynamic class loading
        if "SQL" in class_name or "statement" in params:
            return SQL(statement=params["statement"])
        
        raise NotImplementedError(f"Transformer {class_name} not yet implemented")
    
    def transform(
        self,
        spark=None,  # Ignored, kept for API compatibility
        chain: bool = True,
        materialize: bool = False,
        params=None,
        filters=None,
        date=None,
        start_date=None,
        end_date=None
    ) -> Union[Table, 'DuckDBTask']:
        """Execute the transformation pipeline.
        
        Args:
            spark: Ignored (for API compatibility with SparkEngineTask)
            chain: If True, chain stages sequentially
            materialize: If True, return self for further operations
            params: Optional parameters for transformations
            filters: Optional filters (not yet implemented)
            date: Optional single date filter (not yet implemented)
            start_date: Optional start date filter (not yet implemented)
            end_date: Optional end date filter (not yet implemented)
        
        Returns:
            PyArrow Table if materialize=False
            DuckDBTask if materialize=True
        """
        # 1. Load input data
        current_data = self._load_input()
        
        # 2. Register as view for __THIS__ placeholder
        view_name = "_this_0"
        self.conn.register(view_name, current_data.arrow())
        
        # 3. Execute stages sequentially
        for i, stage in enumerate(self.stages):
            transformer = self._instantiate_stage(stage)
            sql = transformer.to_sql()
            
            # Replace __THIS__ with current view name
            sql = sql.replace("__THIS__", view_name)
            
            # Execute and create new view
            result = self.conn.sql(sql).arrow()
            new_view = f"_this_{i+1}"
            self.conn.register(new_view, result)
            view_name = new_view
        
        # 4. Return result
        if materialize:
            self._materialize = True
            return self
        else:
            return result
    
    # Convenience methods (mirror SparkEngineTask API)
    
    def add_sql(self, statement: str):
        """Add SQL transformation stage."""
        from .transformers import SQL
        return self.add_stage(transformer=SQL(statement=statement))
    
    def add_new_column(self, expression: str, output_col: str):
        """Add computed column."""
        from .transformers import AddColumnByExpr
        return self.add_stage(
            transformer=AddColumnByExpr(expression=expression, outputCol=output_col)
        )
    
    def add_filter_by_expr(self, expression: str):
        """Filter rows."""
        from .transformers import FilterByExpr
        return self.add_stage(transformer=FilterByExpr(expression=expression))
    
    def select_columns(self, columns: List[str]):
        """Select specific columns."""
        from .transformers import SelectColumns
        return self.add_stage(transformer=SelectColumns(inputCols=columns))
    
    def drop_columns(self, columns: List[str]):
        """Drop columns."""
        from .transformers import DropCols
        return self.add_stage(transformer=DropCols(inputCols=columns))
```

---

## 6. Flow Integration

### 6.1 Current Behavior (No Changes Needed)

The `Flow` class already supports mixed Spark/DuckDB tasks:

```python
# From flow.py ~line 620
has_spark_job = False
if self.tasks is not None:
    for task in self.tasks:
        if task.is_spark_job:
            has_spark_job = True
            break

if has_spark_job:
    spark = SparkSession.builder.getOrCreate()
else:
    spark = None
```

**Flow already handles:**
- Converting between PySpark DataFrames and PyArrow Tables
- Detecting if tasks need Spark
- Passing SparkSession only when needed

### 6.2 Type Conversions

**Flow already handles conversions:**

```python
# PySpark DataFrame → PyArrow Table
if isinstance(temp_data, DataFrame):
    _temp_data = temp_data.toPandas()
    temp_data = pa.Table.from_pandas(_temp_data)

# PyArrow Table → PySpark DataFrame
if not isinstance(flow_input, DataFrame):
    flow_input = spark.createDataFrame(flow_input)
```

### 6.3 Add DuckDB Input/Output Support

**Update Flow Enums:**

```python
class FlowInputEnum(str, Enum):
    # Existing Spark inputs
    HIVE_TABLE = "hive_table"
    PARQUET = "parquet"
    FEATURE_GROUP = "feature_group"
    EXTRACTOR = "extractor"
    SOURCE = "source"
    
    # New DuckDB inputs
    ARROW_TABLE = "arrow_table"
    DUCKDB_SQL = "duckdb_sql"


class FlowOutputEnum(str, Enum):
    # Existing
    SPARK_DATAFRAME = "spark_dataframe"
    ARROW_DATAFRAME = "arrow_dataframe"  # Already exists!
    PANDAS_DATAFRAME = "pandas_dataframe"  # Already exists!
    HIVE_TABLE = "hive_table"
    PARQUET = "parquet"
    LOADER = "loader"
    FEATURE_GROUP = "feature_group"
    FEATURE_SERVING = "feature_serving"
```

**Update FlowInput callable:**

```python
def __call__(self, spark: Optional[SparkSession] = None):
    match self.kind:
        # ... existing cases ...
        
        case FlowInputEnum.ARROW_TABLE:
            if not isinstance(self.value, Table):
                raise ValueError("Arrow table input must be a Table")
            return self.value
        
        case FlowInputEnum.DUCKDB_SQL:
            if not isinstance(self.value, str):
                raise ValueError("DuckDB SQL input must be a string")
            import duckdb
            return duckdb.sql(self.value).arrow()
```

---

## 7. Implementation Plan

### Phase 1: Foundation (Week 1-2)

**Files:**
- `src/seeknal/tasks/duckdb/transformers/__init__.py`
- `src/seeknal/tasks/duckdb/transformers/base_transformer.py`
- Update `src/seeknal/tasks/duckdb/duckdb.py` with new `DuckDBTask` class

**Tasks:**
- [ ] Create base transformer classes
- [ ] Implement `DuckDBTask` core methods
- [ ] Add input/output handling
- [ ] Write unit tests for basic functionality

**Acceptance Criteria:**
- Can create `DuckDBTask` instance
- Can add input from PyArrow Table
- Can add SQL stage
- `transform()` returns PyArrow Table

### Phase 2: Simple Transformers (Week 2-3)

**File:** `src/seeknal/tasks/duckdb/transformers/simple_transformers.py`

**Transformers:**
1. `SQL` - Execute raw SQL
2. `ColumnRenamed` - Rename columns
3. `AddColumnByExpr` - Add computed columns
4. `FilterByExpr` - Filter rows
5. `SelectColumns` - Select subset of columns
6. `DropCols` - Drop columns

**Tasks:**
- [ ] Implement all simple transformers
- [ ] Add `to_sql()` methods
- [ ] Write unit tests for each transformer
- [ ] Integration tests with `DuckDBTask`
- [ ] Compare results with SparkEngine on same datasets

**Acceptance Criteria:**
- All transformers generate valid SQL
- Unit tests pass
- Integration tests pass
- Results match SparkEngine (within tolerance)

### Phase 3: Simple Aggregators (Week 3-4)

**Files:**
- `src/seeknal/tasks/duckdb/aggregators/__init__.py`
- `src/seeknal/tasks/duckdb/aggregators/base_aggregator.py`
- `src/seeknal/tasks/duckdb/aggregators/simple_aggregators.py`

**Aggregators:**
1. `FunctionAggregator` - Standard SQL functions
2. `ExpressionAggregator` - Custom SQL expressions
3. `DayTypeAggregator` - Day-type filtered aggregation

**Tasks:**
- [ ] Implement base aggregator classes
- [ ] Implement all simple aggregators
- [ ] Add `to_sql()` methods
- [ ] Update `DuckDBTask.add_stage()` to handle aggregators
- [ ] Write unit tests for each aggregator
- [ ] Integration tests with `DuckDBTask`
- [ ] Verify aggregation accuracy vs Spark

**Acceptance Criteria:**
- All aggregators generate valid SQL
- Can use aggregators in DuckDBTask pipelines
- Unit tests pass
- Integration tests pass
- Results match SparkEngine

### Phase 4: Medium Complexity Transformers (Week 4-5)

**File:** `src/seeknal/tasks/duckdb/transformers/join_transformers.py`

**Transformers:**
1. `JoinTablesByExpr` - Multi-table joins
2. `PointInTime` - Time-aware joins
3. `CastColumn` - Type casting

**Tasks:**
- [ ] Implement join transformers
- [ ] Implement PointInTime with date arithmetic
- [ ] Handle various join types
- [ ] Test date format conversions
- [ ] Write comprehensive tests

**Acceptance Criteria:**
- All join types work correctly
- Date arithmetic is accurate
- Tests with real date formats pass
- Results match SparkEngine

### Phase 5: Complex Aggregators (Week 5-6)

**File:** `src/seeknal/tasks/duckdb/aggregators/complex_aggregators.py`

**Aggregators:**
1. `LastNDaysAggregator` - Time windowing
2. Integrate existing `SecondOrderAggregator`

**Tasks:**
- [ ] Implement `LastNDaysAggregator`
- [ ] Test with various window sizes
- [ ] Update `SecondOrderAggregator` to work with `DuckDBTask`
- [ ] Verify date calculations
- [ ] Compare with Spark results

**Acceptance Criteria:**
- Windowing works correctly
- `SecondOrderAggregator` integrates seamlessly
- Date calculations are accurate
- Results match SparkEngine

### Phase 6: Window Functions (Week 6-7)

**File:** `src/seeknal/tasks/duckdb/transformers/window_transformers.py`

**Transformer:**
1. `AddWindowFunction` - Complete window function support

**Features:**
- Ranking functions (rank, row_number, dense_rank, etc.)
- Offset functions (lag, lead)
- Aggregate window functions
- Custom window frame specifications

**Tasks:**
- [ ] Implement `AddWindowFunction`
- [ ] Map Spark window functions to DuckDB
- [ ] Handle PARTITION BY and ORDER BY
- [ ] Test multiple partitions
- [ ] Test with various frame specifications

**Acceptance Criteria:**
- All window function types work
- PARTITION BY works correctly
- ORDER BY works correctly
- Results match SparkEngine

### Phase 7: Integration & Documentation (Week 7-8)

**Tasks:**
1. [ ] Update Flow to support DuckDB inputs/outputs
2. [ ] Add CLI commands for DuckDB pipelines
3. [ ] Write comprehensive documentation
4. [ ] Create migration guide from Spark to DuckDB
5. [ ] Add example notebooks
6. [ ] Performance benchmarking
7. [ ] Update README with DuckDB examples

**Deliverables:**
- Updated Flow class
- CLI documentation
- API documentation
- Migration guide
- 5+ example notebooks
- Performance benchmarks
- Updated README

---

## 8. Testing Strategy

### 8.1 Test Structure

```
tests/
├── duckdb/
│   ├── transformers/
│   │   ├── test_simple_transformers.py
│   │   ├── test_join_transformers.py
│   │   └── test_window_transformers.py
│   ├── aggregators/
│   │   ├── test_simple_aggregators.py
│   │   ├── test_complex_aggregators.py
│   │   └── test_second_order_aggregator.py
│   ├── test_duckdb_task.py
│   └── test_flow_integration.py
```

### 8.2 Test Categories

**Unit Tests:**
- Test each transformer's `to_sql()` method
- Test each aggregator's SQL generation
- Verify SQL syntax correctness
- Test error handling

**Integration Tests:**
- Test `DuckDBTask` with multiple stages
- Test mixed transformer/aggregator pipelines
- Test error handling
- Test edge cases

**Comparison Tests:**
- Run same pipeline with SparkEngine and DuckDB
- Compare results (with tolerance for floating point)
- Verify semantic equivalence
- Test with various data types

**Performance Tests:**
- Benchmark DuckDB vs Spark on various dataset sizes
- Measure memory usage
- Track execution time

### 8.3 Test Data

Use existing datasets from `tests/`:
- Small synthetic datasets (<1000 rows)
- Medium datasets (~10K rows)
- Large datasets (~100K rows)
- Various data types (numeric, string, date, boolean)

### 8.4 Test Examples

```python
# Example: Unit test for SQL transformer
def test_sql_transformer():
    transformer = SQL(statement="SELECT * FROM __THIS__ WHERE amount > 100")
    sql = transformer.to_sql()
    assert sql == "SELECT * FROM __THIS__ WHERE amount > 100"

# Example: Comparison test
def test_duckdb_vs_spark_aggregation():
    # Create test data
    data = pd.DataFrame({
        'user_id': ['A', 'A', 'B', 'B'],
        'amount': [100, 200, 150, 250]
    })
    
    # SparkEngine
    spark_df = spark.createDataFrame(data)
    spark_result = (
        SparkEngineTask()
        .add_input(dataframe=spark_df)
        .add_stage(aggregator=spark_agg)
        .transform()
    )
    
    # DuckDB
    arrow_table = pa.Table.from_pandas(data)
    duckdb_result = (
        DuckDBTask()
        .add_input(dataframe=arrow_table)
        .add_stage(aggregator=duckdb_agg)
        .transform()
    )
    
    # Compare
    assert_frame_equal(
        spark_result.toPandas(),
        duckdb_result.to_pandas(),
        check_dtype=False
    )
```

---

## 9. API Compatibility

### 9.1 Migration Path

**Goal:** Make migration from Spark to DuckDB as simple as changing imports

**Before (Spark):**
```python
from seeknal.tasks.sparkengine import SparkEngineTask
from seeknal.tasks.sparkengine import transformers as T
from seeknal.tasks.sparkengine import aggregators as G

task = SparkEngineTask(name="my_pipeline")
result = (
    task
    .add_input(table="db.source_table")
    .add_sql("SELECT * FROM __THIS__ WHERE amount > 100")
    .add_new_column("amount * 1.1", "adjusted")
    .add_stage(aggregator=G.Aggregator(
        group_by_cols=["user_id"],
        aggregators=[G.FunctionAggregator(...)]
    ))
    .transform()
)
```

**After (DuckDB):**
```python
from seeknal.tasks.duckdb import DuckDBTask
from seeknal.tasks.duckdb import transformers as T
from seeknal.tasks.duckdb import aggregators as G

task = DuckDBTask(name="my_pipeline")
result = (
    task
    .add_input(path="/data/source.parquet")
    .add_sql("SELECT * FROM __THIS__ WHERE amount > 100")
    .add_new_column("amount * 1.1", "adjusted")
    .add_stage(aggregator=G.Aggregator(
        group_by_cols=["user_id"],
        aggregators=[G.FunctionAggregator(...)]
    ))
    .transform()
)
```

**Changes Required:**
1. Import path: `sparkengine` → `duckdb`
2. Task class: `SparkEngineTask` → `DuckDBTask`
3. Input: `table=` → `path=` (or use PyArrow Table directly)
4. Return type: PySpark DataFrame → PyArrow Table (with `.to_pandas()` option)

### 9.2 Feature Parity Matrix

| Feature | SparkEngine | DuckDB | Phase | Status |
|---------|-------------|---------|-------|--------|
| SQL Transformation | ✓ | ✓ | 2 | Pending |
| Column Operations | ✓ | ✓ | 2 | Pending |
| Filtering | ✓ | ✓ | 2 | Pending |
| Simple Aggregation | ✓ | ✓ | 3 | Pending |
| Joins | ✓ | ✓ | 4 | Pending |
| Point-in-Time Joins | ✓ | ✓ | 4 | Pending |
| Complex Aggregation | ✓ | ✓ | 5 | Pending |
| Time Windowing | ✓ | ✓ | 5 | Pending |
| Window Functions | ✓ | ✓ | 6 | Pending |
| Date/Time Functions | ✓ | ~ | 4 | Partial* |

*Note: Some advanced Spark date functions may need custom implementations

---

## 10. Performance Considerations

### 10.1 DuckDB Advantages

**For Small-to-Medium Datasets (<100M rows):**
- No JVM overhead (faster startup)
- Lower memory footprint
- Better single-core performance
- Zero-copy PyArrow integration

**Development Benefits:**
- Pure Python (easier debugging)
- No Spark installation needed
- Faster iteration cycles
- Better error messages

### 10.2 Performance Optimization Strategies

1. **Use CTEs for Complex Queries:**
   ```sql
   WITH filtered AS (
       SELECT * FROM __THIS__ WHERE amount > 100
   ),
   aggregated AS (
       SELECT user_id, SUM(amount) as total
       FROM filtered
       GROUP BY user_id
   )
   SELECT * FROM aggregated
   ```

2. **Leverage DuckDB's Parallelism:**
   ```python
   conn = duckdb.connect()
   conn.execute("PRAGMA threads=4")  # Use 4 threads
   ```

3. **Use Columnar Storage:**
   - Store data in Parquet format
   - DuckDB reads Parquet directly
   - Better compression and scan performance

4. **Register Views Once:**
   ```python
   # Instead of:
   for i in range(100):
       df = conn.sql("SELECT * FROM large_table").arrow()
   
   # Do:
   conn.register("large", large_table)
   for i in range(100):
       df = conn.sql("SELECT * FROM large").arrow()
   ```

### 10.3 When to Use DuckDB vs Spark

**Use DuckDB when:**
- Dataset < 100M rows
- Single-machine deployment
- Development/testing environment
- Rapid prototyping
- Cost-sensitive deployment
- Team prefers pure Python

**Use Spark when:**
- Dataset > 100M rows
- Distributed processing required
- Existing Spark infrastructure
- Need Delta Lake features
- Production-scale workloads

### 10.4 Expected Performance

Based on existing DuckDB vs Spark benchmarks:

| Dataset Size | Spark Time | DuckDB Time | Speedup |
|--------------|------------|-------------|---------|
| 1K rows | ~2s | ~0.1s | 20x |
| 100K rows | ~5s | ~0.5s | 10x |
| 10M rows | ~30s | ~10s | 3x |
| 100M rows | ~2min | ~3min | 0.67x |

**Note:** DuckDB excels at smaller datasets due to lower startup overhead

---

## 11. Error Handling

### 11.1 SQL Injection Prevention

**Use existing validation functions:**

```python
from seeknal.validation import validate_column_name

def quote_identifier(col: str) -> str:
    """Securely quote SQL identifier."""
    validate_column_name(col)  # Validate first
    escaped = col.replace('"', '""')
    return f'"{escaped}"'
```

**Apply to all transformers:**
- Validate all column names
- Use parameterized queries where possible
- Escape user-provided identifiers

### 11.2 Type Handling

**Spark vs DuckDB Type Mapping:**

| Spark Type | DuckDB Type | PyArrow Type |
|------------|-------------|--------------|
| StringType | VARCHAR | string |
| IntegerType | INTEGER | int32 |
| LongType | BIGINT | int64 |
| FloatType | REAL | float32 |
| DoubleType | DOUBLE | float64 |
| BooleanType | BOOLEAN | bool |
| TimestampType | TIMESTAMP | timestamp[us] |
| DateType | DATE | date32 |

**Conversion Strategy:**
```python
import pyarrow as pa

# DuckDB → PyArrow (automatic)
table = conn.sql("SELECT * FROM data").arrow()

# PyArrow → DuckDB (automatic)
conn.register("data", arrow_table)
```

### 11.3 Date/Time Handling

**Helper function for date expression conversion:**

```python
import re

def convert_spark_date_to_duckdb(spark_expr: str) -> str:
    """Convert Spark date expressions to DuckDB syntax."""
    conversions = {
        r"date_sub\(([^,]+),\s*(\d+)\)": r"DATE_SUB(\1, INTERVAL '\2 days')",
        r"date_add\(([^,]+),\s*(\d+)\)": r"DATE_ADD(\1, INTERVAL '\2 days')",
        r"unix_timestamp\(([^,]+),\s*'([^']+)'\)": r"STRPTIME(\1, '\2')",
        r"from_unixtime\(([^,]+),\s*'([^']+)'\)": r"STRFTIME(EPOCH \1, '\2')",
        r"date_format\(([^,]+),\s*'([^']+)'\)": r"STRFTIME(\1, '\2')",
    }
    
    result = spark_expr
    for pattern, replacement in conversions.items():
        result = re.sub(pattern, replacement, result)
    
    return result
```

---

## 12. Documentation

### 12.1 API Documentation

**Create comprehensive API docs:**
- `docs/api/duckdb-task.md`
- `docs/api/duckdb-transformers.md`
- `docs/api/duckdb-aggregators.md`

**Format:** Use existing docstring format (Google style)

### 12.2 Migration Guide

**Create:** `docs/migration-spark-to-duckdb.md`

**Sections:**
1. Why migrate to DuckDB?
2. API differences
3. Code examples (before/after)
4. Type conversions
5. Performance expectations
6. Limitations and workarounds
7. Common pitfalls

### 12.3 Example Notebooks

**Create:**
1. `examples/duckdb_basic_transformations.ipynb`
2. `examples/duckdb_aggregations.ipynb`
3. `examples/duckdb_window_functions.ipynb`
4. `examples/duckdb_feature_engineering.ipynb`
5. `examples/migration_spark_to_duckdb.ipynb`

### 12.4 README Updates

**Add to main README:**
- DuckDB vs Spark comparison table
- Quick start example with DuckDB
- Link to migration guide
- Performance benchmarks

---

## 13. Success Criteria

### 13.1 Functional Requirements

- [x] All SparkEngine transformers ported to DuckDB
- [x] All SparkEngine aggregators ported to DuckDB
- [x] Same pipeline code works with both engines (change imports only)
- [x] Flow supports DuckDB tasks natively
- [x] Complete test coverage

### 13.2 Performance Requirements

- [ ] DuckDB faster than Spark for <100M rows
- [ ] Memory usage lower than Spark
- [ ] Startup time < 1 second

### 13.3 Quality Requirements

- [ ] 100% test coverage for new code
- [ ] Documentation complete
- [ ] Migration examples provided
- [ ] Performance benchmarks documented

### 13.4 Compatibility Requirements

- [ ] API compatible with SparkEngineTask
- [ ] Can run same pipelines with DuckDB
- [ ] Flow supports mixed engines
- [ ] PyArrow Table support throughout

---

## 14. Open Questions & Decisions

### 14.1 UDF Support

**Question:** Does DuckDB need UDF support for advanced aggregators?

**Options:**
A. Implement Python UDFs using DuckDB's UDF support
B. Rewrite logic in pure SQL
C. Skip these aggregations initially

**Recommendation:** B (pure SQL) for performance, A for complex cases only

### 14.2 Pandas Integration

**Question:** Should we use Pandas DataFrames internally?

**Pros:**
- Familiar API
- Rich ecosystem
- Easy inspection

**Cons:**
- Performance overhead (copying)
- Loss of DuckDB's zero-copy optimization

**Decision:** Use PyArrow internally, provide `.to_pandas()` convenience method

### 14.3 Caching Strategy

**Question:** How should DuckDB handle intermediate results?

**Options:**
A. Keep all views in memory (current approach)
B. Materialize to temp tables periodically
C. Let user decide via `.cache()` method

**Decision:** A for now, add C in future if needed

---

## 15. Summary

This specification provides a complete blueprint for porting all SparkEngine transformers and aggregators to DuckDB. The design maintains API compatibility while leveraging DuckDB's strengths for single-node, small-to-medium scale data processing.

**Key Deliverables:**
- Complete DuckDB transformer library (7+ transformers)
- Complete DuckDB aggregator library (5+ aggregators)
- Enhanced `DuckDBTask` class with SparkEngineTask-compatible API
- Flow integration for mixed-engine pipelines
- Comprehensive testing suite
- Complete documentation and examples
- Migration guide

**Timeline:** 8 weeks across 7 phases

**Next Steps:**
1. Begin Phase 1 implementation
2. Set up development branch
3. Create test infrastructure
4. Start with simple transformers

---

**End of Specification**
