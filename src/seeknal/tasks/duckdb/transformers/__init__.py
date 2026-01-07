"""
DuckDB Transformers for Seeknal.

This module provides transformer classes that mirror the SparkEngine transformers
but use DuckDB as the execution engine. All transformers generate SQL that is
executed by DuckDB.

Example:
    >>> from seeknal.tasks.duckdb import DuckDBTask
    >>> from seeknal.tasks.duckdb.transformers import SQL, FilterByExpr
    >>>
    >>> task = DuckDBTask(name="example")
    >>> result = task.add_input(path="data.parquet") \\
    ...              .add_sql("SELECT * FROM __THIS__ WHERE amount > 100") \\
    ...              .transform()
"""

from .base_transformer import (
    DuckDBTransformer,
    DuckDBClassName,
)

from .simple_transformers import (
    SQL,
    ColumnRenamed,
    AddColumnByExpr,
    FilterByExpr,
    SelectColumns,
    DropCols,
)

from .medium_transformers import (
    JoinTablesByExpr,
    PointInTime,
    Time,
    JoinType,
    TableJoinDef,
    CastColumn,
)

from .window_transformers import (
    AddWindowFunction,
    WindowFunction,
)

__all__ = [
    # Base classes
    "DuckDBTransformer",
    "DuckDBClassName",
    
    # Simple transformers
    "SQL",
    "ColumnRenamed",
    "AddColumnByExpr",
    "FilterByExpr",
    "SelectColumns",
    "DropCols",
    
    # Medium transformers
    "JoinTablesByExpr",
    "PointInTime",
    "Time",
    "JoinType",
    "TableJoinDef",
    "CastColumn",
    
    # Complex transformers
    "AddWindowFunction",
    "WindowFunction",
]
