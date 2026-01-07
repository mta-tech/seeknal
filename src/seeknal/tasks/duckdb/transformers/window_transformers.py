"""
Window function transformers for DuckDB.

These transformers add window function columns for ranking, offset, and aggregate operations.
"""

from typing import List, Optional, Union
from enum import Enum
from pydantic import field_validator

from seeknal.validation import validate_column_name

from .base_transformer import DuckDBTransformer, DuckDBClassName


class WindowFunction(str, Enum):
    """Window function types supported by DuckDB."""

    # Aggregate functions
    AVG = "avg"
    SUM = "sum"
    COUNT = "count"
    MAX = "max"
    MIN = "min"
    STDDEV = "stddev"
    VARIANCE = "variance"

    # Ranking functions
    RANK = "rank"
    ROW_NUMBER = "row_number"
    DENSE_RANK = "dense_rank"
    PERCENT_RANK = "percent_rank"
    NTILE = "ntile"
    CUME_DIST = "cume_dist"

    # Offset functions
    LAG = "lag"
    LEAD = "lead"


class AddWindowFunction(DuckDBTransformer):
    """Add window function column.

    Supports various window operations including ranking, offset, and
    aggregate window functions.

    Attributes:
        inputCol: Column to apply window function on
        offset: Offset for LAG/LEAD functions (optional, can be int or str)
        windowFunction: Type of window function
        partitionCols: Columns to partition by
        orderCols: Columns to order within partition (optional)
        ascending: Sort order for ORDER BY (default: False)
        outputCol: Name of the output column
        expression: Custom expression for advanced functions (optional)

    Example:
        >>> from seeknal.tasks.duckdb.transformers import AddWindowFunction, WindowFunction
        >>>
        >>> transformer = AddWindowFunction(
        ...     inputCol="amount",
        ...     windowFunction=WindowFunction.LAG,
        ...     partitionCols=["user_id"],
        ...     orderCols=["date"],
        ...     offset=1,
        ...     outputCol="prev_amount"
        ... )
        >>> sql = transformer.to_sql()
    """

    inputCol: str
    offset: Optional[Union[str, int]] = None
    windowFunction: WindowFunction
    partitionCols: List[str]
    orderCols: Optional[List[str]] = None
    ascending: bool = False
    outputCol: str
    expression: Optional[str] = None
    kind: DuckDBClassName = DuckDBClassName.ADD_WINDOW_FUNCTION
    class_name: str = "seeknal.tasks.duckdb.transformers.AddWindowFunction"
    description: Optional[str] = None
    params: dict = {}

    @field_validator('offset')
    @classmethod
    def convert_offset_to_str(cls, v):
        """Convert offset to string if it's an integer."""
        if v is None:
            return None
        return str(v)

    def model_post_init(self, __context):
        """Validate and set parameters."""
        validate_column_name(self.inputCol)
        validate_column_name(self.outputCol)

        for col in self.partitionCols:
            validate_column_name(col)

        if self.orderCols:
            for col in self.orderCols:
                validate_column_name(col)

        self.params = {
            "inputCol": self.inputCol,
            "offset": self.offset,
            "windowFunction": self.windowFunction,
            "partitionCols": self.partitionCols,
            "orderCols": self.orderCols,
            "ascending": self.ascending,
            "outputCol": self.outputCol,
            "expression": self.expression,
        }

    def to_sql(self) -> str:
        """Generate window function SQL.

        Returns:
            SQL SELECT statement with window function
        """
        # Build partition by clause
        partition_clause = ""
        if self.partitionCols:
            quoted_cols = [f'"{col}"' for col in self.partitionCols]
            partition_clause = f"PARTITION BY {', '.join(quoted_cols)}"

        # Build order by clause
        order_clause = ""
        if self.orderCols:
            direction = "ASC" if self.ascending else "DESC"
            quoted_cols = [f'"{col}" {direction}' for col in self.orderCols]
            order_clause = f"ORDER BY {', '.join(quoted_cols)}"

        # Build window frame
        window_frame = " ".join([partition_clause, order_clause]).strip()

        # Map window functions
        func_name = self.windowFunction.value.upper()

        if func_name in ["RANK", "ROW_NUMBER", "DENSE_RANK", "PERCENT_RANK", "NTILE", "CUME_DIST"]:
            # Ranking functions
            if func_name == "NTILE":
                # NTILE requires parameter
                if self.expression:
                    sql = f'{func_name}({self.expression}) OVER ({window_frame})'
                else:
                    raise ValueError("NTILE requires expression parameter")
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
