"""
Simple DuckDB transformers.

These transformers handle basic data transformation operations like
column operations, filtering, and SQL execution.
"""

from typing import List, Optional
from enum import Enum

from seeknal.validation import validate_column_name

from .base_transformer import DuckDBTransformer, DuckDBClassName


class SQL(DuckDBTransformer):
    """Execute SQL statement on the dataset.

    Uses __THIS__ as placeholder for the current dataset. This is the most
    flexible transformer as it allows arbitrary SQL.

    Attributes:
        statement: SQL statement to execute
        kind: Transformer kind enum
        class_name: Fully qualified class name

    Example:
        >>> transformer = SQL(statement="SELECT * FROM __THIS__ WHERE amount > 100")
        >>> sql = transformer.to_sql()
        >>> print(sql)
        SELECT * FROM __THIS__ WHERE amount > 100
    """

    statement: str
    kind: DuckDBClassName = DuckDBClassName.SQL
    class_name: str = "seeknal.tasks.duckdb.transformers.SQL"
    description: Optional[str] = None
    params: dict = {}

    def model_post_init(self, __context):
        """Set params after initialization."""
        self.params = {"statement": self.statement}

    def to_sql(self) -> str:
        """Return the SQL statement."""
        return self.statement


class ColumnRenamed(DuckDBTransformer):
    """Rename a single column in the dataset.

    Attributes:
        inputCol: Current column name
        outputCol: New column name

    Example:
        >>> transformer = ColumnRenamed(inputCol="old_name", outputCol="new_name")
        >>> # Generates: SELECT "old_name" AS "new_name", ...
    """

    inputCol: str
    outputCol: str
    kind: DuckDBClassName = DuckDBClassName.COLUMN_RENAMED
    class_name: str = "seeknal.tasks.duckdb.transformers.ColumnRenamed"
    description: Optional[str] = None
    params: dict = {}

    def model_post_init(self, __context):
        """Set params after initialization."""
        self.params = {"inputCol": self.inputCol, "outputCol": self.outputCol}

    def to_sql(self, input_columns: Optional[List[str]] = None) -> str:
        """Generate SQL for column renaming.

        Args:
            input_columns: List of all column names in the dataset (optional, will use SELECT * otherwise)

        Returns:
            SQL SELECT statement with column alias
        """
        validate_column_name(self.inputCol)
        validate_column_name(self.outputCol)

        if input_columns is None:
            # If no columns provided, use SELECT * with rename
            return f'SELECT *, "{self.inputCol}" AS "{self.outputCol}" FROM __THIS__'

        renamed_cols = []
        for col in input_columns:
            if col == self.inputCol:
                renamed_cols.append(f'"{self.inputCol}" AS "{self.outputCol}"')
            else:
                renamed_cols.append(f'"{col}"')

        return f"SELECT {', '.join(renamed_cols)}"


class AddColumnByExpr(DuckDBTransformer):
    """Add new column computed from expression.

    The expression can reference any existing columns and use SQL functions.

    Attributes:
        expression: SQL expression to compute the new column
        outputCol: Name of the new column

    Example:
        >>> transformer = AddColumnByExpr(
        ...     expression="amount * 1.1",
        ...     outputCol="adjusted_amount"
        ... )
        >>> # Generates: SELECT *, (amount * 1.1) AS "adjusted_amount"
    """

    expression: str
    outputCol: str
    kind: DuckDBClassName = DuckDBClassName.ADD_COLUMN_BY_EXPR
    class_name: str = "seeknal.tasks.duckdb.transformers.AddColumnByExpr"
    description: Optional[str] = None
    params: dict = {}

    def model_post_init(self, __context):
        """Set params after initialization."""
        self.params = {"expression": self.expression, "outputCol": self.outputCol}

    def to_sql(self, existing_cols: Optional[List[str]] = None) -> str:
        """Generate SQL for adding computed column.

        Args:
            existing_cols: List of existing column names

        Returns:
            SQL SELECT statement with new computed column
        """
        if existing_cols is None:
            # If no columns provided, just select * plus new column
            return f'SELECT *, ({self.expression}) AS "{self.outputCol}" FROM __THIS__'

        validate_column_name(self.outputCol)

        quoted_cols = [f'"{col}"' for col in existing_cols]
        all_cols = quoted_cols + [f"({self.expression}) AS \"{self.outputCol}\""]

        return f"SELECT {', '.join(all_cols)} FROM __THIS__"


class FilterByExpr(DuckDBTransformer):
    """Filter dataset by SQL expression.

    Only rows that satisfy the expression are kept.

    Attributes:
        expression: SQL boolean expression for filtering

    Example:
        >>> transformer = FilterByExpr(expression="status = 'active' AND amount > 0")
        >>> # Generates: SELECT * FROM __THIS__ WHERE status = 'active' AND amount > 0
    """

    expression: str
    kind: DuckDBClassName = DuckDBClassName.FILTER_BY_EXPR
    class_name: str = "seeknal.tasks.duckdb.transformers.FilterByExpr"
    description: Optional[str] = None
    params: dict = {}

    def model_post_init(self, __context):
        """Set params after initialization."""
        self.params = {"expression": self.expression}

    def to_sql(self) -> str:
        """Generate SQL for filtering."""
        return f"SELECT * FROM __THIS__ WHERE {self.expression}"


class SelectColumns(DuckDBTransformer):
    """Select specific columns from the dataset.

    Drops all other columns except the specified ones.

    Attributes:
        inputCols: List of column names to keep

    Example:
        >>> transformer = SelectColumns(inputCols=["user_id", "name", "email"])
        >>> # Generates: SELECT "user_id", "name", "email" FROM __THIS__
    """

    inputCols: List[str]
    kind: DuckDBClassName = DuckDBClassName.SELECT_COLUMNS
    class_name: str = "seeknal.tasks.duckdb.transformers.SelectColumns"
    description: Optional[str] = None
    params: dict = {}

    def model_post_init(self, __context):
        """Set params after initialization."""
        self.params = {"inputCols": self.inputCols}

    def to_sql(self) -> str:
        """Generate SQL for selecting columns."""
        for col in self.inputCols:
            validate_column_name(col)

        cols = ", ".join([f'"{col}"' for col in self.inputCols])
        return f"SELECT {cols} FROM __THIS__"


class DropCols(DuckDBTransformer):
    """Drop specific columns from the dataset.

    Keeps all columns except the specified ones.

    Attributes:
        inputCols: List of column names to drop

    Example:
        >>> transformer = DropCols(inputCols=["temp_col", "debug_info"])
        >>> # Drops temp_col and debug_info, keeps everything else
    """

    inputCols: List[str]
    kind: DuckDBClassName = DuckDBClassName.DROP_COLS
    class_name: str = "seeknal.tasks.duckdb.transformers.DropCols"
    description: Optional[str] = None
    params: dict = {}

    def model_post_init(self, __context):
        """Set params after initialization."""
        self.params = {"inputCols": self.inputCols}

    def to_sql(self, all_columns: List[str]) -> str:
        """Generate SQL for dropping columns.

        Args:
            all_columns: List of all column names in the dataset

        Returns:
            SQL SELECT statement excluding specified columns

        Raises:
            ValueError: If all_columns is None
        """
        if all_columns is None:
            raise ValueError("all_columns must be provided for DropCols")

        for col in self.inputCols:
            validate_column_name(col)

        keep_cols = [col for col in all_columns if col not in self.inputCols]

        if not keep_cols:
            raise ValueError(
                f"Dropping all columns is not allowed. Cannot drop {self.inputCols}"
            )

        cols = ", ".join([f'"{col}"' for col in keep_cols])
        return f"SELECT {cols} FROM __THIS__"
