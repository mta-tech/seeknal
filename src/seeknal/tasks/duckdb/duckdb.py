from typing import List, Optional, Union
from ..base import Task
from dataclasses import dataclass, field
import duckdb
from pyarrow import Table
import os
import pandas as pd

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
        >>> from seeknal.tasks.duckdb import DuckDBTask
        >>>
        >>> task = DuckDBTask(name="process_data")
        >>> result = task.add_input(path="data.parquet") \\
        ...              .add_sql("SELECT * FROM __THIS__ WHERE amount > 100") \\
        ...              .add_new_column("amount * 1.1", "adjusted") \\
        ...              .transform()
    """

    name: Optional[str] = None
    description: Optional[str] = None
    default_input_path: str = "."
    default_output_path: str = "."
    _materialize: bool = False
    is_spark_job: bool = False
    stages: List[dict] = field(default_factory=list)
    kind: str = "DuckDBTask"
    conn: Optional[duckdb.DuckDBPyConnection] = None

    def __post_init__(self):
        self.is_spark_job = False
        self.kind = "DuckDBTask"
        self.conn = duckdb.connect(database=":memory:")

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
            self.conn.sql(
                f"SET s3_secret_access_key='{os.getenv('S3_SECRET_ACCESS_KEY', '')}'"
            )

    def add_input(
        self,
        dataframe: Optional[Table] = None,
        path: Optional[str] = None,
        sql: Optional[str] = None,
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

    def add_common_yaml(self, common_yaml: str):
        """Add common YAML configuration (placeholder for compatibility)."""
        return self

    def add_sql(self, statement: str):
        """Add SQL transformation stage.

        Args:
            statement: SQL statement to execute

        Returns:
            DuckDBTask: self for method chaining

        Example:
            >>> task.add_sql("SELECT * FROM __THIS__ WHERE amount > 100")
        """
        from .transformers import SQL

        sql_obj = SQL(statement=statement)
        self.stages.append(
            {
                "type": "transformer",
                "class_name": sql_obj.class_name,
                "params": {
                    "statement": statement,
                    "kind": sql_obj.kind,
                    "class_name": sql_obj.class_name,
                    "description": sql_obj.description,
                },
            }
        )
        return self

    def add_new_column(self, expression: str, output_col: str):
        """Add computed column.

        Args:
            expression: SQL expression to compute the column
            output_col: Name of the new column

        Returns:
            DuckDBTask: self for method chaining

        Example:
            >>> task.add_new_column("amount * 1.1", "adjusted_amount")
        """
        from .transformers import AddColumnByExpr

        transformer = AddColumnByExpr(expression=expression, outputCol=output_col)
        self.stages.append(
            {
                "type": "transformer",
                "class_name": transformer.class_name,
                "params": {
                    "expression": expression,
                    "outputCol": output_col,
                    "kind": transformer.kind,
                    "class_name": transformer.class_name,
                    "description": transformer.description,
                },
            }
        )
        return self

    def add_filter_by_expr(self, expression: str):
        """Filter rows by expression.

        Args:
            expression: SQL boolean expression

        Returns:
            DuckDBTask: self for method chaining

        Example:
            >>> task.add_filter_by_expr("status = 'active' AND amount > 0")
        """
        from .transformers import FilterByExpr

        transformer = FilterByExpr(expression=expression)
        self.stages.append(
            {
                "type": "transformer",
                "class_name": transformer.class_name,
                "params": {
                    "expression": expression,
                    "kind": transformer.kind,
                    "class_name": transformer.class_name,
                    "description": transformer.description,
                },
            }
        )
        return self

    def select_columns(self, columns: List[str]):
        """Select specific columns.

        Args:
            columns: List of column names to keep

        Returns:
            DuckDBTask: self for method chaining

        Example:
            >>> task.select_columns(["user_id", "name", "email"])
        """
        from .transformers import SelectColumns

        transformer = SelectColumns(inputCols=columns)
        self.stages.append(
            {
                "type": "transformer",
                "class_name": transformer.class_name,
                "params": {
                    "inputCols": columns,
                    "kind": transformer.kind,
                    "class_name": transformer.class_name,
                    "description": transformer.description,
                },
            }
        )
        return self

    def drop_columns(self, columns: List[str]):
        """Drop columns.

        Args:
            columns: List of column names to drop

        Returns:
            DuckDBTask: self for method chaining

        Example:
            >>> task.drop_columns(["temp_col", "debug_info"])
        """
        from .transformers import DropCols

        transformer = DropCols(inputCols=columns)
        self.stages.append(
            {
                "type": "transformer",
                "class_name": transformer.class_name,
                "params": {
                    "inputCols": columns,
                    "kind": transformer.kind,
                    "class_name": transformer.class_name,
                    "description": transformer.description,
                },
            }
        )
        return self

    def add_stage(
        self,
        transformer: Optional["DuckDBTransformer"] = None,
        aggregator: Optional["DuckDBAggregator"] = None,
        id: Optional[str] = None,
        class_name: Optional[str] = None,
        params: Optional[dict] = None,
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
            self.stages.append(
                {
                    "type": "transformer",
                    "class_name": transformer.class_name,
                    "params": transformer.model_dump(),
                }
            )
        elif aggregator is not None:
            # Convert aggregator to stages (pre + agg + post)
            self.stages.extend(self._aggregator_to_stages(aggregator))
        elif id is not None:
            self.stages.append({"type": "id", "id": id})
        elif class_name is not None:
            if params is None:
                raise ValueError("params must be defined with class_name")
            self.stages.append(
                {"type": "transformer", "class_name": class_name, "params": params}
            )

        return self

    def _aggregator_to_stages(self, aggregator: "DuckDBAggregator") -> List[dict]:
        """Convert aggregator to list of stage dictionaries.

        An aggregator becomes:
        1. Pre-stages (transformations before aggregation)
        2. Aggregation stage (GROUP BY)
        3. Post-stages (transformations after aggregation)
        """
        from .transformers import SQL, ColumnRenamed, AddColumnByExpr

        stages = []

        # Add pre-stages
        if aggregator.pre_stages:
            for stage in aggregator.pre_stages:
                stages.append(
                    {"type": "transformer", "class_name": stage.class_name, "params": stage.params}
                )

        # Build aggregation SQL
        select_parts = []

        # Add group by columns
        for col in aggregator.group_by_cols:
            select_parts.append(f'"{col}"')

        # Add aggregations
        for agg_func in aggregator.aggregators:
            select_parts.append(agg_func.to_sql())

        # Build GROUP BY clause
        group_by = ", ".join([f'"{col}"' for col in aggregator.group_by_cols])

        agg_sql = f"SELECT {', '.join(select_parts)} FROM __THIS__ GROUP BY {group_by}"

        sql_obj = SQL(statement=agg_sql)
        stages.append(
            {
                "type": "transformer",
                "class_name": sql_obj.class_name,
                "params": {
                    "statement": agg_sql,
                    "kind": sql_obj.kind,
                    "class_name": sql_obj.class_name,
                    "description": sql_obj.description,
                },
            }
        )

        # Add post-stages
        if aggregator.post_stages:
            for stage in aggregator.post_stages:
                stages.append(
                    {"type": "transformer", "class_name": stage.class_name, "params": stage.params}
                )

        # Add column renames
        if aggregator.renamed_cols:
            for rename in aggregator.renamed_cols:
                from .transformers import ColumnRenamed
                rename_trans = ColumnRenamed(inputCol=rename.name, outputCol=rename.newName)
                stages.append(
                    {
                        "type": "transformer",
                        "class_name": rename_trans.class_name,
                        "params": {
                            "inputCol": rename.name,
                            "outputCol": rename.newName,
                            "kind": rename_trans.kind,
                            "class_name": rename_trans.class_name,
                            "description": rename_trans.description,
                        },
                    }
                )

        # Add column expressions
        if aggregator.col_by_expression:
            for expr in aggregator.col_by_expression:
                from .transformers import AddColumnByExpr
                expr_trans = AddColumnByExpr(expression=expr.expression, outputCol=expr.newName)
                stages.append(
                    {
                        "type": "transformer",
                        "class_name": expr_trans.class_name,
                        "params": {
                            "expression": expr.expression,
                            "outputCol": expr.newName,
                            "kind": expr_trans.kind,
                            "class_name": expr_trans.class_name,
                            "description": expr_trans.description,
                        },
                    }
                )

        return stages

    def _load_input(self) -> duckdb.DuckDBPyRelation:
        """Load input data into DuckDB relation."""
        if "dataframe" in self.input:
            # Register Arrow table directly
            table_name = "_input_table"
            self.conn.register(table_name, self.input["dataframe"])
            return self.conn.table(table_name)
        elif "path" in self.input:
            # Load from path and register as table
            table_name = "_input_table"
            rel = self.conn.sql(f"SELECT * FROM '{self.input['path']}'")
            self.conn.register(table_name, rel.arrow())
            return self.conn.table(table_name)
        elif "sql" in self.input:
            # Execute SQL and register as table
            table_name = "_input_table"
            rel = self.conn.sql(self.input["sql"])
            self.conn.register(table_name, rel.arrow())
            return self.conn.table(table_name)
        else:
            raise ValueError("Invalid input configuration")

    def _instantiate_stage(self, stage: dict):
        """Instantiate transformer from stage dict."""
        from .transformers import (
            SQL, FilterByExpr, AddColumnByExpr, SelectColumns, DropCols,
            JoinTablesByExpr, PointInTime, CastColumn, ColumnRenamed, AddWindowFunction
        )
        from .transformers import WindowFunction as WindowFunctionEnum

        class_name = stage["params"].get("class_name", stage.get("class_name", ""))

        # Handle different stage formats
        params = stage.get("params", {})

        # Check if params is nested
        if "params" in params and "class_name" not in params:
            params = params["params"]

        # Simple transformers
        if "SQL" in class_name or "statement" in params:
            return SQL(statement=params.get("statement", ""))
        elif "ColumnRenamed" in class_name:
            return ColumnRenamed(
                inputCol=params.get("inputCol", ""),
                outputCol=params.get("outputCol", "")
            )
        elif "FilterByExpr" in class_name or ("expression" in params and "inputCol" not in params and "outputCol" not in params):
            return FilterByExpr(expression=params.get("expression", ""))
        elif "AddColumnByExpr" in class_name:
            return AddColumnByExpr(
                expression=params.get("expression", ""), outputCol=params.get("outputCol", "")
            )
        elif "SelectColumns" in class_name:
            return SelectColumns(inputCols=params.get("inputCols", []))
        elif "DropCols" in class_name:
            # DropCols needs to know all columns - we'll handle it specially in transform
            return DropCols(inputCols=params.get("inputCols", []))
        elif "JoinTablesByExpr" in class_name:
            return JoinTablesByExpr(
                select_stm=params.get("select_stm", "*"),
                alias=params.get("alias", "a"),
                tables=params.get("tables", []),
            )
        elif "PointInTime" in class_name:
            return PointInTime(
                how=params.get("how", "past"),
                offset=params.get("offset", 0),
                length=params.get("length"),
                feature_date=params.get("feature_date", "event_time"),
                feature_date_format=params.get("feature_date_format", "yyyy-MM-dd"),
                app_date=params.get("app_date"),
                app_date_format=params.get("app_date_format", "yyyy-MM-dd"),
                broadcast=params.get("broadcast", False),
                spine=params.get("spine"),
                join_type=params.get("join_type", "INNER JOIN"),
                col_id=params.get("col_id", "id"),
                spine_col_id=params.get("spine_col_id", "id"),
                keep_cols=params.get("keep_cols"),
            )
        elif "CastColumn" in class_name:
            return CastColumn(
                inputCol=params.get("inputCol"),
                outputCol=params.get("outputCol"),
                dataType=params.get("dataType"),
            )
        elif "AddWindowFunction" in class_name:
            # Convert window function string to enum
            wf = params.get("windowFunction")
            if isinstance(wf, str):
                wf = WindowFunctionEnum(wf)

            return AddWindowFunction(
                inputCol=params.get("inputCol"),
                offset=params.get("offset"),
                windowFunction=wf,
                partitionCols=params.get("partitionCols", []),
                orderCols=params.get("orderCols"),
                ascending=params.get("ascending", False),
                outputCol=params.get("outputCol"),
                expression=params.get("expression"),
            )

        # Default to SQL for raw statements
        if "statement" in params:
            return SQL(statement=params["statement"])

        raise NotImplementedError(f"Transformer {class_name} not yet implemented")

    def transform(
        self,
        spark=None,
        chain: bool = True,
        materialize: bool = False,
        params=None,
        filters=None,
        date=None,
        start_date=None,
        end_date=None,
    ) -> Union[Table, "DuckDBTask"]:
        """Execute the transformation pipeline.

        Args:
            spark: Ignored (for API compatibility with SparkEngineTask)
            chain: If True, chain stages sequentially
            materialize: If True, return self for further operations
            params: Optional parameters for transformations
                - return_as_pandas: bool, return pandas DataFrame instead of PyArrow
            filters: Optional filters (not yet implemented)
            date: Optional single date filter (not yet implemented)
            start_date: Optional start date filter (not yet implemented)
            end_date: Optional end date filter (not yet implemented)

        Returns:
            PyArrow Table if materialize=False
            DuckDBTask if materialize=True
            Pandas DataFrame if params['return_as_pandas']=True
        """
        # 1. Load input data
        current_rel = self._load_input()

        # 2. Execute stages sequentially using CTEs
        for i, stage in enumerate(self.stages):
            transformer = self._instantiate_stage(stage)
            
            # Special handling for DropCols which needs all columns
            from .transformers import DropCols
            if isinstance(transformer, DropCols):
                # Get current column names
                if i == 0:
                    # First stage - get columns from input
                    all_cols = list(self.conn.table("_input_table").arrow().read_all().column_names)
                else:
                    # Subsequent stages - get columns from previous result
                    all_cols = list(self.conn.table(f"_result_{i}").arrow().read_all().column_names)
                
                sql = transformer.to_sql(all_cols)
            else:
                sql = transformer.to_sql()

            # Replace __THIS__ with the actual relation or subquery
            if i == 0:
                # First stage, use _input_table directly
                if "__THIS__" in sql:
                    sql = sql.replace("__THIS__", "_input_table")
            else:
                # Subsequent stages, wrap previous in CTE
                if "__THIS__" in sql:
                    sql = sql.replace("__THIS__", f"_result_{i}")

            # Execute SQL
            current_rel = self.conn.sql(sql)

            # Register result for next stage
            result_name = f"_result_{i+1}"
            # Force execution and get Arrow table (use read_all())
            arrow_reader = current_rel.arrow()
            arrow_result = arrow_reader.read_all()
            self.conn.register(result_name, arrow_result)

        # 3. Return result
        if materialize:
            self._materialize = True
            return self
        else:
            # Get final result as Arrow table
            arrow_reader = current_rel.arrow()
            final_result = arrow_reader.read_all()

            # Check if user wants pandas DataFrame
            if params is not None and params.get("return_as_pandas") is True:
                return final_result.to_pandas()

            return final_result
