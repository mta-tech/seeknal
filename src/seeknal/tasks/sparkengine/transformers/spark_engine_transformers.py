from enum import Enum
from typing import List, Optional, Union
import uuid

from pydantic import model_validator, field_validator

from . import Transformer, ClassName
from .base_transformer import _init_transformer
from pyspark.sql import DataFrame
from dataclasses import dataclass, field

module_name = "tech.mta.seeknal.transformers."


class ColumnRenamed(Transformer):
    """
    Provide new renamed column from a column
    Attributes:
        inputCol: target column
        outputCol: new column name
    """

    inputCol: str
    outputCol: str

    def model_post_init(self, __context):
        params = _init_transformer(
            kind=ClassName.COLUMN_RENAMED,
            description=self.description,
            **{"inputCol": self.inputCol, "outputCol": self.outputCol},
        )
        object.__setattr__(self, 'params', params["params"])
        object.__setattr__(self, 'class_name', params["class_name"])
        object.__setattr__(self, 'kind', params["kind"])
        object.__setattr__(self, 'description', params["description"])


class SQL(Transformer):
    """
    Implements the transformations which are defined by SQL statement.

    Attributes:
        statement (str): SQL statement. __THIS__ keyword to refer current input.
    """

    statement: str

    def model_post_init(self, __context):
        params = _init_transformer(
            kind=ClassName.SQL,
            description=self.description,
            **{"statement": self.statement}
        )
        object.__setattr__(self, 'params', params["params"])
        object.__setattr__(self, 'class_name', params["class_name"])
        object.__setattr__(self, 'kind', params["kind"])
        object.__setattr__(self, 'description', params["description"])


class AddColumnByExpr(Transformer):
    """
    Add new column by specifying expression to create the column

    Attributes:
        expression (str): expression for creating new column
        outputCol (str): result column name
    """

    expression: str
    outputCol: str

    def model_post_init(self, __context):
        params = _init_transformer(
            kind=ClassName.ADD_COLUMN_BY_EXPR,
            description=self.description,
            **{"expression": self.expression, "outputCol": self.outputCol},
        )
        object.__setattr__(self, 'params', params["params"])
        object.__setattr__(self, 'class_name', params["class_name"])
        object.__setattr__(self, 'kind', params["kind"])
        object.__setattr__(self, 'description', params["description"])


class WindowFunction(str, Enum):
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
    LAST_DISTINCT = "last_distinct"


class AddWindowFunction(Transformer):
    """
    Add new column by Window function.
    
    Attributes:
        inputCol (str): Input column
        offset (str, optional): Offset supplied to the window function. Only applies for ntile, lag, lead
        windowFunction (WindowFunction): Configured window function
        partitionCols (List[str]): Partition columns
        orderCols (List[str], optional): Columns to order the partition window
        ascending (bool, optional): If set to true, use ascending order for orderCols. Default to False
        outputCol (str): Output column name
        expression (str, optional): If last_distinct window function configured, define value expression.
    """

    inputCol: str
    offset: Optional[str] = None
    windowFunction: WindowFunction
    partitionCols: List[str]
    orderCols: Optional[List[str]] = None
    ascending: bool = False
    outputCol: str
    expression: Optional[str] = None

    def model_post_init(self, __context):
        transformer_params = {
            "inputCol": self.inputCol,
            "windowFunction": self.windowFunction.value,
            "partitionCols": self.partitionCols,
            "outputCol": self.outputCol,
        }
        
        if self.offset is not None:
            transformer_params["offset"] = self.offset
        if self.orderCols is not None:
            transformer_params["orderCols"] = self.orderCols
        if self.ascending:
            transformer_params["ascending"] = self.ascending
        if self.expression is not None:
            transformer_params["expression"] = self.expression
        params = _init_transformer(
            kind=ClassName.ADD_WINDOW_FUNCTION,
            description=self.description,
            **transformer_params,
        )
        object.__setattr__(self, 'params', params["params"])
        object.__setattr__(self, 'class_name', params["class_name"])
        object.__setattr__(self, 'kind', params["kind"])
        object.__setattr__(self, 'description', params["description"])


class Time(str, Enum):
    NOW = "now"
    FUTURE = "future"
    PAST = "past"


class PointInTime(SQL):
    """
    Perform time-aware join of features with respect to application dates defined in a seed table.
    """

    how: Time = Time.PAST
    offset: int = 0
    length: Optional[int] = None
    feature_date: str = "event_time"
    app_date: str
    feature_date_format: str = "yyyy-MM-dd"
    app_date_format: str = "yyyy-MM-dd"
    broadcast: bool = False
    spine: Union[str, DataFrame]
    join_type: str = "INNER JOIN"
    col_id: str = "id"
    spine_col_id: str = "id"
    keep_cols: Optional[List[str]] = None
    statement: str = ""

    def model_post_init(self, __context):
        if isinstance(self.spine, DataFrame):
            random_name = "f" + str(uuid.uuid4())[:8]
            self.spine.createOrReplaceTempView(f"table_{random_name}")
            spine_name = f"table_{random_name}"
            object.__setattr__(self, 'spine', spine_name)
        statement = self._construct_statement(self.model_dump())
        params = _init_transformer(
            kind=ClassName.SQL,
            description=self.description,
            **{"statement": statement},
        )
        object.__setattr__(self, 'params', params["params"])
        object.__setattr__(self, 'class_name', params["class_name"])
        object.__setattr__(self, 'kind', params["kind"])
        object.__setattr__(self, 'description', params["description"])

    @staticmethod
    def _construct_statement(values: dict):
        """
        construct sql statement
        """
        selector = ["a.*"]
        where_spec_template = "(1=1)"
        if values["keep_cols"] is not None:
            selector.extend(values["keep_cols"])

        broadcast_hint = " /*+ BROADCAST(b) */ " if values["broadcast"] else ""

        comparator_op = values["how"]
        app_date_spec_template = "date_format(date_sub(from_unixtime(unix_timestamp(b.{app_date}, '{app_date_format}'), 'yyyy-MM-dd'), {offset_spec}), '{feature_date_format}')"  # noqa
        app_date_spec = app_date_spec_template.format(
            offset_spec=values["offset"],
            app_date=values["app_date"],
            app_date_format=values["app_date_format"],
            feature_date_format=values["feature_date_format"],
        )
        where_spec = where_spec_template
        if values["how"] == Time.NOW:
            comparator_op = "="
        elif values["how"] == Time.PAST:
            comparator_op = "<="
            if values["length"]:
                limit_expr = app_date_spec_template.format(
                    offset_spec="({}+{}-1)".format(values["offset"], values["length"]),
                    app_date=values["app_date"],
                    app_date_format=values["app_date_format"],
                    feature_date_format=values["feature_date_format"],
                )
                limit_expr_2 = "(a.{} >= {})".format(values["feature_date"], limit_expr)
                where_spec = "{} AND {}".format(where_spec_template, limit_expr_2)

        elif values["how"] == Time.FUTURE:
            comparator_op = ">="

            if values["length"]:
                limit_expr = app_date_spec_template.format(
                    offset_spec="{}-({}-1)".format(values["offset"], values["length"]),
                    app_date=values["app_date"],
                    app_date_format=values["app_date_format"],
                    feature_date_format=values["feature_date_format"],
                )
                limit_expr_2 = "(a.{} <= {})".format(values["feature_date"], limit_expr)
                where_spec = "{} AND {}".format(where_spec_template, limit_expr_2)

        query = """SELECT {broadcast_hint} {selector}
        FROM __THIS__ a
        {join_type}
        {seed_spec} b
        ON a.{col_id}=b.{spine_col_id} AND a.{feature_date} {comparator_op} {app_date_spec}
        WHERE
        {where_spec}""".format(
            selector=",".join(selector),
            seed_spec=values["spine"],
            spine_col_id=values["spine_col_id"],
            col_id=values["col_id"],
            app_date_spec=app_date_spec,
            comparator_op=comparator_op,
            where_spec=where_spec,
            broadcast_hint=broadcast_hint,
            feature_date=values["feature_date"],
            join_type=values["join_type"],
        )
        return query


class FilterByExpr(Transformer):
    """
    Filter by expression

    Attributes:
        expression (str): expression for filtering
    """

    expression: str

    def model_post_init(self, __context):
        params = _init_transformer(
            kind=ClassName.FILTER_BY_EXPR,
            description=self.description,
            **{"expression": self.expression},
        )
        object.__setattr__(self, 'params', params["params"])
        object.__setattr__(self, 'class_name', params["class_name"])
        object.__setattr__(self, 'kind', params["kind"])
        object.__setattr__(self, 'description', params["description"])


class JoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    LEFT_OUTER = "left_outer"
    RIGHT_OUTER = "right_outer"
    LEFT_SEMI = "left_semi"
    LEFT_ANTI = "left_anti"
    CROSS = "cross"


@dataclass
class TableJoinDef:
    table: Union[str, DataFrame]
    alias: str
    joinType: Union[str, Enum]
    joinExpression: str


class JoinTablesByExpr(SQL):
    """
    Join by expression

    Attributes:
        select_stm (str): Select statement. Defaults to "*"
        alias (str): Alias for the main table. Defaults to "a"
        tables (List[TableJoinDef]): List of tables to join with their join definitions
    """

    select_stm: str = "*"
    alias: str = "a"
    tables: List[TableJoinDef]
    statement: str = ""

    @staticmethod
    def _construct_stm(select_stm: str, alias: str, tables: List[TableJoinDef]):
        join_stm = ""
        for i in tables:
            if isinstance(i.joinType, JoinType):
                i.joinType = i.joinType.value
            if isinstance(i.table, DataFrame):
                random_name = "f" + str(uuid.uuid4())[:8]
                i.table.createOrReplaceTempView(f"table_{random_name}")
                i.table = f"table_{random_name}"

            join_stm += f"{i.joinType} JOIN {i.table} {i.alias} ON {i.joinExpression} "

        query = """SELECT {select_stm} FROM __THIS__ {alias} {join_stm}""".format(
            select_stm=select_stm, alias=alias, join_stm=join_stm
        )
        return query

    def model_post_init(self, __context):

        statement = self._construct_stm(self.select_stm, self.alias, self.tables)
        object.__setattr__(self, 'statement', statement)
        params = _init_transformer(
            kind=ClassName.SQL,
            description=self.description,
            **{"statement": statement},
        )
        object.__setattr__(self, 'params', params["params"])
        object.__setattr__(self, 'class_name', params["class_name"])
        object.__setattr__(self, 'kind', params["kind"])
        object.__setattr__(self, 'description', params["description"])
