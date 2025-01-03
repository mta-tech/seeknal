from typing import List, Optional
from enum import Enum
from pydantic import BaseModel

feat_module_name = "tech.mta.seeknal.transformers."


class ClassName(str, Enum):
    FILTER_BY_EXPR = feat_module_name + "FilterByExpr"
    ADD_DATE = feat_module_name + "AddDate"
    COLUMN_RENAMED = feat_module_name + "ColumnRenamed"
    ADD_COLUMN_BY_EXPR = feat_module_name + "AddColumnByExpr"
    SQL = feat_module_name + "SQL"
    ADD_WINDOW_FUNCTION = feat_module_name + "AddWindowFunction"
    ADD_DATE_DIFFERENCE = feat_module_name + "AddDateDifference"
    ADD_DAY_TYPE = feat_module_name + "AddDayType"
    ADD_ENTROPY = feat_module_name + "AddEntropy"
    ADD_DISTANCE = feat_module_name + "AddLatLongDistance"
    ADD_WEEK = feat_module_name + "AddWeek"
    CAST_COLUMN = feat_module_name + "CastColumn"
    COALESCE = feat_module_name + "Coalesce"
    COLUMN_VALUE_RENAMED = feat_module_name + "ColumnValueRenamed"
    FILTER_BY_COLUMN_VALUE = feat_module_name + "FilterByColumnValue"
    JOIN_BY_EXPR = feat_module_name + "JoinByExpr"
    JOIN_BY_ID = feat_module_name + "JoinById"
    REGEX_FILTER = feat_module_name + "RegexFilter"
    SELECT_COLUMNS = feat_module_name + "SelectColumns"
    UNION_TABLE = feat_module_name + "UnionTable"
    FILTER_BY_IS_IN = feat_module_name + "FilterByIsIn"
    DROP_COLS = feat_module_name + "DropCols"
    MELT = feat_module_name + "Melt"
    STRUCT_ASSEMBLER = feat_module_name + "StructAssembler"


def _init_transformer(
    kind: ClassName, description: Optional[str] = None, **kwargs
):
    new_params = {
        "kind": kind,
        "class_name": kind.value,
        "params": kwargs,
        "transformation_id": None,
        "description": description,
    }
    return new_params


class Transformer(BaseModel):
    kind: Optional[ClassName] = None
    class_name: str = ""
    params: dict = {}
    transformation_id: Optional[str] = None
    description: Optional[str] = None

    def __init__(self, *args, **kwargs):
        if args:
            if len(args) > 1:
                raise Exception("Only one positional argument is allowed")
            if isinstance(args[0], ClassName) == False:
                raise Exception(
                    "Expecting ClassName as positional argument"
                )
            super().__init__(
                kind=args[0],
                class_name=args[0].value,
                params=kwargs,
                transformation_id=None,
                description=None,
            )
        else:
            super().__init__(**kwargs)

    def update_param(self, key: str, value):
        """
        Update parameter

        Args:
            key (str): name of parameter
            value (Any): new value of parameter
        """
        self.params[key] = value
        return self

    def update_description(self, description: str):
        """
        Update description

        Args:
            description (str): description
        """
        self.description = description
        return self

    model_config = {
        "frozen": True,
        "arbitrary_types_allowed": True
    }


class FeatureTransformer(BaseModel):
    """
    Base FeatureTransformer
    """

    name: str
    frequency: str
    params: Optional[dict] = None
    subFeatures: Optional[dict] = None
