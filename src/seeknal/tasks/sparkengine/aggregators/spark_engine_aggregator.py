from enum import Enum
from typing import Optional

from pydantic import model_validator

from . import AggregatorFunction


class AggregateValueType(str, Enum):
    STRING = "string"
    INT = "int"
    LONG = "log"
    DOUBLE = "double"
    FLOAT = "float"


module_name = "tech.mta.seeknal.aggregators."


class FunctionAggregator(AggregatorFunction):
    """
    Aggregate by providing Spark aggregate function

    Attributes:
        inputCol (str): column to be aggregated
        outputCol (str): result output column name
        accumulatorFunction (str): aggregation function to be applied.
            Configured function must an eligble Spark function.
        defaultAggregateValue (str, optional): Default value if value produced Null
        defaultAggregateValueType (AggregateValueType, optional): Result data type

    """

    inputCol: str
    outputCol: str
    accumulatorFunction: str
    defaultAggregateValue: Optional[str] = None
    defaultAggregateValueType: Optional[AggregateValueType] = None

    @model_validator(mode='after')
    def _set_fields(self) -> 'FunctionAggregator':
        params = {}

        params["inputCol"] = self.inputCol
        params["outputCol"] = self.outputCol
        params["accumulatorFunction"] = self.accumulatorFunction
        if self.defaultAggregateValue is not None:
            params["defaultAggregateValue"] = self.defaultAggregateValue
        if self.defaultAggregateValueType is not None:
            params["defaultAggregateValueType"] = self.defaultAggregateValueType.value

        self.params = params
        self.class_name = module_name + "FunctionAggregator"
        return self


class ExpressionAggregator(AggregatorFunction):
    """
    Aggregate by providing an expression

    Attributes:
        expression: SQL statement for aggregation
        outputCol: Result output column name
    """

    expression: str
    outputCol: str

    @model_validator(mode='after')
    def _set_fields(self) -> 'ExpressionAggregator':
        self.params = {
            "outputCol": self.outputCol,
            "expression": self.expression,
        }
        self.class_name = module_name + "ExpressionAggregator"
        return self


class DayTypeAggregator(AggregatorFunction):
    """
    Aggregate by providing function and day type (wkend or wkday)
    Attributes:
        inputCol (str): column to be aggregated
        outputCol (str): result output column name
        accumulatorFunction (str): aggregation function to be applied.
            Configured function must an eligble Spark function.
        weekdayCol (str): column that contains the day is wkend or wkday (true/false)
        defaultAggregateValue (str, optional): Default value if value produced Null
        defaultAggregateValueType (AggregateValueType, optional): Result data type
    """

    class_name: str = module_name + "DayTypeAggregator"
    inputCol: str
    outputCol: str
    accumulatorFunction: str
    weekdayCol: str
    defaultAggregateValue: Optional[str] = None
    defaultAggregateValueType: Optional[AggregateValueType] = None

    @model_validator(mode='after')
    def _set_fields(self) -> 'DayTypeAggregator':
        params = {}

        params["inputCol"] = self.inputCol
        params["outputCol"] = self.outputCol
        params["accumulatorFunction"] = self.accumulatorFunction
        params["weekdayCol"] = self.weekdayCol
        if self.defaultAggregateValue is not None:
            params["defaultAggregateValue"] = self.defaultAggregateValue
        if self.defaultAggregateValueType is not None:
            params["defaultAggregateValueType"] = self.defaultAggregateValueType.value

        self.params = params
        self.class_name = module_name + "DayTypeAggregator"

        return self
