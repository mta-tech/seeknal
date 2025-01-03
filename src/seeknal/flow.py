import copy
import importlib
import json
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, List, Optional, Union

import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
import typer
import yaml
from prefect import flow, task
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from tabulate import tabulate

from .common_artifact import Common, Source
from .context import check_project_id, context, logger, require_project
from .request import FlowRequest
from .tasks.base import Task
from .tasks.duckdb import DuckDBTask
from .tasks.sparkengine import SparkEngineTask
from .tasks.sparkengine.extractors import Extractor
from .utils import to_snake
from .workspace import require_workspace


class FlowOutputEnum(str, Enum):
    SPARK_DATAFRAME = "spark_dataframe"
    ARROW_DATAFRAME = "arrow_dataframe"
    PANDAS_DATAFRAME = "pandas_dataframe"
    HIVE_TABLE = "hive_table"
    PARQUET = "parquet"
    LOADER = "loader"
    FEATURE_GROUP = "feature_group"
    FEATURE_SERVING = "feature_serving"


class FlowInputEnum(str, Enum):
    HIVE_TABLE = "hive_table"
    PARQUET = "parquet"
    FEATURE_GROUP = "feature_group"
    EXTRACTOR = "extractor"
    SOURCE = "source"


@dataclass
class FlowInput:
    value: Optional[Union[str, dict, Extractor]] = None
    kind: FlowInputEnum = FlowInputEnum.HIVE_TABLE

    def __call__(self, spark: Optional[SparkSession] = None):
        match self.kind:
            case FlowInputEnum.HIVE_TABLE:
                if not isinstance(self.value, str):
                    raise ValueError("Hive table input must be a string")
                if spark is not None:
                    return spark.read.table(self.value)
            case FlowInputEnum.PARQUET:
                if not isinstance(self.value, str):
                    raise ValueError("Parquet input must be a string")
                if spark is not None:
                    return spark.read.parquet(self.value)
                else:
                    return pq.read_table(self.value)
            case FlowInputEnum.EXTRACTOR:
                if not isinstance(self.value, Extractor):
                    raise ValueError("Extractor input must be a Extractor class")
                return SparkEngineTask().add_input(extractor=self.value).transform()
            case FlowInputEnum.SOURCE:
                if isinstance(self.value, str):
                    source_id = self.value
                elif isinstance(self.value, Source):
                    source_id = self.value.name
                else:
                    raise ValueError("Source input must be a string or Source class")

                return (
                    SparkEngineTask()
                    .add_common_yaml(Common().as_yaml())
                    .add_input(id=source_id)
                    .transform()
                )
            case FlowInputEnum.FEATURE_GROUP:
                raise NotImplementedError("Feature group input not implemented yet")
            case _:
                raise ValueError("Invalid input kind")

        return self


@dataclass
class FlowOutput:
    value: Optional[Any] = None
    kind: Optional[FlowOutputEnum] = None

    def __call__(
        self, result: Union[DataFrame, pa.Table], spark: Optional[SparkSession] = None
    ):
        match self.kind:
            case FlowOutputEnum.SPARK_DATAFRAME:
                if not isinstance(result, DataFrame):
                    # try convert to spark dataframe
                    return spark.createDataFrame(result.to_pandas())
                return result
            case FlowOutputEnum.ARROW_DATAFRAME:
                if not isinstance(result, pa.Table):
                    # try convert to arrow dataframe
                    if isinstance(self, DataFrame):
                        _temp_data = result.toPandas()
                        return pa.Table.from_pandas(_temp_data)
                return result
            case FlowOutputEnum.PANDAS_DATAFRAME:
                if not isinstance(result, pa.Table):
                    # try convert to pandas dataframe
                    if isinstance(self, DataFrame):
                        return result.toPandas()
                else:
                    return result.to_pandas()
            case FlowOutputEnum.HIVE_TABLE:
                if not isinstance(result, DataFrame):
                    # try convert to spark dataframe
                    _temp_data = spark.createDataFrame(result)
                    _temp_data.write.saveAsTable(self.value)
                else:
                    result.write.saveAsTable(self.value)
                return None
            case FlowOutputEnum.PARQUET:
                if not isinstance(result, DataFrame):
                    # try convert to spark dataframe
                    _temp_data = spark.createDataFrame(result)
                    _temp_data.write.parquet(self.value)
                else:
                    result.write.parquet(self.value)
                return None
            case FlowOutputEnum.LOADER:
                _temp_data = result
                if not isinstance(result, DataFrame):
                    # try convert to spark dataframe
                    _temp_data = spark.createDataFrame(result)

                SparkEngineTask().add_input(dataframe=_temp_data).add_output(
                    loader=self.value
                ).transform(materialize=True)
                return None
            case None:
                return result
        return None


VALID_SPARK_INPUT = [
    FlowInputEnum.HIVE_TABLE,
    FlowInputEnum.PARQUET,
    FlowInputEnum.FEATURE_GROUP,
    FlowInputEnum.EXTRACTOR,
]


@dataclass
class Flow:
    name: str
    input: Optional[FlowInput] = None
    input_date_col: Optional[dict] = None
    tasks: Optional[List[Task]] = None
    output: Optional[FlowOutput] = None
    description: str = ""

    def __post_init__(self):
        self.name = to_snake(self.name)

    def require_saved(func):
        def wrapper(self, *args, **kwargs):
            if not "flow_id" in vars(self):
                raise ValueError("flow not loaded or saved")
            else:
                func(self, *args, **kwargs)

        return wrapper

    def set_input_date_col(self, date_col: str, date_pattern: str = "yyyyMMdd"):
        self.input_date_col = {
            "dateCol": date_col,
            "datePattern": date_pattern,
        }
        return self

    def run(self, params=None, filters=None, date=None, start_date=None, end_date=None):
        # check whether at least one task is a spark job
        has_spark_job = False
        if (self.tasks is not None) or (self.input.kind in VALID_SPARK_INPUT):
            if self.tasks is not None:
                for task in self.tasks:
                    if task.is_spark_job:
                        has_spark_job = True
                        break
            if self.input.kind in VALID_SPARK_INPUT:
                has_spark_job = True
            if has_spark_job:
                spark = SparkSession.builder.getOrCreate()
            else:
                spark = None
        else:
            spark = None
        # taking care the input
        # load data and applying filters
        flow_input = self.input(spark)
        if isinstance(flow_input, DataFrame):
            pre_filter = (
                SparkEngineTask()
                .add_input(dataframe=flow_input)
                .add_common_yaml(Common().as_yaml())
            )
            if self.input_date_col is not None:
                pre_filter.input["params"] = {
                    "dateCol": self.input_date_col["dateCol"],
                    "datePattern": self.input_date_col["datePattern"],
                }
            flow_input = pre_filter.transform(
                spark,
                chain=True,
                materialize=False,
                params=params,
                filters=filters,
                date=date,
                start_date=start_date,
                end_date=end_date,
            )
        elif isinstance(flow_input, pa.Table):
            # todo: implement filters for arrow dataframe
            pass
        else:
            pass

        if self.tasks is None:
            return self.output(flow_input, spark)
        else:
            first_task = self.tasks[0]
            if first_task.is_spark_job:
                if not isinstance(flow_input, DataFrame):
                    flow_input = spark.createDataFrame(flow_input)
            first_task.add_input(dataframe=flow_input).add_common_yaml(
                Common().as_yaml()
            )
            temp_data = first_task.transform(
                spark, chain=True, materialize=False, params=params
            )
            if len(self.tasks) > 1:
                for task in self.tasks[1:]:
                    if task.is_spark_job:
                        if not isinstance(temp_data, DataFrame):
                            temp_data = spark.createDataFrame(temp_data)
                    else:
                        if isinstance(temp_data, DataFrame):
                            _temp_data = temp_data.toPandas()
                            temp_data = pa.Table.from_pandas(_temp_data)
                    new_task = task.add_input(dataframe=temp_data).add_common_yaml(
                        Common().as_yaml()
                    )
                    temp_data = new_task.transform(
                        spark, chain=True, materialize=False, params=params
                    )
            return self.output(temp_data, spark)

    def as_dict(self):
        # removing any reference to spark context
        if self.tasks is not None:
            for task in self.tasks:
                task.input = None
        flow_dict = asdict(self)
        if "input" in flow_dict:
            if flow_dict["input"] is not None:
                if flow_dict["input"]["kind"] is not None:
                    flow_dict["input"]["kind"] = flow_dict["input"]["kind"].value
        if "output" in flow_dict:
            if flow_dict["output"] is not None:
                if flow_dict["output"]["kind"] is not None:
                    flow_dict["output"]["kind"] = flow_dict["output"]["kind"].value
        if "tasks" in flow_dict:
            if flow_dict["tasks"] is not None:
                for index, task in enumerate(flow_dict["tasks"]):
                    task["class_name"] = (
                        self.tasks[index].__module__
                        + "."
                        + type(self.tasks[index]).__name__
                    )
        return flow_dict

    def as_yaml(self):
        flow_dict = self.as_dict()
        return yaml.dump(flow_dict)

    def __str__(self):
        return str(self.as_dict())

    @staticmethod
    def from_dict(flow_dict: dict):
        if "input" in flow_dict:
            flow_input = FlowInput(**flow_dict["input"])
            flow_input_enum = FlowInputEnum(flow_input.kind)
            flow_input.kind = flow_input_enum
        else:
            flow_input = FlowInput()

        if "output" in flow_dict:
            flow_output = FlowOutput(**flow_dict["output"])
            if flow_output.kind is not None:
                flow_output_enum = FlowOutputEnum(flow_output.kind)
                flow_output.kind = flow_output_enum
        else:
            flow_output = FlowOutput()
        arr_tasks = []
        if "tasks" in flow_dict:
            if flow_dict["tasks"] is not None:
                for i in flow_dict["tasks"]:
                    if not "class_name" in i:
                        raise ValueError("Cannot identify task class")
                    class_array = i["class_name"].split(".")
                    class_name = class_array[-1]
                    module_name = ".".join(class_array[0:-1])
                    ParamClass = getattr(
                        importlib.import_module(module_name), class_name
                    )
                    i.pop("class_name")
                    task = ParamClass(**i)
                    arr_tasks.append(task)
        if arr_tasks == []:
            arr_tasks = None

        flow = Flow(
            input=flow_input,
            tasks=arr_tasks,
            output=flow_output,
            name=flow_dict["name"],
        )
        return flow

    @require_workspace
    @require_project
    def get_or_create(self):
        req = FlowRequest(
            body={
                "spec": self.as_dict(),
                "name": self.name,
                "description": self.description,
            }
        )
        flow = req.select_by_name(self.name)
        if flow is None:
            self.flow_id = req.save()
        else:
            logger.warning("Using an existing Flow.")
            my_flow = Flow.from_dict(json.loads(flow.spec))
            self.__dict__.update(my_flow.__dict__)
            self.flow_id = flow.id
        return self

    @require_workspace
    @staticmethod
    def list():
        check_project_id()
        flows = FlowRequest.select_by_project_id(context.project_id)
        if flows:
            flows = [
                {
                    "name": flow.name,
                    "description": flow.description,
                    "flow_spec": yaml.dump(json.loads(flow.spec)),
                    "created_at": pendulum.instance(flow.created_at).format(
                        "YYYY-MM-DD HH:MM:SS"
                    ),
                    "updated_at": pendulum.instance(flow.updated_at).format(
                        "YYYY-MM-DD HH:MM:SS"
                    ),
                }
                for flow in flows
            ]

            typer.echo(tabulate(flows, headers="keys", tablefmt="github"))
        else:
            typer.echo("No flow found")

    @require_workspace
    @require_saved
    @require_project
    def update(
        self,
        name: Optional[str] = None,
        input: Optional[FlowInput] = None,
        tasks: Optional[List[Task]] = None,
        output: Optional[FlowOutput] = None,
        description: str = "",
    ):
        if self.flow_id is None:
            raise ValueError("Flow not saved yet")
        flow = FlowRequest.select_by_id(self.flow_id)
        my_flow = Flow.from_dict(json.loads(flow.spec))
        if flow is None:
            raise ValueError("Flow not found")
        if name is None:
            name = flow.name
        if input is None:
            input = my_flow.input
        if output is None:
            output = my_flow.output
        if tasks is None:
            tasks = my_flow.tasks
        if description is None:
            description = flow.description
        new_flow = Flow(
            name=name, input=input, tasks=tasks, output=output, description=description
        )
        req = FlowRequest(
            body={"spec": new_flow.as_dict(), "name": name, "description": description}
        )
        req.save()
        self.__dict__.update(new_flow.__dict__)

    @require_workspace
    @require_saved
    @require_project
    def delete(self):
        if self.flow_id is None:
            raise ValueError("Invalid. Make sure load flow with get_or_create()")
        return FlowRequest.delete_by_id(self.flow_id)


@flow
def run_flow(
    flow_name: Optional[str] = None,
    flow: Optional[Flow] = None,
    params=None,
    filters=None,
    date=None,
    start_date=None,
    end_date=None,
    name="run_flow",
):
    def run_flow_by_instance(flow: Flow):
        return flow.run(params, filters, date, start_date, end_date)

    @require_project
    def run_flow_by_name(name: str):
        flow = Flow(name=name).get_or_create()
        return run_flow_by_instance(flow)

    if flow_name is not None:
        return run_flow_by_name(flow_name)
    elif flow is not None:
        return run_flow_by_instance(flow)
