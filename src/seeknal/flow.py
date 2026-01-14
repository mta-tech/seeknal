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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from tabulate import tabulate

from .common_artifact import Common, Source
from .context import check_project_id, context, logger, require_project
from .request import FlowRequest
from .tasks.base import Task
from .tasks.duckdb import DuckDBTask
from .tasks.sparkengine import SparkEngineTask
from .tasks.sparkengine.py_impl.base import BaseExtractorPySpark as Extractor
from .utils import to_snake
from .workspace import require_workspace


class FlowOutputEnum(str, Enum):
    """Enumeration of supported flow output types.

    Defines the possible output formats for Flow execution results.

    Attributes:
        SPARK_DATAFRAME: Output as a PySpark DataFrame.
        ARROW_DATAFRAME: Output as a PyArrow Table.
        PANDAS_DATAFRAME: Output as a Pandas DataFrame.
        HIVE_TABLE: Write output to a Hive table.
        PARQUET: Write output to Parquet files.
        LOADER: Use a custom loader for output.
        FEATURE_GROUP: Output to a feature group.
        FEATURE_SERVING: Output for feature serving.
    """

    SPARK_DATAFRAME = "spark_dataframe"
    ARROW_DATAFRAME = "arrow_dataframe"
    PANDAS_DATAFRAME = "pandas_dataframe"
    HIVE_TABLE = "hive_table"
    PARQUET = "parquet"
    LOADER = "loader"
    FEATURE_GROUP = "feature_group"
    FEATURE_SERVING = "feature_serving"


class FlowInputEnum(str, Enum):
    """Enumeration of supported flow input types.

    Defines the possible input sources for Flow data ingestion.

    Attributes:
        HIVE_TABLE: Read input from a Hive table.
        PARQUET: Read input from Parquet files.
        FEATURE_GROUP: Read input from a feature group.
        EXTRACTOR: Use a custom extractor for input.
        SOURCE: Read input from a defined Source.
    """

    HIVE_TABLE = "hive_table"
    PARQUET = "parquet"
    FEATURE_GROUP = "feature_group"
    EXTRACTOR = "extractor"
    SOURCE = "source"


@dataclass
class FlowInput:
    """Configuration for flow input data source.

    Defines how data is loaded into a Flow for processing. Supports multiple
    input types including Hive tables, Parquet files, extractors, and sources.

    Attributes:
        value: The input specification. Can be a table name (str), path (str),
            configuration (dict), or Extractor instance depending on the kind.
        kind: The type of input source (default: HIVE_TABLE).

    Example:
        >>> # Read from a Hive table
        >>> flow_input = FlowInput(value="my_database.my_table", kind=FlowInputEnum.HIVE_TABLE)
        >>> # Read from Parquet files
        >>> flow_input = FlowInput(value="/path/to/data.parquet", kind=FlowInputEnum.PARQUET)
    """

    value: Optional[Union[str, dict, Extractor]] = None
    kind: FlowInputEnum = FlowInputEnum.HIVE_TABLE

    def __call__(self, spark: Optional[SparkSession] = None):
        """Load data from the configured input source.

        Args:
            spark: Optional SparkSession for Spark-based inputs. Required for
                HIVE_TABLE input type.

        Returns:
            DataFrame or PyArrow Table containing the loaded data, depending
            on the input type and available Spark session.

        Raises:
            ValueError: If the value type doesn't match the expected type for
                the input kind.
            NotImplementedError: If FEATURE_GROUP input type is used (not yet
                implemented).
        """
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
    """Configuration for flow output destination.

    Defines how flow results are returned or persisted. Supports multiple
    output formats including DataFrames, Hive tables, and Parquet files.

    Attributes:
        value: The output destination. For file-based outputs, this is the
            path or table name. For DataFrame outputs, this is typically None.
        kind: The type of output format (default: None, returns data as-is).

    Example:
        >>> # Return as Spark DataFrame
        >>> output = FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME)
        >>> # Write to Hive table
        >>> output = FlowOutput(value="my_db.output_table", kind=FlowOutputEnum.HIVE_TABLE)
    """

    value: Optional[Any] = None
    kind: Optional[FlowOutputEnum] = None

    def _to_spark_dataframe(self, result: Union[DataFrame, pa.Table], spark: SparkSession) -> DataFrame:
        """Convert result to Spark DataFrame."""
        if isinstance(result, DataFrame):
            return result
        return spark.createDataFrame(result.to_pandas())

    def _to_arrow_table(self, result: Union[DataFrame, pa.Table]) -> pa.Table:
        """Convert result to PyArrow Table."""
        if isinstance(result, pa.Table):
            return result
        if isinstance(result, DataFrame):
            return pa.Table.from_pandas(result.toPandas())
        return result

    def _to_pandas_dataframe(self, result: Union[DataFrame, pa.Table]):
        """Convert result to Pandas DataFrame."""
        if isinstance(result, pa.Table):
            return result.to_pandas()
        if isinstance(result, DataFrame):
            return result.toPandas()
        return result

    def _write_to_hive(self, result: Union[DataFrame, pa.Table], spark: SparkSession, table_name: str) -> None:
        """Write result to a Hive table."""
        df = self._to_spark_dataframe(result, spark) if spark else result
        df.write.saveAsTable(table_name)

    def _write_parquet(self, result: Union[DataFrame, pa.Table], spark: SparkSession, path: str) -> None:
        """Write result to Parquet files."""
        df = self._to_spark_dataframe(result, spark) if spark else result
        df.write.parquet(path)

    def _write_with_loader(self, result: Union[DataFrame, pa.Table], spark: SparkSession, loader) -> None:
        """Write result using a custom loader."""
        from .tasks.sparkengine import SparkEngineTask
        df = self._to_spark_dataframe(result, spark) if spark else result
        SparkEngineTask().add_input(dataframe=df).add_output(loader=loader).transform(materialize=True)

    def __call__(
        self, result: Union[DataFrame, pa.Table], spark: Optional[SparkSession] = None
    ):
        """Process and output the flow result.

        Converts the result to the specified output format and optionally
        persists it to the configured destination.

        Args:
            result: The data to output, either as a PySpark DataFrame or
                PyArrow Table.
            spark: Optional SparkSession for Spark-based outputs.

        Returns:
            The processed result in the specified format, or None if the
            output was written to storage (HIVE_TABLE, PARQUET, LOADER).
        """
        if self.kind is None:
            return result

        converters = {
            FlowOutputEnum.SPARK_DATAFRAME: lambda r: self._to_spark_dataframe(r, spark) if spark else r,
            FlowOutputEnum.ARROW_DATAFRAME: self._to_arrow_table,
            FlowOutputEnum.PANDAS_DATAFRAME: self._to_pandas_dataframe,
            FlowOutputEnum.HIVE_TABLE: lambda r: self._write_to_hive(r, spark, self.value),
            FlowOutputEnum.PARQUET: lambda r: self._write_parquet(r, spark, self.value),
            FlowOutputEnum.LOADER: lambda r: self._write_with_loader(r, spark, self.value),
        }

        converter = converters.get(self.kind)
        if converter:
            return converter(result)
        return None


VALID_SPARK_INPUT = [
    FlowInputEnum.HIVE_TABLE,
    FlowInputEnum.PARQUET,
    FlowInputEnum.FEATURE_GROUP,
    FlowInputEnum.EXTRACTOR,
]


@dataclass
class Flow:
    """A data processing pipeline that chains inputs, tasks, and outputs.

    Flow is the core abstraction for defining data pipelines in seeknal. It connects
    a data source (input), a series of transformation tasks, and an output destination.
    Flows can be saved to and loaded from the seeknal backend for reuse and scheduling.

    Attributes:
        name: Unique identifier for the flow (automatically converted to snake_case).
        input: Configuration for the input data source.
        input_date_col: Optional date column configuration for filtering input data.
            Contains 'dateCol' (column name) and 'datePattern' (date format).
        tasks: Optional list of Task instances to execute in sequence.
        output: Configuration for the output destination.
        description: Human-readable description of the flow's purpose.

    Example:
        >>> from seeknal.flow import Flow, FlowInput, FlowOutput, FlowInputEnum, FlowOutputEnum
        >>> from seeknal.tasks.sparkengine import SparkEngineTask
        >>>
        >>> # Create a simple flow
        >>> flow = Flow(
        ...     name="my_etl_flow",
        ...     input=FlowInput(value="source_table", kind=FlowInputEnum.HIVE_TABLE),
        ...     tasks=[SparkEngineTask()],
        ...     output=FlowOutput(kind=FlowOutputEnum.SPARK_DATAFRAME),
        ...     description="ETL flow for processing source data"
        ... )
        >>>
        >>> # Run the flow
        >>> result = flow.run(start_date="2024-01-01", end_date="2024-01-31")
    """

    name: str
    input: Optional[FlowInput] = None
    input_date_col: Optional[dict] = None
    tasks: Optional[List[Task]] = None
    output: Optional[FlowOutput] = None
    description: str = ""

    def __post_init__(self):
        """Initialize the flow and convert name to snake_case."""
        self.name = to_snake(self.name)

    def require_saved(func):
        """Decorator that ensures the flow has been saved before method execution.

        Args:
            func: The method to wrap.

        Returns:
            Wrapped function that checks for flow_id before execution.

        Raises:
            ValueError: If the flow has not been saved (no flow_id).
        """

        def wrapper(self, *args, **kwargs):
            if not "flow_id" in vars(self):
                raise ValueError("flow not loaded or saved")
            else:
                func(self, *args, **kwargs)

        return wrapper

    def set_input_date_col(self, date_col: str, date_pattern: str = "yyyyMMdd"):
        """Configure the date column for input data filtering.

        Sets up date-based filtering on the input data, allowing the flow
        to process data within specific date ranges.

        Args:
            date_col: Name of the column containing date values.
            date_pattern: Date format pattern (default: "yyyyMMdd").

        Returns:
            Self for method chaining.

        Example:
            >>> flow.set_input_date_col("event_date", "yyyy-MM-dd")
        """
        self.input_date_col = {
            "dateCol": date_col,
            "datePattern": date_pattern,
        }
        return self

    def _requires_spark(self) -> bool:
        """Check if the flow requires Spark session.

        Determines whether a Spark session is needed based on the input type
        and whether any tasks are Spark jobs.

        Returns:
            True if Spark is required, False otherwise.
        """
        if self.input.kind in VALID_SPARK_INPUT:
            return True
        if self.tasks:
            return any(task.is_spark_job for task in self.tasks)
        return False

    def run(self, params=None, filters=None, date=None, start_date=None, end_date=None):
        """Execute the flow pipeline.

        Runs the complete flow: loads input data, applies filters, executes
        all tasks in sequence, and returns the output in the configured format.

        Args:
            params: Optional dictionary of parameters to pass to tasks.
            filters: Optional filters to apply to the input data.
            date: Optional single date for filtering (mutually exclusive with
                start_date/end_date).
            start_date: Optional start date for date range filtering.
            end_date: Optional end date for date range filtering.

        Returns:
            The processed data in the format specified by the output configuration.

        Example:
            >>> # Run with date range
            >>> result = flow.run(start_date="2024-01-01", end_date="2024-01-31")
            >>> # Run with parameters
            >>> result = flow.run(params={"threshold": 0.5})
        """
        spark = SparkSession.builder.getOrCreate() if self._requires_spark() else None
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
        """Convert the flow to a dictionary representation.

        Serializes the flow configuration to a dictionary suitable for
        storage or transmission. Removes Spark context references.

        Returns:
            Dictionary containing the flow's configuration including
            name, input, output, tasks, and description.
        """
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
        """Convert the flow to a YAML string representation.

        Returns:
            YAML-formatted string of the flow configuration.
        """
        flow_dict = self.as_dict()
        return yaml.dump(flow_dict)

    def __str__(self):
        """Return string representation of the flow."""
        return str(self.as_dict())

    @staticmethod
    def from_dict(flow_dict: dict):
        """Create a Flow instance from a dictionary.

        Deserializes a flow configuration dictionary back into a Flow object.
        Reconstructs input, output, and task configurations.

        Args:
            flow_dict: Dictionary containing flow configuration with keys
                like 'name', 'input', 'output', 'tasks'.

        Returns:
            Flow instance with the configured settings.

        Raises:
            ValueError: If a task in the dictionary is missing 'class_name'.

        Example:
            >>> flow_config = {"name": "my_flow", "input": {...}, "output": {...}}
            >>> flow = Flow.from_dict(flow_config)
        """
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
        """Save or retrieve the flow from the backend.

        If a flow with the same name exists in the current project, loads
        its configuration. Otherwise, saves this flow as a new entry.

        Returns:
            Self with flow_id populated.

        Note:
            Requires an active workspace and project context.
        """
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
        """List all flows in the current project.

        Displays a formatted table of flows including name, description,
        specification, and timestamps.

        Note:
            Requires an active workspace and project context.
            Outputs directly to the console using typer.echo.
        """
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
        """Update the flow configuration in the backend.

        Updates the saved flow with new configuration values. Any parameter
        not provided will retain its existing value.

        Args:
            name: New name for the flow.
            input: New input configuration.
            tasks: New list of tasks.
            output: New output configuration.
            description: New description.

        Raises:
            ValueError: If the flow has not been saved yet or not found.

        Note:
            Requires the flow to be saved first via get_or_create().
        """
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
        """Delete the flow from the backend.

        Removes the saved flow from the seeknal backend permanently.

        Returns:
            Result of the delete operation.

        Raises:
            ValueError: If the flow has not been saved yet.

        Note:
            Requires the flow to be saved first via get_or_create().
        """
        if self.flow_id is None:
            raise ValueError("Invalid. Make sure load flow with get_or_create()")
        return FlowRequest.delete_by_id(self.flow_id)


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
    """Execute a flow by name or instance.

    Convenience function to run a flow either by providing its name
    (loads from backend) or a Flow instance directly.

    Args:
        flow_name: Name of a saved flow to load and run.
        flow: Flow instance to run directly.
        params: Optional dictionary of parameters to pass to tasks.
        filters: Optional filters to apply to the input data.
        date: Optional single date for filtering.
        start_date: Optional start date for date range filtering.
        end_date: Optional end date for date range filtering.
        name: Internal name for the operation (default: "run_flow").

    Returns:
        The processed data from the flow execution.

    Example:
        >>> # Run by flow name
        >>> result = run_flow(flow_name="my_saved_flow", start_date="2024-01-01")
        >>> # Run by instance
        >>> result = run_flow(flow=my_flow_instance, params={"key": "value"})
    """

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
