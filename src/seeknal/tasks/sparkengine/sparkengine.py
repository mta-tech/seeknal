import copy
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Union

import pendulum
import typer
import yaml
from seeknal.context import logger as logger_spark
from pyspark.ml.wrapper import JavaWrapper
from pyspark.sql import DataFrame, SparkSession

from ..base import Task
from .aggregators import Aggregator
from .extractors import Extractor
from .loaders import Loader
from .transformers import (
    SQL,
    AddColumnByExpr,
    ClassName,
    FeatureTransformer,
    FilterByExpr,
    PointInTime,
    Transformer,
)


@dataclass
class Stage:
    """
    Define pipeline stage
    """

    id: Optional[str] = None
    class_name: Optional[str] = None
    params: Optional[dict] = None
    feature: Optional[FeatureTransformer] = None
    aggregator: Optional[Aggregator] = None
    transformer: Optional[Transformer] = None


@dataclass
class Stages:
    """
    Define stages
    """

    stages: List[Stage]


@dataclass
class SparkEngineTask(Task):
    """
    Define Spark Engine task
    """

    name: Optional[str] = None
    description: Optional[str] = None
    feature: Optional[dict] = None
    default_input_db: str = "base_input"
    default_output_db: str = "base_output"
    _materialize: bool = False
    is_spark_job: bool = True
    stages: Optional[List[dict]] = field(default_factory=list)
    kind: str = "SparkEngineTask"

    def __post_init__(self):
        self.is_spark_job = True
        self.kind = "SparkEngineTask"

    def set_date_col(self, date_col: str, date_pattern: str = "yyyyMMdd"):
        """
        set date column

        Args:
            date_col (str): a column in source dataset that represent date
            date_pattern (str): date pattern format are used by the date column
        """
        if self.input is None:
            self.input = {}
            self.input = {"params": {"dateCol": date_col, "datePattern": date_pattern}}
        else:
            if "params" not in self.input:
                self.input["params"] = {
                    "dateCol": date_col,
                    "datePattern": date_pattern,
                }
            else:
                self.input["params"].update(
                    {"dateCol": date_col, "datePattern": date_pattern}
                )
        return self

    def set_default_input_db(self, db_name: str):
        """
        set default input database

        Args:
            db_name (str): database name
        """
        self.default_input_db = db_name
        return self

    def set_default_output_db(self, db_name: str):
        """
        set default output database

        Args:
            db_name (str): database name
        """
        self.default_output_db = db_name
        return self

    def add_input(
        self,
        id: Optional[str] = None,
        table: Optional[str] = None,
        source: Optional[str] = None,
        params: Optional[dict] = None,
        extractor: Optional[Extractor] = None,
        dataframe: Optional[DataFrame] = None,
    ):
        """
        Add input to the transformation pipeline

        Args:
            id (str, optional): reference source id name
            table (str, optional): table name
            source (str, optional): source name which tell where the data comes from e.g., file, RDMS, etc
            params (dict, optional): parameters for given source
            extractor (Extractor, optional): define input using Extractor object
            dataframe (DataFrame, optional): define input using Spark dataframe

        Source and params accept Spark Engine supported connectors or Spark dataframe.
        Example of a usage of `add_input()`:

        1). Define table as input::

            SparkEngineTask(name="create_dataset")
                .add_input(table="telco_sample.db_charging_hourly")

        2). Define connector as input::

            SparkEngineTask(name="create_dataset")
                .add_input(source="file", params={"format": "parquet", "path": "path/to/parquet"})

        3). Define connector with Extractor object::

            SparkEngineTask(name="create_dataset")
                .add_input(extractor=Extractor(source="file", params={...}))

        4). Define input using dataframe::

            df = spark.read.table("telco_sample.db_charging_hourly")
            SparkEngineTask(name="create_dataset")
                .add_input(dataframe=df)

        """
        if (
            id is None
            and table is None
            and source is None
            and extractor is None
            and dataframe is None
        ):
            raise ValueError("id or hive_table or source or dataframe must be defined")
        if id is not None:
            self.input = {"id": id}
        elif table is not None:
            self.input = {"table": table, "source": "hive"}
        elif source is not None:
            if params is None:
                raise ValueError("params must be defined")
            self.input = {"source": source}
        elif extractor is not None:
            self.input = extractor.dict(exclude_none=True)
        elif dataframe is not None:
            self.input = {"dataframe": dataframe}

        if params is not None:
            if "params" in self.input:  # type: ignore
                self.input["params"].update(params)  # type: ignore
            else:
                self.input["params"] = params  # type: ignore

        return self

    def add_stage(
        self,
        id: Optional[str] = None,
        class_name: Optional[str] = None,
        params: Optional[dict] = None,
        feature: Optional[FeatureTransformer] = None,
        aggregator: Optional[Aggregator] = None,
        transformer: Optional[Transformer] = None,
        stage: Optional[Stage] = None,
        stages: Optional[Stages] = None,
        tags: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """
        Add stage to the transformation pipeline

        Args:
            id (str, optional): reference transformation id that available in a project
            class_name (str, optional): class name of transformation function
            params (dict, optional): parameters for given class_name
            feature (FeatureTransformer, optional): include FeatureTransformer to the stage
            aggregator (Aggregator, optional): add aggregator transformer to the stage
            transformer (Transformer, optional): add transformer object to the stage
            stage (Stage, optional): add Stage class to the stage
            stages (Stages, optional): combine list of stages to this stage pipeline
            tags (str, optional): add tag to the stage
            description (str, optional): add description to the stage

        `class_name` and `params` should follow Transformers that supported in Spark Engine. Example of usage:
        ::

            from seeknal.tasks.sparkengine import transformers as F
            from seeknal.tasks.sparkengine import aggregators as G

            dummy_aggregation = G.Aggregator(group_by_cols=["msisdn", "day"], aggregators=[
              G.AggregatorFunction(
                class_name="tech.mta.seeknal.aggregators.FunctionAggregator",
                params={
                  "inputCol": "comm_count_call_in",
                  "outputCol": "comm_count_call_in",
                  "accumulatorFunction": "first"
                }
              )
            ])

            res = SparkEngineTask(name="create_dataset")
                .add_input(hive_table="telco_sample.db_charging_hourly") \
                .set_date_col(date_col="date_id") \
                .add_stage(transformer=F.ColumnRenamed(inputCol="id", outputCol="id")) \
                .add_stage(transformer=F.Transformer(F.SparkEngineClassName.COLUMN_RENAMED, inputCol="id", outputCol="msisdn")) \
                .add_stage(aggregator=dummy_aggregation) \
                .transform()
        """
        if (
            id is None
            and class_name is None
            and feature is None
            and aggregator is None
            and transformer is None
            and stage is None
            and stages is None
        ):
            raise ValueError(
                "id or class_name or feature or aggregator must be defined"
            )
        if self.stages is None:
            self.stages = []
        if id is not None:
            self.stages.append({"id": id})
        elif class_name is not None:
            if params is None:
                raise ValueError("params must be defined")
            self.stages.append({"className": class_name, "params": params})
        elif feature is not None:
            self.stages.append({"id": "feature"})
            if self.feature is not None:
                typer.echo(
                    "Only one `feature` can be added. This will replace the prior one."
                )
            self.feature = feature.dict()
        elif aggregator is not None:
            aggregator_dict = aggregator.dict()
            function_aggr = []
            for k in aggregator_dict["aggregators"]:
                key_val = {"className": k["class_name"], "params": k["params"]}
                function_aggr.append(key_val)

            aggregator_step = {
                "className": "tech.mta.seeknal.transformers.GroupByColumns",
                "aggregators": function_aggr,
            }
            group_by_params = {
                "inputCols": aggregator.group_by_cols,
            }

            if aggregator.pivot_key_col:
                group_by_params["pivotKeyCol"] = aggregator.pivot_key_col  # type: ignore
            if aggregator.pivot_value_cols:
                group_by_params["pivotValueCols"] = aggregator.pivot_value_cols
            if aggregator.col_by_expression:
                group_by_params["colByExpression"] = aggregator_dict[
                    "col_by_expression"
                ]
            if aggregator.renamed_cols:
                group_by_params["renamedCols"] = aggregator_dict["renamed_cols"]

            aggregator_step["params"] = group_by_params
            steps = [aggregator_step]
            if aggregator_dict["pre_stages"] is not None:
                pre_steps = [
                    {"className": k["class_name"], "params": k["params"]}
                    for k in aggregator_dict["pre_stages"]
                ]
                steps = pre_steps + steps
            if aggregator_dict["post_stages"] is not None:
                post_steps = [
                    {"className": k["class_name"], "params": k["params"]}
                    for k in aggregator_dict["post_stages"]
                ]
                steps = steps + post_steps

            self.stages += steps
        elif transformer is not None:
            if isinstance(transformer, PointInTime):
                try:
                    if self.input["params"]["dateCol"]:
                        transformer.update_param(
                            "feature_date", self.input["params"]["dateCol"]
                        )
                    if self.input["params"]["datePattern"]:
                        transformer.update_param(
                            "feature_date_format", self.input["params"]["datePattern"]
                        )
                except KeyError:
                    logger_spark.debug(
                        "Won't update 'dateCol', 'datePattern' for PointInTime transformer - none were specified."
                    )

            self.stages.append(
                {"className": transformer.class_name, "params": transformer.params}
            )
        elif stage is not None:
            if stage.id is not None:
                self.stages.append({"id": stage.id})
            elif stage.class_name is not None:
                if stage.params is None:
                    raise ValueError("params must be defined")
                self.stages.append(
                    {"className": stage.class_name, "params": stage.params}
                )
            elif stage.feature is not None:
                self.stages.append({"id": "feature"})
                if self.feature is not None:
                    logger_spark.warning(
                        "Only one `feature` can be added. This will replace the prior one."
                    )
                self.feature = stage.feature.dict()
            elif stage.aggregator is not None:
                aggregator_dict = stage.aggregator.dict()
                function_aggr = []
                for k in aggregator_dict["aggregators"]:
                    key_val = {"className": k["class_name"], "params": k["params"]}
                    function_aggr.append(key_val)

                aggregator_step = {
                    "className": "tech.mta.seeknal.transformers.GroupByColumns",
                    "aggregators": function_aggr,
                }
                group_by_params = {
                    "inputCols": stage.aggregator.group_by_cols,
                }

                if stage.aggregator.pivot_key_col:
                    group_by_params["pivotKeyCol"] = stage.aggregator.pivot_key_col  # type: ignore
                if stage.aggregator.pivot_value_cols:
                    group_by_params[
                        "pivotValueCols"
                    ] = stage.aggregator.pivot_value_cols
                if stage.aggregator.col_by_expression:
                    group_by_params["colByExpression"] = aggregator_dict[
                        "col_by_expression"
                    ]
                if stage.aggregator.renamed_cols:
                    group_by_params["renamedCols"] = aggregator_dict["renamed_cols"]

                aggregator_step["params"] = group_by_params

                steps = [aggregator_step]
                if aggregator_dict["pre_stages"] is not None:
                    pre_steps = [
                        {"className": k["class_name"], "params": k["params"]}
                        for k in aggregator_dict["pre_stages"]
                    ]
                    steps = pre_steps + steps
                if aggregator_dict["post_stages"] is not None:
                    post_steps = [
                        {"className": k["class_name"], "params": k["params"]}
                        for k in aggregator_dict["post_stages"]
                    ]
                    steps = steps + post_steps
                self.stages += steps
            elif stage.transformer is not None:
                self.stages.append(
                    {
                        "className": stage.transformer.class_name,
                        "params": stage.transformer.params,
                    }
                )
        elif stages is not None:
            for s in stages.stages:
                self.add_stage(stage=s)

        if tags is not None:
            self.stages[-1]["tags"] = tags

        if description is not None:
            self.stages[-1]["description"] = description

        return self

    def add_stages(self, stages: Stages):
        """
        Add stages to the pipeline

        Args:
            stages (Stages): append stages into this task pipeline
        """
        for s in stages.stages:
            self.add_stage(stage=s)
        return self

    def add_sql(self, statement: str):
        """
        Add SQL transformation to the stage

        Args:
            statement (str): SQL statement
        """
        self.add_stage(transformer=SQL(statement=statement))
        return self

    def add_new_column(self, expression: str, output_col: str):
        """
        Add new column by expression

        Args:
            expression (str): expression to create new column
            output_col (str): output column name
        """
        self.add_stage(
            transformer=AddColumnByExpr(expression=expression, outputCol=output_col)
        )
        return self

    def add_filter_by_expr(self, expression: str):
        """
        Filter by expression

        Args:
            expression (str): expression to filter
        """
        self.add_stage(transformer=FilterByExpr(expression=expression))
        return self

    def select_columns(self, columns: List[str]):
        """
        Select columns

        Args:
            columns (List[str]): list of columns to be selected
        """
        self.add_stage(
            transformer=Transformer(
                SparkEngineClassName.SELECT_COLUMNS, inputCols=columns
            )
        )
        return self

    def drop_columns(self, columns: List[str]):
        """
        Drop columns

        Args:
            columns (List[str]): list of columns to be dropped
        """
        self.add_stage(
            transformer=Transformer(SparkEngineClassName.DROP_COLS, inputCols=columns)
        )
        return self

    def update_stage(self, number: int, **kwargs):
        """
        Update stage
        """
        if self.stages is not None:
            _temp = SparkEngineTask().add_stage(**kwargs)
            self.stages[number] = _temp.stages[0]
        else:
            raise Exception("No stages defined")

        return self

    def remove_stage(self, number: int):
        """
        Remove stage
        """
        if self.stages is not None:
            self.stages.pop(number)
        else:
            raise Exception("No stages defined")

        return self

    def insert_stage(self, number: int, **kwargs):
        """
        Insert stage
        """
        if self.stages is not None:
            _temp = SparkEngineTask().add_stage(**kwargs)
            self.stages.insert(number, _temp.stages[0])
        else:
            raise Exception("No stages defined")

        return self

    def _create_spec_from_attributes(self):
        """
        Create a task spec from the attributes
        """
        task_spec = {"pipeline": {}}
        if self.name is not None:
            task_spec["name"] = self.name
        if self.description is not None:
            task_spec["description"] = self.description
        if self.input is not None:
            if "dataframe" not in self.input:
                task_spec["pipeline"]["input"] = self.input
            else:
                _input_spec = self.input.copy()
                _input_spec.pop("dataframe")
                task_spec["pipeline"]["input"] = _input_spec
        if self.output is not None:
            task_spec["pipeline"]["output"] = self.output
        if self.stages is not None:
            task_spec["pipeline"]["stages"] = self.stages
        if self.feature is not None:
            feature_spec_dict = {k: v for k, v in self.feature.items() if v is not None}
            task_spec["feature"] = feature_spec_dict
        return task_spec

    def add_yaml(self, yaml_string: str):
        """
        Add yaml string to the pipeline

        Args:
            yaml_string (str): yaml string
        """
        task_spec = yaml.load(yaml_string, Loader=yaml.FullLoader)
        if "pipeline" in task_spec:
            if "input" in task_spec["pipeline"]:
                self.input = task_spec["pipeline"]["input"]
            if "output" in task_spec["pipeline"]:
                self.output = task_spec["pipeline"]["output"]
            if "stages" in task_spec["pipeline"]:
                self.stages = task_spec["pipeline"]["stages"]
        if "feature" in task_spec:
            self.feature = task_spec["feature"]
        if "name" in task_spec:
            self.name = task_spec["name"]
        if "description" in task_spec:
            self.description = task_spec["description"]
        return self

    def add_yaml_file(self, yaml_path: str):
        """
        Add yaml file to the pipeline

        Args:
            yaml_path (str): yaml file path
        """
        with open(yaml_path, "r") as f:
            task_spec = f.read()
        self.add_yaml(task_spec)
        return self

    def to_yaml_file(self, yaml_path: str):
        """
        Save yaml string to a file

        Args:
            yaml_path (str): yaml file path
        """
        task_spec = self._create_spec_from_attributes()
        with open(yaml_path, "w") as f:
            f.write(yaml.dump(task_spec))

    def print_yaml(self):
        """
        Print yaml string
        """
        task_spec = self._create_spec_from_attributes()
        typer.echo(yaml.dump(task_spec))
        return self

    def add_common_yaml(self, common_config: str):
        """
        Add common config to the pipeline

        Args:
            common_config (str): common config
        """
        self.common = common_config
        return self

    def add_common_yaml_file(self, common_config_path: str):
        """
        Add common config file to the pipeline

        Args:
            common_config_path (str): common config file path
        """
        with open(common_config_path, "r") as f:
            common_config = f.read()
        self.add_common_yaml(common_config)
        return self

    def print_common_yaml(self):
        """
        Print yaml string
        """
        typer.echo(self.common)
        return self

    def add_output(
        self,
        id: Optional[str] = None,
        table: Optional[str] = None,
        partitions: Optional[List[str]] = None,
        path: Optional[str] = None,
        source: Optional[str] = None,
        params: Optional[dict] = None,
        repartition: Optional[int] = None,
        loader: Optional[Loader] = None,
    ):
        """
        Define output for the pipeline transformation

        Args:
            id (str, optional): reference source id name
            table (str, optional): table name
            partitions (List[str], optional): set which column(s) become partition(s)
            path (str, optional): specify path location
            source (str, optional): source name which tell where the data comes from e.g., file, RDMS, etc
            params (dict, optional): parameters for given source
            repatition (int, optional): repartition dataset before writing into a sink location
            loader (Loader, optional): add Loader object

        Source and params should follow Spark Engine supported connectors. Example of a usage of `add_output()`:

        1). Define table as output::

            SparkEngineTask(name="create_dataset")
                .add_output(table="telco_sample.db_charging_hourly", partitions=["date_id"], path="path/to/loc")

        2). Define connector as output::

            SparkEngineTask(name="create_dataset")
                .add_output(source="file", params={"format": "parquet", "path": "path/to/parquet"})

        3). Define connector with Loader object::

            SparkEngineTask(name="create_dataset")
                .add_output(loader=Loader(source="file", params={...}))

        """
        if id is None and table is None and source is None and loader is None:
            raise ValueError("Must specify id or table or source")

        if id is not None:
            self.output = {"id": id}
        elif table is not None:
            self.output = {"table": table}
            if path is not None:
                self.output["path"] = path
        elif source is not None:
            if params is None:
                raise ValueError("Params must be specified")
            self.output = {"source": source, "params": params}
        elif loader is not None:
            self.output = loader.model_dump(exclude_none=True)

        if partitions is not None:
            self.output["partitions"] = partitions  # type: ignore

        if repartition is not None:
            self.output["repartition"] = repartition  # type: ignore
        return self

    @staticmethod
    def _parse_date_time(
        date: Optional[str],
        date_pattern: str,
        late_days: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_date_between: bool = False,
    ) -> Optional[List]:
        """
        Get execution date time
        if date is equal to 'today' then will use today's date minus late_days
        otherwise, will create array of date string based on date, start_date and end_date

        Parameters
        ----------
        date : str
            user specified date to run
        date_pattern : str
            date pattern to use
        late_days : str
            number of days late for given current date
        start_date : str
            user specified start date to run
        end_date : str
            user specified end date to run
        use_date_between : bool
            whether use date between filter when given a date range

        Returns
        -------
        date to run : Optional(list of str)
        """
        date_to_run = None
        date_to_run_one = None
        if isinstance(date, int):
            date_to_run_one = [str(date)]
        elif isinstance(date, str):
            date_to_run_one = [
                SparkEngineTask._convert_supplied_context(
                    date=date, late_days=late_days, date_pattern=date_pattern
                )
            ]
        elif isinstance(date, list):
            date_to_run_one = [
                SparkEngineTask._convert_supplied_context(
                    date=str(x), late_days=late_days, date_pattern=date_pattern
                )
                for x in date
            ]

        date_to_run_two = None
        if start_date is not None:
            if end_date is None:
                end_date = pendulum.now().format(date_pattern.upper())
            start_context = SparkEngineTask._convert_supplied_context(
                date=start_date, late_days=late_days, date_pattern=date_pattern
            )
            end_context = SparkEngineTask._convert_supplied_context(
                date=end_date, late_days=late_days, date_pattern=date_pattern
            )
            if start_context is None:
                start_context = start_date
            if end_context is None:
                end_context = end_date
            start = pendulum.parse(start_context)
            end = pendulum.parse(end_context)
            if use_date_between == False:
                period = pendulum.period(start, end)
                date_to_run_two = [
                    y.format(date_pattern.upper()) for y in period.range("days")
                ]
            else:
                date_to_run_two = [
                    start.format(date_pattern.upper()),
                    end.format(date_pattern.upper()),
                ]
        if date_to_run_one is not None or date_to_run_two is not None:
            date_to_run = []
            if date_to_run_one is not None:
                date_to_run += date_to_run_one
            if date_to_run_two is not None:
                date_to_run += date_to_run_two

        return date_to_run

    @staticmethod
    def _convert_supplied_context(
        date: str, late_days: int = 0, date_pattern: str = "YYYYMMDD"
    ):
        date_to_run = None
        rgx = re.compile(r"(today)?([-+]?\d+(?:\.\d+)?)")
        if date == "today":
            date_to_run = pendulum.today()
            date_to_run = date_to_run.add(days=-late_days).format(date_pattern.upper())
        elif rgx.match(date):
            _group = rgx.match(date)
            if _group.group(1) == "today":
                date_to_run = pendulum.today()
                date_to_run = (
                    date_to_run.add(days=eval(_group.group(2)))
                    .add(days=-late_days)
                    .format(date_pattern.upper())
                )
            else:
                date_to_run = date

        if date_to_run is None:
            return date

        return date_to_run

    def _init_feature_engine_job(
        self,
        params: Optional[dict] = None,
        filters: Optional[dict] = None,
        date: Optional[Union[str, datetime]] = None,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ):
        """
        Initialize Spark Engine job

        Args:
            params: parameters for Spark Engine job
            filters: filters for Spark Engine job
            date: date selection for Spark Engine job
        """
        job_spec = yaml.dump(self._create_spec_from_attributes())
        _java_obj = JavaWrapper._new_java_obj(
            "tech.mta.seeknal.SparkEngineAPI"
        )
        self._wrapper = JavaWrapper(_java_obj)
        self._wrapper._call_java("setJobSpec", None, job_spec)
        if params is not None:
            self._wrapper._call_java("setInlineParam", params)
        if filters is not None:
            self._wrapper._call_java("setFilter", filters)
        if self.common is not None:
            self._wrapper._call_java("setCommonObject", None, self.common)
            self._wrapper._call_java("setInputSourceId", self.default_input_db)
            self._wrapper._call_java("setOutputSourceId", self.default_output_db)
        date_col = "date_id"
        date_pattern = "yyyyMMdd"
        if self.input is not None:
            if "params" in self.input:
                if "dateCol" in self.input["params"]:
                    date_col = self.input["params"]["dateCol"]
                if "datePattern" in self.input["params"]:
                    date_pattern = self.input["params"]["datePattern"]
        if date is not None:
            if isinstance(date, datetime):
                date = pendulum.instance(date).format(date_pattern.upper())
        if start_date is not None:
            if isinstance(start_date, datetime):
                start_date = pendulum.instance(start_date).format(date_pattern.upper())
        if end_date is not None:
            if isinstance(end_date, datetime):
                end_date = pendulum.instance(end_date).format(date_pattern.upper())
        _dt = SparkEngineTask._parse_date_time(
            date=date,
            date_pattern=date_pattern,
            late_days=0,
            start_date=start_date,
            end_date=end_date,
            use_date_between=True,
        )
        if _dt is not None:
            if len(_dt) == 1:
                self._wrapper._call_java("setDate", _dt)
            else:
                _job_spec = self._create_spec_from_attributes().copy()
                _job_spec["pipeline"]["stages"].insert(
                    0,
                    {
                        "className": "tech.mta.seeknal.transformers.FilterByExpr",
                        "params": {
                            "expression": f"{date_col} >= '{_dt[0]}' AND {date_col} <= '{_dt[1]}'"
                        },
                    },
                )
                _yml_dump = yaml.dump(_job_spec)
                self._wrapper._call_java("setJobSpec", None, _yml_dump)
        self._wrapper._call_java("setDatePattern", date_pattern)
        self._date_pattern = date_pattern
        self._date_col = date_col

    def transform(
        self,
        spark: Optional[SparkSession] = None,
        chain: bool = False,
        materialize: bool = False,
        params=None,
        filters=None,
        date=None,
        start_date: Optional[Union[str, datetime]] = None,
        end_date: Optional[Union[str, datetime]] = None,
    ) -> Union[Task, DataFrame]:
        """
        Run transformation for given job spec
        Args:
            params: parameters for Spark Engine job
            filters: filters for Spark Engine job
            date: date selection for Spark Engine job

        Returns:
            DataFrame: transformed dataframe
        """
        self._init_feature_engine_job(params, filters, date, start_date, end_date)
        self._params = params
        self._filters = filters
        self._date = date
        if materialize is False:
            if "dataframe" in self.input:
                res = self._wrapper._call_java(
                    "transform", self.input["dataframe"], self._date_col
                )
            else:
                res = self._wrapper._call_java("transform")
            return res
        else:
            if self.input is not None:
                if "dataframe" in self.input:
                    res = self._wrapper._call_java(
                        "transform", self.input["dataframe"], self._date_col
                    )
                    if date is not None:
                        self._wrapper._call_java("materialize", res, date)
                    else:
                        self._wrapper._call_java("materialize", res, "")

                else:
                    self._wrapper._call_java("run")
            else:
                self._wrapper._call_java("run")
            self._materialize = True
            return self

    def evaluate(
        self,
        statement: Optional[str] = None,
        maxResults=10,
        truncate=True,
        params=None,
        filters=None,
        date=None,
    ):
        """
        Evaluate transformation for given job spec
        Args:
            statement: SQL statement to evaluate
            maxResults: number of results to show
            truncate: truncate results
            params: parameters for Spark Engine job
            filters: filters for Spark Engine job
            date: date selection for Spark Engine job
        """
        res = self.transform(None, params=params, filters=filters, date=date)
        if isinstance(res, DataFrame):
            abc = res
        else:
            raise Exception()
        if statement is not None:
            res = (
                SparkEngineTask()
                .add_input(dataframe=abc)
                .add_stage(transformer=SQL(statement=statement))
                .transform()
            )
            if isinstance(res, DataFrame):
                res.show(maxResults, truncate)
        else:
            abc.show(maxResults, truncate)
        return self

    def get_output_dataframe(self) -> Union[DataFrame, None]:
        """
        Get output dataframe
        """
        if self._materialize is True:
            feat_cls = SparkEngineTask()
            feat_cls.input = self.output
            if "params" not in feat_cls.input:
                feat_cls.input["params"] = {}
            feat_cls.input["params"]["dateCol"] = self._date_col
            feat_cls.input["params"]["datePattern"] = self._date_pattern
            res = feat_cls.transform(
                SparkSession.builder.getOrCreate(),
                params=self._params,
                filters=self._filters,
                date=self._date,
            )
            if isinstance(res, DataFrame):
                return res
            else:
                return None
        else:
            logger_spark.error(
                "No results to return, please materialize the transformation  with `transform_and_save()` method"
            )
            return None

    def show_output_dataframe(self, max_results=10):
        """
        Show results after materialize the transformation.

        Args:
            max_results (int): number of results to show
        """
        res = self.get_output_dataframe()
        if res is not None:
            res.show(max_results)

    def is_materialized(self) -> bool:
        """
        Check if the transformation is materialized
        """
        return self._materialize

    @staticmethod
    def _empty_copy(obj):
        class Empty(obj.__class__):
            def __init__(self):
                pass

        newcopy = Empty()
        newcopy.__class__ = obj.__class__
        return newcopy

    def copy(self):
        """
        Create a copy of this object
        """
        newcopy = SparkEngineTask._empty_copy(self)
        if self.input is not None:
            if "dataframe" in self.input:
                random_name = "f" + str(uuid.uuid4())[:8]
                self.input["dataframe"].createOrReplaceTempView(f"table_{random_name}")
                self.input = {"table": f"table_{random_name}"}
        _dict = copy.deepcopy(self.__dict__)
        newcopy.__dict__.update(_dict)
        return newcopy

    def get_date_available(
        self,
        after_date: Optional[Union[str, datetime]] = None,
        limit: int = 100000,
    ) -> List[str]:
        if self.input is None:
            raise ValueError("input must be defined")
        src_feat = SparkEngineTask()
        src_feat.input = self.input
        if "dateCol" not in self.input["params"]:
            raise ValueError("must provide dateCol inside of input params")
        date_col = self.input["params"]["dateCol"]
        if after_date is not None:
            date_pattern = "yyyyMMdd"
            if "datePattern" in self.input["params"]:
                date_pattern = self.input["params"]["datePattern"]
            if isinstance(after_date, datetime):
                after_date = pendulum.instance(after_date).format(date_pattern.upper())
            src_feat.add_sql(
                "SELECT {0} FROM __THIS__ WHERE {0} >= '{1}'".format(
                    date_col, after_date
                )
            )
        df = src_feat.add_sql(
            "SELECT {0}, count(*) as count FROM __THIS__ group by {0} having count > 0 LIMIT {1}".format(
                date_col, limit
            )
        ).transform()
        res = df.orderBy(date_col).collect()
        date_available = []
        if len(res) > 0:
            date_available = [i[date_col] for i in res]
            date_available.sort()
        return date_available
