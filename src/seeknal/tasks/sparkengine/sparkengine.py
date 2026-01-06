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
    """Configuration for a single pipeline stage in a SparkEngine transformation.

    A Stage represents one step in a data transformation pipeline. It can contain
    a reference to an existing transformation by ID, a custom class with parameters,
    or one of several built-in transformation types (feature, aggregator, transformer).

    Attributes:
        id: Reference ID of a pre-defined transformation available in the project.
        class_name: Fully qualified class name of a custom Spark transformation.
        params: Parameters dictionary passed to the transformation class.
        feature: A FeatureTransformer instance for feature engineering operations.
        aggregator: An Aggregator instance for grouping and aggregation operations.
        transformer: A Transformer instance for general data transformations.

    Example:
        Create a stage with a custom transformer::

            stage = Stage(
                class_name="tech.mta.seeknal.transformers.ColumnRenamed",
                params={"inputCol": "old_name", "outputCol": "new_name"}
            )

        Create a stage referencing an existing transformation::

            stage = Stage(id="my_predefined_transform")
    """

    id: Optional[str] = None
    class_name: Optional[str] = None
    params: Optional[dict] = None
    feature: Optional[FeatureTransformer] = None
    aggregator: Optional[Aggregator] = None
    transformer: Optional[Transformer] = None


@dataclass
class Stages:
    """Container for multiple pipeline stages.

    A Stages object holds a collection of Stage instances that can be added
    to a SparkEngineTask pipeline as a batch. This is useful for grouping
    related transformations together or reusing common transformation sequences.

    Attributes:
        stages: List of Stage instances to be executed in order.

    Example:
        Create a reusable set of stages::

            common_stages = Stages(stages=[
                Stage(transformer=ColumnRenamed(inputCol="id", outputCol="user_id")),
                Stage(transformer=FilterByExpr(expression="status = 'active'"))
            ])

            task = SparkEngineTask(name="my_task")
            task.add_stages(common_stages)
    """

    stages: List[Stage]


@dataclass
class SparkEngineTask(Task):
    """A Spark-based data transformation task using the Spark Engine framework.

    SparkEngineTask provides a fluent interface for building data transformation
    pipelines that run on Apache Spark. It supports various input sources (Hive tables,
    files, DataFrames), transformation stages (SQL, aggregations, custom transformers),
    and output destinations.

    The task follows a builder pattern where methods return `self` for chaining::

        result = (
            SparkEngineTask(name="my_pipeline")
            .add_input(table="database.source_table")
            .set_date_col(date_col="event_date", date_pattern="yyyyMMdd")
            .add_sql("SELECT * FROM __THIS__ WHERE amount > 0")
            .add_stage(transformer=ColumnRenamed(inputCol="id", outputCol="user_id"))
            .transform()
        )

    Attributes:
        name: Optional name identifier for the task.
        description: Optional human-readable description of the task's purpose.
        feature: Optional feature configuration dictionary for feature engineering.
        default_input_db: Default database name for input sources (default: "base_input").
        default_output_db: Default database name for output destinations (default: "base_output").
        _materialize: Internal flag indicating if transformation results were materialized.
        is_spark_job: Flag indicating this is a Spark-based job (always True).
        stages: List of transformation stage configurations to be applied in order.
        kind: Task type identifier (always "SparkEngineTask").

    Example:
        Basic transformation pipeline::

            from seeknal.tasks.sparkengine import SparkEngineTask
            from seeknal.tasks.sparkengine import transformers as T

            task = SparkEngineTask(name="process_data", description="Process daily data")
            result = (
                task
                .add_input(table="raw_db.events")
                .set_date_col("event_date")
                .add_filter_by_expr("status = 'completed'")
                .add_new_column("amount * 1.1", "adjusted_amount")
                .transform()
            )
            result.show()

        Pipeline with aggregation::

            from seeknal.tasks.sparkengine import aggregators as G

            agg = G.Aggregator(
                group_by_cols=["user_id", "date"],
                aggregators=[
                    G.AggregatorFunction(
                        class_name="tech.mta.seeknal.aggregators.FunctionAggregator",
                        params={"inputCol": "amount", "outputCol": "total", "accumulatorFunction": "sum"}
                    )
                ]
            )

            result = (
                SparkEngineTask(name="aggregate_data")
                .add_input(table="raw_db.transactions")
                .add_stage(aggregator=agg)
                .transform()
            )

    See Also:
        - :class:`Stage`: Individual transformation step configuration.
        - :class:`Stages`: Container for multiple stages.
        - :mod:`seeknal.tasks.sparkengine.transformers`: Available transformers.
        - :mod:`seeknal.tasks.sparkengine.aggregators`: Available aggregators.
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
        """Set the date column for date-based filtering and partitioning.

        Configures which column in the input dataset represents dates and
        the format pattern used by that column. This is used for date-based
        filtering and data selection operations.

        Args:
            date_col: Name of the column containing date values.
            date_pattern: Date format pattern (e.g., "yyyyMMdd", "yyyy-MM-dd").
                Defaults to "yyyyMMdd".

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Example:
            ::

                task = SparkEngineTask(name="my_task")
                task.set_date_col(date_col="event_date", date_pattern="yyyy-MM-dd")
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
        """Set the default input database for the pipeline.

        Configures the default database name to use when resolving
        input table references that don't specify a database.

        Args:
            db_name: Name of the default input database.

        Returns:
            SparkEngineTask: The current instance for method chaining.
        """
        self.default_input_db = db_name
        return self

    def set_default_output_db(self, db_name: str):
        """Set the default output database for the pipeline.

        Configures the default database name to use when writing
        output to tables that don't specify a database.

        Args:
            db_name: Name of the default output database.

        Returns:
            SparkEngineTask: The current instance for method chaining.
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
        """Add an input source to the transformation pipeline.

        Configures where the pipeline reads its input data from. Supports
        multiple input types including Hive tables, file-based sources,
        custom extractors, and existing Spark DataFrames.

        Args:
            id: Reference ID of a pre-defined source in the project configuration.
            table: Fully qualified Hive table name (e.g., "database.table_name").
            source: Source type identifier (e.g., "file", "jdbc", "hive").
            params: Parameters dictionary for the source connector (required with source).
            extractor: An Extractor object defining the input configuration.
            dataframe: An existing Spark DataFrame to use as input.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Raises:
            ValueError: If none of id, table, source, extractor, or dataframe is provided.
            ValueError: If source is provided without params.

        Example:
            Define table as input::

                SparkEngineTask(name="create_dataset")
                    .add_input(table="telco_sample.db_charging_hourly")

            Define connector as input::

                SparkEngineTask(name="create_dataset")
                    .add_input(source="file", params={"format": "parquet", "path": "path/to/parquet"})

            Define connector with Extractor object::

                SparkEngineTask(name="create_dataset")
                    .add_input(extractor=Extractor(source="file", params={...}))

            Define input using dataframe::

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
        """Add multiple stages to the pipeline at once.

        Appends all stages from a Stages container to this task's pipeline.
        Stages are added in the order they appear in the container.

        Args:
            stages: A Stages object containing the stages to append.

        Returns:
            SparkEngineTask: The current instance for method chaining.
        """
        for s in stages.stages:
            self.add_stage(stage=s)
        return self

    def add_sql(self, statement: str):
        """Add a SQL transformation stage to the pipeline.

        Adds a stage that executes a SQL statement on the current dataset.
        Use ``__THIS__`` as a placeholder for the current dataset in the SQL.

        Args:
            statement: SQL statement to execute. Use ``__THIS__`` to reference
                the current dataset.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Example:
            ::

                task.add_sql("SELECT id, name, amount * 2 as doubled FROM __THIS__ WHERE status = 'active'")
        """
        self.add_stage(transformer=SQL(statement=statement))
        return self

    def add_new_column(self, expression: str, output_col: str):
        """Add a new column computed from an expression.

        Creates a new column in the dataset based on a SQL expression.
        The expression can reference existing columns.

        Args:
            expression: SQL expression to compute the new column value.
            output_col: Name of the new column to create.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Example:
            ::

                task.add_new_column("price * quantity", "total_amount")
                task.add_new_column("CONCAT(first_name, ' ', last_name)", "full_name")
        """
        self.add_stage(
            transformer=AddColumnByExpr(expression=expression, outputCol=output_col)
        )
        return self

    def add_filter_by_expr(self, expression: str):
        """Add a filter stage using a SQL expression.

        Filters the dataset to include only rows that satisfy the
        given SQL expression condition.

        Args:
            expression: SQL boolean expression for filtering rows.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Example:
            ::

                task.add_filter_by_expr("status = 'active' AND amount > 100")
        """
        self.add_stage(transformer=FilterByExpr(expression=expression))
        return self

    def select_columns(self, columns: List[str]):
        """Select specific columns from the dataset.

        Reduces the dataset to only include the specified columns,
        dropping all others.

        Args:
            columns: List of column names to retain in the dataset.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Example:
            ::

                task.select_columns(["user_id", "name", "email"])
        """
        self.add_stage(
            transformer=Transformer(
                SparkEngineClassName.SELECT_COLUMNS, inputCols=columns
            )
        )
        return self

    def drop_columns(self, columns: List[str]):
        """Remove specific columns from the dataset.

        Drops the specified columns from the dataset, keeping all others.

        Args:
            columns: List of column names to remove from the dataset.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Example:
            ::

                task.drop_columns(["temp_column", "debug_info"])
        """
        self.add_stage(
            transformer=Transformer(SparkEngineClassName.DROP_COLS, inputCols=columns)
        )
        return self

    def update_stage(self, number: int, **kwargs):
        """Update an existing stage in the pipeline.

        Replaces a stage at the specified index with a new stage
        configuration. The new stage is built from the provided keyword
        arguments using the same format as :meth:`add_stage`.

        Args:
            number: Zero-based index of the stage to update.
            **kwargs: Stage configuration arguments (same as :meth:`add_stage`).

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Raises:
            Exception: If no stages are defined in the pipeline.
        """
        if self.stages is not None:
            _temp = SparkEngineTask().add_stage(**kwargs)
            self.stages[number] = _temp.stages[0]
        else:
            raise Exception("No stages defined")

        return self

    def remove_stage(self, number: int):
        """Remove a stage from the pipeline.

        Removes the stage at the specified index from the pipeline.
        Subsequent stages shift down to fill the gap.

        Args:
            number: Zero-based index of the stage to remove.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Raises:
            Exception: If no stages are defined in the pipeline.
        """
        if self.stages is not None:
            self.stages.pop(number)
        else:
            raise Exception("No stages defined")

        return self

    def insert_stage(self, number: int, **kwargs):
        """Insert a new stage at a specific position in the pipeline.

        Inserts a stage at the specified index, shifting subsequent stages
        to make room. The new stage is built from the provided keyword
        arguments using the same format as :meth:`add_stage`.

        Args:
            number: Zero-based index where the new stage will be inserted.
            **kwargs: Stage configuration arguments (same as :meth:`add_stage`).

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Raises:
            Exception: If no stages are defined in the pipeline.
        """
        if self.stages is not None:
            _temp = SparkEngineTask().add_stage(**kwargs)
            self.stages.insert(number, _temp.stages[0])
        else:
            raise Exception("No stages defined")

        return self

    def _create_spec_from_attributes(self):
        """Create a task specification dictionary from current attributes.

        Returns:
            dict: Task specification containing pipeline configuration.
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
        """Configure the pipeline from a YAML string.

        Parses a YAML string containing pipeline configuration and applies
        it to this task. This allows defining pipelines in YAML format
        for easier configuration management.

        Args:
            yaml_string: YAML-formatted string containing pipeline configuration.

        Returns:
            SparkEngineTask: The current instance for method chaining.
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
        """Configure the pipeline from a YAML file.

        Reads a YAML file containing pipeline configuration and applies
        it to this task.

        Args:
            yaml_path: Path to the YAML configuration file.

        Returns:
            SparkEngineTask: The current instance for method chaining.
        """
        with open(yaml_path, "r") as f:
            task_spec = f.read()
        self.add_yaml(task_spec)
        return self

    def to_yaml_file(self, yaml_path: str):
        """Save the current pipeline configuration to a YAML file.

        Exports the task's pipeline configuration to a YAML file,
        which can be loaded later or used for documentation.

        Args:
            yaml_path: Path where the YAML file will be saved.
        """
        task_spec = self._create_spec_from_attributes()
        with open(yaml_path, "w") as f:
            f.write(yaml.dump(task_spec))

    def print_yaml(self):
        """Print the current pipeline configuration as YAML to console.

        Outputs the task's pipeline configuration in YAML format,
        useful for debugging and documentation.

        Returns:
            SparkEngineTask: The current instance for method chaining.
        """
        task_spec = self._create_spec_from_attributes()
        typer.echo(yaml.dump(task_spec))
        return self

    def add_common_yaml(self, common_config: str):
        """Set common configuration from a YAML string.

        Adds shared configuration (such as database connections, common
        parameters) that can be referenced by the pipeline.

        Args:
            common_config: YAML-formatted string with common configuration.

        Returns:
            SparkEngineTask: The current instance for method chaining.
        """
        self.common = common_config
        return self

    def add_common_yaml_file(self, common_config_path: str):
        """Load common configuration from a YAML file.

        Reads shared configuration from a file and applies it to the task.

        Args:
            common_config_path: Path to the common configuration YAML file.

        Returns:
            SparkEngineTask: The current instance for method chaining.
        """
        with open(common_config_path, "r") as f:
            common_config = f.read()
        self.add_common_yaml(common_config)
        return self

    def print_common_yaml(self):
        """Print the common configuration YAML to console.

        Outputs the task's common configuration in YAML format.

        Returns:
            SparkEngineTask: The current instance for method chaining.
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
        """Define the output destination for the pipeline transformation.

        Configures where the pipeline writes its transformed data. Supports
        multiple output types including Hive tables, file-based outputs,
        and custom loaders.

        Args:
            id: Reference ID of a pre-defined output in the project configuration.
            table: Fully qualified Hive table name for output (e.g., "database.table").
            partitions: List of column names to use for partitioning the output.
            path: Filesystem path for the output data.
            source: Output type identifier (e.g., "file", "jdbc").
            params: Parameters dictionary for the output connector (required with source).
            repartition: Number of partitions for the output DataFrame before writing.
            loader: A Loader object defining the output configuration.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Raises:
            ValueError: If none of id, table, source, or loader is provided.
            ValueError: If source is provided without params.

        Example:
            Define table as output::

                SparkEngineTask(name="create_dataset")
                    .add_output(table="db.output_table", partitions=["date_id"], path="path/to/loc")

            Define connector as output::

                SparkEngineTask(name="create_dataset")
                    .add_output(source="file", params={"format": "parquet", "path": "path/to/parquet"})

            Define connector with Loader object::

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
        """Execute the transformation pipeline and return the result.

        Runs all configured stages in the pipeline on the input data and
        returns the transformed DataFrame. Optionally materializes (writes)
        the results to the configured output destination.

        Args:
            spark: Optional SparkSession instance. If None, uses the active session.
            chain: If True, enables chaining behavior (reserved for future use).
            materialize: If True, writes results to the output destination and
                returns the task instance for further operations.
            params: Dictionary of inline parameters passed to the Spark Engine job.
            filters: Dictionary of filters to apply to the pipeline.
            date: Single date or list of dates for date-based data selection.
            start_date: Start date for date range filtering.
            end_date: End date for date range filtering.

        Returns:
            DataFrame: The transformed Spark DataFrame when materialize=False.
            SparkEngineTask: The current instance when materialize=True.

        Example:
            Get transformed DataFrame::

                df = task.transform()
                df.show()

            Materialize and retrieve output::

                task.transform(materialize=True)
                output_df = task.get_output_dataframe()
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
        """Evaluate and display transformation results for testing.

        Runs the transformation pipeline and displays the results to the
        console. Optionally applies an additional SQL statement before
        displaying.

        Args:
            statement: Optional SQL statement to apply to the transformed data
                before display. Use ``__THIS__`` to reference the dataset.
            maxResults: Maximum number of rows to display (default: 10).
            truncate: If True, truncates long column values in display.
            params: Dictionary of inline parameters for the Spark Engine job.
            filters: Dictionary of filters to apply to the pipeline.
            date: Date or date list for date-based data selection.

        Returns:
            SparkEngineTask: The current instance for method chaining.

        Example:
            ::

                task.evaluate()  # Show first 10 rows
                task.evaluate(maxResults=20)  # Show first 20 rows
                task.evaluate(statement="SELECT COUNT(*) FROM __THIS__")  # Show count
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
        """Retrieve the output DataFrame after materialization.

        Reads the materialized output data from the configured output
        destination and returns it as a DataFrame. Only works after
        calling :meth:`transform` with ``materialize=True``.

        Returns:
            DataFrame: The materialized output as a Spark DataFrame.
            None: If the transformation was not materialized.

        Note:
            Call this method after :meth:`transform(materialize=True)`
            to access the written data.
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
        """Display the materialized output DataFrame to console.

        Shows the contents of the materialized output data. Only works
        after calling :meth:`transform` with ``materialize=True``.

        Args:
            max_results: Maximum number of rows to display (default: 10).
        """
        res = self.get_output_dataframe()
        if res is not None:
            res.show(max_results)

    def is_materialized(self) -> bool:
        """Check if the transformation results have been materialized.

        Returns:
            bool: True if :meth:`transform` was called with ``materialize=True``
                and the results were written to the output destination.
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
        """Create a deep copy of this SparkEngineTask.

        Creates a new SparkEngineTask instance with the same configuration.
        If the input contains a DataFrame, it is converted to a temporary
        table reference to enable copying.

        Returns:
            SparkEngineTask: A new instance with copied configuration.
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
