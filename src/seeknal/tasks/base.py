from typing import List, Optional, Union
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from dataclasses import dataclass
from pyarrow import Table


@dataclass
class Task(ABC):
    """Abstract base class for data processing tasks.

    Task defines the interface for all data processing tasks in seeknal.
    It provides a common structure for defining inputs, transformation stages,
    and outputs. Subclasses implement specific task types (e.g., SparkTask,
    DuckDBTask) with their respective execution engines.

    A task follows a pipeline pattern where data flows through:
    1. Input configuration (add_input)
    2. Transformation stages (transform)
    3. Output configuration

    Attributes:
        is_spark_job: Whether this task runs as a Spark job. If True, the task
            requires a SparkSession for execution.
        kind: The type identifier for this task (e.g., 'spark', 'duckdb').
            Used for serialization and task routing.
        name: Optional name for the task, used for identification and logging.
        description: Optional description explaining the task's purpose.
        common: Optional path to a common YAML configuration file containing
            shared definitions like table schemas or transformations.
        input: Optional dictionary containing input configuration, including
            data sources like Hive tables or DataFrames.
        stages: Optional list of transformation stage dictionaries, each
            defining a step in the data processing pipeline.
        output: Optional dictionary containing output configuration, specifying
            where and how to write the processed data.
        date: Optional date string for time-based filtering or partitioning
            of data during processing.

    Example:
        Subclasses implement specific task types::

            @dataclass
            class SparkTask(Task):
                is_spark_job: bool = True
                kind: str = "spark"

                def add_input(self, hive_table=None, dataframe=None):
                    # Implementation for Spark inputs
                    ...

                def transform(self, spark, chain=True, ...):
                    # Spark transformation logic
                    ...
    """

    is_spark_job: bool
    kind: str
    name: Optional[str] = None
    description: Optional[str] = None
    common: Optional[str] = None
    input: Optional[dict] = None
    stages: Optional[List[dict]] = None
    output: Optional[dict] = None
    date: Optional[str] = None

    @abstractmethod
    def add_input(
        self,
        hive_table: Optional[str] = None,
        dataframe: Optional[Union[DataFrame, Table]] = None,
    ):
        """Add an input data source to the task.

        Configures the task with input data from either a Hive table or
        an in-memory DataFrame. This method supports method chaining to
        allow fluent task configuration.

        Args:
            hive_table: The fully qualified Hive table name (database.table)
                to read data from. Mutually exclusive with dataframe.
            dataframe: A PySpark DataFrame or PyArrow Table containing
                the input data. Mutually exclusive with hive_table.

        Returns:
            Task: The current task instance for method chaining.

        Example:
            Configure input from a Hive table::

                task.add_input(hive_table="mydb.customers")

            Configure input from a DataFrame::

                df = spark.read.parquet("/data/customers")
                task.add_input(dataframe=df)
        """
        return self

    @abstractmethod
    def transform(
        self,
        spark: Optional[SparkSession],
        chain: bool = True,
        materialize: bool = False,
        params=None,
        filters=None,
        date=None,
        start_date=None,
        end_date=None,
    ):
        """Execute the task's transformation stages on the input data.

        Processes the input data through all configured transformation stages
        and returns the resulting DataFrame. The transformation can optionally
        be filtered by date ranges.

        Args:
            spark: SparkSession instance for executing Spark operations.
                Required for Spark-based tasks, may be None for other task types.
            chain: If True, stages are executed sequentially with each stage
                receiving the output of the previous stage. If False, each stage
                operates on the original input independently.
            materialize: If True, intermediate results are materialized to
                improve performance for complex transformations with multiple
                downstream operations.
            params: Optional dictionary of parameters to pass to transformation
                stages, allowing dynamic configuration of transformations.
            filters: Optional dictionary of filter conditions to apply to
                the data during transformation.
            date: Optional date string for single-date filtering.
            start_date: Optional start date for date range filtering.
                Used with end_date to define a date range.
            end_date: Optional end date for date range filtering.
                Used with start_date to define a date range.

        Returns:
            DataFrame: The transformed data as a PySpark DataFrame or
                equivalent data structure depending on the task type.

        Example:
            Execute transformation with date filtering::

                result = task.transform(
                    spark=spark,
                    start_date="2024-01-01",
                    end_date="2024-01-31"
                )
        """
        pass

    @abstractmethod
    def add_common_yaml(self, common_yaml: str):
        """Add a common YAML configuration file to the task.

        Loads shared configuration definitions from a YAML file, such as
        table schemas, column mappings, or reusable transformation templates.
        This enables configuration reuse across multiple tasks.

        Args:
            common_yaml: The filesystem path to the common YAML configuration
                file containing shared definitions.

        Returns:
            Task: The current task instance for method chaining.

        Example:
            Load common definitions::

                task.add_common_yaml("/config/common_schemas.yaml")
        """
        return self
