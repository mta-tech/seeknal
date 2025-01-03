from typing import List, Optional, Union
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from dataclasses import dataclass
from pyarrow import Table


@dataclass
class Task(ABC):

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
        pass

    @abstractmethod
    def add_common_yaml(self, common_yaml: str):
        return self
