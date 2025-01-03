from typing import List, Optional
from ..base import Task
from dataclasses import dataclass, field
import duckdb
from pyarrow import Table
import os


@dataclass
class DuckDBTask(Task):

    is_spark_job: bool = False
    stages: List[dict] = field(default_factory=list)
    kind: str = "DuckDBTask"

    def __post_init__(self):
        self.is_spark_job = False
        self.kind = "DuckDBTask"
        duckdb.sql("INSTALL httpfs")
        duckdb.sql("LOAD httpfs")
        if os.getenv("S3_ENDPOINT") is not None:
            duckdb.sql("SET s3_endpoint='{}'".format(os.getenv("S3_ENDPOINT")))
            duckdb.sql(
                "SET s3_access_key_id='{}'".format(os.getenv("S3_ACCESS_KEY_ID", ""))
            )
            duckdb.sql(
                "SET s3_secret_access_key='{}'".format(
                    os.getenv("S3_SECRET_ACCESS_KEY", "")
                )
            )

    def add_input(self, dataframe: Optional[Table] = None, path: Optional[str] = None):
        if dataframe is not None:
            self.input = {"dataframe": dataframe}
        elif path is not None:
            self.input = {"path": path}
        return self

    def add_common_yaml(self, common_yaml: str):
        return self

    def add_sql(self, sql: str):
        self.stages.append({"sql": sql})
        return self

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
    ):
        if "dataframe" in self.input:
            temp_data = self.input["dataframe"]
        elif "path" in self.input:
            temp_data = duckdb.sql(
                "SELECT * FROM '{}'".format(self.input["path"])
            ).arrow()
        for query in self.stages:
            temp_data = duckdb.sql(
                query["sql"].replace("__THIS__", "temp_data")
            ).arrow()
        if params is not None:
            if params["return_as_pandas"] is True:
                return temp_data.to_pandas()
        return temp_data
