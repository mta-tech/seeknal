from dataclasses import dataclass
from typing import Optional, Any, List, Dict
import pandas as pd
from pydantic import BaseModel
from sqlalchemy import create_engine
from pyspark.sql import DataFrame, SparkSession
from urllib.parse import quote_plus

class PostgreSQLLoaderConfig(BaseModel):
    """PostgreSQL loader configuration

    You can connect using either individual parameters or a connection URI.
    If uri is provided, it will take precedence over individual parameters.

    URI format: postgresql://user:password@host:port/database

    Attributes:
        uri: PostgreSQL connection URI (optional)
        host: PostgreSQL host (if not using URI)
        port: PostgreSQL port (if not using URI)
        database: Database name (if not using URI)
        user: Database user (if not using URI)
        password: Database password (if not using URI)
        schema: Database schema (default: public)
        table: Target table name
        if_exists: What to do if table exists ('fail', 'replace', 'append')
        batch_size: Number of rows per batch for writing
        index: Whether to write DataFrame index as a column
        connect_args: Additional connection arguments (optional)
    """
    uri: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = 5432
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: str = "public"
    table: str
    if_exists: str = "fail"
    batch_size: int = 10000
    index: bool = False
    connect_args: Optional[Dict[str, Any]] = None

    def get_connection_string(self) -> str:
        """Generate PostgreSQL connection string

        Returns URI if provided, otherwise builds it from individual parameters.
        """
        if self.uri:
            return self.uri
        
        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError(
                "Must provide either 'uri' or all of: 'host', 'database', 'user', 'password'"
            )
        
        # URL encode password to handle special characters
        encoded_password = quote_plus(self.password)
        
        return f"postgresql://{self.user}:{encoded_password}@{self.host}:{self.port}/{self.database}"

@dataclass
class PostgreSQLLoader:
    """PostgreSQL data loader for Seeknal

    This loader allows writing Spark DataFrames to PostgreSQL tables
    with configurable batch size and existing table handling.

    Example:
        ```python
        # Connect using URI
        config = PostgreSQLLoaderConfig(
            uri="postgresql://user:pass@localhost:5432/mydb",
            table="customer_features",
            if_exists="append"
        )

        # Or connect using individual parameters
        config = PostgreSQLLoaderConfig(
            host="localhost",
            port=5432,
            database="mydb",
            user="user",
            password="pass",
            table="customer_features",
            if_exists="append"
        )

        loader = PostgreSQLLoader(config=config)
        loader(df)
        ```
    """
    config: PostgreSQLLoaderConfig

    def load(self, df: DataFrame) -> None:
        """Load Spark DataFrame to PostgreSQL

        Args:
            df: Spark DataFrame to write to PostgreSQL
        """
        # Convert Spark DataFrame to pandas
        pdf = df.toPandas()
        
        # Create SQLAlchemy engine with optional connect_args
        engine = create_engine(
            self.config.get_connection_string(),
            connect_args=self.config.connect_args or {}
        )
        
        # Write to PostgreSQL in batches
        pdf.to_sql(
            name=self.config.table,
            schema=self.config.schema,
            con=engine,
            if_exists=self.config.if_exists,
            index=self.config.index,
            chunksize=self.config.batch_size
        )

    def __call__(
        self,
        result: Optional[DataFrame] = None,
        spark: Optional[SparkSession] = None,
        write: bool = True,
        *args: Any,
        **kwds: Any,
    ) -> None:
        """Callable interface for the loader

        Args:
            result: Spark DataFrame to write
            spark: Optional SparkSession (not used in this loader)
            write: Whether to actually write the data
            *args: Additional positional arguments
            **kwds: Additional keyword arguments
        """
        if write and result is not None:
            self.load(result)
