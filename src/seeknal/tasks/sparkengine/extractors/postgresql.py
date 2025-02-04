from dataclasses import dataclass
from typing import Optional, Dict, Any
from urllib.parse import quote_plus

from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import create_engine
import pandas as pd
from pyspark.sql import SparkSession

class PostgreSQLConfig(BaseModel):
    """PostgreSQL connection configuration

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
        table: Table name
        query: Custom SQL query (optional)
        connect_args: Additional connection arguments (optional)
    """
    model_config = ConfigDict(arbitrary_types_allowed=True)

    uri: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = 5432
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    schema: str = "public"
    table: Optional[str] = None
    query: Optional[str] = None
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

class PostgreSQLExtractor:
    """PostgreSQL data extractor for Seeknal

    This extractor allows reading data from PostgreSQL tables or custom queries
    and converting them to Spark DataFrames.

    Example:
        ```python
        # Connect using URI
        config = PostgreSQLConfig(
            uri="postgresql://user:pass@localhost:5432/mydb",
            table="customers"
        )

        # Or connect using individual parameters
        config = PostgreSQLConfig(
            host="localhost",
            port=5432,
            database="mydb",
            user="user",
            password="pass",
            table="customers"
        )

        extractor = PostgreSQLExtractor(config=config)
        df = extractor(spark)
        ```
    """
    class_name = "PostgreSQLExtractor"

    def __init__(self, config: PostgreSQLConfig):
        self.config = config

    def extract(self, spark: SparkSession) -> Any:
        """Extract data from PostgreSQL and convert to Spark DataFrame

        Args:
            spark: SparkSession instance

        Returns:
            Spark DataFrame containing the extracted data
        """
        # Create SQLAlchemy engine with optional connect_args
        engine = create_engine(
            self.config.get_connection_string(),
            connect_args=self.config.connect_args or {}
        )

        # Determine SQL query
        if self.config.query:
            query = self.config.query
        else:
            if not self.config.table:
                raise ValueError("Either table or query must be specified")
            query = f"SELECT * FROM {self.config.schema}.{self.config.table}"

        # Read data using pandas (more efficient for PostgreSQL)
        pdf = pd.read_sql(query, engine)
        
        # Convert to Spark DataFrame
        return spark.createDataFrame(pdf)

    def __call__(self, spark: Optional[SparkSession] = None) -> Any:
        """Callable interface for the extractor

        Args:
            spark: Optional SparkSession. If not provided, will try to get active session.

        Returns:
            Spark DataFrame containing the extracted data
        """
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        return self.extract(spark)
