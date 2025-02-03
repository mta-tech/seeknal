from dataclasses import dataclass
from typing import Optional, List, Dict
from pydantic import BaseModel
import psycopg2
from pyspark.sql import DataFrame, SparkSession

class PostgreSQLUpdateConfig(BaseModel):
    """PostgreSQL update configuration

    Attributes:
        uri: PostgreSQL connection URI
        table: Target table name
        update_columns: List of columns to update
        id_column: Column to use in WHERE clause
    """
    uri: str
    table: str
    update_columns: List[str]
    id_column: str = "id"

class PostgreSQLUpdater:
    """PostgreSQL data updater for Seeknal
    
    This transformer allows updating multiple columns in PostgreSQL tables using Spark DataFrame data.
    
    Example:
        ```python
        config = PostgreSQLUpdateConfig(
            uri="postgresql://user:pass@host:port/db",
            table="widget_data",
            update_columns=["widget_data", "status", "last_updated"],
            id_column="id"
        )
        
        updater = PostgreSQLUpdater(config=config)
        updater.transform(df)
        ```
    """
    
    def __init__(self, config: PostgreSQLUpdateConfig):
        self.config = config
        
    def _build_update_query(self) -> str:
        """Build the SQL UPDATE query with multiple columns"""
        set_clause = ", ".join(f"{col} = %s" for col in self.config.update_columns)
        return f"""
        UPDATE {self.config.table}
        SET {set_clause}
        WHERE {self.config.id_column} = %s
        """
        
    def _update_partition(self, partition):
        """Update PostgreSQL records within each Spark partition"""
        connection = psycopg2.connect(self.config.uri)
        cursor = connection.cursor()
        
        update_query = self._build_update_query()
        
        try:
            for row in partition:
                try:
                    # Prepare values for all update columns
                    update_values = []
                    for col in self.config.update_columns:
                        value = row[col]
                        # Handle string escaping if needed
                        if isinstance(value, str):
                            value = value.replace("'", '"')
                        update_values.append(value)
                    
                    # Add the ID value for the WHERE clause
                    update_values.append(row[self.config.id_column])
                    
                    cursor.execute(update_query, tuple(update_values))
                except Exception as e:
                    print(f"Error updating {row[self.config.id_column]}: {str(e)}")
            
            connection.commit()
        finally:
            cursor.close()
            connection.close()
    
    def transform(self, df: DataFrame) -> DataFrame:
        """Transform method to update PostgreSQL database
        
        Args:
            df: Input DataFrame containing data to update
            
        Returns:
            The input DataFrame (unchanged)
        """
        # Ensure required columns exist
        required_cols = set(self.config.update_columns) | {self.config.id_column}
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Apply updates to each partition
        df.rdd.foreachPartition(self._update_partition)
        
        return df