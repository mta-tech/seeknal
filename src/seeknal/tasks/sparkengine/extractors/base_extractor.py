from typing import Optional

from pydantic import BaseModel, model_validator


class Extractor(BaseModel):
    """
    Extractor base class. This class use for define input for data transformation

    Attributes:
        id (str, optional): reference id referred common source
        table (str, optional): Hive table name
        source (str, optional): source name
        params (dict, optional): parameters for the specified source
        repartition (int, optional): set number of partitions when read the data from the source
        limit (int, optional): set number of limit records when read the data from the source
        connId (str, optional): connection ID for the data source
    """

    id: Optional[str] = None
    table: Optional[str] = None
    source: Optional[str] = None
    params: Optional[dict] = None
    repartition: Optional[int] = None
    limit: Optional[int] = None
    connId: Optional[str] = None

#    @model_validator(mode='after')
#    def _set_fields(self) -> 'Extractor':
#        """
#        This is a validator that sets the field values based on the
#        populate some other values
#
#        Returns:
#            Extractor: The updated Extractor instance
#        """
#        if self.hive_table is not None:
#            self.table = self.hive_table
#            self.hive_table = None
#        if self.conn_id is not None:
#            self.connId = self.conn_id
#            self.conn_id = None
#        return self
