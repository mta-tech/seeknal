from typing import List, Optional

from pydantic import BaseModel, model_validator


class Loader(BaseModel):
    """
    Loader base class. This class use for define output for data transformation

    Attributes:
        id (str, optional): reference id referred common source
        table (str, optional): table name
        source (str, optional): source name
        params (dict, optional): parameters for the specified target
        repartition (int, optional): set number of partitions when write the data from the target
        limit (int, optional): set number of limit records when write the data from the target
        partitions (List[str], optional): set which columns become partition columns when writing it as hive table
        path (str, optional): set location where the hive table write into
        connId (str, optional): set connection id
    """

    id: Optional[str] = None
    table: Optional[str] = None
    source: Optional[str] = None
    params: Optional[dict] = None
    repartition: Optional[int] = None
    limit: Optional[int] = None
    partitions: Optional[List[str]] = None
    path: Optional[str] = None
    connId: Optional[str] = None

#    @model_validator(mode='after')
#    def _set_fields(self) -> 'Loader':
#        """This is a validator that sets the field values based on the
#        the user's input.
#
#        Returns:
#            Loader: The updated Loader instance with transformed fields.
#        """
#        if self.hive_table is not None:
#            self.table = self.hive_table
#            self.hive_table = None
#        if self.conn_id is not None:
#            self.connId = self.conn_id
#            self.conn_id = None
#        return self
