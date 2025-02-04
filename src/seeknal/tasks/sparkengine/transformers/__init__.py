import json
from .base_transformer import FeatureTransformer, Transformer, ClassName
from .spark_engine_transformers import (
    SQL,
    AddWindowFunction,
    AddColumnByExpr,
    ColumnRenamed,
    WindowFunction,
    PointInTime,
    Time,
    FilterByExpr,
    JoinTablesByExpr,
    JoinType,
    TableJoinDef,
)
from .postgresql_updater import PostgreSQLUpdater, PostgreSQLUpdateConfig

def extract_widget_type(widget_data):
    try:
        if not widget_data:
            return ""
        if isinstance(widget_data, str):
            data = json.loads(widget_data)
        else:
            data = widget_data
            
        if isinstance(data, list) and len(data) > 0:
            return data[0].get("widget_type", "")
        elif isinstance(data, dict):
            return data.get("widget_type", "")
        return ""
    except Exception as e:
        return json.dumps({"error": str(e)})
