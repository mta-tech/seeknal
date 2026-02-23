from datetime import datetime
from typing import Optional
from sqlmodel import Field, SQLModel, MetaData

# Create a single metadata instance
metadata = MetaData()
SQLModel.metadata = metadata

class ProjectTable(SQLModel, table=True):
    __tablename__: str = "project"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    description: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class WorkspaceTable(SQLModel, table=True):
    __tablename__: str = "workspace"
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    name: str
    description: str
    private: bool
    default_input_db: Optional[str] = None
    default_output_db: Optional[str] = None
    offline_store_id: Optional[int] = None
    online_store_id: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class EntityTable(SQLModel, table=True):
    __tablename__: str = "entity"
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    description: Optional[str] = None
    join_keys: str
    pii_keys: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class SourceTable(SQLModel, table=True):
    __tablename__: str = "source"
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    entity_id: Optional[int] = None
    name: str
    params: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class TransformationTable(SQLModel, table=True):
    __tablename__: str = "transformation"
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    name: str
    class_name: str
    params: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class RuleTable(SQLModel, table=True):
    __tablename__: str = "rule"
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    name: str
    params: str
    description: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class SessionTable(SQLModel, table=True):
    __tablename__: str = "session"
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    name: str
    description: str
    spec: Optional[str] = None
    seeds: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class FlowTable(SQLModel, table=True):
    __tablename__: str = "flow"
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    name: str
    description: str
    spec: Optional[str] = None
    author: Optional[str] = None
    last_run: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class OfflineWatermarkTable(SQLModel, table=True):
    __tablename__: str = "offline_watermark"
    id: Optional[int] = Field(default=None, primary_key=True)
    feature_group_id: int = Field(foreign_key="feature_group.id")
    feature_group_version_id: int = Field(foreign_key="feature_group_version.id")
    date: datetime
    created_at: datetime = Field(default_factory=datetime.now)

class OfflineStoreTable(SQLModel, table=True):
    __tablename__: str = "offline_store"
    id: Optional[int] = Field(default=None, primary_key=True)
    kind: str
    params: str
    name: Optional[str] = None

class OnlineStoreTable(SQLModel, table=True):
    __tablename__: str = "online_store"
    id: Optional[int] = Field(default=None, primary_key=True)
    kind: str
    params: str
    name: Optional[str] = None

class FeatureGroupTable(SQLModel, table=True):
    __tablename__: str = "feature_group"
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    entity_id: int = Field(foreign_key="entity.id")
    name: str
    description: str
    offline: bool
    online: bool
    materialization_params: str
    flow_id: Optional[int] = Field(foreign_key="flow.id")
    author: str
    online_watermark: Optional[str] = None
    offline_store: Optional[int] = None
    online_store: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class FeatureGroupVersionTable(SQLModel, table=True):
    __tablename__: str = "feature_group_version"
    id: Optional[int] = Field(default=None, primary_key=True)
    feature_group_id: int = Field(foreign_key="feature_group.id")
    version: int
    avro_schema: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class FeatureTable(SQLModel, table=True):
    __tablename__: str = "features"
    id: Optional[int] = Field(default=None, primary_key=True)
    project_id: int = Field(foreign_key="project.id")
    feature_group_id: int = Field(foreign_key="feature_group.id")
    feature_group_version_id: int = Field(foreign_key="feature_group_version.id")
    name: str
    datatype: str
    online_datatype: str
    description: str
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)

class OnlineTable(SQLModel, table=True):
    __tablename__: str = "online_table"
    id: Optional[int] = Field(default=None, primary_key=True)
    entity_id: int = Field(foreign_key="entity.id")
    project_id: int = Field(foreign_key="project.id")
    online_store_id: int = Field(foreign_key="online_store.id")
    description: str
    name: str
    author: str
    delete_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.now)

class FeatureGroupOnlineTable(SQLModel, table=True):
    __tablename__: str = "feature_group_online_table"
    id: Optional[int] = Field(default=None, primary_key=True)
    online_table_id: int = Field(foreign_key="online_table.id")
    feature_group_id: int = Field(foreign_key="feature_group.id")
    features: Optional[str] = None
    exclude_features: Optional[str] = None

class OnlineWatermarkTable(SQLModel, table=True):
    __tablename__: str = "online_watermark"
    id: Optional[int] = Field(default=None, primary_key=True)
    online_table_id: int = Field(foreign_key="online_table.id")
    date: datetime
    created_at: datetime = Field(default_factory=datetime.now)

class WorkspaceSourceTable(SQLModel, table=True):
    __tablename__: str = "workspace_source"
    id: Optional[int] = Field(default=None, primary_key=True)
    workspace_id: int = Field(foreign_key="workspace.id")
    source_id: int = Field(foreign_key="source.id")

class WorkspaceRuleTable(SQLModel, table=True):
    __tablename__: str = "workspace_rule"
    id: Optional[int] = Field(default=None, primary_key=True)
    workspace_id: int = Field(foreign_key="workspace.id")
    rule_id: int = Field(foreign_key="rule.id")

class WorkspaceTransformationTable(SQLModel, table=True):
    __tablename__: str = "workspace_transformation"
    id: Optional[int] = Field(default=None, primary_key=True)
    workspace_id: int = Field(foreign_key="workspace.id")
    transformation_id: int = Field(foreign_key="transformation.id")

class WorkspaceFlowTable(SQLModel, table=True):
    __tablename__: str = "workspace_flow"
    id: Optional[int] = Field(default=None, primary_key=True)
    workspace_id: int = Field(foreign_key="workspace.id")
    flow_id: int = Field(foreign_key="flow.id")

class WorkspaceFeatureGroupTable(SQLModel, table=True):
    __tablename__: str = "workspace_feature_group"
    id: Optional[int] = Field(default=None, primary_key=True)
    workspace_id: int = Field(foreign_key="workspace.id")
    feature_group_id: int = Field(foreign_key="feature_group.id")

class WorkspaceOnlineTable(SQLModel, table=True):
    __tablename__: str = "workspace_online_table"
    id: Optional[int] = Field(default=None, primary_key=True)
    workspace_id: int = Field(foreign_key="workspace.id")
    online_table_id: int = Field(foreign_key="online_table.id")


class ReplSourceTable(SQLModel, table=True):
    """Data source connection storage for REPL with encrypted credentials."""

    __tablename__: str = "repl_source"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)
    source_type: str  # postgres, mysql, sqlite, parquet, csv

    # URL with password masked (for display)
    masked_url: str

    # Encrypted sensitive parts (password, auth tokens)
    encrypted_credentials: Optional[str] = None

    # Non-sensitive connection parts
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None

    # Metadata
    workspace_id: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
