import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from box import BoxKeyError
from sqlmodel import Session as SQLSession
from sqlmodel import create_engine, delete, desc, select, update, or_
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from sqlalchemy.exc import OperationalError, TimeoutError

from .context import context, DEFAULT_SEEKNAL_DB_PATH
from .utils import check_is_dict_same
from .db_utils import (
    build_database_url,
    sanitize_database_exceptions,
    with_sanitized_exceptions,
    DatabaseSecurityError,
)
import libsql_experimental as libsql
from .models import *  # Import metadata first

# Build secure database URL based on configuration
if context.get("database"):
    if context.get("database").get("TURSO_DATABASE_URL") is not None:
        try:
            _turso_database_url = context.database.TURSO_DATABASE_URL
            _turso_auth_token = context.database.TURSO_AUTH_TOKEN
            secure_db_url = build_database_url(
                turso_database_url=_turso_database_url,
                turso_auth_token=_turso_auth_token,
            )
        except BoxKeyError:
            raise ValueError("Config TURSO_DATABASE_URL and TURSO_AUTH_TOKEN is required.")
    else:
        try:
            secure_db_url = build_database_url(
                local_db_path=context.database.SEEKNAL_DB_PATH,
            )
        except BoxKeyError:
            raise ValueError("Config SEEKNAL_DB_PATH is required.")
else:
    secure_db_url = build_database_url(
        default_db_path=DEFAULT_SEEKNAL_DB_PATH,
    )

# Wrap engine creation with sanitized exception handler to prevent credential leaks
try:
    with sanitize_database_exceptions():
        engine = create_engine(
            secure_db_url.get_connection_url(),
            connect_args={
                'check_same_thread': False,
                'timeout': 30  # Increase timeout to 30 seconds
            },
            pool_pre_ping=True,  # Add connection health check
            pool_recycle=3600,  # Recycle connections after 1 hour
            echo=False
        )
except DatabaseSecurityError:
    # Re-raise with sanitized message (credentials already masked)
    raise

@with_sanitized_exceptions()
@retry(
    stop=stop_after_attempt(5),  # Increase attempts from 3 to 5
    wait=wait_exponential(multiplier=2, min=4, max=30),  # Increase wait times
    retry=retry_if_exception_type((OperationalError, TimeoutError))
)
def initialize_database():
    """Initialize the database schema with sanitized error handling."""
    metadata.create_all(engine)

initialize_database()

@with_sanitized_exceptions()
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((OperationalError, TimeoutError))
)
def get_db_session():
    """Get a database session with sanitized error handling."""
    return SQLSession(engine)

@dataclass
class RequestFactory:

    body: dict = field(default_factory=dict)
    headers: dict = field(default_factory=dict)

    def __post_init__(self):
        pass


@dataclass
class ProjectRequest(RequestFactory):
    def __post_init__(self):
        pass

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(ProjectTable).where(ProjectTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def select_by_name(name):
        with get_db_session() as session:
            statement = select(ProjectTable).where(ProjectTable.name == name)
            return session.exec(statement).first()

    @staticmethod
    def select_all():
        with get_db_session() as session:
            statement = select(ProjectTable)
            return session.exec(statement).all()

    def save(self):
        def save_to_db():
            name = self.body.get("name")
            if name is None:
                raise ValueError("Project name is required.")
            description = self.body.get("description", "")
            check = ProjectRequest.select_by_name(name)
            if check is None:
                with get_db_session() as session:
                    project = ProjectTable(name=name, description=description)
                    session.add(project)
                    session.commit()
                    session.refresh(project)
                    return project.id
            else:
                with get_db_session() as session:
                    check.name = name
                    check.description = description
                    check.updated_at = datetime.now()
                    session.add(check)
                    session.commit()
                    session.refresh(check)
                    return check.id

        return save_to_db()


@dataclass
class WorkspaceRequest(RequestFactory):
    def __post_init__(self):
        pass

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(WorkspaceTable).where(WorkspaceTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def select_by_name_and_project_id(name, project_id):
        with get_db_session() as session:
            statement = select(WorkspaceTable).where(
                WorkspaceTable.name == name and WorkspaceTable.project_id == project_id
            )
            return session.exec(statement).first()

    @staticmethod
    def select_by_project_id():
        with get_db_session() as session:
            statement = select(WorkspaceTable).where(WorkspaceTable.private == False)
            return session.exec(statement).all()

    def save(self):
        def save_to_db():
            name = self.body.get("name")
            if name is None:
                raise ValueError("Workspace name is required.")
            description = self.body.get("description", "")
            project_id = context.project_id
            private = self.body.get("private", True)
            offline_store_id = self.body.get("offline_store_id", None)
            check = WorkspaceRequest.select_by_name_and_project_id(name, project_id)
            if check is None:
                with get_db_session() as session:
                    workspace = WorkspaceTable(
                        name=name,
                        description=description,
                        project_id=project_id,
                        private=private,
                    )
                    session.add(workspace)
                    session.commit()
                    session.refresh(workspace)
                    return workspace.id
            else:
                with get_db_session() as session:
                    check.name = name
                    check.description = description
                    check.private = private
                    check.offline_store_id = offline_store_id
                    check.updated_at = datetime.now()
                    session.add(check)
                    session.commit()
                    session.refresh(check)
                    return check.id

        return save_to_db()


@dataclass
class EntityRequest(RequestFactory):
    def __post_init__(self):
        pass

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(EntityTable).where(EntityTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def select_by_name(name):
        with get_db_session() as session:
            statement = select(EntityTable).where(EntityTable.name == name)
            return session.exec(statement).first()

    @staticmethod
    def select_all():
        with get_db_session() as session:
            statement = select(EntityTable)
            return session.exec(statement).all()

    def save(self):
        def save_to_db():
            name = self.body.get("name")
            if name is None:
                raise ValueError("Entity name is required.")
            description = self.body.get("description", "")
            join_keys = self.body.get("join_keys")
            if join_keys is None:
                raise ValueError("Entity join_keys is required.")
            pii_keys = self.body.get("pii_keys")
            if pii_keys is not None:
                pii_keys = ",".join(pii_keys) if pii_keys is not None else None
            check = EntityRequest.select_by_name(name)
            if check is None:
                with get_db_session() as session:
                    entity = EntityTable(
                        name=name,
                        description=description,
                        join_keys=",".join(join_keys),
                        pii_keys=pii_keys,
                    )
                    session.add(entity)
                    session.commit()
                    session.refresh(entity)
                    return entity.id
            else:
                with get_db_session() as session:
                    check.name = name
                    check.description = description
                    check.join_keys = ",".join(join_keys)
                    check.pii_keys = pii_keys
                    check.updated_at = datetime.now()
                    session.add(check)
                    session.commit()
                    session.refresh(check)
                    return check.id

        return save_to_db()


@dataclass
class SourceRequest(RequestFactory):
    @staticmethod
    def select_by_project_id(id):
        with get_db_session() as session:
            statement = (
                select(SourceTable)
                .join(WorkspaceSourceTable)
                .where(WorkspaceSourceTable.workspace_id == context.workspace_id)
                .where(SourceTable.project_id == id)
            )
            return session.exec(statement).all()

    @staticmethod
    def select_by_name(name):
        with get_db_session() as session:
            statement = (
                select(SourceTable)
                .join(WorkspaceSourceTable)
                .where(WorkspaceSourceTable.workspace_id == context.workspace_id)
                .where(SourceTable.name == name)
            )
            return session.exec(statement).first()

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(SourceTable).where(SourceTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def delete_by_id(id):
        with get_db_session() as session:
            # First delete workspace source mappings
            stm = (
                select(WorkspaceSourceTable)
                .where(WorkspaceSourceTable.source_id == id)
            )
            workspace_sources = session.exec(stm).all()
            for ws in workspace_sources:
                session.delete(ws)
            session.commit()

            # Then delete the source itself
            item = SourceRequest.select_by_id(id)
            if item:
                session.delete(item)
                session.commit()
            return True

    def save(self):
        def save_to_db():
            name = self.body.get("name")
            if name is None:
                raise ValueError("Source name is required.")
            details = self.body.get("details", None)
            if details is None:
                raise ValueError("Source details is required.")
            description = self.body.get("description", "")
            entity = self.body.get("entity")
            if entity is not None:
                entity_record = EntityRequest.select_by_name(entity.name)
                if entity_record is None:
                    raise ValueError("Entity {} does not exist.".format(entity.name))
                entity_id = entity_record.id
            else:
                entity_id = None

            details_dict = details.dict(exclude_none=True)

            check = SourceRequest.select_by_name(name)
            if check is None:
                with get_db_session() as session:
                    source = SourceTable(
                        name=name,
                        params=json.dumps(details_dict),
                        description=description,
                        project_id=context.project_id,
                        entity_id=entity_id,
                    )
                    session.add(source)
                    session.commit()
                    session.refresh(source)
                    workspace_map = WorkspaceSourceTable(
                        workspace_id=context.workspace_id, source_id=source.id
                    )
                    session.add(workspace_map)
                    session.commit()
                    session.refresh(workspace_map)
                    return source.id
            else:
                with get_db_session() as session:
                    check.name = name
                    check.params = json.dumps(details_dict)
                    check.description = description
                    check.entity_id = entity_id
                    check.updated_at = datetime.now()
                    session.add(check)
                    session.commit()
                    session.refresh(check)
                    return check.id

        return save_to_db()


@dataclass
class RuleRequest(RequestFactory):
    @staticmethod
    def select_by_project_id(id):
        with get_db_session() as session:
            statement = (
                select(RuleTable)
                .join(WorkspaceRuleTable)
                .where(RuleTable.project_id == id)
                .where(WorkspaceRuleTable.workspace_id == context.workspace_id)
            )
            return session.exec(statement).all()

    @staticmethod
    def select_by_name(name):
        with get_db_session() as session:
            statement = (
                select(RuleTable)
                .join(WorkspaceRuleTable)
                .where(RuleTable.name == name)
                .where(WorkspaceRuleTable.workspace_id == context.workspace_id)
            )
            return session.exec(statement).first()

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(RuleTable).where(RuleTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def delete_by_id(id):
        with get_db_session() as session:
            # First delete workspace mappings
            stm = (
                select(WorkspaceRuleTable)
                .where(WorkspaceRuleTable.rule_id == id)
                .where(WorkspaceRuleTable.workspace_id == context.workspace_id)
            )
            workspace_map = session.exec(stm).all()
            for w in workspace_map:
                session.delete(w)
            session.flush()  # Ensure workspace mappings are deleted

            # Then delete the rule
            item = session.get(RuleTable, id)
            if item:
                session.delete(item)
                session.commit()
                return True
            return False

    def save(self):
        def save_to_db():
            name = self.body.get("name")
            if name is None:
                raise ValueError("Source name is required.")

            value = self.body.get("value")
            value_dict = {"rule": {"value": value}}
            description = self.body.get("description", "")
            check = RuleRequest.select_by_name(name)
            if check is None:
                with get_db_session() as session:
                    source = RuleTable(
                        name=name,
                        params=json.dumps(value_dict),
                        description=description,
                        project_id=context.project_id,
                    )
                    session.add(source)
                    session.commit()
                    session.refresh(source)
                    workspace_map = WorkspaceRuleTable(
                        workspace_id=context.workspace_id, rule_id=source.id
                    )
                    session.add(workspace_map)
                    session.commit()
                    return source.id
            else:
                with get_db_session() as session:
                    check.name = name
                    check.params = json.dumps(value_dict)
                    check.description = description
                    check.updated_at = datetime.now()
                    session.add(check)
                    session.commit()
                    session.refresh(check)
                    return check.id

        return save_to_db()


class FlowRequest(RequestFactory):
    @staticmethod
    def select_by_project_id(id):
        with get_db_session() as session:
            statement = (
                select(FlowTable)
                .join(WorkspaceFlowTable)
                .where(WorkspaceFlowTable.workspace_id == context.workspace_id)
                .where(FlowTable.project_id == id)
            )
            return session.exec(statement).all()

    @staticmethod
    def select_by_name(name):
        with get_db_session() as session:
            statement = (
                select(FlowTable)
                .join(WorkspaceFlowTable)
                .where(FlowTable.name == name)
                .where(WorkspaceFlowTable.workspace_id == context.workspace_id)
            )
            return session.exec(statement).first()

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(FlowTable).where(FlowTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def delete_by_id(id):
        with get_db_session() as session:
            # First, get the flow
            item = session.get(FlowTable, id)
            if item is None:
                raise ValueError("Flow not found")

            try:
                # Clear flow_id references in feature groups first
                stm = update(FeatureGroupTable).where(FeatureGroupTable.flow_id == id).values(flow_id=None)
                session.exec(stm)
                session.flush()  # Ensure feature group updates are committed

                # Delete workspace mappings
                stm = (
                    select(WorkspaceFlowTable)
                    .where(WorkspaceFlowTable.flow_id == id)
                    .where(WorkspaceFlowTable.workspace_id == context.workspace_id)
                )
                workspace_map = session.exec(stm).all()
                for wm in workspace_map:
                    session.delete(wm)
                session.flush()  # Ensure workspace mappings are deleted

                # Now we can safely delete the flow
                session.delete(item)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e
        return True

    def save(self):
        def save_to_db():
            name = self.body.get("name")
            if name is None:
                raise ValueError("Flow name is required.")
            description = self.body.get("description", "")
            spec = self.body.get("spec")
            if spec is None:
                raise ValueError("Flow spec is required.")
            author = context.secrets.SEEKNAL_USERNAME
            check = FlowRequest.select_by_name(name)
            if check is None:
                with get_db_session() as session:
                    flow = FlowTable(
                        name=name,
                        spec=json.dumps(spec),
                        description=description,
                        author=author,
                        project_id=context.project_id,
                    )
                    session.add(flow)
                    session.commit()
                    session.refresh(flow)
                    workspace_map = WorkspaceFlowTable(
                        workspace_id=context.workspace_id, flow_id=flow.id
                    )
                    session.add(workspace_map)
                    session.commit()
                    return flow.id
            else:
                with get_db_session() as session:
                    check.name = name
                    check.spec = json.dumps(spec)
                    check.description = description
                    check.updated_at = datetime.now()
                    session.add(check)
                    session.commit()
                    session.refresh(check)
                    return check.id

        return save_to_db()


@dataclass
class FeatureRequest(RequestFactory):
    @staticmethod
    def select_by_feature_group_id_and_version(id, version):
        with get_db_session() as session:
            statement = (
                select(FeatureTable)
                .join(FeatureGroupVersionTable)
                .where(FeatureTable.feature_group_id == id)
                .where(FeatureGroupVersionTable.version == version)
            )
            return session.exec(statement).all()

    @staticmethod
    def select_by_feature_group_id(id):
        with get_db_session() as session:
            statement = (
                select(FeatureTable)
                .join(FeatureGroupVersionTable)
                .where(FeatureTable.feature_group_id == id)
            )
            return session.exec(statement).all()

    @staticmethod
    def select_by_name_and_feature_group_id(name, feature_group_id, version_id):
        with get_db_session() as session:
            statement = (
                select(FeatureTable)
                .join(FeatureGroupVersionTable)
                .where(FeatureTable.name == name)
                .where(FeatureTable.feature_group_id == feature_group_id)
                .where(FeatureGroupVersionTable.id == version_id)
            )

            return session.exec(statement).first()

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(FeatureTable).where(FeatureTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def delete_by_id(id):
        item = FeatureRequest.select_by_id(id)
        with get_db_session() as session:
            session.delete(item)
            session.commit()
            return True

    @staticmethod
    def delete_by_feature_group_id_and_version(id, version):
        item = FeatureRequest.select_by_feature_group_id_and_version(id, version)
        with get_db_session() as session:
            for i in item:
                session.delete(i)
                session.commit()
        return True

    @staticmethod
    def delete_by_feature_group_id(id):
        item = FeatureRequest.select_by_feature_group_id(id)
        with get_db_session() as session:
            for i in item:
                session.delete(i)
                session.commit()
        return True

    def save(self):
        def save_to_db():
            metadata = self.body.get("metadata")
            datatype = self.body.get("datatype")
            online_datatype = self.body.get("onlineDatatype")
            name = metadata.get("name")
            description = metadata.get("description", "")
            feature_group_id = self.body.get("feature_group_id")
            version_id = self.body.get("version_id")

            check = FeatureRequest.select_by_name_and_feature_group_id(
                name, feature_group_id, version_id
            )
            if check is None:
                with get_db_session() as session:
                    feat = FeatureTable(
                        name=name,
                        feature_group_id=feature_group_id,
                        feature_group_version_id=version_id,
                        datatype=datatype,
                        online_datatype=online_datatype,
                        description=description,
                        project_id=context.project_id,
                    )
                    session.add(feat)
                    session.commit()
                    session.refresh(feat)
                return feat.id, feat.created_at, feat.updated_at
            else:
                with get_db_session() as session:
                    check.name = name
                    check.description = description
                    check.updated_at = datetime.now()
                    session.add(check)
                    session.commit()
                    session.refresh(check)
                return check.id, check.created_at, check.updated_at

        return save_to_db()


class FeatureGroupRequest(RequestFactory):
    @staticmethod
    def select_by_project_id(id):
        with get_db_session() as session:
            statement = (
                select(FeatureGroupTable)
                .join(WorkspaceFeatureGroupTable)
                .where(FeatureGroupTable.project_id == id)
                .where(WorkspaceFeatureGroupTable.workspace_id == context.workspace_id)
            )

            return session.exec(statement).all()

    @staticmethod
    def select_by_name(name):
        with get_db_session() as session:
            statement = (
                select(FeatureGroupTable)
                .join(WorkspaceFeatureGroupTable)
                .where(FeatureGroupTable.name == name)
                .where(WorkspaceFeatureGroupTable.workspace_id == context.workspace_id)
            )
            return session.exec(statement).first()

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(FeatureGroupTable).where(FeatureGroupTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def select_version_by_feature_group_id(id):
        with get_db_session() as session:
            statement = (
                select(FeatureGroupVersionTable)
                .where(FeatureGroupVersionTable.feature_group_id == id)
                .order_by(desc(FeatureGroupVersionTable.version))
            )
            return session.exec(statement).all()

    @staticmethod
    def select_by_feature_group_id_and_version(id, version):
        with get_db_session() as session:
            statement = (
                select(FeatureGroupVersionTable)
                .where(FeatureGroupVersionTable.feature_group_id == id)
                .where(FeatureGroupVersionTable.version == version)
            )
            return session.exec(statement).first()

    @staticmethod
    def select_watermarks_by_version_id(feature_group_id, feature_group_version_id):
        with get_db_session() as session:
            stm = (
                select(OfflineWatermarkTable)
                .where(OfflineWatermarkTable.feature_group_id == feature_group_id)
                .where(
                    OfflineWatermarkTable.feature_group_version_id
                    == feature_group_version_id
                )
            )
            return session.exec(stm).all()

    @staticmethod
    def delete_by_id(id):
        item = FeatureGroupRequest.select_by_id(id)
        if item is None:
            raise ValueError("Feature group not found")

        with get_db_session() as session:
            try:
                # Get all versions for this feature group
                versions = session.exec(
                    select(FeatureGroupVersionTable).where(
                        FeatureGroupVersionTable.feature_group_id == id
                    )
                ).all()
                version_ids = [v.id for v in versions]

                # Delete all features that reference this feature group and its versions
                features = session.exec(
                    select(FeatureTable).where(
                        or_(
                            FeatureTable.feature_group_id == id,
                            FeatureTable.feature_group_version_id.in_(version_ids)
                        )
                    )
                ).all()
                
                for feature in features:
                    session.delete(feature)
                session.flush()

                # Delete offline watermarks that reference this feature group and its versions
                watermarks = session.exec(
                    select(OfflineWatermarkTable).where(
                        or_(
                            OfflineWatermarkTable.feature_group_id == id,
                            OfflineWatermarkTable.feature_group_version_id.in_(version_ids)
                        )
                    )
                ).all()
                
                for watermark in watermarks:
                    session.delete(watermark)
                session.flush()

                # Delete online table mappings
                stm = select(FeatureGroupOnlineTable).where(
                    FeatureGroupOnlineTable.feature_group_id == id
                )
                online_map = session.exec(stm).all()
                for o in online_map:
                    session.delete(o)
                session.flush()

                # Delete workspace mappings
                stm = (
                    select(WorkspaceFeatureGroupTable)
                    .where(WorkspaceFeatureGroupTable.feature_group_id == id)
                    .where(WorkspaceFeatureGroupTable.workspace_id == context.workspace_id)
                )
                workspace_map = session.exec(stm).all()
                for w in workspace_map:
                    session.delete(w)
                session.flush()

                # Delete feature group versions
                for v in versions:
                    session.delete(v)
                session.flush()

                # Finally delete the feature group itself
                session.delete(item)
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

        return True

    @staticmethod
    def get_offline_store_by_id(id):
        with get_db_session() as session:
            stm = select(OfflineStoreTable).where(OfflineStoreTable.id == id)
            return session.exec(stm).first()

    @staticmethod
    def get_offline_store_by_name(kind, name):
        with get_db_session() as session:
            stm = (
                select(OfflineStoreTable)
                .where(OfflineStoreTable.kind == kind)
                .where(OfflineStoreTable.name == name)
            )
            return session.exec(stm).first()

    @staticmethod
    def get_offline_stores():
        with get_db_session() as session:
            stm = select(OfflineStoreTable)
            return session.exec(stm).all()

    @staticmethod
    def save_offline_store(kind, value, name):
        with get_db_session() as session:
            offline_store_db = OfflineStoreTable(
                kind=kind, params=json.dumps(value), name=name
            )
            session.add(offline_store_db)
            session.commit()
            session.refresh(offline_store_db)
            return offline_store_db

    def add_offline_watermarks(self, date: List[datetime]):
        def save_to_db():
            feature_group_id = self.body.get("feature_group_id")
            feature_group_version_id = self.body.get("feature_group_version_id")
            existing_watermark = FeatureGroupRequest.select_watermarks_by_version_id(
                feature_group_id, feature_group_version_id
            )
            existing_date = set([i.date for i in existing_watermark])
            new_watermarks = list(existing_date ^ set(date))

            with get_db_session() as session:
                for d in new_watermarks:
                    watermark = OfflineWatermarkTable(
                        feature_group_id=feature_group_id,
                        feature_group_version_id=feature_group_version_id,
                        date=d,
                    )
                    session.add(watermark)
                    session.commit()
                    session.refresh(watermark)

        save_to_db()

    def update_materialization(self):
        def save_to_db():
            feature_group_id = self.body.get("feature_group_id")
            materialization_params = self.body.get("materialization_params")
            del materialization_params["offline_materialization"]["store"]
            with get_db_session() as session:
                stm = select(FeatureGroupTable).where(
                    FeatureGroupTable.id == feature_group_id
                )
                feature_group = session.exec(stm).first()
                feature_group.materialization_params = json.dumps(
                    materialization_params
                )
                feature_group.offline = materialization_params["offline"]
                feature_group.online = materialization_params["online"]
                session.add(feature_group)
                session.commit()
                session.refresh(feature_group)

        save_to_db()

    def save(self):
        def save_to_db():
            name = self.body.get("name")
            if name is None:
                raise ValueError("Flow name is required.")
            description = self.body.get("description", "")
            materialization_params = self.body.get("materialization_params")
            if materialization_params is None:
                raise ValueError("materialization_params is required.")
            author = context.secrets.SEEKNAL_USERNAME
            offline = self.body.get("offline", True)
            online = self.body.get("online", False)
            avro_schema = self.body.get("avro_schema")
            if avro_schema is None:
                raise ValueError("avro_schema is required.")
            entity_id = self.body.get("entity_id")
            flow_id = self.body.get("flow_id")
            features = self.body.get("features")

            check = FeatureGroupRequest.select_by_name(name)
            if check is None:
                offline_store = materialization_params["offline_materialization"][
                    "store"
                ]
                if offline_store is None:
                    workspace = WorkspaceRequest.select_by_id(context.workspace_id)
                    if workspace.offline_store_id is not None:
                        offline_store_db = FeatureGroupRequest.get_offline_store_by_id(
                            workspace.offline_store_id
                        )
                    else:
                        exist_offline_store = (
                            FeatureGroupRequest.get_offline_store_by_name(
                                "hive_table", "default"
                            )
                        )
                        if exist_offline_store is None:
                            offline_store_db = self.save_offline_store(
                                "hive_table", None, "default"
                            )
                        else:
                            offline_store_db = exist_offline_store
                else:
                    if offline_store["name"] is None:
                        offline_name = "default"
                    else:
                        offline_name = offline_store["name"]
                    exist_offline_store = FeatureGroupRequest.get_offline_store_by_name(
                        offline_store["kind"], offline_name
                    )
                    if exist_offline_store is None:
                        offline_store_db = self.save_offline_store(
                            offline_store["kind"], offline_store["value"], offline_name
                        )
                    else:
                        offline_store_db = exist_offline_store
                del materialization_params["offline_materialization"]["store"]
                with get_db_session() as session:
                    fg = FeatureGroupTable(
                        name=name,
                        project_id=context.project_id,
                        entity_id=entity_id,
                        description=description,
                        flow_id=flow_id,
                        author=author,
                        offline=offline,
                        online=online,
                        offline_store=offline_store_db.id,
                        materialization_params=json.dumps(materialization_params),
                    )
                    session.add(fg)
                    session.commit()
                    session.refresh(fg)
                with get_db_session() as session:
                    workspace_map = WorkspaceFeatureGroupTable(
                        workspace_id=context.workspace_id, feature_group_id=fg.id
                    )
                    session.add(workspace_map)
                    session.commit()
                with get_db_session() as session:
                    list_version = (
                        FeatureGroupRequest.select_version_by_feature_group_id(fg.id)
                    )
                    version = 1
                    if list_version is not None:
                        largest_num = 0
                        for i in list_version:
                            if i.version > largest_num:
                                largest_num = i.version
                        version = largest_num + 1
                    feature_group_version = FeatureGroupVersionTable(
                        feature_group_id=fg.id,
                        version=version,
                        avro_schema=json.dumps(avro_schema),
                    )
                    session.add(feature_group_version)
                    session.commit()
                    session.refresh(feature_group_version)

                for feature in features:
                    body = {
                        **feature,
                        "version_id": feature_group_version.id,
                        "feature_group_id": fg.id,
                    }
                    feat_id, feat_created_at, feat_updated_at = FeatureRequest(
                        body=body
                    ).save()
                    feature["feature_id"] = feat_id
                    feature["created_at"] = feat_created_at
                    feature["updated_at"] = feat_updated_at
                return (
                    fg.id,
                    features,
                    feature_group_version,
                    fg.offline_store,
                    fg.online_store,
                )
            else:
                feature_group_version = (
                    FeatureGroupRequest.select_version_by_feature_group_id(check.id)
                )
                with get_db_session() as session:
                    check.name = name
                    check.description = description
                    check.updated_at = datetime.now()
                    curr_features = (
                        FeatureRequest.select_by_feature_group_id_and_version(
                            check.id, feature_group_version.version
                        )
                    )
                    current_features = []
                    for feature in curr_features:
                        current_features.append(
                            {
                                "name": feature.name,
                                "datatype": feature.datatype,
                                "onlineDatatype": feature.online_datatype,
                            }
                        )
                    current_features.sort(key=lambda x: x["name"])
                    current_features_dict = {
                        item["name"]: item for item in current_features
                    }
                    new_features = []
                    for feature in features:
                        new_features.append(
                            {
                                "name": feature["metadata"]["name"],
                                "datatype": feature["datatype"],
                                "onlineDatatype": feature["onlineDatatype"],
                            }
                        )
                    new_features.sort(key=lambda x: x["name"])
                    new_features_dict = {item["name"]: item for item in new_features}
                    if not check_is_dict_same(current_features_dict, new_features_dict):
                        # create new version
                        with get_db_session() as session:
                            version = 1
                            if feature_group_version is not None:
                                version = version + 1
                            feature_group_version = FeatureGroupVersionTable(
                                feature_group_id=check.id,
                                version=version,
                                avro_schema=json.dumps(avro_schema),
                            )
                            session.add(feature_group_version)
                            session.commit()
                            session.refresh(feature_group_version)
                        for feature in features:
                            body = {
                                **feature,
                                "version_id": feature_group_version.id,
                                "feature_group_id": check.id,
                            }
                            feat_id, feat_created_at, feat_updated_at = FeatureRequest(
                                body=body
                            ).save()
                            feature["feature_id"] = feat_id
                            feature["created_at"] = feat_created_at
                            feature["updated_at"] = feat_updated_at

                    session.add(check)
                    session.commit()
                    session.refresh(check)
                    return check.id, features, feature_group_version

        return save_to_db()

    @staticmethod
    def delete_by_feature_group_id(id):
        with get_db_session() as session:
            try:
                # Get all versions for this feature group
                versions = session.exec(
                    select(FeatureGroupVersionTable).where(
                        FeatureGroupVersionTable.feature_group_id == id
                    )
                ).all()
                version_ids = [v.id for v in versions]

                # Delete all features that reference this feature group and its versions
                features = session.exec(
                    select(FeatureTable).where(
                        or_(
                            FeatureTable.feature_group_id == id,
                            FeatureTable.feature_group_version_id.in_(version_ids)
                        )
                    )
                ).all()
                
                for feature in features:
                    session.delete(feature)
                session.flush()
                
                return True
            except Exception as e:
                session.rollback()
                raise e


class OnlineTableRequest(RequestFactory):
    @staticmethod
    def select_by_name(name):
        with get_db_session() as session:
            statement = (
                select(OnlineTable)
                .join(WorkspaceOnlineTable)
                .where(OnlineTable.name == name)
                .where(WorkspaceOnlineTable.workspace_id == context.workspace_id)
            )
            return session.exec(statement).first()

    @staticmethod
    def select_by_id(id):
        with get_db_session() as session:
            statement = select(OnlineTable).where(OnlineTable.id == id)
            return session.exec(statement).first()

    @staticmethod
    def delete_by_id(id):
        item = OnlineTableRequest.select_by_id(id)
        if not item:
            return
            
        with get_db_session() as session:
            # Delete feature group mappings first
            stm = select(FeatureGroupOnlineTable).where(FeatureGroupOnlineTable.online_table_id == id)
            feature_group_maps = session.exec(stm).all()
            for fg in feature_group_maps:
                session.delete(fg)
            session.commit()
            
            # Delete online watermarks
            stm = select(OnlineWatermarkTable).where(OnlineWatermarkTable.online_table_id == id)
            watermarks = session.exec(stm).all()
            for w in watermarks:
                session.delete(w)
            session.commit()
            
            # Delete workspace mappings
            stm = select(WorkspaceOnlineTable).where(WorkspaceOnlineTable.online_table_id == id)
            workspace_maps = session.exec(stm).all()
            for w in workspace_maps:
                session.delete(w)
            session.commit()
            
            # Finally delete the online table
            session.delete(item)
            session.commit()
        return True

    @staticmethod
    def get_feature_group_from_online_table(id):
        with get_db_session() as session:
            stm = select(FeatureGroupOnlineTable).where(
                FeatureGroupOnlineTable.online_table_id == id
            )
            return session.exec(stm).all()

    @staticmethod
    def delete_feature_group_from_online_table(id):
        items = OnlineTableRequest.get_feature_group_from_online_table(id)
        with get_db_session() as session:
            for item in items:
                session.delete(item)
                session.commit()

        return True

    @staticmethod
    def get_online_store_by_id(id):
        with get_db_session() as session:
            stm = select(OnlineStoreTable).where(OnlineStoreTable.id == id)
            return session.exec(stm).first()

    @staticmethod
    def get_online_store_by_name(kind, name):
        with get_db_session() as session:
            stm = (
                select(OnlineStoreTable)
                .where(OnlineStoreTable.kind == kind)
                .where(OnlineStoreTable.name == name)
            )
            return session.exec(stm).first()

    @staticmethod
    def get_online_store():
        with get_db_session() as session:
            stm = select(OnlineStoreTable)
            return session.exec(stm).all()

    @staticmethod
    def save_online_store(kind, value, name):
        with get_db_session() as session:
            online_store_db = OnlineStoreTable(
                kind=kind, params=json.dumps(value), name=name
            )
            session.add(online_store_db)
            session.commit()
            session.refresh(online_store_db)
            return online_store_db

    @staticmethod
    def get_online_watermarks(id):
        with get_db_session() as session:
            stm = select(OnlineWatermarkTable).where(
                OnlineWatermarkTable.online_table_id == id
            )
            return session.exec(stm).all()

    @staticmethod
    def delete_online_watermarks(id):
        items = OnlineTableRequest.get_online_watermarks(id)
        with get_db_session() as session:
            for item in items:
                session.delete(item)
                session.commit()

        return True

    @staticmethod
    def add_online_watermarks(id, date: List[datetime]):
        def save_to_db():
            OnlineTableRequest.delete_online_watermarks(id)
            with get_db_session() as session:
                for d in date:
                    watermark = OnlineWatermarkTable(online_table_id=id, date=d)
                    session.add(watermark)
                    session.commit()
                    session.refresh(watermark)

        save_to_db()

    @staticmethod
    def update_delete_at(id, ttl):
        with get_db_session() as session:
            stm = select(OnlineTable).where(OnlineTable.id == id)
            online_table = session.exec(stm).first()
            if ttl is not None:
                expiration_date = datetime.now() + ttl
            else:
                expiration_date = None
            online_table.delete_at = expiration_date
            session.add(online_table)
            session.commit()
            session.refresh(online_table)

    def save(self):
        def save_to_db():
            name = self.body.get("name")
            description = self.body.get("description", "")
            author = context.secrets.SEEKNAL_USERNAME
            entity_id = self.body.get("entity")
            online_store = self.body.get("online_store", None)
            ttl = self.body.get("ttl", None)
            feature_lookups = self.body.get("feature_lookups", None)
            watermarks = self.body.get("watermarks")
            check = self.select_by_name(name)
            if check is None:
                if online_store is None:
                    workspace = WorkspaceRequest.select_by_id(context.workspace_id)
                    if workspace.online_store_id is not None:
                        online_store_db = self.get_online_store_by_id(
                            workspace.online_store_id
                        )
                    else:
                        exist_online_store = (
                            OnlineTableRequest.get_online_store_by_name(
                                "file", "default"
                            )
                        )
                        if exist_online_store is None:
                            online_store_db = self.save_online_store(
                                "file", None, "default"
                            )
                        else:
                            online_store_db = exist_online_store
                else:
                    online_name = "default" if online_store.name is None else online_store.name
                    exist_online_store = self.get_online_store_by_name(
                        online_store.kind, online_name
                    )
                    if exist_online_store is None:
                        online_store_db = self.save_online_store(
                            online_store.kind, online_store.value, online_name
                        )
                    else:
                        online_store_db = exist_online_store
                with get_db_session() as session:
                    if ttl is not None:
                        expiration_date = datetime.now() + ttl
                    else:
                        expiration_date = None
                    online_table = OnlineTable(
                        name=name,
                        project_id=context.project_id,
                        entity_id=entity_id,
                        description=description,
                        online_store_id=online_store_db.id,
                        author=author,
                        delete_at=expiration_date,
                    )
                    session.add(online_table)
                    session.commit()
                    session.refresh(online_table)
                with get_db_session() as session:
                    workspace_map = WorkspaceOnlineTable(
                        workspace_id=context.workspace_id,
                        online_table_id=online_table.id,
                    )
                    session.add(workspace_map)
                    session.commit()
                if feature_lookups is not None:
                    with get_db_session() as session:
                        for lookup in feature_lookups:
                            feature_group = FeatureGroupOnlineTable(
                                online_table_id=online_table.id,
                                feature_group_id=lookup.source.feature_group_id,
                                features=",".join(lookup.features)
                                if lookup.features is not None
                                else None,
                                exclude_features=",".join(lookup.exclude_features)
                                if lookup.exclude_features is not None
                                else None,
                            )
                            session.add(feature_group)
                            session.commit()
                self.add_online_watermarks(online_table.id, watermarks)
                return online_table.id

        save_to_db()
