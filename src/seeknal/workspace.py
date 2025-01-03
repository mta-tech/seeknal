from typing import List, Optional
from dataclasses import dataclass, field

import typer
import pendulum
from tabulate import tabulate

from .context import context, require_project, get_workspace_id, check_project_id
from .utils import to_snake
from .request import WorkspaceRequest
from .account import Account
from .featurestore.featurestore import OfflineStore


def require_workspace(func):
    def wrapper(*args, **kwargs):
        check_project_id()
        if get_workspace_id() is None:
            Workspace(name=Account.get_username(), private=True).get_or_create()
        return func(*args, **kwargs)

    return wrapper

@dataclass
class Workspace:
    name: Optional[str] = None
    description: str = ""
    private: bool = False

    def __post_init__(self):
        if self.name is None:
            self.name = Account.get_username()
        self.name = to_snake(self.name)

    @require_project
    def get_or_create(self):
        req = WorkspaceRequest(body=vars(self))
        workspace = req.select_by_name_and_project_id(self.name, context.project_id)
        if workspace is None:
            self.workspace_id = req.save()
        else:
            self.__dict__.update(workspace.__dict__)
            self.workspace_id = workspace.id
        context.workspace_id = self.workspace_id
        return self

    def attach_offline_store(self, offline_store: OfflineStore):
        self.get_or_create()
        offline_store = offline_store.get_or_create()
        req = WorkspaceRequest(
            body={**vars(self), "offline_store_id": offline_store.id}
        )
        req.save()
        typer.echo(
            f"Attached offline store: {offline_store.name} to workspace: {self.name}"
        )
        return self
    
    @staticmethod
    def current():
        return WorkspaceRequest().select_by_id(context.workspace_id)