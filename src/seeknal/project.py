from typing import List, Optional
from dataclasses import dataclass, field

import typer
import pendulum
from tabulate import tabulate

from .context import context
from .utils import to_snake
from .request import ProjectRequest
from .exceptions import ProjectNotFoundError


@dataclass
class Project:
    name: str
    description: str = ""

    def __post_init__(self):
        self.name = to_snake(self.name)

    def get_or_create(self):
        req = ProjectRequest(body=self.__dict__)
        project = req.select_by_name(self.name)
        if project is None:
            self.project_id = req.save()
        else:
            self.__dict__.update(project.__dict__)
            self.project_id = project.id
        context.project_id = self.project_id
        return self

    def update(self, name=None, description=None):
        if self.project_id is None:
            raise ValueError("Invalid. Make sure load project with get_or_create()")
        project = ProjectRequest.select_by_id(self.project_id)
        if project is None:
            raise ProjectNotFoundError("Project doesn't exists.")
        if name is None:
            name = project.name
        if description is None:
            description = project.description
        req = ProjectRequest(
            body={
                "name": name,
                "description": description,
            }
        )
        req.save()
        self.name = name
        self.description = description
        return self

    @staticmethod
    def list():
        req = ProjectRequest()
        projects = req.select_all()
        projects = list(
            map(
                lambda x: {
                    "name": x.name,
                    "description": x.description,
                    "created_at": pendulum.instance(x.created_at).format(
                        "YYYY-MM-DD HH:MM:SS"
                    ),
                    "updated_at": pendulum.instance(x.updated_at).format(
                        "YYYY-MM-DD HH:MM:SS"
                    ),
                },
                projects,
            )
        )
        tabular = tabulate(projects, headers="keys", tablefmt="github")
        typer.echo(tabular)

    @staticmethod
    def get_by_id(id):
        project = ProjectRequest.select_by_id(id)
        if project is None:
            raise ProjectNotFoundError("Project doesn't exists.")
        return Project(
            name=project.name, description=project.description, common=project.common
        )
