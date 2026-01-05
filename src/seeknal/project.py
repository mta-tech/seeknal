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
    """
    A class used to define and manage projects in Seeknal.

    Projects serve as the top-level organizational unit for grouping related
    entities, feature views, and data sources. Each project has a unique name
    and optional description.

    Args:
        name (str): The name of the project. Will be converted to snake_case.
        description (str): A description of the project. Defaults to empty string.

    Attributes:
        project_id (int): The unique identifier assigned after saving to the database.
            Only available after calling get_or_create().

    Example:
        >>> project = Project(name="my_project", description="My feature store project")
        >>> project = project.get_or_create()
        >>> print(project.project_id)
    """

    name: str
    description: str = ""

    def __post_init__(self):
        self.name = to_snake(self.name)

    def get_or_create(self):
        """
        Get an existing project by name or create a new one if it doesn't exist.

        This method checks if a project with the current name exists in the database.
        If found, it loads the existing project's data into this instance.
        If not found, it creates a new project with the current attributes.
        The project_id is set on the global context for subsequent operations.

        Returns:
            Project: The current instance with project_id populated.

        Example:
            >>> project = Project(name="my_project", description="Test project")
            >>> project = project.get_or_create()
            >>> print(project.project_id)
        """
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
        """
        Update the project's name and/or description.

        Updates the project in the database with the provided values.
        If a parameter is None, the existing value is preserved.
        The project must be loaded via get_or_create() before calling this method.

        Args:
            name (str, optional): New name for the project. If None, keeps current name.
            description (str, optional): New description for the project. If None,
                keeps current description.

        Returns:
            Project: The current instance with updated attributes.

        Raises:
            ValueError: If the project has not been loaded via get_or_create().
            ProjectNotFoundError: If the project no longer exists in the database.

        Example:
            >>> project = Project(name="my_project").get_or_create()
            >>> project = project.update(description="Updated description")
        """
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
        """
        List all projects in a tabular format.

        Retrieves all projects from the database and displays them in a
        formatted table using GitHub-style markdown format. The table includes
        the project name, description, creation time, and last update time.

        Note:
            This is a static method that outputs directly to the console.
            It does not return any value.

        Example:
            >>> Project.list()
            | name       | description   | created_at          | updated_at          |
            |------------|---------------|---------------------|---------------------|
            | my_project | Test project  | 2024-01-15 10:30:00 | 2024-01-15 10:30:00 |
        """
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
        """
        Retrieve a project by its unique identifier.

        Fetches a project from the database using its ID and returns a new
        Project instance with the loaded data.

        Args:
            id (int): The unique identifier of the project to retrieve.

        Returns:
            Project: A new Project instance with the loaded data.

        Raises:
            ProjectNotFoundError: If no project exists with the given ID.

        Example:
            >>> project = Project.get_by_id(1)
            >>> print(project.name)
        """
        project = ProjectRequest.select_by_id(id)
        if project is None:
            raise ProjectNotFoundError("Project doesn't exists.")
        return Project(
            name=project.name, description=project.description, common=project.common
        )
