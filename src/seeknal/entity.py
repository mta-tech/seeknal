import json
from typing import List, Optional
from dataclasses import dataclass, field
import typer
from tabulate import tabulate
import hashlib

from .context import context as seeknal_context, logger
from .utils import to_snake
from .models import metadata, EntityTable
from .request import EntityRequest
from .exceptions import EntityNotFoundError, EntityNotSavedError


def require_saved(func):
    def wrapper(self, *args, **kwargs):
        if not "entity_id" in vars(self):
            raise EntityNotSavedError("Entity not loaded or saved")
        else:
            func(self, *args, **kwargs)

    return wrapper


@dataclass
class Entity:
    """
    A class used to define entity

    Args:
        join_keys (List[str]): Set join keys
        pii_keys (Optional, List[str]): Set pii keys given join keys
        description (str): Description of specified entity
    """

    name: str
    join_keys: Optional[List[str]] = None
    pii_keys: Optional[List[str]] = None
    description: Optional[str] = None

    def __post_init__(self):
        self.name = to_snake(self.name)

    def get_or_create(self):
        req = EntityRequest(body=vars(self))
        entity = req.select_by_name(self.name)
        if entity is None:
            self.entity_id = req.save()
        else:
            self.entity_id = entity.id
            self.name = entity.name
            self.join_keys = entity.join_keys.split(",")
            if entity.pii_keys is not None:
                self.pii_keys = entity.pii_keys.split(",")
            else:
                self.pii_keys = None
            self.description = entity.description
        return self

    def list():
        entities = EntityRequest.select_all()
        if entities:
            entities = [
                {
                    "name": entity.name,
                    "join_keys": entity.join_keys,
                    "pii_keys": entity.pii_keys,
                    "description": entity.description,
                }
                for entity in entities
            ]
            typer.echo(tabulate(entities, headers="keys", tablefmt="github"))
        else:
            typer.echo("No entities found.")

    @require_saved
    def update(self, name=None, description=None, pii_keys=None):
        entity = EntityRequest.select_by_id(self.entity_id)
        if entity is None:
            raise EntityNotFoundError("Entity not found.")
        if name is None:
            name = entity.name
        if description is None:
            description = entity.description
        if pii_keys is None:
            pii_keys = entity.pii_keys
        req = EntityRequest(
            body={
                "name": name,
                "description": description,
                "pii_keys": pii_keys,
                "join_keys": entity.join_keys.split(","),
            }
        )
        req.save()
        self.name = name
        self.description = description
        self.pii_keys = pii_keys

    def set_key_values(self, *args):
        key_values = {}
        for idx, i in enumerate(self.join_keys):
            key_values[i] = args[idx]

        self.key_values = key_values
        return self
