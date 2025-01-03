import json
from dataclasses import dataclass, field
from typing import Optional, Union, List

import requests
import typer
import yaml
from tabulate import tabulate

from .context import context as seeknal_context, require_project, check_project_id
from .context import logger
from .tasks.sparkengine.transformers import Transformer
from .project import Project
from .utils import to_snake
from .tasks.sparkengine.loaders import Loader
from .request import SourceRequest, RuleRequest, EntityRequest
from .entity import Entity
from .workspace import require_workspace


class Dataset(Loader):
    pass


@dataclass
class Source:
    name: str
    details: Optional[Dataset] = None
    description: str = ""
    entity: Optional[Entity] = None

    def require_saved(func):
        def wrapper(self, *args, **kwargs):
            if not "source_id" in vars(self):
                raise ValueError("source not loaded or saved")
            else:
                func(self, *args, **kwargs)

        return wrapper

    @require_workspace
    @require_project
    def get_or_create(self):
        req = SourceRequest(body=self.__dict__)
        source = req.select_by_name(self.name)
        if source is None:
            self.source_id = req.save()
        else:
            logger.warning("Using an existing Source.")
            self.source_id = source.id
            self.name = source.name
            self.description = source.description
            self.details = Dataset(**json.loads(source.params))
            if source.entity_id is not None:
                entity_name = EntityRequest.select_by_id(source.entity_id).name
                self.entity = Entity(name=entity_name).get_or_create()
            else:
                self.entity = None
        return self

    @require_workspace
    @staticmethod
    def list():
        check_project_id()
        sources = SourceRequest.select_by_project_id(seeknal_context.project_id)
        if sources:
            _src = []
            for source in sources:
                if source.entity_id is not None:
                    entity_name = EntityRequest.select_by_id(source.entity_id).name
                else:
                    entity_name = ""
                _src.append(
                    {
                        "name": source.name,
                        "description": source.description,
                        "details": json.loads(source.params),
                        "entity": entity_name,
                    }
                )
            typer.echo(tabulate(_src, headers="keys", tablefmt="github"))
        else:
            typer.echo("No source found.")

    @require_workspace
    @require_saved
    @require_project
    def update(
        self,
        name=None,
        details: Optional[Dataset] = None,
        description=None,
        entity=None,
    ):
        source = SourceRequest.select_by_id(self.source_id)
        if source is None:
            raise ValueError("Source doesn't exists.")
        if name is None:
            name = source.name
        if description is None:
            description = source.description
        if details is None:
            details = Dataset(**json.loads(source.params))
        if entity is None:
            if source.entity_id is not None:
                entity_table = EntityRequest.select_by_id(source.entity_id)
                entity = Entity(name=entity_table.name).get_or_create()
            else:
                entity = None
        req = SourceRequest(
            body={
                "name": name,
                "details": details,
                "description": description,
                "entity": entity,
            }
        )
        req.save()
        self.name = name
        self.details = details
        self.description = description
        self.entity = entity

    @require_workspace
    @require_saved
    @require_project
    def delete(self):
        return SourceRequest.delete_by_id(self.source_id)


@dataclass
class Rule:
    name: str
    value: Optional[Union[str, List]] = None
    description: str = ""

    def require_saved(func):
        def wrapper(self, *args, **kwargs):
            if not "rule_id" in vars(self):
                raise ValueError("rule not loaded or saved")
            else:
                func(self, *args, **kwargs)

        return wrapper

    @require_workspace
    @require_project
    def get_or_create(self):
        req = RuleRequest(body=self.__dict__)
        rule = req.select_by_name(self.name)
        if rule is None:
            self.rule_id = req.save()
        else:
            logger.warning("Using an existing Rule.")
            self.rule_id = rule.id
            self.name = rule.name
            self.description = rule.description
            self.value = json.loads(rule.params)["rule"]["value"]
        return self

    @require_workspace
    @staticmethod
    def list():
        rules = RuleRequest.select_by_project_id(seeknal_context.project_id)
        if rules:
            rules = [
                {
                    "name": rule.name,
                    "value": json.loads(rule.params)["rule"]["value"],
                    "description": rule.description,
                }
                for rule in rules
            ]
            typer.echo(tabulate(rules, headers="keys", tablefmt="github"))
        else:
            typer.echo("No rule found.")

    @require_saved
    @require_workspace
    @require_project
    def update(
        self, name=None, value: Optional[Union[str, List]] = None, description=None
    ):
        if self.rule_id is None:
            raise ValueError("Invalid. Make sure load rule with get_or_create()")
        rule = RuleRequest.select_by_id(self.rule_id)
        if rule is None:
            raise ValueError("Rule doesn't exists.")
        if name is None:
            name = rule.name
        if description is None:
            description = rule.description
        if value is None:
            value = rule.params
        req = RuleRequest(
            body={
                "name": name,
                "value": value,
                "description": description,
            }
        )
        req.save()
        self.name = name
        self.value = value
        self.description = description

    @require_saved
    @require_project
    def delete(self):
        return RuleRequest.delete_by_id(self.rule_id)


@dataclass
class Common:
    @require_workspace
    @staticmethod
    def as_dict():
        check_project_id()
        sources = SourceRequest.select_by_project_id(seeknal_context.project_id)
        source_list = []
        if sources:
            for source in sources:
                source_list.append(
                    {
                        "id": source.name,
                        **json.loads(source.params),
                    }
                )
        else:
            source_list = []
        rules = RuleRequest.select_by_project_id(seeknal_context.project_id)
        if rules:
            rule_list = [
                {
                    "id": rule.name,
                    "rule": json.loads(rule.params)["rule"],
                }
                for rule in rules
            ]
        else:
            rule_list = []
        return {
            "sources": source_list,
            "rules": rule_list,
        }

    @staticmethod
    def as_yaml():
        return yaml.dump(Common.as_dict())
