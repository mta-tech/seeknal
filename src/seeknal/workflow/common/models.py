"""Pydantic models for common configuration files.

Defines the schema for seeknal/common/sources.yml, rules.yml,
and transformations.yml.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, field_validator


class SourceDef(BaseModel):
    """A reusable source definition with named parameters.

    Example YAML::

        sources:
          - id: traffic
            ref: source.traffic_daily
            params:
              dateCol: date_id
              idCol: msisdn
    """

    id: str
    ref: str
    params: Dict[str, str] = {}

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Source id must not be empty")
        return v.strip()


class RuleDef(BaseModel):
    """A reusable SQL filter/condition expression.

    Example YAML::

        rules:
          - id: callExpression
            value: "service_type = 'Voice'"
    """

    id: str
    value: str

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Rule id must not be empty")
        return v.strip()

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Rule value must not be empty")
        return v.strip()


class TransformationDef(BaseModel):
    """A reusable SQL snippet (e.g., JOIN clause).

    Example YAML::

        transformations:
          - id: enrichWithRegion
            sql: "LEFT JOIN ref('source.regions') r ON t.region_id = r.id"
    """

    id: str
    sql: str

    @field_validator("id")
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Transformation id must not be empty")
        return v.strip()

    @field_validator("sql")
    @classmethod
    def validate_sql(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Transformation sql must not be empty")
        return v.strip()


class SourcesConfig(BaseModel):
    """Top-level model for seeknal/common/sources.yml."""

    sources: List[SourceDef] = []


class RulesConfig(BaseModel):
    """Top-level model for seeknal/common/rules.yml."""

    rules: List[RuleDef] = []


class TransformationsConfig(BaseModel):
    """Top-level model for seeknal/common/transformations.yml."""

    transformations: List[TransformationDef] = []
