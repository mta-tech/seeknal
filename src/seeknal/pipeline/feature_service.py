"""Declarative Feature Service resources for Seeknal projects."""

from __future__ import annotations

import re
from functools import wraps
from typing import Any, Callable

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from seeknal.pipeline.decorators import _set_node_meta

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_FEATURE_GROUP_REF_RE = re.compile(r"^feature_group\.[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class FeatureView(BaseModel):
    """Feature selection from one local Feature Group."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ref: str
    features: tuple[str, ...] = Field(min_length=1)

    @field_validator("ref")
    @classmethod
    def validate_ref(cls, value: str) -> str:
        if not _FEATURE_GROUP_REF_RE.fullmatch(value):
            raise ValueError(
                "Feature Service views must reference a qualified "
                "feature_group.<name>"
            )
        return value

    @field_validator("features")
    @classmethod
    def validate_features(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("Feature Service view features must be unique")
        for feature in value:
            if not _IDENTIFIER_RE.fullmatch(feature):
                raise ValueError(f"Invalid feature name: {feature!r}")
        return value


class FeatureServiceSpec(BaseModel):
    """Normalized Python/YAML Feature Service authoring contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    version: str
    variant: str = "default"
    owner: str
    description: str | None = None
    consumer: str | None = None
    tags: tuple[str, ...] = ()
    views: tuple[FeatureView, ...] = Field(min_length=1)

    @field_validator("name", "version", "variant")
    @classmethod
    def validate_identifier(cls, value: str) -> str:
        if not _IDENTIFIER_RE.fullmatch(value):
            raise ValueError(f"Invalid Feature Service identifier: {value!r}")
        return value

    @field_validator("owner")
    @classmethod
    def validate_owner(cls, value: str) -> str:
        owner = value.strip()
        if not owner:
            raise ValueError("Feature Service owner must not be blank")
        return owner

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("Feature Service tags must be unique")
        return value

    @model_validator(mode="after")
    def validate_view_refs(self) -> "FeatureServiceSpec":
        refs = [view.ref for view in self.views]
        if len(set(refs)) != len(refs):
            raise ValueError(
                "Feature Service views must reference unique Feature Groups"
            )
        return self

    def node_metadata(self) -> dict[str, Any]:
        """Return normalized registry metadata shared by Python and YAML."""

        return {
            "kind": "feature_service",
            "name": self.name,
            "id": f"feature_service.{self.name}",
            "version": self.version,
            "variant": self.variant,
            "owner": self.owner,
            "description": self.description,
            "consumer": self.consumer,
            "tags": list(self.tags),
            "views": [
                {"ref": view.ref, "features": list(view.features)}
                for view in self.views
            ],
            "inputs": [{"ref": view.ref} for view in self.views],
        }


def feature_service(
    *,
    name: str,
    version: str,
    owner: str,
    views: list[FeatureView | dict[str, Any]],
    variant: str = "default",
    description: str | None = None,
    consumer: str | None = None,
    tags: list[str] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Declare a contract-only Feature Service in a Python pipeline."""

    spec = FeatureServiceSpec(
        name=name,
        version=version,
        variant=variant,
        owner=owner,
        description=description,
        consumer=consumer,
        tags=tuple(tags or ()),
        views=tuple(views),
    )

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        node_id = f"feature_service.{spec.name}"

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        metadata = spec.node_metadata()
        metadata["func"] = func
        _set_node_meta(wrapper, node_id, metadata)
        return wrapper

    return decorator


__all__ = ["FeatureServiceSpec", "FeatureView", "feature_service"]
