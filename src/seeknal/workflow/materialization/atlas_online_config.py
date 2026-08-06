"""Strict configuration for the Atlas online serving materializer."""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


class AtlasOnlineMaterializationConfig(BaseModel):
    """A Seeknal-owned publication into Atlas' read-only serving schema."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["atlas_online"] = "atlas_online"
    connection: str
    table: str
    mode: Literal["replace"] = "replace"
    entity_keys: tuple[str, ...] = Field(min_length=1)
    event_time_column: str | None = None
    ttl_seconds: int | None = Field(default=None, ge=0)
    batch_size: int = Field(default=5_000, ge=1, le=100_000)
    revision: str
    definition_sha: str
    schema_sha: str
    publish_run_id: str

    @field_validator("connection", "publish_run_id")
    @classmethod
    def require_nonblank(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("value must not be blank")
        return normalized

    @field_validator("table")
    @classmethod
    def validate_table(cls, value: str) -> str:
        if not _IDENTIFIER_RE.fullmatch(value) or len(value) > 40:
            raise ValueError(
                "Atlas online table must be a SQL identifier of at most 40 characters"
            )
        return value

    @field_validator("entity_keys")
    @classmethod
    def validate_entity_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("Atlas online entity keys must be unique")
        for key in value:
            if not _IDENTIFIER_RE.fullmatch(key):
                raise ValueError(f"Invalid Atlas online entity key: {key!r}")
        return value

    @field_validator("event_time_column")
    @classmethod
    def validate_event_time_column(cls, value: str | None) -> str | None:
        if value is not None and not _IDENTIFIER_RE.fullmatch(value):
            raise ValueError(f"Invalid Atlas online event time column: {value!r}")
        return value

    @field_validator("revision", "definition_sha", "schema_sha")
    @classmethod
    def validate_sha256(cls, value: str) -> str:
        if not _SHA256_RE.fullmatch(value):
            raise ValueError("Applied revision fields must be lowercase SHA-256 values")
        return value


__all__ = ["AtlasOnlineMaterializationConfig"]
