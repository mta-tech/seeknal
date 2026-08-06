"""Compile applied Seeknal Feature Services into Atlas publication contracts."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from seeknal.dag.manifest import Manifest, Node, NodeType
from seeknal.workflow.state import (
    NodeFingerprint,
    NodeState,
    compute_dag_fingerprints,
    load_state,
)

_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TYPE_MAP = {
    "str": "string",
    "string": "string",
    "text": "string",
    "varchar": "string",
    "int": "int64",
    "integer": "int64",
    "int64": "int64",
    "bigint": "int64",
    "float": "float64",
    "float32": "float64",
    "float64": "float64",
    "double": "float64",
    "decimal": "float64",
    "number": "float64",
    "bool": "boolean",
    "boolean": "boolean",
    "date": "timestamp",
    "datetime": "timestamp",
    "timestamp": "timestamp",
    "timestamptz": "timestamp",
}


class FeatureServiceCompilationError(ValueError):
    """Raised when applied state cannot prove a publishable service snapshot."""


class _WireModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=False)


class _WireFeatureField(_WireModel):
    name: str
    data_type: Literal["string", "int64", "float64", "boolean", "timestamp"] = Field(
        alias="dataType"
    )
    description: str | None
    default_value: Any | None = Field(alias="defaultValue")


class _WireEntityKey(_WireModel):
    semantic_name: str = Field(alias="semanticName")
    physical_name: str = Field(alias="physicalName")
    data_type: Literal["string", "int64", "boolean"] = Field(alias="dataType")
    aliases: list[str]
    required: bool
    ordinal: int = Field(ge=0)


class _WireView(_WireModel):
    view_id: str = Field(alias="viewId")
    revision: str
    schema_revision: str = Field(alias="schemaRevision")
    source_locator: str = Field(alias="sourceLocator")
    schema_hash: str = Field(alias="schemaHash", pattern=r"^[a-f0-9]{64}$")
    execution_mode: Literal["batch"] = Field(alias="executionMode")
    fields: list[_WireFeatureField] = Field(min_length=1)
    entity_keys: list[_WireEntityKey] = Field(alias="entityKeys", min_length=1)
    event_timestamp_field: str | None = Field(alias="eventTimestampField")
    created_timestamp_field: str | None = Field(alias="createdTimestampField")
    ttl_seconds: int | None = Field(alias="ttlSeconds", ge=0)


class _WireSelection(_WireModel):
    view: _WireView
    features: list[str] = Field(min_length=1)
    ordinal: int = Field(ge=0)


class _WirePublication(_WireModel):
    service_id: str = Field(alias="serviceId")
    version: str
    variant: str
    owner: str
    description: str | None
    consumer: str | None
    tags: list[str]
    compatibility_kind: Literal["native"] = Field(alias="compatibilityKind")
    entity_keys: list[_WireEntityKey] = Field(alias="entityKeys", min_length=1)
    request_fields: list[Any] = Field(alias="requestFields")
    selections: list[_WireSelection] = Field(min_length=1)
    execution_modes: list[Literal["batch"]] = Field(alias="executionModes")
    transformation_order: list[str] = Field(alias="transformationOrder")


@dataclass(frozen=True)
class CompiledFeatureService:
    selector: str
    environment: str | None
    manifest_path: Path
    state_path: Path
    payload: dict[str, Any]

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(
            self.payload,
            indent=indent,
            sort_keys=True,
            ensure_ascii=False,
        )


def _data_type(value: Any, *, field_name: str) -> str:
    normalized = str(value).strip().lower()
    try:
        return _TYPE_MAP[normalized]
    except KeyError as exc:
        raise FeatureServiceCompilationError(
            f"Feature {field_name!r} uses unsupported dtype {value!r}"
        ) from exc


def _feature_fields(node: Node) -> list[dict[str, Any]]:
    raw = node.config.get("features")
    if not isinstance(raw, dict) or not raw:
        raise FeatureServiceCompilationError(
            f"{node.id} must declare a non-empty features mapping"
        )
    fields: list[dict[str, Any]] = []
    for name, definition in raw.items():
        if not isinstance(name, str) or not _SQL_IDENTIFIER_RE.fullmatch(name):
            raise FeatureServiceCompilationError(
                f"{node.id} contains invalid feature name {name!r}"
            )
        if isinstance(definition, str):
            dtype = definition
            description = None
            default_value = None
        elif isinstance(definition, dict):
            dtype = definition.get("dtype") or definition.get("type")
            description = definition.get("description")
            default_value = definition.get("default")
        else:
            raise FeatureServiceCompilationError(
                f"{node.id} feature {name!r} must be a mapping or dtype string"
            )
        if not dtype:
            raise FeatureServiceCompilationError(
                f"{node.id} feature {name!r} is missing dtype"
            )
        fields.append(
            {
                "name": name,
                "dataType": _data_type(dtype, field_name=name),
                "description": description,
                "defaultValue": default_value,
            }
        )
    return fields


def _entity_keys(node: Node, fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entity = node.config.get("entity")
    if isinstance(entity, str):
        join_keys = [f"{entity}_id"]
    elif isinstance(entity, dict):
        join_keys = entity.get("join_keys") or entity.get("joinKeys") or []
    else:
        join_keys = []
    if not isinstance(join_keys, list) or not join_keys:
        raise FeatureServiceCompilationError(f"{node.id} must declare entity.join_keys")
    by_name = {field["name"]: field for field in fields}
    keys: list[dict[str, Any]] = []
    for ordinal, name in enumerate(join_keys):
        field = by_name.get(name)
        if field is None:
            raise FeatureServiceCompilationError(
                f"{node.id} entity key {name!r} is not declared as a feature"
            )
        if field["dataType"] not in {"string", "int64", "boolean"}:
            raise FeatureServiceCompilationError(
                f"{node.id} entity key {name!r} has unsupported key type"
            )
        keys.append(
            {
                "semanticName": name,
                "physicalName": name,
                "dataType": field["dataType"],
                "aliases": [],
                "required": True,
                "ordinal": ordinal,
            }
        )
    return keys


def _atlas_target(node: Node) -> dict[str, Any]:
    targets = node.config.get("materializations") or []
    matches = [
        target
        for target in targets
        if isinstance(target, dict) and target.get("type") == "atlas_online"
    ]
    if len(matches) != 1:
        raise FeatureServiceCompilationError(
            f"{node.id} must declare exactly one atlas_online materialization"
        )
    target = matches[0]
    table = target.get("table")
    if not isinstance(table, str) or not _SQL_IDENTIFIER_RE.fullmatch(table):
        raise FeatureServiceCompilationError(
            f"{node.id} atlas_online table must be a SQL identifier"
        )
    return target


def _materialization_evidence(
    node: Node,
    state: NodeState,
    target: dict[str, Any],
    *,
    current_fingerprint: NodeFingerprint,
    run_id: str,
) -> dict[str, Any]:
    materialization = state.metadata.get("materialization")
    if (
        not isinstance(materialization, dict)
        or materialization.get("success") is not True
    ):
        raise FeatureServiceCompilationError(
            f"{node.id} has no successful applied materialization evidence"
        )
    candidates = []
    for result in materialization.get("results", []):
        if not isinstance(result, dict):
            continue
        write_result = result.get("write_result")
        if (
            result.get("type") == "atlas_online"
            and result.get("success") is True
            and isinstance(write_result, dict)
            and write_result.get("table") == target["table"]
        ):
            candidates.append(write_result)
    if len(candidates) != 1:
        raise FeatureServiceCompilationError(
            f"{node.id} has no unambiguous atlas_online publication evidence"
        )
    evidence = candidates[0]
    if state.fingerprint is None:
        raise FeatureServiceCompilationError(f"{node.id} has no applied fingerprint")
    if current_fingerprint != state.fingerprint:
        raise FeatureServiceCompilationError(
            f"{node.id} manifest fingerprint does not match applied state"
        )
    if evidence.get("revision") != state.fingerprint.combined:
        raise FeatureServiceCompilationError(
            f"{node.id} materialized revision does not match applied state"
        )
    if evidence.get("definition_sha") != state.fingerprint.content_hash:
        raise FeatureServiceCompilationError(
            f"{node.id} materialized definition does not match applied state"
        )
    if evidence.get("schema_sha") != state.fingerprint.schema_hash:
        raise FeatureServiceCompilationError(
            f"{node.id} materialized schema does not match applied state"
        )
    if not isinstance(run_id, str) or not run_id or any(
        character.isspace() for character in run_id
    ):
        raise FeatureServiceCompilationError(
            "applied run identity is invalid"
        )
    publish_run_id = evidence.get("publish_run_id")
    if (
        not isinstance(publish_run_id, str)
        or not publish_run_id
        or any(character.isspace() for character in publish_run_id)
    ):
        raise FeatureServiceCompilationError(
            f"{node.id} materialized run identity is invalid"
        )
    if publish_run_id != run_id:
        raise FeatureServiceCompilationError(
            f"{node.id} materialized run does not match applied state"
        )
    return evidence


def _manifest_fingerprints(manifest: Manifest) -> dict[str, NodeFingerprint]:
    """Recompute fingerprints exactly as the workflow apply path does."""
    manifest_nodes = {
        node_id: {
            "kind": node.node_type.value,
            "config": node.config,
            "file_path": node.file_path or "unknown.yml",
            "columns": node.columns,
        }
        for node_id, node in manifest.nodes.items()
    }
    upstream = {
        node_id: manifest.get_upstream_nodes(node_id)
        for node_id in manifest.nodes
    }
    return compute_dag_fingerprints(manifest_nodes, upstream)


def _view_schema_hash(view: dict[str, Any]) -> str:
    canonical = {
        "viewId": view["viewId"],
        "revision": view["revision"],
        "schemaRevision": view["schemaRevision"],
        "sourceLocator": view["sourceLocator"],
        "viewType": view["executionMode"],
        "fields": view["fields"],
        "entityKeys": sorted(view["entityKeys"], key=lambda item: item["ordinal"]),
        "eventTimestampField": view.get("eventTimestampField"),
        "createdTimestampField": view.get("createdTimestampField"),
        "ttlSeconds": view.get("ttlSeconds"),
    }
    encoded = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class FeatureServiceCompiler:
    """Compile only from the manifest and successful state of an applied run."""

    def __init__(
        self,
        project_path: str | Path = ".",
        *,
        environment: str | None = None,
    ) -> None:
        self.project_path = Path(project_path).resolve()
        self.environment = environment

    @property
    def target_path(self) -> Path:
        target = self.project_path / "target"
        if self.environment:
            return target / "environments" / self.environment
        return target

    def compile(self, selector: str) -> CompiledFeatureService:
        if not selector.startswith("feature_service."):
            raise FeatureServiceCompilationError(
                "Feature Service selector must be feature_service.<name>"
            )
        manifest_path = self.target_path / "manifest.json"
        state_path = self.target_path / "run_state.json"
        try:
            manifest = Manifest.load(str(manifest_path))
        except FileNotFoundError as exc:
            raise FeatureServiceCompilationError(
                f"Applied manifest is missing: {manifest_path}"
            ) from exc
        state = load_state(state_path)
        if state is None:
            raise FeatureServiceCompilationError(
                f"Applied run state is missing: {state_path}"
            )
        service = manifest.get_node(selector)
        if service is None or service.node_type is not NodeType.FEATURE_SERVICE:
            raise FeatureServiceCompilationError(
                f"Feature Service not found in applied manifest: {selector}"
            )
        manifest_fingerprints = _manifest_fingerprints(manifest)

        service_config = service.config
        selections: list[dict[str, Any]] = []
        service_keys: list[dict[str, Any]] | None = None
        for ordinal, configured_view in enumerate(service_config.get("views", [])):
            if not isinstance(configured_view, dict):
                raise FeatureServiceCompilationError(
                    f"{selector} contains an invalid view selection"
                )
            view_ref = configured_view.get("ref")
            selected_features = configured_view.get("features")
            feature_group = (
                manifest.get_node(view_ref) if isinstance(view_ref, str) else None
            )
            if (
                feature_group is None
                or feature_group.node_type is not NodeType.FEATURE_GROUP
            ):
                raise FeatureServiceCompilationError(
                    f"{selector} references missing Feature Group {view_ref!r}"
                )
            node_state = state.nodes.get(feature_group.id)
            if node_state is None or not node_state.is_success():
                raise FeatureServiceCompilationError(
                    f"{feature_group.id} has not completed successfully"
                )
            fields = _feature_fields(feature_group)
            field_names = {field["name"] for field in fields}
            if (
                not isinstance(selected_features, list)
                or not selected_features
                or any(name not in field_names for name in selected_features)
            ):
                raise FeatureServiceCompilationError(
                    f"{selector} selects unknown or empty features from {feature_group.id}"
                )
            keys = _entity_keys(feature_group, fields)
            if service_keys is None:
                service_keys = keys
            elif service_keys != keys:
                raise FeatureServiceCompilationError(
                    "All selected Feature Groups must use identical typed entity keys"
                )
            target = _atlas_target(feature_group)
            evidence = _materialization_evidence(
                feature_group,
                node_state,
                target,
                current_fingerprint=manifest_fingerprints[feature_group.id],
                run_id=state.run_id,
            )
            event_time = target.get("event_time_column")
            if event_time is not None and event_time not in field_names:
                raise FeatureServiceCompilationError(
                    f"{feature_group.id} event_time_column is not a declared feature"
                )
            view = {
                "viewId": feature_group.name,
                "revision": evidence["revision"],
                "schemaRevision": evidence["schema_sha"],
                "sourceLocator": (
                    f"seeknal:feature-group:{target['table']}:{evidence['revision']}"
                ),
                "executionMode": "batch",
                "fields": fields,
                "entityKeys": keys,
                "eventTimestampField": event_time,
                "createdTimestampField": None,
                "ttlSeconds": target.get("ttl_seconds"),
            }
            view["schemaHash"] = _view_schema_hash(view)
            selections.append(
                {
                    "view": view,
                    "features": list(selected_features),
                    "ordinal": ordinal,
                }
            )

        if not selections or service_keys is None:
            raise FeatureServiceCompilationError(
                f"{selector} contains no Feature Group selections"
            )
        service_state = state.nodes.get(selector)
        if service_state is None or not service_state.is_success():
            raise FeatureServiceCompilationError(
                f"{selector} has not been applied successfully"
            )
        if service_state.fingerprint is None:
            raise FeatureServiceCompilationError(
                f"{selector} has no applied fingerprint"
            )
        if manifest_fingerprints[selector] != service_state.fingerprint:
            raise FeatureServiceCompilationError(
                f"{selector} manifest fingerprint does not match applied state"
            )
        payload = {
            "serviceId": service_config.get("name") or service.name,
            "version": service_config["version"],
            "variant": service_config.get("variant", "default"),
            "owner": service_config["owner"],
            "description": service_config.get("description"),
            "consumer": service_config.get("consumer"),
            "tags": list(service_config.get("tags") or []),
            "compatibilityKind": "native",
            "entityKeys": service_keys,
            "requestFields": [],
            "selections": selections,
            "executionModes": ["batch"],
            "transformationOrder": [],
        }
        payload = _WirePublication.model_validate(payload).model_dump(
            mode="json",
            by_alias=True,
        )
        return CompiledFeatureService(
            selector=selector,
            environment=self.environment,
            manifest_path=manifest_path,
            state_path=state_path,
            payload=payload,
        )


__all__ = [
    "CompiledFeatureService",
    "FeatureServiceCompilationError",
    "FeatureServiceCompiler",
]
