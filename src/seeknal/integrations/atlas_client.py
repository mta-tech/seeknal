"""Atlas contract client for Seeknal apply-time metadata dual writes."""

from __future__ import annotations

import getpass
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from seeknal.integrations.atlas_config import atlas_config


class AtlasContractError(RuntimeError):
    """Raised when Atlas contract sync fails."""


class AtlasPolicyDenied(AtlasContractError):
    """Raised when Atlas explicitly denies a Seeknal operation."""


class AtlasAuthError(AtlasContractError):
    """Raised when the caller's Atlas session is missing or expired (HTTP 401).

    Carries a user-facing, re-authentication hint. It is only raised after an
    automatic token refresh has been attempted and failed (or no refresh token was
    available), so the right remedy is always to log in again. Subclasses
    :class:`AtlasContractError` so existing ``except AtlasContractError`` handlers
    keep working while callers may special-case the auth message.
    """


#: One-line, user-facing hint shown when the Atlas session is missing/expired and
#: could not be refreshed automatically. Surfaced verbatim by the CLI (no traceback).
SESSION_EXPIRED_HINT = (
    "Your Atlas session has expired. Run `seeknal auth login` to sign in again."
)


@dataclass(frozen=True)
class AtlasContractConfig:
    """Environment-backed Atlas client configuration."""

    base_url: str
    token: str | None = None
    timeout_seconds: float = 10.0


@dataclass(frozen=True)
class AtlasApplyContext:
    """Stable payload shared across Atlas contract calls during apply."""

    run_id: str
    project_name: str
    environment: str
    actor: str
    classification: str
    asset: dict[str, Any]
    upstreams: list[dict[str, Any]]


def create_atlas_contract_client_from_env() -> "AtlasContractClient | None":
    """Create a client only when Atlas sync is explicitly configured."""

    base_url = os.getenv("ATLAS_API_URL", "").strip() or atlas_config().api_url
    if not base_url:
        return None

    timeout = float(os.getenv("ATLAS_API_TIMEOUT_SECONDS", "10"))
    token = os.getenv("ATLAS_API_TOKEN")
    return AtlasContractClient(
        AtlasContractConfig(base_url=base_url.rstrip("/"), token=token, timeout_seconds=timeout)
    )


def _asset_type_for_node(node_type: str) -> str:
    mapping = {
        "source": "source",
        "transform": "transform",
        "feature_group": "feature_group",
        "second_order_aggregation": "second_order_aggregation",
        "semantic_model": "semantic_model",
        "profile": "profile",
        "rule": "rule",
    }
    return mapping.get(node_type, node_type)


def _default_namespace(project_name: str, environment: str, asset_type: str) -> str:
    return f"{project_name}/{environment}/{asset_type}"


def _extract_classification(yaml_data: dict[str, Any]) -> str:
    return (
        yaml_data.get("classification")
        or yaml_data.get("data_classification")
        or (yaml_data.get("metadata") or {}).get("classification")
        or "internal"
    )


def _extract_upstreams(
    yaml_data: dict[str, Any],
    *,
    project_name: str,
    environment: str,
) -> list[dict[str, Any]]:
    upstreams: list[dict[str, Any]] = []
    for raw_input in yaml_data.get("inputs", []) or []:
        if not isinstance(raw_input, dict):
            continue
        ref = raw_input.get("ref")
        if not ref or not isinstance(ref, str):
            continue

        if "." in ref:
            node_type, name = ref.split(".", 1)
        else:
            node_type, name = "transform", ref

        asset_type = _asset_type_for_node(node_type)
        upstreams.append(
            {
                "asset_type": asset_type,
                "name": name,
                "namespace": _default_namespace(project_name, environment, asset_type),
                "source_system": "seeknal",
                "source_id": f"{asset_type}:{name}",
                "metadata": {"ref": ref},
            }
        )
    return upstreams


def _build_asset_ref(
    *,
    node_type: str,
    name: str,
    yaml_data: dict[str, Any],
    target_path: Path,
    project_name: str,
    environment: str,
) -> dict[str, Any]:
    asset_type = _asset_type_for_node(node_type)
    description = yaml_data.get("description")
    tags = list(yaml_data.get("tags", []) or [])
    return {
        "asset_type": asset_type,
        "name": name,
        "namespace": _default_namespace(project_name, environment, asset_type),
        "source_system": "seeknal",
        "source_id": f"{asset_type}:{name}",
        "description": description,
        "tags": tags,
        "metadata": {
            "kind": node_type,
            "path": str(target_path),
            "inputs": len(yaml_data.get("inputs", []) or []),
        },
    }


class AtlasContractClient:
    """Minimal HTTP client for Atlas Phase 1 contract endpoints."""

    def __init__(self, config: AtlasContractConfig):
        self.config = config

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
        return headers

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        try:
            response = httpx.post(
                url,
                json=payload,
                headers=self._headers(),
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            message = exc.response.text.strip() or str(exc)
            raise AtlasContractError(f"Atlas contract request failed for {path}: {message}") from exc
        except httpx.HTTPError as exc:
            raise AtlasContractError(f"Atlas contract request failed for {path}: {exc}") from exc

    def preflight_apply(
        self,
        *,
        node_type: str,
        name: str,
        yaml_data: dict[str, Any],
        target_path: Path,
        project_path: Path,
    ) -> AtlasApplyContext:
        """Run the policy gate before Seeknal mutates local artifacts."""

        project_name = os.getenv("SEEKNAL_PROJECT_NAME", project_path.name)
        environment = os.getenv("ATLAS_ENVIRONMENT", os.getenv("SEEKNAL_ENV", "dev"))
        actor = os.getenv("ATLAS_ACTOR", getpass.getuser())
        asset = _build_asset_ref(
            node_type=node_type,
            name=name,
            yaml_data=yaml_data,
            target_path=target_path,
            project_name=project_name,
            environment=environment,
        )
        context = AtlasApplyContext(
            run_id=str(uuid.uuid4()),
            project_name=project_name,
            environment=environment,
            actor=actor,
            classification=_extract_classification(yaml_data),
            asset=asset,
            upstreams=_extract_upstreams(
                yaml_data,
                project_name=project_name,
                environment=environment,
            ),
        )
        decision = self._post(
            "/api/contracts/policy-check",
            {
                "operation": "apply",
                "project_name": context.project_name,
                "environment": context.environment,
                "classification": context.classification,
                "asset": context.asset,
            },
        )
        if not decision.get("allowed", False):
            reason = decision.get("reason", "Atlas denied seeknal apply")
            self._safe_report(
                context,
                status="denied",
                error_message=reason,
                details={"phase": "policy-check"},
            )
            raise AtlasPolicyDenied(reason)
        return context

    def complete_apply(
        self,
        context: AtlasApplyContext,
        *,
        manifest_updated: bool,
        no_parse: bool,
    ) -> dict[str, Any]:
        """Persist Atlas metadata once local apply has completed."""

        try:
            asset_response = self._post(
                "/api/contracts/assets/register",
                {
                    "project_name": context.project_name,
                    "environment": context.environment,
                    "asset": context.asset,
                },
            )
            asset = asset_response["asset"]
            canonical_asset_id = (asset.get("metadata_json") or {}).get("canonical_asset_id")

            if context.upstreams:
                self._post(
                    "/api/contracts/lineage/publish",
                    {
                        "project_name": context.project_name,
                        "environment": context.environment,
                        "asset": context.asset,
                        "upstreams": context.upstreams,
                    },
                )

            self._post(
                "/api/contracts/runs/report",
                {
                    "run_id": context.run_id,
                    "operation": "apply",
                    "status": "completed",
                    "project_name": context.project_name,
                    "environment": context.environment,
                    "actor": context.actor,
                    "asset": context.asset,
                    "canonical_asset_id": canonical_asset_id,
                    "details": {
                        "manifest_updated": manifest_updated,
                        "no_parse": no_parse,
                        "upstream_count": len(context.upstreams),
                    },
                },
            )
            return asset
        except AtlasContractError as exc:
            self._safe_report(
                context,
                status="failed",
                error_message=str(exc),
                details={"phase": "complete-apply"},
            )
            raise

    def report_local_failure(
        self,
        context: AtlasApplyContext,
        *,
        error_message: str,
        local_apply_completed: bool,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Best-effort failure report for local apply errors."""

        payload_details = {
            "phase": "local-apply",
            "local_apply_completed": local_apply_completed,
        }
        if details:
            payload_details.update(details)

        self._safe_report(
            context,
            status="failed",
            error_message=error_message,
            details=payload_details,
        )

    def _safe_report(
        self,
        context: AtlasApplyContext,
        *,
        status: str,
        error_message: str | None,
        details: dict[str, Any],
    ) -> None:
        try:
            self._post(
                "/api/contracts/runs/report",
                {
                    "run_id": context.run_id,
                    "operation": "apply",
                    "status": status,
                    "project_name": context.project_name,
                    "environment": context.environment,
                    "actor": context.actor,
                    "asset": context.asset,
                    "error_message": error_message,
                    "details": details,
                },
            )
        except AtlasContractError:
            pass
