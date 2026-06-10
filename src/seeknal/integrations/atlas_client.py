"""Atlas contract client for Seeknal apply-time metadata dual writes."""

from __future__ import annotations

import getpass
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

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

#: Hint for the never-logged-in case: Atlas rejected the request and we had no token
#: to send at all (no ATLAS_API_TOKEN and no credentials file).
NOT_LOGGED_IN_HINT = (
    "Atlas requires authentication. Run `seeknal auth login` to sign in."
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

    # Lazy import: atlas_governance imports from this module, so a top-level
    # import here would be circular.
    from seeknal.integrations.atlas_governance import user_token_from_credentials

    timeout = float(os.getenv("ATLAS_API_TIMEOUT_SECONDS", "10"))
    # An explicit service token wins; otherwise fall back to the logged-in user's
    # token so apply-time contract calls authenticate after `seeknal auth login`,
    # matching the governance gate and the catalog client.
    token = os.getenv("ATLAS_API_TOKEN") or user_token_from_credentials()
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


def _extract_owners(yaml_data: dict[str, Any]) -> list[str]:
    """Normalize a node's `owner`/`owners` YAML into a de-duplicated list.

    Sources declare a single `owner:` string; richer nodes may carry an
    `owners:` list. Both are forwarded so Atlas/DataHub can set the ownership
    aspect instead of leaving seeknal-applied datasets unassigned.
    """

    owners: list[str] = []

    def _add(value: Any) -> None:
        text = str(value).strip()
        if text and text not in owners:
            owners.append(text)

    owner = yaml_data.get("owner")
    if isinstance(owner, (list, tuple)):
        for item in owner:
            _add(item)
    elif owner:
        _add(owner)

    extra = yaml_data.get("owners")
    if isinstance(extra, (list, tuple)):
        for item in extra:
            _add(item)
    elif extra:
        _add(extra)

    return owners


def _extract_columns(yaml_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize a node's column metadata into ``[{name, description?, data_type?}]``.

    Merges two YAML shapes seeknal uses: ``columns`` (a ``{name: description}``
    mapping, or a list of dicts) and ``schema`` (a list of ``{name, data_type}``).
    Forwarded so the backend can emit ``schemaMetadata`` / per-column
    ``editableSchemaMetadata`` descriptions for Phase 4 column-level parity.
    """

    cols: dict[str, dict[str, Any]] = {}

    def _slot(name: Any) -> dict[str, Any] | None:
        text = str(name).strip()
        if not text:
            return None
        return cols.setdefault(text, {"name": text})

    for entry in yaml_data.get("schema", []) or []:
        if isinstance(entry, dict) and entry.get("name"):
            slot = _slot(entry["name"])
            if slot is not None and entry.get("data_type"):
                slot["data_type"] = str(entry["data_type"])

    columns = yaml_data.get("columns")
    if isinstance(columns, dict):
        for name, value in columns.items():
            slot = _slot(name)
            if slot is None:
                continue
            if isinstance(value, dict):
                desc = value.get("desc") or value.get("description")
                if desc:
                    slot["description"] = str(desc)
                dtype = value.get("data_type") or value.get("dtype")
                if dtype:
                    slot["data_type"] = str(dtype)
            elif value:
                slot["description"] = str(value)
    elif isinstance(columns, (list, tuple)):
        for entry in columns:
            if not isinstance(entry, dict) or not entry.get("name"):
                continue
            slot = _slot(entry["name"])
            if slot is None:
                continue
            desc = entry.get("desc") or entry.get("description")
            if desc:
                slot["description"] = str(desc)
            dtype = entry.get("data_type") or entry.get("dtype")
            if dtype:
                slot["data_type"] = str(dtype)

    return list(cols.values())


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
    metadata: dict[str, Any] = {
        "kind": node_type,
        "path": str(target_path),
        "inputs": len(yaml_data.get("inputs", []) or []),
    }
    owners = _extract_owners(yaml_data)
    if owners:
        metadata["owners"] = owners
    columns = _extract_columns(yaml_data)
    if columns:
        metadata["columns"] = columns
    return {
        "asset_type": asset_type,
        "name": name,
        "namespace": _default_namespace(project_name, environment, asset_type),
        "source_system": "seeknal",
        "source_id": f"{asset_type}:{name}",
        "description": description,
        "tags": tags,
        "metadata": metadata,
    }


class AtlasContractClient:
    """Minimal HTTP client for Atlas Phase 1 contract endpoints.

    Pass an explicit ``client`` (e.g. backed by :class:`httpx.MockTransport`) and/or
    ``token_refresher`` in tests; both default to the production behavior.
    """

    def __init__(
        self,
        config: AtlasContractConfig,
        *,
        client: httpx.Client | None = None,
        token_refresher: "Callable[[], str | None] | None" = None,
    ):
        self.config = config
        self._client = client
        # Mutable bearer copy so a 401 can be recovered by refreshing the access token
        # (the frozen config stays the source of the *initial* token).
        self._token = config.token
        self._token_refresher = token_refresher

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _refresh_token(self) -> str | None:
        if self._token_refresher is not None:
            return self._token_refresher()
        # Lazy import: atlas_governance imports from this module (circular otherwise).
        from seeknal.integrations.atlas_governance import refresh_access_token

        return refresh_access_token()

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.config.base_url}{path}"
        refreshed = False
        while True:
            try:
                if self._client is not None:
                    response = self._client.post(url, json=payload, headers=self._headers())
                else:
                    response = httpx.post(
                        url,
                        json=payload,
                        headers=self._headers(),
                        timeout=self.config.timeout_seconds,
                    )
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                # Transparently refresh the access token once on a 401, then retry —
                # mirrors GovernanceGate._post / AtlasCatalogClient._send so a single
                # `seeknal auth login` covers the apply-time contract surface too.
                if exc.response.status_code == 401:
                    if not refreshed:
                        refreshed = True
                        new_token = self._refresh_token()
                        if new_token:
                            self._token = new_token
                            continue
                    hint = SESSION_EXPIRED_HINT if self._token else NOT_LOGGED_IN_HINT
                    raise AtlasAuthError(hint) from exc
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

    def publish_lineage(
        self,
        *,
        project_name: str,
        environment: str,
        asset: dict[str, Any],
        upstreams: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Publish an ``asset → upstreams`` lineage edge set to Atlas/DataHub.

        Standalone counterpart to the apply-time lineage publish, usable from
        ``seeknal atlas lineage publish`` without the optional
        ``atlas-data-platform`` package — it hits the same
        ``/api/contracts/lineage/publish`` endpoint over plain HTTP.
        """

        return self._post(
            "/api/contracts/lineage/publish",
            {
                "project_name": project_name,
                "environment": environment,
                "asset": asset,
                "upstreams": upstreams,
            },
        )

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
