"""API-token based tenant routing for Seeknal gateway/worker deployments.

This module is intentionally small and dependency-light: the first production
pass supports static token records from JSON/YAML config or environment. When no
registry is configured, callers stay in legacy compatibility mode.
"""

from __future__ import annotations

from dataclasses import dataclass
import hmac
import json
import os
from pathlib import Path
from typing import Any

from seeknal.ask.gateway.tenant import (
    DEFAULT_TENANT,
    task_queue_for_tenant,
    validate_tenant_id,
)


class TokenAuthError(ValueError):
    """Raised when an API token is missing or invalid."""


@dataclass(frozen=True)
class TokenPrincipal:
    """Resolved API token claims used for tenant-scoped routing."""

    token: str
    tenant_id: str
    task_queue: str
    name: str | None = None
    worker_id: str | None = None
    callback_token: str | None = None
    callback_url: str | None = None
    project_path: str | None = None
    temporal_address: str | None = None
    temporal_namespace: str | None = None
    worker_transport: str | None = None
    role: str | None = None

    @property
    def callback_bearer_token(self) -> str:
        """Token a worker should use when posting callback events."""
        return self.callback_token or self.token


@dataclass(frozen=True)
class WorkerRuntimeConfig:
    """Runtime settings returned to a worker after token authentication."""

    tenant_id: str
    task_queue: str
    callback_url: str | None = None
    callback_auth_token: str | None = None
    temporal_address: str | None = None
    temporal_namespace: str | None = None
    project_path: str | None = None
    worker_transport: str | None = None

    def public_dict(self) -> dict[str, str | None]:
        return {
            "tenant_id": self.tenant_id,
            "task_queue": self.task_queue,
            "callback_url": self.callback_url,
            "callback_auth_token": self.callback_auth_token,
            "temporal_address": self.temporal_address,
            "temporal_namespace": self.temporal_namespace,
            "project_path": self.project_path,
            "worker_transport": self.worker_transport,
        }


class TokenRegistry:
    """In-memory API token registry.

    Token records are intentionally server-side. The caller presents only the
    bearer token; tenant, queue and callback details are derived here.
    """

    def __init__(self, principals: list[TokenPrincipal] | None = None) -> None:
        self._principals = tuple(principals or [])

    @property
    def enabled(self) -> bool:
        return bool(self._principals)

    def resolve(self, token: str | None) -> TokenPrincipal:
        """Resolve a client/worker API token."""
        if not token:
            raise TokenAuthError("API token is required")
        for principal in self._principals:
            if hmac.compare_digest(principal.token, token):
                return principal
        raise TokenAuthError("Invalid API token")

    def resolve_callback(self, token: str | None) -> TokenPrincipal:
        """Resolve a worker callback bearer token.

        Distinct callback tokens are accepted only on callback ingress; they do
        not grant access to user/session APIs or workflow submission.
        """
        if not token:
            raise TokenAuthError("API token is required")
        for principal in self._principals:
            callback_token = principal.callback_token or principal.token
            if hmac.compare_digest(callback_token, token):
                return principal
        raise TokenAuthError("Invalid API token")

    def worker_config_for(
        self,
        token: str | None,
        *,
        default_callback_url: str | None = None,
        default_temporal_address: str | None = None,
        default_temporal_namespace: str | None = None,
        default_project_path: str | None = None,
        default_worker_transport: str | None = None,
    ) -> WorkerRuntimeConfig:
        principal = self.resolve(token)
        return WorkerRuntimeConfig(
            tenant_id=principal.tenant_id,
            task_queue=principal.task_queue,
            callback_url=principal.callback_url or default_callback_url,
            callback_auth_token=principal.callback_bearer_token,
            temporal_address=principal.temporal_address or default_temporal_address,
            temporal_namespace=principal.temporal_namespace or default_temporal_namespace,
            project_path=principal.project_path or default_project_path,
            worker_transport=principal.worker_transport or default_worker_transport,
        )


def extract_bearer_token_from_headers(headers: Any) -> str | None:
    """Extract an API token from Authorization or compatibility headers."""
    auth_header = headers.get("Authorization", "") if headers is not None else ""
    if auth_header.startswith("Bearer "):
        return auth_header[7:].strip()
    token = headers.get("X-Seeknal-Api-Token") if headers is not None else None
    return token.strip() if isinstance(token, str) and token.strip() else None


def extract_api_token(request_or_websocket: Any) -> str | None:
    """Extract API token from headers, with query fallback for SSE/WebSocket.

    Browser EventSource/WebSocket APIs cannot reliably set Authorization
    headers, so secure deployments may use short-lived query tokens for those
    transports. Header tokens remain preferred.
    """
    token = extract_bearer_token_from_headers(
        getattr(request_or_websocket, "headers", None)
    )
    if token:
        return token
    query_params = getattr(request_or_websocket, "query_params", {}) or {}
    for key in ("api_token", "access_token"):
        value = query_params.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def load_token_registry(config_path: str | Path | None = None) -> TokenRegistry:
    """Load token registry from a path or environment.

    Resolution order:
    1. explicit config_path
    2. SEEKNAL_TOKEN_CONFIG / SEEKNAL_API_TOKENS_FILE path
    3. SEEKNAL_API_TOKENS JSON payload
    4. disabled empty registry
    """
    raw: Any | None = None
    resolved_path = (
        config_path
        or os.environ.get("SEEKNAL_TOKEN_CONFIG")
        or os.environ.get("SEEKNAL_API_TOKENS_FILE")
    )
    if resolved_path:
        path = Path(resolved_path).expanduser()
        raw_text = path.read_text()
        if path.suffix.lower() in {".yml", ".yaml"}:
            import yaml

            raw = yaml.safe_load(raw_text)
        else:
            raw = json.loads(raw_text)
    else:
        raw_env = os.environ.get("SEEKNAL_API_TOKENS")
        if raw_env:
            raw = json.loads(raw_env)
    if raw is None:
        return TokenRegistry()
    return TokenRegistry(_parse_token_records(raw))


def _parse_token_records(raw: Any) -> list[TokenPrincipal]:
    if isinstance(raw, dict) and "tokens" in raw:
        raw = raw["tokens"]

    items: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        # Compact mapping form:
        # {"sk-token": {"tenant_id": "acme", ...}}
        for token, record in raw.items():
            if isinstance(record, str):
                record = {"tenant_id": record}
            if not isinstance(record, dict):
                raise ValueError(
                    "Token mapping values must be objects or tenant strings"
                )
            items.append({"token": token, **record})
    elif isinstance(raw, list):
        for record in raw:
            if not isinstance(record, dict):
                raise ValueError("Token records must be objects")
            items.append(dict(record))
    else:
        raise ValueError(
            "Token registry must be a list, mapping, or {tokens: [...]} object"
        )

    principals: list[TokenPrincipal] = []
    seen: set[str] = set()
    for record in items:
        token = str(record.get("token") or record.get("api_token") or "").strip()
        if not token:
            raise ValueError("Token record is missing token")
        if token in seen:
            raise ValueError("Duplicate API token in token registry")
        seen.add(token)
        tenant_id = validate_tenant_id(
            str(record.get("tenant_id") or record.get("tenant") or DEFAULT_TENANT)
        )
        task_queue = str(
            record.get("task_queue")
            or record.get("queue")
            or task_queue_for_tenant(tenant_id)
        )
        principals.append(
            TokenPrincipal(
                token=token,
                tenant_id=tenant_id,
                task_queue=task_queue,
                name=_optional_str(record.get("name")),
                worker_id=_optional_str(record.get("worker_id")),
                callback_token=_optional_str(
                    record.get("callback_token") or record.get("callback_auth_token")
                ),
                callback_url=_optional_str(record.get("callback_url")),
                project_path=_optional_str(record.get("project_path")),
                temporal_address=_optional_str(record.get("temporal_address")),
                temporal_namespace=_optional_str(record.get("temporal_namespace")),
                worker_transport=_optional_str(
                    record.get("worker_transport") or record.get("transport")
                ),
                role=_optional_str(record.get("role")),
            )
        )
    return principals


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
