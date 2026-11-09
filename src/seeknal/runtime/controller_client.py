"""Run-controller HTTP client for Atlas Runtime."""

from __future__ import annotations

import json
from typing import Any

import httpx

from seeknal.serving.grant_token import _b64url_decode

from .run_grant import RunGrantError
from .run_plan import PlanNode


class RunControllerError(Exception):
    """Base exception for run controller errors."""


class RunRevoked(RunControllerError):
    """Raised when the run has been revoked."""

    def __init__(self, message: str, *, code: str = "run_revoked", table: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.table = table


class NodeNotAuthorized(RunControllerError):
    """Raised when a node start is not authorized."""

    def __init__(self, message: str, *, code: str = "not_authorized", table: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.table = table


class RunControllerUnavailable(RunControllerError):
    """Raised when the controller is unavailable, times out, or fails closed."""


def extract_jti(grant_token: str) -> str:
    """Extract jti claim from unverified JWS payload of a grant token."""
    if not isinstance(grant_token, str):
        raise RunGrantError("Grant token must be a string")
    parts = grant_token.split(".")
    if len(parts) != 3:
        raise RunGrantError(f"Expected 3 segments in grant token, got {len(parts)}")
    try:
        payload_bytes = _b64url_decode(parts[1])
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:
        raise RunGrantError(f"Failed to decode grant payload: {exc}") from exc

    if not isinstance(payload, dict):
        raise RunGrantError("Grant payload must be a JSON object")

    jti = payload.get("jti")
    if not jti or not isinstance(jti, str):
        raise RunGrantError("Grant payload missing 'jti' claim")
    return jti


class RunControllerClient:
    """Client for Atlas Runtime run-controller."""

    def __init__(
        self,
        base_url: str,
        grant_token: str,
        *,
        timeout_seconds: float = 10,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.grant_token = grant_token
        self.timeout_seconds = timeout_seconds
        self.transport = transport
        self.jti = extract_jti(grant_token)

    def node_start(self, node: PlanNode) -> None:
        """Notify controller of node start; raises on revocation, authz denial, or error."""
        url = f"{self.base_url}/api/runtime/v1/runs/{self.jti}/nodes/start"
        headers = {
            "Authorization": f"Bearer {self.grant_token}",
            "Content-Type": "application/json",
        }
        body: dict[str, Any] = {
            "node_id": node.id,
            "reads": list(node.reads),
            "writes": list(node.writes),
        }

        try:
            with httpx.Client(transport=self.transport, timeout=self.timeout_seconds) as client:
                response = client.post(url, headers=headers, json=body)
        except httpx.RequestError as exc:
            raise RunControllerUnavailable(f"Network error calling controller: {exc}") from exc
        except Exception as exc:
            raise RunControllerUnavailable(f"Error calling controller: {exc}") from exc

        if response.status_code == 200:
            try:
                data = response.json()
            except Exception as exc:
                raise RunControllerUnavailable(
                    f"Failed to parse 200 JSON response: {exc}"
                ) from exc
            if isinstance(data, dict) and data.get("allowed") is True:
                return
            raise RunControllerUnavailable(f"Unexpected response payload: {data}")

        if response.status_code == 401:
            raise RunGrantError("Run grant unauthorized (401)")

        if response.status_code == 403:
            try:
                data = response.json()
            except Exception:
                data = {}
            error_code = data.get("error", "forbidden") if isinstance(data, dict) else "forbidden"
            table = data.get("table") if isinstance(data, dict) else None

            if error_code == "run_revoked":
                raise RunRevoked(
                    f"Run revoked ({error_code})" + (f" on table {table}" if table else ""),
                    code=error_code,
                    table=table,
                )
            raise NodeNotAuthorized(
                f"Node not authorized ({error_code})" + (f" on table {table}" if table else ""),
                code=error_code,
                table=table,
            )

        raise RunControllerUnavailable(
            f"Run controller unavailable (HTTP {response.status_code}): {response.text}"
        )
