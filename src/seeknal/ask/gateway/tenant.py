"""Tenant resolution helpers for multi-tenant gateway routing.

Pattern A: shared Temporal client + task queue per tenant. Tenant is
resolved at request time from either the ``X-Tenant-ID`` HTTP header or
a ``tenant`` query parameter (the latter for SSE/WebSocket clients that
can't set custom headers in the browser). When neither is present, the
default tenant (``"default"``) is used, which routes to the legacy
``seeknal-ask`` task queue for backward compatibility.

Storage keys (Redis, SSE broadcaster, session store, session locks) are
tenant-prefixed so two tenants using the same ``session_id`` never
collide.
"""

from __future__ import annotations

import re
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.websockets import WebSocket


# Tenant IDs follow the same character class as session IDs but are
# shorter. Keep it URL-safe (no path traversal, no Redis key injection).
TENANT_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")

DEFAULT_TENANT = "default"
DEFAULT_TASK_QUEUE = "seeknal-ask"


class InvalidTenantError(ValueError):
    """Raised when an X-Tenant-ID header or ?tenant= query param is malformed."""


def validate_tenant_id(tenant_id: str) -> str:
    """Validate a tenant ID string. Returns the tenant ID unchanged on success."""
    if not TENANT_ID_RE.match(tenant_id):
        raise InvalidTenantError(
            f"Invalid tenant_id format: {tenant_id!r}. "
            "Must match [a-zA-Z0-9_-]{1,64}."
        )
    return tenant_id


def resolve_tenant(request: "Request") -> str:
    """Resolve the tenant for a Starlette HTTP request.

    Order of precedence:
    1. ``X-Tenant-ID`` header
    2. ``?tenant=`` query parameter
    3. ``DEFAULT_TENANT``

    Raises ``InvalidTenantError`` (ValueError) if the provided value is
    malformed.
    """
    tenant_id = request.headers.get("X-Tenant-ID")
    if not tenant_id:
        tenant_id = request.query_params.get("tenant")
    if not tenant_id:
        return DEFAULT_TENANT
    return validate_tenant_id(tenant_id)


def resolve_tenant_ws(websocket: "WebSocket") -> str:
    """Resolve the tenant for a Starlette WebSocket connection.

    WebSocket clients in browsers can't set custom headers, so the
    primary source here is the ``?tenant=`` query parameter. Headers
    are still checked first for programmatic clients.
    """
    tenant_id = websocket.headers.get("X-Tenant-ID")
    if not tenant_id:
        tenant_id = websocket.query_params.get("tenant")
    if not tenant_id:
        return DEFAULT_TENANT
    return validate_tenant_id(tenant_id)


def task_queue_for_tenant(tenant_id: str) -> str:
    """Map a tenant ID to its Temporal task queue name.

    Special case: ``"default"`` maps to the legacy ``"seeknal-ask"``
    queue so existing single-tenant deployments continue working with
    zero config changes. Any other tenant gets ``"seeknal-ask-{id}"``.
    """
    if tenant_id == DEFAULT_TENANT:
        return DEFAULT_TASK_QUEUE
    return f"{DEFAULT_TASK_QUEUE}-{tenant_id}"


def make_workflow_id(tenant_id: str, session_id: str) -> str:
    """Build a tenant-prefixed Temporal workflow ID.

    Default tenant omits the prefix to match the legacy ``ask-{sid}-{ts}``
    format, keeping Temporal history queries backward compatible.
    """
    ts = int(time.time())
    if tenant_id == DEFAULT_TENANT:
        return f"ask-{session_id}-{ts}"
    return f"ask-{tenant_id}-{session_id}-{ts}"


def scoped_key(tenant_id: str, session_id: str) -> str:
    """Compose a tenant-scoped composite key for in-memory dicts.

    Used by ``SSEBroadcaster`` and ``_session_locks`` so the same
    session_id across tenants doesn't collide in process-local state.
    """
    return f"{tenant_id}:{session_id}"
