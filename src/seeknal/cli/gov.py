"""
Seeknal CLI - Atlas data-access governance.

A command group for interacting with the Atlas governance backend from the engine
side. The first command lets a user request access to a dataset; the request is
created server-side and returned with an auto-assigned id (e.g. ``AR-12``).

Usage:
    seeknal gov request-access warehouse.namespace.table --reason "need it for X"
    seeknal gov request-access prod.gold.customer --reason "..." --access write
    seeknal gov request-access prod.gold.customer --reason "..." --duration 30d
"""

from __future__ import annotations

import os
from typing import Any, Optional

import httpx
import typer

from seeknal.integrations.atlas_governance import (
    user_email_from_credentials,
    user_token_from_credentials,
)
from seeknal.ui.output import echo_error, echo_info, echo_success

gov_app = typer.Typer(
    name="gov",
    help="Atlas data-access governance (request access, etc.).",
)

#: Seconds to wait for the Atlas governance API before giving up.
_REQUEST_TIMEOUT_SECONDS = 30.0


def _atlas_base_url() -> str:
    """Resolve the Atlas API base URL from the environment.

    Honours ``SEEKNAL_API_URL`` first, then ``ATLAS_API_URL``, defaulting to the
    local dev server. A trailing slash is stripped so paths join cleanly.
    """

    base = os.getenv("SEEKNAL_API_URL") or os.getenv("ATLAS_API_URL") or "http://localhost:8000"
    return base.rstrip("/")


def _requester_email() -> str:
    """Best-effort caller identity for the request body.

    Prefers the email/username claim from the logged-in credentials token, falling
    back to the ``USER`` environment variable, then an empty string.
    """

    return user_email_from_credentials() or os.getenv("USER", "")


@gov_app.command("request-access")
def request_access(
    dataset: str = typer.Argument(
        ...,
        help="Fully-qualified table, e.g. warehouse.namespace.table.",
    ),
    reason: str = typer.Option(
        ...,
        "--reason",
        help="Justification for the access request.",
    ),
    access: str = typer.Option(
        "read",
        "--access",
        help="Access type requested (e.g. read, write).",
    ),
    duration: str = typer.Option(
        "90d",
        "--duration",
        help="Requested grant duration (e.g. 90d).",
    ),
    urn: Optional[str] = typer.Option(
        None,
        "--urn",
        help="Optional entity URN for the dataset.",
    ),
    priority: str = typer.Option(
        "medium",
        "--priority",
        help="Request priority (e.g. low, medium, high).",
    ),
) -> None:
    """Request access to a dataset via the Atlas governance backend.

    Builds a governance access-request body and ``POST``s it to
    ``{API}/governance/access-requests``. When the user has logged in, the stored
    access token is sent as a bearer credential so the request is attributed to
    them. On success the created request id and status are printed; a non-2xx
    response prints an error and exits with code 1.
    """

    body: dict[str, Any] = {
        "requester": _requester_email(),
        "entityName": dataset,
        "entityUrn": urn or "",
        "requestedAccess": access,
        "reason": reason,
        "duration": duration,
        "priority": priority,
        "type": "dataset",
    }

    headers = {"Content-Type": "application/json"}
    token = user_token_from_credentials()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    url = f"{_atlas_base_url()}/governance/access-requests"
    try:
        response = httpx.post(url, json=body, headers=headers, timeout=_REQUEST_TIMEOUT_SECONDS)
    except httpx.HTTPError as exc:
        echo_error(f"Access request failed: {exc}")
        raise typer.Exit(1)

    if not (200 <= response.status_code < 300):
        detail = response.text.strip() or f"HTTP {response.status_code}"
        echo_error(f"Access request rejected ({response.status_code}): {detail}")
        raise typer.Exit(1)

    try:
        record = response.json()
    except ValueError:
        record = {}

    request_id = record.get("id") or "(unknown)"
    status = record.get("status") or "pending"
    echo_success(f"Access request {request_id} created for {dataset}")
    echo_info(f"Status: {status} | access: {access} | duration: {duration}")
