from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx

from seeknal.publish.exceptions import (
    PublishAuthError,
    PublishContentTypeError,
    PublishServerError,
    PublishTooLargeError,
)


@dataclass
class PublishResponse:
    slug: str
    share_url: str
    owner_secret: str
    created_at: str


@dataclass
class RevokeResponse:
    slug: str
    revoked_at: str


class PublishClient:
    """HTTP client for interacting with a Seeknal Report Server."""

    def __init__(self, server: str, api_key: Optional[str] = None) -> None:
        self._server = server.rstrip("/")
        self._api_key = api_key

    def _base_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self._api_key:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    def _raise_for_status(self, response: httpx.Response) -> None:
        if response.status_code == 401:
            raise PublishAuthError("Authentication failed — check your api_key")
        if response.status_code == 413:
            raise PublishTooLargeError("Payload too large — reduce build size")
        if response.status_code == 415:
            raise PublishContentTypeError("Unsupported media type returned by server")
        if response.status_code >= 400:
            raise PublishServerError(response.status_code, response.text[:500])

    def publish(
        self,
        tarball_path: Path,
        report_name: str,
        report_title: Optional[str] = None,
    ) -> PublishResponse:
        headers = self._base_headers()
        headers["Content-Type"] = "application/gzip"
        headers["X-Seeknal-Report-Name"] = report_name
        if report_title:
            headers["X-Seeknal-Report-Title"] = report_title

        with open(tarball_path, "rb") as fh:
            data = fh.read()

        with httpx.Client() as client:
            response = client.post(
                f"{self._server}/publish",
                content=data,
                headers=headers,
            )

        self._raise_for_status(response)
        body = response.json()
        return PublishResponse(
            slug=body["slug"],
            share_url=body["share_url"],
            owner_secret=body["owner_secret"],
            created_at=body["created_at"],
        )

    def revoke(self, slug: str, owner_secret: str) -> RevokeResponse:
        headers = self._base_headers()
        headers["X-Seeknal-Owner-Secret"] = owner_secret

        with httpx.Client() as client:
            response = client.post(
                f"{self._server}/revoke/{slug}",
                headers=headers,
            )

        self._raise_for_status(response)
        body = response.json()
        return RevokeResponse(slug=body["slug"], revoked_at=body["revoked_at"])
