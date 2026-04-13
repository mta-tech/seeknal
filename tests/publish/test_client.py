from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from seeknal.publish.client import PublishClient, PublishResponse, RevokeResponse
from seeknal.publish.exceptions import PublishAuthError, PublishServerError, PublishTooLargeError

_RealHttpxClient = httpx.Client


def _make_tarball(tmp_path: Path) -> Path:
    p = tmp_path / "report.tar.gz"
    p.write_bytes(b"\x1f\x8b\x00fake_tarball_content")
    return p


def _transport_patch(handler):
    """Context manager: patch httpx.Client in client.py to inject MockTransport."""
    transport = httpx.MockTransport(handler)

    def factory(**kw):
        return _RealHttpxClient(transport=transport)

    return patch("seeknal.publish.client.httpx.Client", factory)


class TestPublishSendsCorrectHeaders:
    def test_publish_sends_correct_headers(self, tmp_path: Path):
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(
                201,
                json={
                    "slug": "abc123",
                    "share_url": "http://test-server/r/abc123",
                    "owner_secret": "ownsecret",
                    "created_at": "2026-01-01T00:00:00Z",
                },
            )

        tarball = _make_tarball(tmp_path)
        client = PublishClient(server="http://test-server", api_key="my-api-key")

        with _transport_patch(handler):
            result = client.publish(tarball, "my-report")

        assert len(captured) == 1
        req = captured[0]
        assert req.headers["content-type"] == "application/gzip"
        assert req.headers["authorization"] == "Bearer my-api-key"
        assert req.headers["x-seeknal-report-name"] == "my-report"
        assert isinstance(result, PublishResponse)
        assert result.slug == "abc123"


class TestPublishMaps201ToResponse:
    def test_publish_maps_201_to_response(self, tmp_path: Path):
        success_body = {
            "slug": "rep-xyz",
            "share_url": "http://test-server/r/rep-xyz",
            "owner_secret": "s3cr3t",
            "created_at": "2026-04-01T12:00:00Z",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(201, json=success_body)

        tarball = _make_tarball(tmp_path)
        client = PublishClient(server="http://test-server", api_key="key")

        with _transport_patch(handler):
            result = client.publish(tarball, "my-report")

        assert isinstance(result, PublishResponse)
        assert result.slug == "rep-xyz"
        assert result.share_url == "http://test-server/r/rep-xyz"
        assert result.owner_secret == "s3cr3t"
        assert result.created_at == "2026-04-01T12:00:00Z"


class TestPublishMaps401ToAuthError:
    def test_publish_maps_401_to_auth_error(self, tmp_path: Path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(401, text="Unauthorized")

        tarball = _make_tarball(tmp_path)
        client = PublishClient(server="http://test-server", api_key="bad-key")

        with _transport_patch(handler):
            with pytest.raises(PublishAuthError):
                client.publish(tarball, "my-report")


class TestPublishMaps413ToTooLargeError:
    def test_publish_maps_413_to_too_large_error(self, tmp_path: Path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(413, text="Payload Too Large")

        tarball = _make_tarball(tmp_path)
        client = PublishClient(server="http://test-server", api_key="key")

        with _transport_patch(handler):
            with pytest.raises(PublishTooLargeError):
                client.publish(tarball, "my-report")


class TestPublishMaps500ToServerError:
    def test_publish_maps_500_to_server_error(self, tmp_path: Path):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="Internal Server Error")

        tarball = _make_tarball(tmp_path)
        client = PublishClient(server="http://test-server", api_key="key")

        with _transport_patch(handler):
            with pytest.raises(PublishServerError) as exc_info:
                client.publish(tarball, "my-report")

        assert exc_info.value.status_code == 500


class TestRevokeSendsOwnerSecretHeader:
    def test_revoke_sends_owner_secret_header(self, tmp_path: Path):
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(
                200,
                json={"slug": "rep-xyz", "revoked_at": "2026-04-02T00:00:00Z"},
            )

        client = PublishClient(server="http://test-server", api_key="key")

        with _transport_patch(handler):
            result = client.revoke("rep-xyz", "owner-secret-value")

        assert len(captured) == 1
        req = captured[0]
        assert req.headers["x-seeknal-owner-secret"] == "owner-secret-value"
        assert isinstance(result, RevokeResponse)
        assert result.slug == "rep-xyz"
        assert result.revoked_at == "2026-04-02T00:00:00Z"


class TestNoAuthHeaderWhenNoApiKey:
    def test_no_auth_header_when_no_api_key(self, tmp_path: Path):
        captured: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(
                201,
                json={
                    "slug": "open-slug",
                    "share_url": "http://test-server/r/open-slug",
                    "owner_secret": "s",
                    "created_at": "2026-01-01T00:00:00Z",
                },
            )

        tarball = _make_tarball(tmp_path)
        client = PublishClient(server="http://test-server", api_key=None)

        with _transport_patch(handler):
            client.publish(tarball, "open-report")

        req = captured[0]
        assert "authorization" not in {k.lower() for k in req.headers}
