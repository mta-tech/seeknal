from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from seeknal.report_server.auth import AuthError, require_publish_auth
from seeknal.report_server.config import ServerConfig


def _make_request(auth_header: str | None = None) -> MagicMock:
    request = MagicMock()
    headers = {}
    if auth_header is not None:
        headers["Authorization"] = auth_header
    request.headers = headers
    return request


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def test_open_mode_allows_missing_header() -> None:
    config = ServerConfig(auth_mode="open", api_keys=[])
    request = _make_request(auth_header=None)
    _run(require_publish_auth(request, config))  # must not raise


def test_api_key_mode_rejects_missing_header() -> None:
    config = ServerConfig(auth_mode="api_key", api_keys=["valid-key"])
    request = _make_request(auth_header=None)
    with pytest.raises(AuthError):
        _run(require_publish_auth(request, config))


def test_api_key_mode_rejects_wrong_key() -> None:
    config = ServerConfig(auth_mode="api_key", api_keys=["correct-key"])
    request = _make_request(auth_header="Bearer wrong-key")
    with pytest.raises(AuthError):
        _run(require_publish_auth(request, config))


def test_api_key_mode_accepts_correct_key() -> None:
    config = ServerConfig(auth_mode="api_key", api_keys=["correct-key"])
    request = _make_request(auth_header="Bearer correct-key")
    _run(require_publish_auth(request, config))  # must not raise
