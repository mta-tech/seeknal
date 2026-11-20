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
    return asyncio.run(coro)


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


def test_blank_api_key_rejected_on_startup_and_empty_bearer_never_authenticates(
    tmp_path,
    monkeypatch,
) -> None:
    from seeknal.report_server.app import create_app

    monkeypatch.setattr("seeknal.report_server.config.is_insecure_path", lambda _p: False)
    monkeypatch.setattr("seeknal.report_server.storage.is_insecure_path", lambda _p: False)

    for bad_keys in (["real-secret-key", ""], ["real-secret-key", "   "]):
        cfg = ServerConfig(
            data_dir=tmp_path / "blank-key-data",
            auth_mode="api_key",
            api_keys=bad_keys,
        )
        with pytest.raises(ValueError) as exc_info:
            cfg.validate()
        msg = str(exc_info.value)
        assert "SEEKNAL_REPORT_SERVER_KEYS" in msg
        assert "real-secret-key" not in msg

        with pytest.raises(ValueError) as app_exc_info:
            create_app(cfg)
        app_msg = str(app_exc_info.value)
        assert "SEEKNAL_REPORT_SERVER_KEYS" in app_msg
        assert "real-secret-key" not in app_msg

    # Even if validate() was not called on a ServerConfig with an empty key,
    # an empty or whitespace bearer token must never authenticate.
    unvalidated_cfg = ServerConfig(auth_mode="api_key", api_keys=["real-secret-key", ""])
    for header in ("Bearer ", "Bearer    "):
        request = _make_request(auth_header=header)
        with pytest.raises(AuthError):
            _run(require_publish_auth(request, unvalidated_cfg))

