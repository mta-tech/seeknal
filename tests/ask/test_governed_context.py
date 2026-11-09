"""Tests for GovernedAskContext (TASK-SPEC.md § Governed mode)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from seeknal.ask.governed import (
    GovernedAskConfigError,
    GovernedAskContext,
    PROHIBITED_CREDENTIAL_VARS,
)


@pytest.fixture
def token_file(tmp_path: Path) -> Path:
    token_path = tmp_path / "token.jwt"
    token_path.write_text("initial-session-token", encoding="utf-8")
    os.chmod(token_path, 0o600)
    return token_path


def _set_valid_base_env(monkeypatch: pytest.MonkeyPatch, token_file: Path) -> None:
    # Clear any prohibited variables that might be in the environment
    for var in PROHIBITED_CREDENTIAL_VARS:
        monkeypatch.delenv(var, raising=False)
    for key in list(os.environ):
        if key.startswith("SEEKNAL_PG_") and "PASSWORD" in key:
            monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("ATLAS_ASK_MODE", "governed")
    monkeypatch.setenv("ATLAS_CATALOG_URL", "https://catalog.example.com")
    monkeypatch.setenv("ATLAS_ASK_SESSION_TOKEN_FILE", str(token_file))


@pytest.mark.parametrize(
    "prohibited_var",
    [
        *PROHIBITED_CREDENTIAL_VARS,
        "SEEKNAL_PG_PASSWORD",
        "SEEKNAL_PG_PRIMARY_PASSWORD",
        "SEEKNAL_PG_USER_PASSWORD",
    ],
)
def test_from_env_refuses_prohibited_variables(
    monkeypatch: pytest.MonkeyPatch,
    token_file: Path,
    prohibited_var: str,
):
    _set_valid_base_env(monkeypatch, token_file)
    monkeypatch.setenv(prohibited_var, "static-secret-credential")

    with pytest.raises(Exception) as exc_info:
        GovernedAskContext.from_env()

    assert prohibited_var in str(exc_info.value)


def test_from_env_refuses_missing_catalog_url(
    monkeypatch: pytest.MonkeyPatch,
    token_file: Path,
):
    _set_valid_base_env(monkeypatch, token_file)
    monkeypatch.delenv("ATLAS_CATALOG_URL", raising=False)

    with pytest.raises(Exception) as exc_info:
        GovernedAskContext.from_env()

    assert "ATLAS_CATALOG_URL" in str(exc_info.value)


def test_from_env_refuses_http_without_flag(
    monkeypatch: pytest.MonkeyPatch,
    token_file: Path,
):
    _set_valid_base_env(monkeypatch, token_file)
    monkeypatch.setenv("ATLAS_CATALOG_URL", "http://catalog.example.com")
    monkeypatch.delenv("ATLAS_ASK_ALLOW_HTTP", raising=False)

    with pytest.raises(Exception) as exc_info:
        GovernedAskContext.from_env()

    assert "https" in str(exc_info.value).lower() or "allow_http" in str(exc_info.value).lower()


def test_from_env_allows_http_with_flag(
    monkeypatch: pytest.MonkeyPatch,
    token_file: Path,
):
    _set_valid_base_env(monkeypatch, token_file)
    monkeypatch.setenv("ATLAS_CATALOG_URL", "http://catalog.example.com")
    monkeypatch.setenv("ATLAS_ASK_ALLOW_HTTP", "true")

    ctx = GovernedAskContext.from_env()
    assert ctx.catalog_url == "http://catalog.example.com"


def test_from_env_refuses_group_world_readable_token_file(
    monkeypatch: pytest.MonkeyPatch,
    token_file: Path,
):
    _set_valid_base_env(monkeypatch, token_file)
    os.chmod(token_file, 0o644)

    with pytest.raises(Exception) as exc_info:
        GovernedAskContext.from_env()

    assert "group- or world-readable" in str(exc_info.value).lower() or "mode" in str(exc_info.value).lower()


def test_token_is_reread_when_file_changes(
    monkeypatch: pytest.MonkeyPatch,
    token_file: Path,
):
    _set_valid_base_env(monkeypatch, token_file)
    ctx = GovernedAskContext.from_env()

    # First read returns initial token
    assert ctx.client._read_token() == "initial-session-token"

    # Controller updates token file
    token_file.write_text("refreshed-session-token-v2", encoding="utf-8")
    os.chmod(token_file, 0o600)

    # Next call dynamically re-reads the updated token without process restart
    assert ctx.client._read_token() == "refreshed-session-token-v2"
