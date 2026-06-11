"""Tests for `seeknal auth config` set/show and `auth login --host` one-step setup."""

from __future__ import annotations

import httpx
from typer.testing import CliRunner

import seeknal.cli.auth as auth_mod
from seeknal.cli.auth import auth_app

runner = CliRunner()


def _isolate_config(monkeypatch, tmp_path):
    path = tmp_path / "atlas.json"
    monkeypatch.setenv("SEEKNAL_ATLAS_CONFIG_PATH", str(path))
    return path


def test_config_set_host_derives_and_persists(monkeypatch, tmp_path):
    path = _isolate_config(monkeypatch, tmp_path)
    result = runner.invoke(auth_app, ["config", "set", "--host", "myhost"])
    assert result.exit_code == 0
    assert "http://myhost:8000" in result.output
    assert "http://myhost:4200" in result.output
    assert "http://myhost:8080/realms/atlas" in result.output
    assert path.exists()


def test_config_set_requires_host_or_api_url(monkeypatch, tmp_path):
    _isolate_config(monkeypatch, tmp_path)
    result = runner.invoke(auth_app, ["config", "set"])
    assert result.exit_code == 1
    assert "Provide --host" in result.output


def test_config_show_marks_env_override(monkeypatch, tmp_path):
    _isolate_config(monkeypatch, tmp_path)
    runner.invoke(auth_app, ["config", "set", "--host", "fhost"])
    monkeypatch.setenv("ATLAS_API_URL", "http://envhost:8000")
    result = runner.invoke(auth_app, ["config", "show"])
    assert result.exit_code == 0
    assert "http://envhost:8000  [env]" in result.output
    assert "http://fhost:4200  [config]" in result.output


def test_login_host_persists_config_before_auth(monkeypatch, tmp_path):
    path = _isolate_config(monkeypatch, tmp_path)

    def _boom(*args, **kwargs):
        raise httpx.HTTPError("discovery offline in test")

    # Fail OIDC discovery fast so the test never reaches a real Keycloak.
    monkeypatch.setattr(auth_mod, "_discover_endpoints", _boom)
    result = runner.invoke(auth_app, ["login", "--host", "lhost", "--no-browser"])
    assert path.exists()  # config persisted before the (failed) login
    assert result.exit_code == 1
