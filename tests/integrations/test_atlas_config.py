"""Tests for the persisted Atlas connection config (atlas_config) + precedence."""

from __future__ import annotations

import json

import seeknal.integrations.atlas_catalog as catalog_mod
import seeknal.integrations.atlas_governance as gov_mod
from seeknal.integrations.atlas_config import (
    AtlasConfig,
    atlas_config,
    derive_config,
    save_atlas_config,
)


def _point_at(monkeypatch, tmp_path):
    path = tmp_path / "atlas.json"
    monkeypatch.setenv("SEEKNAL_ATLAS_CONFIG_PATH", str(path))
    return path


def test_absent_config_is_empty(monkeypatch, tmp_path):
    _point_at(monkeypatch, tmp_path)
    assert atlas_config() == AtlasConfig()


def test_save_load_roundtrip_is_0600(monkeypatch, tmp_path):
    path = _point_at(monkeypatch, tmp_path)
    save_atlas_config(derive_config(host="atlas-dev-server"))
    assert oct(path.stat().st_mode & 0o777) == "0o600"
    loaded = atlas_config()
    assert loaded.api_url == "http://atlas-dev-server:8000"
    assert loaded.portal_url == "http://atlas-dev-server:4200"
    assert loaded.keycloak_issuer == "http://atlas-dev-server:8080/realms/atlas"
    assert loaded.oidc_client_id == "seeknal-cli"


def test_derive_explicit_overrides_win():
    cfg = derive_config(host="h", portal_url="http://portal:9999", realm="custom")
    assert cfg.api_url == "http://h:8000"
    assert cfg.portal_url == "http://portal:9999"  # explicit wins
    assert cfg.keycloak_issuer == "http://h:8080/realms/custom"


def test_derive_from_api_url_host():
    cfg = derive_config(api_url="https://atlas.example:8000")
    assert cfg.portal_url == "http://atlas.example:4200"
    assert cfg.keycloak_issuer == "http://atlas.example:8080/realms/atlas"


def test_garbled_config_yields_empty(monkeypatch, tmp_path):
    path = _point_at(monkeypatch, tmp_path)
    path.write_text("not json", encoding="utf-8")
    assert atlas_config() == AtlasConfig()
    path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")
    assert atlas_config() == AtlasConfig()


def test_catalog_factory_uses_config_when_env_unset(monkeypatch, tmp_path):
    _point_at(monkeypatch, tmp_path)
    save_atlas_config(derive_config(host="cfg-host"))
    for var in ("ATLAS_API_URL", "ATLAS_PORTAL_URL", "ATLAS_API_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    client = catalog_mod.create_catalog_client_from_env()
    assert client is not None
    assert client.config.base_url == "http://cfg-host:8000"
    assert client._portal_url == "http://cfg-host:4200"


def test_env_var_wins_over_config(monkeypatch, tmp_path):
    _point_at(monkeypatch, tmp_path)
    save_atlas_config(derive_config(host="cfg-host"))
    monkeypatch.setenv("ATLAS_API_URL", "http://env-host:8000")
    client = catalog_mod.create_catalog_client_from_env()
    assert client is not None and client.config.base_url == "http://env-host:8000"


def test_oidc_issuer_falls_back_to_config(monkeypatch, tmp_path):
    _point_at(monkeypatch, tmp_path)
    save_atlas_config(derive_config(host="kc-host"))
    monkeypatch.delenv("KEYCLOAK_ISSUER", raising=False)
    assert gov_mod._oidc_issuer() == "http://kc-host:8080/realms/atlas"
