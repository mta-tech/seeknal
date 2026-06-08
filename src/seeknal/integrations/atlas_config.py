"""Persisted Atlas connection config, so the CLI doesn't need 3 env vars per shell.

The integration needs three connection settings — the seeknal-api URL, the portal
URL, and the Keycloak issuer. Requiring them as environment variables in every shell
is error-prone (the recurring "not synced" / "session expired" confusion). This
module persists them once to ``~/.config/seeknal/atlas.json`` (override
``SEEKNAL_ATLAS_CONFIG_PATH``) and the ``*_from_env`` factories fall back to it.

Precedence everywhere is **environment variable > this config file > built-in
default**, so existing env-var users are unaffected. The file holds only *stable*
connection settings; tokens live separately in the credentials file. Reading never
raises: a missing or malformed file yields an empty config.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

#: Default config location (override with ``SEEKNAL_ATLAS_CONFIG_PATH``).
_DEFAULT_CONFIG_PATH = "~/.config/seeknal/atlas.json"

#: Standard ATLAS deployment ports/realm used when deriving from a single host.
_DEFAULT_API_PORT = 8000
_DEFAULT_PORTAL_PORT = 4200
_DEFAULT_KEYCLOAK_PORT = 8080
_DEFAULT_REALM = "atlas"
_DEFAULT_CLIENT_ID = "seeknal-cli"


@dataclass(frozen=True)
class AtlasConfig:
    """The stable Atlas connection settings. Empty strings mean "not configured"."""

    api_url: str = ""
    portal_url: str = ""
    keycloak_issuer: str = ""
    oidc_client_id: str = ""


def config_path() -> Path:
    """Resolve the Atlas config file path (env override, else the default)."""

    raw = os.getenv("SEEKNAL_ATLAS_CONFIG_PATH", "").strip() or _DEFAULT_CONFIG_PATH
    return Path(raw).expanduser()


def atlas_config() -> AtlasConfig:
    """Load the persisted Atlas config. Never raises — an absent/garbled file or a
    non-object payload yields an empty :class:`AtlasConfig`, preserving the env-var
    path."""

    try:
        data = json.loads(config_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return AtlasConfig()
    if not isinstance(data, dict):
        return AtlasConfig()

    def _str(key: str) -> str:
        value = data.get(key)
        return value.strip() if isinstance(value, str) else ""

    return AtlasConfig(
        api_url=_str("api_url").rstrip("/"),
        portal_url=_str("portal_url").rstrip("/"),
        keycloak_issuer=_str("keycloak_issuer").rstrip("/"),
        oidc_client_id=_str("oidc_client_id"),
    )


def save_atlas_config(config: AtlasConfig) -> Path:
    """Persist ``config`` atomically (mode 0600 from creation). Returns the path."""

    path = config_path()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    payload = {
        "api_url": config.api_url,
        "portal_url": config.portal_url,
        "keycloak_issuer": config.keycloak_issuer,
        "oidc_client_id": config.oidc_client_id,
    }
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), prefix=".atlas-", suffix=".tmp")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, indent=2))
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return path


def _host_of(value: str) -> str:
    """Return the bare hostname from a URL or a ``host[:port]`` string."""

    if not value:
        return ""
    if "://" in value:
        return urlparse(value).hostname or ""
    # bare "host" or "host:port"
    return value.split("/")[0].split(":")[0]


def _api_from_host(host: str) -> str:
    """Build the API URL from a ``--host`` value, honoring an explicit scheme or port.

    ``atlas-dev`` → ``http://atlas-dev:8000`` (standard port); ``atlas:9000`` →
    ``http://atlas:9000`` (the given port is kept); ``https://atlas:8443`` is used
    verbatim. portal/keycloak still derive from the bare host (override them
    explicitly for non-standard ports).
    """

    if not host:
        return ""
    cleaned = host.rstrip("/")
    if "://" in cleaned:
        return cleaned
    if ":" in cleaned:  # host:port — honor the port the user gave
        return f"http://{cleaned}"
    return f"http://{cleaned}:{_DEFAULT_API_PORT}"


def derive_config(
    *,
    host: str = "",
    api_url: str = "",
    portal_url: str = "",
    keycloak_issuer: str = "",
    realm: str = _DEFAULT_REALM,
    oidc_client_id: str = "",
) -> AtlasConfig:
    """Build a full :class:`AtlasConfig` from a single ``host`` (or ``api_url``).

    Any explicit ``portal_url``/``keycloak_issuer`` wins; otherwise they are derived
    from the host using the standard ATLAS ports (api 8000, portal 4200, keycloak
    8080 + realm). This is what powers ``seeknal auth config set --host atlas-dev``.
    """

    base_host = _host_of(host or api_url)
    resolved_api = api_url.rstrip("/") or _api_from_host(host)
    resolved_portal = portal_url.rstrip("/") or (
        f"http://{base_host}:{_DEFAULT_PORTAL_PORT}" if base_host else ""
    )
    resolved_keycloak = keycloak_issuer.rstrip("/") or (
        f"http://{base_host}:{_DEFAULT_KEYCLOAK_PORT}/realms/{realm or _DEFAULT_REALM}"
        if base_host
        else ""
    )
    return AtlasConfig(
        api_url=resolved_api,
        portal_url=resolved_portal,
        keycloak_issuer=resolved_keycloak,
        oidc_client_id=oidc_client_id.strip() or _DEFAULT_CLIENT_ID,
    )
