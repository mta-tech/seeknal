"""OAuth2 client credentials token source for Seeknal Serving discovery."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import httpx

from seeknal.serving.discovery.config import DiscoveryConfigError, DiscoveryError


class ClientCredentialsTokenSource:
    """OAuth2 client credentials token source with caching and invalidation."""

    def __init__(
        self,
        cfg: Any,
        client: httpx.Client,
        env: Mapping[str, str] = os.environ,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.cfg = cfg
        self.client = client
        self.env = env
        self.clock = clock

        if isinstance(cfg, Mapping):
            self.token_url: str = str(cfg.get("token_url", ""))
            self.client_id: str = str(cfg.get("client_id", ""))
            self.client_secret_env: str | None = cfg.get("client_secret_env")
            self.client_secret_file: str | None = cfg.get("client_secret_file")
            self.scope: str | None = cfg.get("scope")
            self.audience: str | None = cfg.get("audience")
        else:
            self.token_url = getattr(cfg, "token_url", "")
            self.client_id = getattr(cfg, "client_id", "")
            self.client_secret_env = getattr(cfg, "client_secret_env", None)
            self.client_secret_file = getattr(cfg, "client_secret_file", None)
            self.scope = getattr(cfg, "scope", None)
            self.audience = getattr(cfg, "audience", None)

        self._lock = threading.Lock()
        self._cached_token: str | None = None
        self._cached_until: float = 0.0

    def _read_secret(self) -> str:
        """Resolve client secret from environment or secret file."""
        if self.client_secret_env:
            val = self.env.get(self.client_secret_env)
            if not val:
                raise DiscoveryError(
                    f"OAuth client secret environment variable '{self.client_secret_env}' not set"
                )
            return val
        elif self.client_secret_file:
            path = self.client_secret_file
            if not os.path.exists(path):
                raise DiscoveryError(f"OAuth client secret file not found: '{path}'")
            st = os.stat(path)
            if (st.st_mode & 0o044) != 0:
                raise DiscoveryConfigError(
                    f"OAuth client secret file '{path}' is group- or world-readable"
                )
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            if content.endswith("\r\n"):
                return content[:-2]
            elif content.endswith("\n"):
                return content[:-1]
            return content
        else:
            raise DiscoveryError("No OAuth client secret configured")

    def token(self) -> str:
        """Obtain a valid access token, caching until expires_at - 30s or for 60s."""
        with self._lock:
            now = self.clock()
            if self._cached_token is not None and now < self._cached_until:
                return self._cached_token

            try:
                secret = self._read_secret()
                data: dict[str, str] = {
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": secret,
                }
                if self.scope is not None:
                    data["scope"] = self.scope
                if self.audience is not None:
                    data["audience"] = self.audience

                resp = self.client.post(
                    self.token_url,
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                resp.raise_for_status()
                payload = resp.json()
            except DiscoveryConfigError:
                raise
            except Exception as exc:
                raise DiscoveryError(
                    f"OAuth token request failed: {type(exc).__name__}"
                ) from None

            if not isinstance(payload, Mapping) or "access_token" not in payload:
                raise DiscoveryError("OAuth response missing 'access_token'")

            access_token = payload["access_token"]
            if not isinstance(access_token, str) or not access_token:
                raise DiscoveryError("OAuth response contains invalid 'access_token'")

            raw_expires_in = payload.get("expires_in")
            if raw_expires_in is not None:
                try:
                    exp_sec = float(raw_expires_in)
                    cached_until = now + exp_sec - 30.0
                except (TypeError, ValueError):
                    cached_until = now + 60.0
            else:
                cached_until = now + 60.0

            self._cached_token = access_token
            self._cached_until = cached_until
            return self._cached_token

    def invalidate(self) -> None:
        """Drop the cached token."""
        with self._lock:
            self._cached_token = None
            self._cached_until = 0.0
