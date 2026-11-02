"""Cube semantic layer discovery source."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import httpx

from seeknal.serving.discovery.config import (
    DiscoveryConfigError,
    DiscoveryError,
    EngineConfig,
)


VALID_TWIN_METHODS = frozenset({"redact", "null", "year", "last4"})


@dataclass(frozen=True)
class CubeMeta:
    """Discovered semantic cube or view metadata."""

    name: str
    measures: tuple[str, ...]
    dimensions: tuple[str, ...]
    mask_variants: Mapping[str, tuple[str, ...]] = field(default_factory=dict)


def _sanitize(msg: str, *secrets: str | None) -> str:
    """Remove token and secret values from error messages."""
    for s in secrets:
        if s and s in msg:
            msg = msg.replace(s, "***")
    return msg


def _mint_cube_token(
    secret: bytes | str,
    claims: Mapping[str, Any] | None,
    now: float,
) -> str:
    """Mint a compact HS256 JWT using stdlib only."""
    iat = int(now)
    exp = iat + 300

    payload: dict[str, Any] = dict(claims or {})
    payload["iat"] = iat
    payload["exp"] = exp

    header = {"alg": "HS256", "typ": "JWT"}
    header_json = json.dumps(header, separators=(",", ":")).encode("utf-8")
    payload_json = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    header_b64 = base64.urlsafe_b64encode(header_json).rstrip(b"=").decode("ascii")
    payload_b64 = base64.urlsafe_b64encode(payload_json).rstrip(b"=").decode("ascii")

    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")
    secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret

    sig = hmac.new(secret_bytes, signing_input, hashlib.sha256).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=").decode("ascii")

    return f"{header_b64}.{payload_b64}.{sig_b64}"


class CubeSource:
    """Discovers cubes and views from Cube's REST metadata API."""

    def __init__(
        self,
        engine_config: EngineConfig,
        client: httpx.Client,
        env: Mapping[str, str] = os.environ,
        *,
        timeout: float = 10.0,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.engine_config = engine_config
        self.client = client
        self.env = env
        self.timeout = timeout
        self.clock = clock

        self._token: str | None = None
        if self.engine_config.token_env:
            self._token = self.env.get(self.engine_config.token_env)

    def _get_secret(self) -> str:
        """Resolve Cube API secret from environment or secret file."""
        if self.engine_config.api_secret_env:
            val = self.env.get(self.engine_config.api_secret_env)
            if not val:
                raise DiscoveryError(
                    f"Cube API secret environment variable '{self.engine_config.api_secret_env}' not set"
                )
            return val
        elif self.engine_config.api_secret_file:
            path = self.engine_config.api_secret_file
            if not os.path.exists(path):
                raise DiscoveryError(f"Cube API secret file not found: '{path}'")
            st = os.stat(path)
            if (st.st_mode & 0o044) != 0:
                raise DiscoveryConfigError(
                    f"Cube API secret file '{path}' is group- or world-readable"
                )
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            if content.endswith("\r\n"):
                return content[:-2]
            elif content.endswith("\n"):
                return content[:-1]
            return content
        else:
            raise DiscoveryError("No Cube API secret configured")

    def discover(self) -> list[CubeMeta]:
        """Fetch /cubejs-api/v1/meta and return list of CubeMeta."""
        if not self.engine_config.url:
            raise DiscoveryError(
                f"Cube engine '{self.engine_config.name}' has no URL configured"
            )

        token = self._token
        secret = None
        if self.engine_config.api_secret_env or self.engine_config.api_secret_file:
            secret = self._get_secret()
            token = _mint_cube_token(
                secret=secret,
                claims=self.engine_config.api_claims,
                now=self.clock(),
            )

        headers = {}
        if token:
            headers["Authorization"] = token

        url = f"{self.engine_config.url.rstrip('/')}/cubejs-api/v1/meta"

        try:
            resp = self.client.get(
                url,
                headers=headers,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except DiscoveryError:
            raise
        except Exception as exc:
            msg = _sanitize(str(exc), token, secret)
            raise DiscoveryError(f"Failed to fetch Cube metadata: {msg}") from exc

        cubes_raw: list[Any] = data.get("cubes") or []
        result: list[CubeMeta] = []

        for entry in cubes_raw:
            if not isinstance(entry, dict):
                continue
            cube_name = entry.get("name")
            if not cube_name or not isinstance(cube_name, str):
                continue
            cube_name = cube_name.strip()
            prefix = f"{cube_name}."

            measures: list[str] = []
            for m in entry.get("measures") or []:
                m_name = m.get("name") if isinstance(m, dict) else str(m)
                if not m_name:
                    continue
                m_name = str(m_name).strip()
                if m_name.startswith(prefix):
                    m_name = m_name[len(prefix) :]
                measures.append(m_name)

            raw_dims: list[str] = []
            for d in entry.get("dimensions") or []:
                d_name = d.get("name") if isinstance(d, dict) else str(d)
                if not d_name:
                    continue
                d_name = str(d_name).strip()
                if d_name.startswith(prefix):
                    d_name = d_name[len(prefix) :]
                raw_dims.append(d_name)

            raw_dims_set = set(raw_dims)
            twin_map: dict[str, list[str]] = {}
            twin_dims: set[str] = set()
            for d in raw_dims:
                if "__" in d:
                    base, method = d.rsplit("__", 1)
                    if method in VALID_TWIN_METHODS and base in raw_dims_set and base != d:
                        twin_dims.add(d)
                        if base not in twin_map:
                            twin_map[base] = []
                        if method not in twin_map[base]:
                            twin_map[base].append(method)

            ordinary_dims = [d for d in raw_dims if d not in twin_dims]
            mask_variants = {b: tuple(methods) for b, methods in twin_map.items()}

            result.append(
                CubeMeta(
                    name=cube_name,
                    measures=tuple(measures),
                    dimensions=tuple(ordinary_dims),
                    mask_variants=mask_variants,
                )
            )

        return result
