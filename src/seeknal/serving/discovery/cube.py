"""Cube semantic layer discovery source."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from seeknal.serving.discovery.config import DiscoveryError, EngineConfig


@dataclass(frozen=True)
class CubeMeta:
    """Discovered semantic cube or view metadata."""

    name: str
    measures: tuple[str, ...]
    dimensions: tuple[str, ...]


def _sanitize(msg: str, token: str | None) -> str:
    """Remove token value from error messages."""
    if token and token in msg:
        return msg.replace(token, "***")
    return msg


class CubeSource:
    """Discovers cubes and views from Cube's REST metadata API."""

    def __init__(
        self,
        engine_config: EngineConfig,
        client: httpx.Client,
        env: Mapping[str, str] = os.environ,
        *,
        timeout: float = 10.0,
    ) -> None:
        self.engine_config = engine_config
        self.client = client
        self.env = env
        self.timeout = timeout

        self._token: str | None = None
        if self.engine_config.token_env:
            self._token = self.env.get(self.engine_config.token_env)

    @property
    def _headers(self) -> dict[str, str]:
        headers = {}
        if self._token:
            headers["Authorization"] = self._token
        return headers

    def discover(self) -> list[CubeMeta]:
        """Fetch /cubejs-api/v1/meta and return list of CubeMeta."""
        if not self.engine_config.url:
            raise DiscoveryError(
                f"Cube engine '{self.engine_config.name}' has no URL configured"
            )

        url = f"{self.engine_config.url.rstrip('/')}/cubejs-api/v1/meta"

        try:
            resp = self.client.get(
                url,
                headers=self._headers,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            msg = _sanitize(str(exc), self._token)
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

            dimensions: list[str] = []
            for d in entry.get("dimensions") or []:
                d_name = d.get("name") if isinstance(d, dict) else str(d)
                if not d_name:
                    continue
                d_name = str(d_name).strip()
                if d_name.startswith(prefix):
                    d_name = d_name[len(prefix) :]
                dimensions.append(d_name)

            result.append(
                CubeMeta(
                    name=cube_name,
                    measures=tuple(measures),
                    dimensions=tuple(dimensions),
                )
            )

        return result
