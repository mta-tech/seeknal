"""Iceberg REST catalog (Lakekeeper) discovery source."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

import httpx

from seeknal.serving.discovery.config import (
    DiscoveryConfig,
    DiscoveryError,
    LakekeeperConfig,
)
from seeknal.serving.discovery.oauth import ClientCredentialsTokenSource


@dataclass(frozen=True)
class IcebergTable:
    """Discovered Iceberg table metadata."""

    namespace: str
    name: str
    columns: tuple[str, ...]
    keys: tuple[str, ...]
    row_count: int | None
    current_snapshot_id: int | None
    snapshot_ms: int | None


def _sanitize(msg: str, token: str | None) -> str:
    """Remove token value from error messages."""
    if token and token in msg:
        return msg.replace(token, "***")
    return msg


class LakekeeperSource:
    """Discovers tables, schemas, and snapshots from an Iceberg REST catalog."""

    def __init__(
        self,
        config: LakekeeperConfig | DiscoveryConfig,
        client: httpx.Client,
        env: Mapping[str, str] = os.environ,
        *,
        timeout: float | None = None,
        clock: Callable[[], float] = time.time,
        token_source: ClientCredentialsTokenSource | None = None,
    ) -> None:
        if isinstance(config, DiscoveryConfig):
            if config.lakekeeper is None:
                raise DiscoveryError("Lakekeeper is not configured in DiscoveryConfig")
            self.config = config.lakekeeper
            self.timeout = timeout if timeout is not None else config.timeout_seconds
        else:
            self.config = config
            self.timeout = timeout if timeout is not None else 10.0

        self.client = client
        self.env = env
        self.clock = clock
        self.warnings: list[str] = []

        self._token: str | None = None
        self._token_source: ClientCredentialsTokenSource | None = None

        if token_source is not None:
            self._token_source = token_source
        elif self.config.oauth is not None:
            self._token_source = ClientCredentialsTokenSource(
                self.config.oauth,
                client=self.client,
                env=self.env,
                clock=self.clock,
            )
        elif self.config.token_env:
            self._token = self.env.get(self.config.token_env)

    @property
    def _headers(self) -> dict[str, str]:
        headers = {}
        if self._token_source is not None:
            token = self._token_source.token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        elif self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _sanitize(self, msg: str) -> str:
        """Remove token value from error messages."""
        if self._token and self._token in msg:
            msg = msg.replace(self._token, "***")
        if self._token_source is not None and self._token_source._cached_token:
            cached = self._token_source._cached_token
            if cached and cached in msg:
                msg = msg.replace(cached, "***")
        return msg

    def _get(self, url: str, params: dict | None = None) -> httpx.Response:
        """Issue GET request to Lakekeeper, retrying once on 401 with token invalidation."""
        headers = self._headers
        resp = self.client.get(
            url, params=params, headers=headers, timeout=self.timeout
        )
        if resp.status_code == 401 and self._token_source is not None:
            self._token_source.invalidate()
            headers = self._headers
            resp = self.client.get(
                url, params=params, headers=headers, timeout=self.timeout
            )
        if resp.status_code == 401:
            raise DiscoveryError("Lakekeeper request unauthorized (401)")
        resp.raise_for_status()
        return resp

    def discover(self) -> list[IcebergTable]:
        """Discover Iceberg tables following steps 1-4 of the Iceberg REST spec."""
        self.warnings.clear()
        base_url = self.config.url.rstrip("/")

        # 1. GET {url}/v1/config?warehouse=<warehouse>
        try:
            resp = self._get(
                f"{base_url}/v1/config",
                params={"warehouse": self.config.warehouse},
            )
            cfg_data = resp.json()
        except DiscoveryError:
            raise
        except Exception as exc:
            msg = self._sanitize(str(exc))
            raise DiscoveryError(f"Failed to fetch Lakekeeper config: {msg}") from exc

        overrides = cfg_data.get("overrides") or {}
        defaults = cfg_data.get("defaults") or {}
        prefix = overrides.get("prefix") or defaults.get("prefix") or ""
        prefix_clean = prefix.strip("/")

        v1_base = f"{base_url}/v1/{prefix_clean}" if prefix_clean else f"{base_url}/v1"

        # 2. GET {url}/v1/{prefix}/namespaces
        raw_namespaces: list[Any] = []
        page_token: str | None = None
        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token
            try:
                resp = self._get(
                    f"{v1_base}/namespaces",
                    params=params,
                )
                ns_data = resp.json()
            except DiscoveryError:
                raise
            except Exception as exc:
                msg = self._sanitize(str(exc))
                raise DiscoveryError(f"Failed to list namespaces: {msg}") from exc

            raw_namespaces.extend(ns_data.get("namespaces") or [])
            page_token = ns_data.get("next-page-token")
            if not page_token:
                break

        # Process namespaces
        single_level_namespaces: list[str] = []
        for item in raw_namespaces:
            if isinstance(item, (list, tuple)):
                if len(item) == 1:
                    ns_name = str(item[0])
                else:
                    self.warnings.append(
                        f"Skipping multi-level namespace {'.'.join(str(p) for p in item)}"
                    )
                    continue
            elif isinstance(item, str):
                if "." in item or "\x1f" in item:
                    self.warnings.append(
                        f"Skipping multi-level namespace {item}"
                    )
                    continue
                ns_name = item
            else:
                continue

            if self.config.namespaces is not None:
                if ns_name not in self.config.namespaces:
                    continue

            single_level_namespaces.append(ns_name)

        # 3. GET {url}/v1/{prefix}/namespaces/{ns}/tables
        tables_to_fetch: list[tuple[str, str]] = []  # (ns, table)
        for ns_name in single_level_namespaces:
            page_token = None
            while True:
                params = {}
                if page_token:
                    params["pageToken"] = page_token
                try:
                    resp = self._get(
                        f"{v1_base}/namespaces/{ns_name}/tables",
                        params=params,
                    )
                    tbl_data = resp.json()
                except DiscoveryError:
                    raise
                except Exception as exc:
                    msg = self._sanitize(str(exc))
                    raise DiscoveryError(
                        f"Failed to list tables in namespace '{ns_name}': {msg}"
                    ) from exc

                identifiers = tbl_data.get("identifiers") or tbl_data.get("tables") or []
                for ident in identifiers:
                    if isinstance(ident, dict):
                        tbl_name = ident.get("name")
                    else:
                        tbl_name = str(ident)
                    if tbl_name:
                        tables_to_fetch.append((ns_name, tbl_name))

                page_token = tbl_data.get("next-page-token")
                if not page_token:
                    break

        # 4. GET {url}/v1/{prefix}/namespaces/{ns}/tables/{table}
        discovered_tables: list[IcebergTable] = []
        for ns_name, tbl_name in tables_to_fetch:
            try:
                resp = self._get(
                    f"{v1_base}/namespaces/{ns_name}/tables/{tbl_name}",
                )
                data = resp.json()
                metadata = data.get("metadata") or data

                # Parse schema
                current_schema_id = metadata.get("current-schema-id")
                schemas = metadata.get("schemas") or []
                target_schema = None
                for s in schemas:
                    if s.get("schema-id") == current_schema_id:
                        target_schema = s
                        break
                if target_schema is None:
                    target_schema = metadata.get("schema")
                if target_schema is None and schemas:
                    target_schema = schemas[0]

                if target_schema is None:
                    raise ValueError(f"No schema found in table metadata for '{ns_name}.{tbl_name}'")

                fields = target_schema.get("fields") or []
                columns = tuple(str(f["name"]) for f in fields if "name" in f)

                # Parse identifier-field-ids
                id_to_name = {
                    f["id"]: str(f["name"])
                    for f in fields
                    if "id" in f and "name" in f
                }
                ident_ids = target_schema.get("identifier-field-ids") or []
                keys = tuple(id_to_name[fid] for fid in ident_ids if fid in id_to_name)

                # Parse snapshots
                raw_snap_id = metadata.get("current-snapshot-id")
                current_snapshot_id = (
                    int(raw_snap_id)
                    if raw_snap_id is not None and raw_snap_id != -1
                    else None
                )

                row_count: int | None = None
                snapshot_ms: int | None = None

                if current_snapshot_id is not None:
                    snapshots = metadata.get("snapshots") or []
                    for snap in snapshots:
                        if snap.get("snapshot-id") == current_snapshot_id:
                            summary = snap.get("summary") or {}
                            if "total-records" in summary:
                                try:
                                    row_count = int(summary["total-records"])
                                except (TypeError, ValueError):
                                    row_count = None
                            if "timestamp-ms" in snap:
                                try:
                                    snapshot_ms = int(snap["timestamp-ms"])
                                except (TypeError, ValueError):
                                    snapshot_ms = None
                            break

                discovered_tables.append(
                    IcebergTable(
                        namespace=ns_name,
                        name=tbl_name,
                        columns=columns,
                        keys=keys,
                        row_count=row_count,
                        current_snapshot_id=current_snapshot_id,
                        snapshot_ms=snapshot_ms,
                    )
                )
            except DiscoveryError as exc:
                if "401" in str(exc):
                    raise
                msg = self._sanitize(str(exc))
                self.warnings.append(
                    f"Failed to read metadata for table '{ns_name}.{tbl_name}': {msg}"
                )
                continue
            except Exception as exc:
                msg = self._sanitize(str(exc))
                self.warnings.append(
                    f"Failed to read metadata for table '{ns_name}.{tbl_name}': {msg}"
                )
                continue

        return discovered_tables
