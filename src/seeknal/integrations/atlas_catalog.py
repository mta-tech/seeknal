"""Read-only Atlas data-catalog client, active only when Atlas is configured.

This is the *catalog* surface of the Atlas integration: a GET-only client over the
unified-catalog read API. Where :mod:`seeknal.integrations.atlas_client` handles
apply-time dual writes and :mod:`seeknal.integrations.atlas_governance` enforces
runtime access, this module lets a caller *browse* the catalog — list, fetch,
search, and sample assets — plus a single write (:meth:`AtlasCatalogClient.annotate`)
that upserts annotations onto an existing asset.

Like the governance gate it is **config-activated**: with no ``ATLAS_API_URL`` the
factory returns ``None`` and callers can skip catalog features entirely. It reuses
the same :class:`~seeknal.integrations.atlas_client.AtlasContractConfig` and the
per-user token resolution so a single Atlas configuration drives every surface.

The backend asset shape is a JSON dict::

    {id, asset_type, name, namespace, source_system, source_id, canonical_source,
     metadata: {description, tags, canonical_asset_id}, created_at, updated_at}

:class:`Dataset` is the typed projection of that dict the rest of Seeknal consumes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence
from urllib.parse import quote

import httpx

from seeknal.integrations.atlas_client import (
    AtlasAuthError,
    AtlasContractConfig,
    AtlasContractError,
    SESSION_EXPIRED_HINT,
)
from seeknal.integrations.atlas_config import atlas_config
from seeknal.integrations.atlas_governance import (
    refresh_access_token,
    user_token_from_credentials,
)

__all__ = ["Dataset", "AtlasCatalogClient", "create_catalog_client_from_env"]


def _columns_from_schema_fields(raw: Any) -> tuple[tuple[str, str], ...]:
    """Normalise a catalog ``schemaFields`` list into ``(name, type)`` pairs.

    Tolerant of the shapes the catalog surfaces: ``{name, type}`` (portal/iceberg),
    ``{fieldPath, nativeDataType}`` (DataHub), or ``{name, dataType}``. Entries
    without a usable name are dropped; a non-list input yields ``()``.
    """

    if not isinstance(raw, list):
        return ()
    columns: list[tuple[str, str]] = []
    for field_def in raw:
        if not isinstance(field_def, dict):
            continue
        name = field_def.get("name") or field_def.get("fieldPath")
        if not name:
            continue
        col_type = (
            field_def.get("type")
            or field_def.get("nativeDataType")
            or field_def.get("dataType")
            or ""
        )
        columns.append((str(name), str(col_type)))
    return tuple(columns)


@dataclass(frozen=True)
class Dataset:
    """Typed projection of a unified-catalog asset.

    Construct from a raw backend asset dict via :meth:`from_api`. ``description``,
    ``tags``, and ``canonical_id`` are flattened out of the nested ``metadata`` block
    the backend returns, while the full ``metadata`` dict is retained verbatim.
    """

    id: str
    name: str
    namespace: str = ""
    asset_type: str = ""
    source_system: str = ""
    description: str = ""
    tags: tuple[str, ...] = ()
    canonical_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    #: ``(name, type)`` column pairs when the catalog exposes a schema for the
    #: dataset (Lakekeeper/iceberg tables and cubes via the portal). Empty when the
    #: source carries no schema. Lets ``seeknal dataset show`` render the columns the
    #: web detail view shows, without reading any data.
    columns: tuple[tuple[str, str], ...] = ()

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> "Dataset":
        """Map a raw backend asset dict to a :class:`Dataset`.

        Tolerant of missing fields: any absent key falls back to its dataclass
        default. ``description``, ``tags``, and ``canonical_asset_id`` are read from
        the nested ``metadata`` block; the whole ``metadata`` dict is preserved.
        """

        metadata = d.get("metadata") or {}
        raw_tags = metadata.get("tags") or []
        tags = tuple(str(tag) for tag in raw_tags if tag)
        return cls(
            id=str(d.get("id", "")),
            name=str(d.get("name", "")),
            namespace=str(d.get("namespace", "")),
            asset_type=str(d.get("asset_type", "")),
            source_system=str(d.get("source_system", "")),
            description=str(metadata.get("description", "")),
            tags=tags,
            canonical_id=str(metadata.get("canonical_asset_id", "")),
            metadata=dict(metadata),
            columns=_columns_from_schema_fields(
                metadata.get("schemaFields") or metadata.get("columns")
            ),
        )

    @classmethod
    def from_portal(cls, d: dict[str, Any]) -> "Dataset":
        """Map a portal ``/api/datasets`` row (the web catalog shape) to a Dataset.

        The portal aggregates Lakekeeper Iceberg tables and DataHub cube products and
        returns rows shaped as ``{urn, name:"{ns}.{table}", displayName, namespace,
        platform, type, tags}`` — where ``name`` is already the fully-qualified
        ``namespace.table``. We keep the short ``displayName`` as :attr:`name` and the
        ``namespace`` separate so :attr:`fqn` recomposes the governed identifier
        (``namespace.name``) without doubling the namespace. ``platform`` (iceberg/cube)
        becomes ``source_system`` and ``type`` (table/cube) becomes ``asset_type`` so the
        CLI renders the same columns the registry path produces.
        """

        namespace = str(d.get("namespace", "") or "")
        full_name = str(d.get("name", "") or "")
        display = str(d.get("displayName", "") or "")
        if display:
            name = display
        elif namespace and full_name.startswith(f"{namespace}."):
            name = full_name[len(namespace) + 1 :]
        else:
            name = full_name
        raw_tags = d.get("tags") or []
        tags = tuple(str(tag) for tag in raw_tags if tag)
        return cls(
            id=str(d.get("urn") or full_name),
            name=name,
            namespace=namespace,
            asset_type=str(d.get("type", "") or ""),
            source_system=str(d.get("platform", "") or ""),
            description=str(d.get("description", "") or ""),
            tags=tags,
            canonical_id="",
            metadata={
                "urn": d.get("urn"),
                "fullName": full_name,
                "displayName": display,
                "platform": d.get("platform"),
            },
            columns=_columns_from_schema_fields(d.get("schemaFields")),
        )

    @property
    def fqn(self) -> str:
        """Fully-qualified name for the dataset.

        Prefers the canonical asset id when present; otherwise composes
        ``"{namespace}.{name}"`` (dropping the leading dot when no namespace is set).
        """

        if self.canonical_id:
            return self.canonical_id
        if self.namespace:
            return f"{self.namespace}.{self.name}"
        return self.name


class AtlasCatalogClient:
    """GET-only HTTP client for the Atlas unified-catalog read API.

    Construct via :func:`create_catalog_client_from_env`. Pass an explicit ``client``
    (e.g. backed by :class:`httpx.MockTransport`) in tests. Mirrors the headers and
    HTTP/error handling of :class:`~seeknal.integrations.atlas_governance.GovernanceGate`.
    """

    def __init__(
        self,
        config: AtlasContractConfig,
        *,
        client: httpx.Client | None = None,
        token_refresher: Callable[[], str | None] | None = None,
        portal_url: str | None = None,
    ) -> None:
        self.config = config
        self._client = client
        # ``config.token`` is the initial bearer; keep a mutable copy so a 401-driven
        # refresh can swap in a new access token without rebuilding the frozen config.
        self._token = config.token
        self._token_refresher = token_refresher or refresh_access_token
        # When set (env ``ATLAS_PORTAL_URL``), ``list_datasets`` reads the portal's
        # ``/api/datasets`` aggregator — the *same* source the web UI lists — so the CLI
        # catalog matches the web (Lakekeeper tables + cubes) instead of the seeknal
        # asset registry. Unset → the registry path (``/api/assets``) is used.
        self._portal_url = (portal_url or "").rstrip("/") or None

    # -- HTTP plumbing -------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        return headers

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Issue a single HTTP request via the injected client or a one-shot call."""

        if self._client is not None:
            if method == "GET":
                return self._client.get(url, params=params, headers=self._headers())
            return self._client.post(url, json=json_body, headers=self._headers())
        if method == "GET":
            return httpx.get(
                url,
                params=params,
                headers=self._headers(),
                timeout=self.config.timeout_seconds,
            )
        return httpx.post(
            url,
            json=json_body,
            headers=self._headers(),
            timeout=self.config.timeout_seconds,
        )

    def _send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        base: str | None = None,
    ) -> Any:
        """Send a request, transparently refreshing the token once on HTTP 401.

        On a 401 the stored refresh token is used to mint a new access token and the
        request is retried exactly once. When no refresh is possible (or the retry is
        still 401), an :class:`AtlasAuthError` carrying a re-authentication hint is
        raised — never a raw traceback. Other HTTP/transport failures map to
        :class:`AtlasContractError` as before.

        ``base`` overrides the request host (default :attr:`config.base_url`) so the
        same per-user-token + 401-refresh plumbing can reach the portal aggregator at
        ``ATLAS_PORTAL_URL`` as well as the seeknal-api backend.
        """

        url = f"{base or self.config.base_url}{path}"
        refreshed = False
        while True:
            try:
                response = self._request(method, url, params=params, json_body=json_body)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 401:
                    if not refreshed:
                        refreshed = True
                        new_token = self._token_refresher()
                        if new_token:
                            self._token = new_token
                            continue
                    raise AtlasAuthError(SESSION_EXPIRED_HINT) from exc
                message = exc.response.text.strip() or str(exc)
                raise AtlasContractError(
                    f"Atlas catalog request failed for {path}: {message}"
                ) from exc
            except httpx.HTTPError as exc:
                raise AtlasContractError(
                    f"Atlas catalog request failed for {path}: {exc}"
                ) from exc

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        clean = {
            key: value for key, value in (params or {}).items() if value is not None
        }
        return self._send("GET", path, params=clean)

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._send("POST", path, json_body=payload)

    # -- Public API ----------------------------------------------------------

    def list_datasets(
        self,
        *,
        query: str | None = None,
        asset_type: str | None = None,
        namespace: str | None = None,
        source_system: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Dataset]:
        """List catalog assets, optionally filtered. Returns a list of :class:`Dataset`.

        When ``ATLAS_PORTAL_URL`` is configured this reads the portal's
        ``/api/datasets`` aggregator (the same Lakekeeper-tables + cube-products catalog
        the web UI lists) so the CLI matches the web. Otherwise it reads the seeknal
        asset registry: GET ``/api/assets`` (``query``→``q``, ``asset_type``→``type``),
        which returns a JSON array of asset dicts.
        """

        if self._portal_url is not None:
            return self._list_from_portal(
                query=query,
                asset_type=asset_type,
                namespace=namespace,
                source_system=source_system,
                limit=limit,
                offset=offset,
            )

        params = {
            "q": query,
            "type": asset_type,
            "namespace": namespace,
            "source_system": source_system,
            "limit": limit,
            "offset": offset,
        }
        resp = self._get("/api/assets", params)
        return [Dataset.from_api(asset) for asset in (resp or [])]

    def _list_from_portal(
        self,
        *,
        query: str | None,
        asset_type: str | None,
        namespace: str | None,
        source_system: str | None,
        limit: int,
        offset: int,
    ) -> list[Dataset]:
        """List datasets from the portal ``/api/datasets`` aggregator (web parity).

        The portal accepts ``q``/``namespace``/``platform``/``limit``/``offset`` and
        returns ``{"datasets": [...], "total", "facets"}`` (a bare array is also
        tolerated). ``source_system`` is forwarded as the portal's ``platform`` filter;
        ``asset_type`` (which the portal does not filter on) is applied client-side so
        the CLI's ``--type`` flag keeps working across both catalog sources.
        """

        params = {
            "q": query,
            "namespace": namespace,
            "platform": source_system,
            "limit": limit,
            "offset": offset,
        }
        clean = {key: value for key, value in params.items() if value is not None}
        resp = self._send("GET", "/api/datasets", params=clean, base=self._portal_url)
        if isinstance(resp, dict):
            rows = resp.get("datasets") or resp.get("results") or resp.get("items") or []
        else:
            rows = resp or []
        datasets = [Dataset.from_portal(row) for row in rows if isinstance(row, dict)]
        if asset_type:
            datasets = [d for d in datasets if d.asset_type == asset_type]
        return datasets

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Fetch a single catalog asset by id.

        GET ``/api/assets/{dataset_id}`` and project the result into a :class:`Dataset`.
        """

        resp = self._get(f"/api/assets/{dataset_id}")
        return Dataset.from_api(resp)

    def search(
        self,
        query: str,
        *,
        asset_types: Sequence[str] | None = None,
        tags: Sequence[str] | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Dataset]:
        """Full-text search across the catalog. Returns a list of :class:`Dataset`.

        GET ``/api/search`` with ``q=query``. The response may be a bare JSON array or
        a dict wrapping the hits under ``results``/``items``/``assets``; both shapes are
        handled, and each hit is mapped tolerantly via :meth:`Dataset.from_api`.
        """

        params = {
            "q": query,
            "asset_types": list(asset_types) if asset_types else None,
            "tags": list(tags) if tags else None,
            "limit": limit,
            "offset": offset,
        }
        resp = self._get("/api/search", params)
        if isinstance(resp, dict):
            hits = resp.get("results") or resp.get("items") or resp.get("assets") or []
        else:
            hits = resp or []
        return [Dataset.from_api(hit) for hit in hits if isinstance(hit, dict)]

    def sample(
        self, identifier: str, *, limit: int = 20, offset: int = 0
    ) -> dict[str, Any]:
        """Fetch a row sample for an asset, returning the backend JSON verbatim.

        GET ``/api/sample`` with the required ``identifier`` plus ``limit``/``offset``.
        The response is returned unmodified for the caller to render.
        """

        return self._get(
            "/api/sample",
            {"identifier": identifier, "limit": limit, "offset": offset},
        )

    def lineage(self, urn: str) -> dict[str, Any]:
        """Fetch a dataset's upstream/downstream lineage, returning the JSON verbatim.

        When the portal is configured, reads its DataHub-backed lineage route
        (``GET /api/datasets/{urn}/lineage`` → ``{dataset, upstreamLineage,
        downstreamLineage}``, empty arrays when DataHub has no lineage). Otherwise
        falls back to the backend relationships endpoint
        (``GET /api/assets/{urn}/relationships``). The ``urn`` is URL-encoded.
        """

        encoded = quote(urn, safe="")
        if self._portal_url is not None:
            return self._send(
                "GET", f"/api/datasets/{encoded}/lineage", base=self._portal_url
            )
        return self._get(f"/api/assets/{encoded}/relationships")

    def annotate(
        self,
        dataset_id: str,
        *,
        tags: Sequence[str] | None = None,
        description: str | None = None,
        owners: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        """Upsert the unified-catalog asset annotations.

        Fetches the current asset, unions any new ``tags`` with the existing ones,
        overrides ``description`` when supplied, records ``owners`` in metadata when
        given, then POSTs the merged asset to ``/api/contracts/assets/register``.
        Returns the backend response.
        """

        current = self.get_dataset(dataset_id)

        merged_tags = list(current.tags)
        if tags:
            for tag in tags:
                if tag and tag not in merged_tags:
                    merged_tags.append(tag)

        description_value = (
            description if description is not None else current.description
        )

        metadata = dict(current.metadata)
        metadata["description"] = description_value
        metadata["tags"] = merged_tags
        if owners:
            metadata["owners"] = list(owners)

        source_id = str(
            current.metadata.get("source_id", "")
            or f"{current.asset_type}:{current.name}"
        )
        asset = {
            "asset_type": current.asset_type,
            "name": current.name,
            "namespace": current.namespace,
            "source_system": current.source_system,
            "source_id": source_id,
            "description": description_value,
            "tags": merged_tags,
            "metadata": metadata,
        }
        payload = {
            "project_name": os.getenv("SEEKNAL_PROJECT_NAME") or current.namespace,
            "environment": os.getenv("ATLAS_ENVIRONMENT", "dev"),
            "asset": asset,
        }
        return self._post("/api/contracts/assets/register", payload)


def create_catalog_client_from_env() -> "AtlasCatalogClient | None":
    """Build a catalog client only when Atlas is configured.

    Returns ``None`` when ``ATLAS_API_URL`` is unset, signalling catalog features are
    inactive. Otherwise mirrors
    :func:`~seeknal.integrations.atlas_governance.create_governance_gate_from_env`:
    an explicit ``ATLAS_API_TOKEN`` wins, else the logged-in user's token is used so
    per-user identity flows to Atlas.
    """

    cfg = atlas_config()
    base_url = os.getenv("ATLAS_API_URL", "").strip() or cfg.api_url
    if not base_url:
        return None

    timeout = float(os.getenv("ATLAS_API_TIMEOUT_SECONDS", "10"))
    token = os.getenv("ATLAS_API_TOKEN") or user_token_from_credentials()
    config = AtlasContractConfig(
        base_url=base_url.rstrip("/"),
        token=token,
        timeout_seconds=timeout,
    )
    # When the portal is configured, ``list_datasets`` lists from its ``/api/datasets``
    # aggregator so the CLI catalog matches the web (Lakekeeper tables + cube products).
    portal_url = (os.getenv("ATLAS_PORTAL_URL", "").strip() or cfg.portal_url) or None
    return AtlasCatalogClient(config, portal_url=portal_url)
