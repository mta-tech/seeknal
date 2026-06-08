"""Tests for the Atlas data-catalog client (AtlasCatalogClient)."""

from __future__ import annotations

import json

import httpx

from seeknal.integrations.atlas_catalog import (
    AtlasCatalogClient,
    Dataset,
    create_catalog_client_from_env,
)
from seeknal.integrations.atlas_client import AtlasAuthError, AtlasContractConfig

ASSET = {
    "id": "abc-123",
    "asset_type": "source",
    "name": "sales",
    "namespace": "retail_demo",
    "source_system": "lakekeeper",
    "source_id": "source:sales",
    "metadata": {
        "description": "Retail sales",
        "tags": ["gold"],
        "canonical_asset_id": "atlas.retail_demo.sales",
    },
}


def _client(handler) -> AtlasCatalogClient:
    transport = httpx.MockTransport(handler)
    return AtlasCatalogClient(
        AtlasContractConfig(base_url="http://atlas", token="tok"),
        client=httpx.Client(transport=transport),
    )


def test_list_datasets_maps_assets_and_params():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["url"] = str(request.url)
        assert request.headers["Authorization"] == "Bearer tok"
        return httpx.Response(200, json=[ASSET])

    datasets = _client(handler).list_datasets(query="sales", asset_type="source", limit=5)
    assert seen["path"] == "/api/assets"
    assert "q=sales" in seen["url"] and "type=source" in seen["url"] and "limit=5" in seen["url"]
    assert len(datasets) == 1
    d = datasets[0]
    assert d.name == "sales" and d.namespace == "retail_demo"
    assert d.tags == ("gold",) and d.description == "Retail sales"
    assert d.fqn == "atlas.retail_demo.sales"


def test_get_dataset_hits_id_path():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/assets/abc-123"
        return httpx.Response(200, json=ASSET)

    assert _client(handler).get_dataset("abc-123").name == "sales"


def test_search_handles_dict_and_list_shapes():
    assert len(_client(lambda r: httpx.Response(200, json={"results": [ASSET]})).search("x")) == 1
    assert len(_client(lambda r: httpx.Response(200, json={"items": [ASSET]})).search("x")) == 1
    assert len(_client(lambda r: httpx.Response(200, json=[ASSET])).search("x")) == 1


def test_sample_requires_identifier_param():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/sample"
        assert request.url.params.get("identifier") == "retail_demo.sales"
        assert request.url.params.get("limit") == "3"
        return httpx.Response(200, json={"columns": ["a"], "rows": [[1]]})

    out = _client(handler).sample("retail_demo.sales", limit=3)
    assert out["rows"] == [[1]]


def test_annotate_merges_tags_then_registers():
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path == "/api/assets/abc-123":
            return httpx.Response(200, json=ASSET)
        if request.url.path == "/api/contracts/assets/register":
            body = json.loads(request.content)
            asset = body["asset"]
            assert "pii" in asset["tags"] and "gold" in asset["tags"]  # union with existing
            assert asset["description"] == "new desc"
            return httpx.Response(200, json={"asset": asset})
        return httpx.Response(404)

    _client(handler).annotate("abc-123", tags=["pii"], description="new desc")
    assert calls == ["/api/assets/abc-123", "/api/contracts/assets/register"]


def test_dataset_from_api_is_tolerant_of_missing_fields():
    d = Dataset.from_api({"id": "1", "name": "n"})
    assert d.namespace == "" and d.tags == () and d.description == ""
    assert d.fqn == "n"


def test_factory_returns_none_without_atlas_api_url(monkeypatch):
    monkeypatch.delenv("ATLAS_API_URL", raising=False)
    assert create_catalog_client_from_env() is None


def test_factory_builds_client_with_env(monkeypatch):
    monkeypatch.setenv("ATLAS_API_URL", "http://atlas:8000/")
    monkeypatch.setenv("ATLAS_API_TOKEN", "svc")
    client = create_catalog_client_from_env()
    assert client is not None
    assert client.config.base_url == "http://atlas:8000" and client.config.token == "svc"


def test_refreshes_token_on_401_and_retries():
    """A 401 triggers a single refresh + retry with the new bearer, transparently."""

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            assert request.headers["Authorization"] == "Bearer tok"
            return httpx.Response(401, json={"detail": "Token has expired"})
        assert request.headers["Authorization"] == "Bearer refreshed"
        return httpx.Response(200, json=[ASSET])

    client = AtlasCatalogClient(
        AtlasContractConfig(base_url="http://atlas", token="tok"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        token_refresher=lambda: "refreshed",
    )
    datasets = client.list_datasets(query="sales")
    assert calls["n"] == 2
    assert len(datasets) == 1 and datasets[0].name == "sales"


def test_raises_auth_error_when_refresh_unavailable():
    """When the session cannot be refreshed, raise AtlasAuthError with a login hint."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"detail": "Token has expired"})

    client = AtlasCatalogClient(
        AtlasContractConfig(base_url="http://atlas", token="tok"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        token_refresher=lambda: None,
    )
    try:
        client.list_datasets(query="sales")
    except AtlasAuthError as exc:
        assert "seeknal auth login" in str(exc)
    else:  # pragma: no cover - explicit failure if no error raised
        raise AssertionError("expected AtlasAuthError")


PORTAL_ICEBERG = {
    "urn": "urn:li:dataset:(urn:li:dataPlatform:iceberg,retail_demo.sales,PROD)",
    "name": "retail_demo.sales",
    "displayName": "sales",
    "namespace": "retail_demo",
    "platform": "iceberg",
    "type": "table",
    "tags": ["Restricted"],
}
PORTAL_CUBE = {
    "urn": "urn:li:dataset:(urn:li:dataPlatform:cube,cube.public.Orders,PROD)",
    "name": "cube.public.Orders",
    "displayName": "Orders",
    "namespace": "public",
    "platform": "cube",
    "type": "cube",
    "tags": [],
}


def _portal_client(handler) -> AtlasCatalogClient:
    transport = httpx.MockTransport(handler)
    return AtlasCatalogClient(
        AtlasContractConfig(base_url="http://atlas", token="tok"),
        client=httpx.Client(transport=transport),
        portal_url="http://portal",
    )


def test_dataset_from_portal_iceberg_recomposes_fqn_without_doubling():
    d = Dataset.from_portal(PORTAL_ICEBERG)
    assert d.name == "sales" and d.namespace == "retail_demo"
    assert d.asset_type == "table" and d.source_system == "iceberg"
    assert d.tags == ("Restricted",)
    assert d.fqn == "retail_demo.sales"  # not retail_demo.retail_demo.sales


def test_dataset_from_portal_cube_shape():
    d = Dataset.from_portal(PORTAL_CUBE)
    assert d.name == "Orders" and d.namespace == "public"
    assert d.asset_type == "cube" and d.source_system == "cube"
    assert d.fqn == "public.Orders"


def test_dataset_from_portal_strips_namespace_when_no_display_name():
    d = Dataset.from_portal(
        {"name": "retail_demo.sales", "namespace": "retail_demo", "platform": "iceberg", "type": "table"}
    )
    assert d.name == "sales" and d.fqn == "retail_demo.sales"


def test_list_from_portal_targets_portal_and_maps_rows():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["host"] = request.url.host
        seen["path"] = request.url.path
        seen["url"] = str(request.url)
        assert request.headers["Authorization"] == "Bearer tok"
        return httpx.Response(200, json={"datasets": [PORTAL_ICEBERG, PORTAL_CUBE], "total": 2})

    datasets = _portal_client(handler).list_datasets(query="sa", namespace="retail_demo", limit=5)
    assert seen["host"] == "portal" and seen["path"] == "/api/datasets"
    assert "q=sa" in seen["url"] and "namespace=retail_demo" in seen["url"] and "limit=5" in seen["url"]
    assert [d.fqn for d in datasets] == ["retail_demo.sales", "public.Orders"]


def test_list_from_portal_forwards_platform_and_filters_type_client_side():
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200, json={"datasets": [PORTAL_ICEBERG, PORTAL_CUBE]})

    # source_system -> portal ?platform=; asset_type filtered client-side.
    datasets = _portal_client(handler).list_datasets(source_system="cube", asset_type="cube")
    assert "platform=cube" in seen["url"]
    assert [d.fqn for d in datasets] == ["public.Orders"]


def test_list_from_portal_tolerates_bare_array():
    datasets = _portal_client(
        lambda r: httpx.Response(200, json=[PORTAL_ICEBERG])
    ).list_datasets()
    assert [d.fqn for d in datasets] == ["retail_demo.sales"]


def test_list_uses_assets_registry_when_portal_unset():
    """Without ATLAS_PORTAL_URL the registry path (/api/assets) is used (backward compat)."""

    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        return httpx.Response(200, json=[ASSET])

    datasets = _client(handler).list_datasets()  # _client has no portal_url
    assert seen["path"] == "/api/assets"
    assert datasets[0].fqn == "atlas.retail_demo.sales"


def test_factory_reads_portal_url(monkeypatch):
    monkeypatch.setenv("ATLAS_API_URL", "http://atlas:8000")
    monkeypatch.setenv("ATLAS_PORTAL_URL", "http://portal:4200/")
    client = create_catalog_client_from_env()
    assert client is not None and client._portal_url == "http://portal:4200"


def test_post_path_also_refreshes_on_401():
    """The shared sender covers POST (annotate) too, not just GET."""

    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if request.url.path == "/api/assets/abc-123":
            return httpx.Response(200, json=ASSET)  # get_dataset inside annotate()
        # /api/contracts/assets/register (the POST)
        if calls["n"] <= 2:
            return httpx.Response(401, json={"detail": "expired"})
        return httpx.Response(200, json={"ok": True})

    client = AtlasCatalogClient(
        AtlasContractConfig(base_url="http://atlas", token="tok"),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        token_refresher=lambda: "refreshed",
    )
    result = client.annotate("abc-123", tags=["pii"])
    assert result == {"ok": True}
