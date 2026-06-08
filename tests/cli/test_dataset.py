"""Tests for the `seeknal dataset` CLI command group."""

from __future__ import annotations

from unittest.mock import MagicMock

from typer.testing import CliRunner

import seeknal.cli.dataset as dataset_mod
from seeknal.cli.dataset import _extract_sample, dataset_app
from seeknal.integrations.atlas_catalog import Dataset
from seeknal.integrations.atlas_client import (
    AtlasAuthError,
    AtlasPolicyDenied,
    SESSION_EXPIRED_HINT,
)
from seeknal.integrations.atlas_governance import AccessDecision

runner = CliRunner()

DS = Dataset(
    id="abc-123",
    name="sales",
    namespace="retail_demo",
    asset_type="source",
    source_system="lakekeeper",
    description="Retail sales",
    tags=("gold",),
    canonical_id="atlas.retail_demo.sales",
    columns=(("sale_id", "long"), ("region", "string")),
)


def _fake_client() -> MagicMock:
    client = MagicMock()
    client.list_datasets.return_value = [DS]
    client.get_dataset.return_value = DS
    client.sample.return_value = {"columns": ["sale_id", "region"], "rows": [[1, "us-east"]]}
    client.annotate.return_value = {"ok": True}
    client.lineage.return_value = {
        "upstreamLineage": [{"name": "raw.sales", "type": "DATASET"}],
        "downstreamLineage": [{"name": "gold.sales_daily", "type": "DATASET"}],
    }
    return client


def _patch(monkeypatch, *, client, gate="allow"):
    monkeypatch.setattr(dataset_mod, "create_catalog_client_from_env", lambda: client)
    if gate == "allow":
        gate = MagicMock()
        gate.check_access.return_value = AccessDecision(allowed=True)
        gate.enforce_access.return_value = AccessDecision(allowed=True)
    monkeypatch.setattr(dataset_mod, "create_governance_gate_from_env", lambda: gate)
    return gate


def test_not_configured_exits_2(monkeypatch):
    _patch(monkeypatch, client=None)
    result = runner.invoke(dataset_app, ["list"])
    assert result.exit_code == 2
    assert "not configured" in result.output.lower()


def test_list_renders_datasets(monkeypatch):
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["list"])
    assert result.exit_code == 0
    assert "sales" in result.output and "retail_demo" in result.output


def test_list_json(monkeypatch):
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["list", "--json"])
    assert result.exit_code == 0
    assert '"name": "sales"' in result.output


def test_list_hints_to_set_portal_url_when_unset(monkeypatch):
    monkeypatch.delenv("ATLAS_PORTAL_URL", raising=False)
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["list"])
    assert result.exit_code == 0
    assert "ATLAS_PORTAL_URL" in result.output


def test_list_no_hint_when_portal_set(monkeypatch):
    monkeypatch.setenv("ATLAS_PORTAL_URL", "http://portal:4200")
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["list"])
    assert result.exit_code == 0
    assert "Set ATLAS_PORTAL_URL" not in result.output


def test_show_prints_metadata_and_access(monkeypatch):
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["show", "sales"])
    assert result.exit_code == 0
    assert "retail_demo" in result.output and "ALLOW" in result.output


def test_show_renders_columns(monkeypatch):
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["show", "sales"])
    assert result.exit_code == 0
    assert "columns" in result.output
    assert "sale_id" in result.output and "region" in result.output and "(long)" in result.output


def test_show_json_includes_columns(monkeypatch):
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["show", "sales", "--json"])
    assert result.exit_code == 0
    assert '"name": "sale_id"' in result.output and '"type": "long"' in result.output


def test_show_with_sample(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["show", "sales", "--sample"])
    assert result.exit_code == 0
    assert "us-east" in result.output
    client.sample.assert_called_once()


def test_resolve_ambiguous_name_warns_and_picks_first(monkeypatch):
    client = _fake_client()
    d1 = Dataset(id="urn:1", name="customers", namespace="curated", asset_type="table")
    d2 = Dataset(id="urn:2", name="customers", namespace="analytics", asset_type="table")
    client.list_datasets.return_value = [d1, d2]
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["show", "customers"])
    assert result.exit_code == 0
    assert "matches 2 datasets" in result.output
    assert "curated.customers" in result.output and "analytics.customers" in result.output


def test_annotate_calls_client(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["annotate", "sales", "--tag", "pii", "--description", "d"])
    assert result.exit_code == 0
    client.annotate.assert_called_once()


def test_annotate_requires_a_flag(monkeypatch):
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["annotate", "sales"])
    assert result.exit_code == 1
    assert "at least one" in result.output.lower()


def test_query_denied_exits_1_with_hint(monkeypatch):
    gate = MagicMock()
    gate.enforce_access.side_effect = AtlasPolicyDenied("OpenFGA denies can_select")
    _patch(monkeypatch, client=_fake_client(), gate=gate)
    result = runner.invoke(dataset_app, ["query", "sales"])
    assert result.exit_code == 1
    assert "request-access" in result.output


def test_query_allowed_prints_rows(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["query", "sales"])
    assert result.exit_code == 0
    assert "us-east" in result.output


def test_query_accepts_direct_table_identifier(monkeypatch):
    client = _fake_client()
    client.list_datasets.return_value = []  # not a registered asset
    gate = _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["query", "retail_demo.sales"])
    assert result.exit_code == 0
    gate.enforce_access.assert_called_with(resource="retail_demo.sales", action="read")
    client.sample.assert_called_with("retail_demo.sales", limit=20)


def test_request_access_prints_ar_id(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)
    response = MagicMock()
    response.status_code = 200
    response.json.return_value = {"id": "AR-9", "status": "pending"}
    response.content = b"{}"
    monkeypatch.setattr(dataset_mod.httpx, "post", lambda *a, **k: response)
    result = runner.invoke(dataset_app, ["request-access", "sales", "--reason", "need it"])
    assert result.exit_code == 0
    assert "AR-9" in result.output


def test_lineage_renders_tree(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["lineage", "sales"])
    assert result.exit_code == 0
    assert "upstream" in result.output and "raw.sales" in result.output
    assert "downstream" in result.output and "gold.sales_daily" in result.output


def test_lineage_empty_is_friendly(monkeypatch):
    client = _fake_client()
    client.lineage.return_value = {"upstreamLineage": [], "downstreamLineage": []}
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["lineage", "sales"])
    assert result.exit_code == 0
    assert "No lineage recorded" in result.output


def test_lineage_json(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["lineage", "sales", "--json"])
    assert result.exit_code == 0
    assert '"name": "raw.sales"' in result.output and '"downstream"' in result.output


def test_use_prints_iceberg_source(monkeypatch):
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["use", "sales"])
    assert result.exit_code == 0
    assert "kind: source" in result.output
    assert "source: iceberg" in result.output
    assert "table: atlas.retail_demo.sales" in result.output


def test_use_rejects_cube(monkeypatch):
    client = _fake_client()
    cube = Dataset(
        id="urn:cube", name="Orders", namespace="public", asset_type="cube", source_system="cube"
    )
    client.list_datasets.return_value = [cube]
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["use", "Orders"])
    assert result.exit_code == 1
    assert "cube" in result.output.lower()


def test_use_write_creates_source_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["use", "sales", "--write"])
    assert result.exit_code == 0
    dest = tmp_path / "seeknal" / "sources" / "sales.yml"
    assert dest.exists()
    content = dest.read_text()
    assert "source: iceberg" in content and "table: atlas.retail_demo.sales" in content


def test_extract_sample_unwraps_backend_values_envelope():
    # The Atlas /api/sample backend returns columns as schema dicts and rows
    # wrapped as {"values": {col: val}} -- the shape that previously rendered blank.
    data = {
        "columns": [{"name": "sale_id"}, {"name": "customer_name"}, {"name": "region"}],
        "rows": [
            {"values": {"sale_id": 1, "customer_name": "Alice", "region": "us-east"}},
            {"values": {"sale_id": 2, "customer_name": "Bob", "region": "us-west"}},
        ],
    }
    columns, rows = _extract_sample(data)
    assert columns == ["sale_id", "customer_name", "region"]
    assert rows == [[1, "Alice", "us-east"], [2, "Bob", "us-west"]]


def test_query_renders_backend_values_envelope(monkeypatch):
    client = _fake_client()
    client.sample.return_value = {
        "columns": [{"name": "sale_id"}, {"name": "region"}],
        "rows": [{"values": {"sale_id": 1, "region": "us-east"}}],
    }
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["query", "retail_demo.sales"])
    assert result.exit_code == 0
    assert "us-east" in result.output
    assert "1 row(s)." in result.output


def test_show_presents_expired_session_without_traceback(monkeypatch):
    client = _fake_client()
    client.list_datasets.side_effect = AtlasAuthError(SESSION_EXPIRED_HINT)
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["show", "sales"])
    assert result.exit_code == 1
    assert "seeknal auth login" in result.output
    assert "Traceback" not in result.output


def test_query_presents_expired_session_without_traceback(monkeypatch):
    client = _fake_client()
    client.list_datasets.side_effect = AtlasAuthError(SESSION_EXPIRED_HINT)
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["query", "retail_demo.sales"])
    assert result.exit_code == 1
    assert "seeknal auth login" in result.output
    assert "Traceback" not in result.output


def test_request_access_refreshes_token_on_401(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)

    calls = {"n": 0}
    ok = MagicMock(status_code=200)
    ok.json.return_value = {"id": "AR-12", "status": "pending"}
    ok.content = b"{}"
    expired = MagicMock(status_code=401)
    expired.text = "Token has expired"

    def fake_post(*a, **k):
        calls["n"] += 1
        return expired if calls["n"] == 1 else ok

    monkeypatch.setattr(dataset_mod.httpx, "post", fake_post)
    monkeypatch.setattr(dataset_mod, "refresh_access_token", lambda: "fresh-token")

    result = runner.invoke(dataset_app, ["request-access", "sales", "--reason", "x"])
    assert result.exit_code == 0
    assert calls["n"] == 2  # 401 -> refresh -> retry succeeds
    assert "AR-12" in result.output


def test_request_access_friendly_when_refresh_fails(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)
    expired = MagicMock(status_code=401)
    expired.text = "Token has expired"
    monkeypatch.setattr(dataset_mod.httpx, "post", lambda *a, **k: expired)
    monkeypatch.setattr(dataset_mod, "refresh_access_token", lambda: None)

    result = runner.invoke(dataset_app, ["request-access", "sales", "--reason", "x"])
    assert result.exit_code == 1
    assert "seeknal auth login" in result.output
    assert "Traceback" not in result.output
