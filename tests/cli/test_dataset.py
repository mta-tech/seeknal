"""Tests for the `seeknal dataset` CLI command group."""

from __future__ import annotations

from unittest.mock import MagicMock

from typer.testing import CliRunner

import seeknal.cli.dataset as dataset_mod
from seeknal.cli.dataset import dataset_app
from seeknal.integrations.atlas_catalog import Dataset
from seeknal.integrations.atlas_client import AtlasPolicyDenied
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
)


def _fake_client() -> MagicMock:
    client = MagicMock()
    client.list_datasets.return_value = [DS]
    client.get_dataset.return_value = DS
    client.sample.return_value = {"columns": ["sale_id", "region"], "rows": [[1, "us-east"]]}
    client.annotate.return_value = {"ok": True}
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


def test_show_prints_metadata_and_access(monkeypatch):
    _patch(monkeypatch, client=_fake_client())
    result = runner.invoke(dataset_app, ["show", "sales"])
    assert result.exit_code == 0
    assert "retail_demo" in result.output and "ALLOW" in result.output


def test_show_with_sample(monkeypatch):
    client = _fake_client()
    _patch(monkeypatch, client=client)
    result = runner.invoke(dataset_app, ["show", "sales", "--sample"])
    assert result.exit_code == 0
    assert "us-east" in result.output
    client.sample.assert_called_once()


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
