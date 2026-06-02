"""Tests for the runtime Atlas governance gate.

These exercise the gate in isolation using an injected ``httpx.MockTransport`` so no
real Atlas backend is required. The behaviours under test are the ones that make the
gate trustworthy: it is inactive unless configured, it delegates decisions to Atlas,
it is fail-closed by default, and audit logging can never break a read.
"""

from __future__ import annotations

from typing import Callable

import httpx
import pytest

from seeknal.integrations.atlas_client import AtlasContractConfig, AtlasPolicyDenied
from seeknal.integrations.atlas_governance import (
    MASK_TOKEN,
    AccessDecision,
    GovernanceGate,
    apply_column_masks,
    create_governance_gate_from_env,
    govern_query,
    govern_read,
    mask_rows,
    mask_value,
    referenced_tables,
)

BASE_URL = "http://atlas.test"


def _gate(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    fail_open: bool = False,
) -> GovernanceGate:
    """Build a gate whose HTTP calls are served by ``handler`` via MockTransport."""

    client = httpx.Client(transport=httpx.MockTransport(handler))
    config = AtlasContractConfig(base_url=BASE_URL, token="t0ken", timeout_seconds=5.0)
    return GovernanceGate(config, fail_open=fail_open, client=client)


# ---------------------------------------------------------------------------
# Activation
# ---------------------------------------------------------------------------


def test_gate_inactive_without_atlas_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ATLAS_API_URL", raising=False)
    assert create_governance_gate_from_env() is None


def test_gate_active_when_atlas_url_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ATLAS_API_URL", "https://atlas.example.com/")
    monkeypatch.setenv("ATLAS_API_TOKEN", "abc")
    gate = create_governance_gate_from_env()
    assert isinstance(gate, GovernanceGate)
    # trailing slash trimmed
    assert gate.config.base_url == "https://atlas.example.com"
    assert gate.config.token == "abc"


# ---------------------------------------------------------------------------
# check_access / enforce_access
# ---------------------------------------------------------------------------


def test_check_access_allowed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/contracts/access-check"
        assert request.headers["Authorization"] == "Bearer t0ken"
        return httpx.Response(200, json={"allowed": True, "classification": "internal"})

    decision = _gate(handler).check_access(resource="prod.gold.sales", actor="alice")
    assert decision == AccessDecision(
        allowed=True, reason="", masked_columns=(), classification="internal"
    )
    assert decision.has_masking is False


def test_check_access_denied_with_reason() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"allowed": False, "reason": "no grant"})

    decision = _gate(handler).check_access(resource="prod.gold.finance", actor="bob")
    assert decision.allowed is False
    assert decision.reason == "no grant"


def test_check_access_returns_masked_columns() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "allowed": True,
                "masked_columns": ["nik", "npwp"],
                "classification": "restricted",
            },
        )

    decision = _gate(handler).check_access(resource="prod.gold.customer", actor="carol")
    assert decision.allowed is True
    assert decision.masked_columns == ("nik", "npwp")
    assert decision.classification == "restricted"
    assert decision.has_masking is True


def test_enforce_access_returns_decision_when_allowed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"allowed": True, "masked_columns": ["nik"]})

    decision = _gate(handler).enforce_access(resource="prod.gold.customer", actor="carol")
    assert decision.allowed is True
    assert decision.masked_columns == ("nik",)


def test_enforce_access_raises_when_denied() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/contracts/access-check":
            return httpx.Response(200, json={"allowed": False, "reason": "denied by policy"})
        return httpx.Response(200, json={})  # audit endpoint

    with pytest.raises(AtlasPolicyDenied, match="denied by policy"):
        _gate(handler).enforce_access(resource="prod.gold.finance", actor="bob")


# ---------------------------------------------------------------------------
# Fail-closed / fail-open on transport errors
# ---------------------------------------------------------------------------


def test_fail_closed_on_transport_error_by_default() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("atlas down", request=request)

    decision = _gate(handler).check_access(resource="prod.gold.sales")
    assert decision.allowed is False
    assert "fail-closed" in decision.reason


def test_enforce_access_raises_when_atlas_unreachable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("atlas down", request=request)

    with pytest.raises(AtlasPolicyDenied):
        _gate(handler).enforce_access(resource="prod.gold.sales")


def test_fail_open_when_explicitly_enabled() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("atlas down", request=request)

    decision = _gate(handler, fail_open=True).check_access(resource="prod.gold.sales")
    assert decision.allowed is True
    assert "fail-open" in decision.reason


def test_server_5xx_is_fail_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="unavailable")

    decision = _gate(handler).check_access(resource="prod.gold.sales")
    assert decision.allowed is False


# ---------------------------------------------------------------------------
# Audit never raises
# ---------------------------------------------------------------------------


def test_audit_never_raises_on_server_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    # Must not raise.
    assert _gate(handler).audit(action="read", resource="prod.gold.sales", status="success") is None


def test_audit_never_raises_on_connect_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("atlas down", request=request)

    assert _gate(handler).audit(action="read", resource="prod.gold.sales") is None


# ---------------------------------------------------------------------------
# Pure masking helpers
# ---------------------------------------------------------------------------


def test_mask_value_variants() -> None:
    assert mask_value(None) is None
    assert mask_value("3201011234567") == MASK_TOKEN
    assert mask_value("3201011234567", keep=6) == f"320101{MASK_TOKEN}"
    # value shorter than keep is returned unchanged
    assert mask_value("ab", keep=6) == "ab"
    assert mask_value(12345, keep=0) == MASK_TOKEN


def test_apply_column_masks_redacts_only_listed_columns() -> None:
    rows = [
        {"customer_id": "C-001", "nik": "3201011234567", "arpu": 152000},
        {"customer_id": "C-002", "nik": "3273010000000", "arpu": 98000},
    ]
    masked = apply_column_masks(rows, ["nik"], keep=6)
    assert masked == [
        {"customer_id": "C-001", "nik": f"320101{MASK_TOKEN}", "arpu": 152000},
        {"customer_id": "C-002", "nik": f"327301{MASK_TOKEN}", "arpu": 98000},
    ]
    # original rows untouched
    assert rows[0]["nik"] == "3201011234567"


def test_apply_column_masks_no_columns_is_passthrough_copy() -> None:
    rows = [{"a": 1, "b": 2}]
    out = apply_column_masks(rows, [])
    assert out == rows
    assert out is not rows
    assert out[0] is not rows[0]


def test_apply_column_masks_ignores_absent_columns() -> None:
    rows = [{"a": 1}]
    out = apply_column_masks(rows, ["nik"], keep=4)
    assert out == [{"a": 1}]


# ---------------------------------------------------------------------------
# Positional masking (execute_oneshot shape) + govern_read
# ---------------------------------------------------------------------------


def test_mask_rows_masks_named_columns() -> None:
    columns = ["customer_id", "nik", "arpu"]
    rows = [("C-001", "3201011234567", 152000), ("C-002", "3273010000000", 98000)]
    out = mask_rows(columns, rows, ["nik"], keep=6)
    assert out == [
        ("C-001", f"320101{MASK_TOKEN}", 152000),
        ("C-002", f"327301{MASK_TOKEN}", 98000),
    ]
    # input not mutated
    assert rows[0] == ("C-001", "3201011234567", 152000)


def test_mask_rows_ignores_absent_and_empty() -> None:
    columns = ["a", "b"]
    rows = [(1, 2)]
    assert mask_rows(columns, rows, []) == [(1, 2)]
    assert mask_rows(columns, rows, ["nik"]) == [(1, 2)]


def test_mask_rows_preserves_none() -> None:
    assert mask_rows(["nik"], [(None,)], ["nik"], keep=4) == [(None,)]


def test_govern_read_passthrough_when_gate_none() -> None:
    rows = [("3201011234567",)]
    out = govern_read(None, resource="prod.t", columns=["nik"], rows=rows)
    assert out == [("3201011234567",)]


def test_govern_read_masks_when_allowed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"allowed": True, "masked_columns": ["nik"]})

    out = govern_read(
        _gate(handler),
        resource="prod.gold.customer",
        columns=["customer_id", "nik"],
        rows=[("C-001", "3201011234567")],
        keep=6,
    )
    assert out == [("C-001", f"320101{MASK_TOKEN}")]


def test_govern_read_raises_when_denied() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/contracts/access-check":
            return httpx.Response(200, json={"allowed": False, "reason": "no grant"})
        return httpx.Response(200, json={})

    with pytest.raises(AtlasPolicyDenied):
        govern_read(
            _gate(handler),
            resource="prod.gold.finance",
            columns=["x"],
            rows=[("v",)],
        )


# ---------------------------------------------------------------------------
# referenced_tables + govern_query (ad-hoc SQL reads)
# ---------------------------------------------------------------------------


def test_referenced_tables_from_and_join() -> None:
    sql = "SELECT a.id FROM prod.gold.customer a JOIN prod.gold.orders b ON a.id = b.cid"
    assert referenced_tables(sql) == ["prod.gold.customer", "prod.gold.orders"]


def test_referenced_tables_excludes_ctes() -> None:
    sql = "WITH recent AS (SELECT * FROM prod.gold.orders) SELECT * FROM recent"
    assert referenced_tables(sql) == ["prod.gold.orders"]


def test_referenced_tables_tableless_is_empty() -> None:
    assert referenced_tables("SELECT 1 AS one") == []


def test_govern_query_passthrough_when_gate_none() -> None:
    rows = [("C-001", "3201011234567")]
    out = govern_query(
        None,
        sql="SELECT * FROM prod.gold.customer",
        columns=["customer_id", "nik"],
        rows=rows,
    )
    assert out == [("C-001", "3201011234567")]


def test_govern_query_masks_referenced_table_columns() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"allowed": True, "masked_columns": ["nik"]})

    out = govern_query(
        _gate(handler),
        sql="SELECT customer_id, nik FROM prod.gold.customer",
        columns=["customer_id", "nik"],
        rows=[("C-001", "3201011234567")],
        keep=6,
    )
    assert out == [("C-001", f"320101{MASK_TOKEN}")]


def test_govern_query_raises_when_a_table_denied() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/contracts/access-check":
            return httpx.Response(200, json={"allowed": False, "reason": "no grant"})
        return httpx.Response(200, json={})

    with pytest.raises(AtlasPolicyDenied):
        govern_query(
            _gate(handler),
            sql="SELECT * FROM prod.gold.finance",
            columns=["x"],
            rows=[("v",)],
        )


def test_govern_query_tableless_does_not_call_atlas() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("Atlas must not be called for a tableless query")

    out = govern_query(
        _gate(handler),
        sql="SELECT 1 AS one",
        columns=["one"],
        rows=[(1,)],
    )
    assert out == [(1,)]
