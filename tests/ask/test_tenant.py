"""Unit tests for the tenant resolution module."""

from __future__ import annotations

import pytest

from seeknal.ask.gateway.tenant import (
    DEFAULT_TENANT,
    DEFAULT_TASK_QUEUE,
    InvalidTenantError,
    make_workflow_id,
    resolve_tenant,
    resolve_tenant_ws,
    scoped_key,
    task_queue_for_tenant,
    validate_tenant_id,
)


class TestValidateTenantId:
    def test_accepts_alphanumeric(self):
        assert validate_tenant_id("acme") == "acme"

    def test_accepts_hyphens_underscores(self):
        assert validate_tenant_id("acme_corp-2") == "acme_corp-2"

    def test_accepts_max_length(self):
        tid = "a" * 64
        assert validate_tenant_id(tid) == tid

    def test_rejects_empty(self):
        with pytest.raises(InvalidTenantError):
            validate_tenant_id("")

    def test_rejects_too_long(self):
        with pytest.raises(InvalidTenantError):
            validate_tenant_id("a" * 65)

    def test_rejects_slash(self):
        with pytest.raises(InvalidTenantError):
            validate_tenant_id("acme/globex")

    def test_rejects_spaces(self):
        with pytest.raises(InvalidTenantError):
            validate_tenant_id("acme corp")

    def test_rejects_special_chars(self):
        with pytest.raises(InvalidTenantError):
            validate_tenant_id("acme.corp")


class TestTaskQueueForTenant:
    def test_default_tenant_uses_legacy_queue(self):
        assert task_queue_for_tenant("default") == "seeknal-ask"

    def test_default_constant(self):
        assert task_queue_for_tenant(DEFAULT_TENANT) == DEFAULT_TASK_QUEUE

    def test_custom_tenant_gets_prefixed_queue(self):
        assert task_queue_for_tenant("acme") == "seeknal-ask-acme"

    def test_tenant_with_dash(self):
        assert task_queue_for_tenant("acme-corp") == "seeknal-ask-acme-corp"


class TestMakeWorkflowId:
    def test_default_tenant_omits_prefix(self):
        wf_id = make_workflow_id("default", "sess123")
        assert wf_id.startswith("ask-sess123-")
        assert "default" not in wf_id

    def test_custom_tenant_includes_prefix(self):
        wf_id = make_workflow_id("acme", "sess123")
        assert wf_id.startswith("ask-acme-sess123-")

    def test_workflow_id_has_timestamp_suffix(self):
        wf_id = make_workflow_id("acme", "sess123")
        # Format: ask-acme-sess123-{unix_timestamp}
        parts = wf_id.split("-")
        ts = parts[-1]
        assert ts.isdigit()
        assert int(ts) > 1_000_000_000


class TestScopedKey:
    def test_composite_format(self):
        assert scoped_key("acme", "sess123") == "acme:sess123"

    def test_default_tenant(self):
        assert scoped_key("default", "abc") == "default:abc"


class _FakeRequest:
    """Minimal Request stub for resolve_tenant tests."""

    def __init__(self, headers=None, query_params=None):
        self.headers = headers or {}
        self.query_params = query_params or {}


class TestResolveTenant:
    def test_header_wins(self):
        req = _FakeRequest(headers={"X-Tenant-ID": "acme"})
        assert resolve_tenant(req) == "acme"

    def test_query_param_fallback(self):
        req = _FakeRequest(query_params={"tenant": "globex"})
        assert resolve_tenant(req) == "globex"

    def test_header_beats_query_param(self):
        req = _FakeRequest(
            headers={"X-Tenant-ID": "acme"},
            query_params={"tenant": "globex"},
        )
        assert resolve_tenant(req) == "acme"

    def test_default_when_neither_set(self):
        req = _FakeRequest()
        assert resolve_tenant(req) == DEFAULT_TENANT

    def test_rejects_invalid_header(self):
        req = _FakeRequest(headers={"X-Tenant-ID": "acme/../evil"})
        with pytest.raises(InvalidTenantError):
            resolve_tenant(req)

    def test_rejects_invalid_query_param(self):
        req = _FakeRequest(query_params={"tenant": "a" * 100})
        with pytest.raises(InvalidTenantError):
            resolve_tenant(req)


class TestResolveTenantWs:
    def test_query_param_fallback(self):
        ws = _FakeRequest(query_params={"tenant": "acme"})
        assert resolve_tenant_ws(ws) == "acme"

    def test_default_when_neither_set(self):
        ws = _FakeRequest()
        assert resolve_tenant_ws(ws) == DEFAULT_TENANT
