"""Middleware tests: role-gated wrapping (AC15)."""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import patch

import pytest

from seeknal.ask.access.middleware import (
    AccessError,
    InsufficientRoleError,
    MissingToolRoleError,
    make_role_gated_tool,
)
from seeknal.ask.access.registry import register_tool_role, tool_min_role
from seeknal.ask.access.roles import Role


class _FakeContext:
    def __init__(self, role=None, user_id=None):
        self.requestor_role = role
        if user_id is not None:
            self.telegram_user_id = user_id


@pytest.fixture
def gated_sync_tool():
    name = "_test_gated_sync"
    register_tool_role(name, Role.OPERATOR, overwrite=True)

    def _test_gated_sync(arg: int = 0) -> int:
        return arg * 2

    wrapped = make_role_gated_tool(_test_gated_sync)
    yield wrapped
    # Cleanup
    tool_min_role.pop(name, None)


@pytest.fixture
def gated_async_tool():
    name = "_test_gated_async"
    register_tool_role(name, Role.OPERATOR, overwrite=True)

    async def _test_gated_async(arg: int = 0) -> int:
        return arg + 1

    wrapped = make_role_gated_tool(_test_gated_async)
    yield wrapped
    tool_min_role.pop(name, None)


def test_unregistered_tool_fail_closed_at_wrap_time():
    def _never_registered():
        return 1

    with pytest.raises(MissingToolRoleError):
        make_role_gated_tool(_never_registered)


def test_role_gating_blocks_insufficient_role(gated_sync_tool):
    ctx = _FakeContext(role=Role.VIEWER)
    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context", return_value=ctx
    ):
        with pytest.raises(InsufficientRoleError) as exc_info:
            gated_sync_tool(arg=5)
    assert exc_info.value.required == Role.OPERATOR
    assert exc_info.value.actual == Role.VIEWER


def test_role_gating_passes_sufficient_role(gated_sync_tool):
    ctx = _FakeContext(role=Role.ADMIN)
    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context", return_value=ctx
    ):
        result = gated_sync_tool(arg=5)
    assert result == 10


def test_role_gating_blocks_no_role(gated_sync_tool):
    ctx = _FakeContext(role=None)
    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context", return_value=ctx
    ):
        with pytest.raises(InsufficientRoleError):
            gated_sync_tool(arg=1)


def test_async_role_gating(gated_async_tool):
    ctx = _FakeContext(role=Role.OPERATOR)
    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context", return_value=ctx
    ):
        result = asyncio.run(gated_async_tool(arg=10))
    assert result == 11


def test_access_error_not_runtime_error_subclass():
    assert not issubclass(AccessError, RuntimeError)


def test_log_record_shape(gated_sync_tool, caplog):
    """AC15 contract: log fields are exact."""
    ctx = _FakeContext(role=Role.VIEWER, user_id=12345)
    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context", return_value=ctx
    ):
        with caplog.at_level(logging.INFO, logger="seeknal.access"):
            with pytest.raises(InsufficientRoleError):
                gated_sync_tool(arg=1)
    records = [r for r in caplog.records if r.name == "seeknal.access"]
    assert records, "Expected at least one seeknal.access log record"
    rec = records[0]
    assert rec.event == "role_denied"
    assert rec.user_id == 12345
    assert rec.tool == "_test_gated_sync"
    assert rec.required == "operator"
    assert rec.actual == "viewer"


def test_resolve_role_string_input(gated_sync_tool):
    ctx = _FakeContext(role="admin")
    with patch(
        "seeknal.ask.agents.tools._context.get_tool_context", return_value=ctx
    ):
        # Should succeed thanks to _resolve_role's string handling
        assert gated_sync_tool(arg=2) == 4


def test_signature_preserved(gated_sync_tool):
    """Decorator preserves docstring and name (via @wraps)."""
    assert gated_sync_tool.__name__ == "_test_gated_sync"
