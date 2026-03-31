"""Tests for seeknal ask security hooks."""

import asyncio

import pytest

from seeknal.ask.agents.hooks import (
    _sql_security_handler,
    get_security_hooks,
)
from pydantic_deep.capabilities.hooks import HookEvent, HookInput, HookResult


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _make_input(sql: str) -> HookInput:
    """Create a HookInput for execute_sql with given SQL."""
    return HookInput(
        event=HookEvent.PRE_TOOL_USE.value,
        tool_name="execute_sql",
        tool_input={"sql": sql},
    )


# ---------------------------------------------------------------------------
# SQL security hook tests
# ---------------------------------------------------------------------------


class TestSqlSecurityHook:
    def test_allows_valid_select(self):
        result = asyncio.run(_sql_security_handler(_make_input("SELECT * FROM customers")))
        assert result.allow is True

    def test_allows_valid_with_select(self):
        result = asyncio.run(
            _sql_security_handler(_make_input("WITH cte AS (SELECT 1) SELECT * FROM cte"))
        )
        assert result.allow is True

    def test_blocks_drop_table(self):
        result = asyncio.run(_sql_security_handler(_make_input("DROP TABLE customers")))
        assert result.allow is False
        assert "SQL validation error" in result.reason

    def test_blocks_install(self):
        result = asyncio.run(_sql_security_handler(_make_input("INSTALL httpfs")))
        assert result.allow is False

    def test_blocks_dangerous_functions(self):
        result = asyncio.run(
            _sql_security_handler(_make_input("SELECT read_parquet('/etc/passwd')"))
        )
        assert result.allow is False

    def test_blocks_http_functions(self):
        result = asyncio.run(
            _sql_security_handler(_make_input("SELECT http_get('http://evil.com')"))
        )
        assert result.allow is False

    def test_handler_exception_returns_clean_result(self):
        """Even if validation throws unexpectedly, hook returns HookResult, never raises."""
        # Create a HookInput with missing sql key to trigger unexpected path
        hook_input = HookInput(
            event=HookEvent.PRE_TOOL_USE.value,
            tool_name="execute_sql",
            tool_input={},  # Missing 'sql' key
        )
        result = asyncio.run(_sql_security_handler(hook_input))
        # Should return a result (allow or deny), not raise
        assert isinstance(result, HookResult)


class TestGetSecurityHooks:
    def test_returns_list_of_hooks(self):
        hooks = get_security_hooks()
        assert len(hooks) == 1
        hook = hooks[0]
        assert hook.event == HookEvent.PRE_TOOL_USE
        assert hook.matcher == "execute_sql"
        assert hook.handler is not None
