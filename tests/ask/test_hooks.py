"""Tests for seeknal ask hooks (security + self-correction)."""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from seeknal.ask.agents.hooks import (
    _sql_security_handler,
    _sql_self_correction_handler,
    get_ask_hooks,
    get_security_hooks,
)
from pydantic_deep.capabilities.hooks import HookEvent, HookInput, HookResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pre_input(sql: str) -> HookInput:
    """Create a HookInput for execute_sql PRE_TOOL_USE."""
    return HookInput(
        event=HookEvent.PRE_TOOL_USE.value,
        tool_name="execute_sql",
        tool_input={"sql": sql},
    )


def _make_post_input(tool_result: str) -> HookInput:
    """Create a HookInput for execute_sql POST_TOOL_USE."""
    return HookInput(
        event=HookEvent.POST_TOOL_USE.value,
        tool_name="execute_sql",
        tool_input={"sql": "SELECT 1"},
        tool_result=tool_result,
    )


def _error_json(category: str, message: str, hint: str | None = None) -> str:
    """Build a structured error JSON string."""
    return json.dumps({
        "category": category,
        "retryable": category.startswith("retryable"),
        "message": message,
        "hint": hint,
    })


# ---------------------------------------------------------------------------
# PRE_TOOL_USE: SQL security hook tests (existing, unchanged)
# ---------------------------------------------------------------------------


class TestSqlSecurityHook:
    def test_allows_valid_select(self):
        result = asyncio.run(_sql_security_handler(_make_pre_input("SELECT * FROM customers")))
        assert result.allow is True

    def test_allows_valid_with_select(self):
        result = asyncio.run(
            _sql_security_handler(_make_pre_input("WITH cte AS (SELECT 1) SELECT * FROM cte"))
        )
        assert result.allow is True

    def test_blocks_drop_table(self):
        result = asyncio.run(_sql_security_handler(_make_pre_input("DROP TABLE customers")))
        assert result.allow is False
        assert "SQL validation error" in result.reason

    def test_blocks_install(self):
        result = asyncio.run(_sql_security_handler(_make_pre_input("INSTALL httpfs")))
        assert result.allow is False

    def test_blocks_dangerous_functions(self):
        result = asyncio.run(
            _sql_security_handler(_make_pre_input("SELECT read_parquet('/etc/passwd')"))
        )
        assert result.allow is False

    def test_blocks_http_functions(self):
        result = asyncio.run(
            _sql_security_handler(_make_pre_input("SELECT http_get('http://evil.com')"))
        )
        assert result.allow is False

    def test_handler_exception_returns_clean_result(self):
        """Even if validation throws unexpectedly, hook returns HookResult, never raises."""
        hook_input = HookInput(
            event=HookEvent.PRE_TOOL_USE.value,
            tool_name="execute_sql",
            tool_input={},  # Missing 'sql' key
        )
        result = asyncio.run(_sql_security_handler(hook_input))
        assert isinstance(result, HookResult)

    # -- `query` alias: execute_sql accepts `sql` OR `query`; the hook must validate
    #    whichever the tool will actually run (regression for the query-alias hard-block) --

    def test_allows_valid_select_via_query_alias(self):
        """A model that passes only `query=` (no `sql`) must still be allowed."""
        hook_input = HookInput(
            event=HookEvent.PRE_TOOL_USE.value,
            tool_name="execute_sql",
            tool_input={"query": "SELECT * FROM customers"},
        )
        result = asyncio.run(_sql_security_handler(hook_input))
        assert result.allow is True

    def test_allows_query_alias_when_sql_is_none(self):
        """Regression: sql=None + query=... must not hard-block (was NoneType.strip())."""
        hook_input = HookInput(
            event=HookEvent.PRE_TOOL_USE.value,
            tool_name="execute_sql",
            tool_input={"sql": None, "query": "SELECT 1"},
        )
        result = asyncio.run(_sql_security_handler(hook_input))
        assert result.allow is True

    def test_blocks_dangerous_function_via_query_alias(self):
        """The guard must still validate the aliased SQL, not wave it through."""
        hook_input = HookInput(
            event=HookEvent.PRE_TOOL_USE.value,
            tool_name="execute_sql",
            tool_input={"query": "SELECT read_parquet('/etc/passwd')"},
        )
        result = asyncio.run(_sql_security_handler(hook_input))
        assert result.allow is False


# ---------------------------------------------------------------------------
# POST_TOOL_USE: SQL self-correction hook tests
# ---------------------------------------------------------------------------


class TestSqlSelfCorrectionHook:
    def test_passes_through_successful_result(self):
        """Non-JSON result (success) is not modified."""
        hook_input = _make_post_input("| id | name |\n| --- | --- |\n| 1 | Alice |")
        result = asyncio.run(_sql_self_correction_handler(hook_input))
        assert result.modified_result is None

    def test_passes_through_non_json_result(self):
        """Arbitrary non-JSON text is not modified."""
        hook_input = _make_post_input("Query executed successfully but returned no results.")
        result = asyncio.run(_sql_self_correction_handler(hook_input))
        assert result.modified_result is None

    def test_passes_through_json_without_category(self):
        """JSON without 'category' key is not an error — pass through."""
        hook_input = _make_post_input(json.dumps({"data": [1, 2, 3]}))
        result = asyncio.run(_sql_self_correction_handler(hook_input))
        assert result.modified_result is None

    def test_passes_through_empty_result(self):
        """Empty tool_result is not modified."""
        hook_input = _make_post_input("")
        result = asyncio.run(_sql_self_correction_handler(hook_input))
        assert result.modified_result is None

    def test_passes_through_none_result(self):
        """None tool_result is not modified."""
        hook_input = HookInput(
            event=HookEvent.POST_TOOL_USE.value,
            tool_name="execute_sql",
            tool_input={"sql": "SELECT 1"},
            tool_result=None,
        )
        result = asyncio.run(_sql_self_correction_handler(hook_input))
        assert result.modified_result is None

    @patch("seeknal.ask.agents.tools._context.get_tool_context")
    def test_enriches_missing_ref_with_table_names(self, mock_get_ctx):
        """missing_ref error gets available table names appended to hint."""
        mock_ctx = MagicMock()
        mock_ctx.repl.execute_oneshot.return_value = (
            ["name"],
            [("customers",), ("orders",), ("products",)],
        )
        mock_get_ctx.return_value = mock_ctx

        error = _error_json(
            "retryable_missing_ref",
            "Catalog Error: Table 'foo' does not exist",
        )
        hook_input = _make_post_input(error)
        result = asyncio.run(_sql_self_correction_handler(hook_input))

        assert result.modified_result is not None
        data = json.loads(result.modified_result)
        assert "customers" in data["hint"]
        assert "orders" in data["hint"]
        assert "products" in data["hint"]
        assert "Available tables:" in data["hint"]

    @patch("seeknal.ask.agents.tools._context.get_tool_context")
    def test_enriches_missing_ref_preserves_existing_hint(self, mock_get_ctx):
        """Existing hint is preserved when appending table names."""
        mock_ctx = MagicMock()
        mock_ctx.repl.execute_oneshot.return_value = (["name"], [("t1",)])
        mock_get_ctx.return_value = mock_ctx

        error = _error_json(
            "retryable_missing_ref",
            "Catalog Error: Table 'x' does not exist",
            hint="Check spelling",
        )
        hook_input = _make_post_input(error)
        result = asyncio.run(_sql_self_correction_handler(hook_input))

        assert result.modified_result is not None
        data = json.loads(result.modified_result)
        assert data["hint"].startswith("Check spelling")
        assert "t1" in data["hint"]

    def test_enriches_syntax_error_with_interval_hint(self):
        """Syntax error mentioning INTERVAL gets timestamp casting hint."""
        error = _error_json(
            "retryable_syntax",
            "Parser Error: Cannot use INTERVAL with string literal",
        )
        hook_input = _make_post_input(error)
        result = asyncio.run(_sql_self_correction_handler(hook_input))

        assert result.modified_result is not None
        data = json.loads(result.modified_result)
        assert "CAST" in data["hint"]
        assert "TIMESTAMP" in data["hint"]

    def test_enriches_syntax_error_with_group_by_hint(self):
        """Syntax error mentioning GROUP BY gets column hint."""
        error = _error_json(
            "retryable_syntax",
            "Parser Error: column must appear in GROUP BY clause",
        )
        hook_input = _make_post_input(error)
        result = asyncio.run(_sql_self_correction_handler(hook_input))

        assert result.modified_result is not None
        data = json.loads(result.modified_result)
        assert "GROUP BY" in data["hint"]

    def test_no_enrichment_for_generic_syntax_error(self):
        """Syntax error without known patterns is not enriched."""
        error = _error_json(
            "retryable_syntax",
            "Parser Error: unexpected token 'xyz'",
        )
        hook_input = _make_post_input(error)
        result = asyncio.run(_sql_self_correction_handler(hook_input))

        # No enrichment -> modified_result stays None
        assert result.modified_result is None

    def test_terminal_errors_not_enriched(self):
        """Terminal errors are not enriched — passed through unchanged."""
        error = _error_json("terminal_crash", "IO Error: file not found")
        hook_input = _make_post_input(error)
        result = asyncio.run(_sql_self_correction_handler(hook_input))
        assert result.modified_result is None

    @patch("seeknal.ask.agents.tools._context.get_tool_context", side_effect=RuntimeError("no ctx"))
    def test_missing_ref_enrichment_survives_context_error(self, mock_get_ctx):
        """If get_tool_context() fails, missing_ref enrichment is skipped gracefully."""
        error = _error_json(
            "retryable_missing_ref",
            "Catalog Error: Table 'foo' does not exist",
        )
        hook_input = _make_post_input(error)
        result = asyncio.run(_sql_self_correction_handler(hook_input))
        # Should not raise — returns unchanged
        assert result.modified_result is None

    def test_handler_exception_never_propagates(self):
        """Even with completely broken input, the handler returns HookResult."""
        hook_input = HookInput(
            event=HookEvent.POST_TOOL_USE.value,
            tool_name="execute_sql",
            tool_input={"sql": "SELECT 1"},
            tool_result="{invalid json",
        )
        result = asyncio.run(_sql_self_correction_handler(hook_input))
        assert isinstance(result, HookResult)
        assert result.modified_result is None


# ---------------------------------------------------------------------------
# get_ask_hooks (replaces get_security_hooks)
# ---------------------------------------------------------------------------


class TestGetAskHooks:
    def test_returns_three_hooks(self):
        # PRE sql_security + POST sql_self_correction + POST csv_upload_reminder
        hooks = get_ask_hooks()
        assert len(hooks) == 3

    def test_first_hook_is_pre_tool_use(self):
        hooks = get_ask_hooks()
        assert hooks[0].event == HookEvent.PRE_TOOL_USE
        assert hooks[0].matcher == "execute_sql"

    def test_second_hook_is_post_tool_use(self):
        hooks = get_ask_hooks()
        assert hooks[1].event == HookEvent.POST_TOOL_USE
        assert hooks[1].matcher == "execute_sql"

    def test_backward_compat_alias(self):
        """get_security_hooks is an alias for get_ask_hooks."""
        assert get_security_hooks is get_ask_hooks


# ---------------------------------------------------------------------------
# r4: csv_upload_reminder hook — run_forecast dispatch + config gate
# ---------------------------------------------------------------------------


class TestCsvUploadReminderHookDispatch:
    """Verify the reminder handler dispatches correctly per tool_name.

    T5/T6 from the FC2d r4 plan: the matcher regex ``execute_sql|run_forecast``
    routes BOTH tools to ``_csv_upload_reminder_handler``; the handler then
    branches on ``tool_name`` to apply the right success/skip rule.
    """

    @pytest.mark.asyncio
    async def test_nudge_after_run_forecast_success(self):
        """T5: forecast returns 7-blok ``## Ringkasan`` → nudge appended."""
        from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

        hi = HookInput(
            event=HookEvent.POST_TOOL_USE,
            tool_name="run_forecast",
            tool_input={},
            tool_result="## Ringkasan\nForecast for next 3 periods using autoets.\n\n## Kualitas Proyeksi\n**BAIK**",
            tool_error=None,
        )
        out = await _csv_upload_reminder_handler(hi)
        assert out.modified_result is not None
        assert "upload_to_s3" in out.modified_result
        assert "data=" in out.modified_result  # data-mode hint, not SQL hint

    @pytest.mark.asyncio
    async def test_no_nudge_after_run_forecast_kesalahan(self):
        """T6a: forecast returns ``## Kesalahan`` (validation/exec error) → skip."""
        from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

        hi = HookInput(
            event=HookEvent.POST_TOOL_USE,
            tool_name="run_forecast",
            tool_input={},
            tool_result="## Kesalahan\n\n**Forecast SQL rejected by policy check.**",
            tool_error=None,
        )
        out = await _csv_upload_reminder_handler(hi)
        assert out.modified_result is None

    @pytest.mark.asyncio
    async def test_no_nudge_after_run_forecast_ditolak(self):
        """T6b: forecast returns ``## Ditolak`` (engine refuse) → skip."""
        from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

        hi = HookInput(
            event=HookEvent.POST_TOOL_USE,
            tool_name="run_forecast",
            tool_input={},
            tool_result="## Ditolak\n\nData tidak cukup (tersedia 18 bulan, minimum 24)",
            tool_error=None,
        )
        out = await _csv_upload_reminder_handler(hi)
        assert out.modified_result is None

    @pytest.mark.asyncio
    async def test_no_nudge_after_run_forecast_empty_result(self):
        """Defensive: empty result string → skip (no KeyError)."""
        from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

        hi = HookInput(
            event=HookEvent.POST_TOOL_USE,
            tool_name="run_forecast",
            tool_input={},
            tool_result="",
            tool_error=None,
        )
        out = await _csv_upload_reminder_handler(hi)
        assert out.modified_result is None

    @pytest.mark.asyncio
    async def test_execute_sql_still_uses_row_count(self):
        """r5: threshold=1, so even 1 row triggers auto-upload.
        Only EMPTY result (0 rows) skips.
        """
        from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

        # Empty table (0 rows) → below threshold 1 → no nudge
        empty_table = "| id |\n|---|\n\n"
        hi = HookInput(
            event=HookEvent.POST_TOOL_USE,
            tool_name="execute_sql",
            tool_input={"sql": "SELECT * FROM empty"},
            tool_result=empty_table,
            tool_error=None,
        )
        out = await _csv_upload_reminder_handler(hi)
        assert out.modified_result is None

        # 25-row table → triggers upload + nudge
        rows = "\n".join(f"| {i} |" for i in range(25))
        big_table = f"| id |\n|---|\n{rows}\n"
        hi2 = HookInput(
            event=HookEvent.POST_TOOL_USE,
            tool_name="execute_sql",
            tool_input={"sql": "SELECT * FROM big"},
            tool_result=big_table,
            tool_error=None,
        )
        with patch("seeknal.ask.agents.tools.upload_to_s3.upload_to_s3", return_value="Upload complete. Download: http://x"):
            out2 = await _csv_upload_reminder_handler(hi2)
        assert out2.modified_result is not None
        assert "Auto-uploaded" in out2.modified_result


class TestCsvUploadReminderConfigGate:
    """T4: get_hooks_config must read csv_upload_reminder; get_ask_hooks honors it."""

    def test_get_hooks_config_reads_csv_upload_reminder_true(self):
        from seeknal.ask.config import get_hooks_config

        cfg = get_hooks_config({"agent_harness": {"hooks": {"csv_upload_reminder": True}}})
        assert cfg["csv_upload_reminder"] is True

    def test_get_hooks_config_reads_csv_upload_reminder_false(self):
        from seeknal.ask.config import get_hooks_config

        cfg = get_hooks_config({"agent_harness": {"hooks": {"csv_upload_reminder": False}}})
        assert cfg["csv_upload_reminder"] is False

    def test_get_hooks_config_defaults_csv_upload_reminder_true(self):
        from seeknal.ask.config import get_hooks_config

        # When the yaml omits the key, the default should be True (opt-out).
        cfg = get_hooks_config({"agent_harness": {"hooks": {}}})
        assert cfg["csv_upload_reminder"] is True

    def test_get_ask_hooks_skips_csv_reminder_when_disabled(self):
        """T4b: csv_upload_reminder: false → hook NOT registered (only 2 hooks)."""
        hooks = get_ask_hooks({"csv_upload_reminder": False})
        # sql_security + sql_self_correction (no csv_reminder)
        assert len(hooks) == 2
        assert all("csv_upload_reminder" not in (h.handler.__name__ or "") for h in hooks)

    def test_get_ask_hooks_registers_csv_reminder_with_extended_matcher(self):
        """T4c: csv_upload_reminder: true → hook registered with the r4 matcher."""
        hooks = get_ask_hooks({"csv_upload_reminder": True})
        csv_hooks = [
            h for h in hooks
            if h.event == HookEvent.POST_TOOL_USE
            and "csv_upload_reminder" in (h.handler.__name__ or "")
        ]
        assert len(csv_hooks) == 1
        # Matcher is a regex that must match both execute_sql and run_forecast.
        assert csv_hooks[0].matcher == "execute_sql|run_forecast"
