"""Tests for the POST_TOOL_USE CSV auto-upload hook (FC2d r4).

The r4 hook no longer just nudges — for ``execute_sql`` results above the
``_CSV_REMINDER_MIN_ROWS`` threshold it AUTOMATICALLY calls ``upload_to_s3``
(Mode 1) and re-executes the agent's SQL. ``upload_to_s3`` itself is mocked
here so the tests don't require iba-storage / SeaweedFS.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from seeknal.ask.agents.hooks import (
    _CSV_REMINDER_MIN_ROWS,
    _count_result_rows,
    _csv_upload_reminder_handler,
    get_ask_hooks,
)


def _make_result(rows: int) -> str:
    header = "| id | value |\n|---|---|\n"
    body = "".join(f"| {i} | {i * 10} |\n" for i in range(rows))
    return header + body


class _HI:
    """Stub HookInput — includes tool_input for the r4 auto-upload path."""

    def __init__(
        self,
        tool_name: str,
        tool_result: str,
        tool_input: dict | None = None,
    ):
        self.event = "post_tool_use"
        self.tool_name = tool_name
        self.tool_result = tool_result
        self.tool_input = tool_input or {}
        self.tool_error = None


# ---------------------------------------------------------------------------
# Row counting (unchanged by r4)
# ---------------------------------------------------------------------------


def test_count_rows_parses_markdown_table():
    assert _count_result_rows(_make_result(25)) == 25
    assert _count_result_rows(_make_result(5)) == 5
    assert _count_result_rows("") == 0


# ---------------------------------------------------------------------------
# r4 auto-upload (execute_sql)
# ---------------------------------------------------------------------------


def test_auto_upload_called_above_threshold_with_sql():
    """Above threshold + SQL in tool_input → upload_to_s3 IS called,
    modified_result announces the auto-upload.
    """
    hi = _HI(
        "execute_sql",
        _make_result(30),
        tool_input={"sql": "SELECT id FROM warehouse.public.t_produk_3_erba"},
    )
    with patch(
        "seeknal.ask.agents.tools.upload_to_s3.upload_to_s3",
        return_value="Upload complete. Download (valid 8h): http://x/y.csv",
    ) as up_mock:
        result = asyncio.run(_csv_upload_reminder_handler(hi))
    up_mock.assert_called_once()
    # filename derived from SQL table; sql passed through
    call_kwargs = up_mock.call_args
    assert call_kwargs.kwargs.get("sql") == "SELECT id FROM warehouse.public.t_produk_3_erba"
    assert result.modified_result is not None
    assert "Auto-uploaded" in result.modified_result
    assert "30 rows" in result.modified_result


def test_auto_upload_skipped_below_threshold():
    """Below threshold → no upload call, no modified_result.
    r5: threshold lowered to 1, so only EMPTY results skip upload.
    """
    hi = _HI(
        "execute_sql",
        _make_result(max(0, _CSV_REMINDER_MIN_ROWS - 1)),  # 0 rows = empty
        tool_input={"sql": "SELECT id FROM x"},
    )
    with patch("seeknal.ask.agents.tools.upload_to_s3.upload_to_s3") as up_mock:
        result = asyncio.run(_csv_upload_reminder_handler(hi))
    up_mock.assert_not_called()
    assert result.modified_result is None


def test_auto_upload_skipped_when_no_sql_in_tool_input():
    """Above threshold but tool_input missing SQL → fallback nudge only."""
    hi = _HI("execute_sql", _make_result(40), tool_input={})
    with patch("seeknal.ask.agents.tools.upload_to_s3.upload_to_s3") as up_mock:
        result = asyncio.run(_csv_upload_reminder_handler(hi))
    up_mock.assert_not_called()
    assert result.modified_result is not None
    assert "auto-upload skipped" in result.modified_result.lower()


def test_auto_upload_swallows_upload_failure():
    """upload_to_s3 raises → hook reports failure in modified_result, never crashes."""
    hi = _HI(
        "execute_sql",
        _make_result(15),
        tool_input={"sql": "SELECT 1"},
    )
    with patch(
        "seeknal.ask.agents.tools.upload_to_s3.upload_to_s3",
        side_effect=RuntimeError("storage unreachable"),
    ):
        result = asyncio.run(_csv_upload_reminder_handler(hi))
    assert result.modified_result is not None
    assert "auto-upload failed" in result.modified_result.lower()
    assert "RuntimeError" in result.modified_result


def test_no_dispatch_for_non_matched_tool():
    """Defensive: an unexpected tool_name returns HookResult() unchanged."""
    hi = _HI("preview_query", _make_result(100), tool_input={"sql": "SELECT 1"})
    with patch("seeknal.ask.agents.tools.upload_to_s3.upload_to_s3") as up_mock:
        result = asyncio.run(_csv_upload_reminder_handler(hi))
    up_mock.assert_not_called()
    assert result.modified_result is None


# ---------------------------------------------------------------------------
# run_forecast branch (Mode 2 nudge — unchanged by r4)
# ---------------------------------------------------------------------------


def test_forecast_success_nudges_data_mode():
    hi = _HI("run_forecast", "## Ringkasan\nForecast for next 3 periods.")
    with patch("seeknal.ask.agents.tools.upload_to_s3.upload_to_s3") as up_mock:
        result = asyncio.run(_csv_upload_reminder_handler(hi))
    up_mock.assert_not_called()  # nudge only, no auto-upload for forecast
    assert result.modified_result is not None
    assert "data=" in result.modified_result


def test_forecast_kesalahan_no_nudge():
    hi = _HI("run_forecast", "## Kesalahan\nSQL rejected")
    with patch("seeknal.ask.agents.tools.upload_to_s3.upload_to_s3") as up_mock:
        result = asyncio.run(_csv_upload_reminder_handler(hi))
    up_mock.assert_not_called()
    assert result.modified_result is None


def test_forecast_ditolak_no_nudge():
    hi = _HI("run_forecast", "## Ditolak\nInsufficient history")
    with patch("seeknal.ask.agents.tools.upload_to_s3.upload_to_s3") as up_mock:
        result = asyncio.run(_csv_upload_reminder_handler(hi))
    up_mock.assert_not_called()
    assert result.modified_result is None


# ---------------------------------------------------------------------------
# Registration / config gating (unchanged by r4)
# ---------------------------------------------------------------------------


def test_hook_registered_in_get_ask_hooks():
    from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

    hooks = get_ask_hooks({"enabled": True})
    handlers = [h.handler for h in hooks]
    assert _csv_upload_reminder_handler in handlers


def test_hook_gated_by_config():
    from seeknal.ask.agents.hooks import _csv_upload_reminder_handler

    hooks = get_ask_hooks({"enabled": True, "csv_upload_reminder": False})
    handlers = [h.handler for h in hooks]
    assert _csv_upload_reminder_handler not in handlers


def test_hook_matcher_covers_execute_sql_and_run_forecast():
    """r4 matcher is a regex matching BOTH tools so pydantic_deep routes both."""
    hooks = get_ask_hooks({"enabled": True, "csv_upload_reminder": True})
    csv_hooks = [
        h for h in hooks
        if h.event.value == "post_tool_use"
        and "csv_upload_reminder" in (h.handler.__name__ or "")
    ]
    assert len(csv_hooks) == 1
    assert csv_hooks[0].matcher == "execute_sql|run_forecast"
