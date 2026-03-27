"""Tests for seeknal heartbeat runner."""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from seeknal.ask.gateway.heartbeat.runner import (
    HeartbeatRunner,
    _is_active_hours,
    _is_heartbeat_ok,
    _load_state,
    _parse_interval,
    _save_state,
)


# ---------------------------------------------------------------------------
# _parse_interval
# ---------------------------------------------------------------------------


class TestParseInterval:
    def test_30m(self):
        assert _parse_interval("30m") == 1800

    def test_1h(self):
        assert _parse_interval("1h") == 3600

    def test_2h(self):
        assert _parse_interval("2h") == 7200

    def test_90s(self):
        assert _parse_interval("90s") == 90

    def test_with_whitespace(self):
        assert _parse_interval("  30m  ") == 1800

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid interval format"):
            _parse_interval("abc")

    def test_no_unit_raises(self):
        with pytest.raises(ValueError, match="Invalid interval format"):
            _parse_interval("30")

    def test_invalid_unit_raises(self):
        with pytest.raises(ValueError, match="Invalid interval format"):
            _parse_interval("30d")


# ---------------------------------------------------------------------------
# _is_active_hours
# ---------------------------------------------------------------------------


class TestIsActiveHours:
    def test_no_config_returns_true(self):
        assert _is_active_hours({}) is True

    def test_none_active_hours_returns_true(self):
        assert _is_active_hours({"active_hours": None}) is True

    def test_inside_window(self):
        """Mock datetime.now to be inside the active window."""
        with patch(
            "seeknal.ask.gateway.heartbeat.runner.datetime"
        ) as mock_dt:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "14:00"
            mock_dt.now.return_value = mock_now
            config = {
                "active_hours": {
                    "start": "08:00",
                    "end": "22:00",
                    "timezone": "Asia/Jakarta",
                }
            }
            assert _is_active_hours(config) is True

    def test_outside_window(self):
        """Mock datetime.now to be outside the active window."""
        with patch(
            "seeknal.ask.gateway.heartbeat.runner.datetime"
        ) as mock_dt:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "03:00"
            mock_dt.now.return_value = mock_now
            config = {
                "active_hours": {
                    "start": "08:00",
                    "end": "22:00",
                    "timezone": "Asia/Jakarta",
                }
            }
            assert _is_active_hours(config) is False

    def test_at_boundary_start(self):
        """Exactly at start time should be active."""
        with patch(
            "seeknal.ask.gateway.heartbeat.runner.datetime"
        ) as mock_dt:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "08:00"
            mock_dt.now.return_value = mock_now
            config = {
                "active_hours": {
                    "start": "08:00",
                    "end": "22:00",
                }
            }
            assert _is_active_hours(config) is True

    def test_at_boundary_end(self):
        """Exactly at end time should be active."""
        with patch(
            "seeknal.ask.gateway.heartbeat.runner.datetime"
        ) as mock_dt:
            mock_now = MagicMock()
            mock_now.strftime.return_value = "22:00"
            mock_dt.now.return_value = mock_now
            config = {
                "active_hours": {
                    "start": "08:00",
                    "end": "22:00",
                }
            }
            assert _is_active_hours(config) is True


# ---------------------------------------------------------------------------
# _is_heartbeat_ok
# ---------------------------------------------------------------------------


class TestIsHeartbeatOk:
    def test_starts_with_ok(self):
        assert _is_heartbeat_ok("HEARTBEAT_OK everything is fine") is True

    def test_ends_with_ok(self):
        assert _is_heartbeat_ok("All checks passed. HEARTBEAT_OK") is True

    def test_ok_only(self):
        assert _is_heartbeat_ok("HEARTBEAT_OK") is True

    def test_ok_with_whitespace(self):
        assert _is_heartbeat_ok("  HEARTBEAT_OK  ") is True

    def test_not_ok(self):
        assert _is_heartbeat_ok("Found 3 issues in the pipeline") is False

    def test_ok_in_middle_not_at_boundaries(self):
        assert (
            _is_heartbeat_ok("Prefix HEARTBEAT_OK Suffix") is False
        )

    def test_empty_string(self):
        assert _is_heartbeat_ok("") is False


# ---------------------------------------------------------------------------
# State save/load
# ---------------------------------------------------------------------------


class TestState:
    def test_save_and_load(self, tmp_path):
        state = {
            "last_run": "2026-03-27T10:00:00",
            "last_result": "ok",
            "last_message": "HEARTBEAT_OK",
        }
        _save_state(tmp_path, state)
        loaded = _load_state(tmp_path)
        assert loaded == state

    def test_load_missing_returns_empty(self, tmp_path):
        assert _load_state(tmp_path) == {}

    def test_save_creates_seeknal_dir(self, tmp_path):
        _save_state(tmp_path, {"test": True})
        assert (tmp_path / ".seeknal").is_dir()
        assert (tmp_path / ".seeknal" / "heartbeat_state.json").exists()

    def test_load_corrupted_returns_empty(self, tmp_path):
        seeknal_dir = tmp_path / ".seeknal"
        seeknal_dir.mkdir()
        (seeknal_dir / "heartbeat_state.json").write_text("not json{{{")
        assert _load_state(tmp_path) == {}


# ---------------------------------------------------------------------------
# HeartbeatRunner.run_once
# ---------------------------------------------------------------------------


class TestRunOnce:
    def test_missing_heartbeat_md(self, tmp_path):
        """run_once returns warning when HEARTBEAT.md is missing."""
        runner = HeartbeatRunner(config={}, channels={})
        result = asyncio.run(runner.run_once(tmp_path))
        assert "HEARTBEAT.md not found" in result
        # State file should be written
        state = _load_state(tmp_path)
        assert state["last_result"] == "skipped"

    def test_empty_heartbeat_md(self, tmp_path):
        """run_once returns warning when HEARTBEAT.md is empty."""
        seeknal_dir = tmp_path / ".seeknal"
        seeknal_dir.mkdir()
        (seeknal_dir / "HEARTBEAT.md").write_text("")
        runner = HeartbeatRunner(config={}, channels={})
        result = asyncio.run(runner.run_once(tmp_path))
        assert "empty" in result.lower()
        state = _load_state(tmp_path)
        assert state["last_result"] == "skipped"

    @patch("seeknal.ask.gateway.heartbeat.runner.HeartbeatRunner._run_agent")
    def test_ok_response_suppressed(self, mock_agent, tmp_path):
        """HEARTBEAT_OK response suppresses delivery."""
        seeknal_dir = tmp_path / ".seeknal"
        seeknal_dir.mkdir()
        (seeknal_dir / "HEARTBEAT.md").write_text("Check all pipelines")

        mock_agent.return_value = "HEARTBEAT_OK all pipelines healthy"

        mock_channel = AsyncMock()
        runner = HeartbeatRunner(
            config={"target": {"channel": "telegram", "chat_id": "-100123"}},
            channels={"telegram": mock_channel},
        )
        result = asyncio.run(runner.run_once(tmp_path))

        assert "HEARTBEAT_OK" in result
        mock_channel.deliver.assert_not_awaited()

        state = _load_state(tmp_path)
        assert state["last_result"] == "ok"

    @patch("seeknal.ask.gateway.heartbeat.runner.HeartbeatRunner._run_agent")
    def test_findings_delivered(self, mock_agent, tmp_path):
        """Non-OK response is delivered to the configured channel."""
        seeknal_dir = tmp_path / ".seeknal"
        seeknal_dir.mkdir()
        (seeknal_dir / "HEARTBEAT.md").write_text("Check all pipelines")

        findings = "Found 3 failed nodes in the pipeline."
        mock_agent.return_value = findings

        mock_channel = AsyncMock()
        runner = HeartbeatRunner(
            config={"target": {"channel": "telegram", "chat_id": "-100123"}},
            channels={"telegram": mock_channel},
        )
        result = asyncio.run(runner.run_once(tmp_path))

        assert result == findings
        mock_channel.deliver.assert_awaited_once_with(
            "telegram:-100123", findings
        )

        state = _load_state(tmp_path)
        assert state["last_result"] == "findings"

    @patch("seeknal.ask.gateway.heartbeat.runner.HeartbeatRunner._run_agent")
    def test_missing_channel_logs_warning(self, mock_agent, tmp_path):
        """Delivery is skipped when channel is not in channels dict."""
        seeknal_dir = tmp_path / ".seeknal"
        seeknal_dir.mkdir()
        (seeknal_dir / "HEARTBEAT.md").write_text("Check pipelines")

        mock_agent.return_value = "Problems found"

        runner = HeartbeatRunner(
            config={"target": {"channel": "slack", "chat_id": "C123"}},
            channels={},  # No slack channel
        )
        # Should not raise
        result = asyncio.run(runner.run_once(tmp_path))
        assert result == "Problems found"

    @patch("seeknal.ask.gateway.heartbeat.runner.HeartbeatRunner._run_agent")
    def test_no_target_config(self, mock_agent, tmp_path):
        """Delivery is skipped when no target is configured."""
        seeknal_dir = tmp_path / ".seeknal"
        seeknal_dir.mkdir()
        (seeknal_dir / "HEARTBEAT.md").write_text("Check pipelines")

        mock_agent.return_value = "Issues detected"

        runner = HeartbeatRunner(config={}, channels={})
        # Should not raise
        result = asyncio.run(runner.run_once(tmp_path))
        assert result == "Issues detected"

    @patch("seeknal.ask.gateway.heartbeat.runner.HeartbeatRunner._run_agent")
    def test_agent_called_with_heartbeat_content(self, mock_agent, tmp_path):
        """Agent is called with HEARTBEAT.md content as the question."""
        seeknal_dir = tmp_path / ".seeknal"
        seeknal_dir.mkdir()
        heartbeat_content = "Check row counts for all tables"
        (seeknal_dir / "HEARTBEAT.md").write_text(heartbeat_content)

        mock_agent.return_value = "HEARTBEAT_OK"

        runner = HeartbeatRunner(config={}, channels={})
        asyncio.run(runner.run_once(tmp_path))

        mock_agent.assert_awaited_once()
        call_args = mock_agent.call_args
        assert call_args[0][0] == tmp_path  # project_path
        assert call_args[0][1] == heartbeat_content  # question
        assert call_args[0][2].startswith("heartbeat-")  # session_name


# ---------------------------------------------------------------------------
# HeartbeatRunner constructor
# ---------------------------------------------------------------------------


class TestHeartbeatRunnerInit:
    def test_default_interval(self):
        runner = HeartbeatRunner(config={}, channels={})
        assert runner._interval == 1800  # 30m default

    def test_custom_interval(self):
        runner = HeartbeatRunner(config={"every": "1h"}, channels={})
        assert runner._interval == 3600

    def test_channels_stored(self):
        ch = {"telegram": MagicMock()}
        runner = HeartbeatRunner(config={}, channels=ch)
        assert runner._channels == ch
