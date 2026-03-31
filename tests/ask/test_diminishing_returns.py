"""Tests for diminishing returns detection in the Ralph Loop."""

from unittest.mock import MagicMock, patch

import pytest

from seeknal.ask.agents.agent import (
    _DIMINISHING_RETURNS_MSG,
    _LOW_OUTPUT_RETRIES,
    _LOW_OUTPUT_THRESHOLD,
    _MAX_RALPH_RETRIES,
    _NO_RESPONSE,
    ask,
)


def _make_result(output: str):
    """Create a mock agent result with the given output."""
    result = MagicMock()
    result.output = output
    result.all_messages.return_value = []
    return result


class TestDiminishingReturnsConstants:
    def test_low_output_retries(self):
        assert _LOW_OUTPUT_RETRIES == 3

    def test_low_output_threshold(self):
        assert _LOW_OUTPUT_THRESHOLD == 500

    def test_max_ralph_retries(self):
        assert _MAX_RALPH_RETRIES == 3

    def test_diminishing_returns_message_is_helpful(self):
        assert "rephrasing" in _DIMINISHING_RETURNS_MSG
        assert len(_DIMINISHING_RETURNS_MSG) > 20


class TestDiminishingReturnsInAsk:
    @patch("seeknal.ask.agents.quality.check_answer_quality", return_value=(True, ""))
    def test_first_attempt_with_answer_returns_immediately(self, _mock_quality):
        """If the first attempt produces text, no retries needed."""
        agent = MagicMock()
        agent.run_sync.return_value = _make_result("Here are 42 customers.")
        deps = MagicMock()

        answer = ask(agent, deps, [], "how many customers?")
        assert answer == "Here are 42 customers."
        assert agent.run_sync.call_count == 1

    @patch("seeknal.ask.agents.quality.check_answer_quality", return_value=(True, ""))
    def test_retry_produces_answer(self, _mock_quality):
        """First attempt empty, retry produces text -> returns retry answer."""
        agent = MagicMock()
        agent.run_sync.side_effect = [
            _make_result(""),     # first attempt: empty
            _make_result("Found 10 orders."),  # retry: has text
        ]
        deps = MagicMock()

        answer = ask(agent, deps, [], "how many orders?")
        assert answer == "Found 10 orders."
        assert agent.run_sync.call_count == 2

    def test_all_retries_empty_returns_diminishing(self):
        """All retries produce empty -> diminishing returns message."""
        agent = MagicMock()
        # 1 initial + 3 retries = 4 calls, all empty
        agent.run_sync.return_value = _make_result("")
        deps = MagicMock()

        answer = ask(agent, deps, [], "?")
        # With _MAX_RALPH_RETRIES=3 and _LOW_OUTPUT_RETRIES=3,
        # all retries are empty (0 chars < 500), so diminishing returns triggers
        assert answer == _DIMINISHING_RETURNS_MSG

    def test_diminishing_returns_stops_early(self):
        """3+ consecutive low-output retries -> graceful stop message."""
        agent = MagicMock()
        # All calls produce empty string (0 chars < 500 threshold)
        agent.run_sync.return_value = _make_result("")
        deps = MagicMock()

        answer = ask(agent, deps, [], "complex question")
        assert answer == _DIMINISHING_RETURNS_MSG


class TestDiminishingReturnsInStreaming:
    """Verify the streaming path imports the same constants."""

    def test_streaming_imports_constants(self):
        from seeknal.ask.streaming import (
            _DIMINISHING_RETURNS_MSG as stream_msg,
            _LOW_OUTPUT_RETRIES as stream_retries,
            _LOW_OUTPUT_THRESHOLD as stream_threshold,
        )
        assert stream_msg == _DIMINISHING_RETURNS_MSG
        assert stream_retries == _LOW_OUTPUT_RETRIES
        assert stream_threshold == _LOW_OUTPUT_THRESHOLD
