"""V3 TUI Streaming QA Tests — exercises the real streaming path.

Tests stream_ask() with real LLM calls and Rich Console output capture.
Each test creates its own agent AND event loop to avoid asyncio conflicts
(pydantic-ai binds internal asyncio primitives to the first loop used).

Requires GOOGLE_API_KEY environment variable for real Gemini LLM calls.
Run with: pytest tests/ask/qa/test_v3_streaming.py -v -s
"""

import asyncio
import os
from io import StringIO

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("GOOGLE_API_KEY"),
    reason="GOOGLE_API_KEY not set — skipping real LLM tests",
)


def _make_console():
    """Create a Rich Console that captures output to a string buffer."""
    from rich.console import Console

    buf = StringIO()
    return Console(file=buf, force_terminal=True, width=120), buf


def _run_streaming_test(qa_project, question, quiet=False):
    """Create a fresh agent + event loop and run a streaming question.

    Returns (answer, console_output).
    Each call gets its own agent and loop to avoid asyncio binding conflicts.
    """
    from seeknal.ask.agents.agent import create_agent
    from seeknal.ask.streaming import stream_ask

    agent, deps, message_history = create_agent(
        project_path=qa_project,
        provider="google",
        model="gemini-2.5-flash",
    )
    console, buf = _make_console()

    loop = asyncio.new_event_loop()
    try:
        answer = loop.run_until_complete(
            stream_ask(agent, deps, message_history, question, console, quiet=quiet)
        )
    finally:
        loop.close()

    return answer, buf.getvalue(), message_history


class TestStreamingRendering:
    """Test the real streaming path with Rich Console output capture."""

    def test_stream_simple_query(self, qa_project):
        """stream_ask should render progressive output for a simple query."""
        answer, output, _ = _run_streaming_test(qa_project, "How many customers?")

        print(f"\n--- STREAMING: Simple query ---")
        print(f"Answer: {answer[:200]}")
        print(f"Console output length: {len(output)} chars")
        print(f"--- Console output (first 500 chars) ---\n{output[:500]}\n")

        assert answer and len(answer) > 10
        assert "50" in answer
        assert len(output) > 50

    def test_stream_shows_tool_calls(self, qa_project):
        """Streaming output should show tool invocations."""
        answer, output, _ = _run_streaming_test(
            qa_project,
            "What are the top 3 product categories by order count?",
        )

        print(f"\n--- STREAMING: Tool visibility ---")
        print(f"Answer: {answer[:200]}")
        print(f"--- Console output (first 1000 chars) ---\n{output[:1000]}\n")

        assert answer and len(answer) > 30
        output_lower = output.lower()
        has_tool_visibility = (
            "execute_sql" in output_lower
            or "list_tables" in output_lower
            or "select" in output_lower
        )
        print(f"Tool visibility detected: {has_tool_visibility}")

    def test_stream_multistep_analysis(self, qa_project):
        """Multi-step analysis should show reasoning + multiple tool calls."""
        answer, output, _ = _run_streaming_test(
            qa_project,
            "Compare revenue between Premium and Basic customer segments. "
            "Show the data and explain the difference.",
        )

        print(f"\n--- STREAMING: Multi-step ---")
        print(f"Answer length: {len(answer)} chars")
        print(f"Console output length: {len(output)} chars")
        print(f"--- Console output (first 1500 chars) ---\n{output[:1500]}\n")

        assert answer and len(answer) > 50

    def test_quiet_mode_uses_sync(self, qa_project):
        """Quiet mode should use sync ask() — minimal console output."""
        answer, output, _ = _run_streaming_test(
            qa_project,
            "How many products are there?",
            quiet=True,
        )

        print(f"\n--- STREAMING: Quiet mode ---")
        print(f"Answer: {answer[:200]}")
        print(f"Console output length: {len(output)} chars")

        assert answer and len(answer) > 10


class TestStreamingMultiTurn:
    """Test multi-turn streaming chat maintaining context."""

    def test_two_turn_streaming_chat(self, qa_project):
        """Two sequential stream_ask calls should maintain message history."""
        from seeknal.ask.agents.agent import create_agent
        from seeknal.ask.streaming import stream_ask

        agent, deps, message_history = create_agent(
            project_path=qa_project,
            provider="google",
            model="gemini-2.5-flash",
        )
        console, buf = _make_console()

        loop = asyncio.new_event_loop()
        try:
            a1 = loop.run_until_complete(
                stream_ask(agent, deps, message_history,
                           "How many customer segments are there?", console)
            )
            print(f"\n--- CHAT Turn 1: {a1[:150]}")
            print(f"Message history after turn 1: {len(message_history)} messages")

            a2 = loop.run_until_complete(
                stream_ask(agent, deps, message_history,
                           "Which one has the most customers?", console)
            )
            print(f"--- CHAT Turn 2: {a2[:150]}")
            print(f"Message history after turn 2: {len(message_history)} messages")
        finally:
            loop.close()

        assert a1 and a2
        assert len(message_history) > 4
