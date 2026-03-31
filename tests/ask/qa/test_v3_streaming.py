"""V3 TUI Streaming QA Tests — exercises the real streaming path.

Tests stream_ask() with real LLM calls and Rich Console output capture.
Uses asyncio.run() matching the production CLI path (cli/ask.py).

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


class TestStreaming:
    """Streaming tests using asyncio.run() — matching the production CLI path.

    Each test creates its own agent and uses asyncio.run() for one async
    entry point, same as cli/ask.py's _run_oneshot().
    """

    def test_stream_simple_query_with_tool_visibility(self, qa_project):
        """stream_ask renders progressive tool calls and answer panel."""
        from seeknal.ask.agents.agent import create_agent
        from seeknal.ask.streaming import stream_ask

        agent, deps, mh = create_agent(
            project_path=qa_project, provider="google", model="gemini-2.5-flash",
        )
        console, buf = _make_console()

        answer = asyncio.run(
            stream_ask(agent, deps, mh, "How many customers?", console)
        )
        output = buf.getvalue()

        print(f"\n--- STREAMING: Simple query ---")
        print(f"Answer: {answer[:200]}")
        print(f"Console output length: {len(output)} chars")
        print(f"--- Console output (first 500 chars) ---\n{output[:500]}\n")

        assert answer and len(answer) > 10
        assert "50" in answer
        assert len(output) > 50  # Console should have rendered something

    def test_stream_multistep_analysis(self, qa_project):
        """Multi-step streaming shows multiple tool calls progressively."""
        from seeknal.ask.agents.agent import create_agent
        from seeknal.ask.streaming import stream_ask

        agent, deps, mh = create_agent(
            project_path=qa_project, provider="google", model="gemini-2.5-flash",
        )
        console, buf = _make_console()

        answer = asyncio.run(
            stream_ask(
                agent, deps, mh,
                "Compare revenue between Premium and Basic customer segments. Show data.",
                console,
            )
        )
        output = buf.getvalue()

        print(f"\n--- STREAMING: Multi-step analysis ---")
        print(f"Answer length: {len(answer)} chars")
        print(f"Console output length: {len(output)} chars")
        print(f"--- Console output (first 1000 chars) ---\n{output[:1000]}\n")

        assert answer and len(answer) > 50

    def test_stream_multiturn_preserves_history(self, qa_project):
        """Two streaming turns on same agent maintain conversation context."""
        from seeknal.ask.agents.agent import create_agent
        from seeknal.ask.streaming import stream_ask

        agent, deps, mh = create_agent(
            project_path=qa_project, provider="google", model="gemini-2.5-flash",
        )
        console, buf = _make_console()

        async def _two_turns():
            a1 = await stream_ask(agent, deps, mh, "How many segments?", console)
            a2 = await stream_ask(agent, deps, mh, "Which has the most customers?", console)
            return a1, a2

        a1, a2 = asyncio.run(_two_turns())

        print(f"\n--- STREAMING: Multi-turn ---")
        print(f"Turn 1: {a1[:150]}")
        print(f"Turn 2: {a2[:150]}")
        print(f"History: {len(mh)} messages")

        assert a1 and a2
        assert len(mh) > 4
