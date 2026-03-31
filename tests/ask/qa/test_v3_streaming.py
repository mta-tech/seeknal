"""V3 TUI Streaming QA Tests — exercises the real streaming path.

Tests stream_ask() with real LLM calls and Rich Console output capture.
Uses a single event loop for all tests to avoid asyncio binding conflicts
(pydantic-ai's HTTP client retains loop references across agent instances).

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
    """All streaming tests share one event loop to avoid asyncio conflicts.

    pydantic-ai's HTTP client (httpcore/anyio) binds to the event loop.
    Creating multiple loops causes 'Event loop is closed' errors when
    old connections try to clean up on dead loops.
    """

    def test_all_streaming_scenarios(self, qa_project):
        """Run all streaming scenarios in sequence on one event loop."""
        from seeknal.ask.agents.agent import create_agent
        from seeknal.ask.streaming import stream_ask

        loop = asyncio.new_event_loop()

        try:
            # --- Scenario 1: Simple query with tool visibility ---
            agent, deps, mh = create_agent(
                project_path=qa_project, provider="google", model="gemini-2.5-flash",
            )
            console, buf = _make_console()

            answer = loop.run_until_complete(
                stream_ask(agent, deps, mh, "How many customers?", console)
            )
            output = buf.getvalue()

            print(f"\n--- STREAMING 1: Simple query ---")
            print(f"Answer: {answer[:200]}")
            print(f"Console output length: {len(output)} chars")
            print(f"Has tool visibility: {'execute_sql' in output.lower() or 'list_tables' in output.lower()}")
            assert answer and len(answer) > 10
            assert "50" in answer
            assert len(output) > 50

            # --- Scenario 2: Multi-step analysis ---
            agent2, deps2, mh2 = create_agent(
                project_path=qa_project, provider="google", model="gemini-2.5-flash",
            )
            console2, buf2 = _make_console()

            answer2 = loop.run_until_complete(
                stream_ask(
                    agent2, deps2, mh2,
                    "Compare revenue between Premium and Basic segments.",
                    console2,
                )
            )
            output2 = buf2.getvalue()

            print(f"\n--- STREAMING 2: Multi-step ---")
            print(f"Answer length: {len(answer2)} chars")
            print(f"Console output length: {len(output2)} chars")
            assert answer2 and len(answer2) > 50

            # --- Scenario 3: Multi-turn on same agent ---
            agent3, deps3, mh3 = create_agent(
                project_path=qa_project, provider="google", model="gemini-2.5-flash",
            )
            console3, buf3 = _make_console()

            a3_1 = loop.run_until_complete(
                stream_ask(agent3, deps3, mh3, "How many segments?", console3)
            )
            print(f"\n--- STREAMING 3: Turn 1 ---")
            print(f"Answer: {a3_1[:150]}")
            print(f"History: {len(mh3)} messages")

            a3_2 = loop.run_until_complete(
                stream_ask(agent3, deps3, mh3, "Which has the most customers?", console3)
            )
            print(f"--- STREAMING 3: Turn 2 ---")
            print(f"Answer: {a3_2[:150]}")
            print(f"History: {len(mh3)} messages")

            assert a3_1 and a3_2
            assert len(mh3) > 4

            # --- Scenario 4: Quiet mode ---
            agent4, deps4, mh4 = create_agent(
                project_path=qa_project, provider="google", model="gemini-2.5-flash",
            )
            console4, buf4 = _make_console()

            answer4 = loop.run_until_complete(
                stream_ask(agent4, deps4, mh4, "How many products?", console4, quiet=True)
            )
            output4 = buf4.getvalue()

            print(f"\n--- STREAMING 4: Quiet mode ---")
            print(f"Answer: {answer4[:200]}")
            print(f"Console output (quiet): {len(output4)} chars")
            assert answer4 and len(answer4) > 10

            print(f"\n--- ALL 4 STREAMING SCENARIOS PASSED ---")

        finally:
            loop.close()
