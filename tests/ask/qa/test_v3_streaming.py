"""V3 TUI Streaming QA Tests — exercises the real streaming path.

One consolidated test runs multiple streaming scenarios sequentially
within a single asyncio.run() call, matching how the CLI works and
avoiding event loop contamination between tests.

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
    """Streaming tests consolidated into one asyncio.run() entry point.

    pydantic-ai's HTTP client (httpcore/anyio) retains event loop references.
    Multiple asyncio.run() calls in one process cause 'Event loop is closed'
    errors. The CLI has the same constraint — one asyncio.run() per process.
    """

    def test_streaming_scenarios(self, qa_project):
        """Run all streaming scenarios in one async session."""

        async def _run_all():
            from seeknal.ask.agents.agent import create_agent
            from seeknal.ask.streaming import stream_ask

            results = {}

            # --- Scenario 1: Simple query with tool visibility ---
            agent1, deps1, mh1 = create_agent(
                project_path=qa_project, provider="google", model="gemini-2.5-flash",
            )
            console1, buf1 = _make_console()
            answer1 = await stream_ask(agent1, deps1, mh1, "How many customers?", console1)
            output1 = buf1.getvalue()

            print(f"\n--- STREAMING 1: Simple query ---")
            print(f"Answer: {answer1[:200]}")
            print(f"Console output: {len(output1)} chars")
            print(f"Tool visibility: {'execute_sql' in output1.lower() or 'list_tables' in output1.lower()}")
            assert answer1 and "50" in answer1
            assert len(output1) > 50
            results["simple"] = True

            # --- Scenario 2: Multi-step analysis ---
            agent2, deps2, mh2 = create_agent(
                project_path=qa_project, provider="google", model="gemini-2.5-flash",
            )
            console2, buf2 = _make_console()
            answer2 = await stream_ask(
                agent2, deps2, mh2,
                "Compare revenue between Premium and Basic segments. Show data.",
                console2,
            )
            output2 = buf2.getvalue()

            print(f"\n--- STREAMING 2: Multi-step ---")
            print(f"Answer: {len(answer2)} chars")
            print(f"Console output: {len(output2)} chars")
            assert answer2 and len(answer2) > 50
            results["multistep"] = True

            # --- Scenario 3: Multi-turn on same agent ---
            agent3, deps3, mh3 = create_agent(
                project_path=qa_project, provider="google", model="gemini-2.5-flash",
            )
            console3, buf3 = _make_console()
            a3_1 = await stream_ask(agent3, deps3, mh3, "How many segments?", console3)
            a3_2 = await stream_ask(agent3, deps3, mh3, "Which has the most customers?", console3)

            print(f"\n--- STREAMING 3: Multi-turn ---")
            print(f"Turn 1: {a3_1[:150]}")
            print(f"Turn 2: {a3_2[:150]}")
            print(f"History: {len(mh3)} messages")
            assert a3_1 and a3_2
            assert len(mh3) > 4
            results["multiturn"] = True

            print(f"\n--- ALL STREAMING SCENARIOS PASSED: {results} ---")

        asyncio.run(_run_all())
