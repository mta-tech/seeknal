"""Data Analysis QA tests — multi-turn SQL analysis with real Gemini LLM.

Tests the agent's ability to explore data, write SQL queries, perform
cross-table analysis, and derive business insights across multi-turn
conversations using the e-commerce dataset.

Run with: GOOGLE_API_KEY=... uv run pytest tests/ask/qa/test_data_analysis_qa.py -v -s
"""

import os
import pytest

from tests.ask.qa.conftest import assert_answer_quality, assert_keywords, fresh_agent, safe_ask

pytestmark = pytest.mark.skipif(
    not os.environ.get("GOOGLE_API_KEY"),
    reason="GOOGLE_API_KEY not set — skipping real LLM tests",
)


def _ask(agent, deps, mh, question):
    from seeknal.ask.agents.agent import ask
    return safe_ask(ask, agent, deps, mh, question)


class TestTableDiscoveryAndSchema:
    """Agent discovers tables, understands schemas, and counts rows."""

    def test_multi_turn_discovery(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh, "What tables and data are available in this project?")
        print(f"\n--- Turn 1: Tables available ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Describe the transform_orders_cleaned table — what columns does it have?"
        )
        print(f"\n--- Turn 2: Describe table ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh, "How many rows does each table have?")
        print(f"\n--- Turn 3: Row counts ---\n{a3}\n")
        assert_answer_quality(a3)


class TestCrossTableAnalysis:
    """Agent performs multi-table SQL analysis with joins and aggregations."""

    def test_multi_turn_analysis(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "What's the total revenue by customer segment? "
            "Join orders with customers to get segments. "
            "Don't ask follow-up questions — just show the results."
        )
        print(f"\n--- Turn 1: Revenue by segment ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Which product category performs best in each city? "
            "Show the top category by revenue for each city."
        )
        print(f"\n--- Turn 2: Category by city ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Show me the month-over-month revenue trend for 2024."
        )
        print(f"\n--- Turn 3: Monthly trend ---\n{a3}\n")
        assert_answer_quality(a3)

        a4 = _ask(agent, deps, mh,
            "What's the relationship between a customer's order count "
            "and their average order value?"
        )
        print(f"\n--- Turn 4: Correlation ---\n{a4}\n")
        assert_answer_quality(a4)


class TestSegmentComparison:
    """Agent compares customer segments and derives business insights."""

    def test_multi_turn_comparison(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "Compare Premium vs Basic customers on total spend and order frequency. "
            "Don't ask follow-up questions — answer directly with the data."
        )
        print(f"\n--- Turn 1: Segment comparison ---\n{a1}\n")
        assert_answer_quality(a1)
        assert_keywords(a1, ["Premium", "Basic", "spend", "order", "segment"], threshold=0.2)

        a2 = _ask(agent, deps, mh,
            "Which customer segment has the best retention? "
            "Look at recency — days since last purchase."
        )
        print(f"\n--- Turn 2: Retention ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Based on everything we've analyzed, what are the top 3 "
            "actionable insights for the business?"
        )
        print(f"\n--- Turn 3: Insights ---\n{a3}\n")
        assert_answer_quality(a3, min_length=50)
