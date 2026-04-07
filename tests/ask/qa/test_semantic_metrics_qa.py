"""Business Metrics QA tests — semantic layer and metric definitions with real Gemini LLM.

Tests the agent's ability to bootstrap semantic models from data,
query metrics through the semantic layer, define new metrics, and
save them as YAML definitions.

Run with: GOOGLE_API_KEY=... uv run pytest tests/ask/qa/test_semantic_metrics_qa.py -v -s
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


class TestSemanticBootstrap:
    """Agent bootstraps semantic models from existing data tables."""

    def test_bootstrap_semantic_models(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "Use bootstrap_semantic_model to auto-discover and generate "
            "semantic models from data tables. Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: Bootstrap ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Summarize what was discovered — tables, columns, classifications."
        )
        print(f"\n--- Turn 2: Review schema ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Apply any generated semantic model draft files to the project."
        )
        print(f"\n--- Turn 3: Apply ---\n{a3}\n")
        assert_answer_quality(a3)


class TestMetricQueries:
    """Agent queries business metrics using SQL."""

    def test_metric_queries_workflow(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "What's the total revenue for each product category? "
            "Rank highest to lowest. Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: Revenue by category ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Show monthly order count and revenue trend with "
            "month-over-month growth rate."
        )
        print(f"\n--- Turn 2: Monthly growth ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "What's the average order value by customer segment and city?"
        )
        print(f"\n--- Turn 3: AOV by segment+city ---\n{a3}\n")
        assert_answer_quality(a3)

        a4 = _ask(agent, deps, mh,
            "Calculate revenue per unique customer for each segment. "
            "Which segment generates the most revenue per customer?"
        )
        print(f"\n--- Turn 4: Revenue per customer ---\n{a4}\n")
        assert_answer_quality(a4)


class TestMetricDefinitions:
    """Agent defines and saves new business metric definitions."""

    def test_define_and_save_metrics(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "Define a metric called 'revenue_per_customer' (total revenue / "
            "unique customers). Use save_metric to save it. "
            "Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: Define metric ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Verify the metric was saved. What metrics exist in the project?"
        )
        print(f"\n--- Turn 2: Verify saved ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Calculate revenue per customer for each product category. "
            "Which category has the highest per-customer value?"
        )
        print(f"\n--- Turn 3: Use metric ---\n{a3}\n")
        assert_answer_quality(a3)
