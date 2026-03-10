"""Pipeline context QA tests — verify agent can read and explain pipeline definitions.

These tests check that the agent uses read_pipeline and search_pipelines tools
to answer "how" and "why" questions about data production logic.

Run with: GOOGLE_API_KEY=... uv run pytest tests/ask/qa/test_pipeline_context.py -v -s
"""

import os
import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("GOOGLE_API_KEY"),
    reason="GOOGLE_API_KEY not set — skipping real LLM tests",
)


def _fresh_agent(qa_project):
    """Create a fresh agent with its own thread for isolated multi-turn."""
    from seeknal.ask.agents.agent import create_agent
    return create_agent(
        project_path=qa_project,
        provider="google",
        model="gemini-2.5-flash",
    )


class TestPipelineAwareness:
    """Tests that verify the agent can discover and explain pipeline logic."""

    def test_list_pipelines(self, qa_project):
        """Agent should know what pipeline files exist."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "What pipeline definitions exist in this project? "
            "List the sources, transforms, and feature groups."
        )
        print(f"\n{'='*60}\nPipeline List\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 30
        assert any(w in answer.lower() for w in [
            "raw_orders", "raw_customers", "orders_cleaned",
            "source", "transform",
        ])

    def test_explain_revenue_calculation(self, qa_project):
        """Agent should read pipeline YAML to explain how revenue is calculated."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "How is revenue calculated in the orders_cleaned transform? "
            "Read the pipeline definition and explain the SQL logic."
        )
        print(f"\n{'='*60}\nRevenue Calculation\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 50
        # Should reference the SQL logic: amount * quantity
        assert any(w in answer.lower() for w in [
            "amount", "quantity", "revenue", "multiply", "product",
        ])

    def test_search_for_revenue(self, qa_project):
        """Agent should use search_pipelines to find where revenue is defined."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Which pipeline defines the 'revenue' column? "
            "Search through the pipeline files to find it."
        )
        print(f"\n{'='*60}\nSearch Revenue\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 30
        assert "orders_cleaned" in answer.lower() or "revenue" in answer.lower()

    def test_combined_pipeline_and_data(self, qa_project):
        """Agent should combine pipeline knowledge with data query results."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        # Turn 1: Ask about data
        a1 = ask(agent, config,
            "What is the total revenue across all orders?"
        )
        print(f"\n{'='*60}\nTurn 1 — Total Revenue\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 20

        # Turn 2: Ask about the pipeline logic
        a2 = ask(agent, config,
            "Now explain how that revenue number was calculated. "
            "Read the pipeline definition that produces the orders_cleaned table."
        )
        print(f"\n{'='*60}\nTurn 2 — Pipeline Explanation\n{'='*60}\n{a2}\n")
        assert a2 and len(a2) > 50
        assert any(w in a2.lower() for w in [
            "amount", "quantity", "multiply", "sql", "transform",
        ])
