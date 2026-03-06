"""End-to-end QA tests for Seeknal Ask with real Gemini LLM calls.

Requires GOOGLE_API_KEY environment variable.
Run with: uv run pytest tests/ask/qa/test_ask_e2e.py -v -s

Tests three question categories against a realistic e-commerce data pipeline:
- WHAT: Factual data questions (counts, aggregations, listings)
- WHY:  Analytical questions (root cause, explanation, reasoning)
- HOW:  Methodology questions (how data flows, how metrics are computed)
"""

import os
import re
import pytest
from pathlib import Path

# Skip entire module if no API key
pytestmark = pytest.mark.skipif(
    not os.environ.get("GOOGLE_API_KEY"),
    reason="GOOGLE_API_KEY not set — skipping real LLM tests",
)


@pytest.fixture(scope="session")
def agent_and_config(qa_project):
    """Create a real agent with Gemini against the QA project."""
    from seeknal.ask.agents.agent import create_agent

    agent, config = create_agent(
        project_path=qa_project,
        provider="google",
        model="gemini-2.5-flash",
    )
    return agent, config


class TestWhatQuestions:
    """WHAT — Factual data questions that require SQL execution."""

    def test_how_many_customers(self, agent_and_config):
        """Agent should count customers and return ~50."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config, "How many customers are in the dataset?")

        print(f"\n--- WHAT: How many customers? ---\n{answer}\n")
        assert answer and len(answer) > 10
        # Should mention the number 50 somewhere
        assert "50" in answer

    def test_what_categories_exist(self, agent_and_config):
        """Agent should list product/order categories."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config, "What product categories exist in the orders data?")

        print(f"\n--- WHAT: Categories? ---\n{answer}\n")
        assert answer and len(answer) > 10
        # Should find at least some of the categories
        categories_found = sum(
            1 for c in ["Electronics", "Fashion", "Food", "Home", "Sports"]
            if c.lower() in answer.lower()
        )
        assert categories_found >= 3, f"Only found {categories_found} categories in answer"

    def test_what_cities_have_most_customers(self, agent_and_config):
        """Agent should identify cities with most customers."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config,
            "What are the top 3 cities by number of customers?"
        )

        print(f"\n--- WHAT: Top cities? ---\n{answer}\n")
        assert answer and len(answer) > 10
        # Should mention at least one Indonesian city
        cities_found = sum(
            1 for c in ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang"]
            if c in answer
        )
        assert cities_found >= 1, "No Indonesian cities found in answer"


class TestWhyQuestions:
    """WHY — Analytical questions requiring reasoning over data."""

    def test_why_revenue_varies_by_category(self, agent_and_config):
        """Agent should analyze why some categories perform better."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config,
            "Why do some product categories generate more revenue than others? "
            "Show me the data and explain."
        )

        print(f"\n--- WHY: Revenue by category? ---\n{answer}\n")
        assert answer and len(answer) > 50
        # Should contain analytical reasoning, not just numbers
        analysis_indicators = ["revenue", "category", "order"]
        found = sum(1 for w in analysis_indicators if w.lower() in answer.lower())
        assert found >= 2, "Answer lacks analytical content about revenue and categories"

    def test_why_customer_segments_differ(self, agent_and_config):
        """Agent should compare customer segments and explain differences."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config,
            "Why might Premium customers behave differently from Basic customers? "
            "Look at spending patterns and order frequency."
        )

        print(f"\n--- WHY: Segment differences? ---\n{answer}\n")
        assert answer and len(answer) > 50
        # Should mention segments
        assert "premium" in answer.lower() or "basic" in answer.lower() or "segment" in answer.lower()


class TestHowQuestions:
    """HOW — Methodology questions about data pipeline and metrics."""

    def test_how_is_revenue_calculated(self, agent_and_config):
        """Agent should explain how revenue is computed from the data."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config,
            "How is revenue calculated in this dataset? "
            "What columns contribute to it?"
        )

        print(f"\n--- HOW: Revenue calculation? ---\n{answer}\n")
        assert answer and len(answer) > 30
        # Should reference the relevant columns
        relevant_terms = ["amount", "quantity", "revenue", "order"]
        found = sum(1 for t in relevant_terms if t.lower() in answer.lower())
        assert found >= 2, "Answer doesn't explain revenue calculation"

    def test_how_many_orders_were_cancelled(self, agent_and_config):
        """Agent should find cancelled orders and explain the rate."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config,
            "How many orders were cancelled vs completed? "
            "What is the cancellation rate?"
        )

        print(f"\n--- HOW: Cancellation rate? ---\n{answer}\n")
        assert answer and len(answer) > 30
        # Should mention both statuses
        assert "cancel" in answer.lower() or "complet" in answer.lower()

    def test_how_data_pipeline_is_structured(self, agent_and_config):
        """Agent should describe the available data tables and entities."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config,
            "How is this data pipeline structured? "
            "What tables and entities are available for analysis?"
        )

        print(f"\n--- HOW: Pipeline structure? ---\n{answer}\n")
        assert answer and len(answer) > 50
        # Should mention entities or tables
        structure_terms = ["customer", "order", "table", "entity", "transform"]
        found = sum(1 for t in structure_terms if t.lower() in answer.lower())
        assert found >= 2, "Answer doesn't describe pipeline structure"


class TestMultiTurnConversation:
    """Test multi-turn conversation with memory."""

    def test_followup_question(self, qa_project):
        """Agent should remember context from previous question."""
        from seeknal.ask.agents.agent import create_agent, ask

        # Create fresh agent for this test (own thread)
        agent, config = create_agent(
            project_path=qa_project,
            provider="google",
            model="gemini-2.5-flash",
        )

        # First question
        answer1 = ask(agent, config,
            "What is the total revenue from Electronics orders?"
        )
        print(f"\n--- MULTI-TURN Q1 ---\n{answer1}\n")
        assert answer1 and len(answer1) > 10

        # Follow-up referencing previous context
        answer2 = ask(agent, config,
            "How does that compare to Fashion?"
        )
        print(f"\n--- MULTI-TURN Q2 (follow-up) ---\n{answer2}\n")
        assert answer2 and len(answer2) > 10
        # Should reference Fashion (the follow-up category)
        assert "fashion" in answer2.lower() or "Fashion" in answer2


class TestEdgeCases:
    """Edge cases and error handling with real LLM."""

    def test_question_about_nonexistent_data(self, agent_and_config):
        """Agent should gracefully handle questions about missing data."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config,
            "What is the average delivery time for orders?"
        )
        print(f"\n--- EDGE: Missing data ---\n{answer}\n")
        assert answer and len(answer) > 10
        # Should indicate that delivery time data doesn't exist
        # (no delivery_time column in our dataset)

    def test_ambiguous_question(self, agent_and_config):
        """Agent should handle ambiguous questions sensibly."""
        from seeknal.ask.agents.agent import ask

        agent, config = agent_and_config
        answer = ask(agent, config,
            "Show me the top performers"
        )
        print(f"\n--- EDGE: Ambiguous ---\n{answer}\n")
        assert answer and len(answer) > 10
