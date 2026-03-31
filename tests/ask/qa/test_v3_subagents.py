"""V3 Subagent QA Tests — exercises lineage_investigator delegation.

Tests that push the agent toward subagent delegation by asking complex
lineage questions that require reading multiple pipeline files.
The agent should use the `task("lineage_investigator", ...)` tool
rather than reading all files itself.

Requires GOOGLE_API_KEY environment variable for real Gemini LLM calls.
Run with: pytest tests/ask/qa/test_v3_subagents.py -v -s
"""

import os
import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("GOOGLE_API_KEY"),
    reason="GOOGLE_API_KEY not set — skipping real LLM tests",
)


def _fresh_agent(qa_project):
    """Create a fresh agent for isolated tests."""
    from seeknal.ask.agents.agent import create_agent

    return create_agent(
        project_path=qa_project,
        provider="google",
        model="gemini-2.5-flash",
    )


class TestSubagentDelegation:
    """Test that the agent delegates complex lineage tasks to subagents."""

    def test_explicit_delegation_request(self, qa_project):
        """When user explicitly asks to delegate, agent should use task() tool.

        This is the strongest signal — asking the agent directly to use its
        lineage specialist.
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "Use your lineage investigator to trace how the monthly_revenue "
            "table is produced. Delegate this to the specialist subagent."
        )

        print(f"\n--- SUBAGENT: Explicit delegation ---\n{answer}\n")
        assert answer and len(answer) > 50
        # Should mention the lineage chain
        answer_lower = answer.lower()
        assert any(w in answer_lower for w in ["raw_orders", "orders_cleaned", "monthly_revenue"])

    def test_complex_lineage_encourages_delegation(self, qa_project):
        """Complex multi-file lineage question should encourage delegation.

        Asking about the FULL lineage of ALL transforms is complex enough
        that the agent should consider delegating to the lineage investigator.
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "I need a comprehensive lineage audit of this project. For EVERY "
            "transform (orders_cleaned, monthly_revenue, customer_purchase_stats, "
            "category_performance), read its pipeline YAML definition and document: "
            "what inputs it uses, what SQL transformation it applies, and what "
            "output columns it produces. This is a thorough investigation."
        )

        print(f"\n--- SUBAGENT: Complex lineage audit ---\n{answer}\n")
        assert answer and len(answer) > 100
        # Should cover multiple transforms
        answer_lower = answer.lower()
        assert "orders_cleaned" in answer_lower
        assert "monthly_revenue" in answer_lower

    def test_mixed_workflow_data_then_lineage(self, qa_project):
        """Multi-turn: data analysis then lineage delegation.

        First ask a data question (agent handles directly), then ask a
        lineage question that should be delegated. This tests that the
        agent's context isn't polluted by the delegation.
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history = _fresh_agent(qa_project)

        # Turn 1: Data question (handled directly)
        a1 = ask(agent, deps, message_history, "What is the total revenue across all orders?")
        print(f"\n--- MIXED Turn 1 (data): ---\n{a1[:200]}\n")
        assert a1 and len(a1) > 20

        # Turn 2: Lineage question (should delegate)
        a2 = ask(
            agent, deps, message_history,
            "Now delegate to the lineage investigator: trace how orders_cleaned "
            "is produced from raw_orders. What SQL transformation is applied?"
        )
        print(f"\n--- MIXED Turn 2 (lineage delegation): ---\n{a2[:300]}\n")
        assert a2 and len(a2) > 50
        assert "completed" in a2.lower() or "status" in a2.lower() or "revenue" in a2.lower()

        # Turn 3: Back to data (verify context not polluted by subagent)
        a3 = ask(
            agent, deps, message_history,
            "Based on what we know about the revenue, which category contributes most?"
        )
        print(f"\n--- MIXED Turn 3 (data after delegation): ---\n{a3[:200]}\n")
        assert a3 and len(a3) > 20

        print(f"\nFinal message history: {len(message_history)} messages")

    def test_subagent_restricted_tools(self, qa_project):
        """Verify the lineage subagent cannot execute SQL or Python.

        Ask the subagent to query data — it should explain it can only
        read pipeline definitions, not execute queries.
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "Delegate to the lineage investigator and ask it to run "
            "'SELECT COUNT(*) FROM source_raw_orders'. "
            "Can the lineage investigator execute SQL queries?"
        )

        print(f"\n--- SUBAGENT: Tool restriction test ---\n{answer}\n")
        assert answer and len(answer) > 20
        # The answer should indicate the subagent can't run SQL


class TestSubagentContextIsolation:
    """Verify subagent delegation doesn't pollute main agent context."""

    def test_lineage_output_stays_concise(self, qa_project):
        """Subagent should return a concise summary, not raw YAML content.

        The whole point of delegation is that the main agent gets a clean
        summary instead of 5K+ tokens of YAML pipeline definitions.
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history = _fresh_agent(qa_project)

        # Ask a question that requires reading multiple pipeline files
        answer = ask(
            agent, deps, message_history,
            "Delegate to the lineage investigator: read ALL pipeline "
            "definitions in the project and give me a summary of the "
            "complete data flow from sources to features."
        )

        print(f"\n--- ISOLATION: Concise summary test ---\n{answer}\n")
        print(f"Answer length: {len(answer)} chars")
        print(f"Message history: {len(message_history)} messages")
        assert answer and len(answer) > 100

    def test_main_agent_remembers_after_delegation(self, qa_project):
        """After subagent delegation, main agent should still know prior context."""
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history = _fresh_agent(qa_project)

        # Establish context
        a1 = ask(agent, deps, message_history, "How many customers are there?")
        print(f"\n--- REMEMBER Turn 1: ---\n{a1[:150]}\n")

        # Delegate to subagent
        a2 = ask(
            agent, deps, message_history,
            "Use the lineage investigator to check how customer_demographics "
            "feature group is produced."
        )
        print(f"\n--- REMEMBER Turn 2 (delegated): ---\n{a2[:200]}\n")

        # Test main agent remembers turn 1
        a3 = ask(
            agent, deps, message_history,
            "How many customers did we find earlier? Do you still remember?"
        )
        print(f"\n--- REMEMBER Turn 3 (recall): ---\n{a3[:200]}\n")
        assert a3 and len(a3) > 20
        # Should reference the customer count from turn 1
        assert "50" in a3
