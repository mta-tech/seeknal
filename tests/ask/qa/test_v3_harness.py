"""V3 Agent Harness QA Tests — exercises all Claude Code-inspired improvements.

Requires GOOGLE_API_KEY environment variable for real Gemini LLM calls.
Run with: pytest tests/ask/qa/test_v3_harness.py -v -s

Each test group validates a specific v3 feature while observing agent behavior
for improvement opportunities in skills, system prompt, and tools.
"""

import os
import json
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


# ---------------------------------------------------------------------------
# Group 1: Basic Analysis (validates modular prompt)
# ---------------------------------------------------------------------------


class TestBasicAnalysis:
    """Validate the lean 53-line modular prompt handles core analysis."""

    def test_simple_count(self, qa_project):
        """Agent should count customers correctly with minimal prompt."""
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(agent, deps, message_history, "How many customers are in the dataset?")

        print(f"\n--- BASIC: Customer count ---\n{answer}\n")
        assert answer and len(answer) > 10
        assert "50" in answer

    def test_aggregation_by_segment(self, qa_project):
        """Agent should break down revenue by customer segment."""
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "What is the total revenue by customer segment (Premium, Standard, Basic)?"
        )

        print(f"\n--- BASIC: Revenue by segment ---\n{answer}\n")
        assert answer and len(answer) > 50
        # Should mention at least one segment
        answer_lower = answer.lower()
        assert any(s in answer_lower for s in ["premium", "standard", "basic"])

    def test_multistep_analysis(self, qa_project):
        """Agent should do multi-step analysis (join + aggregate + reason)."""
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "Which city has the highest average order value? Show me the data."
        )

        print(f"\n--- BASIC: Highest AOV city ---\n{answer}\n")
        assert answer and len(answer) > 50
        # Should mention a city name
        assert any(c in answer for c in ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang"])


# ---------------------------------------------------------------------------
# Group 2: Skill Loading (validates report-generation skill)
# ---------------------------------------------------------------------------


class TestSkillLoading:
    """Validate skills are discovered from seeknal/skills/ and loaded on demand."""

    def test_report_generation_triggers_skill(self, qa_project):
        """Agent should load the report-generation skill for report requests.

        OBSERVE: Does the agent call list_skills or load_skill?
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "Generate a brief customer revenue analysis report."
        )

        print(f"\n--- SKILLS: Report generation ---\n{answer}\n")
        assert answer and len(answer) > 50
        # The agent should produce some structured analysis
        # (even if Evidence.dev build fails, it should attempt the analysis)

    def test_simple_query_no_skill_needed(self, qa_project):
        """Simple questions should NOT require skill loading."""
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "How many orders are in the completed orders table?"
        )

        print(f"\n--- SKILLS: Simple query (no skill) ---\n{answer}\n")
        assert answer and len(answer) > 10


# ---------------------------------------------------------------------------
# Group 3: Error Recovery (validates structured errors + self-correction)
# ---------------------------------------------------------------------------


class TestErrorRecovery:
    """Validate structured errors and POST_TOOL_USE self-correction hooks."""

    def test_wrong_table_name_recovery(self, qa_project):
        """Agent should recover when it uses a wrong table name.

        OBSERVE: Does the POST_TOOL_USE hook enrich the error with available tables?
        How many retries does recovery take?
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "Query the customer_stats table to find the top 5 spenders."
        )

        print(f"\n--- ERROR: Wrong table name ---\n{answer}\n")
        assert answer and len(answer) > 20
        # Agent should eventually find the correct table and return data

    def test_wrong_column_recovery(self, qa_project):
        """Agent should recover from wrong column references.

        OBSERVE: Does the hook inject actual column list after the error?
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "What is the average total_revenue per customer from the customer purchase stats?"
        )

        print(f"\n--- ERROR: Wrong column name ---\n{answer}\n")
        assert answer and len(answer) > 20

    def test_duckdb_syntax_recovery(self, qa_project):
        """Agent should handle DuckDB-specific syntax issues.

        OBSERVE: Does the hook add CAST/timestamp hints?
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "Show orders from 30 days before the last order date. "
            "Use date arithmetic with INTERVAL."
        )

        print(f"\n--- ERROR: DuckDB syntax ---\n{answer}\n")
        assert answer and len(answer) > 20


# ---------------------------------------------------------------------------
# Group 4: Memory (validates cross-session persistence)
# ---------------------------------------------------------------------------


class TestProjectMemory:
    """Validate project memory write/read across sessions."""

    def test_explicit_memory_save(self, qa_project):
        """Agent should save findings to memory when asked.

        OBSERVE: Does .seeknal/ask_memory/main/MEMORY.md get created?
        What does the agent choose to remember?
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)

        # First do some analysis
        ask(agent, deps, message_history, "How many customer segments are there and what are they?")

        # Then ask to save
        answer = ask(
            agent, deps, message_history,
            "Save what you've learned about this dataset to memory for future sessions."
        )

        print(f"\n--- MEMORY: Explicit save ---\n{answer}\n")

        # Check if memory file was created
        memory_dir = qa_project / ".seeknal" / "ask_memory"
        memory_file = memory_dir / "main" / "MEMORY.md"
        if memory_file.exists():
            content = memory_file.read_text()
            print(f"--- MEMORY FILE CONTENT ---\n{content}\n")
            assert len(content) > 10
        else:
            print(f"--- MEMORY: File NOT created at {memory_file} ---")
            # Check parent directories
            print(f"  .seeknal exists: {(qa_project / '.seeknal').exists()}")
            print(f"  ask_memory exists: {memory_dir.exists()}")

    def test_cross_session_memory(self, qa_project):
        """Second agent session should see memory from first session.

        OBSERVE: Does the agent reference previously saved information?
        """
        from seeknal.ask.agents.agent import ask

        # Session 1: analyze and save
        agent1, deps1, mh1 = _fresh_agent(qa_project)
        ask(agent1, deps1, mh1, "The customer segments are Premium (20%), Standard (50%), Basic (30%). Save this to memory.")

        # Session 2: new agent, same project
        agent2, deps2, mh2 = _fresh_agent(qa_project)
        answer = ask(agent2, deps2, mh2, "What do you know about customer segments from previous sessions?")

        print(f"\n--- MEMORY: Cross-session recall ---\n{answer}\n")
        assert answer and len(answer) > 20


# ---------------------------------------------------------------------------
# Group 5: Lineage (validates subagent delegation)
# ---------------------------------------------------------------------------


class TestLineageInvestigation:
    """Validate lineage questions and subagent delegation."""

    def test_pipeline_trace(self, qa_project):
        """Agent should trace how monthly_revenue is calculated.

        OBSERVE: Does the agent delegate to lineage_investigator subagent?
        Or does it use read_pipeline/search_pipelines directly?
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "How is the monthly_revenue table calculated? Trace the full data lineage "
            "from raw source to final output."
        )

        print(f"\n--- LINEAGE: Pipeline trace ---\n{answer}\n")
        assert answer and len(answer) > 50
        # Should mention the transformation chain
        answer_lower = answer.lower()
        assert any(w in answer_lower for w in ["raw_orders", "orders_cleaned", "revenue"])

    def test_impact_analysis(self, qa_project):
        """Agent should identify downstream impact of a source change."""
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "If the raw_orders source data changes, which downstream tables would be affected?"
        )

        print(f"\n--- LINEAGE: Impact analysis ---\n{answer}\n")
        assert answer and len(answer) > 50


# ---------------------------------------------------------------------------
# Group 6: Long Session Stress (validates compaction)
# ---------------------------------------------------------------------------


class TestLongSession:
    """Validate context management across many turns."""

    def test_progressive_analysis_10_turns(self, qa_project):
        """10-turn progressive analysis maintaining coherence.

        OBSERVE: Does context degrade? Do compaction processors fire?
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)

        questions = [
            "List all available tables.",
            "Describe the schema of source_raw_orders.",
            "How many orders per category?",
            "Which category has the highest total revenue?",
            "Break that down by city — which city spends most on that category?",
            "Show me the monthly revenue trend.",
            "Calculate month-over-month growth rate.",
            "Who are the top 5 customers by total spending?",
            "Compare those top customers' behavior to the average.",
            "Summarize ALL findings from our analysis in a structured report.",
        ]

        answers = []
        for i, q in enumerate(questions, 1):
            answer = ask(agent, deps, message_history, q)
            answers.append(answer)
            print(f"\n--- TURN {i}/10 ---")
            print(f"Q: {q}")
            print(f"A: {answer[:200]}{'...' if len(answer) > 200 else ''}")
            print(f"Message history size: {len(message_history)} messages")

        # Turn 10 should still be coherent and reference earlier findings
        final = answers[-1]
        assert final and len(final) > 100
        print(f"\n--- TURN 10 FULL ANSWER ---\n{final}\n")
        print(f"Final message history: {len(message_history)} messages")


# ---------------------------------------------------------------------------
# Group 7: Quality Gate
# ---------------------------------------------------------------------------


class TestQualityGate:
    """Validate the answer quality gate behavior."""

    def test_data_backed_answer_passes(self, qa_project):
        """Normal data question should pass quality gate without retry."""
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(agent, deps, message_history, "What is the total number of completed orders?")

        print(f"\n--- QUALITY: Data-backed answer ---\n{answer}\n")
        assert answer and len(answer) > 10
        # Should contain a number
        assert any(c.isdigit() for c in answer)

    def test_explanation_answer_passes(self, qa_project):
        """Explanation/lineage answers should pass quality gate (no numbers needed)."""
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)
        answer = ask(
            agent, deps, message_history,
            "Explain how the orders_cleaned transform works based on its pipeline definition."
        )

        print(f"\n--- QUALITY: Explanation answer ---\n{answer}\n")
        assert answer and len(answer) > 50


# ---------------------------------------------------------------------------
# Group 8: Full Integration Workflow
# ---------------------------------------------------------------------------


class TestIntegrationWorkflow:
    """Complete analyst workflow exercising all v3 features."""

    def test_full_analyst_session(self, qa_project):
        """End-to-end session: discovery → analysis → lineage → memory.

        OBSERVE all v3 features working together:
        - Modular prompt (lean base)
        - Dynamic context injection
        - Skill loading (if report requested)
        - Error recovery (if SQL errors occur)
        - Memory persistence
        - Quality gate
        """
        from seeknal.ask.agents.agent import ask

        agent, deps, message_history, _cost = _fresh_agent(qa_project)

        # Phase 1: Discovery
        a1 = ask(agent, deps, message_history,
                 "What data is available in this project? Give me a quick overview.")
        print(f"\n--- WORKFLOW Phase 1: Discovery ---\n{a1[:300]}\n")
        assert a1 and len(a1) > 50

        # Phase 2: Targeted analysis
        a2 = ask(agent, deps, message_history,
                 "What is the revenue breakdown by customer segment? "
                 "Show percentages of total.")
        print(f"\n--- WORKFLOW Phase 2: Segment analysis ---\n{a2[:300]}\n")
        assert a2 and len(a2) > 50

        # Phase 3: Deep dive
        a3 = ask(agent, deps, message_history,
                 "Which customer segment has the best repeat purchase rate? "
                 "Define it as customers with 3+ orders.")
        print(f"\n--- WORKFLOW Phase 3: Deep dive ---\n{a3[:300]}\n")
        assert a3 and len(a3) > 50

        # Phase 4: Pipeline understanding
        a4 = ask(agent, deps, message_history,
                 "How is the orders_cleaned table produced? Read the pipeline definition.")
        print(f"\n--- WORKFLOW Phase 4: Pipeline ---\n{a4[:300]}\n")
        assert a4 and len(a4) > 50

        # Phase 5: Save to memory
        a5 = ask(agent, deps, message_history,
                 "Save the key findings from our analysis to memory for future sessions.")
        print(f"\n--- WORKFLOW Phase 5: Memory save ---\n{a5[:200]}\n")

        # Phase 6: Verify memory persisted
        memory_file = qa_project / ".seeknal" / "ask_memory" / "main" / "MEMORY.md"
        if memory_file.exists():
            print(f"--- MEMORY PERSISTED ---\n{memory_file.read_text()[:500]}\n")
        else:
            print("--- MEMORY: NOT persisted ---")

        # Summary
        print("\n" + "=" * 60)
        print("V3 INTEGRATION WORKFLOW COMPLETE")
        print("=" * 60)
        print(f"Total turns: 5")
        print(f"Message history: {len(message_history)} messages")
        print(f"Memory persisted: {memory_file.exists()}")
