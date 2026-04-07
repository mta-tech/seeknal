"""Pipeline Building QA tests — end-to-end pipeline creation with real Gemini LLM.

Tests the agent's ability to profile data, draft pipeline nodes,
validate and apply them, plan execution, and inspect outputs.
Uses the `pipeline_project` fixture which has only raw CSVs —
the agent must build everything from scratch.

Run with: GOOGLE_API_KEY=... uv run pytest tests/ask/qa/test_pipeline_building_qa.py -v -s
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


class TestEndToEndPipelineBuild:
    """Agent builds a complete pipeline from raw CSVs: profile → draft → validate → apply → plan."""

    def test_build_pipeline_from_scratch(self, pipeline_project):
        agent, deps, mh, _cost = fresh_agent(pipeline_project)

        a1 = _ask(agent, deps, mh,
            "Profile the data files in this project using profile_data. "
            "What CSV files are available and what columns do they have? "
            "Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: Profile data ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Create source nodes for the customers and orders CSV files "
            "using draft_node. Don't ask follow-up questions."
        )
        print(f"\n--- Turn 2: Create sources ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Create a transform node called 'customer_orders' that joins "
            "orders with customers on customer_id and calculates "
            "total_amount = amount * quantity."
        )
        print(f"\n--- Turn 3: Create transform ---\n{a3}\n")
        assert_answer_quality(a3)

        a4 = _ask(agent, deps, mh,
            "Validate all draft files using dry_run_draft."
        )
        print(f"\n--- Turn 4: Validate ---\n{a4}\n")
        assert_answer_quality(a4)

        a5 = _ask(agent, deps, mh,
            "Apply all validated drafts to the project."
        )
        print(f"\n--- Turn 5: Apply ---\n{a5}\n")
        assert_answer_quality(a5)

        a6 = _ask(agent, deps, mh,
            "Show the pipeline execution plan."
        )
        print(f"\n--- Turn 6: Pipeline plan ---\n{a6}\n")
        assert_answer_quality(a6)


class TestPipelineModification:
    """Agent reads, modifies, and inspects existing pipeline definitions."""

    def test_read_and_modify_pipeline(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "List all pipeline definitions in this project. "
            "What sources, transforms, and feature groups exist? "
            "Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: List pipelines ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Read the orders_cleaned transform definition. "
            "Show the SQL and explain what it does."
        )
        print(f"\n--- Turn 2: Read transform ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Show the data lineage for this pipeline."
        )
        print(f"\n--- Turn 3: Lineage ---\n{a3}\n")
        assert_answer_quality(a3)

        a4 = _ask(agent, deps, mh,
            "Show a sample of the orders_cleaned output data. How many rows?"
        )
        print(f"\n--- Turn 4: Inspect output ---\n{a4}\n")
        assert_answer_quality(a4)

        a5 = _ask(agent, deps, mh,
            "What tables are available to query after the pipeline has run?"
        )
        print(f"\n--- Turn 5: Available outputs ---\n{a5}\n")
        assert_answer_quality(a5)
