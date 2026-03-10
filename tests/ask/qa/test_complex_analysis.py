"""Complex multi-turn analysis tests with real Gemini LLM calls.

These tests simulate realistic analyst workflows that require:
- Multi-step data exploration with progressive refinement
- Cross-table JOINs and correlated subqueries
- Iterative hypothesis testing (ask → data → refine → ask again)
- Building on previous answers to reach deeper insights

Run with: GOOGLE_API_KEY=... uv run pytest tests/ask/qa/test_complex_analysis.py -v -s
"""

import os
import re
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


class TestCohortAnalysisWorkflow:
    """Simulate an analyst doing RFM-style cohort analysis across 5 turns."""

    def test_rfm_deep_dive(self, qa_project):
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        # Turn 1: Segment customers by purchase frequency
        a1 = ask(agent, config,
            "Segment customers into 3 tiers based on their total number of orders: "
            "High (5+ orders), Medium (3-4 orders), Low (1-2 orders). "
            "Show me the count and average total_spent for each tier."
        )
        print(f"\n{'='*60}\nRFM Turn 1 — Frequency tiers\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        # Should contain tier labels or counts
        assert any(w in a1.lower() for w in ["high", "medium", "low", "tier"])

        # Turn 2: Drill into the High tier
        a2 = ask(agent, config,
            "For the High frequency tier, what are their favorite categories? "
            "Show the distribution."
        )
        print(f"\n{'='*60}\nRFM Turn 2 — High tier categories\n{'='*60}\n{a2}\n")
        assert a2 and len(a2) > 30
        assert any(c.lower() in a2.lower() for c in
                    ["electronics", "fashion", "food", "home", "sports", "category"])

        # Turn 3: Compare recency across tiers
        a3 = ask(agent, config,
            "Now compare the average days_since_last_purchase across those "
            "3 tiers. Which tier has the most recent activity?"
        )
        print(f"\n{'='*60}\nRFM Turn 3 — Recency by tier\n{'='*60}\n{a3}\n")
        assert a3 and len(a3) > 30
        assert "days" in a3.lower() or "recent" in a3.lower()

        # Turn 4: Identify at-risk high-value customers
        a4 = ask(agent, config,
            "Find customers who spent more than $2000 total but haven't "
            "purchased in over 100 days. These are at-risk high-value customers. "
            "List their customer_id, city, total_spent, and days_since_last_purchase."
        )
        print(f"\n{'='*60}\nRFM Turn 4 — At-risk high-value\n{'='*60}\n{a4}\n")
        assert a4 and len(a4) > 20
        # Should show customer IDs or say none found
        assert "C" in a4 or "customer" in a4.lower() or "no " in a4.lower()

        # Turn 5: Synthesize the analysis
        a5 = ask(agent, config,
            "Based on everything we've explored, summarize the key findings "
            "about customer behavior patterns. What actionable recommendations "
            "would you give to the business?"
        )
        print(f"\n{'='*60}\nRFM Turn 5 — Synthesis\n{'='*60}\n{a5}\n")
        assert a5 and len(a5) > 100
        # Should reference previous findings
        assert any(w in a5.lower() for w in ["recommend", "insight", "finding", "action", "suggest"])


class TestCrossTableJoinAnalysis:
    """Tests requiring the agent to JOIN across multiple tables."""

    def test_city_category_revenue_matrix(self, qa_project):
        """Agent must join customers with orders to build a city x category matrix."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        # Turn 1: Build the cross-tabulation
        a1 = ask(agent, config,
            "Create a revenue breakdown by city AND category. "
            "I need a matrix showing total revenue for each city-category pair. "
            "Which city-category combination generates the most revenue?"
        )
        print(f"\n{'='*60}\nCross-Table Turn 1 — City x Category matrix\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        # Should have city and category names
        has_city = any(c in a1 for c in ["Jakarta", "Surabaya", "Bandung", "Medan", "Semarang"])
        has_cat = any(c.lower() in a1.lower() for c in
                      ["electronics", "fashion", "food", "home", "sports"])
        assert has_city and has_cat, "Missing city or category in cross-tabulation"

        # Turn 2: Find concentration risk
        a2 = ask(agent, config,
            "Which city has the most concentrated revenue "
            "(i.e., most revenue comes from a single category)? "
            "Calculate each city's top category as a percentage of that city's total revenue."
        )
        print(f"\n{'='*60}\nCross-Table Turn 2 — Concentration risk\n{'='*60}\n{a2}\n")
        assert a2 and len(a2) > 50
        assert "%" in a2 or "percent" in a2.lower()

        # Turn 3: Identify growth opportunities
        a3 = ask(agent, config,
            "Based on the matrix, which city-category pairs have very low "
            "revenue but high customer counts? These could be growth opportunities."
        )
        print(f"\n{'='*60}\nCross-Table Turn 3 — Growth opportunities\n{'='*60}\n{a3}\n")
        assert a3 and len(a3) > 50


class TestTimeSeriesAnalysis:
    """Tests requiring temporal analysis and trend detection."""

    def test_monthly_trend_and_seasonality(self, qa_project):
        """Agent must analyze monthly trends and identify patterns."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        # Turn 1: Show monthly trend
        a1 = ask(agent, config,
            "Show me the monthly revenue trend for 2024. "
            "Which months had the highest and lowest revenue?"
        )
        print(f"\n{'='*60}\nTimeSeries Turn 1 — Monthly trend\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        assert "2024" in a1 or "month" in a1.lower()

        # Turn 2: Month-over-month growth
        a2 = ask(agent, config,
            "Calculate the month-over-month revenue growth rate. "
            "Which month had the biggest spike or drop?"
        )
        print(f"\n{'='*60}\nTimeSeries Turn 2 — MoM growth\n{'='*60}\n{a2}\n")
        assert a2 and len(a2) > 50
        # Should mention growth/change/increase/decrease
        assert any(w in a2.lower() for w in ["growth", "change", "increase", "decrease", "drop", "spike", "%"])

        # Turn 3: Correlate with order count
        a3 = ask(agent, config,
            "Is the revenue change driven by more orders or higher order values? "
            "Compare the monthly order count trend vs the average order value trend."
        )
        print(f"\n{'='*60}\nTimeSeries Turn 3 — Volume vs Value\n{'='*60}\n{a3}\n")
        assert a3 and len(a3) > 50
        assert any(w in a3.lower() for w in ["order", "value", "volume", "count", "average"])


class TestHypothesisTesting:
    """Agent iteratively tests business hypotheses against the data."""

    def test_new_customer_value_hypothesis(self, qa_project):
        """Test: 'Newer customers spend more per order than older ones.'"""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        # Turn 1: State hypothesis and get initial data
        a1 = ask(agent, config,
            "I have a hypothesis: customers who joined more recently (2023 H2) "
            "have a higher average order value than those who joined earlier (2023 H1). "
            "Can you test this? Split customers by join_date into two cohorts "
            "and compare their avg_order_value."
        )
        print(f"\n{'='*60}\nHypothesis Turn 1 — Test cohort AOV\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        assert any(w in a1.lower() for w in ["cohort", "average", "order", "h1", "h2", "join", "hypothesis"])

        # Turn 2: Check if the difference is meaningful
        a2 = ask(agent, config,
            "Is that difference statistically meaningful or just noise? "
            "How many customers and orders are in each cohort? "
            "Show the sample sizes and standard deviation if possible."
        )
        print(f"\n{'='*60}\nHypothesis Turn 2 — Sample sizes\n{'='*60}\n{a2}\n")
        assert a2 and len(a2) > 50

        # Turn 3: Explore confounding factors
        a3 = ask(agent, config,
            "Could the difference be explained by segment mix? "
            "Check if the newer cohort has more Premium customers than the older one."
        )
        print(f"\n{'='*60}\nHypothesis Turn 3 — Confounding segment mix\n{'='*60}\n{a3}\n")
        assert a3 and len(a3) > 50
        assert "premium" in a3.lower() or "segment" in a3.lower()

        # Turn 4: Conclude
        a4 = ask(agent, config,
            "Given all the evidence, should we accept or reject the hypothesis "
            "that newer customers spend more per order? Give me your verdict "
            "with supporting data."
        )
        print(f"\n{'='*60}\nHypothesis Turn 4 — Verdict\n{'='*60}\n{a4}\n")
        assert a4 and len(a4) > 50
        assert any(w in a4.lower() for w in ["accept", "reject", "evidence", "conclusion", "verdict", "support"])


class TestComplexSQLGeneration:
    """Tests that force the agent to generate non-trivial SQL."""

    def test_percentile_and_ranking(self, qa_project):
        """Agent must use window functions for percentile ranking."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        a1 = ask(agent, config,
            "Rank all customers by total_spent and show their percentile. "
            "Who are the top 10% spenders? Show their customer_id, city, "
            "segment, total_spent, and percentile rank."
        )
        print(f"\n{'='*60}\nComplex SQL — Percentile ranking\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        # Should show customer data with rankings
        assert "C" in a1  # customer IDs start with C
        assert any(w in a1.lower() for w in ["percentile", "rank", "top", "10%", "percent"])

    def test_running_total_and_cumulative(self, qa_project):
        """Agent must compute a running total of monthly revenue."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        a1 = ask(agent, config,
            "Show the cumulative (running total) revenue by month for 2024. "
            "I want to see each month's revenue and the running total up to that month."
        )
        print(f"\n{'='*60}\nComplex SQL — Running total\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        assert any(w in a1.lower() for w in ["cumulative", "running", "total"])

    def test_customer_retention_cohort(self, qa_project):
        """Agent must build a retention-style analysis with JOINs and date logic."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        a1 = ask(agent, config,
            "For customers who made their first purchase in Q1 2024, how many "
            "of them made a repeat purchase in Q2 2024? What is the retention rate "
            "from Q1 to Q2?"
        )
        print(f"\n{'='*60}\nComplex SQL — Retention cohort\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        assert any(w in a1.lower() for w in ["retention", "repeat", "q1", "q2", "%", "rate"])

        # Follow-up: extend to Q3, Q4
        a2 = ask(agent, config,
            "Extend that to Q3 and Q4 as well. Show the full retention curve "
            "for the Q1 2024 cohort across all quarters."
        )
        print(f"\n{'='*60}\nComplex SQL — Full retention curve\n{'='*60}\n{a2}\n")
        assert a2 and len(a2) > 50
