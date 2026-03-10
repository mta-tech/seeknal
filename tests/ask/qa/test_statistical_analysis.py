"""Statistical analysis QA tests — verify agent can perform complex statistics.

These tests require the agent to compute distributions, correlations,
concentration metrics, outlier detection, and variance decomposition
using DuckDB SQL on the e-commerce dataset.

Run with: GOOGLE_API_KEY=... uv run pytest tests/ask/qa/test_statistical_analysis.py -v -s
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


class TestDistributionAnalysis:
    """Tests that verify the agent can analyze statistical distributions."""

    def test_revenue_distribution_stats(self, qa_project):
        """Agent should compute mean, median, stddev, skewness of revenue."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Analyze the distribution of revenue in orders_cleaned. "
            "Calculate the mean, median, standard deviation, and skewness. "
            "Is the distribution skewed? Use DuckDB statistical functions."
        )
        print(f"\n{'='*60}\nRevenue Distribution\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 80
        assert any(w in answer.lower() for w in [
            "mean", "average", "median", "standard deviation", "stddev",
            "skew", "skewness",
        ])

    def test_percentile_analysis(self, qa_project):
        """Agent should compute percentiles and interpret them."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Calculate the 10th, 25th, 50th, 75th, 90th, and 99th percentiles "
            "of order revenue in orders_cleaned. "
            "What does the spread between p50 and p90 tell us about the distribution?"
        )
        print(f"\n{'='*60}\nPercentile Analysis\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 80
        assert any(w in answer.lower() for w in [
            "percentile", "p50", "p90", "median", "quantile",
        ])


class TestConcentrationAnalysis:
    """Tests for Pareto/concentration analysis on revenue."""

    def test_pareto_analysis(self, qa_project):
        """Agent should identify if 80/20 rule applies to customer revenue."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Perform a Pareto analysis on customer spending. "
            "What percentage of customers account for 80% of total revenue? "
            "Does the 80/20 rule apply here? "
            "Query the customer purchase stats or orders_cleaned table."
        )
        print(f"\n{'='*60}\nPareto Analysis\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 80
        assert any(w in answer.lower() for w in [
            "pareto", "80", "20", "percent", "concentration", "top",
        ])

    def test_gini_coefficient(self, qa_project):
        """Agent should compute a Gini-like concentration metric."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Calculate a Gini coefficient (or similar concentration metric) "
            "for total customer spending. A Gini of 0 means perfect equality, "
            "1 means one customer has all revenue. "
            "You can approximate it using the relative mean absolute difference formula. "
            "Is revenue concentrated among a few customers or spread evenly?"
        )
        print(f"\n{'='*60}\nGini Coefficient\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 60
        assert any(w in answer.lower() for w in [
            "gini", "concentration", "inequality", "equal", "coefficient",
        ])


class TestOutlierDetection:
    """Tests for statistical outlier detection."""

    def test_zscore_outliers(self, qa_project):
        """Agent should detect outliers using Z-scores on order amounts."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Detect outlier orders using Z-scores on the revenue column "
            "in orders_cleaned. An outlier has |Z-score| > 2. "
            "How many outliers are there? What are their characteristics? "
            "Show the SQL you used."
        )
        print(f"\n{'='*60}\nZ-Score Outliers\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 80
        assert any(w in answer.lower() for w in [
            "z-score", "zscore", "z score", "outlier", "standard deviation",
        ])

    def test_iqr_outliers(self, qa_project):
        """Agent should detect outliers using IQR method."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Use the IQR (interquartile range) method to detect outlier orders "
            "by revenue in orders_cleaned. "
            "Calculate Q1, Q3, IQR, then find orders below Q1-1.5*IQR "
            "or above Q3+1.5*IQR. How many outliers exist with each method?"
        )
        print(f"\n{'='*60}\nIQR Outliers\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 80
        assert any(w in answer.lower() for w in [
            "iqr", "interquartile", "q1", "q3", "outlier",
        ])


class TestCorrelationAnalysis:
    """Tests for correlation and relationship analysis."""

    def test_spending_vs_frequency(self, qa_project):
        """Agent should analyze correlation between order count and total spent."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Is there a correlation between how many orders a customer places "
            "and their average order value? Calculate the Pearson correlation "
            "coefficient between total_orders and avg_order_value from "
            "the customer purchase stats. Interpret the result."
        )
        print(f"\n{'='*60}\nSpending vs Frequency\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 80
        assert any(w in answer.lower() for w in [
            "correlation", "corr", "pearson", "relationship",
            "positive", "negative", "weak", "strong",
        ])

    def test_segment_comparison(self, qa_project):
        """Agent should compare statistical measures across customer segments."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)
        answer = ask(agent, config,
            "Compare the Premium, Standard, and Basic customer segments. "
            "For each segment, calculate: count, mean total_spent, "
            "standard deviation of total_spent, and coefficient of variation "
            "(stddev/mean). Which segment has the most variable spending behavior?"
        )
        print(f"\n{'='*60}\nSegment Comparison\n{'='*60}\n{answer}\n")
        assert answer and len(answer) > 80
        assert any(w in answer.lower() for w in [
            "premium", "standard", "basic",
        ])
        assert any(w in answer.lower() for w in [
            "coefficient of variation", "cv", "variab", "deviation",
        ])


class TestMultiTurnStatisticalWorkflow:
    """Multi-turn tests combining statistical techniques progressively."""

    def test_progressive_customer_analysis(self, qa_project):
        """Agent builds up a statistical profile of customers over 3 turns."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        # Turn 1: Basic distribution
        a1 = ask(agent, config,
            "Give me the basic descriptive statistics for customer total_spent: "
            "count, mean, median, min, max, stddev, and the ratio of stddev to mean."
        )
        print(f"\n{'='*60}\nTurn 1 — Descriptive Stats\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        assert any(w in a1.lower() for w in ["mean", "median", "average"])

        # Turn 2: Segmented analysis
        a2 = ask(agent, config,
            "Now break that down by city. Which city has the highest median "
            "total_spent? Which city has the widest spread (largest IQR or stddev)?"
        )
        print(f"\n{'='*60}\nTurn 2 — By City\n{'='*60}\n{a2}\n")
        assert a2 and len(a2) > 50
        assert any(city in a2 for city in [
            "Jakarta", "Surabaya", "Bandung", "Medan", "Semarang",
        ])

        # Turn 3: Combine with pipeline context
        a3 = ask(agent, config,
            "Based on these statistics, which customers would you flag as "
            "high-value outliers (spending > mean + 2*stddev)? "
            "List their IDs and how the revenue they generated was calculated. "
            "Check the pipeline definition for the revenue formula."
        )
        print(f"\n{'='*60}\nTurn 3 — Outlier + Pipeline\n{'='*60}\n{a3}\n")
        assert a3 and len(a3) > 50
        assert any(w in a3.lower() for w in [
            "outlier", "high-value", "customer", "revenue", "amount",
        ])

    def test_category_statistical_deep_dive(self, qa_project):
        """Multi-turn: category performance with advanced statistics."""
        from seeknal.ask.agents.agent import ask

        agent, config = _fresh_agent(qa_project)

        # Turn 1: Rank categories
        a1 = ask(agent, config,
            "Rank the product categories by total revenue. "
            "Also compute each category's share of total revenue as a percentage "
            "and the cumulative percentage (running total of share). "
            "This is a cumulative distribution analysis."
        )
        print(f"\n{'='*60}\nTurn 1 — Category Ranking\n{'='*60}\n{a1}\n")
        assert a1 and len(a1) > 50
        assert any(w in a1.lower() for w in [
            "electronics", "fashion", "food", "home", "sports",
        ])

        # Turn 2: Within-category variance
        a2 = ask(agent, config,
            "For the top 2 categories by revenue, analyze the within-category "
            "order size distribution. Calculate skewness and kurtosis if possible, "
            "or at least the ratio of mean to median (a skewness proxy). "
            "Are large orders driving the category totals, or is it volume?"
        )
        print(f"\n{'='*60}\nTurn 2 — Within-Category Variance\n{'='*60}\n{a2}\n")
        assert a2 and len(a2) > 50
        assert any(w in a2.lower() for w in [
            "skew", "kurtosis", "mean", "median", "distribution",
            "volume", "large order",
        ])
