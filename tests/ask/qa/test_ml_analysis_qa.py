"""Machine Learning Analysis QA tests — multi-turn ML workflows with real Gemini LLM.

Tests the agent's ability to perform clustering, statistical testing,
predictive modeling, and anomaly detection using execute_python with
sklearn, scipy, and pandas on the e-commerce dataset.

Run with: GOOGLE_API_KEY=... uv run pytest tests/ask/qa/test_ml_analysis_qa.py -v -s
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


class TestCustomerSegmentation:
    """Agent performs RFM analysis and K-Means clustering via execute_python."""

    def test_rfm_clustering_workflow(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "I want to do customer segmentation using RFM analysis. "
            "Use execute_python to calculate Recency (days since last purchase "
            "from 2025-01-01), Frequency (total orders), and Monetary (total spend) "
            "per customer. Show the top 10 by monetary value. "
            "Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: RFM calculation ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Cluster the customers into 3 segments using K-Means on the "
            "standardized RFM features. Use sklearn. "
            "Show cluster centers and customer counts per cluster."
        )
        print(f"\n--- Turn 2: K-Means clustering ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Profile each cluster: what characterizes high-value vs "
            "low-value customers?"
        )
        print(f"\n--- Turn 3: Cluster profiles ---\n{a3}\n")
        assert_answer_quality(a3)

        a4 = _ask(agent, deps, mh,
            "List the top 5 customers at risk of churning — those with "
            "high recency who used to be frequent buyers. "
            "Show their customer IDs and RFM values. Don't ask follow-up questions."
        )
        print(f"\n--- Turn 4: Churn risk ---\n{a4}\n")
        assert_answer_quality(a4)


class TestStatisticalTesting:
    """Agent performs hypothesis tests using scipy.stats via execute_python."""

    def test_statistical_tests_workflow(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "Is there a statistically significant difference in order amounts "
            "between Premium and Basic customer segments? "
            "Use scipy.stats t-test. Report the t-statistic and p-value. "
            "Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: T-test ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Run a chi-square test to check whether category preference "
            "depends on customer city. Report the statistic and p-value."
        )
        print(f"\n--- Turn 2: Chi-square ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Calculate the 95% confidence interval for average order value. "
            "Don't ask follow-up questions — compute it directly."
        )
        print(f"\n--- Turn 3: Confidence interval ---\n{a3}\n")
        assert_answer_quality(a3)


class TestPredictiveAnalytics:
    """Agent builds regression models using sklearn via execute_python."""

    def test_regression_workflow(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "Build a linear regression to predict customer total spend "
            "based on age and order count. Use sklearn. Split 80/20. "
            "Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: Build regression ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "What are the model coefficients? Which feature is the strongest "
            "predictor of total spend?"
        )
        print(f"\n--- Turn 2: Feature importance ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "What's the R-squared and RMSE on the test set?"
        )
        print(f"\n--- Turn 3: Model evaluation ---\n{a3}\n")
        assert_answer_quality(a3)

        a4 = _ask(agent, deps, mh,
            "Which customers are predicted to spend more than they actually did? "
            "List the top 5 growth opportunities."
        )
        print(f"\n--- Turn 4: Growth opportunities ---\n{a4}\n")
        assert_answer_quality(a4)


class TestAnomalyDetection:
    """Agent detects outliers and anomalies in the data."""

    def test_anomaly_detection_workflow(self, qa_project):
        agent, deps, mh, _cost = fresh_agent(qa_project)

        a1 = _ask(agent, deps, mh,
            "Find outlier orders by amount using z-scores or IQR with "
            "execute_python. How many outliers? Don't ask follow-up questions."
        )
        print(f"\n--- Turn 1: Outlier detection ---\n{a1}\n")
        assert_answer_quality(a1)

        a2 = _ask(agent, deps, mh,
            "Find customers whose spending significantly deviates from "
            "their segment average. Don't ask follow-up questions."
        )
        print(f"\n--- Turn 2: Customer anomalies ---\n{a2}\n")
        assert_answer_quality(a2)

        a3 = _ask(agent, deps, mh,
            "Summarize all unusual patterns found. "
            "What should the business pay attention to? "
            "Don't ask follow-up questions — just give the summary."
        )
        print(f"\n--- Turn 3: Pattern summary ---\n{a3}\n")
        assert_answer_quality(a3, min_length=50)
