"""Tests for answer quality gate."""

from unittest.mock import MagicMock, patch

import pytest

from seeknal.ask.agents.quality import (
    _DATA_PATTERN,
    _EXPLANATION_KEYWORDS,
    check_answer_quality,
)


class TestCheckAnswerQuality:
    def test_empty_answer_fails(self):
        passes, reason = check_answer_quality("")
        assert passes is False
        assert "too short" in reason

    def test_short_answer_fails(self):
        passes, reason = check_answer_quality("No data found.")
        assert passes is False
        assert "too short" in reason

    def test_answer_with_integer_passes(self):
        passes, _ = check_answer_quality(
            "The database contains 42 customers across all regions in the dataset."
        )
        assert passes is True

    def test_answer_with_percentage_passes(self):
        passes, _ = check_answer_quality(
            "Customer retention rate improved to 85% compared to the previous quarter."
        )
        assert passes is True

    def test_answer_with_dollar_amount_passes(self):
        passes, _ = check_answer_quality(
            "Total revenue for Q4 was $1,234,567 representing strong growth."
        )
        assert passes is True

    def test_answer_with_decimal_passes(self):
        passes, _ = check_answer_quality(
            "The average order value across all segments is 3.14 units per customer."
        )
        assert passes is True

    def test_explanation_about_pipeline_passes(self):
        passes, _ = check_answer_quality(
            "The customer data is produced by a pipeline that reads from the raw "
            "CSV source and applies cleaning transformations before loading."
        )
        assert passes is True

    def test_explanation_about_lineage_passes(self):
        passes, _ = check_answer_quality(
            "The data lineage shows that this table is derived from three upstream "
            "sources that are merged in the consolidation step of processing."
        )
        assert passes is True

    def test_explanation_about_transform_passes(self):
        passes, _ = check_answer_quality(
            "The transform step applies aggregation by customer_id and calculates "
            "the running total using a window function for analysis purposes."
        )
        assert passes is True

    def test_vague_answer_without_data_fails(self):
        passes, reason = check_answer_quality(
            "I looked at the data and it seems interesting. There are some patterns "
            "that might be worth exploring further in a deeper analysis session."
        )
        assert passes is False
        assert "lacks specific data" in reason

    def test_generic_non_answer_fails(self):
        passes, reason = check_answer_quality(
            "I wasn't able to determine the answer from the available information. "
            "The data might not contain what we need for this particular analysis."
        )
        assert passes is False
        assert "lacks specific data" in reason


class TestDataPattern:
    """Unit tests for the numeric data regex pattern."""

    def test_matches_plain_integer(self):
        assert _DATA_PATTERN.search("There are 50 customers")

    def test_matches_comma_separated_number(self):
        assert _DATA_PATTERN.search("Revenue is 1,234,567")

    def test_matches_percentage(self):
        assert _DATA_PATTERN.search("Growth rate: 42%")

    def test_matches_dollar_amount(self):
        assert _DATA_PATTERN.search("Cost was $500")

    def test_matches_decimal(self):
        assert _DATA_PATTERN.search("Average is 3.14")

    def test_no_match_on_pure_text(self):
        assert not _DATA_PATTERN.search("no numbers here at all just words")


class TestExplanationKeywords:
    def test_keywords_are_lowercase(self):
        for kw in _EXPLANATION_KEYWORDS:
            assert kw == kw.lower()

    def test_contains_expected_keywords(self):
        assert "pipeline" in _EXPLANATION_KEYWORDS
        assert "lineage" in _EXPLANATION_KEYWORDS
        assert "transform" in _EXPLANATION_KEYWORDS


class TestQualityGateInAsk:
    """Test the quality gate integration in ask()."""

    def test_good_answer_not_retried(self):
        """Answer with data passes quality gate — no extra call."""
        from seeknal.ask.agents.agent import ask

        agent = MagicMock()
        result = MagicMock()
        result.output = (
            "There are 42 customers in the database across all regions "
            "and segments in the current dataset."
        )
        result.all_messages.return_value = []
        agent.run_sync.return_value = result

        answer = ask(agent, MagicMock(), [], "how many customers?")
        assert "42 customers" in answer
        # Only 1 call (initial), no quality retry
        assert agent.run_sync.call_count == 1

    def test_vague_answer_triggers_one_retry(self):
        """Vague answer triggers exactly one quality retry."""
        from seeknal.ask.agents.agent import ask

        agent = MagicMock()
        vague_result = MagicMock()
        vague_result.output = (
            "I looked at the data and it seems interesting. There are some patterns "
            "that might be worth exploring further in a deeper analysis session."
        )
        vague_result.all_messages.return_value = []

        good_result = MagicMock()
        good_result.output = "There are 150 customers with 42 orders each on average."
        good_result.all_messages.return_value = []

        agent.run_sync.side_effect = [vague_result, good_result]

        answer = ask(agent, MagicMock(), [], "summarize the data")
        assert "150" in answer
        # 1 initial + 1 quality retry = 2 calls
        assert agent.run_sync.call_count == 2

    def test_vague_retry_still_vague_returns_anyway(self):
        """If quality retry also fails, return whatever we have (no infinite loop)."""
        from seeknal.ask.agents.agent import ask

        agent = MagicMock()
        vague = MagicMock()
        vague.output = (
            "I looked at the data and it seems interesting. There are some patterns "
            "that might be worth exploring further in a deeper analysis session."
        )
        vague.all_messages.return_value = []

        still_vague = MagicMock()
        still_vague.output = (
            "The data has some interesting characteristics that could be explored "
            "further with additional analysis and deeper investigation of patterns."
        )
        still_vague.all_messages.return_value = []

        agent.run_sync.side_effect = [vague, still_vague]

        answer = ask(agent, MagicMock(), [], "what can you tell me?")
        # Returns the retry answer even though it also doesn't pass
        assert "characteristics" in answer
        # Only 2 calls total (no infinite loop)
        assert agent.run_sync.call_count == 2
