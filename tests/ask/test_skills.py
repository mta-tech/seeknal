"""Tests for seeknal ask skills."""

import pytest

from seeknal.ask.agents.skills import REPORT_SKILL_CONTENT, get_ask_skills


class TestGetAskSkills:
    def test_returns_list(self):
        skills = get_ask_skills()
        assert isinstance(skills, list)
        assert len(skills) >= 1

    def test_report_skill_exists(self):
        skills = get_ask_skills()
        names = [s.name for s in skills]
        assert "report-generation" in names

    def test_report_skill_has_description(self):
        skills = get_ask_skills()
        report_skill = next(s for s in skills if s.name == "report-generation")
        assert report_skill.description
        assert "Evidence.dev" in report_skill.description

    def test_report_skill_has_content(self):
        skills = get_ask_skills()
        report_skill = next(s for s in skills if s.name == "report-generation")
        assert report_skill.content
        assert len(report_skill.content) > 100


class TestReportSkillContent:
    """Verify the skill content preserves all Evidence.dev instructions."""

    def test_contains_quality_bar(self):
        assert "Report Quality Bar" in REPORT_SKILL_CONTENT

    def test_contains_analysis_process(self):
        assert "Analysis Process" in REPORT_SKILL_CONTENT

    def test_contains_evidence_markdown_syntax(self):
        assert "Evidence Markdown Syntax" in REPORT_SKILL_CONTENT

    def test_contains_component_examples(self):
        assert "BigValue" in REPORT_SKILL_CONTENT
        assert "BarChart" in REPORT_SKILL_CONTENT
        assert "LineChart" in REPORT_SKILL_CONTENT
        assert "DataTable" in REPORT_SKILL_CONTENT
        assert "ScatterPlot" in REPORT_SKILL_CONTENT
        assert "Histogram" in REPORT_SKILL_CONTENT
        assert "FunnelChart" in REPORT_SKILL_CONTENT

    def test_contains_report_writing_rules(self):
        assert "Report Writing Rules" in REPORT_SKILL_CONTENT

    def test_contains_report_content_pattern(self):
        assert "Report Content Pattern" in REPORT_SKILL_CONTENT
        assert "SECTION 1" in REPORT_SKILL_CONTENT
        assert "SECTION 5" in REPORT_SKILL_CONTENT

    def test_contains_final_answer_requirements(self):
        assert "Final Answer Requirements" in REPORT_SKILL_CONTENT

    def test_contains_report_codification(self):
        assert "Report Codification" in REPORT_SKILL_CONTENT
        assert "save_report_exposure" in REPORT_SKILL_CONTENT


class TestSystemPromptDoesNotContainReportInstructions:
    """Verify the base prompt no longer has report details."""

    def test_base_prompt_no_evidence_syntax(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        assert "BigValue" not in SYSTEM_PROMPT
        assert "BarChart" not in SYSTEM_PROMPT
        assert "Evidence Markdown Syntax" not in SYSTEM_PROMPT

    def test_base_prompt_no_context_placeholder(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        assert "{context}" not in SYSTEM_PROMPT

    def test_base_prompt_has_skill_reference(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        assert "report-generation" in SYSTEM_PROMPT
        assert "skill" in SYSTEM_PROMPT.lower()

    def test_base_prompt_has_memory_guidance(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        assert "memory" in SYSTEM_PROMPT.lower()

    def test_base_prompt_has_core_sections(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        assert "Your Capabilities" in SYSTEM_PROMPT
        assert "Workflow" in SYSTEM_PROMPT
        assert "DuckDB SQL Rules" in SYSTEM_PROMPT
        assert "Security" in SYSTEM_PROMPT

    def test_base_prompt_is_lean(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        # The base prompt should be significantly shorter than the original ~132 lines
        line_count = len(SYSTEM_PROMPT.strip().splitlines())
        assert line_count < 70, f"Base prompt is {line_count} lines, expected < 70"
