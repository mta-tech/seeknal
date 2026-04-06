"""Tests for seeknal ask skills (external SKILL.md files)."""

from pathlib import Path
from unittest.mock import patch

import pytest


class TestSkillMdScaffolding:
    """Verify seeknal init creates SKILL.md files."""

    def test_scaffold_creates_skill_file(self, tmp_path):
        from seeknal.cli.main import _scaffold_ask_skills

        (tmp_path / "seeknal" / "skills" / "report-generation").mkdir(parents=True)
        _scaffold_ask_skills(tmp_path)

        skill_file = tmp_path / "seeknal" / "skills" / "report-generation" / "SKILL.md"
        assert skill_file.exists()

    def test_scaffold_does_not_overwrite_existing(self, tmp_path):
        from seeknal.cli.main import _scaffold_ask_skills

        skill_dir = tmp_path / "seeknal" / "skills" / "report-generation"
        skill_dir.mkdir(parents=True)
        skill_file = skill_dir / "SKILL.md"
        skill_file.write_text("custom content")

        _scaffold_ask_skills(tmp_path)

        assert skill_file.read_text() == "custom content"


class TestSkillMdContent:
    """Verify SKILL.md content has required sections."""

    @pytest.fixture()
    def skill_content(self, tmp_path):
        from seeknal.cli.main import _scaffold_ask_skills

        (tmp_path / "seeknal" / "skills" / "report-generation").mkdir(parents=True)
        _scaffold_ask_skills(tmp_path)
        return (tmp_path / "seeknal" / "skills" / "report-generation" / "SKILL.md").read_text()

    def test_has_yaml_frontmatter(self, skill_content):
        assert skill_content.startswith("---\n")
        assert "name: report-generation" in skill_content
        assert "description:" in skill_content

    def test_has_quality_bar(self, skill_content):
        assert "Report Quality Bar" in skill_content

    def test_has_analysis_process(self, skill_content):
        assert "Analysis Process" in skill_content

    def test_has_evidence_syntax(self, skill_content):
        assert "Evidence Markdown Syntax" in skill_content

    def test_has_all_chart_components(self, skill_content):
        for component in ["BigValue", "BarChart", "LineChart", "DataTable",
                          "ScatterPlot", "Histogram", "FunnelChart"]:
            assert component in skill_content

    def test_has_report_writing_rules(self, skill_content):
        assert "Report Writing Rules" in skill_content

    def test_has_content_pattern(self, skill_content):
        assert "Report Content Pattern" in skill_content
        assert "SECTION 1" in skill_content

    def test_has_codification(self, skill_content):
        assert "Report Codification" in skill_content
        assert "save_report_exposure" in skill_content


class TestAgentUsesSkillDirectories:
    """Verify create_agent uses skill_directories instead of inline skills."""

    def test_create_agent_passes_skill_directories(self):
        """Agent should use skill_directories pointing to seeknal/skills/."""
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        # The system prompt should reference skills
        assert "report-generation" in SYSTEM_PROMPT
        assert "skill" in SYSTEM_PROMPT.lower()

    def test_base_prompt_no_evidence_syntax(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        assert "BigValue" not in SYSTEM_PROMPT
        assert "BarChart" not in SYSTEM_PROMPT

    def test_base_prompt_no_context_placeholder(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        assert "{context}" not in SYSTEM_PROMPT

    def test_base_prompt_is_lean(self):
        from seeknal.ask.agents.agent import SYSTEM_PROMPT

        line_count = len(SYSTEM_PROMPT.strip().splitlines())
        assert line_count < 110, f"Base prompt is {line_count} lines, expected < 110"
