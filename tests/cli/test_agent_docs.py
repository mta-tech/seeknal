"""Tests for CLAUDE.md and AGENTS.md generation."""

from pathlib import Path
from unittest.mock import patch

import pytest

from seeknal.cli.main import _generate_agent_docs, _generate_agent_docs_force, _generate_skills, _generate_skills_force


@pytest.fixture
def project_dir(tmp_path):
    """Create a minimal project directory."""
    (tmp_path / "seeknal_project.yml").write_text("name: test_project")
    return tmp_path


@pytest.fixture
def project_with_profiles(tmp_path):
    """Create a project directory with profiles.yml."""
    (tmp_path / "seeknal_project.yml").write_text("name: test_project")
    (tmp_path / "profiles.yml").write_text(
        "connections:\n  local_pg:\n    type: postgresql\n  my_iceberg:\n    type: iceberg\n"
    )
    return tmp_path


class TestGenerateAgentDocs:
    def test_creates_claude_md(self, project_dir):
        _generate_agent_docs(project_dir, "test_project")
        assert (project_dir / "CLAUDE.md").exists()
        content = (project_dir / "CLAUDE.md").read_text()
        assert "test_project" in content
        assert "seeknal" in content.lower()

    def test_creates_agents_md(self, project_dir):
        _generate_agent_docs(project_dir, "test_project")
        assert (project_dir / "AGENTS.md").exists()
        content = (project_dir / "AGENTS.md").read_text()
        assert "test_project" in content

    def test_skips_existing_files(self, project_dir):
        (project_dir / "CLAUDE.md").write_text("custom content")
        _generate_agent_docs(project_dir, "test_project")
        # Should not overwrite
        assert (project_dir / "CLAUDE.md").read_text() == "custom content"

    def test_includes_connections(self, project_with_profiles):
        _generate_agent_docs(project_with_profiles, "test_project")
        content = (project_with_profiles / "CLAUDE.md").read_text()
        assert "local_pg" in content
        assert "my_iceberg" in content


class TestGenerateAgentDocsForce:
    def test_overwrites_existing(self, project_dir):
        (project_dir / "CLAUDE.md").write_text("old content")
        _generate_agent_docs_force(project_dir, "test_project")
        content = (project_dir / "CLAUDE.md").read_text()
        assert content != "old content"
        assert "test_project" in content


class TestGenerateSkills:
    def test_creates_skill_directories(self, project_dir):
        _generate_skills(project_dir, "test_project")
        skills_dir = project_dir / ".claude" / "skills"
        assert skills_dir.exists()

        # Check at least some skills were created
        expected = ["seeknal-pipeline", "seeknal-feature-group", "seeknal-model"]
        for name in expected:
            skill_file = skills_dir / name / "SKILL.md"
            assert skill_file.exists(), f"Missing skill: {name}"

    def test_skips_existing_skills(self, project_dir):
        skill_dir = project_dir / ".claude" / "skills" / "seeknal-pipeline"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("custom skill")

        _generate_skills(project_dir, "test_project")
        # Should not overwrite
        assert (skill_dir / "SKILL.md").read_text() == "custom skill"

    def test_all_eight_skills_created(self, project_dir):
        _generate_skills(project_dir, "test_project")
        skills_dir = project_dir / ".claude" / "skills"

        expected_skills = [
            "seeknal-pipeline", "seeknal-feature-group", "seeknal-model",
            "seeknal-analytics", "seeknal-rules", "seeknal-deploy",
            "seeknal-init", "seeknal-debug",
        ]
        for name in expected_skills:
            skill_file = skills_dir / name / "SKILL.md"
            assert skill_file.exists(), f"Missing skill: {name}"
            content = skill_file.read_text()
            assert len(content) > 50, f"Skill {name} is too short"


class TestGenerateSkillsForce:
    def test_overwrites_existing(self, project_dir):
        skill_dir = project_dir / ".claude" / "skills" / "seeknal-pipeline"
        skill_dir.mkdir(parents=True)
        (skill_dir / "SKILL.md").write_text("old content")

        _generate_skills_force(project_dir, "test_project")
        content = (skill_dir / "SKILL.md").read_text()
        assert content != "old content"
        assert "seeknal" in content.lower()
