"""Tests for capability declarations in Ask skill frontmatter."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic_deep.toolsets.skills.exceptions import SkillValidationError

from seeknal.ask.agents.skills import (
    SkillCapabilityValidationError,
    get_ask_skill_capabilities,
    legal_actions_for_loaded_skills,
)


def _write_skill(root: Path, name: str, frontmatter: str) -> None:
    skill_file = root / name / "SKILL.md"
    skill_file.parent.mkdir(parents=True)
    skill_file.write_text(f"---\n{frontmatter}\n---\n\n# {name}\n", encoding="utf-8")


def test_skill_without_capability_fields_remains_ungated(tmp_path: Path) -> None:
    _write_skill(
        tmp_path,
        "plain-analysis",
        'name: plain-analysis\ndescription: "Plain analysis instructions"\ntags: [analysis]',
    )

    capabilities = get_ask_skill_capabilities([tmp_path])

    assert capabilities["plain-analysis"].tools == frozenset()
    assert capabilities["plain-analysis"].actions == frozenset()
    assert legal_actions_for_loaded_skills({"plain-analysis"}, capabilities) == frozenset()


def test_declared_capabilities_are_exposed_from_skill_frontmatter(tmp_path: Path) -> None:
    _write_skill(
        tmp_path,
        "reporting",
        'name: reporting\ndescription: "Publish a report"\ntools: [execute_sql, list_tables]\nactions: [write_report]',
    )

    capabilities = get_ask_skill_capabilities([tmp_path])

    assert capabilities["reporting"].tools == frozenset({"execute_sql", "list_tables"})
    assert capabilities["reporting"].actions == frozenset({"write_report"})


def test_legal_actions_follow_the_currently_loaded_skills(tmp_path: Path) -> None:
    _write_skill(
        tmp_path,
        "core",
        'name: core\ndescription: "Core actions"\nactions: [visualize]',
    )
    _write_skill(
        tmp_path,
        "reporting",
        'name: reporting\ndescription: "Report actions"\nactions: [write_report]',
    )
    capabilities = get_ask_skill_capabilities([tmp_path])

    assert legal_actions_for_loaded_skills({"core"}, capabilities) == frozenset({"visualize"})
    assert legal_actions_for_loaded_skills({"core", "reporting"}, capabilities) == frozenset(
        {"visualize", "write_report"}
    )


@pytest.mark.parametrize(
    "frontmatter",
    [
        'name: malformed\ndescription: "Bad capability shape"\nactions: write_report',
        'name: malformed\ndescription: "Blank capability"\ntools: [""]',
    ],
)
def test_malformed_capability_frontmatter_fails_loudly(
    tmp_path: Path, frontmatter: str
) -> None:
    _write_skill(tmp_path, "malformed", frontmatter)

    with pytest.raises(SkillCapabilityValidationError, match="malformed"):
        get_ask_skill_capabilities([tmp_path])


def test_invalid_yaml_frontmatter_fails_loudly(tmp_path: Path) -> None:
    _write_skill(
        tmp_path,
        "invalid-yaml",
        'name: invalid-yaml\ndescription: "Bad YAML"\nactions: [write_report',
    )

    with pytest.raises(SkillValidationError, match="Failed to parse YAML frontmatter"):
        get_ask_skill_capabilities([tmp_path])
