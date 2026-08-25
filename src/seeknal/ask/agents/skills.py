"""Seeknal Ask skill definitions.

Provides on-demand skills that the agent loads via ``load_skill()`` when needed.
This avoids injecting large instruction blocks (like Evidence.dev report syntax)
into every system prompt — they're loaded only when the agent decides to use them.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Set
from dataclasses import dataclass
from pathlib import Path

from pydantic_deep.toolsets.skills.directory import SkillsDirectory


class SkillCapabilityValidationError(ValueError):
    """Raised when a skill's declared capability frontmatter is invalid."""


@dataclass(frozen=True)
class SkillCapabilities:
    """Tool and action names declared by one Ask skill.

    ``tools`` are non-committing capabilities. ``actions`` are committing
    capabilities that a later pydantic-ai output-tool resolver can offer only
    while this skill is loaded.
    """

    tools: frozenset[str] = frozenset()
    actions: frozenset[str] = frozenset()


_CORE_SKILL_NAME = "core"
_CORE_ACTIONS = frozenset({"visualize", "ask_user"})


def _capability_names(
    skill: Skill,
    field: str,
) -> frozenset[str]:
    """Validate and normalize one optional capability list from metadata."""
    metadata = skill.metadata or {}
    if field not in metadata:
        return frozenset()

    value = metadata[field]
    if not isinstance(value, list):
        raise SkillCapabilityValidationError(
            f"Skill '{skill.name}' has malformed '{field}' frontmatter: "
            "expected a list of non-empty strings."
        )
    if any(not isinstance(name, str) or not name.strip() for name in value):
        raise SkillCapabilityValidationError(
            f"Skill '{skill.name}' has malformed '{field}' frontmatter: "
            "expected a list of non-empty strings."
        )
    return frozenset(value)


def skill_capabilities(skill: Skill) -> SkillCapabilities:
    """Return the validated capability declarations carried by ``skill``."""
    return SkillCapabilities(
        tools=_capability_names(skill, "tools"),
        actions=_capability_names(skill, "actions"),
    )


def get_ask_skill_capabilities(
    skill_directories: Iterable[str | Path],
) -> dict[str, SkillCapabilities]:
    """Load declared capabilities from Ask ``SKILL.md`` directories.

    pydantic-deep parses normal SKILL.md frontmatter and preserves unknown
    fields in ``Skill.metadata``. This adapter gives Seeknal an explicit,
    validated contract for its optional ``tools`` and ``actions`` fields while
    leaving every existing skill's loading behaviour unchanged.

    Directories are processed in order; later definitions replace earlier
    entries of the same skill name, matching Ask's skill-directory precedence.
    """
    capabilities: dict[str, SkillCapabilities] = {}
    for directory in skill_directories:
        skills = SkillsDirectory(path=directory).skills.values()
        for skill in skills:
            capabilities[skill.name] = skill_capabilities(skill)
    return capabilities


def action_capabilities(
    skill_directories: Iterable[str | Path],
) -> dict[str, SkillCapabilities]:
    """Return skill capabilities with DF's always-loaded core actions.

    ``core`` is a built-in baseline rather than a discoverable pydantic-deep
    skill. Keeping it here avoids changing the default skills registry while
    making the opt-in action path faithfully mirror DF's always-on core.
    """
    capabilities = get_ask_skill_capabilities(skill_directories)
    capabilities[_CORE_SKILL_NAME] = SkillCapabilities(actions=_CORE_ACTIONS)
    return capabilities


def derive_loaded_skills(messages: Iterable[object]) -> set[str]:
    """Reconstruct skills whose ``load_skill`` calls returned successfully."""
    from pydantic_ai.messages import ToolCallPart, ToolReturnPart

    loaded = {_CORE_SKILL_NAME}
    requested: dict[str, str] = {}
    for message in messages:
        for part in getattr(message, "parts", ()):
            if isinstance(part, ToolCallPart) and part.tool_name == "load_skill":
                args = part.args
                skill_name = args.get("skill_name", args.get("name")) if isinstance(args, dict) else None
                if isinstance(skill_name, str) and skill_name:
                    requested[part.tool_call_id] = skill_name
            elif isinstance(part, ToolReturnPart) and part.tool_name == "load_skill":
                skill_name = requested.get(part.tool_call_id)
                content = part.content
                if (
                    skill_name
                    and part.outcome == "success"
                    and isinstance(content, str)
                    and f"<name>{skill_name}</name>" in content
                ):
                    loaded.add(skill_name)
    return loaded


def legal_actions_for_loaded_skills(
    loaded_skills: Set[str],
    capabilities: Mapping[str, SkillCapabilities],
) -> frozenset[str]:
    """Return committing actions owned by skills loaded in the current turn.

    This is the framework-neutral equivalent of Data Formulator's
    ``_legal_actions()``. Future pydantic-ai ``prepare_output_tools`` wiring
    should call it per model step; this module deliberately performs no agent
    integration itself.
    """
    legal_actions: set[str] = set()
    for skill_name in loaded_skills:
        capability = capabilities.get(skill_name)
        if capability:
            legal_actions.update(capability.actions)
    return frozenset(legal_actions)


def prepare_action_output_tools(
    capabilities: Mapping[str, SkillCapabilities],
):
    """Build pydantic-ai's per-step output-tool gate for declared actions."""

    async def prepare_output_tools(ctx, definitions):
        legal_actions = legal_actions_for_loaded_skills(
            derive_loaded_skills(ctx.messages), capabilities
        )
        return [definition for definition in definitions if definition.name in legal_actions]

    return prepare_output_tools


def prepare_regular_action_tools(
    capabilities: Mapping[str, SkillCapabilities],
    action_names: Collection[str],
):
    """Build a per-step gate for non-terminal, skill-declared actions."""
    gated_names = frozenset(action_names)

    async def prepare_tools(ctx, definitions):
        legal_actions = legal_actions_for_loaded_skills(
            derive_loaded_skills(ctx.messages), capabilities
        )
        return [
            definition
            for definition in definitions
            if definition.name not in gated_names or definition.name in legal_actions
        ]

    return prepare_tools
