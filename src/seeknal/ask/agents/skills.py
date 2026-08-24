"""Seeknal Ask skill definitions.

Provides on-demand skills that the agent loads via ``load_skill()`` when needed.
This avoids injecting large instruction blocks (like Evidence.dev report syntax)
into every system prompt — they're loaded only when the agent decides to use them.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Set
from dataclasses import dataclass
from pathlib import Path

from pydantic_deep import Skill
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
    """Reconstruct loaded skills from recorded ``load_skill`` tool calls."""
    from pydantic_ai.messages import ToolCallPart

    loaded = {_CORE_SKILL_NAME}
    for message in messages:
        for part in getattr(message, "parts", ()):
            if not isinstance(part, ToolCallPart) or part.tool_name != "load_skill":
                continue
            args = part.args
            if not isinstance(args, dict):
                continue
            skill_name = args.get("skill_name", args.get("name"))
            if isinstance(skill_name, str) and skill_name:
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

REPORT_SKILL_CONTENT = """\
When asked to create a report, dashboard, or visualization, produce a
**professional, insight-driven** Evidence.dev report. Follow this structure:

### Report Quality Bar

A professional report MUST have:
1. **Executive Summary** — 3-4 BigValue KPIs answering the core question up front
2. **Multi-angle Visual Analysis** — Each section explores a DIFFERENT dimension with a DIFFERENT chart type
3. **Data-backed Narrative** — Between every chart, write 1-2 sentences interpreting the data with SPECIFIC numbers ("Premium customers spend 2.3x more per order than Basic" — NOT "Premium customers tend to spend more")
4. **Actionable Recommendations** — Tied to specific data points ("Target Bandung for Premium acquisition — 0 Premium customers despite $1,786 avg spend" — NOT "consider expanding to new markets")

### Analysis Process

BEFORE calling generate_report, you MUST:
1. Run execute_sql to explore ALL relevant tables (not just one)
2. Run at least 3-5 queries covering: aggregates, distributions, cross-table JOINs, rankings, trends
3. Calculate derived metrics in your queries: percentages of total, ratios between segments, rankings
4. Identify the 3 most interesting findings — these become the report's narrative spine

DO NOT generate a report after looking at only one table.
DO NOT write 5 BarCharts of the same query. Each chart must show different data.
DO NOT write generic insights. Every recommendation must cite a specific number from the data.

### Evidence Markdown Syntax

SQL queries in fenced blocks:
```sql query_name
SELECT ... FROM table_name
```

Components (SINGLE curly braces only — never double braces):
- <BigValue data={query_name} value=column_name />
- <BarChart data={query_name} x=column y=column />
- <LineChart data={query_name} x=date_col y=value_col />
- <AreaChart data={query_name} x=date_col y=value_col />
- <DataTable data={query_name} />
- <ScatterPlot data={query_name} x=col1 y=col2 />
- <Histogram data={query_name} x=column bins=20 />
- <FunnelChart data={query_name} name=stage value=count />

### Report Writing Rules

- Name queries descriptively (revenue_by_month, top_customers)
- Use markdown headers (##) to create clear sections
- Write concise analytical commentary between charts — explain WHAT the data shows and WHY it matters
- Do NOT include semicolons in SQL queries
- Use the same table names from list_tables output
- Each chart should have a descriptive title prop
- Use percentage columns, rankings, and comparisons — not just raw counts
- Vary chart types: don't use 5 BarCharts in a row

### Report Content Pattern

Follow this pattern for each section of the report page content:

SECTION 1 — Executive KPIs:
  SQL query → BigValue components (3-4 headline numbers)

SECTION 2 — Primary breakdown:
  SQL query with % of total → BarChart + brief insight text with specific numbers → DataTable

SECTION 3 — Secondary dimension:
  SQL query with different grouping/JOIN → different chart type (LineChart, ScatterPlot, etc.) + insight

SECTION 4 — Cross-analysis:
  SQL query JOINing 2+ tables → stacked/grouped BarChart or heatmap-style DataTable + insight

SECTION 5 — Recommendations:
  Markdown text with specific, data-cited action items (e.g., "Premium segment has 2.3x higher AOV but only 16% of customers — upsell campaigns targeting Standard customers with AOV > $400 could expand this segment by ~30%")

### Final Answer Requirements

After generating a report, your final answer MUST include:
1. A brief summary of the key findings with SPECIFIC numbers
2. The path to the generated HTML report
3. 2-3 actionable recommendations grounded in the data

Do NOT just say "I created a report" — the answer itself should be valuable standalone content.

### Report Codification

After completing analysis, if the user wants to save it as a repeatable report:
- Call save_report_exposure with a snake_case name, distilled prompt, table refs, and format
- Re-run with: seeknal ask report --exposure {name}
"""


def get_ask_skills() -> list[Skill]:
    """Return the list of skills available to the seeknal ask agent.

    Returns:
        List of Skill instances. Currently contains:
        - report-generation: Evidence.dev report instructions
    """
    return [
        Skill(
            name="report-generation",
            description=(
                "Evidence.dev report generation — chart syntax, quality bar, "
                "section patterns, and codification as YAML exposures"
            ),
            content=REPORT_SKILL_CONTENT,
        ),
    ]
