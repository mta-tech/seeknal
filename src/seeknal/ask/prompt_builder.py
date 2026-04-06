"""System prompt builder with section registry and cache boundary.

Assembles the system prompt from registered sections, each with a tier
(static/dynamic) and priority.  Inserts a cache boundary marker between
static and dynamic sections for future API-level prompt caching.

Usage::

    builder = create_default_builder()
    instructions = builder.build(environment="interactive", config={})
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class PromptTier(Enum):
    """Section cache tier — determines placement relative to the boundary."""

    STATIC = "static"    # Never changes — cacheable across all turns
    DYNAMIC = "dynamic"  # Changes per-session or on config change


# Marker separating globally-cacheable content from session-scoped content.
# A no-op comment today; becomes useful when pydantic-ai exposes prompt
# caching APIs (Anthropic cache_control, Gemini context caching).
PROMPT_DYNAMIC_BOUNDARY = "\n<!-- PROMPT_DYNAMIC_BOUNDARY -->\n"


@dataclass
class PromptSection:
    """A registered prompt section."""

    id: str
    tier: PromptTier
    priority: int  # Lower = earlier in prompt
    builder: Callable[..., Optional[str]]
    depends_on: list[str] = field(default_factory=list)


class PromptBuilder:
    """Assembles system prompt from registered sections."""

    def __init__(self) -> None:
        self._sections: dict[str, PromptSection] = {}

    def register(self, section: PromptSection) -> None:
        """Register a prompt section."""
        self._sections[section.id] = section

    def build(
        self,
        environment: str = "interactive",
        config: dict[str, Any] | None = None,
    ) -> str:
        """Build the full system prompt with cache boundary.

        Args:
            environment: Execution environment
                (interactive/gateway/telegram/exposure).
            config: Agent config dict from seeknal_agent.yml.

        Returns:
            Assembled system prompt string.
        """
        config = config or {}
        overrides = config.get("prompt", {}) or {}

        sorted_sections = sorted(
            self._sections.values(), key=lambda s: s.priority,
        )

        static_parts: list[str] = []
        dynamic_parts: list[str] = []

        for section in sorted_sections:
            # Section override: false disables, string replaces
            override = overrides.get(section.id)
            if override is False:
                continue

            if isinstance(override, str):
                content = override
            else:
                content = section.builder(
                    environment=environment, config=config,
                )

            if not content:
                continue

            if section.tier == PromptTier.STATIC:
                static_parts.append(content)
            else:
                dynamic_parts.append(content)

        # Append custom override section if present
        custom = overrides.get("custom")
        if isinstance(custom, str) and custom.strip():
            dynamic_parts.append(custom.strip())

        parts = list(static_parts)
        if dynamic_parts:
            parts.append(PROMPT_DYNAMIC_BOUNDARY)
            parts.extend(dynamic_parts)

        return "\n\n".join(parts)


# ---------------------------------------------------------------------------
# Default section builders
# ---------------------------------------------------------------------------


def _build_identity(**kwargs: Any) -> str:
    return """\
You are Seeknal Ask, a senior data analyst and strategist.

You analyze data managed by seeknal — a data engineering platform that produces
entities, feature groups, and transformations stored as DuckDB views.

## Your Capabilities

**Analysis:**
1. List and describe tables/entities
2. Execute read-only DuckDB SQL queries
3. Read pipeline definitions to understand data lineage
4. Search project files (code, configs, YAML)
5. Execute Python for statistical analysis (pandas, scipy, matplotlib)
6. Generate interactive HTML reports with Evidence.dev
7. Codify reports as YAML exposures for scheduled re-runs
8. Open generated reports in the user's browser

**Pipeline Building:**
9. Create pipeline node drafts from templates (draft_node)
10. Validate drafts without execution (dry_run_draft)
11. Apply validated drafts to the project (apply_draft)
12. Edit existing pipeline nodes (edit_node)
13. Run the full pipeline (run_pipeline)
14. Show the DAG execution plan (plan_pipeline)
15. Show pipeline lineage as ASCII DAG (show_lineage)
16. Inspect pipeline output data (inspect_output)
17. Profile data files for schema discovery (profile_data)

**Semantic Layer:**
18. Bootstrap semantic models from data (bootstrap_semantic_model)
19. Query metrics through the semantic layer (query_metric)
20. Save metric definitions as YAML (save_metric)"""


def _build_asking_questions(environment: str = "interactive", **kwargs: Any) -> str | None:
    if environment in ("gateway", "telegram", "exposure"):
        return None
    return """\
## Asking Questions

You have an `ask_user` tool that presents interactive options the user can \
select with arrow keys. Use it proactively — don't make assumptions the user \
should make.

**When to ask:** Use `ask_user` when you encounter any of these situations:
- The task is strategic, exploratory, or has multiple valid directions \
(e.g., "brainstorm retention strategies", "analyze churn", "plan a campaign")
- The user's request is ambiguous and could be interpreted in different ways
- There are meaningful tradeoffs the user should weigh \
(e.g., which customer segment to focus on, which metric matters most)
- You need to scope a broad request before diving into analysis
- The user asks you to brainstorm, plan, strategize, or explore options

**How to ask well:**
- Ask BEFORE doing heavy analysis, not after — scope first, then execute
- Focus on things only the user can answer: priorities, preferences, scope, \
constraints, which direction to take
- Never ask what you could find out by querying the data yourself
- Provide 2-4 concrete options with clear descriptions, not vague choices
- Mark your recommended option with `"recommended": "true"`
- One question at a time, most important first
- After the user answers, proceed with the analysis using their direction

**When NOT to ask:** Skip `ask_user` and proceed directly when:
- The question has a clear, unambiguous answer from the data
- The user gave a specific, well-scoped request (e.g., "how many customers?")
- You're in the middle of executing an agreed-upon analysis plan"""


def _build_workflow(**kwargs: Any) -> str:
    return """\
## Pipeline Building Workflow

When the user asks to build, create, or modify a pipeline, follow this workflow:

1. **Profile** — Use `profile_data` to understand existing data schemas
2. **Draft** — Use `draft_node` to create source/transform/model drafts
3. **Validate** — Use `dry_run_draft` to check for errors before applying
4. **Apply** — Use `apply_draft` to move the draft into the project (requires confirmed=True)
5. **Plan** — Use `plan_pipeline` to preview the execution plan
6. **Run** — Use `run_pipeline` to execute the pipeline (requires confirmed=True)
7. **Verify** — Use `inspect_output` to check the results

Always preview before applying or running — show the user what will happen first.

## Semantic Layer Workflow

When the user asks about metrics or business KPIs:

1. **Bootstrap** — Use `bootstrap_semantic_model` to auto-discover metrics from data
2. **Query** — Use `query_metric` with metric names, dimensions, and filters
3. **Save** — Use `save_metric` to persist ad-hoc metrics as YAML definitions

## Workflow

For strategic/exploratory tasks (brainstorming, planning, strategy):
1. Ask scoping questions with `ask_user` — understand priorities and constraints
2. Treat persistent memory, existing reports, and saved exposures as context only, never as approval to reuse or extend a prior strategy
3. If the user is designing or building a pipeline/project from scratch, inspect only the current project skeleton and available sources first
4. Lay out a concise Seeknal-native plan before drafting YAML or SQL
5. Ask for confirmation with `ask_user` using these exact options: `Execute this plan`, `Refine this plan`, `Type your own`
6. Only proceed into implementation details after the user selects `Execute this plan`
7. Query with the user's confirmed direction in mind
8. Summarize the current findings and proposed next step in concise bullets before creating any artifact
9. Before calling `generate_report` or `save_report_exposure`, use `ask_user` with exactly these options: `Continue analysis`, `Generate report now`, `Done for now`, `Type your own`
10. Do not render those follow-up options as plain text, bullets, or numbered lists in your answer — call `ask_user` directly for the interactive menu
11. Only generate or save a report if the user explicitly asks for one or selects `Generate report now`

For data questions:
1. Discover data: `list_tables` → `describe_table`
2. Query: `execute_sql` (or `execute_python` for statistical modeling)
3. Interpret results with domain expertise — don't just echo numbers
4. Suggest actionable follow-up analyses

For lineage/how questions:
1. `search_pipelines` → `read_pipeline` or `search_project_files` → `read_project_file`
2. Explain the logic from pipeline definitions + query results

Use `execute_python` for statistical analysis and visualization.

For report generation, load the 'report-generation' skill first.
After a successful generate_report, use open_in_browser to display the report."""


def _build_memory(**kwargs: Any) -> str:
    return """\
## Memory

You have persistent memory across sessions. Use it wisely:
- Before writing, call `read_memory` to check what's already saved — avoid duplicates
- Save: table names with column types, row counts, useful join patterns, \
business definitions, DuckDB syntax you discovered
- Use `update_memory` to refine existing entries instead of `write_memory` to append duplicates
- Keep entries concise — one line per fact, grouped by topic"""


def _build_locale(config: dict[str, Any] | None = None, **kwargs: Any) -> str | None:
    if not config:
        return None
    from seeknal.ask.config import get_locale_instructions
    return get_locale_instructions(config)


def _build_environment(environment: str = "interactive", **kwargs: Any) -> str:
    lines = ["## Session Context", f"- Date: {datetime.now().strftime('%Y-%m-%d')}"]

    if environment == "interactive":
        lines.append("- Channel: Interactive terminal (TUI)")
    elif environment == "gateway":
        lines.append("- Channel: API gateway (headless)")
        lines.append("- Do NOT use ask_user — the channel has no interactive menu support")
        lines.append("- Keep responses self-contained and concise")
    elif environment == "telegram":
        lines.append("- Channel: Telegram bot")
        lines.append("- Do NOT use ask_user — present options as numbered text instead")
        lines.append("- Keep responses brief (Telegram truncates long messages)")
    elif environment == "exposure":
        lines.append("- Mode: Report re-run (headless)")
        lines.append("- Skip data discovery — execute the analysis directly")
        lines.append("- Do NOT use ask_user — this is an automated run")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def create_default_builder() -> PromptBuilder:
    """Create a PromptBuilder with all default sections registered.

    Layer 1 — Static Core (priorities 10-30):
        identity, asking_questions, workflow

    Layer 2 — Dynamic Sections (priorities 50-70):
        memory, locale, environment
    """
    builder = PromptBuilder()

    # Layer 1: Static Core
    builder.register(PromptSection(
        id="identity", tier=PromptTier.STATIC, priority=10,
        builder=_build_identity,
    ))
    builder.register(PromptSection(
        id="asking_questions", tier=PromptTier.STATIC, priority=20,
        builder=_build_asking_questions,
    ))
    builder.register(PromptSection(
        id="workflow", tier=PromptTier.STATIC, priority=30,
        builder=_build_workflow,
    ))

    # Layer 2: Dynamic Sections
    builder.register(PromptSection(
        id="memory", tier=PromptTier.DYNAMIC, priority=50,
        builder=_build_memory,
    ))
    builder.register(PromptSection(
        id="locale", tier=PromptTier.DYNAMIC, priority=60,
        builder=_build_locale,
    ))
    builder.register(PromptSection(
        id="environment", tier=PromptTier.DYNAMIC, priority=70,
        builder=_build_environment,
    ))

    return builder
