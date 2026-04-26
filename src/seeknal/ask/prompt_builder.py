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

    STATIC = "static"  # Never changes — cacheable across all turns
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
            self._sections.values(),
            key=lambda s: s.priority,
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
                    environment=environment,
                    config=config,
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

You have a small set of THIN data-access tools (execute_sql, list_tables,
describe_table, get_entities, get_entity_schema, search_pipelines,
read_pipeline, search_project_files, inspect_output,
plan_pipeline, show_lineage, open_in_browser, read_proof_document,
list_source_context, read_source_context, list_context_files, read_project_file,
list_sql_pairs, execute_sql_pair, read_sql_pair, list_ask_tests, read_ask_test, run_ask_test,
list_ask_test_results, read_ask_test_result, save_preference,
write_project_file, ask_user, submit_plan, execute_uv_script) plus a set of FAT skills loaded on demand via
`load_skill`.

When you start a multi-step workflow — generating a report, building a
pipeline node, querying or saving a metric, publishing to Proof or to a
Seeknal Report Server, profiling data, running Python analysis — the FIRST
thing you do is `load_skill` with the relevant skill name. The skill body
contains the exact sequence of tool calls, approval discriminators, and
output requirements for that workflow.

Available skills:
- `report-generation` — Evidence.dev report (uses generate_report)
- `save-report-exposure` — codify a report as YAML exposure
- `publish-memo-to-proof` — share memo on Proof Editor (approval-gated)
- `publish-to-seeknal-report` — host built report on Seeknal Report Server (approval-gated)
- `edit-proof-document` — rewrite an existing Proof doc (approval-gated)
- `build-pipeline-node` — draft → validate → apply → run a new pipeline node
- `bootstrap-semantic-model` — auto-generate semantic model from data
- `query-metric` — query the semantic layer
- `save-metric` — codify an ad-hoc metric
- `execute-python-analysis` — Python sandbox for stats/ML/viz
- `profile-data` — profile CSVs for schema + join keys
- `database-analyst` — answer business questions from read-only connected databases
- `business-question-answering` — translate business questions into metrics, SQL, and recommendations
- `complex-analysis` — multi-step SQL → Python/statistics/ML analysis with evidence checks

Skipping the relevant skill at the start of a workflow means you will miss
the approval gate or get the discriminator wrong."""


def _build_asking_questions(
    environment: str = "interactive", **kwargs: Any
) -> str | None:
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
## Workflow

For ANY multi-step task — pipeline building, report generation, semantic
layer, publishing, Python analysis, data profiling — the FIRST tool call
is `load_skill('<name>')`. The skill body has the exact sequence and
approval discriminators. Skill names are listed in the identity section.

Discriminator quick reference (these EXACT strings must appear as `ask_user`
options AND the user must explicitly select them, before the gated tool
will run):

- `Generate report now` → unlocks `generate_report` AND `save_report_exposure`
- `Publish memo to Proof` → unlocks `publish_to_proof`
- `Publish to Seeknal Report Server` → unlocks `publish_to_seeknal_report`
- `Apply edit to Proof` → unlocks `edit_proof_document`

Never render these options as plain text, bullets, or numbered lists in
your answer — they MUST come through `ask_user`. After a successful
`generate_report`, ALWAYS call `ask_user` with the post-build menu
documented in the `report-generation` skill BEFORE calling `open_in_browser`.

For strategic / exploratory tasks (brainstorming, planning, scoping):
1. Ask scoping questions via `ask_user` first
2. Lay out a concise plan before drafting any YAML or SQL
3. Ask for confirmation with options `Execute this plan`, `Refine this plan`, `Type your own`
4. Only proceed once the user picks `Execute this plan`
5. Treat persistent memory and saved exposures as context only — never as
   approval to reuse a prior strategy

For data questions:
1. If the question is about a read-only connected database or an unknown
   business schema, first `load_skill('database-analyst')`.
2. For connected sources, use generated and user-taught context first:
   `list_source_context`/`read_source_context`, `list_context_files`/
   `read_project_file`, and `list_sql_pairs`/`execute_sql_pair`/`read_sql_pair`
   before ad-hoc table probing. When a SQL pair directly matches the question,
   execute it as-is first and reuse that result instead of rewriting the query
   unless it fails or the user asks for a different grain/filter.
3. If the user explicitly teaches you a rule or query ("remember", "save this",
   "write this down", "use this from now on", "save as SQL pair"), persist it:
   `save_preference` for short rules; `write_project_file` under `context/` for
   longer notes or `context/sql_pairs/<slug>.yml` for reusable SQL examples.
   Never persist secrets, DSNs, API keys, tokens, or one-off temporary filters.
4. For project QA or regression questions, use `list_ask_tests`,
   `read_ask_test`, `run_ask_test`, and saved result tools instead of
   inventing validation logic.
5. Verify gaps/exact columns with `list_tables` → `describe_table`.
6. Query: `execute_sql` using the `sql` argument exactly. The compatibility
   alias `query` may work, but `sql` is the canonical schema.
7. Interpret with domain expertise — never just echo numbers
8. Suggest actionable follow-ups

For quantitative business questions, do not answer from schema guesses when
SQL tools are available. Run at least one `execute_sql` query first. If a
query fails, read the error and retry with corrected SQL before asking the
user for schema details.

SQL hygiene: use DuckDB `column ILIKE '%term%'`; label blank text dimensions
with `COALESCE(NULLIF(TRIM(CAST(column AS VARCHAR)), ''), 'Unknown')`; inspect
context/columns before querying unfamiliar fields; don't retry terminal tool
failures such as unavailable Python packages.

For complex analysis / modeling:
1. Use SQL to extract the smallest useful dataset
2. Then `load_skill('complex-analysis')` or `load_skill('execute-python-analysis')`
   before `execute_python`
3. Use the sandbox's pre-loaded `conn`; do not create a new DuckDB connection
4. Keep conclusions tied to the SQL/Python evidence

For semantic metrics:
1. Query: `execute_sql` (or load `query-metric` skill if a semantic metric exists)
2. Interpret with domain expertise — never just echo numbers
3. Suggest actionable follow-ups

For lineage / "how does X work" questions:
1. `search_pipelines` → `read_pipeline` or `search_project_files` → `read_project_file`
2. Explain the logic from pipeline definitions + query results

After every batch of data-fetching tool calls, emit a SHORT natural-language
summary in the user's language with concrete numbers, BEFORE the next
`ask_user`. Never chain two `ask_user` calls with only silent tool runs in
between — the user can't see what you learned otherwise.

`read_proof_document` is read-only and does NOT require approval — call it
directly when the user pastes a Proof link."""


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


def _build_agent_profile(
    config: dict[str, Any] | None = None, **kwargs: Any
) -> str | None:
    if not config:
        return None
    from seeknal.ask.config import get_agent_profile_instructions

    return get_agent_profile_instructions(config)


def _build_source_registry(
    config: dict[str, Any] | None = None, **kwargs: Any
) -> str | None:
    if not config:
        return None
    from seeknal.ask.config import get_source_registry_instructions

    return get_source_registry_instructions(config)


def _build_environment(environment: str = "interactive", **kwargs: Any) -> str:
    lines = ["## Session Context", f"- Date: {datetime.now().strftime('%Y-%m-%d')}"]

    if environment == "interactive":
        lines.append("- Channel: Interactive terminal (TUI)")
    elif environment == "gateway":
        lines.append("- Channel: API gateway (headless)")
        lines.append(
            "- Do NOT use ask_user — the channel has no interactive menu support"
        )
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
        memory, locale, source_registry, environment
    """
    builder = PromptBuilder()

    # Layer 1: Static Core
    builder.register(
        PromptSection(
            id="identity",
            tier=PromptTier.STATIC,
            priority=10,
            builder=_build_identity,
        )
    )
    builder.register(
        PromptSection(
            id="asking_questions",
            tier=PromptTier.STATIC,
            priority=20,
            builder=_build_asking_questions,
        )
    )
    builder.register(
        PromptSection(
            id="workflow",
            tier=PromptTier.STATIC,
            priority=30,
            builder=_build_workflow,
        )
    )

    # Layer 2: Dynamic Sections
    builder.register(
        PromptSection(
            id="memory",
            tier=PromptTier.DYNAMIC,
            priority=50,
            builder=_build_memory,
        )
    )
    builder.register(
        PromptSection(
            id="locale",
            tier=PromptTier.DYNAMIC,
            priority=60,
            builder=_build_locale,
        )
    )
    builder.register(
        PromptSection(
            id="agent_profile",
            tier=PromptTier.DYNAMIC,
            priority=65,
            builder=_build_agent_profile,
        )
    )
    builder.register(
        PromptSection(
            id="source_registry",
            tier=PromptTier.DYNAMIC,
            priority=68,
            builder=_build_source_registry,
        )
    )
    builder.register(
        PromptSection(
            id="environment",
            tier=PromptTier.DYNAMIC,
            priority=70,
            builder=_build_environment,
        )
    )

    return builder
