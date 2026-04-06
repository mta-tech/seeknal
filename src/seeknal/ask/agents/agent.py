"""Deep agent for Seeknal Ask.

Uses pydantic-deep (built on pydantic-ai) for planning and auto-summarization.
LLM generates SQL -> execute_sql tool -> hook validates -> error? -> LLM retries.
For complex analyses, the agent decomposes tasks via the planning tool.
"""

import uuid
from pathlib import Path
from typing import Optional

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery

SYSTEM_PROMPT = """\
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
20. Save metric definitions as YAML (save_metric)

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
- You're in the middle of executing an agreed-upon analysis plan

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

For advanced analysis:
1. Query data with `execute_sql` first
2. Use `execute_python` for stats, visualizations, complex pandas ops
3. Pre-loaded: `conn` (DuckDB), `pd`, `np`, `plt`, `sklearn`, `scipy` — \
ONLY these packages are available. For sklearn, import submodules directly \
(e.g., `from sklearn.cluster import KMeans`)
4. Each `execute_python` call is isolated — variables do NOT persist \
between calls. Always re-query data at the start of each call

For report generation, load the 'report-generation' skill first.
After a successful generate_report, use open_in_browser to display the report.

## DuckDB SQL Rules

- Do NOT include trailing semicolons
- Use `CAST('2024-01-01' AS TIMESTAMP)` before INTERVAL arithmetic
- Use `CAST(COUNT(*) AS BIGINT)` for counts
- Use `CAST(SUM(x) AS DOUBLE)` for aggregations
- All non-aggregate SELECT columns must be in GROUP BY
- Struct fields: `column.field`
- Case-insensitive: use `ILIKE`
- In Python code: NEVER put `#` comments inside SQL strings — DuckDB does \
not recognize `#` as a comment. Use `--` for SQL comments, or place `#` \
comments outside the SQL string

## Security

- Only SELECT/WITH queries (read-only)
- Never reference file paths in SQL
- Only query tables from list_tables output

## Error Handling

Tool errors include a JSON structure with 'category' and 'retryable' fields.
For retryable errors, adjust your approach based on the 'hint'.
For terminal errors, explain the limitation to the user.

## Memory

You have persistent memory across sessions. Use it wisely:
- Before writing, call `read_memory` to check what's already saved — avoid duplicates
- Save: table names with column types, row counts, useful join patterns, \
business definitions, DuckDB syntax you discovered
- Use `update_memory` to refine existing entries instead of `write_memory` to append duplicates
- Keep entries concise — one line per fact, grouped by topic
"""


_NO_RESPONSE = "No response generated."

# Ralph Loop: max retries when agent returns tool calls but no final text
_MAX_RALPH_RETRIES = 3

# Diminishing returns: if this many consecutive retries produce fewer than
# _LOW_OUTPUT_THRESHOLD chars each, stop the Ralph Loop early.
_LOW_OUTPUT_RETRIES = 3
_LOW_OUTPUT_THRESHOLD = 500

_DIMINISHING_RETURNS_MSG = (
    "I explored the data but couldn't produce a definitive answer. "
    "Please try rephrasing your question or breaking it into smaller parts."
)


def create_agent(
    project_path: Path,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    style: Optional[str] = None,
    budget: Optional[float] = None,
    include_web: bool = False,
):
    """Create a Seeknal Ask agent.

    Args:
        project_path: Path to the seeknal project root.
        provider: LLM provider ("google" or "ollama").
        model: Model name override.
        api_key: API key override.
        style: Output style name (concise, explanatory, formal, conversational).
        budget: Max USD budget for the session (None = unlimited).
        include_web: Enable web search/fetch tools.

    Returns:
        A tuple of (agent, deps, message_history) where agent is a pydantic-ai
        Agent, deps is DeepAgentDeps, and message_history is the initial
        (empty) conversation history list.
    """
    from pydantic_deep import create_deep_agent, DeepAgentDeps, LocalBackend

    from seeknal.ask.agents.context_toolset import SeeknaContextToolset
    from seeknal.ask.agents.hooks import get_ask_hooks
    from seeknal.ask.agents.providers import get_model_string
    from seeknal.ask.agents.subagents import get_subagent_configs
    from seeknal.ask.agents.tools.ask_user import interactive_ask_user
    from seeknal.ask.agents.tools.toolset import create_ask_toolset
    from seeknal.ask.processors import MicrocompactProcessor, SqlResultCompactor
    from seeknal.ask.security import configure_safe_connection
    from seeknal.cli.repl import REPL

    # Create singleton REPL with safe connection
    repl = REPL(project_path=project_path, skip_history=True)
    configure_safe_connection(repl.conn)

    # Discover project artifacts for context
    discovery = ArtifactDiscovery(project_path)

    # Set tool context (per-session via ContextVar — safe for concurrent sessions)
    session_id = uuid.uuid4().hex[:8]
    set_tool_context(ToolContext(
        repl=repl,
        artifact_discovery=discovery,
        project_path=project_path,
        session_id=session_id,
    ))

    # Load project-level agent config (seeknal_agent.yml)
    import seeknal.ask.config as _ask_config
    from seeknal.ask.config import (
        load_agent_config, get_locale_instructions, get_request_limit,
        get_background_threshold,
    )

    agent_config = load_agent_config(project_path)

    # Set request limit (read by streaming.py at each agent.iter() call)
    _ask_config._active_request_limit = get_request_limit(agent_config)
    # Set background threshold (read by tool functions)
    _ask_config._active_background_threshold = get_background_threshold(agent_config)

    # Build final system prompt: base + locale (if configured)
    instructions = SYSTEM_PROMPT
    locale_snippet = get_locale_instructions(agent_config)
    if locale_snippet:
        instructions = instructions + locale_snippet

    # Resolve model string
    model_string = get_model_string(provider=provider, model=model, api_key=api_key)

    # Build toolsets: ask tools + dynamic project context injection
    context_toolset = SeeknaContextToolset(discovery)
    toolsets_list = [create_ask_toolset(), context_toolset]

    # Add web search toolset if requested (local DuckDuckGo, avoids Gemini builtin conflict)
    if include_web:
        try:
            from pydantic_ai.common_tools.duckduckgo import duckduckgo_search_tool
            from pydantic_ai.toolsets import FunctionToolset

            toolsets_list.append(FunctionToolset(
                tools=[duckduckgo_search_tool()],
                id="seeknal-web",
            ))
        except ImportError:
            import warnings
            warnings.warn(
                "Web search requires 'duckduckgo-search' package. "
                "Install with: pip install duckduckgo-search"
            )

    # Ensure .seeknal directories exist (checkpoints scoped per session)
    seeknal_dir = project_path / ".seeknal"
    seeknal_dir.mkdir(exist_ok=True)
    (seeknal_dir / "plans").mkdir(exist_ok=True)
    checkpoint_dir = seeknal_dir / "checkpoints" / session_id
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Context files: auto-discover SEEKNAL_ASK.md for user-provided instructions
    context_files = []
    ask_md = project_path / "SEEKNAL_ASK.md"
    if ask_md.exists():
        context_files.append(str(ask_md))

    # Cost tracking callback
    _cost_info = {}

    def _on_cost_update(cost_info):
        _cost_info["latest"] = cost_info

    # Create pydantic-deep agent with full feature set
    agent = create_deep_agent(
        model=model_string,
        instructions=instructions,
        toolsets=toolsets_list,
        hooks=get_ask_hooks(),
        # Skills
        include_skills=True,
        skill_directories=[str(project_path / "seeknal" / "skills")],
        # Planning: todo checklist + interactive ask_user via planner subagent
        include_todo=True,
        include_plan=True,
        plans_dir=".seeknal/plans",
        # Context management: 3-tier compaction + user context files
        context_manager=True,
        history_processors=[MicrocompactProcessor(), SqlResultCompactor()],
        context_files=context_files,
        # Memory: persistent schema knowledge across sessions
        include_memory=True,
        memory_dir=".seeknal/ask_memory",
        # Checkpoints: save/rewind in long chat sessions
        include_checkpoints=True,
        checkpoint_frequency="every_turn",
        max_checkpoints=20,
        # Cost tracking
        cost_tracking=True,
        cost_budget_usd=budget,
        on_cost_update=_on_cost_update,
        # Output style
        output_style=style or "concise",
        # Subagents
        include_subagents=True,
        subagents=get_subagent_configs(),
        # Web search: use local DuckDuckGo fallback (not builtin google_search)
        # because Gemini can't mix builtin tools with function tools.
        include_web=False,
        # Disabled: seeknal has domain-specific alternatives
        include_filesystem=False,
        include_execute=False,
    )

    # Use LocalBackend with interactive ask_user callback
    deps = DeepAgentDeps(
        backend=LocalBackend(root_dir=str(project_path)),
        ask_user=interactive_ask_user,
    )
    message_history = []

    return agent, deps, message_history, _cost_info


def ask(
    agent,
    deps,
    message_history: list,
    question: str,
) -> str:
    """Send a question to the agent and get a response (sync).

    Uses the Ralph Loop technique: if the agent ends on tool calls without
    producing a final text response, nudge it to summarize its findings.

    Includes two refinements:
    - **Diminishing returns detection**: if 3+ consecutive retries each
      produce fewer than 500 chars, stop early with a graceful message.
    - **Answer quality gate**: checks the final answer for specific data
      and does one retry if the answer is too vague.

    Args:
        agent: The pydantic-ai Agent.
        deps: DeepAgentDeps instance.
        message_history: Conversation history (mutated in place).
        question: User's natural language question.

    Returns:
        The agent's text response.
    """
    from pydantic_ai.usage import UsageLimits
    from seeknal.ask.config import _active_request_limit

    _usage_limits = UsageLimits(request_limit=_active_request_limit)

    result = agent.run_sync(
        question,
        deps=deps,
        message_history=message_history,
        usage_limits=_usage_limits,
    )
    # Update message history for multi-turn
    message_history.clear()
    message_history.extend(result.all_messages())

    response = result.output or ""
    if response:
        return _quality_gate(agent, deps, message_history, response)

    # Ralph Loop: nudge the agent to produce a text summary
    low_output_streak = 0

    for _ in range(_MAX_RALPH_RETRIES):
        result = agent.run_sync(
            "Please summarize your findings from the tool calls above "
            "and provide your analysis as a text response.",
            deps=deps,
            message_history=message_history,
            usage_limits=_usage_limits,
        )
        message_history.clear()
        message_history.extend(result.all_messages())

        response = result.output or ""
        if response:
            return _quality_gate(agent, deps, message_history, response)

        # Diminishing returns: track low-output retries
        output_chars = len(response)
        if output_chars < _LOW_OUTPUT_THRESHOLD:
            low_output_streak += 1
        else:
            low_output_streak = 0

        if low_output_streak >= _LOW_OUTPUT_RETRIES:
            return _DIMINISHING_RETURNS_MSG

    return _NO_RESPONSE


def _quality_gate(
    agent,
    deps,
    message_history: list,
    answer: str,
) -> str:
    """Check answer quality and retry once if the answer lacks specific data.

    This is output validation — not intent routing. If the answer contains
    numbers or is clearly an explanation, it passes. Otherwise, one retry
    is attempted with guidance to cite data.

    Args:
        agent: The pydantic-ai Agent.
        deps: DeepAgentDeps instance.
        message_history: Conversation history (mutated in place).
        answer: The agent's answer to validate.

    Returns:
        The original answer if it passes, or the retry answer (regardless
        of whether the retry passes — no infinite loop).
    """
    from seeknal.ask.agents.quality import check_answer_quality

    passes, reason = check_answer_quality(answer)
    if passes:
        return answer

    # One quality retry with guidance
    from pydantic_ai.usage import UsageLimits
    from seeknal.ask.config import _active_request_limit

    result = agent.run_sync(
        reason,
        deps=deps,
        message_history=message_history,
        usage_limits=UsageLimits(request_limit=_active_request_limit),
    )
    message_history.clear()
    message_history.extend(result.all_messages())

    retry_answer = result.output or ""
    return retry_answer if retry_answer else answer
