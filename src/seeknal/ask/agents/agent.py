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


# System prompt is now assembled by prompt_builder.py via create_default_builder().
# This module-level reference is populated for backward compatibility with code
# that imports SYSTEM_PROMPT directly.
def _build_default_prompt() -> str:
    from seeknal.ask.prompt_builder import create_default_builder

    return create_default_builder().build()


SYSTEM_PROMPT: str = _build_default_prompt()


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

# Shipped alongside the package — each subdirectory here is a SKILL.md
# bundle that pydantic-deep's SkillsToolset will auto-discover.
_BUILTIN_SKILLS_DIR = Path(__file__).parent.parent / "builtin_skills"


def _resolve_skill_directories(project_path: Path) -> list[str]:
    """Return the ordered skill search path for the ask agent.

    Built-ins come first so they're always discoverable; project-local
    skills are appended so users can extend or override.
    """
    dirs: list[str] = []
    if _BUILTIN_SKILLS_DIR.exists():
        dirs.append(str(_BUILTIN_SKILLS_DIR))
    dirs.append(str(project_path / "seeknal" / "skills"))
    return dirs


def _build_connected_source_context(
    repl,
    *,
    max_tables: int = 20,
    max_columns: int = 16,
) -> str | None:
    """Return a compact attached-source schema snapshot for analyst mode.

    This is not a separate data pipeline or materialized context-sync step. It
    is a per-session read-only schema hint so the model starts with the same
    basic database awareness a human analyst would get from a catalog browser.
    """
    attached = sorted(getattr(repl, "attached", set()) or [])
    if not attached:
        return None

    lines: list[str] = [
        "## Connected Source Schema Snapshot",
        "",
        "Queryable attached database tables discovered at session start:",
    ]
    tables_seen = 0

    for source_name in attached:
        try:
            sql = f"""
                SELECT table_schema, table_name, table_type
                FROM "{source_name}".information_schema.tables
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name
            """
            _columns, rows = repl.execute_oneshot(sql, limit=max_tables + 1)
        except Exception as exc:  # noqa: BLE001 - prompt context is best-effort
            lines.append(f"- {source_name}: schema discovery failed ({exc})")
            continue

        for schema, table, table_type in rows:
            if tables_seen >= max_tables:
                lines.append(f"- ... ({max_tables} table context limit reached)")
                return "\n".join(lines)

            qualified = f"{source_name}.{schema}.{table}"
            tables_seen += 1
            lines.append(f"- `{qualified}` ({table_type or 'table'})")

            try:
                col_sql = f"""
                    SELECT column_name, data_type
                    FROM "{source_name}".information_schema.columns
                    WHERE table_schema = '{str(schema).replace("'", "''")}'
                      AND table_name = '{str(table).replace("'", "''")}'
                    ORDER BY ordinal_position
                """
                _col_headers, col_rows = repl.execute_oneshot(
                    col_sql,
                    limit=max_columns + 1,
                )
            except Exception:  # noqa: BLE001 - skip per-table detail on failure
                continue

            rendered_cols = [
                f"{col_name} {data_type}"
                for col_name, data_type in col_rows[:max_columns]
            ]
            if rendered_cols:
                suffix = " ..." if len(col_rows) > max_columns else ""
                lines.append(f"  columns: {', '.join(rendered_cols)}{suffix}")

    if tables_seen == 0:
        return None
    return "\n".join(lines)


def _build_generated_source_context_index(
    project_path: Path,
    *,
    max_files: int = 60,
) -> str | None:
    """Return a compact index of generated source-context files."""
    root = project_path / ".seeknal" / "context" / "sources"
    if not root.exists():
        return None

    supported = {".md", ".yml", ".yaml", ".jsonl", ".txt"}
    priority = {
        "SOURCE.md": 0,
        "relationships.md": 1,
        "overview.md": 2,
        "columns.md": 3,
        "profiling.md": 4,
        "preview.md": 5,
    }
    files = [
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in supported
    ]
    if not files:
        return None

    files.sort(key=lambda p: (priority.get(p.name, 9), p.relative_to(root).as_posix()))
    lines = [
        "## Generated Source Context Index",
        "",
        "Use these files before ad-hoc schema probing when answering connected-source questions:",
    ]
    for path in files[:max_files]:
        rel = path.relative_to(root).as_posix()
        lines.append(f"- `{rel}`")
    if len(files) > max_files:
        lines.append(
            f"- ... {len(files) - max_files} more files; use `list_source_context` to narrow"
        )
    return "\n".join(lines)


def create_agent(
    project_path: Path,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    style: Optional[str] = None,
    budget: Optional[float] = None,
    include_web: bool = False,
    environment: str = "interactive",
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
        environment: Execution environment — "interactive" (TTY), "gateway"
            (API/SSE), "telegram", or "exposure" (headless report re-run).

    Returns:
        A tuple of (agent, deps, message_history, cost_info).
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
    from seeknal.ask.config import (
        load_agent_config,
        get_request_limit,
        get_background_threshold,
        get_context_budget,
        get_ask_toolset_mode,
    )

    # Load project-level agent config (seeknal_agent.yml)
    agent_config = load_agent_config(project_path)
    # Internal-only prompt helper metadata. The key is deliberately namespaced
    # to avoid conflicting with user-facing seeknal_agent.yml fields.
    agent_config = dict(agent_config)
    agent_config["__project_path"] = str(project_path)

    # Create singleton REPL with safe connection
    repl = REPL(project_path=project_path, skip_history=True)
    configure_safe_connection(repl.conn)

    # Discover project artifacts for context
    discovery = ArtifactDiscovery(project_path)

    # Set tool context (per-session via ContextVar — safe for concurrent sessions)
    session_id = uuid.uuid4().hex[:8]
    ask_toolset_mode = get_ask_toolset_mode(agent_config)
    analysis_toolset = ask_toolset_mode == "analysis"

    set_tool_context(
        ToolContext(
            repl=repl,
            artifact_discovery=discovery,
            project_path=project_path,
            session_id=session_id,
            disable_quality_gate=analysis_toolset,
        )
    )

    # Set request limit and background threshold on the per-session ToolContext
    # (NOT on module-level globals — avoids race conditions across concurrent sessions)
    from seeknal.ask.agents.tools._context import get_tool_context

    tool_ctx = get_tool_context()
    tool_ctx.request_limit = get_request_limit(agent_config)
    tool_ctx.background_threshold = get_background_threshold(agent_config)
    if analysis_toolset:
        tool_ctx.tool_call_limit = 24

    # Build final system prompt via section registry (4-layer architecture)
    from seeknal.ask.prompt_builder import create_default_builder

    builder = create_default_builder()
    instructions = builder.build(environment=environment, config=agent_config)
    if analysis_toolset:
        instructions += """

## Read-only connected-source mode

This session is optimized for an existing read-only database source. The
available Seeknal Ask tools are deliberately narrow: `list_tables`,
`describe_table`, `execute_sql`, `preview_query`, generated source-context
read tools, SQL-pair read tools, project-memory tools, and `execute_python`.

- Do not look for pipeline-building, publishing, semantic-artifact, external-write,
  or database-write tools in this mode. The only write tools intentionally
  available are project-local memory tools (`save_preference` and
  `write_project_file`) for explicit user teaching.
- Do not ask the user to correct tool arguments. If a table/query fails, use
  `list_tables`/`describe_table` or the database error suggestion and retry.
- For domain-specific SQL patterns, relationships, profiling, or generated
  table docs, use `list_source_context`/`read_source_context`,
  `list_context_files`/`read_project_file`, and `list_sql_pairs`/
  `execute_sql_pair`/`read_sql_pair` instead of guessing or hardcoding.
- Teach mode: when the user explicitly says "remember", "write this down",
  "save this", "use this from now on", or "save as a SQL pair", persist that
  instruction as project-local memory. Use `save_preference` for one-sentence
  rules; use `write_project_file` under `context/` for longer notes, glossaries,
  join patterns, or `context/sql_pairs/<slug>.yml` reusable SQL examples. Do
  not save secrets, passwords, DSNs, API keys, tokens, or temporary one-off
  instructions.
- When saving a SQL pair, validate the supplied SQL with `preview_query` or
  `execute_sql` when practical, include the prompt/intent/sql/notes fields,
  and tell the user the exact file path.
- For project QA, use `list_ask_tests`/`read_ask_test` and `run_ask_test`
  before inventing validation logic. Ask tests are executable QA oracles;
  SQL pairs are examples for steering, not regression tests.
- For business questions over a connected source, prefer the context-first
  path: `list_source_context(query='<business term or table clue>')` and
  `list_sql_pairs(query='<business term>')`, execute the relevant SQL pair
  with `execute_sql_pair` when the pair directly matches the question, and
  read the relevant `SOURCE.md`,
  `relationships.md`, `columns.md`, `profiling.md`, context note, or SQL pair,
  then use `list_tables`/`describe_table` only for gaps or verification. Do not
  brute-force unrelated tables when generated or user-taught context already
  narrows the surface.
- Treat project-owned SQL pairs as authoritative examples: execute a matching
  pair as-is first, do not rewrite it unless execution fails or the user asks
  for a different filter/grain, and reuse successful query results instead of
  rerunning the same or near-identical SQL.
- DuckDB dialect: use `col ILIKE '%text%'`, not `ILIKE(col, '%text%')`.
  For text dimensions that may contain blanks, normalize with
  `NULLIF(TRIM(CAST(col AS VARCHAR)), '')` before `COALESCE`, grouping, or
  labeling.
- Treat opaque coded dimensions as codes unless a data dictionary, source
  context file, or query result provides the mapping. Do not infer human labels
  for numeric/string codes from general domain knowledge; answer with
  `code=<value>` and note that the label mapping was not found. A code label
  is valid only when the dictionary category/source matches the exact field
  being analyzed; do not reuse labels from another category that happens to
  share the same code value.
- If a tool reports a terminal/unavailable dependency, do not retry the same
  failed tool path. Fall back to a text/table answer using the evidence already
  collected.
- Keep read-only business answers bounded: after roughly 12-20 useful discovery/
  SQL/Python tool calls, stop exploring and answer with caveats. Do not create
  charts or run Python unless the user asks for modeling/visualization or SQL
  cannot express the needed analysis. A request for a "trend" is a table/text
  analysis request, not an implicit chart request; do not announce or attempt a
  visualization unless the user explicitly asks for a chart, plot, dashboard,
  report, or visual.
- After a tool result, answer in natural language with concrete values. Do not
  output raw JSON tool-call text as the final answer.
- For direct tool-call requests, copy the supplied SQL/table/code exactly
  unless the tool error proves it needs correction.
"""
        connected_context = _build_connected_source_context(repl)
        if connected_context:
            instructions += f"\n\n{connected_context}"
        generated_context_index = _build_generated_source_context_index(project_path)
        if generated_context_index:
            instructions += f"\n\n{generated_context_index}"

    # Inject persistent user preferences from preferences.yml (if present)
    # so they carry across sessions. See save_preference tool for writes.
    from seeknal.ask.agents.tools.save_preference import load_preferences

    _prefs = load_preferences(project_path)
    if _prefs:
        _pref_block = (
            "\n\n## User preferences (persistent)\n\n"
            + "\n".join(f"- {p}" for p in _prefs)
            + (
                "\n\nThese were saved via save_preference in earlier sessions. "
                "Apply them unless the user overrides in this session."
            )
        )
        instructions = instructions + _pref_block

    # Resolve model string
    model_string = get_model_string(provider=provider, model=model, api_key=api_key)

    # Build toolsets: ask tools + dynamic project context injection
    context_budget = get_context_budget(agent_config)
    context_toolset = SeeknaContextToolset(discovery, context_budget=context_budget)
    toolsets_list = [
        create_ask_toolset(
            mode=ask_toolset_mode,
            include_ask_user=(environment == "interactive" and not analysis_toolset),
        ),
        context_toolset,
    ]

    # Add web tools if requested. Prefers Firecrawl (search + scrape) when
    # FIRECRAWL_API_KEY is set, otherwise falls back to DuckDuckGo search-only.
    if include_web:
        import os
        from pydantic_ai.toolsets import FunctionToolset

        if os.environ.get("FIRECRAWL_API_KEY"):
            from seeknal.ask.agents.tools.web_firecrawl import web_scrape, web_search

            toolsets_list.append(
                FunctionToolset(
                    tools=[web_search, web_scrape],
                    id="seeknal-web",
                )
            )
        else:
            try:
                from pydantic_ai.common_tools.duckduckgo import duckduckgo_search_tool

                toolsets_list.append(
                    FunctionToolset(
                        tools=[duckduckgo_search_tool()],
                        id="seeknal-web",
                    )
                )
            except ImportError:
                import warnings

                warnings.warn(
                    "Web search requires 'ddgs' package or FIRECRAWL_API_KEY. "
                    "Install with: pip install ddgs"
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

    # Detect pydantic-deep API version for web/thinking capability params.
    # v0.3.1 uses `include_web` (default False).
    # v0.3.4+ uses `web_search`, `web_fetch`, `thinking` (default True).
    # Google cannot mix function tools with built-in tools, so disable them.
    import inspect

    _sig = inspect.signature(create_deep_agent)
    _web_kwargs: dict = {}
    if "web_search" in _sig.parameters:
        # v0.3.4+: disable built-in capabilities (breaks Google provider)
        _web_kwargs = {"web_search": False, "web_fetch": False, "thinking": False}

    # Create pydantic-deep agent with full feature set
    agent = create_deep_agent(
        model=model_string,
        instructions=instructions,
        toolsets=toolsets_list,
        hooks=get_ask_hooks(),
        # Connected-source analysis is safer and more user-legible when tool
        # calls are sequential. It lets harness state from an authoritative
        # SQL pair stop follow-up drift before sibling tool calls run, while
        # leaving the full/default Seeknal Ask mode unchanged.
        max_concurrency=1 if analysis_toolset else None,
        **_web_kwargs,
        # Skills: bundled built-ins (report-generation, etc.) + per-project
        # user skills. Built-ins ship as SKILL.md files under
        # src/seeknal/ask/builtin_skills/ so `load_skill(...)` resolves
        # even in projects that have no local seeknal/skills/ dir.
        include_skills=True,
        skill_directories=_resolve_skill_directories(project_path),
        # Planning: todo checklist + interactive ask_user via planner subagent
        include_todo=not analysis_toolset,
        include_plan=(environment == "interactive" and not analysis_toolset),
        plans_dir=".seeknal/plans",
        # Context management: 3-tier compaction + user context files
        context_manager=not analysis_toolset,
        # Keep zero-cost history compaction active in read-only analysis mode
        # too. Connected-source chats often accumulate many SQL result tables;
        # without compaction, a simple follow-up can resend the full previous
        # exploration transcript and explode token usage. Context-manager,
        # memory, and subagents stay disabled for analysis mode, but these
        # deterministic processors are safe and preserve the final answer plus
        # compact SQL digests for multi-turn continuity.
        history_processors=(
            [
                MicrocompactProcessor(keep_recent_turns=2 if analysis_toolset else 3),
                SqlResultCompactor(min_chars=250 if analysis_toolset else 500),
            ]
        ),
        context_files=context_files,
        # Memory: persistent schema knowledge across sessions
        include_memory=not analysis_toolset,
        memory_dir=".seeknal/ask_memory",
        # Checkpoints: save/rewind in long chat sessions
        include_checkpoints=not analysis_toolset,
        checkpoint_frequency="every_turn",
        max_checkpoints=20,
        # Cost tracking
        cost_tracking=True,
        cost_budget_usd=budget,
        on_cost_update=_on_cost_update,
        # Output style
        output_style=style or "concise",
        # Subagents
        include_subagents=not analysis_toolset,
        subagents=[] if analysis_toolset else get_subagent_configs(),
        # Disabled: seeknal has domain-specific alternatives
        include_filesystem=False,
        include_execute=False,
    )

    # Use LocalBackend with interactive ask_user callback.
    # Gateway/headless environments have no TTY — pass ask_user=None so the
    # planner subagent auto-selects the recommended option instead of calling
    # input() on a non-TTY stdin (which blocks or raises EOFError).
    _ask_user_cb = (
        interactive_ask_user
        if environment == "interactive" and not analysis_toolset
        else None
    )
    deps = DeepAgentDeps(
        backend=LocalBackend(root_dir=str(project_path)),
        ask_user=_ask_user_cb,
    )
    message_history = []

    return agent, deps, message_history, _cost_info


def compact_history_for_analysis_mode(message_history: list) -> None:
    """Apply deterministic history compaction for read-only analysis sessions.

    pydantic-deep receives the same processors at agent construction time, but
    the Seeknal streaming/gateway harness can call pydantic-ai directly. Run the
    zero-cost processors here as a harness-level guard so multi-turn connected
    database chats do not resend every prior SQL table verbatim.
    """
    if not message_history:
        return
    try:
        from seeknal.ask.agents.tools._context import get_tool_context

        if not getattr(get_tool_context(), "disable_quality_gate", False):
            return
    except RuntimeError:
        return

    from seeknal.ask.processors import MicrocompactProcessor, SqlResultCompactor

    compacted = MicrocompactProcessor(keep_recent_turns=1)(message_history)
    compacted = SqlResultCompactor(min_chars=250)(compacted)
    message_history.clear()
    message_history.extend(compacted)


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
    from seeknal.ask.agents.tools._context import get_tool_context

    # Prefer per-session limits from the tool context; fall back to the
    # ToolContext default (100) when no context is set — typical for unit
    # tests that mock the agent without constructing a full session.
    try:
        ctx = get_tool_context()
        _request_limit = ctx.request_limit
    except RuntimeError:
        _request_limit = 100
    compact_history_for_analysis_mode(message_history)
    _usage_limits = UsageLimits(request_limit=_request_limit)

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
    from seeknal.ask.agents.tools._context import get_tool_context

    try:
        if getattr(get_tool_context(), "disable_quality_gate", False):
            return answer
    except RuntimeError:
        pass

    from seeknal.ask.agents.quality import check_answer_quality

    passes, reason = check_answer_quality(answer)
    if passes:
        return answer

    # One quality retry with guidance
    from pydantic_ai.usage import UsageLimits

    # Same fallback as `ask()` — support unit tests that mock the agent
    # without constructing a full session + ToolContext.
    try:
        ctx = get_tool_context()
        _request_limit = ctx.request_limit
    except RuntimeError:
        _request_limit = 100
    result = agent.run_sync(
        reason,
        deps=deps,
        message_history=message_history,
        usage_limits=UsageLimits(request_limit=_request_limit),
    )
    message_history.clear()
    message_history.extend(result.all_messages())

    retry_answer = result.output or ""
    return retry_answer if retry_answer else answer
