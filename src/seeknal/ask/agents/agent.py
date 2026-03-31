"""Deep agent for Seeknal Ask.

Uses pydantic-deep (built on pydantic-ai) for planning and auto-summarization.
LLM generates SQL -> execute_sql tool -> hook validates -> error? -> LLM retries.
For complex analyses, the agent decomposes tasks via the planning tool.
"""

from pathlib import Path
from typing import Optional

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery

SYSTEM_PROMPT = """\
You are Seeknal Ask, a senior data analyst and strategist.

You analyze data managed by seeknal — a data engineering platform that produces
entities, feature groups, and transformations stored as DuckDB views.

## Your Capabilities

1. List and describe tables/entities
2. Execute read-only DuckDB SQL queries
3. Read pipeline definitions to understand data lineage
4. Search project files (code, configs, YAML)
5. Execute Python for statistical analysis (pandas, scipy, matplotlib)
6. Generate interactive HTML reports with Evidence.dev
7. Codify reports as YAML exposures for scheduled re-runs

## Workflow

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
3. Pre-loaded: `conn` (DuckDB), `pd`, `np`, `plt`

For report generation, load the 'report-generation' skill first.

When you discover schema quirks, business definitions, or useful query patterns, \
save them to memory for future sessions.

## DuckDB SQL Rules

- Do NOT include trailing semicolons
- Use `CAST('2024-01-01' AS TIMESTAMP)` before INTERVAL arithmetic
- Use `CAST(COUNT(*) AS BIGINT)` for counts
- Use `CAST(SUM(x) AS DOUBLE)` for aggregations
- All non-aggregate SELECT columns must be in GROUP BY
- Struct fields: `column.field`
- Case-insensitive: use `ILIKE`

## Security

- Only SELECT/WITH queries (read-only)
- Never reference file paths in SQL
- Only query tables from list_tables output

## Error Handling

Tool errors include a JSON structure with 'category' and 'retryable' fields.
For retryable errors, adjust your approach based on the 'hint'.
For terminal errors, explain the limitation to the user.
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
):
    """Create a Seeknal Ask agent.

    Args:
        project_path: Path to the seeknal project root.
        provider: LLM provider ("google" or "ollama").
        model: Model name override.
        api_key: API key override.

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
    from seeknal.ask.agents.tools.toolset import create_ask_toolset
    from seeknal.ask.processors import MicrocompactProcessor, SqlResultCompactor
    from seeknal.ask.security import configure_safe_connection
    from seeknal.cli.repl import REPL

    # Create singleton REPL with safe connection
    repl = REPL(project_path=project_path, skip_history=True)
    configure_safe_connection(repl.conn)

    # Discover project artifacts for context
    discovery = ArtifactDiscovery(project_path)

    # Set tool context (global, used by all tools)
    set_tool_context(ToolContext(
        repl=repl,
        artifact_discovery=discovery,
        project_path=project_path,
    ))

    # Resolve model string
    model_string = get_model_string(provider=provider, model=model, api_key=api_key)

    # Build toolsets: ask tools + dynamic project context injection
    context_toolset = SeeknaContextToolset(discovery)

    # Ensure .seeknal directory exists for memory storage
    (project_path / ".seeknal").mkdir(exist_ok=True)

    # Create pydantic-deep agent
    agent = create_deep_agent(
        model=model_string,
        instructions=SYSTEM_PROMPT,
        toolsets=[create_ask_toolset(), context_toolset],
        hooks=get_ask_hooks(),
        # Skills: discovered from SKILL.md files in seeknal/skills/ (Claude Code-style)
        include_skills=True,
        skill_directories=[str(project_path / "seeknal" / "skills")],
        # Enable planning for complex multi-step analyses
        include_todo=True,
        # Auto-summarize when approaching context window limit (Tier 3)
        context_manager=True,
        # Multi-tier compaction: Tier 1 (microcompact) + Tier 2 (SQL snip)
        history_processors=[MicrocompactProcessor(), SqlResultCompactor()],
        # Project memory: persistent schema knowledge across sessions
        include_memory=True,
        memory_dir=".seeknal/ask_memory",
        # Disable features we don't use yet
        include_filesystem=False,
        include_subagents=True,
        subagents=get_subagent_configs(),
        include_plan=False,
        include_checkpoints=False,
        include_teams=False,
        include_web=False,
        include_execute=False,
    )

    # Use LocalBackend rooted at project path for disk-persistent memory
    deps = DeepAgentDeps(backend=LocalBackend(root_dir=str(project_path)))
    message_history = []

    return agent, deps, message_history


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
    result = agent.run_sync(
        question,
        deps=deps,
        message_history=message_history,
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
    result = agent.run_sync(
        reason,
        deps=deps,
        message_history=message_history,
    )
    message_history.clear()
    message_history.extend(result.all_messages())

    retry_answer = result.output or ""
    return retry_answer if retry_answer else answer
