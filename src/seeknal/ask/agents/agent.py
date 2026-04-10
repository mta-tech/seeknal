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
    from seeknal.ask.config import (
        load_agent_config, get_request_limit,
        get_background_threshold, get_context_budget,
    )

    agent_config = load_agent_config(project_path)

    # Set request limit and background threshold on the per-session ToolContext
    # (NOT on module-level globals — avoids race conditions across concurrent sessions)
    from seeknal.ask.agents.tools._context import get_tool_context
    tool_ctx = get_tool_context()
    tool_ctx.request_limit = get_request_limit(agent_config)
    tool_ctx.background_threshold = get_background_threshold(agent_config)

    # Build final system prompt via section registry (4-layer architecture)
    from seeknal.ask.prompt_builder import create_default_builder

    builder = create_default_builder()
    instructions = builder.build(environment=environment, config=agent_config)

    # Resolve model string
    model_string = get_model_string(provider=provider, model=model, api_key=api_key)

    # Build toolsets: ask tools + dynamic project context injection
    context_budget = get_context_budget(agent_config)
    context_toolset = SeeknaContextToolset(discovery, context_budget=context_budget)
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
        include_plan=(environment == "interactive"),
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
        # Web search disabled: seeknal has domain-specific DuckDuckGo toolset
        # added via toolsets_list when include_web=True.
        web_search=False,
        web_fetch=False,
        # Disabled: seeknal has domain-specific alternatives
        include_filesystem=False,
        include_execute=False,
    )

    # Use LocalBackend with interactive ask_user callback.
    # Gateway/headless environments have no TTY — pass ask_user=None so the
    # planner subagent auto-selects the recommended option instead of calling
    # input() on a non-TTY stdin (which blocks or raises EOFError).
    _ask_user_cb = interactive_ask_user if environment == "interactive" else None
    deps = DeepAgentDeps(
        backend=LocalBackend(root_dir=str(project_path)),
        ask_user=_ask_user_cb,
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
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    _usage_limits = UsageLimits(request_limit=ctx.request_limit)

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
    from seeknal.ask.agents.tools._context import get_tool_context

    ctx = get_tool_context()
    result = agent.run_sync(
        reason,
        deps=deps,
        message_history=message_history,
        usage_limits=UsageLimits(request_limit=ctx.request_limit),
    )
    message_history.clear()
    message_history.extend(result.all_messages())

    retry_answer = result.output or ""
    return retry_answer if retry_answer else answer
