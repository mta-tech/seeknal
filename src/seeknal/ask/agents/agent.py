"""LangGraph ReAct agent for Seeknal Ask.

Single-agent architecture with self-correction via the ReAct loop:
LLM generates SQL -> execute_sql tool -> error? -> LLM retries.
"""

from pathlib import Path
from typing import Optional

from langchain_core.messages import HumanMessage, SystemMessage
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver

from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.agents.tools.list_tables import list_tables
from seeknal.ask.agents.tools.describe_table import describe_table
from seeknal.ask.agents.tools.get_entities import get_entities
from seeknal.ask.agents.tools.get_entity_schema import get_entity_schema
from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery

TOOLS = [execute_sql, list_tables, describe_table, get_entities, get_entity_schema]

SYSTEM_PROMPT = """You are Seeknal Ask, an AI data analyst for seeknal projects.

You help users explore and analyze data managed by seeknal — a data engineering platform
that produces entities, feature groups, and intermediate transformations stored as DuckDB views.

## Your Capabilities

You can:
1. List available tables and entities
2. Describe table schemas (columns and types)
3. Execute read-only SQL queries against the data
4. Explain results and suggest follow-up analyses

## Workflow

When a user asks a data question:
1. First, use `list_tables` or `get_entities` to discover available data
2. Use `describe_table` or `get_entity_schema` to understand the schema
3. Write and execute a DuckDB SQL query with `execute_sql`
4. Summarize the results in natural language
5. Suggest relevant follow-up questions

## DuckDB SQL Rules

- Use `CAST('2024-01-01' AS TIMESTAMP)` before INTERVAL arithmetic (no auto-cast)
- Use `CAST(COUNT(*) AS BIGINT)` for counts (DuckDB returns HUGEINT)
- Use `CAST(SUM(x) AS DOUBLE)` for numeric aggregations
- All non-aggregate columns in SELECT must be in GROUP BY
- Access struct fields with dot notation: `column.field`
- String comparisons are case-sensitive by default; use `ILIKE` for case-insensitive

## Security

- Only SELECT/WITH queries are allowed (read-only)
- Never reference file paths or use file-reading functions
- Only query tables that appear in list_tables output

## Data Context

{context}
"""


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
        A tuple of (agent, config) where agent is a LangGraph compiled graph
        and config is the default invocation config.
    """
    from seeknal.ask.agents.providers import get_llm
    from seeknal.ask.security import configure_safe_connection
    from seeknal.cli.repl import REPL

    # Create singleton REPL with safe connection
    repl = REPL(project_path=project_path, skip_history=True)
    configure_safe_connection(repl.conn)

    # Discover project artifacts for context
    discovery = ArtifactDiscovery(project_path)
    context = discovery.get_context_for_prompt()

    # Set tool context (global, used by all tools)
    set_tool_context(ToolContext(
        repl=repl,
        artifact_discovery=discovery,
        project_path=project_path,
    ))

    # Create LLM
    llm = get_llm(provider=provider, model=model, api_key=api_key)

    # Build system prompt with project context
    system_message = SystemMessage(
        content=SYSTEM_PROMPT.format(context=context)
    )

    # Create ReAct agent with memory
    checkpointer = MemorySaver()
    agent = create_react_agent(
        llm,
        tools=TOOLS,
        checkpointer=checkpointer,
        prompt=system_message,
    )

    config = {"configurable": {"thread_id": "default"}}
    return agent, config


def ask(
    agent,
    config: dict,
    question: str,
) -> str:
    """Send a question to the agent and get a response.

    Args:
        agent: The compiled LangGraph agent.
        config: Agent invocation config (with thread_id).
        question: User's natural language question.

    Returns:
        The agent's text response.
    """
    result = agent.invoke(
        {"messages": [HumanMessage(content=question)]},
        config=config,
    )
    # Extract the last AI message
    messages = result.get("messages", [])
    for msg in reversed(messages):
        if hasattr(msg, "content") and msg.type == "ai" and msg.content:
            content = msg.content
            # Gemini returns content as list of blocks: [{'type':'text','text':'...'}]
            if isinstance(content, list):
                parts = []
                for block in content:
                    if isinstance(block, dict) and "text" in block:
                        parts.append(block["text"])
                    elif isinstance(block, str):
                        parts.append(block)
                return "\n".join(parts) if parts else "No response generated."
            return content
    return "No response generated."
