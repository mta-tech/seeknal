"""Deep agent for Seeknal Ask.

Uses deepagents (built on LangGraph) for planning and auto-summarization.
LLM generates SQL -> execute_sql tool -> error? -> LLM retries.
For complex analyses, the agent decomposes tasks via the planning tool.
"""

from pathlib import Path
from typing import Optional

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import MemorySaver

from seeknal.ask.agents.tools.execute_sql import execute_sql
from seeknal.ask.agents.tools.list_tables import list_tables
from seeknal.ask.agents.tools.describe_table import describe_table
from seeknal.ask.agents.tools.get_entities import get_entities
from seeknal.ask.agents.tools.get_entity_schema import get_entity_schema
from seeknal.ask.agents.tools.read_pipeline import read_pipeline
from seeknal.ask.agents.tools.search_pipelines import search_pipelines
from seeknal.ask.agents.tools.search_project_files import search_project_files
from seeknal.ask.agents.tools.read_project_file import read_project_file
from seeknal.ask.agents.tools.execute_python import execute_python
from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery

TOOLS = [
    execute_sql, list_tables, describe_table,
    get_entities, get_entity_schema,
    read_pipeline, search_pipelines,
    search_project_files, read_project_file,
    execute_python,
]

SYSTEM_PROMPT = """You are Seeknal Ask, an AI data analyst for seeknal projects.

You help users explore and analyze data managed by seeknal — a data engineering platform
that produces entities, feature groups, and intermediate transformations stored as DuckDB views.

## Your Capabilities

You can:
1. List available tables and entities
2. Describe table schemas (columns and types)
3. Execute read-only SQL queries against the data
4. Read pipeline definitions to understand how data is produced
5. Search across pipelines to find specific calculations or columns
6. Explain results and suggest follow-up analyses
7. Search across all project files (code, configs, YAML) using regex patterns
8. Read any file in the project to understand custom transforms, configs, or scripts
9. Execute Python code for advanced analysis (pandas, scipy, matplotlib)

## Tool Selection Guide

| Question Type | Primary Tool | Alternative |
|---|---|---|
| "What tables exist?" | list_tables | get_entities |
| "What columns does X have?" | describe_table | get_entity_schema |
| "How many rows / what's the average?" | execute_sql | execute_python if statistical modeling needed |
| "Where is column X defined?" | search_project_files | search_pipelines for pipeline metadata |
| "How is this pipeline built?" | read_pipeline | read_project_file for non-pipeline files |
| "Show me a histogram / correlation" | execute_python | — |
| "What's the data lineage for X?" | search_project_files → read_project_file | — |

## Workflow

When a user asks a data question:
1. First, use `list_tables` or `get_entities` to discover available data
2. Use `describe_table` or `get_entity_schema` to understand the schema
3. Write and execute a DuckDB SQL query with `execute_sql`
4. Summarize the results in natural language
5. Suggest relevant follow-up questions

When a user asks HOW data is produced or WHY a calculation works a certain way:
1. Use `search_pipelines` to find the relevant pipeline definition (structured metadata)
2. Or use `search_project_files` for broader searches across all project files
3. Use `read_pipeline` or `read_project_file` to read the full definition
4. Explain the logic based on the pipeline definition AND query results

When a user needs advanced analysis beyond SQL:
1. Use `execute_sql` to query data first
2. Use `execute_python` for statistical tests, visualizations, or complex pandas operations
3. Pre-loaded: `conn` (DuckDB), `pd` (pandas), `np` (numpy), `plt` (matplotlib)
4. Query data with `conn.sql('SELECT ...').df()` to get a DataFrame

For complex multi-step analyses, break your work into clear sub-tasks.

## DuckDB SQL Rules

- Use `CAST('2024-01-01' AS TIMESTAMP)` before INTERVAL arithmetic (no auto-cast)
- Use `CAST(COUNT(*) AS BIGINT)` for counts (DuckDB returns HUGEINT)
- Use `CAST(SUM(x) AS DOUBLE)` for numeric aggregations
- All non-aggregate columns in SELECT must be in GROUP BY
- Access struct fields with dot notation: `column.field`
- String comparisons are case-sensitive by default; use `ILIKE` for case-insensitive

## Security

- Only SELECT/WITH queries are allowed (read-only)
- Never reference file paths or use file-reading functions in SQL
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
    system_prompt = SYSTEM_PROMPT.format(context=context)

    # Create deep agent with planning and auto-summarization
    checkpointer = MemorySaver()
    agent = _create_agent_graph(
        llm, system_prompt, checkpointer
    )

    config = {"configurable": {"thread_id": "default"}}
    return agent, config


def _create_agent_graph(llm, system_prompt: str, checkpointer):
    """Create the agent graph, trying deepagents first with ReAct fallback."""
    try:
        from deepagents import create_deep_agent

        return create_deep_agent(
            model=llm,
            tools=TOOLS,
            system_prompt=system_prompt,
            checkpointer=checkpointer,
        )
    except ImportError:
        # Fallback to ReAct agent if deepagents not installed
        from langchain_core.messages import SystemMessage
        from langgraph.prebuilt import create_react_agent

        return create_react_agent(
            llm,
            tools=TOOLS,
            checkpointer=checkpointer,
            prompt=SystemMessage(content=system_prompt),
        )


_NO_RESPONSE = "No response generated."

# Ralph Loop: max retries when agent returns tool calls but no final text
_MAX_RALPH_RETRIES = 3


def _normalize_content(content) -> str:
    """Normalize Gemini's list[dict] content format to plain string.

    Gemini returns content as [{'type':'text','text':'...'}] or plain str.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and "text" in block:
                parts.append(block["text"])
            elif isinstance(block, str):
                parts.append(block)
        return "".join(parts)
    return str(content) if content else ""


def _extract_response(messages: list) -> str:
    """Extract the last AI text message from agent output."""
    for msg in reversed(messages):
        if hasattr(msg, "content") and msg.type == "ai" and msg.content:
            text = _normalize_content(msg.content)
            if text:
                return text
    return ""


def ask(
    agent,
    config: dict,
    question: str,
) -> str:
    """Send a question to the agent and get a response.

    Uses the Ralph Loop technique: if the agent ends on tool calls without
    producing a final text response, nudge it to summarize its findings.
    Retries up to _MAX_RALPH_RETRIES times before giving up.

    Args:
        agent: The compiled LangGraph agent.
        config: Agent invocation config (with thread_id).
        question: User's natural language question.

    Returns:
        The agent's text response.
    """
    # Initial invocation
    result = agent.invoke(
        {"messages": [HumanMessage(content=question)]},
        config=config,
    )
    response = _extract_response(result.get("messages", []))
    if response:
        return response

    # Ralph Loop: nudge the agent to produce a text summary
    for _ in range(_MAX_RALPH_RETRIES):
        result = agent.invoke(
            {"messages": [HumanMessage(
                content="Please summarize your findings from the tool calls above "
                        "and provide your analysis as a text response."
            )]},
            config=config,
        )
        response = _extract_response(result.get("messages", []))
        if response:
            return response

    return _NO_RESPONSE
