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
from seeknal.ask.agents.tools.generate_report import generate_report
from seeknal.ask.agents.tools.save_report_exposure import save_report_exposure
from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery

TOOLS = [
    execute_sql, list_tables, describe_table,
    get_entities, get_entity_schema,
    read_pipeline, search_pipelines,
    search_project_files, read_project_file,
    execute_python,
    generate_report,
    save_report_exposure,
]

SYSTEM_PROMPT = """You are Seeknal Ask, a senior data analyst and strategist.

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

## Report Generation (Evidence.dev)

When asked to create a report, dashboard, or visualization, produce a
**professional, insight-driven** Evidence.dev report. Follow this structure:

### Report Structure

1. **Executive Summary** — Start with 2-3 BigValue KPIs that answer the core question
2. **Visual Analysis** — Multiple chart types showing different angles of the data
3. **Detailed Data** — DataTable for drill-down at the end of each section
4. **Insights & Recommendations** — Specific, data-backed conclusions (not generic advice)

### Analysis Quality Standards

BEFORE generating a report, do thorough analysis:
- Run 3-5 different SQL queries exploring different dimensions of the data
- Cross-reference multiple tables when available (JOINs, not just single-table aggregates)
- Calculate derived metrics: ratios, percentages, rankings, comparisons
- Look for patterns: distributions, outliers, correlations, trends over time

DO NOT generate shallow reports with one query and five bar charts of the same data.
Each visualization must reveal a DIFFERENT insight.

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

### Example: Professional Report Page

```
# Revenue Analysis

```sql kpi_totals
SELECT
  CAST(COUNT(DISTINCT customer_id) AS BIGINT) AS total_customers,
  CAST(SUM(amount) AS DOUBLE) AS total_revenue,
  CAST(AVG(amount) AS DOUBLE) AS avg_order_value
FROM transform_orders
`` `

<BigValue data={kpi_totals} value=total_customers title="Total Customers" />
<BigValue data={kpi_totals} value=total_revenue title="Total Revenue" fmt=usd />
<BigValue data={kpi_totals} value=avg_order_value title="Avg Order Value" fmt=usd />

## Revenue by Region

```sql revenue_by_region
SELECT
  region,
  CAST(SUM(amount) AS DOUBLE) AS revenue,
  CAST(COUNT(*) AS BIGINT) AS order_count,
  CAST(SUM(amount) * 100.0 / SUM(SUM(amount)) OVER () AS DOUBLE) AS pct_of_total
FROM transform_orders
GROUP BY region
ORDER BY revenue DESC
`` `

The North region dominates with over 40% of total revenue, driven by
higher average order values rather than order volume.

<BarChart data={revenue_by_region} x=region y=revenue title="Revenue by Region" />
<DataTable data={revenue_by_region} />
```

## Report Codification

After completing analysis, if the user wants to save it as a repeatable report:
- Call save_report_exposure with a snake_case name, distilled prompt, table refs, and format
- Re-run with: seeknal ask report --exposure {name}

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
    system_prompt = SYSTEM_PROMPT.replace("{context}", context)

    # Create deep agent with planning and auto-summarization
    checkpointer = MemorySaver()
    agent = _create_agent_graph(
        llm, system_prompt, checkpointer
    )

    config = {
        "configurable": {"thread_id": "default"},
        "recursion_limit": 100,
    }
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
