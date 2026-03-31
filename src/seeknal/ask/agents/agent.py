"""Deep agent for Seeknal Ask.

Uses pydantic-deep (built on pydantic-ai) for planning and auto-summarization.
LLM generates SQL -> execute_sql tool -> hook validates -> error? -> LLM retries.
For complex analyses, the agent decomposes tasks via the planning tool.
"""

from pathlib import Path
from typing import Optional

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery

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


_NO_RESPONSE = "No response generated."

# Ralph Loop: max retries when agent returns tool calls but no final text
_MAX_RALPH_RETRIES = 3


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
    from pydantic_deep import create_deep_agent, DeepAgentDeps

    from seeknal.ask.agents.hooks import get_security_hooks
    from seeknal.ask.agents.providers import get_model_string
    from seeknal.ask.agents.tools.toolset import create_ask_toolset
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

    # Resolve model string
    model_string = get_model_string(provider=provider, model=model, api_key=api_key)

    # Build system prompt with project context
    system_prompt = SYSTEM_PROMPT.replace("{context}", context)

    # Create pydantic-deep agent
    agent = create_deep_agent(
        model=model_string,
        instructions=system_prompt,
        toolsets=[create_ask_toolset()],
        hooks=get_security_hooks(),
        # Enable planning for complex multi-step analyses
        include_todo=True,
        # Auto-summarize when approaching context window limit
        context_manager=True,
        # Disable features we don't use
        include_filesystem=False,
        include_subagents=False,
        include_skills=False,
        include_memory=False,
        include_plan=False,
        include_checkpoints=False,
        include_teams=False,
        include_web=False,
        include_execute=False,
    )

    deps = DeepAgentDeps()
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
    Retries up to _MAX_RALPH_RETRIES times before giving up.

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
        return response

    # Ralph Loop: nudge the agent to produce a text summary
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
            return response

    return _NO_RESPONSE
