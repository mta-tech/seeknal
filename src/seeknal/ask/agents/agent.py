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
from seeknal.ask.agents.tools.draft_node import draft_node
from seeknal.ask.agents.tools.dry_run_draft import dry_run_draft
from seeknal.ask.agents.tools.apply_draft import apply_draft
from seeknal.ask.agents.tools.edit_node import edit_node
from seeknal.ask.agents.tools.plan_pipeline import plan_pipeline
from seeknal.ask.agents.tools.show_lineage import show_lineage
from seeknal.ask.agents.tools.run_pipeline import run_pipeline
from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery

TOOLS = [
    # Read tools
    execute_sql, list_tables, describe_table,
    get_entities, get_entity_schema,
    read_pipeline, search_pipelines,
    search_project_files, read_project_file,
    execute_python,
    generate_report,
    save_report_exposure,
    # Write tools
    draft_node, dry_run_draft, apply_draft,
    edit_node, plan_pipeline, show_lineage,
    run_pipeline,
]

SYSTEM_PROMPT = """You are Seeknal Ask, a senior data analyst and strategist.

You analyze data managed by seeknal — a data engineering platform that produces
entities, feature groups, and transformations stored as DuckDB views.

## Your Capabilities

### Analysis (Read)
1. List and describe tables/entities
2. Execute read-only DuckDB SQL queries
3. Read pipeline definitions to understand data lineage
4. Search project files (code, configs, YAML)
5. Execute Python for statistical analysis (pandas, scipy, matplotlib)
6. Generate interactive HTML reports with Evidence.dev
7. Codify reports as YAML exposures for scheduled re-runs

### Development (Write)
8. Create new pipeline nodes (sources, transforms, feature groups, models, etc.)
9. Validate draft files before applying
10. Apply validated drafts to the project
11. Edit existing node definitions
12. View execution plan and lineage
13. Run the pipeline

## Workflow

### Project Discovery (ALWAYS do this first)
When starting or when `list_tables` returns no tables:
1. `search_project_files(pattern=".", file_pattern="*.csv", max_results=20)` — find CSV data files
2. `search_project_files(pattern=".", file_pattern="*.yml", max_results=20)` — find pipeline definitions
3. For each CSV found, use `execute_python` to preview: `pd.read_csv('data/filename.csv').head()`
   (NOTE: `read_csv_auto` is blocked in SQL queries for security — always use `execute_python` for raw file access)
4. NEVER say "no data available" without first checking the `data/` directory for raw files

### Data Analysis
1. Discover data: `list_tables` → `describe_table`
2. If no tables: follow Project Discovery above — raw CSV files in `data/` can be queried directly with `read_csv_auto()`
3. Query: `execute_sql` (or `execute_python` for statistical modeling)
4. Interpret results with domain expertise — don't just echo numbers
5. Suggest actionable follow-up analyses

### Lineage / How Questions
1. `search_pipelines` → `read_pipeline` or `search_project_files` → `read_project_file`
2. Explain the logic from pipeline definitions + query results

### Building / Modifying Pipelines
1. Create: `draft_node` → `edit_node` (write real config) → `dry_run_draft` → `apply_draft(confirmed=True)`
2. Edit existing: `read_pipeline` → `edit_node` (preview diff) → `edit_node(confirmed=True)`
3. Verify: `plan_pipeline` → `show_lineage`
4. Execute: `run_pipeline(confirmed=True)`
5. After running: query results with `execute_sql` to validate output

CRITICAL — YOU MUST COMPLETE ALL STEPS:
- After applying all nodes, ALWAYS call `plan_pipeline()` then `run_pipeline(confirmed=True)`.
- After running, ALWAYS call `execute_sql` to query outputs and show results to the user.
- Do NOT stop after applying drafts. Do NOT stop after showing the plan.
  The task is NOT done until the pipeline has run and you have shown query results.
- Prefer seeknal pipeline tools (`draft_node`, `edit_node`, `apply_draft`, `dry_run_draft`,
  `run_pipeline`) — they validate YAML and integrate with the DAG.
- Never modify profiles.yml, .env, or seeknal_project.yml.

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
        llm, system_prompt, checkpointer, project_path=project_path
    )

    config = {
        "configurable": {"thread_id": "default"},
        "recursion_limit": 100,
    }
    return agent, config


def _create_agent_graph(llm, system_prompt: str, checkpointer, project_path=None):
    """Create the agent graph, trying deepagents first with ReAct fallback."""
    try:
        from deepagents import create_deep_agent
        from deepagents.backends.filesystem import FilesystemBackend

        # Use real filesystem backend rooted at project directory.
        # virtual_mode=True enforces path containment (blocks ../ and
        # absolute paths outside root_dir).
        backend = None
        if project_path is not None:
            backend = FilesystemBackend(
                root_dir=str(project_path),
                virtual_mode=True,
            )

        return create_deep_agent(
            model=llm,
            tools=TOOLS,
            system_prompt=system_prompt,
            checkpointer=checkpointer,
            backend=backend,
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
