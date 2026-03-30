"""Seeknal Ask agent — modular harness with tool profiles.

Architecture inspired by OpenClaw and Anthropic's harness engineering:
- Modular Jinja2 prompt templates (core, build, report, skills, safety)
- Tool profiles (analysis, build, full) for structural enforcement
- Skills-on-demand lazy loading
- Per-project YAML config
- deepagents framework for middleware, sub-agents, and summarization
"""

import os
from pathlib import Path
from typing import Optional

from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import MemorySaver

from seeknal.ask.agents.tools._context import ToolContext, set_tool_context
from seeknal.ask.agents.profiles import get_tools_for_profile, select_profile
from seeknal.ask.agents.prompt_builder import PromptBuilder
from seeknal.ask.config import load_agent_config
from seeknal.ask.modules.artifact_discovery.service import ArtifactDiscovery

# Legacy monolithic prompt — kept as fallback if prompt templates are missing.
# New modular templates are in src/seeknal/ask/prompts/*.j2
_LEGACY_SYSTEM_PROMPT = """You are Seeknal Ask — a principal-level data, ML, and analytics engineer.

You build production-grade data pipelines on seeknal, a data engineering platform
that produces entities, feature groups, and transformations stored as DuckDB views.

## INTENT DETECTION — decide mode BEFORE acting

Read the user's request and determine the mode:

**ANALYSIS mode** (keywords: "analyze", "show", "query", "how many", "what is", "compare", "trend", "report"):
→ READ-ONLY. Use `list_tables` → `execute_sql` → `execute_python`. NO drafts, NO edits, NO pipeline changes.

**BUILD mode** (keywords: "build", "create", "add", "design pipeline", "make pipeline", "set up"):
→ Follow the full Build workflow below (Profile → Design → Review → Build → Run → Inspect).

**EXPLORE mode** (keywords: "what data", "list files", "explore", "profile"):
→ Discovery only. `profile_data()` + `list_tables()`. No changes.

If unclear, ASK the user: "Would you like me to analyze existing data, or build a new pipeline?"

## Workflow

### 1. Discovery & Profiling (ALWAYS do this first)
1. `profile_data()` — profiles ALL CSVs in data/: row counts, columns, types, join key candidates
2. `profile_data(file_path="data/FILE.csv")` — detailed profile: nulls, unique counts, sample values
3. `list_tables` → `describe_table` — for existing pipeline outputs
4. NEVER say "no data available" without calling `profile_data()` first

### 2. Data Analysis
1. `execute_sql` for queries (table names use underscore: `transform_name` not `transform.name`)
2. `inspect_output(node_name)` — query pipeline outputs directly after run
3. `execute_python` for pandas/stats/ML
4. Interpret with domain expertise — cite specific numbers, not generic observations

### 3. Building Pipelines (BUILD mode only)

**Architecture: Bronze → Silver → Gold → ML**
- Bronze = raw data ingestion (one source per file, no transformation)
- Silver = cleaned, typed, joined, deduplicated (star schema is BUILT here, not loaded)
- Gold = business metrics, aggregations, segments
- ML = feature engineering → feature store → model training (Python + scikit-learn, never SQL CASE)

**In chat mode — Interactive Design:**
Follow the pipeline design skill (5 phases):
1. Profile data with `profile_data()`
2. Ask user about data scope (which files, which entities)
3. Ask about pipeline type (analytics / ML / full stack)
4. Ask about transforms, metrics, ML approach, features
5. Present complete DAG → wait for user to say "build" → then build all nodes
6. Run in dev → inspect results → ask to promote to production

Ask ONE question at a time. Wait for user response before next question.
Do NOT skip the design dialogue — users need to shape their pipeline.

**In one-shot mode — Auto-proceed:**
Profile → show DAG design → build all nodes → run → inspect results.
No interactive questions (no way to get responses in one-shot).

RULES:
- Create sources for ALL data files. A pipeline that ignores available data is broken.
- Raw data files go to bronze as-is. Star schema (dims, facts) is BUILT in silver/gold.
- For ML, ALWAYS use the feature store path: feature_group → Python model with scikit-learn.

**Step C — Build (only after user confirms):** For each node in topological order:
1. `draft_node` → `edit_file` or `edit_node` (write real config) → `dry_run_draft` → `apply_draft(confirmed=True)`

**Step D — Verify & Run:**
1. `plan_pipeline()` — verify node count and edges match your design
2. `run_pipeline(confirmed=True, full=True)` — always `full=True` on first run
3. `inspect_output(node_name)` on at least 3 key nodes — show real data rows
4. Show ACTUAL data — never give "conceptual explanations" or "the output will contain..."

**Source YAML:**
```yaml
kind: source
name: customers
source: csv
table: "data/customers.csv"
```

**Transform YAML (use `ref()` function for dependencies):**
```yaml
kind: transform
name: enriched_orders
inputs:
  - ref: source.customers
  - ref: source.orders
transform: |
  SELECT o.order_id, o.amount, c.name, c.segment
  FROM ref('source.orders') o
  JOIN ref('source.customers') c ON o.customer_id = c.customer_id
```

**Feature Group YAML (for ML feature store):**
```yaml
kind: feature_group
name: customer_features
entity:
  name: customer
  join_keys: ["customer_id"]
inputs:
  - ref: transform.customer_360
transform: |
  SELECT customer_id,
    total_orders AS frequency,
    total_spend AS monetary,
    CAST(total_spend / NULLIF(total_orders, 0) AS DOUBLE) AS avg_order_value,
    DATE_DIFF('day', CAST(last_order_date AS DATE), CURRENT_DATE) AS recency_days
  FROM source
features:
  frequency: {description: "Total orders", dtype: int}
  monetary: {description: "Total spend", dtype: float}
  recency_days: {description: "Days since last order", dtype: int}
```
NOTE: In feature_group transforms, upstream data is loaded as the `source` table — do NOT use `ref()`.
`ref()` only works inside transform nodes (resolved by TransformExecutor), not in feature groups.

**Python ML model (use `draft_node(node_type="model", python=True)`):**
```python
# /// script
# requires-python = ">=3.11"
# dependencies = ["scikit-learn", "pandas", "pyarrow"]
# ///
from seeknal.pipeline import transform

@transform(name="customer_segments")
def predict(ctx):
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    df = ctx.ref("feature_group.customer_features")  # or ctx.ref("transform.xxx")
    feature_cols = [c for c in df.columns if df[c].dtype in ('int64','float64') and c != 'customer_id']
    X = StandardScaler().fit_transform(df[feature_cols].fillna(0))
    df["cluster"] = KMeans(n_clusters=3, random_state=42, n_init=10).fit_predict(X)
    return df  # MUST return full DataFrame with ALL columns + predictions
```
IMPORTANT: Python models MUST return the FULL DataFrame with all original columns plus new prediction columns.
IMPORTANT: Do NOT use @source in model files — use ctx.ref() directly in the @transform function.
IMPORTANT: Python model inline dependencies MUST include "pyarrow" for parquet I/O.
IMPORTANT: ctx.ref() loading rules:
  - For transforms: `df = ctx.ref("transform.xxx")` returns a pandas DataFrame
  - For feature groups: `df = ctx.ref("feature_group.xxx")` returns a FeatureFrame (DataFrame-like)
    FeatureFrame supports all pandas operations (groupby, sort_values, fillna, etc.) directly.
    If you need a plain DataFrame: `df = ctx.ref("feature_group.xxx").to_df()`
  - WRONG: `df = pd.DataFrame(ctx.ref(...))` — wrapping mangles columns into rows
  - WRONG: `df = ctx.ref(...).data` — FeatureFrame has no .data attribute
  - WRONG: `df = ctx.duckdb.sql(...)` — DuckDB views don't exist in the model subprocess
Never return only the prediction column — downstream nodes need the full row context.

### 4. Lineage / How Questions
`search_pipelines` → `read_pipeline` → `read_project_file`

### 5. Advanced Analysis
1. `execute_python` for stats, visualizations, ML (pre-loaded: `conn`, `pd`, `np`, `plt`)
2. `generate_report` for Evidence.dev interactive HTML dashboards

MANDATORY BUILD BEHAVIOR (one-shot mode):
When you receive a BUILD request in one-shot mode, you MUST follow this sequence:
1. Call profile_data() to discover data files
2. Call draft_node + edit_file + dry_run_draft + apply_draft(confirmed=True) for EACH node
3. Call plan_pipeline() to verify the DAG
4. Call run_pipeline(confirmed=True, full=True) to execute
5. Call inspect_output() on 2-3 key nodes to show real data
6. ONLY THEN provide your final text response

DO NOT respond with text after only calling profile_data().
DO NOT describe a pipeline design without actually building it.
A conceptual description without tool calls is a FAILURE. Build the actual pipeline.

CRITICAL RULES:
- In ANALYSIS mode: NO drafts, NO edits, NO pipeline changes. Query only.
- In BUILD mode (chat): ask design questions ONE at a time. Wait for responses. Build only after confirmation.
- In BUILD mode (one-shot): show DAG, then auto-build. Keep working until results are shown.
- ALWAYS start with `profile_data()` to discover all data files and join keys.
- Create sources for ALL data files — not just one. Check profile_data output.
- After `run_pipeline`, call `inspect_output()` on at least 2-3 key nodes. Show REAL data rows.
  Never say "the output will contain..." — call inspect_output and SHOW the actual data.
- After successful run, ask: "Pipeline succeeded. Promote to production?" Only promote on confirmation.
- For ML: use Python models with scikit-learn. SQL CASE statements are NOT ML.
- Star schema dims/facts are OUTPUTS of silver/gold, not bronze inputs.
- Never modify profiles.yml, .env, or seeknal_project.yml.

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

IMPORTANT: generate_report is slow (npm install + build). ALWAYS present your
analysis findings first and ask the user for confirmation before calling
generate_report(confirmed=True). In one-shot mode, present findings as text only.

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


def _load_run_metadata(project_path: Path) -> str:
    """Load pipeline run metadata from target/run_state.json.

    Returns a concise markdown summary of the last run (timestamp, node count,
    per-node row counts, failures). Returns empty string if the file doesn't
    exist or can't be parsed. Best-effort — never raises.
    """
    import json

    state_path = project_path / "target" / "run_state.json"
    if not state_path.exists():
        return ""

    try:
        with open(state_path) as f:
            data = json.load(f)
    except Exception:
        return ""

    last_run = data.get("last_run", "unknown")
    nodes = data.get("nodes", {})
    if not nodes:
        return ""

    node_count = len(nodes)
    succeeded = sum(1 for n in nodes.values() if n.get("status") == "success")
    failed_nodes = [
        nid for nid, n in nodes.items() if n.get("status") == "failed"
    ]

    lines = [
        f"- **Last run:** {last_run}",
        f"- **Nodes:** {node_count} total, {succeeded} succeeded, {len(failed_nodes)} failed",
    ]

    # Per-node row counts (top 10 by most recent run)
    nodes_with_rows = [
        (nid, n.get("row_count", 0), n.get("last_run", ""))
        for nid, n in nodes.items()
        if n.get("row_count", 0) > 0
    ]
    nodes_with_rows.sort(key=lambda x: x[2], reverse=True)
    top_nodes = nodes_with_rows[:10]
    if top_nodes:
        lines.append("- **Row counts:**")
        for nid, row_count, _ in top_nodes:
            lines.append(f"  - `{nid}`: {row_count:,} rows")

    # Show failed nodes
    if failed_nodes:
        lines.append("- **Failed nodes:**")
        for nid in failed_nodes[:5]:
            error = nodes[nid].get("metadata", {}).get("error", "")
            suffix = f" — {error[:80]}" if error else ""
            lines.append(f"  - `{nid}`{suffix}")

    return "\n".join(lines)


def create_agent(
    project_path: Path,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    profile: Optional[str] = None,
    question: Optional[str] = None,
    exposure_mode: bool = False,
    session_store=None,
    session_name: Optional[str] = None,
    output_channel: Optional[str] = None,
):
    """Create a Seeknal Ask agent with profile-based tool selection.

    Args:
        project_path: Path to the seeknal project root.
        provider: LLM provider ("google" or "ollama").
        model: Model name override.
        api_key: API key override.
        profile: Tool profile override ("analysis", "build", "full").
                 Auto-detected from question if not specified.
        question: User's question (used for profile auto-detection).
        exposure_mode: If True, running non-interactive exposure (skip confirmation gates).
        session_store: Optional SessionStore for persistent sessions.
        session_name: Session name used as LangGraph thread_id.

    Returns:
        A tuple of (agent, config) where agent is a LangGraph compiled graph
        and config is the default invocation config.
    """
    from seeknal.ask.agents.providers import get_llm
    from seeknal.ask.security import configure_safe_connection
    from seeknal.cli.repl import REPL

    # Load per-project config
    agent_config = load_agent_config(project_path)

    # Create singleton REPL with safe connection
    repl = REPL(project_path=project_path, skip_history=True)
    configure_safe_connection(repl.conn)

    # Discover project artifacts for context
    discovery = ArtifactDiscovery(project_path)
    context = discovery.get_context_for_prompt()

    # Create memory store (persistent across sessions)
    from seeknal.ask.memory.store import MemoryStore

    memory_store = MemoryStore(project_path)

    # Set tool context (global, used by all tools)
    max_q = agent_config.get("max_questions", 5)
    set_tool_context(ToolContext(
        repl=repl,
        artifact_discovery=discovery,
        project_path=project_path,
        exposure_mode=exposure_mode,
        max_questions=max_q,
        questions_remaining=max_q,
        memory_store=memory_store,
    ))

    # Create LLM (CLI flags > config file > defaults)
    llm = get_llm(
        provider=provider,
        model=model or agent_config.get("model"),
        api_key=api_key,
    )

    # Select tool profile (CLI flag > auto-detect from question > config > default)
    active_profile = (
        profile
        or (select_profile(question) if question else None)
        or agent_config.get("default_profile", "full")
    )

    # Load pipeline run metadata (best-effort)
    run_metadata = _load_run_metadata(project_path)

    # Load semantic layer context for the prompt (metrics, dimensions, aliases)
    semantic_context = discovery._discover_semantic_layer()

    # Load memory context for prompt injection
    memory_context = memory_store.load_context()

    # Build modular system prompt from templates
    builder = PromptBuilder()
    defaults = agent_config.get("defaults", {})
    system_prompt = builder.build(
        active_profile, context=context, exposure_mode=exposure_mode,
        defaults=defaults, run_metadata=run_metadata,
        semantic_context=semantic_context,
        memory_context=memory_context,
        output_channel=output_channel,
    )

    # Get tools for the active profile
    tools = get_tools_for_profile(
        active_profile,
        disabled_tools=agent_config.get("disabled_tools"),
    )

    # Build subagent specs from the FULL tool list (not profile-filtered).
    # The profile restricts what the parent can call directly, but subagents
    # define their own tool sets and should always be available.
    from seeknal.ask.agents.subagents import get_subagent_specs
    all_tools_for_subagents = get_tools_for_profile("full")
    subagent_specs = get_subagent_specs(
        all_tools=all_tools_for_subagents,
        config=agent_config,
        llm=llm,
    )

    # Create deep agent with planning and auto-summarization.
    # Always use MemorySaver for async compatibility with astream_events.
    # Session metadata is tracked separately by SessionStore.
    checkpointer = MemorySaver()
    thread_id = session_name or "default"

    # Read compaction config from agent settings
    compaction_config = agent_config.get("compaction") if agent_config else None

    agent = _create_agent_graph(
        llm, system_prompt, checkpointer,
        project_path=project_path, tools=tools,
        subagents=subagent_specs or None,
        compaction_config=compaction_config,
    )

    config = {
        "configurable": {"thread_id": thread_id},
        "recursion_limit": 500,
    }
    return agent, config


_SENSITIVE_ENV_PATTERNS = ("api_key", "secret", "token", "password", "credential")


def _get_clean_env() -> dict[str, str]:
    """Return a copy of os.environ with sensitive variables stripped."""
    env = os.environ.copy()
    for key in list(env.keys()):
        if any(s in key.lower() for s in _SENSITIVE_ENV_PATTERNS):
            del env[key]
    return env


def _create_agent_graph(llm, system_prompt: str, checkpointer,
                        project_path=None, tools=None, subagents=None,
                        compaction_config=None):
    """Create the agent graph, trying deepagents first with ReAct fallback."""
    # Lazy import for backward compat — use full tools if none specified
    if tools is None:
        from seeknal.ask.agents.profiles import get_tools_for_profile
        tools = get_tools_for_profile("full")

    try:
        from deepagents import create_deep_agent
        from deepagents.backends.local_shell import LocalShellBackend

        # Use LocalShellBackend for file ops + shell execution.
        # virtual_mode=True enforces path containment for file operations.
        # Shell commands (execute tool) run with sanitized env and timeout.
        backend = None
        if project_path is not None:
            backend = LocalShellBackend(
                root_dir=str(project_path),
                virtual_mode=True,
                timeout=120,
                max_output_bytes=100_000,
                env=_get_clean_env(),
                inherit_env=False,
            )

        kwargs = {}
        if subagents:
            kwargs["subagents"] = subagents

        # Context window compaction — auto-triggered + agent-callable tool
        try:
            from deepagents.middleware.summarization import (
                SummarizationMiddleware,
                SummarizationToolMiddleware,
            )

            comp = compaction_config or {}
            trigger_tokens = comp.get("trigger_tokens", 50000)
            keep_messages = comp.get("keep_messages", 10)

            summarization = SummarizationMiddleware(
                model=llm,
                backend=backend,
                trigger=("tokens", trigger_tokens),
                keep=("messages", keep_messages),
            )
            tool_middleware = SummarizationToolMiddleware(summarization)
            kwargs["middleware"] = [summarization, tool_middleware]
            logger.info(
                "Context compaction enabled: trigger=%d tokens, keep=%d messages",
                trigger_tokens, keep_messages,
            )
        except (ImportError, Exception) as exc:
            logger.debug("Summarization middleware not available: %s", exc)

        return create_deep_agent(
            model=llm,
            tools=tools,
            system_prompt=system_prompt,
            checkpointer=checkpointer,
            backend=backend,
            **kwargs,
        )
    except ImportError:
        # Fallback to ReAct agent if deepagents not installed
        from langchain_core.messages import SystemMessage
        from langgraph.prebuilt import create_react_agent

        return create_react_agent(
            llm,
            tools=tools,
            checkpointer=checkpointer,
            prompt=SystemMessage(content=system_prompt),
        )


_NO_RESPONSE = "No response generated."

# Ralph Loop: max retries when agent returns tool calls but no final text
_MAX_RALPH_RETRIES = 3

# Transient error patterns worth retrying
_RETRYABLE_PATTERNS = ("503", "429", "UNAVAILABLE", "RESOURCE_EXHAUSTED", "timed out")


def _scan_partial_work(project_path: Path) -> str:
    """Scan project for draft files left by a failed subagent.

    Returns a human-readable summary of unapplied drafts, or empty string
    if no partial work is found. Used by Ralph Loop nudges to give the
    main agent context about what the subagent accomplished before failing.
    """
    drafts = sorted(project_path.glob("draft_*.yml")) + sorted(
        project_path.glob("draft_*.py")
    )
    if not drafts:
        return ""
    names = [d.name for d in drafts[:10]]
    suffix = f" (and {len(drafts) - 10} more)" if len(drafts) > 10 else ""
    return (
        f"Partial work found: {names}{suffix}. "
        "Apply remaining drafts with dry_run_draft(file) then "
        "apply_draft(file, confirmed=True). "
        "Create any missing nodes with draft_node + edit_node."
    )


def _invoke_with_retry(agent, messages, config, max_retries=3):
    """Invoke agent with exponential backoff on transient errors."""
    import time

    for attempt in range(max_retries + 1):
        try:
            return agent.invoke(messages, config=config)
        except Exception as e:
            msg = str(e).lower()
            is_retryable = any(kw.lower() in msg for kw in _RETRYABLE_PATTERNS)
            if is_retryable and attempt < max_retries:
                wait = min(2 ** attempt * 5, 60)
                import warnings
                warnings.warn(
                    f"LLM error (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Retrying in {wait}s..."
                )
                time.sleep(wait)
                continue
            raise


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

    The agent decides autonomously what to do (analysis vs build). The harness
    does NOT use keyword matching for intent detection — the system prompt
    handles intent. The only quality check: if the agent ran a pipeline
    without inspecting results, nudge once.

    Args:
        agent: The compiled LangGraph agent.
        config: Agent invocation config (with thread_id).
        question: User's natural language question.

    Returns:
        The agent's text response.
    """
    # Initial invocation
    result = _invoke_with_retry(
        agent,
        {"messages": [HumanMessage(content=question)]},
        config,
    )
    messages = result.get("messages", [])
    response = _extract_response(messages)

    # Quality check: if agent ran pipeline without verification, nudge once.
    # Verification = inspect_output directly OR task(subagent_type="verifier").
    if response:
        tool_names_called = set()
        for msg in messages:
            if not hasattr(msg, "tool_calls"):
                continue
            for tc in msg.tool_calls:
                name = tc.get("name", "")
                tool_names_called.add(name)
                # Track subagent type for task() calls
                if name == "task":
                    args = tc.get("args", {})
                    subagent_type = args.get("subagent_type", "")
                    tool_names_called.add(f"task:{subagent_type}")
        has_run = "run_pipeline" in tool_names_called
        has_verified = (
            "inspect_output" in tool_names_called
            or "task:verifier" in tool_names_called
        )
        if has_run and not has_verified:
            inspect_result = _invoke_with_retry(
                agent,
                {"messages": [HumanMessage(
                    content="The pipeline ran but you did not verify the output. "
                            "Delegate to the verifier subagent via task(), or call "
                            "inspect_output() on 2-3 key nodes to show real data rows."
                )]},
                config,
            )
            inspect_response = _extract_response(inspect_result.get("messages", []))
            if inspect_response:
                return inspect_response

    if response:
        return response

    # Ralph Loop: agent returned no text — nudge to continue/summarize
    for retry_idx in range(_MAX_RALPH_RETRIES):
        partial = ""
        try:
            from seeknal.ask.agents.tools._context import get_tool_context
            partial = _scan_partial_work(get_tool_context().project_path)
        except RuntimeError:
            pass
        base_nudge = (
            "You have not provided a final response. "
            "If you were building a pipeline, continue calling tools until done. "
            "If you were analyzing data, summarize your findings as text."
        )
        nudge = f"{base_nudge}\n\n{partial}" if partial else base_nudge
        result = _invoke_with_retry(
            agent,
            {"messages": [HumanMessage(content=nudge)]},
            config,
        )
        response = _extract_response(result.get("messages", []))
        if response:
            return response

    return _NO_RESPONSE
