# Seeknal Ask: AI-Powered Data Analysis Agent

> **Estimated Time:** 15 minutes | **Difficulty:** Intermediate
>
> **Prerequisites:** A seeknal project with data materialized (`seeknal run` has been executed). Ask is included in the default seeknal installation.

---

## What You'll Learn

By completing this tutorial, you will:

1. Ask **one-shot questions** about your data in natural language
2. Use **interactive chat** for multi-turn exploratory analysis
3. Understand the **16 tools + 11 skills** the agent uses automatically
4. Configure **LLM providers** (Google Gemini, Ollama)
5. Generate **interactive HTML reports** and **deterministic report exposures**
6. **Publish reports** to the Seeknal Report Server for sharing

---

## Overview

`seeknal ask` is an AI-powered agent that understands your seeknal project — tables, entities, pipelines, and code. It uses 16 thin tools for fast data access and 11 built-in skills for multi-step workflows. You ask questions in plain English, and the agent discovers data, writes SQL, runs analysis, and explains results.

```
You: "What are the top 5 customers by revenue?"

Agent: [list_tables] → [describe_table] → [execute_sql] →

| customer_id | total_revenue |
|-------------|---------------|
| C0041       | 4,523.80      |
| C0026       | 3,297.83      |
| ...         | ...           |

Customer C0041 leads with $4,523.80 in total spend across 6 orders...
```

The agent works by chaining tools together. It decides which tools to call, interprets results, retries on errors, and provides actionable insights — not just raw numbers.

---

## Part 1: One-Shot Questions (3 minutes)

The simplest way to use Seeknal Ask — pass your question directly:

```bash
seeknal ask "How many customers do I have?"
```

The agent will:

1. Auto-detect your project directory
2. Discover available tables with `list_tables`
3. Write and execute a SQL query
4. Return a clear answer with context

### More examples

```bash
# Aggregations
seeknal ask "What is the average order value by month?"

# Comparisons
seeknal ask "Which city has the highest revenue?"

# Lineage questions
seeknal ask "How is the orders_cleaned transform defined?"

# Data quality
seeknal ask "Are there any null values in the customer_id column?"
```

### Specifying a project

If you're not inside a seeknal project directory:

```bash
seeknal ask --project /path/to/my/project "How many orders?"
```

### Quiet mode

Suppress step-by-step tool output and show only the final answer:

```bash
seeknal ask -q "Total revenue last quarter"
```

---

## Part 2: Interactive Chat (3 minutes)

For exploratory analysis where one question leads to the next, use chat mode:

```bash
seeknal ask chat
```

This starts a multi-turn session where the agent remembers context:

```
You: Show me a summary of the orders table
Agent: [describes table, shows row count, column types]

You: What's the distribution of order amounts?
Agent: [runs SQL with percentiles and histogram buckets]

You: Break that down by customer segment
Agent: [joins with customers table, segments the distribution]
```

Type `exit`, `quit`, or press `Ctrl-C` to end the session.

### Chat with options

```bash
# Use a specific provider
seeknal ask chat --provider ollama --model llama3

# Named sessions — resume later
seeknal ask chat --name "q1-analysis"
seeknal ask chat --session "q1-analysis"

# Output style and budget cap
seeknal ask chat --style explanatory --budget 5.0

# Enable web search
seeknal ask chat --web

# With quiet mode
seeknal ask chat -q

# Explicit project path
seeknal ask chat --project ./my-project
```

---

## Part 3: The Agent's Toolbox (5 minutes)

The agent uses a **thin tools + fat skills** architecture. 16 thin tools handle fast, atomic operations. 11 built-in skills provide multi-step workflows that load on demand. You never call these directly — the agent decides when and how to use them.

### Thin Tools (always available)

#### Data Discovery

| Tool | What It Does |
|------|-------------|
| `list_tables` | Lists all tables/views available in DuckDB |
| `describe_table` | Shows columns, types, row count, and sample values for a table |
| `get_entities` | Lists all entities defined in the project |
| `get_entity_schema` | Shows the schema for a specific entity |
| `profile_data` | Profiles CSV/parquet files for schema, nulls, uniques, and join-key candidates |

#### Data Analysis

| Tool | What It Does |
|------|-------------|
| `execute_sql` | Runs read-only DuckDB SQL queries (with auto-retry on errors) |
| `execute_python` | Runs Python code in a sandboxed subprocess with `pandas`, `numpy`, `scipy`, and `matplotlib` pre-loaded |
| `execute_uv_script` | Runs prototype Python scripts through `uv run` with custom PEP 723 dependencies |
| `query_metric` | Queries business metrics from the semantic layer |

#### Project Understanding

| Tool | What It Does |
|------|-------------|
| `read_pipeline` | Reads a pipeline YAML/Python definition file |
| `search_pipelines` | Searches pipeline files by keyword |
| `search_project_files` | Searches all project files (code, configs, YAML) |
| `read_project_file` | Reads any file in the project directory |

#### Reporting & Publishing

| Tool | What It Does |
|------|-------------|
| `generate_report` | Creates an interactive HTML report using Evidence.dev |
| `save_report_exposure` | Saves a report as a YAML exposure for scheduled re-runs |
| `publish_to_seeknal_report` | Publishes a built report to the Seeknal Report Server |
| `open_in_browser` | Opens a generated report in the browser |

### Built-in Skills (loaded on demand)

Skills are multi-step workflows that the agent discovers and loads when needed. They provide detailed instructions for complex tasks while keeping the agent's baseline context lean:

| Skill | What It Does |
|-------|-------------|
| `report-generation` | End-to-end Evidence.dev report: data exploration, draft approval, build, and codification |
| `build-pipeline-node` | Scaffold, validate, apply, and run a new pipeline node from natural language |
| `profile-data` | Comprehensive data profiling with quality assessment and join-key detection |
| `execute-python-analysis` | Guided statistical/ML/visualization work in the Python sandbox |
| `query-metric` | Semantic layer metric queries with automatic joins and time grain resolution |
| `save-metric` | Codify ad-hoc metric queries as permanent YAML definitions |
| `save-report-exposure` | Codify completed analyses as repeatable YAML exposure specs |
| `bootstrap-semantic-model` | Auto-generate semantic model YAML from CSV/parquet files |
| `publish-to-seeknal-report` | Guided publishing workflow to the Seeknal Report Server |
| `publish-memo-to-proof` | Publish markdown memos to Proof Editor with shareable URLs |
| `edit-proof-document` | Apply full-document rewrites to Proof Editor documents |

### How tools chain together

For a question like "What's the revenue trend by month?", the agent typically:

```
1. list_tables          → discovers available tables
2. describe_table       → checks column names and types
3. execute_sql          → runs the aggregation query
4. (interprets)         → provides insight, not just numbers
```

For statistical questions, the agent adds Python:

```
1. execute_sql          → pulls raw data
2. execute_python       → runs scipy stats, creates matplotlib charts
3. (interprets)         → explains statistical significance
```

---

## Part 4: LLM Providers (2 minutes)

Seeknal Ask supports two LLM providers:

### Google Gemini (default)

Set your API key:

```bash
export GOOGLE_API_KEY="your-api-key"
```

The default model is `gemini-2.0-flash`. Override with `--model`:

```bash
seeknal ask --model gemini-2.5-pro "Complex analysis question"
```

### Ollama (local)

Run models locally with no API key required:

```bash
# Start Ollama first
ollama serve

# Use with seeknal
seeknal ask --provider ollama "How many orders?"
seeknal ask --provider ollama --model llama3 "Revenue by month"
```

### Provider comparison

| | Google Gemini | Ollama |
|---|---|---|
| **Setup** | API key required | Local install, no key |
| **Speed** | Fast (cloud) | Depends on hardware |
| **Cost** | Pay per token | Free |
| **Privacy** | Data sent to Google | Fully local |
| **Best for** | Production, complex analysis | Development, sensitive data |

---

## Part 5: Report Generation (2 minutes)

Beyond one-shot questions, the agent can create full interactive reports.

### AI-guided reports

Let the agent explore your data and build a report:

```bash
seeknal ask report "customer segmentation analysis"
```

The agent will ask scoping questions, run analyses, and generate an Evidence.dev HTML dashboard.

### Deterministic report exposures

For recurring reports with pinned SQL and charts, define a YAML exposure:

```bash
seeknal ask report --exposure monthly_kpis
```

This runs a predefined report from `seeknal/exposures/`. See the [Report Exposures Tutorial](report-exposures.md) for a complete walkthrough.

### Managing reports

```bash
# List all generated reports
seeknal ask report list

# Preview a report with live reload
seeknal ask report serve my-report
```

---

## Part 6: Publishing Reports (2 minutes)

After generating a report, you can publish it to a **Seeknal Report Server** and share the URL with your team.

### Start the Report Server

```bash
pip install seeknal[report-server]
seeknal report-server start
```

### Configure your project

Add to your project's `.env`:

```bash
SEEKNAL_PUBLISH_SERVER=http://localhost:8787
SEEKNAL_PUBLISH_TOKEN=your-server-key
```

### Publish from chat

After a report is built, the chat TUI offers a "Publish to Seeknal Report Server" menu option. You can also ask the agent directly:

```
You: publish this report to the report server
Agent: [publish_to_seeknal_report] → Published! URL: http://localhost:8787/r/abc123
```

### Expose as an API

For web clients and bots, use the gateway:

```bash
seeknal gateway start                    # WebSocket + SSE + REST
seeknal gateway start --telegram         # With Telegram bot
```

---

## Quick Reference

```bash
# One-shot question
seeknal ask "your question here"

# Interactive chat
seeknal ask chat

# Named sessions
seeknal ask chat --name "my-analysis"
seeknal ask chat --session "my-analysis"

# Chat options
seeknal ask chat --style explanatory --budget 5.0 --web

# Quiet mode (final answer only)
seeknal ask -q "your question"

# Specify provider and model
seeknal ask --provider ollama --model llama3 "question"

# Generate interactive report
seeknal ask report "topic"

# Run deterministic exposure
seeknal ask report --exposure exposure_name

# List and serve reports
seeknal ask report list
seeknal ask report serve report-name

# Report Server
seeknal report-server start

# Gateway (API)
seeknal gateway start
```

---

## Related Topics

- [Report Exposures Tutorial](report-exposures.md) — Build deterministic reports with pinned SQL and charts
- [Exposures Concept](../concepts/exposures.md) — Understand how exposures connect to the DAG
- [Gateway CLI](../cli/gateway.md) — HTTP gateway for web clients and bots
- [Report Server CLI](../cli/report-server.md) — Host and share published reports
- [CLI Reference](../reference/cli.md) — All commands and flags

---

*Last updated: April 2026 | Seeknal Documentation*
