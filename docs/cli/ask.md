---
summary: AI-powered natural language data analysis, chat, and report generation
read_when: You want to query data with natural language or generate reports
related:
  - repl
  - run
  - entity
---

# seeknal ask

AI-powered data analysis agent. Ask questions about your seeknal project data in natural language, start interactive chat sessions, and generate interactive HTML reports.

## Synopsis

```bash
seeknal ask [QUESTION] [OPTIONS]
seeknal ask chat [OPTIONS]
seeknal ask report [TOPIC] [OPTIONS]
seeknal ask report --exposure NAME [OPTIONS]
seeknal ask report serve NAME [OPTIONS]
seeknal ask report list [OPTIONS]
```

## Description

The `ask` command provides an AI agent that understands your seeknal project — tables, entities, pipelines, and code. It uses 12 built-in tools to discover data, write SQL, run Python analysis, and explain results.

Three modes of operation:

1. **One-shot** — Pass a question directly, get an answer
2. **Chat** — Interactive multi-turn session with conversation memory
3. **Report** — Generate interactive HTML dashboards with charts and narratives

## Prerequisites

Install the ask dependencies:

```bash
pip install seeknal[ask]
```

Set up an LLM provider:

```bash
# Google Gemini (default)
export GOOGLE_API_KEY="your-api-key"

# Or use Ollama (local, no API key)
ollama serve
```

Your project must have data materialized (`seeknal run` has been executed).

## Options

### Global Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--provider`, `-p` | TEXT | `google` | LLM provider: `google`, `ollama` |
| `--model`, `-m` | TEXT | None | Model name override (e.g., `gemini-2.5-pro`, `llama3`) |
| `--project` | PATH | Auto-detected | Project path |
| `--quiet`, `-q` | FLAG | False | Suppress step-by-step output, show only final answer |

### Report Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--exposure`, `-e` | TEXT | None | Run a predefined report exposure by name |

### Report Serve Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `NAME` | TEXT | Required | Report name (slug) |
| `--port` | INT | 3000 | Dev server port |

## Examples

### One-shot questions

```bash
# Simple aggregation
seeknal ask "How many customers do I have?"

# Analysis
seeknal ask "What is the average order value by month?"

# Lineage question
seeknal ask "How is the orders_cleaned transform defined?"

# Quiet mode — only the final answer
seeknal ask -q "Total revenue last quarter"

# Specify project path
seeknal ask --project /path/to/project "How many orders?"
```

### Interactive chat

```bash
# Start a chat session
seeknal ask chat

# Chat with a specific provider
seeknal ask chat --provider ollama --model llama3

# Chat with quiet mode
seeknal ask chat -q
```

In chat mode, type `exit`, `quit`, or press `Ctrl-C` to end the session.

### Report generation

```bash
# AI-guided report — the agent explores data and builds a dashboard
seeknal ask report "customer segmentation analysis"

# Deterministic report — run a predefined YAML exposure
seeknal ask report --exposure monthly_kpis

# List existing reports
seeknal ask report list

# Live-preview a report with Evidence dev server
seeknal ask report serve my-report
seeknal ask report serve my-report --port 8080
```

### Provider selection

```bash
# Use Google Gemini (default)
seeknal ask "revenue by month"

# Use a specific Gemini model
seeknal ask --model gemini-2.5-pro "complex analysis question"

# Use Ollama (local, no API key)
seeknal ask --provider ollama "How many orders?"
seeknal ask --provider ollama --model llama3 "Revenue by month"
```

## Agent Tools

The agent has 12 tools it calls automatically based on your question:

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables/views in DuckDB |
| `describe_table` | Show columns, types, row count, sample values |
| `get_entities` | List all project entities |
| `get_entity_schema` | Show entity schema |
| `execute_sql` | Run read-only DuckDB SQL queries |
| `execute_python` | Run Python in sandboxed subprocess (pandas, numpy, scipy, matplotlib) |
| `read_pipeline` | Read a pipeline YAML/Python definition |
| `search_pipelines` | Search pipeline files by keyword |
| `search_project_files` | Search all project files |
| `read_project_file` | Read any project file |
| `generate_report` | Create an interactive HTML report (Evidence.dev) |
| `save_report_exposure` | Save a report as a YAML exposure for re-runs |

## Report Exposures

Report exposures are YAML files in `seeknal/exposures/` that define repeatable reports:

```yaml
kind: exposure
name: monthly_kpis
type: report
params:
  prompt: "Analyze monthly business performance..."
  format: both
inputs:
  - ref: transform.monthly_revenue
sections:
  - title: Revenue Overview
    queries:
      - name: total_revenue
        sql: "SELECT SUM(revenue) as revenue FROM transform_monthly_revenue"
        chart: BigValue
        value: [revenue]
```

Reports with `sections` run in **deterministic mode** — you control the SQL and charts, the LLM only writes narrative commentary.

Reports without `sections` run in **AI-guided mode** — the LLM explores data and decides what to analyze.

## Output

| Output | Location |
|--------|----------|
| HTML dashboard | `target/reports/{slug}/build/index.html` |
| Markdown report | `target/reported/{slug}/{date}.md` |

## See Also

- [Seeknal Ask Tutorial](../tutorials/seeknal-ask-agent.md) - Complete tutorial with examples
- [Report Exposures Tutorial](../tutorials/report-exposures.md) - Build deterministic reports
- [Exposures Concept](../concepts/exposures.md) - How exposures connect to the DAG
- [seeknal repl](repl.md) - Interactive SQL REPL
