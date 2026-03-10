# Seeknal Ask: AI-Powered Data Analysis Agent

> **Estimated Time:** 15 minutes | **Difficulty:** Intermediate
>
> **Prerequisites:** A seeknal project with data materialized (`seeknal run` has been executed). Install with `pip install seeknal[ask]`.

---

## What You'll Learn

By completing this tutorial, you will:

1. Ask **one-shot questions** about your data in natural language
2. Use **interactive chat** for multi-turn exploratory analysis
3. Understand the **12 built-in tools** the agent uses automatically
4. Configure **LLM providers** (Google Gemini, Ollama)
5. Generate **interactive HTML reports** and **deterministic report exposures**

---

## Overview

`seeknal ask` is an AI-powered agent that understands your seeknal project — tables, entities, pipelines, and code. You ask questions in plain English, and the agent discovers data, writes SQL, runs analysis, and explains results.

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

# With quiet mode
seeknal ask chat -q

# Explicit project path
seeknal ask chat --project ./my-project
```

---

## Part 3: The Agent's Toolbox (5 minutes)

The agent has 12 tools it calls automatically based on your question. You never call these directly — the agent decides when and how to use them.

### Data Discovery

| Tool | What It Does |
|------|-------------|
| `list_tables` | Lists all tables/views available in DuckDB |
| `describe_table` | Shows columns, types, row count, and sample values for a table |
| `get_entities` | Lists all entities defined in the project |
| `get_entity_schema` | Shows the schema for a specific entity |

### Data Analysis

| Tool | What It Does |
|------|-------------|
| `execute_sql` | Runs read-only DuckDB SQL queries (with auto-retry on errors) |
| `execute_python` | Runs Python code in a sandboxed subprocess with `pandas`, `numpy`, `scipy`, and `matplotlib` pre-loaded |

### Project Understanding

| Tool | What It Does |
|------|-------------|
| `read_pipeline` | Reads a pipeline YAML/Python definition file |
| `search_pipelines` | Searches pipeline files by keyword |
| `search_project_files` | Searches all project files (code, configs, YAML) |
| `read_project_file` | Reads any file in the project directory |

### Report Generation

| Tool | What It Does |
|------|-------------|
| `generate_report` | Creates an interactive HTML report using Evidence.dev |
| `save_report_exposure` | Saves a report as a YAML exposure for scheduled re-runs |

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

## Quick Reference

```bash
# One-shot question
seeknal ask "your question here"

# Interactive chat
seeknal ask chat

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
```

---

## Related Topics

- [Report Exposures Tutorial](report-exposures.md) — Build deterministic reports with pinned SQL and charts
- [Exposures Concept](../concepts/exposures.md) — Understand how exposures connect to the DAG
- [CLI Reference](../reference/cli.md) — All commands and flags

---

*Last updated: March 2026 | Seeknal Documentation*
