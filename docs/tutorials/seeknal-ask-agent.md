# Seeknal Ask: AI-Powered Data Analysis Agent

> **Estimated Time:** 20 minutes | **Difficulty:** Intermediate
>
> **Prerequisites:** A seeknal project with data materialized (`seeknal run` has been executed). Install with `pip install seeknal[ask]`.

---

## What You'll Learn

By completing this tutorial, you will:

1. Ask **one-shot questions** about your data in natural language
2. **Build complete pipelines** from natural language descriptions
3. Use **interactive chat** for multi-turn exploratory analysis
4. Understand the **21+ built-in tools** the agent uses automatically
5. Configure **LLM providers** (Google Gemini, Ollama)
6. Generate **interactive HTML reports** and **deterministic report exposures**

---

## Overview

`seeknal ask` is an AI-powered agent that understands your seeknal project — tables, entities, pipelines, and code. You ask questions in plain English, and the agent discovers data, writes SQL, runs analysis, builds pipelines, and explains results.

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

The agent operates in two primary modes:
- **Analysis mode** — Read-only queries and exploration (default for "analyze", "show", "query" keywords)
- **Build mode** — Creates complete data pipelines from natural language (activated by "build", "pipeline", "create" keywords)

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

## Part 2: Building Pipelines (5 minutes)

The agent can build complete data pipelines from a natural language description. Describe what you want — sources, transforms, feature groups, ML models — and the agent handles everything.

```bash
seeknal ask "Build a pipeline from CSV files in data/. \
  Bronze: all CSVs as sources. \
  Silver: enriched_orders (orders JOIN customers with revenue). \
  Gold: revenue_by_segment, product_performance. \
  ML: customer_features (RFM), KMeans segmentation model."
```

### What happens behind the scenes

The agent follows a strict build workflow:

```
1. profile_data()        → discover CSV files, columns, types, join keys
2. draft_node()          → create draft YAML for each node
3. edit_file()           → write real SQL/config into the draft
4. dry_run_draft()       → validate syntax, schema, ref/input consistency
5. apply_draft()         → move to seeknal/ project structure
6. (repeat 2-5 for each node in topological order)
7. plan_pipeline()       → verify DAG structure
8. run_pipeline()        → execute the full pipeline
9. inspect_output()      → show real data from key nodes
```

### Build validation

The agent validates every non-source node before applying:

- **YAML syntax** — valid YAML structure
- **Schema validation** — correct `kind`, required fields
- **SQL correctness** — DuckDB can parse and preview the query
- **ref/input consistency** — every `ref()` in SQL is declared in the `inputs:` list

If validation fails, the agent fixes the issue and re-validates before proceeding.

### Node types

| Type | Description | Example |
|------|-------------|---------|
| `source` | Raw data ingestion (CSV, Parquet) | `source: csv`, `table: "data/orders.csv"` |
| `transform` | SQL transformation with `ref()` dependencies | JOINs, aggregations, CTEs |
| `feature_group` | ML feature store entity | Uses `FROM source` (not `ref()`) |
| `model` | Python ML model (scikit-learn) | KMeans, RandomForest, predict_proba |

### Inspecting results

After the pipeline runs, the agent calls `inspect_output()` to show actual data rows — not conceptual descriptions. The tool supports fuzzy name matching, so `inspect_output("enriched_orders")` finds `transform_enriched_orders.parquet` automatically.

---

## Part 3: Interactive Chat (3 minutes)

> *Analysis and build are also available in chat mode — the agent detects intent from each message.*

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

## Part 4: The Agent's Toolbox (5 minutes)

The agent has 21+ tools organized into three profiles. Tools are loaded based on your question — the agent decides when and how to use them.

### Data Discovery

| Tool | What It Does |
|------|-------------|
| `list_tables` | Lists all tables/views available in DuckDB |
| `describe_table` | Shows columns, types, row count, and sample values for a table |
| `get_entities` | Lists all entities defined in the project |
| `get_entity_schema` | Shows the schema for a specific entity |
| `profile_data` | Discovers CSV files with row counts, column types, and join key candidates |

### Data Analysis

| Tool | What It Does |
|------|-------------|
| `execute_sql` | Runs read-only DuckDB SQL queries (with auto-retry on errors) |
| `execute_python` | Runs Python code in a sandboxed subprocess with `pandas`, `numpy`, `scipy`, and `matplotlib` pre-loaded |
| `inspect_output` | Queries pipeline output parquets directly with fuzzy name matching |

### Pipeline Building

| Tool | What It Does |
|------|-------------|
| `draft_node` | Creates a draft YAML/Python template for a pipeline node |
| `edit_file` | Edits a draft file with string replacement (shows diff preview) |
| `edit_node` | Replaces entire node content |
| `dry_run_draft` | Validates YAML syntax, schema, SQL, and ref/input consistency |
| `apply_draft` | Moves validated draft to `seeknal/` project structure |
| `plan_pipeline` | Shows DAG node count and dependency edges |
| `run_pipeline` | Executes the pipeline (requires confirmation) |

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

For an **analysis** question like "What's the revenue trend by month?":

```
1. list_tables          → discovers available tables
2. describe_table       → checks column names and types
3. execute_sql          → runs the aggregation query
4. (interprets)         → provides insight, not just numbers
```

For a **build** request like "Build a pipeline from CSV files":

```
1. profile_data         → discovers files, columns, types
2. draft_node           → creates source/transform templates
3. edit_file            → writes SQL/config
4. dry_run_draft        → validates before applying
5. apply_draft          → moves to project structure
6. plan_pipeline        → verifies DAG
7. run_pipeline         → executes everything
8. inspect_output       → shows real data rows
```

For **statistical** questions, the agent adds Python:

```
1. execute_sql          → pulls raw data
2. execute_python       → runs scipy stats, creates matplotlib charts
3. (interprets)         → explains statistical significance
```

---

## Part 5: LLM Providers (2 minutes)

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

## Part 6: Report Generation (2 minutes)

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
# One-shot analysis
seeknal ask "your question here"

# One-shot pipeline build
seeknal ask "Build a pipeline from CSV files in data/..."

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
