---
summary: AI-powered natural language data analysis, chat, reports, and Ask QA
read_when: You want to query data with natural language, generate reports, or validate Ask answers
related:
  - repl
  - run
  - entity
  - source
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
seeknal ask test [OPTIONS]
```

## Description

The `ask` command provides an AI agent that understands your seeknal project — tables, entities, pipelines, connected read-only sources, SQL examples, and project-owned QA tests. It uses thin tools for fast data access and fat skills for multi-step workflows like report generation, pipeline building, data profiling, and database analysis. Skills load on demand via progressive disclosure, keeping the agent's context lean.

Four modes of operation:

1. **One-shot** — Pass a question directly, get an answer
2. **Chat** — Interactive multi-turn session with conversation memory
3. **Report** — Generate interactive HTML dashboards with charts and narratives
4. **Test** — Run project-local prompt-to-SQL QA cases against the real Ask harness

## Prerequisites

Ask is included in the default seeknal installation. Set up an LLM provider:

```bash
# Google Gemini (default)
export GOOGLE_API_KEY="your-api-key"

# Or use Ollama (local, no API key)
ollama serve
```

For pipeline projects, run `seeknal run` before asking about materialized outputs.
For read-only connected-source projects, configure a source with
`seeknal source connect`, run `seeknal source sync`, then ask questions against
the attached database.

## Options

### Global Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--provider`, `-p` | TEXT | `google` | LLM provider: `google`, `ollama` |
| `--model`, `-m` | TEXT | None | Model name override (e.g., `gemini-2.5-pro`, `llama3`) |
| `--project` | PATH | Auto-detected | Project path (auto-loads `<path>/.env`) |
| `--quiet`, `-q` | FLAG | False | Suppress step-by-step output, show only final answer |
| `--web` | FLAG | False | Enable DuckDuckGo web search tools |

### Chat Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--style`, `-s` | TEXT | `concise` | Output style: `concise`, `explanatory`, `formal`, `conversational` |
| `--budget` | FLOAT | None | Max USD budget for this session |
| `--session` | TEXT | None | Resume an existing named session |
| `--name` | TEXT | None | Create a session with this name |

### Report Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--exposure`, `-e` | TEXT | None | Run a predefined report exposure by name |

### Report Serve Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `NAME` | TEXT | Required | Report name (slug) |
| `--port` | INT | 3000 | Dev server port |

### Test Options

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--project` | PATH | Auto-detected | Project path |
| `--select`, `-s` | TEXT | None | Run one test by name or YAML filename |
| `--sql-only` | FLAG | False | Execute expected SQL only; skip the LLM agent |
| `--provider`, `-p` | TEXT | Config default | LLM provider for agent mode |
| `--model`, `-m` | TEXT | Config default | Model override for agent mode |
| `--output-dir` | PATH | Project test outputs | Result JSON directory |
| `--json` | FLAG | False | Print full JSON result |

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

# Chat with named session
seeknal ask chat --name "q1-revenue-analysis"

# Resume a session
seeknal ask chat --session "q1-revenue-analysis"

# Chat with output style and budget cap
seeknal ask chat --style explanatory --budget 5.0

# Enable web search for benchmarks
seeknal ask chat --web

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

### Ask SQL tests

Ask tests are executable project QA assets, inspired by Nao's `tests/*.yml`
pattern.  They are separate from `seeknal/sql_pairs/`: SQL pairs are examples
the agent may read as context, while Ask tests are regression oracles.

Create a YAML file under `seeknal/tests/`, `context/tests/`, or top-level
`tests/`:

```yaml
name: total_revenue
prompt: What is the total revenue from all orders?
sql: |
  SELECT SUM(amount) AS total_revenue
  FROM orders
assert:
  answer_contains:
    - total revenue
```

For stricter agent QA, compare a markdown/JSON table in the agent answer to
the expected SQL rows:

```yaml
name: revenue_by_year
prompt: Show revenue by year as a table
sql: |
  SELECT year, revenue
  FROM revenue_by_year
assert:
  compare: dataframe
  numeric_tolerance: 0.01
```

Run only the SQL oracle:

```bash
seeknal ask test --project . --sql-only
```

Run the real Ask agent and compare the answer with generic assertions and
sampled expected SQL values:

```bash
seeknal ask test --project .
seeknal ask test --project . --select total_revenue
seeknal ask test --project . --json
```

Results are saved to `seeknal/tests/outputs/` when that directory exists, or
`tests/outputs/` for projects using top-level tests.

In interactive Ask chat/TUI, the agent can use thin QA tools to inspect and run
the same project tests:

- `list_ask_tests`
- `read_ask_test`
- `run_ask_test`
- `list_ask_test_results`
- `read_ask_test_result`

This keeps the CLI as the engine and the TUI as the cockpit for investigating
failing SQL or agent-answer tests.


### Teaching the agent in tap-in mode

In read-only connected-source projects, Ask keeps the database read-only but can
write small project-local memory when the user explicitly teaches it something.
Use natural language prompts such as:

```text
Remember: revenue means net_sales, not gross_sales.
Write this down: join products to companies through company_id.
Save this as a SQL pair for AMDK trend by industry scale: ...
```

The agent should choose the lightest durable store:

| User teaching | Stored as | Tool |
|---------------|-----------|------|
| Short rule or preference | `preferences.yml` | `save_preference` |
| Glossary, join pattern, or caveat | `context/*.md` | `write_project_file` |
| Reusable prompt-to-SQL example | `context/sql_pairs/*.yml` | `write_project_file` |

Future sessions can rediscover these notes with `list_context_files`,
`read_project_file`, `list_sql_pairs`, and `read_sql_pair`. The memory tools
reject obvious secrets and connection strings; keep DSNs/API keys in `.env`,
not in project memory.

### SQL pairs for context

SQL pairs are examples the Ask agent can read during normal chat. They are not
pass/fail tests.

Create one reusable prompt-to-SQL example under `seeknal/sql_pairs/`:

```yaml
name: total_revenue
prompt: What is the total revenue?
intent: Compute total revenue from the orders table
sql: |
  SELECT SUM(amount) AS total_revenue
  FROM orders
notes: |
  Use this pattern for total revenue questions. Keep filters explicit.
tags:
  - revenue
```

In chat, the agent can call `list_sql_pairs` and `read_sql_pair` to load the
example before writing SQL. For important questions, keep both files:

- `seeknal/sql_pairs/<name>.yml` — context/pattern for answering
- `seeknal/tests/<name>.yml` — executable regression oracle

### Read-only connected sources

For users who already have analytical tables in PostgreSQL or another supported
database, configure a read-only source instead of creating a pipeline:

```bash
export BPOM_DATABASE_URL="postgresql://user:pass@host/db?sslmode=require"

seeknal source connect bpom \
  --connector postgresql \
  --namespace bpom \
  --dsn-env BPOM_DATABASE_URL \
  --description "BPOM analytical database"

seeknal source sync bpom --project .
seeknal source test bpom --project .
seeknal ask chat --project .
```

The sync command writes derived metadata under `.seeknal/context/sources/`.
During chat, the agent can use `list_source_context` and
`read_source_context` before ad-hoc table probing.

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

The agent has thin tools for fast data access, context lookup, QA, and safe
analysis:

| Tool | Description |
|------|-------------|
| `list_tables` | List all tables/views in DuckDB |
| `describe_table` | Show columns, types, row count, sample values |
| `get_entities` | List all project entities |
| `get_entity_schema` | Show entity schema |
| `execute_sql` | Run read-only DuckDB SQL queries |
| `preview_query` | Estimate row/column size before running a query |
| `execute_python` | Run Python in sandboxed subprocess (pandas, numpy, scipy, matplotlib) |
| `list_context_files` | List user-authored project memory files under `context/` |
| `read_project_file` | Read project/context files safely, excluding secrets |
| `write_project_file` | Save user-taught project memory under `context/` |
| `save_preference` | Save short durable user preferences to `preferences.yml` |
| `list_source_context` | List generated connected-source context files |
| `read_source_context` | Read generated table/source context |
| `list_sql_pairs` | List reusable prompt-to-SQL examples |
| `read_sql_pair` | Read one SQL pair example |
| `list_ask_tests` | List project-local Ask SQL QA tests |
| `read_ask_test` | Read one Ask SQL QA test |
| `run_ask_test` | Run Ask SQL QA tests from chat/TUI |
| `list_ask_test_results` | List saved Ask SQL test run outputs |
| `read_ask_test_result` | Read a saved Ask SQL test run output |
| `read_pipeline` | Read a pipeline YAML/Python definition |
| `search_pipelines` | Search pipeline files by keyword |
| `search_project_files` | Search all project files |
| `read_project_file` | Read any project file |
| `generate_report` | Create an interactive HTML report (Evidence.dev) |
| `save_report_exposure` | Save a report as a YAML exposure for re-runs |
| `profile_data` | Profile CSV/parquet files for schema and quality |
| `query_metric` | Query business metrics from the semantic layer |
| `publish_to_seeknal_report` | Publish a report to the Seeknal Report Server |
| `open_in_browser` | Open a generated report in the browser |

## Built-in Skills

The agent also has built-in skills for multi-step workflows. Skills are loaded on demand — the agent discovers them from frontmatter and loads the full instructions only when needed, keeping context lean:

| Skill | Description |
|-------|-------------|
| `report-generation` | End-to-end Evidence.dev report: exploration, approval gate, build, codification |
| `build-pipeline-node` | Scaffold, validate, apply, and run a new pipeline node |
| `profile-data` | Profile data files for schema, nulls, uniques, join-key candidates |
| `database-analyst` | Explore connected databases, source context, and SQL examples before answering |
| `business-question-answering` | Answer business questions with SQL evidence, assumptions, and follow-up paths |
| `complex-analysis` | Multi-step SQL/Python analysis, statistics, modeling, and visualization |
| `execute-python-analysis` | Statistical/ML/visualization work beyond SQL |
| `query-metric` | Query metrics with automatic joins and time grain resolution |
| `save-metric` | Codify ad-hoc metrics as permanent YAML definitions |
| `save-report-exposure` | Codify analysis as repeatable YAML exposure specs |
| `bootstrap-semantic-model` | Auto-generate semantic model YAML from data files |
| `publish-to-seeknal-report` | Publish reports to the Seeknal Report Server |
| `publish-memo-to-proof` | Publish markdown memos to Proof Editor |
| `edit-proof-document` | Apply rewrites to Proof Editor documents |

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
| Markdown report | `target/reports/{slug}/{date}.md` |

## See Also

- [Seeknal Ask Tutorial](../tutorials/seeknal-ask-agent.md) - Complete tutorial with examples
- [Report Exposures Tutorial](../tutorials/report-exposures.md) - Build deterministic reports
- [Exposures Concept](../concepts/exposures.md) - How exposures connect to the DAG
- [seeknal gateway](gateway.md) - HTTP gateway for web clients and bots
- [seeknal report-server](report-server.md) - Host and share published reports
- [seeknal source](source.md) - Configure connected read-only sources
- [seeknal repl](repl.md) - Interactive SQL REPL
