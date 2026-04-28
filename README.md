<div align="center">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logos/seeknal-mark-dark.svg">
      <source media="(prefers-color-scheme: light)" srcset="docs/assets/logos/seeknal-mark.svg">
      <img src="docs/assets/logos/seeknal-mark-dark.svg" alt="Seeknal" width="96" height="96">
    </picture>
    <h1>Seeknal</h1>
    <p><strong>Transform data with SQL and Python. Build ML features with point-in-time joins. Materialize to PostgreSQL and Iceberg — all from one CLI.</strong></p>
    <p>
        <a href="https://pypi.org/project/seeknal/"><img src="https://img.shields.io/pypi/v/seeknal.svg" alt="PyPI version"></a>
        <a href="https://pypi.org/project/seeknal/"><img src="https://img.shields.io/pypi/pyversions/seeknal.svg" alt="Python versions"></a>
        <a href="LICENSE"><img src="https://img.shields.io/github/license/mta-tech/seeknal.svg" alt="License"></a>
        <a href="https://github.com/mta-tech/seeknal/actions"><img src="https://img.shields.io/github/actions/workflow/status/mta-tech/seeknal/release.yml" alt="CI"></a>
    </p>
</div>

Seeknal is an all-in-one platform for data and AI/ML engineering. Define pipelines in YAML or Python, run them through a safe `draft → dry-run → apply` workflow, and materialize outputs to PostgreSQL and Apache Iceberg simultaneously. Python 3.11+ required.

## Quick Start

```bash
pip install seeknal
# Optional, only for distributed Spark execution:
# pip install "seeknal[spark]"

seeknal init --name my_project
seeknal draft --name my_pipeline --type transform
seeknal dry-run
seeknal apply
```

Explore your data interactively or search docs from the terminal:

```bash
seeknal repl          # Interactive SQL on pipeline outputs
seeknal docs query    # Search documentation from the CLI
```

```sql
SELECT customer_id, COUNT(*) as order_count
FROM target.my_transform
GROUP BY customer_id;
```

## Key Features

**Dual Pipeline Authoring** — Write pipelines in YAML, Python decorators, or both:

```python
from seeknal.pipeline import source, transform

@source(name="orders", source="csv", table="data/orders.csv")
def orders():
    pass

@transform(name="order_metrics", inputs=["source.orders"])
def order_metrics(ctx):
    df = ctx.ref("source.orders")
    return ctx.duckdb.sql(
        "SELECT customer_id, SUM(amount) as total FROM df GROUP BY customer_id"
    ).df()
```

**Multi-Target Materialization** — Write to PostgreSQL and Iceberg from a single node:

```yaml
materializations:
  - type: postgresql
    connection: local_pg
    table: analytics.my_table
    mode: upsert_by_key
    unique_keys: [id]
  - type: iceberg
    table: atlas.namespace.my_table
```

**Environment Management** — Isolated namespaces with per-environment profiles:

```bash
seeknal env plan dev --profile profiles-dev.yml
seeknal env apply dev
seeknal run --env dev
```

**Feature Store** — Define ML features in YAML or Python with entity keys, point-in-time joins, and automatic versioning. Supports offline (batch) and online (real-time) serving.

```yaml
# seeknal/feature_groups/customer_features.yml
kind: feature_group
name: customer_features
entity:
  name: customer
  join_keys: ["customer_id"]
materialization:
  event_time_col: latest_order_date
  offline: { enabled: true, format: parquet }
  online: { enabled: false, ttl: 7d }
features:
  total_orders: { dtype: integer }
  total_spent: { dtype: float }
  avg_order_value: { dtype: float }
inputs:
  - ref: transform.customer_orders
```

```python
# Or use Python decorators
@feature_group(name="customer_rfm", entity="customer")
def customer_rfm(ctx):
    df = ctx.ref("transform.clean_transactions")
    return ctx.duckdb.sql("""
        SELECT CustomerID, COUNT(DISTINCT InvoiceNo) as frequency,
               SUM(TotalAmount) as monetary_value
        FROM df GROUP BY CustomerID
    """).df()
```

```bash
seeknal entity list                           # Cross-feature-group consolidation
seeknal entity show customer                  # Inspect entity schema and feature groups
```

**Interactive SQL REPL** — Auto-registers parquets, PostgreSQL, and Iceberg sources at startup. Query pipeline outputs, explore data, iterate on SQL — all without leaving the terminal.

**AI-Powered Thinking Partner** — `seeknal ask chat` is your collaborative partner for data work. The agent uses thin tools for fast data access and fat skills for multi-step workflows like report generation, pipeline building, database analysis, and data profiling — all loaded on demand to keep responses fast:

```bash
seeknal ask chat                        # Start a brainstorm / build session (interactive TUI)
seeknal ask "What are the top 5 customers by revenue?"  # Quick one-shot question
seeknal ask report "customer analysis"  # Generate interactive HTML dashboard
seeknal ask test --project . --sql-only # Validate project prompt-to-SQL tests
seeknal ask chat --web                  # Enable web search for benchmarks
```

`seeknal ask chat` launches an interactive terminal UI (Bun + React + Ink) with streaming tokens, tool visualization, and arrow-key `ask_user` picker for approval gates. The TUI is bundled inside the wheel; end users do not need Bun or Node. One-shot (`seeknal ask "..."`) and report (`seeknal ask report`) commands use Python-only rendering with no TUI.

Ask it to answer questions against existing read-only databases with `seeknal source connect`, reuse project SQL examples from `seeknal/sql_pairs/`, and validate important questions with executable `seeknal/tests/` QA oracles. Ask it to build a pipeline from scratch, and it will draft a plan, walk you through the design, and wait for your go-ahead before generating code. Publish reports to a self-hosted **Seeknal Report Server** and share them with your team via a URL.

For editable installs (`pip install -e .`), set `SEEKNAL_TUI_BINARY_PATH` to your local TUI build. See `src/seeknal/ask/tui/README.md` for full TypeScript contributing guide and development workflow.

```bash
seeknal report-server start             # Host published reports
seeknal gateway start                   # Expose ask as an API (WebSocket/SSE/REST)
seeknal gateway worker --gateway-url http://gateway:8000 --api-token "$SEEKNAL_API_TOKEN"  # Token-routed Temporal worker
```

Supports Google Gemini (default), OpenAI-compatible providers, Anthropic-compatible providers, and Ollama (local). Use `--provider ollama` for fully local, private analysis.

## Documentation

| | |
|---|---|
| **[Getting Started](docs/index.md)** | Installation, configuration, first pipeline |
| **[CLI Reference](docs/reference/cli.md)** | All commands and flags |
| **[YAML Schema](docs/reference/yaml-schema.md)** | Pipeline YAML reference |
| **[CLI Docs Search](docs/cli/docs.md)** | Search documentation from the terminal (`seeknal docs`) |
| **Tutorials** | [YAML Pipelines](docs/tutorials/yaml-pipeline-tutorial.md) · [Python Pipelines](docs/tutorials/python-pipelines-tutorial.md) · [Mixed](docs/tutorials/mixed-yaml-python-pipelines.md) · [Seeknal Ask Agent](docs/tutorials/seeknal-ask-agent.md) · [Report Exposures](docs/tutorials/report-exposures.md) |
| **Guides** | [Python Pipelines](docs/guides/python-pipelines.md) · [Testing & Audits](docs/guides/testing-and-audits.md) · [Iceberg Materialization](docs/iceberg-materialization.md) · [Training to Serving](docs/guides/training-to-serving.md) |
| **Servers** | [Gateway Server](docs/cli/gateway.md) · [Report Server](docs/cli/report-server.md) |
| **Concepts** | [Point-in-Time Joins](docs/concepts/point-in-time-joins.md) · [Virtual Environments](docs/concepts/virtual-environments.md) · [Exposures](docs/concepts/exposures.md) · [Glossary](docs/concepts/glossary.md) |

## Changelog

### v2.9.0 (April 2026)

**Read-only Ask source harness + project SQL QA** — Adds a TUI-first workflow for users who already have analytical tables in a database and want Seeknal Ask to answer business questions without building a pipeline.

- **Connected-source registry**: `seeknal source connect/status/inspect/sync/test` writes `seeknal_agent.yml`, generates `.seeknal/context/sources/` metadata, and verifies read-only database attachments.
- **SQL pairs for context**: `seeknal/sql_pairs/*.yml` stores prompt-to-SQL examples the Ask agent can discover with `list_sql_pairs` / `read_sql_pair`.
- **Ask SQL tests**: `seeknal ask test` runs project-local prompt-to-SQL QA cases from `seeknal/tests/`, including SQL-only oracle checks and agent-answer checks.
- **TUI QA cockpit**: Ask chat can list, read, run, and inspect Ask tests via thin tools over the same test engine.
- **Structured grading**: Ask tests support `assert.compare: dataframe` for markdown/JSON table comparison against expected SQL rows.
- **Init guidance**: `seeknal init` now scaffolds `AGENTS.md`, `CLAUDE.md`, `seeknal/sql_pairs/`, and `seeknal/tests/` for project-local agent conventions.

### v2.8.0 (April 2026)

**OpenAI/Anthropic providers + SQL safety + context files** — Adds two new LLM provider families, execution guards on `execute_sql`, a pre-execution `preview_query` tool, persistent context files, and durable preferences.

- **OpenAI + Anthropic support**: `gpt-4o`, `claude-*`, Azure OpenAI, Together, Groq, vLLM, LM Studio, and any OpenAI-compatible proxy via `SEEKNAL_ASK_OPENAI_BASE_URL` / `SEEKNAL_ASK_ANTHROPIC_BASE_URL`
- **`execute_sql` guards**: rows capped at 500, columns at 50, per-cell length at 200 chars, 50 KB markdown budget — every truncation emits an actionable notice with accurate total row count
- **`preview_query` tool**: four pre-execution safety probes (row count, column count, JOIN fan-out, dry-run reachability) — blocks queries returning ≥100k rows; pure aggregations auto-skip
- **Context files**: `list_context_files` and `write_project_file` tools scan/write `{project}/context/` with path-traversal guards
- **Durable preferences**: `save_preference` appends to `preferences.yml`; preferences are injected into the system prompt on every session

### v2.7.1 (April 2026)

**Gateway pairing + `execute_uv_script` + pipeline runtime helpers** — Additive batch combining gateway Telegram pairing, a new agent tool for running uv-managed scripts, and lightweight per-node runtime helpers.

- **Gateway pairing**: `FilePairingStore`, `TelegramLinkStore`, `PublicSessionStore` wired into lifespan; `/pair` Telegram command for admin-generated codes
- **`execute_uv_script` tool**: run arbitrary uv-managed Python scripts from the agent with full dependency isolation
- **Pipeline runtime**: `ctx.llm` (Ask-aligned text/JSON generation) and `ctx.state` (lightweight per-node persistent state) helpers available inside `@transform` functions
- **Config discovery**: `find_agent_config_path()` locates `seeknal_agent.yml` under project root or `seeknal/` directory

## Install from Source

For development or contributing:

```bash
git clone https://github.com/mta-tech/seeknal.git
cd seeknal
uv venv --python 3.11 && source .venv/bin/activate
uv pip install -e ".[all]"
```

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, code style, testing, and PR guidelines.

## License

Seeknal is [Apache 2.0 licensed](LICENSE).
