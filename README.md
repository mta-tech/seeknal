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

**AI-Powered Thinking Partner** — `seeknal ask chat` is your collaborative partner for data work. The agent uses 16 tools for fast data access and 11 built-in skills for multi-step workflows like report generation, pipeline building, and data profiling — all loaded on demand to keep responses fast:

```bash
seeknal ask chat                        # Start a brainstorm / build session
seeknal ask "What are the top 5 customers by revenue?"  # Quick one-shot question
seeknal ask report "customer analysis"  # Generate interactive HTML dashboard
seeknal ask chat --web                  # Enable web search for benchmarks
```

Ask it to build a pipeline from scratch, and it will draft a plan, walk you through the design, and wait for your go-ahead before generating code. Publish reports to a self-hosted **Seeknal Report Server** and share them with your team via a URL.

```bash
seeknal report-server start             # Host published reports
seeknal gateway start                   # Expose ask as an API (WebSocket/SSE/REST)
```

Supports Google Gemini (default) and Ollama (local). Use `--provider ollama` for fully local, private analysis.

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

### v2.6.0 (April 2026)

**Skills-Powered Agent + Report Server** — The ask agent now uses a thin-tools/fat-skills architecture: 16 lean tools for fast data access, 11 built-in skills for multi-step workflows (reports, pipelines, profiling, metrics, publishing). Skills load on demand via progressive disclosure, keeping the agent's context lean.

- **Seeknal Report Server** (`seeknal report-server start`): self-hosted server for publishing and sharing reports via unique URLs — publish from the chat TUI or the agent tool
- **11 built-in skills**: report generation, pipeline building, data profiling, Python analysis, semantic model bootstrap, metric query/save, report exposure codification, Proof Editor publishing
- **Chat enhancements**: `--style` (concise/explanatory/formal/conversational), `--budget` (USD cap), `--web` (DuckDuckGo search), `--session`/`--name` (named session resume)
- **Gateway improvements**: cloud-only backend mode, standalone workers, Redis multi-replica, split topology
- **Auto `.env` loading**: `--project <path>` loads `<path>/.env` automatically
- **Error UX**: network errors classified with actionable hints; error logs saved to `~/.seeknal/logs/`

### v2.5.0 (April 2026)

**Seeknal as Your Thinking Partner** — `seeknal ask chat` is now a collaborative partner that brainstorms, builds pipelines, and trains models with you through conversation. It always asks for confirmation before acting — you stay in control.

- **Interactive chat mode** (`seeknal ask chat`): multi-turn brainstorm and build sessions with persistent history, streaming UI with Claude Code-inspired visual hierarchy
- **Confirmation-first workflow**: the agent proposes plans and analysis directions, then waits for your go-ahead via interactive menus before executing
- **Pipeline and ML building**: describe what you want to build in plain language — the agent drafts YAML pipelines, feature groups, or model training code and checks in before generating
- **Session management**: create, resume, list, and delete sessions with full message persistence (`seeknal session list/show/delete`)
- **Iceberg REST catalog support**: integrates with any Iceberg REST catalog provider (Lakekeeper, Tabular, Polaris, etc.)
- **Gateway server**: WebSocket, SSE, and REST endpoints for web clients; optional Telegram bot integration
- **UI refresh**: animated fox mascot, interactive arrow-key menus, real token/tool counters, subordinate reasoning display

### v2.4.0 (March 2026)

**Seeknal Ask — AI-Powered Data Agent** — Natural language data analysis with 12 built-in tools:

```bash
seeknal ask "What are the top 5 customers by revenue?"
seeknal ask chat                                        # Interactive multi-turn session
seeknal ask report "customer segmentation"              # AI-guided HTML dashboard
seeknal ask report --exposure monthly_kpis              # Deterministic report exposure
seeknal ask report serve my-report                      # Live-preview with Evidence dev server
```

- **One-shot & chat modes**: Ask questions or start multi-turn sessions with conversation memory
- **12 agent tools**: Data discovery, SQL execution, Python analysis (pandas/scipy/matplotlib), pipeline inspection, and report generation
- **Report exposures**: Define repeatable reports in YAML with pinned SQL queries, chart types (BigValue, BarChart, LineChart, AreaChart, DataTable), and LLM-generated narratives
- **Deterministic reports**: `sections` key pins SQL and charts — LLM only writes commentary
- **Dual output**: Both interactive HTML dashboards and standalone Markdown reports
- **LLM providers**: Google Gemini (default) and Ollama (local, no API key)
- **Subprocess sandbox**: Python execution runs in isolated subprocess with restricted imports

### v2.3.0 (March 2026)

**Incremental Detection** — Automatically skip unchanged data sources and process only new data:

```yaml
# PostgreSQL watermark-based incremental detection
- kind: source
  name: events
  source: postgresql
  table: public.events
  freshness:
    time_column: created_at  # Tracks MAX(created_at) watermark
  params:
    connection: my_pg
```

- **PostgreSQL Incremental**: Watermark-based detection using `MAX(time_column)` comparison. Automatically generates `WHERE time_col > 'watermark' OR time_col IS NULL` for incremental reads.
- **Iceberg Incremental**: Snapshot-based detection comparing current snapshot ID. Supports partition pruning for time-partitioned tables.
- **Skip Optimization**: If fingerprint and watermark match, source execution is skipped entirely.
- **Cascade Invalidation**: Dependent nodes are automatically invalidated when source data changes.
- **Full Refresh**: Use `--full` flag to ignore stored watermarks and reload all data.

**Other Changes**:
- Enhanced QA automation with multi-spec execution support
- Pipeline error logging with `--verbose` mode
- Security fix: Updated `cryptography` to 46.0.5 (CVE-2026-26007)

### v2.2.2 (February 2026)

- Entity consolidation for per-entity feature views
- Multi-target materialization (PostgreSQL + Iceberg from single node)
- Environment-aware execution with namespace prefixing

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
