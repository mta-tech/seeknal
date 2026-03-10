<div align="center">
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

**AI-Powered Data Agent** — Ask questions in natural language, get SQL-backed answers with actionable insights. 12 built-in tools for data discovery, analysis, Python execution, and report generation:

```bash
seeknal ask "What are the top 5 customers by revenue?"
seeknal ask chat                        # Multi-turn interactive session
seeknal ask report "customer analysis"  # Generate interactive HTML dashboard
seeknal ask report --exposure monthly_kpis  # Run deterministic report exposure
```

Supports Google Gemini (default) and Ollama (local) as LLM providers. Use `--provider ollama` for fully local, private analysis.

## Documentation

| | |
|---|---|
| **[Getting Started](docs/index.md)** | Installation, configuration, first pipeline |
| **[CLI Reference](docs/reference/cli.md)** | All commands and flags |
| **[YAML Schema](docs/reference/yaml-schema.md)** | Pipeline YAML reference |
| **[CLI Docs Search](docs/cli/docs.md)** | Search documentation from the terminal (`seeknal docs`) |
| **Tutorials** | [YAML Pipelines](docs/tutorials/yaml-pipeline-tutorial.md) · [Python Pipelines](docs/tutorials/python-pipelines-tutorial.md) · [Mixed](docs/tutorials/mixed-yaml-python-pipelines.md) · [Seeknal Ask Agent](docs/tutorials/seeknal-ask-agent.md) · [Report Exposures](docs/tutorials/report-exposures.md) |
| **Guides** | [Python Pipelines](docs/guides/python-pipelines.md) · [Testing & Audits](docs/guides/testing-and-audits.md) · [Iceberg Materialization](docs/iceberg-materialization.md) · [Training to Serving](docs/guides/training-to-serving.md) |
| **Concepts** | [Point-in-Time Joins](docs/concepts/point-in-time-joins.md) · [Virtual Environments](docs/concepts/virtual-environments.md) · [Exposures](docs/concepts/exposures.md) · [Glossary](docs/concepts/glossary.md) |

## Changelog

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
