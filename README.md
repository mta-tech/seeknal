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

**Feature Store** — Point-in-time joins, automatic versioning, offline and online serving. Powered by DuckDB (single-node, <100M rows) or Apache Spark (distributed).

```python
from seeknal.featurestore.duckdbengine.feature_group import FeatureGroupDuckDB, FeatureLookup, Materialization, HistoricalFeaturesDuckDB
from seeknal.entity import Entity

fg = FeatureGroupDuckDB(
    name="user_features",
    entity=Entity(name="user", join_keys=["user_id"]),
    materialization=Materialization(event_time_col="event_time"),
)
fg.set_dataframe(df).set_features()
fg.write(feature_start_time=datetime(2024, 1, 1))

# Point-in-time join (prevents data leakage)
hist = HistoricalFeaturesDuckDB(lookups=[FeatureLookup(source=fg)])
training_df = hist.to_dataframe(feature_start_time=datetime(2024, 1, 1))
```

**Interactive SQL REPL** — Auto-registers parquets, PostgreSQL, and Iceberg sources at startup. Query pipeline outputs, explore data, iterate on SQL — all without leaving the terminal.

## Documentation

| | |
|---|---|
| **[Getting Started](docs/index.md)** | Installation, configuration, first pipeline |
| **[CLI Reference](docs/reference/cli.md)** | All commands and flags |
| **[YAML Schema](docs/reference/yaml-schema.md)** | Pipeline YAML reference |
| **[CLI Docs Search](docs/cli/docs.md)** | Search documentation from the terminal (`seeknal docs`) |
| **Tutorials** | [YAML Pipelines](docs/tutorials/yaml-pipeline-tutorial.md) · [Python Pipelines](docs/tutorials/python-pipelines-tutorial.md) · [Mixed](docs/tutorials/mixed-yaml-python-pipelines.md) |
| **Guides** | [Python Pipelines](docs/guides/python-pipelines.md) · [Testing & Audits](docs/guides/testing-and-audits.md) · [Iceberg Materialization](docs/iceberg-materialization.md) · [Training to Serving](docs/guides/training-to-serving.md) |
| **Concepts** | [Point-in-Time Joins](docs/concepts/point-in-time-joins.md) · [Virtual Environments](docs/concepts/virtual-environments.md) · [Glossary](docs/concepts/glossary.md) |

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
