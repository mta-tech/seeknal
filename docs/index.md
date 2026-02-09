# Seeknal Documentation

Seeknal is an all-in-one platform for data and AI/ML engineering — combine feature store capabilities, YAML-defined data pipelines, and multi-engine processing (DuckDB + Spark) in a single CLI tool.

## Choose Your Path

| I am a... | Start here | Then explore |
|-----------|-----------|--------------|
| **New user** | [Installation & Quick Start](getting-started-comprehensive.md) | [DuckDB Quick Start](duckdb-getting-started.md) |
| **Data Engineer** | [YAML Pipeline Tutorial](tutorials/yaml-pipeline-tutorial.md) | [Virtual Environments](tutorials/phase2-data-eng-environments.md) |
| **Analytics Engineer** | [YAML Pipeline Tutorial](tutorials/yaml-pipeline-tutorial.md) | [Change Categorization](tutorials/phase2-analytics-eng-metrics.md) · [Semantic Layer](guides/semantic-layer.md) |
| **ML Engineer** | [Getting Started](getting-started-comprehensive.md) | [Python Pipelines](tutorials/python-pipelines-tutorial.md) · [Parallel Execution](tutorials/phase2-ml-eng-parallel.md) |

## Concepts

Learn the mental model behind Seeknal.

- [Glossary](concepts/glossary.md) — Definitions of all key terms
- [Point-in-Time Joins](concepts/point-in-time-joins.md) — Prevent data leakage in ML features
- [Virtual Environments](concepts/virtual-environments.md) — Isolated workspaces for safe development
- [Change Categorization](concepts/change-categorization.md) — BREAKING, NON_BREAKING, and METADATA changes
- [Python API vs YAML Workflows](concepts/python-vs-yaml.md) — Choose the right paradigm

## Guides

Task-oriented walkthroughs for specific workflows.

- [Testing & Audits](guides/testing-and-audits.md) — Data quality validation with `seeknal audit`
- [Semantic Layer & Metrics](guides/semantic-layer.md) — Define and query metrics with `seeknal query`
- [Training to Serving](guides/training-to-serving.md) — End-to-end ML feature workflow
- [Seeknal vs dbt vs SQLMesh vs Feast](guides/comparison.md) — Feature comparison

## Reference

Lookup documentation for commands, schemas, and configuration.

- [CLI Commands](reference/cli.md) — All 35+ commands with flags and examples
- [YAML Schema](reference/yaml-schema.md) — Every field for all node kinds
- [Configuration](reference/configuration.md) — Project files, profiles, and environment variables
- [Python API](api/index.md) — Module reference

## Tutorials

Step-by-step learning paths with copy-pasteable code.

- [YAML Pipeline Tutorial](tutorials/yaml-pipeline-tutorial.md) — Build a complete pipeline from scratch (75 min)
- [Python Pipelines](tutorials/python-pipelines-tutorial.md) — Write Python feature transforms (45 min)
- [Mixed YAML + Python](tutorials/mixed-yaml-python-pipelines.md) — Combine both paradigms (60 min)
- [Virtual Environments](tutorials/phase2-data-eng-environments.md) — Safe development with environments (45 min)
- [Parallel Execution](tutorials/phase2-ml-eng-parallel.md) — Speed up large pipelines (45 min)
- [Change Categorization](tutorials/phase2-analytics-eng-metrics.md) — Understand change impact (20 min)
- [E-Commerce Walkthrough](tutorials/workflow-tutorial-ecommerce.md) — Real-world example

## Additional Resources

- [DuckDB Getting Started](duckdb-getting-started.md) — DuckDB engine quickstart
- [DuckDB Flow Guide](duckdb-flow-guide.md) — DuckDB flow patterns
- [Spark Transformers Reference](spark-transformers-reference.md) — Spark-specific reference
- [Iceberg Materialization](iceberg-materialization.md) — Apache Iceberg integration
- [DAGRunner Documentation](workflows/runner.md) — Workflow runner internals

## Quick Links

<div class="grid cards" markdown>

-   :material-code-braces: **API Reference**

    ---

    Explore the complete API documentation for all Seeknal modules.

    [:octicons-arrow-right-24: API Reference](api/index.md)

-   :material-book-open-variant: **Examples**

    ---

    Learn through practical code examples demonstrating common patterns.

    [:octicons-arrow-right-24: Examples](examples/index.md)

-   :material-database: **Iceberg Materialization**

    ---

    Persist pipeline outputs to Apache Iceberg tables with ACID transactions and time travel.

    [:octicons-arrow-right-24: Iceberg Materialization](iceberg-materialization.md)

</div>
