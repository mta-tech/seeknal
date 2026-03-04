# Welcome to Seeknal

> **Build data pipelines and ML features in minutes, not days.**

Seeknal is an all-in-one platform for data and AI/ML engineering. Define transformations in YAML or Python, run them with DuckDB or Spark, and deploy features to production with a single CLI.

---

## What Can You Build With Seeknal?

| For ML Engineers | For Data Engineers | For Analytics Engineers |
|------------------|-------------------|-------------------------|
| **Feature stores** with point-in-time correctness | **ELT pipelines** with incremental execution | **Semantic layers** with consistent metrics |
| **Training datasets** from raw events | **Data transformations** with SQL | **Business metrics** with change tracking |
| **Online serving** for real-time inference | **Multi-engine workflows** (DuckDB + Spark) | **Self-serve analytics** for stakeholders |

**Common use cases:** Recommendation systems, churn prediction, customer segmentation, real-time dashboards, A/B test analysis, fraud detection.

---

## Get Started in 10 Minutes

**[→ Quick Start Guide](getting-started-comprehensive.md)**

1. Install Seeknal (`pip install seeknal` or from [GitHub Releases](https://github.com/mta-tech/seeknal/releases))
2. Load your data (CSV, Parquet, database)
3. Transform with SQL
4. Run your first pipeline

No infrastructure required. Works on your laptop.

---

## How Seeknal Works: The Pipeline Builder

Seeknal's workflow is inspired by modern infrastructure tools like `terraform` and `kubectl`:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│     init        │ →   │     draft       │ →   │     apply       │ →   │   run --env     │
│  (setup project)│     │  (write YAML)   │     │  (save changes) │     │  (execute)      │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Step-by-Step

1. **`seeknal init`** - Create a new project
2. **`seeknal draft`** - Generate YAML templates for sources, transforms, feature groups
3. **`seeknal apply`** - Save your pipeline definition (like `git commit`)
4. **`seeknal run --env prod`** - Execute in production with safety checks

**Key benefits:** Dry-run validation, change detection, rollback support, multi-environment support, and multi-target materialization (PostgreSQL + Iceberg).

---

## Choose Your Learning Path

Seeknal supports different workflows depending on your role and goals.

### 🆕 New to Seeknal?

Start here if you're evaluating or just getting started:

**[→ Quick Start Guide (10 min)](getting-started-comprehensive.md)**

Install, load data, create features, and run your first pipeline.

---

### 🏗️ Data Engineer Path

**[→ Start Data Engineer Path](getting-started/data-engineer-path/)**

**Goal:** Build reliable ELT pipelines with incremental execution and production safety.

**Start with:** [YAML Pipeline Tutorial (75 min)](tutorials/yaml-pipeline-tutorial.md)

**Then learn:**
- [Environment Management](tutorials/environment-management.md) - Safe development with isolated environments
- [Incremental Models](concepts/second-order-aggregations.md) - Efficient incremental processing
- [Change Categorization](concepts/change-categorization.md) - Understand breaking vs. non-breaking changes

**Typical use case:** "I need to transform raw data into analytics-ready tables, incrementally, with production safety."

---

### 📊 Analytics Engineer Path

**[→ Start Analytics Engineer Path](getting-started/analytics-engineer-path/)**

**Goal:** Define metrics and build a semantic layer for self-serve analytics.

**Start with:** [YAML Pipeline Tutorial (75 min)](tutorials/yaml-pipeline-tutorial.md)

**Then learn:**
- [Semantic Layer & Metrics](guides/semantic-layer.md) - Define and query consistent metrics
- [Change Categorization](tutorials/metrics-change-tracking.md) - Track metric changes over time
- [Testing & Audits](guides/testing-and-audits.md) - Validate data quality

**Typical use case:** "I need consistent metrics across dashboards and tools, with change tracking."

---

### 🤖 ML Engineer Path

**[→ Start ML Engineer Path](getting-started/ml-engineer-path/)**

**Goal:** Build feature stores with point-in-time joins for ML models.

**Start with:** [Getting Started (30 min)](getting-started-comprehensive.md)

**Then learn:**
- [Python Pipelines](tutorials/python-pipelines-tutorial.md) - Feature engineering with Python
- [Training to Serving](guides/training-to-serving.md) - End-to-end ML workflow
- [Parallel Execution](tutorials/parallel-execution.md) - Speed up large pipelines

**Typical use case:** "I need features for training that prevent data leakage, with online serving."

---

## Concepts

Learn the mental model behind Seeknal.

- [Glossary](concepts/glossary.md) — Definitions of all key terms
- [Point-in-Time Joins](concepts/point-in-time-joins.md) — Prevent data leakage in ML features
- [Second-Order Aggregations](concepts/second-order-aggregations.md) — Hierarchical rollups and multi-level analytics
- [Virtual Environments](concepts/virtual-environments.md) — Isolated workspaces for safe development
- [Change Categorization](concepts/change-categorization.md) — BREAKING, NON_BREAKING, and METADATA changes
- [Python API vs YAML Workflows](concepts/python-vs-yaml.md) — Choose the right paradigm

## Guides

Task-oriented walkthroughs for specific workflows.

- [Python Pipelines](guides/python-pipelines.md) — Write Python feature transforms and custom logic
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
- [CLI Docs Search](cli/docs.md) — Search documentation from the terminal (`seeknal docs`)

## Tutorials

Step-by-step learning paths with copy-pasteable code.

- [YAML Pipeline Tutorial](tutorials/yaml-pipeline-tutorial.md) — Build a complete pipeline from scratch (75 min)
- [Mixed YAML + Python](tutorials/mixed-yaml-python-pipelines.md) — Combine both paradigms (60 min)
- [Environment Management](tutorials/environment-management.md) — Safe development with environments (45 min)
- [Parallel Execution](tutorials/parallel-execution.md) — Speed up large pipelines (45 min)
- [Change Categorization](tutorials/metrics-change-tracking.md) — Understand change impact (20 min)
- [E-Commerce Walkthrough](tutorials/workflow-tutorial-ecommerce.md) — Real-world example

## Additional Resources

- [DuckDB Getting Started](duckdb-getting-started.md) — DuckDB engine quickstart
- [DuckDB Flow Guide](duckdb-flow-guide.md) — DuckDB flow patterns
- [Spark Transformers Reference](spark-transformers-reference.md) — Spark-specific reference
- [Iceberg Materialization](iceberg-materialization.md) — Apache Iceberg integration
- [DAGRunner Documentation](workflows/runner.md) — Workflow runner internals
