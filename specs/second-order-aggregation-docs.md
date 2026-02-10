---
title: "Second-Order Aggregation & Python Pipeline Documentation + Evaluation"
type: feat
date: 2026-02-09
status: ready
---

# Plan: Second-Order Aggregation & Python Pipeline Documentation + Evaluation

## Overview

Add comprehensive documentation for second-order aggregations and Python pipelines — both already implemented in Seeknal. Then evaluate all documentation from Data Engineer and ML Engineer perspectives to identify remaining gaps.

**Key Deliverables:**
- Dedicated concept page for second-order aggregations with visual diagrams and use cases
- Dedicated guide page for Python pipelines covering all decorators (@source, @transform, @feature_group, @aggregation, @second_order_aggregation, @model, @rule, @exposure)
- Updates to existing docs (CLI reference, Python vs YAML, comparison, training-to-serving) to incorporate both features
- Two-persona documentation evaluation (Data Engineer + ML Engineer)

## Task Description

**Second-order aggregations** (`kind: second_order_aggregation`) are already implemented in the codebase with full DuckDB and Spark support, a Python decorator (`@second_order_aggregation`), YAML schema, and an executor. They're partially documented in the YAML tutorial (section 8.8), YAML schema reference, and glossary. However, they're missing from the CLI reference, Python vs YAML guide, comparison page, and training-to-serving guide. There's also no dedicated concept page explaining the pattern, use cases, and architecture.

**Python pipelines** are fully implemented with decorators (`@source`, `@transform`, `@feature_group`, `@aggregation`, `@second_order_aggregation`, `@model`, `@rule`, `@exposure`) in `src/seeknal/pipeline/decorators.py`. A tutorial exists at `docs/tutorials/python-pipelines-tutorial.md`, but there's no dedicated guide page covering all decorators comprehensively as a reference, and the Python pipeline approach isn't well-integrated into the broader documentation structure (comparison page, training-to-serving, etc.).

## Objective

1. Create a dedicated concept page explaining second-order aggregations with visual diagrams
2. Create a dedicated guide page for Python pipelines covering all decorators as a reference
3. Update all existing documentation to incorporate second-order aggregations and Python pipelines as first-class features
4. Evaluate all documentation from Data Engineer and ML Engineer perspectives

## Relevant Files

### Existing Docs to Update
- `docs/reference/cli.md` — Add `seeknal draft second-order-aggregation` command
- `docs/concepts/python-vs-yaml.md` — Add second-order aggregation examples for both paradigms
- `docs/guides/comparison.md` — Add second-order aggregations as a Seeknal differentiator
- `docs/guides/training-to-serving.md` — Show second-order aggregations in ML workflow
- `docs/concepts/glossary.md` — Enhance the existing glossary entry

### New Files to Create
- `docs/concepts/second-order-aggregations.md` — Dedicated concept page
- `docs/guides/python-pipelines.md` — Python pipeline guide covering all decorators
- `docs/assessments/2026-02-09-post-improvement-evaluation.md` — Evaluation report

### Source Files to Reference (read-only)
- `src/seeknal/workflow/executors/second_order_aggregation_executor.py` — Executor implementation
- `src/seeknal/tasks/duckdb/aggregators/second_order_aggregator.py` — DuckDB aggregator
- `src/seeknal/pipeline/decorators.py` — All Python pipeline decorators (@source, @transform, @feature_group, @aggregation, @second_order_aggregation, @model, @rule, @exposure)
- `docs/tutorials/yaml-pipeline-tutorial.md` — Existing YAML tutorial section 8.8
- `docs/tutorials/python-pipelines-tutorial.md` — Existing Python pipeline tutorial
- `docs/reference/yaml-schema.md` — Existing schema reference
- `docs/brainstorms/2026-01-30-second-order-aggregations-brainstorm.md` — Design brainstorm

## Team Orchestration

### Team Members

| Name | Agent Type | Role |
|------|-----------|------|
| docs-writer | general-purpose | Create concept page, Python pipelines guide, and update existing docs |
| data-eng-evaluator | general-purpose | Data Engineer persona evaluating all documentation |
| ml-eng-evaluator | general-purpose | ML Engineer persona evaluating all documentation |

### Execution Strategy

- **Phase 1** (Tasks 1-3): docs-writer creates second-order aggregation concept page, Python pipelines guide, and updates existing docs (Task 1 and 2 in parallel, Task 3 depends on both)
- **Phase 2** (Tasks 4-5): data-eng-evaluator and ml-eng-evaluator run in parallel after docs are complete
- **Phase 3** (Task 6): Consolidate evaluation into report

## Step by Step Tasks

### Task 1: Create Second-Order Aggregations Concept Page
- **Assigned To:** docs-writer
- **Agent Type:** general-purpose
- **Depends On:** none
- **Description:** Create `docs/concepts/second-order-aggregations.md` — a dedicated concept page.

  **Read** these source files for accuracy:
  - `src/seeknal/workflow/executors/second_order_aggregation_executor.py` — executor logic, validation, supported types
  - `src/seeknal/tasks/duckdb/aggregators/second_order_aggregator.py` — AggregationSpec types (basic, basic_days, ratio, since)
  - `src/seeknal/pipeline/decorators.py` — `@second_order_aggregation` decorator API
  - `docs/brainstorms/2026-01-30-second-order-aggregations-brainstorm.md` — design decisions
  - `docs/tutorials/yaml-pipeline-tutorial.md` (section 8.8) — existing tutorial content

  **Structure:**
  ```markdown
  # Second-Order Aggregations

  ## What Are Second-Order Aggregations?

  (2-paragraph explanation: aggregations of aggregations for multi-level feature engineering)

  ## Visual Concept

  ```
  Raw Data     →   First-Order Aggregation   →   Second-Order Aggregation
  (transactions)   (user_id: daily metrics)      (region_id: user summaries)

  ┌──────────────┐   ┌──────────────────┐   ┌──────────────────────┐
  │ user  amount │   │ user  total_30d  │   │ region  avg_user_30d │
  │ A     $50    │──▶│ A     $500       │──▶│ West    $450         │
  │ A     $30    │   │ B     $200       │   │ East    $300         │
  │ B     $200   │   │ C     $700       │   └──────────────────────┘
  └──────────────┘   └──────────────────┘
  ```

  ## Use Cases

  1. **Hierarchical rollups**: user → store → region → country
  2. **Cross-time windows**: daily → weekly patterns
  3. **Comparative analytics**: recent vs historical ratios
  4. **Distribution features**: stddev, percentile across entities

  ## YAML Definition

  (Complete YAML example with all aggregation types: basic, window, ratio, since)

  ## Python Definition

  (@second_order_aggregation decorator example)

  ## Aggregation Types

  | Type | Description | Example |
  |------|-------------|---------|
  | `basic` | Simple aggregate across entities | `[sum, avg, count, stddev]` |
  | `window` (basic_days) | Time-window cross-aggregation | `window: [7, 7]` |
  | `ratio` | Compare two time windows | `numerator: [1, 7], denominator: [8, 30]` |
  | `since` | Conditional aggregations | `condition: "flag == 1"` |

  ## Feature Naming Convention

  (Auto-generated names: `{feature}_{AGG}`, `{feature}_{AGG}_{lower}_{upper}`, etc.)

  ## How It Works (Architecture)

  (Execution flow: parse YAML → validate → resolve source → build specs → execute with DuckDB/Spark → output)

  ## Relationship to First-Order Aggregations

  (Key differences: dict vs list format, source reference, id_col semantics)

  ## CLI Commands

  - `seeknal draft second-order-aggregation <name>` — Generate template
  - `seeknal run` — Execute as part of pipeline
  - `seeknal run --nodes second_order_aggregation.<name>` — Run specific node

  ## Common Pitfalls

  1. Source aggregation must include the grouping column
  2. application_date_col must exist in source output
  3. Feature dict format (not list format like first-order)
  ```

  **See Also** section linking to glossary, YAML schema, tutorial section 8.8, and comparison page.

### Task 2: Create Python Pipelines Guide
- **Assigned To:** docs-writer
- **Agent Type:** general-purpose
- **Depends On:** none
- **Description:** Create `docs/guides/python-pipelines.md` — a comprehensive guide to Python pipeline development with all decorators.

  **Read** `src/seeknal/pipeline/decorators.py` to extract all decorators, their parameters, and examples.
  Also read `docs/tutorials/python-pipelines-tutorial.md` for existing tutorial content.

  **Structure:**
  ```markdown
  # Python Pipelines Guide

  Seeknal's Python pipeline API provides decorators for defining pipeline nodes as Python functions. This guide covers all available decorators and patterns.

  ## Overview

  Python pipelines let you define data transformations, feature engineering, and ML models using Python functions with decorators.

  ```python
  from seeknal.pipeline import source, transform, feature_group, aggregation, second_order_aggregation, model, rule, exposure
  ```

  ## Decorator Reference

  ### @source
  Define data ingestion from files or databases.
  (Parameters: name, source, table, format, schema, columns, description, owner, tags)
  (Example with CSV, Parquet, and database sources)

  ### @transform
  Define SQL or Python transformations.
  (Parameters: name, inputs, description, owner, tags, materialization)
  (Example with ctx.ref() and DuckDB SQL)

  ### @feature_group
  Define ML feature groups with entity keys and materialization.
  (Parameters: name, entity, join_keys, features, materialization, description, owner, tags)
  (Example with FeatureGroup creation)

  ### @aggregation
  Define first-order aggregations on entity-level data.
  (Parameters: name, id_col, feature_date_col, inputs, description, owner, tags)

  ### @second_order_aggregation
  Define aggregations of aggregations for multi-level analytics.
  (Parameters: name, source, id_col, feature_date_col, application_date_col, description, owner, tags, materialization)
  (Example with cross-entity aggregation)

  ### @model
  Define ML model training and inference.
  (Parameters: name, inputs, description, owner, tags)

  ### @rule
  Define data quality validation rules.
  (Parameters: name, inputs, description, owner, tags)

  ### @exposure
  Define data exports to external systems.
  (Parameters: name, inputs, type, config, description, owner, tags)

  ## Pipeline Context

  ### ctx.ref()
  Reference upstream node outputs.

  ### ctx.duckdb
  Access DuckDB connection for SQL queries.

  ## Running Python Pipelines

  ```bash
  seeknal run                          # Auto-discovers .py files in seeknal/ directory
  seeknal run --parallel --max-workers 4
  seeknal run --nodes transform.clean_users
  ```

  ## Mixed YAML + Python

  Combine YAML and Python nodes in the same pipeline. Python nodes can reference YAML nodes and vice versa.

  ## Common Patterns

  - RFM analysis with @transform + @feature_group
  - ML training pipeline with @model
  - Data quality with @rule
  - Export with @exposure
  ```

  Read the actual decorator source code to document accurate parameters, types, and defaults.

### Task 3: Update Existing Docs with Second-Order Aggregation and Python Pipeline Content
- **Assigned To:** docs-writer
- **Agent Type:** general-purpose
- **Depends On:** Task 1, Task 2
- **Description:** Update existing documentation files to incorporate second-order aggregations and Python pipelines.

  **Updates:**

  1. **`docs/reference/cli.md`** — Add entry for `seeknal draft second-order-aggregation`:
     - Add to the Quick Reference table
     - Add full command documentation with arguments, options, and examples
     - Mention in the `seeknal run` section that second-order aggregation nodes are supported

  2. **`docs/concepts/python-vs-yaml.md`** — Add second-order aggregation examples:
     - In the YAML section: show the YAML definition
     - In the Python section: show the `@second_order_aggregation` decorator
     - In the comparison table: mention second-order aggregations work in both paradigms
     - Add Python pipeline decorator reference link

  3. **`docs/guides/comparison.md`** — Add as Seeknal differentiators:
     - In the Seeknal vs dbt section: dbt has no equivalent to second-order aggregations
     - In the Seeknal vs SQLMesh section: SQLMesh has no equivalent
     - In feature comparison table: add rows for second-order aggregations and Python decorator API
     - Mention Python pipeline approach as unique Seeknal advantage

  4. **`docs/guides/training-to-serving.md`** — Show in ML workflow:
     - Add a section showing how second-order aggregations create cross-entity features
     - Example: user features → region features for model training
     - Add Python pipeline examples alongside YAML examples

  5. **`docs/concepts/glossary.md`** — Enhance existing entries with links to new pages; add Python Pipeline term

  6. **`docs/index.md`** — Add links to new concept page and Python pipelines guide

  **Read** each file first, then make surgical edits. Don't rewrite entire files.

### Task 4: Data Engineer Documentation Evaluation
- **Assigned To:** data-eng-evaluator
- **Agent Type:** general-purpose
- **Depends On:** Task 1, Task 2, Task 3
- **Description:** You are **Priya**, a Senior Data Engineer at a mid-size fintech company. You have 5 years of experience with Airflow, dbt, and Spark. You're evaluating Seeknal's documentation.

  **Your workflow priorities:**
  1. ETL pipeline definition and scheduling
  2. Data source ingestion from databases and files
  3. Transform chains with SQL
  4. Environment management (dev/staging/prod)
  5. Incremental processing for large datasets
  6. CLI-driven workflow
  7. Multi-level aggregations for analytics

  **Assessment tasks:**
  1. Start from `docs/index.md` — Can you navigate to all relevant content?
  2. Read `docs/reference/cli.md` — Is the CLI reference comprehensive? Can you find all commands?
  3. Read `docs/tutorials/yaml-pipeline-tutorial.md` — Can you build a real pipeline including second-order aggregations?
  4. Read `docs/concepts/second-order-aggregations.md` — Is the concept clear? Can you use it?
  5. Read `docs/concepts/virtual-environments.md` — Is environment workflow clear?
  6. Read `docs/reference/yaml-schema.md` — Can you look up any YAML field?
  7. Read `docs/reference/configuration.md` — Can you configure Seeknal?
  8. Read `docs/guides/testing-and-audits.md` — Can you set up data quality checks?
  9. Read `docs/concepts/python-vs-yaml.md` — Is the decision guide helpful?
  10. Read `docs/concepts/glossary.md` — Are all terms defined?

  **Score each dimension 1-5:**
  - Discoverability, Time-to-first-value, Conceptual clarity, Practical examples, Error guidance, Workflow completeness, Reference quality

  **Compare against dbt and SQLMesh** documentation standards.
  **End with top 5 prioritized remaining improvements.**

  **Files to read:** docs/index.md, docs/reference/cli.md, docs/reference/yaml-schema.md, docs/reference/configuration.md, docs/concepts/glossary.md, docs/concepts/second-order-aggregations.md, docs/concepts/virtual-environments.md, docs/concepts/python-vs-yaml.md, docs/tutorials/yaml-pipeline-tutorial.md, docs/guides/testing-and-audits.md, docs/guides/python-pipelines.md, docs/tutorials/python-pipelines-tutorial.md

### Task 5: ML Engineer Documentation Evaluation
- **Assigned To:** ml-eng-evaluator
- **Agent Type:** general-purpose
- **Depends On:** Task 1, Task 2, Task 3
- **Description:** You are **Sofia**, an ML Engineer at a healthcare AI startup. You have 4 years of experience with Python, scikit-learn, and feature stores (Feast, Tecton). You're evaluating Seeknal's documentation.

  **Your workflow priorities:**
  1. Feature engineering with Python and SQL
  2. Feature store for training and serving
  3. Point-in-time correctness (no data leakage)
  4. Multi-level feature aggregations (user → region, daily → weekly)
  5. Parallel execution for large feature pipelines
  6. Materialization to offline/online stores
  7. Version management for feature group schemas

  **Assessment tasks:**
  1. Start from `docs/index.md` — Can you navigate to ML-relevant content?
  2. Read `docs/concepts/point-in-time-joins.md` — Is PIT join concept clear?
  3. Read `docs/concepts/second-order-aggregations.md` — Can you use this for hierarchical features?
  4. Read `docs/guides/training-to-serving.md` — Is the end-to-end ML workflow clear?
  5. Read `docs/guides/semantic-layer.md` — Can you define and query metrics?
  6. Read `docs/tutorials/yaml-pipeline-tutorial.md` — Can you build ML features from this?
  7. Read `docs/concepts/python-vs-yaml.md` — When to use Python vs YAML for ML?
  8. Read `docs/guides/comparison.md` — How does Seeknal compare to Feast/Tecton?
  9. Read `docs/reference/cli.md` — Can you find all ML-relevant commands?
  10. Read `docs/concepts/glossary.md` — Are ML-specific terms defined?

  **Score each dimension 1-5:**
  - Discoverability, Time-to-first-value, Conceptual clarity, Practical examples, Error guidance, Workflow completeness, Reference quality

  **Compare against Feast, Tecton, dbt, and SQLMesh** documentation standards.
  **End with top 5 prioritized remaining improvements.**

  **Files to read:** docs/index.md, docs/concepts/point-in-time-joins.md, docs/concepts/second-order-aggregations.md, docs/guides/training-to-serving.md, docs/guides/semantic-layer.md, docs/tutorials/yaml-pipeline-tutorial.md, docs/concepts/python-vs-yaml.md, docs/guides/comparison.md, docs/reference/cli.md, docs/concepts/glossary.md, docs/guides/python-pipelines.md, docs/tutorials/python-pipelines-tutorial.md

### Task 6: Consolidate Evaluation Report
- **Assigned To:** orchestrator (me)
- **Depends On:** Task 4, Task 5
- **Description:** Synthesize both persona evaluations into `docs/assessments/2026-02-09-post-improvement-evaluation.md`:
  1. Score comparison table (2 personas x 7 dimensions)
  2. Score delta from initial assessment (2.95) to post-improvement
  3. Cross-persona themes (issues mentioned by both)
  4. Persona-specific gaps
  5. Remaining improvement priorities
  6. Comparison to dbt/SQLMesh benchmarks

## Acceptance Criteria

- [ ] `docs/concepts/second-order-aggregations.md` exists with visual diagrams, all 4 aggregation types, YAML and Python examples
- [ ] `docs/guides/python-pipelines.md` exists with all 8 decorators documented with parameters and examples
- [ ] CLI reference includes `seeknal draft second-order-aggregation` command
- [ ] Python vs YAML guide includes second-order aggregation examples and links to Python pipelines guide
- [ ] Comparison page lists second-order aggregations and Python decorator API as differentiators
- [ ] Training-to-serving guide shows cross-entity feature engineering with both YAML and Python examples
- [ ] Data Engineer evaluation scores all 7 dimensions with justification
- [ ] ML Engineer evaluation scores all 7 dimensions with justification
- [ ] Consolidated evaluation report compares scores to initial 2.95 baseline

## Validation Commands

```bash
# Verify new files exist
ls docs/concepts/second-order-aggregations.md docs/guides/python-pipelines.md

# Verify CLI reference includes second-order-aggregation
grep -c 'second-order-aggregation' docs/reference/cli.md

# Verify Python pipelines guide covers all decorators
grep -c '@source\|@transform\|@feature_group\|@aggregation\|@second_order_aggregation\|@model\|@rule\|@exposure' docs/guides/python-pipelines.md

# Verify comparison page mentions second-order
grep -c 'second.order' docs/guides/comparison.md

# Verify evaluation report
ls docs/assessments/2026-02-09-post-improvement-evaluation.md
```

## Notes

- Read actual source code for accuracy of all technical documentation
- The concept page should be the authoritative reference — tutorials and schema reference already exist
- Evaluation should read ALL documentation, not just new files, to assess the complete experience
- Compare improvement scores against the initial 2.95/5.0 baseline
