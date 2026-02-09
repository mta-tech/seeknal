---
title: "World-Class Documentation: Apply Assessment Improvements"
type: feat
date: 2026-02-09
status: ready
source_plan: docs/assessments/2026-02-09-documentation-assessment-report.md
---

# Plan: World-Class Documentation

## Overview

Apply all improvements from the documentation assessment (score: 2.95/5.0) to reach world-class documentation quality (target: 4.1/5.0), matching or exceeding dbt and SQLMesh standards.

**Key Deliverables:**
- Navigable documentation homepage with persona paths
- CLI command reference page with all 35+ commands, flags, and examples
- Concepts section: glossary, point-in-time joins, virtual environments, change categorization
- Reference section: CLI, YAML schema, configuration
- Guides section: testing/audits, semantic layer, comparison page, Python vs YAML decision guide

## Task Description

Transform Seeknal documentation from scattered tutorials with good content but poor navigation into a structured, world-class documentation site organized by SQLMesh/dbt standards: Getting Started, Guides, Concepts, Reference, and Tutorials.

## Objective

Raise documentation quality from 2.95 to 4.1+ across all persona dimensions by:
1. Fixing structural issues (navigation, reference, glossary) — Immediate
2. Filling critical content gaps (semantic layer, testing, environments, PIT joins) — Short-term
3. Differentiating (comparison page, best practices, training-to-serving guide) — Medium-term

## Relevant Files

### Existing Files to Modify
- `docs/index.md` — Rewrite as navigable homepage
- `README.md` — Add links to new docs structure

### New Files to Create
- `docs/reference/cli.md` — CLI command reference
- `docs/reference/yaml-schema.md` — YAML schema reference
- `docs/reference/configuration.md` — Configuration reference
- `docs/concepts/glossary.md` — 25+ term glossary
- `docs/concepts/point-in-time-joins.md` — PIT joins with diagrams
- `docs/concepts/virtual-environments.md` — Environment architecture
- `docs/concepts/change-categorization.md` — BREAKING/NON_BREAKING/METADATA reference
- `docs/concepts/python-vs-yaml.md` — Decision guide
- `docs/guides/testing-and-audits.md` — Data quality documentation
- `docs/guides/semantic-layer.md` — Metrics and semantic models
- `docs/guides/comparison.md` — Seeknal vs dbt vs SQLMesh vs Feast
- `docs/guides/training-to-serving.md` — ML workflow end-to-end

### Source Files to Reference (read-only)
- `src/seeknal/cli/main.py` — All CLI commands and flags
- `src/seeknal/workflow/audits.py` — Audit framework
- `src/seeknal/workflow/semantic/models.py` — Semantic model definitions
- `src/seeknal/workflow/semantic/compiler.py` — Metric compiler
- `src/seeknal/dag/diff.py` — Change categorization engine
- `src/seeknal/workflow/environment.py` — Virtual environment manager
- `src/seeknal/featurestore/duckdbengine/feature_group.py` — Feature store API

## Team Orchestration

### Team Members

| Name | Agent Type | Role |
|------|-----------|------|
| nav-and-reference | general-purpose | Build navigation homepage, CLI reference, YAML schema reference, configuration reference |
| concepts-writer | general-purpose | Write glossary, PIT joins, virtual environments, change categorization, Python vs YAML guide |
| guides-writer | general-purpose | Write testing/audits, semantic layer, comparison page, training-to-serving guide |

### Execution Strategy

- **Phase 1** (Tasks 1-4): nav-and-reference builds the structural foundation — homepage, CLI ref, YAML schema, config ref
- **Phase 2** (Tasks 5-9): concepts-writer and guides-writer work in parallel — concepts and guides are independent
- **Phase 3** (Task 10): Final cross-linking pass after all content exists

## Step by Step Tasks

### Task 1: Rewrite Documentation Homepage
- **Assigned To:** nav-and-reference
- **Agent Type:** general-purpose
- **Depends On:** none
- **Description:** Rewrite `docs/index.md` to be a navigable plain-markdown documentation homepage. Must work both as raw markdown in GitHub and as rendered MkDocs site. Include:

  **Structure:**
  ```
  # Seeknal Documentation

  ## What is Seeknal?
  (2-sentence value prop)

  ## Choose Your Path
  - **New to Seeknal?** → Getting Started Guide
  - **Data Engineers** → YAML Pipeline Tutorial → Environments Tutorial
  - **Analytics Engineers** → YAML Tutorial → Change Categorization → Semantic Layer
  - **ML Engineers** → Feature Store Guide → Python Pipelines → Parallel Execution

  ## Documentation Sections

  ### Getting Started
  - [Installation & Quick Start](getting-started-comprehensive.md) — 30 min
  - [DuckDB Quick Start](duckdb-getting-started.md) — 15 min

  ### Concepts
  - [Glossary](concepts/glossary.md)
  - [Point-in-Time Joins](concepts/point-in-time-joins.md)
  - [Virtual Environments](concepts/virtual-environments.md)
  - [Change Categorization](concepts/change-categorization.md)
  - [Python API vs YAML Workflows](concepts/python-vs-yaml.md)

  ### Guides
  - [Testing & Audits](guides/testing-and-audits.md)
  - [Semantic Layer & Metrics](guides/semantic-layer.md)
  - [Training to Serving (ML)](guides/training-to-serving.md)
  - [Seeknal vs dbt vs SQLMesh](guides/comparison.md)

  ### Reference
  - [CLI Commands](reference/cli.md)
  - [YAML Schema](reference/yaml-schema.md)
  - [Configuration](reference/configuration.md)
  - [Python API](api/index.md)

  ### Tutorials
  - [YAML Pipeline Tutorial](tutorials/yaml-pipeline-tutorial.md) — 75 min
  - [Python Pipelines](tutorials/python-pipelines-tutorial.md) — 45 min
  - [Mixed YAML+Python](tutorials/mixed-yaml-python-pipelines.md) — 60 min
  - [Virtual Environments (Data Eng)](tutorials/phase2-data-eng-environments.md) — 45 min
  - [Parallel Execution (ML Eng)](tutorials/phase2-ml-eng-parallel.md) — 45 min
  - [Change Categorization (Analytics)](tutorials/phase2-analytics-eng-metrics.md) — 20 min
  ```

  Keep the MkDocs card syntax but ADD plain-markdown fallback above it. Create the `docs/concepts/`, `docs/guides/`, and `docs/reference/` directories.

  **Files:** Edit `docs/index.md`. Create empty placeholder files for all new paths to avoid broken links.

### Task 2: Create CLI Command Reference
- **Assigned To:** nav-and-reference
- **Agent Type:** general-purpose
- **Depends On:** Task 1
- **Description:** Create `docs/reference/cli.md` — a comprehensive CLI reference page documenting every command.

  **Read** `src/seeknal/cli/main.py` to extract all commands, their arguments, options, types, defaults, and docstrings. Run `seeknal --help` and individual `seeknal <cmd> --help` for each command.

  **Format for each command:**
  ```markdown
  ## seeknal run

  Execute a feature transformation flow or YAML pipeline.

  **Usage:**
  ```bash
  seeknal run [FLOW_NAME] [OPTIONS]
  ```

  **Arguments:**
  | Name | Type | Required | Description |
  |------|------|----------|-------------|
  | `FLOW_NAME` | TEXT | No | Name of the flow to run (omit for YAML pipeline mode) |

  **Options:**
  | Flag | Type | Default | Description |
  |------|------|---------|-------------|
  | `--parallel` | FLAG | false | Enable parallel execution |
  | `--max-workers` | INT | 8 | Max parallel workers |
  | `--full` | FLAG | false | Ignore cache, run all nodes |
  | `--env` | TEXT | None | Execute in virtual environment |
  ...

  **Examples:**
  ```bash
  # Run YAML pipeline
  seeknal run

  # Run with parallel execution
  seeknal run --parallel --max-workers 4

  # Run in virtual environment
  seeknal run --env staging --parallel
  ```

  **See Also:** [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md)
  ```

  Cover ALL commands: init, run, plan, promote, parse, diff, dry-run, apply, draft, audit, query, deploy-metrics, repl, materialize, list, show, validate, validate-features, debug, clean, delete, delete-table, info, connection-test, starrocks-setup-catalog. Plus subcommand groups: version, source, iceberg, env, atlas.

### Task 3: Create YAML Schema Reference
- **Assigned To:** nav-and-reference
- **Agent Type:** general-purpose
- **Depends On:** Task 1
- **Description:** Create `docs/reference/yaml-schema.md` — document every valid YAML field for each node kind.

  **Read** `src/seeknal/cli/main.py` (the executor dispatch), `src/seeknal/workflow/executor.py`, and the YAML tutorial's examples to extract all valid fields.

  **Structure:**
  ```markdown
  # YAML Schema Reference

  ## Source
  | Field | Type | Required | Description |
  | `kind` | string | Yes | Must be "source" |
  | `name` | string | Yes | Unique node name |
  | `source` | string | Yes | Data source path or type |
  | `format` | string | No | File format (csv, parquet, json) |
  | `description` | string | No | Human-readable description |
  | `columns` | object | No | Column definitions with types |

  ## Transform
  (same pattern)

  ## Feature Group
  (same pattern, include features, entity, materialization, tests, online)

  ## Aggregation
  ## Model
  ## Rule
  ## Exposure
  ```

  Use actual YAML examples from `docs/tutorials/yaml-pipeline-tutorial.md` and `docs/tutorials/workflow-tutorial-ecommerce.md` as source of truth.

### Task 4: Create Configuration Reference
- **Assigned To:** nav-and-reference
- **Agent Type:** general-purpose
- **Depends On:** Task 1
- **Description:** Create `docs/reference/configuration.md` by reorganizing content from `docs/examples/configuration.md` into reference format.

  **Read** `docs/examples/configuration.md` and `src/seeknal/cli/main.py` (init command generates config files).

  Document:
  - `seeknal_project.yml` fields
  - `profiles.yml` fields
  - Environment variables (SEEKNAL_BASE_CONFIG_PATH, SEEKNAL_USER_CONFIG_PATH, etc.)
  - `config.toml` structure
  - DuckDB vs Spark engine configuration

### Task 5: Create Glossary
- **Assigned To:** concepts-writer
- **Agent Type:** general-purpose
- **Depends On:** Task 1
- **Description:** Create `docs/concepts/glossary.md` with 25+ terms.

  **Terms to define** (extract definitions from README.md, CLAUDE.md, and tutorials):
  - Node, DAG, Source, Transform, Feature Group, Aggregation, Model, Rule, Exposure
  - Entity, Join Keys, Materialization, Offline Store, Online Store
  - Flow, Task, DuckDBTask, SparkEngineTask
  - Manifest, State, Fingerprint, Cache
  - Environment, Plan, Apply, Promote, Draft, Dry Run
  - Change Categorization (BREAKING, NON_BREAKING, METADATA)
  - Point-in-Time Join, Data Leakage, Feature Start Time
  - Semantic Model, Metric, Dimension
  - Parallel Execution, Topological Layer, Max Workers
  - Audit, Validation

  Each term: 1-3 sentence definition + "See also" cross-links.

### Task 6: Create Point-in-Time Joins Concept Page
- **Assigned To:** concepts-writer
- **Agent Type:** general-purpose
- **Depends On:** Task 1
- **Description:** Create `docs/concepts/point-in-time-joins.md` — the most important concept for ML engineers.

  **Read** README.md (feature store section), `docs/getting-started-comprehensive.md`, and `src/seeknal/featurestore/duckdbengine/feature_group.py` to understand the PIT join implementation.

  **Include:**
  1. What is a point-in-time join? (2 paragraphs)
  2. Why it matters — data leakage example (show incorrect results WITHOUT PIT join vs correct WITH)
  3. ASCII timeline diagram showing events and feature lookup windows
  4. `feature_start_time` and `event_time_col` parameter explanations
  5. Code example using HistoricalFeatures with PIT join
  6. Comparison: How Seeknal's PIT joins compare to Feast/Tecton
  7. Common pitfalls and troubleshooting

### Task 7: Create Virtual Environments and Change Categorization Concept Pages
- **Assigned To:** concepts-writer
- **Agent Type:** general-purpose
- **Depends On:** Task 1
- **Description:** Create two concept pages:

  **1. `docs/concepts/virtual-environments.md`:**
  - Read `src/seeknal/workflow/environment.py` and `docs/tutorials/phase2-data-eng-environments.md`
  - Explain what environments are (isolated workspaces)
  - Architecture: how state/cache is managed per environment
  - Cost model: does `seeknal promote` copy data or metadata?
  - Workflow: plan → apply → validate → promote
  - Comparison to dbt targets and SQLMesh virtual environments

  **2. `docs/concepts/change-categorization.md`:**
  - Read `src/seeknal/dag/diff.py` and `docs/tutorials/phase2-analytics-eng-metrics.md`
  - Define BREAKING, NON_BREAKING, METADATA with all rules
  - Table of every change type and its categorization
  - Downstream impact calculation algorithm
  - `seeknal diff` command reference with all flags
  - CI/CD integration patterns

  **3. `docs/concepts/python-vs-yaml.md`:**
  - Decision guide: when to use Python API (Flow/Task) vs YAML workflows (seeknal run)
  - Comparison table of capabilities
  - Migration path between the two

### Task 8: Create Testing & Audits Guide
- **Assigned To:** guides-writer
- **Agent Type:** general-purpose
- **Depends On:** Task 1
- **Description:** Create `docs/guides/testing-and-audits.md` documenting Seeknal's data quality features.

  **Read** `src/seeknal/workflow/audits.py`, `src/seeknal/cli/main.py` (audit and validate-features commands), and `src/seeknal/feature_validation/` directory.

  **Include:**
  1. **Audits** (`seeknal audit`)
     - What audits are (production-time data quality checks on cached outputs)
     - How to define audits in YAML (not_null, unique, custom SQL)
     - Running audits: `seeknal audit` and `seeknal audit source.users`
     - Audit output format: PASS/FAIL with severity levels
     - CI/CD integration
  2. **Feature Validation** (`seeknal validate-features`)
     - What feature validation is (development-time quality checks)
     - How to configure: warn vs fail modes
     - Available validators
  3. **YAML `tests` block**
     - How to define inline tests in feature group YAML
     - Supported test types: not_null, unique
  4. **Comparison to dbt tests and SQLMesh audits**

### Task 9: Create Semantic Layer Guide and Comparison Page
- **Assigned To:** guides-writer
- **Agent Type:** general-purpose
- **Depends On:** Task 1
- **Description:** Create two guide pages:

  **1. `docs/guides/semantic-layer.md`:**
  - Read `src/seeknal/workflow/semantic/models.py`, `src/seeknal/workflow/semantic/compiler.py`, `src/seeknal/workflow/semantic/deploy.py`
  - Read `src/seeknal/cli/main.py` (query and deploy-metrics commands)
  - Explain semantic models (kind: semantic_model), metrics (kind: metric)
  - Directory structure: `seeknal/semantic_models/` and `seeknal/metrics/`
  - Tutorial: define a semantic model, define metrics, query with `seeknal query`
  - Deploy to StarRocks with `seeknal deploy-metrics`
  - Complete working examples

  **2. `docs/guides/comparison.md`:**
  - Seeknal vs dbt: YAML syntax similarity + Seeknal's advantages (feature store, PIT joins, multi-engine, Python pipelines)
  - Seeknal vs SQLMesh: plan/apply similarity + Seeknal's advantages (feature store, mixed pipelines)
  - Seeknal vs Feast/Tecton: feature store overlap + Seeknal's advantages (integrated pipelines, YAML definition, no external orchestrator)
  - Feature comparison table
  - When to use each tool

  **3. `docs/guides/training-to-serving.md`:**
  - Read README.md (DuckDB feature store section) and `docs/getting-started-comprehensive.md`
  - End-to-end ML workflow: define features → create training data (HistoricalFeatures) → train model → serve features (OnlineFeatures)
  - Training/serving parity: same feature definitions produce consistent values
  - Code examples for each step
  - Common pitfalls (feature drift, schema mismatch)

### Task 10: Cross-Linking and Final QA Pass
- **Assigned To:** nav-and-reference (resumed)
- **Agent Type:** general-purpose
- **Depends On:** Task 5, Task 6, Task 7, Task 8, Task 9
- **Description:** Final pass to ensure all documents cross-link correctly.

  1. Verify all links in `docs/index.md` resolve to existing files
  2. Add "See Also" cross-links between related pages
  3. Add glossary term links from tutorials to glossary entries
  4. Ensure consistent formatting across all new pages
  5. Verify every concept, guide, and reference page includes time estimates where applicable
  6. Update `README.md` to link to the new documentation structure

## Acceptance Criteria

- [ ] `docs/index.md` is navigable in raw markdown AND rendered MkDocs
- [ ] CLI reference covers all 35+ commands with flags, types, defaults, and examples
- [ ] YAML schema reference documents every field for all 7 node kinds
- [ ] Glossary defines 25+ terms with cross-links
- [ ] Point-in-time joins page includes visual timeline diagram and code example
- [ ] Virtual environments concept page explains architecture and cost model
- [ ] Change categorization page lists all rules with categorization table
- [ ] Testing/audits guide documents `seeknal audit`, `seeknal validate-features`, and YAML tests block
- [ ] Semantic layer guide includes complete tutorial from definition to query
- [ ] Comparison page covers Seeknal vs dbt vs SQLMesh vs Feast
- [ ] Training-to-serving guide shows end-to-end ML workflow
- [ ] Python vs YAML decision guide helps users choose the right paradigm
- [ ] All internal links resolve correctly
- [ ] All new pages follow consistent formatting (headings, code blocks, tables)

## Validation Commands

```bash
# Verify all new files exist
ls docs/index.md docs/reference/cli.md docs/reference/yaml-schema.md docs/reference/configuration.md
ls docs/concepts/glossary.md docs/concepts/point-in-time-joins.md docs/concepts/virtual-environments.md
ls docs/concepts/change-categorization.md docs/concepts/python-vs-yaml.md
ls docs/guides/testing-and-audits.md docs/guides/semantic-layer.md docs/guides/comparison.md
ls docs/guides/training-to-serving.md

# Verify no broken internal links
grep -r '\[.*\](.*\.md)' docs/index.md | grep -v http

# Verify CLI reference covers all commands
grep -c '## seeknal' docs/reference/cli.md  # Should be 35+

# Verify glossary has 25+ terms
grep -c '^## ' docs/concepts/glossary.md  # Should be 25+
```

## Notes

- Preserve all existing MkDocs syntax in `docs/index.md` but add plain-markdown fallback
- Do NOT modify existing tutorial files — only add new files and update index
- Read source code to ensure accuracy of all reference documentation
- Use ASCII art for diagrams (not image files) to keep docs portable
- Follow the YAML tutorial's style: copy-pasteable code, expected output, checkpoints
