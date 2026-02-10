---
title: "Phase 2: Safety & Speed — Virtual Environments, Change Categorization, Parallel Execution"
type: feat
date: 2026-02-09
status: ready
source_plan: docs/plans/2026-02-09-feat-phase2-safety-speed-plan.md
---

# Plan: Phase 2 — Safety & Speed

## Overview

Three features that upgrade Seeknal from a functional pipeline tool into a production-safe platform:

1. **Feature 8: Model Change Categorization** — Classify changes as BREAKING / NON_BREAKING / METADATA
2. **Feature 1: Virtual Environments & Plan/Apply** — Isolated dev/staging environments
3. **Feature 9: Parallel Node Execution** — Concurrent execution of independent DAG nodes

**Key Deliverables:**
- Change categorization engine with complete classification rules (~25 tests)
- `seeknal env plan/apply/promote/list/delete` CLI subcommands (~25 tests)
- `seeknal run --parallel --max-workers N` parallel execution (~20 tests)

**Architecture Note:**
- Build on existing `diff.py`, `runner.py`, `state.py` infrastructure
- Follow existing CLI patterns: Typer commands, `_echo_*` helpers, `handle_cli_error` decorator
- DuckDB is primary engine; thread-local connections for parallel execution

## Task Description

Implement Phase 2 of the SQLMesh-inspired features plan. Phase 1 (fingerprinting + audits) is complete with 153 passing tests. Phase 2 adds safety mechanisms (change categorization, environments) and performance (parallel execution).

The existing CLI style uses:
- `_echo_success()` / `_echo_error()` / `_echo_warning()` / `_echo_info()` for colored output
- `handle_cli_error()` decorator for consistent error handling
- `typer.Argument()` for positional args, `typer.Option()` for flags
- `app.add_typer()` for subcommand groups (version, source, iceberg, atlas)
- Rich table formatting for tabular output
- `--project-path` option pattern for project directory override
- `--dry-run` flag pattern for preview without side effects

All new CLI commands MUST follow these exact patterns. No new output helpers, no different formatting conventions.

## Objective

- Enable safe pipeline development with change preview and environment isolation
- Classify changes by impact to prevent unnecessary rebuilds and surface risks
- Reduce execution time on wide DAGs through parallel node execution
- Maintain backward compatibility with all existing CLI commands

## Problem Statement

See source plan: `docs/plans/2026-02-09-feat-phase2-safety-speed-plan.md` — Problem Statement section.

## Proposed Solution

See source plan: Phase A, Phase B, Phase C sections for complete technical approach.

## Relevant Files

### Existing Files (modify)
- `src/seeknal/dag/diff.py` — Add ChangeCategory, classify_change(), category-aware rebuild
- `src/seeknal/dag/manifest.py` — Already has NodeType, Node (read-only reference)
- `src/seeknal/workflow/runner.py` — Add `_get_topological_layers()`, `env_path` parameter
- `src/seeknal/workflow/state.py` — Already has NodeFingerprint, RunState (read-only reference)
- `src/seeknal/workflow/executors/base.py` — Add thread-local DuckDB connection support
- `src/seeknal/cli/main.py` — Add `env` subcommand group, `--parallel` flag to `run`

### New Files to Create
- `src/seeknal/workflow/environment.py` — EnvironmentManager, EnvironmentConfig, EnvironmentPlan
- `src/seeknal/workflow/parallel.py` — ParallelDAGRunner
- `tests/dag/test_change_categorization.py` — ~25 tests
- `tests/workflow/test_environments.py` — ~25 tests
- `tests/workflow/test_parallel.py` — ~20 tests

## Implementation Phases

### Phase A: Model Change Categorization (Feature 8)

**Dependencies:** None
**Stories:** 4 tasks

### Phase B: Virtual Environments (Feature 1)

**Dependencies:** Phase A (uses change categorization in plan output)
**Stories:** 4 tasks

### Phase C: Parallel Execution (Feature 9)

**Dependencies:** None (can run in parallel with Phase B)
**Stories:** 4 tasks

## Team Orchestration

### Team Members

#### builder-categorization
- **Name:** builder-categorization
- **Role:** Backend — Change categorization engine
- **Agent Type:** general-purpose
- **Focus:** `src/seeknal/dag/diff.py`, `tests/dag/test_change_categorization.py`
- **Resume:** true

#### builder-environments
- **Name:** builder-environments
- **Role:** Backend + CLI — Virtual environments system
- **Agent Type:** general-purpose
- **Focus:** `src/seeknal/workflow/environment.py`, `src/seeknal/cli/main.py`, `tests/workflow/test_environments.py`
- **Resume:** true

#### builder-parallel
- **Name:** builder-parallel
- **Role:** Backend + CLI — Parallel execution engine
- **Agent Type:** general-purpose
- **Focus:** `src/seeknal/workflow/parallel.py`, `src/seeknal/workflow/runner.py`, `tests/workflow/test_parallel.py`
- **Resume:** true

#### dx-evaluator-data-eng
- **Name:** dx-evaluator-data-eng
- **Role:** DX Evaluator — Data Engineer persona
- **Agent Type:** general-purpose
- **Focus:** Evaluate CLI commands, YAML pipeline DX, error messages from a Data Engineer perspective. Check that `seeknal env plan/apply/promote` workflows feel natural for someone managing production pipelines. Verify `--parallel` flag and `--continue-on-error` are intuitive. Check plan output readability for large DAGs. Ensure existing `seeknal run`, `seeknal parse`, `seeknal apply` commands are NOT broken or changed.
- **Resume:** true

#### dx-evaluator-analytics-eng
- **Name:** dx-evaluator-analytics-eng
- **Role:** DX Evaluator — Analytics Engineer persona
- **Agent Type:** general-purpose
- **Focus:** Evaluate CLI commands and YAML from an Analytics Engineer perspective. Check that change categorization output is understandable (BREAKING/NON_BREAKING/METADATA labels). Verify `seeknal env plan dev` output clearly shows what will change and why. Ensure metric query commands (`seeknal query`) still work. Check that environment promote workflow makes sense for someone publishing dashboards.
- **Resume:** true

#### dx-evaluator-ml-eng
- **Name:** dx-evaluator-ml-eng
- **Role:** DX Evaluator — ML Engineer persona
- **Agent Type:** general-purpose
- **Focus:** Evaluate CLI and Python pipeline DX from an ML Engineer perspective. Check that parallel execution improves iteration speed for feature engineering. Verify environment isolation works for experiment branches. Ensure Python pipeline nodes (`kind: python`) work correctly with parallel execution. Check that `seeknal run --parallel --max-workers 4` helps with multi-source feature pipelines.
- **Resume:** true

#### docs-data-eng
- **Name:** docs-data-eng
- **Role:** Documentation — Data Engineer use case tutorial + example YAML pipeline
- **Agent Type:** general-purpose
- **Focus:** Write a complete tutorial and working example YAML pipeline for a **Data Engineer** use case: "Production Pipeline Deployment with Environment Safety". Covers: building a multi-source ETL pipeline, using `seeknal env plan` to preview changes, applying in dev environment, promoting to production. Includes tested YAML files, CLI commands, and expected output. Tutorial goes in `docs/tutorials/`, example YAML in `examples/phase2-data-eng/`.
- **Resume:** true

#### docs-analytics-eng
- **Name:** docs-analytics-eng
- **Role:** Documentation — Analytics Engineer use case tutorial + example YAML pipeline
- **Agent Type:** general-purpose
- **Focus:** Write a complete tutorial and working example YAML pipeline for an **Analytics Engineer** use case: "Metric Definition & Safe Schema Evolution". Covers: defining semantic models + metrics in YAML, using change categorization to understand impact of schema changes, querying metrics with `seeknal query`, deploying to StarRocks with `seeknal deploy-metrics`. Includes tested YAML files, CLI commands, and expected output. Tutorial goes in `docs/tutorials/`, example YAML in `examples/phase2-analytics-eng/`.
- **Resume:** true

#### docs-ml-eng
- **Name:** docs-ml-eng
- **Role:** Documentation — ML Engineer use case tutorial + example YAML pipeline
- **Agent Type:** general-purpose
- **Focus:** Write a complete tutorial and working example YAML pipeline for an **ML Engineer** use case: "Feature Engineering at Scale with Parallel Execution". Covers: building a multi-source feature pipeline (10+ sources), running with `--parallel` for speed, using environments to experiment with feature definitions, auditing feature quality. Includes tested YAML files, CLI commands, and expected output. Tutorial goes in `docs/tutorials/`, example YAML in `examples/phase2-ml-eng/`.
- **Resume:** true

### Orchestration Strategy

```
Phase A: builder-categorization (blocking — Phase B depends on this)
    ↓ (when complete)
Phase B: builder-environments    ← depends on Phase A
Phase C: builder-parallel        ← independent, starts with Phase B
    ↓ (when B and C complete)
DX Evaluation: All 3 evaluators run in parallel
    ↓ (when all evaluators complete)
Fix: Builders address DX feedback
    ↓ (when fixes complete)
Documentation: All 3 doc writers run in parallel
    ↓ (when all docs complete)
Final: Verify all tutorials' YAML examples work with the built features
```

**Parallel execution:**
- Phase B and Phase C run simultaneously (different files, no overlap)
- All 3 DX evaluators run simultaneously after builders finish
- DX evaluators are READ-ONLY — they evaluate and report, builders fix
- All 3 doc writers run simultaneously after DX fixes are done
- Each doc writer creates a complete, tested tutorial with working YAML examples

## Step by Step Tasks

### 1. Implement ChangeCategory enum and classification logic
- **Task ID:** phase-a-categorization-engine
- **Depends On:** none
- **Assigned To:** builder-categorization
- **Agent Type:** general-purpose
- **Parallel:** false (blocking for Phase B)

**Implementation:**
1. Add `ChangeCategory` enum to `src/seeknal/dag/diff.py` (BREAKING, NON_BREAKING, METADATA)
2. Extend `NodeChange` dataclass with `category` and `changed_details` fields
3. Implement `classify_change()` static method on `ManifestDiff` following the complete rule table from the source plan (A2)
4. Implement `_classify_column_change()` — compare old/new column dicts for adds, removes, type changes
5. Implement `_classify_config_change()` — deep-diff config dict with per-key categorization
6. Implement `_severity()` helper for max() precedence ordering

**Acceptance Criteria:**
- [ ] `ChangeCategory` enum with 3 values
- [ ] `classify_change()` handles all 20+ rules in the classification table
- [ ] Precedence: BREAKING > NON_BREAKING > METADATA for multi-field changes
- [ ] Unknown config keys default to NON_BREAKING

### 2. Integrate categorization with rebuild logic and plan output
- **Task ID:** phase-a-rebuild-and-output
- **Depends On:** phase-a-categorization-engine
- **Assigned To:** builder-categorization
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Modify `get_nodes_to_rebuild()` to return `dict[str, ChangeCategory]` instead of `set[str]`
2. BREAKING and NON_BREAKING propagate downstream via BFS; METADATA does not
3. Add `_bfs_downstream()` helper method (extract from existing `get_nodes_to_rebuild`)
4. Add `format_plan_output()` method for CLI display using existing `_echo_*` patterns
5. Write ~25 tests in `tests/dag/test_change_categorization.py`

**Acceptance Criteria:**
- [ ] Category-aware rebuild map
- [ ] METADATA changes don't propagate downstream
- [ ] BREAKING shows downstream impact count in plan output
- [ ] Plan output uses `_echo_info`/`_echo_warning`/`_echo_error` pattern (no new helpers)
- [ ] ~25 tests passing
- [ ] All existing diff.py behavior unchanged (backward compatible return type)

### 3. Implement EnvironmentManager and CLI commands
- **Task ID:** phase-b-environment-system
- **Depends On:** phase-a-rebuild-and-output
- **Assigned To:** builder-environments
- **Agent Type:** general-purpose
- **Parallel:** true (runs alongside phase-c-parallel-engine)

**Implementation:**
1. Create `src/seeknal/workflow/environment.py` with:
   - `EnvironmentConfig`, `EnvironmentPlan`, `EnvironmentRef` dataclasses
   - `EnvironmentManager` class with `plan()`, `apply()`, `promote()`, `list_environments()`, `delete_environment()`, `cleanup_expired()`
2. Add `env_app = typer.Typer()` / `app.add_typer(env_app, name="env")` to `main.py`
3. Implement 5 CLI commands: `env plan`, `env apply`, `env promote`, `env list`, `env delete`
4. Follow existing CLI patterns exactly:
   - `typer.Argument(...)` for env_name
   - `typer.Option(".", "--project-path")` for project directory
   - `_echo_info()` for progress, `_echo_success()` for completion, `_echo_error()` for failures
   - `handle_cli_error()` decorator on each command
   - `typer.confirm()` for destructive operations (promote, delete)
5. Modify `DAGRunner.__init__()` to accept optional `env_path` parameter
6. Implement reference loading for unchanged nodes
7. Write ~25 tests in `tests/workflow/test_environments.py`

**CLI Style Requirements (MUST follow existing patterns):**
- `seeknal env plan dev` — NOT `seeknal environment plan dev` (short subcommand names like `version`, `source`)
- `seeknal env list` — uses `_echo_info` for headers, table formatting for list
- `seeknal env promote dev prod` — positional args (like `seeknal version diff fg --from 1 --to 2`)
- `seeknal env delete dev` — requires `typer.confirm()` before deletion
- Error messages: `_echo_error("Environment 'dev' not found")` — consistent with existing style
- Success: `_echo_success("Plan saved to target/environments/dev/plan.json")`

**Acceptance Criteria:**
- [ ] 5 CLI commands working with correct Typer patterns
- [ ] Environment isolation (separate state/cache per env)
- [ ] Plan staleness detection
- [ ] Atomic promote via temp dir + rename
- [ ] TTL-based cleanup
- [ ] Production references for unchanged nodes
- [ ] Existing `seeknal apply` and `seeknal run` unchanged
- [ ] ~25 tests passing

### 4. Implement parallel execution engine
- **Task ID:** phase-c-parallel-engine
- **Depends On:** none
- **Assigned To:** builder-parallel
- **Agent Type:** general-purpose
- **Parallel:** true (runs alongside phase-b-environment-system)

**Implementation:**
1. Add `_get_topological_layers()` to `DAGRunner` in `runner.py`
2. Create `src/seeknal/workflow/parallel.py` with `ParallelDAGRunner` class
3. Implement per-thread DuckDB connections via `threading.local()` in `ExecutionContext`
4. Implement upstream cache loading per thread
5. Implement batch state updates per layer (single writer with `threading.Lock`)
6. Add `--parallel` and `--max-workers` flags to existing `seeknal run` command
7. Also wire `--parallel` into `seeknal env apply` (for Phase B integration)
8. Write ~20 tests in `tests/workflow/test_parallel.py`

**CLI Style Requirements:**
- `seeknal run --parallel` — flag on existing command, NOT a new command
- `seeknal run --parallel --max-workers 4` — consistent with existing `--option value` pattern
- Progress output: `_echo_info(f"[Layer {i+1}/{total}] Running {len(layer)} nodes in parallel...")`
- Completion: `_echo_success(f"Pipeline complete: {total} nodes, {parallel_layers} layers")`
- Error: existing `_echo_error()` pattern with `--continue-on-error` flag

**Acceptance Criteria:**
- [ ] Topological layer computation correct for all DAG shapes
- [ ] Parallel execution with ThreadPoolExecutor
- [ ] Per-thread DuckDB connections (no sharing)
- [ ] Batch state updates per layer
- [ ] Failed nodes → downstream skipped
- [ ] `--continue-on-error` works
- [ ] `--max-workers 1` equivalent to sequential
- [ ] Existing sequential `seeknal run` unchanged
- [ ] ~20 tests passing

### 5. DX Evaluation — Data Engineer perspective
- **Task ID:** dx-eval-data-eng
- **Depends On:** phase-b-environment-system, phase-c-parallel-engine
- **Assigned To:** dx-evaluator-data-eng
- **Agent Type:** general-purpose
- **Parallel:** true (all 3 evaluators run simultaneously)

**Evaluation Checklist:**
1. **CLI consistency**: Do new commands follow existing `seeknal` CLI style? Check:
   - `seeknal env plan dev` — does output match `seeknal run` output style?
   - `seeknal env list` — does table format match `seeknal list feature-groups`?
   - `seeknal run --parallel` — is the flag discoverable? Does `seeknal run --help` show it?
2. **YAML pipeline DX**: Do existing YAML pipelines work unchanged with the new features?
   - Source nodes: unchanged
   - Transform nodes: unchanged
   - Aggregation nodes: unchanged
   - Exposure nodes: unchanged
3. **Python pipeline DX**: Do existing Python pipeline nodes work with parallel execution?
4. **Error messages**: Are error messages clear and actionable?
   - "Plan is stale — run `seeknal env plan dev` to refresh"
   - "Environment 'dev' not found"
   - "Node transform.X failed: [error]. Downstream nodes skipped."
5. **Help text**: Is `seeknal env --help` comprehensive? Does it list all subcommands?
6. **Backward compatibility**: Run `seeknal --help` and verify ALL existing commands appear unchanged
7. **Workflow flow**: Does the plan → apply → promote workflow feel natural?
   - Is the promote syntax `seeknal env promote dev prod` intuitive?
   - Is `--force` flag for stale plans clear?

**Output:** Written report with findings, categorized as MUST-FIX, SHOULD-FIX, NICE-TO-HAVE

### 6. DX Evaluation — Analytics Engineer perspective
- **Task ID:** dx-eval-analytics-eng
- **Depends On:** phase-b-environment-system, phase-c-parallel-engine
- **Assigned To:** dx-evaluator-analytics-eng
- **Agent Type:** general-purpose
- **Parallel:** true

**Evaluation Checklist:**
1. **Change categorization clarity**: Is the BREAKING/NON_BREAKING/METADATA labeling understandable?
   - Would an AE know what "NON_BREAKING" means without reading docs?
   - Is the plan output clear about what will rebuild and why?
   - Is the downstream impact tree readable for complex DAGs?
2. **Metric query integration**: Does `seeknal query --metrics total_revenue --compile` still work?
3. **Environment workflow for dashboards**: Does the env workflow make sense for someone publishing metric definitions?
   - `seeknal env plan staging` → see metric SQL changes
   - `seeknal env apply staging` → test against staging data
   - `seeknal env promote staging prod` → deploy to production
4. **CLI discoverability**: Can an AE find the new commands from `seeknal --help`?
5. **Existing commands unchanged**: Verify `seeknal query`, `seeknal deploy-metrics` still work

**Output:** Written report with findings

### 7. DX Evaluation — ML Engineer perspective
- **Task ID:** dx-eval-ml-eng
- **Depends On:** phase-b-environment-system, phase-c-parallel-engine
- **Assigned To:** dx-evaluator-ml-eng
- **Agent Type:** general-purpose
- **Parallel:** true

**Evaluation Checklist:**
1. **Parallel execution for feature engineering**: Does `--parallel` help with multi-source feature pipelines?
   - 10 source nodes → should parallelize nicely
   - Feature group computation → depends on transforms, correct layer ordering?
2. **Environment isolation for experiments**: Can an ML eng safely experiment?
   - Modify feature definitions in `seeknal env plan experiment`
   - See which features changed (BREAKING = feature removed, NON_BREAKING = formula changed)
   - Apply and test without affecting production feature store
3. **Python pipeline compatibility**: Do `kind: python` nodes work with parallel execution?
   - Thread safety of Python executors
   - Process isolation concerns
4. **Existing workflow intact**: `seeknal run`, `seeknal materialize`, `seeknal repl` unchanged
5. **Performance expectations**: Is `--max-workers` clear? What's the expected speedup?

**Output:** Written report with findings

### 8. Address DX feedback
- **Task ID:** dx-fixes
- **Depends On:** dx-eval-data-eng, dx-eval-analytics-eng, dx-eval-ml-eng
- **Assigned To:** builder-categorization, builder-environments, builder-parallel (as needed)
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Review all 3 DX evaluation reports
2. Address all MUST-FIX items
3. Address SHOULD-FIX items that are low-effort
4. Document NICE-TO-HAVE items for future work
5. Re-run all tests to confirm no regressions

**Acceptance Criteria:**
- [ ] All MUST-FIX items resolved
- [ ] All ~70 new tests still passing
- [ ] All 153 existing Feature 11 tests still passing
- [ ] Existing CLI commands verified unchanged

### 9. Documentation — Data Engineer Tutorial: "Production Pipeline Deployment with Environment Safety"
- **Task ID:** docs-tutorial-data-eng
- **Depends On:** dx-fixes
- **Assigned To:** docs-data-eng
- **Agent Type:** general-purpose
- **Parallel:** true (all 3 doc writers run simultaneously)

**Use Case:** A Data Engineer at an e-commerce company manages a nightly ETL pipeline that ingests orders, customer data, and inventory from 3 PostgreSQL sources, transforms them into a clean data warehouse schema, and produces daily aggregates for reporting. They need to:
- Safely modify a transform's SQL without breaking production
- Preview which downstream nodes will rebuild
- Test changes in a dev environment before deploying

**Deliverables:**

1. **Tutorial:** `docs/tutorials/phase2-data-eng-environments.md`
   - Title: "Production Pipeline Deployment with Environment Safety"
   - Estimated Time: 45 minutes | Difficulty: Intermediate
   - Follows existing tutorial format (see `yaml-pipeline-tutorial.md` for template)
   - Sections:
     - Part 1: Setup — Create a 3-source pipeline (orders, customers, inventory)
     - Part 2: First Run — Execute the full pipeline with `seeknal run`
     - Part 3: Make a Change — Modify `transform.clean_orders` SQL (add a new column)
     - Part 4: Plan the Change — `seeknal env plan dev` → see NON_BREAKING classification
     - Part 5: Apply in Dev — `seeknal env apply dev` → execute in isolation
     - Part 6: Verify — Query dev environment data, compare with production
     - Part 7: Promote — `seeknal env promote dev` → deploy to production
     - Part 8: Breaking Change — Remove a column, see BREAKING classification + downstream impact
     - Part 9: Parallel Execution — Re-run with `seeknal run --parallel --max-workers 4`
   - Each section shows: CLI command, expected output, explanation

2. **Example YAML Pipeline:** `examples/phase2-data-eng/`
   ```
   examples/phase2-data-eng/
   ├── README.md
   ├── 01_source_orders.yml
   ├── 02_source_customers.yml
   ├── 03_source_inventory.yml
   ├── 04_transform_clean_orders.yml
   ├── 05_transform_customer_orders.yml
   ├── 06_aggregation_daily_revenue.yml
   ├── 07_rule_order_validation.yml
   └── 08_exposure_warehouse.yml
   ```

3. **Tests:** `tests/tutorials/test_data_eng_tutorial.py`
   - Test that all example YAML files parse correctly
   - Test that the pipeline DAG builds without cycles
   - Test that change categorization works for the tutorial's breaking/non-breaking scenarios
   - Test that dry-run mode produces expected output

**Acceptance Criteria:**
- [ ] Tutorial follows existing format (time estimate, difficulty, TOC, step-by-step)
- [ ] All example YAML files parse correctly with `seeknal parse`
- [ ] Pipeline DAG builds without cycles
- [ ] CLI commands in tutorial produce output matching documented expectations
- [ ] Tests pass verifying tutorial scenarios
- [ ] No references to non-existent commands or flags

### 10. Documentation — Analytics Engineer Tutorial: "Metric Definition & Safe Schema Evolution"
- **Task ID:** docs-tutorial-analytics-eng
- **Depends On:** dx-fixes
- **Assigned To:** docs-analytics-eng
- **Agent Type:** general-purpose
- **Parallel:** true

**Use Case:** An Analytics Engineer at a SaaS company defines revenue metrics for executive dashboards. They maintain semantic models mapping database tables to business entities, and define metrics (total revenue, MRR, churn rate) that Metabase dashboards consume. They need to:
- Define semantic models and metrics in YAML
- Query metrics from the CLI to verify SQL compilation
- Evolve the schema safely (add dimensions, rename measures) without breaking dashboards
- Deploy metrics as StarRocks materialized views

**Deliverables:**

1. **Tutorial:** `docs/tutorials/phase2-analytics-eng-metrics.md`
   - Title: "Metric Definition & Safe Schema Evolution"
   - Estimated Time: 60 minutes | Difficulty: Intermediate
   - Sections:
     - Part 1: Setup — Create source + transform pipeline for a SaaS subscriptions dataset
     - Part 2: Define Semantic Models — `seeknal/semantic_models/subscriptions.yml` with entities, dimensions, measures
     - Part 3: Define Metrics — `seeknal/metrics/revenue.yml` with simple, ratio, derived metrics
     - Part 4: Query Metrics — `seeknal query --metrics total_mrr --dimensions plan_type --compile`
     - Part 5: Add a Dimension — Add `region` dimension, see NON_BREAKING classification
     - Part 6: Rename a Measure — Rename `revenue` → `gross_revenue`, see BREAKING + downstream impact
     - Part 7: Environment Safety — `seeknal env plan staging` → review → `seeknal env apply staging`
     - Part 8: Deploy Metrics — `seeknal deploy-metrics --dry-run` → see generated MV DDL
     - Part 9: Change Categorization Deep Dive — table showing all category types with examples

2. **Example YAML Pipeline:** `examples/phase2-analytics-eng/`
   ```
   examples/phase2-analytics-eng/
   ├── README.md
   ├── seeknal/
   │   ├── 01_source_subscriptions.yml
   │   ├── 02_source_customers.yml
   │   ├── 03_transform_subscription_summary.yml
   │   ├── semantic_models/
   │   │   └── subscriptions.yml
   │   └── metrics/
   │       ├── revenue.yml
   │       └── retention.yml
   ```

3. **Tests:** `tests/tutorials/test_analytics_eng_tutorial.py`
   - Test that semantic model YAML parses correctly
   - Test that metric YAML parses correctly
   - Test that `MetricCompiler` compiles the tutorial's metrics to valid SQL
   - Test that change categorization for the rename scenario produces BREAKING
   - Test that deploy dry-run produces valid DDL

**Acceptance Criteria:**
- [ ] Tutorial follows existing format
- [ ] Semantic model and metric YAML files parse correctly
- [ ] `seeknal query --compile` produces valid SQL for all tutorial metrics
- [ ] Change categorization examples are accurate
- [ ] Deploy dry-run DDL is valid StarRocks syntax
- [ ] Tests pass verifying all tutorial scenarios

### 11. Documentation — ML Engineer Tutorial: "Feature Engineering at Scale with Parallel Execution"
- **Task ID:** docs-tutorial-ml-eng
- **Depends On:** dx-fixes
- **Assigned To:** docs-ml-eng
- **Agent Type:** general-purpose
- **Parallel:** true

**Use Case:** An ML Engineer at a fintech company builds a fraud detection feature pipeline. The pipeline ingests 8 different data sources (transactions, user profiles, device fingerprints, IP geolocation, merchant data, card data, historical fraud labels, real-time alerts), creates 20+ features, and trains a model. They need to:
- Run the wide DAG fast using parallel execution
- Experiment with new features in a dev environment
- Audit feature quality before training
- Understand which feature changes are breaking

**Deliverables:**

1. **Tutorial:** `docs/tutorials/phase2-ml-eng-parallel.md`
   - Title: "Feature Engineering at Scale with Parallel Execution"
   - Estimated Time: 60 minutes | Difficulty: Intermediate-Advanced
   - Sections:
     - Part 1: Setup — Create a wide pipeline with 8 sources (using CSV sample data)
     - Part 2: Sequential Baseline — `seeknal run` → note execution time
     - Part 3: Parallel Execution — `seeknal run --parallel --max-workers 4` → compare speedup
     - Part 4: Understanding Layers — Show topological layer visualization
     - Part 5: Feature Experimentation — `seeknal env plan experiment` to test new features
     - Part 6: Audit Integration — Define audits on feature group (not_null, value ranges)
     - Part 7: Breaking Change Awareness — Remove a feature, see BREAKING → model retraining needed
     - Part 8: Python Pipeline Integration — Mix YAML + Python nodes, verify parallel safety
     - Part 9: Production Workflow — Full cycle: env plan → apply → audit → promote

2. **Example YAML Pipeline:** `examples/phase2-ml-eng/`
   ```
   examples/phase2-ml-eng/
   ├── README.md
   ├── 01_source_transactions.yml
   ├── 02_source_user_profiles.yml
   ├── 03_source_devices.yml
   ├── 04_source_ip_geo.yml
   ├── 05_source_merchants.yml
   ├── 06_source_cards.yml
   ├── 07_source_fraud_labels.yml
   ├── 08_source_alerts.yml
   ├── 09_transform_user_enrichment.yml
   ├── 10_transform_transaction_features.yml
   ├── 11_transform_device_risk.yml
   ├── 12_feature_group_fraud_features.yml
   ├── 13_aggregation_user_risk_score.yml
   ├── 14_rule_feature_quality.yml
   └── 15_exposure_training_data.yml
   ```

3. **Tests:** `tests/tutorials/test_ml_eng_tutorial.py`
   - Test that all 15 YAML files parse correctly
   - Test that the wide DAG produces correct topological layers (sources in layer 0)
   - Test that parallel execution with `--max-workers 1` produces same results as sequential
   - Test that audit rules validate correctly
   - Test that environment isolation works for the experiment scenario

**Acceptance Criteria:**
- [ ] Tutorial follows existing format
- [ ] All 15 example YAML files parse correctly
- [ ] Wide DAG produces 8 sources in parallel layer 0
- [ ] Parallel execution produces same results as sequential
- [ ] Audit rules fire correctly on sample data
- [ ] Tests pass verifying all tutorial scenarios

### 12. Final Verification — All tutorials tested end-to-end
- **Task ID:** final-verification
- **Depends On:** docs-tutorial-data-eng, docs-tutorial-analytics-eng, docs-tutorial-ml-eng
- **Assigned To:** builder-categorization (or any available builder)
- **Agent Type:** general-purpose
- **Parallel:** false

**Implementation:**
1. Run all tutorial tests:
   ```bash
   pytest tests/tutorials/ -v
   ```
2. Run all Phase 2 tests:
   ```bash
   pytest tests/dag/test_change_categorization.py tests/workflow/test_environments.py tests/workflow/test_parallel.py -v
   ```
3. Run all existing Feature 11 tests (regression):
   ```bash
   pytest tests/connections/test_starrocks.py tests/cli/test_repl_starrocks.py tests/workflow/test_starrocks_executors.py tests/workflow/test_semantic.py tests/workflow/test_semantic_deploy.py tests/cli/test_query_command.py -v
   ```
4. Verify `seeknal --help` shows all commands correctly

**Acceptance Criteria:**
- [ ] All tutorial tests pass
- [ ] All ~70 Phase 2 tests pass
- [ ] All 153 Feature 11 tests pass (regression)
- [ ] All example YAML pipelines in `examples/phase2-*/` are valid
- [ ] `seeknal --help` shows `env` subcommand group

## Acceptance Criteria

### Functional Requirements
- [ ] `ChangeCategory` enum classifies all 20+ change types correctly
- [ ] `seeknal env plan <name>` shows categorized diff with downstream impact
- [ ] `seeknal env apply <name>` executes in isolated environment
- [ ] `seeknal env promote <from> <to>` atomically promotes to target
- [ ] `seeknal env list` shows all environments with status
- [ ] `seeknal env delete <name>` removes environment with confirmation
- [ ] `seeknal run --parallel --max-workers N` executes independent nodes concurrently
- [ ] Plan staleness detection with `--force` override
- [ ] TTL-based environment cleanup

### Non-Functional Requirements
- [ ] All new CLI commands use existing `_echo_*` helpers (no new output patterns)
- [ ] All new CLI commands use `handle_cli_error()` decorator
- [ ] All new CLI commands use existing `typer.Argument`/`typer.Option` patterns
- [ ] Parallel execution: 2-4x speedup on 10-source DAG
- [ ] State consistency: parallel run produces same final state as sequential
- [ ] Environment isolation: env run does NOT modify production state

### Quality Gates
- [ ] ~70 new tests passing (25 + 25 + 20)
- [ ] All 153 existing Feature 11 tests still passing
- [ ] All existing CLI commands verified unchanged (help text, behavior)
- [ ] 3 DX evaluation reports completed and MUST-FIX items addressed
- [ ] No new security vulnerabilities (SQL injection in plan output, path traversal in env names)

## Success Metrics

| Metric | Current | Target | Measurement |
|--------|---------|--------|-------------|
| Change categorization accuracy | N/A | 95%+ | Manual verification on 20+ test cases |
| Environment isolation | N/A | 100% | No production state mutation during env runs |
| Parallel speedup (10-source DAG) | 1x (sequential) | 2-4x | Benchmark timing comparison |
| New test count | 0 | ~70 | pytest count |
| Existing test regression | 153 passing | 153 passing | pytest verification |
| CLI backward compatibility | All commands work | All commands work | `seeknal --help` verification |

## Dependencies & Prerequisites

### External Dependencies
| Dependency | Version | Purpose | Risk |
|------------|---------|---------|------|
| concurrent.futures | stdlib | Thread pool for parallel execution | None (stdlib) |
| threading | stdlib | Locks, thread-local storage | None (stdlib) |
| DuckDB | existing | Per-thread connections | Low (well-tested thread safety when isolated) |

### Internal Dependencies
| Dependency | Status | Notes |
|------------|--------|-------|
| Phase 1 (Features 2 & 4) | Complete | 153 tests passing |
| `src/seeknal/dag/diff.py` | Exists | Extend with categorization |
| `src/seeknal/workflow/runner.py` | Exists | Extend with layers + env_path |
| `src/seeknal/workflow/state.py` | Exists | Read-only reference |

## Risk Analysis & Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| False METADATA classification | Low | High (stale data) | METADATA only for description/owner/tags — conservative |
| Promote corrupts production | Low | High | Atomic promote via temp dir + rename |
| DuckDB thread safety | Low | High | Strict per-thread connections |
| Memory exhaustion (parallel) | Medium | Medium | Cap max_workers at 8 |
| State corruption (parallel) | Low | High | Batch updates per layer with Lock |
| CLI backward incompatibility | Low | High | DX evaluators verify all existing commands |

## Validation Commands

```bash
# Run all new tests
pytest tests/dag/test_change_categorization.py tests/workflow/test_environments.py tests/workflow/test_parallel.py -v

# Run all existing Feature 11 tests (regression check)
pytest tests/connections/test_starrocks.py tests/cli/test_repl_starrocks.py tests/workflow/test_starrocks_executors.py tests/workflow/test_semantic.py tests/workflow/test_semantic_deploy.py tests/cli/test_query_command.py -v

# Verify CLI backward compatibility
seeknal --help
seeknal env --help
seeknal run --help
```

## Notes

- Source plan: `docs/plans/2026-02-09-feat-phase2-safety-speed-plan.md` contains full technical design
- Phase C (parallel) is independent and can run alongside Phase B (environments)
- DX evaluators are READ-ONLY — they review code and CLI output, they don't write code
- The 3 persona evaluators (Data Eng, Analytics Eng, ML Eng) match the project's target personas

---

## Checklist Summary

### Phase A: Change Categorization (Feature 8)
- [ ] Task 1: ChangeCategory enum + classification logic
- [ ] Task 2: Rebuild integration + plan output + tests

### Phase B: Virtual Environments (Feature 1)
- [ ] Task 3: EnvironmentManager + CLI commands + tests

### Phase C: Parallel Execution (Feature 9)
- [ ] Task 4: ParallelDAGRunner + CLI flags + tests

### DX Evaluation
- [ ] Task 5: Data Engineer evaluation
- [ ] Task 6: Analytics Engineer evaluation
- [ ] Task 7: ML Engineer evaluation

### Final
- [ ] Task 8: Address DX feedback + regression check
