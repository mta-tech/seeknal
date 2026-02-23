# QA Automation: Medallion E2E Pipeline Testing

**Date:** 2026-02-20
**Status:** Brainstorm
**Inspired by:** [Bowser](https://github.com/disler/bowser) — composable agent-driven automation

## What We're Building

A **spec-driven QA automation system** using Claude Code agents that:

1. **Discovers** YAML test specs from `qa/specs/` directory
2. **Scaffolds** complete seeknal project folders with medallion pipelines
3. **Executes** pipelines against live infrastructure (CSV, Iceberg via atlas-dev-server, PostgreSQL local)
4. **Validates** outputs end-to-end: DAG structure, pipeline execution, output existence, row counts, and latest feature behavior

The system tests the full medallion pattern (Bronze → Silver → Gold) across all three source/environment types, validating 15+ recently shipped features.

## Why This Approach (Spec-Driven Discovery)

Inspired by Bowser's architecture where **YAML user stories auto-integrate into the orchestration pipeline**:

- **Adding a new test scenario = dropping a new YAML spec file** — no code changes needed
- **Parallel fan-out**: Orchestrator discovers specs and spawns one agent per spec
- **Composable**: Each spec is independently testable
- **Extensible**: Future source types (Kafka, Hive, StarRocks) just need a new spec file

### Alternative Approaches Considered

| Approach | Why Not |
|----------|---------|
| Simplified Team QA (hardcoded agents) | Test scenarios buried in agent prompts, not declarative |
| Single Sequential Agent | No parallelism, doesn't demonstrate Bowser pattern |
| Traditional pytest E2E | Not agent-driven, no interactive debugging capability |

## Key Decisions

### 1. Form Factor: Claude Code Skill + Agent Team
A `/qa` skill is the entry point. It spawns an orchestrator agent that reads specs and fans out parallel worker agents.

### 2. Execution: Full Pipeline Against Live Infrastructure
Not just DAG validation — agents run `seeknal run` against real databases and verify outputs exist with correct row counts.

### 3. Infrastructure: Assume Pre-Existing
- **Iceberg**: atlas-dev-server with Lakekeeper already running
- **PostgreSQL**: Local docker-compose already running
- **CSV**: File-based, no infrastructure needed

### 4. Output: Dedicated QA Directory in Repo
Test projects created under `qa/` directory (gitignored). Persist after runs for manual inspection.

### 5. Team Structure: Orchestrator + Parallel Agents Per Spec
- **Orchestrator**: Discovers specs, creates shared project scaffold, spawns workers
- **Workers**: One per spec file. Each handles its source type's bronze/silver/gold layers

### 6. Validation Depth: Full
- DAG builds correctly (node count, edge count, topological order)
- Pipeline runs without errors (exit code 0)
- Output artifacts exist (parquet files, PostgreSQL tables)
- Row counts match expectations
- Latest features work (multi-target materialization, source defaults, named refs, etc.)

## Architecture: Bowser-Inspired Layers

```
Layer 4: /qa skill (entry point — like Bowser's Justfile)
    ↓
Layer 3: Orchestrator agent (discovers specs, fans out — like Bowser's Commands)
    ↓
Layer 2: Worker agents (one per spec — like Bowser's Subagents)
    ↓
Layer 1: seeknal CLI + DAGBuilder (actual execution — like Bowser's Skills)
```

## Spec Format (YAML)

Each spec in `qa/specs/` defines a complete test scenario:

```yaml
name: csv-medallion-pipeline
description: Test CSV source through full medallion pipeline
source_type: csv
infrastructure:
  requires: []  # CSV needs no external infra
project:
  name: qa-csv-medallion
  output_dir: qa/runs/csv-medallion
pipeline:
  bronze:
    sources:
      - name: raw_orders
        source: csv
        table: "data/orders.csv"
      - name: raw_customers
        source: csv
        table: "data/customers.csv"
  silver:
    transforms:
      - name: clean_orders
        inputs: [source.raw_orders]
        features_tested: [named_refs]
  gold:
    transforms:
      - name: customer_ltv
        inputs: [transform.clean_orders, source.raw_customers]
        features_tested: [named_refs]
validation:
  dag:
    expected_nodes: 4
    expected_edges: 3
  execution:
    exit_code: 0
  outputs:
    - name: customer_ltv
      type: parquet
      min_rows: 5
features_tested:
  - named_refs
  - csv_source
```

## Features to Test Per Source Type

### CSV Agent
- CSV file sources (bronze)
- Named ref() syntax in transforms
- Basic materialization (Iceberg default)

### Iceberg Agent
- Iceberg sources via Lakekeeper (bronze)
- Source defaults from profiles.yml (catalog_uri, warehouse)
- Environment variable interpolation (${LAKEKEEPER_URI:default})
- Inline param override priority
- Named ref() syntax
- Iceberg materialization

### PostgreSQL Agent
- PostgreSQL source with profile-based connections (bronze)
- Pushdown query optimization (query: field)
- Multi-target materialization (PostgreSQL + Iceberg)
- Three PostgreSQL write modes: full, incremental_by_time, upsert_by_key
- Source defaults with postgres → postgresql alias normalization
- Profile path plumbing (--profile flag)

## Success Criteria

1. All 3 spec-driven agents pass their full validation suite
2. Test projects persist under `qa/runs/` for manual inspection
3. Adding a 4th source type test requires only a new YAML spec file
4. Total QA run completes in < 10 minutes (parallel execution)

## Resolved Questions

1. **Seed data strategy**: Each spec embeds its own data — self-contained specs with own data/ directory. No cross-contamination.
2. **Spec runner implementation**: Claude Code agent with Team tools — pure agent approach using TeamCreate + Task to fan out workers.
3. **Failure handling**: Best-effort — if one agent fails, others continue. Report all results at the end.

## Open Questions

None — all questions resolved.
