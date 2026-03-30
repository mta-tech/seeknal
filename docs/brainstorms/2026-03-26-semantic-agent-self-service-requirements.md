---
date: 2026-03-26
topic: semantic-agent-self-service
---

# Semantic Agent Self-Service

## Problem Frame

Business analysts using `seeknal ask` get raw SQL answers with no awareness of the project's semantic layer. The agent generates ad-hoc SQL every time, producing inconsistent metric definitions across sessions. Meanwhile, seeknal has a fully-built semantic layer (SemanticModel, Metric, MetricCompiler with auto-join, MetricRegistry) that encodes the correct business definitions — but the agent doesn't know it exists.

**Who is affected:** Both SQL-literate analysts (who want consistency) and metric-native analysts (who think in "revenue" not "SUM(amount)").

**What changes:** The semantic layer becomes the agent's primary query interface. Business vocabulary (aliases, descriptions) lives directly on semantic model definitions. A bootstrap command enables zero-config onboarding.

## Requirements

### Core: Wire Semantic Layer to Agent

- R1. New agent tool `query_metric` that accepts metric names, dimensions, and filters. It loads project semantic models, constructs a MetricQuery, compiles SQL via MetricCompiler, executes via DuckDB, and returns formatted results.
- R2. ArtifactDiscovery discovers semantic model and metric YAML files alongside existing pipeline artifacts. A new prompt section (`semantic.j2`) lists available metrics with their descriptions, measures, dimensions, and aliases.
- R3. The agent prefers `query_metric` over raw `execute_sql` for questions that map to defined metrics. Falls back to `execute_sql` when no semantic model covers the question. The LLM decides which path based on available metrics in context.
- R4. `query_metric` returns results with metadata: metric name used, dimensions applied, filters applied, and the compiled SQL (collapsible in streaming output).

### Glossary: Unified via Semantic Layer

- R5. `Measure`, `Dimension`, and `Metric` dataclasses gain an `aliases: list[str]` field. Aliases are alternative names for the same concept (e.g., "MRR" for `monthly_recurring_revenue`, "rev" for `total_revenue`).
- R6. The semantic prompt section renders aliases alongside measure/dimension names so the agent can match natural language terms to canonical definitions.
- R7. Aliases are defined in semantic model YAML files — no separate glossary store.

### Bootstrap: Opt-in CLI Command

- R8. New CLI command `seeknal semantic bootstrap` profiles project data (CSV/parquet tables), infers entities (join key candidates), dimensions (categorical/datetime columns), and measures (numeric columns), then writes draft `semantic_model.*.yml` files.
- R9. The bootstrap output is a starting point — users review and edit before using. Draft files are clearly marked as auto-generated.
- R10. The agent can also trigger bootstrap via a `bootstrap_semantic_model` tool when asked to "set up metrics" or similar, producing draft YAML the user reviews.

### Context: Pipeline Run Metadata

- R11. At agent startup, read `target/run_state.json` and inject recent run metadata into the system prompt: last run timestamp, per-node row counts, success/failure status.

### Save-as-Metric: Exploration to Governance

- R12. After answering an analytical question via `query_metric`, the agent can offer to save the query as a permanent metric definition by writing a `metric.{name}.yml` file.
- R13. The save operation validates against MetricRegistry for name conflicts and measure references before writing.

## Success Criteria

- An analyst can ask "show me revenue by region" and get results compiled through the semantic layer (not raw SQL) when a `total_revenue` measure is defined.
- An analyst can ask "what is MRR?" and the agent resolves "MRR" to the correct measure via aliases.
- A new project can run `seeknal semantic bootstrap` against CSV files and get draft semantic models in under 30 seconds.
- The agent's system prompt includes available metrics, dimensions, and aliases when semantic models exist.
- `run_state.json` metadata appears in the agent's context (last run, row counts).
- An analyst can save an ad-hoc metric query as a permanent metric YAML.

## Scope Boundaries

- **No separate glossary store** — business vocabulary lives on semantic model definitions (aliases + descriptions). No SQLite glossary module.
- **No conversational drill-down** — deferred. No query_stack or context carry-over between turns.
- **No pipeline impact analysis** — deferred.
- **No client-server / FlightSQL** — deferred. This work is all local, single-process.
- **No auto-harvesting of glossary terms from conversations** — manual via YAML and save-as-metric only.
- **No changes to the MetricCompiler itself** — we consume it as-is.

## Key Decisions

- **Unified glossary over separate store**: Aliases on Measure/Dimension/Metric instead of a SQLite-backed glossary module. Rationale: one source of truth, no drift, less code.
- **Prefer MetricCompiler, fallback to raw SQL**: The agent checks for matching metrics first. If a match exists, uses `query_metric`. If not, falls back to `execute_sql`. The LLM decides which path — no hard enforcement.
- **Bootstrap is opt-in CLI command**: `seeknal semantic bootstrap` runs explicitly, not automatically on first `seeknal ask`. Produces draft YAML for review.
- **Run metadata in prompt, not a tool**: Injected at startup, not fetched on-demand. Keeps it simple (~20 lines).

## Dependencies / Assumptions

- Semantic layer code (`workflow/semantic/models.py`, `compiler.py`) is stable and tested.
- `profile_data` tool already discovers column types, cardinality, and join key candidates.
- ArtifactDiscovery already scans `seeknal/` for YAML files — extending to extract semantic model metadata is incremental.
- `PromptBuilder` already supports `**kwargs` to templates and composing multiple `.j2` sections.

## Outstanding Questions

### Deferred to Planning
- [Affects R1][Technical] How should `query_metric` handle multi-metric queries (e.g., "revenue AND customer_count by region")? Does MetricCompiler support multiple metrics in one compilation?
- [Affects R1][Needs research] Should `query_metric` reuse the existing `seeknal query` CLI subprocess path, or directly instantiate MetricCompiler in-process?
- [Affects R8][Technical] What heuristics distinguish a dimension column from a measure column during bootstrap? Should it use DuckDB type inference or statistical profiling?
- [Affects R4][Technical] How should streaming.py render `query_metric` results — same as `execute_sql` table format, or with a metric metadata header?
- [Affects R10][Technical] Should the agent bootstrap tool write directly or go through the draft/apply pattern?

## Next Steps

-> `/ce:plan` for structured implementation planning
