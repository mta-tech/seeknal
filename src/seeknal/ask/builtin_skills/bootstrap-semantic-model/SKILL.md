---
name: bootstrap-semantic-model
description: "Auto-generate draft semantic model YAML from CSVs and parquets by profiling columns into entities/dimensions/measures"
tags: [semantic-layer, bootstrap, scaffolding]
version: "1.0.0"
---

# Bootstrap Semantic Model

Use this workflow when the user wants to stand up a semantic layer for a
project that doesn't have one yet, or add new tables to an existing semantic
layer. The tool profiles data files (CSVs in `data/`, parquets in
`target/intermediate/`), classifies columns into entities / dimensions /
measures, and writes draft YAML files under `.seeknal/drafts/`.

## Tool you will call

- `bootstrap_semantic_model` — generates draft semantic model files

## When to use

Trigger when the user says things like:

- "set up a semantic model"
- "create a semantic layer"
- "I want to query with metrics"
- "bootstrap metrics from my data"

## Phase 1 — Discover the data surface

Before bootstrapping, confirm what data is actually available:

1. Use `list_tables` to see what's already registered in DuckDB.
2. Check `data/*.csv` and `target/intermediate/*.parquet` via
   `search_project_files` to understand the file inventory.
3. If the user mentioned a specific table, pass its name as the `table_name`
   argument. Otherwise, leave empty to bootstrap ALL discovered tables.

Running a bootstrap on a project with no data returns a helpful error
explaining where to put CSV files or how to run a pipeline to generate parquets.

## Phase 2 — Call the tool

Required args:

- `table_name` (optional): if provided, only this table is profiled.
  If empty, all discovered tables are profiled.

The tool:
1. Profiles each table via DuckDB (column types, row counts, cardinality hints)
2. Classifies columns into `entities`, `dimensions`, `measures` based on
   naming patterns and cardinality
3. Writes `draft_semantic_model_<table>.yml` under `.seeknal/drafts/`
4. Returns a summary listing each generated draft + its classified columns

## Phase 3 — Review and apply

The bootstrap ALWAYS writes drafts, never directly-applied files. The user
must:

1. Review each draft — the classification is a best guess, not ground truth
2. Adjust entities (join keys), dimensions (group-by columns), and measures
   (aggregatable values) as needed
3. Apply via the `build-pipeline-node` skill:
   `apply_draft(file_path='.seeknal/drafts/draft_semantic_model_<name>.yml',
   confirmed=True)`

You may need to use `edit_node` on the draft to adjust before applying.

## Phase 4 — Next steps

After applying semantic model drafts, suggest to the user:

1. Query via `query_metric` (see the `query-metric` skill)
2. Save ad-hoc metrics via `save_metric` (see the `save-metric` skill)

## Output locations

- Bootstrap drafts: `.seeknal/drafts/draft_semantic_model_<table>.yml`
- Applied semantic models: `seeknal/semantic_models/<name>.yml`
