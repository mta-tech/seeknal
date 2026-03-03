---
title: "Pipeline Tags: Select-by-Tag Filtering for plan, run, and lineage"
type: feat
status: active
date: 2026-02-27
---

# Pipeline Tags: Select-by-Tag Filtering

## What We're Building

Add `--tags` (include filter) to `seeknal plan`, `seeknal run`, and `seeknal lineage` so users can run and visualize subsets of their pipeline by tag. Tags are already a first-class field in both YAML and Python decorators — the missing piece is **select-by-tag** filtering with automatic upstream dependency resolution.

### User Story

```bash
# Tag nodes in Python
@transform(name="churn_model", tags=["churn_pipeline"], ...)

# Tag nodes in YAML
kind: transform
name: sales_enriched
tags: [etl, sales]

# Run only the churn pipeline (auto-includes upstream deps)
seeknal run --tags churn_pipeline

# Plan showing only the churn subgraph
seeknal plan --tags churn_pipeline

# Lineage filtered to tagged subgraph
seeknal lineage --tags churn_pipeline

# Combine with exclude (include A but skip B)
seeknal run --tags churn_pipeline --exclude-tags experimental
```

## Why This Approach

### What Already Exists (80% done)

| Feature | Status |
|---------|--------|
| `tags:` in YAML nodes | Supported |
| `tags=` in all Python decorators | Supported |
| `DAGNode.tags` / `Node.tags` | Supported |
| Tags in `manifest.json` | Supported |
| Tags in `plan` / `run --show-plan` output | Displayed (brackets) |
| `--exclude-tags` on `run` | Fully implemented |
| Tag changes = METADATA (no re-run) | Supported |

### What's Missing

1. **`--tags` include filter** on `run` — run only nodes WITH specified tags + upstream deps
2. **`--tags` on `plan`** — show only the filtered subgraph
3. **`--tags` on `lineage`** — visualize only the filtered subgraph
4. **Tags in lineage HTML** — node tooltips/badges showing tags

## Key Decisions

### 1. Upstream Auto-Include

When `--tags churn_pipeline` is specified:
- Find all nodes matching any tag in the set
- Walk upstream from those nodes to collect all required dependencies
- Run the full subgraph (deps + tagged nodes)

This mirrors dbt's `--select tag:X+` behavior and ensures the pipeline never fails due to missing upstream data.

### 2. OR-Only Logic

`--tags churn_pipeline,etl` matches nodes that have **any** of the specified tags. No AND operators — keeps the CLI simple and covers 90% of use cases.

### 3. Plan Filters Too

`seeknal plan --tags X` shows only the subgraph that would run (tagged nodes + upstream deps), matching `run --tags` behavior exactly.

### 4. Lineage Integration

- `seeknal lineage --tags X` — render only the tagged subgraph + upstream
- Tags shown in lineage HTML tooltip/sidebar for each node
- ASCII lineage also respects `--tags` filter

### 5. Interaction with Existing `--exclude-tags`

Both flags compose naturally:
```bash
seeknal run --tags churn_pipeline --exclude-tags experimental
```
1. First: select nodes matching `--tags` + upstream deps
2. Then: remove nodes matching `--exclude-tags` from the selection

### 6. Interaction with `--nodes`

```bash
seeknal run --nodes transform.churn_model --tags etl
```
Union: both `--nodes` targets and `--tags` matches are included.

## Scope

### In Scope

- [x] `--tags` flag on `seeknal run` with upstream auto-include
- [x] `--tags` flag on `seeknal plan` with subgraph filtering
- [x] `--tags` flag on `seeknal lineage` with subgraph filtering
- [x] Tags displayed in lineage HTML node tooltip
- [x] Tags displayed in ASCII lineage output

### Out of Scope

- Tag inheritance (child nodes auto-inherit parent tags)
- Tag validation (no predefined tag vocabulary)
- AND/NOT boolean operators
- Tag-based scheduling or CI triggers
- `seeknal draft` auto-tagging

## Examples

### Python

```python
@source(name="transactions", source="csv", table="data/txns.csv", tags=["etl", "churn_pipeline"])
def transactions(ctx=None): pass

@feature_group(name="customer_daily_agg", tags=["churn_pipeline"])
def customer_daily_agg(ctx): ...

@transform(name="churn_model", tags=["churn_pipeline", "ml"])
def churn_model(ctx): ...
```

### YAML

```yaml
kind: source
name: products
tags: [etl, catalog]
source: csv
table: data/products.csv
```

### CLI

```bash
# Run just the churn pipeline
seeknal run --tags churn_pipeline
# Output:
#   1. transactions [RUNNING]         [etl, churn_pipeline]
#   2. customer_daily_agg [RUNNING]   [churn_pipeline]
#   3. churn_model [RUNNING]          [churn_pipeline, ml]

# Plan the ETL subset
seeknal plan --tags etl

# Lineage for ML nodes
seeknal lineage --tags ml

# Exclude experimental from churn pipeline
seeknal run --tags churn_pipeline --exclude-tags experimental
```

## Open Questions

None — all key design decisions resolved during brainstorming.
