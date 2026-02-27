# Chapter 11: Pipeline Tags

> **Duration:** 15 minutes | **Difficulty:** Intermediate | **Format:** YAML, Python & CLI

Learn to organize pipeline nodes with tags and run, plan, or visualize filtered subsets of your pipeline.

---

## What You'll Build

A tagged pipeline where you can selectively run and visualize logical groups of nodes:

```
seeknal run --tags churn_pipeline        →  Run only churn nodes + upstream deps
seeknal plan --tags churn_pipeline       →  Show filtered execution plan
seeknal lineage --tags churn_pipeline    →  Visualize tagged subgraph
seeknal lineage --ascii                  →  ASCII tree with [tag] annotations
```

**After this chapter, you'll have:**

- YAML and Python nodes tagged with logical pipeline names
- The ability to run subsets of your pipeline by tag
- Filtered plan and lineage views
- Understanding of tag composition rules (`--tags` + `--exclude-tags`)

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 2: Transformations](2-transformations.md) — Transforms created and pipeline runnable
- [ ] [Chapter 8: Python Pipelines](8-python-pipelines.md) — Familiar with `@transform` decorator

---

## Part 1: Adding Tags to YAML Nodes (3 minutes)

Tags are simple string labels you add to any node. They're useful for grouping nodes into logical pipelines — like "churn_pipeline", "revenue_pipeline", or "data_quality".

### Tag a YAML Transform

Open `seeknal/transforms/sales_enriched.yml` and add a `tags` field:

```yaml
kind: transform
name: sales_enriched
description: Sales data enriched with product info
tags:
  - revenue_pipeline
transform: |
  SELECT
    e.event_id,
    e.product_id,
    p.product_name,
    p.price,
    e.quantity,
    e.quantity * p.price AS total_amount
  FROM ref('transform.events_cleaned') e
  JOIN ref('source.products') p ON e.product_id = p.product_id
inputs:
  - ref: transform.events_cleaned
  - ref: source.products
```

### Tag the Downstream Node Too

Open `seeknal/transforms/sales_summary.yml` and add the same tag:

```yaml
kind: transform
name: sales_summary
description: Aggregated sales by product
tags:
  - revenue_pipeline
transform: |
  SELECT
    product_name,
    SUM(total_amount) AS total_revenue,
    SUM(quantity) AS total_quantity
  FROM ref('transform.sales_enriched')
  GROUP BY product_name
inputs:
  - ref: transform.sales_enriched
```

!!! tip "Tags Are Free-Form"
    Tags are simple strings — no schema or registration required. Use whatever naming convention works for your team: `churn_pipeline`, `ml`, `etl`, `daily`, `experimental`.

---

## Part 2: Adding Tags to Python Nodes (3 minutes)

All Python decorators (`@source`, `@transform`, `@feature_group`, `@exposure`) accept a `tags` parameter.

### Create a Tagged Python Transform

Create `seeknal/pipelines/revenue_forecast.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
# ]
# ///

"""Transform: Simple revenue forecast based on recent trends."""

import pandas as pd
from seeknal.pipeline import transform


@transform(
    name="revenue_forecast",
    description="30-day revenue forecast from recent sales trends",
    tags=["revenue_pipeline", "forecasting"],
)
def revenue_forecast(ctx):
    """Compute a naive revenue forecast from sales summary."""
    summary = ctx.ref("transform.sales_summary")

    # Simple forecast: project current revenue forward
    summary["forecast_30d"] = summary["total_revenue"] * 1.1
    summary["forecast_type"] = "naive_trend"

    return summary[["product_name", "total_revenue", "forecast_30d", "forecast_type"]]
```

Notice this node has **two tags**: `revenue_pipeline` and `forecasting`. A node can have as many tags as you need.

### Apply and Run

```bash
seeknal apply seeknal/pipelines/revenue_forecast.py
seeknal run
```

**Checkpoint:** `seeknal plan` should show `revenue_forecast` in the execution plan.

---

## Part 3: Running by Tag (4 minutes)

### Select Nodes by Tag

Run only the `revenue_pipeline` nodes and their upstream dependencies:

```bash
seeknal run --tags revenue_pipeline
```

**Expected output:**
```
ℹ Execution Plan:
------------------------------------------------------------
  1. RUN  events_cleaned
  2. RUN  sales_enriched [revenue_pipeline]
  3. RUN  sales_summary [revenue_pipeline]
  4. RUN  revenue_forecast [revenue_pipeline, forecasting]

Showing 4 of 9 nodes (filtered by tags: revenue_pipeline), 4 to run
```

Notice that `events_cleaned` is included even though it has no tags — it's an **upstream dependency** of `sales_enriched`. Seeknal automatically includes all transitive upstream nodes to ensure the filtered subgraph can execute correctly.

Untagged nodes like `source.sales_snapshot` or rules are excluded from the filtered view.

### Zero Match

If you specify a tag that no node has:

```bash
seeknal run --tags nonexistent_tag
```

```
⚠ No nodes found with tags: nonexistent_tag. Nothing to run.
```

### OR Logic

Multiple tags use OR logic — nodes matching **any** specified tag are included:

```bash
seeknal run --tags revenue_pipeline,forecasting
```

This selects nodes tagged with `revenue_pipeline` OR `forecasting` (plus their upstream deps).

---

## Part 4: Composing Filters (3 minutes)

### Include + Exclude

Use `--tags` to select and `--exclude-tags` to remove specific nodes:

```bash
seeknal run --tags revenue_pipeline --exclude-tags forecasting
```

This runs the revenue pipeline but skips the forecast node. The order is always: **include first, then exclude**.

!!! warning "Upstream Exclusion Warning"
    If `--exclude-tags` removes a node that is a required upstream dependency of an included node, Seeknal emits a warning:
    ```
    ⚠ Warning: upstream node 'transform.X' excluded by --exclude-tags but required by 'transform.Y'. Execution may fail.
    ```

### Tags + Nodes (Union)

Combine `--tags` with `--nodes` for a union of both sets:

```bash
seeknal run --tags revenue_pipeline --nodes source.sales_snapshot
```

Both the revenue pipeline nodes AND `source.sales_snapshot` (plus their respective upstream deps) are included.

### Full Override

`--full` always overrides `--tags`:

```bash
seeknal run --full --tags revenue_pipeline
```

```
ℹ --full flag set, ignoring --tags filter. Running all nodes.
```

---

## Part 5: Filtered Plan and Lineage (2 minutes)

### Filtered Plan

See what would run without actually executing:

```bash
seeknal plan --tags revenue_pipeline
```

```
Execution Plan (production mode):
------------------------------------------------------------
  1. ● UNCHANGED  events_cleaned
  2. ● UNCHANGED  sales_enriched [revenue_pipeline]
  3. ● UNCHANGED  sales_summary [revenue_pipeline]
  4. ★ NEW        revenue_forecast [revenue_pipeline, forecasting]

Showing 4 of 9 nodes (filtered by tags: revenue_pipeline)
```

The plan shows only the filtered subgraph. Tags appear in brackets after each node name.

### Filtered Lineage

Visualize only the tagged subgraph:

```bash
seeknal lineage --tags revenue_pipeline --ascii
```

```
source.products
└── transform.sales_enriched [revenue_pipeline]
    └── transform.sales_summary [revenue_pipeline]
        └── transform.revenue_forecast [revenue_pipeline, forecasting]
transform.events_cleaned
└── transform.sales_enriched [revenue_pipeline]
    └── transform.sales_summary [revenue_pipeline]
        └── transform.revenue_forecast [revenue_pipeline, forecasting]
```

Tags appear as `[tag1, tag2]` suffixes in the ASCII tree.

### HTML Lineage with Tags

For the interactive HTML view:

```bash
seeknal lineage --tags revenue_pipeline
```

In the HTML visualization, click any node to see its tags displayed as badge chips in the detail panel. Tags appear for all node types — sources, transforms, feature groups, etc.

!!! note "Tags + Focus Node Conflict"
    You cannot combine `--tags` with a positional `node_id`:
    ```bash
    seeknal lineage source.products --tags revenue_pipeline  # Error!
    ```
    ```
    ✗ Cannot use --tags and node_id together. Use one or the other.
    ```
    Use `--tags` for tag-based filtering or a positional node ID for focus mode — not both.

---

## Filter Composition Summary

| Combination | Behavior |
|-------------|----------|
| `--tags A,B` | OR logic — matches nodes with **any** specified tag |
| `--tags X --exclude-tags Y` | Include first, then exclude |
| `--tags X --nodes Y` | Union of both sets (each with own upstream deps) |
| `--full --tags X` | `--full` wins, `--tags` ignored with info message |
| `--tags X --types Y` | Tags first, then types filter within tag-matched set |

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. "No nodes found with tags: X"**

    - Symptom: Warning and nothing runs
    - Fix: Check tag spelling. Tags are case-sensitive. Use `seeknal plan` (without `--tags`) to see all nodes and their tags.

    **2. Missing upstream nodes in output**

    - Symptom: Untagged upstream nodes appear in the filtered run
    - This is expected! Upstream dependencies are auto-included to ensure the subgraph can execute. Only **downstream** untagged nodes are excluded.

    **3. Tags not showing in plan/lineage**

    - Symptom: Tags don't appear in brackets
    - Fix: Make sure you've applied the updated YAML/Python files (`seeknal apply`).

---

## Summary

In this chapter, you learned:

- [x] **YAML Tags** — Add `tags:` list to any YAML node definition
- [x] **Python Tags** — Use `tags=["..."]` parameter in any decorator
- [x] **Run by Tag** — `seeknal run --tags X` runs tagged nodes + upstream deps
- [x] **Plan by Tag** — `seeknal plan --tags X` shows filtered execution plan
- [x] **Lineage by Tag** — `seeknal lineage --tags X` visualizes tagged subgraph
- [x] **Filter Composition** — Combine `--tags`, `--exclude-tags`, `--nodes`, `--full`

**Key Commands:**

```bash
# Run filtered by tags
seeknal run --tags churn_pipeline
seeknal run --tags ml,etl                   # OR logic
seeknal run --tags ml --exclude-tags experimental

# Plan filtered by tags
seeknal plan --tags revenue_pipeline

# Lineage filtered by tags
seeknal lineage --tags revenue_pipeline             # HTML with tag badges
seeknal lineage --tags revenue_pipeline --ascii      # ASCII tree with [tags]
seeknal lineage --exclude-tags experimental --ascii  # Exclude specific tags
```

---

## What's Next?

You've completed all chapters in the Advanced Guide! Explore other persona paths or dive into the reference docs:

| Path | Focus | Time |
|------|-------|------|
| **[Data Engineer →](../data-engineer-path/)** | ELT pipelines, incremental processing, production environments | ~75 min |
| **[Analytics Engineer →](../analytics-engineer-path/)** | Semantic models, business metrics, BI deployment | ~75 min |
| **[ML Engineer →](../ml-engineer-path/)** | Feature stores, aggregations, entity consolidation | ~115 min |

---

## See Also

- **[CLI Reference](../../reference/cli.md)** — All `seeknal run`, `seeknal plan`, and `seeknal lineage` flags
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Full `tags` field documentation
