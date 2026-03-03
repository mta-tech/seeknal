# Chapter 4: Lineage & Inspection

> **Duration:** 17 minutes | **Difficulty:** Beginner | **Format:** CLI

Learn to visualize data lineage and inspect intermediate pipeline outputs for debugging and documentation.

---

## What You'll Build

Interactive lineage visualizations of your pipeline DAG:

```
seeknal lineage                          →  Full DAG (HTML)
seeknal lineage transform.sales_enriched →  Focused node view
seeknal lineage transform.X --column Y   →  Column-level trace
seeknal lineage --ascii                  →  ASCII tree to stdout
seeknal inspect transform.sales_enriched →  Data preview
```

**After this chapter, you'll have:**
- An interactive HTML lineage diagram of your entire pipeline
- Focused node views showing upstream and downstream dependencies
- Column-level lineage tracing data flow through transforms
- ASCII tree output for terminal use and AI agent consumption
- Ability to inspect intermediate node outputs for debugging

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 2: Transformations](2-transformations.md) — Transforms created
- [ ] [Chapter 3: Data Rules](3-data-rules.md) — Rules added and pipeline executed with `seeknal run`

---

## Part 1: Full DAG Lineage (5 minutes)

### Generate Lineage Visualization

From your project directory, run:

```bash
seeknal lineage
```

**Expected output:**
```
✓ Lineage visualization generated: target/lineage.html
```

This opens an interactive HTML page in your browser showing the complete DAG:

```
source.products ────────────────────┐
                                    ├──→ transform.sales_enriched ──→ transform.sales_summary
source.sales_events ──→ transform.events_cleaned ──┘
                        │
source.sales_snapshot   ├──→ rule.not_null_quantity
                        └──→ rule.positive_quantity

source.products ──→ rule.valid_prices
```

### Understanding the Visualization

The HTML visualization uses [Cytoscape.js](https://js.cytoscape.org/) and provides:

- **Color-coded nodes** — Sources, transforms, and rules each have distinct colors
- **Edges** — Arrows show data flow direction (upstream → downstream)
- **Interactive** — Click nodes to see details, drag to rearrange, scroll to zoom
- **Column info** — Hover over nodes to see their output columns

!!! tip "Custom Output Path"
    By default, the lineage HTML is saved to `target/lineage.html`. Override with:
    ```bash
    seeknal lineage --output my-dag.html
    ```

!!! tip "No Browser"
    Use `--no-open` to generate the HTML without opening the browser:
    ```bash
    seeknal lineage --no-open
    ```

**Checkpoint:** You should see an interactive graph with 9 nodes (3 sources, 3 transforms, 3 rules) connected by edges.

---

## Part 2: Focused Node Lineage (5 minutes)

### Focus on a Specific Node

To see only the upstream and downstream of a specific node:

```bash
seeknal lineage transform.sales_enriched
```

This generates a focused view showing only nodes connected to `sales_enriched`:

```
source.products ──────────────────┐
                                  ├──→ transform.sales_enriched ──→ transform.sales_summary
transform.events_cleaned ─────────┘
```

Unrelated nodes (like `source.sales_snapshot` and the rules) are hidden.

### Column-Level Lineage

Trace how a specific column flows through the pipeline:

```bash
seeknal lineage transform.sales_enriched --column total_amount
```

This highlights the data flow for the `total_amount` column:

```
source.sales_events.quantity  ──┐
                                ├──→ transform.sales_enriched.total_amount
source.products.price  ─────────┘
        (quantity * price = total_amount)
```

Column-level lineage uses SQL parsing to detect:
- **Direct mappings** — Column passed through unchanged
- **Expressions** — Column derived from multiple inputs
- **Aggregations** — Column produced by aggregate functions

Try tracing other columns:

```bash
# Trace a direct mapping
seeknal lineage transform.events_cleaned --column event_id

# Trace an aggregation
seeknal lineage transform.sales_summary --column total_revenue
```

**Checkpoint:** The focused view should show only the relevant subset of your DAG, with column dependencies highlighted.

---

## Part 2b: ASCII Tree Output (2 minutes)

### Terminal-Friendly Lineage

For quick terminal use, piping to other tools, or AI agent consumption, use `--ascii` to print the DAG as a tree to stdout instead of generating HTML:

```bash
seeknal lineage --ascii
```

**Expected output:**
```
source.products
├── rule.valid_prices
├── transform.sales_enriched
│   └── transform.sales_summary
source.sales_events
└── transform.events_cleaned
    ├── rule.not_null_quantity
    ├── rule.positive_quantity
    └── transform.sales_enriched
        └── transform.sales_summary
source.sales_snapshot
```

The tree uses Unicode box-drawing characters (`├──`, `└──`, `│`) — the same style as the `tree` command. Source nodes (no upstream dependencies) appear as root-level entries.

### Focused ASCII Output

The `--ascii` flag works with node focus too:

```bash
seeknal lineage transform.sales_enriched --ascii
```

```
source.products
└── transform.sales_enriched
    └── transform.sales_summary
transform.events_cleaned
└── transform.sales_enriched
    └── transform.sales_summary
```

Only upstream and downstream nodes of the focused node are shown — the same BFS filter as the HTML view.

!!! tip "Pipeable Output"
    ASCII output has no ANSI colors by default, so it pipes cleanly:
    ```bash
    seeknal lineage --ascii | grep "transform\."
    seeknal lineage --ascii > lineage.txt
    ```

!!! info "When to Use ASCII vs HTML"
    | Mode | Best For |
    |------|----------|
    | HTML (default) | Interactive exploration, column lineage, presentations |
    | `--ascii` | Quick terminal checks, CI/CD logs, piping to grep/less, AI agents |

**Checkpoint:** `seeknal lineage --ascii` should print a tree showing all 9 pipeline nodes.

---

## Part 3: Inspecting Intermediate Outputs (5 minutes)

### List Available Outputs

After running your pipeline with `seeknal run`, intermediate results are stored as Parquet files. List them:

```bash
seeknal inspect --list
```

**Expected output:**
```
Available intermediate outputs:
  source.products
  source.sales_events
  source.sales_snapshot
  transform.events_cleaned
  transform.sales_enriched
  transform.sales_summary
```

### Preview Node Data

Inspect the output of any node:

```bash
seeknal inspect transform.sales_enriched
```

This shows the first 10 rows in a formatted table.

```bash
# Show more rows
seeknal inspect transform.sales_enriched --limit 20

# Show only the schema (column names and types)
seeknal inspect transform.sales_enriched --schema
```

### Debugging with Inspect

When a transform produces unexpected results, use `inspect` to trace the issue:

```bash
# 1. Check the source data
seeknal inspect source.sales_events

# 2. Check the cleaned intermediate
seeknal inspect transform.events_cleaned

# 3. Check the final output
seeknal inspect transform.sales_enriched
```

This lets you pinpoint exactly where data issues are introduced.

**Checkpoint:** `seeknal inspect --list` shows all 6 pipeline nodes. Inspecting any node shows its output data.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. "No intermediate output found"**

    - Symptom: `seeknal inspect` says no data for a node
    - Fix: Run `seeknal run` first. Intermediate parquets are only created during pipeline execution.

    **2. Lineage HTML is empty**

    - Symptom: HTML file opens but no nodes shown
    - Fix: Ensure your project has applied nodes (`seeknal plan` should show nodes). If you only drafted but never applied, there's nothing to visualize.

    **3. Column lineage shows "unknown"**

    - Symptom: Column transformation type is "unknown"
    - Fix: Column lineage uses SQL parsing (SQLGlot). Complex expressions or non-standard SQL may not be fully traced. This is informational only — the pipeline still works correctly.

    **4. Browser doesn't open**

    - Symptom: `seeknal lineage` runs but no browser window
    - Fix: Open `target/lineage.html` manually, or use `--output` to specify a known path.

---

## Summary

In this chapter, you learned:

- [x] **Full DAG Lineage** — `seeknal lineage` generates an interactive HTML visualization
- [x] **Focused Lineage** — `seeknal lineage <node>` shows only relevant nodes
- [x] **Column Lineage** — `seeknal lineage <node> --column <col>` traces data flow
- [x] **ASCII Tree Output** — `seeknal lineage --ascii` prints a pipeable tree to stdout
- [x] **Intermediate Inspection** — `seeknal inspect <node>` previews pipeline output data
- [x] **Schema Inspection** — `seeknal inspect <node> --schema` shows column types

**Key Commands:**
```bash
seeknal lineage                              # Full DAG visualization (HTML)
seeknal lineage <node>                       # Focused node view
seeknal lineage <node> --column <col>        # Column-level trace
seeknal lineage --ascii                      # ASCII tree to stdout
seeknal lineage <node> --ascii               # Focused ASCII tree
seeknal lineage --output <path> --no-open    # Custom output, no browser
seeknal lineage --tags revenue_pipeline      # Tag-filtered HTML lineage
seeknal lineage --tags ml --ascii            # Tag-filtered ASCII tree
seeknal inspect --list                       # List available outputs
seeknal inspect <node>                       # Preview node data
seeknal inspect <node> --schema              # Show schema only
seeknal inspect <node> --limit 20            # More rows
```

---

## What's Next?

In **[Chapter 5: Named ref() References](5-named-refs.md)**, you'll learn to use named `ref()` syntax in transform SQL for self-documenting, reorder-safe pipeline definitions.

!!! tip "Tag-Based Filtering"
    In **[Chapter 11: Pipeline Tags](11-pipeline-tags.md)**, you'll learn to filter lineage by tags: `seeknal lineage --tags revenue_pipeline` shows only the tagged subgraph with tag annotations.

---

## See Also

- **[CLI Reference](../../reference/cli.md)** — All `seeknal lineage` and `seeknal inspect` flags
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Source, transform, and rule schemas
