---
title: "ASCII Lineage Output for CLI"
type: feat
date: 2026-02-24
status: brainstorm
---

# ASCII Lineage Output for CLI

## What We're Building

Add an `--ascii` flag to `seeknal lineage` that prints the pipeline DAG as a tree to stdout instead of generating HTML. The output uses Unicode box-drawing characters (like the `tree` command) and is readable by both humans in terminals and AI agents exploring the pipeline.

**Command:**
```bash
seeknal lineage --ascii                          # Full DAG tree
seeknal lineage transform.enriched --ascii       # Focus on node (upstream + downstream)
```

**Example output:**
```
source.orders
├── transform.enriched_orders
│   ├── feature_group.sales_features
│   └── rule.positive_quantity
├── rule.not_null_quantity
source.products
├── transform.enriched_orders
└── rule.valid_prices
```

## Why This Approach

- **`--ascii` flag** — simple boolean flag on the existing `lineage` command, no new commands needed
- **Tree format** — familiar pattern (`tree`, `git log --graph`), easy to scan in a terminal, easy for AI agents to parse
- **Same focus behavior** — `seeknal lineage node_id --ascii` shows upstream + downstream of that node, matching the HTML version's focus mode
- **Stdout output** — pipeable to grep, less, files. No browser or file I/O needed

## Key Decisions

1. **Flag: `--ascii`** — Not `--format` enum. Simpler, one flag. HTML remains the default when `--ascii` is not set.
2. **Tree view** — Not a DAG box-drawing layout. Trees are simpler and sufficient for showing dependency flow. Nodes with multiple parents appear under each parent (repeated).
3. **Node type included** — Each line shows the full node ID (e.g., `source.orders`, `transform.enriched`) so the type is implicit from the prefix.
4. **Root nodes** — Nodes with no upstream dependencies (sources) are tree roots. Each root starts a new tree section at the top level.
5. **Focus mode** — When a `node_id` is provided, show only the subgraph relevant to that node (upstream + downstream), same as the HTML focus behavior.
6. **No column detail** — Keep it compact. Column info belongs in the HTML view or `seeknal repl`.
7. **Audience: both humans and AI agents** — Clean, parseable text output. No ANSI colors by default (pipeable), but could add `--color` later.

## Scope

### In Scope
- `--ascii` flag on `seeknal lineage` command
- Full DAG tree output to stdout
- Node focus (upstream + downstream) when node_id is provided
- Unicode box-drawing characters (├──, └──, │)

### Out of Scope
- Column-level lineage in ASCII (use HTML for that)
- ANSI color output (future enhancement)
- JSON/YAML output formats (separate feature)
- New CLI commands

## Open Questions

_None — all questions resolved during brainstorming._
