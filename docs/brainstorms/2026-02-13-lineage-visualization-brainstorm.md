# Lineage Visualization for Seeknal

**Date**: 2026-02-13
**Status**: Brainstorm
**Inspired By**: SQLMesh lineage visualization (web UI + CLI)
**Related**: `docs/brainstorms/2026-02-08-sqlmesh-inspired-features-brainstorm.md` (Feature 3: Column-Level Lineage)
**SQLMesh Reference**: `~/project/self/bmad-new/sqlmesh`

---

## What We're Building

Interactive lineage visualization for Seeknal pipelines via a `seeknal lineage` CLI command that generates a self-contained HTML file with embedded React Flow. Supports two levels: node-level DAG overview and column-level drill-down on node click.

## Why This Feature

Seeknal already has the **hard part done** — column-level lineage extraction (`src/seeknal/dag/lineage.py`, 84 tests), DAG manifest (`src/seeknal/dag/manifest.py`), change detection (`src/seeknal/dag/diff.py`), and SQL semantic diffing (`src/seeknal/dag/sql_diff.py`). But there's **zero visualization** — users can't see their pipeline graph or trace column flows. This is table stakes for any modern data pipeline tool.

### Existing Infrastructure (Already Built)

| Component | Location | Status |
|-----------|----------|--------|
| Column lineage extraction | `src/seeknal/dag/lineage.py` | Complete (84 tests) |
| DAG manifest (nodes + edges) | `src/seeknal/dag/manifest.py` | Complete |
| DAG builder from YAML/Python | `src/seeknal/workflow/dag.py` | Complete |
| Change detection | `src/seeknal/dag/diff.py` | Complete |
| SQL semantic diff | `src/seeknal/dag/sql_diff.py` | Complete |
| SQL parser | `src/seeknal/dag/sql_parser.py` | Complete |
| `seeknal parse` command | `src/seeknal/cli/main.py` | Complete |

### What's Missing (This Feature)

- HTML rendering of the DAG graph
- Column-level lineage visualization
- `seeknal lineage` CLI command
- Interactive exploration (zoom, pan, click, highlight)

## Why This Approach

### SQLMesh Reference Architecture

SQLMesh uses two visualization approaches:

| Context | Technology | Purpose |
|---------|------------|---------|
| CLI (`sqlmesh dag`) | vis.js via SQLGlot's `GraphHTML` | Quick HTML export |
| Web UI (full app) | React Flow + Dagre/ELK.js | Interactive column drill-down |

We're following the **Web UI approach** (React Flow + Dagre) but packaging it as a **self-contained HTML file** rather than a full web app. This gives us the rich interactivity of SQLMesh's web UI without the infrastructure overhead of a server.

### Why React Flow over vis.js

- vis.js is simpler but limited for column-level drill-down
- React Flow is what SQLMesh uses for its interactive lineage (proven at scale)
- React Flow supports custom node types (needed for column ports)
- Dagre layout algorithm provides clean left-to-right graph rendering

### Why Self-Contained HTML over Web Server

- No server process to manage
- Shareable (email/Slack the HTML file)
- Works offline
- Simpler to implement than a full web app
- Can be embedded in CI/CD artifacts

---

## Feature Design

### CLI Interface

```bash
# Full DAG visualization
seeknal lineage

# Focused on specific node (highlights upstream/downstream)
seeknal lineage transform.clean_orders

# Column-level lineage for a specific column
seeknal lineage transform.clean_orders --column total_amount

# Custom output path
seeknal lineage --output my-lineage.html

# Don't auto-open browser
seeknal lineage --no-open
```

### Behavior

1. `seeknal lineage` runs `seeknal parse` internally (builds manifest)
2. Computes column lineage for all transform/aggregation nodes
3. Serializes manifest + lineage data as JSON
4. Embeds JSON into an HTML template with React Flow + Dagre
5. Writes to `target/lineage.html` (or custom path)
6. Auto-opens in default browser

### Visualization Features

#### Node-Level DAG (Default View)

- Nodes colored by type (source=blue, transform=green, feature_group=purple, etc.)
- Edges show dependencies
- Left-to-right Dagre layout
- Zoom/pan controls
- Click node to see details (columns, SQL, config)
- Highlight upstream/downstream on node selection

#### Column-Level Drill-Down (On Node Click)

- Expand node to show columns as ports
- Column-to-column edges show data flow
- SQL expression shown per column transformation
- Trace a column from source to output
- Impact analysis: "what breaks if I remove this column?"

### Data Format (Embedded in HTML)

```json
{
  "nodes": [
    {
      "id": "source.raw_users",
      "type": "source",
      "label": "raw_users",
      "columns": ["user_id", "name", "email", "created_at"],
      "sql": null,
      "description": "Raw user table"
    },
    {
      "id": "transform.clean_users",
      "type": "transform",
      "label": "clean_users",
      "columns": ["user_id", "name", "email", "signup_month"],
      "sql": "SELECT user_id, name, email, DATE_TRUNC('month', created_at) AS signup_month FROM ...",
      "description": "Cleaned user data"
    }
  ],
  "edges": [
    {"source": "source.raw_users", "target": "transform.clean_users"}
  ],
  "columnLineage": {
    "transform.clean_users": {
      "user_id": {"upstream": [{"node": "source.raw_users", "column": "user_id", "type": "direct"}]},
      "signup_month": {"upstream": [{"node": "source.raw_users", "column": "created_at", "type": "expression"}]}
    }
  }
}
```

### Node Type Color Scheme

| Node Type | Color | Hex |
|-----------|-------|-----|
| source | Blue | `#3B82F6` |
| transform | Green | `#10B981` |
| feature_group | Purple | `#8B5CF6` |
| aggregation | Orange | `#F59E0B` |
| second_order_aggregation | Amber | `#D97706` |
| exposure | Red | `#EF4444` |
| rule | Gray | `#6B7280` |
| python | Teal | `#14B8A6` |
| semantic_model | Indigo | `#6366F1` |
| metric | Pink | `#EC4899` |

---

## Key Decisions

1. **Self-contained HTML** — No server needed. Generate and open. Shareable.
2. **React Flow + Dagre** — Same proven stack as SQLMesh's web UI, supports column drill-down.
3. **`seeknal lineage` command** — New top-level CLI command, not a subcommand of `parse` or `dag`.
4. **Auto-open browser** — `seeknal lineage` generates HTML and opens it. `--no-open` flag to suppress.
5. **Embed data as JSON** — Manifest + column lineage serialized into the HTML template. No API needed.
6. **Two-level visualization** — Node-level DAG overview + column-level drill-down on click.
7. **Left-to-right layout** — Dagre with `rankdir: 'LR'`, same as SQLMesh. Data flows left to right.
8. **CDN for JS libraries** — React, ReactDOM, React Flow, Dagre loaded from CDN. Keeps template small. Fallback: inline for offline use.

## Open Questions

1. **Offline support**: Should we inline React Flow + Dagre into the HTML (larger file, ~2MB) or use CDN links (smaller but requires internet)?
2. **Change detection overlay**: Should the lineage view show change status (BREAKING/NON_BREAKING/METADATA) when comparing against previous manifest?
3. **Search/filter**: Should the HTML include a search bar to find nodes/columns? (Nice to have, not MVP)
4. **Export formats**: Should `seeknal lineage` also support `--format mermaid` or `--format dot` for non-HTML use cases?
5. **Large graph performance**: For pipelines with 200+ nodes, should we implement node collapsing or pagination?

## Complexity Assessment

| Component | Effort | Risk |
|-----------|--------|------|
| CLI command (`seeknal lineage`) | 1-2 days | Low |
| Manifest → JSON serialization | 1 day | Low |
| Column lineage → JSON serialization | 1-2 days | Medium (integration with existing lineage.py) |
| HTML template with React Flow | 3-5 days | Medium (embedded React app in single HTML) |
| Dagre layout integration | 1-2 days | Low |
| Column drill-down UI | 2-3 days | Medium |
| **Total** | **~2 weeks** | **Medium** |

## Success Criteria

1. `seeknal lineage` generates an interactive HTML file and opens it in the browser
2. All pipeline node types are rendered with correct colors and labels
3. Edges show dependency flow left-to-right
4. Clicking a node shows its columns, SQL, and description
5. Column-level lineage edges appear when drilling into a node
6. Zoom, pan, and node selection work smoothly
7. Works for pipelines with up to 100 nodes without performance issues

---

## SQLMesh Reference Files

For implementation reference:

| What | SQLMesh Location |
|------|-----------------|
| Core lineage computation | `sqlmesh/core/lineage.py` |
| API endpoint format | `web/server/api/endpoints/lineage.py` |
| React Flow components | `web/common/src/components/Lineage/` |
| Dagre layout config | `web/common/src/components/Lineage/layout/dagreLayout.ts` |
| Custom nodes | `web/common/src/components/Lineage/node/` |
| Custom edges | `web/common/src/components/Lineage/edge/EdgeWithGradient.tsx` |
| CLI HTML export | `sqlmesh/core/context.py:2159` (render_dag method) |
| GraphHTML (vis.js) | Via SQLGlot dependency |

---

## Seeknal Files to Modify/Create

| Action | File |
|--------|------|
| Create | `src/seeknal/dag/visualize.py` — Manifest/lineage → JSON + HTML generation |
| Create | `src/seeknal/templates/lineage.html` — HTML template with embedded React Flow |
| Modify | `src/seeknal/cli/main.py` — Add `seeknal lineage` command |
| Read | `src/seeknal/dag/lineage.py` — Existing column lineage (integrate) |
| Read | `src/seeknal/dag/manifest.py` — Existing manifest structure (serialize) |
| Create | `tests/dag/test_visualize.py` — Tests for visualization |
| Create | `tests/cli/test_lineage_command.py` — CLI tests |
