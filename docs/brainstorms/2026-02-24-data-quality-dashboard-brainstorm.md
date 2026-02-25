---
title: Data Quality Dashboard
type: feature
date: 2026-02-24
status: brainstorm
tags: [data-quality, visualization, dashboard, profiling, rules]
---

# Data Quality Dashboard

## What We're Building

A data quality dashboard (`seeknal dq`) that visualizes profile stats and rule check results from pipeline runs. It follows the same dual-output pattern as `seeknal lineage` — generating both an interactive HTML report (with trend charts via Chart.js) and a detailed ASCII terminal output.

The dashboard reads from:
- **run_state.json** — rule pass/fail/warn results and profile metadata
- **Profile parquets** — per-column stats (null%, avg, stddev, distinct_count, etc.)
- **dq_history.parquet** — append-only historical results for trend visualization

### CLI Interface

```bash
seeknal dq                     # Open HTML dashboard in browser
seeknal dq --ascii             # Print detailed ASCII report to terminal
seeknal dq products_stats      # Focus on a specific profile/rule node
seeknal dq --output report.html # Write HTML to custom path
seeknal dq --no-open           # Generate HTML without opening browser
```

## Why This Approach

### Follow lineage's architecture exactly

The lineage visualization has a proven three-layer pattern:

1. **Data layer** — `RunState` + profile parquets + `dq_history.parquet`
2. **Builder layer** — `DQDataBuilder` → frozen `DQData` dataclass (JSON-serializable)
3. **Renderer layer** — `HTMLRenderer` (Jinja2 + Chart.js) and `render_dq_ascii()` (typer.echo)

This was chosen over extending the lineage template because:
- Clean separation of concerns (lineage = structure, DQ = quality)
- Doesn't risk breaking the lineage feature
- Follows the established pattern, reducing cognitive overhead

### Alternative considered: Extend lineage with DQ overlay

Add pass/fail colors to lineage nodes and a side panel for DQ details. Rejected because it mixes two distinct concerns and makes the lineage template significantly more complex.

## Key Decisions

1. **Scope: Both profiles + rules** — The dashboard shows profile stats tables per node AND rule check pass/fail/warn status. Gives a complete data quality picture.

2. **History: Full trend with append-only parquet** — After each pipeline run, DQ results are appended to `target/dq_history.parquet` with schema:
   ```
   run_id      (VARCHAR)   — e.g., "20260224_170809"
   timestamp   (TIMESTAMP) — when the run completed
   node_id     (VARCHAR)   — e.g., "rule.check_orders" or "profile.products_stats"
   node_type   (VARCHAR)   — "rule" or "profile"
   column_name (VARCHAR)   — column name, or "_table_" for table-level
   metric      (VARCHAR)   — metric name (e.g., "avg", "null_percent", "passed")
   value       (DOUBLE)    — numeric value
   status      (VARCHAR)   — "pass", "warn", "fail", or NULL for profile metrics
   detail      (VARCHAR)   — JSON for extra context (threshold expression, message)
   ```
   This is queryable with DuckDB, grows naturally, and requires no schema migrations.

3. **CLI command: `seeknal dq`** — Short and memorable, matches the `seeknal lineage` pattern.

4. **ASCII: Detailed per-node breakdown** — Shows full stats table per profile node and per-check rule results:
   ```
   Data Quality Report (run: 20260224_170809)
   ═══════════════════════════════════════════════════════════

   █ Profile: products_stats (6 rows, 6 cols)
     Column    │ null%  │ distinct │ min    │ avg    │ max
     ──────────┼────────┼──────────┼────────┼────────┼─────
     price     │  0.0%  │    6     │  9.99  │ 85.58  │ 249.99
     quantity  │  0.0%  │    4     │    1   │  2.5   │    5
     name      │  0.0%  │    6     │   --   │  --    │  --

   █ Rules
     ✔ valid_prices         PASS  Range: 0/6 violations
     ⚠ products_quality [1] WARN  price.avg=85.6 (> 50)

   ═══════════════════════════════════════════════════════════
   4 rules: 3 passed, 1 warning, 0 failed
   ```

5. **Chart library: Chart.js via CDN** — Lightweight (~70KB), CDN-hosted with SRI hash (like Cytoscape.js in lineage), ideal for trend sparklines and bar charts. No build step.

6. **Template location: `src/seeknal/dag/templates/dq_report.html.j2`** — Lives alongside `lineage.html.j2`. Self-contained HTML with embedded CSS/JS.

## Architecture Overview

```
seeknal dq [--ascii] [--output] [node_id]
       │
       ▼
CLI: load RunState + build Manifest
       │
       ▼
DQDataBuilder.build(state, manifest, history_path)
       │   reads: run_state.json → rule results + profile metadata
       │   reads: target/intermediate/profile_*.parquet → column stats
       │   reads: target/dq_history.parquet → historical trends
       │
       ▼
DQData (frozen dataclass)
  ├── profiles: list[ProfileSummary]     # per-profile node stats
  │     └── columns: list[ColumnStats]   # per-column metrics
  ├── rules: list[RuleResult]            # pass/fail/warn per check
  ├── trends: dict[str, list[TrendPoint]] # metric → value over time
  └── metadata: DQMetadata               # run_id, timestamp, node counts
       │
       ├──[--ascii]──▶ render_dq_ascii(dq_data) → stdout
       │
       └──[HTML]──▶ HTMLRenderer.render(dq_data, output_path)
                         │  json.dumps(dataclasses.asdict(dq_data))
                         │  Jinja2: dq_report.html.j2
                         │  Chart.js: trend sparklines, bar charts
                         ▼
                    target/dq_report.html → webbrowser.open()
```

## History Collection (Post-Run Hook)

After each pipeline run, `runner.py`'s run completion step appends DQ results to `dq_history.parquet`:
- Extract rule metadata (passed, violations, status) from `ExecutionSummary.results`
- Extract key profile metrics from profile parquets
- Append rows with current `run_id` and timestamp
- Use DuckDB `COPY ... TO ... (APPEND)` or pandas concat + write

## HTML Dashboard Sections

1. **Header** — Project name, run timestamp, overall health score (% rules passing)
2. **Summary cards** — Total rules, passed/warned/failed counts with color coding
3. **Profile cards** — One card per profile node with stats table (null%, avg, min/max, distribution)
4. **Rule results table** — Sortable table of all rule checks with status badges
5. **Trend charts** — Chart.js line charts showing key metrics over recent runs (row_count, null%, rule pass rate)
6. **Detail panel** — Click a rule/profile for full details (similar to lineage's node detail panel)

## Files to Create/Modify

| File | Action | Purpose |
|------|--------|---------|
| `src/seeknal/dag/dq.py` | Create | `DQData`, `DQDataBuilder`, `render_dq_ascii()`, `generate_dq_html()` |
| `src/seeknal/dag/templates/dq_report.html.j2` | Create | Self-contained HTML dashboard template |
| `src/seeknal/cli/main.py` | Modify | Add `seeknal dq` command |
| `src/seeknal/workflow/runner.py` | Modify | Append DQ results to history parquet after run |
| `tests/dag/test_dq.py` | Create | Tests for DQDataBuilder, ASCII renderer |

## Open Questions

*None — all resolved during brainstorming.*
