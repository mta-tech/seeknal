---
title: "Interactive HTML Reports via Evidence.dev Integration"
date: 2026-03-08
status: brainstorm
participants: [user, claude]
---

# Interactive HTML Reports via Evidence.dev Integration

## What We're Building

Enable `seeknal ask` to produce interactive HTML reports using [Evidence.dev](https://evidence.dev) as the rendering engine. Two entry points:

1. **Agent tool (`generate_report`)** — callable during any chat/one-shot session. The agent scaffolds an evidence.dev project, writes markdown pages with SQL queries + evidence chart/table components, runs `evidence build`, and returns the HTML output path.

2. **CLI command (`seeknal ask report "topic"`)** — an interactive workflow where the agent asks 2-3 clarifying questions about report scope, then autonomously runs analyses, generates evidence pages, and builds the final HTML report.

**Artifact**: Static HTML site in `target/reports/{name}/build/` that can be opened in any browser. Self-contained interactive dashboards with charts, tables, and filters.

## Why This Approach

- **Evidence.dev is a natural fit**: It natively uses DuckDB + SQL, same as seeknal. No data transformation needed — evidence reads seeknal's parquet files directly.
- **Agent writes evidence markdown directly**: The LLM generates real evidence component syntax (`<BarChart>`, `<DataTable>`, etc.) — no intermediate DSL layer. This gives full access to evidence's 30+ visualization components.
- **`evidence build` produces portable HTML**: Output is a static site that can be shared, hosted, or viewed offline. No server needed to consume reports.
- **Approach A (Agent Tool + CLI)**: Works in both interactive chat and autonomous report modes. The agent tool can be invoked mid-conversation ("now generate a report from this analysis") or via the dedicated CLI command.

## Key Decisions

### 1. Evidence.dev as rendering engine (not standalone HTML)
Evidence provides professional-grade interactive charts (ECharts), responsive layout, built-in data tables with sorting/filtering, and a markdown-first authoring model. Generating raw HTML with inline JS would be fragile and produce lower-quality output.

### 2. Agent writes evidence markdown directly
Include a condensed evidence component reference in the system prompt. The LLM has enough context to generate correct `<BarChart data={query_name} x=col y=col>` syntax. This avoids a translation layer and gives full component flexibility.

### 3. DuckDB source pointing at seeknal parquets
The evidence project's `sources/` config points DuckDB at seeknal's `target/intermediate/` and `target/cache/` directories. Reports reference the same table names the agent already knows. No data copying.

### 4. Output at `target/reports/{name}/`
Follows seeknal's convention of putting generated artifacts under `target/`. Evidence project structure lives here, built HTML at `target/reports/{name}/build/`.

### 5. Interactive-then-build workflow for CLI
`seeknal ask report "customer analysis"` → agent asks 2-3 scoping questions → runs autonomous multi-step analysis → generates evidence pages → runs `evidence build` → outputs path to HTML.

### 6. Node.js + evidence CLI as runtime dependency
`evidence build` requires Node.js. The `generate_report` tool checks for `npx evidence` availability and returns a clear error if missing. This is an opt-in capability — seeknal ask works fine without it.

## Architecture Sketch

```
User: "seeknal ask report 'customer segmentation'"
  │
  ├─ Agent asks 2-3 scoping questions
  │   "What metrics? Which time period? Any filters?"
  │
  ├─ Agent runs analyses using existing tools
  │   ├─ execute_sql: revenue by segment, cohort analysis
  │   ├─ execute_python: statistical tests, distributions
  │   └─ describe_table, list_tables: schema discovery
  │
  ├─ Agent calls generate_report tool
  │   ├─ Scaffolds evidence project at target/reports/{name}/
  │   │   ├─ evidence.plugins.yaml  (DuckDB source config)
  │   │   ├─ pages/
  │   │   │   ├─ index.md           (overview dashboard)
  │   │   │   ├─ segmentation.md    (detailed analysis)
  │   │   │   └─ methodology.md     (how the analysis was done)
  │   │   └─ sources/seeknal/       (symlink or config to parquets)
  │   │
  │   ├─ Runs: npx evidence build --output target/reports/{name}/build
  │   └─ Returns: "Report built at target/reports/{name}/build/index.html"
  │
  └─ Agent presents final answer with report link
```

## Evidence Component Reference (for system prompt)

The agent needs to know these core components:

| Component | Usage | Key Props |
|-----------|-------|-----------|
| `<BarChart>` | Bar charts | data, x, y, series, type(grouped/stacked) |
| `<LineChart>` | Time series | data, x, y, series |
| `<AreaChart>` | Area charts | data, x, y, series, type(stacked) |
| `<ScatterPlot>` | Correlations | data, x, y, series |
| `<DataTable>` | Interactive tables | data, search, sort, rows |
| `<BigValue>` | KPI cards | data, value, title, comparison |
| `<Histogram>` | Distributions | data, x, bins |
| `<Heatmap>` | Matrices | data, x, y, value |
| `<FunnelChart>` | Funnels | data, name, value |
| `<SankeyChart>` | Flow diagrams | data, source, target, value |

SQL queries in evidence markdown:
```markdown
​```sql categories
SELECT category, SUM(revenue) as total
FROM transform_revenue
GROUP BY category
ORDER BY total DESC
​```

<BarChart data={categories} x=category y=total />
```

## New Components

### 1. Agent Tool: `generate_report`

```python
@tool
def generate_report(title: str, pages: list[dict]) -> str:
    """Generate an interactive HTML report using Evidence.dev.

    Args:
        title: Report title.
        pages: List of page definitions, each with:
            - name: Page filename (e.g., "overview")
            - content: Evidence-compatible markdown with SQL + components
    """
```

### 2. CLI Command: `seeknal ask report`

```
seeknal ask report "topic" [--project PATH] [--provider google] [--model gemini-2.5-flash]
```

### 3. Evidence Project Scaffolder

Python module that:
- Creates evidence project directory structure
- Configures DuckDB source to read seeknal parquets
- Writes page markdown files
- Runs `npx evidence build`
- Returns build output path

## Resolved Questions

1. **Evidence installation** → Auto-install on first use. When `generate_report` is called and evidence isn't found, automatically scaffold via `npm init evidence-app`. Zero manual steps — only Node.js/npx is required.

2. **Multi-page vs single-page** → Single page by default. One page containing all analyses. Agent can add more pages if the analysis is complex enough, but the default is a single cohesive report page.

3. **Report persistence** → Ephemeral in `target/`. Reports live under `target/reports/{name}/` and are gitignored like other build artifacts. Regenerate anytime.

4. **Live preview** → Both build and serve. `seeknal ask report` builds static HTML. Additionally, `seeknal ask report serve` runs `evidence dev` for interactive live-editing/viewing of the generated report.

## What This Is NOT

- Not a replacement for the terminal-based ask experience — reports are an additional output format
- Not a general-purpose BI tool — reports are generated from seeknal project data specifically
- Not a hosted solution — output is static HTML files
