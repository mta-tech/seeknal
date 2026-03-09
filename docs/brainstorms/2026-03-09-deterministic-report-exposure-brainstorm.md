---
title: Deterministic Report Exposure with Pinned Queries
topic: deterministic-report-exposure
date: 2026-03-09
status: complete
---

# Deterministic Report Exposure with Pinned Queries

## What We're Building

A new deterministic report rendering path for exposure YAML files. Instead of the LLM deciding what SQL to run and how to structure the Evidence page, the exposure YAML defines **sections** with **pinned SQL queries** and **chart configurations**. The system programmatically generates the Evidence markdown skeleton, executes the pinned queries against DuckDB to get actual results, then calls the LLM **only** to write analytical narrative between charts.

This produces reports that are structurally identical across runs — same sections, same queries, same charts — with only the narrative commentary varying (and even that is constrained by the actual query results passed as context).

## The Problem

Currently, the entire report is LLM-generated freeform output:

1. **Query variability** — The LLM decides which tables to explore, what SQL to write, and what aggregations to use. Even at temperature=0, query selection varies across runs.
2. **Structure variability** — The LLM generates the full Evidence markdown page. Section count, chart types, and layout change between runs.
3. **Quality inconsistency** — Some runs produce professional 6-section reports; others produce shallow 3-section reports with repetitive BarCharts.
4. **Build fragility** — LLM-generated Evidence markdown may have syntax errors (double braces, semicolons, invalid component props) that cause build failures.

## Why This Approach

**Pinned SQL in YAML** was chosen over alternatives because:

- **vs. LLM-generates-then-pins**: Adds complexity (auto-capture mechanism) and the first run is still non-deterministic. The user knows what queries they want — let them specify directly.
- **vs. External .sql files**: Over-engineered for this use case. SQL queries are short and belong with their chart config. Separate files add indirection without benefit.
- **vs. Full template (no LLM)**: Loses the analytical commentary that makes reports valuable. Pure data-and-charts reports are dashboards, not analyst reports.
- **vs. Quality-floor validation**: Still fundamentally non-deterministic. Retry loops add latency and don't guarantee convergence.

**Narrative-only LLM** was chosen because:
- The LLM excels at interpreting data and writing insights — that's where it adds value
- Removing SQL generation and page structure from the LLM's responsibility eliminates the two largest sources of non-determinism
- The narrative is constrained by actual query results passed as context, bounding what the LLM can say

## Key Decisions

1. **Inline SQL in YAML** — Queries are defined directly in the exposure file under each section's `queries:` key. No external .sql files.

2. **Section-based structure** — Each section has a `title`, optional `queries` (with chart config), and a `narrative` flag. Sections render in order.

3. **LLM writes narrative only** — The system generates the Evidence markdown skeleton (SQL blocks + chart components) programmatically. The LLM receives pre-executed query results and writes analytical commentary for sections with `narrative: true`.

4. **Both modes coexist** — Exposures WITHOUT `sections:` key fall back to the existing agent-driven flow (Mode 2a). Exposures WITH `sections:` key use the new deterministic renderer (Mode 2b). Interactive reports (Mode 1) are unchanged.

5. **Chart config in YAML** — Each query specifies its `chart` type and relevant props (`x`, `y`, `value`, `series`, `title`, etc.). The system maps these to Evidence component syntax.

## Proposed YAML Schema

```yaml
kind: exposure
name: customer_segmentation_report
type: report
description: Customer segmentation analysis with penetration strategies
params:
  format: both  # markdown | html | both
inputs:
  - ref: source.raw_customers
  - ref: source.raw_orders
  - ref: transform.customer_purchase_stats

sections:
  - title: Executive Summary
    description: Key business metrics at a glance
    queries:
      - name: executive_kpis
        sql: |
          SELECT
            CAST(COUNT(DISTINCT customer_id) AS BIGINT) AS total_customers,
            CAST(SUM(revenue) AS DOUBLE) AS total_revenue,
            CAST(AVG(amount) AS DOUBLE) AS avg_order_value
          FROM transform_orders_cleaned
        chart: BigValue
        value: [total_customers, total_revenue, avg_order_value]
        labels: [Total Customers, Total Revenue, Avg Order Value]
    narrative: false

  - title: Segment Performance
    description: Revenue and customer distribution by segment
    queries:
      - name: segment_breakdown
        sql: |
          SELECT segment,
            CAST(COUNT(DISTINCT customer_id) AS BIGINT) AS customer_count,
            CAST(SUM(revenue) AS DOUBLE) AS segment_revenue,
            (SUM(revenue) / (SELECT SUM(revenue) FROM transform_orders_cleaned)) * 100 AS revenue_pct
          FROM entity_customer c
          JOIN transform_orders_cleaned o ON c.customer_id = o.customer_id
          GROUP BY segment
          ORDER BY segment_revenue DESC
        chart: BarChart
        x: segment
        y: revenue_pct
        title: Revenue Share by Segment (%)
      - name: segment_detail
        sql: "..."
        chart: DataTable
    narrative: true

  - title: Monthly Revenue Trends
    queries:
      - name: monthly_trend
        sql: |
          SELECT STRFTIME(order_date, '%Y-%m') AS month,
            CAST(SUM(revenue) AS DOUBLE) AS revenue
          FROM transform_orders_cleaned
          GROUP BY month ORDER BY month
        chart: LineChart
        x: month
        y: revenue
        title: Monthly Revenue
    narrative: true

  - title: Recommendations
    narrative: true  # LLM-only section, no queries
```

## Rendering Pipeline

1. **Load & validate** — Parse YAML, validate `sections` schema
2. **Execute queries** — Run each section's SQL against DuckDB, capture column names + rows
3. **Generate Evidence skeleton** — Programmatically build the .md page:
   - For each section: `## {title}` header
   - For each query: SQL fenced block + chart component (from YAML props)
   - For `narrative: true` sections: insert `{LLM_NARRATIVE}` placeholder
4. **LLM narrative pass** — Send the skeleton + query results to the LLM with a focused prompt: "Write analytical commentary for each section. Reference specific numbers from the data."
5. **Assemble final page** — Replace `{LLM_NARRATIVE}` placeholders with LLM output
6. **Scaffold & build** — Feed the completed markdown to existing `scaffold_report()` + `build_report()`

## Supported Chart Types and Props

| Chart | Required Props | Optional Props |
|-------|---------------|----------------|
| BigValue | `value` (list of columns) | `labels` (display names) |
| BarChart | `x`, `y` | `series`, `title`, `sort` |
| LineChart | `x`, `y` | `series`, `title` |
| AreaChart | `x`, `y` | `series`, `title` |
| DataTable | — | `title` |
| ScatterPlot | `x`, `y` | `series`, `title` |
| Histogram | `x` | `bins`, `title` |
| FunnelChart | `name`, `value` | `title` |

## Backward Compatibility

- Existing exposures with only `params.prompt` (no `sections`) continue to work via the agent-driven path
- The `sections` key is optional — its presence toggles the deterministic renderer
- `params.prompt` can coexist with `sections` — it's used as context for the LLM narrative pass
- `inputs` continue to work for Jinja2 resolution in SQL templates within sections

## Resolved Questions

1. **Should SQL support Jinja2 templates?** — Yes, reuse existing exposure Jinja2 resolution for `{{ inputs.table.columns }}` etc. within section SQL strings. This is already implemented in `exposure.py`.

2. **What if a pinned query fails?** — Return the error in the narrative slot for that section. Don't fail the entire report. The user can fix the SQL and re-run.

3. **How does the LLM get query results for narrative?** — Pre-execute all queries, format results as markdown tables, and include them in the narrative prompt context. The LLM never calls `execute_sql` in deterministic mode.

## Scope Boundaries

**In scope:**
- New `sections` schema in exposure YAML
- Deterministic Evidence markdown renderer from sections
- LLM narrative-only pass with query results as context
- Backward compatibility with prompt-only exposures

**Out of scope:**
- Auto-capture of LLM-generated queries into YAML (future enhancement)
- Multi-page reports (stick with single index page)
- Interactive chart configuration UI
- Custom CSS/theming in YAML
