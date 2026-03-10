# Exposures

> Exposures are the final layer of a Seeknal pipeline. They define **how processed data leaves the system** — whether as exported files, API payloads, database inserts, or interactive reports.

---

## What Is an Exposure?

An exposure is a pipeline node (`kind: exposure`) that describes a downstream consumer of your data. While sources, transforms, and feature groups move data *into and through* your pipeline, exposures move it *out* — to dashboards, APIs, files, or stakeholders.

```
source → transform → feature_group → exposure
                                         ↓
                                    dashboard / file / API / report
```

### Why Exposures Matter

Without exposures, your pipeline is a closed loop. Data comes in, gets transformed, and sits in Parquet files. Exposures answer: **"Who uses this data, and how?"**

- **Documentation** — Exposures make downstream dependencies explicit in the DAG
- **Automation** — `seeknal run` executes the full pipeline including exports
- **Lineage** — You can trace from raw source all the way to the final consumer

---

## Exposure Types

| Type | Purpose | Output |
|------|---------|--------|
| `file` | Export data to files | Parquet, CSV, JSON |
| `api` | Push data to REST APIs | HTTP POST/PUT |
| `database` | Write to external databases | PostgreSQL, etc. |
| `report` | Generate analytical reports | HTML + Markdown |

### Standard Exposures

Standard exposures (`file`, `api`, `database`) are defined in YAML and executed as part of `seeknal run`:

```yaml
kind: exposure
name: daily_metrics_export
type: file
params:
  path: outputs/daily_metrics.parquet
  format: parquet
inputs:
  - ref: transform.daily_metrics
```

### Report Exposures

Report exposures (`type: report`) are a special category. Instead of exporting raw data, they generate **human-readable analysis** — interactive HTML dashboards and standalone Markdown documents. They are executed via `seeknal ask report --exposure <name>`.

Report exposures come in two modes:

#### AI-Guided Reports

The LLM explores your data and generates the analysis:

```yaml
kind: exposure
name: quarterly_review
type: report
params:
  prompt: "Create a quarterly business review analyzing revenue trends..."
  format: both
inputs:
  - ref: transform.monthly_revenue
  - ref: transform.customer_stats
```

#### Deterministic Reports

You define the exact SQL queries, charts, and layout. The LLM only writes narrative commentary:

```yaml
kind: exposure
name: monthly_kpis
type: report
description: Monthly KPI Dashboard
params:
  prompt: "You are analyzing monthly business performance..."
  format: both
inputs:
  - ref: transform.monthly_revenue

sections:
  - title: Revenue Overview
    queries:
      - name: total_revenue
        sql: |
          SELECT CAST(SUM(revenue) AS DOUBLE) AS revenue
          FROM transform_monthly_revenue
        chart: BigValue
        value: [revenue]
        labels: [Total Revenue]
    narrative: false
```

The `sections` key is what makes a report deterministic. See the [Report Exposures Tutorial](../tutorials/report-exposures.md) for a complete walkthrough.

---

## How Exposures Connect to the DAG

Exposures use `inputs` to declare their upstream dependencies:

```yaml
inputs:
  - ref: source.raw_customers       # References a source node
  - ref: transform.orders_cleaned   # References a transform node
  - ref: feature_group.user_features # References a feature group
```

The `ref` format is `<kind>.<name>`. When the pipeline runs, Seeknal ensures all upstream nodes execute before the exposure.

For report exposures, inputs serve two purposes:

1. **DAG ordering** — Ensures the data exists before the report runs
2. **Context** — The report engine can inspect input tables (columns, row counts) to resolve template variables in prompts

---

## Output Locations

| Exposure Type | Output Path |
|---------------|-------------|
| Standard (`file`) | As specified in `params.path` |
| Report (HTML) | `target/reports/{slug}/build/index.html` |
| Report (Markdown) | `target/reported/{slug}/{date}.md` |

Report exposures with `format: both` produce both HTML and Markdown outputs.

---

## Key Differences: Standard vs Report Exposures

| Aspect | Standard Exposure | Report Exposure |
|--------|-------------------|-----------------|
| **Execution** | `seeknal run` | `seeknal ask report --exposure <name>` |
| **Output** | Data files/API calls | Interactive HTML + Markdown |
| **LLM Required** | No | Yes (for narratives) |
| **Sections** | N/A | Optional — enables deterministic mode |
| **Charts** | N/A | BigValue, BarChart, LineChart, AreaChart, DataTable |

---

## Related Topics

- [Report Exposures Tutorial](../tutorials/report-exposures.md) — Hands-on guide to building report exposures
- [YAML Schema Reference](../reference/yaml-schema.md) — Full field reference for all node types
- [Pipeline Builder Workflow](pipeline-builder.md) — Understand the draft/apply/run lifecycle

---

*Last updated: March 2026 | Seeknal Documentation*
