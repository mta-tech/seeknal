---
title: "Report Exposure: Rendered Markdown & Organized Report Definitions"
date: 2026-03-09
status: brainstorm
participants: [user, claude]
---

# Report Exposure: Rendered Markdown & Organized Report Definitions

## What We're Building

Two complementary modes for generating persistent, organized reports from seeknal data:

**Mode 1: Discovery → Codification (Interactive)**
User runs `seeknal ask report "topic"` interactively. The agent asks scoping questions via a rich interactive picker (arrow-key selectable menus), runs analyses, and produces a report. Once done, the agent asks: *"Save this as a regular report?"* If yes, it auto-generates a `kind: exposure, type: report` YAML file in `seeknal/exposures/` — codifying the ad-hoc analysis into a repeatable spec.

**Mode 2: Spec → Execution (Repeatable)**
User defines a report exposure in YAML (`seeknal/exposures/report_name.yml`). Running `seeknal ask report --exposure report_name` executes the prompt directly against the data — no discovery phase, no interactive questions. The agent follows the prompt, produces analysis, and saves output to `target/reported/{name}/{date}.md`.

Both modes produce the agent's final analysis answer as a rendered `.md` file in `target/reported/`.

## Why This Approach

- **Discovery-to-codification flow**: Ad-hoc analyses naturally evolve into repeatable reports. The agent bridges the gap by generating the YAML spec from a successful interactive session.
- **Exposure fits naturally**: Seeknal already has exposures for file/api/database/notification. Reports are another form of "expose data to the outside world."
- **YAML definitions are declarative and versionable**: Report specs live alongside pipeline definitions — teams can review, version, and share them.
- **Timestamped history**: `target/reported/{name}/{date}.md` creates an analysis trail. Compare this week's report to last week's.
- **Format flexibility**: `markdown`, `html` (Evidence dashboard), or `both`.

## Key Decisions

### 1. Two modes: interactive discovery + repeatable spec
Mode 1 is exploratory: agent asks questions, user selects answers, agent analyzes. Mode 2 is deterministic: agent follows a pre-defined prompt with no interaction. The bridge between them is auto-generated YAML.

### 2. Rich interactive picker for Mode 1
The agent presents choices via a TUI library (InquirerPy or questionary) with arrow-key navigation and highlighting. Not just numbered options — a polished CLI UX for scoping questions like "What aspects to focus on?", "What time period?", "Which segments?"

### 3. Auto-generate YAML exposure from interactive session
After a successful analysis in Mode 1, the agent offers to save it as a report exposure. If accepted, it creates `seeknal/exposures/{name}.yml` with the prompt distilled from the conversation, the tables used as inputs, and the format used. No manual YAML authoring needed.

### 4. Direct prompt execution for Mode 2 (no discovery)
When running from a YAML spec, the agent skips the discovery phase (no list_tables, describe_table warm-up). It takes the prompt as-is and jumps straight to execute_sql/execute_python. Faster, more predictable, best for scheduled/repeatable runs.

### 5. Jinja-style template variables in prompt
The `prompt` field supports Jinja2 variables resolved at runtime:
- `{{ inputs.table_name.columns }}` — column names from input tables
- `{{ run_date }}` — current date
- `{{ params.custom_var }}` — user-defined parameters

### 6. New exposure type, not a new kind
Reports are `kind: exposure, type: report` — reuses existing exposure executor infrastructure, DAG integration, and YAML validation.

### 7. On-demand CLI only (not auto-run)
Report exposures do NOT run during `seeknal run`. LLM calls are expensive — only run when explicitly requested.

### 8. Inputs as hints, full table access
`inputs:` refs are highlighted in the agent's prompt as primary focus, but the agent has full access to all project tables.

### 9. Configurable format: markdown, html, or both
- `markdown` (default): Agent's analysis saved as `.md` in `target/reported/`
- `html`: Evidence build → interactive dashboard in `target/reports/`
- `both`: Both artifacts produced

### 10. Timestamped subdirectory organization
`target/reported/{name}/{YYYY-MM-DD}.md`. Multiple runs per day overwrite the same file.

## Architecture Sketch

```
Mode 1: Interactive Discovery → Codification
=============================================

$ seeknal ask report "customer analysis"
  │
  ├─ Agent asks scoping questions (rich interactive picker)
  │   ┌─────────────────────────────────┐
  │   │ What aspects to focus on?       │
  │   │ ► Revenue by segment            │
  │   │   Purchase frequency            │
  │   │   Churn risk                    │
  │   │   All of the above             │
  │   └─────────────────────────────────┘
  │
  ├─ Agent runs analyses (execute_sql, execute_python)
  ├─ Agent presents final answer
  ├─ Agent saves answer → target/reported/{name}/{date}.md
  │
  ├─ Agent asks: "Save as a regular report?"
  │   User: "Yes"
  │
  └─ Agent auto-generates:
     seeknal/exposures/customer_analysis.yml
     ┌─────────────────────────────────────────┐
     │ kind: exposure                          │
     │ name: customer_analysis                 │
     │ type: report                            │
     │ params:                                 │
     │   prompt: >                             │
     │     Analyze revenue by customer segment,│
     │     purchase frequency patterns...      │
     │   format: markdown                      │
     │ inputs:                                 │
     │   - ref: transform.customer_stats       │
     │   - ref: source.raw_customers           │
     └─────────────────────────────────────────┘


Mode 2: Spec → Execution (Repeatable)
======================================

$ seeknal ask report --exposure customer_analysis
  │
  ├─ Load seeknal/exposures/customer_analysis.yml
  ├─ Resolve Jinja variables in prompt
  ├─ Create agent with prompt (skip discovery)
  ├─ Agent executes directly (execute_sql → analyze → answer)
  │
  ├─ format=markdown → target/reported/customer_analysis/2026-03-09.md
  ├─ format=html     → target/reports/customer-analysis/build/index.html
  └─ format=both     → both of the above
```

## YAML Schema

```yaml
kind: exposure
name: monthly_business_report
type: report
description: "Monthly business intelligence overview with revenue trends and customer insights"
owner: analytics-team
tags: [monthly, business]

params:
  prompt: >
    Analyze monthly revenue trends from {{ inputs.monthly_revenue.columns }}
    for period ending {{ run_date }}.
    Identify top-performing categories, segment customers by purchase
    behavior, and highlight any anomalies or significant changes.
  format: both              # markdown | html | both
  output_path: target/reported/   # optional override

inputs:
  - ref: transform.monthly_revenue
  - ref: transform.category_performance
  - ref: transform.customer_purchase_stats
  - ref: source.raw_customers
```

### Template Variables

| Variable | Resolves To |
|----------|-------------|
| `{{ run_date }}` | Current date (YYYY-MM-DD) |
| `{{ inputs.table_name.columns }}` | Comma-separated column names from input table |
| `{{ inputs.table_name.row_count }}` | Row count of input table |
| `{{ params.custom_key }}` | Any custom param defined in the YAML |

## CLI Integration

```bash
# Mode 1: Interactive report with scoping questions
seeknal ask report "customer analysis"

# Mode 2: Run from YAML spec
seeknal ask report --exposure monthly_business_report

# Run all report exposures
seeknal ask report --all

# List defined report exposures
seeknal ask report list --exposures

# Provider/model override
seeknal ask report --exposure monthly_business_report -p google -m gemini-2.5-flash
```

## Output Structure

```
target/
├── reported/                              # Agent's rendered markdown answers
│   ├── monthly_business_report/
│   │   ├── 2026-03-09.md                 # Latest run
│   │   ├── 2026-03-08.md                 # Previous run
│   │   └── 2026-03-07.md
│   └── customer_analysis/
│       └── 2026-03-09.md
│
├── reports/                               # Evidence HTML dashboards (unchanged)
│   └── monthly-business-report/
│       └── build/index.html
│
seeknal/
├── exposures/                             # Report YAML definitions
│   ├── monthly_business_report.yml
│   └── customer_analysis.yml             # Auto-generated from Mode 1
```

## Resolved Questions

1. **Where do report YAMLs live?** → `seeknal/exposures/` or anywhere under `seeknal/` — follows existing conventions for pipeline files.

2. **Ad-hoc reports without YAML?** → `seeknal ask report "topic"` works exactly as today (interactive chat). Exposure-based reports are the repeatable extension.

3. **CLI conflict?** → `--exposure` flag distinguishes YAML-defined from ad-hoc. Without it, existing interactive flow.

4. **Should target/reported/ be gitignored?** → Yes, like all `target/` subdirectories.

5. **Multiple runs per day?** → Overwrite same `{date}.md`. Daily granularity is the default.

6. **Auto-generated YAML quality?** → Agent distills the conversation into a clean prompt. User can edit the YAML afterward to refine.

7. **TUI library choice?** → InquirerPy (actively maintained, supports arrow-key selection, checkboxes, fuzzy search). Falls back to numbered input if not installed.

## What This Is NOT

- Not replacing the existing interactive `seeknal ask report "topic"` — that's Mode 1
- Not auto-running during `seeknal run` — on-demand only
- Not a scheduling system — use cron/airflow to call `seeknal ask report --exposure` on a schedule
- Not changing how Evidence HTML reports work — `target/reports/` stays as-is
