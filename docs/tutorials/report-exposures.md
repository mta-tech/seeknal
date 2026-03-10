# Report Exposures: Automated Data Analysis Reports

> **Estimated Time:** 20 minutes | **Difficulty:** Advanced
>
> **Prerequisites:** Completed the [Getting Started](../getting-started-comprehensive.md) tutorial. A seeknal project with data already materialized (`seeknal run` has been executed).

---

## What You'll Learn

By completing this tutorial, you will:

1. Understand the two report modes: **AI-guided** and **deterministic**
2. Create a deterministic report exposure with pinned SQL, charts, and narratives
3. Generate both a **standalone Markdown report** and an **interactive HTML dashboard**
4. Preview your report with the Evidence dev server
5. Iterate on sections and build a comprehensive analysis

---

## Overview

Report exposures (`type: report`) turn your pipeline data into professional analysis reports. Unlike standard exposures that export raw data, report exposures generate human-readable documents with charts, tables, and written commentary.

### Two Modes

| Mode | How It Works | Best For |
|------|-------------|----------|
| **AI-Guided** | LLM explores data and decides what to analyze | Ad-hoc exploration, one-off analysis |
| **Deterministic** | You define exact SQL, charts, layout; LLM writes narrative | Recurring reports, dashboards, auditable output |

This tutorial focuses on **deterministic reports** — the production-ready approach where you control the data and presentation while the LLM adds analytical commentary.

---

## Part 1: Your First Report Exposure (5 minutes)

### Create the Exposure File

Report exposures live in `seeknal/exposures/`. Create a file called `sales_dashboard.yml`:

```yaml
kind: exposure
name: sales_dashboard
type: report
description: Weekly Sales Dashboard
params:
  prompt: >
    You are analyzing weekly sales performance for an e-commerce business.
    Focus on actionable insights and reference specific numbers.
  format: both
inputs:
  - ref: source.raw_orders
  - ref: transform.orders_cleaned

sections:
  - title: Sales Overview
    description: Key sales metrics at a glance
    queries:
      - name: sales_kpis
        sql: |
          SELECT
            CAST(COUNT(*) AS BIGINT) AS total_orders,
            CAST(SUM(revenue) AS DOUBLE) AS total_revenue,
            CAST(AVG(revenue) AS DOUBLE) AS avg_order_value
          FROM transform_orders_cleaned
        chart: BigValue
        value: [total_orders, total_revenue, avg_order_value]
        labels: [Total Orders, Total Revenue, Avg Order Value]
    narrative: false
```

### Understanding the Structure

Let's break down each part:

**Header** — Standard exposure metadata:

```yaml
kind: exposure          # Node type
name: sales_dashboard   # Unique identifier (used in CLI)
type: report            # Exposure subtype
description: ...        # Human-readable description
```

**Params** — Report configuration:

```yaml
params:
  prompt: >             # Context for the LLM when writing narratives
    You are analyzing...
  format: both          # Output format: markdown, html, or both
```

**Inputs** — Upstream dependencies (DAG references):

```yaml
inputs:
  - ref: source.raw_orders           # Ensures data exists
  - ref: transform.orders_cleaned    # before the report runs
```

**Sections** — The report layout (what makes it deterministic):

```yaml
sections:
  - title: Sales Overview           # Section heading
    description: Key sales metrics  # Subtitle
    queries:                        # SQL queries for this section
      - name: sales_kpis           # Query identifier
        sql: |                     # The actual SQL
          SELECT ...
        chart: BigValue            # Chart type
        value: [col1, col2]        # Which columns to display
        labels: [Label 1, Label 2] # Display labels
    narrative: false               # Skip LLM commentary for this section
```

### Generate the Report

```bash
seeknal ask report --exposure sales_dashboard
```

This will:
1. Execute the SQL queries against your project data
2. Generate any LLM narratives for sections with `narrative: true`
3. Build an interactive HTML report (powered by Evidence.dev)
4. Save a standalone Markdown report to `target/reported/`

**Output:**
```
Running exposure: sales_dashboard
Project: .
Sections: 1

Report saved: target/reported/sales-dashboard/2026-03-10.md
Report built: target/reports/weekly-sales-dashboard/build/index.html
```

---

## Part 2: Chart Types and Queries (5 minutes)

Seeknal supports several chart types. Each query in a section specifies how to visualize its results.

### BigValue — KPI Cards

Display single metrics prominently:

```yaml
- name: kpi_metrics
  sql: |
    SELECT
      CAST(COUNT(DISTINCT customer_id) AS BIGINT) AS customers,
      CAST(SUM(revenue) AS DOUBLE) AS revenue
    FROM transform_orders_cleaned
  chart: BigValue
  value: [customers, revenue]
  labels: [Active Customers, Total Revenue]
```

The SQL must return **exactly one row**. Each column in `value` becomes a KPI card.

### BarChart — Categorical Comparisons

```yaml
- name: revenue_by_category
  sql: |
    SELECT category, CAST(SUM(revenue) AS DOUBLE) AS total_revenue
    FROM transform_orders_cleaned
    GROUP BY category
    ORDER BY total_revenue DESC
  chart: BarChart
  x: category
  y: total_revenue
  title: Revenue by Product Category
```

### LineChart — Trends Over Time

```yaml
- name: monthly_revenue
  sql: |
    SELECT month, CAST(total_revenue AS DOUBLE) AS revenue
    FROM transform_monthly_revenue
    ORDER BY month
  chart: LineChart
  x: month
  y: revenue
  title: Monthly Revenue Trend
```

### AreaChart — Filled Trends

Same syntax as LineChart, but filled below the line:

```yaml
- name: monthly_aov
  sql: |
    SELECT month, CAST(avg_order_value AS DOUBLE) AS aov
    FROM transform_monthly_revenue
    ORDER BY month
  chart: AreaChart
  x: month
  y: aov
  title: Average Order Value Over Time
```

### DataTable — Detailed Data

```yaml
- name: top_customers
  sql: |
    SELECT customer_id, name, total_spent, total_orders
    FROM transform_customer_purchase_stats
    ORDER BY total_spent DESC
    LIMIT 10
  chart: DataTable
  title: Top 10 Customers
```

### Combining Charts in One Section

A section can have multiple queries, each with its own chart:

```yaml
- title: Category Performance
  description: Revenue and order volume by product category
  queries:
    - name: category_chart
      sql: |
        SELECT category, CAST(SUM(revenue) AS DOUBLE) AS revenue
        FROM transform_orders_cleaned
        GROUP BY category ORDER BY revenue DESC
      chart: BarChart
      x: category
      y: revenue
      title: Revenue by Category
    - name: category_table
      sql: |
        SELECT category,
          CAST(SUM(revenue) AS DOUBLE) AS revenue,
          CAST(COUNT(*) AS BIGINT) AS orders,
          CAST(AVG(revenue) AS DOUBLE) AS avg_value
        FROM transform_orders_cleaned
        GROUP BY category ORDER BY revenue DESC
      chart: DataTable
  narrative: true
```

---

## Part 3: Narratives and Sections (3 minutes)

### Controlling Narratives

Each section has a `narrative` flag:

```yaml
sections:
  - title: Executive Summary
    narrative: false          # No commentary — just the charts/tables

  - title: Segment Analysis
    narrative: true           # LLM writes analytical commentary

  - title: Recommendations
    description: >
      Strategic recommendations based on the data above.
      Propose specific actions for each customer segment.
    narrative: true           # Narrative-only section (no queries needed)
```

When `narrative: true`, the LLM receives the section's query results and writes 2-3 sentences of analysis citing specific numbers from the data.

When a section has `narrative: true` but **no queries**, it becomes a narrative-only section. The LLM writes commentary based on all preceding sections — useful for recommendations or summaries.

### The Prompt

The `params.prompt` provides context to the LLM for all narrative sections:

```yaml
params:
  prompt: >
    You are a revenue operations analyst reviewing an Indonesian e-commerce
    business with 42 active customers and 127 completed orders across 2024.
    Focus on retention risks, growth levers, and operational efficiency.
    Reference specific numbers and percentages.
```

A good prompt tells the LLM:
- **Who they are** — Analyst role and expertise
- **What they're looking at** — Business context and scale
- **What to focus on** — Specific analytical themes
- **How to write** — Style guidance (cite numbers, be specific)

---

## Part 4: A Complete Example (5 minutes)

Here's a full 7-section report exposure that produces a comprehensive analysis:

```yaml
kind: exposure
name: customer_segmentation_deterministic
type: report
description: Customer Segmentation Analysis (Deterministic)
params:
  prompt: >
    You are analyzing an Indonesian e-commerce customer base segmented into
    Basic, Standard, and Premium tiers across 50 customers and 127 completed
    orders. Focus on actionable insights and reference specific numbers.
  format: both
inputs:
  - ref: source.raw_customers
  - ref: source.raw_orders
  - ref: transform.orders_cleaned
  - ref: transform.customer_purchase_stats
  - ref: transform.monthly_revenue
  - ref: transform.category_performance

sections:
  # Section 1: KPI dashboard (no narrative)
  - title: Executive Summary
    description: Key business metrics at a glance
    queries:
      - name: executive_kpis
        sql: |
          SELECT
            CAST(COUNT(DISTINCT customer_id) AS BIGINT) AS total_customers,
            CAST(SUM(revenue) AS DOUBLE) AS total_revenue,
            CAST(AVG(revenue) AS DOUBLE) AS avg_order_value,
            CAST(COUNT(*) AS BIGINT) AS total_orders
          FROM transform_orders_cleaned
        chart: BigValue
        value: [total_customers, total_revenue, avg_order_value, total_orders]
        labels: [Total Customers, Total Revenue, Avg Order Value, Total Orders]
    narrative: false

  # Section 2: Segment breakdown (BarChart + DataTable + narrative)
  - title: Segment Performance
    description: Revenue and customer distribution by customer segment
    queries:
      - name: segment_revenue
        sql: |
          SELECT
            c.segment,
            CAST(COUNT(DISTINCT c.customer_id) AS BIGINT) AS customer_count,
            CAST(SUM(o.revenue) AS DOUBLE) AS segment_revenue
          FROM source_raw_customers c
          JOIN transform_orders_cleaned o ON c.customer_id = o.customer_id
          GROUP BY c.segment
          ORDER BY segment_revenue DESC
        chart: BarChart
        x: segment
        y: segment_revenue
        title: Revenue by Customer Segment
      - name: segment_detail
        sql: |
          SELECT
            c.segment,
            CAST(COUNT(DISTINCT c.customer_id) AS BIGINT) AS customers,
            CAST(SUM(o.revenue) AS DOUBLE) AS total_revenue,
            CAST(AVG(o.revenue) AS DOUBLE) AS avg_order_value,
            CAST(COUNT(o.order_id) AS BIGINT) AS total_orders
          FROM source_raw_customers c
          JOIN transform_orders_cleaned o ON c.customer_id = o.customer_id
          GROUP BY c.segment
          ORDER BY total_revenue DESC
        chart: DataTable
    narrative: true

  # Section 3: Monthly trends (LineChart + narrative)
  - title: Monthly Revenue Trends
    description: How revenue evolved over 2024
    queries:
      - name: monthly_trend
        sql: |
          SELECT
            month,
            CAST(total_revenue AS DOUBLE) AS revenue,
            CAST(total_orders AS BIGINT) AS orders
          FROM transform_monthly_revenue
          ORDER BY month
        chart: LineChart
        x: month
        y: revenue
        title: Monthly Revenue (2024)
    narrative: true

  # Section 4: Category performance (BarChart + DataTable + narrative)
  - title: Category Performance
    description: Revenue by product category
    queries:
      - name: category_revenue
        sql: |
          SELECT
            category,
            CAST(total_revenue AS DOUBLE) AS revenue,
            CAST(total_orders AS BIGINT) AS orders,
            CAST(avg_order_value AS DOUBLE) AS avg_value
          FROM transform_category_performance
          ORDER BY total_revenue DESC
        chart: BarChart
        x: category
        y: revenue
        title: Revenue by Category
      - name: category_table
        sql: |
          SELECT
            category,
            CAST(total_revenue AS DOUBLE) AS revenue,
            CAST(total_orders AS BIGINT) AS orders,
            CAST(avg_order_value AS DOUBLE) AS avg_value,
            CAST(unique_customers AS BIGINT) AS customers
          FROM transform_category_performance
          ORDER BY total_revenue DESC
        chart: DataTable
    narrative: true

  # Section 5: Top customers (DataTable + narrative)
  - title: Top Customers
    description: Highest-value customers by total spend
    queries:
      - name: top_customers
        sql: |
          SELECT
            cps.customer_id,
            c.name,
            c.segment,
            c.city,
            CAST(cps.total_orders AS BIGINT) AS orders,
            CAST(cps.total_spent AS DOUBLE) AS total_spent,
            CAST(cps.avg_order_value AS DOUBLE) AS avg_value,
            cps.favorite_category
          FROM transform_customer_purchase_stats cps
          JOIN source_raw_customers c ON cps.customer_id = c.customer_id
          ORDER BY cps.total_spent DESC
          LIMIT 10
        chart: DataTable
        title: Top 10 Customers by Total Spend
    narrative: true

  # Section 6: Geographic distribution (BarChart + narrative)
  - title: Geographic Distribution
    description: Revenue across cities
    queries:
      - name: city_revenue
        sql: |
          SELECT
            c.city,
            CAST(SUM(o.revenue) AS DOUBLE) AS city_revenue
          FROM source_raw_customers c
          JOIN transform_orders_cleaned o ON c.customer_id = o.customer_id
          GROUP BY c.city
          ORDER BY city_revenue DESC
        chart: BarChart
        x: city
        y: city_revenue
        title: Revenue by City
    narrative: true

  # Section 7: Recommendations (narrative-only)
  - title: Recommendations
    description: >
      Strategic recommendations based on all the data above.
      Propose specific, data-backed actions for each customer segment.
    narrative: true
```

Generate it:

```bash
seeknal ask report --exposure customer_segmentation_deterministic
```

---

## Part 5: Viewing Reports (2 minutes)

### Markdown Output

The standalone Markdown file is saved with today's date:

```bash
cat target/reported/customer-segmentation-deterministic/2026-03-10.md
```

This file is **fully readable without any tools** — SQL results are rendered as Markdown tables, charts become ASCII art:

```
Revenue by Customer Segment

Standard │ ████████████████████████████████████████ 32,746.37
   Basic │ ██████████████████ 15,110.71
 Premium │ ██████████ 8,979.00
```

### HTML Dashboard

The interactive HTML report uses [Evidence.dev](https://evidence.dev/) for rich visualizations:

```bash
# List all generated reports
seeknal ask report list

# Live-preview with hot reload
seeknal ask report serve customer-segmentation-analysis-deterministic
```

The serve command:
1. Generates source data from the DuckDB tables
2. Starts a dev server on `http://localhost:3000`
3. Opens your browser automatically

Use `--port 8080` to change the port:

```bash
seeknal ask report serve customer-segmentation-analysis-deterministic --port 8080
```

### Specifying a Different Project

If your project is not in the current directory:

```bash
seeknal ask report --exposure sales_dashboard --project path/to/project
seeknal ask report serve my-report --project path/to/project
seeknal ask report list --project path/to/project
```

---

## SQL Guidelines

### Type Casting

Always cast aggregate results explicitly. This ensures compatibility across DuckDB and Evidence:

```sql
-- Good: explicit casts
SELECT
  CAST(COUNT(*) AS BIGINT) AS total_orders,
  CAST(SUM(revenue) AS DOUBLE) AS total_revenue,
  CAST(AVG(revenue) AS DOUBLE) AS avg_order_value
FROM transform_orders_cleaned

-- Bad: implicit types (may cause HUGEINT issues)
SELECT COUNT(*) AS total_orders, SUM(revenue) AS total_revenue
FROM transform_orders_cleaned
```

### Table Naming

SQL queries reference tables by their pipeline name with the kind prefix:

| Pipeline Node | Table Name in SQL |
|---------------|-------------------|
| `source.raw_customers` | `source_raw_customers` |
| `transform.orders_cleaned` | `transform_orders_cleaned` |
| `feature_group.user_features` | `feature_group_user_features` |

### JOINs Across Tables

You can JOIN any tables that are declared in `inputs`:

```sql
SELECT c.segment, SUM(o.revenue) AS revenue
FROM source_raw_customers c
JOIN transform_orders_cleaned o ON c.customer_id = o.customer_id
GROUP BY c.segment
```

---

## AI-Guided Reports (Alternative Mode)

For one-off analysis where you don't need deterministic output, use AI-guided mode by omitting `sections`:

```yaml
kind: exposure
name: quarterly_review
type: report
params:
  prompt: >
    Create a quarterly business review. Analyze revenue trends,
    customer segments, and product performance. Include charts.
  format: both
inputs:
  - ref: source.raw_customers
  - ref: transform.orders_cleaned
```

Or skip the YAML entirely and use the CLI directly:

```bash
seeknal ask report "customer segmentation analysis"
```

The LLM will explore your data, write SQL queries, choose chart types, and generate the full report autonomously.

---

## Tips and Best Practices

### Report Design

- **Start with KPIs** — Use `BigValue` + `narrative: false` for an executive summary
- **Mix chart types** — BarChart for comparisons, LineChart for trends, DataTable for detail
- **End with recommendations** — A narrative-only section that synthesizes all findings
- **Use descriptive section titles** — They become the report's table of contents

### SQL Best Practices

- **Always CAST aggregates** — `CAST(COUNT(*) AS BIGINT)`, `CAST(SUM(x) AS DOUBLE)`
- **ORDER BY for charts** — BarCharts look better sorted by the y-axis value
- **LIMIT for tables** — Large tables are hard to read; show top 10-20 rows
- **Use CASE for bucketing** — Create readable categories from continuous values

### Prompt Writing

- **Be specific about the domain** — "Indonesian e-commerce" not just "business"
- **Set the analytical tone** — "Focus on retention risks and growth levers"
- **Request numbers** — "Reference specific numbers and percentages"

---

## Summary

You've learned how to:

- Create deterministic report exposures with pinned SQL and charts
- Use all chart types: BigValue, BarChart, LineChart, AreaChart, DataTable
- Control narratives with the `narrative` flag
- Generate both Markdown and HTML reports
- Preview reports with `seeknal ask report serve`

**Key commands:**

```bash
seeknal ask report --exposure <name>           # Generate a report
seeknal ask report serve <slug>                # Live preview
seeknal ask report list                        # List reports
seeknal ask report "topic"                     # AI-guided (no YAML needed)
```

---

## Next Steps

- **[Exposures Concept](../concepts/exposures.md)** — Understand how report exposures fit into the pipeline
- **[YAML Schema Reference](../reference/yaml-schema.md)** — Full field reference for exposure nodes
- **[Entity Consolidation Guide](../guides/entity-consolidation.md)** — Combine features across groups

---

*Last updated: March 2026 | Seeknal Documentation*
