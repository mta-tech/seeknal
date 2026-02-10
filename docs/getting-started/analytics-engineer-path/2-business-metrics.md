# Chapter 2: Create Business Metrics

> **Duration:** 30 minutes | **Difficulty:** Intermediate | **Format:** YAML & Python

Learn to define business metrics that provide consistent, governed KPIs across your organization.

---

## What You'll Build

A comprehensive metrics layer for e-commerce:

```
Business Metrics
├── Simple Metrics (Total Revenue, Order Count)
├── Ratio Metrics (Conversion Rate, AOV)
├── Cumulative Metrics (Running Totals, YTD)
└── Derived Metrics (Revenue per Customer, Growth Rate)
```

**After this chapter, you'll have:**
- Reusable business metrics defined once
- Time-based aggregations (MoM, YoY)
- Metric composition and reusability
- Foundation for self-serve analytics

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Semantic Models](1-semantic-models.md) — Semantic layer foundation
- [ ] Comfortable with SQL aggregations and window functions
- [ ] Understanding of basic business metrics (KPIs)

---

## Part 1: Create Simple Metrics (8 minutes)

### Understanding Simple Metrics

Simple metrics aggregate a single measure:

| Metric | Definition | SQL |
|--------|------------|-----|
| Total Revenue | Sum of all revenue | `SUM(revenue)` |
| Order Count | Number of orders | `COUNT(*)` |
| Unique Customers | Distinct customers | `COUNT(DISTINCT customer_id)` |

=== "YAML Approach"

    Create a simple metric:

    ```bash
    seeknal draft metric --name total_revenue --type simple
    ```

    Edit `metrics/total_revenue.yaml`:

    ```yaml
    kind: metric
    name: total_revenue
    type: simple
    description: "Total revenue from completed orders"

    # Based on semantic model
    model: orders

    # Measure to aggregate
    measure: total_revenue

    # Filters
    filters:
      - status = 'completed'

    # Time granularity
    granularity: day

    # Metadata
    metadata:
      owner: "finance-team"
      category: "revenue"
      tags: ["kpi", "executive-dashboard"]
    ```

    Create order count metric:

    ```yaml
    kind: metric
    name: order_count
    type: simple
    description: "Total number of orders"

    model: orders
    measure: order_count

    filters:
      - order_date >= '2024-01-01'

    granularity: day

    metadata:
      owner: "operations-team"
      category: "volume"
      tags: ["kpi", "operational"]
    ```

=== "Python Approach"

    Create `metrics.py`:

    ```python
    from seeknal.semantic_layer import SimpleMetric

    # Total revenue metric
    total_revenue = SimpleMetric(
        name="total_revenue",
        description="Total revenue from completed orders",
        model="orders",
        measure="total_revenue",
        filters={"status": "completed"},
        granularity="day"
    )

    # Order count metric
    order_count = SimpleMetric(
        name="order_count",
        description="Total number of orders",
        model="orders",
        measure="order_count",
        granularity="day"
    )

    # Save metrics
    total_revenue.save("metrics/total_revenue.yaml")
    order_count.save("metrics/order_count.yaml")

    print("Simple metrics created!")
    ```

### Query Simple Metrics

```bash
# Query total revenue by day
seeknal query --metric total_revenue --group-by order_date --days 30
```

**Expected output:**
```
order_date   | total_revenue
-------------|--------------
2024-01-01   | $12,450.00
2024-01-02   | $15,230.00
2024-01-03   | $11,100.00
...
```

---

## Part 2: Create Ratio Metrics (10 minutes)

### Understanding Ratio Metrics

Ratio metrics combine two measures:

| Metric | Numerator | Denominator | Formula |
|--------|-----------|-------------|---------|
| Conversion Rate | Users who purchased | Total visitors | `purchases / visitors * 100` |
| Avg Order Value | Total revenue | Order count | `SUM(revenue) / COUNT(*)` |
| Repeat Rate | Repeat customers | Total customers | `repeat / total * 100` |

=== "YAML Approach"

    Create conversion rate metric:

    ```bash
    seeknal draft metric --name conversion_rate --type ratio
    ```

    Edit `metrics/conversion_rate.yaml`:

    ```yaml
    kind: metric
    name: conversion_rate
    type: ratio
    description: "Percentage of visitors who make a purchase"

    model: orders

    # Numerator (purchasers)
    numerator:
      measure: unique_customers
      filters:
        - status = 'completed'

    # Denominator (total visitors)
    denominator:
      measure: unique_customers
      filters:
        - created_at >= '2024-01-01'  # All visitors

    # Format as percentage
    format: percent

    # Time granularity
    granularity: week

    metadata:
      owner: "growth-team"
      category: "growth"
      tags: ["kpi", "executive-dashboard"]
    ```

    Create average order value:

    ```yaml
    kind: metric
    name: avg_order_value
    type: ratio
    description: "Average revenue per order"

    model: orders

    numerator:
      measure: total_revenue
      filters:
        - status = 'completed'

    denominator:
      measure: order_count
      filters:
        - status = 'completed'

    format: currency
    granularity: day

    metadata:
      owner: "finance-team"
      category: "revenue"
      tags: ["kpi", "executive-dashboard"]
    ```

=== "Python Approach"

    ```python
    from seeknal.semantic_layer import RatioMetric

    # Conversion rate
    conversion_rate = RatioMetric(
        name="conversion_rate",
        description="Percentage of visitors who make a purchase",
        model="orders",
        numerator={
            "measure": "unique_customers",
            "filters": {"status": "completed"}
        },
        denominator={
            "measure": "unique_customers"
        },
        format="percent",
        granularity="week"
    )

    # Average order value
    avg_order_value = RatioMetric(
        name="avg_order_value",
        description="Average revenue per order",
        model="orders",
        numerator={
            "measure": "total_revenue",
            "filters": {"status": "completed"}
        },
        denominator={
            "measure": "order_count",
            "filters": {"status": "completed"}
        },
        format="currency",
        granularity="day"
    )

    conversion_rate.save("metrics/conversion_rate.yaml")
    avg_order_value.save("metrics/avg_order_value.yaml")
    ```

### Query Ratio Metrics

```bash
# Conversion rate by week
seeknal query --metric conversion_rate --group-by order_date --weeks 12
```

**Expected output:**
```
week_start | conversion_rate
-----------|----------------
2024-01-01  | 3.2%
2024-01-08  | 3.5%
2024-01-15  | 3.8%
...
```

---

## Part 3: Create Cumulative Metrics (12 minutes)

### Understanding Cumulative Metrics

Cumulative metrics aggregate over time windows:

| Metric | Type | Description |
|--------|------|-------------|
| Running Total Revenue | Cumulative | Revenue from start to current date |
| YTD Revenue | Year-to-Date | Revenue since Jan 1 of current year |
| MoM Growth | Period-over-Period | Comparison with previous month |

=== "YAML Approach"

    Create running total metric:

    ```bash
    seeknal draft metric --name running_total_revenue --type cumulative
    ```

    Edit `metrics/running_total_revenue.yaml`:

    ```yaml
    kind: metric
    name: running_total_revenue
    type: cumulative
    description: "Cumulative revenue from start of period"

    model: orders

    # Base measure
    measure: total_revenue

    # Cumulative window
    window:
      type: running
      start: "2024-01-01"  # Start of cumulative period

    filters:
      - status = 'completed'

    granularity: day

    metadata:
      owner: "finance-team"
      category: "revenue"
      tags: ["trend", "executive-dashboard"]
    ```

    Create year-to-date metric:

    ```yaml
    kind: metric
    name: ytd_revenue
    type: cumulative
    description: "Year-to-date revenue"

    model: orders

    measure: total_revenue

    window:
      type: ytd  # Year-to-date

    filters:
      - status = 'completed'

    granularity: day

    metadata:
      owner: "finance-team"
      category: "revenue"
      tags: ["executive-dashboard", "quarterly-report"]
    ```

=== "Python Approach"

    ```python
    from seeknal.semantic_layer import CumulativeMetric

    # Running total
    running_total = CumulativeMetric(
        name="running_total_revenue",
        description="Cumulative revenue from start of period",
        model="orders",
        measure="total_revenue",
        window={
            "type": "running",
            "start": "2024-01-01"
        },
        filters={"status": "completed"},
        granularity="day"
    )

    # Year-to-date
    ytd_revenue = CumulativeMetric(
        name="ytd_revenue",
        description="Year-to-date revenue",
        model="orders",
        measure="total_revenue",
        window={"type": "ytd"},
        filters={"status": "completed"},
        granularity="day"
    )

    running_total.save("metrics/running_total_revenue.yaml")
    ytd_revenue.save("metrics/ytd_revenue.yaml")
    ```

### Create Derived Metrics

Derived metrics combine other metrics:

```yaml
kind: metric
name: revenue_per_customer
type: derived
description: "Average revenue per customer"

model: orders

# Formula using other metrics
expression: total_revenue / unique_customers

format: currency
granularity: month

metadata:
  owner: "finance-team"
  category: "efficiency"
  tags: ["kpi", "executive-dashboard"]
```

### Time Comparison Metrics

```python
# Month-over-month growth
mom_growth = Metric(
    name="mom_revenue_growth",
    description="Month-over-month revenue growth rate",
    model="orders",
    base_metric="total_revenue",
    comparison={
        "type": "period_over_period",
        "period": "month",
        "calculation": "growth_rate"
    },
    format="percent",
    granularity="month"
)
```

### Query Cumulative Metrics

```bash
# Running total by day
seeknal query --metric running_total_revenue --order-by order_date --days 90
```

**Expected output:**
```
order_date   | running_total_revenue
-------------|----------------------
2024-01-01   | $12,450.00
2024-01-02   | $27,680.00
2024-01-03   | $38,780.00
2024-01-04   | $52,100.00
...
```

---

## Metric Governance

!!! tip "Metric Naming Conventions"
    Follow these patterns:
    - **Revenue**: `total_revenue`, `ytd_revenue`, `running_total_revenue`
    - **Rates**: `conversion_rate`, `retention_rate`, `growth_rate`
    - **Averages**: `avg_order_value`, `avg_session_duration`
    - **Counts**: `order_count`, `user_count`, `transaction_count`

!!! warning "Avoid Metric Proliferation"
    Before creating a new metric, ask:
    - Does a similar metric exist?
    - Can I filter an existing metric instead?
    - Is this a temporary analysis or permanent KPI?

    Too many metrics = confusion and maintenance burden.

---

## Summary

In this chapter, you learned:

- [x] **Simple Metrics** — Single-measure aggregations
- [x] **Ratio Metrics** — Numerator/denominator calculations
- [x] **Cumulative Metrics** — Running totals and YTD
- [x] **Derived Metrics** — Compositions of other metrics
- [x] **Time Comparisons** — MoM, YoY growth rates
- [x] **Metric Governance** — Naming conventions and best practices

**Key Commands:**
```bash
seeknal draft metric --name <name> --type <simple|ratio|cumulative>
seeknal apply metrics/<name>.yaml
seeknal query --metric <name> --group-by <dimension>
```

---

## What's Next?

[Chapter 3: Deploy for Self-Serve Analytics →](3-self-serve-analytics.md)

Deploy your semantic models and metrics to StarRocks, and enable BI tool integration for self-serve analytics.

---

## See Also

- **[Semantic Layer Guide](../../guides/semantic-layer.md)** — Deep dive on semantic layer
- **[Building Blocks: Semantic Models](../../building-blocks/semantic-models.md)** — Semantic model and metrics reference
- **[Concepts: Business Metrics](../../concepts/business-metrics.md)** — Metric design principles