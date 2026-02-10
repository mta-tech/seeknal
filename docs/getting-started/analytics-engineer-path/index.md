# Analytics Engineer Path

> **Time Commitment:** 3 chapters × 20-30 minutes | **Prerequisites:** [Quick Start](../../quick-start/) completed

Create semantic models and business metrics that enable self-serve analytics for your entire organization.

---

## Who This Path Is For

This path is ideal for:

- **Analytics Engineers** building semantic layers and metrics
- **Data Analysts** transitioning to engineered analytics
- **BI Developers** creating governed data models
- **Product Managers** defining KPIs and business metrics

**You'll love this path if you:**
- Want to define business logic once, reuse everywhere
- Care about data governance and consistency
- Need to enable self-serve analytics
- Prefer SQL-based data modeling

---

## What You'll Learn

By completing this path, you will:

1. **Define Semantic Models** — Create reusable entities, dimensions, and measures
2. **Create Business Metrics** — Build KPIs, ratios, and cumulative metrics
3. **Deploy for Self-Serve Analytics** — Enable BI tools with governed metrics

| Chapter | Focus | Duration |
|---------|-------|----------|
| [Chapter 1](1-semantic-models.md) | Define Semantic Models | 25 min |
| [Chapter 2](2-business-metrics.md) | Create Business Metrics | 30 min |
| [Chapter 3](3-self-serve-analytics.md) | Deploy for BI Consumption | 25 min |

**Total Time:** ~80 minutes

---

## Prerequisites

Before starting this path, ensure you've completed:

- [ ] [Quick Start](../../quick-start/) — Understand the pipeline builder workflow
- [ ] Python 3.11+ installed
- [ ] Seeknal CLI installed
- [ ] Basic SQL knowledge (SELECT, JOIN, GROUP BY)

!!! tip "New to Seeknal?"
    Start with the [Quick Start](../../quick-start/) if you haven't already. It takes 10 minutes and teaches the fundamentals.

---

## Chapter Overview

### Chapter 1: Define Semantic Models (25 minutes)

**Use Case:** E-commerce analytics foundation

You'll create a semantic model that:
- Defines entities (customers, products, orders)
- Specifies dimensions (attributes for filtering/grouping)
- Creates measures (aggregatable metrics)

**What You'll Learn:**
- How to structure a semantic model
- Entity relationships and joins
- Dimension types (categorical, time, geographic)
- Measure definitions and aggregation

**Start Chapter 1 →** [1-semantic-models.md](1-semantic-models.md)

---

### Chapter 2: Create Business Metrics (25 minutes)

**Use Case:** E-commerce KPIs and performance indicators

You'll build metrics that:
- Calculate simple KPIs (total revenue, order count)
- Create ratio metrics (conversion rate, AOV)
- Define cumulative metrics (running totals)
- Derive complex metrics from simpler ones

**What You'll Learn:**
- Metric types (simple, ratio, cumulative, derived)
- Time-based calculations (MoM, YoY growth)
- Metric composition and reusability
- Business logic centralization

**Start Chapter 2 →** [2-business-metrics.md](2-business-metrics.md)

---

### Chapter 3: Deploy for Self-Serve Analytics (30 minutes)

**Use Case:** Enable BI tools with governed metrics

You'll learn to:
- Deploy semantic models to StarRocks
- Create materialized views for performance
- Connect BI tools (Tableau, Power BI, Metabase)
- Enable self-serve analytics for stakeholders

**What You'll Learn:**
- StarRocks deployment and configuration
- Materialization strategies
- BI tool integration patterns
- Governance and access control

**Start Chapter 3 →** [3-self-serve-analytics.md](3-self-serve-analytics.md)

---

## What Makes This Path Different

### Analytics Engineer Focus

Unlike generic SQL tutorials, this path teaches semantic layer design:

```yaml
# Real-world semantic model
semantic_model:
  name: orders

  entities:
    - name: order
      id: order_id
      type: transaction

    - name: customer
      id: customer_id
      type: profile

  dimensions:
    - name: order_status
      type: categorical
      values: [pending, completed, cancelled]

    - name: order_date
      type: time
      granularity: [day, week, month]

  measures:
    - name: total_revenue
      expression: SUM(revenue)
      type: currency

    - name: avg_order_value
      expression: AVG(revenue)
      type: numeric
```

### Single Source of Truth

Every metric defined once, used everywhere:

- **Consistency**: Same definition across all tools
- **Governance**: Controlled changes with reviews
- **Discovery**: Findable metric catalog
- **Documentation**: Self-documenting semantic models

---

## Real-World Use Cases

After completing this path, you'll be ready to:

### Executive Dashboards
```
Semantic Models → Business Metrics → BI Tool → Decisions
                          ↓
                    KPI Catalog
```

### Self-Serve Analytics
```
Business Question → Semantic Layer → SQL → Answer
                         ↓
                   governed metrics
```

### Data Products
```
Raw Data → Semantic Models → Metrics → API → Applications
```

---

## See Also

Want to explore related topics?

- **Data Engineer Path** — ELT pipelines and data infrastructure
- **ML Engineer Path** — Feature stores for ML applications
- **[Semantic Layer Guide](../../guides/semantic-layer.md)** — Deep dive on semantic layer
- **[Glossary](../../concepts/glossary.md)** — Metric and semantic model terminology

---

## After This Path

Once you complete the Analytics Engineer path, you can:

1. **Explore Other Paths**
   - [Data Engineer Path](../data-engineer-path/) — Data pipelines and infrastructure
   - [ML Engineer Path](../ml-engineer-path/) — Feature stores and ML workflows

2. **Dive Deeper**
   - [Semantic Layer Guide](../../guides/semantic-layer.md) — Advanced semantic layer concepts
   - [CLI Reference](../../reference/cli.md) — All commands and flags
   - [YAML Schema Reference](../../reference/yaml-schema.md) — Semantic model and metric schemas

3. **Build Your Semantic Layer**
   - Define semantic models for your data
   - Create business metrics and KPIs
   - Deploy for self-serve analytics

---

## Quick Reference

| Concept | Chapter | Key Command |
|---------|---------|-------------|
| Semantic Models | 1 | `seeknal draft semantic-model` |
| Measures | 1 | Measure definitions in YAML |
| Simple Metrics | 2 | `seeknal draft metric --type simple` |
| Ratio Metrics | 2 | `seeknal draft metric --type ratio` |
| Cumulative Metrics | 2 | `seeknal draft metric --type cumulative` |
| StarRocks Deploy | 3 | `seeknal deploy-semantic-layer` |
| BI Integration | 3 | Connection strings and ODBC/JDBC |

---

## Before You Start

**Recommended reading:**
- [Glossary](../../concepts/glossary.md) — Metric and semantic model terminology
- [Point-in-Time Joins](../../concepts/point-in-time-joins.md) — Understanding temporal correctness

**Helpful skills:**
- Basic SQL (SELECT, JOIN, GROUP BY, HAVING)
- Understanding of data modeling (star schema, snowflake)
- Familiarity with BI tools (Tableau, Power BI, Looker)

---

**Ready to enable self-serve analytics?**

[Start Chapter 1: Define Semantic Models →](1-semantic-models.md)
