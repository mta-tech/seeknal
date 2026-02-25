# Analytics Engineer Path

**Duration:** ~75 minutes | **Format:** YAML | **Prerequisites:** SQL, [DE Path Chapter 1](../data-engineer-path/1-elt-pipeline.md) completed

Build semantic models and business metrics that enable self-serve analytics for your organization.

---

## What You'll Learn

The Analytics Engineer path teaches you to build a governed semantic layer with Seeknal. You'll learn to:

1. **Build Semantic Models** - Define entities, dimensions, and measures on top of existing transforms
2. **Define Business Metrics** - Create simple, ratio, cumulative, and derived metrics
3. **Deploy for Self-Serve** - Deploy metrics as StarRocks materialized views for BI tools

---

## Prerequisites

Before starting this path, ensure you have:

- **[DE Path Chapter 1](../data-engineer-path/1-elt-pipeline.md)** completed — You'll build on the `orders_cleaned` transform
- Basic SQL knowledge (JOINs, aggregations, window functions)
- Familiarity with business metrics (KPIs, ratios)

---

## Chapters

### Chapter 1: Build a Semantic Model (~25 minutes)

Define a semantic model on top of your e-commerce data:

```
orders_cleaned → Semantic Model → Queryable Metrics
                      ├── Entities (order, customer)
                      ├── Dimensions (date, status)
                      └── Measures (revenue, count, AOV)
```

**You'll build:**
- A semantic model YAML with entities, dimensions, and measures
- Queryable semantic layer using `seeknal query`
- Foundation for business metrics in Chapter 2

**[Start Chapter 1 →](1-semantic-models.md)**

---

### Chapter 2: Define Business Metrics (~25 minutes)

Create reusable business metrics from your semantic model:

```
Semantic Model → Metrics (YAML) → seeknal query
                    ├── Simple (Total Revenue)
                    ├── Ratio (AOV)
                    ├── Cumulative (Running Total)
                    └── Derived (Revenue Growth)
```

**You'll build:**
- Simple metrics (total revenue, order count)
- Ratio metrics (average order value)
- Cumulative and derived metrics
- Multi-metric queries with dimensions

**[Start Chapter 2 →](2-business-metrics.md)**

---

### Chapter 3: Deploy to StarRocks (~25 minutes)

Deploy your metrics as materialized views for BI consumption:

```
Metrics → seeknal deploy-metrics → StarRocks MVs → BI Tools
                                        ↓
                                  MySQL Protocol
```

**You'll learn:**
- StarRocks connection configuration
- Deploying metrics as materialized views
- DDL preview with `--dry-run`
- BI tool integration via MySQL protocol

**[Start Chapter 3 →](3-self-serve-analytics.md)**

---

## What You'll Build

By the end of this path, you'll have a complete semantic layer:

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Semantic Model** | YAML | Define entities, dimensions, measures |
| **Simple Metrics** | YAML | Aggregate single measures |
| **Ratio Metrics** | YAML | Numerator/denominator calculations |
| **Cumulative Metrics** | YAML | Running totals over time |
| **Derived Metrics** | YAML | Compose metrics from other metrics |
| **StarRocks MVs** | Materialized Views | Fast BI queries |

---

## Key Commands You'll Learn

```bash
# Query semantic layer metrics
seeknal query --metrics total_revenue,order_count --dimensions status

# Show generated SQL without executing
seeknal query --metrics total_revenue --compile

# Deploy metrics to StarRocks
seeknal deploy-metrics --connection starrocks://root@localhost:9030/analytics

# Preview DDL before deploying
seeknal deploy-metrics --connection starrocks://... --dry-run

# Interactive SQL on your data
seeknal repl
```

---

## Resources

### Reference
- [CLI Reference](../../reference/cli.md) — All commands and flags
- [YAML Schema Reference](../../reference/yaml-schema.md) — Semantic model and metric schemas

### Related Paths
- [Data Engineer Path](../data-engineer-path/) — ELT pipelines (prerequisite)
- [ML Engineer Path](../ml-engineer-path/) — Feature stores and ML pipelines

---

*Last updated: February 2026 | Seeknal Documentation*
