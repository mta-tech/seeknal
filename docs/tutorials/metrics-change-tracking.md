# Phase 2 Tutorial: Analytics Engineering with Change Categorization

Learn how change categorization and virtual environments help you safely evolve metric definitions in a SaaS revenue analytics pipeline.

## Overview

Build a revenue pipeline with sources (subscriptions, payments, customers), transforms (revenue base, customer metrics), and aggregations (MRR, churn). Then make schema changes and see how Seeknal categorizes them to prevent breaking downstream consumers.

```bash
seeknal init --name revenue-analytics && cd revenue-analytics && mkdir -p workflows
```

## 1. Define Sources

Copy source YAMLs from `examples/metrics-change-tracking/` to `workflows/`:
- `01_source_subscriptions.yml` — subscription records
- `02_source_payments.yml` — payment transactions
- `03_source_customers.yml` — customer profiles

```bash
seeknal plan workflows/  # ✓ Parsed 3 sources, generated manifest.json
```

> **Note:** `seeknal parse` also works as a backward-compatible alias.

## 2. Define Transforms

**File**: `workflows/04_transform_revenue_base.yml`
```yaml
kind: transform
name: revenue_base
transform: |
  SELECT s.subscription_id, s.customer_id, s.mrr, p.payment_amount, p.payment_date
  FROM source.subscriptions s LEFT JOIN source.payments p
    ON s.subscription_id = p.subscription_id WHERE s.status = 'active'
inputs: [{ref: source.subscriptions}, {ref: source.payments}]
```

**File**: `workflows/05_transform_customer_metrics.yml`
```yaml
kind: transform
name: customer_metrics
transform: |
  SELECT c.customer_id, c.company_name, c.industry,
    COUNT(DISTINCT r.subscription_id) as subscription_count, SUM(r.mrr) as total_mrr
  FROM source.customers c LEFT JOIN transform.revenue_base r ON c.customer_id = r.customer_id
  GROUP BY c.customer_id, c.company_name, c.industry
inputs: [{ref: source.customers}, {ref: transform.revenue_base}]
```

## 3. Define Aggregations

Copy aggregation YAMLs from `examples/metrics-change-tracking/`:
- `06_aggregation_monthly_revenue.yml` — MRR metrics with 30d/90d windows
- `07_aggregation_churn_analysis.yml` — subscription stability metrics

```bash
seeknal plan workflows/  # ✓ 3 sources, 2 transforms, 2 aggregations, 7 nodes
```

## 4. Make a Schema Change (Non-Breaking)

Add `country` column to `customer_metrics` SELECT and GROUP BY.

```bash
seeknal diff workflows/
# [CHANGED - rebuild this node] transform.customer_metrics
#     columns: added: country
# Summary: 1 non-breaking change(s) — only this node rebuilds
```

**NON_BREAKING** because we added (not removed) a column. Downstream queries still valid.

## 5. Make a Breaking Change

Rename `total_mrr` to `total_monthly_recurring_revenue` in the SELECT.

```bash
seeknal diff workflows/
# [BREAKING - rebuilds downstream] transform.customer_metrics
#     columns: removed: total_mrr; added: total_monthly_recurring_revenue
#     Downstream impact (1 nodes):
#       -> REBUILD aggregation.churn_rate_analysis
# Summary: 1 breaking change(s) — downstream nodes will rebuild
```

**BREAKING** because we removed a column that downstream aggregation references.

## 6. Environment Safety

Plan the change in a staging environment:

```bash
seeknal plan staging --path workflows/
# Environment: staging, Current state: 7 nodes
# [BREAKING] transform.customer_metrics
#     Downstream: aggregation.churn_rate_analysis
# Action required: Update downstream consumers first
```

> **Note:** `seeknal env plan staging` also works as a backward-compatible alias.

## 7. Apply and Promote

Update `churn_analysis.yml` feature column to `total_monthly_recurring_revenue`.

```bash
seeknal run --env staging --path workflows/
seeknal run --env staging --select +churn_rate_analysis
seeknal promote staging
# ✓ Rebuilt transform + aggregation, promoted staging → production
```

> **Note:** `seeknal env apply staging` and `seeknal env promote staging production` also work as backward-compatible aliases.

## 8. Change Categorization Reference

| Category | Examples | Impact |
|----------|----------|--------|
| **BREAKING** | Column removed/renamed/type-changed, node renamed, input removed | Rebuild this + ALL downstream nodes |
| **NON_BREAKING** | Column added, input added, config changed | Rebuild this node only |
| **METADATA** | Description/tags/owner updated | No rebuilds, manifest updated |

## Summary

You built a revenue pipeline and saw how `seeknal diff` categorizes changes to prevent breaking downstream consumers. BREAKING changes (removed columns) cascade rebuilds, NON_BREAKING changes (added columns) rebuild one node, and METADATA changes update docs without execution.

**Next**: Semantic models, data quality rules, CI/CD with `seeknal env plan`
