# Chapter 4: Entity Consolidation

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** Python Pipeline + CLI

Learn to consolidate multiple feature groups into unified per-entity views, build training datasets by combining SOA with entity features, and explore consolidated entities in the REPL.

---

## What You'll Build

A consolidated entity store that merges feature groups for the same entity:

```
feature_group.customer_features (Ch1) ──┐
                                        ├──→ Entity Consolidation ──→ entity_customer
feature_group.product_preferences ──────┘         (automatic)              ↓
                                                                    REPL Exploration
                                                                           ↓
                                                    SOA training features + entity features
                                                                           ↓
                                                           seeknal entity list/show
```

**After this chapter, you'll have:**
- A second feature group (`product_preferences`) for the same customer entity
- Automatic consolidation into `target/feature_store/customer/features.parquet`
- A training dataset combining SOA temporal features with entity profile features
- CLI commands to inspect consolidated entities

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Feature Store](1-feature-store.md) — `feature_group.customer_features` created
- [ ] [Chapter 2: Second-Order Aggregation](2-second-order-aggregation.md) — `feature_group.customer_daily_agg` and SOA engine
- [ ] Pipeline runs successfully with `seeknal run`

---

## Part 1: Add a Second Feature Group (8 minutes)

### Why Multiple Feature Groups?

In practice, ML models need features from different perspectives of the same entity. For example, a customer entity might have:

- **customer_features**: Purchase behavior (total orders, revenue, recency)
- **product_preferences**: Product category preferences (top category, diversity)

Each feature group is computed independently but shares the same entity (join key: `customer_id`).

### Draft a Second Feature Group

```bash
seeknal draft feature-group product_preferences --python --deps pandas,duckdb
```

Edit `draft_feature_group_product_preferences.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Feature Group: Customer product category preferences."""

from seeknal.pipeline import feature_group


@feature_group(
    name="product_preferences",
    entity="customer",
    description="Customer product category preference features",
)
def product_preferences(ctx):
    """Compute product preference features per customer."""
    txn = ctx.ref("source.transactions")

    return ctx.duckdb.sql("""
        SELECT
            customer_id,
            -- Most purchased category
            MODE(product_category) AS top_category,
            -- Category diversity (number of distinct categories)
            CAST(COUNT(DISTINCT product_category) AS BIGINT) AS category_count,
            -- Electronics affinity (fraction of orders in electronics)
            CAST(
                SUM(CASE WHEN product_category = 'electronics' THEN 1 ELSE 0 END)
                AS DOUBLE
            ) / CAST(COUNT(*) AS DOUBLE) AS electronics_ratio,
            CURRENT_TIMESTAMP AS event_time
        FROM txn
        GROUP BY customer_id
    """).df()
```

### Apply and Run

```bash
seeknal dry-run draft_feature_group_product_preferences.py
seeknal apply draft_feature_group_product_preferences.py
```

Now run the full pipeline:

```bash
seeknal run
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
1/3: transactions [RUNNING]
  SUCCESS in 0.01s
2/3: customer_features [RUNNING]
  SUCCESS in 1.2s
3/3: product_preferences [RUNNING]
  SUCCESS in 1.1s
✓ State saved
✓ Entity consolidation: 1 entities, 2 FGs
```

**Checkpoint:** After the run, both feature groups are written independently AND a consolidated entity view is created automatically.

---

## Part 2: Explore the Consolidated Entity (7 minutes)

### Inspect with CLI

```bash
# List all consolidated entities
seeknal entity list
```

**Expected output:**
```
  customer              2 FGs  7 features  [ready]  consolidated: 2026-02-26T...
```

```bash
# Show detailed catalog
seeknal entity show customer
```

**Expected output:**
```
Entity: customer
Join keys: customer_id
Consolidated at: 2026-02-26T10:30:00
Schema version: 1
Feature groups: 2

  customer_features:
    Features: total_orders, total_revenue, avg_order_value, first_order_date, last_order_date, days_since_first_order
    Rows: 6
    Event time col: event_time

  product_preferences:
    Features: top_category, category_count, electronics_ratio
    Rows: 6
    Event time col: event_time
```

### Explore in REPL

```bash
seeknal repl
```

The REPL automatically registers consolidated entities as views named `entity_{name}`:

```sql
-- View the consolidated entity with struct columns
SELECT * FROM entity_customer LIMIT 3;
```

Each feature group's features are wrapped in a **struct column**:

```sql
-- Access features using dot notation
SELECT
    customer_id,
    customer_features.total_revenue,
    customer_features.total_orders,
    product_preferences.top_category,
    product_preferences.electronics_ratio
FROM entity_customer
ORDER BY customer_features.total_revenue DESC;
```

!!! info "Why Struct Columns?"
    Struct columns provide **schema isolation** — each feature group's columns are namespaced under the FG name. This means:

    - Two FGs can have columns with the same name (e.g., `count`) without conflict
    - You always know which FG a feature came from
    - The schema is self-documenting

### Verify the Storage

```bash
# The consolidated parquet
ls target/feature_store/customer/

# Expected:
# _entity_catalog.json
# features.parquet
```

```sql
-- Verify struct column types in REPL
DESCRIBE entity_customer;

-- Expected:
-- customer_id           VARCHAR
-- event_time            TIMESTAMP
-- customer_features     STRUCT(total_orders BIGINT, total_revenue DOUBLE, ...)
-- product_preferences   STRUCT(top_category VARCHAR, category_count BIGINT, ...)
```

**Checkpoint:** You should see the consolidated entity with struct-namespaced columns from both feature groups.

---

## Part 3: Build Training Dataset with SOA (10 minutes)

### Why SOA for Training Data?

In practice, ML training datasets are built by **aggregating historical features over time windows**. Raw feature group output captures point-in-time values, but ML models need patterns like:

- "How much did this customer spend in the last 7 days?"
- "Is spending trending up or down?"
- "What's the average daily order count?"

The SOA engine from Chapter 2 is the right tool for this — you already used it to compute regional meta-features. Now you'll use the **same engine** with a different `id_col` to produce per-customer training features.

### Create the Training Feature SOA

This SOA aggregates daily customer features (from Chapter 2's `feature_group.customer_daily_agg`) into per-customer training features with time-window context:

```bash
seeknal draft second-order-aggregation customer_training_features
```

Edit `draft_second_order_aggregation_customer_training_features.yml`:

```yaml
kind: second_order_aggregation
name: customer_training_features
description: "Per-customer training features aggregated from daily purchase data"
id_col: customer_id
feature_date_col: order_date
application_date_col: application_date
source: feature_group.customer_daily_agg
features:
  daily_amount:
    basic: [sum, mean, max]
  daily_count:
    basic: [sum, mean]
  recent_spending:
    window: [1, 7]
    basic: [sum]
    source_feature: daily_amount
  spending_trend:
    ratio:
      numerator: [1, 7]
      denominator: [8, 14]
      aggs: [sum]
    source_feature: daily_amount
```

!!! tip "Same Engine, Different Grouping"
    Compare this with Chapter 2's `region_metrics`:

    | | Chapter 2 (region_metrics) | Chapter 4 (customer_training_features) |
    |-|---------------------------|---------------------------------------|
    | **id_col** | `region` | `customer_id` |
    | **Purpose** | Regional meta-features | Per-customer training features |
    | **Output rows** | 4 (one per region) | 6 (one per customer) |
    | **Use case** | Understanding regional patterns | ML model input |

    The SOA engine handles both — the `id_col` determines the grouping key.

### Apply and Run

```bash
seeknal apply draft_second_order_aggregation_customer_training_features.yml
seeknal run
```

**Expected output:**
```
...
customer_training_features [RUNNING]
  SUCCESS in 0.8s
  Rows: 6
✓ State saved
✓ Consolidated entity 'customer': 2 FGs, 6 rows in 0.02s
```

### Build the Complete Training Dataset

Now create a transform that combines the SOA training features with entity-consolidated features and labels:

```bash
seeknal draft transform training_dataset --python --deps pandas,duckdb
```

Edit `draft_transform_training_dataset.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Transform: Combine SOA features with entity features for model training."""

from seeknal.pipeline import transform


@transform(
    name="training_dataset",
    description="Training dataset combining historical aggregations with entity features",
)
def training_dataset(ctx):
    """Build training dataset from SOA + entity features + labels."""
    # SOA-aggregated historical features (time-window aware)
    soa = ctx.ref("second_order_aggregation.customer_training_features")

    # Entity-level features from consolidated store
    # .to_df() converts FeatureFrame → plain DataFrame for DuckDB SQL
    prefs = ctx.ref("feature_group.product_preferences").to_df()

    # Churn labels (from Chapter 3 data)
    labels = ctx.ref("source.churn_labels")

    return ctx.duckdb.sql("""
        SELECT
            s.customer_id,
            -- Historical spending patterns (from SOA)
            s.daily_amount_SUM AS total_spend,
            s.daily_amount_MEAN AS avg_daily_spend,
            s.daily_amount_MAX AS max_daily_spend,
            s.daily_count_SUM AS total_orders,
            -- Product preferences (from entity consolidation)
            p.category_count,
            p.electronics_ratio,
            -- Labels
            CAST(l.churned AS INTEGER) AS churned
        FROM soa s
        LEFT JOIN prefs p ON s.customer_id = p.customer_id
        LEFT JOIN labels l ON s.customer_id = l.customer_id
    """).df()
```

!!! info "Why combine SOA + entity features?"
    The SOA provides **temporal patterns** (spending trends, recent vs historical behavior), while entity-consolidated feature groups provide **static profiles** (category preferences, product affinity). Together, they create a rich training dataset that captures both temporal dynamics and entity characteristics.

```bash
seeknal apply draft_transform_training_dataset.py
seeknal run
```

### Verify the Results

```bash
seeknal repl
```

```sql
-- View the complete training dataset
SELECT * FROM transform_training_dataset;

-- Check the feature columns
DESCRIBE transform_training_dataset;

-- Expected columns:
-- customer_id        VARCHAR
-- total_spend        DOUBLE    (from SOA: sum of daily amounts)
-- avg_daily_spend    DOUBLE    (from SOA: mean of daily amounts)
-- max_daily_spend    DOUBLE    (from SOA: max daily spend)
-- total_orders       BIGINT    (from SOA: count of transactions)
-- category_count     BIGINT    (from entity FG: product diversity)
-- electronics_ratio  DOUBLE    (from entity FG: electronics affinity)
-- churned            INTEGER   (label)
```

```sql
-- Preview: who's likely to churn?
SELECT
    customer_id,
    ROUND(total_spend, 2) AS total_spend,
    total_orders,
    category_count,
    ROUND(electronics_ratio, 2) AS elec_ratio,
    churned
FROM transform_training_dataset
ORDER BY total_spend;
```

**Checkpoint:** You should see 6 customers with combined SOA + entity features. Customers with lower spend and fewer categories are more likely churned.

---

## Part 4: Manual Consolidation (Optional)

### When to Use seeknal consolidate

Consolidation happens automatically after `seeknal run`. But sometimes you need to trigger it manually:

```bash
# After running individual nodes
seeknal run --nodes feature_group.customer_features
seeknal consolidate  # Re-consolidate with updated data

# After removing a feature group from the project
seeknal consolidate --prune  # Remove stale FG columns
```

### Incremental Behavior

Consolidation is **incremental** — only entities with changed feature groups are re-consolidated:

```bash
# Only customer_features changed, product_preferences unchanged
seeknal run --nodes feature_group.customer_features

# Consolidation only re-processes the "customer" entity
# using new customer_features + existing product_preferences
```

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. SOA source not found**

    - Symptom: `ValueError: Source 'feature_group.customer_daily_agg' not found`
    - Fix: Ensure Chapter 2's `customer_daily_agg` feature group is applied and has run successfully before creating the SOA.

    **2. Entity not found**

    - Symptom: `FileNotFoundError: Entity 'X' not found`
    - Fix: Run the pipeline first (`seeknal run`) to create the consolidated parquet. Feature groups must succeed before consolidation can run.

    **3. Mismatched join keys**

    - Symptom: `ValueError: Join key mismatch for entity 'customer'`
    - Fix: All feature groups for the same entity must use the same join keys. Check that both FGs have `entity="customer"` with `customer_id` in their output.

    **4. REPL view not showing**

    - Symptom: `entity_customer` view not found in REPL
    - Fix: The consolidated parquet must exist at `target/feature_store/customer/features.parquet`. Run `seeknal run` or `seeknal consolidate` first.

    **5. Training dataset has NULL columns**

    - Symptom: `category_count` or `electronics_ratio` columns are all NULL
    - Fix: Ensure `product_preferences` feature group ran successfully and the `customer_id` values match between SOA output and feature group output.

---

## Summary

In this chapter, you learned:

- [x] **Multiple Feature Groups** — Create separate FGs for the same entity
- [x] **Automatic Consolidation** — Entity views are created after `seeknal run`
- [x] **Struct Columns** — FG features are namespaced in DuckDB struct columns
- [x] **SOA for Training Data** — Reuse the SOA engine with `id_col: customer_id` for per-customer features
- [x] **Training Dataset** — Combine SOA temporal features + entity profiles + labels
- [x] **CLI Commands** — `seeknal entity list/show` and `seeknal consolidate`

**Key Commands:**
```bash
seeknal entity list                                              # List consolidated entities
seeknal entity show <name>                                       # Show entity catalog
seeknal consolidate                                              # Manual consolidation
seeknal consolidate --prune                                      # Remove stale FG columns
seeknal repl                                                     # Query entity_<name> views
```

---

## ML Engineer Path Complete!

You've completed the ML Engineer path. Here's the full pipeline you built:

```
                    ┌──→ feature_group.customer_features ──┐
source.transactions ├──→ feature_group.product_preferences ┤
                    │                                      ├──→ Entity Consolidation
                    │                                      │         ↓
                    │                                      │    entity_customer
                    │                                      │
                    └──→ feature_group.customer_daily_agg ──→ SOA: region_metrics
                              ↓                                 (regional meta-features)
                    SOA: customer_training_features
                              ↓
                    transform.training_dataset ←── feature_group.product_preferences
                              ↓                              (entity features)
                    transform.training_data ←──── source.churn_labels
                              ↓
                    transform.churn_model
                              ↓
                    REPL: Query predictions        seeknal validate-features
```

### What's Next?

- **[Advanced Guide: Python Pipelines](../advanced/8-python-pipelines.md)** — Mixed YAML + Python, advanced decorators
- **[Entity Consolidation Guide](../../guides/entity-consolidation.md)** — Deep dive into materialization and storage
- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — Full decorator reference
- **[Data Engineer Path](../data-engineer-path/)** — Build production ELT pipelines
- **[Analytics Engineer Path](../analytics-engineer-path/)** — Semantic layers and metrics

---

## See Also

- **[Entity Consolidation Guide](../../guides/entity-consolidation.md)** — Iceberg/PostgreSQL materialization
- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — All decorators and patterns
- **[CLI Reference](../../reference/cli.md#entity-consolidation)** — Entity and consolidate commands
