# Chapter 4: Entity Consolidation

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** Python Pipeline + CLI

Learn to consolidate multiple feature groups into unified per-entity views, retrieve features across feature groups with `ctx.features()`, and explore consolidated entities in the REPL.

---

## What You'll Build

A consolidated entity store that merges feature groups for the same entity:

```
feature_group.customer_features (Ch1) ──┐
                                        ├──→ Entity Consolidation ──→ entity_customer
feature_group.product_preferences ──────┘         (automatic)              ↓
                                                                    REPL Exploration
                                                                           ↓
                                                              ctx.features() retrieval
                                                                           ↓
                                                           seeknal entity list/show
```

**After this chapter, you'll have:**
- A second feature group (`product_preferences`) for the same customer entity
- Automatic consolidation into `target/feature_store/customer/features.parquet`
- Cross-FG feature retrieval with `ctx.features()` and `ctx.entity()`
- CLI commands to inspect consolidated entities

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: Feature Store](1-feature-store.md) — `feature_group.customer_features` created
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

## Part 3: Cross-FG Feature Retrieval (10 minutes)

### Using ctx.features()

The `ctx.features()` method lets you select specific features from across multiple feature groups in a single call:

```python
# In a transform node:
df = ctx.features("customer", [
    "customer_features.total_revenue",
    "customer_features.total_orders",
    "product_preferences.electronics_ratio",
])
```

This returns a **flat DataFrame** with columns:
- `customer_id`
- `event_time`
- `customer_features__total_revenue`
- `customer_features__total_orders`
- `product_preferences__electronics_ratio`

### Build a Training Data Transform

Create a transform that uses `ctx.features()` to prepare ML training data:

```bash
seeknal draft transform training_features --python --deps pandas,duckdb
```

Edit `draft_transform_training_features.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Transform: Prepare training features from consolidated entity store."""

from seeknal.pipeline import transform


@transform(
    name="training_features",
    description="Training-ready features selected from consolidated entity store",
)
def training_features(ctx):
    """Select and prepare features for model training."""
    # Select specific features across feature groups
    df = ctx.features("customer", [
        "customer_features.total_orders",
        "customer_features.total_revenue",
        "customer_features.avg_order_value",
        "product_preferences.category_count",
        "product_preferences.electronics_ratio",
    ])

    return df
```

### Apply and Run

```bash
seeknal dry-run draft_transform_training_features.py
seeknal apply draft_transform_training_features.py
seeknal run
```

### Verify the Results

```bash
seeknal repl
```

```sql
-- View training features (flat columns, no structs)
SELECT * FROM training_features LIMIT 5;

-- Verify the columns are flattened
DESCRIBE training_features;

-- Expected columns:
-- customer_id                               VARCHAR
-- event_time                                TIMESTAMP
-- customer_features__total_orders           BIGINT
-- customer_features__total_revenue          DOUBLE
-- customer_features__avg_order_value        DOUBLE
-- product_preferences__category_count       BIGINT
-- product_preferences__electronics_ratio    DOUBLE
```

### Using ctx.entity()

If you need all features with struct columns intact (e.g., for exploration or analysis):

```python
# In a transform or notebook:
df = ctx.entity("customer")
# Returns full DataFrame with struct columns
```

### Point-in-Time Retrieval

Both `ctx.features()` and `ctx.entity()` support an `as_of` parameter for point-in-time filtering:

```python
# Get features as they were on a specific date
df = ctx.features("customer", [
    "customer_features.total_revenue",
], as_of="2026-01-15")

# Returns the latest row per customer_id before the cutoff date
```

This is essential for training ML models without data leakage — you only see features that would have been available at the prediction time.

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
    **1. Feature reference format**

    - Symptom: `ValueError: Invalid feature format`
    - Fix: Use `fg_name.feature_name` format (dot-separated), not `fg_name__feature_name`

    ```python
    # Correct
    ctx.features("customer", ["customer_features.total_revenue"])

    # Wrong — double underscore is the output format, not the input
    ctx.features("customer", ["customer_features__total_revenue"])
    ```

    **2. Entity not found**

    - Symptom: `FileNotFoundError: Entity 'X' not found`
    - Fix: Run the pipeline first (`seeknal run`) to create the consolidated parquet. Feature groups must succeed before consolidation can run.

    **3. Mismatched join keys**

    - Symptom: `ValueError: Join key mismatch for entity 'customer'`
    - Fix: All feature groups for the same entity must use the same join keys. Check that both FGs have `entity="customer"` with `customer_id` in their output.

    **4. REPL view not showing**

    - Symptom: `entity_customer` view not found in REPL
    - Fix: The consolidated parquet must exist at `target/feature_store/customer/features.parquet`. Run `seeknal run` or `seeknal consolidate` first.

---

## Summary

In this chapter, you learned:

- [x] **Multiple Feature Groups** — Create separate FGs for the same entity
- [x] **Automatic Consolidation** — Entity views are created after `seeknal run`
- [x] **Struct Columns** — FG features are namespaced in DuckDB struct columns
- [x] **ctx.features()** — Select specific features across FGs (flat DataFrame)
- [x] **ctx.entity()** — Get all features with struct columns intact
- [x] **Point-in-Time** — Use `as_of` to prevent data leakage in training
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
source.transactions ──→ feature_group.customer_features ──┐
                    └──→ feature_group.product_preferences ┤
                                                           ├──→ Entity Consolidation
                                                           │         ↓
                                                           │    entity_customer
                                                           │         ↓
                                                           ├──→ transform.training_features
                                                           │         (ctx.features)
                                                           │
                                                           ├──→ transform.training_data
                                                           │         ↓
source.churn_labels ──────────────────────────────→ transform.churn_model
                                                                ↓
                                                   REPL: Query predictions

second_order_aggregation.region_metrics
         ↓
 Regional meta-features                              seeknal validate-features
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
