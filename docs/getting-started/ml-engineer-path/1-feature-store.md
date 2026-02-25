# Chapter 1: Build a Feature Store

> **Duration:** 30 minutes | **Difficulty:** Intermediate | **Format:** Python Pipeline

Learn to build a feature store using Python pipeline decorators — `@source` for data ingestion, `@feature_group` for feature engineering, `ctx.ref()` for referencing upstream nodes, and schema evolution for iterating on features.

---

## What You'll Build

A feature store for customer purchase analytics using Python decorators:

```
source.transactions (Python) ──→ feature_group.customer_features (Python)
                                          ↓
                                    REPL Exploration (seeknal repl)
```

**After this chapter, you'll have:**
- A Python source declared via `@source` decorator
- A feature group built with `@feature_group` and `ctx.ref()`
- Schema evolution workflow for adding new features
- PEP 723 per-file dependency management

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [DE Path Chapter 1](../data-engineer-path/1-elt-pipeline.md) — E-commerce pipeline set up
- [ ] [MLE Path Overview](index.md) — Introduction to this path
- [ ] Python 3.11+ and `uv` installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- [ ] Understanding of ML features and training data

---

## Part 1: Python Source Declaration (8 minutes)

### Create the Data

Create sample transaction data that will feed your feature group:

```bash
mkdir -p data
cat > data/transactions.csv << 'EOF'
customer_id,order_id,order_date,revenue,product_category,region
CUST-100,ORD-1001,2026-01-10,49.99,electronics,north
CUST-100,ORD-1002,2026-01-15,99.99,clothing,north
CUST-101,ORD-1003,2026-01-11,199.50,electronics,south
CUST-101,ORD-1004,2026-01-18,210.00,electronics,south
CUST-102,ORD-1005,2026-01-13,75.25,food,east
CUST-103,ORD-1006,2026-01-14,45.99,clothing,west
CUST-104,ORD-1007,2026-01-16,199.95,electronics,north
CUST-105,ORD-1008,2026-01-12,250.00,electronics,south
CUST-100,ORD-1009,2026-01-19,35.00,food,north
CUST-101,ORD-1010,2026-01-20,89.99,clothing,south
EOF
```

### Draft a Python Source

```bash
seeknal draft source transactions --python --deps pandas
```

This creates `draft_source_transactions.py`. Edit it:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
# ]
# ///

"""Source: Customer transaction data for feature engineering."""

from seeknal.pipeline import source


@source(
    name="transactions",
    source="csv",
    table="data/transactions.csv",
    description="Raw customer transactions with order details",
)
def transactions(ctx=None):
    """Declare transaction data source from CSV."""
    pass
```

!!! info "PEP 723 Dependency Management"
    The `# /// script` header declares dependencies **per file**. Each Python node runs in its own isolated virtual environment managed by `uv`:

    - No global `requirements.txt` — no version conflicts between nodes
    - `uv` creates and caches environments automatically
    - Dependencies are installed on first run, then cached

    **Don't** list `seeknal` itself — it's injected automatically via `sys.path`.

### Validate and Apply

```bash
seeknal dry-run draft_source_transactions.py
seeknal apply draft_source_transactions.py
```

**Checkpoint:** The file moves to `seeknal/pipelines/transactions.py`.

---

## Part 2: Build a Feature Group (12 minutes)

### Why @feature_group?

The `@feature_group` decorator defines a feature computation node that:

- References upstream sources/transforms via `ctx.ref()`
- Computes features using DuckDB SQL or pandas
- Automatically tracks schema versions
- Supports offline/online materialization

### Draft a Python Feature Group

```bash
seeknal draft feature-group customer_features --python --deps pandas,duckdb
```

Edit `draft_feature_group_customer_features.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Feature Group: Customer purchase behavior features."""

from seeknal.pipeline import feature_group


@feature_group(
    name="customer_features",
    entity="customer",
    description="Customer purchase behavior features for ML models",
)
def customer_features(ctx):
    """Compute customer-level features from transaction data."""
    # Reference the Python source
    txn = ctx.ref("source.transactions")

    return ctx.duckdb.sql("""
        SELECT
            customer_id,
            CAST(COUNT(DISTINCT order_id) AS BIGINT) AS total_orders,
            CAST(SUM(revenue) AS DOUBLE) AS total_revenue,
            CAST(AVG(revenue) AS DOUBLE) AS avg_order_value,
            MIN(order_date) AS first_order_date,
            MAX(order_date) AS last_order_date,
            CURRENT_TIMESTAMP AS event_time
        FROM txn
        GROUP BY customer_id
    """).df()
```

!!! tip "ctx.ref() returns a DataFrame"
    `ctx.ref("source.transactions")` loads the intermediate parquet from the source node and returns a pandas DataFrame. DuckDB can query it directly by variable name — no need to `register()` it.

### Key Concepts

| Concept | Description |
|---------|-------------|
| `@feature_group(name=...)` | Registers this function as a feature group node |
| `entity="customer"` | Declares the entity (join key) for this feature group |
| `ctx.ref("source.X")` | References upstream node output as DataFrame |
| `ctx.duckdb.sql(...)` | Runs DuckDB SQL on local DataFrames |

### Apply and Run

```bash
seeknal dry-run draft_feature_group_customer_features.py
seeknal apply draft_feature_group_customer_features.py
```

Now run the pipeline:

```bash
seeknal plan
seeknal run
```

**Expected output:**
```
Seeknal Pipeline Execution
============================================================
1/2: transactions [RUNNING]
  SUCCESS in 0.01s
  Rows: 10
2/2: customer_features [RUNNING]
  SUCCESS in 1.2s
  Rows: 6
✓ State saved
```

### Explore in REPL

```bash
seeknal repl
```

```sql
-- View customer features
SELECT * FROM customer_features;

-- Check feature distributions
SELECT
    COUNT(*) AS num_customers,
    ROUND(AVG(total_revenue), 2) AS avg_revenue,
    MAX(total_orders) AS max_orders
FROM customer_features;
```

**Checkpoint:** You should see features for 6 customers with `total_orders`, `total_revenue`, `avg_order_value`, etc.

---

## Part 3: Evolving the Feature Schema (10 minutes)

### Why Feature Evolution Matters

As your ML models mature, you'll need to add, modify, or remove features. With Seeknal workflow pipelines, schema changes are straightforward:

1. Edit the `@feature_group` function
2. Re-run the pipeline
3. Verify the new schema in the REPL

### Add a New Feature

Update `seeknal/pipelines/customer_features.py` to add a derived feature:

```python
@feature_group(
    name="customer_features",
    entity="customer",
    description="Customer purchase behavior features for ML models",
)
def customer_features(ctx):
    """Compute customer-level features from transaction data."""
    txn = ctx.ref("source.transactions")

    return ctx.duckdb.sql("""
        SELECT
            customer_id,
            CAST(COUNT(DISTINCT order_id) AS BIGINT) AS total_orders,
            CAST(SUM(revenue) AS DOUBLE) AS total_revenue,
            CAST(AVG(revenue) AS DOUBLE) AS avg_order_value,
            MIN(order_date) AS first_order_date,
            MAX(order_date) AS last_order_date,
            CAST(DATEDIFF('day', MIN(order_date), CURRENT_DATE) AS BIGINT)
                AS days_since_first_order,
            CURRENT_TIMESTAMP AS event_time
        FROM txn
        GROUP BY customer_id
    """).df()
```

Re-run:

```bash
seeknal run
```

### Verify the New Schema

```bash
seeknal repl
```

```sql
-- Check the updated schema
DESCRIBE customer_features;

-- Verify the new feature has values
SELECT
    customer_id,
    total_orders,
    days_since_first_order
FROM customer_features
ORDER BY days_since_first_order DESC;
```

**Expected output:** Each customer now has a `days_since_first_order` column alongside the original features.

### Track Changes with Lineage

Use the lineage command to see how `customer_features` connects to its upstream sources:

```bash
seeknal lineage feature_group.customer_features --ascii
```

This shows the DAG path from `source.transactions` to `feature_group.customer_features`.

!!! success "Congratulations!"
    You've built a feature store using Python pipeline decorators with DuckDB-powered SQL, schema evolution, and pipeline lineage.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. `uv` not installed**

    - Symptom: `FileNotFoundError: [Errno 2] No such file or directory: 'uv'`
    - Fix: Install uv: `curl -LsSf https://astral.sh/uv/install.sh | sh`

    **2. Missing PEP 723 dependency**

    - Symptom: `ModuleNotFoundError: No module named 'pandas'`
    - Fix: Add the missing package to the `# dependencies = [...]` header in your Python file. Don't add `seeknal` — it's injected automatically.

    **3. DuckDB can't find the DataFrame variable**

    - Symptom: `Catalog Error: Table with name "df" does not exist`
    - Fix: Assign `ctx.ref()` to a **local variable** before using it in SQL. DuckDB resolves variable names from the local scope.

    ```python
    # Correct
    txn = ctx.ref("source.transactions")
    result = ctx.duckdb.sql("SELECT * FROM txn").df()

    # Wrong — no local variable named 'data'
    result = ctx.duckdb.sql("SELECT * FROM data").df()
    ```

    **4. Entity key mismatch**

    - Symptom: Empty or misaligned join results
    - Fix: Ensure the entity join key column (`customer_id`) exists in your feature group output DataFrame

---

## Summary

In this chapter, you learned:

- [x] **Python Sources** — Declare data sources using `@source` in Python files
- [x] **Feature Groups** — Build features with `@feature_group` and `ctx.ref()`
- [x] **ctx.duckdb.sql()** — Run SQL queries on DataFrames inside Python pipelines
- [x] **PEP 723 Dependencies** — Per-file dependency isolation with `uv`
- [x] **Schema Evolution** — Evolve features by editing the function and re-running

**Key Commands:**
```bash
seeknal draft source <name> --python                          # Python source template
seeknal draft feature-group <name> --python                   # Python feature group template
seeknal dry-run <file>.py                                     # Preview Python node
seeknal apply <file>.py                                       # Apply to project
seeknal plan                                                  # Generate execution plan
seeknal run                                                   # Execute pipeline
seeknal repl                                                  # Interactive verification
seeknal lineage <node> --ascii                                # View DAG lineage
```

---

## What's Next?

[Chapter 2: Second-Order Aggregations →](2-second-order-aggregation.md)

Generate hierarchical features from transaction data using Python-based second-order aggregation decorators.

---

## See Also

- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — Full decorator reference and patterns
- **[CLI Reference](../../reference/cli.md)** — All commands and flags
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Feature group YAML schema
