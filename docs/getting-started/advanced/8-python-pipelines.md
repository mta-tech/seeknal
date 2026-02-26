# Chapter 8: Python Pipelines

> **Duration:** 25 minutes | **Difficulty:** Intermediate | **Format:** Python & CLI

Learn to build pipeline nodes using Python decorators, reference existing YAML nodes from Python, and leverage the full Python ecosystem inside your Seeknal pipeline.

---

## What You'll Build

A Python-powered analytics layer that sits on top of your existing YAML pipeline:

```
source.products (YAML) ──────────────────────┐
                                             │
source.sales_events (YAML) ──→ ...          │
  └── transform.sales_enriched (YAML) ──────┼──→ transform.category_insights (Python)
                                             │
source.exchange_rates (Python) ──────────────┴──→ transform.customer_analytics (Python)
```

**After this chapter, you'll have:**
- A Python source node declared via `@source` decorator
- Python transforms referencing existing YAML nodes via `ctx.ref()`
- A mixed YAML + Python pipeline running end-to-end
- Understanding of PEP 723 dependency management

---

## Prerequisites

Before starting, ensure you've completed:

- [ ] [Chapter 1: File Sources](1-file-sources.md) — Sources loaded (products, sales_events)
- [ ] [Chapter 2: Transformations](2-transformations.md) — `transform.sales_enriched` created
- [ ] Python 3.11+ and `uv` installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

---

## Part 1: Python Source Declaration (8 minutes)

### When to Use Python Sources

YAML sources declare data inputs via `.yml` files. Python sources do the same via `@source` decorators in `.py` files — useful when you want to keep source declarations alongside Python transforms in the same project.

!!! note "How Python Sources Work"
    The `@source` decorator is **declarative** — it tells Seeknal where to find data (CSV, Parquet, database, etc.). The SourceExecutor handles the actual data loading, just like YAML sources. The function body is not used for data loading.

    For **custom data generation logic**, use `@transform` instead (see Part 2 and Part 3).

### Create the Data File

First, create the exchange rates CSV that the source will load:

```bash
mkdir -p data
cat > data/exchange_rates.csv << 'EOF'
region,currency,rate_to_usd
north,USD,1.0
south,EUR,1.08
east,GBP,1.27
west,JPY,0.0067
EOF
```

### Draft a Python Source

```bash
seeknal draft source exchange_rates --python --deps pandas
```

This creates `draft_source_exchange_rates.py`. Open it — notice the PEP 723 header and decorator structure.

Edit `draft_source_exchange_rates.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
# ]
# ///

"""Source: Currency exchange rates for multi-region revenue analysis."""

from seeknal.pipeline import source


@source(
    name="exchange_rates",
    source="csv",
    table="data/exchange_rates.csv",
    description="Currency exchange rates by region",
)
def exchange_rates(ctx=None):
    """Declare exchange rate lookup source from CSV."""
    pass
```

!!! info "PEP 723 Dependency Management"
    The `# /// script` header declares dependencies **per file**. Each Python node runs in its own isolated virtual environment managed by `uv`:

    - No global `requirements.txt` — no version conflicts between nodes
    - `uv` creates and caches environments automatically
    - Dependencies are installed on first run, then cached

    **Don't** list `seeknal` itself — it's injected automatically via `sys.path`.

### Key Concepts

| Concept | Description |
|---------|-------------|
| `@source(name=...)` | Registers this function as a source node in the DAG |
| `source="csv"` | Source type — tells SourceExecutor how to load data |
| `table="data/..."` | File path (relative to project root) |
| `ctx=None` | Context is optional for sources (required for transforms) |

### Validate and Apply

```bash
seeknal dry-run draft_source_exchange_rates.py
seeknal apply draft_source_exchange_rates.py
```

**Checkpoint:** The dry-run shows a preview of the Python source configuration. The file moves to `seeknal/pipelines/exchange_rates.py`.

---

## Part 2: Python Transform with ctx.ref() (8 minutes)

### Referencing YAML Nodes from Python

The core power of mixed pipelines: Python nodes can reference **any** upstream node — YAML or Python — using `ctx.ref("kind.name")`.

### Draft a Python Transform

```bash
seeknal draft transform customer_analytics --python --deps pandas,duckdb
```

Edit `draft_transform_customer_analytics.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Transform: Customer analytics from enriched sales data."""

from seeknal.pipeline import transform


@transform(
    name="customer_analytics",
    description="Per-region revenue analytics with currency conversion",
)
def customer_analytics(ctx):
    """Join enriched sales with exchange rates for USD-normalized revenue."""
    # Reference existing YAML transform
    enriched = ctx.ref("transform.sales_enriched")

    # Reference Python source
    rates = ctx.ref("source.exchange_rates")

    return ctx.duckdb.sql("""
        SELECT
            e.region,
            r.currency,
            r.rate_to_usd,
            COUNT(*) AS order_count,
            SUM(e.total_amount) AS local_revenue,
            ROUND(SUM(e.total_amount) * r.rate_to_usd, 2) AS revenue_usd
        FROM enriched e
        LEFT JOIN rates r ON e.region = r.region
        GROUP BY e.region, r.currency, r.rate_to_usd
        ORDER BY revenue_usd DESC
    """).df()
```

!!! tip "ctx.ref() returns a DataFrame"
    `ctx.ref("transform.sales_enriched")` loads the intermediate parquet from the YAML pipeline and returns a pandas DataFrame. DuckDB can query it directly by variable name — no need to `register()` it unless you want a custom table alias.

### Key Differences from YAML Transforms

| Aspect | YAML Transform | Python Transform |
|--------|----------------|------------------|
| **SQL location** | `transform:` YAML field | `ctx.duckdb.sql(...)` in Python |
| **Input declaration** | `inputs:` list in YAML | `ctx.ref()` calls in function body |
| **Dependencies** | Built-in (DuckDB) | PEP 723 header |
| **Custom logic** | SQL only | Full Python + SQL |
| **Return type** | Implicit (SQL result) | Explicit `return df` |

### Apply

```bash
seeknal dry-run draft_transform_customer_analytics.py
seeknal apply draft_transform_customer_analytics.py
```

**Checkpoint:** The dry-run shows the transform configuration. The file moves to `seeknal/pipelines/customer_analytics.py`.

---

## Part 3: Advanced Python Transform (5 minutes)

### Using Python Libraries in Transforms

Create a second transform that uses pandas operations not easily expressed in SQL:

```bash
seeknal draft transform category_insights --python --deps pandas,duckdb
```

Edit `draft_transform_category_insights.py`:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "pyarrow",
#     "duckdb",
# ]
# ///

"""Transform: Category-level insights with pandas analytics."""

from seeknal.pipeline import transform
import pandas as pd


@transform(
    name="category_insights",
    description="Category performance ranking and share analysis",
)
def category_insights(ctx):
    """Compute category market share and performance ranking."""
    enriched = ctx.ref("transform.sales_enriched")

    # Use pandas for operations that are cleaner than SQL
    if not isinstance(enriched, pd.DataFrame):
        enriched = enriched.df()

    # Filter out NULL categories (orphan products)
    df = enriched[enriched["category"].notna()].copy()

    # Aggregate by category
    summary = df.groupby("category").agg(
        order_count=("event_id", "count"),
        total_units=("quantity", "sum"),
        total_revenue=("total_amount", "sum"),
        avg_order_value=("total_amount", "mean"),
    ).reset_index()

    # Add market share (percentage of total revenue)
    total = summary["total_revenue"].sum()
    summary["revenue_share_pct"] = round(summary["total_revenue"] / total * 100, 2)

    # Rank categories
    summary["rank"] = summary["total_revenue"].rank(ascending=False).astype(int)

    return summary.sort_values("rank")
```

```bash
seeknal dry-run draft_transform_category_insights.py
seeknal apply draft_transform_category_insights.py
```

**Checkpoint:** Applied to `seeknal/pipelines/category_insights.py`.

---

## Part 4: Run the Mixed Pipeline (4 minutes)

### View the Execution Plan

```bash
seeknal plan
```

You should see both YAML and Python nodes in the DAG:

```
source.products                                          (YAML)
source.sales_events                                      (YAML)
source.sales_snapshot                                    (YAML)
source.exchange_rates                                    (Python)
transform.events_cleaned                                 (YAML)
transform.sales_enriched                                 (YAML)
transform.customer_analytics                             (Python)
transform.category_insights                              (Python)
```

### Execute

```bash
seeknal run
```

**Expected output:**
```
source.products: SUCCESS in 0.01s
source.sales_events: SUCCESS in 0.01s
source.sales_snapshot: SUCCESS in 0.01s
source.exchange_rates: SUCCESS in 0.01s
transform.events_cleaned: SUCCESS in 0.02s
transform.sales_enriched: SUCCESS in 0.02s
transform.customer_analytics: SUCCESS in 1.5s
transform.category_insights: SUCCESS in 1.3s
```

!!! info "Why Python Transforms Are Slower"
    Python **transforms** run via `uv run` in a subprocess with an isolated virtual environment. The first run installs dependencies (cached for subsequent runs). Python **sources** are loaded by the SourceExecutor directly (same as YAML sources), so they're fast. YAML nodes execute directly in the main DuckDB process.

### Explore in REPL

```bash
seeknal repl
```

```sql
-- Revenue by region with USD conversion
SELECT * FROM customer_analytics;

-- Category market share ranking
SELECT category, total_revenue, revenue_share_pct, rank
FROM category_insights
ORDER BY rank;
```

**Checkpoint:** Both Python transform outputs are queryable in the REPL alongside YAML node outputs.

---

## What Could Go Wrong?

!!! danger "Common Pitfalls"
    **1. `uv` not installed**

    - Symptom: `FileNotFoundError: [Errno 2] No such file or directory: 'uv'`
    - Fix: Install uv: `curl -LsSf https://astral.sh/uv/install.sh | sh`

    **2. Missing PEP 723 dependency**

    - Symptom: `ModuleNotFoundError: No module named 'pandas'`
    - Fix: Add the missing package to the `# dependencies = [...]` header in your Python file. Don't add `seeknal` — it's injected automatically.

    **3. `ctx` is None in a transform**

    - Symptom: `AttributeError: 'NoneType' object has no attribute 'ref'`
    - Fix: Ensure your transform function accepts `ctx` as a parameter: `def my_transform(ctx):`. Sources use `ctx=None` (optional), but transforms **require** it.

    **4. DuckDB can't find the DataFrame variable**

    - Symptom: `Catalog Error: Table with name "df" does not exist`
    - Fix: Assign `ctx.ref()` to a **local variable** before using it in SQL. DuckDB resolves variable names from the local scope.

    ```python
    # Correct
    enriched = ctx.ref("transform.sales_enriched")
    result = ctx.duckdb.sql("SELECT * FROM enriched").df()

    # Wrong — no local variable named 'data'
    result = ctx.duckdb.sql("SELECT * FROM data").df()
    ```

    **5. Return type is not a DataFrame**

    - Symptom: `TypeError: Cannot convert dict to DataFrame`
    - Fix: Always return a pandas DataFrame from decorated functions. Wrap dicts with `pd.DataFrame(data)` or use `.df()` on DuckDB results.

---

## Summary

In this chapter, you learned:

- [x] **Python Sources** — Declare data sources using `@source` in Python files
- [x] **Python Transforms** — Process data with `@transform` and `ctx.ref()`
- [x] **PEP 723 Dependencies** — Per-file dependency isolation with `uv`
- [x] **Mixed Pipelines** — Python nodes referencing YAML nodes seamlessly
- [x] **ctx.duckdb** — Run SQL queries on DataFrames inside Python transforms
- [x] **ctx.ref()** — Reference any upstream node: `ctx.ref("source.X")`, `ctx.ref("transform.Y")`

**Python vs YAML Decision Guide:**

| Use YAML When | Use Python When |
|---------------|-----------------|
| Loading files or database tables | Custom computation or API calls |
| SQL transforms (filter, join, aggregate) | ML models or statistical analysis |
| Simple, declarative pipelines | Complex business logic |
| No external library needed | Need pandas, scikit-learn, requests, etc. |

**Decorator Reference:**

| Decorator | Purpose | `ctx` Required? |
|-----------|---------|-----------------|
| `@source(name=...)` | Data ingestion | Optional (`ctx=None`) |
| `@transform(name=...)` | Data transformation | Yes |
| `@feature_group(name=...)` | ML feature engineering | Yes |
| `@materialize(type=...)` | Multi-target output (stackable) | N/A (wraps other decorators) |

**Key Commands:**
```bash
seeknal draft source <name> --python       # Python source template
seeknal draft transform <name> --python    # Python transform template
seeknal draft source <name> --python --deps pandas,requests  # With deps
seeknal dry-run <draft_file>.py            # Preview Python node
seeknal apply <draft_file>.py              # Apply to project
seeknal plan                               # View mixed DAG
seeknal run                                # Execute all nodes
seeknal repl                               # Query results
```

---

## What's Next?

In **[Chapter 9: Database & External Sources](9-database-sources.md)**, you'll learn to load data from PostgreSQL, StarRocks (MySQL), and Iceberg tables using connection profiles and pushdown queries.

Or explore other resources:

- **[Python Pipelines Guide](../../guides/python-pipelines.md)** — Full decorator reference and patterns
- **[Mixed YAML + Python Tutorial](../../tutorials/mixed-yaml-python-pipelines.md)** — Comprehensive mixed pipeline examples

---

## See Also

- **[Python Pipelines Tutorial](../../tutorials/python-pipelines-tutorial.md)** — End-to-end Python pipeline (RFM analysis)
- **[CLI Reference](../../reference/cli.md)** — All commands and flags
- **[YAML Schema Reference](../../reference/yaml-schema.md)** — Source, transform, and profile schemas
