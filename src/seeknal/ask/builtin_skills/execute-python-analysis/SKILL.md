---
name: execute-python-analysis
description: "Run Python code in an isolated subprocess for statistical/ML/visualization work beyond what SQL can express"
tags: [analysis, python, sandbox, pandas, matplotlib]
version: "1.0.0"
---

# Execute Python Analysis

Use this workflow when the analysis requires Python capabilities that SQL
cannot express — statistical tests, visualization, machine learning, custom
algorithms, or complex pandas transformations. For simple data queries,
prefer `execute_sql` instead.

## Tool you will call

- `execute_python` — runs code in an isolated subprocess sandbox

## When to use

Use `execute_python` for:

- Statistical tests (`scipy.stats.ttest_ind`, chi-square, ANOVA)
- Correlation matrices / regression models
- Histograms, scatter plots, box plots via `matplotlib`
- Clustering / classification via `scikit-learn`
- Complex pandas operations (pivot tables, rolling windows, reindex)
- Custom algorithms that don't fit in SQL

DO NOT use `execute_python` for:

- Simple aggregations / grouping → use `execute_sql`
- Multi-table JOINs → use `execute_sql`
- Metric queries with time grains → use `query_metric`
- File I/O (reading data) → use `conn.sql('SELECT * FROM ...')` inside the code

## Phase 1 — Understand the sandbox

Each `execute_python` call runs in a FRESH isolated subprocess. This has
implications you MUST account for:

### The `conn` object is ALREADY connected to your project

**CRITICAL — do NOT create your own DuckDB connection.** The sandbox
pre-loads a `conn` object (a `SafeConnection` wrapper around the real
project DuckDB) with EVERY project table/view already registered. It is
ready to use on the first line.

❌ **WRONG** — creates a new empty DB, loses all project data:
```python
import duckdb
conn = duckdb.connect(':memory:')        # shadows the sandbox conn
df = conn.execute("SELECT * FROM transform_daily_revenue").df()
# → CatalogException: Table with name transform_daily_revenue does not exist!
```

✅ **RIGHT** — use the pre-loaded `conn` directly, no imports:
```python
df = conn.sql("SELECT * FROM transform_daily_revenue").df()
df.head(10)
```

The pre-loaded `conn` has:
- Every source/transform/feature_group/model registered as a view, using the
  kind-prefixed naming (`source_customers`, `transform_daily_revenue`,
  `feature_group_customer_features`, etc.)
- All project intermediate parquets from `target/intermediate/` mounted

Do NOT `import duckdb` — the sandbox does not expect you to instantiate one.
If you're unsure what tables exist, run `SHOW TABLES`:
```python
tables = conn.sql("SHOW TABLES").df()
print(tables)
```

### Other sandbox properties

1. **No persistence between calls.** Variables from a previous call do NOT
   exist in the next call. Re-query data at the start of every call:
   ```python
   df = conn.sql("SELECT * FROM customers").df()
   ```

2. **Limited package set.** These names may be pre-imported when installed in
   the current Seeknal environment:
   - `conn` — **pre-loaded DuckDB SafeConnection** (see above, do not re-create)
   - `pd` — pandas
   - `np` — numpy
   - `plt` — matplotlib.pyplot (Agg backend), or `None` if unavailable
   - `matplotlib` — the full matplotlib package, or `None` if unavailable
   - `sklearn` — scikit-learn (import submodules like `sklearn.cluster`)
   - `scipy` — scipy (import submodules like `scipy.stats`)

   DO NOT import `statsmodels`, `xgboost`, `lightgbm`, `tensorflow`, `torch`,
   `plotly`, or any other package — they are NOT installed. `ModuleNotFoundError`
   means you chose the wrong library.

3. **Last expression is returned (Jupyter style).** Put a bare expression on
   the last line to capture its value:
   ```python
   df = conn.sql("SELECT * FROM orders").df()
   df.describe()  # ← captured and returned
   ```

## Phase 2 — The SQL-comment gotcha

DuckDB does NOT recognize `#` as a comment. If you put a Python-style `#`
comment inside a SQL string, the query fails with `ParserException`.

WRONG:
```python
conn.sql("""
    SELECT customer_id, total  # get order totals
    FROM orders
""").df()
```

RIGHT:
```python
# Get order totals — comment is Python, outside the SQL string
conn.sql("""
    SELECT customer_id, total
    FROM orders
""").df()
```

Or use SQL `--` comments inside the string:
```python
conn.sql("""
    SELECT customer_id, total  -- this is a SQL comment
    FROM orders
""").df()
```

## Phase 3 — Plots

Plots are captured automatically at the end of the call when matplotlib is
available. Check `plt is not None` before plotting. If it is `None`, do not
try to install/import matplotlib; provide a text/table answer or non-visual
statistics instead.

```python
if plt is None:
    print("Plotting unavailable in this environment; returning table summary.")
else:
    plt.figure(figsize=(10, 6))
    plt.hist(df['age'], bins=20)
    plt.title('Age Distribution')
```

When plotting is available, just use `plt`:

```python
plt.figure(figsize=(10, 6))
plt.hist(df['age'], bins=20)
plt.title('Age Distribution')
```

Do NOT call `plt.show()` — the sandbox captures all open figures to temp
PNG files and lists their paths in the return value. The agent can reference
these paths when building reports.

## Phase 4 — Error recovery

When `execute_python` returns an error, read the error type and retry:

- `ModuleNotFoundError` or `terminal_dependency_unavailable` → the package
  isn't available; do not retry the same import/charting path. Use another
  available library or return a text/table answer.
- `NameError` → a variable from a previous call; re-query at the start
- `ParserException` with `#` → Python comment inside a SQL string (see Phase 2)
- `Execution timed out` → simplify the query or break into smaller steps

The tool surfaces a targeted hint for the first two cases automatically.

## Args reference

- `code`: Python code to execute. Multi-line supported. Uses `exec` for
  statements, `eval` for the last expression.
