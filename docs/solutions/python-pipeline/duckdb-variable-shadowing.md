---
category: python-pipeline
component: PipelineContext, DuckDB
tags: [duckdb, python-pipeline, decorators, variable-scoping]
date_resolved: 2026-02-20
related_build: named-refs-common-config
---

# DuckDB Variable Shadowing in Python Pipelines

## Problem Symptom

When running a Python pipeline transform, DuckDB throws:

```
_duckdb.InvalidInputException: Invalid Input Error: Python Object "subscriber_daily"
of type "function" found on line "...medallion.py:108" not suitable for replacement scans.
Make sure that "subscriber_daily" is either a pandas.DataFrame, duckdb.DuckDBPyRelation, ...
```

## Investigation Steps

1. The runner script calls `func(ctx)` which invokes the decorated function
2. Inside the function, `ctx.ref("source.subscriber_daily")` loads data but isn't assigned
3. The SQL `FROM subscriber_daily` causes DuckDB to scan for a Python variable named `subscriber_daily`
4. DuckDB finds the module-level `subscriber_daily` **function** (the `@source` decorated function) instead of a DataFrame

## Root Cause

DuckDB's replacement scan feature automatically resolves unquoted identifiers in SQL to Python local variables. When `ctx.ref()` is called without assigning the result to a local variable, DuckDB searches up the scope chain and finds the decorated function from the module scope.

```python
# BAD - ctx.ref() not assigned, DuckDB finds the function
@transform(name="subscriber_profile", inputs=["source.subscriber_daily"])
def subscriber_profile(ctx):
    ctx.ref("source.subscriber_daily")  # Loads data but doesn't assign!
    return ctx.duckdb.sql("SELECT * FROM subscriber_daily").df()  # Finds FUNCTION

# GOOD - assigned to local variable, DuckDB finds the DataFrame
@transform(name="subscriber_profile", inputs=["source.subscriber_daily"])
def subscriber_profile(ctx):
    sd = ctx.ref("source.subscriber_daily")  # Assigned to 'sd'
    return ctx.duckdb.sql("SELECT * FROM sd").df()  # Finds DataFrame
```

## Working Solution

Always assign `ctx.ref()` to a local variable that matches the SQL `FROM` reference:

```python
# Single input
def my_transform(ctx):
    df = ctx.ref("source.my_source")
    return ctx.duckdb.sql("SELECT * FROM df WHERE active").df()

# Multiple inputs
def enriched(ctx):
    t = ctx.ref("source.traffic")
    c = ctx.ref("source.cell_sites")
    return ctx.duckdb.sql("SELECT t.*, c.region FROM t JOIN c ON t.cell_id = c.cell_id").df()
```

**Important:** Avoid SQL reserved keywords as variable names. `at`, `in`, `on`, `as`, `by`, `to` will cause SQL parse errors.

## Prevention Strategies

1. Always assign `ctx.ref()` return value to a local variable
2. Use short, non-reserved names: `t`, `s`, `c`, `df`, `tr`, `sd`
3. The variable name must match the SQL `FROM`/`JOIN` reference exactly
4. Test transforms locally with `python target/_runner_<name>.py` to catch these errors early

## Cross-References

- `src/seeknal/pipeline/context.py` — PipelineContext.ref() implementation
- `src/seeknal/workflow/executors/python_executor.py` — Runner script generation
