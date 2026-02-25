# Python SOA with Built-in Aggregation Engine

**Date:** 2026-02-25
**Status:** Complete
**Type:** Feature

## What We're Building

A Python `@second_order_aggregation` decorator that uses the **built-in SOA engine** (`SecondOrderAggregator`) — the same engine the YAML SOA path uses. Users declare a `features=` dict on the decorator instead of writing manual pandas/DuckDB aggregation code.

**Python version:**
```python
@second_order_aggregation(
    name="region_metrics",
    id_col="region",
    feature_date_col="order_date",
    application_date_col="application_date",
    features={
        "daily_amount": {"basic": ["sum", "mean", "max", "stddev"]},
        "daily_count": {"basic": ["sum", "mean"]},
    },
)
def region_metrics(ctx):
    return ctx.ref("transform.customer_daily_agg")
```

**Multi-source example:**
```python
@second_order_aggregation(
    name="region_metrics",
    id_col="region",
    feature_date_col="order_date",
    application_date_col="application_date",
    features={
        "daily_amount": {"basic": ["sum", "mean", "max", "stddev"]},
    },
)
def region_metrics(ctx):
    txn = ctx.ref("transform.customer_daily_agg")
    labels = ctx.ref("source.region_labels")
    return txn.merge(labels, on="region")
```

**Equivalent YAML version:**
```yaml
kind: second_order_aggregation
name: region_metrics
id_col: region
feature_date_col: order_date
application_date_col: application_date
source: transform.customer_daily_agg
features:
  daily_amount:
    basic: [sum, mean, max, stddev]
  daily_count:
    basic: [sum, mean]
```

## Why This Approach

The YAML SOA engine (`SecondOrderAggregator`) already supports basic, window, ratio, and since aggregations with optimized DuckDB SQL generation. The Python decorator should tap into this same engine rather than requiring users to reimplement aggregation logic manually.

Benefits:
- **Consistency**: Same engine, same output column naming (`{feature}_{AGG}`), same capabilities
- **Python expressiveness**: PEP 723 deps, programmatic feature specs, pre-processing hooks
- **DRY**: No duplicate aggregation logic between YAML and Python paths

## Key Decisions

### 1. Function body loads data via `ctx.ref()`
The function body is **required** — it loads source data using `ctx.ref()` (supports multiple sources), optionally joins/filters/reshapes, and returns the DataFrame for the SOA engine. No `source=` parameter on the decorator.

Data flow:
```
function body: ctx.ref() loads + joins multiple sources
    ↓
returned DataFrame
    ↓
built-in SOA engine (SecondOrderAggregator)
    ↓
output (parquet + DuckDB view)
```

Dependencies are discovered via `ctx.ref()` AST extraction — same mechanism as `@transform`.

**Rationale:** Consistent with `@transform` pattern. Multiple source support via `ctx.ref()` is more flexible than a single `source=` parameter. Users can join, filter, and enrich data before the engine runs.

### 2. `features=` always required
The `features` parameter is mandatory on the decorator. No dual mode — the old path where the function body does all aggregation manually is removed.

**Rationale:** Simpler implementation, one code path. The built-in engine already covers basic, window, ratio, since — no need for a manual fallback.

### 3. Same `features` dict structure as YAML
The Python `features=` dict uses the exact same structure as the YAML `features:` block. No translation layer needed — the existing `_build_aggregation_specs()` in the executor works unchanged.

```python
features={
    "daily_amount": {"basic": ["sum", "mean", "max", "stddev"]},
    "recent_spending": {
        "window": [1, 7],
        "basic": ["sum"],
        "source_feature": "daily_amount",
    },
    "spending_trend": {
        "ratio": {"numerator": [1, 7], "denominator": [8, 14], "aggs": ["sum"]},
        "source_feature": "daily_amount",
    },
}
```

## Architectural Changes Required

### Files to modify:

1. **`src/seeknal/pipeline/decorators.py`** — Add `features` (required) parameter to `@second_order_aggregation`. Remove `source` parameter (dependencies come from `ctx.ref()`). Function signature changes from `(ctx, df)` to `(ctx)` — same as `@transform`. Store `features` in metadata.

2. **`src/seeknal/workflow/dag.py` `_parse_python_file()`** — Add `kind_str == "second_order_aggregation"` branch that copies `id_col`, `feature_date_col`, `application_date_col`, `features` from `node_meta` into `yaml_data`. Merge AST-extracted `ctx.ref()` dependencies into `inputs` (same as `@transform` branch).

3. **`src/seeknal/workflow/executors/__init__.py` `get_executor()`** — Two-phase execution for Python SOA: first run function via `PythonExecutor` to get the pre-processed DataFrame (parquet), then run `SecondOrderAggregationExecutor` on that result. OR: extend `SecondOrderAggregationExecutor` to handle Python SOA nodes by running the function in-process first, then feeding the result to the engine.

4. **`src/seeknal/workflow/dry_run.py` `extract_decorators()`** — Add `"second_order_aggregation"` to the recognized decorator names list.

5. **`src/seeknal/pipeline/discoverer.py` `RefVisitor`** — Add `"second_order_aggregation"` to the decorator skip list.

## Resolved Questions

- **Function body required?** — Yes. Function loads data via `ctx.ref()` and returns DataFrame for the engine. Supports multiple sources.
- **`source=` parameter?** — Removed. Dependencies discovered via `ctx.ref()` AST extraction (same as `@transform`).
- **Function signature?** — `def fn(ctx)` — same as `@transform`. No `df` parameter. User loads data themselves.
- **Backward compatibility?** — No. `features=` is always required. Single code path through built-in engine.
- **Features dict format?** — Same structure as YAML `features:` block. Reuses existing `_build_aggregation_specs()`.
