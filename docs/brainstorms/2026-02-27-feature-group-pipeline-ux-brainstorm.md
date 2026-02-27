---
title: "Rethink Feature Group Pipeline UX: FeatureFrame + PIT Joins"
type: feat
status: active
date: 2026-02-27
---

# Rethink Feature Group Pipeline UX

## What We're Building

A unified feature group experience for ML engineers writing Python pipelines. Three capabilities:

1. **`ctx.ref("feature_group.X")`** returns a `FeatureFrame` — duck-typed DataFrame-like object with `.pit_join()` support
2. **`ctx.features("entity", [...], spine=..., date_col=...)`** returns cross-FG features with optional PIT join
3. **FeatureGroupExecutor** writes intermediate parquet (fixing the current bug) and entity consolidation produces the offline store

### The Problem

Currently, to use feature groups with PIT joins in a Python pipeline, users must manually reconstruct `FeatureGroupDuckDB` objects:

```python
# Current: Too much ceremony
fg = FeatureGroupDuckDB(
    name="customer_daily_agg",
    entity=Entity(name="customer", join_keys=["customer_id"]),
    materialization=Materialization(event_time_col="order_date"),
)
fg.set_dataframe(daily_features).set_features()
fg.write()

lookup = FeatureLookup(source=fg)
hist = HistoricalFeaturesDuckDB(lookups=[lookup])
hist.using_spine(labels, date_col="label_date")
result = hist.to_dataframe_with_spine()
```

There are also critical bugs: `ctx.ref("feature_group.X")` fails because `FeatureGroupExecutor` never writes the intermediate parquet file that `ctx.ref()` looks for. Entity consolidation is broken for the same reason.

### The Ideal Experience

```python
@transform(name="training_data")
def training_data(ctx):
    labels = ctx.ref("source.churn_labels")

    # Single-FG PIT join — one line
    training = ctx.ref("feature_group.customer_features").pit_join(
        spine=labels,
        date_col="label_date",
        keep_cols=["customer_id", "churned"]
    )

    # Multi-FG entity selection with PIT join
    multi = ctx.features(
        "customer",
        ["customer_features.total_orders", "daily_agg.avg_revenue"],
        spine=labels,
        date_col="label_date",
        keep_cols=["churned"]
    )

    return training
```

## Why This Approach

### FeatureFrame (duck-typed DataFrame-like)

- **Backward compatible**: All existing code that does `df = ctx.ref("feature_group.X"); df.head()` continues to work
- **Progressive disclosure**: Users start with DataFrame operations, discover `.pit_join()` when they need it
- **No pandas subclassing**: Avoids the fragile `pd.DataFrame` inheritance. Instead, delegates `__getitem__`, `head()`, `shape`, `columns`, etc. to the inner DataFrame
- **Thin**: Stores only `entity_name`, `join_keys`, `event_time_col`, and the inner DataFrame. PIT logic is pure DuckDB SQL

### Pipeline Parquet + Entity Consolidation (not legacy FeatureGroupDuckDB.write())

- **FeatureGroupExecutor** writes `target/intermediate/feature_group_{name}.parquet` — same as every other node type
- **Entity consolidation** (already exists in `consolidation/`) reads intermediate parquets and produces `target/feature_store/{entity}/features.parquet`
- **`ctx.features()`** reads from the consolidated entity parquet — already implemented in `PipelineContext.features()`
- **`FeatureGroupDuckDB.write()`** stays for standalone/notebook usage — not used in the pipeline path

This means `@feature_group` nodes in a pipeline never touch `FeatureGroupDuckDB.write()`. The executor handles everything.

## Key Decisions

1. **FeatureFrame is duck-typed, not a pandas subclass** — delegates common methods to inner DataFrame. Passes `isinstance` checks via `__class__` override only if needed (start without it, add if tests require it)

2. **`ctx.ref("feature_group.X")` returns FeatureFrame; `ctx.ref("source.X")` and `ctx.ref("transform.X")` return plain DataFrame** — only feature_group refs get the enriched object. Detection is by node_id prefix.

3. **PIT join uses DuckDB SQL internally** — `FeatureFrame.pit_join()` opens a temporary DuckDB connection, registers spine + features, runs the ROW_NUMBER window join, returns a plain DataFrame. Same logic as `HistoricalFeaturesDuckDB.to_dataframe_with_spine()` but without the object ceremony.

4. **`ctx.features()` gains `spine` and `date_col` parameters** — extends the existing method signature. Without spine, returns latest features (current behavior). With spine, does PIT join across all selected FGs.

5. **FeatureGroupExecutor writes intermediate parquet** — this fixes `ctx.ref()`, entity consolidation, and `ctx.features()` all at once. The executor no longer calls `FeatureGroupDuckDB.write()` — it writes the intermediate parquet and creates the DuckDB view (same as transform/source executors).

6. **Entity metadata flows through intermediate storage** — the intermediate parquet for FG nodes includes a sidecar `_metadata.json` with `entity_name`, `join_keys`, `event_time_col`. This is how `ctx.ref()` knows to return FeatureFrame instead of plain DataFrame.

## Scope

### In Scope
- `FeatureFrame` class with `.pit_join()`, `.as_of()`, `.to_df()`
- Fix `FeatureGroupExecutor` to write intermediate parquet
- Extend `ctx.features()` to support `spine`/`date_col` parameters for PIT joins
- Metadata sidecar for FG intermediate parquets
- Update Chapter 3 docs to use new API
- Tests for FeatureFrame, PIT joins, ctx.features() with spine

### Out of Scope
- Online serving (FeatureGroupDuckDB.serve() stays separate)
- Spark engine support (DuckDB only)
- FeatureGroupDuckDB API changes (standalone API stays as-is)
- Multi-target materialization for feature groups

## Resolved Questions

1. **`FeatureFrame.__class__` override?** — **No, start without it.** Users call `.to_df()` when they need a real DataFrame. Add `__class__` override later only if users hit `isinstance` issues in practice (YAGNI).

2. **`.pit_join()` return type?** — **Plain DataFrame.** PIT join is a terminal operation — the result goes to sklearn/modeling. Returning DataFrame makes it clear this is final data.

3. **`ctx.features()` with PIT join and multiple FGs?** — Entity consolidation renames all event_time columns to `event_time`. Should work transparently. Verify during implementation.

## Alternatives Considered

**B: ctx.pit_join() helper** — Simpler, but less fluent. Users can't discover PIT join capability by exploring the return value of `ctx.ref()`. Rejected because the feature group reference naturally carries entity metadata.

**C: Unified ctx.features() only** — Too opinionated. Users sometimes want the raw FG DataFrame without entity-level abstraction. Rejected because it forces all FG access through the entity layer.
