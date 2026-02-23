# Second-Order Aggregations for Seeknal Pipelines

**Date:** 2026-01-30
**Status:** Brainstorm
**Author:** Claude Code

---

## What We're Building

Add support for **second-order aggregations** to Seeknal's YAML and Python pipelines. Second-order aggregations enable advanced feature engineering patterns like:

- **Aggregations of aggregations**: Calculate statistics on already-aggregated features (e.g., `avg(user_30d_sum)` across users, `stddev(daily_total_amount)` over time)
- **Cross-time windows**: Aggregate across different time periods (e.g., weekly patterns, monthly totals, quarter-over-quarter changes)

### Example Use Cases

```yaml
# Aggregate user-level features to region level
kind: second_order_aggregation
name: region_user_metrics
id_col: region_id
feature_date_col: date
source: aggregation.user_metrics  # Reference to first-order aggregation
features:
  total_users:
    basic: [count]
  avg_user_spend_30d:
    basic: [avg, stddev]
    source_feature: total_spend_30d
```

```yaml
# Calculate time-series patterns on daily aggregations
kind: second_order_aggregation
name: weekly_volume_patterns
id_col: merchant_id
feature_date_col: date
source: aggregation.daily_transaction_volume
features:
  weekly_total:
    window: [7, 7]  # Aggregate 7-day windows into weekly totals
    basic: [sum]
  vol_7d_vs_30d:
    ratio:
      numerator: [7, 7]
      denominator: [30, 30]
      aggs: [sum]
```

---

## Why This Approach

**Decision: New `second_order_aggregation` node type (not extending existing aggregation node)**

### Rationale

1. **Clear separation of concerns**: First-order aggregations (raw data → features) vs second-order (features → meta-features)
2. **Explicit dependency tracking**: Source aggregation must be executed first
3. **Independent configuration**: Different ID columns, date columns, window semantics
4. **Validation simplicity**: Can validate source aggregation exists and has expected schema
5. **Backwards compatibility**: Existing aggregation nodes unchanged
6. **Documentation clarity**: Users immediately understand this is a different operation

### Trade-offs

| Pro | Con |
|-----|-----|
| Clear intent and semantics | More verbose (extra node) |
| Stronger validation | More files to manage |
| Easier to test | Learning curve for new type |
| Can reference multiple sources | Slightly more complex DAG |

---

## Key Decisions

### 1. Node Type: `second_order_aggregation`

New node type distinct from `aggregation`, with its own executor.

### 2. Required Fields

```yaml
kind: second_order_aggregation
name: string                    # Node name
id_col: string                  # Entity ID column for grouping
feature_date_col: string         # Date column for time-based operations
source: string                  # Reference to upstream aggregation node (kind.name)
features: map                   # Feature specifications
```

### 3. Feature Specification Types

Support multiple aggregation patterns:

| Type | Description | Example |
|------|-------------|---------|
| `basic` | Aggregate across entities/time | `avg`, `stddev`, `min`, `max` |
| `window` | Time-window cross-aggregation | `7d` sums into `7d` blocks |
| `ratio` | Compare two windows | `(7d sum) / (30d sum)` |
| `percentile` | Rank-based aggregations | `percentile_95`, `median` |
| `delta` | Period-over-period change | `woe_week`, `mom_month` |

### 4. Source Reference Pattern

Use explicit `source` field referencing upstream aggregation:

```yaml
source: aggregation.user_daily_metrics
```

Or support multiple sources:

```yaml
sources:
  - aggregation.user_daily_metrics
  - aggregation.user_lifetime_metrics
```

### 5. Python Decorator Support

Add `@second_order_aggregation` decorator:

```python
@second_order_aggregation(
    name="region_metrics",
    source="aggregation.user_metrics",
    id_col="region_id",
    feature_date_col="date"
)
def region_metrics(ctx, df: pd.DataFrame) -> pd.DataFrame:
    # df contains pre-aggregated user_metrics
    return df.groupby("region_id").agg({
        "total_spend_30d": ["mean", "std"],
        "transaction_count_30d": "sum"
    })
```

Or with helper functions:

```python
@transform(name="region_metrics")
def region_metrics(ctx):
    user_agg = ctx.ref("aggregation.user_metrics")
    return ctx.second_order_aggregate(
        user_agg,
        id_col="region_id",
        features={
            "avg_user_spend": ctx.agg("total_spend_30d", "avg"),
            "user_count": ctx.agg("*", "count")
        }
    )
```

### 6. Output Modes

Support both materialization modes (as requested):

```yaml
materialization:
  enabled: true  # Output as feature group
  table: "warehouse.feature_groups.region_user_metrics"
```

Or leave disabled for intermediate use:

```yaml
materialization:
  enabled: false  # Used as input to other nodes only
```

---

## Open Questions

### Q1: Feature Naming Convention

**Decision: Follow existing second-order aggregator naming pattern**

The existing `SecondOrderAggregator` in `src/seeknal/tasks/duckdb/aggregators/` already has established naming:
- Basic: `{feature}_{AGG}` → `amount_SUM`, `amount_AVG`
- Days: `{feature}_{AGG}_{lower}_{upper}` → `amount_SUM_1_30`
- Ratio: `{feature}_{AGG}{l1}_{u1}_{AGG}{l2}_{u2}` → `amount_SUM1_7_SUM30_90`
- Since: `SINCE_{AGG}_{feature}_GEO_D`

**Resolution:** YAML and Python APIs will generate column names following this pattern. No changes needed.

### Q2: How to handle schema evolution when source aggregation changes?

**Options:**
1. Strict validation - fail if source schema changes
2. Lenient validation - warn but continue
3. Explicit feature mapping - map source features by name

**Recommendation:** Start with strict validation, add mapping syntax if needed.

### Q2: Should we support ad-hoc (inline) second-order aggregations in transforms?

**Option:** Allow `ctx.second_order()` in any transform:

```python
@transform(name="user_with_region_avg")
def user_with_region_avg(ctx):
    users = ctx.ref("feature_group.users")
    return ctx.with_second_order(
        users,
        group_by="region_id",
        features={"avg_spend": "avg(total_spend)"}
    )
```

**Decision:** Defer to v2 - start with dedicated node type first.

### Q3: How to handle time-based cross-window aggregations?

**Example:** Daily data → weekly aggregations

**Options:**
1. Explicit `truncate_to` parameter (week, month, quarter)
2. Window syntax like `[7, 7]` (aggregate 7-day windows)
3. Date part extraction (extract week from date, then group)

**Recommendation:** Start with option 2 (window syntax), already familiar from rolling aggregations.

### Q4: Should we support multi-level hierarchies (>2 levels)?

**Example:** user → store → region → country

**Decision:** Single hop first (one second-order node can reference one source). Users can chain nodes for deeper hierarchies.

### Q5: Edge Cases (null handling, empty groups, granularity mismatch)

**Resolution:** Already handled by existing `SecondOrderAggregator` implementation:
- Empty groups return NULL for aggregations
- Division by zero produces NULL via `NULLIF`
- Many-to-one joins handled naturally by GROUP BY
- Large datasets supported via DuckDB (or Spark fallback)

**Action:** Reuse existing validation and execution logic - no new edge case handling needed in v1.

---

## Implementation Sketch

### YAML Schema

```yaml
kind: second_order_aggregation
name: region_user_features
description: "Aggregate user features to region level"
owner: "data-science"
id_col: region_id
feature_date_col: date
source: aggregation.user_daily_features
features:
  # Basic aggregations of aggregated features
  total_users:
    basic: [count]
  avg_user_spend_30d:
    basic: [mean, stddev, median]
    source_feature: total_spend_30d
  max_user_transactions_30d:
    basic: [max]
    source_feature: transaction_count_30d

  # Cross-time window aggregations
  weekly_total_volume:
    window: [7, 7]
    basic: [sum]
    source_feature: daily_volume

  # Ratio aggregations
  recent_vs_historical_ratio:
    ratio:
      numerator: [1, 7]
      denominator: [8, 30]
      aggs: [sum]
    source_feature: transaction_amount

inputs:
  - ref: aggregation.user_daily_features
tags: [second-order, feature-engineering]
```

### Executor Flow

```
1. Parse YAML - validate schema
2. Resolve source - load upstream aggregation output
3. Validate source schema - check features exist
4. Build SQL/Pandas aggregation query
5. Execute with DuckDB (or Spark for large data)
6. Return DataFrame (and materialize if enabled)
```

### Python API

```python
from seeknal.pipeline import second_order_aggregation, PipelineContext

@second_order_aggregation(
    name="region_metrics",
    source="aggregation.user_metrics",
    id_col="region_id",
    feature_date_col="date"
)
def region_metrics(ctx: PipelineContext, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate user-level metrics to region level.

    Args:
        ctx: Pipeline context
        df: Pre-aggregated user metrics DataFrame

    Returns:
        Region-level aggregated DataFrame
    """
    return df.groupby("region_id").agg({
        "total_spend_30d": ["mean", "std", "median"],
        "transaction_count": ["sum", "count"],
        "last_active_date": "max"
    })
```

---

## Success Criteria

1. **YAML Support**: Define and execute second-order aggregations via YAML
2. **Python Support**: `@second_order_aggregation` decorator works end-to-end
3. **Source Validation**: Fails fast if upstream aggregation doesn't exist or has wrong schema
4. **Materialization**: Can output to feature groups or use as intermediate
5. **Documentation**: Tutorial examples for both YAML and Python
6. **Tests**: Unit tests for executor, integration tests for full pipeline

---

## Related Work

- **Existing**: `src/seeknal/tasks/*/aggregators/second_order_aggregator.py` (Spark/DuckDB implementations)
- **Related**: `aggregation` node type (first-order only)
- **Pattern**: Similar to how `transform` nodes can reference `source` nodes

---

## Next Steps

1. ✅ Brainstorm complete
2. ⏳ Create implementation plan (`/workflows:plan`)
3. ⏳ Implement YAML schema + executor
4. ⏳ Implement Python decorator
5. ⏳ Add tests
6. ⏳ Write documentation

---

*This brainstorm document captures the WHAT. See implementation plan for HOW.*
