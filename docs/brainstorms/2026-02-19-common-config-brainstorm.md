---
date: 2026-02-19
topic: common-config
---

# Common Configuration Layer for Seeknal Pipelines

## What We're Building

A shared configuration layer in `seeknal/common/` that provides source registries with column mappings, reusable SQL rules, reusable transformations, and a priority cascade. This is the evolution of the `common.yml` concept from the [Eureka AI Feature Engine](~/project/eureka/ai_feature_engine/conf/common.yml), adapted for Seeknal's YAML + SQL pipeline model.

**Core idea:** Define sources, column aliases, and rules ONCE in `seeknal/common/`, then reference them with `{{ }}` syntax across all transforms and nodes.

**Before (every transform repeats column names):**
```yaml
kind: transform
name: daily_calls
inputs:
  - ref: source.traffic
transform: |
  SELECT date_id, msisdn, COUNT(*) as call_count
  FROM ref('source.traffic')
  WHERE service_type = 'Voice'
  GROUP BY date_id, msisdn
```

**After (common config drives consistency):**
```yaml
kind: transform
name: daily_calls
inputs:
  - ref: source.traffic
transform: |
  SELECT {{traffic.dateCol}}, {{traffic.idCol}}, COUNT(*) as call_count
  FROM ref('source.traffic')
  WHERE {{rules.callExpression}}
  GROUP BY {{traffic.dateCol}}, {{traffic.idCol}}
```

## Why This Approach

### From Feature Engine to Seeknal

The Feature Engine (`ai_feature_engine`) already proved this pattern works at scale with hundreds of feature configs sharing one `common.yml`. Seeknal, as its evolution, adopts the same concept but adapts it:

| Aspect | Feature Engine | Seeknal |
|--------|---------------|---------|
| Config format | Single `common.yml` | Modular `seeknal/common/` directory |
| SQL generation | Java code from dimensions | User-written SQL with `{{ }}` placeholders |
| Input reference | `pipeline.input.id: traffic_day` | `ref('source.traffic')` + `{{traffic.dateCol}}` |
| Rules | Java-integrated filter expressions | SQL snippets injected via `{{ }}` |
| Transformations | Java class references | SQL snippets or node references |

### Template Syntax Consistency

Seeknal already uses `{{ }}` for parameterization (dates, env vars, runtime context). Adding source params and rules to the same template system means ONE resolution pipeline, ZERO new syntax to learn.

## Key Decisions

### 1. Directory structure: Modular files in `seeknal/common/`

```
seeknal/common/
  sources.yml       # Source registry with aliases + column mappings
  rules.yml         # Reusable SQL filter expressions
  transformations.yml  # Reusable SQL snippets (optional)
```

Each file is optional. Users can start with just `sources.yml` and add more as needed.

### 2. Source registry with column mappings (params)

```yaml
# seeknal/common/sources.yml
sources:
  - id: traffic
    ref: source.traffic_daily
    params:
      dateCol: date_id
      idCol: msisdn
      entityCol: site_id
      hitCol: hit
      durationCol: duration

  - id: reload
    ref: source.reload_daily
    params:
      dateCol: date_id
      idCol: msisdn
      reloadAmountCol: reload_total

  - id: subscriber
    ref: source.subscriber_daily
    params:
      dateCol: date_id
      idCol: msisdn
      deviceOsCol: handset_os
      subTypeCol: sub_type
```

**Usage in transform SQL:** `{{traffic.dateCol}}` resolves to `date_id`

**Why `id` instead of `ref` as key:** Multiple common configs might point to the same source with different column mappings (e.g., same table used as `traffic` and `dpi_traffic` with different focus columns).

### 3. Reusable SQL rules

```yaml
# seeknal/common/rules.yml
rules:
  - id: callExpression
    value: "service_type = 'Voice'"
  - id: smsExpression
    value: "service_type = 'Sms'"
  - id: dataExpression
    value: "service_type = 'Data'"
  - id: activeSubscriber
    value: "life_cycle IN ('ACTIVE', 'GRACE', 'EXPIRED')"
  - id: weekdayOnly
    value: "EXTRACT(DOW FROM date_id) BETWEEN 1 AND 5"
```

**Usage in transform SQL:** `{{rules.callExpression}}` resolves to `service_type = 'Voice'`

### 4. Reusable SQL transformations (optional)

```yaml
# seeknal/common/transformations.yml
transformations:
  - id: enrichWithRegion
    sql: |
      LEFT JOIN ref('source.regions') r ON {{traffic.entityCol}} = r.site_id
  - id: filterActive
    sql: |
      WHERE {{rules.activeSubscriber}}
```

**Usage in transform SQL:** `{{transforms.enrichWithRegion}}` resolves to the SQL snippet.

### 5. Priority cascade

Resolution order (highest priority wins):
1. **Explicit in node** - params defined directly in the YAML node
2. **Node-level params** - `params:` section of the node
3. **Common sources params** - from `seeknal/common/sources.yml`
4. **Common rules** - from `seeknal/common/rules.yml`
5. **Code defaults** - built-in date functions, env vars, etc.

This matches the Feature Engine's cascade: `explicit > input_config_params > rule > default`

### 6. Template syntax: `{{ }}` (same as existing parameterization)

**Source column params:** `{{source_alias.paramName}}`
- `{{traffic.dateCol}}` -> `date_id`
- `{{subscriber.idCol}}` -> `msisdn`

**Rules:** `{{rules.ruleId}}`
- `{{rules.callExpression}}` -> `service_type = 'Voice'`

**Transformations:** `{{transforms.transformId}}`
- `{{transforms.enrichWithRegion}}` -> the SQL snippet

**Existing params still work:** `{{today}}`, `{{env:VAR}}`, `{{run_id}}`

### 7. Backward compatible: 100% opt-in

- Projects without `seeknal/common/` work exactly as before
- No changes to existing YAML syntax
- `{{ }}` placeholders only resolved when they match known patterns

## Implementation Scope

### Files to create:
1. `src/seeknal/workflow/common/` - New module
   - `__init__.py` - `CommonConfig` loader
   - `loader.py` - YAML loading + validation
   - `models.py` - Pydantic models for sources, rules, transformations

### Files to modify:
2. `src/seeknal/workflow/parameters/resolver.py` - Extend `ParameterResolver` to load common config and resolve `{{source.param}}` and `{{rules.id}}` patterns
3. `src/seeknal/workflow/dag.py` - Load common config at DAG build time
4. `src/seeknal/workflow/executors/transform_executor.py` - Resolve `{{ }}` in SQL before execution
5. `src/seeknal/workflow/executor.py` - Resolve `{{ }}` in dry-run preview

### Docs:
6. `docs/building-blocks/common-config.md` - New doc page
7. `docs/reference/yaml-schema.md` - Update with common config schema

## Example: Complete Workflow

**Project structure:**
```
my-project/
  seeknal/
    common/
      sources.yml
      rules.yml
    sources/
      traffic_daily.yml
    transforms/
      daily_calls.yml
      daily_sms.yml
```

**seeknal/common/sources.yml:**
```yaml
sources:
  - id: traffic
    ref: source.traffic_daily
    params:
      dateCol: date_id
      idCol: msisdn
      durationCol: duration
```

**seeknal/common/rules.yml:**
```yaml
rules:
  - id: callExpression
    value: "service_type = 'Voice'"
  - id: smsExpression
    value: "service_type = 'Sms'"
```

**seeknal/transforms/daily_calls.yml:**
```yaml
kind: transform
name: daily_calls
inputs:
  - ref: source.traffic_daily
transform: |
  SELECT
    {{traffic.dateCol}},
    {{traffic.idCol}},
    COUNT(*) as call_count,
    SUM({{traffic.durationCol}}) as total_duration
  FROM ref('source.traffic_daily')
  WHERE {{rules.callExpression}}
  GROUP BY {{traffic.dateCol}}, {{traffic.idCol}}
```

**After resolution, the SQL becomes:**
```sql
SELECT
  date_id,
  msisdn,
  COUNT(*) as call_count,
  SUM(duration) as total_duration
FROM input_0
WHERE service_type = 'Voice'
GROUP BY date_id, msisdn
```

## Interaction with `ref()` Syntax

The `ref()` named input syntax (brainstormed separately) and common config are complementary:

- `ref('source.traffic_daily')` -> resolves to DuckDB view name (runtime)
- `{{traffic.dateCol}}` -> resolves to column name `date_id` (template time)

Resolution order: `{{ }}` templates resolved first, then `ref()` resolved at execution time.

## Open Questions

1. **Validation**: Should we warn when a `{{ }}` placeholder doesn't match any known source, rule, or parameter? -> Yes, fail fast with clear error.

2. **Circular references**: Should rules reference other rules? (e.g., `{{rules.weekdayCall}}` that uses `{{rules.callExpression}} AND {{rules.weekdayOnly}}`) -> Suggest NO for now, keep it flat.

3. **`seeknal init` scaffold**: Should `seeknal init` create a `seeknal/common/` skeleton? -> Yes, with commented examples.

## Next Steps

-> Plan and implement. Suggested order:
1. First: `ref()` named input syntax (smaller scope, already brainstormed)
2. Then: Common config layer (builds on parameter resolution, needs `ref()` first)
