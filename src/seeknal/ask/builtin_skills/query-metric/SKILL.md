---
name: query-metric
description: "Query business metrics through the semantic layer with automatic joins, time grain resolution, and alias resolution"
tags: [semantic-layer, metrics, query]
version: "1.0.0"
---

# Query Metric

Use this workflow when the user asks a question that can be answered by a
metric defined in the semantic layer (`seeknal/semantic_models/*.yml` or
`seeknal/metrics/*.yml`). Prefer this over `execute_sql` because the compiler
handles aggregation, joins, and time grains automatically — no manual SQL.

## Tool you will call

- `query_metric` — compiles + executes a semantic layer query

## When to use

Use `query_metric` INSTEAD of `execute_sql` when:

- The question maps to a named metric (e.g. "total revenue", "active users")
- The aggregation involves time grains (daily / weekly / monthly)
- Multiple metrics need to be co-queried with the same dimensions
- The user wants a metric by its alias rather than its canonical name

Fall back to `execute_sql` when the question requires one-off analytical SQL
with no corresponding metric definition.

## Phase 1 — Discover available metrics

Before calling `query_metric`, confirm what's defined. The semantic layer is
discovered by scanning:

- `seeknal/semantic_models/*.yml` — entities, dimensions, measures
- `seeknal/metrics/*.yml` — named metric definitions (simple or ratio)

Use `search_project_files` or `read_project_file` to inspect them. If nothing
exists yet, point the user at the `bootstrap-semantic-model` skill.

## Phase 2 — Call the tool

Required args:

- `metrics`: comma-separated metric names (e.g. `"total_revenue,order_count"`).
  Aliases are resolved case-insensitively, so `"Total Revenue"` works too.
- `dimensions` (optional): comma-separated group-by columns. Time dimensions
  use the `__grain` suffix — `ordered_at__month`, `created_at__day`,
  `signup_date__quarter`. Valid grains: `day`, `week`, `month`, `quarter`,
  `year`.
- `filters` (optional): comma-separated SQL filter expressions. Each filter
  is a standalone predicate — `"region = 'US'"`, `"order_date >= '2025-01-01'"`.
  Multiple filters are AND-ed.
- `order_by` (optional): comma-separated order columns. Prefix `-` for DESC
  — `"-total_revenue"`.
- `limit` (optional, default 100): maximum rows.

## Phase 3 — Interpret the result

The tool returns:

1. A metadata header listing the resolved metrics / dimensions / filters
2. The compiled SQL (always shown — this is intentional for agent debugging)
3. A markdown table with the result rows and a row count

The compiled SQL is useful for:

- Verifying the aggregation is what you expected
- Copy-pasting into `execute_sql` for one-off variations
- Explaining the query back to the user

## Error paths

- `No semantic models found` → point user at `bootstrap-semantic-model` skill
- `Unknown metric` → the tool lists available metrics + aliases; suggest
  the closest match
- `Compilation error` → the MetricCompiler rejected the query structure;
  usually a dimension/filter that references a column not in the model
- `Query execution error` → DuckDB couldn't run the compiled SQL; usually
  means the underlying view isn't registered (user needs to run a pipeline
  first to materialize the intermediate parquets)

## Final answer requirements

Your final answer MUST:

1. State the question the metric answered
2. Cite SPECIFIC numbers from the result (not "revenue was high")
3. If the result was empty, explain WHY (filter too strict, no data in range)
4. Optionally surface 1-2 derived insights the user can act on
