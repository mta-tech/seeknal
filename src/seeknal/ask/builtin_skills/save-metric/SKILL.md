---
name: save-metric
description: "Codify an ad-hoc metric query as a permanent seeknal/metrics/{name}.yml definition reusable via query_metric"
tags: [semantic-layer, metrics, codification]
version: "1.0.0"
---

# Save Metric

Use this workflow when the user wants to codify an ad-hoc metric query as
a reusable metric definition. The tool writes a YAML file under
`seeknal/metrics/` that future `query_metric` calls can reference by name.

## Tool you will call

- `save_metric` (gated by `confirmed=True`)

## When to use

Trigger when the user says things like:

- "save this as a metric"
- "make this reusable"
- "add X to the semantic layer"
- "I want to call this by name"
- After a successful `query_metric` exploration that revealed a useful
  combination of measures the user wants to formalize.

## Phase 1 — Decide the metric type

`save_metric` supports two types:

### Simple metric
A single measure from a semantic model. Use this for direct aggregations.
Required arg: `measure` (e.g. `"total_revenue"`)

Example: a `gross_revenue` metric that wraps the `total_revenue` measure.

### Ratio metric
A ratio of two measures. Use this for rates, conversion percentages, or
any derived metric expressed as `numerator / denominator`.
Required args: `numerator`, `denominator` (both measure names)

Example: `conversion_rate = purchases / visitors`.

## Phase 2 — Preview mode

Always call `save_metric(..., confirmed=False)` FIRST to see the YAML that
will be written. The tool returns a preview block showing:

- The target path (`seeknal/metrics/{name}.yml`)
- The full YAML content
- Instructions to re-call with `confirmed=True`

Show the preview to the user verbatim so they can verify the structure
BEFORE committing.

## Phase 3 — Commit

When the user approves, call `save_metric(..., confirmed=True)`. The tool:

- Validates the metric name (`^[a-zA-Z_][a-zA-Z0-9_]*$` — Python-identifier rules)
- Validates the type (`simple` or `ratio`)
- Checks that type-specific fields are present (`measure` for simple;
  `numerator` + `denominator` for ratio)
- Rejects duplicate names (existing metric must be removed first)
- Writes to `seeknal/metrics/{name}.yml` under `fs_lock`

## Phase 4 — Confirm

After successful save, your final answer MUST include:

1. The path the metric was saved to (`seeknal/metrics/{name}.yml`)
2. An example `query_metric` call that uses the new metric
3. A note that the metric is immediately available for future sessions

## Required args reference

- `name`: Metric identifier (Python-identifier rules).
- `metric_type`: `"simple"` (default) or `"ratio"`.
- `measure`: Required if `metric_type="simple"`.
- `numerator`, `denominator`: Required if `metric_type="ratio"`.
- `description` (optional): Human-readable doc string.
- `confirmed`: Must be `True` to actually write.

## Error paths

- `Invalid metric name` → name must start with letter/underscore, alphanumeric + underscore only
- `Unsupported metric_type` → must be `simple` or `ratio`
- `Missing measure` → simple metrics need a measure
- `Missing numerator/denominator` → ratio metrics need both
- `A metric named 'X' already exists` → delete the existing YAML or pick a different name
