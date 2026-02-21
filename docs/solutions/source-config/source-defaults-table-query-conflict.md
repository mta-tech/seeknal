---
category: source-config
component: source-executor
tags: [source-defaults, source-executor, pushdown-query, table, mutual-exclusivity, dag-builder]
date_resolved: 2026-02-21
related_build: project-aware-repl-env-aware-execution
related_tasks: [known-bugs-memory, qa-spec-interpreter]
---

# Source Executor Rejects table + query When table Comes From source_defaults

## Problem Symptom

A source node with an explicit inline `query:` param (pushdown query) failed with a validation error even though no `table:` was written in the node's YAML:

**Error:** `Cannot specify both 'table' and 'query' for a source node`

The `table` field was injected automatically from `source_defaults`, not from the user's node config.

## Root Cause

`_merge_source_defaults()` merged all default fields unconditionally into node params before source executor validation ran. When `source_defaults` for a connection included a `table` field (intended as a fallback), it was injected even when the node already had an explicit `query:` param. The source executor then correctly rejected the combination — but the error was misleading because the `table` came from defaults, not from the user.

```yaml
# profiles.yml — table in source_defaults
source_defaults:
  postgresql:
    host: localhost
    table: public.raw_events  # injected as fallback

# pipeline YAML — user intended to override with a pushdown query
sources:
  - name: filtered_events
    source: postgresql
    params:
      query: "SELECT * FROM public.raw_events WHERE region = 'us-east'"
      # No 'table:' here — but source_defaults injects one anyway
```

The merge produced `params = {table: "public.raw_events", query: "SELECT ..."}` which the validator correctly rejected — but for the wrong reason from the user's perspective.

## Working Solution

In `_merge_source_defaults()`, skip any default key that is already present in the node's explicit params. Defaults fill missing fields only — they never overwrite or conflict with explicit user intent:

```python
def _merge_source_defaults(self, node_yaml: dict, defaults: dict) -> None:
    existing_params = node_yaml.get("params") or {}

    # Before (broken): unconditional merge could inject conflicting keys
    # merged = {**defaults, **existing_params}  # 'table' still present from defaults

    # After (fixed): only inject keys not already set by the user
    for key, value in defaults.items():
        if key not in existing_params:
            existing_params[key] = value

    node_yaml["params"] = existing_params
```

Additionally, apply conflict-awareness: if `query` is present in existing params, explicitly skip injecting `table` from defaults regardless of the iteration order.

**Result:** Source nodes with explicit pushdown queries receive connection credentials (`host`, `port`, `user`, `password`) from defaults but do not receive a conflicting `table` param.

## Prevention Strategies

1. Treat `source_defaults` as additive-only: defaults fill missing fields, never overwrite explicit ones
2. Add an explicit test: source node with `query:` inline + `table:` in `source_defaults` should execute using only the query
3. When two params are mutually exclusive in validation (like `table` and `query`), the merge logic must be aware of that exclusivity and skip the conflicting default

## Test Cases Added

- `test_pushdown_query_table_mutual_exclusivity`

## Cross-References

- Spec: specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md
- Related: docs/solutions/profile-loading/source-defaults-alias-normalization-mismatch.md
- `src/seeknal/workflow/dag.py` — `_merge_source_defaults()` merge logic
- `src/seeknal/workflow/executors/source_executor.py` — mutual exclusivity validation
