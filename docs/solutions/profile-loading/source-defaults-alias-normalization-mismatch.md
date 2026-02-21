---
category: profile-loading
component: profile-loader
tags: [profile-loader, source-defaults, alias, normalization, postgres, postgresql]
date_resolved: 2026-02-21
related_build: project-aware-repl-env-aware-execution
related_tasks: [task-5-env, qa-spec-interpreter, known-bugs-memory]
---

# source_defaults Alias Normalization Mismatch: postgres vs postgresql Key

## Problem Symptom

A pipeline with PostgreSQL sources received no connection credentials despite a correct `source_defaults` block in `profiles.yml`. The source node ran without error but used no injected defaults — silent no-op.

**Impact:** Source nodes silently received no defaults. Downstream failures (missing `host`, missing `connection`) were confusing because no error was raised at the merge step.

## Root Cause

`ProfileLoader.load_source_defaults()` normalizes the `source_type` argument before lookup (e.g., `postgres` → `postgresql`). However, the keys in the `source_defaults` YAML dict itself are **not** normalized at load time. When a user writes `postgres:` as the key in `profiles.yml`, the normalized lookup for `postgresql` finds nothing and returns an empty dict — no error, no warning.

```yaml
# profiles.yml — user wrote 'postgres' as key
source_defaults:
  postgres:           # <-- not canonical
    host: localhost
    port: 5432
```

```python
# ProfileLoader.load_source_defaults() normalizes the argument...
source_type = "postgresql"  # normalized from "postgres"
# ...but not the dict keys
defaults = self._source_defaults.get("postgresql", {})  # finds nothing!
```

## Working Solution

In `_merge_source_defaults()` (or `load_source_defaults()`), try both the canonical key and known aliases when looking up the defaults dict:

```python
# In ProfileLoader.load_source_defaults() or DAGBuilder._merge_source_defaults()
ALIASES = {
    "postgresql": ["postgresql", "postgres"],
    "mysql": ["mysql", "mariadb"],
}

def _get_source_defaults(self, source_type: str) -> dict:
    canonical = normalize_source_type(source_type)  # postgres -> postgresql
    for key in ALIASES.get(canonical, [canonical]):
        if key in self._source_defaults:
            return self._source_defaults[key]
    return {}
```

Alternatively, normalize all keys at YAML load time so the in-memory dict always uses canonical names.

**Result:** After fix, source nodes with `source: postgres` or `source: postgresql` both correctly receive defaults from either a `postgres:` or `postgresql:` key in `profiles.yml`.

## Prevention Strategies

1. Always normalize both the lookup key AND the dict keys in any alias-aware resolution — asymmetric normalization produces silent failures
2. Add a QA test that writes `postgres:` (non-canonical) as the key in `profiles.yml` and asserts defaults are applied to a `postgresql` source node
3. Document that both `postgres` and `postgresql` are valid keys in `source_defaults` to reduce user confusion

## Test Cases Added

- `test_postgres_alias_resolves` (`tests/workflow/test_dag_source_defaults.py`)

## Cross-References

- Spec: specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md
- Related: docs/solutions/source-config/source-defaults-table-query-conflict.md
- `src/seeknal/workflow/profile_loader.py:478-481` — `load_source_defaults()` normalization gap
- `src/seeknal/workflow/dag.py` — `_merge_source_defaults()` call site
