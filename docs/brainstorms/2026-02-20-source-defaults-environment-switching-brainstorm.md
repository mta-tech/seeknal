---
title: "Source Defaults & Environment Switching"
date: 2026-02-20
status: brainstorm
author: fitrakacamarga
---

# Source Defaults & Environment Switching

## What We're Building

A `source_defaults:` section in `profiles.yml` that provides shared infrastructure parameters (catalog_uri, warehouse, connection, host, etc.) for all source types. Users write minimal source definitions (just `source:` type and `table:` name), and the executor auto-fills infrastructure params from the profile. Environment switching happens by pointing to different profile files (`--profile profiles-dev.yml` vs `--profile profiles-prod.yml`).

## Why This Approach

### Problem

Every Iceberg source repeats `catalog_uri` and `warehouse`. Every PostgreSQL source either repeats connection params or needs `connection: local_pg`. When switching environments (dev → prod), users must change every source definition or maintain parallel files.

**Current (verbose, duplicated):**
```yaml
# seeknal/sources/events.yml
kind: source
name: events
source: iceberg
table: atlas.ns.events
params:
  catalog_uri: http://172.19.0.9:8181    # repeated in every Iceberg source
  warehouse: seeknal-warehouse            # repeated in every Iceberg source
```

```python
@source(name="orders", source="iceberg",
        table="atlas.ns.orders",
        catalog_uri="http://172.19.0.9:8181",   # repeated
        warehouse="seeknal-warehouse")            # repeated
def orders(ctx):
    return ctx.ref('source.orders')
```

**After (clean, DRY):**
```yaml
# seeknal/sources/events.yml
kind: source
name: events
source: iceberg
table: atlas.ns.events
# catalog_uri + warehouse auto-filled from profile source_defaults
```

```python
@source(name="orders", source="iceberg",
        table="atlas.ns.orders")
# catalog_uri + warehouse auto-filled from profile source_defaults
def orders(ctx):
    return ctx.ref('source.orders')
```

### Why profiles.yml (not seeknal/common/)

1. **Already supports env var interpolation** — `${VAR:default}` works out of the box
2. **Already loaded per-project** — `--profile profiles.yml` flag exists
3. **Credential handling** — ProfileLoader clears sensitive values after use
4. **Infrastructure, not logic** — catalog_uri/warehouse are infrastructure concerns, not business logic. They belong alongside `connections:` and `materialization:`, not in the project's source config.
5. **Not committed to git** — Infrastructure details stay out of the codebase

## Key Decisions

### D1: Location — `source_defaults:` section in profiles.yml

```yaml
# profiles.yml
source_defaults:
  iceberg:
    catalog_uri: http://172.19.0.9:8181
    warehouse: seeknal-warehouse
  postgresql:
    connection: local_pg          # references connections: section
  starrocks:
    connection: default           # references starrocks: section
```

**Rationale:** Consistent with existing `connections:` and `materialization:` sections. All infrastructure config in one place.

### D2: Override priority — inline params win

When a source specifies params that overlap with `source_defaults`, inline params take precedence:

```
inline params > source_defaults > env vars
```

This lets users opt out per-source while keeping the common case clean.

**Example:** A source that needs a different warehouse:
```yaml
kind: source
name: special_source
source: iceberg
table: atlas.other_ns.special
params:
  warehouse: other-warehouse    # overrides source_defaults.iceberg.warehouse
  # catalog_uri still inherited from source_defaults
```

### D3: Environment switching — separate profile files

```bash
# Development
seeknal run --profile profiles-dev.yml

# Production
seeknal run --profile profiles-prod.yml
```

**Rationale:** Simple, no new syntax. Each file has its own `source_defaults:`. The `--profile` flag already exists. Env vars still work within each file for secrets (`${PG_PASSWORD}`).

### D4: Python @source — automatic inheritance

No changes to the `@source` decorator API. The DAG builder auto-fills missing params from `source_defaults` based on the `source=` type. Zero code change for users.

### D5: Scope — all source types

`source_defaults:` supports keys for all source types: `iceberg`, `postgresql`, `starrocks`, and future backends. Each source type's defaults are a flat dict of params that get merged into the source's `params` at DAG build time.

### D6: PostgreSQL source_defaults can reference connections:

```yaml
source_defaults:
  postgresql:
    connection: local_pg    # resolved from connections: section
```

This means PostgreSQL sources don't even need `params: { connection: local_pg }` — the default connection is applied automatically.

## Implementation Sketch (High Level)

1. **ProfileLoader** — Add `load_source_defaults(source_type: str) -> dict` method
2. **DAG builder** — After parsing source node, merge `source_defaults[source_type]` under inline params
3. **Source executor** — No changes needed (receives merged params as before)
4. **Python pipeline** — No changes to decorators. DAG builder handles merging at build time.

**Touch points:**
- `src/seeknal/workflow/materialization/profile_loader.py` (new method)
- `src/seeknal/workflow/dag.py` (merge logic in `_parse_yaml_file` and `_parse_python_file`)
- Tests for new ProfileLoader method + DAG builder merge behavior

## Open Questions

_None — all key decisions resolved through brainstorming._

## Out of Scope

- Multi-environment sections within a single profiles.yml (use separate files instead)
- Materialization defaults per source type (already handled by `materialization:` section)
- Common config changes (seeknal/common/ stays as-is for param aliases and SQL snippets)
