# Brainstorm: Environment-Aware Iceberg & PostgreSQL in `seeknal env`

**Date:** 2026-02-21
**Status:** Draft
**Author:** Claude + Fitra

## What We're Building

Make `seeknal env` fully environment-aware for Iceberg and PostgreSQL sources and materialization targets, so that each virtual environment (dev, staging, prod) can:

1. **Read from env-specific sources** — dev env reads from dev databases
2. **Write to env-specific targets** — dev env materializes to dev schemas/namespaces
3. **Promote with re-materialization** — promoting from dev to prod triggers fresh writes to production targets

Currently, `seeknal env` provides excellent local cache isolation (plan/apply/promote with zero-copy refs), but external targets (PostgreSQL tables, Iceberg tables) are always written to the same production locations regardless of which env is active.

## Why This Approach

**Per-env profile files** (`profiles-{env}.yml`) because:

- Simple mental model: one file per environment
- Full flexibility: different connections, different source_defaults, different namespaces
- Convention-based fallback: auto-prefix schema/namespace with env name when no per-env profile exists
- Composable: teams can template profiles for different infrastructure

**Rejected alternatives:**

- Single profile with env sections — config file gets unwieldy
- Convention-only — can't switch source connections, too limited

## Key Decisions

### 1. Profile Resolution

When running `seeknal run --env dev`:
1. Check for `profiles-dev.yml` in project root
2. If not found, check `~/.seeknal/profiles-dev.yml`
3. If not found, use default `profiles.yml` with convention-based namespacing
4. `--profile` flag always overrides (highest priority)

### 2. Convention-Based Namespacing (Fallback)

When no per-env profile exists, auto-prefix targets with env name:
- PostgreSQL: `analytics.table` → `dev_analytics.table` (prefix schema)
- Iceberg: `atlas.namespace.table` → `atlas.dev_namespace.table` (prefix namespace)
- Schema/namespace auto-created if not exists

### 3. Source Isolation

Sources switch per env via profile connections:
- `profiles-dev.yml` has `connections.local_pg` pointing to dev database
- `profiles-prod.yml` has `connections.local_pg` pointing to prod database
- Same source YAML, different actual databases

### 4. Promotion Re-materialization

`seeknal env promote dev` (to prod):
1. Copies intermediate cache and state (existing behavior)
2. **NEW:** Re-runs materialization against production targets using production profile
3. Validates row counts match between env and prod outputs

### 5. Profile Path Propagation Fix

The dispatcher currently ignores `--profile` flag. Fix:
- Thread `profile_path` through `ExecutionContext` → `MaterializationDispatcher`
- Both PostgreSQL and Iceberg backends respect the active profile

## Scope

### In Scope
- Per-env profile auto-discovery (`profiles-{env}.yml`)
- Convention-based namespace prefixing as fallback
- Profile path propagation to MaterializationDispatcher
- Promotion with re-materialization to prod targets
- Source reading respects env profile connections

### Out of Scope
- Per-env common config (`seeknal/common/` stays shared)
- Env-specific Python pipeline code
- Multi-cluster Iceberg (single Lakekeeper assumed)

## Open Questions

(None — all resolved during brainstorming)

## Resolved Questions

1. **Core problem?** → Profile mismatch + broader target isolation needed
2. **Target isolation strategy?** → Convention-based prefix + profile override
3. **Naming convention?** → Prefix schema/namespace (e.g., `dev_analytics`)
4. **Promotion behavior?** → Re-materialize to production targets
5. **Source isolation?** → Yes, sources switch per env via profile connections
6. **Approach?** → Per-env profile files with convention-based fallback
