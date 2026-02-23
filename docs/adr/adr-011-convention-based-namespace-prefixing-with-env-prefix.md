---
adr_id: ADR-011
date: 2026-02-21
status: accepted
title: Convention-based namespace prefixing with {env}_ prefix in MaterializationDispatcher
---

# ADR-011: Convention-based namespace prefixing with {env}_ prefix in MaterializationDispatcher

## Context

When running a pipeline in a non-production environment (dev, staging) without a per-environment profile, materialization targets would write to the same production schemas and Iceberg namespaces as a production run, causing data pollution. A convention-based isolation mechanism is needed that works without requiring separate per-env credentials or full profile configuration.

## Decision

`MaterializationDispatcher.dispatch()` accepts an optional `env_name` parameter. When set, `_prefix_target()` is called per target before routing to the PostgreSQL or Iceberg helper. For PostgreSQL, `schema.table` becomes `{env}_schema.table`. For Iceberg, `catalog.ns.table` becomes `catalog.{env}_ns.table`. The prefix applies only to the schema or namespace component — not the catalog name or table name. The original target dict is never mutated; `_prefix_target()` returns a copy.

**Rationale:**
- Prefixing the schema/namespace (not the table name) keeps table names consistent for lineage tracking while achieving full write isolation between environments
- Convention-based prefixing works without a per-env profile — it is the default isolation mechanism for any `--env` run
- Schema/namespace auto-creation is triggered automatically before each write, so no manual setup is required
- Copy-on-write (`dict(target)`) prevents subtle bugs from dict mutation being visible across multiple dispatch calls

**Consequences:**
- A dev run of `analytics.orders` writes to `dev_analytics.orders` — completely isolated from production
- No per-env profile is needed for basic schema isolation; a per-env profile additionally provides source credential isolation
- Alphabetical schema listing groups env schemas together (prefix rather than suffix), making them easy to identify in a catalog browser
- The table name component is preserved exactly, maintaining lineage graph consistency across environments

## Alternatives Considered

- Require a per-env profile for any `--env` run: rejected, too much configuration burden for quick dev testing and iteration
- Prefix the table name instead of the schema: rejected, table names must stay consistent across environments for lineage tracking to work correctly
- Use a suffix instead of a prefix: rejected, a prefix causes env schemas to sort together alphabetically in catalog UIs, making them easier to identify and clean up

## Related

- Spec: specs/2026-02-21-feat-project-aware-repl-plan-feat-env-aware-iceberg-postgresql-plan-merged.md
- Tasks: task-7-env, task-10-env
- Related ADRs: ADR-006
