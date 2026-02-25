---
summary: Manage virtual environments for safe pipeline development
read_when: You want to test changes in isolation before promoting to production
related:
  - plan
  - run
  - promote
---

# seeknal env

Manage virtual environments for safe pipeline development. Virtual environments
allow you to plan, test, and execute changes in isolation before promoting them
to production.

## Synopsis

```bash
seeknal env [COMMAND] [OPTIONS]
```

## Description

The `env` command group provides a plan/apply/promote workflow for pipeline
development:

1. **Plan**: Create an isolated environment with change analysis
2. **Apply**: Execute the plan in the virtual environment
3. **Promote**: Promote validated changes to production

This workflow ensures changes are tested before affecting production data.

## Commands

| Command | Description |
|---------|-------------|
| `env plan <name> [--profile PATH]` | Preview changes in a virtual environment |
| `env apply <name> [--profile PATH]` | Execute a plan in a virtual environment |
| `env promote <from> [to] [--rematerialize] [--dry-run]` | Promote environment to production |
| `env list` | List all virtual environments |
| `env delete <name>` | Delete a virtual environment |

## Examples

### Create an environment plan

```bash
seeknal env plan dev
```

### Create an environment plan with a custom profile

```bash
seeknal env plan dev --profile profiles-dev.yml
```

### Apply changes in the environment

```bash
seeknal env apply dev
```

### Apply changes with a custom profile

```bash
seeknal env apply dev --profile profiles-dev.yml
```

### Apply with parallel execution

```bash
seeknal env apply dev --parallel
```

### Promote to production

```bash
seeknal env promote dev
```

### Promote with rematerialization

```bash
seeknal env promote dev --rematerialize
```

### Preview promotion without executing

```bash
seeknal env promote dev --dry-run
```

### List all environments

```bash
seeknal env list
```

### Delete an environment

```bash
seeknal env delete dev
```

## Workflow

```
1. seeknal env plan dev     → Creates dev environment with change plan
2. Review the plan output
3. seeknal env apply dev    → Executes plan in isolation
4. Validate results
5. seeknal env promote dev  → Promotes to production
```

## Per-Environment Profiles

Seeknal supports per-environment profile files for isolated database credentials
and connection settings.

### Auto-Discovery Priority

When `--profile` is not specified, Seeknal searches for profiles in this order:

| Priority | Location | Example |
|----------|----------|---------|
| 1 | `--profile` flag | `seeknal env plan dev --profile custom.yml` |
| 2 | Project root convention | `profiles-dev.yml` in project directory |
| 3 | Home directory convention | `~/.seeknal/profiles-dev.yml` |
| 4 | Default | `profiles.yml` or `~/.seeknal/profiles.yml` |

### Example

```bash
# Explicit profile
seeknal env plan dev --profile profiles-dev.yml

# Auto-discovery (finds profiles-dev.yml automatically)
seeknal env plan dev
```

The selected profile path is persisted in `plan.json`, so `env apply` uses
the same profile without re-specifying it.

## Namespace Prefixing

When running in a virtual environment, materialization targets are automatically
prefixed to prevent collisions with production data:

| Target | Production | Environment `dev` |
|--------|------------|-------------------|
| PostgreSQL | `analytics.orders` | `dev_analytics.orders` |
| Iceberg | `catalog.ns.orders` | `catalog.dev_ns.orders` |

The prefix applies to the schema (PostgreSQL) or namespace (Iceberg) only — table
names and catalog names are never modified.

## Environment Cleanup

Environments are automatically cleaned up after 7 days of inactivity (no access).
Use `seeknal env list` to check TTL status, or `seeknal env delete` for manual cleanup.

## See Also

- [seeknal plan](plan.md) - Analyze changes and show plan
- [seeknal run](run.md) - Execute pipeline
- [seeknal promote](promote.md) - Promote to production
