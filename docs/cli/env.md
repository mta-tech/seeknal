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
| `env plan <name>` | Preview changes in a virtual environment |
| `env apply <name>` | Execute a plan in a virtual environment |
| `env promote <from> [to]` | Promote environment to production |
| `env list` | List all virtual environments |
| `env delete <name>` | Delete a virtual environment |

## Examples

### Create an environment plan

```bash
seeknal env plan dev
```

### Apply changes in the environment

```bash
seeknal env apply dev
```

### Apply with parallel execution

```bash
seeknal env apply dev --parallel
```

### Promote to production

```bash
seeknal env promote dev
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

## See Also

- [seeknal plan](plan.md) - Analyze changes and show plan
- [seeknal run](run.md) - Execute pipeline
- [seeknal promote](promote.md) - Promote to production
