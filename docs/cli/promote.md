---
summary: Promote environment changes to production
read_when: You have tested changes in an environment and want to promote them
related:
  - env
  - plan
  - run
---

# seeknal promote

Promote environment changes to production. This command copies the validated
environment state to the production target directory.

## Synopsis

```bash
seeknal promote [OPTIONS] ENV_NAME [TARGET]
```

## Description

The `promote` command is the final step in the plan/apply/promote workflow.
It takes changes that have been validated in a virtual environment and promotes
them to production.

**Important**: Only promote after you have:
1. Created an environment plan (`seeknal env plan <name>`)
2. Executed the plan (`seeknal env apply <name>`)
3. Validated the results

## Options

| Option | Description |
|--------|-------------|
| `ENV_NAME` | Environment to promote (e.g., `dev`, `staging`) |
| `TARGET` | Target environment (default: `prod`) |
| `--project-path` | Project directory (default: current directory) |

## Examples

### Promote dev to production

```bash
seeknal promote dev
```

### Promote staging to prod explicitly

```bash
seeknal promote staging prod
```

## Workflow

```
1. seeknal env plan dev       → Create isolated environment
2. seeknal env apply dev      → Test changes in isolation
3. Validate results           → Manual verification
4. seeknal promote dev        → Promote to production
```

## Safety Checks

The promote command will:
- Verify the environment exists
- Check for successful execution in the environment
- Copy manifest and state to production target

## See Also

- [seeknal env](env.md) - Manage virtual environments
- [seeknal plan](plan.md) - Analyze changes and show plan
- [seeknal run](run.md) - Execute pipeline
