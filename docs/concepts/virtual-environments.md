# Virtual Environments

Virtual environments in Seeknal provide isolated workspaces for safely developing, testing, and promoting data pipeline changes without affecting production.

## What Are Virtual Environments?

A virtual environment is an isolated workspace where you can:
- Test changes to YAML pipeline definitions
- Preview impact before execution
- Validate transformations safely
- Promote verified changes to production

Think of environments as Git branches for your data pipeline state - they let you experiment without breaking production.

## Architecture

### Directory Structure

Environments are stored under `target/environments/<name>/`:

```
target/
├── manifest.json           # Production manifest
├── run_state.json          # Production execution state
├── cache/                  # Production data cache
└── environments/
    ├── dev/
    │   ├── env_config.json     # Environment metadata
    │   ├── plan.json           # Saved execution plan
    │   ├── manifest.json       # Environment manifest
    │   ├── run_state.json      # Environment execution state
    │   ├── refs.json           # References to production data
    │   └── cache/              # Environment-specific cache
    └── staging/
        └── ...
```

### State Isolation

Each environment maintains:
- **Isolated manifest**: Independent YAML definitions
- **Isolated state**: Separate execution history
- **Isolated cache**: Environment-specific data outputs
- **Production references**: Links to unchanged production data

### Cost Model

**Metadata-only promotion** (default):
- Only execution state and manifest are copied
- No data duplication
- Fast promotion (milliseconds)
- Production references remain valid

**Data copy** (optional):
- Full cache duplication for complete isolation
- Used when data formats change
- Slower promotion but more isolated

## Workflow

### 1. Plan

Preview what will change before execution:

```bash
seeknal plan dev
```

This creates a plan that shows:
- Modified nodes and their change categories
- Downstream impact analysis
- Total nodes to execute
- Saved to `target/environments/dev/plan.json`

**Plan staleness detection**: Plans include a manifest fingerprint. If you modify YAMLs after planning, `apply` will detect staleness and require re-planning.

### 2. Apply

Execute the plan in the environment:

```bash
seeknal run --env dev
```

This:
- Validates plan staleness
- Executes changed nodes and their downstream dependents
- Saves results to environment cache
- Updates environment state

**Parallel execution** is supported:

```bash
seeknal run --env dev --parallel --max-workers 8
```

### 3. Validate

Query and validate environment results before promoting:

```bash
# Check environment status
seeknal env list

# Compare with production
seeknal diff

# Run tests against environment data
pytest tests/ --env dev
```

### 4. Promote

Move verified changes to production:

```bash
seeknal promote dev
```

This:
- Validates environment has been applied
- Copies environment state to production
- Copies environment cache to production
- Uses atomic two-phase commit (temp → rename)

**Atomic promotion**: Uses temporary directory with rollback capability to prevent corruption if interrupted.

## CLI Commands

### Planning

```bash
# Create environment plan
seeknal plan <env_name>

# Backward-compatible alias
seeknal env plan <env_name>
```

### Applying

```bash
# Execute plan (sequential)
seeknal run --env <env_name>

# Execute plan (parallel)
seeknal run --env <env_name> --parallel

# Execute with specific workers
seeknal run --env <env_name> --parallel --max-workers 8

# Force apply even if plan is stale
seeknal run --env <env_name> --force

# Backward-compatible alias
seeknal env apply <env_name>
```

### Promoting

```bash
# Promote to production
seeknal promote <env_name>

# Promote to another environment
seeknal env promote <source_env> <target_env>
```

### Management

```bash
# List all environments
seeknal env list

# Delete an environment
seeknal env delete <env_name>

# Delete an expired or unused environment
seeknal env delete old-experiment
```

## Environment Configuration

Each environment has metadata in `env_config.json`:

```json
{
  "name": "dev",
  "created_at": "2026-02-09T10:00:00",
  "last_accessed": "2026-02-09T12:30:00",
  "promoted_from": null,
  "ttl_seconds": 604800
}
```

**TTL (Time-To-Live)**: Environments auto-expire after 7 days of inactivity by default. Use `seeknal env delete <name>` to remove expired environments.

## Production References

For unchanged nodes, environments create references to production data instead of re-executing:

```json
{
  "refs": [
    {
      "node_id": "source.orders",
      "output_path": "target/cache/source/orders.parquet",
      "fingerprint": "abc123..."
    }
  ]
}
```

This enables:
- Fast environment creation (no redundant execution)
- Cost savings (no data duplication)
- Consistency (unchanged nodes use proven production data)

**Fingerprint validation**: References include a hash of the first 4KB of the cache file. If production data changes, the reference is invalidated.

## Comparison to Other Tools

### vs dbt targets

| Feature | Seeknal Environments | dbt targets |
|---------|---------------------|-------------|
| Purpose | Isolated dev/staging workspaces | Database schema selection |
| State isolation | Full state + cache per environment | Shared state, different schemas |
| Change preview | `seeknal plan` shows impact | `dbt compile` for SQL only |
| Promotion | Metadata-only copy | Manual schema migration |
| Cost model | References unchanged nodes | Re-runs everything |

### vs SQLMesh virtual environments

| Feature | Seeknal Environments | SQLMesh Virtual Environments |
|---------|---------------------|------------------------------|
| Implementation | File-based state + cache | Database views + CTEs |
| Execution | Full DAG execution | Virtual layer only |
| Change detection | Hash-based with categories | Schema fingerprints |
| Promotion | State copy + atomic commit | Pointer swap |
| Data isolation | Cache per environment | Shared base tables |

### vs Databricks environments

| Feature | Seeknal Environments | Databricks Environments |
|---------|---------------------|------------------------|
| Cost | Metadata-only (free) | Full cluster per env ($$$$) |
| Speed | Milliseconds to promote | Minutes to spin up |
| Isolation | Process-level | Cluster-level |
| Use case | Dev/staging for pipelines | Separate workspaces for teams |

## Best Practices

### Naming Conventions

```bash
dev        # Personal development
staging    # Pre-production validation
hotfix     # Emergency fixes
exp-ml     # Experimental features
```

### Workflow Patterns

**Individual development:**
```bash
seeknal plan dev
seeknal run --env dev
# validate locally
seeknal promote dev
```

**Team staging:**
```bash
# Developer 1
seeknal plan staging
seeknal run --env staging
# request review

# Developer 2 validates
seeknal env list
pytest tests/ --env staging

# Team lead promotes
seeknal promote staging
```

**CI/CD integration:**
```yaml
# .github/workflows/deploy.yml
- name: Plan changes
  run: seeknal plan ci-${{ github.sha }}

- name: Apply in CI environment
  run: seeknal run --env ci-${{ github.sha }} --parallel

- name: Run tests
  run: pytest tests/ --env ci-${{ github.sha }}

- name: Promote to production
  if: github.ref == 'refs/heads/main'
  run: seeknal promote ci-${{ github.sha }}
```

### Environment Hygiene

- Delete environments after promotion: `seeknal env delete dev`
- Use TTL auto-cleanup for CI environments
- Prefix temporary environments: `exp-`, `test-`, `ci-`
- Avoid long-lived personal environments (use `dev` consistently)

## Advanced Features

### Plan Fingerprinting

Plans include a SHA256 fingerprint of the manifest. This prevents applying a plan after YAMLs have changed:

```bash
seeknal plan dev
# ... edit YAML files ...
seeknal run --env dev
# ERROR: Plan is stale -- manifest has changed
```

To apply anyway: `seeknal run --env dev --force`

### Parallel Execution

Environments support parallel execution with automatic batching:

```bash
seeknal run --env dev --parallel
```

The executor:
- Groups nodes into topological batches
- Executes batches in parallel (respects dependencies)
- Uses 4 workers by default (configurable with `--max-workers`)

### Environment Promotion Chain

Promote changes through multiple environments:

```bash
# dev → staging
seeknal promote dev
seeknal env promote dev staging

# staging → production
seeknal promote staging
```

This creates a promotion history in `env_config.json`:

```json
{
  "name": "staging",
  "promoted_from": "dev",
  "last_accessed": "2026-02-09T14:00:00"
}
```

## Troubleshooting

### Plan staleness errors

**Problem**: `Plan is stale -- manifest has changed`

**Solution**:
```bash
# Re-plan with latest changes
seeknal plan dev

# Or force apply (not recommended)
seeknal run --env dev --force
```

### Environment not found

**Problem**: `Environment 'dev' not found`

**Solution**:
```bash
# Create plan first
seeknal plan dev
```

### Promotion fails

**Problem**: `Environment 'dev' has not been applied`

**Solution**:
```bash
# Apply the plan before promoting
seeknal run --env dev
seeknal promote dev
```

### Expired environment

**Problem**: Environment deleted by TTL cleanup

**Solution**:
```bash
# Recreate environment
seeknal plan dev
seeknal run --env dev
```

## Per-Environment Profiles

Each environment can use its own `profiles.yml` for isolated database connections:

```bash
# Create environment with custom profile
seeknal env plan staging --profile profiles-staging.yml
```

Seeknal auto-discovers per-env profiles following a priority chain:
1. Explicit `--profile` flag (highest priority)
2. `profiles-{env}.yml` in the project root
3. `~/.seeknal/profiles-{env}.yml`
4. Default `profiles.yml`

The profile path is persisted in `plan.json`, so `env apply` automatically uses the same profile.

## Namespace Prefixing (Convention-Based Isolation)

When materializing in a virtual environment, Seeknal prefixes the schema or namespace
with the environment name to prevent production data conflicts:

- **PostgreSQL**: `schema.table` → `{env}_schema.table`
- **Iceberg**: `catalog.namespace.table` → `catalog.{env}_namespace.table`

This provides environment isolation without requiring separate databases or profiles.
The prefix is applied automatically — no configuration needed.

## TTL and Cleanup

Environments have a default time-to-live (TTL) of 7 days since last access.
Expired environments are **not** removed automatically in the background. Use `seeknal env list` to check environment status and `seeknal env delete` to remove stale environments manually.

```bash
# Check environment status and TTL
seeknal env list

# Delete an expired or unused environment
seeknal env delete old-experiment
```

## See Also

- **Concepts**: [Change Categorization](change-categorization.md), [Plan](glossary.md#plan), [Promote](glossary.md#promote)
- **Tutorials**: [YAML Pipeline Tutorial](../tutorials/yaml-pipeline-tutorial.md), [Virtual Environments Tutorial](../tutorials/phase2-data-eng-environments.md)
- **Reference**: [CLI Environment Commands](../reference/cli.md#virtual-environments), [Configuration Reference](../reference/configuration.md)
