# Plan/Apply Workflow Guide

## Overview

The plan/apply workflow provides safe, controlled deployments through isolated environments. Preview changes before affecting production.

## Workflow

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐
│   Develop   │ ──→ │    Plan      │ ──→ │   Apply     │
└─────────────┘      └──────────────┘      └─────────────┘
                           │                     │
                           ▼                     ▼
                    Review Changes        Execute in
                    (Categorized)          Isolation
                                                 │
                                                 ▼
                                          ┌─────────────┐
                                          │   Promote    │
                                          └─────────────┘
                                                 │
                                                 ▼
                                          ┌─────────────┐
                                          │ Production   │
                                          └─────────────┘
```

## Key Concepts

### Environments

Isolated spaces for testing changes:

- **Production** - Live data, user-facing
- **Dev/Staging** - Isolated testing environments
- **Virtual** - References production for unchanged nodes

### Plan

A **plan** captures:
- What changed (added/removed/modified nodes)
- Change categorization (breaking/non-breaking/metadata)
- Downstream impact analysis
- Execution estimate

## Commands

### 1. Plan Changes

```bash
# Create plan in dev environment
seeknal plan dev

# Shows categorized changes:
# [BREAKING - rebuilds downstream] source_node
#   columns: removed: old_column
# [CHANGED - rebuild this node] transform_node
#   sql: keys changed: sql
# [METADATA - no rebuild needed] report_node
```

### 2. Apply Plan

```bash
# Execute plan in isolated environment
seeknal env apply dev

# Or with parallel execution
seeknal env apply dev --parallel
```

### 3. Test & Validate

```bash
# Run tests against dev environment
pytest tests/integration/
seeknal test --env dev

# Query dev outputs
seeknal query --env dev "SELECT * FROM output_table LIMIT 10"
```

### 4. Promote to Production

```bash
# Atomic promotion to production
seeknal env promote dev prod

# Or promote to another environment
seeknal env promote dev staging
```

### 5. Manage Environments

```bash
# List all environments
seeknal env list

# Delete an environment
seeknal env delete dev

# Cleanup expired environments
seeknal env cleanup
```

## Examples

### Example 1: Add New Column

```bash
# 1. Modify node SQL
# 2. Plan changes
seeknal plan dev

# Output shows:
# [CHANGED - rebuild this node] my_node
#   columns: added: email

# 3. Apply in dev
seeknal env apply dev

# 4. Verify
seeknal test --env dev

# 5. Promote
seeknal env promote dev prod
```

### Example 2: Breaking Schema Change

```bash
# Plan shows:
# [BREAKING - rebuilds downstream] source_node
#   columns: removed: user_id
#   Downstream impact (12 nodes)

# Review impact, decide to proceed
seeknal env apply dev

# Test thoroughly
seeknal test --env dev

# Promote during maintenance window
seeknal env promote dev prod
```

### Example 3: Multiple Changes

```bash
# Create plan
seeknal plan staging

# Review summary:
# 2 node(s) added
# 5 node(s) modified
# 1 BREAKING change
# 4 NON_BREAKING changes

# Apply
seeknal env apply staging

# Run full test suite
pytest tests/ --env staging

# If all tests pass, promote
seeknal env promote staging prod
```

## Best Practices

### 1. Always Plan First

```bash
# Never skip planning
seeknal plan dev  # Review changes first
seeknal env apply dev
```

### 2. Test in Isolation

```bash
# Apply to dev environment
seeknal env apply dev

# Run validation
seeknal validate --env dev
pytest tests/e2e/ --env dev
```

### 3. Atomic Promotion

Promotion uses temp directory + rename for atomicity:

```bash
# Safe: atomic operation
seeknal env promote dev prod

# If interrupted, production is untouched
# Temp files cleaned up automatically
```

### 4. TTL-Based Cleanup

Environments auto-expire (default 7 days):

```bash
# Cleanup expired environments
seeknal env cleanup

# Or set custom TTL
seeknal env plan dev --ttl 24  # hours
```

## Environment Reference

### Virtual Environments

Virtual environments reference production outputs for unchanged nodes:

```json
{
  "refs": [
    {
      "node_id": "unchanged_node",
      "output_path": "target/cache/source/unchanged_node.parquet",
      "fingerprint": "abc123..."
    }
  ]
}
```

This enables:
- Fast environment creation
- Minimal re-execution
- Storage efficiency

## Troubleshooting

### Plan Stale Error

```bash
# Error: Plan is stale
# Solution: Re-plan
seeknal plan dev
seeknal env apply dev
```

### Environment Not Found

```bash
# Error: Environment 'dev' not found
# Solution: Create plan first
seeknal plan dev
```

### Promotion Failed

```bash
# Error: Environment has not been applied
# Solution: Apply first
seeknal env apply dev
seeknal env promote dev prod
```

## API Usage

Programmatic environment management:

```python
from seeknal.workflow.environment import EnvironmentManager
from seeknal.dag.manifest import Manifest

manager = EnvironmentManager(target_path="target")

# Create plan
manifest = Manifest.load("manifest.json")
plan = manager.plan("dev", manifest)

# Apply plan
result = manager.apply("dev")

# Promote
manager.promote("dev", "prod")

# Cleanup
deleted = manager.cleanup_expired()
```
