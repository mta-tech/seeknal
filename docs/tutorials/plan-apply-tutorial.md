# Plan/Apply Workflow Tutorial

> **Estimated Time:** 40 minutes | **Difficulty:** Intermediate

Learn how to use Seeknal's plan/apply workflow for safe, controlled deployments with isolated testing environments.

---

## Prerequisites

- Completed [Getting Started Guide](../getting-started-comprehensive.md)
- Working Seeknal project with defined flows
- Basic understanding of YAML pipeline configuration

## What You'll Learn

1. **Create plans** showing categorized changes before execution
2. **Apply changes** in isolated dev environments
3. **Test thoroughly** before promoting to production
4. **Promote atomically** from dev to production

---

## Part 1: Understanding Plan/Apply Workflow (5 minutes)

### Why Plan/Apply?

The plan/apply workflow provides:

- **Preview before execution** - See exactly what will change
- **Change categorization** - Understand impact (breaking vs non-breaking)
- **Isolated testing** - Test changes without affecting production
- **Atomic promotion** - Safe rollout to production

### Workflow Overview

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌──────────┐
│ Develop │ -> │  Plan   │ -> │  Apply  │ -> │ Promote  │
└─────────┘    └─────────┘    └─────────┘    └──────────┘
                     │              │              │
                     ▼              ▼              ▼
              Review         Test in         Move to
              Changes        Isolation       Production
```

---

## Part 2: Creating Your First Plan (10 minutes)

### Step 2.1: Make a Change

Modify your project - add a new column to an existing node:

```python
# Before: source_nodes.py
task.add_sql("""
    SELECT
        user_id,
        event_time,
        action_type
    FROM raw_events
""")
```

```python
# After: source_nodes.py
task.add_sql("""
    SELECT
        user_id,
        event_time,
        action_type,
        device_type  -- NEW COLUMN
    FROM raw_events
""")
```

### Step 2.2: Create Plan

```bash
# Create plan for dev environment
seeknal plan dev
```

Output:
```
Creating plan for environment: dev

Changes detected:
  Modified (1):
    ~ source_nodes (columns, sql)
      columns: added: device_type
      category: NON_BREAKING
      Downstream impact: 0 nodes

Summary: 0 added, 1 modified, 0 removed
         0 BREAKING, 1 NON_BREAKING, 0 METADATA

Plan created: target/environments/dev/plan.json
```

### Step 2.3: Review Plan File

```bash
# View plan details
cat target/environments/dev/plan.json
```

Or use JSON output:

```bash
seeknal plan dev --output plan.json --format json
```

---

## Part 3: Applying Changes in Dev (10 minutes)

### Step 3.1: Apply Plan to Dev Environment

```bash
# Apply changes in isolated environment
seeknal env apply dev
```

Output:
```
Applying plan to environment: dev

Executing with 2 parallel workers:
  ✓ source_nodes (NON_BREAKING) - 2.3s - 1500 rows

Environment ready at: target/environments/dev/
Status: applied
```

### Step 3.2: Inspect Dev Environment

```bash
# List all environments
seeknal env list

# Output:
# Environments:
#   dev    applied    2024-01-10 10:30    TTL: 7 days
#   prod  promoted   2024-01-09 09:00    TTL: never
```

### Step 3.3: Query Dev Outputs

```bash
# Query dev environment results
seeknal query --env dev "SELECT * FROM source_nodes LIMIT 10"

# Verify new column exists
seeknal query --env dev "DESCRIBE source_nodes"

# Output should show:
# - user_id
# - event_time
# - action_type
# - device_type  ← NEW COLUMN
```

---

## Part 4: Testing Before Promotion (10 minutes)

### Step 4.1: Run Integration Tests

```bash
# Run tests against dev environment
pytest tests/integration/ --env dev -v
```

### Step 4.2: Validate Data Quality

```python
# validation_script.py
from seeknal.context import context

# Connect to dev environment
context.set_environment("dev")

# Run validation queries
result = context.execute("""
    SELECT
        COUNT(*) as total_rows,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(device_type) as devices_with_type
    FROM source_nodes
""")

assert result['devices_with_type'] > 0, "New column not populated!"
print("✓ Validation passed")
```

```bash
python validation_script.py
```

### Step 4.3: Compare Dev vs Prod

```bash
# Row counts comparison
echo "Dev row count:"
seeknal query --env dev "SELECT COUNT(*) FROM source_nodes"

echo "Prod row count:"
seeknal query --env prod "SELECT COUNT(*) FROM source_nodes"
```

---

## Part 5: Handling Breaking Changes (5 minutes)

### Step 5.1: Create Breaking Change

Remove a column that downstream nodes depend on:

```python
# Remove device_type column
task.add_sql("""
    SELECT
        user_id,
        event_time,
        action_type
    FROM raw_events
    -- device_type REMOVED
""")
```

### Step 5.2: Plan Shows Breaking Change

```bash
seeknal plan dev
```

Output:
```
Changes detected:
  Modified (1):
    ~ source_nodes (columns, sql)
      columns: removed: device_type
      category: BREAKING
      Downstream impact (3 nodes):
        -> REBUILD user_aggregates (uses device_type)
        -> REBUILD device_features (uses device_type)
        -> REBUILD final_output (depends on user_aggregates)

Summary: 1 BREAKING change - will rebuild 3 downstream nodes
```

### Step 5.3: Review and Decide

The plan shows all affected nodes. You can:
- **Proceed** - Accept the downstream rebuilds
- **Revert** - Keep the column to avoid impact
- **Update downstream** - Modify downstream SQL first

---

## Part 6: Promoting to Production

### Step 6.1: Atomic Promotion

```bash
# Promote dev to production
seeknal env promote dev prod
```

What happens:
1. Temp directory created
2. Dev outputs copied to temp
3. Atomic rename to production
4. Original production preserved as backup

Output:
```
Promoting environment: dev -> prod

Preparing promotion...
  ✓ Backed up production to: target/prod.backup.20240110_103000
  ✓ Copied dev outputs to temp
  ✓ Atomically renamed to production

Promotion complete!
Rollback available: target/prod.backup.20240110_103000
```

### Step 6.2: Verify Production

```bash
# Quick verification
seeknal query --env prod "SELECT COUNT(*) FROM source_nodes"

# Run production tests
pytest tests/e2e/ --env prod
```

### Step 6.3: Rollback if Needed

```bash
# Restore from backup
cp -r target/prod.backup.20240110_103000/* target/outputs/

# Or use seeknal command
seeknal env rollback --backup target/prod.backup.20240110_103000
```

---

## Part 7: Environment Management

### List Environments

```bash
seeknal env list

# Output:
# Name    Status      Created              TTL
# dev     applied    2024-01-10 10:30     7 days
# staging applied    2024-01-09 15:00     7 days
# prod    promoted   2024-01-08 09:00     never
```

### Delete Environment

```bash
# Delete unused environment
seeknal env delete dev

# Output: Environment 'dev' deleted
```

### Cleanup Expired

```bash
# Remove environments past TTL
seeknal env cleanup

# Output: Deleted 2 expired environments
```

---

## Part 8: Advanced Scenarios

### Scenario 1: Multi-Stage Promotion

```bash
# Dev -> Staging -> Prod
seeknal plan staging
seeknal env apply staging
# Test staging...
seeknal env promote staging prod
```

### Scenario 2: Virtual Environments

Virtual environments reference production for unchanged nodes:

```python
# Dev environment config
{
    "name": "dev",
    "type": "virtual",
    "base": "prod",
    "refs": [
        {"node": "unchanged_node", "output": "prod_cache/unchanged_node"}
    ]
}
```

Benefits:
- Fast environment creation
- Minimal re-execution
- Storage efficiency

### Scenario 3: Force Apply

Skip staleness check:

```bash
seeknal env apply dev --force
```

Use only when:
- You understand the risks
- Plan is known to be stale but valid

---

## Troubleshooting

### Issue: Plan Stale Error

**Symptom:** `Error: Plan is stale`

**Cause:** Underlying manifest changed since plan was created

**Solution:** Re-create plan

```bash
seeknal plan dev
seeknal env apply dev
```

### Issue: Promotion Failed

**Symptom:** `Error: Environment has not been applied`

**Cause:** Trying to promote unapplied environment

**Solution:** Apply first

```bash
seeknal env apply dev
seeknal env promote dev prod
```

### Issue: Environment Not Found

**Symptom:** `Error: Environment 'dev' not found`

**Cause:** Plan not created

**Solution:** Create plan first

```bash
seeknal plan dev  # Creates the environment
```

---

## Summary

You've learned how to:

1. ✅ Create plans showing categorized changes
2. ✅ Apply changes in isolated dev environments
3. ✅ Test thoroughly before promotion
4. ✅ Promote atomically to production
5. ✅ Handle breaking changes safely
6. ✅ Rollback if needed

## Next Steps

- [Interval Tracking Guide](../guides/interval-tracking.md) - Time-series incremental processing
- [Change Detection Guide](../guides/change-detection.md) - SQL-aware change detection
- [State Backends Guide](../guides/state-backends.md) - Database state for production

## Quick Reference

```bash
# Create plan
seeknal plan <env>

# Apply plan
seeknal env apply <env>

# Promote
seeknal env promote <source> <target>

# List environments
seeknal env list

# Delete environment
seeknal env delete <env>

# Cleanup expired
seeknal env cleanup
```
