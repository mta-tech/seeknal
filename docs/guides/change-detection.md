# Change Detection Guide

## Overview

Change detection uses SQL-aware diffing to automatically categorize changes as **breaking** or **non-breaking**, ensuring efficient incremental rebuilds.

## Key Concepts

### Change Categories

| Category | Impact | Downstream |
|----------|--------|------------|
| **BREAKING** | Schema/logic change affecting outputs | Rebuild this + all downstream |
| **NON_BREAKING** | Change only affects this node | Rebuild this node only |
| **METADATA** | Description, tags, etc. | No rebuild needed |

### SQL-Aware Analysis

Changes are analyzed semantically:

- Column added → **NON_BREAKING**
- Column removed → **BREAKING**
- SQL formatting → **METADATA**
- Table added → **NON_BREAKING**
- Table removed → **BREAKING**

## CLI Commands

### View Changes

```bash
# Production mode (vs last run)
seeknal plan

# Environment mode (categorized changes)
seeknal plan dev
```

Output shows:
```
Changes detected:
  Added (2):
    + new_node
    + another_node

  Modified (3):
    ~ source_table (columns, sql)
      columns: added: email, removed: old_col
      sql: keys changed: sql
      Downstream impact (5 nodes):
        -> REBUILD downstream_1
        -> REBUILD downstream_2

Summary: 2 node(s) added, 3 node(s) modified
```

### Change Types

The CLI displays fine-grained change types:

- **COLUMN_ADDED** - New column in SELECT
- **SQL_LOGIC_CHANGED** - Query logic modified
- **TABLE_REMOVED** - Source table dropped
- **CONFIG_CHANGED** - Configuration updated

## Breaking vs Non-Breaking

### Breaking Changes

Require full rebuild of downstream nodes:

```sql
-- Before: Breaking
SELECT id, name FROM users
SELECT id, email FROM users  -- Column removed!
```

### Non-Breaking Changes

Only this node rebuilds:

```sql
-- Before: Non-Breaking
SELECT id, name FROM users
SELECT id, name, email FROM users  -- Column added!
```

## Column-Level Lineage

Track column dependencies to understand impact:

```python
from seeknal.dag.lineage import LineageBuilder

builder = LineageBuilder()
deps = builder.get_column_dependencies(manifest, "output_table", "user_id")

# Shows: output_table.user_id ← users.id ← raw.user_id
```

## Diff API

Programmatic access to change detection:

```python
from seeknal.dag.diff import ManifestDiff
from seeknal.dag.manifest import Manifest

old_manifest = Manifest.load("manifest.old.json")
new_manifest = Manifest.load("manifest.new.json")

diff = ManifestDiff.compare(old_manifest, new_manifest)

# Check what changed
if diff.has_changes():
    print(f"Added: {len(diff.added_nodes)}")
    print(f"Removed: {len(diff.removed_nodes)}")
    print(f"Modified: {len(diff.modified_nodes)}")

# Get categorized changes
to_rebuild = diff.get_nodes_to_rebuild(new_manifest)
for node_id, category in to_rebuild.items():
    print(f"{node_id}: {category.value}")
```

## Best Practices

### 1. Use Environments for Testing

```bash
# Create plan in dev environment
seeknal plan dev

# Apply dev changes
seeknal env apply dev

# Test, then promote to production
seeknal env promote dev prod
```

### 2. Review Impact Before Running

```bash
# Always check changes first
seeknal plan

# Look for "BREAKING" indicators
# Check downstream impact count
# Verify SQL diffs look correct
```

### 3. Leverage Non-Breaking Changes

Add columns without full rebuilds:

```sql
-- Safe: Non-breaking addition
SELECT
    id,
    name,
    email  -- New column
FROM users

-- Avoid: Breaking removal
SELECT id, email  -- Removed 'name' column!
FROM users
```

## Troubleshooting

### Unexpected Breaking Changes

If a change is marked breaking but shouldn't be:

1. Check for column type changes
2. Verify no columns were removed
3. Look for dropped tables

### Large Downstream Impact

If many nodes need rebuild:

1. Consider if change can be made non-breaking
2. Use `seeknal plan` to identify root cause
3. Schedule during low-traffic periods

## Configuration

Change detection is automatic. No configuration needed.

For custom behavior, extend `ManifestDiff`:

```python
from seeknal.dag.diff import ManifestDiff

class CustomDiff(ManifestDiff):
    def _classify_sql_change(self, old_sql, new_sql):
        # Custom logic here
        return super()._classify_sql_change(old_sql, new_sql)
```
