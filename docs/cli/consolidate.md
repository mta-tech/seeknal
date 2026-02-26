---
summary: Manually trigger entity consolidation
read_when: You want to consolidate feature groups into per-entity views
related:
  - entity
  - run
  - repl
---

# seeknal consolidate

Manually trigger entity consolidation. Merges feature group outputs that
share the same entity into unified per-entity views with struct-namespaced
columns.

## Synopsis

```bash
seeknal consolidate [OPTIONS]
```

## Description

Consolidation happens automatically after `seeknal run`. Use this command when
you need to trigger consolidation manually, for example:

- After running individual nodes with `seeknal run --nodes`
- After removing a feature group from the project
- To prune stale feature group columns

## Options

| Option | Description |
|--------|-------------|
| `--project`, `-p` | Project name |
| `--path` | Project path (default: current directory) |
| `--prune` | Remove stale FG columns not in current manifest |

## Examples

### Basic consolidation

```bash
seeknal consolidate
```

### After running individual nodes

```bash
seeknal run --nodes feature_group.customer_features
seeknal consolidate    # Re-consolidate with updated data
```

### Prune stale feature group columns

```bash
# After removing a feature group from the project
seeknal consolidate --prune
```

### Specify project path

```bash
seeknal consolidate --path /my/project
```

## Output Example

```
ℹ Found 2 entities to consolidate...
✓   customer: 2 FGs, 6 rows (0.1s)
✓   product: 1 FGs, 10 rows (0.0s)
✓ Consolidation complete: 2/2 entities
```

## See Also

- [seeknal entity](entity.md) - Inspect consolidated entities
- [seeknal run](run.md) - Execute pipeline (auto-consolidates)
- [seeknal repl](repl.md) - Query entity views interactively
