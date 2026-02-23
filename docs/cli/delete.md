---
summary: Delete resources from the project
read_when: You need to remove a resource that is no longer needed
related:
  - list
  - show
---

# seeknal delete

Delete resources from the Seeknal project.

## Synopsis

```bash
seeknal delete [OPTIONS] RESOURCE_TYPE RESOURCE_NAME
```

## Description

Removes resources and optionally their associated data.

## Options

| Option | Description |
|--------|-------------|
| `--force` | Skip confirmation prompt |
| `--keep-data` | Keep associated data files |

## Examples

### Delete a feature group

```bash
seeknal delete feature-group old_features
```

### Force delete without confirmation

```bash
seeknal delete feature-group old_features --force
```

## See Also

- [seeknal list](list.md) - List resources
- [seeknal show](show.md) - Show resource details
