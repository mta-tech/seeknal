---
summary: Delete an online table and its associated data files
read_when: You need to remove an online feature serving table
related:
  - delete
  - clean
---

# seeknal delete-table

Delete an online table from the feature store, including all associated data
files and metadata. This operation is irreversible.

## Synopsis

```bash
seeknal delete-table [OPTIONS] TABLE_NAME
```

## Description

The `delete-table` command removes an online table and all its data from the
online store. It also cleans up metadata from the database. By default, it
checks for dependent feature groups and asks for confirmation before deleting.

Use this command when you no longer need an online serving table or want to
recreate it from scratch.

## Options

| Option | Description |
|--------|-------------|
| `TABLE_NAME` | Name of the online table to delete |
| `--force`, `-f` | Skip confirmation and bypass dependency warnings |

## Examples

### Delete a table with confirmation

```bash
seeknal delete-table user_features_online
```

### Force delete without confirmation

```bash
seeknal delete-table user_features_online --force
```

## Warning Messages

If the table has dependent feature groups, you will see a warning:

```
Table 'user_features_online' has 2 dependent feature group(s):
  - user_features
  - user_behavior

Deleting this table may affect these feature groups.
Are you sure you want to delete this table? [y/N]
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Table deleted successfully |
| 1 | Table not found or deletion failed |

## See Also

- [seeknal delete](delete.md) - Delete a feature group
- [seeknal clean](clean.md) - Clean old feature data
