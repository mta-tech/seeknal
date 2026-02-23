---
summary: Remove old feature data based on TTL or date
read_when: You need to clean up historical feature data to save storage
related:
  - delete
  - delete-table
---

# seeknal clean

Remove old feature data from a feature group based on a date threshold or TTL
(time-to-live) setting. This helps manage storage costs by removing outdated
historical data.

## Synopsis

```bash
seeknal clean [OPTIONS] FEATURE_GROUP
```

## Description

The `clean` command removes historical feature data that is older than a specified
date or TTL period. Use this to manage storage costs and comply with data retention
policies.

You must specify either `--before` to delete data before a specific date, or
`--ttl` to delete data older than a number of days.

## Options

| Option | Description |
|--------|-------------|
| `FEATURE_GROUP` | Name of the feature group to clean |
| `--before`, `-b` | Delete data before this date (YYYY-MM-DD format) |
| `--ttl` | Delete data older than TTL days |
| `--dry-run` | Show what would be deleted without actually deleting |

## Examples

### Clean data older than 90 days

```bash
seeknal clean user_features --ttl 90
```

### Clean data before a specific date

```bash
seeknal clean user_features --before 2024-01-01
```

### Preview what would be deleted

```bash
seeknal clean user_features --ttl 30 --dry-run
```

## See Also

- [seeknal delete](delete.md) - Delete a feature group completely
- [seeknal delete-table](delete-table.md) - Delete an online table
