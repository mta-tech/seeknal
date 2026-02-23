---
summary: Manage feature group versions for schema evolution
read_when: You need to track, compare, or rollback feature group schemas
related:
  - validate-features
  - materialize
---

# seeknal version

Manage feature group versions for tracking schema evolution and enabling safe
rollbacks. Feature groups are automatically versioned when their schema changes.

## Synopsis

```bash
seeknal version [COMMAND] [OPTIONS] FEATURE_GROUP
```

## Description

The `version` command group provides tools for managing feature group versions:

- **List**: View all versions with creation dates
- **Show**: Display detailed schema for a version
- **Diff**: Compare schemas between versions

This enables ML teams to track schema evolution, identify breaking changes,
and safely roll back to previous versions when needed.

## Commands

| Command | Description |
|---------|-------------|
| `version list <fg>` | List all versions of a feature group |
| `version show <fg>` | Show version details (latest or specific) |
| `version diff <fg>` | Compare schemas between two versions |

## Examples

### List all versions

```bash
seeknal version list user_features
```

### List last 5 versions

```bash
seeknal version list user_features --limit 5
```

### Show latest version details

```bash
seeknal version show user_features
```

### Show specific version

```bash
seeknal version show user_features --version 2
```

### Compare two versions

```bash
seeknal version diff user_features --from 1 --to 2
```

### Output as JSON

```bash
seeknal version list user_features --format json
```

## Version List Output

```
Versions for feature group: user_features
--------------------------------------------------
  Version  Created At              Features
  3        2024-02-15 10:30:00    15
  2        2024-02-10 14:22:00    12
  1        2024-01-20 09:15:00    10
```

## Version Show Output

```
Feature Group: user_features
Version: 3
--------------------------------------------------
  Created At:    2024-02-15 10:30:00
  Updated At:    2024-02-15 10:30:00
  Feature Count: 15

Schema:
--------------------------------------------------
  Fields:
    - user_id: string
    - email: string
    - age: integer
    - total_orders: bigint
    - lifetime_value: double
    ...
```

## Version Diff Output

```
Feature Group: user_features
Comparing version 1 → 2
============================================================

Added (3):
  + lifetime_value: double
  + avg_order_value: double
  + last_order_date: timestamp

Removed (1):
  - legacy_field: string

Modified (1):
  ~ age: int → bigint

Summary: 3 added, 1 removed, 1 modified
```

## Materializing Specific Versions

To materialize a specific version (useful for rollbacks):

```bash
seeknal materialize user_features --start-date 2024-01-01 --version 1
```

## See Also

- [seeknal validate-features](validate-features.md) - Validate feature group data
- [seeknal materialize](materialize.md) - Materialize features to stores
