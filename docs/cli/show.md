---
summary: Show details of a specific resource
read_when: You need detailed information about a specific resource
related:
  - list
  - version
---

# seeknal show

Show detailed information about a specific resource.

## Synopsis

```bash
seeknal show [OPTIONS] RESOURCE_TYPE RESOURCE_NAME
```

## Description

Displays detailed information including configuration, schema, and metadata.

## Options

| Option | Description |
|--------|-------------|
| `--format` | Output format: table, json, yaml |
| `--version` | Show specific version (for feature groups) |

## Examples

### Show feature group details

```bash
seeknal show feature-group my_features
```

### Show source configuration

```bash
seeknal show source my_source
```

## See Also

- [seeknal list](list.md) - List all resources
- [seeknal version](version.md) - Manage versions
