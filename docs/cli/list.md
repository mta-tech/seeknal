---
summary: List resources in the current project
read_when: You need to see what resources exist in your project
related:
  - show
  - init
---

# seeknal list

List resources in the current Seeknal project.

## Synopsis

```bash
seeknal list [OPTIONS] [RESOURCE_TYPE]
```

## Description

Lists resources such as feature groups, sources, pipelines, and environments.

## Options

| Option | Description |
|--------|-------------|
| `--format` | Output format: table, json, yaml (default: table) |
| `--type` | Filter by resource type |

## Resource Types

- `feature-groups` - List feature groups
- `sources` - List data sources
- `pipelines` - List pipelines
- `environments` - List environments

## Examples

### List all resources

```bash
seeknal list
```

### List feature groups

```bash
seeknal list feature-groups
```

### JSON output

```bash
seeknal list --format json
```

## See Also

- `seeknal show` - Show resource details
- `seeknal init` - Initialize a project
