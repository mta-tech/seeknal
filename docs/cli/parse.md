---
summary: Parse YAML files and generate execution manifest
read_when: You want to validate and analyze the DAG without running it
related:
  - plan
  - run
  - diff
---

# seeknal parse

Parse YAML files in the seeknal/ directory and generate an execution manifest.
This validates the DAG structure without executing any nodes.

## Synopsis

```bash
seeknal parse [OPTIONS]
```

## Description

The `parse` command reads all YAML files in the `seeknal/` directory, builds the
DAG (Directed Acyclic Graph), and generates a `manifest.json` file in the
`target/` directory.

This is useful for:
- Validating YAML syntax and structure
- Detecting circular dependencies
- Viewing the complete DAG structure
- Comparing changes between runs

## Options

| Option | Description |
|--------|-------------|
| `--project-path` | Project directory (default: current directory) |
| `--output`, `-o` | Output manifest path (default: target/manifest.json) |

## Examples

### Parse current project

```bash
seeknal parse
```

### Parse with custom output location

```bash
seeknal parse --output ./my_manifest.json
```

## Output Example

```
Building DAG from seeknal/ directory...

DAG built: 15 nodes, 18 edges

Node Summary:
  - source: 3
  - transform: 5
  - feature_group: 4
  - model: 2
  - aggregation: 1

Changes detected:
  Added (2):
    + transform.new_feature
    + feature_group.user_metrics
  Modified (1):
    ~ transform.clean_data (sql)
  Removed (1):
    - transform.old_process

Summary: 2 added, 1 modified, 1 removed

Manifest saved to target/manifest.json
```

## See Also

- [seeknal plan](plan.md) - Analyze changes and show execution plan
- [seeknal run](run.md) - Execute pipeline
- [seeknal diff](diff.md) - Show changes in pipeline files
