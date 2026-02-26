---
summary: Seeknal CLI command reference and overview
read_when: You need to understand available CLI commands
related:
  - init
  - run
  - list
---

# Seeknal CLI Reference

The Seeknal CLI (`seeknal`) provides commands for managing data pipelines, feature stores, entity consolidation, and ML features.

## Quick Reference

| Command | Description |
|---------|-------------|
| `init` | Initialize a new Seeknal project |
| `run` | Execute YAML/Python pipeline |
| `plan` | Analyze changes and show execution plan |
| `repl` | Interactive SQL REPL or one-shot queries |
| `list` | List resources in the current project |
| `show` | Show details of a specific resource |
| `entity` | Manage consolidated entity feature stores |
| `consolidate` | Manually trigger entity consolidation |
| `lineage` | Generate interactive lineage visualization |
| `dq` | Generate data quality dashboard |

## Command Categories

### Project Management

- `init` - Initialize a new project
- `info` - Show version information
- `env` - Manage virtual environments

### Pipeline Operations

- `run` - Execute pipeline (DAG-based, incremental, parallel)
- `plan` - Analyze changes and show execution plan
- `diff` - Show changes in pipeline files
- `draft` - Generate template files for new nodes
- `apply` - Apply a draft file to production
- `dry-run` - Validate and preview without executing

### Resource Management

- `list` - List resources
- `show` - Show resource details
- `delete` - Delete resources
- `delete-table` - Delete online tables

### Feature Store & Entity

- `entity list` - List consolidated entities
- `entity show` - Show entity catalog details
- `consolidate` - Manually trigger entity consolidation
- `validate-features` - Validate feature group data quality
- `version list` - List feature group versions
- `version show` - Show version details
- `version diff` - Compare versions
- `clean` - Remove old feature data

### Data Exploration

- `repl` - Start interactive SQL REPL or one-shot queries
- `inspect` - Inspect intermediate pipeline output
- `intervals` - List completed intervals
- `query` - Query metrics from semantic layer

### Visualization & Quality

- `lineage` - Generate interactive lineage visualization (HTML or ASCII)
- `dq` - Generate data quality dashboard (HTML or ASCII)
- `audit` - Run data quality audits

### Integration

- `atlas` - Manage Apache Iceberg tables
- `connection-test` - Test database connectivity
- `download-sample-data` - Download sample datasets

## Getting Help

Run `seeknal COMMAND --help` for detailed information about any command.

## See Also

- [Getting Started Guide](../getting-started-comprehensive.md)
- [API Reference](../api/index.md)
- [Examples](../examples/index.md)
