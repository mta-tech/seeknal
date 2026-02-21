---
summary: Seeknal CLI command reference and overview
read_when: You need to understand available CLI commands
related:
  - init
  - run
  - list
---

# Seeknal CLI Reference

The Seeknal CLI (`seeknal`) provides commands for managing feature stores, data pipelines, and ML features.

## Quick Reference

| Command | Description |
|---------|-------------|
| `init` | Initialize a new Seeknal project |
| `run` | Execute a YAML or Python pipeline |
| `list` | List resources in the current project |
| `show` | Show details of a specific resource |
| `delete` | Delete resources |
| `validate` | Validate configurations |
| `version` | Manage feature group versions |

## Command Categories

### Project Management

- `init` - Initialize a new project
- `info` - Show version information
- `env` - Manage virtual environments

### Pipeline Operations

- `run` - Execute a pipeline
- `plan` - Analyze changes and show execution plan
- `diff` - Show changes in pipeline files
- `apply` - Apply a file to production
- `dry-run` - Validate and preview without executing

### Resource Management

- `list` - List resources
- `show` - Show resource details
- `delete` - Delete resources
- `delete-table` - Delete online tables

### Feature Operations

- `validate-features` - Validate feature group data quality
- `clean` - Remove old feature data
- `debug` - Debug feature group issues

### Version Control

- `version list` - List feature group versions
- `version show` - Show version details
- `version diff` - Compare versions

### Data Operations

- `inspect` - Inspect intermediate pipeline output
- `intervals` - List completed intervals
- `repl` - Start interactive SQL REPL
- `query` - Query metrics from semantic layer

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
