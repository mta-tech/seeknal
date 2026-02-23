---
summary: Execute a YAML or Python pipeline
read_when: You want to run a data transformation pipeline
related:
  - plan
  - validate
---

# seeknal run

Execute a Seeknal pipeline.

## Synopsis

```bash
seeknal run [OPTIONS] PIPELINE_PATH
```

## Description

Executes a pipeline defined in YAML or Python, running all transformation tasks in sequence.

## Options

| Option | Description |
|--------|-------------|
| `--env` | Environment to run in |
| `--dry-run` | Preview without executing |
| `--param` | Set pipeline parameters (key=value) |
| `--feature-start-time` | Start time for feature data |

## Examples

### Run a YAML pipeline

```bash
seeknal run pipeline.yaml
```

### Run with parameters

```bash
seeknal run pipeline.yaml --param date=2024-01-01
```

### Dry run

```bash
seeknal run pipeline.yaml --dry-run
```

## See Also

- [seeknal plan](plan.md) - Preview changes
- [seeknal validate](validate.md) - Validate configuration
