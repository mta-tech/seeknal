---
summary: Run data quality audits on cached node outputs
read_when: You want to verify data quality without re-running the pipeline
related:
  - validate-features
  - validate
  - run
---

# seeknal audit

Run data quality audits on cached node outputs from the last pipeline execution.
Audits are defined in node YAML configurations and executed against the cached
parquet files in the target directory.

## Synopsis

```bash
seeknal audit [OPTIONS] [NODE]
```

## Description

The `audit` command executes audit rules defined in your YAML node configurations
against the cached outputs from the last `seeknal run` execution. This allows you
to verify data quality without re-executing the entire pipeline.

Audits are defined using the `audits:` key in your node YAML files and can include
checks like row count thresholds, null value checks, uniqueness constraints, and
custom SQL assertions.

## Options

| Option | Description |
|--------|-------------|
| `NODE` | Specific node to audit (e.g., `source.users`). If omitted, audits all nodes |
| `--target-path` | Path to target directory (default: `target`) |

## Examples

### Audit all nodes

```bash
seeknal audit
```

### Audit a specific node

```bash
seeknal audit source.users
```

### Audit a transform

```bash
seeknal audit transform.clean_data
```

## Defining Audits in YAML

```yaml
# seeknal/transforms/clean_data.yml
kind: transform
name: clean_data
description: Clean and validate user data
audits:
  - type: row_count
    min_count: 100
  - type: not_null
    columns: [user_id, email]
  - type: unique
    columns: [user_id]
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | All audits passed |
| 1 | One or more audits failed |

## See Also

- [seeknal validate-features](validate-features.md) - Validate feature group data
- [seeknal validate](validate.md) - Validate configurations
- [seeknal run](run.md) - Execute pipeline
