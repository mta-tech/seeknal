---
summary: Validate configurations and connections
read_when: You want to verify your Seeknal setup is working correctly
related:
  - validate-features
  - connection-test
  - dry-run
---

# seeknal validate

Validate Seeknal configurations and database connections. This command checks
that your environment is properly configured before running pipelines.

## Synopsis

```bash
seeknal validate [OPTIONS]
```

## Description

The `validate` command performs the following checks:

1. **Config file**: Verifies `config.toml` exists and is readable
2. **Database connection**: Tests the SQLite/Turso connection
3. **Projects**: Lists available projects in the database

Run this command after installation or when troubleshooting connection issues.

## Options

| Option | Description |
|--------|-------------|
| `--config`, `-c` | Path to config file (default: `~/.seeknal/config.toml`) |

## Examples

### Validate default configuration

```bash
seeknal validate
```

### Validate with custom config path

```bash
seeknal validate --config /path/to/config.toml
```

## Output Example

```
Validating configuration...
ℹ Config file found: ~/.seeknal/config.toml
ℹ Validating database connection...
✓ Database connection successful (3 projects found)
✓ All validations passed
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | All validations passed |
| 1 | Validation failed |

## See Also

- [seeknal validate-features](validate-features.md) - Validate feature group data
- [seeknal connection-test](connection-test.md) - Test database connectivity
- [seeknal dry-run](dry-run.md) - Validate pipeline files
