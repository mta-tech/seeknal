---
summary: Test database connectivity
read_when: You need to verify a database connection is working
related:
  - validate
  - source
---

# seeknal connection-test

Test connectivity to a database using a connection profile or direct URL.
Currently supports StarRocks databases.

## Synopsis

```bash
seeknal connection-test [OPTIONS] NAME
```

## Description

The `connection-test` command verifies that Seeknal can successfully connect to
a database. This is useful for troubleshooting connection issues and validating
connection profiles before running pipelines.

You can test a connection using either:
1. A profile name defined in `profiles.yml`
2. A direct database URL

## Options

| Option | Description |
|--------|-------------|
| `NAME` | Connection profile name or database URL (e.g., `starrocks://...`) |
| `--profile`, `-p` | Profile name in profiles.yml (default: `default`) |

## Examples

### Test a saved connection profile

```bash
seeknal connection-test default
```

### Test with a specific profile

```bash
seeknal connection-test analytics --profile production
```

### Test with a direct StarRocks URL

```bash
seeknal connection-test starrocks://user:password@host:9030/database
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Connection successful |
| 1 | Connection failed |

## See Also

- [seeknal validate](validate.md) - Validate configurations
- [seeknal source](source.md) - Manage data sources
