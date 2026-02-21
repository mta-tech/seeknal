---
summary: Show version information for Seeknal and its dependencies
read_when: You need to check installed versions for troubleshooting
related:
  - validate
---

# seeknal info

Display version information for Seeknal and its key dependencies including
Python, PySpark, and DuckDB.

## Synopsis

```bash
seeknal info
```

## Description

The `info` command shows version information for Seeknal and its dependencies.
This is useful for troubleshooting, reporting bugs, and verifying your
environment is correctly configured.

## Examples

### Show version information

```bash
seeknal info
```

## Output Example

```
Seeknal version: 1.0.0
Python version: 3.11.5
PySpark version: 3.5.0
DuckDB version: 0.10.0
```

## See Also

- [seeknal validate](validate.md) - Validate configurations
