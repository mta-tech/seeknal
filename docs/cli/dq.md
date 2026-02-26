---
summary: Generate data quality dashboard from profiles and rules
read_when: You want to inspect data quality results from pipeline runs
related:
  - validate-features
  - audit
  - lineage
---

# seeknal dq

Generate a data quality dashboard from profile statistics and rule execution
results. Supports interactive HTML reports and ASCII terminal output.

## Synopsis

```bash
seeknal dq [OPTIONS] [NODE_ID]
```

## Description

The `dq` command collects data quality information from the most recent pipeline
run — including profile statistics, rule pass/fail results, and pipeline node
statuses — and generates a visual report.

## Options

| Option | Description |
|--------|-------------|
| `NODE_ID` | Focus on a specific profile or rule node |
| `--project`, `-p` | Project name |
| `--path` | Project path (default: current directory) |
| `--output`, `-o` | Output HTML file path (default: `target/dq_report.html`) |
| `--no-open` | Don't auto-open browser |
| `--ascii` | Print ASCII report to stdout instead of HTML |

## Examples

### Full DQ report (HTML)

```bash
seeknal dq
```

### ASCII report

```bash
seeknal dq --ascii
```

### Focus on specific node

```bash
seeknal dq rule.check_orders
```

### Custom output path

```bash
seeknal dq --output report.html
```

### Generate without opening browser

```bash
seeknal dq --no-open
```

## See Also

- [seeknal validate-features](validate-features.md) - Validate feature group data quality
- [seeknal audit](audit.md) - Run data quality audits
- [seeknal lineage](lineage.md) - Interactive lineage visualization
