---
summary: Generate interactive lineage visualization
read_when: You want to see the data lineage or DAG of your pipeline
related:
  - plan
  - run
  - dq
---

# seeknal lineage

Generate an interactive lineage visualization showing the data flow between
pipeline nodes. Supports HTML output with interactive features and ASCII
tree output for terminal use.

## Synopsis

```bash
seeknal lineage [OPTIONS] [NODE_ID]
```

## Description

The `lineage` command builds the pipeline DAG and generates a visualization
showing how data flows from sources through transforms to feature groups
and models.

## Options

| Option | Description |
|--------|-------------|
| `NODE_ID` | Node to focus on (e.g., `transform.clean_orders`) |
| `--column`, `-c` | Column to trace lineage for (requires node argument) |
| `--project`, `-p` | Project name |
| `--path` | Project path (default: current directory) |
| `--output`, `-o` | Output HTML file path (default: `target/lineage.html`) |
| `--no-open` | Don't auto-open browser |
| `--ascii` | Print DAG as ASCII tree to stdout instead of HTML |

## Examples

### Full DAG visualization

```bash
seeknal lineage
```

### Focus on a specific node

```bash
seeknal lineage transform.clean_orders
```

### Trace column lineage

```bash
seeknal lineage transform.clean_orders --column total_revenue
```

### ASCII tree output

```bash
seeknal lineage --ascii
seeknal lineage feature_group.customer_features --ascii
```

### Custom output path

```bash
seeknal lineage --output dag.html
```

## See Also

- [seeknal plan](plan.md) - Preview changes and execution order
- [seeknal dq](dq.md) - Data quality dashboard
