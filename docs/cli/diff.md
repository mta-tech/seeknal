---
summary: Show changes in pipeline files since last apply
read_when: You want to see what has changed in your pipeline definitions
related:
  - plan
  - apply
  - parse
---

# seeknal diff

Show changes in pipeline files since the last apply. This command works like
`git diff` for your Seeknal pipelines, showing unified YAML diffs with semantic
annotations (BREAKING/NON_BREAKING/METADATA).

## Synopsis

```bash
seeknal diff [OPTIONS] [NODE]
```

## Description

The `diff` command compares your current pipeline YAML files against the last
saved manifest. It categorizes changes by their impact level:

- **BREAKING**: Changes that require rebuilding downstream nodes
- **NON_BREAKING**: Changes that only require rebuilding this node
- **METADATA**: Changes that don't affect data (description, tags)

This helps you understand the impact of your changes before running the pipeline.

## Options

| Option | Description |
|--------|-------------|
| `NODE` | Specific node to diff (e.g., `sources/orders`). If omitted, shows all changes |
| `--type`, `-t` | Filter by node type |
| `--stat` | Show summary statistics only (like git diff --stat) |
| `--project-path`, `-p` | Project directory (default: current directory) |

## Examples

### Show all changes

```bash
seeknal diff
```

### Diff a specific node

```bash
seeknal diff sources/orders
```

### Filter by node type

```bash
seeknal diff --type transforms
```

### Show summary statistics

```bash
seeknal diff --stat
```

## Output Format

```
Changes since last apply:

  Modified:
    seeknal/transforms/clean_data.yml    [BREAKING] +5 -2
    seeknal/sources/users.yml            [METADATA] +1 -0

  New (not yet applied):
    seeknal/feature_groups/user_behavior.yml

  2 modified, 1 new, 0 deleted
```

## See Also

- [seeknal plan](plan.md) - Analyze changes and show execution plan
- [seeknal apply](apply.md) - Apply file to production
- [seeknal parse](parse.md) - Parse and generate manifest
