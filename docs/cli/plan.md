---
summary: Analyze changes and show execution plan
read_when: You want to see what will run before executing the pipeline
related:
  - run
  - parse
  - diff
---

# seeknal plan

Analyze changes since the last run and show what will be executed. This command
helps you understand the impact of your changes before running the pipeline.

## Synopsis

```bash
seeknal plan [OPTIONS] [ENV_NAME]
```

## Description

The `plan` command has two modes:

**Without environment name**: Shows changes since the last run and the execution
plan for the production environment.

**With environment name**: Creates an isolated environment plan with change
categorization (breaking/non-breaking/metadata).

The plan output shows:
- New nodes to be created
- Modified nodes and their change category
- Removed nodes
- Downstream impact of changes
- Execution order

## Options

| Option | Description |
|--------|-------------|
| `ENV_NAME` | Environment name (optional). Creates isolated plan if provided |
| `--project-path` | Project directory (default: current directory) |

## Examples

### Show changes since last run

```bash
seeknal plan
```

### Plan changes in dev environment

```bash
seeknal plan dev
```

### Plan for staging

```bash
seeknal plan staging
```

## Output Example

```
Building DAG from seeknal/ directory...
DAG built: 15 nodes, 18 edges

Node Summary:
  - source: 3
  - transform: 5
  - feature_group: 4
  - model: 2

Changes detected:
  [BREAKING] transform.clean_data
    - sql: WHERE clause changed
    -> REBUILD: feature_group.user_metrics, model.churn_prediction

  [CHANGED] source.orders
    - description updated
    -> REBUILD: transform.order_summary

  [NEW] transform.new_feature

  [REMOVED] transform.old_process

Execution Plan:
  1. RUN     source.orders
  2. CACHED  source.users
  3. RUN     transform.clean_data
  4. RUN     transform.new_feature
  5. RUN     transform.order_summary
  6. RUN     feature_group.user_metrics
  7. RUN     model.churn_prediction

Total: 7 nodes, 5 to run
```

## Change Categories

| Category | Description | Impact |
|----------|-------------|--------|
| BREAKING | Schema or SQL changes | Rebuild this node and all downstream |
| NON_BREAKING | Config changes | Rebuild this node only |
| METADATA | Description/tags only | No rebuild needed |

## See Also

- [seeknal run](run.md) - Execute pipeline
- [seeknal parse](parse.md) - Parse and generate manifest
- [seeknal diff](diff.md) - Show detailed file changes
