---
summary: Manage execution intervals for incremental materialization
read_when: You need to track, backfill, or restate time-based data intervals
related:
  - run
  - plan
---

# seeknal intervals

Manage execution intervals for incremental materialization. Interval tracking
enables efficient incremental processing by tracking which time intervals have
been completed and which need to be executed or restated.

## Synopsis

```bash
seeknal intervals [COMMAND] [OPTIONS]
```

## Description

The `intervals` command group provides tools for managing time-based incremental
processing:

- **List**: View completed intervals for a node
- **Pending**: See intervals that haven't been executed yet
- **Restatement**: Mark intervals for reprocessing
- **Backfill**: Execute missing intervals

This is essential for pipelines that process time-series data incrementally.

## Commands

| Command | Description |
|---------|-------------|
| `intervals list <node>` | List completed intervals for a node |
| `intervals pending <node>` | Show pending intervals (not yet executed) |
| `intervals restatement-add <node>` | Mark interval for restatement |
| `intervals restatement-list <node>` | List restatement intervals |
| `intervals restatement-clear <node>` | Clear restatement intervals |
| `intervals backfill <node>` | Execute backfill for missing intervals |

## Examples

### List completed intervals

```bash
seeknal intervals list transform.clean_data
```

### Show pending intervals

```bash
seeknal intervals pending feature_group.user_features --start 2024-01-01
```

### Mark interval for restatement

```bash
seeknal intervals restatement-add feature_group.user_features \
    --start 2024-01-01 --end 2024-01-31
```

### List restatement intervals

```bash
seeknal intervals restatement-list feature_group.user_features
```

### Clear restatement intervals

```bash
seeknal intervals restatement-clear feature_group.user_features
```

### Execute backfill

```bash
seeknal intervals backfill feature_group.user_features \
    --start 2024-01-01 --end 2024-01-31
```

## Use Cases

### Incremental Processing
Track which daily/hourly intervals have been processed to avoid reprocessing.

### Backfill
Fill in missing historical data without reprocessing everything.

### Restatement
Reprocess specific time ranges when data corrections are needed.

## See Also

- [seeknal run](run.md) - Execute pipeline with interval flags
- [seeknal plan](plan.md) - Analyze changes and show plan
