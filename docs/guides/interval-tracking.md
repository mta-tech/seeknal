# Interval Tracking Guide

## Overview

Interval tracking enables incremental data processing by tracking which time ranges have been processed for each node. This is essential for:

- **Backfilling historical data** - Process missing historical time periods
- **Incremental updates** - Only process new data since last run
- **Scheduled runs** - Automatically determine pending intervals using cron schedules

## Key Concepts

### Intervals

An **interval** represents a time range `[start, end]` that has been processed for a node.

```python
from seeknal.workflow.intervals import Interval, IntervalSet

# Create an interval
interval = Interval(
    start=datetime(2024, 1, 1, 0, 0),
    end=datetime(2024, 1, 1, 23, 59, 59)
)
```

### Interval Calculator

The `IntervalCalculator` generates time intervals based on cron schedules.

```python
from seeknal.workflow.intervals import create_interval_calculator

# Daily schedule
calc = create_interval_calculator("@daily")

# Get pending intervals
pending = calc.get_pending_intervals(
    completed_intervals=[],
    start=datetime(2024, 1, 1),
    end=datetime(2024, 1, 31)
)
```

### Schedule Presets

- `@daily` - One interval per day
- `@hourly` - One interval per hour
- `@weekly` - One interval per week
- `@monthly` - One interval per month
- `@yearly` - One interval per year
- Custom cron: `0 2 * * *` (daily at 2am)

## CLI Commands

### List Completed Intervals

```bash
seeknal intervals list <node_id>
```

Shows all completed intervals for a node.

### Check Pending Intervals

```bash
seeknal intervals pending <node_id> --start 2024-01-01 --end 2024-01-31
```

Shows intervals that need to be processed.

### Backfill Missing Data

```bash
seeknal intervals backfill <node_id> \
    --start 2024-01-01 \
    --end 2024-01-31 \
    --schedule @daily
```

Processes all missing intervals for a date range.

## Incremental Runs

Use `--start` and `--end` with `seeknal run`:

```bash
# Process specific date range
seeknal run --start 2024-01-01 --end 2024-01-31

# Backfill with restart handling
seeknal run --backfill --start 2024-01-01 --end 2024-01-31
```

## Configuration

Set schedule in node YAML:

```yaml
# seeknal/transforms/my_node.yml
my_node:
  schedule: "@daily"
  sql: |
    SELECT * FROM source WHERE date >= '{{ start }}' AND date < '{{ end }}'
```

## State Tracking

Completed intervals are stored in `run_state.json`:

```json
{
  "nodes": {
    "my_node": {
      "completed_intervals": [
        ["2024-01-01T00:00:00", "2024-01-01T23:59:59"],
        ["2024-01-02T00:00:00", "2024-01-02T23:59:59"]
      ]
    }
  }
}
```

## Advanced Usage

### Custom Cron Schedules

```python
# Every 6 hours
calc = create_interval_calculator("0 */6 * * *")

# Weekly on Sunday at 2am
calc = create_interval_calculator("0 2 * * 0")
```

### Restatement Handling

Re-process specific intervals to fix bad data:

```bash
seeknal run --restate \
    --start 2024-01-01 \
    --end 2024-01-01 \
    --node my_node
```

This marks intervals for re-execution and updates restatement tracking.
