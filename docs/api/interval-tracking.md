# Interval Tracking API Reference

This module provides interval calculation and tracking capabilities for time-series incremental data processing.

## Module: `seeknal.workflow.intervals`

### Classes

#### `IntervalCalculator`

Calculate time intervals for incremental processing based on cron schedules.

```python
from seeknal.workflow.intervals import IntervalCalculator

calculator = IntervalCalculator()
```

**Methods:**

##### `calculate_intervals(start_date, end_date, schedule)`

Calculate intervals for a date range based on a cron schedule.

**Parameters:**
- `start_date` (str | datetime) - Start date for interval calculation
- `end_date` (str | datetime) - End date for interval calculation
- `schedule` (str) - Cron expression or shorthand (e.g., `@daily`, `0 2 * * *`)

**Returns:** `list[tuple[datetime, datetime]]` - List of (start, end) datetime tuples

**Raises:**
- `ValueError` - If cron expression is invalid
- `ValueError` - If start_date is after end_date

**Example:**
```python
from seeknal.workflow.intervals import IntervalCalculator
from datetime import datetime

calculator = IntervalCalculator()

# Calculate daily intervals for January 2024
intervals = calculator.calculate_intervals(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31),
    schedule="@daily"
)

# Returns: [(2024-01-01 00:00, 2024-01-02 00:00), ...]
```

##### `get_next_interval(current_interval, schedule)`

Get the next interval in a sequence.

**Parameters:**
- `current_interval` (tuple[datetime, datetime]) - Current interval (start, end)
- `schedule` (str) - Cron expression or shorthand

**Returns:** `tuple[datetime, datetime]` - Next interval (start, end)

**Example:**
```python
next_interval = calculator.get_next_interval(
    current_interval=(datetime(2024, 1, 1), datetime(2024, 1, 2)),
    schedule="@daily"
)
# Returns: (2024-01-02 00:00, 2024-01-03 00:00)
```

##### `get_completed_intervals(run_state)`

Extract completed intervals from a run state.

**Parameters:**
- `run_state` (RunState) - Run state object containing node states

**Returns:** `dict[str, list[tuple[datetime, datetime]]]` - Dict mapping node_id to completed intervals

**Example:**
```python
completed = calculator.get_completed_intervals(run_state)
# Returns: {"source_node": [(2024-01-01, 2024-01-02), ...]}
```

##### `get_pending_intervals(completed_intervals, start_date, end_date, schedule)`

Find intervals that haven't been completed yet.

**Parameters:**
- `completed_intervals` (list[tuple[datetime, datetime]]) - Already completed intervals
- `start_date` (datetime) - Start date for calculation
- `end_date` (datetime) - End date for calculation
- `schedule` (str) - Cron expression or shorthand

**Returns:** `list[tuple[datetime, datetime]]` - Pending intervals

**Example:**
```python
pending = calculator.get_pending_intervals(
    completed_intervals=[(datetime(2024, 1, 1), datetime(2024, 1, 2))],
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 5),
    schedule="@daily"
)
# Returns intervals for Jan 2, 3, 4, 5 (Jan 1 already completed)
```

##### `get_missing_intervals(completed_intervals, start_date, end_date, schedule)`

Find gaps in completed intervals.

**Parameters:**
- `completed_intervals` (list[tuple[datetime, datetime]]) - Completed intervals (may have gaps)
- `start_date` (datetime) - Range start
- `end_date` (datetime) - Range end
- `schedule` (str) - Cron expression or shorthand

**Returns:** `list[tuple[datetime, datetime]]` - Missing intervals that need backfill

**Example:**
```python
missing = calculator.get_missing_intervals(
    completed_intervals=[
        (datetime(2024, 1, 1), datetime(2024, 1, 3)),  # Gap after this
        (datetime(2024, 1, 5), datetime(2024, 1, 7)),  # Gap before this
    ],
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    schedule="@daily"
)
# Returns: [(2024-01-03, 2024-01-04), (2024-01-04, 2024-01-05)]
```

##### `merge_intervals(intervals)`

Merge overlapping or adjacent intervals.

**Parameters:**
- `intervals` (list[tuple[datetime, datetime]]) - Intervals to merge

**Returns:** `list[tuple[datetime, datetime]]` - Merged intervals

**Example:**
```python
merged = calculator.merge_intervals([
    (datetime(2024, 1, 1), datetime(2024, 1, 2)),
    (datetime(2024, 1, 2), datetime(2024, 1, 3)),
    (datetime(2024, 1, 5), datetime(2024, 1, 6)),
])
# Returns: [(2024-01-01, 2024-01-03), (2024-01-05, 2024-01-06)]
```

---

### Functions

#### `parse_cron_schedule(schedule: str) -> CronSchedule`

Parse a cron expression or interval shorthand into a schedule object.

**Parameters:**
- `schedule` (str) - Cron expression (e.g., `0 2 * * *`) or shorthand (`@daily`, `@hourly`)

**Returns:** `CronSchedule` object compatible with croniter

**Supported Shorthands:**
- `@yearly` or `@annually` - Once per year
- `@monthly` - Once per month
- `@weekly` - Once per week
- `@daily` or `@midnight` - Once per day
- `@hourly` - Once per hour

**Example:**
```python
from seeknal.workflow.intervals import parse_cron_schedule

schedule = parse_cron_schedule("@daily")
# or
schedule = parse_cron_schedule("0 2 * * *")  # Daily at 2am
```

#### `interval_to_string(interval) -> str`

Convert an interval tuple to a human-readable string.

**Parameters:**
- `interval` (tuple[datetime, datetime]) - Interval to format

**Returns:** `str` - Formatted interval string

**Example:**
```python
from seeknal.workflow.intervals import interval_to_string

s = interval_to_string((datetime(2024, 1, 1), datetime(2024, 1, 2)))
# Returns: "2024-01-01 to 2024-01-02"
```

#### `string_to_interval(s: str) -> tuple[datetime, datetime]`

Parse an interval string back to a tuple.

**Parameters:**
- `s` (str) - Interval string (format: "YYYY-MM-DD to YYYY-MM-DD")

**Returns:** `tuple[datetime, datetime]` - Parsed interval

**Example:**
```python
from seeknal.workflow.intervals import string_to_interval

interval = string_to_interval("2024-01-01 to 2024-01-02")
# Returns: (datetime(2024, 1, 1), datetime(2024, 1, 2))
```

---

## CLI Commands

### `seeknal intervals`

Show completed intervals for all nodes.

**Usage:**
```bash
seeknal intervals show
```

**Output:**
```
Completed Intervals:

source_events:
  2024-01-01 to 2024-01-02 ✓
  2024-01-02 to 2024-01-03 ✓
  2024-01-03 to 2024-01-04 ✓

user_aggregations:
  2024-01-01 to 2024-01-02 ✓
  2024-01-02 to 2024-01-03 ✓
```

### `seeknal intervals pending`

Show pending intervals for a given schedule.

**Usage:**
```bash
seeknal intervals pending --schedule @daily
seeknal intervals pending --schedule "@daily" --end-date 2024-01-31
```

**Options:**
- `--schedule` - Cron expression or shorthand (required)
- `--start-date` - Start date for calculation (default: last completed + 1)
- `--end-date` - End date for calculation (default: now)

### `seeknal intervals complete`

Mark an interval as complete (manually track completion).

**Usage:**
```bash
seeknal intervals complete --interval "2024-01-01" --node source_events
```

**Options:**
- `--interval` - Interval to mark complete (format: YYYY-MM-DD or YYYY-MM-DD to YYYY-MM-DD)
- `--node` - Node ID (default: all nodes)

### `seeknal intervals restate`

Mark intervals for reprocessing (restatement).

**Usage:**
```bash
seeknal intervals restate --start 2024-01-01 --end 2024-01-07 --reason "Fixed aggregation bug"
```

**Options:**
- `--start` - Start date for restatement
- `--end` - End date for restatement
- `--reason` - Reason for restatement (logged)
- `--node` - Specific node to restate (default: all)

---

## Data Structures

### `CronSchedule`

Schedule object compatible with croniter library.

**Attributes:**
- `minute` (int) - Minute (0-59)
- `hour` (int) - Hour (0-23)
- `day` (int) - Day of month (1-31)
- `month` (int) - Month (1-12)
- `day_of_week` (int) - Day of week (0-6, 0=Monday)

### `NodeState` (Extended)

Extended node state with interval tracking.

**Attributes:**
- `completed_intervals` (list[tuple[datetime, datetime]]) - Completed intervals
- `completed_partitions` (list[str]) - Completed partition identifiers
- `restatement_intervals` (list[tuple[datetime, datetime]]) - Intervals to reprocess

---

## Examples

### Example 1: Calculate Daily Intervals

```python
from seeknal.workflow.intervals import IntervalCalculator
from datetime import datetime

calculator = IntervalCalculator()

# Get all daily intervals for January 2024
intervals = calculator.calculate_intervals(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 31),
    schedule="@daily"
)

print(f"January has {len(intervals)} daily intervals")
# Output: January has 31 daily intervals
```

### Example 2: Find Missing Intervals

```python
from seeknal.workflow.intervals import IntervalCalculator
from datetime import datetime

calculator = IntervalCalculator()

# We have intervals for Jan 1-2 and Jan 5-7
completed = [
    (datetime(2024, 1, 1), datetime(2024, 1, 3)),
    (datetime(2024, 1, 5), datetime(2024, 1, 7)),
]

# Find what's missing
missing = calculator.get_missing_intervals(
    completed_intervals=completed,
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    schedule="@daily"
)

print(f"Missing {len(missing)} intervals: {missing}")
# Output: Missing 2 intervals: [(2024-01-03, 2024-01-04), (2024-01-04, 2024-01-05)]
```

### Example 3: Process Pending Intervals

```python
from seeknal.workflow.intervals import IntervalCalculator
from datetime import datetime

calculator = IntervalCalculator()

# Get what needs to be processed
pending = calculator.get_pending_intervals(
    completed_intervals=[
        (datetime(2024, 1, 1), datetime(2024, 1, 3)),
    ],
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    schedule="@daily"
)

# Process each pending interval
for start, end in pending:
    print(f"Processing interval: {start.date()} to {end.date()}")
    # Your processing logic here
```

### Example 4: Hourly Intervals

```python
from seeknal.workflow.intervals import IntervalCalculator
from datetime import datetime, timedelta

calculator = IntervalCalculator()

# Calculate hourly intervals for a day
end = datetime(2024, 1, 2)
start = datetime(2024, 1, 1)

intervals = calculator.calculate_intervals(
    start_date=start,
    end_date=end,
    schedule="@hourly"
)

print(f"Day has {len(intervals)} hourly intervals")
# Output: Day has 24 hourly intervals
```

---

## See Also

- [Interval Tracking Guide](../guides/interval-tracking.md) - User guide for interval tracking
- [Interval Backfill Tutorial](../tutorials/interval-backfill-tutorial.md) - Tutorial with examples
- [Change Detection API](change-detection.md) - API for change detection
