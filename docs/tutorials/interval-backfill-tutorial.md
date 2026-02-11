# Interval Backfill Tutorial

> **Estimated Time:** 30 minutes | **Difficulty:** Intermediate

Learn how to use Seeknal's interval tracking and backfill capabilities to efficiently process time-series data and handle missing or failed data ranges.

---

## Prerequisites

- Completed [Getting Started Guide](../getting-started-comprehensive.md)
- Seeknal installed with interval tracking enabled
- Basic understanding of cron expressions

## What You'll Learn

1. **Configure interval tracking** for your data sources
2. **Process data incrementally** - only new intervals
3. **Backfill missing data** for historical ranges
4. **Handle failed intervals** with selective reprocessing

---

## Part 1: Understanding Interval Tracking (5 minutes)

### What are Intervals?

An **interval** is a time range that has been processed by Seeknal. Interval tracking enables:

- **Incremental processing** - Only process new data since last run
- **Backfill support** - Reprocess specific time ranges
- **Audit trail** - Track which intervals have been completed
- **Failure recovery** - Re-process failed intervals without affecting completed ones

### Interval Storage

Completed intervals are stored in the node state:

```python
NodeState.completed_intervals = [
    (datetime(2024, 1, 1), datetime(2024, 1, 2)),
    (datetime(2024, 1, 2), datetime(2024, 1, 3)),
    # ...
]
```

---

## Part 2: Setting Up Interval Tracking (10 minutes)

### Step 2.1: Configure Schedule

Define a cron schedule for your data source:

```python
from seeknal.workflow.intervals import IntervalCalculator
from seeknal.project import Project

# Set up your project
project = Project(name="ecommerce")
project.get_or_create()

# Define your source with interval schedule
from seeknal.flow import Flow, FlowInput, FlowOutput
from seeknal.tasks.duckdb import DuckDBTask

task = DuckDBTask()
task.add_sql("SELECT * FROM events WHERE event_time >= '__START__' AND event_time < '__END__'")

flow = Flow(
    name="events_flow",
    input=FlowInput(kind="database", value="raw_events"),
    tasks=[task],
    output=FlowOutput()
)

# Configure with schedule
flow.set_interval_schedule(schedule="@daily")
```

### Step 2.2: Run with Interval Tracking

```bash
# First run - processes all available data
seeknal run --project-path /path/to/project

# Check completed intervals
seeknal intervals show
```

Output:
```
Completed Intervals for events_flow:
  2024-01-01 to 2024-01-02 ✓
  2024-01-02 to 2024-01-03 ✓
  2024-01-03 to 2024-01-04 ✓
```

---

## Part 3: Incremental Processing (5 minutes)

### Step 3.1: Process Only New Data

When you run again, Seeknal automatically processes only new intervals:

```bash
# Second run - only processes new data
seeknal run --project-path /path/to/project

# Output: Processing 1 new interval (2024-01-04 to 2024-01-05)
```

### Step 3.2: Check Pending Intervals

```bash
# Show what's pending
seeknal intervals pending --schedule @daily

# Output:
# Pending intervals (1):
#   2024-01-05 to 2024-01-06
```

---

## Part 4: Backfill Missing Data (10 minutes)

### Step 4.1: Identify Missing Intervals

```python
from seeknal.workflow.intervals import IntervalCalculator
from datetime import datetime

calculator = IntervalCalculator()

# Find gaps in completed intervals
missing = calculator.get_missing_intervals(
    completed_intervals=[
        (datetime(2024, 1, 1), datetime(2024, 1, 3)),  # Gap here!
        (datetime(2024, 1, 5), datetime(2024, 1, 7)),
    ],
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 7),
    schedule="@daily"
)

print(f"Missing {len(missing)} intervals")
# Output: Missing 2 intervals (2024-01-03, 2024-01-04)
```

### Step 4.2: Run Backfill

```bash
# Backfill specific date range
seeknal run \
  --project-path /path/to/project \
  --start-date 2024-01-03 \
  --end-date 2024-01-04 \
  --backfill
```

### Step 4.3: Verify Backfill

```bash
seeknal intervals show

# Output now includes:
#   2024-01-03 to 2024-01-04 ✓ (backfilled)
```

---

## Part 5: Advanced Backfill Scenarios

### Scenario 1: Restate Specific Intervals

Re-process intervals due to logic changes:

```bash
# Mark intervals for reprocessing
seeknal intervals restate \
  --start 2024-01-01 \
  --end 2024-01-07 \
  --reason "Fixed aggregation bug"

# Run reprocessing
seeknal run --backfill
```

### Scenario 2: Partition-Based Backfill

For large datasets, use partition-level tracking:

```python
# Enable partition tracking
flow.set_partition_tracking(
    partition_column="event_date",
    partition_format="%Y-%m-%d"  # e.g., 2024-01-01
)
```

```bash
# Backfill specific partitions
seeknal run \
  --partitions 2024-01-01,2024-01-02,2024-01-03 \
  --backfill
```

### Scenario 3: Hourly Intervals

```python
# Set hourly schedule
flow.set_interval_schedule(schedule="0 * * * *")  # Every hour
```

```bash
# Backfill last 24 hours
seeknal run \
  --start-date "$(date -u -d '24 hours ago' +%Y-%m-%dT%H:%M:%S)" \
  --end-date "$(date -u +%Y-%m-%dT%H:%M:%S)" \
  --backfill
```

---

## Part 6: Troubleshooting

### Issue: Intervals Not Being Tracked

**Symptom:** `seeknal intervals show` shows no intervals

**Solution:** Ensure interval schedule is configured:

```python
# Check your flow has schedule set
flow = Flow.load("events_flow")
print(flow.interval_schedule)  # Should not be None
```

### Issue: Backfill Processes Too Much Data

**Symptom:** Backfill processes intervals already completed

**Solution:** Verify interval state is being saved:

```bash
# Check state file
cat target/run_state.json | grep completed_intervals
```

### Issue: Interval Overlap Detected

**Symptom:** Error about overlapping intervals

**Solution:** Intervals are automatically merged. This is normal for continuous processing:

```python
# Merged intervals look like:
merged = [
    (datetime(2024, 1, 1), datetime(2024, 1, 7)),  # Merged range
]
```

---

## Summary

You've learned how to:

1. ✅ Configure interval tracking with cron schedules
2. ✅ Process data incrementally
3. ✅ Backfill missing historical data
4. ✅ Restate intervals for reprocessing
5. ✅ Handle partition-level tracking

## Next Steps

- [Change Detection Guide](../guides/change-detection.md) - Learn about SQL-aware change detection
- [Plan/Apply Workflow Guide](../guides/plan-apply-workflow.md) - Safe deployments with environments
- [State Backends Guide](../guides/state-backends.md) - Database state for distributed execution

## Quick Reference

```bash
# Show completed intervals
seeknal intervals show

# Show pending intervals
seeknal intervals pending --schedule @daily

# Mark interval complete
seeknal intervals complete --interval "2024-01-01"

# Backfill date range
seeknal run --start-date 2024-01-01 --end-date 2024-01-31 --backfill

# Backfill missing intervals only
seeknal run --backfill

# Restate intervals
seeknal intervals restate --start 2024-01-01 --end 2024-01-07 --reason "Fixed bug"
```
