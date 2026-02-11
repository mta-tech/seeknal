---
category: data-validation
component: cli-commands
tags: [validation, date-parsing, cli, error-handling, user-experience]
date_resolved: 2026-02-11
related_build: fix-integration-security-issues
related_tasks: [validation-001]
---

# Date Validation Issues with fromisoformat()

## Problem Symptom

CLI commands (materialize, cleanup, backfill, pending-intervals) accepted dates that were technically valid but unreasonable for data engineering use cases. For example:
- Dates like `2024-13-45` (invalid month/day)
- Years far in the past (`1900-01-01`) or future (`2150-01-01`)
- Dates more than a year in the future

**Issue:** `datetime.fromisoformat()` has limited validation and accepts unreasonable dates

**Impact:** Confusing error messages downstream, potential data quality issues, poor user experience.

## Investigation Steps

1. **Edge case testing** - Tested `fromisoformat()` with invalid dates like `'2024-13-45'`
2. **Behavior analysis** - Found that some invalid dates are accepted or produce unexpected results (overflow, rollover)
3. **CLI review** - Reviewed date handling in materialize, cleanup, backfill, and pending-intervals commands
4. **Domain requirements** - Identified reasonable date bounds for data engineering context

## Root Cause

`datetime.fromisoformat()` is designed for parsing ISO 8601 format strings but doesn't validate whether the date makes sense for the application domain:

```python
# Problem: fromisoformat accepts but doesn't validate
from datetime import datetime

# These work but shouldn't for data engineering:
dt = datetime.fromisoformat("1900-01-01")  # Too far in past
dt = datetime.fromisoformat("2150-01-01")  # Too far in future
dt = datetime.fromisoformat("2024-13-01")  # May raise or rollover
```

For a data engineering platform, dates need validation appropriate to the domain.

## Working Solution

Created `parse_date_safely()` function with comprehensive validation:

### 1. Safe Date Parsing Function

```python
from datetime import datetime, timedelta

def parse_date_safely(date_str: str, param_name: str = "date") -> datetime:
    """
    Parse a date string with comprehensive validation.

    Args:
        date_str: Date string in YYYY-MM-DD or ISO timestamp format
        param_name: Parameter name for error messages

    Returns:
        datetime object

    Raises:
        ValueError: If date is invalid, out of range, or too far in future
    """
    try:
        dt = datetime.fromisoformat(date_str)
    except ValueError as e:
        raise ValueError(
            f"Invalid {param_name} format: '{date_str}'. "
            f"Expected YYYY-MM-DD format. Details: {e}"
        )

    # Validate year range
    if dt.year < 2000:
        raise ValueError(
            f"Invalid {param_name}: year {dt.year} is before 2000. "
            f"Dates before 2000 are not supported."
        )

    if dt.year > 2100:
        raise ValueError(
            f"Invalid {param_name}: year {dt.year} is after 2100. "
            f"Dates after 2100 are not supported."
        )

    # Validate not too far in future
    max_future = datetime.now() + timedelta(days=365)
    if dt > max_future:
        raise ValueError(
            f"Invalid {param_name}: {dt.strftime('%Y-%m-%d')} is more than "
            f"one year in the future. Current date: {datetime.now().strftime('%Y-%m-%d')}."
        )

    return dt
```

### 2. Updated CLI Commands

Applied `parse_date_safely()` to all CLI date parameters:

```python
# materialize command
@app.command()
def materialize(
    feature_group: str = typer.Argument(...),
    start_date: str = typer.Option(..., "--start-date", "-s"),
    end_date: Optional[str] = typer.Option(None, "--end-date", "-e"),
):
    start = parse_date_safely(start_date, "start_date")
    end = parse_date_safely(end_date, "end_date") if end_date else None

# cleanup command
@app.command()
def cleanup(
    feature_group: str = typer.Argument(...),
    before_date: str = typer.Option(..., "--before-date", "-b"),
):
    before = parse_date_safely(before_date, "before_date")

# backfill command
@app.command()
def backfill(
    feature_group: str = typer.Argument(...),
    start_date: str = typer.Option(..., "--start-date", "-s"),
    end_date: str = typer.Option(..., "--end-date", "-e"),
):
    start = parse_date_safely(start_date, "start_date")
    end = parse_date_safely(end_date, "end_date")

# pending-intervals command
@app.command()
def pending_intervals(
    feature_group: str = typer.Argument(...),
    as_of_date: str = typer.Option(..., "--as-of-date", "-d"),
):
    as_of = parse_date_safely(as_of_date, "as_of_date")
```

### 3. Validation Rules

| Rule | Bound | Rationale |
|------|-------|-----------|
| Minimum year | 2000 | Data from before 2000 unlikely in this context |
| Maximum year | 2100 | Reasonable upper bound for data engineering |
| Future limit | 1 year ahead | Prevents errors from far-future dates |
| Format | YYYY-MM-DD | ISO 8601 date format |

**Result:** Invalid dates rejected with clear, actionable error messages. Users get helpful guidance on what went wrong.

## Prevention Strategies

1. **Always validate dates from user input** - Never trust raw date strings
2. **Set domain-appropriate bounds** - Consider what dates make sense for your application
3. **Provide context in error messages** - Include parameter name and expected format
4. **Test edge cases** - Leap years, month boundaries, invalid dates, far future/past
5. **Consider timezone awareness** - Use timezone-aware datetime when needed
6. **Document date expectations** - Clearly specify valid date ranges in API/docs

## Test Cases Added

```python
def test_year_before_2000_is_rejected():
    """Test that years before 2000 are rejected."""
    with pytest.raises(ValueError, match="year 1999 is before 2000"):
        parse_date_safely("1999-12-31")

def test_year_2101_is_rejected():
    """Test that years after 2100 are rejected."""
    with pytest.raises(ValueError, match="year 2101 is after 2100"):
        parse_date_safely("2101-01-01")

def test_one_year_and_one_day_ahead_is_rejected():
    """Test that dates >1 year in future are rejected."""
    future_date = (datetime.now() + timedelta(days=366)).strftime("%Y-%m-%d")
    with pytest.raises(ValueError, match="more than one year in the future"):
        parse_date_safely(future_date)

def test_invalid_month_is_rejected():
    """Test that invalid months are rejected."""
    with pytest.raises(ValueError, match="Invalid.*month"):
        parse_date_safely("2024-13-01")

def test_invalid_day_for_february_is_rejected():
    """Test that invalid days are rejected (Feb 30)."""
    with pytest.raises(ValueError, match="Invalid.*day"):
        parse_date_safely("2024-02-30")

def test_valid_date_accepts_yyyy_mm_dd():
    """Test that valid YYYY-MM-DD dates are accepted."""
    result = parse_date_safely("2024-06-15")
    assert result.year == 2024
    assert result.month == 6
    assert result.day == 15

def test_valid_date_accepts_iso_timestamp():
    """Test that valid ISO timestamps are accepted."""
    result = parse_date_safely("2024-06-15T14:30:00")
    assert result.year == 2024
    assert result.month == 6
    assert result.day == 15
    assert result.hour == 14
    assert result.minute == 30

def test_error_message_includes_parameter_name():
    """Test that error messages include the parameter name."""
    with pytest.raises(ValueError, match="Invalid start_date"):
        parse_date_safely("1999-01-01", param_name="start_date")
```

## Cross-References

- Related: `src/seeknal/cli/main.py` - CLI command implementations
- Related: `tests/cli/test_date_validation.py` - Test suite
- Python datetime docs: https://docs.python.org/3/library/datetime.html
- ISO 8601 standard: https://en.wikipedia.org/wiki/ISO_8601

## Related Patterns

This fix implements the "Safe date parsing with range validation" pattern:

```python
# CORRECT - Comprehensive validation
def parse_date_safely(date_str: str, param_name: str = "date") -> datetime:
    dt = datetime.fromisoformat(date_str)
    if dt.year < 2000 or dt.year > 2100:
        raise ValueError(f"Year out of valid range: {dt.year}")
    if dt > datetime.now() + timedelta(days=365):
        raise ValueError(f"Date too far in future: {dt}")
    return dt

# WRONG - No validation
dt = datetime.fromisoformat(date_str)  # Accepts any date
```

## Status

**Fully resolved** - All CLI date parameters use `parse_date_safely()` with comprehensive validation and clear error messages.
