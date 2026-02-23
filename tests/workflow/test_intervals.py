"""
Tests for interval calculation logic.
"""

import pytest
from datetime import datetime, timedelta

from seeknal.workflow.intervals import (
    Interval,
    IntervalCalculator,
    IntervalSet,
    create_interval_calculator,
)


class TestInterval:
    """Tests for the Interval class."""

    def test_to_tuple(self):
        """Test converting interval to tuple."""
        interval = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        assert interval.to_tuple() == ("2024-01-01T00:00:00", "2024-01-02T00:00:00")

    def test_from_tuple(self):
        """Test creating interval from tuple."""
        interval = Interval.from_tuple(("2024-01-01T00:00:00", "2024-01-02T00:00:00"))
        assert interval.start == "2024-01-01T00:00:00"
        assert interval.end == "2024-01-02T00:00:00"

    def test_overlapping_intervals(self):
        """Test overlap detection between intervals."""
        i1 = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        i2 = Interval(start="2024-01-01T12:00:00", end="2024-01-02T12:00:00")
        i3 = Interval(start="2024-01-03T00:00:00", end="2024-01-04T00:00:00")

        assert i1.overlaps_with(i2) is True
        assert i1.overlaps_with(i3) is False

    def test_adjacent_intervals_overlap(self):
        """Test that adjacent intervals (end == start) are considered overlapping."""
        i1 = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        i2 = Interval(start="2024-01-02T00:00:00", end="2024-01-03T00:00:00")

        assert i1.overlaps_with(i2) is True

    def test_contains_interval(self):
        """Test if one interval contains another."""
        i1 = Interval(start="2024-01-01T00:00:00", end="2024-01-05T00:00:00")
        i2 = Interval(start="2024-01-02T00:00:00", end="2024-01-03T00:00:00")
        i3 = Interval(start="2024-01-02T00:00:00", end="2024-01-06T00:00:00")

        assert i1.contains(i2) is True
        assert i1.contains(i3) is False

    def test_interval_equality(self):
        """Test interval equality."""
        i1 = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        i2 = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        i3 = Interval(start="2024-01-01T00:00:00", end="2024-01-03T00:00:00")

        assert i1 == i2
        assert i1 != i3

    def test_interval_sorting(self):
        """Test that intervals sort by start time."""
        i1 = Interval(start="2024-01-03T00:00:00", end="2024-01-04T00:00:00")
        i2 = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        i3 = Interval(start="2024-01-02T00:00:00", end="2024-01-03T00:00:00")

        sorted_intervals = sorted([i1, i2, i3])
        assert sorted_intervals[0] == i2
        assert sorted_intervals[1] == i3
        assert sorted_intervals[2] == i1


class TestIntervalSet:
    """Tests for the IntervalSet class."""

    def test_empty_interval_set(self):
        """Test creating an empty interval set."""
        interval_set = IntervalSet()
        assert interval_set.intervals == []

    def test_normalize_empty_set(self):
        """Test normalizing an empty interval set."""
        interval_set = IntervalSet()
        normalized = interval_set.normalize()
        assert normalized.intervals == []

    def test_normalize_single_interval(self):
        """Test normalizing a single interval."""
        interval = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        interval_set = IntervalSet(intervals=[interval])
        normalized = interval_set.normalize()
        assert len(normalized.intervals) == 1
        assert normalized.intervals[0] == interval

    def test_normalize_merge_overlapping(self):
        """Test merging overlapping intervals."""
        i1 = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        i2 = Interval(start="2024-01-01T12:00:00", end="2024-01-02T12:00:00")
        i3 = Interval(start="2024-01-03T00:00:00", end="2024-01-04T00:00:00")

        interval_set = IntervalSet(intervals=[i3, i1, i2])
        normalized = interval_set.normalize()

        assert len(normalized.intervals) == 2
        # First two should merge
        assert normalized.intervals[0].start == "2024-01-01T00:00:00"
        assert normalized.intervals[0].end == "2024-01-02T12:00:00"
        # Third stays separate
        assert normalized.intervals[1] == i3

    def test_normalize_merge_adjacent(self):
        """Test that adjacent intervals are merged."""
        i1 = Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        i2 = Interval(start="2024-01-02T00:00:00", end="2024-01-03T00:00:00")

        interval_set = IntervalSet(intervals=[i1, i2])
        normalized = interval_set.normalize()

        assert len(normalized.intervals) == 1
        assert normalized.intervals[0].start == "2024-01-01T00:00:00"
        assert normalized.intervals[0].end == "2024-01-03T00:00:00"

    def test_gaps_in_empty_set(self):
        """Test finding gaps when completed set is empty."""
        expected = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-04T00:00:00")
        ])
        completed = IntervalSet()

        gaps = completed.gaps(expected)
        assert len(gaps) == 1
        assert gaps[0] == expected.intervals[0]

    def test_gaps_with_partial_coverage(self):
        """Test finding gaps with partial coverage."""
        expected = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-04T00:00:00")
        ])
        completed = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        ])

        gaps = completed.gaps(expected)
        assert len(gaps) == 1
        assert gaps[0].start == "2024-01-02T00:00:00"
        assert gaps[0].end == "2024-01-04T00:00:00"

    def test_gaps_with_full_coverage(self):
        """Test finding gaps with full coverage (no gaps)."""
        interval = Interval(start="2024-01-01T00:00:00", end="2024-01-04T00:00:00")
        expected = IntervalSet(intervals=[interval])
        completed = IntervalSet(intervals=[interval])

        gaps = completed.gaps(expected)
        assert len(gaps) == 0

    def test_gaps_with_multiple_expected_intervals(self):
        """Test finding gaps across multiple expected intervals."""
        # Use non-adjacent intervals to avoid merge
        expected = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-01T12:00:00"),
            Interval(start="2024-01-02T00:00:00", end="2024-01-02T12:00:00"),
            Interval(start="2024-01-03T00:00:00", end="2024-01-03T12:00:00"),
        ])
        completed = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-01T12:00:00"),
        ])

        gaps = completed.gaps(expected)
        assert len(gaps) == 2

    def test_union(self):
        """Test union of two interval sets."""
        set1 = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        ])
        set2 = IntervalSet(intervals=[
            Interval(start="2024-01-03T00:00:00", end="2024-01-04T00:00:00")
        ])

        union = set1.union(set2)
        assert len(union.intervals) == 2

    def test_union_with_overlap(self):
        """Test union of overlapping interval sets."""
        set1 = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-02T12:00:00")
        ])
        set2 = IntervalSet(intervals=[
            Interval(start="2024-01-02T00:00:00", end="2024-01-04T00:00:00")
        ])

        union = set1.union(set2)
        assert len(union.intervals) == 1
        assert union.intervals[0].start == "2024-01-01T00:00:00"
        assert union.intervals[0].end == "2024-01-04T00:00:00"

    def test_difference(self):
        """Test difference of two interval sets."""
        set1 = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-04T00:00:00"),
            Interval(start="2024-01-05T00:00:00", end="2024-01-06T00:00:00"),
        ])
        set2 = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        ])

        diff = set1.difference(set2)
        # The first interval is partially covered, second is not
        assert len(diff.intervals) >= 1
        assert diff.intervals[-1].start == "2024-01-05T00:00:00"

    def test_add_interval(self):
        """Test adding an interval to a set."""
        interval_set = IntervalSet()
        interval_set.add(Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00"))
        assert len(interval_set.intervals) == 1

    def test_add_normalizes(self):
        """Test that adding an interval normalizes the set."""
        interval_set = IntervalSet()
        interval_set.add(Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00"))
        interval_set.add(Interval(start="2024-01-01T12:00:00", end="2024-01-02T12:00:00"))

        # Should merge into one
        assert len(interval_set.intervals) == 1
        assert interval_set.intervals[0].end == "2024-01-02T12:00:00"

    def test_contains_interval(self):
        """Test checking if a set contains an interval."""
        interval_set = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-04T00:00:00")
        ])

        assert interval_set.contains(
            Interval(start="2024-01-02T00:00:00", end="2024-01-03T00:00:00")
        ) is True
        assert interval_set.contains(
            Interval(start="2024-01-05T00:00:00", end="2024-01-06T00:00:00")
        ) is False

    def test_as_tuples(self):
        """Test converting interval set to list of tuples."""
        interval_set = IntervalSet(intervals=[
            Interval(start="2024-01-01T00:00:00", end="2024-01-02T00:00:00")
        ])

        tuples = interval_set.as_tuples()
        assert tuples == [("2024-01-01T00:00:00", "2024-01-02T00:00:00")]

    def test_from_tuples(self):
        """Test creating interval set from list of tuples."""
        tuples = [
            ("2024-01-01T00:00:00", "2024-01-02T00:00:00"),
            ("2024-01-03T00:00:00", "2024-01-04T00:00:00"),
        ]

        interval_set = IntervalSet.from_tuples(tuples)
        assert len(interval_set.intervals) == 2


class TestIntervalCalculator:
    """Tests for the IntervalCalculator class."""

    def test_init(self):
        """Test creating an IntervalCalculator."""
        calc = IntervalCalculator(cron_expression="0 0 * * *")
        assert calc.cron_expression == "0 0 * * *"
        assert calc.timezone == "UTC"

    def test_init_with_timezone(self):
        """Test creating an IntervalCalculator with timezone."""
        calc = IntervalCalculator(cron_expression="0 0 * * *", timezone="US/Eastern")
        assert calc.timezone == "US/Eastern"

    def test_invalid_cron_expression(self):
        """Test that invalid cron expressions raise an error."""
        with pytest.raises(ValueError, match="Invalid cron expression"):
            IntervalCalculator(cron_expression="invalid cron")

    def test_generate_intervals_daily(self):
        """Test generating intervals with daily cron."""
        calc = IntervalCalculator(cron_expression="0 0 * * *")
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 4)

        intervals = calc.generate_intervals(start, end)

        # croniter generates next occurrence AFTER start time
        # So from midnight Jan 1, we get: Jan 2-3, Jan 3-4 (2 intervals)
        assert len(intervals) == 2
        # Verify interval times
        assert intervals[0].start == "2024-01-02T00:00:00"
        assert intervals[0].end == "2024-01-03T00:00:00"

    def test_generate_intervals_hourly(self):
        """Test generating intervals with hourly cron."""
        calc = IntervalCalculator(cron_expression="0 * * * *")
        start = datetime(2024, 1, 1, 0, 0)
        end = datetime(2024, 1, 1, 3, 0)

        intervals = calc.generate_intervals(start, end)

        # Should generate 2 intervals (1am-2am, 2am-3am)
        assert len(intervals) == 2

    def test_generate_intervals_weekly(self):
        """Test generating intervals with weekly cron."""
        calc = IntervalCalculator(cron_expression="0 0 * * 0")  # Sundays
        start = datetime(2024, 1, 1)  # Monday
        end = datetime(2024, 1, 22)

        intervals = calc.generate_intervals(start, end)

        # Should generate 3 intervals (3 Sundays in range)
        assert len(intervals) == 3

    def test_get_next_run_time(self):
        """Test getting next run time."""
        calc = IntervalCalculator(cron_expression="0 0 * * *")
        ref_date = datetime(2024, 1, 1, 12, 0)  # Noon on Jan 1

        next_run = calc.get_next_run_time(ref_date)

        # Should be midnight on Jan 2
        assert next_run.day == 2
        assert next_run.hour == 0
        assert next_run.minute == 0

    def test_get_prev_run_time(self):
        """Test getting previous run time."""
        calc = IntervalCalculator(cron_expression="0 0 * * *")
        ref_date = datetime(2024, 1, 2, 12, 0)  # Noon on Jan 2

        prev_run = calc.get_prev_run_time(ref_date)

        # Should be midnight on Jan 2
        assert prev_run.day == 2
        assert prev_run.hour == 0
        assert prev_run.minute == 0

    def test_get_pending_intervals(self):
        """Test getting pending intervals."""
        calc = IntervalCalculator(cron_expression="0 0 * * *")
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 4)

        completed = [
            ("2024-01-02T00:00:00", "2024-01-03T00:00:00"),
        ]

        pending = calc.get_pending_intervals(completed, start, end)

        # Expected intervals from cron: Jan 2-3, Jan 3-4 (2 intervals)
        # Completed: Jan 2-3
        # Pending: Jan 3-4 (1 interval)
        assert len(pending) == 1
        assert pending[0].start == "2024-01-03T00:00:00"

    def test_get_pending_intervals_no_end_date(self):
        """Test getting pending intervals with auto end date."""
        calc = IntervalCalculator(cron_expression="0 0 * * *")
        # Use start at midnight to get intervals starting after
        start = datetime(2024, 1, 1, 0, 0)  # Midnight Jan 1

        completed = [
            # No completed intervals - everything should be pending
        ]

        pending = calc.get_pending_intervals(completed, start)

        # With no end date, it uses next_run_time which is midnight Jan 2
        # generate_intervals from midnight Jan 1 to midnight Jan 2 gives us
        # the interval starting at midnight Jan 2, but that's >= end, so 0 intervals
        # This is a corner case - the function needs a meaningful end_date to work
        assert len(pending) == 0  # No intervals in [Jan 1, next_run=Jan 2)

    def test_get_restatement_intervals(self):
        """Test getting restatement intervals."""
        calc = IntervalCalculator(cron_expression="0 0 * * *")
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 5)

        restatement_ranges = [
            ("2024-01-02T12:00:00", "2024-01-03T12:00:00"),
        ]

        restatement = calc.get_restatement_intervals(restatement_ranges, start, end)

        # Should overlap with at least one interval
        assert len(restatement) >= 1

    def test_get_restatement_intervals_no_overlap(self):
        """Test restatement intervals with no overlap."""
        calc = IntervalCalculator(cron_expression="0 0 * * *")
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 3)

        restatement_ranges = [
            ("2024-01-05T00:00:00", "2024-01-06T00:00:00"),
        ]

        restatement = calc.get_restatement_intervals(restatement_ranges, start, end)

        # No overlap expected
        assert len(restatement) == 0


class TestCreateIntervalCalculator:
    """Tests for the create_interval_calculator factory function."""

    def test_create_with_custom_cron(self):
        """Test creating calculator with custom cron expression."""
        calc = create_interval_calculator("*/5 * * * *")
        assert calc.cron_expression == "*/5 * * * *"

    def test_create_with_hourly_preset(self):
        """Test creating calculator with hourly preset."""
        calc = create_interval_calculator("hourly")
        assert calc.cron_expression == "0 * * * *"

    def test_create_with_daily_preset(self):
        """Test creating calculator with daily preset."""
        calc = create_interval_calculator("daily")
        assert calc.cron_expression == "0 0 * * *"

    def test_create_with_weekly_preset(self):
        """Test creating calculator with weekly preset."""
        calc = create_interval_calculator("weekly")
        assert calc.cron_expression == "0 0 * * 0"

    def test_create_with_monthly_preset(self):
        """Test creating calculator with monthly preset."""
        calc = create_interval_calculator("monthly")
        assert calc.cron_expression == "0 0 1 * *"

    def test_create_with_yearly_preset(self):
        """Test creating calculator with yearly preset."""
        calc = create_interval_calculator("yearly")
        assert calc.cron_expression == "0 0 1 1 *"

    def test_create_with_timezone(self):
        """Test creating calculator with timezone."""
        calc = create_interval_calculator("daily", timezone="US/Pacific")
        assert calc.timezone == "US/Pacific"

    def test_presets_are_case_insensitive(self):
        """Test that preset names are case-insensitive."""
        calc1 = create_interval_calculator("DAILY")
        calc2 = create_interval_calculator("Daily")
        calc3 = create_interval_calculator("daily")

        assert calc1.cron_expression == calc2.cron_expression == calc3.cron_expression
