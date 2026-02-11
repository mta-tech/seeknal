"""
Interval calculation logic for Seeknal workflow execution.

This module provides functionality for calculating intervals based on cron schedules,
merging overlapping intervals, detecting gaps, and managing interval state for
incremental materialization.

Key features:
- Cron-based interval generation using python-croniter
- Interval merge logic for handling overlaps
- Gap detection for finding missing intervals
- Comparison between completed and expected intervals
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Set, Tuple

from croniter import croniter

logger = logging.getLogger(__name__)


@dataclass
class Interval:
    """
    Represents a time interval with start and end timestamps.

    Attributes:
        start: Start timestamp as ISO string
        end: End timestamp as ISO string
    """

    start: str
    end: str

    def to_tuple(self) -> Tuple[str, str]:
        """Convert to tuple for serialization."""
        return (self.start, self.end)

    @classmethod
    def from_tuple(cls, interval: Tuple[str, str]) -> "Interval":
        """Create from tuple."""
        return cls(start=interval[0], end=interval[1])

    def overlaps_with(self, other: "Interval") -> bool:
        """Check if this interval overlaps with another."""
        self_start = datetime.fromisoformat(self.start)
        self_end = datetime.fromisoformat(self.end)
        other_start = datetime.fromisoformat(other.start)
        other_end = datetime.fromisoformat(other.end)

        return not (self_end < other_start or self_start > other_end)

    def contains(self, other: "Interval") -> bool:
        """Check if this interval fully contains another."""
        self_start = datetime.fromisoformat(self.start)
        self_end = datetime.fromisoformat(self.end)
        other_start = datetime.fromisoformat(other.start)
        other_end = datetime.fromisoformat(other.end)

        return self_start <= other_start and self_end >= other_end

    def __lt__(self, other: "Interval") -> bool:
        """Compare intervals by start time for sorting."""
        return self.start < other.start

    def __eq__(self, other: object) -> bool:
        """Check equality of intervals."""
        if not isinstance(other, Interval):
            return NotImplemented
        return self.start == other.start and self.end == other.end


@dataclass
class IntervalSet:
    """
    A collection of non-overlapping, sorted intervals.

    Provides operations for merging intervals, finding gaps, and
    comparing interval sets.

    Attributes:
        intervals: List of Interval objects (sorted, non-overlapping)
    """

    intervals: List[Interval] = field(default_factory=list)

    def normalize(self) -> "IntervalSet":
        """
        Merge overlapping intervals and sort by start time.

        Returns:
            A new IntervalSet with merged, sorted intervals.
        """
        if not self.intervals:
            return IntervalSet(intervals=[])

        # Sort by start time
        sorted_intervals = sorted(self.intervals, key=lambda i: i.start)

        merged: List[Interval] = []
        for interval in sorted_intervals:
            if not merged:
                merged.append(interval)
            else:
                last = merged[-1]
                if interval.overlaps_with(last) or last.end == interval.start:
                    # Merge - extend the end time if needed
                    last_end = datetime.fromisoformat(last.end)
                    interval_end = datetime.fromisoformat(interval.end)
                    new_end = max(last_end, interval_end).isoformat()
                    merged[-1] = Interval(start=last.start, end=new_end)
                else:
                    merged.append(interval)

        return IntervalSet(intervals=merged)

    def gaps(self, expected: "IntervalSet") -> List[Interval]:
        """
        Find gaps in this interval set compared to expected intervals.

        Returns intervals from expected that are not covered by this set.

        Args:
            expected: The expected IntervalSet to compare against

        Returns:
            List of Interval objects representing the gaps
        """
        self_normalized = self.normalize()
        expected_normalized = expected.normalize()

        if not expected_normalized.intervals:
            return []

        if not self_normalized.intervals:
            return expected_normalized.intervals

        gaps: List[Interval] = []

        for expected_interval in expected_normalized.intervals:
            covered = False

            for completed_interval in self_normalized.intervals:
                if completed_interval.contains(expected_interval):
                    covered = True
                    break
                elif completed_interval.overlaps_with(expected_interval):
                    # Partial overlap - find the uncovered portion
                    exp_start = datetime.fromisoformat(expected_interval.start)
                    exp_end = datetime.fromisoformat(expected_interval.end)
                    comp_start = datetime.fromisoformat(completed_interval.start)
                    comp_end = datetime.fromisoformat(completed_interval.end)

                    # Gap before completed interval
                    if exp_start < comp_start:
                        gaps.append(Interval(start=expected_interval.start, end=comp_start.isoformat()))

                    # Gap after completed interval
                    if exp_end > comp_end:
                        gaps.append(Interval(start=comp_end.isoformat(), end=expected_interval.end))

                    covered = True
                    break

            if not covered:
                gaps.append(expected_interval)

        # Normalize gaps to handle any edge cases
        gap_set = IntervalSet(intervals=gaps)
        return gap_set.normalize().intervals

    def union(self, other: "IntervalSet") -> "IntervalSet":
        """
        Return the union of this interval set with another.

        Args:
            other: Another IntervalSet

        Returns:
            A new IntervalSet containing the union of both sets
        """
        return IntervalSet(intervals=self.intervals + other.intervals).normalize()

    def difference(self, other: "IntervalSet") -> "IntervalSet":
        """
        Return intervals in this set that are not in the other set.

        Args:
            other: Another IntervalSet to subtract

        Returns:
            A new IntervalSet with intervals not in the other set
        """
        result = []
        self_normalized = self.normalize()
        other_normalized = other.normalize()

        for interval in self_normalized.intervals:
            is_covered = False
            for other_interval in other_normalized.intervals:
                if other_interval.contains(interval):
                    is_covered = True
                    break
                elif other_interval.overlaps_with(interval):
                    # Partial overlap - could be more complex, but for now
                    # we'll just skip this interval
                    is_covered = True
                    break

            if not is_covered:
                result.append(interval)

        return IntervalSet(intervals=result)

    def add(self, interval: Interval) -> None:
        """Add an interval and normalize the set."""
        self.intervals.append(interval)
        normalized = self.normalize()
        self.intervals = normalized.intervals

    def contains(self, interval: Interval) -> bool:
        """Check if the set contains the given interval."""
        for existing in self.intervals:
            if existing.contains(interval):
                return True
        return False

    def as_tuples(self) -> List[Tuple[str, str]]:
        """Convert to list of tuples for serialization."""
        return [interval.to_tuple() for interval in self.intervals]

    @classmethod
    def from_tuples(cls, tuples: List[Tuple[str, str]]) -> "IntervalSet":
        """Create from list of tuples."""
        intervals = [Interval.from_tuple(t) for t in tuples]
        return cls(intervals=intervals).normalize()


class IntervalCalculator:
    """
    Calculate intervals based on cron schedules and time ranges.

    Supports various cron schedules for determining execution intervals,
    with methods for finding pending and completed intervals.

    Attributes:
        cron_expression: Cron expression for interval generation
        timezone: Optional timezone string (default: UTC)
    """

    def __init__(self, cron_expression: str, timezone: Optional[str] = None):
        """
        Initialize the IntervalCalculator.

        Args:
            cron_expression: Valid cron expression (e.g., "0 0 * * *" for daily at midnight)
            timezone: Optional timezone string (default: UTC)
        """
        self.cron_expression = cron_expression
        self.timezone = timezone or "UTC"
        self._validate_cron()

    def _validate_cron(self) -> None:
        """Validate the cron expression."""
        try:
            # Test with a base datetime to ensure it's valid
            base = datetime(2024, 1, 1)
            croniter(self.cron_expression, base)
        except Exception as e:
            raise ValueError(f"Invalid cron expression '{self.cron_expression}': {e}")

    def generate_intervals(
        self,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Interval]:
        """
        Generate intervals between start_date and end_date based on cron schedule.

        Each interval starts at a cron execution time and ends at the next
        cron execution time.

        Args:
            start_date: Start datetime for interval generation
            end_date: End datetime for interval generation

        Returns:
            List of Interval objects
        """
        try:
            base = start_date
            cron = croniter(self.cron_expression, base)

            intervals: List[Interval] = []

            # Get the first occurrence on or after start_date
            current_start = cron.get_next(datetime)

            while current_start < end_date:
                # Get the next occurrence for the end of this interval
                current_end = cron.get_next(datetime)

                # Ensure we don't go past end_date
                if current_end > end_date:
                    current_end = end_date

                intervals.append(
                    Interval(
                        start=current_start.isoformat(),
                        end=current_end.isoformat(),
                    )
                )

                current_start = current_end

                # Safety check to prevent infinite loops
                if len(intervals) > 10000:
                    logger.warning(
                        f"Interval generation exceeded 10000 intervals, stopping at {current_start}"
                    )
                    break

            return intervals

        except Exception as e:
            logger.error(f"Failed to generate intervals: {e}")
            raise

    def get_next_run_time(self, from_date: Optional[datetime] = None) -> datetime:
        """
        Get the next scheduled run time based on the cron expression.

        Args:
            from_date: Reference datetime (default: now)

        Returns:
            Next run datetime
        """
        if from_date is None:
            from_date = datetime.now()

        cron = croniter(self.cron_expression, from_date)
        return cron.get_next(datetime)

    def get_prev_run_time(self, from_date: Optional[datetime] = None) -> datetime:
        """
        Get the previous scheduled run time based on the cron expression.

        Args:
            from_date: Reference datetime (default: now)

        Returns:
            Previous run datetime
        """
        if from_date is None:
            from_date = datetime.now()

        cron = croniter(self.cron_expression, from_date)
        return cron.get_prev(datetime)

    def get_pending_intervals(
        self,
        completed_intervals: List[Tuple[str, str]],
        start_date: datetime,
        end_date: Optional[datetime] = None,
    ) -> List[Interval]:
        """
        Get intervals that need to be executed (not in completed_intervals).

        Args:
            completed_intervals: List of (start, end) tuples already completed
            start_date: Start date for interval calculation
            end_date: Optional end date (default: next cron run time)

        Returns:
            List of Interval objects that need to be executed
        """
        if end_date is None:
            end_date = self.get_next_run_time(start_date)

        # Generate expected intervals
        expected = IntervalSet(
            intervals=self.generate_intervals(start_date, end_date)
        )

        # Convert completed intervals to IntervalSet
        completed = IntervalSet.from_tuples(completed_intervals)

        # Find gaps (pending intervals)
        pending_intervals = completed.gaps(expected)

        return pending_intervals

    def get_restatement_intervals(
        self,
        restatement_ranges: List[Tuple[str, str]],
        start_date: datetime,
        end_date: Optional[datetime] = None,
    ) -> List[Interval]:
        """
        Get intervals that need to be restated based on restatement ranges.

        Args:
            restatement_ranges: List of (start, end) tuples to restate
            start_date: Start date for interval calculation
            end_date: Optional end date (default: next cron run time)

        Returns:
            List of Interval objects that need restatement
        """
        if end_date is None:
            end_date = self.get_next_run_time(start_date)

        # Generate expected intervals
        expected = IntervalSet(
            intervals=self.generate_intervals(start_date, end_date)
        )

        # Convert restatement ranges to IntervalSet
        restatement = IntervalSet.from_tuples(restatement_ranges)

        # Find overlap between expected and restatement ranges
        restatement_intervals: List[Interval] = []

        for expected_interval in expected.intervals:
            for range_interval in restatement.intervals:
                if expected_interval.overlaps_with(range_interval):
                    # Create the intersection
                    exp_start = datetime.fromisoformat(expected_interval.start)
                    exp_end = datetime.fromisoformat(expected_interval.end)
                    range_start = datetime.fromisoformat(range_interval.start)
                    range_end = datetime.fromisoformat(range_interval.end)

                    # Intersection
                    overlap_start = max(exp_start, range_start)
                    overlap_end = min(exp_end, range_end)

                    if overlap_start < overlap_end:
                        restatement_intervals.append(
                            Interval(
                                start=overlap_start.isoformat(),
                                end=overlap_end.isoformat(),
                            )
                        )

        # Normalize to merge any overlapping intervals
        return IntervalSet(intervals=restatement_intervals).normalize().intervals


def create_interval_calculator(
    cron_expression: str,
    timezone: Optional[str] = None,
) -> IntervalCalculator:
    """
    Factory function to create an IntervalCalculator.

    Provides a convenient interface for creating interval calculators
    with common cron presets.

    Args:
        cron_expression: Cron expression or preset name
        timezone: Optional timezone string

    Returns:
        Configured IntervalCalculator instance

    Examples:
        >>> # Using custom cron expression
        >>> calc = create_interval_calculator("0 0 * * *")
        >>> # Using preset (hourly)
        >>> calc = create_interval_calculator("hourly")
    """
    # Common presets
    presets = {
        "hourly": "0 * * * *",
        "daily": "0 0 * * *",
        "weekly": "0 0 * * 0",
        "monthly": "0 0 1 * *",
        "yearly": "0 0 1 1 *",
    }

    expr = presets.get(cron_expression.lower(), cron_expression)

    return IntervalCalculator(cron_expression=expr, timezone=timezone)
