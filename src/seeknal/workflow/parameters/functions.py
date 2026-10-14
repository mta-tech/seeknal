"""Built-in parameter functions for YAML parameterization.

Functions support the syntax:
- {{today}}           -> 2025-02-10
- {{today(-1)}}       -> 2025-02-09
- {{month_start}}     -> 2025-02-01
- {{env:VAR|default}} -> os.environ.get("VAR", "default")
- {{run_id}}          -> UUID
- {{run_date}}        -> ISO timestamp
"""

from datetime import date, timedelta
import os
from typing import Optional


def today(offset_days: int = 0, base_date: Optional[date] = None) -> str:
    """Return today's date in ISO format.

    Args:
        offset_days: Days offset from today (negative for past)
        base_date: Date to treat as "today" (e.g. from ``seeknal run --date``)

    Returns:
        ISO formatted date string (YYYY-MM-DD)

    Examples:
        >>> today()  # Returns today's date like "2025-02-10"
        >>> today(-1)  # Returns yesterday
        >>> today(7)  # Returns a week from now
    """
    d = (base_date or date.today()) + timedelta(days=offset_days)
    return d.isoformat()


def yesterday(base_date: Optional[date] = None) -> str:
    """Return yesterday's date in ISO format.

    Convenience function for today(-1).

    Args:
        base_date: Date to treat as "today" (e.g. from ``seeknal run --date``)

    Returns:
        ISO formatted date string (YYYY-MM-DD)
    """
    return today(-1, base_date=base_date)


def month_start(months_offset: int = 0, base_date: Optional[date] = None) -> str:
    """Return first day of current/offset month.

    Args:
        months_offset: Months offset (negative for past months)
        base_date: Date to treat as "today" (e.g. from ``seeknal run --date``)

    Returns:
        ISO formatted date string (YYYY-MM-DD)

    Examples:
        >>> month_start()  # First day of current month
        >>> month_start(-1)  # First day of last month
    """
    today_date = base_date or date.today()

    # Calculate target month/year
    year = today_date.year
    month = today_date.month + months_offset

    # Handle year rollover
    while month < 1:
        month += 12
        year -= 1
    while month > 12:
        month -= 12
        year += 1

    return date(year, month, 1).isoformat()


def year_start(years_offset: int = 0, base_date: Optional[date] = None) -> str:
    """Return first day of current/offset year.

    Args:
        years_offset: Years offset (negative for past years)
        base_date: Date to treat as "today" (e.g. from ``seeknal run --date``)

    Returns:
        ISO formatted date string (YYYY-MM-DD)

    Examples:
        >>> year_start()  # First day of current year
        >>> year_start(-1)  # First day of last year
    """
    today_date = base_date or date.today()
    return date(today_date.year + years_offset, 1, 1).isoformat()


def env_var(var_name: str, default: Optional[str] = None) -> str:
    """Get environment variable with optional default.

    Args:
        var_name: Environment variable name
        default: Default value if not set

    Returns:
        Environment variable value or default

    Raises:
        ValueError: If var_name not set and no default provided

    Examples:
        >>> env_var("HOME")  # Returns HOME directory
        >>> env_var("MISSING", "default")  # Returns "default"
        >>> env_var("API_KEY")  # Raises ValueError if not set
    """
    value = os.environ.get(var_name)
    if value is None:
        if default is not None:
            return default
        raise ValueError(
            f"Environment variable '{var_name}' not set and no default provided"
        )
    return value


# Built-in function registry for easy lookup
# Format: function_name -> (callable, takes_args)
FUNCTION_REGISTRY = {
    "today": (today, True),
    "yesterday": (yesterday, False),
    "month_start": (month_start, True),
    "year_start": (year_start, True),
    "env": (env_var, True),
}


# Functions whose notion of "today" follows ``seeknal run --date``
DATE_FUNCTIONS = frozenset({"today", "yesterday", "month_start", "year_start"})


def get_function(name: str):
    """Get a parameter function by name.

    Args:
        name: Function name

    Returns:
        Tuple of (function, takes_args) or None if not found
    """
    return FUNCTION_REGISTRY.get(name)
