"""Shared type conversion utilities for parameter resolution."""

from typing import Any, Type


def convert_to_bool(value: Any) -> bool:
    """Convert value to boolean with consistent rules.

    Accepts multiple true/false representations:
    - True: 'true', '1', 'yes', 'on' (case-insensitive)
    - False: 'false', '0', 'no', 'off' (case-insensitive)
    - For non-strings: uses Python's truthiness

    Args:
        value: The value to convert to boolean.

    Returns:
        True or False based on the conversion rules.

    Examples:
        >>> convert_to_bool('true')
        True
        >>> convert_to_bool('TRUE')
        True
        >>> convert_to_bool('1')
        True
        >>> convert_to_bool('yes')
        True
        >>> convert_to_bool('on')
        True
        >>> convert_to_bool('false')
        False
        >>> convert_to_bool('0')
        False
        >>> convert_to_bool('no')
        False
        >>> convert_to_bool('off')
        False
        >>> convert_to_bool(True)
        True
        >>> convert_to_bool(False)
        False
        >>> convert_to_bool(1)
        True
        >>> convert_to_bool(0)
        False
        >>> convert_to_bool('')
        False
        >>> convert_to_bool('random')
        True  # Non-empty strings are truthy
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.lower().strip()
        if normalized in ('true', '1', 'yes', 'on'):
            return True
        if normalized in ('false', '0', 'no', 'off'):
            return False
    return bool(value)


def convert_to_type(value: Any, target_type: Type) -> Any:
    """Convert value to target type with consistent rules.

    Args:
        value: The value to convert.
        target_type: The target type (bool, int, float, str).

    Returns:
        The converted value.

    Examples:
        >>> convert_to_type('true', bool)
        True
        >>> convert_to_type('100', int)
        100
        >>> convert_to_type('3.14', float)
        3.14
        >>> convert_to_type(123, str)
        '123'
    """
    if target_type == bool:
        return convert_to_bool(value)
    if target_type == int:
        return int(value)
    if target_type == float:
        return float(value)
    if target_type == str:
        return str(value)
    # For other types, try direct conversion
    return target_type(value)
