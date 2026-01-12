"""
Common formatters utility module for Seeknal.

This module provides reusable formatting functions for CLI output,
tables, and status messages. These functions are used throughout
the Seeknal CLI to provide consistent formatting and styling.

Example:
    >>> from seeknal.utils.formatters import format_table, format_status_message
    >>> data = [["Name", "Age"], ["Alice", 30], ["Bob", 25]]
    >>> print(format_table(data))
    >>> print(format_status_message("Success!", "success"))
"""

from typing import List, Dict, Any, Optional, Union
from tabulate import tabulate
import typer


# =============================================================================
# Table Formatting
# =============================================================================

def format_table(
    headers: List[str],
    data: List[List[Any]],
    tablefmt: str = "simple",
    maxcolwidths: Optional[List[int]] = None,
) -> str:
    """Format data as a table.

    Args:
        headers: Column headers for the table.
        data: Table data as a list of rows (each row is a list of values).
        tablefmt: Table format style (simple, grid, pipe, html, etc.).
        maxcolwidths: Optional list of maximum column widths.

    Returns:
        A formatted table string.

    Example:
        >>> headers = ["Name", "Age", "City"]
        >>> data = [["Alice", 30, "NYC"], ["Bob", 25, "LA"]]
        >>> print(format_table(headers, data))
    """
    return tabulate(
        data,
        headers=headers,
        tablefmt=tablefmt,
        maxcolwidths=maxcolwidths,
    )


def format_dict_table(
    data: List[Dict[str, Any]],
    tablefmt: str = "simple",
    keys: Optional[List[str]] = None,
) -> str:
    """Format a list of dictionaries as a table.

    Args:
        data: List of dictionaries to format.
        tablefmt: Table format style.
        keys: Optional list of keys to include (in order). If None, uses
            all keys from the first dictionary.

    Returns:
        A formatted table string.

    Example:
        >>> data = [
        ...     {"name": "Alice", "age": 30},
        ...     {"name": "Bob", "age": 25}
        ... ]
        >>> print(format_dict_table(data, keys=["name", "age"]))
    """
    if not data:
        return "No data"

    # Use provided keys or extract from first item
    if keys is None:
        keys = list(data[0].keys())

    # Convert to list of lists
    table_data = [[row.get(key, "") for key in keys] for row in data]

    return format_table(keys, table_data, tablefmt=tablefmt)


# =============================================================================
# Status Message Formatting
# =============================================================================

def format_status_message(
    message: str,
    status: str = "info",
    bold: bool = False,
) -> str:
    """Format a status message with appropriate coloring.

    Args:
        message: The message to format.
        status: Status type (success, error, warning, info).
        bold: Whether to make the text bold.

    Returns:
        A formatted string with appropriate styling.

    Example:
        >>> print(format_status_message("Operation completed", "success"))
        >>> print(format_status_message("An error occurred", "error"))
    """
    colors = {
        "success": typer.colors.GREEN,
        "error": typer.colors.RED,
        "warning": typer.colors.YELLOW,
        "info": typer.colors.BLUE,
    }

    fg_color = colors.get(status, typer.colors.WHITE)
    return typer.style(message, fg=fg_color, bold=bold)


def format_success(message: str, symbol: str = "✓") -> str:
    """Format a success message.

    Args:
        message: The success message.
        symbol: Optional symbol to prefix (default: checkmark).

    Returns:
        A formatted success message string.
    """
    return format_status_message(f"{symbol} {message}", "success")


def format_error(message: str, symbol: str = "✗") -> str:
    """Format an error message.

    Args:
        message: The error message.
        symbol: Optional symbol to prefix (default: cross mark).

    Returns:
        A formatted error message string.
    """
    return format_status_message(f"{symbol} {message}", "error")


def format_warning(message: str, symbol: str = "⚠") -> str:
    """Format a warning message.

    Args:
        message: The warning message.
        symbol: Optional symbol to prefix (default: warning sign).

    Returns:
        A formatted warning message string.
    """
    return format_status_message(f"{symbol} {message}", "warning")


def format_info(message: str, symbol: str = "ℹ") -> str:
    """Format an info message.

    Args:
        message: The info message.
        symbol: Optional symbol to prefix (default: info sign).

    Returns:
        A formatted info message string.
    """
    return format_status_message(f"{symbol} {message}", "info")


# =============================================================================
# Diff/Change Formatting
# =============================================================================

def format_diff_item(
    item: Union[str, Dict[str, Any]],
    symbol: str = "",
    color: str = "white",
) -> str:
    """Format a diff item (added, removed, or modified).

    Args:
        item: The item to format (string or dict with 'name' and 'type').
        symbol: Symbol to prefix (e.g., '+', '-', '~').
        color: Color name (red, green, yellow, blue, white).

    Returns:
        A formatted diff item string.

    Example:
        >>> format_diff_item({"name": "age", "type": "int"}, "+", "green")
        '+ age: int'
    """
    if isinstance(item, dict):
        name = item.get("name", "unknown")
        item_type = item.get("type", "unknown")
        # Handle complex types
        if isinstance(item_type, dict):
            item_type = str(item_type.get("type", item_type))
        elif isinstance(item_type, list):
            item_type = " | ".join(str(t) for t in item_type)
        result = f"  {symbol} {name}: {item_type}"
    else:
        result = f"  {symbol} {item}"

    colors = {
        "green": typer.colors.GREEN,
        "red": typer.colors.RED,
        "yellow": typer.colors.YELLOW,
        "blue": typer.colors.BLUE,
        "white": typer.colors.WHITE,
    }

    fg_color = colors.get(color, typer.colors.WHITE)
    return typer.style(result, fg=fg_color)


def format_diff_summary(
    added: int,
    removed: int,
    modified: int,
    separator: str = "-" * 60,
) -> str:
    """Format a diff summary line.

    Args:
        added: Number of added items.
        removed: Number of removed items.
        modified: Number of modified items.
        separator: Separator string.

    Returns:
        A formatted summary string.

    Example:
        >>> print(format_diff_summary(5, 2, 1))
        ------ Summary: 5 added, 2 removed, 1 modified
    """
    return f"{separator}\nSummary: {added} added, {removed} removed, {modified} modified"


# =============================================================================
# List/Enumeration Formatting
# =============================================================================

def format_list(
    items: List[Any],
    prefix: str = "  - ",
    numbered: bool = False,
) -> str:
    """Format a list of items.

    Args:
        items: List of items to format.
        prefix: Prefix for each item (used if numbered=False).
        numbered: If True, use numbered list instead of bullet prefix.

    Returns:
        A formatted list string.

    Example:
        >>> format_list(["apple", "banana", "cherry"])
        '  - apple\n  - banana\n  - cherry'
    """
    if numbered:
        return "\n".join(f"  {i}. {item}" for i, item in enumerate(items, 1))
    return "\n".join(f"{prefix}{item}" for item in items)


def format_key_value(
    data: Dict[str, Any],
    indent: int = 2,
    separator: str = ": ",
) -> str:
    """Format key-value pairs.

    Args:
        data: Dictionary of key-value pairs.
        indent: Number of spaces for indentation.
        separator: Separator between key and value.

    Returns:
        A formatted key-value string.

    Example:
        >>> format_key_value({"name": "Alice", "age": 30})
        'name: Alice\\nage: 30'
    """
    prefix = " " * indent
    return "\n".join(f"{prefix}{key}{separator}{value}" for key, value in data.items())


# =============================================================================
# Number/Quantity Formatting
# =============================================================================

def format_number(
    value: Union[int, float],
    precision: int = 2,
) -> str:
    """Format a number with optional thousands separator.

    Args:
        value: The number to format.
        precision: Decimal places for floats.

    Returns:
        A formatted number string.

    Example:
        >>> format_number(1234567)
        '1,234,567'
        >>> format_number(1234.5678, 2)
        '1,234.57'
    """
    if isinstance(value, float):
        return f"{value:,.{precision}f}"
    return f"{value:,}"


def format_percentage(value: float, precision: int = 1) -> str:
    """Format a value as a percentage.

    Args:
        value: Decimal value (e.g., 0.123 for 12.3%).
        precision: Decimal places.

    Returns:
        A formatted percentage string.

    Example:
        >>> format_percentage(0.1234, 1)
        '12.3%'
    """
    return f"{value * 100:.{precision}f}%"


def format_bytes(size_bytes: int) -> str:
    """Format bytes in human-readable format.

    Args:
        size_bytes: Size in bytes.

    Returns:
        A formatted size string (e.g., "1.5 MB").

    Example:
        >>> format_bytes(1536000)
        '1.46 MB'
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


# =============================================================================
# Duration Formatting
# =============================================================================

def format_duration(seconds: float) -> str:
    """Format a duration in seconds to human-readable format.

    Args:
        seconds: Duration in seconds.

    Returns:
        A formatted duration string.

    Example:
        >>> format_duration(3661)
        '1h 1m 1s'
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    
    minutes = int(seconds // 60)
    seconds_remainder = int(seconds % 60)
    
    if minutes < 60:
        return f"{minutes}m {seconds_remainder}s"
    
    hours = minutes // 60
    minutes_remainder = minutes % 60
    return f"{hours}h {minutes_remainder}m {seconds_remainder}s"


# =============================================================================
# JSON/YAML Formatting
# =============================================================================

def format_json(data: Any, indent: int = 2) -> str:
    """Format data as JSON string.

    Args:
        data: Data to serialize.
        indent: JSON indentation level.

    Returns:
        A formatted JSON string.
    """
    import json
    return json.dumps(data, indent=indent, default=str)


def format_yaml(data: Any) -> str:
    """Format data as YAML string.

    Args:
        data: Data to serialize.

    Returns:
        A formatted YAML string.
    """
    import yaml
    return yaml.dump(data, default_flow_style=False, sort_keys=False)
