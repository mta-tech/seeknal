"""Helper functions for accessing resolved parameters in Python scripts.

Provides a clean, explicit API for parameter access with type conversion.
"""

import os
from typing import Any, Dict, Optional, Type, TypeVar

T = TypeVar('T')


def get_param(
    name: str,
    default: Optional[Any] = None,
    type: Optional[Type[T]] = None,
) -> Any:
    """Get a resolved parameter value.

    Parameters are resolved from YAML and made available via environment
    variables with a SEEKNAL_PARAM_ prefix.

    Args:
        name: Parameter name (without prefix)
        default: Default value if parameter not found
        type: Expected type for conversion (int, float, bool, str)

    Returns:
        Parameter value, converted to specified type if provided

    Raises:
        KeyError: If parameter not found and no default provided

    Examples:
        Get a string parameter:
        >>> run_date = get_param("run_date")

        Get with type conversion:
        >>> batch_size = get_param("batch_size", type=int)
        >>> ratio = get_param("ratio", type=float)

        Get with default:
        >>> region = get_param("region", default="us-east-1")

        Boolean conversion:
        >>> enabled = get_param("enabled", type=bool)
    """
    env_name = f"SEEKNAL_PARAM_{name.upper()}"
    value = os.environ.get(env_name, default)

    if value is None:
        if default is not None:
            return default
        raise KeyError(
            f"Parameter '{name}' not found and no default provided. "
            f"Looking for environment variable: {env_name}"
        )

    # Type conversion
    if type is not None:
        if type == bool:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        if type == int:
            return int(value)
        if type == float:
            return float(value)
        if type == str:
            return str(value)
        # For other types, try direct conversion
        return type(value)

    return value


def list_params() -> Dict[str, str]:
    """List all available Seeknal parameters.

    Returns:
        Dictionary of parameter names to values (without prefix)

    Examples:
        >>> list_params()
        {'run_date': '2025-02-10', 'run_id': 'abc-123', 'batch_size': '100'}
    """
    params = {}
    prefix = "SEEKNAL_PARAM_"
    for key, value in os.environ.items():
        if key.startswith(prefix):
            param_name = key[len(prefix):].lower()
            params[param_name] = value
    return params


def has_param(name: str) -> bool:
    """Check if a parameter exists.

    Args:
        name: Parameter name (without prefix)

    Returns:
        True if parameter exists, False otherwise

    Examples:
        >>> has_param("run_date")
        True
        >>> has_param("nonexistent")
        False
    """
    env_name = f"SEEKNAL_PARAM_{name.upper()}"
    return env_name in os.environ
